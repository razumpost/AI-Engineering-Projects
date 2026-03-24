from __future__ import annotations

from typing import Any

from ..knowledge.models import ProjectRequirements
from ..normalization.candidate_classifier import classify_candidates
from .plan_models import TopologyDecision


def _pool_items(pool: Any) -> list[Any]:
    return list(getattr(pool, "items", []) or [])


def _classified_by_id(pool: Any) -> dict[str, Any]:
    return {c.candidate_id: c for c in classify_candidates(_pool_items(pool))}


def _line_qty(line: Any) -> float:
    try:
        return float(getattr(line, "qty", 0) or 0)
    except Exception:
        return 0.0


def _set_line_qty(line: Any, qty: int | float) -> None:
    setattr(line, "qty", int(qty) if float(qty).is_integer() else float(qty))


def _text(line: Any) -> str:
    return " ".join(
        [
            str(getattr(line, "sku", "") or ""),
            str(getattr(line, "manufacturer", "") or ""),
            str(getattr(line, "name", "") or ""),
            str(getattr(line, "description", "") or ""),
        ]
    ).casefold()


def _line_key(line: Any) -> str:
    # Prefer stable domain id, fallback to python identity.
    lid = getattr(line, "line_id", None)
    return str(lid) if lid else f"pyid:{id(line)}"


def _drop_lines(spec: Any, to_drop: list[Any]) -> None:
    if not to_drop:
        return
    drop_keys = {_line_key(x) for x in to_drop}
    items = list(getattr(spec, "items", []) or [])
    setattr(spec, "items", [x for x in items if _line_key(x) not in drop_keys])


def _family_lines(spec: Any, source_pool: Any) -> dict[str, list[Any]]:
    cls_by_id = _classified_by_id(source_pool)
    out: dict[str, list[Any]] = {}
    for line in list(getattr(spec, "items", []) or []):
        cid = getattr(line, "candidate_id", None)
        if not cid:
            continue
        cls = cls_by_id.get(cid)
        if not cls or not cls.family:
            continue
        out.setdefault(cls.family, []).append(line)
    return out


def _pick_best_line(lines: list[Any]) -> Any:
    def score(line: Any) -> tuple[float, float, float]:
        # Prefer known price, then larger qty, then longer description
        price = 0.0
        try:
            price = float(getattr(line, "unit_price_rub", 0) or 0)
        except Exception:
            price = 0.0
        q = _line_qty(line)
        desc_len = float(len(str(getattr(line, "description", "") or "")))
        return (1.0 if price > 0 else 0.0, q, desc_len)

    return sorted(lines, key=score, reverse=True)[0]


def resolve_quantities(spec: Any, source_pool: Any, requirements: ProjectRequirements, topology: TopologyDecision) -> list[str]:
    warnings: list[str] = []

    seat = int(requirements.caps.seat_count or 0)
    cam = int(requirements.caps.camera_count or 1)

    fam = _family_lines(spec, source_pool)

    # --- Remove videobars in discussion/camera configs (by text safety)
    has_discussion = bool(fam.get("delegate_unit") or fam.get("chairman_unit") or fam.get("conference_controller") or fam.get("conference_central_unit"))
    if cam >= 1 or has_discussion or topology.topology_key == "meeting_room_delegate_dsp":
        vb = [x for x in list(getattr(spec, "items", []) or []) if ("видеобар" in _text(x) or "videobar" in _text(x))]
        if vb:
            _drop_lines(spec, vb)
            warnings.append("[conflict] videobar removed (redundant for meeting room with cameras/discussion).")
            fam = _family_lines(spec, source_pool)

    # --- Remove LED-like lines in meeting_room (safety)
    if requirements.room_type == "meeting_room":
        led = [x for x in list(getattr(spec, "items", []) or []) if ("светодиод" in _text(x) or "шаг пикс" in _text(x) or "яркость" in _text(x))]
        if led:
            _drop_lines(spec, led)
            warnings.append("[meeting_room] removed LED screen items (not requested).")
            fam = _family_lines(spec, source_pool)

    # --- DISPLAY: keep exactly ONE across display_panel + interactive_panel
    display_lines: list[Any] = []
    for f in ("display_panel", "interactive_panel"):
        display_lines.extend(fam.get(f, []))
    if display_lines:
        best = _pick_best_line(display_lines)
        _set_line_qty(best, 1)
        _drop_lines(spec, [x for x in display_lines if _line_key(x) != _line_key(best)])
        fam = _family_lines(spec, source_pool)

    # --- CAMERAS: keep one line, qty = requested cam count
    camera_lines: list[Any] = []
    for f in ("ptz_camera", "fixed_conference_camera"):
        camera_lines.extend(fam.get(f, []))
    if not camera_lines:
        camera_lines = [x for x in list(getattr(spec, "items", []) or []) if "камера" in _text(x) or "ptz" in _text(x)]
    if camera_lines:
        best = _pick_best_line(camera_lines)
        _set_line_qty(best, cam)
        _drop_lines(spec, [x for x in camera_lines if _line_key(x) != _line_key(best)])
        fam = _family_lines(spec, source_pool)

    # --- CONFERENCE singletons
    for f in ("conference_controller", "conference_central_unit", "dsp"):
        lines = fam.get(f, [])
        if lines:
            best = _pick_best_line(lines)
            _set_line_qty(best, 1)
            _drop_lines(spec, [x for x in lines if _line_key(x) != _line_key(best)])
    fam = _family_lines(spec, source_pool)

    # --- CHAIRMAN 1шт; DELEGATES = seat-1
    for line in fam.get("chairman_unit", []):
        _set_line_qty(line, 1)

    delegate_lines = fam.get("delegate_unit", [])
    if delegate_lines and seat > 0:
        best = _pick_best_line(delegate_lines)
        delegates = max(1, seat - 1) if fam.get("chairman_unit") else seat
        _set_line_qty(best, delegates)
        _drop_lines(spec, [x for x in delegate_lines if _line_key(x) != _line_key(best)])

    # --- Audio playback: if any speakers exist -> at least 2 (except soundbar=1)
    fam = _family_lines(spec, source_pool)
    for f in ("wall_speaker", "ceiling_speaker"):
        lines = fam.get(f, [])
        if not lines:
            continue
        best = _pick_best_line(lines)
        if _line_qty(best) < 2:
            _set_line_qty(best, 2)
        _drop_lines(spec, [x for x in lines if _line_key(x) != _line_key(best)])

    if fam.get("soundbar"):
        best = _pick_best_line(fam["soundbar"])
        _set_line_qty(best, 1)
        _drop_lines(spec, [x for x in fam["soundbar"] if _line_key(x) != _line_key(best)])

    # --- drop zero qty
    items = [x for x in list(getattr(spec, "items", []) or []) if _line_qty(x) > 0]
    setattr(spec, "items", items)

    return warnings