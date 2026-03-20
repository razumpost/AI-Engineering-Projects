from __future__ import annotations

from typing import Any

from ..knowledge.models import ProjectRequirements
from ..normalization.candidate_classifier import classify_candidate, classify_candidates
from .plan_models import TopologyDecision


def _pool_items(pool: Any) -> list[Any]:
    return list(getattr(pool, "items", []) or [])


def _classified_by_id(pool: Any) -> dict[str, Any]:
    return {c.candidate_id: c for c in classify_candidates(_pool_items(pool))}


def _line_candidate_id(line: Any) -> str | None:
    cid = getattr(line, "candidate_id", None)
    if cid:
        return str(cid)
    meta = getattr(line, "meta", None)
    if isinstance(meta, dict) and meta.get("candidate_id"):
        return str(meta.get("candidate_id"))
    return None


def _line_qty(line: Any) -> float:
    try:
        return float(getattr(line, "qty", 0) or 0)
    except Exception:
        return 0.0


def _set_line_qty(line: Any, qty: int | float) -> None:
    try:
        setattr(line, "qty", int(qty) if float(qty).is_integer() else float(qty))
    except Exception:
        setattr(line, "qty", qty)


def _line_price_flag(line: Any) -> int:
    try:
        up = getattr(line, "unit_price_rub", None)
        if up not in (None, "", 0, 0.0):
            return 1
    except Exception:
        pass

    try:
        money = getattr(line, "unit_price", None)
        amt = getattr(money, "amount", None)
        if amt not in (None, "", 0, 0.0):
            return 1
    except Exception:
        pass

    return 0


def _line_family(line: Any, cls_by_id: dict[str, Any]) -> str | None:
    cid = _line_candidate_id(line)
    if cid and cid in cls_by_id and getattr(cls_by_id[cid], "family", None):
        return cls_by_id[cid].family

    class _LineLike:
        candidate_id = cid or "line"
        category = getattr(line, "category", None)
        sku = getattr(line, "sku", None)
        manufacturer = getattr(line, "manufacturer", None)
        name = getattr(line, "name", None) or getattr(line, "description", "") or ""
        description = getattr(line, "description", None)

    c = classify_candidate(_LineLike())  # text-based fallback
    return c.family


def _family_lines(spec: Any, source_pool: Any) -> dict[str, list[Any]]:
    cls_by_id = _classified_by_id(source_pool)
    out: dict[str, list[Any]] = {}
    for line in list(getattr(spec, "items", []) or []):
        fam = _line_family(line, cls_by_id)
        if not fam:
            continue
        out.setdefault(fam, []).append(line)
    return out


def _pick_best_line(lines: list[Any]) -> Any:
    def score(line: Any) -> tuple[float, int]:
        return (_line_qty(line), _line_price_flag(line))

    return sorted(lines, key=score, reverse=True)[0]


def _drop_lines(spec: Any, to_drop: list[Any]) -> None:
    if not to_drop:
        return
    drop_ids = {id(x) for x in to_drop}
    items = list(getattr(spec, "items", []) or [])
    keep = [x for x in items if id(x) not in drop_ids]
    setattr(spec, "items", keep)


def _collapse_singleton_family_group(spec: Any, fam_lines: dict[str, list[Any]], families: set[str], qty: int | None = None) -> None:
    lines: list[Any] = []
    for family in families:
        lines.extend(fam_lines.get(family, []))

    if not lines:
        return

    best = _pick_best_line(lines)
    if qty is not None:
        _set_line_qty(best, qty)

    _drop_lines(spec, [line for line in lines if line is not best])


def resolve_quantities(
    spec: Any,
    source_pool: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
) -> list[str]:
    warnings: list[str] = []
    fam_lines = _family_lines(spec, source_pool)

    seat_count = int(requirements.caps.seat_count or 0)
    camera_count = int(requirements.caps.camera_count or 1)

    # main display: exactly one
    if requirements.room_type == "meeting_room":
        _collapse_singleton_family_group(
            spec,
            fam_lines,
            {"display_panel", "interactive_panel", "projector", "projection_screen"},
            qty=1,
        )

    fam_lines = _family_lines(spec, source_pool)

    # cameras: collapse to one line with qty=camera_count
    camera_lines: list[Any] = []
    for family in {"ptz_camera", "fixed_conference_camera"}:
        camera_lines.extend(fam_lines.get(family, []))
    if camera_lines:
        best = _pick_best_line(camera_lines)
        _set_line_qty(best, camera_count)
        _drop_lines(spec, [x for x in camera_lines if x is not best])

    fam_lines = _family_lines(spec, source_pool)

    # BYOD gateway: exactly one
    _collapse_singleton_family_group(
        spec,
        fam_lines,
        {"byod_usb_hdmi_gateway", "byod_wireless_presentation", "usb_c_dock"},
        qty=1,
    )

    fam_lines = _family_lines(spec, source_pool)

    # processing/control singletons
    for singleton_family in {
        "conference_controller",
        "dsp",
        "usb_dsp_bridge",
        "presentation_switcher",
        "matrix_switcher",
        "simple_io_hub",
        "amplifier",
    }:
        lines = fam_lines.get(singleton_family, [])
        if not lines:
            continue
        best = _pick_best_line(lines)
        _set_line_qty(best, 1)
        _drop_lines(spec, [x for x in lines if x is not best])

    fam_lines = _family_lines(spec, source_pool)

    # chairman=1; delegate = seat-1 if chairman present else seat
    chairman_lines = fam_lines.get("chairman_unit", [])
    delegate_lines = fam_lines.get("delegate_unit", [])

    if chairman_lines:
        for line in chairman_lines:
            _set_line_qty(line, 1)

    if delegate_lines:
        if chairman_lines and seat_count > 1:
            target_delegate_qty = max(1, seat_count - 1)
        elif seat_count > 0:
            target_delegate_qty = seat_count
        else:
            target_delegate_qty = max(1, int(_line_qty(delegate_lines[0]) or 1))

        best_delegate = _pick_best_line(delegate_lines)
        _set_line_qty(best_delegate, target_delegate_qty)
        _drop_lines(spec, [x for x in delegate_lines if x is not best_delegate])

    fam_lines = _family_lines(spec, source_pool)

    # speakers: min 2
    for family in {"wall_speaker", "ceiling_speaker"}:
        lines = fam_lines.get(family, [])
        if not lines:
            continue
        total = sum(_line_qty(x) for x in lines)
        if total < 2:
            best = _pick_best_line(lines)
            _set_line_qty(best, 2)

    # soundbar/speakerphone: singleton
    for family in {"soundbar", "speakerphone"}:
        lines = fam_lines.get(family, [])
        if not lines:
            continue
        best = _pick_best_line(lines)
        _set_line_qty(best, 1)
        _drop_lines(spec, [x for x in lines if x is not best])

    # cables/accessories: qty>=1
    for family in {"cable_cat", "cable_hdmi", "cable_usb", "adapters_kit"}:
        lines = fam_lines.get(family, [])
        for line in lines:
            if _line_qty(line) <= 0:
                _set_line_qty(line, 1)

    # drop non-positive qty
    items = list(getattr(spec, "items", []) or [])
    items = [x for x in items if _line_qty(x) > 0]
    setattr(spec, "items", items)

    fam_lines = _family_lines(spec, source_pool)
    if requirements.room_type == "meeting_room":
        has_display = any(fam_lines.get(x) for x in {"display_panel", "interactive_panel"})
        if not has_display:
            warnings.append("missing_display_after_quantity_resolution")

    _ = topology
    return warnings