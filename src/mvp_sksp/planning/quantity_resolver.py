from __future__ import annotations

from decimal import Decimal
from typing import Any

from ..knowledge.models import ProjectRequirements
from .plan_models import TopologyDecision


def _get_items(spec: Any) -> list[Any]:
    return list(getattr(spec, "items", []) or [])


def _text_of_line(line: Any) -> str:
    parts = [
        getattr(line, "manufacturer", "") or "",
        getattr(line, "sku", "") or "",
        getattr(line, "name", "") or "",
        getattr(line, "description", "") or "",
    ]
    return " ".join(parts).strip().casefold()


def _to_like_type(value: int, like: Any) -> Any:
    if isinstance(like, Decimal):
        return Decimal(str(value))
    if isinstance(like, int):
        return int(value)
    if isinstance(like, float):
        return float(value)
    # unknown qty type -> keep int
    return int(value)


def _set_qty(line: Any, value: int) -> None:
    like = getattr(line, "qty", 1)
    try:
        setattr(line, "qty", _to_like_type(value, like))
    except Exception:
        # Some models can be frozen; if so, we can't mutate here.
        # Postprocess should still continue.
        pass


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        if isinstance(v, bool):
            return default
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip().replace(",", ".")
        return int(float(s))
    except Exception:
        return default


def resolve_quantities(
    spec: Any,
    source_pool: Any | None = None,
    requirements: ProjectRequirements | None = None,
    topology: TopologyDecision | None = None,
) -> list[str]:
    """
    Backward/forward compatible.

    postprocess_spec() calls:
      resolve_quantities(spec, source_pool, requirements, topology)

    Older code may call:
      resolve_quantities(spec)
      resolve_quantities(spec, requirements)
    """
    warnings: list[str] = []

    items = _get_items(spec)
    if not items:
        return ["quantity_resolver: spec.items empty; skip"]

    if requirements is None:
        # keep silent and don't crash
        return ["quantity_resolver: requirements is None; skip"]

    seat = _safe_int(getattr(requirements.caps, "seat_count", None), default=0)
    display_count = _safe_int(getattr(requirements.caps, "display_count", None), default=1)
    camera_count = _safe_int(getattr(requirements.caps, "camera_count", None), default=1)

    # 1) normalize empty qty -> 1
    for line in items:
        q = getattr(line, "qty", None)
        if q in (None, 0, 0.0, Decimal("0")):
            _set_qty(line, 1)

    # 2) heuristic quantities by keywords (works for your SKSP-ingested data)
    for line in items:
        txt = _text_of_line(line)
        q = getattr(line, "qty", 1)

        # Conference system: chairman + delegates
        if ("председател" in txt) or ("chairman" in txt):
            _set_qty(line, 1)
            continue

        if ("делегат" in txt) or ("delegate" in txt):
            if seat >= 2:
                _set_qty(line, max(1, seat - 1))
            else:
                # if seats unknown, keep as-is but warn once
                if seat == 0:
                    warnings.append("qty_hint: delegate units present but seat_count unknown")
            continue

        # gooseneck mics often match delegates+chairman
        if ("гусиная шея" in txt) or ("gooseneck" in txt) or ("микрофон" in txt and "конференц" in txt):
            if seat >= 1:
                _set_qty(line, max(1, seat + 1))  # delegates + chairman
            else:
                warnings.append("qty_hint: microphones present but seat_count unknown")
            continue

        # Displays
        if ("панель" in txt) or ("дисплей" in txt) or ("display" in txt) or ("tv" in txt) or ("телевиз" in txt):
            _set_qty(line, max(1, display_count))
            continue

        # Cameras
        if ("камера" in txt) or ("ptz" in txt) or ("videobar" in txt):
            _set_qty(line, max(1, camera_count))
            continue

        # Meeting-room speakers (very rough)
        if requirements.room_type == "meeting_room" and (
            "акуст" in txt or "колон" in txt or "speaker" in txt or "soundbar" in txt
        ):
            # default stereo pair
            _set_qty(line, 2 if "soundbar" not in txt else 1)
            continue

        # Cables / accessories: keep as-is (often already correct), but ensure >=1
        if ("кабель" in txt) or ("cable" in txt) or ("комплект" in txt) or ("расход" in txt):
            if _safe_int(q, 1) <= 0:
                _set_qty(line, 1)

    return warnings