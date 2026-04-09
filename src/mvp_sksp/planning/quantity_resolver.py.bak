from __future__ import annotations

from typing import Any

from ..knowledge.loader import load_knowledge_map
from .plan_models import LineItemPlan


def _km():
    return load_knowledge_map()


def _count(plan: list[LineItemPlan], family: str) -> int:
    return sum(int(li.qty or 0) for li in plan if li.family == family)


def _set_qty(plan: list[LineItemPlan], family: str, qty: int) -> None:
    for li in plan:
        if li.family == family:
            li.qty = int(qty)


def _cap(qty: int, min_value: int | None, max_value: int | None) -> int:
    if min_value is not None:
        qty = max(qty, int(min_value))
    if max_value is not None:
        qty = min(qty, int(max_value))
    return max(0, int(qty))


def resolve_quantities(
    plan: list[LineItemPlan],
    project_meta: dict[str, Any] | None = None,
) -> list[LineItemPlan]:
    """Apply quantity rules from KB.

    project_meta may contain:
      - room_count
      - camera_count
      - display_count
      - seat_count
    """
    km = _km()
    meta = project_meta or {}

    room_count = int(meta.get("room_count") or 1)
    camera_count = int(meta.get("camera_count") or _count(plan, "ptz_camera") or 0)
    display_count = int(meta.get("display_count") or _count(plan, "display") or 0)
    seat_count = int(meta.get("seat_count") or 0)

    for family, rule in km.quantity_rules.items():
        if not any(li.family == family for li in plan):
            continue

        if rule.rule == "one":
            qty = 1
        elif rule.rule == "one_per_room":
            qty = room_count
        elif rule.rule == "same_as_camera_count":
            qty = max(1, camera_count)
        elif rule.rule == "same_as_display_count":
            qty = max(1, display_count)
        elif rule.rule == "same_as_seat_count":
            qty = max(1, seat_count)
        elif rule.rule == "seat_count_minus_one":
            qty = max(1, seat_count - 1)
        elif rule.rule == "one_per_display":
            qty = max(1, display_count)
        elif rule.rule == "one_per_camera":
            qty = max(1, camera_count)
        else:
            # custom: keep existing qty
            continue

        qty = _cap(qty, rule.min_value, rule.max_value)
        _set_qty(plan, family, qty)

    return plan