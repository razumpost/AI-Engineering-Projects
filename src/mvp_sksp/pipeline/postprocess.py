from __future__ import annotations

from typing import Any, Sequence

from ..knowledge.models import ProjectRequirements
from ..planning.dependency_resolver import resolve_dependencies
from ..planning.plan_models import TopologyDecision
from ..planning.quantity_resolver import resolve_quantities
from ..planning.role_expander import ExpandedRole
from .explain_fallback import build_fallback_explanations
from .price_validator import validate_prices
from .spec_mapper import (
    _classified_by_id,
    _line_family,
    merge_duplicate_candidate_lines,
    normalize_categories,
    sort_spec_items,
)

__all__ = ["postprocess_spec"]


def _has_discussion_intent(requirements: ProjectRequirements, topology: TopologyDecision) -> bool:
    if topology.topology_key == "meeting_room_discussion_only":
        return True

    try:
        conf = dict(getattr(requirements, "confidence", {}) or {})
        if float(conf.get("discussion", 0.0) or 0.0) > 0:
            return True
    except Exception:
        pass

    seat_count = int(requirements.caps.seat_count or 0)
    camera_count = int(requirements.caps.camera_count or 0)
    display_count = int(requirements.caps.display_count or 0)

    if bool(requirements.flags.control) and seat_count >= 8 and camera_count == 0 and display_count == 0:
        return True

    return False


def _allowed_families(
    roles: Sequence[ExpandedRole],
    topology: TopologyDecision,
    requirements: ProjectRequirements,
) -> set[str]:
    allowed: set[str] = set()

    for role in roles:
        allowed.update(role.allowed_families or [])
        allowed.update(role.preferred_families or [])

    for fams in (topology.preferred_families or {}).values():
        allowed.update(fams or [])

    allowed.update(
        {
            "cabling_av",
            "mounting_kit",
            "power_accessories",
            "power_supply_discussion",
            "managed_switch",
            "poe_switch",
            "discussion_central_unit",
            "discussion_dsp",
            "conference_controller",
        }
    )

    if _has_discussion_intent(requirements, topology):
        allowed.update(
            {
                "delegate_unit",
                "chairman_unit",
                "discussion_central_unit",
                "discussion_dsp",
                "power_supply_discussion",
                "tabletop_mic",
                "ceiling_mic_array",
            }
        )

    return allowed


def _discussion_forbidden_families() -> set[str]:
    return {
        "display_panel",
        "interactive_panel",
        "projector",
        "projection_screen",
        "ptz_camera",
        "fixed_conference_camera",
        "videobar",
        "presentation_switcher",
        "matrix_switcher",
        "simple_io_hub",
        "byod_usb_hdmi_gateway",
        "byod_wireless_presentation",
        "usb_c_dock",
    }


def _prune_items_outside_allowed_families(
    spec: Any,
    source_pool: Any,
    roles: Sequence[ExpandedRole],
    topology: TopologyDecision,
    requirements: ProjectRequirements,
) -> list[str]:
    allowed = _allowed_families(roles, topology, requirements)
    cls_by_id = _classified_by_id(source_pool)

    discussion_intent = _has_discussion_intent(requirements, topology)
    discussion_blocked = _discussion_forbidden_families()

    items = list(getattr(spec, "items", []) or [])
    kept: list[Any] = []
    warnings: list[str] = []

    for line in items:
        fam = _line_family(line, cls_by_id)

        if not fam:
            kept.append(line)
            continue

        if discussion_intent and fam in discussion_blocked:
            warnings.append(
                f"Line dropped by discussion gate: family={fam}, sku={getattr(line, 'sku', '')}, name={getattr(line, 'name', '')}"
            )
            continue

        if fam not in allowed:
            warnings.append(
                f"Line dropped by family gate: family={fam}, sku={getattr(line, 'sku', '')}, name={getattr(line, 'name', '')}"
            )
            continue

        kept.append(line)

    setattr(spec, "items", kept)
    return warnings


def postprocess_spec(
    *,
    spec: Any,
    filtered_pool: Any,
    source_pool: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
    roles: Sequence[ExpandedRole],
) -> Any:
    _ = filtered_pool

    merge_duplicate_candidate_lines(spec)

    dep_warnings_1 = resolve_dependencies(spec, source_pool, requirements, topology)
    merge_duplicate_candidate_lines(spec)

    qty_warnings = resolve_quantities(spec, source_pool, requirements, topology)
    merge_duplicate_candidate_lines(spec)

    gate_warnings = _prune_items_outside_allowed_families(spec, source_pool, roles, topology, requirements)
    merge_duplicate_candidate_lines(spec)

    # после family gate ещё раз восстанавливаем обязательные dependency placeholder’ы
    dep_warnings_2 = resolve_dependencies(spec, source_pool, requirements, topology)
    merge_duplicate_candidate_lines(spec)

    normalize_categories(spec, source_pool)
    sort_spec_items(spec)

    validate_prices(spec, source_pool)
    build_fallback_explanations(spec=spec, requirements=requirements, topology=topology)

    warnings = dep_warnings_1 + qty_warnings + gate_warnings + dep_warnings_2
    if warnings and hasattr(spec, "apply_warnings"):
        cur = list(getattr(spec, "apply_warnings", []) or [])
        seen = set(cur)
        for w in warnings:
            if w not in seen:
                cur.append(w)
                seen.add(w)
        setattr(spec, "apply_warnings", cur)

    return spec