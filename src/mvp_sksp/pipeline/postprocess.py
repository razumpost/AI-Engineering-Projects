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


def _allowed_families(roles: Sequence[ExpandedRole], topology: TopologyDecision) -> set[str]:
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
    return allowed


def _prune_items_outside_allowed_families(spec: Any, source_pool: Any, roles: Sequence[ExpandedRole], topology: TopologyDecision) -> list[str]:
    allowed = _allowed_families(roles, topology)
    cls_by_id = _classified_by_id(source_pool)

    items = list(getattr(spec, "items", []) or [])
    kept: list[Any] = []
    warnings: list[str] = []

    for line in items:
        fam = _line_family(line, cls_by_id)

        # если family не определена — пока не выкидываем автоматически
        if not fam:
            kept.append(line)
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

    dep_warnings = resolve_dependencies(spec, source_pool, requirements, topology)
    merge_duplicate_candidate_lines(spec)

    qty_warnings = resolve_quantities(spec, source_pool, requirements, topology)
    merge_duplicate_candidate_lines(spec)

    gate_warnings = _prune_items_outside_allowed_families(spec, source_pool, roles, topology)
    merge_duplicate_candidate_lines(spec)

    normalize_categories(spec, source_pool)
    sort_spec_items(spec)

    validate_prices(spec, source_pool)
    build_fallback_explanations(spec=spec, requirements=requirements, topology=topology)

    warnings = dep_warnings + qty_warnings + gate_warnings
    if warnings and hasattr(spec, "apply_warnings"):
        cur = list(getattr(spec, "apply_warnings", []) or [])
        seen = set(cur)
        for w in warnings:
            if w not in seen:
                cur.append(w)
                seen.add(w)
        setattr(spec, "apply_warnings", cur)

    return spec