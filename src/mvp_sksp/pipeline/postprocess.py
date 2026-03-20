from __future__ import annotations

from typing import Any, Sequence

from ..knowledge.models import ProjectRequirements
from ..planning.dependency_resolver import resolve_dependencies
from ..planning.plan_models import TopologyDecision
from ..planning.quantity_resolver import resolve_quantities
from ..planning.role_expander import ExpandedRole
from .spec_mapper import merge_duplicate_candidate_lines, normalize_categories, sort_spec_items


def postprocess_spec(
    *,
    spec: Any,
    filtered_pool: Any,
    source_pool: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
    roles: Sequence[ExpandedRole],
) -> Any:
    _ = roles  # reserved for further refinement

    merge_duplicate_candidate_lines(spec)
    dep_warnings = resolve_dependencies(spec, source_pool, requirements, topology)
    merge_duplicate_candidate_lines(spec)
    qty_warnings = resolve_quantities(spec, source_pool, requirements, topology)
    merge_duplicate_candidate_lines(spec)
    normalize_categories(spec, source_pool)
    sort_spec_items(spec)

    warnings = dep_warnings + qty_warnings
    if warnings and hasattr(spec, "risks"):
        existing = list(getattr(spec, "risks", []) or [])
        seen = set(existing)
        for w in warnings:
            if w not in seen:
                existing.append(w)
        setattr(spec, "risks", existing)

    return spec