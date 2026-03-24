from __future__ import annotations

from typing import Any, Sequence

from ..knowledge.models import ProjectRequirements
from ..planning.dependency_resolver import resolve_dependencies
from ..planning.plan_models import TopologyDecision
from ..planning.quantity_resolver import resolve_quantities
from ..planning.role_expander import ExpandedRole
from .explain_fallback import build_fallback_explanations
from .price_validator import validate_prices
from .spec_mapper import merge_duplicate_candidate_lines, normalize_categories, sort_spec_items

__all__ = ["postprocess_spec"]


def postprocess_spec(
    *,
    spec: Any,
    filtered_pool: Any,
    source_pool: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
    roles: Sequence[ExpandedRole],
) -> Any:
    _ = (filtered_pool, roles)

    merge_duplicate_candidate_lines(spec)

    dep_warnings = resolve_dependencies(spec, source_pool, requirements, topology)
    merge_duplicate_candidate_lines(spec)

    qty_warnings = resolve_quantities(spec, source_pool, requirements, topology)
    merge_duplicate_candidate_lines(spec)

    normalize_categories(spec, source_pool)
    sort_spec_items(spec)

    validate_prices(spec, source_pool)
    build_fallback_explanations(spec=spec, requirements=requirements, topology=topology)

    warnings = dep_warnings + qty_warnings
    if warnings and hasattr(spec, "apply_warnings"):
        cur = list(getattr(spec, "apply_warnings", []) or [])
        seen = set(cur)
        for w in warnings:
            if w not in seen:
                cur.append(w)
        setattr(spec, "apply_warnings", cur)

    return spec