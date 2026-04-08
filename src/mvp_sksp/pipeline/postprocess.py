# src/mvp_sksp/pipeline/postprocess.py
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


def _project_meta_from_context(
    *,
    spec: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
    roles: Sequence[ExpandedRole],
) -> dict[str, Any]:
    caps = getattr(requirements, "caps", {}) or {}
    flags = getattr(requirements, "flags", {}) or {}

    def _cap_int(name: str) -> int | None:
        v = None
        if isinstance(caps, dict):
            v = caps.get(name)
        else:
            v = getattr(caps, name, None)
        if v in (None, "", 0):
            return None
        try:
            return int(v)
        except Exception:
            return None

    room_count = _cap_int("room_count") or 1
    seat_count = _cap_int("seat_count") or 0
    camera_count = _cap_int("camera_count")
    display_count = _cap_int("display_count")

    spec_items = list(getattr(spec, "items", []) or [])

    if camera_count is None:
        camera_count = 0
        for line in spec_items:
            family = (getattr(line, "family", None) or "").strip().casefold()
            category = (getattr(line, "category", None) or "").strip().casefold()
            desc = (
                (getattr(line, "description", None) or "")
                + " "
                + (getattr(line, "name", None) or "")
            ).casefold()
            if "ptz_camera" == family or "camera" in family or "камера" in desc or "ptz" in desc or "camera" in category:
                try:
                    camera_count += int(getattr(line, "qty", 1) or 1)
                except Exception:
                    camera_count += 1

    if display_count is None:
        display_count = 0
        for line in spec_items:
            family = (getattr(line, "family", None) or "").strip().casefold()
            category = (getattr(line, "category", None) or "").strip().casefold()
            desc = (
                (getattr(line, "description", None) or "")
                + " "
                + (getattr(line, "name", None) or "")
            ).casefold()
            if (
                "display" in family
                or "panel" in family
                or "дисплей" in desc
                or "панель" in desc
                or "экран" in desc
                or "display" in category
            ):
                try:
                    display_count += int(getattr(line, "qty", 1) or 1)
                except Exception:
                    display_count += 1

    if display_count <= 0:
        room_type = (getattr(requirements, "room_type", None) or "").strip().casefold()
        topo_name = (getattr(topology, "topology_id", None) or getattr(topology, "name", None) or "").strip().casefold()
        if "led" in room_type or "display" in room_type or "videowall" in room_type or "экран" in room_type:
            display_count = 1
        elif "display" in topo_name or "screen" in topo_name or "led" in topo_name:
            display_count = 1

    if camera_count <= 0:
        if isinstance(flags, dict):
            if flags.get("vks"):
                camera_count = max(camera_count, 1)
        else:
            if getattr(flags, "vks", False):
                camera_count = max(camera_count, 1)

    return {
        "room_count": int(room_count or 1),
        "seat_count": int(seat_count or 0),
        "camera_count": int(camera_count or 0),
        "display_count": int(display_count or 0),
        "topology": getattr(topology, "topology_id", None) or getattr(topology, "name", None),
        "roles": [getattr(r, "role_key", None) for r in roles if getattr(r, "role_key", None)],
    }


def _plan_items(spec: Any) -> list[Any]:
    items = getattr(spec, "items", None)
    if items is None:
        return []
    if isinstance(items, list):
        return items
    try:
        return list(items)
    except Exception:
        return []


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

    project_meta = _project_meta_from_context(
        spec=spec,
        requirements=requirements,
        topology=topology,
        roles=roles,
    )

    plan_items = _plan_items(spec)
    qty_result = resolve_quantities(plan_items, project_meta)

    qty_warnings: list[str] = []
    if isinstance(qty_result, list):
        # quantity_resolver модифицирует plan_items in-place и обычно возвращает сам список
        pass
    elif qty_result is None:
        pass
    else:
        try:
            qty_warnings = list(qty_result)
        except Exception:
            qty_warnings = []

    merge_duplicate_candidate_lines(spec)
    normalize_categories(spec, source_pool)
    sort_spec_items(spec)

    validate_prices(spec, source_pool)
    build_fallback_explanations(spec=spec, requirements=requirements, topology=topology)

    warnings = list(dep_warnings or []) + list(qty_warnings or [])
    if warnings and hasattr(spec, "apply_warnings"):
        cur = list(getattr(spec, "apply_warnings", []) or [])
        seen = set(cur)
        for w in warnings:
            if w not in seen:
                cur.append(w)
        setattr(spec, "apply_warnings", cur)

    if hasattr(spec, "meta") and isinstance(getattr(spec, "meta", None), dict):
        spec.meta["project_meta"] = project_meta
    elif hasattr(spec, "meta"):
        try:
            setattr(spec, "meta", {"project_meta": project_meta})
        except Exception:
            pass

    return spec