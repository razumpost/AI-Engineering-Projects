from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from ..knowledge.models import ProjectRequirements
from ..normalization.candidate_classifier import classify_candidates
from .conflict_resolver import forbidden_families_for_requirements
from .plan_models import ClassifiedCandidate, TopologyDecision
from .role_expander import ExpandedRole


@dataclass
class RoleCoverageDebug:
    role_key: str
    required: bool
    selected_candidate_ids: list[str] = field(default_factory=list)
    selected_families: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class CoveragePlannerResult:
    filtered_pool: Any
    kept_candidate_ids: list[str] = field(default_factory=list)
    dropped_candidate_ids: list[str] = field(default_factory=list)
    role_debug: list[RoleCoverageDebug] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _candidate_quality(item: Any) -> float:
    score = 0.0
    if getattr(item, "manufacturer", None):
        score += 1.0
    if getattr(item, "sku", None):
        score += 1.0
    if getattr(item, "unit_price_rub", None) not in (None, "", 0, 0.0):
        score += 0.75
    if getattr(item, "description", None):
        score += 0.5
    evidence = getattr(item, "evidence_task_ids", None) or []
    if evidence:
        score += min(1.0, len(evidence) * 0.2)
    return score


def _role_score(
    item: Any,
    cls: ClassifiedCandidate,
    role: ExpandedRole,
    topology: TopologyDecision,
    requirements: ProjectRequirements,
) -> float:
    score = cls.family_confidence * 10.0 + _candidate_quality(item)

    if cls.family in role.preferred_families:
        score += 4.0
    if cls.family in topology.preferred_families.get(role.role_key, []):
        score += 3.0
    if requirements.room_type in cls.room_fit:
        score += 1.0

    text = " ".join(
        [
            str(getattr(item, "sku", "") or ""),
            str(getattr(item, "manufacturer", "") or ""),
            str(getattr(item, "name", "") or ""),
            str(getattr(item, "description", "") or ""),
        ]
    ).casefold()

    if role.role_key.startswith("room_camera") and ("ptz" in text or "ndi" in text):
        score += 2.0
    if role.role_key == "room_display_main" and ("интерактив" in text or "panel" in text or "display" in text):
        score += 1.5
    if role.role_key == "room_audio_capture" and (
        "делегат" in text or "chairman" in text or "gooseneck" in text or "микрофон" in text
    ):
        score += 1.0
    if role.role_key == "room_byod_ingest" and ("byod" in text or "usb-c" in text or "type-c" in text):
        score += 2.0

    return score


def _rebuild_pool_like(pool: Any, tasks: list[Any], items: list[Any]) -> Any:
    pool_type = pool.__class__
    try:
        return pool_type(tasks=tasks, items=items)
    except Exception:
        class _Pool:
            def __init__(self, tasks: list[Any], items: list[Any]) -> None:
                self.tasks = tasks
                self.items = items

        return _Pool(tasks=tasks, items=items)


def _task_ids_from_items(items: Iterable[Any]) -> set[int]:
    out: set[int] = set()
    for item in items:
        ids = getattr(item, "evidence_task_ids", None) or []
        for tid in ids:
            try:
                out.add(int(tid))
            except Exception:
                continue
    return out


def _preserve_order_ids(items: list[Any], selected_ids: set[str]) -> list[str]:
    out: list[str] = []
    for item in items:
        cid = getattr(item, "candidate_id")
        if cid in selected_ids:
            out.append(cid)
    return out


def _top_n_for_role(role_key: str, required: bool) -> int:
    if role_key == "room_audio_capture":
        return 2
    if role_key == "room_cabling_and_accessories":
        return 2
    return 1 if required else 1


def build_filtered_pool_for_coverage(
    *,
    pool: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
    roles: list[ExpandedRole],
) -> CoveragePlannerResult:
    items = list(getattr(pool, "items", []))
    tasks = list(getattr(pool, "tasks", []))

    classified = classify_candidates(items)
    by_candidate_id = {c.candidate_id: c for c in classified}
    item_by_id = {getattr(i, "candidate_id"): i for i in items}

    forbidden = forbidden_families_for_requirements(requirements)
    eligible_ids: set[str] = set()
    dropped_ids: set[str] = set()
    warnings: list[str] = []

    for cls in classified:
        cid = cls.candidate_id
        if not cls.family:
            dropped_ids.add(cid)
            continue
        if cls.family in forbidden:
            dropped_ids.add(cid)
            continue
        if cls.room_fit and requirements.room_type not in cls.room_fit:
            dropped_ids.add(cid)
            continue
        eligible_ids.add(cid)

    role_priority = {r.role_key: idx for idx, r in enumerate(roles)}
    ordered_roles = sorted(
        roles,
        key=lambda r: (
            0 if r.role_key in topology.required_roles else 1,
            role_priority.get(r.role_key, 999),
        ),
    )

    kept_ids: set[str] = set()
    role_debug: list[RoleCoverageDebug] = []

    for role in ordered_roles:
        debug = RoleCoverageDebug(role_key=role.role_key, required=role.role_key in topology.required_roles)
        allowed = set(role.allowed_families)
        if not allowed:
            debug.warnings.append("no_allowed_families")
            role_debug.append(debug)
            continue

        matched: list[tuple[float, Any, ClassifiedCandidate]] = []
        for cid in eligible_ids:
            cls = by_candidate_id.get(cid)
            item = item_by_id.get(cid)
            if cls is None or item is None:
                continue
            if cls.family not in allowed:
                continue
            matched.append((_role_score(item, cls, role, topology, requirements), item, cls))

        matched.sort(key=lambda x: x[0], reverse=True)
        selected = matched[: _top_n_for_role(role.role_key, debug.required)]

        if not selected and debug.required:
            debug.warnings.append("uncovered_required_role")
            warnings.append(f"Role {role.role_key} has no matching candidates in filtered pool")

        for _, item, cls in selected:
            cid = getattr(item, "candidate_id")
            kept_ids.add(cid)
            debug.selected_candidate_ids.append(cid)
            debug.selected_families.append(str(cls.family))

        role_debug.append(debug)

    kept_items = [item_by_id[cid] for cid in _preserve_order_ids(items, kept_ids)]
    kept_task_ids = _task_ids_from_items(kept_items)
    kept_tasks = [t for t in tasks if getattr(t, "task_id", None) in kept_task_ids]
    if not kept_tasks:
        kept_tasks = tasks[:]

    filtered_pool = _rebuild_pool_like(pool, kept_tasks, kept_items)

    return CoveragePlannerResult(
        filtered_pool=filtered_pool,
        kept_candidate_ids=list(_preserve_order_ids(items, kept_ids)),
        dropped_candidate_ids=list(_preserve_order_ids(items, dropped_ids)),
        role_debug=role_debug,
        warnings=warnings,
    )
