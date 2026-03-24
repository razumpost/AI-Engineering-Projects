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
    if getattr(item, "description", None) or getattr(item, "name", None):
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
    score = 0.0
    score += float(getattr(cls, "family_confidence", 0.0) or 0.0) * 10.0
    score += _candidate_quality(item)

    if cls.family in (role.preferred_families or []):
        score += 4.0
    if cls.family in (topology.preferred_families.get(role.role_key, []) if topology.preferred_families else []):
        score += 3.0
    if requirements.room_type in (cls.room_fit or []):
        score += 1.0

    # HARD safety gates
    if requirements.room_type == "meeting_room" and cls.family in {"videowall_controller", "speaker_100v", "led_cabinet"}:
        score -= 10_000.0

    if topology.topology_key == "meeting_room_delegate_dsp" and cls.family == "videobar":
        score -= 10_000.0

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


def build_filtered_pool_for_coverage(
    *,
    pool: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
    roles: list[ExpandedRole],
) -> CoveragePlannerResult:
    items = list(getattr(pool, "items", []) or [])
    tasks = list(getattr(pool, "tasks", []) or [])

    classified = classify_candidates(items)
    by_candidate_id = {c.candidate_id: c for c in classified}
    item_by_id = {getattr(i, "candidate_id"): i for i in items}

    forbidden = set(forbidden_families_for_requirements(requirements))

    eligible_ids: set[str] = set()
    dropped_ids: set[str] = set()
    warnings: list[str] = []

    for cls in classified:
        cid = cls.candidate_id
        item = item_by_id.get(cid)

        if not cls.family or item is None:
            dropped_ids.add(cid)
            continue

        if cls.family in forbidden:
            dropped_ids.add(cid)
            continue

        # HARD meeting_room bans
        if requirements.room_type == "meeting_room" and cls.family in {"videowall_controller", "speaker_100v", "led_cabinet"}:
            dropped_ids.add(cid)
            continue

        # HARD topology ban
        if topology.topology_key == "meeting_room_delegate_dsp" and cls.family == "videobar":
            dropped_ids.add(cid)
            continue

        if cls.room_fit and requirements.room_type not in cls.room_fit:
            dropped_ids.add(cid)
            continue

        eligible_ids.add(cid)

    role_debug: list[RoleCoverageDebug] = []
    kept_ids: set[str] = set()

    # deterministic role order: required first
    ordered_roles = sorted(
        roles,
        key=lambda r: (0 if r.role_key in topology.required_roles else 1, r.role_key),
    )

    for role in ordered_roles:
        debug = RoleCoverageDebug(role_key=role.role_key, required=role.required)

        allowed = set(role.allowed_families or [])
        if topology.topology_key == "meeting_room_delegate_dsp":
            allowed.discard("videobar")
        if requirements.room_type == "meeting_room":
            allowed.discard("videowall_controller")
            allowed.discard("speaker_100v")
            allowed.discard("led_cabinet")

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

        top_n = 1
        if role.role_key == "room_cabling_and_accessories":
            top_n = 3

        selected = matched[:top_n]
        if not selected and role.role_key in topology.required_roles:
            debug.warnings.append("uncovered_required_role")
            warnings.append(f"Role {role.role_key} has no matching candidates in filtered pool")

        for _, item, cls in selected:
            cid = getattr(item, "candidate_id")
            kept_ids.add(cid)
            debug.selected_candidate_ids.append(cid)
            debug.selected_families.append(str(cls.family))

        role_debug.append(debug)

    # keep a few "support" families if available
    support_families = {"managed_switch", "poe_switch", "dsp", "wireless_receiver", "conference_controller", "presentation_switcher"}
    for cid in list(eligible_ids):
        if cid in kept_ids:
            continue
        cls = by_candidate_id.get(cid)
        if cls and cls.family in support_families:
            kept_ids.add(cid)

    kept_in_order = [getattr(i, "candidate_id") for i in items if getattr(i, "candidate_id") in kept_ids]
    kept_items = [item_by_id[cid] for cid in kept_in_order]

    kept_task_ids = _task_ids_from_items(kept_items)
    kept_tasks = [t for t in tasks if getattr(t, "task_id", None) in kept_task_ids] or tasks[:]

    filtered_pool = _rebuild_pool_like(pool, kept_tasks, kept_items)

    return CoveragePlannerResult(
        filtered_pool=filtered_pool,
        kept_candidate_ids=kept_in_order,
        dropped_candidate_ids=[getattr(i, "candidate_id") for i in items if getattr(i, "candidate_id") in dropped_ids],
        role_debug=role_debug,
        warnings=warnings,
    )


def items_to_preserve_order(items: list[Any], selected_ids: set[str]) -> list[str]:
    out: list[str] = []
    for item in items:
        cid = getattr(item, "candidate_id")
        if cid in selected_ids:
            out.append(cid)
    return out