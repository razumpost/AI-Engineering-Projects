from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

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


def _pool_items(pool: Any) -> list[Any]:
    return list(getattr(pool, "items", []) or [])


def _pool_tasks(pool: Any) -> list[Any]:
    return list(getattr(pool, "tasks", []) or [])


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


def _candidate_quality(item: Any) -> float:
    score = 0.0
    if getattr(item, "manufacturer", None):
        score += 1.0
    if getattr(item, "sku", None):
        score += 1.0
    price = getattr(item, "unit_price_rub", None)
    if price not in (None, "", 0, 0.0):
        score += 0.75
    if getattr(item, "description", None) or getattr(item, "name", None):
        score += 0.5
    ev = getattr(item, "evidence_task_ids", None) or []
    score += min(1.0, len(ev) * 0.2)
    return score


def _text(item: Any) -> str:
    return " ".join(
        [
            str(getattr(item, "sku", "") or ""),
            str(getattr(item, "manufacturer", "") or ""),
            str(getattr(item, "name", "") or ""),
            str(getattr(item, "description", "") or ""),
        ]
    ).casefold()


def _role_predicate(role_key: str, cls: ClassifiedCandidate, item: Any) -> bool:
    t = _text(item)

    if role_key in {"room_byod_ingest", "room_usb_bridge_or_byod_gateway"}:
        # must look like BYOD/USB-C gateway / wireless share
        if cls.family in {"hdmi_splitter", "videowall_controller"}:
            return False
        if "usb" not in t and "type-c" not in t and "usb-c" not in t and "wireless" not in t and "byod" not in t:
            return False
        if "splitter" in t or "сплиттер" in t or "1:4" in t or "1x4" in t:
            return False
        return True

    if role_key == "room_audio_playback":
        # exclude mounts
        if cls.family == "mounting_kit":
            return False
        # require audio-ish tokens (so крепление не пройдёт)
        if not ("акуст" in t or "speaker" in t or "soundbar" in t or "колон" in t):
            return False
        return True

    return True


def _role_score(item: Any, cls: ClassifiedCandidate, role: ExpandedRole, topology: TopologyDecision, req: ProjectRequirements) -> float:
    score = float(getattr(cls, "family_confidence", 0.0) or 0.0) * 10.0 + _candidate_quality(item)

    if cls.family in (role.preferred_families or []):
        score += 4.0
    if cls.family in (topology.preferred_families.get(role.role_key, []) if topology.preferred_families else []):
        score += 2.0
    if req.room_type in (cls.room_fit or []):
        score += 1.0

    if req.room_type == "meeting_room" and cls.family in {"videowall_controller", "speaker_100v", "led_cabinet"}:
        score -= 10_000.0
    if topology.topology_key == "meeting_room_delegate_dsp" and cls.family == "videobar":
        score -= 10_000.0

    return score


def _topn_for_role(role_key: str) -> int:
    if role_key in {"room_display_main", "room_camera_main", "room_camera_secondary", "room_signal_switching"}:
        return 1
    if role_key == "room_cabling_and_accessories":
        return 3
    return 1


def build_filtered_pool_for_coverage(*, pool: Any, requirements: ProjectRequirements, topology: TopologyDecision, roles: list[ExpandedRole]) -> CoveragePlannerResult:
    items = _pool_items(pool)
    tasks = _pool_tasks(pool)

    classified = classify_candidates(items)
    cls_by_id = {c.candidate_id: c for c in classified}
    item_by_id = {getattr(i, "candidate_id"): i for i in items}

    forbidden = set(forbidden_families_for_requirements(requirements))
    eligible: set[str] = set()
    dropped: set[str] = set()
    warnings: list[str] = []

    for c in classified:
        cid = c.candidate_id
        it = item_by_id.get(cid)
        if it is None or not c.family:
            dropped.add(cid)
            continue
        if c.family in forbidden:
            dropped.add(cid)
            continue
        if requirements.room_type == "meeting_room" and c.family in {"videowall_controller", "speaker_100v", "led_cabinet"}:
            dropped.add(cid)
            continue
        if topology.topology_key == "meeting_room_delegate_dsp" and c.family == "videobar":
            dropped.add(cid)
            continue
        if c.room_fit and requirements.room_type not in c.room_fit:
            dropped.add(cid)
            continue
        eligible.add(cid)

    kept: set[str] = set()
    role_debug: list[RoleCoverageDebug] = []

    ordered_roles = sorted(roles, key=lambda r: (0 if r.role_key in topology.required_roles else 1, r.role_key))

    for role in ordered_roles:
        dbg = RoleCoverageDebug(role_key=role.role_key, required=role.required)
        allowed = set(role.allowed_families or [])

        if topology.topology_key == "meeting_room_delegate_dsp":
            allowed.discard("videobar")

        if not allowed:
            dbg.warnings.append("no_allowed_families")
            role_debug.append(dbg)
            continue

        scored: list[tuple[float, str]] = []
        for cid in eligible:
            c = cls_by_id.get(cid)
            it = item_by_id.get(cid)
            if c is None or it is None:
                continue
            if c.family not in allowed:
                continue
            if not _role_predicate(role.role_key, c, it):
                continue
            scored.append((_role_score(it, c, role, topology, requirements), cid))

        scored.sort(key=lambda x: x[0], reverse=True)
        picked = [cid for _, cid in scored[: _topn_for_role(role.role_key)]]

        if role.role_key in topology.required_roles and not picked:
            dbg.warnings.append("uncovered_required_role")
            warnings.append(f"Role {role.role_key} has no matching candidates in filtered pool")

        for cid in picked:
            kept.add(cid)
            dbg.selected_candidate_ids.append(cid)
            dbg.selected_families.append(str(cls_by_id[cid].family))

        role_debug.append(dbg)

    # support candidates
    support_families = {"conference_controller", "dsp", "wireless_receiver", "managed_switch", "poe_switch", "presentation_switcher"}
    for cid in eligible:
        if cid in kept:
            continue
        c = cls_by_id.get(cid)
        if c and c.family in support_families:
            kept.add(cid)

    kept_in_order = [getattr(i, "candidate_id") for i in items if getattr(i, "candidate_id") in kept]
    kept_items = [item_by_id[cid] for cid in kept_in_order]

    kept_task_ids: set[int] = set()
    for it in kept_items:
        for tid in getattr(it, "evidence_task_ids", None) or []:
            try:
                kept_task_ids.add(int(tid))
            except Exception:
                pass
    kept_tasks = [t for t in tasks if getattr(t, "task_id", None) in kept_task_ids] or tasks[:]

    return CoveragePlannerResult(
        filtered_pool=_rebuild_pool_like(pool, kept_tasks, kept_items),
        kept_candidate_ids=kept_in_order,
        dropped_candidate_ids=[getattr(i, "candidate_id") for i in items if getattr(i, "candidate_id") in dropped],
        role_debug=role_debug,
        warnings=warnings,
    )