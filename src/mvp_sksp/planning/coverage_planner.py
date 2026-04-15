from __future__ import annotations

import re
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
class CandidateDropDebug:
    candidate_id: str
    family: str | None = None
    reason: str = ""


@dataclass
class CoveragePlannerResult:
    filtered_pool: Any
    kept_candidate_ids: list[str] = field(default_factory=list)
    dropped_candidate_ids: list[str] = field(default_factory=list)
    role_debug: list[RoleCoverageDebug] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    allowed_families: list[str] = field(default_factory=list)
    covered_families: list[str] = field(default_factory=list)
    uncovered_families: list[str] = field(default_factory=list)
    drop_debug: list[CandidateDropDebug] = field(default_factory=list)


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
            str(getattr(item, "model", "") or ""),
            str(getattr(item, "name", "") or ""),
            str(getattr(item, "description", "") or ""),
        ]
    ).casefold()


def _extract_diag_inches(text: str) -> float | None:
    patterns = [
        r'(\d{2,3}(?:[.,]\d)?)\s*(?:"|”|inch|inches|дюйм)',
        r'(\d{2,3}(?:[.,]\d)?)\s*(?:inch|дюйм)',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                return float(m.group(1).replace(",", "."))
            except Exception:
                return None
    return None


def _meeting_room_min_display_diag(req: ProjectRequirements) -> float:
    seats = int(req.caps.seat_count or 0)
    if seats >= 16:
        return 65.0
    if seats >= 10:
        return 55.0
    if seats >= 6:
        return 43.0
    return 32.0


def _role_predicate(role_key: str, cls: ClassifiedCandidate, item: Any, req: ProjectRequirements) -> bool:
    t = _text(item)

    if role_key in {"room_byod_ingest", "room_usb_bridge_or_byod_gateway"}:
        if cls.family in {"hdmi_splitter", "videowall_controller"}:
            return False
        if "usb" not in t and "type-c" not in t and "usb-c" not in t and "wireless" not in t and "byod" not in t:
            return False
        if "splitter" in t or "сплиттер" in t or "1:4" in t or "1x4" in t:
            return False
        return True

    if role_key == "room_signal_switching":
        if cls.family not in {"matrix_switcher", "presentation_switcher", "av_over_ip_tx", "av_over_ip_rx", "simple_io_hub"}:
            return False

        positive_needles = [
            "switcher",
            "matrix",
            "коммутатор",
            "presentation",
            "byod",
            "gateway",
            "usb bridge",
            "wireless presentation",
            "dock",
            "hub",
            "hdbaset",
            "extender",
            "transmitter",
            "receiver",
            "tx",
            "rx",
        ]
        negative_needles = [
            "intel i3",
            "intel i5",
            "intel i7",
            "celeron",
            "ryzen",
            "win 10",
            "win 11",
            "android 5.0",
            "media player",
            "spinetix",
            "slot pc",
            "ops",
            "mini pc",
        ]

        has_positive = any(x in t for x in positive_needles)
        has_negative = any(x in t for x in negative_needles)

        if has_negative and not has_positive:
            return False

        if cls.family == "simple_io_hub" and not has_positive:
            return False

        return True

    if role_key == "room_audio_playback":
        if cls.family in {"mounting_kit", "cabling_av"}:
            return False
        if not (
            "акуст" in t
            or "speaker" in t
            or "soundbar" in t
            or "колон" in t
            or cls.family in {"soundbar", "wall_speaker", "ceiling_speaker", "videobar"}
        ):
            return False
        return True

    if role_key == "room_display_main":
        if cls.family not in {"display_panel", "interactive_panel", "display", "projector"}:
            return False

        if req.room_type == "meeting_room" and cls.family == "projector":
            return False

        hard_mount_needles = [
            "mount kit",
            "ceiling mount",
            "wall mount",
            "pull-out",
            "pull out",
            "bracket",
            "кронштейн",
            "стойка",
            "тележка",
            "trolley",
            "back-to-back",
            "back to back",
            "micro adjustable",
            "height-adjustable",
            "height adjustable",
            "rotate",
            "fixed",
        ]
        if any(x in t for x in hard_mount_needles):
            return False

        bad_needles = [
            "transparent",
            "прозрачн",
            "холодильник",
            "outdoor",
            "window facing",
            "signage",
            "smart display",
            "all in one smart display",
            "23.6",
            "24 ",
            "24\"",
            "32-46",
            "32“",
            "46\"",
        ]
        if any(x in t for x in bad_needles):
            return False

        if req.room_type == "meeting_room":
            min_diag = _meeting_room_min_display_diag(req)
            diag = _extract_diag_inches(t)
            if diag is not None and diag < min_diag:
                return False

        if not (
            "display" in t
            or "дисплей" in t
            or "панель" in t
            or "monitor" in t
            or "экран" in t
            or "interactive" in t
        ):
            return False

        return True

    if role_key in {"room_camera_main", "room_camera_secondary"}:
        if cls.family not in {"ptz_camera", "fixed_conference_camera", "videobar"}:
            return False
        if "smart display" in t or "all in one smart display" in t:
            return False
        if not ("camera" in t or "камера" in t or "ptz" in t or "videobar" in t):
            return False
        return True

    if role_key == "room_audio_capture":
        if cls.family in {"mounting_kit", "cabling_av"}:
            return False
        if req.room_type == "meeting_room" and cls.family in {"delegate_unit", "chairman_unit", "discussion_central_unit"}:
            return False
        if not (
            "microphone" in t
            or "микрофон" in t
            or "beamforming" in t
            or "gooseneck" in t
            or cls.family in {"tabletop_mic", "ceiling_mic_array", "speakerphone", "videobar"}
        ):
            return False
        return True

    return True


def _role_score(
    item: Any,
    cls: ClassifiedCandidate,
    role: ExpandedRole,
    topology: TopologyDecision,
    req: ProjectRequirements,
) -> float:
    score = float(getattr(cls, "family_confidence", 0.0) or 0.0) * 10.0 + _candidate_quality(item)
    t = _text(item)

    if cls.family in (role.preferred_families or []):
        score += 4.0

    topo_pref = topology.preferred_families.get(role.role_key, []) if topology.preferred_families else []
    if cls.family in topo_pref:
        score += 2.0

    if req.room_type in (cls.room_fit or []):
        score += 1.0

    if req.room_type == "meeting_room" and cls.family in {"videowall_controller", "speaker_100v", "led_cabinet"}:
        score -= 10_000.0

    if topology.topology_key == "meeting_room_delegate_dsp" and cls.family == "videobar":
        score -= 10_000.0

    if role.role_key == "room_display_main":
        if "transparent" in t or "прозрачн" in t or "холодильник" in t or "outdoor" in t:
            score -= 500.0
        if any(x in t for x in ["mount kit", "ceiling mount", "wall mount", "trolley", "кронштейн", "стойка", "тележка"]):
            score -= 1000.0
        diag = _extract_diag_inches(t)
        if diag is not None:
            min_diag = _meeting_room_min_display_diag(req)
            if diag >= min_diag:
                score += 8.0
            else:
                score -= 50.0

    if role.role_key in {"room_camera_main", "room_camera_secondary"}:
        if "ptz" in t:
            score += 6.0
        if "videobar" in t:
            score += 2.0

    if role.role_key == "room_audio_capture":
        if "ceiling microphone" in t or "beamforming" in t or "table microphone" in t:
            score += 5.0

    if role.role_key == "room_signal_switching":
        if any(x in t for x in ["switcher", "matrix", "коммутатор", "gateway", "dock", "wireless presentation", "hdbaset"]):
            score += 6.0
        if any(x in t for x in ["intel i5", "intel i7", "win 10", "media player", "spinetix", "ops"]) and not any(
            x in t for x in ["switcher", "matrix", "gateway", "dock", "hub"]
        ):
            score -= 200.0

    return score


def _topn_for_role(role_key: str) -> int:
    if role_key in {"room_display_main", "room_camera_main", "room_camera_secondary", "room_signal_switching"}:
        return 1
    if role_key == "room_cabling_and_accessories":
        return 2
    return 1


def _ordered_unique(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for v in values:
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _allowed_families_from_roles(topology: TopologyDecision, roles: list[ExpandedRole]) -> list[str]:
    vals: list[str] = []

    for role in roles:
        vals.extend(role.allowed_families or [])
        vals.extend(role.preferred_families or [])

    for fams in (topology.preferred_families or {}).values():
        vals.extend(fams or [])

    vals.extend(
        [
            "cabling_av",
            "mounting_kit",
            "power_accessories",
            "managed_switch",
            "poe_switch",
        ]
    )

    return _ordered_unique(vals)


def _required_family_targets(role: ExpandedRole) -> list[str]:
    if role.preferred_families:
        return list(role.preferred_families[:3])
    if role.allowed_families:
        return list(role.allowed_families[:2])
    return []


def _role_is_core_required(role_key: str) -> bool:
    return role_key in {
        "room_display_main",
        "room_camera_main",
        "room_audio_capture",
        "room_audio_playback",
    }


def build_filtered_pool_for_coverage(
    *,
    pool: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
    roles: list[ExpandedRole],
) -> CoveragePlannerResult:
    items = _pool_items(pool)
    tasks = _pool_tasks(pool)

    classified = classify_candidates(items)
    cls_by_id = {c.candidate_id: c for c in classified}
    item_by_id = {getattr(i, "candidate_id"): i for i in items}

    forbidden = set(forbidden_families_for_requirements(requirements))
    allowed_families = set(_allowed_families_from_roles(topology, roles))

    eligible: set[str] = set()
    dropped: set[str] = set()
    warnings: list[str] = []
    drop_debug: list[CandidateDropDebug] = []

    for c in classified:
        cid = c.candidate_id
        it = item_by_id.get(cid)

        if it is None:
            dropped.add(cid)
            drop_debug.append(CandidateDropDebug(candidate_id=cid, family=c.family, reason="missing_item"))
            continue

        if not c.family:
            dropped.add(cid)
            drop_debug.append(CandidateDropDebug(candidate_id=cid, family=None, reason="unclassified"))
            continue

        if c.family in forbidden:
            dropped.add(cid)
            drop_debug.append(CandidateDropDebug(candidate_id=cid, family=c.family, reason="forbidden_family"))
            continue

        if allowed_families and c.family not in allowed_families:
            dropped.add(cid)
            drop_debug.append(CandidateDropDebug(candidate_id=cid, family=c.family, reason="family_not_allowed"))
            continue

        if requirements.room_type == "meeting_room" and c.family in {"videowall_controller", "speaker_100v", "led_cabinet"}:
            dropped.add(cid)
            drop_debug.append(CandidateDropDebug(candidate_id=cid, family=c.family, reason="room_type_exclusion"))
            continue

        if topology.topology_key == "meeting_room_delegate_dsp" and c.family == "videobar":
            dropped.add(cid)
            drop_debug.append(CandidateDropDebug(candidate_id=cid, family=c.family, reason="delegate_topology_exclusion"))
            continue

        if c.room_fit and requirements.room_type not in c.room_fit:
            dropped.add(cid)
            drop_debug.append(CandidateDropDebug(candidate_id=cid, family=c.family, reason="room_fit_mismatch"))
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
            if not _role_predicate(role.role_key, c, it, requirements):
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

    core_required_uncovered = any(
        dbg.required and _role_is_core_required(dbg.role_key) and not dbg.selected_candidate_ids
        for dbg in role_debug
    )

    # если ядро не закрыто, support-монтаж в финальную выдачу не пускаем
    if core_required_uncovered:
        for dbg in role_debug:
            if dbg.role_key == "room_cabling_and_accessories" and dbg.selected_candidate_ids:
                for cid in dbg.selected_candidate_ids:
                    kept.discard(cid)
                dbg.warnings.append("suppressed_until_core_roles_closed")
                dbg.selected_candidate_ids = []
                dbg.selected_families = []

    support_families = {
        "conference_controller",
        "dsp",
        "wireless_receiver",
        "managed_switch",
        "poe_switch",
        "presentation_switcher",
        "discussion_central_unit",
        "discussion_dsp",
        "power_supply_discussion",
        "cabling_av",
        "mounting_kit",
        "power_accessories",
    }

    if not core_required_uncovered:
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

    covered_families = _ordered_unique(
        [
            str(cls_by_id[cid].family)
            for cid in kept_in_order
            if cid in cls_by_id and getattr(cls_by_id[cid], "family", None)
        ]
    )

    uncovered_targets: list[str] = []
    for role, dbg in zip(ordered_roles, role_debug):
        if not role.required:
            continue
        if dbg.selected_candidate_ids:
            continue
        uncovered_targets.extend(_required_family_targets(role))

    uncovered_families = [fam for fam in _ordered_unique(uncovered_targets) if fam not in covered_families]

    return CoveragePlannerResult(
        filtered_pool=_rebuild_pool_like(pool, kept_tasks, kept_items),
        kept_candidate_ids=kept_in_order,
        dropped_candidate_ids=[getattr(i, "candidate_id") for i in items if getattr(i, "candidate_id") in dropped],
        role_debug=role_debug,
        warnings=warnings,
        allowed_families=sorted(allowed_families),
        covered_families=covered_families,
        uncovered_families=uncovered_families,
        drop_debug=drop_debug,
    )