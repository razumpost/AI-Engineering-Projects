from __future__ import annotations

from ..knowledge.loader import load_knowledge_map
from ..knowledge.models import ProjectRequirements
from .plan_models import TopologyDecision


def _flag_enabled(requirements: ProjectRequirements, flag: str) -> bool:
    flags = requirements.flags.model_dump()
    return bool(flags.get(flag, False))


def _preferred_score(preferred_when: dict, requirements: ProjectRequirements) -> float:
    score = 0.0

    seat_count = requirements.caps.seat_count
    camera_count = requirements.caps.camera_count
    display_count = requirements.caps.display_count

    if "min_seat_count" in preferred_when and seat_count is not None:
        if seat_count >= int(preferred_when["min_seat_count"]):
            score += 4.0
    if "max_seat_count" in preferred_when and seat_count is not None:
        if seat_count <= int(preferred_when["max_seat_count"]):
            score += 4.0

    if "min_camera_count" in preferred_when and camera_count is not None:
        if camera_count >= int(preferred_when["min_camera_count"]):
            score += 3.0
    if "max_camera_count" in preferred_when and camera_count is not None:
        if camera_count <= int(preferred_when["max_camera_count"]):
            score += 3.0

    if preferred_when.get("has_camera_count") is True and camera_count is not None:
        score += 2.0
    if preferred_when.get("has_display_count") is True and display_count is not None:
        score += 2.0

    return score


def select_topology(requirements: ProjectRequirements) -> TopologyDecision:
    km = load_knowledge_map()

    best: TopologyDecision | None = None

    for key, topology in km.topology_patterns.items():
        if requirements.room_type not in topology.room_types:
            continue

        missing_required = [flag for flag in topology.requires_flags if not _flag_enabled(requirements, flag)]
        if missing_required:
            continue

        score = 10.0
        reason_parts = [f"room_type={requirements.room_type}"]

        optional_hits = [flag for flag in topology.optional_flags if _flag_enabled(requirements, flag)]
        score += float(len(optional_hits))
        if optional_hits:
            reason_parts.append(f"optional_flags={optional_hits}")

        pref_score = _preferred_score(topology.preferred_when, requirements)
        score += pref_score
        if pref_score:
            reason_parts.append(f"preferred_when=+{pref_score:g}")

        decision = TopologyDecision(
            topology_key=key,
            score=score,
            reason="; ".join(reason_parts),
            required_roles=list(topology.roles.required),
            optional_roles=list(topology.roles.optional),
            preferred_families=dict(topology.preferred_families),
        )

        if best is None or decision.score > best.score:
            best = decision

    if best is not None:
        return best

    room_def = km.room_types[requirements.room_type]
    return TopologyDecision(
        topology_key=f"{requirements.room_type}_fallback",
        score=0.0,
        reason=f"fallback for room_type={requirements.room_type}",
        required_roles=list(room_def.default_roles),
        optional_roles=[],
        preferred_families={},
    )