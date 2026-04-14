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


def _looks_like_discussion_only(requirements: ProjectRequirements) -> bool:
    if requirements.room_type != "meeting_room":
        return False

    seat_count = requirements.caps.seat_count or 0
    camera_count = requirements.caps.camera_count
    display_count = requirements.caps.display_count
    flags = requirements.flags

    if seat_count < 8:
        return False
    if flags.vks or flags.byod:
        return False
    if flags.presentation:
        return False
    if camera_count:
        return False
    if display_count:
        return False

    # discussion-only допускаем только при ЯВНОМ control-сценарии,
    # а не просто от большого количества мест
    return bool(flags.control)


def _discussion_only_topology(requirements: ProjectRequirements) -> TopologyDecision:
    return TopologyDecision(
        topology_key="meeting_room_discussion_only",
        score=100.0,
        reason=(
            "heuristic discussion-only meeting room; "
            f"room_type={requirements.room_type}; "
            f"seat_count={requirements.caps.seat_count or 0}; "
            "no explicit vks/byod/presentation/display/camera; explicit control"
        ),
        required_roles=[
            "room_audio_capture",
            "room_audio_playback",
            "room_cabling_and_accessories",
        ],
        optional_roles=[
            "room_audio_processing",
            "room_conference_controller",
            "room_control_ui",
            "room_control_processor",
            "room_network_access",
        ],
        preferred_families={
            "room_audio_capture": ["delegate_unit", "chairman_unit", "tabletop_mic", "ceiling_mic_array"],
            "room_audio_playback": ["wall_speaker", "ceiling_speaker", "soundbar"],
            "room_audio_processing": ["discussion_dsp", "conference_controller", "dsp"],
            "room_conference_controller": ["discussion_central_unit", "conference_controller"],
            "room_cabling_and_accessories": ["cabling_av", "power_supply_discussion", "power_accessories"],
        },
    )


def _normalize_meeting_room_topology(
    decision: TopologyDecision,
    requirements: ProjectRequirements,
) -> TopologyDecision:
    if requirements.room_type != "meeting_room":
        return decision
    if _looks_like_discussion_only(requirements):
        return decision

    required_roles = [r for r in decision.required_roles if r != "room_conference_controller"]
    optional_roles = [r for r in decision.optional_roles if r != "room_conference_controller"]

    preferred_families = dict(decision.preferred_families)
    preferred_families.pop("room_conference_controller", None)

    if "room_audio_processing" in preferred_families:
        cleaned = [
            f
            for f in preferred_families["room_audio_processing"]
            if f not in {"discussion_dsp", "conference_controller", "discussion_central_unit"}
        ]
        preferred_families["room_audio_processing"] = cleaned or ["dsp", "usb_dsp_bridge"]

    return TopologyDecision(
        topology_key=decision.topology_key,
        score=decision.score,
        reason=decision.reason,
        required_roles=required_roles,
        optional_roles=optional_roles,
        preferred_families=preferred_families,
    )


def select_topology(requirements: ProjectRequirements) -> TopologyDecision:
    if _looks_like_discussion_only(requirements):
        return _discussion_only_topology(requirements)

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

        decision = _normalize_meeting_room_topology(decision, requirements)

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