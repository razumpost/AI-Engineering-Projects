from __future__ import annotations

from dataclasses import dataclass, field

from ..knowledge.loader import load_knowledge_map
from ..knowledge.models import ProjectRequirements, RoleDef


@dataclass(frozen=True)
class ExpandedRole:
    role_key: str
    source: str
    required: bool
    qty_rule: str
    suggested_qty: int | None
    allowed_families: list[str] = field(default_factory=list)
    preferred_families: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _discussion_only_meeting_room(requirements: ProjectRequirements) -> bool:
    if requirements.room_type != "meeting_room":
        return False

    seat_count = requirements.caps.seat_count or 0
    if seat_count < 8:
        return False

    if requirements.flags.vks or requirements.flags.byod or requirements.flags.presentation:
        return False
    if requirements.caps.display_count or requirements.caps.camera_count:
        return False

    return bool(requirements.flags.control)


def _needs_display_role(requirements: ProjectRequirements) -> bool:
    if requirements.room_type != "meeting_room":
        return True
    if _discussion_only_meeting_room(requirements):
        return False
    return bool(
        requirements.flags.presentation
        or requirements.flags.vks
        or requirements.caps.display_count
    )


def _needs_signal_switching(requirements: ProjectRequirements) -> bool:
    if requirements.room_type != "meeting_room":
        return True
    if _discussion_only_meeting_room(requirements):
        return False
    return bool(
        requirements.flags.presentation
        or requirements.flags.byod
        or requirements.flags.vks
        or requirements.caps.display_count
    )


def _enabled_capabilities(requirements: ProjectRequirements, room_default_caps: list[str]) -> list[str]:
    enabled = set()

    for cap in room_default_caps:
        if cap == "presentation" and not _needs_display_role(requirements):
            continue
        enabled.add(cap)

    for cap_key, enabled_flag in requirements.flags.model_dump().items():
        if enabled_flag:
            enabled.add(cap_key)

    return sorted(enabled)


def _filter_allowed_families(role_key: str, role_def: RoleDef, requirements: ProjectRequirements) -> list[str]:
    allowed = list(role_def.allowed_families)
    discussion_only = _discussion_only_meeting_room(requirements)

    if requirements.exclusions.projector or requirements.room_type == "meeting_room":
        allowed = [f for f in allowed if f != "projector"]

    if requirements.exclusions.led or requirements.room_type != "led_screen":
        blocked_led = {
            "led_cabinet",
            "led_processor",
            "sending_card",
            "receiving_card",
            "led_signal_accessories",
            "power_distribution",
            "led_power_accessories",
            "led_structure",
            "led_rigging",
            "led_floor_support",
            "led_spares",
            "led_service_toolkit",
            "transport_case",
        }
        allowed = [f for f in allowed if f not in blocked_led]

    if requirements.room_type != "videowall":
        blocked_wall = {"videowall_panel", "videowall_mount", "videowall_controller", "splitter"}
        allowed = [f for f in allowed if f not in blocked_wall]

    if requirements.exclusions.operator_room:
        blocked_operator = {"operator_monitor", "recorder", "stream_encoder"}
        allowed = [f for f in allowed if f not in blocked_operator]

    if discussion_only:
        if role_key == "room_conference_controller":
            for fam in ["discussion_central_unit", "conference_controller"]:
                if fam not in allowed:
                    allowed.append(fam)

        if role_key == "room_audio_processing":
            for fam in ["discussion_dsp", "dsp", "conference_controller"]:
                if fam not in allowed:
                    allowed.append(fam)

        if role_key == "room_cabling_and_accessories":
            for fam in ["cabling_av", "power_supply_discussion", "power_accessories"]:
                if fam not in allowed:
                    allowed.append(fam)

        if role_key == "room_audio_capture":
            allowed = [f for f in allowed if f not in {"speakerphone", "videobar"}]
            for fam in ["delegate_unit", "chairman_unit", "tabletop_mic", "ceiling_mic_array"]:
                if fam not in allowed:
                    allowed.append(fam)
    else:
        # ordinary meeting room: никаких discussion family по умолчанию
        if role_key == "room_audio_capture":
            allowed = [
                f for f in allowed
                if f not in {"delegate_unit", "chairman_unit", "discussion_central_unit", "conference_controller"}
            ]
        if role_key == "room_audio_processing":
            allowed = [
                f for f in allowed
                if f not in {"discussion_dsp", "conference_controller", "discussion_central_unit"}
            ]
        if role_key == "room_conference_controller":
            allowed = []

    return allowed


def _preferred_families(role_key: str, requirements: ProjectRequirements, allowed: list[str]) -> list[str]:
    seat_count = requirements.caps.seat_count or 0
    discussion_only = _discussion_only_meeting_room(requirements)

    presets: dict[str, list[str]] = {
        "room_display_main": ["display_panel", "interactive_panel"],
        "room_camera_main": ["ptz_camera", "fixed_conference_camera", "videobar"],
        "room_camera_secondary": ["ptz_camera", "fixed_conference_camera"],
        "room_byod_ingest": ["byod_usb_hdmi_gateway", "byod_wireless_presentation", "usb_c_dock"],
        "room_usb_bridge_or_byod_gateway": ["byod_usb_hdmi_gateway", "usb_dsp_bridge", "videobar"],
        "room_signal_switching": ["presentation_switcher", "matrix_switcher", "simple_io_hub"],
        "room_control_ui": ["touch_panel", "keypad_controller"],
        "room_control_processor": ["control_processor"],
        "wall_signal_controller": ["videowall_controller"],
        "led_processor": ["led_processor"],
        "hall_signal_switching": ["matrix_switcher", "presentation_switcher", "av_over_ip_tx", "av_over_ip_rx"],
        "hall_control": ["touch_panel", "control_processor"],
    }

    if role_key == "room_audio_capture":
        if discussion_only:
            presets[role_key] = ["delegate_unit", "chairman_unit", "tabletop_mic", "ceiling_mic_array"]
        elif seat_count >= 8:
            presets[role_key] = ["ceiling_mic_array", "tabletop_mic", "speakerphone", "videobar"]
        else:
            presets[role_key] = ["speakerphone", "videobar", "tabletop_mic"]

    if role_key == "room_audio_playback":
        if discussion_only:
            presets[role_key] = ["wall_speaker", "ceiling_speaker", "soundbar"]
        elif requirements.room_type == "meeting_room":
            presets[role_key] = ["soundbar", "wall_speaker", "ceiling_speaker", "videobar"]
        else:
            presets[role_key] = ["line_array", "active_speaker", "wall_speaker", "ceiling_speaker"]

    if role_key == "room_audio_processing":
        if discussion_only:
            presets[role_key] = ["discussion_dsp", "dsp", "conference_controller"]
        else:
            presets[role_key] = ["dsp", "usb_dsp_bridge"]

    if role_key == "room_conference_controller" and discussion_only:
        presets[role_key] = ["discussion_central_unit", "conference_controller"]

    if role_key == "room_cabling_and_accessories" and discussion_only:
        presets[role_key] = ["cabling_av", "power_supply_discussion", "power_accessories"]

    wanted = presets.get(role_key, [])
    result = [f for f in wanted if f in allowed]
    if not result:
        result = allowed[:]
    return result


def _suggested_qty(role_key: str, qty_rule: str, requirements: ProjectRequirements) -> int | None:
    if role_key == "room_display_main":
        return requirements.caps.display_count or 1

    if role_key == "room_camera_main":
        return 1

    if role_key == "room_camera_secondary":
        cam_count = requirements.caps.camera_count or 1
        return max(0, cam_count - 1) or None

    if qty_rule in {"one", "one_per_room"}:
        return 1

    if qty_rule == "same_as_display_count":
        return requirements.caps.display_count

    if qty_rule == "same_as_camera_count":
        return requirements.caps.camera_count

    if qty_rule == "same_as_seat_count":
        return requirements.caps.seat_count

    if qty_rule == "seat_count_minus_one":
        return max(0, (requirements.caps.seat_count or 0) - 1) or None

    return None


def _build_role(role_key: str, source: str, requirements: ProjectRequirements, role_def: RoleDef) -> ExpandedRole:
    allowed = _filter_allowed_families(role_key, role_def, requirements)
    preferred = _preferred_families(role_key, requirements, allowed)
    suggested_qty = _suggested_qty(role_key, role_def.qty_rule, requirements)

    notes = list(role_def.notes)
    if role_key == "room_camera_secondary" and suggested_qty:
        notes.append("Роль добавлена из caps.camera_count > 1.")
    if role_key == "room_display_main" and requirements.room_type == "meeting_room":
        notes.append("Для переговорной по умолчанию предпочтительнее panel family, не projector.")
    if role_key == "room_audio_capture" and (requirements.caps.seat_count or 0) >= 8 and not _discussion_only_meeting_room(requirements):
        notes.append(
            "Для переговорных на 8+ мест предпочтительнее потолочные/настольные конференц-микрофоны, а не discussion system."
        )
    if _discussion_only_meeting_room(requirements):
        notes.append("Discussion-only meeting room heuristic is active.")

    return ExpandedRole(
        role_key=role_key,
        source=source,
        required=role_def.required,
        qty_rule=role_def.qty_rule,
        suggested_qty=suggested_qty,
        allowed_families=allowed,
        preferred_families=preferred,
        notes=notes,
    )


def expand_required_roles(requirements: ProjectRequirements) -> list[ExpandedRole]:
    km = load_knowledge_map()

    if requirements.room_type not in km.room_types:
        raise ValueError(f"Unknown room_type: {requirements.room_type}")

    out: list[ExpandedRole] = []
    seen: set[str] = set()

    room_def = km.room_types[requirements.room_type]
    discussion_only = _discussion_only_meeting_room(requirements)

    for role_key in room_def.default_roles:
        if role_key == "room_display_main" and not _needs_display_role(requirements):
            continue
        if role_key == "room_signal_switching" and not _needs_signal_switching(requirements):
            continue
        if role_key == "room_conference_controller" and not discussion_only:
            continue

        role_def = km.roles.get(role_key)
        if not role_def or role_key in seen:
            continue

        built = _build_role(role_key, "room_type", requirements, role_def)
        if built.allowed_families:
            out.append(built)
            seen.add(role_key)

    for cap_key in _enabled_capabilities(requirements, room_def.default_capabilities):
        cap_def = km.capabilities.get(cap_key)
        if not cap_def:
            continue
        for role_key in cap_def.adds_roles:
            if role_key == "room_display_main" and not _needs_display_role(requirements):
                continue
            if role_key == "room_signal_switching" and not _needs_signal_switching(requirements):
                continue
            if role_key == "room_conference_controller" and not discussion_only:
                continue

            role_def = km.roles.get(role_key)
            if not role_def or role_key in seen:
                continue

            built = _build_role(role_key, f"capability:{cap_key}", requirements, role_def)
            if built.allowed_families:
                out.append(built)
                seen.add(role_key)

    if requirements.room_type == "meeting_room":
        cam_count = requirements.caps.camera_count or 0
        if cam_count > 1 and "room_camera_secondary" not in seen:
            role_def = km.roles.get("room_camera_secondary")
            if role_def:
                built = _build_role("room_camera_secondary", "derived:camera_count", requirements, role_def)
                if built.allowed_families:
                    out.append(built)
                    seen.add("room_camera_secondary")

    if discussion_only:
        for role_key in ["room_audio_processing", "room_conference_controller"]:
            if role_key not in seen:
                role_def = km.roles.get(role_key)
                if role_def:
                    built = _build_role(role_key, "derived:discussion_only", requirements, role_def)
                    if built.allowed_families:
                        out.append(built)
                        seen.add(role_key)

    return out


def expand_req(requirements: ProjectRequirements) -> ProjectRequirements:
    return requirements