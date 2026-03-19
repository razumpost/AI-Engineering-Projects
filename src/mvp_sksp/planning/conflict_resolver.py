from __future__ import annotations

from ..knowledge.loader import load_knowledge_map
from ..knowledge.models import ProjectRequirements


def forbidden_families_for_requirements(requirements: ProjectRequirements) -> set[str]:
    km = load_knowledge_map()
    forbidden: set[str] = set()

    for family_key, rule in km.conflict_rules.items():
        if rule.forbidden_room_types and requirements.room_type in rule.forbidden_room_types:
            forbidden.add(family_key)
            continue

        if rule.allowed_room_types and requirements.room_type not in rule.allowed_room_types:
            forbidden.add(family_key)

    if requirements.exclusions.led:
        forbidden.update(
            {
                "led_cabinet",
                "led_processor",
                "sending_card",
                "receiving_card",
                "led_signal_accessories",
                "led_structure",
                "led_rigging",
                "led_floor_support",
                "led_spares",
                "led_service_toolkit",
                "transport_case",
            }
        )

    if requirements.exclusions.projector:
        forbidden.update({"projector", "projection_screen"})

    if requirements.exclusions.operator_room:
        forbidden.update({"operator_monitor", "video_mixer", "recorder", "stream_encoder"})

    return forbidden