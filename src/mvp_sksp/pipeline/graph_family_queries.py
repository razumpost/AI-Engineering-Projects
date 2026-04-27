from __future__ import annotations

from typing import Iterable


_FAMILY_TO_PRICE_QUERIES: dict[str, list[str]] = {
    "meeting_room_solution": [
        "conference system",
        "meeting room system",
        "usb conference system",
    ],
    "display": [
        "professional display",
        "conference display",
        "interactive display",
    ],
    "mount_display": [
        "display mount",
        "display trolley",
        "wall mount display",
    ],
    "videowall_solution": [
        "videowall panel",
        "videowall controller",
        "videowall mount",
        "matrix switcher",
    ],
    "videowall_panel": [
        "videowall panel",
        "lcd videowall panel",
        "narrow bezel videowall display",
        "панель для видеостены",
        "дисплей для видеостены",
    ],
    "videowall_mount": [
        "videowall mount",
        "pull-out wall mount",
        "videowall frame",
    ],
    "videowall_controller": [
        "videowall controller",
        "videowall processor",
        "video wall processor",
        "контроллер видеостены",
        "процессор видеостены",
    ],
    "matrix_switcher": [
        "matrix switcher",
        "hdmi matrix switcher",
        "matrix controller",
        "матричный коммутатор",
    ],
    "ptz_camera": [
        "ptz camera",
        "conference camera",
        "usb camera",
    ],
    "camera_controller": [
        "ptz controller",
        "camera controller",
    ],
    "microphone": [
        "conference microphone",
        "table microphone",
        "ceiling microphone",
        "delegate microphone unit",
    ],
    "speakerphone": [
        "speakerphone",
        "conference soundbar",
    ],
    "audio_processor": [
        "audio dsp",
        "conference dsp",
        "audio processor",
    ],
    "network_switch": [
        "network switch",
        "poe switch",
    ],
    "cabling_av": [
        "hdmi cable",
        "usb cable",
        "xlr cable",
        "cat6 cable",
    ],
    "smart_player": [
        "smart player",
        "spinetix player",
        "signage player",
    ],
    "signage_license": [
        "player license",
        "signage software",
        "cms signage",
        "spinetix license",
    ],
    "delegate_unit": [
        "delegate unit",
        "conference delegate unit",
        "пульт делегата",
    ],
    "chairman_unit": [
        "chairman unit",
        "conference chairman unit",
        "пульт председателя",
    ],
    "discussion_central_unit": [
        "discussion central unit",
        "conference central unit",
        "central unit discussion system",
        "центральный блок конференц системы",
    ],
    "discussion_dsp": [
        "discussion dsp",
        "conference dsp",
        "audio dsp",
        "аудиопроцессор конференц системы",
    ],
    "power_supply_discussion": [
        "conference power supply",
        "discussion power supply",
        "delegate system power supply",
        "блок питания конференц системы",
    ],
}


def graph_families_to_queries(family_ids: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for family_id in family_ids:
        for q in _FAMILY_TO_PRICE_QUERIES.get(family_id, []):
            if q not in seen:
                seen.add(q)
                out.append(q)

    return out