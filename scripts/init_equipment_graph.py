from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class FamilyDef:
    family_id: str
    name: str
    kind: str
    description: str
    keywords: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RelationDef:
    rel_type: str
    src_family: str
    dst_family: str
    condition_key: str = "always"
    rationale: str = ""
    min_qty_expr: str = ""
    max_qty_expr: str = ""
    priority: int = 100


FAMILIES: list[FamilyDef] = [
    FamilyDef(
        family_id="meeting_room_solution",
        name="Meeting room solution",
        kind="solution",
        description="Базовое решение для переговорной/комнаты совещаний.",
        keywords=["переговорная", "conference", "meeting room", "комната переговоров"],
    ),
    FamilyDef(
        family_id="display",
        name="Professional display",
        kind="hardware",
        description="Профессиональный дисплей/панель/экран для переговорной или signage.",
        keywords=["display", "дисплей", "панель", "экран", "professional display", "smart display"],
    ),
    FamilyDef(
        family_id="mount_display",
        name="Display mount / stand / trolley",
        kind="mount",
        description="Кронштейн/стойка/тележка для дисплея.",
        keywords=["mount", "кронштейн", "стойка", "тележка", "trolley", "wall mount"],
    ),
    FamilyDef(
        family_id="ptz_camera",
        name="PTZ / conference camera",
        kind="hardware",
        description="Камера для переговорной: PTZ, USB, conference camera.",
        keywords=["ptz", "camera", "камера", "conference camera", "usb camera"],
    ),
    FamilyDef(
        family_id="camera_controller",
        name="Camera controller",
        kind="hardware",
        description="Контроллер PTZ-камер, нужен при нескольких камерах или сложном управлении.",
        keywords=["controller", "ptz controller", "камерный контроллер"],
    ),
    FamilyDef(
        family_id="microphone",
        name="Conference microphone",
        kind="hardware",
        description="Микрофон для переговорной: потолочный, настольный, beamforming.",
        keywords=["microphone", "микрофон", "ceiling microphone", "table microphone", "beamforming"],
    ),
    FamilyDef(
        family_id="speakerphone",
        name="Speakerphone / conference audio bar",
        kind="hardware",
        description="Спикерфон/саундбар для компактной переговорной.",
        keywords=["speakerphone", "soundbar", "conference speaker"],
    ),
    FamilyDef(
        family_id="audio_processor",
        name="Audio DSP / processor",
        kind="hardware",
        description="DSP / аудиопроцессор для более сложной аудиотопологии.",
        keywords=["dsp", "audio processor", "аудиопроцессор"],
    ),
    FamilyDef(
        family_id="network_switch",
        name="Network switch",
        kind="hardware",
        description="Коммутатор для IP-оборудования, камер, конференц-системы, signage.",
        keywords=["switch", "network switch", "коммутатор"],
    ),
    FamilyDef(
        family_id="cabling_av",
        name="AV cabling",
        kind="cable",
        description="Кабельная инфраструктура: HDMI/USB/LAN/XLR и т.д.",
        keywords=["hdmi", "usb", "cable", "кабель", "витая пара", "xlr"],
    ),
    FamilyDef(
        family_id="smart_player",
        name="Smart player / signage player",
        kind="hardware",
        description="Медиаплеер / signage player для управления контентом на дисплее.",
        keywords=["smart player", "spinetix", "player", "signage player", "медиаплеер"],
    ),
    FamilyDef(
        family_id="signage_license",
        name="Signage software / license",
        kind="software",
        description="ПО/лицензия для signage-плеера/системы контента.",
        keywords=["license", "лицензия", "cms", "elementi", "software", "signage software"],
    ),
    FamilyDef(
        family_id="delegate_unit",
        name="Delegate unit",
        kind="hardware",
        description="Пульт делегата для дискуссионной/конференц-системы.",
        keywords=["delegate unit", "пульт делегата", "делегатский пульт"],
    ),
    FamilyDef(
        family_id="chairman_unit",
        name="Chairman unit",
        kind="hardware",
        description="Пульт председателя для дискуссионной системы.",
        keywords=["chairman unit", "пульт председателя"],
    ),
    FamilyDef(
        family_id="discussion_central_unit",
        name="Discussion central unit",
        kind="hardware",
        description="Центральный блок конференц/дискуссионной системы.",
        keywords=["central unit", "discussion system", "центральный блок"],
    ),
    FamilyDef(
        family_id="discussion_dsp",
        name="Discussion DSP",
        kind="hardware",
        description="DSP/процессор для интеграции дискуссионной системы с внешним аудио.",
        keywords=["dsp", "discussion dsp", "conference dsp", "аудиопроцессор"],
    ),
    FamilyDef(
        family_id="power_supply_discussion",
        name="Discussion PSU / extender",
        kind="hardware",
        description="Блок питания/расширения для дискуссионной системы при росте числа пультов.",
        keywords=["power supply", "psu", "блок питания", "расширение"],
    ),
]


RELATIONS: list[RelationDef] = [
    RelationDef(
        rel_type="REQUIRES",
        src_family="meeting_room_solution",
        dst_family="display",
        condition_key="always",
        rationale="В переговорной обычно нужен основной дисплей/панель.",
        priority=10,
    ),
    RelationDef(
        rel_type="REQUIRES",
        src_family="meeting_room_solution",
        dst_family="ptz_camera",
        condition_key="camera_requested",
        rationale="Если в запросе есть камеры/видеосвязь, требуется камера.",
        priority=20,
    ),
    RelationDef(
        rel_type="REQUIRES",
        src_family="meeting_room_solution",
        dst_family="microphone",
        condition_key="microphone_requested",
        rationale="Если нужны микрофоны/захват речи, требуется микрофонная подсистема.",
        priority=30,
    ),
    RelationDef(
        rel_type="OPTIONAL_WITH",
        src_family="meeting_room_solution",
        dst_family="speakerphone",
        condition_key="small_room_usb",
        rationale="Для компактной USB-переговорки возможен спикерфон/саундбар.",
        priority=40,
    ),
    RelationDef(
        rel_type="OPTIONAL_WITH",
        src_family="meeting_room_solution",
        dst_family="audio_processor",
        condition_key="room_large",
        rationale="Для большой переговорной аудиотопология часто требует DSP.",
        priority=50,
    ),
    RelationDef(
        rel_type="OPTIONAL_WITH",
        src_family="display",
        dst_family="mount_display",
        condition_key="always",
        rationale="Дисплею почти всегда нужен способ монтажа/установки.",
        priority=60,
    ),
    RelationDef(
        rel_type="OPTIONAL_WITH",
        src_family="display",
        dst_family="smart_player",
        condition_key="signage",
        rationale="Для signage/контентного режима к дисплею добавляется плеер.",
        priority=70,
    ),
    RelationDef(
        rel_type="OPTIONAL_WITH",
        src_family="smart_player",
        dst_family="signage_license",
        condition_key="signage",
        rationale="Плееру signage часто требуется лицензия/ПО.",
        priority=80,
    ),
    RelationDef(
        rel_type="OPTIONAL_WITH",
        src_family="ptz_camera",
        dst_family="camera_controller",
        condition_key="multi_camera",
        rationale="При нескольких камерах полезен/нужен контроллер.",
        priority=90,
    ),
    RelationDef(
        rel_type="OPTIONAL_WITH",
        src_family="ptz_camera",
        dst_family="network_switch",
        condition_key="ip_av",
        rationale="IP-камеры/сетевая AV-инфраструктура требуют коммутации.",
        priority=100,
    ),
    RelationDef(
        rel_type="OPTIONAL_WITH",
        src_family="microphone",
        dst_family="audio_processor",
        condition_key="room_large",
        rationale="Для большого помещения и сложной акустики часто нужен DSP.",
        priority=110,
    ),
    RelationDef(
        rel_type="OPTIONAL_WITH",
        src_family="meeting_room_solution",
        dst_family="cabling_av",
        condition_key="always",
        rationale="Любое AV-решение требует кабельной инфраструктуры.",
        priority=120,
    ),
    RelationDef(
        rel_type="REQUIRES",
        src_family="delegate_unit",
        dst_family="discussion_central_unit",
        condition_key="always",
        rationale="Пульты делегатов требуют центральный блок.",
        priority=130,
    ),
    RelationDef(
        rel_type="REQUIRES",
        src_family="chairman_unit",
        dst_family="discussion_central_unit",
        condition_key="always",
        rationale="Пульт председателя требует центральный блок.",
        priority=140,
    ),
    RelationDef(
        rel_type="OPTIONAL_WITH",
        src_family="discussion_central_unit",
        dst_family="discussion_dsp",
        condition_key="external_audio_processing",
        rationale="Для интеграции с внешним аудио/заловой акустикой может требоваться DSP.",
        priority=150,
    ),
    RelationDef(
        rel_type="OPTIONAL_WITH",
        src_family="discussion_central_unit",
        dst_family="power_supply_discussion",
        condition_key="delegate_count_gt_20",
        rationale="При большом числе пультов может потребоваться доп. питание/расширение.",
        priority=160,
    ),
    RelationDef(
        rel_type="OPTIONAL_WITH",
        src_family="discussion_central_unit",
        dst_family="network_switch",
        condition_key="ip_av",
        rationale="IP-конференц/дискуссионные системы требуют сетевую инфраструктуру.",
        priority=170,
    ),
    RelationDef(
        rel_type="OPTIONAL_WITH",
        src_family="discussion_central_unit",
        dst_family="cabling_av",
        condition_key="always",
        rationale="Дискуссионная система требует коммутацию/кабели.",
        priority=180,
    ),
]


def _extract_int(patterns: list[str], text: str) -> int | None:
    for p in patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
    return None


def derive_request_flags(request_text: str) -> dict[str, Any]:
    q = (request_text or "").casefold()

    seats = _extract_int(
        [
            r"на\s+(\d+)\s+мест",
            r"(\d+)\s+мест",
            r"(\d+)\s+seats?",
        ],
        q,
    )

    camera_count = _extract_int(
        [
            r"(\d+)\s+камер",
            r"(\d+)\s+cameras?",
        ],
        q,
    )
    if camera_count is None:
        if "две камеры" in q or "два камеры" in q:
            camera_count = 2
        elif "одна камера" in q or "один камера" in q:
            camera_count = 1

    signage = any(x in q for x in ["smart player", "spinetix", "signage", "cms", "лиценз", "license"])
    discussion = any(
        x in q
        for x in [
            "пульт делегата",
            "пульты делегатов",
            "пульт председателя",
            "delegate unit",
            "chairman unit",
            "дискуссион",
            "conference unit",
        ]
    )
    display_requested = any(x in q for x in ["дисплей", "экран", "панель", "display", "screen"])
    microphone_requested = any(x in q for x in ["микрофон", "microphone", "mic"])
    camera_requested = (camera_count or 0) > 0 or any(x in q for x in ["камера", "camera", "ptz"])
    ip_av = any(x in q for x in ["ip", "ndi", "network", "lan", "poe"])

    external_audio_processing = any(
        x in q
        for x in [
            "dsp",
            "аудиопроцессор",
            "внешняя акустика",
            "усилитель",
            "интеграция со звуком",
            "интеграция с аудио",
            "подключение к звуку",
            "подключение к акустике",
            "заловой звук",
            "внешний звук",
            "внешнее аудио",
            "интеграция в существующую аудиосистему",
            "интеграция со звуковой системой",
        ]
    )

    return {
        "request_text": request_text,
        "seats": seats or 0,
        "camera_count": camera_count or 0,
        "camera_requested": camera_requested,
        "microphone_requested": microphone_requested,
        "display_requested": display_requested,
        "signage": signage,
        "discussion": discussion,
        "multi_camera": (camera_count or 0) >= 2,
        "room_large": (seats or 0) >= 16,
        "small_room_usb": ("usb" in q or "byod" in q) and not discussion and (seats or 0) <= 10,
        "external_audio_processing": external_audio_processing,
        "delegate_count_gt_20": (seats or 0) > 20 or "более 20" in q,
        "ip_av": ip_av,
    }


def infer_seed_families(request_text: str) -> list[str]:
    flags = derive_request_flags(request_text)
    q = (request_text or "").casefold()

    seeds: list[str] = []

    def add(family_id: str) -> None:
        if family_id not in seeds:
            seeds.append(family_id)

    if any(x in q for x in ["переговор", "conference", "meeting room", "byod"]):
        add("meeting_room_solution")

    if flags["display_requested"]:
        add("display")

    if flags["camera_requested"]:
        add("ptz_camera")

    if flags["microphone_requested"]:
        add("microphone")

    if flags["signage"]:
        add("smart_player")
        add("signage_license")

    if flags["discussion"]:
        add("delegate_unit")
        add("discussion_central_unit")

    if "председател" in q or "chairman" in q:
        add("chairman_unit")

    return seeds


def condition_matches(condition_key: str, flags: dict[str, Any]) -> bool:
    if condition_key == "always":
        return True
    return bool(flags.get(condition_key, False))