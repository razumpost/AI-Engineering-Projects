from __future__ import annotations

import re

from ..knowledge.models import ProjectRequirements, RequirementCaps, RequirementExclusions, RequirementFlags

_SEATS_RE = re.compile(r"(?:на\s*)?(\d{1,3})\s*(?:мест|чел)", re.IGNORECASE)
_CAM_RE = re.compile(r"(\d{1,2})\s*(?:камер|camera|ptz)", re.IGNORECASE)
_DISPLAY_RE = re.compile(r"(\d{1,2})\s*(?:панел|диспле|экран)", re.IGNORECASE)


def _extract_int(rx: re.Pattern[str], text: str) -> int | None:
    m = rx.search(text or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _contains_any(text: str, phrases: list[str]) -> bool:
    t = (text or "").casefold()
    return any(p in t for p in phrases)


def _detect_room_type(text: str) -> str:
    t = (text or "").casefold()
    if "видеостен" in t:
        return "videowall"
    if "светодиод" in t or " led" in f" {t}" or "лед" in t:
        return "led_screen"
    if any(x in t for x in ["актов", "конференц-зал", "зал", "сцена", "трансляц", "амфитеатр"]):
        return "auditorium"
    return "meeting_room"


def parse_requirements(text: str) -> ProjectRequirements:
    raw = text or ""
    t = raw.casefold()
    room_type = _detect_room_type(raw)

    flags = RequirementFlags(
        vks=_contains_any(t, ["вкс", "видеоконферен", "videoconference", "vc"]),
        byod=_contains_any(t, ["byod", "ноутбук заказчика", "подключение ноутбука", "usb-c"]),
        presentation=_contains_any(t, ["презента", "панел", "диспле", "экран", "вывод контента"]),
        recording=_contains_any(t, ["запис", "record"]),
        streaming=_contains_any(t, ["трансляц", "стрим", "stream"]),
        speech_reinforcement=(room_type == "auditorium") or _contains_any(t, ["озвуч", "звукоусил", "speech reinforcement"]),
        control=_contains_any(t, ["управлен", "control", "тачпанел", "touch panel"]),
    )

    exclusions = RequirementExclusions(
        led=_contains_any(t, ["без led", "без светодиод", "не led", "не светодиод"]),
        projector=_contains_any(t, ["без проектора", "не проектор", "без проектор"]),
        operator_room=_contains_any(t, ["без операторской", "без операторной"]),
    )

    caps = RequirementCaps(
        seat_count=_extract_int(_SEATS_RE, raw),
        camera_count=_extract_int(_CAM_RE, raw),
        display_count=_extract_int(_DISPLAY_RE, raw),
    )

    confidence = {
        "room_type": 0.75,
        "seat_count": 0.95 if caps.seat_count else 0.0,
        "camera_count": 0.95 if caps.camera_count else 0.0,
        "display_count": 0.85 if caps.display_count else 0.0,
        "vks": 0.95 if flags.vks else 0.0,
        "byod": 0.95 if flags.byod else 0.0,
    }

    return ProjectRequirements(
        room_type=room_type,
        caps=caps,
        flags=flags,
        exclusions=exclusions,
        confidence=confidence,
    )
