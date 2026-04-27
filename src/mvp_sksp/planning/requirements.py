from __future__ import annotations

import re

from ..domain.equipment_graph import derive_request_flags
from ..knowledge.models import ProjectRequirements


_WORD_NUM = {
    "один": 1,
    "одна": 1,
    "одно": 1,
    "два": 2,
    "две": 2,
    "три": 3,
    "четыре": 4,
    "пять": 5,
}

_RE_SEATS = re.compile(r"(?:на\s*)?(\d{1,3})\s*(?:мест|чел)", re.IGNORECASE)
_RE_CAM_NUM = re.compile(r"(\d{1,2})\s*(?:камер|ptz)", re.IGNORECASE)
_RE_DISPLAY_NUM = re.compile(
    r"(\d{1,2})\s*(?:диспле(?:й|я|ев)|панел(?:ь|и|ей)|экран(?:а|ов)?|monitor|display)",
    re.IGNORECASE,
)
_RE_WALL_GRID_AXB = re.compile(
    r"(?:видеостен|videowall|стен\w*)?\s*(\d)\s*[xх*]\s*(\d)(?:\s*(?:из\s*)?(?:панел|монитор|экран))?",
    re.IGNORECASE,
)


def _contains_any(text: str, needles: list[str]) -> bool:
    t = (text or "").casefold()
    return any(x.casefold() in t for x in needles)


def _extract_int(regex: re.Pattern[str], text: str) -> int | None:
    m = regex.search(text or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _extract_word_number(text: str, anchors: list[str]) -> int | None:
    t = (text or "").casefold()
    for word, value in _WORD_NUM.items():
        for anchor in anchors:
            if f"{word} {anchor}" in t:
                return value
    return None


def _infer_wall_grid_panel_count(raw: str) -> int | None:
    t = (raw or "")
    m = _RE_WALL_GRID_AXB.search(t)
    if not m:
        return None
    try:
        a = int(m.group(1))
        b = int(m.group(2))
    except Exception:
        return None
    if a <= 0 or b <= 0:
        return None
    return a * b


def _infer_room_type(raw: str) -> str:
    t = (raw or "").casefold()

    if _contains_any(t, ["led экран", "светодиод", "медиафасад", "led cabinet"]):
        return "led_screen"

    if _infer_wall_grid_panel_count(raw or "") or _contains_any(
        t,
        ["видеостен", "videowall", "ситуацион", "цод", "диспетчер", "стена 3*3", "стена 3х3"],
    ):
        return "videowall"

    if _contains_any(t, ["актовый зал", "конференц-зал", "аудитори", "lecture", "hall", "амфитеатр"]):
        return "hall"

    return "meeting_room"


def _explicit_discussion_request(raw: str, graph_flags: dict) -> bool:
    if bool(graph_flags.get("discussion")):
        return True

    return _contains_any(
        raw,
        [
            "пульт делегата",
            "пульты делегатов",
            "пульт председателя",
            "delegate unit",
            "chairman unit",
            "дискуссион",
            "discussion system",
            "conference unit",
            "центральный блок конференц",
            "центральный блок дискуссион",
        ],
    )


def _extract_seat_count(raw: str, graph_flags: dict) -> int | None:
    seats = graph_flags.get("seats")
    if isinstance(seats, int) and seats > 0:
        return seats
    return _extract_int(_RE_SEATS, raw)


def _explicit_camera_request(raw: str) -> bool:
    if _extract_int(_RE_CAM_NUM, raw):
        return True
    if _extract_word_number(raw, ["камеры", "камер", "camera", "ptz"]):
        return True
    return _contains_any(raw, ["камера", "ptz", "webcam"]) or (
        "camera" in (raw or "").casefold()
        and ("ptz" in (raw or "").casefold() or "usb" in (raw or "").casefold() or "conference" in (raw or "").casefold())
    )


def _extract_camera_count(raw: str, graph_flags: dict, *, discussion_mode: bool) -> int | None:
    direct = _extract_int(_RE_CAM_NUM, raw)
    if direct:
        return direct

    by_word = _extract_word_number(raw, ["камеры", "камер", "camera", "ptz"])
    if by_word:
        return by_word

    if discussion_mode:
        return None

    camera_count = graph_flags.get("camera_count")
    if isinstance(camera_count, int) and camera_count > 0:
        return camera_count

    if graph_flags.get("camera_requested"):
        return 1

    return None


def _explicit_display_request(raw: str) -> bool:
    if _extract_int(_RE_DISPLAY_NUM, raw):
        return True
    return _contains_any(raw, ["дисплей", "панель", "экран", "display", "monitor"])


def _extract_display_count(raw: str, graph_flags: dict, *, discussion_mode: bool, room_type: str) -> int | None:
    direct = _extract_int(_RE_DISPLAY_NUM, raw)
    if direct:
        return direct

    wall_panels = _infer_wall_grid_panel_count(raw)
    if wall_panels:
        return wall_panels

    if _contains_any(raw, ["дисплей", "панель", "экран", "display", "monitor"]):
        return 1

    if discussion_mode:
        return None

    if graph_flags.get("display_requested"):
        return 1 if room_type != "videowall" else None

    return None


def _explicit_control_only(raw: str) -> bool:
    return _contains_any(
        raw,
        [
            "управление",
            "система управления",
            "панель управления",
            "тачпанел",
            "touch panel",
            "control processor",
            "процессор управления",
            "автоматизац",
            "control ui",
            "keypad controller",
        ],
    )


def parse_requirements(raw: str) -> ProjectRequirements:
    t = raw or ""
    gf = derive_request_flags(t)

    room_type = _infer_room_type(t)
    discussion_mode = _explicit_discussion_request(t, gf)

    seat_count = _extract_seat_count(t, gf)
    camera_count = _extract_camera_count(t, gf, discussion_mode=discussion_mode)
    display_count = _extract_display_count(t, gf, discussion_mode=discussion_mode, room_type=room_type)

    explicit_projector = _contains_any(t, ["проектор", "projector", "короткофокус"])
    explicit_display = _explicit_display_request(t)
    explicit_camera = _explicit_camera_request(t)

    # Для discussion-запросов presentation/vks не включаем по умолчанию,
    # если пользователь явно не говорил про дисплей/камеры/видеосвязь.
    presentation = False
    if not discussion_mode:
        if room_type == "videowall":
            presentation = bool(explicit_display and _contains_any(t, ["презента"]))
        else:
            presentation = bool(
                explicit_display
                or _contains_any(t, ["презента", "вывод контента", "источник сигнала", "hdmi"])
            )

    flags = {
        "vks": _contains_any(t, ["вкс", "zoom", "teams", "видеосвяз", "conference call"])
        and not discussion_mode
        and room_type != "videowall",
        "byod": _contains_any(t, ["byod", "usb-c", "type-c", "подключение ноутбука", "ноутбук заказчика"]) and not discussion_mode,
        "presentation": presentation,
        "recording": _contains_any(t, ["запись", "recording", "архив"]),
        "streaming": _contains_any(t, ["трансляц", "stream", "стрим"]),
        "speech_reinforcement": _contains_any(t, ["озвучивание", "подзвучка", "speech reinforcement"]),
        # discussion-intent должен жёстко вести в discussion topology
        "control": _explicit_control_only(t) or discussion_mode,
    }

    exclusions = {
        "led": False,
        "projector": bool(explicit_display and not explicit_projector and not discussion_mode),
        "operator_room": _contains_any(t, ["операторская", "operator room"]),
    }

    confidence = {
        "room_type": 0.75 if room_type else 0.0,
        "seat_count": 0.95 if seat_count else 0.0,
        "camera_count": 0.95 if camera_count else 0.0,
        "display_count": 0.85 if display_count else 0.0,
        "vks": 0.95 if flags["vks"] else 0.0,
        "byod": 0.95 if flags["byod"] else 0.0,
        "discussion": 0.99 if discussion_mode else 0.0,
    }

    return ProjectRequirements(
        room_type=room_type,
        caps={
            "seat_count": seat_count,
            "room_count": None,
            "camera_count": camera_count,
            "display_count": display_count,
        },
        flags=flags,
        exclusions=exclusions,
        confidence=confidence,
    )


# aliases for compatibility
build_requirements = parse_requirements
extract_requirements = parse_requirements
derive_requirements = parse_requirements