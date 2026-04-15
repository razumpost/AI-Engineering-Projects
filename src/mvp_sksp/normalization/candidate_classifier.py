from __future__ import annotations

import re
from typing import Any, Iterable, List

from ..planning.plan_models import ClassifiedCandidate


def classify_candidate(candidate: Any) -> ClassifiedCandidate:
    return classify_candidates([candidate])[0]


def classify_candidates(candidates: Iterable[Any]) -> List[ClassifiedCandidate]:
    out: List[ClassifiedCandidate] = []

    for c in candidates:
        manufacturer = _first_str(
            getattr(c, "manufacturer", None),
            getattr(c, "vendor", None),
            getattr(c, "brand", None),
        )
        model = _first_str(getattr(c, "model", None))
        sku = _first_str(
            getattr(c, "sku", None),
            getattr(c, "article", None),
            getattr(c, "partnumber", None),
        )
        name = _first_str(getattr(c, "name", None), getattr(c, "title", None))
        desc = _first_str(getattr(c, "description", None), getattr(c, "desc", None))

        text = _norm_text(f"{manufacturer} {model} {sku} {name} {desc}")
        family, conf, notes = _infer_family(text)

        candidate_id = _first_str(
            getattr(c, "candidate_id", None),
            getattr(c, "id", None),
        ) or "line_like"

        room_fit = _infer_room_fit(family, text)

        out.append(
            ClassifiedCandidate(
                candidate_id=candidate_id,
                family=family,
                family_confidence=conf,
                capabilities=[],
                interfaces=[],
                room_fit=room_fit,
                notes=notes,
            )
        )

    return out


def _first_str(*vals: Any) -> str:
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _norm_text(s: str) -> str:
    s = (s or "").lower().replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _contains_any(text: str, tokens: list[str]) -> bool:
    return any(t and t in text for t in tokens)


def _infer_room_fit(family: str | None, text: str) -> list[str]:
    if family in {
        "delegate_unit",
        "chairman_unit",
        "discussion_central_unit",
        "discussion_dsp",
        "power_supply_discussion",
    }:
        return ["meeting_room", "hall"]

    if family in {
        "display_panel",
        "interactive_panel",
        "ptz_camera",
        "fixed_conference_camera",
        "videobar",
        "tabletop_mic",
        "ceiling_mic_array",
        "speakerphone",
        "soundbar",
        "wall_speaker",
        "ceiling_speaker",
        "presentation_switcher",
        "matrix_switcher",
        "simple_io_hub",
        "cabling_av",
        "mounting_kit",
        "byod_usb_hdmi_gateway",
        "byod_wireless_presentation",
        "usb_c_dock",
        "dsp",
        "usb_dsp_bridge",
    }:
        return ["meeting_room", "hall"]

    if family in {"videowall_panel", "videowall_mount", "videowall_controller"}:
        return ["videowall", "meeting_room"]

    if family in {"led_cabinet"}:
        return ["led_screen"]

    if "переговор" in text or "conference room" in text or "meeting room" in text:
        return ["meeting_room"]
    if "видеостен" in text or "videowall" in text:
        return ["videowall"]
    if "актовый зал" in text or "конференц-зал" in text or "auditorium" in text:
        return ["hall"]

    return []


def _infer_family(text: str) -> tuple[str | None, float, list[str]]:
    notes: list[str] = []

    if not text:
        return None, 0.0, ["unclassified"]

    # ------------------------------------------------------------
    # 0) OPS / players / software ДО cable/display
    # ------------------------------------------------------------
    ops_tokens = [
        "ops",
        "slot pc",
        "ops pc",
        "ops-pc",
        "ops модуль",
        "slot-in pc",
        "embedded pc",
        "intel i3",
        "intel i5",
        "intel i7",
        "celeron",
        "ryzen",
        "mini pc",
        "nmp-",
        "nmp ",
        "hmp",
        "player box",
        "media player",
    ]
    if _contains_any(text, ops_tokens):
        return "simple_io_hub", 0.86, ["rule_match:ops_or_compute"]

    software_tokens = [
        "license",
        "лиценз",
        "software",
        "cms",
        "spinetix",
        "elementi",
        "digital signage",
        "signage software",
        "html5 widgets",
        "smil",
    ]
    if _contains_any(text, software_tokens):
        return "smart_player", 0.84, ["rule_match:software_or_signage"]

    # ------------------------------------------------------------
    # 1) Mount / trolley ДО display
    # ------------------------------------------------------------
    mount_tokens = [
        "mount kit",
        "ceiling mount kit",
        "wall mount",
        "ceiling mount",
        "pull-out wall mount",
        "back-to-back",
        "trolley",
        "тележка",
        "стойка",
        "кронштейн",
        "bracket",
        "mount",
        "rotate",
        "fixed",
        "micro adjustable",
    ]
    if _contains_any(text, mount_tokens):
        if _contains_any(text, ["videowall", "видеостена"]):
            return "videowall_mount", 0.94, ["rule_match:videowall_mount"]
        return "mounting_kit", 0.9, ["rule_match:mounting_kit"]

    # ------------------------------------------------------------
    # 2) Кабели после ops/mount, чтобы не путать с DSP / OPS
    # ------------------------------------------------------------
    cable_tokens = [
        "displayport",
        "display port",
        "hdmi cable",
        "кабель",
        "patch cord",
        "патч-корд",
        "cat5",
        "cat6",
        "utp",
        "sftp",
        "ftp ",
        "xlr",
        "trs",
        "jack 3.5",
        "usb a-b",
        "usb-a",
        "usb-b",
        "usb-c cable",
        "type-c cable",
    ]
    if _contains_any(text, cable_tokens):
        return "cabling_av", 0.95, ["rule_match:cabling_av"]

    # ------------------------------------------------------------
    # 3) Дискуссионные системы / пульты
    # ------------------------------------------------------------
    chairman_tokens = [
        "chairman unit",
        "chairman microphone",
        "пульт председателя",
        "председательский пульт",
        "председательск",
    ]
    if _contains_any(text, chairman_tokens):
        return "chairman_unit", 0.96, ["rule_match:chairman_unit"]

    delegate_tokens = [
        "delegate unit",
        "delegate microphone",
        "пульт делегата",
        "делегатский пульт",
        "настольный пульт",
        "микрофонный пульт",
        "пульт dis",
        "dis настольный",
        "настольный dis",
    ]
    if _contains_any(text, delegate_tokens):
        return "delegate_unit", 0.94, ["rule_match:delegate_unit"]

    conference_brands = [
        "bosch",
        "dis",
        "taiden",
        "relacart",
        "televic",
        "bxb",
        "itc",
        "gonsin",
    ]

    if _contains_any(text, conference_brands) and _contains_any(
        text,
        [
            "chairman",
            "delegate",
            "настольн",
            "пульт",
            "микрофонный пульт",
            "conference unit",
            "discussion unit",
        ],
    ):
        if "chairman" in text or "председ" in text:
            return "chairman_unit", 0.88, ["weak_match:conference_brand+chairman"]
        return "delegate_unit", 0.86, ["weak_match:conference_brand+delegate"]

    central_tokens = [
        "central unit",
        "main unit",
        "control unit",
        "discussion system host",
        "conference central unit",
        "блок управления",
        "центральный блок",
        "центральный контроллер",
        "центральный блок конференц",
        "центральный блок дискуссион",
    ]
    if _contains_any(text, central_tokens):
        return "discussion_central_unit", 0.96, ["rule_match:discussion_central_unit"]

    if _contains_any(text, conference_brands) and _contains_any(
        text,
        [
            "controller",
            "central",
            "main unit",
            "host",
            "control unit",
            "блок управления",
        ],
    ):
        return "discussion_central_unit", 0.88, ["weak_match:conference_brand+central"]

    dsp_tokens = [
        "audio dsp",
        "conference dsp",
        "audio processor",
        "digital signal processor",
        "аудиопроцессор",
        "процессор обработки аудио",
        "dsp processor",
        "dsp unit",
    ]
    if _contains_any(text, dsp_tokens):
        if _contains_any(text, conference_brands) or _contains_any(text, ["discussion", "conference", "конференц"]):
            return "discussion_dsp", 0.94, ["rule_match:discussion_dsp"]
        return "dsp", 0.9, ["rule_match:dsp"]

    power_tokens = [
        "power supply",
        "psu",
        "extension unit",
        "expander",
        "extension box",
        "блок питания",
        "блок расширения",
        "блок расширения конференц",
    ]
    if _contains_any(text, power_tokens) and (
        _contains_any(text, conference_brands)
        or _contains_any(text, ["conference", "discussion", "конференц", "дискуссион"])
    ):
        return "power_supply_discussion", 0.9, ["rule_match:power_supply_discussion"]

    # ------------------------------------------------------------
    # 4) Микрофоны / аудио
    # ------------------------------------------------------------
    if _contains_any(text, ["ceiling microphone", "beamforming", "ceiling mic", "потолочный микрофон"]):
        return "ceiling_mic_array", 0.9, ["rule_match:ceiling_mic_array"]

    if _contains_any(text, ["tabletop mic", "gooseneck", "гусиная шея", "настольный микрофон"]):
        return "tabletop_mic", 0.86, ["rule_match:tabletop_mic"]

    if _contains_any(text, ["clockaudio", "микрофон", "microphone", "conference mic", "conference microphone"]):
        return "tabletop_mic", 0.74, ["weak_match:generic_microphone"]

    if _contains_any(text, ["speakerphone", "спикерфон"]):
        return "speakerphone", 0.92, ["rule_match:speakerphone"]

    if _contains_any(text, ["soundbar", "саундбар"]):
        return "soundbar", 0.9, ["rule_match:soundbar"]

    if _contains_any(text, ["wall speaker", "настенная акустика", "акустическая система настенная"]):
        return "wall_speaker", 0.85, ["rule_match:wall_speaker"]

    if _contains_any(text, ["ceiling speaker", "потолочная акустика"]):
        return "ceiling_speaker", 0.85, ["rule_match:ceiling_speaker"]

    if _contains_any(text, ["amplifier", "усилитель мощности", "power amplifier"]):
        return "amplifier", 0.88, ["rule_match:amplifier"]

    # ------------------------------------------------------------
    # 5) Камеры
    # ------------------------------------------------------------
    if _contains_any(text, ["videobar", "video bar"]):
        return "videobar", 0.92, ["rule_match:videobar"]

    if _contains_any(text, ["ptz", "ptz camera", "conference camera", "камера поворотная"]):
        return "ptz_camera", 0.9, ["rule_match:ptz_camera"]

    if _contains_any(text, ["usb camera", "webcam", "fixed conference camera"]):
        return "fixed_conference_camera", 0.82, ["rule_match:fixed_conference_camera"]

    # ------------------------------------------------------------
    # 6) Дисплеи / экраны / проекторы
    # ------------------------------------------------------------
    if _contains_any(text, ["interactive panel", "interactive display", "интерактивная панель"]):
        return "interactive_panel", 0.9, ["rule_match:interactive_panel"]

    if _contains_any(text, ["projector", "проектор"]):
        return "projector", 0.88, ["rule_match:projector"]

    if _contains_any(text, ["projection screen", "проекционный экран"]):
        return "projection_screen", 0.88, ["rule_match:projection_screen"]

    if _contains_any(text, ["videowall", "видеостена"]):
        if _contains_any(text, ["controller", "processor", "videoprocessor", "контроллер", "процессор"]):
            return "videowall_controller", 0.92, ["rule_match:videowall_controller"]
        return "videowall_panel", 0.9, ["rule_match:videowall_panel"]

    if _contains_any(text, ["led cabinet", "светодиодный экран", "led экран", "novastar"]):
        return "led_cabinet", 0.9, ["rule_match:led_cabinet"]

    if _contains_any(text, ["display", "panel", "lcd", "led tv", "professional display", "дисплей", "панель", "smart display"]):
        return "display_panel", 0.82, ["rule_match:display_panel"]

    # ------------------------------------------------------------
    # 7) Коммутация / BYOD / управление
    # ------------------------------------------------------------
    if _contains_any(text, ["wireless presentation", "instashow", "barco clickshare", "беспроводная презентация"]):
        return "byod_wireless_presentation", 0.88, ["rule_match:byod_wireless_presentation"]

    if _contains_any(text, ["usb hdmi gateway", "byod gateway", "usb bridge", "usb-dsp bridge"]):
        return "byod_usb_hdmi_gateway", 0.86, ["rule_match:byod_usb_hdmi_gateway"]

    if _contains_any(text, ["usb-c dock", "type-c dock", "usb-c hub", "dock station"]):
        return "usb_c_dock", 0.84, ["rule_match:usb_c_dock"]

    if _contains_any(text, ["matrix switcher", "matrix", "матричный коммутатор"]):
        return "matrix_switcher", 0.86, ["rule_match:matrix_switcher"]

    if _contains_any(text, ["presentation switcher", "switcher", "коммутатор презентационный"]):
        return "presentation_switcher", 0.84, ["rule_match:presentation_switcher"]

    if _contains_any(text, ["control processor", "процессор управления"]):
        return "control_processor", 0.9, ["rule_match:control_processor"]

    if _contains_any(text, ["touch panel", "сенсорная панель", "панель управления"]):
        return "touch_panel", 0.88, ["rule_match:touch_panel"]

    if _contains_any(text, ["keypad", "кнопочная панель"]):
        return "keypad_controller", 0.82, ["rule_match:keypad_controller"]

    if _contains_any(text, ["switch", "poe switch", "коммутатор poe"]):
        return "poe_switch", 0.8, ["rule_match:poe_switch"]

    return None, 0.0, ["unclassified"]