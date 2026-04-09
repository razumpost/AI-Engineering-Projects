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

        out.append(
            ClassifiedCandidate(
                candidate_id=candidate_id,
                family=family,
                family_confidence=conf,
                capabilities=[],
                interfaces=[],
                room_fit=[],
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
    for t in tokens:
        if t and t in text:
            return True
    return False


def _infer_family(text: str) -> tuple[str | None, float, list[str]]:
    notes: list[str] = []

    if not text:
        return None, 0.0, ["unclassified"]

    # ------------------------------------------------------------
    # 1) Сначала жестко отсеиваем кабели / displayport, чтобы не путать с DSP
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
    # 2) Дискуссионные системы / пульты
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

    # vendor+context weak/strong rules for conference discussion systems
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
        return "discussion_dsp", 0.94, ["rule_match:discussion_dsp"]

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
    # 3) Микрофоны / аудио
    # ------------------------------------------------------------
    if _contains_any(text, ["ceiling microphone", "beamforming", "ceiling mic", "потолочный микрофон"]):
        return "ceiling_mic_array", 0.9, ["rule_match:ceiling_mic_array"]

    if _contains_any(text, ["tabletop mic", "gooseneck", "гусиная шея", "настольный микрофон"]):
        return "tabletop_mic", 0.86, ["rule_match:tabletop_mic"]

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
    # 4) Камеры
    # ------------------------------------------------------------
    if _contains_any(text, ["videobar", "video bar"]):
        return "videobar", 0.92, ["rule_match:videobar"]

    if _contains_any(text, ["ptz", "ptz camera", "conference camera", "камера поворотная"]):
        return "ptz_camera", 0.9, ["rule_match:ptz_camera"]

    if _contains_any(text, ["usb camera", "webcam", "fixed conference camera"]):
        return "fixed_conference_camera", 0.82, ["rule_match:fixed_conference_camera"]

    # ------------------------------------------------------------
    # 5) Дисплеи / экраны / проекторы
    # ------------------------------------------------------------
    if _contains_any(text, ["interactive panel", "interactive display", "интерактивная панель"]):
        return "interactive_panel", 0.9, ["rule_match:interactive_panel"]

    if _contains_any(text, ["projector", "проектор"]):
        return "projector", 0.88, ["rule_match:projector"]

    if _contains_any(text, ["projection screen", "проекционный экран"]):
        return "projection_screen", 0.88, ["rule_match:projection_screen"]

    if _contains_any(text, ["videowall", "видеостена"]):
        if _contains_any(text, ["mount", "кронштейн", "рам", "каркас"]):
            return "videowall_mount", 0.92, ["rule_match:videowall_mount"]
        if _contains_any(text, ["controller", "processor", "videoprocessor", "контроллер", "процессор"]):
            return "videowall_controller", 0.92, ["rule_match:videowall_controller"]
        return "videowall_panel", 0.9, ["rule_match:videowall_panel"]

    if _contains_any(text, ["led cabinet", "светодиодный экран", "led экран", "led cabinet", "novastar"]):
        return "led_cabinet", 0.9, ["rule_match:led_cabinet"]

    if _contains_any(text, ["display", "panel", "lcd", "led tv", "professional display", "дисплей", "панель"]):
        return "display_panel", 0.82, ["rule_match:display_panel"]

    # ------------------------------------------------------------
    # 6) Коммутация / BYOD / управление
    # ------------------------------------------------------------
    if _contains_any(text, ["wireless presentation", "instashow", "barco clickshare", "беспроводная презентация"]):
        return "byod_wireless_presentation", 0.88, ["rule_match:byod_wireless_presentation"]

    if _contains_any(text, ["usb hdmi gateway", "byod gateway", "usb bridge", "usb-dsp bridge"]):
        return "byod_usb_hdmi_gateway", 0.86, ["rule_match:byod_usb_hdmi_gateway"]

    if _contains_any(text, ["usb-c dock", "type-c dock", "док-станция usb-c"]):
        return "usb_c_dock", 0.86, ["rule_match:usb_c_dock"]

    if _contains_any(text, ["matrix switcher", "матричный коммутатор"]):
        return "matrix_switcher", 0.86, ["rule_match:matrix_switcher"]

    if _contains_any(text, ["presentation switcher", "презентационный коммутатор"]):
        return "presentation_switcher", 0.86, ["rule_match:presentation_switcher"]

    if _contains_any(text, ["managed switch", "коммутатор управляемый"]):
        return "managed_switch", 0.84, ["rule_match:managed_switch"]

    if _contains_any(text, ["poe switch", "poe коммутатор"]):
        return "poe_switch", 0.84, ["rule_match:poe_switch"]

    if _contains_any(text, ["touch panel", "сенсорная панель управления"]):
        return "touch_panel", 0.84, ["rule_match:touch_panel"]

    if _contains_any(text, ["control processor", "процессор управления"]):
        return "control_processor", 0.84, ["rule_match:control_processor"]

    # ------------------------------------------------------------
    # 7) Монтаж / питание / аксессуары
    # ------------------------------------------------------------
    if _contains_any(text, ["mount", "bracket", "кронштейн", "стойка", "крепление", "каркас"]):
        return "mounting_kit", 0.8, ["rule_match:mounting_kit"]

    if _contains_any(text, ["power supply", "адаптер питания", "блок питания"]) and "conference" not in text and "discussion" not in text:
        return "power_accessories", 0.72, ["rule_match:power_accessories"]

    if _contains_any(text, ["adapter", "коннектор", "разъем", "accessory", "аксессуар"]):
        return "adapters_kit", 0.6, ["rule_match:adapters_kit"]

    return None, 0.0, ["unclassified"]


__all__ = ["classify_candidate", "classify_candidates"]