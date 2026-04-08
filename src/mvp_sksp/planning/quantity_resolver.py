from __future__ import annotations

from typing import Any, Dict, List


# =========================
# Helpers (safe access)
# =========================

def _obj_get(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _meta_get(obj: Any, key: str, default: Any = None) -> Any:
    meta = _obj_get(obj, "meta", None)
    if isinstance(meta, dict):
        return meta.get(key, default)
    return default


def _norm(s: Any) -> str:
    return str(s or "").strip().casefold()


# =========================
# Family resolution (CRITICAL FIX)
# =========================

def _guess_family(li: Any) -> str:
    # 1) прямые поля
    for key in ("family", "equipment_family", "graph_family", "category"):
        v = _obj_get(li, key, None)
        if v:
            return _norm(v)

    # 2) meta
    for key in ("family", "equipment_family", "graph_family", "category"):
        v = _meta_get(li, key, None)
        if v:
            return _norm(v)

    # 3) role → family
    role = _norm(_obj_get(li, "role", None) or _meta_get(li, "role", None))

    role_map = {
        "delegate_unit": "delegate_unit",
        "chairman_unit": "chairman_unit",
        "discussion_central_unit": "discussion_central_unit",
        "discussion_dsp": "discussion_dsp",
        "power_supply_discussion": "power_supply_discussion",
        "ptz_camera": "ptz_camera",
        "display": "display",
        "microphone": "microphone",
        "speakerphone": "speakerphone",
        "audio_processor": "audio_processor",
        "cabling_av": "cabling_av",
    }

    if role in role_map:
        return role_map[role]

    # 4) fallback по тексту
    blob = " ".join(
        [
            _norm(_obj_get(li, "name", None)),
            _norm(_obj_get(li, "description", None)),
            _norm(_meta_get(li, "name", None)),
            _norm(_meta_get(li, "description", None)),
        ]
    )

    if any(x in blob for x in ["пульт делегата", "delegate unit"]):
        return "delegate_unit"
    if any(x in blob for x in ["пульт председателя", "chairman unit"]):
        return "chairman_unit"
    if any(x in blob for x in ["central unit", "центральный блок", "discussion system"]):
        return "discussion_central_unit"
    if any(x in blob for x in ["audio dsp", "conference dsp", "аудиопроцессор", "dsp"]):
        return "discussion_dsp"
    if any(x in blob for x in ["power supply", "блок питания", "extender"]):
        return "power_supply_discussion"
    if any(x in blob for x in ["ptz", "conference camera", "usb camera", "камера"]):
        return "ptz_camera"
    if any(x in blob for x in ["display", "дисплей", "панель", "экран"]):
        return "display"
    if any(x in blob for x in ["microphone", "микрофон", "beamforming"]):
        return "microphone"
    if any(x in blob for x in ["speakerphone", "soundbar", "акуст", "speaker"]):
        return "speakerphone"
    if any(x in blob for x in ["cable", "кабель", "hdmi", "usb", "xlr", "cat6"]):
        return "cabling_av"

    return ""


# =========================
# Quantity helpers
# =========================

def _qty_of(li: Any) -> int:
    v = _obj_get(li, "qty", None)
    if v is None:
        v = _meta_get(li, "qty", None)
    try:
        return int(v or 0)
    except Exception:
        return 0


def _count(plan: List[Any], family: str) -> int:
    family = _norm(family)
    total = 0
    for li in plan:
        if _guess_family(li) == family:
            total += _qty_of(li)
    return total


# =========================
# Main resolver
# =========================

def resolve_quantities(plan: List[Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Универсальный расчет количеств с учетом:
    - graph family
    - fallback эвристик
    - meta (например camera_count, seats)
    """

    result: Dict[str, Any] = {}

    # Камеры
    camera_count = int(meta.get("camera_count") or _count(plan, "ptz_camera") or 0)
    if camera_count > 0:
        result["ptz_camera"] = camera_count

    # Дисплеи
    display_count = _count(plan, "display")
    if display_count > 0:
        result["display"] = display_count

    # Микрофоны
    mic_count = _count(plan, "microphone")
    if mic_count > 0:
        result["microphone"] = mic_count

    # Делегатская система
    delegate_count = int(meta.get("seats") or _count(plan, "delegate_unit") or 0)
    if delegate_count > 0:
        result["delegate_unit"] = delegate_count

    chairman_count = _count(plan, "chairman_unit") or (1 if delegate_count > 0 else 0)
    if chairman_count > 0:
        result["chairman_unit"] = chairman_count

    central_unit = _count(plan, "discussion_central_unit") or (
        1 if delegate_count > 0 else 0
    )
    if central_unit > 0:
        result["discussion_central_unit"] = central_unit

    # DSP
    dsp_count = _count(plan, "discussion_dsp")
    if dsp_count > 0:
        result["discussion_dsp"] = dsp_count

    # Питание
    psu_count = _count(plan, "power_supply_discussion")
    if psu_count > 0:
        result["power_supply_discussion"] = psu_count

    # Кабели (обычно не считаем жестко, но оставим)
    cable_count = _count(plan, "cabling_av")
    if cable_count > 0:
        result["cabling_av"] = cable_count

    return result