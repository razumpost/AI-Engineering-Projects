from __future__ import annotations

import copy
import re
from typing import Any, Iterable


def _obj_get(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _norm_text(text: Any) -> str:
    s = str(text or "").strip().casefold()
    s = re.sub(r"\s+", " ", s)
    return s


def _candidate_blob(c: Any) -> str:
    manufacturer = _obj_get(c, "manufacturer", "") or _obj_get(c, "vendor", "")
    model = _obj_get(c, "model", "")
    sku = _obj_get(c, "sku", "")
    name = _obj_get(c, "name", "")
    description = _obj_get(c, "description", "")

    meta = _obj_get(c, "meta", {})
    if isinstance(meta, dict):
        manufacturer = manufacturer or meta.get("manufacturer") or meta.get("vendor") or ""
        model = model or meta.get("model") or ""
        sku = sku or meta.get("sku") or ""
        name = name or meta.get("name") or ""
        description = description or meta.get("description") or ""

        ev = meta.get("evidence_json", {})
        if isinstance(ev, dict):
            manufacturer = manufacturer or ev.get("vendor") or ""
            model = model or ev.get("model") or ""
            sku = sku or ev.get("sku") or ""
            name = name or ev.get("name") or ""
            description = description or ev.get("description") or ""

    return _norm_text(f"{manufacturer} {model} {sku} {name} {description}")


def _classify_text(text: str) -> str:
    if not text:
        return "other"

    if any(
        x in text
        for x in [
            "delegate unit",
            "пульт делегата",
            "chairman unit",
            "пульт председателя",
            "conference microphone unit",
        ]
    ):
        return "microphone"

    if any(
        x in text
        for x in [
            "central unit",
            "discussion system",
            "conference central unit",
            "центральный блок",
            "matrix",
            "switcher",
            "controller",
            "процессор",
            "audio dsp",
            "conference dsp",
            "аудиопроцессор",
            "dsp",
        ]
    ):
        return "controller"

    if any(
        x in text
        for x in [
            "ptz",
            "conference camera",
            "usb camera",
            "webcam",
            "camera",
            "камера",
        ]
    ):
        return "camera"

    if any(
        x in text
        for x in [
            "display",
            "дисплей",
            "панель",
            "экран",
            "monitor",
            "videowall",
            "interactive display",
            "interactive panel",
            "professional display",
        ]
    ):
        return "display"

    if any(
        x in text
        for x in [
            "microphone",
            "микрофон",
            "beamforming",
            "ceiling microphone",
            "table microphone",
        ]
    ):
        return "microphone"

    if any(
        x in text
        for x in [
            "speakerphone",
            "soundbar",
            "speaker",
            "акуст",
            "громкоговор",
            "amplifier",
            "усилитель",
        ]
    ):
        return "audio"

    if any(
        x in text
        for x in [
            "smart player",
            "spinetix",
            "signage",
            "cms",
            "license",
            "лиценз",
            "software",
            "elementi",
        ]
    ):
        return "software"

    if any(x in text for x in ["ops", "slot pc", "ops pc", "ops-пк"]):
        return "ops"

    if any(
        x in text
        for x in [
            "mount",
            "bracket",
            "wall mount",
            "ceiling mount",
            "стойка",
            "кронштейн",
            "тележка",
            "trolley",
        ]
    ):
        return "mount"

    if any(
        x in text
        for x in [
            "cable",
            "кабель",
            "hdmi",
            "usb",
            "displayport",
            "vga",
            "xlr",
            "cat6",
            "cat.6",
            "hdbaset",
        ]
    ):
        return "cable"

    return "other"


def _category_for_candidate(candidate: Any) -> str:
    return _classify_text(_candidate_blob(candidate))


def _with_classification(candidate: Any, category: str) -> Any:
    meta = _obj_get(candidate, "meta", {})
    if not isinstance(meta, dict):
        meta = {}

    new_meta = dict(meta)
    new_meta["classified_category"] = category

    # Pydantic v2
    if hasattr(candidate, "model_copy"):
        try:
            return candidate.model_copy(update={"meta": new_meta})
        except Exception:
            pass

    # dict-like
    if isinstance(candidate, dict):
        new_candidate = dict(candidate)
        new_candidate["meta"] = new_meta
        return new_candidate

    # generic object
    try:
        new_candidate = copy.copy(candidate)
    except Exception:
        new_candidate = candidate

    try:
        setattr(new_candidate, "meta", new_meta)
    except Exception:
        pass

    return new_candidate


def classify_candidate(candidate: Any) -> str:
    return _category_for_candidate(candidate)


def classify_candidates(candidates: Iterable[Any]) -> list[Any]:
    out: list[Any] = []
    for candidate in candidates:
        category = _category_for_candidate(candidate)
        out.append(_with_classification(candidate, category))
    return out