# src/mvp_sksp/adapters/price_classifier.py
from __future__ import annotations


def classify_price_item(name: str, description: str | None = None) -> str:
    text = ((name or "") + " " + (description or "")).lower()

    # DISPLAY / PANEL — ставим раньше camera, чтобы "smart display ... camera" не уезжал в camera
    if any(x in text for x in [
        "display", "дисплей", "панель", "экран", "monitor",
        "smart display", "interactive display", "interactive panel",
        "lcd", "led", "videowall",
        "nextouch", "nextpanel", "eliteboard", "edflat", "mobiscreen",
        "professional display", "signage display",
    ]):
        return "display"

    # CAMERA
    if any(x in text for x in [
        "camera", "камера", "ptz", "hd20", "zoom",
        "videocam", "webcam", "conference camera", "usb camera",
        "videobar camera",
    ]):
        return "camera"

    # MICROPHONE
    if any(x in text for x in [
        "microphone", "микрофон", "mic",
        "shure", "mxa", "beamforming",
        "delegate unit", "chairman unit",
        "пульт делегата", "пульт председателя",
        "conference microphone", "ceiling microphone", "table microphone",
    ]):
        return "microphone"

    # AUDIO
    if any(x in text for x in [
        "speaker", "акуст", "soundbar",
        "audio", "усилитель", "amplifier",
        "громкоговор", "conference speaker", "speakerphone",
    ]):
        return "audio"

    # CONTROLLER / DSP / CORE
    if any(x in text for x in [
        "controller", "процессор", "dsp",
        "videomix", "atem", "matrix", "switcher",
        "central unit", "центральный блок",
        "discussion system", "conference system core",
    ]):
        return "controller"

    # OPS / SLOT PC
    if any(x in text for x in [
        "ops", "slot pc", "ops-pc", "ops pc",
        "встраиваемый компьютер", "ops модуль",
    ]):
        return "ops"

    # MOUNT
    if any(x in text for x in [
        "mount", "bracket", "стойка", "кронштейн", "trolley", "тележка",
        "wall mount", "ceiling mount", "mobile stand", "мобильная стойка",
    ]):
        return "mount"

    # CABLE
    if any(x in text for x in [
        "кабель", "cable", "hdmi", "usb", "displayport", "vga",
        "xlr", "cat6", "cat.6", "витая пара", "hdbaset",
    ]):
        return "cable"

    # SOFTWARE / LICENSE
    if any(x in text for x in [
        "license", "лиценз", "software", "smart player",
        "player license", "spinetix", "elementi", "cms",
        "content management", "signage software", "по ",
    ]):
        return "software"

    return "other"