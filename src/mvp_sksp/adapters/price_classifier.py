from __future__ import annotations

import re


def _norm(text: str) -> str:
    text = (text or "").lower().replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def classify_price_item(name: str, description: str | None = None) -> str:
    text = _norm(f"{name or ''} {description or ''}")

    if not text:
        return "other"

    # Очевидные сервисные/калибровочные аксессуары не считаем core-устройствами.
    if any(x in text for x in ["calibration kit", "калибров", "service toolkit", "toolkit", "монтажный комплект", "комплект юстировки"]):
        return "other"

    # ------------------------------------------------------------
    # 1) DISCUSSION / CONFERENCE SYSTEM
    # ------------------------------------------------------------
    if any(x in text for x in [
        "пульт председателя",
        "chairman unit",
        "chairman microphone",
        "председательский пульт",
    ]):
        return "microphone"

    if any(x in text for x in [
        "пульт делегата",
        "delegate unit",
        "delegate microphone",
        "настольный пульт",
        "микрофонный пульт",
        "пульт dis",
        "настольный dis",
    ]):
        return "microphone"

    if any(x in text for x in [
        "central unit",
        "discussion central unit",
        "conference central unit",
        "control unit",
        "main unit",
        "блок управления",
        "центральный блок",
        "центральный контроллер",
        "discussion system",
        "conference system host",
        "ps 6000",
        "power supply dis",
        "блок питания dis",
        "delegate system power supply",
        "discussion power supply",
        "extension unit",
        "conference controller",
    ]):
        return "controller"

    if any(x in text for x in [
        "audio dsp",
        "conference dsp",
        "audio processor",
        "digital signal processor",
        "аудиопроцессор",
        "процессор обработки аудио",
        "dsp processor",
        "dsp unit",
    ]):
        return "controller"

    if any(x in text for x in [
        "videowall controller",
        "video wall controller",
        "videowall processor",
        "video wall processor",
        "контроллер видеостен",
        "контроллер видеостены",
        "процессор видеостен",
        "процессор видеостены",
        "matrix switcher",
        "hdmi matrix",
        "matrix controller",
        "матричный коммутатор",
        "коммутатор матричный",
        "seamless switcher",
        "multiviewer",
    ]):
        return "controller"

    if any(x in text for x in [
        "bosch",
        "dis",
        "taiden",
        "relacart",
        "televic",
        "bxb",
        "itc conference",
        "gonsin",
    ]) and any(x in text for x in [
        "delegate",
        "chairman",
        "conference",
        "discussion",
        "пульт",
        "микрофонный",
        "central unit",
        "control unit",
    ]):
        if any(x in text for x in ["delegate", "chairman", "пульт", "микрофонный"]):
            return "microphone"
        return "controller"

    # ------------------------------------------------------------
    # 2) AUDIO
    # ------------------------------------------------------------
    if any(x in text for x in [
        "speakerphone",
        "спикерфон",
        "soundbar",
        "саундбар",
        "speaker bar",
        "conference speaker",
        "speaker",
        "акуст",
        "громкоговор",
        "усилитель",
        "amplifier",
        "power amplifier",
    ]):
        return "audio"

    if any(x in text for x in [
        "microphone",
        "микрофон",
        "beamforming",
        "ceiling microphone",
        "table microphone",
        "gooseneck",
        "гусиная шея",
        "настольный микрофон",
        "потолочный микрофон",
        "clockaudio",
        "shure",
        "mxa",
    ]):
        return "microphone"

    # ------------------------------------------------------------
    # 3) DISPLAY / PROJECTOR FIRST
    # ВАЖНО: smart display с integrated camera должен остаться display
    # ------------------------------------------------------------
    if any(x in text for x in [
        "projector",
        "проектор",
        "laser phosphor",
        "throw ratio",
        "ansi lumens",
        "keystone",
        "ust ",
        "ust(",
    ]):
        return "display"

    if any(x in text for x in [
        "smart display",
        "interactive panel",
        "interactive display",
        "интерактивная панель",
        "display",
        "дисплей",
        "панель",
        "экран",
        "monitor",
        "professional display",
        "signage display",
        "lcd",
        "videowall",
        "nextouch",
        "nextpanel",
        "eliteboard",
        "edflat",
        "mobiscreen",
        "samsung oh",
        "oh75f",
        "oh85f",
        "all in one smart display",
    ]):
        return "display"

    # ------------------------------------------------------------
    # 4) CAMERA
    # ------------------------------------------------------------
    if any(x in text for x in [
        "ptz",
        "conference camera",
        "usb camera",
        "webcam",
        "videobar camera",
        "zoom camera",
        "камера",
        "camera",
    ]):
        return "camera"

    # ------------------------------------------------------------
    # 5) OPS / PLAYERS / COMPUTE
    # ------------------------------------------------------------
    if any(x in text for x in [
        "media player",
        "player",
        "ops",
        "slot pc",
        "ops-pc",
        "ops pc",
        "встраиваемый компьютер",
        "ops модуль",
        "intel i5",
        "win 10 pro",
        "win 8 pro",
        "nmp-",
        "nmp ",
        "hmp",
        "diva",
    ]):
        return "ops"

    # ------------------------------------------------------------
    # 6) SOFTWARE / SIGNAGE CMS
    # ------------------------------------------------------------
    if any(x in text for x in [
        "license",
        "лиценз",
        "software",
        "smart player",
        "player license",
        "spinetix",
        "elementi",
        "cms",
        "content management",
        "signage software",
        "digital signage",
        "html5 widgets",
        "w3c widgets",
        "smil",
    ]):
        return "software"

    # ------------------------------------------------------------
    # 7) MOUNTS
    # ------------------------------------------------------------
    if any(x in text for x in [
        "mount",
        "bracket",
        "стойка",
        "кронштейн",
        "trolley",
        "тележка",
        "wall mount",
        "ceiling mount",
        "mobile stand",
        "мобильная стойка",
        "back-to-back",
        "pull-out wall mount",
        "ceiling mount kit",
    ]):
        return "mount"

    # ------------------------------------------------------------
    # 8) CABLES LAST
    # ------------------------------------------------------------
    if any(x in text for x in [
        "кабель",
        "cable",
        "displayport",
        "dvi-d",
        "hdmi cable",
        "hdmi-hdmi",
        "usb cable",
        "vga cable",
        "xlr",
        "cat6",
        "cat.6",
        "витая пара",
        "patch cord",
        "патч-корд",
        "hdbaset cable",
        "ethernet cable",
    ]):
        return "cable"

    return "other"