from __future__ import annotations

import re
from typing import List


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).casefold()


def _extract_display_inches(text: str) -> str | None:
    m = re.search(r"\b(\d{2,3})\s*(?:\"|”|in|inch|дюйм)", text, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"\bdisplay\s*(\d{2,3})\b", text, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"\bдиспле[йя]\s*(\d{2,3})\b", text, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    return None


def build_role_price_queries(text: str) -> List[str]:
    q = _norm(text)
    out: list[str] = []

    def add(*phrases: str) -> None:
        for p in phrases:
            p = p.strip()
            if p and p not in out:
                out.append(p)

    software_first = any(
        x in q for x in ["smart player", "spinetix", "signage", "cms", "license", "лиценз", "software", "по "]
    )

    if software_first:
        add(
            "smart player",
            "spinetix player",
            "signage software",
            "player license",
            "cms signage",
        )

    if any(x in q for x in ["переговор", "conference", "meeting room", "переговорная"]):
        add(
            "conference system",
            "usb conference system",
            "meeting room system",
        )

    if any(x in q for x in ["камера", "camera", "ptz"]):
        add(
            "ptz camera",
            "conference camera",
            "usb camera",
        )

    cam_count_m = re.search(r"\b(одна|один|две|два|три|3|2|1)\s+(?:камеры|камера|camera|cameras)\b", q)
    if cam_count_m:
        add("ptz camera", "conference camera")

    # ВАЖНО:
    # если запрос software-first, не подмешиваем display-хинты автоматически
    if not software_first and any(x in q for x in ["дисплей", "display", "панель", "panel", "экран", "screen"]):
        size = _extract_display_inches(text)
        if size:
            add(
                f"display {size}",
                f"{size} professional display",
                f"{size} interactive display",
            )
        else:
            add(
                "professional display",
                "conference display",
                "interactive display",
            )

    if any(x in q for x in ["микрофон", "microphone", "mic", "микрофоны"]):
        add(
            "conference microphone",
            "ceiling microphone",
            "table microphone",
            "speakerphone",
        )

    if any(x in q for x in ["акуст", "speaker", "колонк", "audio"]):
        add(
            "conference speaker",
            "soundbar conferencing",
            "audio conferencing",
        )

    if not software_first and any(x in q for x in ["мест", "seats", "seat"]):
        add(
            "conference microphone",
            "conference camera",
            "professional display",
        )

    if not out:
        add(text.strip())

    return out[:15]