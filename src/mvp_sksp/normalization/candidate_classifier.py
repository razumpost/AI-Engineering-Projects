from __future__ import annotations

import re
from functools import lru_cache
from typing import Iterable, Protocol

from ..knowledge.loader import load_knowledge_map
from ..planning.plan_models import ClassifiedCandidate


class CandidateLike(Protocol):
    candidate_id: str
    category: str | None
    sku: str | None
    manufacturer: str | None
    name: str
    description: str | None


_TOKEN_RE = re.compile(r"[a-zA-Zа-яА-Я0-9\-\+\./]+")

_INTERFACE_KEYWORDS: dict[str, list[str]] = {
    "hdmi": ["hdmi"],
    "usb": ["usb", "type-c", "usb-c"],
    "network": ["network", "ethernet", "cat", " ip ", "dante", "poe"],
    "dp": ["displayport", " dp", "dp "],
    "audio": ["audio", "xlr", "speaker", "acoustic", "акустик", "колон"],
    "poe": ["poe"],
    "rf": ["rf", "мгц", "mhz", "wireless"],
}


@lru_cache(maxsize=1)
def _km():
    return load_knowledge_map()


def _text(item: CandidateLike) -> str:
    return " ".join(
        [
            str(item.category or ""),
            str(item.sku or ""),
            str(item.manufacturer or ""),
            str(item.name or ""),
            str(item.description or ""),
        ]
    ).casefold()


def _tokenize(text: str) -> set[str]:
    return {m.group(0).casefold() for m in _TOKEN_RE.finditer(text)}


def _room_fit_for_family(family: str) -> list[str]:
    if family in {"videowall_panel", "videowall_mount", "videowall_controller"}:
        return ["videowall"]
    if family in {
        "led_cabinet",
        "led_processor",
        "sending_card",
        "receiving_card",
        "led_signal_accessories",
        "led_structure",
        "led_rigging",
        "led_floor_support",
        "led_spares",
        "led_service_toolkit",
        "transport_case",
    }:
        return ["led_screen", "auditorium"]
    if family in {"projection_screen", "projector", "line_array", "active_speaker", "podium_mic", "video_mixer", "operator_monitor"}:
        return ["auditorium"]
    return ["meeting_room", "auditorium"]


def classify_candidate(item: CandidateLike) -> ClassifiedCandidate:
    km = _km()
    text = _text(item)
    tokens = _tokenize(text)

    best_family: str | None = None
    best_score = 0.0
    best_notes: list[str] = []

    for family_key, family in km.families.items():
        score = 0.0
        notes: list[str] = []

        if item.category and item.category in family.categories:
            score += 3.0
            notes.append(f"category:{item.category}")

        for kw in family.keywords:
            kw_norm = kw.casefold().strip()
            if kw_norm and kw_norm in text:
                score += 2.0
                notes.append(f"kw:{kw_norm}")

        family_tokens = _tokenize(f"{family.key} {family.title}")
        overlap = len(tokens & family_tokens)
        if overlap:
            score += min(2.0, overlap * 0.5)
            notes.append(f"title_overlap:{overlap}")

        if family_key == "interactive_panel" and ("nextpanel" in text or "интерактив" in text):
            score += 4.0
        if family_key == "ptz_camera" and ("ptz" in text or "ndi" in text):
            score += 3.0
        if family_key == "projection_screen" and ("электропривод" in text or "matte white" in text or "fiberglass" in text):
            score += 4.0
        if family_key == "video_mixer" and ("rgblink" in text or "видеомикшер" in text):
            score += 4.0
        if family_key == "wireless_receiver" and ("приемник" in text or "receiver" in text):
            score += 3.0

        if score > best_score:
            best_score = score
            best_family = family_key
            best_notes = notes

    interfaces: list[str] = []
    for iface, kws in _INTERFACE_KEYWORDS.items():
        if any(kw in text for kw in kws):
            interfaces.append(iface)

    if best_family and best_family in km.families:
        family_def = km.families[best_family]
        capabilities = list(family_def.capabilities)
        room_fit = _room_fit_for_family(best_family)
    else:
        capabilities = []
        room_fit = []

    confidence = min(1.0, best_score / 8.0) if best_family else 0.0
    notes = list(best_notes)
    if not best_family:
        notes.append("unclassified")

    return ClassifiedCandidate(
        candidate_id=item.candidate_id,
        family=best_family,
        family_confidence=round(confidence, 3),
        capabilities=capabilities,
        interfaces=interfaces,
        room_fit=room_fit,
        notes=notes,
    )


def classify_candidates(items: Iterable[CandidateLike]) -> list[ClassifiedCandidate]:
    return [classify_candidate(item) for item in items]