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
    if family.startswith("videowall_") or family == "videowall_controller":
        return ["videowall"]
    if family.startswith("led_") or family == "led_cabinet":
        return ["led_screen", "auditorium"]
    if family == "speaker_100v":
        return ["auditorium"]
    return ["meeting_room", "auditorium"]


def _is_videowall_like(text: str) -> bool:
    return ("видеостен" in text) or ("videowall" in text) or (
        "контроллер" in text and ("1x4" in text or "2x2" in text or "3x3" in text or "4x4" in text)
    )


def _is_100v_speaker(text: str) -> bool:
    return (("100v" in text or "100 v" in text or "70v" in text or "70 v" in text) and ("акуст" in text or "speaker" in text or "колон" in text))


def _is_led_like(text: str) -> bool:
    return ("светодиод" in text) or ("шаг пикс" in text) or ("pixel" in text) or ("яркость" in text)


def _is_videobar_like(text: str) -> bool:
    if "видеобар" in text or "videobar" in text:
        return True
    if "nextmeet" in text and ("камера" in text or "микрофон" in text):
        return True
    if ("all-in-one" in text or "все-в-одном" in text) and ("камера" in text and "микрофон" in text and "динамик" in text):
        return True
    return False


def _is_portable_monitor(text: str) -> bool:
    return ("on-lap" in text) or ("gechic" in text) or ("портативн" in text and "монитор" in text)


def _is_mounting(text: str) -> bool:
    if "креплен" in text or "кроншт" in text or "mount" in text:
        if "акуст" in text or "speaker" in text or "soundbar" in text:
            return False
        return True
    return False


def _is_hdmi_splitter(text: str, tokens: set[str]) -> bool:
    if "сплиттер" in text or "splitter" in text:
        return True
    if ("1:4" in text or "1x4" in text) and ("hdmi" in text):
        return True
    if ("de-эмбед" in text or "de-embed" in text or "скалир" in text or "scaling" in text) and ("hdmi" in text):
        return True
    if "1:4" in tokens and "hdmi" in text:
        return True
    return False


def classify_candidate(item: CandidateLike) -> ClassifiedCandidate:
    km = _km()
    text = _text(item)
    tokens = _tokenize(text)

    # --- hard overrides ---
    if _is_videowall_like(text):
        fam = "videowall_controller"
        return ClassifiedCandidate(
            candidate_id=item.candidate_id,
            family=fam,
            family_confidence=1.0,
            capabilities=[],
            interfaces=[],
            room_fit=_room_fit_for_family(fam),
            notes=["override:videowall_like"],
        )

    if _is_100v_speaker(text):
        fam = "speaker_100v"
        return ClassifiedCandidate(
            candidate_id=item.candidate_id,
            family=fam,
            family_confidence=1.0,
            capabilities=[],
            interfaces=[],
            room_fit=_room_fit_for_family(fam),
            notes=["override:100v_speaker"],
        )

    if _is_led_like(text):
        fam = "led_cabinet"
        return ClassifiedCandidate(
            candidate_id=item.candidate_id,
            family=fam,
            family_confidence=1.0,
            capabilities=["presentation"],
            interfaces=[],
            room_fit=_room_fit_for_family(fam),
            notes=["override:led_like"],
        )

    if _is_videobar_like(text):
        fam = "videobar"
        return ClassifiedCandidate(
            candidate_id=item.candidate_id,
            family=fam,
            family_confidence=1.0,
            capabilities=["vks", "presentation"],
            interfaces=["usb", "hdmi"],
            room_fit=["meeting_room", "auditorium"],
            notes=["override:videobar_like"],
        )

    if _is_portable_monitor(text):
        fam = "monitor_portable"
        return ClassifiedCandidate(
            candidate_id=item.candidate_id,
            family=fam,
            family_confidence=1.0,
            capabilities=["presentation"],
            interfaces=["hdmi", "usb"],
            room_fit=["meeting_room", "auditorium"],
            notes=["override:portable_monitor"],
        )

    if _is_mounting(text):
        fam = "mounting_kit"
        return ClassifiedCandidate(
            candidate_id=item.candidate_id,
            family=fam,
            family_confidence=1.0,
            capabilities=[],
            interfaces=[],
            room_fit=["meeting_room", "auditorium"],
            notes=["override:mounting"],
        )

    if _is_hdmi_splitter(text, tokens):
        fam = "hdmi_splitter"
        return ClassifiedCandidate(
            candidate_id=item.candidate_id,
            family=fam,
            family_confidence=1.0,
            capabilities=["presentation"],
            interfaces=["hdmi"],
            room_fit=["meeting_room", "auditorium"],
            notes=["override:hdmi_splitter"],
        )

    # --- normal scoring against knowledge map ---
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
            kw_norm = (kw or "").casefold().strip()
            if not kw_norm:
                continue
            if len(kw_norm) <= 2:
                if kw_norm in tokens:
                    score += 2.0
                    notes.append(f"kw_token:{kw_norm}")
            else:
                if kw_norm in text:
                    score += 2.0
                    notes.append(f"kw:{kw_norm}")

        fam_tokens = _tokenize(f"{family.key} {family.title}")
        overlap = len(tokens & fam_tokens)
        if overlap:
            score += min(2.0, overlap * 0.5)
            notes.append(f"title_overlap:{overlap}")

        if family_key == "interactive_panel" and ("nextpanel" in text or "интерактив" in text):
            score += 4.0
        if family_key == "ptz_camera" and ("ptz" in text or "ndi" in text):
            score += 3.0

        if score > best_score:
            best_score = score
            best_family = family_key
            best_notes = notes

    interfaces: list[str] = []
    if "hdmi" in text:
        interfaces.append("hdmi")
    if "usb" in text or "type-c" in text or "usb-c" in text:
        interfaces.append("usb")
    if "dante" in text:
        interfaces.append("dante")

    if best_family and best_family in km.families:
        family_def = km.families[best_family]
        capabilities = list(family_def.capabilities)
        room_fit = _room_fit_for_family(best_family)
    else:
        capabilities = []
        room_fit = ["meeting_room", "auditorium"]

    confidence = min(1.0, best_score / 8.0) if best_family else 0.0
    notes = list(best_notes) if best_notes else (["unclassified"] if not best_family else [])

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