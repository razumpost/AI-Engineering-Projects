from __future__ import annotations

import re
from functools import lru_cache
from typing import Iterable, Protocol

from ..knowledge.loader import load_knowledge_map
from ..knowledge.models import FamilyDef
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
    return (
        ("100v" in text or "100 v" in text or "70v" in text or "70 v" in text)
        and ("акуст" in text or "speaker" in text or "колон" in text)
    )


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


def _signature_gate(f: FamilyDef, tokens: set[str], text: str) -> tuple[bool, float]:
    """Deterministic gating for family matching.

    Returns: (allowed, bonus_score)
    """
    sig = f.signature
    if sig.must_not:
        for phrase in sig.must_not:
            p = phrase.casefold()
            if not p:
                continue
            if p in text:
                return False, 0.0

    if sig.must_have:
        ok = False
        for phrase in sig.must_have:
            p = phrase.casefold()
            if not p:
                continue
            if p in text:
                ok = True
                break
            # token fallback
            if p in tokens:
                ok = True
                break
        if not ok:
            return False, 0.0

    bonus = 0.0
    for phrase in sig.strong_keywords:
        p = phrase.casefold()
        if not p:
            continue
        if p in text or p in tokens:
            bonus += 0.15
    return True, bonus


def _score_family(text: str, tokens: set[str], f: FamilyDef) -> float:
    allowed, bonus = _signature_gate(f, tokens, text)
    if not allowed:
        return -1.0

    score = 0.0
    for kw in f.keywords:
        k = kw.casefold().strip()
        if not k:
            continue
        if k in text:
            score += 0.12

    for cat in f.categories:
        c = cat.casefold().strip()
        if c and c in text:
            score += 0.05

    score += bonus
    return score


def _hard_overrides(text: str) -> str | None:
    """Fix high-signal misclassifications (kept minimal)."""
    if _is_videobar_like(text):
        return "videobar"
    if _is_videowall_like(text):
        # Let families handle exact type, but controller is typical in results.
        return "videowall_controller"
    if _is_led_like(text):
        return "led_cabinet"
    if _is_100v_speaker(text):
        # Prefer wall_speaker family with tag note handled downstream
        return "wall_speaker"
    return None


def classify_candidates(
    items: Iterable[ClassifiedCandidate],
) -> list[ClassifiedCandidate]:
    km = _km()
    families = list(km.families.values())

    out: list[ClassifiedCandidate] = []
    for it in items:
        t = _text(it)
        tok = _tokenize(t)

        override = _hard_overrides(t)
        if override and override in km.families:
            it.family = override
            it.family_score = 0.95
            it.room_fit = _room_fit_for_family(override)
            if override == "wall_speaker" and _is_100v_speaker(t):
                it.notes.append("обнаружена 100V акустика (уточнить трансляционную линию/усилитель)")
                # store tag signal for dependency resolver
                it.meta = dict(it.meta or {})
                it.meta["tag_100v"] = True
            out.append(it)
            continue

        best_family: str | None = None
        best_score = -1.0
        for f in families:
            s = _score_family(t, tok, f)
            if s > best_score:
                best_score = s
                best_family = f.key

        if best_family and best_score >= 0.12:
            it.family = best_family
            it.family_score = min(0.99, 0.4 + best_score)
            it.room_fit = _room_fit_for_family(best_family)
        else:
            it.family = None
            it.family_score = None
            it.room_fit = None
        out.append(it)

    return out


def pick_best_per_family(
    items: list[ClassifiedCandidate],
    per_family: int = 5,
) -> dict[str, list[ClassifiedCandidate]]:
    fam: dict[str, list[ClassifiedCandidate]] = {}
    for it in items:
        if not it.family:
            continue
        fam.setdefault(it.family, []).append(it)

    for k in fam:
        fam[k].sort(key=lambda x: float(x.score or 0.0), reverse=True)
        fam[k] = fam[k][: int(per_family)]

    return fam