from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

from ..domain.candidates import CandidateItem
from ..planning.plan_models import ClassifiedCandidate


def classify_candidate(candidate: CandidateItem) -> ClassifiedCandidate:
    """Backward-compatible single-item classifier.

    Some pipeline stages import classify_candidate; internally we classify via batch API.
    """
    return classify_candidates([candidate])[0]


def classify_candidates(candidates: Iterable[CandidateItem]) -> List[ClassifiedCandidate]:
    """Heuristic candidate classifier.

    Goal:
      - Provide stable family labels for downstream planning/postprocess.
      - Avoid hard dependency on any ML model here.
      - Keep behavior deterministic and explainable.

    Notes:
      - This is a baseline. We will later replace/augment with graph+RAG/LLM classifier if needed.
    """
    out: List[ClassifiedCandidate] = []
    for c in candidates:
        text = _norm_text(f"{c.manufacturer or ''} {c.model or ''} {c.sku or ''} {c.name} {c.description}")
        family, conf, notes = _infer_family(text)

        out.append(
            ClassifiedCandidate(
                candidate_id=c.candidate_id,
                family=family,
                family_confidence=conf,
                capabilities=[],
                interfaces=[],
                room_fit=[],
                notes=notes,
            )
        )
    return out


def _norm_text(s: str) -> str:
    return " ".join((s or "").lower().replace("\u00a0", " ").split())


def _infer_family(text: str) -> tuple[str | None, float, list[str]]:
    """Return (family, confidence, notes)."""
    notes: list[str] = []

    # Strong signals first
    rules: list[tuple[str, str]] = [
        ("conference_unit", "конференц-система конференц система председателя делегата пульт председателя пульт делегата"),
        ("mic_gooseneck", "микрофон гусиная шея goose"),
        ("mic_wireless", "радиомикрофон wireless ручной петличный головной"),
        ("speaker", "акустика громкоговоритель колонка сателлит сабвуфер soundbar"),
        ("amp", "усилитель power amplifier"),
        ("dsp", "dsp процессор обработки аудио аудиопроцессор"),
        ("mixer", "микшер микшерный пульт"),
        ("camera", "камера ptz zoom видеокамера"),
        ("capture", "плата захвата capture usb capture"),
        ("switch", "коммутатор switch poe"),
        ("router", "маршрутизатор router"),
        ("display", "дисплей панель lcd led tv телевизор"),
        ("projector", "проектор projector"),
        ("screen", "экран projection screen"),
        ("led_wall", "светодиодный экран led кабинет модуль novastar"),
        ("controller", "контроллер processor scalers видеопроцессор"),
        ("mount", "крепление кронштейн стойка настенное потолочное"),
        ("cable", "кабель hdmi dp displayport usb xlr utp sftp cat6 cat5"),
        ("power", "блок питания power supply адаптер"),
        ("accessory", "комплект расходные материалы разъем коннектор"),
    ]

    for fam, kws in rules:
        if _contains_any(text, kws.split()):
            # Confidence heuristic
            conf = 0.75
            if fam in {"cable", "accessory"}:
                conf = 0.55
            if fam in {"led_wall", "conference_unit", "camera", "dsp", "amp"}:
                conf = 0.85
            notes.append(f"rule_match:{fam}")
            return fam, conf, notes

    # Weak fallback: try SKU/manufacturer patterns
    if "relacart" in text:
        notes.append("weak_match:relacart")
        return "conference_unit", 0.55, notes
    if "novastar" in text:
        notes.append("weak_match:novastar")
        return "controller", 0.55, notes

    return None, 0.0, ["unclassified"]


def _contains_any(text: str, tokens: list[str]) -> bool:
    for t in tokens:
        if not t:
            continue
        if t in text:
            return True
    return False