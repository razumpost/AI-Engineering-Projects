from __future__ import annotations

from typing import Any, Iterable, List

from ..planning.plan_models import ClassifiedCandidate


def classify_candidate(obj: Any) -> ClassifiedCandidate:
    """Classify a single candidate-like object.

    Supports:
      - CandidateItem (manufacturer/sku/name/description/model)
      - Spec line-like objects used in spec_mapper (manufacturer/sku/name/description)
    """
    return classify_candidates([obj])[0]


def classify_candidates(objs: Iterable[Any]) -> List[ClassifiedCandidate]:
    """Deterministic heuristic classifier.

    Important: Must be tolerant to missing attributes because spec_mapper passes _LineLike.
    """
    out: List[ClassifiedCandidate] = []
    for o in objs:
        manufacturer = _first_str(getattr(o, "manufacturer", None), getattr(o, "vendor", None), getattr(o, "brand", None))
        model = _first_str(getattr(o, "model", None))
        sku = _first_str(getattr(o, "sku", None), getattr(o, "article", None), getattr(o, "partnumber", None))
        name = _first_str(getattr(o, "name", None), getattr(o, "title", None))
        desc = _first_str(getattr(o, "description", None), getattr(o, "desc", None))

        text = _norm_text(f"{manufacturer} {model} {sku} {name} {desc}")
        family, conf, notes = _infer_family(text)

        candidate_id = _first_str(getattr(o, "candidate_id", None), getattr(o, "id", None)) or "line_like"

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
    return " ".join((s or "").lower().replace("\u00a0", " ").split())


def _infer_family(text: str) -> tuple[str | None, float, list[str]]:
    notes: list[str] = []

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
        ("controller", "контроллер processor scaler видеопроцессор novastar"),
        ("mount", "крепление кронштейн стойка настенное потолочное"),
        ("cable", "кабель hdmi dp displayport usb xlr utp sftp cat6 cat5"),
        ("power", "блок питания power supply адаптер"),
        ("accessory", "комплект расходные материалы разъем коннектор"),
    ]

    for fam, kws in rules:
        if _contains_any(text, kws.split()):
            conf = 0.75
            if fam in {"cable", "accessory"}:
                conf = 0.55
            if fam in {"led_wall", "conference_unit", "camera", "dsp", "amp"}:
                conf = 0.85
            notes.append(f"rule_match:{fam}")
            return fam, conf, notes

    if "relacart" in text:
        notes.append("weak_match:relacart")
        return "conference_unit", 0.55, notes
    if "novastar" in text:
        notes.append("weak_match:novastar")
        return "controller", 0.55, notes

    return None, 0.0, ["unclassified"]


def _contains_any(text: str, tokens: list[str]) -> bool:
    for t in tokens:
        if t and t in text:
            return True
    return False


__all__ = ["classify_candidate", "classify_candidates"]