# File: src/rag_pipeline/query_builder.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from src.rag_pipeline.llm_extractor_yandex import YandexJsonExtractor, YandexJsonExtractorConfig

_RE_SPACES = re.compile(r"\s+")
_RE_EMAIL = re.compile(r"\b[\w.+'-]+@[\w.-]+\.[A-Za-z]{2,}\b")
_RE_PHONE = re.compile(r"(?:(?:(?:\+7|7|8)\s*)?\(?\d{3}\)?\s*[- ]?\d{3}\s*[- ]?\d{2}\s*[- ]?\d{2})")
_RE_INN = re.compile(r"\b\d{10,12}\b")
_RE_KPP = re.compile(r"\b\d{9}\b")
_RE_BIK = re.compile(r"\b\d{9}\b")
_RE_RS = re.compile(r"\b\d{20}\b")
_RE_KS = re.compile(r"\b\d{20}\b")

_RE_RESOLUTION = re.compile(r"\b(\d{3,5})\s*[xх×]\s*(\d{3,5})\b", re.IGNORECASE)
_RE_SIZE_MM = re.compile(r"\b(\d+(?:[\,\.]\d+)?)\s*(мм|mm)\b", re.IGNORECASE)
_RE_SIZE_M = re.compile(r"\b(\d+(?:[\,\.]\d+)?)\s*(м|m)\b", re.IGNORECASE)
_RE_DIM_2D = re.compile(
    r"\b(\d+(?:[\,\.]\d+)?)\s*[xх×]\s*(\d+(?:[\,\.]\d+)?)\s*(мм|mm|м|m)?\b",
    re.IGNORECASE,
)

_RE_PIXEL_PITCH = re.compile(r"\b(?:p(?:itch)?\s*)?(\d+(?:[\,\.]\d+)?)\s*(?:mm|мм)?\b", re.IGNORECASE)
_RE_PROCUREMENT = re.compile(r"\b(44\s*-?\s*фз|223\s*-?\s*фз|44\s*fz|223\s*fz)\b", re.IGNORECASE)
_RE_BUDGET = re.compile(
    r"\b(\d{1,3}(?:[ \u00A0]\d{3})*(?:[\,\.]\d+)?)\s*(₽|руб\.?|rur|rub)\b",
    re.IGNORECASE,
)
_RE_CITY = re.compile(r"\b(москва|санкт[- ]петербург|спб|екатеринбург|новосибирск|казань|нижний новгород)\b", re.IGNORECASE)
_RE_DEADLINE = re.compile(
    r"(?:\bдо\b|\bсрок\b|\bк\b)\s*(\d{1,2}[\.\/]\d{1,2}[\.\/]\d{2,4}|\d{4}-\d{2}-\d{2})",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class QueryBuilderConfig:
    max_query_chars: int = 700
    max_search_queries: int = 5
    use_llm: bool = False


def sanitize_pii(text: str) -> str:
    """Remove obvious PII and financial requisites before LLM call / logging."""
    t = (text or "").strip()
    t = _RE_EMAIL.sub("[EMAIL]", t)
    t = _RE_PHONE.sub("[PHONE]", t)
    t = _RE_RS.sub("[ACCOUNT]", t)
    t = _RE_KS.sub("[ACCOUNT]", t)
    t = _RE_INN.sub("[INN]", t)
    t = _RE_KPP.sub("[KPP]", t)
    t = _RE_BIK.sub("[BIK]", t)
    t = _RE_SPACES.sub(" ", t).strip()
    return t


def _first_unique(items: Sequence[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for x in items:
        v = (x or "").strip()
        if not v:
            continue
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def extract_features(text: str) -> Dict[str, List[str]]:
    """
    Heuristic extractor for structured search query.

    Returns dict of lists:
      - resolutions
      - sizes
      - pixel_pitch
      - procurement
      - budget
      - city
      - deadlines
    """
    t = sanitize_pii(text)
    low = t.lower()

    resolutions = [f"{a}x{b}" for a, b in _RE_RESOLUTION.findall(low)]
    dims: List[str] = []
    for a, b, unit in _RE_DIM_2D.findall(low):
        ua = a.replace(",", ".")
        ub = b.replace(",", ".")
        u = (unit or "").lower()
        if u:
            dims.append(f"{ua}x{ub} {u}")
        else:
            dims.append(f"{ua}x{ub}")

    sizes = [m[0].replace(",", ".") + " mm" for m in _RE_SIZE_MM.findall(low)]
    sizes += [m[0].replace(",", ".") + " m" for m in _RE_SIZE_M.findall(low)]

    procurement = [m.replace(" ", "").replace("-", "").upper() for m in _RE_PROCUREMENT.findall(low)]
    procurement = [p.replace("ФЗ", "FZ") for p in procurement]

    budgets = []
    for num, cur in _RE_BUDGET.findall(low):
        norm = num.replace("\u00A0", " ").replace(" ", "").replace(",", ".")
        budgets.append(f"{norm}{cur}")

    cities = [m for m in _RE_CITY.findall(low)]
    deadlines = [m for m in _RE_DEADLINE.findall(low)]

    pitches: List[str] = []
    for m in _RE_PIXEL_PITCH.findall(low):
        v = float(m.replace(",", "."))
        if 0.3 <= v <= 20.0:
            pitches.append(f"P{v:g}")

    return {
        "resolutions": _first_unique(resolutions),
        "dimensions": _first_unique(dims),
        "sizes": _first_unique(sizes),
        "pixel_pitch": _first_unique(pitches),
        "procurement": _first_unique(procurement),
        "budget": _first_unique(budgets),
        "city": _first_unique(cities),
        "deadlines": _first_unique(deadlines),
    }


def build_structured_query(text: str, *, cfg: Optional[QueryBuilderConfig] = None) -> str:
    """
    Build a compact, structured query from noisy transcript/task header.

    Output is plain text, suitable for embeddings/search.
    """
    c = cfg or QueryBuilderConfig()
    clean = sanitize_pii(text)
    feats = extract_features(clean)

    parts: List[str] = []
    if feats["city"]:
        parts.append("город: " + ", ".join(feats["city"][:2]))
    if feats["procurement"]:
        parts.append("закупка: " + ", ".join(feats["procurement"][:2]))
    if feats["budget"]:
        parts.append("бюджет: " + ", ".join(feats["budget"][:2]))
    if feats["deadlines"]:
        parts.append("сроки: " + ", ".join(feats["deadlines"][:2]))
    if feats["pixel_pitch"]:
        parts.append("шаг пикселя: " + ", ".join(feats["pixel_pitch"][:3]))
    if feats["dimensions"] or feats["sizes"]:
        dim = ", ".join((feats["dimensions"] + feats["sizes"])[:4])
        if dim:
            parts.append("габариты/размеры: " + dim)
    if feats["resolutions"]:
        parts.append("разрешение: " + ", ".join(feats["resolutions"][:3]))

    tail = clean[:250]
    if tail:
        parts.append("контекст: " + tail)

    out = " | ".join(parts)
    out = _RE_SPACES.sub(" ", out).strip()
    return out[: c.max_query_chars]


def build_search_queries(text: str, *, cfg: Optional[QueryBuilderConfig] = None) -> List[str]:
    """
    Produce multiple search queries (for hybrid retrieval / multi-query).

    If cfg.use_llm is True and YC creds exist, uses Yandex JSON extractor, otherwise heuristics.
    """
    c = cfg or QueryBuilderConfig()

    structured = build_structured_query(text, cfg=c)
    if not c.use_llm:
        return [structured]

    extractor_cfg = YandexJsonExtractorConfig.from_env()
    if not extractor_cfg.is_configured:
        return [structured]

    extractor = YandexJsonExtractor(extractor_cfg)
    payload = extractor.extract_search_queries(sanitize_pii(text), max_queries=c.max_search_queries)
    queries = payload.get("search_queries") or []
    queries = [q for q in queries if isinstance(q, str)]
    queries = _first_unique([q.strip() for q in queries if q.strip()])
    return queries[: c.max_search_queries] or [structured]
