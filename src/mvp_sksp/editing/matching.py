from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..domain.spec import LineItem, norm_key
from ..domain.ops import MatchSelector


@dataclass(frozen=True)
class MatchResult:
    line_id: str
    score: float


def _tokenize(s: str) -> set[str]:
    return {t for t in norm_key(s).replace(",", " ").split() if len(t) >= 3}


def score_line(line: LineItem, sel: MatchSelector) -> float:
    score = 0.0
    if sel.category and norm_key(line.category) == norm_key(sel.category):
        score += 2.0
    hay = " ".join([line.name, line.description, line.sku or "", line.manufacturer or "", line.model or ""])
    ht = _tokenize(hay)
    for c in sel.contains:
        ct = _tokenize(c)
        if not ct:
            continue
        inter = len(ht & ct)
        if inter:
            score += inter / max(1.0, len(ct))
    return score


def resolve_single_line(items: list[LineItem], sel: MatchSelector, *, min_score: float = 1.5) -> Optional[str]:
    best: Optional[MatchResult] = None
    for it in items:
        sc = score_line(it, sel)
        if sc < min_score:
            continue
        if best is None or sc > best.score:
            best = MatchResult(line_id=it.line_id, score=sc)
    return best.line_id if best else None
