# src/bitrix_kp_collector/filter_led_kp.py
from __future__ import annotations

from dataclasses import dataclass


def _norm(text: str) -> str:
    return (text or "").lower().replace("ё", "е").strip()


@dataclass(frozen=True)
class KpOriginDecision:
    ok: bool
    reason: str


def decide_kp_origin(
    *,
    filename: str,
    task_title: str = "",
    task_description: str = "",
    include_keywords: list[str] | None = None,
    exclude_keywords: list[str] | None = None,
) -> KpOriginDecision:
    haystack = " ".join([_norm(filename), _norm(task_title), _norm(task_description)])

    for kw in (exclude_keywords or []):
        nkw = _norm(kw)
        if nkw and nkw in haystack:
            return KpOriginDecision(False, f"excluded by keyword '{kw}'")

    inc = [k for k in (include_keywords or []) if _norm(k)]
    if not inc:
        return KpOriginDecision(True, "origin filter disabled")

    for kw in inc:
        if _norm(kw) in haystack:
            return KpOriginDecision(True, f"matched company keyword '{kw}'")

    return KpOriginDecision(False, "no company keywords matched")
