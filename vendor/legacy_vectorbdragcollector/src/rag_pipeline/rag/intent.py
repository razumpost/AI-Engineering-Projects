# =========================
# File: src/rag_pipeline/rag/intent.py
# =========================
from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class RagIntent:
    name: str
    allowed_doc_types: List[str]
    max_context_chars: int = 28_000


_SUMMARY_KEYS = (
    "краткое резюме",
    "резюме",
    "итоги",
    "итог",
    "вывод",
    "суммариз",
    "summary",
    "next steps",
    "следующие шаги",
    "дальнейшие шаги",
    "план действий",
    "что дальше",
    "что делать дальше",
)

_PRICE_KEYS = (
    "цена",
    "цены",
    "стоимость",
    "прайс",
    "прайсы",
    "ррц",
    "вход",
    "сколько стоит",
    "коммерческ",
    "кп",
    "offer",
    "proposal",
    "price",
)


def detect_intent(question: str) -> RagIntent:
    q = (question or "").lower()

    # IMPORTANT: summary has priority over price
    if any(k in q for k in _SUMMARY_KEYS):
        return RagIntent(
            name="summary",
            allowed_doc_types=[],
            max_context_chars=36_000,
        )

    if any(k in q for k in _PRICE_KEYS):
        return RagIntent(
            name="price",
            allowed_doc_types=["price_list", "vendor_kp", "sksps", "customer_kp_snapshot"],
            max_context_chars=32_000,
        )

    return RagIntent(
        name="general",
        allowed_doc_types=[],
        max_context_chars=28_000,
    )
