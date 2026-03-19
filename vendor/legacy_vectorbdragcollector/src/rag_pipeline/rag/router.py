from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import List


class QueryIntent(str, Enum):
    PRICE = "price"
    BOM = "bom"
    QA = "qa"


@dataclass(frozen=True)
class RetrievalPolicy:
    top_k: int
    per_src_limit: int
    source_prefixes: List[str]
    title_keywords: List[str]
    doc_types: List[str]  # values of rag_documents.meta->>'doc_type'


_PRICE_RE = re.compile(r"\b(цена|стоим|прайс|кп|коммерческ|итого|ндс|руб)\b", re.I)
_BOM_RE = re.compile(r"\b(подобрат|состав|комплект|спецификац|сксп|оборудован|что нужно)\b", re.I)


def detect_intent(text: str) -> QueryIntent:
    t = (text or "").strip()
    if not t:
        return QueryIntent.QA
    if _PRICE_RE.search(t):
        return QueryIntent.PRICE
    if _BOM_RE.search(t):
        return QueryIntent.BOM
    return QueryIntent.QA


def policy_for(question: str) -> RetrievalPolicy:
    intent = detect_intent(question)

    if intent == QueryIntent.PRICE:
        # Цены: максимально "золото" -> КП подрядчиков + прайсы + КП заказчику + СкСп.
        return RetrievalPolicy(
            top_k=140,
            per_src_limit=12,
            source_prefixes=["bitrix_file:", "bitrix_chat:"],
            title_keywords=[
                "кп",
                "коммерческ",
                "прайс",
                "снепшот",
                "сксп",
                "счет",
                "счёт",
                "итого",
                "ндс",
            ],
            doc_types=[
                "vendor_kp",
                "price_list",
                "customer_kp_snapshot",
                "sksps",
            ],
        )

    if intent == QueryIntent.BOM:
        # Комплектность: КП заказчику + СкСп как эталон, КП подрядчиков как тех.основание,
        # задачи/чаты допустимы как пояснения.
        return RetrievalPolicy(
            top_k=160,
            per_src_limit=8,
            source_prefixes=["bitrix_file:", "bitrix_task:", "bitrix_chat:"],
            title_keywords=[
                "сксп",
                "спецификац",
                "кп",
                "коммерческ",
                "снепшот",
                "монтаж",
                "пусконалад",
                "доставка",
                "кабель",
                "контроллер",
            ],
            doc_types=[
                "customer_kp_snapshot",
                "sksps",
                "vendor_kp",
                "price_list",
            ],
        )

    return RetrievalPolicy(
        top_k=90,
        per_src_limit=5,
        source_prefixes=["bitrix_file:", "bitrix_task:", "bitrix_chat:"],
        title_keywords=[],
        doc_types=[],
    )

