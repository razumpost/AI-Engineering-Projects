# File: src/rag_pipeline/rag/routing.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Set

_PRICE_PAT = re.compile(r"\b(цена|стоим|прайс|итого|ндс|руб|₽|price|cost)\b", re.IGNORECASE)


@dataclass(frozen=True)
class RoutePlan:
    intent: str  # "price" | "general"
    allow_sources_prefix: Optional[tuple[str, ...]] = None
    allow_doc_types: Optional[Set[str]] = None
    query_expansions: Optional[List[str]] = None
    max_chunks_per_src: int = 2
    top_k: int = 30


def detect_route(question: str) -> RoutePlan:
    q = (question or "").strip()
    if _PRICE_PAT.search(q):
        expansions = [
            q,
            f"{q} цена руб",
            f"{q} итого ндс",
            f"{q} прайс",
            f"{q} коммерческое предложение",
        ]
        return RoutePlan(
            intent="price",
            allow_sources_prefix=("bitrix_file:",),
            allow_doc_types={"vendor_kp", "price_list", "sksps", "customer_kp_snapshot"},
            query_expansions=expansions,
            max_chunks_per_src=2,
            top_k=40,
        )
    return RoutePlan(intent="general", query_expansions=[q], top_k=20, max_chunks_per_src=3)


def is_price_like_text(text: str) -> bool:
    t = (text or "").lower()
    return ("₽" in t) or ("руб" in t) or ("ндс" in t) or ("итого" in t) or ("price" in t)


def filter_hits_by_plan(hits: Iterable[dict], plan: RoutePlan) -> List[dict]:
    out: List[dict] = []
    for h in hits:
        src = str(h.get("src") or "")
        doc_type = h.get("doc_type") or h.get("meta", {}).get("doc_type")
        doc_type = str(doc_type) if doc_type is not None else None

        if plan.allow_sources_prefix:
            if not any(src.startswith(p) for p in plan.allow_sources_prefix):
                continue

        if plan.allow_doc_types and doc_type:
            if doc_type not in plan.allow_doc_types:
                continue

        if plan.intent == "price":
            # если doc_type нет — оставим только те куски, где реально есть признаки цены
            if not doc_type and not is_price_like_text(str(h.get("text") or "")):
                continue

        out.append(h)
    return out


def dedup_and_cap_by_src(hits: List[dict], max_chunks_per_src: int) -> List[dict]:
    seen = set()
    per_src: dict[str, int] = {}
    out: List[dict] = []

    # сортируем: чем больше score — тем выше; если есть dist — чем меньше dist — тем выше
    def key(h: dict) -> tuple:
        score = h.get("score")
        dist = h.get("dist")
        score = float(score) if score is not None else -1.0
        dist = float(dist) if dist is not None else 9e9
        return (-score, dist)

    for h in sorted(hits, key=key):
        src = str(h.get("src") or "unknown")
        chunk = h.get("chunk") if h.get("chunk") is not None else h.get("chunk_id")
        chunk = int(chunk) if chunk is not None else -1

        sig = (src, chunk)
        if sig in seen:
            continue
        seen.add(sig)

        per_src.setdefault(src, 0)
        if per_src[src] >= max_chunks_per_src:
            continue
        per_src[src] += 1

        out.append(h)
    return out


# File: src/rag_pipeline/rag/answer.py
from __future__ import annotations

from typing import Dict, List

from src.rag_pipeline.rag.prompting import build_messages
from src.rag_pipeline.rag.routing import (
    RoutePlan,
    dedup_and_cap_by_src,
    detect_route,
    filter_hits_by_plan,
)
from src.rag_pipeline.rag.yandex_gpt_client import YandexGPTClient


def _merge_hits(all_hits: List[List[Dict]]) -> List[Dict]:
    merged: List[Dict] = []
    for batch in all_hits:
        merged.extend(batch or [])
    return merged


def rag_answer(question: str, retriever, top_k: int = 10) -> Dict:
    """
    retriever.search(query, top_k) -> list[dict]
    dict keys expected:
      - text, src, chunk or chunk_id, score or dist
      - optional: doc_type, title
    """
    plan: RoutePlan = detect_route(question)

    # 1) multi-query retrieval
    all_hits: List[List[Dict]] = []
    for q in (plan.query_expansions or [question]):
        all_hits.append(retriever.search(q, top_k=plan.top_k))

    hits = _merge_hits(all_hits)

    # 2) routing filter (PRICE: only files + only нужные типы)
    hits = filter_hits_by_plan(hits, plan)

    # 3) dedup + cap (важно: разнообразие источников)
    hits = dedup_and_cap_by_src(hits, max_chunks_per_src=plan.max_chunks_per_src)

    # 4) build context & call YaGPT
    messages = build_messages(question, hits)

    llm = YandexGPTClient()
    text = llm.complete(messages, temperature=0.2, max_tokens=900)

    return {"answer": text, "contexts": hits, "route": plan.intent}


# File: src/rag_pipeline/rag/prompting.py
from __future__ import annotations

from typing import Dict, List


def build_messages(question: str, context_blocks: List[Dict]) -> List[Dict[str, str]]:
    system = (
        "Ты — помощник, отвечающий строго по базе знаний компании.\n"
        "Правила:\n"
        "1) Отвечай ТОЛЬКО по предоставленному контексту.\n"
        "2) Если данных недостаточно — скажи, чего не хватает.\n"
        "3) Любое число/цена/срок — только с цитатой [SRC=... chunk=...].\n"
        "4) Для вопроса про цены: перечисли ВСЕ найденные предложения из контекста, не одно.\n"
        "5) Если в разных источниках разные цены на похожее — покажи диапазон и отличия (НДС/монтаж/доставка/проект).\n"
    )

    ctx_lines: List[str] = []
    for i, b in enumerate(context_blocks, 1):
        src = b.get("src", "unknown")
        chunk = b.get("chunk", b.get("chunk_id", ""))
        doc_type = b.get("doc_type") or b.get("meta", {}).get("doc_type") or ""
        title = b.get("title") or ""
        header = f"[SRC={src} chunk={chunk}"
        if doc_type:
            header += f" doc_type={doc_type}"
        if title:
            header += f" title={title}"
        header += "]"
        ctx_lines.append(f"### CONTEXT {i}\n{header}\n{b.get('text','')}\n")

    user = (
        "Контекст:\n"
        + "\n".join(ctx_lines)
        + "\n\nВопрос:\n"
        + question
        + "\n\nСформируй ответ и укажи источники."
    )

    return [{"role": "system", "text": system}, {"role": "user", "text": user}]
