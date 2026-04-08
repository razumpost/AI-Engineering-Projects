from __future__ import annotations

from typing import Optional

from ..adapters.bitrix_links import task_url
from ..adapters.deal_kuzu_retriever import KuzuDealRetriever
from ..adapters.deal_postgres_store import PostgresDealStore
from ..adapters.price_classifier import classify_price_item
from ..adapters.price_layer_store import PriceLayerStore
from ..domain.candidates import CandidatePool, CandidateTask
from ..domain.spec import Spec
from .graph_family_queries import graph_families_to_queries
from .graph_prompt_bridge import expand_graph
from .retrieval import build_candidate_pool_from_repo
from .role_price_hints import build_role_price_queries


_ALLOWED_ROLE_CATEGORIES = {
    "camera",
    "display",
    "microphone",
    "audio",
    "controller",
    "software",
    "ops",
}


def _should_use_role_query(rq: str) -> bool:
    rq_l = (rq or "").casefold().strip()

    banned = {
        "conference system",
        "usb conference system",
        "meeting room system",
    }
    if rq_l in banned:
        return False

    return True


def _item_matches_role_query(rq: str, category: str) -> bool:
    rq_l = (rq or "").casefold()

    if category not in _ALLOWED_ROLE_CATEGORIES:
        return False

    if "camera" in rq_l:
        return category == "camera"

    if "display" in rq_l or "interactive display" in rq_l or "professional display" in rq_l:
        return category == "display"

    if "microphone" in rq_l or "speakerphone" in rq_l:
        return category in {"microphone", "audio"}

    if "speaker" in rq_l or "audio" in rq_l:
        return category in {"audio", "controller"}

    if "player" in rq_l or "license" in rq_l or "cms" in rq_l or "signage" in rq_l or "software" in rq_l:
        return category == "software"

    return False


def _merge_price_search(
    pool: CandidatePool,
    price_store: PriceLayerStore,
    query: str,
    *,
    limit: int = 20,
    filter_by_role: bool = False,
) -> CandidatePool:
    hint_pool = price_store.search_price_candidates(query, limit=limit)

    if filter_by_role:
        filtered_items = []
        for item in hint_pool.items:
            meta = getattr(item, "meta", {}) or {}
            ev = meta.get("evidence_json", {}) or {}

            name = item.name or ""
            desc = ev.get("description")
            category = classify_price_item(name, desc)

            if not _item_matches_role_query(query, category):
                continue

            filtered_items.append(item)

        hint_pool.items = filtered_items

    return pool.merge(hint_pool)


def build_candidate_pool_for_deal(
    deal_id: str,
    transcript_text: str,
    *,
    current_spec: Optional[Spec] = None,
    mode: str = "compose",
    include_global: bool = True,
) -> CandidatePool:
    """
    Итоговый порядок источников:

    1) deal-aware retrieval:
       deal_id -> tasks -> Kuzu snapshots/items
    2) global retrieval:
       старый repo retrieval
    3) direct price-layer retrieval:
       latest_price_candidates по полному запросу
    4) graph-family retrieval:
       latest_price_candidates по family, выведенным из инженерного графа
    5) role-based retrieval:
       latest_price_candidates по role hints
    6) price enrichment:
       если у найденных кандидатов нет цены, дотягиваем её из latest_price_candidates
    """
    pg = PostgresDealStore()
    deal_tasks = pg.get_tasks_for_deal(deal_id)

    tasks = [
        CandidateTask(
            task_id=t.task_id,
            title=t.title,
            url=task_url(t.task_id),
            similarity=1.0,
            snippet="",
        )
        for t in deal_tasks
    ]

    kuzu = KuzuDealRetriever()
    deal_pool = kuzu.retrieve_for_deal(deal_id, tasks=tasks)

    pool = deal_pool

    if include_global:
        global_pool = build_candidate_pool_from_repo(
            transcript_text,
            current_spec=current_spec,
            mode=mode,
        )
        pool = pool.merge(global_pool)

    price_store = PriceLayerStore()

    # 1) прямой поиск по полному запросу
    pool = _merge_price_search(
        pool,
        price_store,
        transcript_text,
        limit=20,
        filter_by_role=False,
    )

    # 2) graph-aware family queries
    try:
        graph_data = expand_graph(transcript_text)
        graph_family_ids = [f["family_id"] for f in graph_data.get("resolved_families", [])]
    except Exception:
        graph_family_ids = []

    graph_queries = graph_families_to_queries(graph_family_ids)
    for gq in graph_queries:
        pool = _merge_price_search(
            pool,
            price_store,
            gq,
            limit=12,
            filter_by_role=False,
        )

    # 3) role-based queries
    role_queries = build_role_price_queries(transcript_text)
    for rq in role_queries:
        if not _should_use_role_query(rq):
            continue
        pool = _merge_price_search(
            pool,
            price_store,
            rq,
            limit=20,
            filter_by_role=True,
        )

    pool = price_store.enrich_pool_prices(pool)
    return pool