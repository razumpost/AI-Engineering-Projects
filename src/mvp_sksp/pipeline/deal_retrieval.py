from __future__ import annotations

from typing import Optional

from ..adapters.bitrix_links import task_url
from ..adapters.deal_kuzu_retriever import KuzuDealRetriever
from ..adapters.deal_postgres_store import PostgresDealStore
from ..adapters.price_classifier import classify_price_item
from ..adapters.price_layer_store import PriceLayerStore
from ..domain.candidates import CandidateItem, CandidatePool, CandidateTask
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
    "cable",
    "mount",
}


def _should_use_role_query(rq: str) -> bool:
    rq_l = (rq or "").casefold().strip()
    banned = {
        "conference system",
        "usb conference system",
        "meeting room system",
    }
    return rq_l not in banned


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

    if "speaker" in rq_l or "audio" in rq_l or "dsp" in rq_l:
        return category in {"audio", "controller"}

    if "player" in rq_l or "license" in rq_l or "cms" in rq_l or "signage" in rq_l or "software" in rq_l:
        return category in {"software", "ops"}

    if "cable" in rq_l or "hdmi" in rq_l or "usb" in rq_l or "cat6" in rq_l:
        return category == "cable"

    return False


def _candidate_text(item: CandidateItem) -> str:
    meta = getattr(item, "meta", {}) or {}
    ev = meta.get("evidence_json", {}) or {}
    return " ".join(
        [
            item.name or "",
            item.description or "",
            item.sku or "",
            item.model or "",
            ev.get("name") or "",
            ev.get("description") or "",
            ev.get("model") or "",
        ]
    ).casefold()


def _candidate_category(item: CandidateItem) -> str:
    meta = getattr(item, "meta", {}) or {}
    ev = meta.get("evidence_json", {}) or {}
    name = item.name or ev.get("name") or ""
    desc = item.description or ev.get("description") or ""
    return classify_price_item(name, desc)


def _is_discussion_graph(graph_family_ids: list[str]) -> bool:
    return any(
        fam in graph_family_ids
        for fam in [
            "delegate_unit",
            "chairman_unit",
            "discussion_central_unit",
            "discussion_dsp",
            "power_supply_discussion",
        ]
    )


def _is_meeting_room_graph(graph_family_ids: list[str]) -> bool:
    return "meeting_room_solution" in graph_family_ids and not _is_discussion_graph(graph_family_ids)


def _graph_allowed_categories(graph_family_ids: list[str]) -> set[str]:
    allowed: set[str] = set()

    if _is_discussion_graph(graph_family_ids):
        allowed.update({"microphone", "controller", "audio", "cable"})

    if _is_meeting_room_graph(graph_family_ids):
        allowed.update({"display", "camera", "microphone", "audio", "controller", "mount", "cable"})

    if "ptz_camera" in graph_family_ids:
        allowed.add("camera")
    if "display" in graph_family_ids or "mount_display" in graph_family_ids:
        allowed.update({"display", "mount"})
    if "smart_player" in graph_family_ids or "signage_license" in graph_family_ids:
        allowed.update({"software", "ops"})

    return allowed


def _discussion_queries() -> list[str]:
    return [
        "пульт делегата",
        "пульт председателя",
        "delegate unit",
        "chairman unit",
        "discussion central unit",
        "conference central unit",
        "central unit dis",
        "discussion system",
        "conference discussion system",
        "dis",
        "bosch dis",
        "taiden",
        "relacart",
        "televic",
        "bxb",
        "ps 6000",
        "блок питания dis",
        "discussion power supply",
        "audio processor conference",
        "conference dsp",
    ]


def _meeting_room_queries() -> list[str]:
    return [
        "professional display",
        "conference camera",
        "ptz camera",
        "conference microphone",
        "ceiling microphone",
        "table microphone",
        "soundbar",
        "audio dsp",
        "display mount",
    ]


def _merge_price_search(
    pool: CandidatePool,
    price_store: PriceLayerStore,
    query: str,
    *,
    limit: int = 20,
    filter_by_role: bool = False,
    allowed_categories: set[str] | None = None,
) -> CandidatePool:
    hint_pool = price_store.search_price_candidates(query, limit=limit)

    filtered_items = []
    for item in hint_pool.items:
        category = _candidate_category(item)

        if allowed_categories and category not in allowed_categories:
            continue

        if filter_by_role and not _item_matches_role_query(query, category):
            continue

        filtered_items.append(item)

    hint_pool.items = filtered_items
    return pool.merge(hint_pool)


def _request_mentions_cabling(request_text: str) -> bool:
    t = (request_text or "").casefold()
    return any(
        x in t
        for x in [
            "кабель",
            "hdmi",
            "usb",
            "витая пара",
            "cat",
            "длина",
            "расстояние",
            "displayport",
            "xlr",
        ]
    )


def _request_mentions_projector(request_text: str) -> bool:
    return any(x in (request_text or "").casefold() for x in ["проектор", "projector", "короткофокус"])


def _request_mentions_signage(request_text: str) -> bool:
    return any(x in (request_text or "").casefold() for x in ["signage", "digital signage", "контент", "смил", "player"])


def _discussion_relevance_score(item: CandidateItem, request_text: str) -> int:
    text = _candidate_text(item)
    category = _candidate_category(item)
    mention_cabling = _request_mentions_cabling(request_text)

    score = 0

    if any(x in text for x in [
        "пульт делегата",
        "delegate unit",
        "пульт председателя",
        "chairman unit",
        "discussion central unit",
        "conference central unit",
        "central unit",
        "discussion system",
        "conference system",
        "ps 6000",
        "блок питания dis",
        "discussion power supply",
        "bosch dis",
        "taiden",
        "relacart",
        "televic",
        "bxb",
        "gonsin",
        "пульт dis",
        "настольный dis",
    ]):
        score += 120

    if any(x in text for x in [
        "microphone",
        "микрофонный пульт",
        "настольный пульт",
        "delegate",
        "chairman",
        "conference",
        "discussion",
    ]):
        score += 45

    if category == "microphone":
        score += 35
    elif category == "controller":
        score += 30
    elif category == "audio":
        score += 12
    elif category == "cable":
        score += 3 if mention_cabling else -35

    if category in {"display", "software", "ops", "mount", "camera"}:
        score -= 200

    if any(x in text for x in [
        "projector",
        "laser phosphor",
        "throw ratio",
        "ansi lumens",
        "viewsonic",
        "media player",
        "spinetix",
        "digital signage",
        "html5 widgets",
        "smil",
        "oh75f",
        "oh85f",
        "outdoor",
    ]):
        score -= 220

    return score


def _meeting_room_relevance_score(item: CandidateItem, request_text: str) -> int:
    text = _candidate_text(item)
    category = _candidate_category(item)

    mention_cabling = _request_mentions_cabling(request_text)
    mention_projector = _request_mentions_projector(request_text)
    mention_signage = _request_mentions_signage(request_text)

    score = 0

    if category == "display":
        score += 65
    elif category == "camera":
        score += 60
    elif category == "microphone":
        score += 55
    elif category == "audio":
        score += 35
    elif category == "controller":
        score += 28
    elif category == "mount":
        score += 15
    elif category == "cable":
        score += 8 if mention_cabling else 2

    if any(x in text for x in ["display", "дисплей", "panel", "панель", "professional display", "interactive display"]):
        score += 45

    if any(x in text for x in ["ptz", "conference camera", "usb camera", "camera", "камера"]):
        score += 45

    if any(x in text for x in ["microphone", "микрофон", "beamforming", "table microphone", "ceiling microphone", "gooseneck"]):
        score += 40

    if any(x in text for x in ["soundbar", "speakerphone", "audio dsp", "audio processor", "dsp"]):
        score += 30

    if category == "mount" and any(x in text for x in ["mount", "bracket", "кронштейн", "стойка", "trolley"]):
        score += 18

    if any(x in text for x in ["delegate", "chairman", "discussion central unit", "discussion system", "ps 6000", "блок питания dis"]):
        score -= 220

    if category in {"software", "ops"}:
        score -= 220

    if any(x in text for x in ["media player", "spinetix", "diva", "hmp300", "hmp350", "nmp-", "nmp ", "digital signage", "html5 widgets", "smil"]):
        score -= 240 if not mention_signage else 0

    if any(x in text for x in ["projector", "ansi lumens", "throw ratio", "laser phosphor"]):
        score -= 180 if not mention_projector else 15

    return score


def _dedupe_items(items: list[CandidateItem]) -> list[CandidateItem]:
    out: list[CandidateItem] = []
    seen: set[str] = set()

    for item in items:
        key = "||".join(
            [
                (item.manufacturer or "").casefold(),
                (item.sku or "").casefold(),
                (item.model or "").casefold(),
                (item.name or "").casefold(),
            ]
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(item)

    return out


def _prune_pool_for_discussion_context(pool: CandidatePool, request_text: str) -> CandidatePool:
    mention_cabling = _request_mentions_cabling(request_text)

    scored: list[tuple[int, CandidateItem]] = []

    for item in pool.items:
        category = _candidate_category(item)
        text = _candidate_text(item)
        score = _discussion_relevance_score(item, request_text)

        if category in {"display", "software", "ops", "mount", "camera"}:
            continue

        if not mention_cabling and category == "cable":
            if not any(x in text for x in ["ps 6000", "power supply", "блок питания dis"]):
                continue

        if score < 20:
            continue

        scored.append((score, item))

    scored.sort(key=lambda x: x[0], reverse=True)
    items = [item for _, item in scored]
    items = _dedupe_items(items)
    return CandidatePool(items=items[:40], tasks=pool.tasks)


def _prune_pool_for_meeting_room_context(pool: CandidatePool, request_text: str) -> CandidatePool:
    mention_cabling = _request_mentions_cabling(request_text)
    mention_projector = _request_mentions_projector(request_text)
    mention_signage = _request_mentions_signage(request_text)

    scored: list[tuple[int, CandidateItem]] = []

    for item in pool.items:
        category = _candidate_category(item)
        text = _candidate_text(item)
        score = _meeting_room_relevance_score(item, request_text)

        if category in {"software", "ops"} and not mention_signage:
            continue

        if any(x in text for x in ["delegate", "chairman", "discussion", "ps 6000", "bosch dis", "taiden"]):
            continue

        if not mention_projector and any(x in text for x in ["projector", "ansi lumens", "throw ratio", "laser phosphor"]):
            continue

        if not mention_signage and any(x in text for x in ["media player", "spinetix", "digital signage", "html5 widgets", "smil", "diva", "hmp300", "hmp350"]):
            continue

        if category == "cable" and not mention_cabling:
            if not any(x in text for x in ["hdmi", "usb", "xlr", "cat6", "mount", "кронштейн"]):
                continue

        if score < 18:
            continue

        scored.append((score, item))

    scored.sort(key=lambda x: x[0], reverse=True)
    items = [item for _, item in scored]
    items = _dedupe_items(items)
    return CandidatePool(items=items[:60], tasks=pool.tasks)


def build_candidate_pool_for_deal(
    deal_id: str,
    transcript_text: str,
    *,
    current_spec: Optional[Spec] = None,
    mode: str = "compose",
    include_global: bool = True,
) -> CandidatePool:
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

    try:
        graph_data = expand_graph(transcript_text)
        graph_family_ids = [f["family_id"] for f in graph_data.get("resolved_families", [])]
    except Exception:
        graph_family_ids = []

    graph_allowed_categories = _graph_allowed_categories(graph_family_ids)
    discussion_graph = _is_discussion_graph(graph_family_ids)
    meeting_room_graph = _is_meeting_room_graph(graph_family_ids)

    graph_queries = graph_families_to_queries(graph_family_ids)

    if discussion_graph:
        for q in _discussion_queries():
            if q not in graph_queries:
                graph_queries.append(q)

    if meeting_room_graph:
        for q in _meeting_room_queries():
            if q not in graph_queries:
                graph_queries.append(q)

    for gq in graph_queries:
        pool = _merge_price_search(
            pool,
            price_store,
            gq,
            limit=30 if discussion_graph else 18,
            filter_by_role=False,
            allowed_categories=(graph_allowed_categories or None) if (discussion_graph or meeting_room_graph) else None,
        )

    pool = _merge_price_search(
        pool,
        price_store,
        transcript_text,
        limit=15 if discussion_graph else 20,
        filter_by_role=False,
        allowed_categories=(graph_allowed_categories or None) if (discussion_graph or meeting_room_graph) else None,
    )

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
            allowed_categories=(graph_allowed_categories or None) if (discussion_graph or meeting_room_graph) else None,
        )

    pool = price_store.enrich_pool_prices(pool)

    if discussion_graph:
        pool = _prune_pool_for_discussion_context(pool, transcript_text)
    elif meeting_room_graph:
        pool = _prune_pool_for_meeting_room_context(pool, transcript_text)

    return pool