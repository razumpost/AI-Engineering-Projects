from __future__ import annotations

from typing import Any, Optional

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


def _is_videowall_graph(graph_family_ids: list[str]) -> bool:
    return any(
        fam in graph_family_ids
        for fam in [
            "videowall_solution",
            "videowall_panel",
            "videowall_mount",
            "videowall_controller",
            "matrix_switcher",
        ]
    )


def _graph_allowed_categories(graph_family_ids: list[str]) -> set[str]:
    allowed: set[str] = set()

    if _is_videowall_graph(graph_family_ids):
        allowed.update({"display", "mount", "controller", "cable"})

    if _is_discussion_graph(graph_family_ids):
        allowed.update({"microphone", "controller", "audio", "cable"})

    if _is_meeting_room_graph(graph_family_ids):
        allowed.update({"display", "camera", "microphone", "audio", "controller", "mount", "cable"})

    if "ptz_camera" in graph_family_ids:
        allowed.add("camera")
    if "display" in graph_family_ids or "mount_display" in graph_family_ids:
        allowed.update({"display", "mount"})
    if (
        "videowall_controller" in graph_family_ids
        or "matrix_switcher" in graph_family_ids
        or "discussion_central_unit" in graph_family_ids
    ):
        allowed.add("controller")
    if "cabling_av" in graph_family_ids:
        allowed.add("cable")
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


def _videowall_queries() -> list[str]:
    return [
        "videowall panel",
        "lcd videowall panel",
        "videowall mount",
        "pull-out wall mount",
        "videowall frame",
        "videowall controller",
        "videowall processor",
        "video wall processor",
        "matrix switcher",
        "hdmi matrix switcher",
    ]


def _merge_price_search(
    pool: CandidatePool,
    price_store: PriceLayerStore,
    query: str,
    *,
    limit: int = 20,
    filter_by_role: bool = False,
    allowed_categories: set[str] | None = None,
    diagnostics: list[dict] | None = None,
    diagnostics_phase: str = "graph",
) -> CandidatePool:
    hint_pool = price_store.search_price_candidates(query, limit=limit)
    raw_items = hint_pool.items

    if diagnostics is not None:
        raw_top: list[dict] = []
        for item in raw_items[:10]:
            raw_top.append(
                {
                    "candidate_id": item.candidate_id,
                    "name": (item.name or "")[:220],
                    "computed_category": _candidate_category(item),
                }
            )

    filtered_items = []
    for item in raw_items:
        category = _candidate_category(item)

        if allowed_categories and category not in allowed_categories:
            continue

        if filter_by_role and not _item_matches_role_query(query, category):
            continue

        filtered_items.append(item)

    if diagnostics is not None:
        filt_top: list[dict] = []
        for item in filtered_items[:10]:
            filt_top.append(
                {
                    "candidate_id": item.candidate_id,
                    "name": (item.name or "")[:220],
                    "computed_category": _candidate_category(item),
                }
            )
        diagnostics.append(
            {
                "phase": diagnostics_phase,
                "query": query,
                "limit": limit,
                "filter_by_role": filter_by_role,
                "allowed_categories": sorted(allowed_categories) if allowed_categories else None,
                "raw_search_count": len(raw_items),
                "raw_top": raw_top,
                "after_category_and_role_filter_count": len(filtered_items),
                "after_filter_top": filt_top,
            }
        )

    hint_pool.items = filtered_items
    return pool.merge(hint_pool)


def _looks_like_videowall_panel_or_controller(item: CandidateItem) -> bool:
    """Diagnostics-only heuristic: whether text resembles videowall panel/controller/matrix core."""
    text = _candidate_text(item)
    cat = _candidate_category(item)
    if cat == "display" and any(
        x in text
        for x in [
            "videowall",
            "видеостен",
            "video wall",
            "lcd videowall",
            "narrow bezel",
            "ultra narrow bezel",
            "панель видеостен",
            "дисплей для видеостен",
        ]
    ):
        return True
    if cat == "controller" and any(
        x in text
        for x in [
            "videowall controller",
            "videowall processor",
            "video wall processor",
            "matrix switcher",
            "hdmi matrix",
            "матричн",
            "коммутатор",
        ]
    ):
        return True
    if any(
        x in text
        for x in [
            "videowall panel",
            "lcd videowall",
            "videowall controller",
            "matrix switcher",
        ]
    ):
        return True
    return False


def _snapshot_pool_top_categories(pool: CandidatePool, top_n: int) -> list[dict]:
    out: list[dict] = []
    for item in pool.items[:top_n]:
        out.append(
            {
                "candidate_id": item.candidate_id,
                "name": (item.name or "")[:220],
                "computed_category": _candidate_category(item),
            }
        )
    return out


def _snapshot_pool_top_scored(
    pool: CandidatePool,
    request_text: str,
    top_n: int,
    *,
    videowall_graph: bool,
    discussion_graph: bool,
    meeting_room_graph: bool,
) -> list[dict]:
    out: list[dict] = []
    for item in pool.items[:top_n]:
        if videowall_graph:
            score = _videowall_relevance_score(item, request_text)
        elif discussion_graph:
            score = _discussion_relevance_score(item, request_text)
        elif meeting_room_graph:
            score = _meeting_room_relevance_score(item, request_text)
        else:
            score = 0
        out.append(
            {
                "candidate_id": item.candidate_id,
                "name": (item.name or "")[:220],
                "computed_category": _candidate_category(item),
                "score": score,
            }
        )
    return out


_VIDEO_WALL_PROBE_QUERIES = [
    "videowall panel",
    "lcd videowall panel",
    "videowall controller",
    "videowall processor",
    "matrix switcher",
]


def _run_videowall_probe_searches(price_store: PriceLayerStore, *, limit: int = 15) -> list[dict]:
    rows: list[dict] = []
    for q in _VIDEO_WALL_PROBE_QUERIES:
        hint = price_store.search_price_candidates(q, limit=limit)
        top: list[dict] = []
        for item in hint.items[:10]:
            top.append(
                {
                    "candidate_id": item.candidate_id,
                    "name": (item.name or "")[:220],
                    "computed_category": _candidate_category(item),
                }
            )
        rows.append(
            {
                "probe_query": q,
                "raw_search_count": len(hint.items),
                "raw_top": top,
            }
        )
    return rows


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


def _videowall_expected_categories(graph_family_ids: list[str]) -> set[str]:
    expected: set[str] = set()
    if "videowall_panel" in graph_family_ids:
        expected.add("display")
    if "videowall_mount" in graph_family_ids:
        expected.add("mount")
    if "videowall_controller" in graph_family_ids or "matrix_switcher" in graph_family_ids:
        expected.add("controller")
    if "cabling_av" in graph_family_ids:
        expected.add("cable")
    if not expected:
        expected.update({"display", "mount", "controller", "cable"})
    return expected


def _videowall_relevance_score(item: CandidateItem, request_text: str) -> int:
    text = _candidate_text(item)
    category = _candidate_category(item)
    mention_cabling = _request_mentions_cabling(request_text)

    score = 0

    if category == "display":
        score += 80
    elif category == "mount":
        score += 20
    elif category == "controller":
        score += 95
    elif category == "cable":
        score += 4 if mention_cabling else -20
    else:
        score -= 120

    if any(x in text for x in ["videowall", "видеостен", "video wall"]):
        score += 90

    if category == "display":
        if any(x in text for x in ["videowall", "видеостен", "narrow bezel", "ultra narrow bezel", "шов"]):
            score += 40
        if any(x in text for x in ["smart display", "all in one", "interactive display", "интерактивн"]):
            score -= 180

    if category == "mount":
        if any(x in text for x in ["videowall mount", "pull-out", "frame", "каркас", "крепление видеостен", "настенн"]):
            score += 40
        if any(x in text for x in ["trolley", "mobile stand", "тележка"]):
            score -= 60

    if category == "controller":
        if any(x in text for x in ["videowall controller", "videowall processor", "video wall processor"]):
            score += 85
        if any(x in text for x in ["matrix switcher", "hdmi matrix", "матричн", "коммутатор"]):
            score += 70

    if category == "cable":
        if any(x in text for x in ["displayport", "hdmi", "cat6", "витая пара"]):
            score += 8

    if any(x in text for x in ["calibration", "калибров", "kit", "комплект", "service tool", "toolkit"]):
        score -= 140

    if any(x in text for x in ["projector", "проектор", "ansi lumens", "throw ratio", "laser phosphor"]):
        score -= 220
    if any(x in text for x in ["ptz", "conference camera", "камера", "webcam"]):
        score -= 220
    if any(x in text for x in ["spinetix", "digital signage", "signage", "html5 widgets", "smil"]):
        score -= 200
    if any(x in text for x in ["ops", "mini pc", "slot pc", "media player"]):
        score -= 200
    if any(x in text for x in ["meeting room", "conference room"]):
        score -= 70

    return score


def _is_accessory_like(item: CandidateItem) -> bool:
    text = _candidate_text(item)
    return any(
        x in text
        for x in [
            "calibration kit",
            "калибров",
            "service toolkit",
            "toolkit",
            "монтажный комплект",
            "комплект юстировки",
            "аксессуар",
            "accessory",
            "spare",
            "зип",
        ]
    )


def _is_videowall_core_candidate(item: CandidateItem) -> bool:
    category = _candidate_category(item)
    text = _candidate_text(item)

    if _is_accessory_like(item):
        return False

    if category == "display":
        return any(x in text for x in ["videowall", "видеостен", "narrow bezel", "ultra narrow bezel", "шов", "panel", "панель", "lcd"])
    if category == "controller":
        return any(
            x in text
            for x in [
                "videowall controller",
                "videowall processor",
                "video wall processor",
                "matrix switcher",
                "hdmi matrix",
                "матричн",
                "коммутатор",
            ]
        )
    return False


def _balance_videowall_items(items: list[CandidateItem], graph_family_ids: list[str]) -> list[CandidateItem]:
    require_panel = "videowall_panel" in graph_family_ids
    require_controller = ("videowall_controller" in graph_family_ids) or ("matrix_switcher" in graph_family_ids)

    display_bucket = [it for it in items if _candidate_category(it) == "display" and not _is_accessory_like(it)]
    controller_bucket = [it for it in items if _candidate_category(it) == "controller" and not _is_accessory_like(it)]
    mount_bucket = [it for it in items if _candidate_category(it) == "mount"]
    cable_bucket = [it for it in items if _candidate_category(it) == "cable"]

    ordered: list[CandidateItem] = []
    used_ids: set[str] = set()

    def push(item: CandidateItem | None) -> None:
        if not item:
            return
        cid = item.candidate_id
        if cid in used_ids:
            return
        used_ids.add(cid)
        ordered.append(item)

    # Ensure core roles appear first when available.
    if require_panel:
        push(display_bucket[0] if display_bucket else None)
    if require_controller:
        push(controller_bucket[0] if controller_bucket else None)

    # Fill early core with remaining display/controller.
    for it in display_bucket[1:3]:
        push(it)
    for it in controller_bucket[1:3]:
        push(it)

    # Add dependencies later, limited to avoid mount/cable flood.
    for it in mount_bucket[:3]:
        push(it)
    for it in cable_bucket[:2]:
        push(it)

    # Tail: remaining items in original score order.
    for it in items:
        push(it)

    return ordered


def _prune_pool_for_videowall_context(
    pool: CandidatePool,
    request_text: str,
    graph_family_ids: list[str],
) -> CandidatePool:
    expected_categories = _videowall_expected_categories(graph_family_ids)
    mention_cabling = _request_mentions_cabling(request_text)

    scored: list[tuple[int, CandidateItem]] = []
    for item in pool.items:
        category = _candidate_category(item)
        if category not in expected_categories:
            continue

        score = _videowall_relevance_score(item, request_text)
        text = _candidate_text(item)

        if category == "display" and "videowall_panel" in graph_family_ids:
            if _is_accessory_like(item):
                continue
            if not any(x in text for x in ["videowall", "видеостен", "narrow bezel", "ultra narrow bezel", "шов", "panel", "панель", "lcd"]):
                continue

        if category == "mount" and "videowall_mount" in graph_family_ids:
            if not any(x in text for x in ["videowall mount", "pull-out", "frame", "каркас", "крепление видеостен", "настенн"]):
                continue

        if category == "controller" and ("videowall_controller" in graph_family_ids or "matrix_switcher" in graph_family_ids):
            if not any(
                x in text
                for x in [
                    "videowall controller",
                    "videowall processor",
                    "video wall processor",
                    "matrix switcher",
                    "hdmi matrix",
                    "матричн",
                    "коммутатор",
                ]
            ):
                continue

        if category == "cable" and "cabling_av" in graph_family_ids and not mention_cabling:
            # Fail-closed for videowall core: don't let cables dominate top when cabling wasn't requested.
            if score < 30:
                continue

        if score < 30:
            continue
        scored.append((score, item))

    scored.sort(key=lambda x: x[0], reverse=True)
    items = [item for _, item in scored]

    items = _dedupe_items(items)
    items = _balance_videowall_items(items, graph_family_ids)
    return CandidatePool(items=items[:50], tasks=pool.tasks)


def build_candidate_pool_for_deal(
    deal_id: str,
    transcript_text: str,
    *,
    current_spec: Optional[Spec] = None,
    mode: str = "compose",
    include_global: bool = True,
    retrieval_diagnostics: dict[str, Any] | None = None,
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
    videowall_graph = _is_videowall_graph(graph_family_ids)

    graph_queries = graph_families_to_queries(graph_family_ids)

    if discussion_graph:
        for q in _discussion_queries():
            if q not in graph_queries:
                graph_queries.append(q)

    if meeting_room_graph:
        for q in _meeting_room_queries():
            if q not in graph_queries:
                graph_queries.append(q)

    if videowall_graph:
        for q in _videowall_queries():
            if q not in graph_queries:
                graph_queries.append(q)

    merge_diag: list[dict] | None = None
    if retrieval_diagnostics is not None:
        merge_diag = []

    for gq in graph_queries:
        pool = _merge_price_search(
            pool,
            price_store,
            gq,
            limit=30 if discussion_graph else (28 if videowall_graph else 18),
            filter_by_role=False,
            allowed_categories=(graph_allowed_categories or None)
            if (discussion_graph or meeting_room_graph or videowall_graph)
            else None,
            diagnostics=merge_diag,
            diagnostics_phase="graph_query",
        )

    pool = _merge_price_search(
        pool,
        price_store,
        transcript_text,
        limit=15 if discussion_graph else (18 if videowall_graph else 20),
        filter_by_role=False,
        allowed_categories=(graph_allowed_categories or None)
        if (discussion_graph or meeting_room_graph or videowall_graph)
        else None,
        diagnostics=merge_diag,
        diagnostics_phase="transcript",
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
            allowed_categories=(graph_allowed_categories or None)
            if (discussion_graph or meeting_room_graph or videowall_graph)
            else None,
            diagnostics=merge_diag,
            diagnostics_phase="role_query",
        )

    pool = price_store.enrich_pool_prices(pool)

    if retrieval_diagnostics is not None:
        retrieval_diagnostics["graph_family_ids"] = list(graph_family_ids)
        retrieval_diagnostics["graph_queries"] = list(graph_queries)
        retrieval_diagnostics["videowall_graph"] = videowall_graph
        retrieval_diagnostics["discussion_graph"] = discussion_graph
        retrieval_diagnostics["meeting_room_graph"] = meeting_room_graph
        retrieval_diagnostics["graph_allowed_categories"] = (
            sorted(graph_allowed_categories) if graph_allowed_categories else []
        )
        retrieval_diagnostics["merge_steps"] = merge_diag or []
        retrieval_diagnostics["pool_before_prune_top20"] = _snapshot_pool_top_categories(pool, 20)
        retrieval_diagnostics["panel_controller_like_before_prune"] = any(
            _looks_like_videowall_panel_or_controller(it) for it in pool.items
        )
        if videowall_graph:
            retrieval_diagnostics["videowall_probe_searches"] = _run_videowall_probe_searches(
                price_store, limit=15
            )

    if discussion_graph:
        pool = _prune_pool_for_discussion_context(pool, transcript_text)
    elif videowall_graph:
        pool = _prune_pool_for_videowall_context(pool, transcript_text, graph_family_ids)
    elif meeting_room_graph:
        pool = _prune_pool_for_meeting_room_context(pool, transcript_text)

    if retrieval_diagnostics is not None:
        retrieval_diagnostics["pool_after_prune_top20"] = _snapshot_pool_top_scored(
            pool,
            transcript_text,
            20,
            videowall_graph=videowall_graph,
            discussion_graph=discussion_graph,
            meeting_room_graph=meeting_room_graph,
        )

    return pool