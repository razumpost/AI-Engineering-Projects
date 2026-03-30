from __future__ import annotations

from typing import Optional

from ..adapters.deal_kuzu_retriever import KuzuDealRetriever
from ..adapters.deal_postgres_store import PostgresDealStore
from ..domain.candidates import CandidatePool
from ..domain.spec import Spec
from .retrieval import build_candidate_pool_from_repo


def build_candidate_pool_for_deal(
    deal_id: str,
    transcript_text: str,
    *,
    current_spec: Optional[Spec] = None,
    mode: str = "compose",
    include_global: bool = True,
) -> CandidatePool:
    """Deal-aware retrieval + optional global retrieval.

    Deal-aware: deal_id -> tasks -> Kuzu snapshots/items.
    Global: existing repo retrieval by text (embeddings/graph/text depending on your adapter).
    """
    pg = PostgresDealStore()
    deal_tasks = pg.get_tasks_for_deal(deal_id)

    kuzu = KuzuDealRetriever()
    deal_pool = kuzu.retrieve_for_deal(
        deal_id,
        tasks=[
            # CandidateTask is in CandidatePool.tasks, but we keep it as in current domain
            # Here deal_kuzu_retriever already expects CandidateTask, so we only pass what it needs.
        ],
    )
    # ^ we'll re-fetch tasks as CandidateTask with url/title
    # (kept below, so deal_pool includes tasks list)

    # Rebuild tasks list cleanly and re-run deal retrieval (keeps code explicit and stable)
    from ..domain.candidates import CandidateTask  # local import to avoid cycles
    from ..adapters.bitrix_links import task_url

    tasks = [
        CandidateTask(task_id=t.task_id, title=t.title, url=task_url(t.task_id), similarity=1.0, snippet="")
        for t in deal_tasks
    ]
    deal_pool = kuzu.retrieve_for_deal(deal_id, tasks=tasks)

    if not include_global:
        return deal_pool

    global_pool = build_candidate_pool_from_repo(transcript_text, current_spec=current_spec, mode=mode)
    return deal_pool.merge(global_pool)