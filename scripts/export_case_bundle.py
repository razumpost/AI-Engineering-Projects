from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from mvp_sksp.adapters.deal_postgres_store import PostgresDealStore
from mvp_sksp.adapters.deal_kuzu_retriever import KuzuDealRetriever


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--deal-id", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--activity-id", default=None)
    ap.add_argument("--max-items", type=int, default=40)
    args = ap.parse_args()

    pg = PostgresDealStore()
    tasks = pg.get_tasks_for_deal(args.deal_id)
    files = pg.get_sksp_files_for_deal(args.deal_id)
    chunks = pg.get_transcript_chunks_for_deal(args.deal_id)
    best_text, best_meta = pg.get_best_transcript_for_deal(args.deal_id, activity_id=args.activity_id)

    kuzu = KuzuDealRetriever()
    # lightweight: just take items for deal (deal snapshots preferred)
    from mvp_sksp.domain.candidates import CandidateTask
    from mvp_sksp.adapters.bitrix_links import task_url

    ctasks = [CandidateTask(task_id=t.task_id, title=t.title, url=task_url(t.task_id), similarity=1.0, snippet="") for t in tasks]
    pool = kuzu.retrieve_for_deal(args.deal_id, tasks=ctasks, limit_items_per_snapshot=10000)

    items_preview: list[dict[str, Any]] = []
    for it in pool.items[: int(args.max_items)]:
        items_preview.append(
            {
                "candidate_id": it.candidate_id,
                "category": it.category,
                "sku": it.sku,
                "manufacturer": it.manufacturer,
                "name": it.name,
                "price": str(it.unit_price_rub) if it.unit_price_rub is not None else None,
                "price_source": it.price_source,
                "evidence_task_ids": it.evidence_task_ids,
            }
        )

    payload = {
        "deal_id": args.deal_id,
        "tasks": [t.__dict__ for t in tasks],
        "sksp_files": [f.__dict__ for f in files],
        "transcript_best_meta": best_meta,
        "transcript_best_text_head": best_text[:2000],
        "transcript_chunks": [
            {
                "chunk_id": c.chunk_id,
                "created_at": c.created_at,
                "activity_id": c.activity_id,
                "path": c.path,
                "content_head": (c.content or "")[:220],
            }
            for c in chunks[:80]
        ],
        "kuzu_items_preview": items_preview,
        "kuzu_items_total": len(pool.items),
    }

    out = Path(args.out).expanduser().resolve()
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("written:", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())