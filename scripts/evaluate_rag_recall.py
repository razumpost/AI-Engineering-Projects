from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from mvp_sksp.adapters.rag_wrappers import retrieve_candidates


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if ln:
                rows.append(json.loads(ln))
    return rows


def _norm_sku(s: str | None) -> str | None:
    if not s:
        return None
    t = s.strip().replace(" ", "")
    return t.casefold() if t else None


def _collect_gt_skus(items: list[dict[str, Any]]) -> dict[str, set[str]]:
    gt: dict[str, set[str]] = defaultdict(set)
    for it in items:
        deal_id = str(it.get("deal_id") or "").strip()
        sku = _norm_sku(str(it.get("sku") or "").strip())
        if deal_id and sku:
            gt[deal_id].add(sku)
    return gt


def _collect_retrieved_skus(transcript: str, k: int) -> set[str]:
    q = (transcript or "").strip()
    if not q:
        return set()

    pool = retrieve_candidates(q)
    skus: list[str] = []
    for it in pool.items:
        s = _norm_sku(it.sku)
        if s:
            skus.append(s)
        if len(skus) >= k:
            break
    return set(skus)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs", required=True)
    ap.add_argument("--items", required=True)
    ap.add_argument("--k", type=int, default=50)
    ap.add_argument("--min-transcript-len", type=int, default=300)
    ap.add_argument("--limit", type=int, default=0, help="limit deals for quick run")
    args = ap.parse_args()

    pairs = _read_jsonl(Path(args.pairs).expanduser().resolve())
    items = _read_jsonl(Path(args.items).expanduser().resolve())
    gt = _collect_gt_skus(items)

    results: list[dict[str, Any]] = []
    for p in pairs:
        deal_id = str(p.get("deal_id") or "").strip()
        t = str(p.get("transcript") or "").strip()
        if not deal_id or len(t) < int(args.min_transcript_len):
            continue

        gt_skus = gt.get(deal_id, set())
        if not gt_skus:
            continue

        retrieved = _collect_retrieved_skus(t, k=int(args.k))
        hit = len(gt_skus & retrieved)
        rec = hit / max(1, len(gt_skus))

        results.append(
            {"deal_id": deal_id, "gt_cnt": len(gt_skus), "hit_cnt": hit, "recall": rec}
        )
        if args.limit and len(results) >= int(args.limit):
            break

    if not results:
        print("no results (check min-transcript-len / KUZU graph availability)")
        return 0

    avg = sum(r["recall"] for r in results) / len(results)
    results.sort(key=lambda r: r["recall"])

    print(f"deals_eval={len(results)} recall@{args.k} avg={avg:.3f}")
    print("worst 10:")
    for r in results[:10]:
        print(json.dumps(r, ensure_ascii=False))

    print("best 5:")
    for r in results[-5:]:
        print(json.dumps(r, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())