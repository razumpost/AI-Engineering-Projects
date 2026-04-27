from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from dotenv import load_dotenv  # type: ignore

from src.mvp_sksp.pipeline.deal_retrieval import build_candidate_pool_for_deal
from src.mvp_sksp.pipeline.graph_prompt_bridge import augment_transcript_with_graph


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_env() -> None:
    load_dotenv(dotenv_path=str(_repo_root() / ".env"), override=False)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--deal-id", required=True, help="ID сделки")
    ap.add_argument("--request", required=True, help="Текст запроса")
    ap.add_argument("--no-graph", action="store_true", help="Не использовать graph expansion")
    ap.add_argument(
        "--retrieval-diagnostics",
        action="store_true",
        help="Собрать и вывести JSON: merge по каждому query, pool до/после prune, probe поиски videowall",
    )
    ap.add_argument(
        "--retrieval-diagnostics-json",
        default="",
        help="Опционально: путь для записи того же JSON в файл",
    )
    return ap.parse_args()


def _print_candidate(idx: int, item: Any) -> None:
    print("=" * 100)
    print(
        f"[{idx}] candidate_id={item.candidate_id} "
        f"category={item.category} vendor={item.manufacturer} sku={item.sku} model={item.model}"
    )
    print(f"name={item.name}")
    print(f"price={item.unit_price_rub} price_source={item.price_source}")
    print(f"task_ids={item.evidence_task_ids}")
    print("meta=" + json.dumps(item.meta, ensure_ascii=False, indent=2, default=str))
    print()


def main() -> int:
    _load_env()
    args = parse_args()

    request_text = args.request
    graph_data = None

    if not args.no_graph:
        request_text, graph_data = augment_transcript_with_graph(args.request)

    if graph_data:
        print("[debug_deal_retrieval] graph_seed_families")
        for x in graph_data.get("seed_families", []):
            print(f"- {x}")
        print()

        print("[debug_deal_retrieval] graph_resolved_families")
        for fam in graph_data.get("resolved_families", []):
            print(f"- {fam['family_id']} | {fam['kind']} | {fam['name']}")
        print()

    retrieval_diagnostics: dict | None = {} if args.retrieval_diagnostics else None

    pool = build_candidate_pool_for_deal(
        deal_id=str(args.deal_id),
        transcript_text=request_text,
        current_spec=None,
        mode="compose",
        include_global=True,
        retrieval_diagnostics=retrieval_diagnostics,
    )

    if retrieval_diagnostics is not None:
        print("[debug_deal_retrieval] retrieval_diagnostics_json")
        diag_json = json.dumps(retrieval_diagnostics, ensure_ascii=False, indent=2, default=str)
        print(diag_json)
        print()
        out_path = (args.retrieval_diagnostics_json or "").strip()
        if out_path:
            Path(out_path).write_text(diag_json, encoding="utf-8")
            print(f"[debug_deal_retrieval] wrote retrieval_diagnostics to {out_path}")
            print()

    print(f"[debug_deal_retrieval] deal_id={args.deal_id}")
    print(f"[debug_deal_retrieval] items={len(pool.items)} tasks={len(pool.tasks)}")
    print()

    for i, item in enumerate(pool.items[:20], start=1):
        _print_candidate(i, item)

    print("[debug_deal_retrieval] top tasks")
    for t in pool.tasks[:20]:
        print(f"- task_id={t.task_id} title={t.title} url={t.url}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())