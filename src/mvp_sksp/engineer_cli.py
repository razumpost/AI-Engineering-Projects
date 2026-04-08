from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv  # type: ignore

from .adapters.deal_postgres_store import PostgresDealStore
from .config import Settings
from .llm.client import YandexFM, YandexFMConfig
from .llm.prompts import compose_prompt, patch_prompt
from .persistence.snapshot_store import make_run_paths, update_last_valid
from .pipeline.deal_retrieval import build_candidate_pool_for_deal
from .pipeline.draft_seed import seed_spec_from_role_candidates
from .pipeline.export import export_xlsx, render_markdown
from .pipeline.orchestrator import compose, patch
from .pipeline.postprocess import postprocess_spec
from .planning.coverage_planner import build_filtered_pool_for_coverage
from .planning.requirements import parse_requirements
from .planning.role_expander import expand_required_roles
from .planning.topology_selector import select_topology


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_env() -> None:
    load_dotenv(dotenv_path=str(_repo_root() / ".env"), override=False)


def _load_transcript(args) -> tuple[str, dict]:
    if args.request and args.request.strip():
        return args.request.strip(), {"source": "cli_request"}

    if args.transcript_file:
        p = Path(args.transcript_file).expanduser().resolve()
        return p.read_text(encoding="utf-8", errors="replace").strip(), {"source": "file", "path": str(p)}

    pg = PostgresDealStore()
    text, meta = pg.get_best_transcript_for_deal(args.deal_id, activity_id=args.activity_id)
    meta["source"] = "db"
    return text, meta


def main() -> int:
    _load_env()

    ap = argparse.ArgumentParser()
    ap.add_argument("--deal-id", required=True)
    ap.add_argument("--activity-id", default=None)
    ap.add_argument("--request", default="", help="Override transcript text; otherwise loads from DB by deal_id.")
    ap.add_argument("--transcript-file", default=None)
    ap.add_argument("--run-dir", default=None)
    ap.add_argument("--interactive", action="store_true", help="Interactive patch loop")
    ap.add_argument("--no-global", action="store_true", help="Disable global retrieval; use deal-only KB")
    ap.add_argument("--min-transcript-len", type=int, default=200)
    args = ap.parse_args()

    s = Settings()
    run_dir = args.run_dir or s.run_dir
    run = make_run_paths(run_dir)

    transcript, tmeta = _load_transcript(args)
    if not transcript:
        raise SystemExit(f"Empty transcript for deal_id={args.deal_id}. Provide --request or check rag_chunks.")
    if len(transcript) < int(args.min_transcript_len):
        print(f"[warn] transcript is short ({len(transcript)} chars). meta={tmeta}")

    requirements = parse_requirements(transcript)
    topology = select_topology(requirements)
    role_plan = expand_required_roles(requirements)

    raw_pool = build_candidate_pool_for_deal(
        args.deal_id,
        transcript,
        current_spec=None,
        mode="compose",
        include_global=not args.no_global,
    )

    # KEY CHANGE:
    # In --no-global mode we must NOT coverage-filter the pool, otherwise we risk filtering to zero.
    # The purpose of --no-global is "use existing deal SKSP snapshot as baseline".
    if args.no_global:
        pool = raw_pool
        role_candidates: dict[str, list[str]] = {}
        coverage_warnings: list[str] = []
    else:
        coverage = build_filtered_pool_for_coverage(
            pool=raw_pool,
            requirements=requirements,
            topology=topology,
            roles=role_plan,
        )
        pool = coverage.filtered_pool
        role_candidates = {d.role_key: d.selected_candidate_ids for d in coverage.role_debug if d.selected_candidate_ids}
        coverage_warnings = list(coverage.warnings or [])

    if os.getenv("MVP_SKSP_DEBUG_PLAN") == "1":
        print("[debug] deal_id =", args.deal_id)
        print("[debug] transcript_meta =", tmeta)
        print("[debug] requirements =", requirements.model_dump(mode="json"))
        print("[debug] topology =", topology.model_dump(mode="json"))
        print("[debug] roles =", [r.role_key for r in role_plan])
        print("[debug] raw_pool items =", len(raw_pool.items), "tasks =", len(raw_pool.tasks))
        print("[debug] used_pool items =", len(pool.items), "tasks =", len(pool.tasks))
        print("[debug] role_candidates size =", {k: len(v) for k, v in role_candidates.items()})
        if coverage_warnings:
            print("[debug] coverage warnings =", coverage_warnings)

    llm = YandexFM(
        YandexFMConfig(
            endpoint=s.yandex_fm_endpoint,
            model_uri=s.yandex_fm_model_uri,
            api_key=s.yandex_fm_api_key,
            iam_token=s.yandex_fm_iam_token,
            folder_id=s.yandex_folder_id,
            timeout_s=float(s.llm_timeout_s),
            connect_timeout_s=float(s.llm_connect_timeout_s),
            max_retries=int(s.llm_max_retries),
        )
    )

    pb = compose_prompt(
        transcript,
        pool,
        requirements=requirements,
        roles=role_plan,
        topology=topology,
        role_candidates=role_candidates,
    )

    seed_spec = seed_spec_from_role_candidates(
        request_text=transcript,
        pool=pool,
        role_candidates=role_candidates,
        requirements=requirements,
        topology=topology,
    )

    try:
        spec = compose(llm=llm, run=run, system=pb.system, user=pb.user, pool=pool, request_text=transcript)
    except Exception as e:
        print(f"[ERROR] compose failed: {e}")
        spec = seed_spec

    spec = postprocess_spec(
        spec=spec,
        filtered_pool=pool,
        source_pool=raw_pool,
        requirements=requirements,
        topology=topology,
        roles=role_plan,
    )
    update_last_valid(run, spec)

    print(render_markdown(spec, pool=pool, settings=s))
    xlsx_path = export_xlsx(spec, run.run_dir / "sksp.xlsx", pool=pool, settings=s)
    print(f"\n[XLSX] saved: {xlsx_path}\n")

    if not args.interactive:
        return 0

    print("\n---\nInteractive mode. Type 'exit' to quit.\n")
    current_pool = pool

    while True:
        try:
            text = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not text:
            continue
        if text.lower() in {"exit", "quit", "q"}:
            break

        patch_pool = build_candidate_pool_for_deal(
            args.deal_id,
            text,
            current_spec=spec,
            mode="patch",
            include_global=not args.no_global,
        )
        current_pool = current_pool.merge(patch_pool)

        pb2 = patch_prompt(text, spec, current_pool)
        try:
            spec = patch(llm=llm, run=run, system=pb2.system, user=pb2.user, pool=current_pool, patch_text=text)
        except Exception as e:
            print(f"[ERROR] patch failed: {e}")

        spec = postprocess_spec(
            spec=spec,
            filtered_pool=current_pool,
            source_pool=current_pool,
            requirements=requirements,
            topology=topology,
            roles=role_plan,
        )
        update_last_valid(run, spec)

        print(render_markdown(spec, pool=current_pool, settings=s))
        xlsx_path = export_xlsx(spec, run.run_dir / "sksp.xlsx", pool=current_pool, settings=s)
        print(f"\n[XLSX] saved: {xlsx_path}\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())