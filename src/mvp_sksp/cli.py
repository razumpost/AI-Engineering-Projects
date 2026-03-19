from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv  # type: ignore

from .config import Settings
from .llm.client import YandexFM, YandexFMConfig
from .llm.prompts import compose_prompt, patch_prompt
from .persistence.snapshot_store import load_last_valid, make_run_paths
from .pipeline.export import export_xlsx, render_markdown
from .pipeline.orchestrator import compose, patch
from .pipeline.retrieval import build_candidate_pool_from_repo
from .planning.coverage_planner import build_filtered_pool_for_coverage
from .planning.requirements import parse_requirements
from .planning.role_expander import expand_required_roles
from .planning.topology_selector import select_topology


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_env() -> None:
    load_dotenv(dotenv_path=str(_repo_root() / ".env"), override=False)


def main() -> int:
    _load_env()

    ap = argparse.ArgumentParser()
    ap.add_argument("--request", required=True, help="User request or call transcript")
    ap.add_argument("--run-dir", default=None)
    ap.add_argument("--interactive", action="store_true", help="Interactive patch loop")
    args = ap.parse_args()

    s = Settings()
    if os.getenv("MVP_SKSP_DEBUG_LLM") == "1":
        print("[debug] endpoint =", s.yandex_fm_endpoint)
        print("[debug] model_uri =", s.yandex_fm_model_uri)
        print("[debug] folder_id =", s.yandex_folder_id)

    run_dir = args.run_dir or s.run_dir
    run = make_run_paths(run_dir)

    requirements = parse_requirements(args.request)
    topology = select_topology(requirements)
    role_plan = expand_required_roles(requirements)

    raw_pool = build_candidate_pool_from_repo(args.request, current_spec=None, mode="compose")
    coverage = build_filtered_pool_for_coverage(
        pool=raw_pool,
        requirements=requirements,
        topology=topology,
        roles=role_plan,
    )
    pool = coverage.filtered_pool
    role_candidates = {
        d.role_key: d.selected_candidate_ids
        for d in coverage.role_debug
        if d.selected_candidate_ids
    }

    if os.getenv("MVP_SKSP_DEBUG_PLAN") == "1":
        print("[debug] requirements =", requirements.model_dump(mode="json"))
        print("[debug] topology =", topology.model_dump(mode="json"))
        print("[debug] role_plan =", [r.role_key for r in role_plan])
        for r in role_plan[:12]:
            print(
                "[debug] role",
                r.role_key,
                "allowed=",
                r.allowed_families,
                "preferred=",
                r.preferred_families,
                "qty=",
                r.suggested_qty,
            )
        print("[debug] raw_pool items =", len(raw_pool.items), "tasks =", len(raw_pool.tasks))
        print("[debug] filtered_pool items =", len(pool.items), "tasks =", len(pool.tasks))
        print("[debug] kept_candidate_ids =", coverage.kept_candidate_ids[:20])
        print("[debug] role_candidates =", role_candidates)
        if coverage.warnings:
            print("[debug] coverage warnings =", coverage.warnings)
        for d in coverage.role_debug:
            print(
                "[debug] coverage role",
                d.role_key,
                "selected=",
                d.selected_candidate_ids,
                "families=",
                d.selected_families,
                "warnings=",
                d.warnings,
            )

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

    pb = compose_prompt(args.request, pool, requirements=requirements, roles=role_plan, topology=topology, role_candidates=role_candidates)

    try:
        spec = compose(llm=llm, run=run, system=pb.system, user=pb.user, pool=pool, request_text=args.request)
    except Exception as e:
        spec = load_last_valid(run)
        print(f"[ERROR] compose failed: {e}")
        if spec is None:
            return 2

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

        patch_pool = build_candidate_pool_from_repo(text, current_spec=spec, mode="patch")
        current_pool = current_pool.merge(patch_pool)

        pb = patch_prompt(text, spec, current_pool)
        try:
            spec = patch(llm=llm, run=run, system=pb.system, user=pb.user, pool=current_pool, patch_text=text)
        except Exception as e:
            print(f"[ERROR] patch failed: {e}")
            spec = load_last_valid(run) or spec

        print(render_markdown(spec, pool=current_pool, settings=s))
        xlsx_path = export_xlsx(spec, run.run_dir / "sksp.xlsx", pool=current_pool, settings=s)
        print(f"\n[XLSX] saved: {xlsx_path}\n")

    return 0