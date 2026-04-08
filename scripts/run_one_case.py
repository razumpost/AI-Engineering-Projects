# scripts/run_one_case.py
from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv  # type: ignore

from mvp_sksp.services import EngineerService


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def main() -> int:
    load_dotenv(dotenv_path=str(_repo_root() / ".env"), override=False)

    ap = argparse.ArgumentParser()
    ap.add_argument("--deal-id", required=True)
    ap.add_argument("--activity-id", default=None)
    ap.add_argument("--transcript-file", default=None)
    ap.add_argument("--request", default=None, help="Inline transcript/request text")
    ap.add_argument("--run-dir", default=None)
    ap.add_argument("--no-global", action="store_true")
    args = ap.parse_args()

    svc = EngineerService()
    result = svc.run(
        deal_id=str(args.deal_id),
        activity_id=args.activity_id,
        transcript_text=args.request,
        transcript_file=args.transcript_file,
        run_dir=args.run_dir,
        include_global=not args.no_global,
    )

    print(f"[run_one_case] job_id={result.job_id} iteration_id={result.iteration_id}")
    print(f"[run_one_case] run_dir={result.run_dir}")
    print(f"[run_one_case] xlsx={result.xlsx_path}")
    print(f"[run_one_case] md={result.markdown_path}")
    print(f"[run_one_case] evidence={result.evidence_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())