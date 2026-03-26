from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _collector_root(system_root: str) -> Path | None:
    if not system_root:
        return None
    p = Path(system_root).expanduser().resolve()
    return p.parent if p.name.startswith(".cognee") else p


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            rows.append(json.loads(ln))
    return rows


def _file_exists(abs_path: str | None) -> bool:
    if not abs_path:
        return False
    try:
        return Path(abs_path).exists()
    except Exception:
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs", default="/tmp/pairs.jsonl")
    ap.add_argument("--head", type=int, default=3)
    ap.add_argument("--show-missing", action="store_true")
    args = ap.parse_args()

    pairs_path = Path(args.pairs).expanduser().resolve()
    rows = _read_jsonl(pairs_path)
    if not rows:
        print("empty")
        return 0

    deal_cnt = len({r.get("deal_id") for r in rows if r.get("deal_id")})
    exts = Counter((str(r.get("sksp_local_path") or "")).split(".")[-1].casefold() for r in rows)

    missing = [r for r in rows if not _file_exists(r.get("sksp_abs_path"))]
    short_t = [r for r in rows if int(r.get("transcript_len") or 0) < 200]

    print(f"pairs={len(rows)} unique_deals={deal_cnt}")
    print("sksp_extensions:", dict(exts))
    print(f"missing_sksp_files={len(missing)} short_transcripts(<200)={len(short_t)}")

    print("\n--- head ---")
    for r in rows[: int(args.head)]:
        print(json.dumps({k: r.get(k) for k in [
            "deal_id", "activity_id", "transcript_len", "sksp_file_id", "sksp_name", "sksp_abs_path", "notes"
        ]}, ensure_ascii=False))

    if args.show_missing and missing:
        print("\n--- missing files (first 20) ---")
        for r in missing[:20]:
            print(json.dumps({k: r.get(k) for k in ["deal_id", "sksp_local_path", "sksp_abs_path"]}, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())