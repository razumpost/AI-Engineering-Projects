from __future__ import annotations

import argparse
import os
import textwrap
from pathlib import Path

from sqlalchemy import create_engine, text


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _collector_root(system_root: str) -> Path | None:
    if not system_root:
        return None
    p = Path(system_root).expanduser().resolve()
    return p.parent if p.name.startswith(".cognee") else p


def _candidate_rel_paths(rel: str | None) -> list[str]:
    rel = (rel or "").strip()
    if not rel:
        return []
    out = [rel]
    if "calls_transcripts_dev" in rel:
        out.append(rel.replace("calls_transcripts_dev", "calls_transcripts"))
    return list(dict.fromkeys(out))


def _read_text_file(fp: Path) -> str:
    return fp.read_text(encoding="utf-8", errors="replace")


def _resolve_file_text(path: str | None, collector_root: Path | None) -> str | None:
    for rel in _candidate_rel_paths(path):
        p = Path(rel)
        if p.is_absolute() and p.exists():
            return _read_text_file(p)
        if collector_root:
            cand = (collector_root / rel).resolve()
            if cand.exists():
                return _read_text_file(cand)
        cand2 = (Path.cwd() / rel).resolve()
        if cand2.exists():
            return _read_text_file(cand2)
    return None


def _wrap_print(text_value: str, width: int, max_lines: int | None) -> None:
    if width <= 0:
        print(text_value)
        return
    lines_out = 0
    for para in text_value.splitlines() if "\n" in text_value else [text_value]:
        for ln in textwrap.wrap(para, width=width, break_long_words=False, replace_whitespace=False):
            print(ln)
            lines_out += 1
            if max_lines is not None and lines_out >= max_lines:
                return


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--activity-id", required=True)
    ap.add_argument("--prefer", choices=["db", "file"], default="file")
    ap.add_argument("--wrap", type=int, default=120, help="Wrap width (0 = no wrap).")
    ap.add_argument("--max-lines", type=int, default=260, help="Max printed lines (0 = unlimited).")
    args = ap.parse_args()

    dsn = _env("DB_DSN")
    if not dsn:
        raise SystemExit("DB_DSN empty. source .env first.")

    system_root = _env("SYSTEM_ROOT_DIRECTORY")
    collector_root = _collector_root(system_root)

    eng = create_engine(dsn, future=True, pool_pre_ping=True)
    q = text(
        """
        SELECT id, created_at::text AS created_at,
               meta::jsonb->>'deal_id' AS deal_id,
               meta::jsonb->>'path' AS path,
               content::text AS content
        FROM rag_chunks
        WHERE meta::jsonb->>'activity_id' = :aid
        ORDER BY created_at ASC NULLS LAST, id ASC
        """
    )
    with eng.connect() as c:
        rows = c.execute(q, {"aid": args.activity_id}).mappings().all()

    if not rows:
        raise SystemExit(f"No rows for activity_id={args.activity_id}")

    path = (rows[0].get("path") or "").strip()
    deal_id = rows[0].get("deal_id")

    merged_db = "\n".join((r.get("content") or "").strip() for r in rows if (r.get("content") or "").strip()).strip()
    file_text = _resolve_file_text(path, collector_root)

    if args.prefer == "file":
        text_out = (file_text or "").strip() or merged_db
    else:
        text_out = merged_db or ((file_text or "").strip())

    print(f"activity_id={args.activity_id} deal_id={deal_id} rows={len(rows)} path={path}")
    max_lines = None if args.max_lines == 0 else int(args.max_lines)
    _wrap_print(text_out, width=int(args.wrap), max_lines=max_lines)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())