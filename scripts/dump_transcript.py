# scripts/dump_transcript.py
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _collector_root(system_root: str) -> Path | None:
    if not system_root:
        return None
    p = Path(system_root).expanduser().resolve()
    # SYSTEM_ROOT_DIRECTORY обычно .../VectorBDRAGcollector_/.cognee_system
    return p.parent if p.name.startswith(".cognee") else p


def _candidate_paths(rel: str) -> list[str]:
    rel = (rel or "").strip()
    if not rel:
        return []
    out = [rel]
    if "calls_transcripts_dev" in rel:
        out.append(rel.replace("calls_transcripts_dev", "calls_transcripts"))
    return list(dict.fromkeys(out))


def _read_file(p: Path, head_lines: int) -> str:
    lines = []
    with p.open("r", encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            lines.append(line.rstrip("\n"))
            if i + 1 >= head_lines:
                break
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", type=int, required=True, help="rag_chunks.id")
    ap.add_argument("--head", type=int, default=120, help="Сколько строк печатать")
    ap.add_argument("--prefer", choices=["db", "file"], default="db", help="Предпочитать DB content или файл")
    args = ap.parse_args()

    db_dsn = _env("DB_DSN")
    if not db_dsn:
        raise SystemExit("DB_DSN is empty. Export it or source your .env first.")

    eng = create_engine(db_dsn, future=True, pool_pre_ping=True)

    q = text(
        """
        SELECT id, created_at,
               meta::jsonb->>'path' AS path,
               content::text AS content
        FROM rag_chunks
        WHERE id = :id
        """
    )

    with eng.connect() as c:
        row = c.execute(q, {"id": int(args.id)}).mappings().first()

    if not row:
        raise SystemExit(f"rag_chunks.id={args.id} not found")

    content = (row.get("content") or "").strip()
    rel_path = (row.get("path") or "").strip()

    print(f"id={row['id']} created_at={row.get('created_at')} path={rel_path}")

    # Decide source
    if args.prefer == "db" and content:
        print("\n--- DB content (head) ---")
        print("\n".join(content.splitlines()[: args.head]))
        return 0

    # Try file
    sys_root = _env("SYSTEM_ROOT_DIRECTORY")
    root = _collector_root(sys_root)
    tried: list[Path] = []

    for rel in _candidate_paths(rel_path):
        p = Path(rel)
        # absolute
        if p.is_absolute() and p.exists():
            print("\n--- FILE content (head) ---")
            print(_read_file(p, args.head))
            return 0
        # relative to collector root
        if root:
            cand = (root / rel).resolve()
            tried.append(cand)
            if cand.exists():
                print("\n--- FILE content (head) ---")
                print(_read_file(cand, args.head))
                return 0
        # relative to cwd
        cand2 = (Path.cwd() / rel).resolve()
        tried.append(cand2)
        if cand2.exists():
            print("\n--- FILE content (head) ---")
            print(_read_file(cand2, args.head))
            return 0

    # fallback to db if exists
    if content:
        print("\n--- DB content (fallback head) ---")
        print("\n".join(content.splitlines()[: args.head]))
        return 0

    print("\nNo content found. Tried paths:")
    for p in tried:
        print(f"  - {p}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())