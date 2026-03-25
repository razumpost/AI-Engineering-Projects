# scripts/export_deal_transcripts.py
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class ChunkRow:
    id: int
    created_at: str | None
    activity_id: str | None
    deal_id: str | None
    path: str | None
    content: str | None
    meta: dict[str, Any]


@dataclass
class TranscriptGroup:
    deal_id: str | None
    activity_id: str | None
    path: str | None
    chunk_ids: list[int]
    created_at_min: str | None
    created_at_max: str | None
    text: str


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _collector_root(system_root: str) -> Path | None:
    if not system_root:
        return None
    p = Path(system_root).expanduser().resolve()
    # typical: .../VectorBDRAGcollector_/.cognee_system
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


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def _as_dict_meta(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            v = json.loads(raw)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


def _chunk_sort_key(row: ChunkRow) -> tuple:
    """
    Best-effort stable ordering for chunks.

    Prefer known keys in meta:
      chunk_index / part / seq / rec_index / offset / start_ms / start
    Fallback:
      created_at, id
    """
    m = row.meta or {}

    def _int_or_none(v: Any) -> int | None:
        try:
            if v is None:
                return None
            if isinstance(v, bool):
                return None
            return int(str(v).strip())
        except Exception:
            return None

    candidates = [
        _int_or_none(m.get("chunk_index")),
        _int_or_none(m.get("part")),
        _int_or_none(m.get("seq")),
        _int_or_none(m.get("rec_index")),
        _int_or_none(m.get("offset")),
        _int_or_none(m.get("start_ms")),
        _int_or_none(m.get("start")),
    ]
    # pick the first defined numeric key
    idx = next((c for c in candidates if c is not None), None)

    # created_at is a string; still ok for ordering within one batch
    return (
        0 if idx is not None else 1,
        idx if idx is not None else 0,
        row.created_at or "",
        row.id,
    )


def _merge_text(chunks: list[ChunkRow]) -> str:
    parts = []
    for c in chunks:
        t = (c.content or "").strip()
        if t:
            parts.append(t)
    # keep deterministic joining
    return "\n".join(parts).strip()


def _fetch_chunks(engine: Engine, deal_id: str) -> list[ChunkRow]:
    q = text(
        """
        SELECT
          id,
          created_at::text AS created_at,
          meta::jsonb->>'activity_id' AS activity_id,
          meta::jsonb->>'deal_id' AS deal_id,
          meta::jsonb->>'path' AS path,
          content::text AS content,
          meta::jsonb AS meta
        FROM rag_chunks
        WHERE meta::jsonb->>'deal_id' = :deal_id
        ORDER BY created_at ASC NULLS LAST, id ASC
        """
    )
    with engine.connect() as c:
        rows = c.execute(q, {"deal_id": deal_id}).mappings().all()

    out: list[ChunkRow] = []
    for r in rows:
        out.append(
            ChunkRow(
                id=int(r["id"]),
                created_at=r.get("created_at"),
                activity_id=(r.get("activity_id") or None),
                deal_id=(r.get("deal_id") or None),
                path=(r.get("path") or None),
                content=r.get("content"),
                meta=_as_dict_meta(r.get("meta")),
            )
        )
    return out


def _group_chunks(chunks: list[ChunkRow]) -> dict[str, list[ChunkRow]]:
    groups: dict[str, list[ChunkRow]] = {}
    for ch in chunks:
        key = ch.activity_id or ch.path or f"chunk:{ch.id}"
        groups.setdefault(key, []).append(ch)
    return groups


def _build_groups(
    chunks: list[ChunkRow],
    collector_root: Path | None,
    prefer_source: str,
) -> list[TranscriptGroup]:
    groups_map = _group_chunks(chunks)
    out: list[TranscriptGroup] = []

    for key, grp in groups_map.items():
        grp_sorted = sorted(grp, key=_chunk_sort_key)
        deal_id = grp_sorted[0].deal_id
        activity_id = grp_sorted[0].activity_id
        path = grp_sorted[0].path
        chunk_ids = [c.id for c in grp_sorted]

        created_at_vals = [c.created_at for c in grp_sorted if c.created_at]
        created_at_min = min(created_at_vals) if created_at_vals else None
        created_at_max = max(created_at_vals) if created_at_vals else None

        db_text = _merge_text(grp_sorted)
        file_text = _resolve_file_text(path, collector_root)

        if prefer_source == "file":
            final_text = (file_text or "").strip() or db_text
        else:
            final_text = db_text or ((file_text or "").strip())

        out.append(
            TranscriptGroup(
                deal_id=deal_id,
                activity_id=activity_id,
                path=path,
                chunk_ids=chunk_ids,
                created_at_min=created_at_min,
                created_at_max=created_at_max,
                text=final_text.strip(),
            )
        )

    # stable ordering: by activity_id/path then timestamps
    out.sort(key=lambda g: ((g.activity_id or ""), (g.path or ""), (g.created_at_min or "")))
    return out


def _write_jsonl(groups: list[TranscriptGroup], out_path: Path | None) -> None:
    fp = out_path.open("w", encoding="utf-8") if out_path else None
    try:
        for g in groups:
            rec = {
                "deal_id": g.deal_id,
                "activity_id": g.activity_id,
                "path": g.path,
                "chunk_ids": g.chunk_ids,
                "created_at_min": g.created_at_min,
                "created_at_max": g.created_at_max,
                "text": g.text,
                "text_len": len(g.text or ""),
            }
            line = _safe_json(rec)
            if fp:
                fp.write(line + "\n")
            else:
                print(line)
    finally:
        if fp:
            fp.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--deal-id", required=True, help="meta->>'deal_id' (например 15062)")
    ap.add_argument("--out", default="", help="Путь для JSONL. Если пусто — печатаем в stdout.")
    ap.add_argument("--prefer", choices=["db", "file"], default="db", help="Предпочтение источника текста.")
    ap.add_argument("--min-len", type=int, default=1, help="Фильтр: пропускать тексты короче N символов.")
    ap.add_argument("--print-stats", action="store_true", help="Печатать статистику по группам.")

    args = ap.parse_args()

    dsn = _env("DB_DSN")
    if not dsn:
        print("DB_DSN is empty. source .env first.", file=os.sys.stderr)
        return 2

    system_root = _env("SYSTEM_ROOT_DIRECTORY")
    collector_root = _collector_root(system_root)

    eng = create_engine(dsn, future=True, pool_pre_ping=True)
    chunks = _fetch_chunks(eng, args.deal_id)
    groups = _build_groups(chunks, collector_root, prefer_source=args.prefer)

    if args.min_len > 1:
        groups = [g for g in groups if len(g.text or "") >= args.min_len]

    if args.print_stats:
        print(f"deal_id={args.deal_id} chunks={len(chunks)} groups={len(groups)}")
        for g in groups[:20]:
            print(
                f"  activity_id={g.activity_id} text_len={len(g.text)} chunks={len(g.chunk_ids)} path={g.path}"
            )

    out_path = Path(args.out).expanduser().resolve() if args.out.strip() else None
    _write_jsonl(groups, out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())