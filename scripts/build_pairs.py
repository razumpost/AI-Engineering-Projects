from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text


# ---------------------------
# Models
# ---------------------------

@dataclass(frozen=True)
class CallChunk:
    id: int
    created_at: str | None
    deal_id: str | None
    activity_id: str | None
    path: str | None
    content: str | None
    meta: dict[str, Any]


@dataclass(frozen=True)
class SkspFile:
    deal_id: str
    task_id: int
    file_id: int
    name: str | None
    local_path: str | None


@dataclass
class PairRecord:
    deal_id: str
    activity_id: str | None
    call_chunk_ids: list[int]
    call_path: str | None
    transcript: str
    transcript_len: int
    sksp_task_id: int | None
    sksp_file_id: int | None
    sksp_name: str | None
    sksp_local_path: str | None
    sksp_abs_path: str | None
    notes: list[str]


# ---------------------------
# Helpers
# ---------------------------

_DIM_RE = re.compile(
    r"(\d{1,3}(?:[.,]\d{1,3})?)\s*(м|mm|см|cm)"
    r"(\s*[xх×]\s*(\d{1,3}(?:[.,]\d{1,3})?)\s*(м|mm|см|cm))?",
    re.IGNORECASE,
)
_TECH_WORDS = [
    "шаг", "пиксел", "pixel", "pitch", "ярк", "нит",
    "радиус", "гибк", "монтаж", "креп", "каркас",
    "контроллер", "процессор", "питание", "электр",
    "led", "светодиод", "экран",
]


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _collector_root(system_root: str) -> Path | None:
    if not system_root:
        return None
    p = Path(system_root).expanduser().resolve()
    # typical: .../VectorBDRAGcollector_/.cognee_system
    return p.parent if p.name.startswith(".cognee") else p


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def _as_dict_meta(v: Any) -> dict[str, Any]:
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            j = json.loads(v)
            return j if isinstance(j, dict) else {}
        except Exception:
            return {}
    return {}


def _merge_text(chunks: list[CallChunk]) -> str:
    parts = []
    for c in chunks:
        t = (c.content or "").strip()
        if t:
            parts.append(t)
    return "\n".join(parts).strip()


def _call_score(text_value: str) -> float:
    t = (text_value or "").casefold()
    if not t.strip():
        return 0.0
    dims = len(_DIM_RE.findall(t))
    tech = sum(1 for w in _TECH_WORDS if w in t)
    length_bonus = min(1.5, len(t) / 2500.0)
    return float(dims * 2.0 + tech * 0.35 + length_bonus)


def _pick_best_call(groups: dict[str, list[CallChunk]]) -> tuple[str | None, list[CallChunk], str, list[str]]:
    notes: list[str] = []
    best_aid = None
    best_chunks: list[CallChunk] = []
    best_text = ""
    best_score = -1.0

    for aid, chunks in groups.items():
        chunks_sorted = sorted(chunks, key=lambda x: (x.created_at or "", x.id))
        text_merged = _merge_text(chunks_sorted)
        sc = _call_score(text_merged)
        if sc > best_score:
            best_score = sc
            best_aid = aid if aid != "NO_ACTIVITY" else None
            best_chunks = chunks_sorted
            best_text = text_merged

    if best_score < 0.8:
        notes.append("weak_call_signal: transcript looks non-technical; chosen by heuristic score")
    return best_aid, best_chunks, best_text, notes


def _sksp_priority(name: str | None, local_path: str | None) -> int:
    n = (name or "").casefold()
    p = (local_path or "").casefold()
    if "сксп" in n or "sksp" in n:
        return 0
    if "sksps" in p:
        return 1
    if n.endswith(".xlsx") or n.endswith(".xls") or n.endswith(".ods"):
        return 2
    return 9


# ---------------------------
# SQL
# ---------------------------

def _get_candidate_deals(eng, limit: int) -> list[str]:
    q = text(
        """
        WITH call_deals AS (
          SELECT DISTINCT meta::jsonb->>'deal_id' AS deal_id
          FROM rag_chunks
          WHERE (meta::jsonb->>'path') ILIKE '%calls_transcripts%'
            AND (meta::jsonb->>'deal_id') IS NOT NULL
        ),
        task_deals AS (
          SELECT
            id AS task_id,
            COALESCE(
              substring(raw::text from 'D_(\\d+)'),
              substring(title from 'ID Сделки:\\s*(\\d+)'),
              substring(title from 'DI Сделки:\\s*(\\d+)')
            ) AS deal_id
          FROM tasks
          WHERE raw::text ~ 'D_\\d+' OR title ~ '(ID|DI) Сделки:\\s*\\d+'
        ),
        sksp_task_files AS (
          SELECT
            td.deal_id,
            td.task_id,
            f.id AS file_id,
            f.name,
            f.local_path
          FROM task_deals td
          JOIN task_files tf ON tf.task_id = td.task_id
          JOIN files f ON f.id = tf.file_id
          WHERE td.deal_id IS NOT NULL
            AND (
              f.name ILIKE '%сксп%'
              OR f.name ILIKE '%sksp%'
              OR f.name ILIKE '%.xlsx%'
              OR f.name ILIKE '%.xls%'
              OR f.name ILIKE '%.ods%'
              OR f.local_path ILIKE '%sksps%'
            )
        )
        SELECT s.deal_id
        FROM sksp_task_files s
        JOIN call_deals c ON c.deal_id = s.deal_id
        GROUP BY s.deal_id
        ORDER BY max(s.file_id) DESC
        LIMIT :lim
        """
    )
    with eng.connect() as c:
        rows = c.execute(q, {"lim": int(limit)}).mappings().all()
    return [str(r["deal_id"]) for r in rows if r.get("deal_id")]


def _get_calls_for_deal(eng, deal_id: str) -> list[CallChunk]:
    q = text(
        """
        SELECT
          id,
          created_at::text AS created_at,
          meta::jsonb->>'deal_id' AS deal_id,
          meta::jsonb->>'activity_id' AS activity_id,
          meta::jsonb->>'path' AS path,
          content::text AS content,
          meta::jsonb AS meta
        FROM rag_chunks
        WHERE (meta::jsonb->>'deal_id') = :deal_id
          AND (meta::jsonb->>'path') ILIKE '%calls_transcripts%'
        ORDER BY created_at ASC NULLS LAST, id ASC
        """
    )
    with eng.connect() as c:
        rows = c.execute(q, {"deal_id": deal_id}).mappings().all()

    out: list[CallChunk] = []
    for r in rows:
        out.append(
            CallChunk(
                id=int(r["id"]),
                created_at=r.get("created_at"),
                deal_id=r.get("deal_id"),
                activity_id=r.get("activity_id") or None,
                path=r.get("path") or None,
                content=r.get("content"),
                meta=_as_dict_meta(r.get("meta")),
            )
        )
    return out


def _get_sksp_files_for_deal(eng, deal_id: str) -> list[SkspFile]:
    q = text(
        """
        WITH deal_tasks AS (
          SELECT id AS task_id
          FROM tasks
          WHERE raw::text ILIKE :dtag
             OR title ILIKE :deal_like
        )
        SELECT
          :deal_id AS deal_id,
          tf.task_id,
          f.id AS file_id,
          f.name,
          f.local_path
        FROM deal_tasks dt
        JOIN task_files tf ON tf.task_id = dt.task_id
        JOIN files f ON f.id = tf.file_id
        WHERE
          f.name ILIKE '%сксп%'
          OR f.name ILIKE '%sksp%'
          OR f.name ILIKE '%.xlsx%'
          OR f.name ILIKE '%.xls%'
          OR f.name ILIKE '%.ods%'
          OR f.local_path ILIKE '%sksps%'
        ORDER BY f.id DESC
        """
    )
    with eng.connect() as c:
        rows = c.execute(
            q,
            {
                "deal_id": deal_id,
                "dtag": f'%D_{deal_id}%',
                "deal_like": f'%{deal_id}%',
            },
        ).mappings().all()

    return [
        SkspFile(
            deal_id=str(r["deal_id"]),
            task_id=int(r["task_id"]),
            file_id=int(r["file_id"]),
            name=r.get("name"),
            local_path=r.get("local_path"),
        )
        for r in rows
    ]


# ---------------------------
# Main
# ---------------------------

def build_pairs(deal_ids: list[str], prefer_sksp_name: bool, out_path: Path | None) -> int:
    dsn = _env("DB_DSN")
    if not dsn:
        raise SystemExit("DB_DSN is empty. Run: set -a; source .env; set +a")

    system_root = _env("SYSTEM_ROOT_DIRECTORY")
    collector_root = _collector_root(system_root)

    eng = create_engine(dsn, future=True, pool_pre_ping=True)

    fp = out_path.open("w", encoding="utf-8") if out_path else None
    written = 0

    try:
        for deal_id in deal_ids:
            calls = _get_calls_for_deal(eng, deal_id)
            if not calls:
                continue

            # group by activity_id
            groups: dict[str, list[CallChunk]] = {}
            for ch in calls:
                key = ch.activity_id or "NO_ACTIVITY"
                groups.setdefault(key, []).append(ch)

            activity_id, best_chunks, transcript, call_notes = _pick_best_call(groups)
            if not transcript.strip():
                continue

            # pick sksp file
            files = _get_sksp_files_for_deal(eng, deal_id)
            if not files:
                continue

            files_sorted = sorted(files, key=lambda f: (_sksp_priority(f.name, f.local_path), -f.file_id))
            sk = files_sorted[0] if prefer_sksp_name else files_sorted[0]

            sk_abs = None
            if collector_root and sk.local_path:
                p = (collector_root / sk.local_path).resolve()
                if p.exists():
                    sk_abs = str(p)
                else:
                    call_notes.append("sksp_file_missing_on_disk")

            rec = PairRecord(
                deal_id=deal_id,
                activity_id=activity_id,
                call_chunk_ids=[c.id for c in best_chunks],
                call_path=best_chunks[0].path if best_chunks else None,
                transcript=transcript,
                transcript_len=len(transcript),
                sksp_task_id=sk.task_id,
                sksp_file_id=sk.file_id,
                sksp_name=sk.name,
                sksp_local_path=sk.local_path,
                sksp_abs_path=sk_abs,
                notes=call_notes,
            )

            line = _safe_json(rec.__dict__)
            if fp:
                fp.write(line + "\n")
            else:
                print(line)
            written += 1

        return written
    finally:
        if fp:
            fp.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=60, help="How many candidate deals to process")
    ap.add_argument("--deal-id", default="", help="Process only one deal_id (optional)")
    ap.add_argument("--out", default="/tmp/pairs.jsonl", help="Output jsonl path (empty = stdout)")
    ap.add_argument("--prefer-sksp-name", action="store_true", help="Prefer files with 'СкСп/sksp' in name")
    args = ap.parse_args()

    dsn = _env("DB_DSN")
    if not dsn:
        raise SystemExit("DB_DSN is empty. Run: set -a; source .env; set +a")

    eng = create_engine(dsn, future=True, pool_pre_ping=True)

    if args.deal_id.strip():
        deal_ids = [args.deal_id.strip()]
    else:
        deal_ids = _get_candidate_deals(eng, limit=int(args.limit))

    out_path = Path(args.out).expanduser().resolve() if args.out.strip() else None
    written = build_pairs(deal_ids, prefer_sksp_name=bool(args.prefer_sksp_name), out_path=out_path)

    print(f"written={written} -> {out_path or 'stdout'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())