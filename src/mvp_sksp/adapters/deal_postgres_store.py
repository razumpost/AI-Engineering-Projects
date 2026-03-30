from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class DealTask:
    task_id: int
    title: str


@dataclass(frozen=True)
class DealFile:
    file_id: int
    name: str
    local_path: str


@dataclass(frozen=True)
class TranscriptChunk:
    chunk_id: int
    created_at: str | None
    activity_id: str | None
    path: str | None
    content: str


class PostgresDealStore:
    """Postgres access layer for deal-scoped data.

    Tables: tasks, task_files, files, rag_chunks.
    """

    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = (dsn or os.getenv("DB_DSN") or os.getenv("DATABASE_URL") or "").strip()
        if not self._dsn:
            raise RuntimeError("DB_DSN/DATABASE_URL is empty. Run: set -a; source .env; set +a")
        self._engine: Engine = create_engine(self._dsn, future=True, pool_pre_ping=True)

    @property
    def engine(self) -> Engine:
        return self._engine

    def get_tasks_for_deal(self, deal_id: str) -> list[DealTask]:
        q = text(
            """
            SELECT id, title
            FROM tasks
            WHERE raw::text ILIKE :dtag
               OR title ILIKE :dlike
            ORDER BY id DESC
            """
        )
        with self._engine.connect() as c:
            rows = c.execute(q, {"dtag": f"%D_{deal_id}%", "dlike": f"%{deal_id}%"}).mappings().all()

        out: list[DealTask] = []
        for r in rows:
            try:
                tid = int(r["id"])
            except Exception:
                continue
            out.append(DealTask(task_id=tid, title=str(r.get("title") or f"Task {tid}")))
        return out

    def get_sksp_files_for_deal(self, deal_id: str) -> list[DealFile]:
        q = text(
            """
            WITH deal_tasks AS (
              SELECT id AS task_id
              FROM tasks
              WHERE raw::text ILIKE :dtag
                 OR title ILIKE :dlike
            )
            SELECT f.id AS file_id, f.name, f.local_path
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
        with self._engine.connect() as c:
            rows = c.execute(
                q,
                {"dtag": f"%D_{deal_id}%", "dlike": f"%{deal_id}%"},
            ).mappings().all()

        out: list[DealFile] = []
        for r in rows:
            try:
                fid = int(r["file_id"])
            except Exception:
                continue
            out.append(
                DealFile(
                    file_id=fid,
                    name=str(r.get("name") or ""),
                    local_path=str(r.get("local_path") or ""),
                )
            )
        return out

    def get_transcript_chunks_for_deal(self, deal_id: str) -> list[TranscriptChunk]:
        q = text(
            """
            SELECT
              id,
              created_at::text AS created_at,
              meta::jsonb->>'activity_id' AS activity_id,
              meta::jsonb->>'path' AS path,
              content::text AS content
            FROM rag_chunks
            WHERE meta::jsonb->>'deal_id' = :deal_id
              AND (meta::jsonb->>'path') ILIKE '%calls_transcripts%'
            ORDER BY created_at ASC NULLS LAST, id ASC
            """
        )
        with self._engine.connect() as c:
            rows = c.execute(q, {"deal_id": str(deal_id)}).mappings().all()

        out: list[TranscriptChunk] = []
        for r in rows:
            out.append(
                TranscriptChunk(
                    chunk_id=int(r["id"]),
                    created_at=r.get("created_at"),
                    activity_id=r.get("activity_id"),
                    path=r.get("path"),
                    content=str(r.get("content") or ""),
                )
            )
        return out

    def get_best_transcript_for_deal(self, deal_id: str, *, activity_id: str | None = None) -> tuple[str, dict[str, Any]]:
        chunks = self.get_transcript_chunks_for_deal(deal_id)
        if not chunks:
            return "", {"deal_id": deal_id, "activity_id": None, "chunk_ids": []}

        groups: dict[str, list[TranscriptChunk]] = {}
        for ch in chunks:
            if activity_id and (ch.activity_id or "") != activity_id:
                continue
            key = ch.activity_id or "NO_ACTIVITY"
            groups.setdefault(key, []).append(ch)

        if not groups:
            return "", {"deal_id": deal_id, "activity_id": activity_id, "chunk_ids": []}

        best_key = None
        best_text = ""
        best_ids: list[int] = []

        for key, grp in groups.items():
            text_merged = "\n".join((g.content or "").strip() for g in grp if (g.content or "").strip()).strip()
            if len(text_merged) > len(best_text):
                best_text = text_merged
                best_key = key
                best_ids = [g.chunk_id for g in grp]

        meta = {"deal_id": deal_id, "activity_id": None if best_key == "NO_ACTIVITY" else best_key, "chunk_ids": best_ids}
        return best_text, meta