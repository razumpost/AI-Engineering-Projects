# src/rag_pipeline/rag_db.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Connection


def _jsonb(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def upsert_document(
    conn: Connection,
    *,
    source: str,
    source_id: str,
    title: Optional[str],
    created_at: Optional[datetime],
    updated_at: Optional[datetime],
    meta: Dict[str, Any],
    url: Optional[str] = None,
) -> int:
    row = conn.execute(
        text(
            """
            insert into rag_documents(source, source_id, title, created_at, updated_at, meta, url)
            values (:source, :sid, :title, :created_at, :updated_at, cast(:meta as jsonb), :url)
            on conflict (source, source_id) do update set
              title = excluded.title,
              created_at = excluded.created_at,
              updated_at = excluded.updated_at,
              meta = excluded.meta,
              url = excluded.url
            returning id;
            """
        ),
        {
            "source": source,
            "sid": str(source_id),
            "title": title,
            "created_at": created_at,
            "updated_at": updated_at,
            "meta": _jsonb(meta or {}),
            "url": url,
        },
    ).mappings().first()
    return int(row["id"])


def delete_doc_chunks(conn: Connection, *, doc_id: int) -> None:
    # Embeddings usually FK -> chunks; delete embeddings first to be safe.
    conn.execute(
        text(
            """
            delete from rag_embeddings
            where chunk_id in (select id from rag_chunks where document_id = :doc_id)
            """
        ),
        {"doc_id": int(doc_id)},
    )
    conn.execute(text("delete from rag_chunks where document_id = :doc_id"), {"doc_id": int(doc_id)})


def upsert_chunk(conn: Connection, *, doc_id: int, chunk_index: int, content: str, meta: Dict[str, Any]) -> int:
    row = conn.execute(
        text(
            """
            insert into rag_chunks(document_id, chunk_index, content, meta)
            values (:doc_id, :chunk_index, :content, cast(:meta as jsonb))
            on conflict (document_id, chunk_index) do update set
              content = excluded.content,
              meta = excluded.meta
            returning id;
            """
        ),
        {"doc_id": int(doc_id), "chunk_index": int(chunk_index), "content": content, "meta": _jsonb(meta or {})},
    ).mappings().first()
    return int(row["id"])


def upsert_embedding(conn: Connection, *, chunk_id: int, vec_str: str) -> None:
    conn.execute(
        text(
            """
            insert into rag_embeddings(chunk_id, embedding)
            values (:chunk_id, cast(:vec as vector))
            on conflict (chunk_id) do update set embedding = excluded.embedding
            """
        ),
        {"chunk_id": int(chunk_id), "vec": vec_str},
    )


def vec_to_pgvector(vec: Sequence[float]) -> str:
    return "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"