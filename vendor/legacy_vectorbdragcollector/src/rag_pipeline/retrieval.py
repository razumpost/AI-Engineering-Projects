# File: src/rag_pipeline/retrieval.py
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .db import get_engine


@dataclass
class RetrievalConfig:
    """Retrieval knobs."""

    only_source: Optional[str] = None
    limit: int = 6
    extra_where: Optional[str] = None
    extra_params: Dict[str, Any] = field(default_factory=dict)


def _as_pgvector_literal(vec: Sequence[float]) -> str:
    # pgvector accepts: '[1,2,3]' (as text cast to vector)
    return "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"


def _cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    import math

    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        xf = float(x)
        yf = float(y)
        dot += xf * yf
        na += xf * xf
        nb += yf * yf
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    return dot / (math.sqrt(na) * math.sqrt(nb))


def search(
    query_embedding: Sequence[float],
    *,
    cfg: Optional[RetrievalConfig] = None,
    engine: Optional[Engine] = None,
) -> List[Dict[str, Any]]:
    """Return top chunks for a query embedding.

    Uses:
      - PostgreSQL + pgvector (distance in SQL)
      - SQLite (scores in Python, embeddings stored as JSON text)

    Expected tables:
      rag_documents, rag_chunks, rag_embeddings
    """
    cfg = cfg or RetrievalConfig()
    engine = engine or get_engine()

    if cfg.limit <= 0:
        return []

    dialect = engine.dialect.name

    if dialect == "postgresql":
        where_parts = ["1=1"]
        params: Dict[str, Any] = {"k": int(cfg.limit)}

        if cfg.only_source:
            where_parts.append("d.source = :only_source")
            params["only_source"] = cfg.only_source

        if cfg.extra_where:
            where_parts.append(f"({cfg.extra_where})")
            params.update(cfg.extra_params or {})

        where_sql = " AND ".join(where_parts)
        params["qvec"] = _as_pgvector_literal(query_embedding)

        sql = text(
            f"""
            SELECT
                c.id AS chunk_id,
                c.document_id AS document_id,
                c.chunk_index AS chunk_index,
                c.content AS content,
                c.meta AS chunk_meta,
                d.source AS source,
                d.source_id AS source_id,
                d.title AS title,
                d.url AS url,
                d.meta AS doc_meta,
                (e.embedding <-> cast(:qvec as vector)) AS distance
            FROM rag_embeddings e
            JOIN rag_chunks c ON c.id = e.chunk_id
            JOIN rag_documents d ON d.id = c.document_id
            WHERE {where_sql}
            ORDER BY e.embedding <-> cast(:qvec as vector) ASC
            LIMIT :k
            """
        )

        with engine.connect() as conn:
            rows = conn.execute(sql, params).mappings().all()

        out: List[Dict[str, Any]] = []
        for r in rows:
            dist = float(r.get("distance", 0.0) or 0.0)
            score = 1.0 / (1.0 + dist)
            out.append(
                {
                    "score": score,
                    "distance": dist,
                    "chunk_id": r["chunk_id"],
                    "document_id": r["document_id"],
                    "chunk_index": r["chunk_index"],
                    "content": r["content"],
                    "source": r.get("source"),
                    "source_id": r.get("source_id"),
                    "title": r.get("title"),
                    "url": r.get("url"),
                    "chunk_meta": r.get("chunk_meta"),
                    "doc_meta": r.get("doc_meta"),
                }
            )
        return out

    if dialect == "sqlite":
        where_parts = ["1=1"]
        params: Dict[str, Any] = {}

        if cfg.only_source:
            where_parts.append("d.source = :only_source")
            params["only_source"] = cfg.only_source

        if cfg.extra_where:
            where_parts.append(f"({cfg.extra_where})")
            params.update(cfg.extra_params or {})

        where_sql = " AND ".join(where_parts)

        sql = text(
            f"""
            SELECT
                c.id AS chunk_id,
                c.document_id AS document_id,
                c.chunk_index AS chunk_index,
                c.content AS content,
                c.meta AS chunk_meta,
                d.source AS source,
                d.source_id AS source_id,
                d.title AS title,
                d.url AS url,
                d.meta AS doc_meta,
                e.embedding AS embedding
            FROM rag_embeddings e
            JOIN rag_chunks c ON c.id = e.chunk_id
            JOIN rag_documents d ON d.id = c.document_id
            WHERE {where_sql}
            """
        )

        with engine.connect() as conn:
            rows = conn.execute(sql, params).mappings().all()

        q = [float(x) for x in query_embedding]
        scored: List[Tuple[float, Dict[str, Any]]] = []
        for r in rows:
            try:
                emb = json.loads(r["embedding"])
            except Exception:
                continue
            s = _cosine_similarity(q, emb)
            scored.append((s, r))

        scored.sort(key=lambda x: x[0], reverse=True)
        out: List[Dict[str, Any]] = []
        for score, r in scored[: cfg.limit]:
            out.append(
                {
                    "score": float(score),
                    "chunk_id": r["chunk_id"],
                    "document_id": r["document_id"],
                    "chunk_index": r["chunk_index"],
                    "content": r["content"],
                    "source": r.get("source"),
                    "source_id": r.get("source_id"),
                    "title": r.get("title"),
                    "url": r.get("url"),
                    "chunk_meta": r.get("chunk_meta"),
                    "doc_meta": r.get("doc_meta"),
                }
            )
        return out

    raise RuntimeError(f"Unsupported DB dialect for retrieval: {dialect}")
