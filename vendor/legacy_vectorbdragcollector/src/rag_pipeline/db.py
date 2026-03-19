# File: src/rag_pipeline/db.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Union

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class DBConfig:
    database_url: str


CfgLike = Optional[Union[DBConfig, str]]


def get_db_config(cfg: CfgLike = None) -> DBConfig:
    """Resolve DB config.

    - None -> DATABASE_URL env var, fallback to sqlite:///rag.db
    - str  -> treated as database_url
    - DBConfig -> returned as-is
    """
    if cfg is None:
        url = os.getenv("DATABASE_URL", "").strip()
        if not url:
            url = "sqlite:///rag.db"
        return DBConfig(database_url=url)

    if isinstance(cfg, DBConfig):
        return cfg

    if isinstance(cfg, str):
        url = cfg.strip()
        if not url:
            raise RuntimeError("database_url is empty")
        return DBConfig(database_url=url)

    raise TypeError(f"Unsupported cfg type: {type(cfg)}")


def get_engine(cfg: CfgLike = None) -> Engine:
    dbc = get_db_config(cfg)
    return create_engine(dbc.database_url, future=True, pool_pre_ping=True)


def ensure_statement_timeout(conn, ms: int) -> None:
    """Best-effort statement timeout (PostgreSQL only)."""
    try:
        if ms and ms > 0 and conn.dialect.name == "postgresql":
            conn.execute(text("set statement_timeout = :ms"), {"ms": int(ms)})
    except Exception:
        return


def init_rag_schema(engine: Engine, *, embedding_dim: Optional[int] = None) -> None:
    """Create (if missing) RAG tables used by src.rag_pipeline.

    Schema aligned with src/rag_pipeline/documents_builder.py:

      rag_documents(
        id, source, source_id, title, url, created_at, updated_at, meta
      )

      rag_chunks(
        id, document_id, chunk_index, content, meta
      )

      rag_embeddings(
        chunk_id, embedding
      )

    PostgreSQL uses JSONB + pgvector.
    SQLite stores JSON/meta/embedding as TEXT.
    """
    if embedding_dim is None:
        embedding_dim = int(os.getenv("EMBEDDING_DIM", "768"))

    dialect = engine.dialect.name

    with engine.begin() as conn:
        if dialect == "postgresql":
            # Extensions (best-effort; may require privileges)
            for ext in ("vector", "pg_trgm"):
                try:
                    conn.execute(text(f"CREATE EXTENSION IF NOT EXISTS {ext}"))  # nosec - ext is hardcoded
                except Exception:
                    pass

            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS rag_documents (
                        id BIGSERIAL PRIMARY KEY,
                        source TEXT NOT NULL,
                        source_id TEXT NOT NULL,
                        title TEXT,
                        url TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        meta JSONB NOT NULL DEFAULT '{}'::jsonb,
                        UNIQUE (source, source_id)
                    );
                    """
                )
            )

            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS rag_chunks (
                        id BIGSERIAL PRIMARY KEY,
                        document_id BIGINT NOT NULL REFERENCES rag_documents(id) ON DELETE CASCADE,
                        chunk_index INTEGER NOT NULL,
                        content TEXT NOT NULL,
                        meta JSONB NOT NULL DEFAULT '{}'::jsonb,
                        UNIQUE (document_id, chunk_index)
                    );
                    """
                )
            )

            conn.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS rag_embeddings (
                        chunk_id BIGINT PRIMARY KEY REFERENCES rag_chunks(id) ON DELETE CASCADE,
                        embedding vector({embedding_dim}) NOT NULL
                    );
                    """
                )
            )

            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_rag_chunks_document_id ON rag_chunks(document_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_rag_chunks_chunk_index ON rag_chunks(chunk_index)"))

            # Optional vector index (best-effort)
            try:
                conn.execute(
                    text(
                        """
                        CREATE INDEX IF NOT EXISTS idx_rag_embeddings_ivfflat
                        ON rag_embeddings USING ivfflat (embedding vector_l2_ops)
                        WITH (lists = 100);
                        """
                    )
                )
            except Exception:
                pass

        elif dialect == "sqlite":
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS rag_documents (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source TEXT NOT NULL,
                        source_id TEXT NOT NULL,
                        title TEXT,
                        url TEXT,
                        created_at TEXT NOT NULL DEFAULT (datetime('now')),
                        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                        meta TEXT NOT NULL DEFAULT '{}',
                        UNIQUE (source, source_id)
                    );
                    """
                )
            )

            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS rag_chunks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        document_id INTEGER NOT NULL,
                        chunk_index INTEGER NOT NULL,
                        content TEXT NOT NULL,
                        meta TEXT NOT NULL DEFAULT '{}',
                        UNIQUE (document_id, chunk_index),
                        FOREIGN KEY (document_id) REFERENCES rag_documents(id) ON DELETE CASCADE
                    );
                    """
                )
            )

            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS rag_embeddings (
                        chunk_id INTEGER PRIMARY KEY,
                        embedding TEXT NOT NULL,
                        FOREIGN KEY (chunk_id) REFERENCES rag_chunks(id) ON DELETE CASCADE
                    );
                    """
                )
            )

            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_rag_chunks_document_id ON rag_chunks(document_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_rag_chunks_chunk_index ON rag_chunks(chunk_index)"))

        else:
            raise RuntimeError(f"Unsupported DB dialect for init_rag_schema: {dialect}")
