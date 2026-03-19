# src/core/database.py
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

_ENGINE: Optional[Engine] = None
SessionLocal: Optional[sessionmaker] = None


def init_db(db_url: str, *, statement_timeout_ms: int = 0) -> None:
    global _ENGINE, SessionLocal

    if not db_url:
        raise ValueError("DATABASE_URL is empty")

    _ENGINE = create_engine(db_url, future=True, pool_pre_ping=True)
    SessionLocal = sessionmaker(bind=_ENGINE, autocommit=False, autoflush=False, future=True)

    if statement_timeout_ms and int(statement_timeout_ms) > 0:
        with _ENGINE.begin() as conn:
            conn.execute(text(f"set statement_timeout = {int(statement_timeout_ms)}"))


def get_engine() -> Engine:
    if _ENGINE is None:
        raise RuntimeError("DB engine is not initialized. Call init_db().")
    return _ENGINE


@contextmanager
def get_session() -> Iterator[Session]:
    if SessionLocal is None:
        raise RuntimeError("SessionLocal is not initialized. Call init_db().")
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
