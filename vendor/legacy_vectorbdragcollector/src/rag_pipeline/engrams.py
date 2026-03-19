from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Any

try:
    from sqlalchemy.engine import Engine
except Exception:  # pragma: no cover
    Engine = Any  # type: ignore

@dataclass(frozen=True)
class EngramsConfig:
    """Config for optional engrams index.

    This project originally had an experimental 'engrams' stage.
    It's not required for init-db/build/query, but we keep a minimal
    implementation so the CLI subcommand doesn't crash.
    """
    enabled: bool = False
    # Optional: create a table with n-grams for trigram search, etc.
    min_n: int = 3
    max_n: int = 5
    table_name: str = "rag_engrams"

def build_engrams(engine: Engine, config: Optional[EngramsConfig] = None, *, logger=None) -> None:
    """Best-effort/no-op engrams builder.

    If enabled=True, this function can be extended later.
    Right now it intentionally does nothing so the pipeline works out of the box.
    """
    cfg = config or EngramsConfig()
    if not cfg.enabled:
        if logger:
            logger.info("Engrams stage is disabled (no-op).")
        return
    # Placeholder for future: populate an n-gram table for lexical search.
    if logger:
        logger.warning("Engrams stage is enabled but not implemented yet; skipping.")


def lookup_engrams(text: str, n: int = 3) -> Sequence[str]:
    """
    Simple utility that returns character n-grams.
    """
    if n <= 0:
        return []
    s = (text or "").strip()
    if len(s) < n:
        return [s] if s else []
    return [s[i : i + n] for i in range(0, len(s) - n + 1)]
