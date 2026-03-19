# src/rag_pipeline/chunker.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class ChunkingConfig:
    chunk_size: int = 1200
    chunk_overlap: int = 200


def chunk_text(text: str, cfg: ChunkingConfig) -> List[str]:
    """
    Simple character-based chunking with overlap.
    Good enough for v1; later можно заменить на токенизацию.
    """
    s = (text or "").strip()
    if not s:
        return []

    size = max(1, int(cfg.chunk_size))
    overlap = max(0, int(cfg.chunk_overlap))
    if overlap >= size:
        overlap = max(0, size // 4)

    out: List[str] = []
    i = 0
    n = len(s)
    while i < n:
        j = min(n, i + size)
        out.append(s[i:j])
        if j == n:
            break
        i = max(0, j - overlap)
    return out
