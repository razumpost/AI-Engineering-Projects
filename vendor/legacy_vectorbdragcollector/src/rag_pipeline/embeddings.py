# src/rag_pipeline/embeddings.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional, Sequence, Union

import numpy as np


@dataclass(frozen=True)
class EmbedderConfig:
    """Embedding model configuration.

    Environment variables:
      - EMB_MODEL: model name (default: intfloat/multilingual-e5-base)
      - EMB_DEVICE: cpu/cuda (default: cpu)
      - EMB_NORMALIZE: 1/0 (default: 1)
      - EMB_BATCH_SIZE: int (default: 32)
    """

    model: str = "intfloat/multilingual-e5-base"
    device: str = "cpu"
    normalize: bool = True
    batch_size: int = 32


Vector = Union[Sequence[float], np.ndarray]


class Embedder:
    def __init__(self, config: Optional[EmbedderConfig] = None):
        self.config = config or EmbedderConfig()
        from sentence_transformers import SentenceTransformer

        self.model = SentenceTransformer(self.config.model, device=self.config.device)

    @staticmethod
    def from_env() -> "Embedder":
        model = os.getenv("EMB_MODEL") or os.getenv("EMBEDDING_MODEL") or "intfloat/multilingual-e5-base"
        device = os.getenv("EMB_DEVICE") or "cpu"
        normalize = (os.getenv("EMB_NORMALIZE") or "1").strip().lower() not in ("0", "false", "no")
        batch_size = int(os.getenv("EMB_BATCH_SIZE") or 32)
        return Embedder(EmbedderConfig(model=model, device=device, normalize=normalize, batch_size=batch_size))

    def _e5_prefixed(self, text: str, *, is_query: bool) -> str:
        t = (text or "").strip()
        if not t:
            return t
        m = (self.config.model or "").lower()
        if "e5" not in m:
            return t
        low = t.lower()
        if low.startswith("query:") or low.startswith("passage:"):
            return t
        return ("query: " + t) if is_query else ("passage: " + t)

    def embed_texts(self, texts: List[str], *, is_query: bool = False) -> List[List[float]]:
        prepared = [self._e5_prefixed(t, is_query=is_query) for t in (texts or [])]
        vecs = self.model.encode(
            prepared,
            batch_size=self.config.batch_size,
            show_progress_bar=False,
            normalize_embeddings=self.config.normalize,
        )
        if isinstance(vecs, np.ndarray):
            return vecs.astype(float).tolist()
        return [list(map(float, v)) for v in vecs]