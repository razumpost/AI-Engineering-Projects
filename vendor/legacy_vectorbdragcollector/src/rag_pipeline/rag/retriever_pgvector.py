# File: src/rag_pipeline/rag/retriever_pgvector.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from ..embeddings import Embedder
from ..retrieval import RetrievalConfig, search as retrieval_search


class PgVectorRetriever:
    """Compatibility wrapper for older code paths (chat/answer)."""

    def __init__(self, cfg: Optional[RetrievalConfig] = None, embedder: Optional[Embedder] = None) -> None:
        self.cfg = cfg or RetrievalConfig()
        self.embedder = embedder or Embedder.from_env()

    def search(
        self,
        query: str,
        *,
        top_k: int = 12,
        doc_types: Optional[Sequence[str]] = None,
        only_sources: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        return retrieval_search(
            query,
            cfg=self.cfg,
            embedder=self.embedder,
            top_k=int(top_k),
            doc_types=doc_types,
            only_sources=only_sources,
        )
