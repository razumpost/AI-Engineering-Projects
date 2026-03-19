# =========================
# File: src/rag_pipeline/rag/chat.py
# =========================
from __future__ import annotations

import os
import sys
from pathlib import Path

from .answer import rag_answer
from .retriever_pgvector import PgVectorRetriever


def _load_dotenv(path: str = ".env") -> None:
    """
    Minimal .env loader (no extra deps).
    Loads only KEY=VALUE lines; does not override existing env vars.
    """
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def main() -> None:
    _load_dotenv()

    q = " ".join(sys.argv[1:]).strip()
    if not q:
        print("Usage: python -m src.rag_pipeline.rag.chat <question>")
        raise SystemExit(2)

    retriever = PgVectorRetriever()
    res = rag_answer(q, retriever, top_k=int(os.getenv("RAG_TOP_K", "12")))

    print(res["answer"])
    print("\n--- SOURCES ---")
    for c in res["contexts"]:
        print(
            f"SRC={c.get('src')} chunk={c.get('chunk')} "
            f"doc_type={c.get('doc_type')} dist={c.get('dist')} score={c.get('score')}"
        )


if __name__ == "__main__":
    main()