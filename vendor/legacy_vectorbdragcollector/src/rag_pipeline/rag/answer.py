# =========================
# FILE: src/rag_pipeline/rag/answer.py
# =========================
from __future__ import annotations

import os
from typing import Any, Dict, List

from .prompting import build_messages, format_sources
from .yandex_gpt_client import YandexGPTClient
from .redaction import RedactionConfig, redact_text


def rag_answer(question: str, retriever, top_k: int = 12) -> str:
    q = (question or "").strip()
    if not q:
        return "Пустой вопрос."

    # 1) Retrieve
    hits: List[Dict[str, Any]] = retriever.search(q, top_k=top_k)

    # 2) Trim
    max_ctx = int(os.getenv("RAG_MAX_CONTEXT_CHARS", "18000"))
    hits = format_sources(hits, max_chars=max_ctx)

    # 3) Build messages
    messages = build_messages(q, hits)

    # 4) Redaction (before any external LLM call)
    if os.getenv("RAG_REDACT", "").strip() == "1":
        cfg = RedactionConfig()
        for h in hits:
            t = h.get("text") or ""
            rt, _ = redact_text(t, cfg=cfg)
            h["text"] = rt

    # 6) DRY_RUN
    if os.getenv("RAG_DRY_RUN", "").strip() == "1":
        return "DRY_RUN: YaGPT is disabled.\n\n--- SOURCES ---\n" + "\n".join(
            [f"SRC={h.get('source')}:{h.get('source_id')} chunk={h.get('chunk_id')}" for h in hits]
        )

    # 7) LLM call
    y = YandexGPTClient()
    return y.chat(messages)
