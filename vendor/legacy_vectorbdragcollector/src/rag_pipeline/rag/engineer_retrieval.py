from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.rag_pipeline.db import get_engine
from src.rag_pipeline.embeddings import Embedder


def _norm(s: Any) -> str:
    return " ".join(str(s or "").replace("\u00a0", " ").split()).strip().casefold()


def _vec_to_pgvector(vec: List[float]) -> str:
    return "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"


def _dedup_blocks(blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for b in blocks:
        cm = b.get("chunk_meta") or {}
        art = _norm(cm.get("article") or cm.get("sku") or "")
        price = _norm(cm.get("unit_price") or cm.get("price_rub") or cm.get("price") or "")
        txt = _norm(b.get("text") or "")
        key = (art, price) if (art or price) else (txt, "")
        if key in seen and any(key):
            continue
        seen.add(key)
        out.append(b)
    return out


@dataclass(frozen=True)
class EngineerRetrievalConfig:
    task_source: str = "task"
    top_tasks: int = 30

    per_task_snapshot_rows: int = 120
    per_file_row_cap: int = 12

    global_snapshot_fallback_rows: int = 60
    global_item_fallback_rows: int = 10  # legacy last resort

    keyword_boost_rows: int = 40
    keyword_boost_max_terms: int = 6

    dedup: bool = True


_BOOST_PATTERNS: List[Tuple[str, Sequence[str]]] = [
    ("mount", ("кронштейн", "креплен", "рама", "стойк", "мобильн", "настенн", "откидн", "creawall", "crea wall")),
    ("controller", ("контроллер", "процессор", "видеопроцессор", "novastar", "vx", "lvp", "controller")),
    ("extender", ("удлинител", "hdbaset", "extender", "hdmi over", "передатчик", "приемник", "tx", "rx", "hdmi")),
    ("cabling", ("кабел", "hdmi", "dp", "usb", "poe", "switch", "коммутатор")),
]


def _extract_boost_terms(query_text: str) -> List[Sequence[str]]:
    q = _norm(query_text)
    terms: List[Sequence[str]] = []
    for _, pats in _BOOST_PATTERNS:
        if any(p in q for p in pats):
            terms.append(pats)
    return terms


def _row_to_block(r: Dict[str, Any], *, score: Optional[float]) -> Dict[str, Any]:
    return {
        "src": r.get("source"),
        "chunk_id": int(r["chunk_id"]),
        "doc_id": int(r["document_id"]),
        "doc_title": str(r.get("title") or ""),
        "doc_meta": r.get("doc_meta") or {},
        "chunk_meta": r.get("chunk_meta") or {},
        "text": str(r.get("content") or ""),
        "score": score,
    }


def retrieve_engineering_context(
    query_text: str,
    *,
    engine: Optional[Engine] = None,
    embedder: Optional[Embedder] = None,
    cfg: Optional[EngineerRetrievalConfig] = None,
) -> Dict[str, Any]:
    engine = engine or get_engine()
    embedder = embedder or Embedder.from_env()
    cfg = cfg or EngineerRetrievalConfig()

    qvec = embedder.embed_texts([query_text], is_query=True)[0]
    qvec_lit = _vec_to_pgvector(qvec)

    # 1) rank tasks
    with engine.connect() as conn:
        task_rows = conn.execute(
            text(
                """
                with hits as (
                  select
                    d.id as doc_id,
                    d.source_id as task_id,
                    min(e.embedding <-> cast(:qvec as vector)) as dist
                  from rag_embeddings e
                  join rag_chunks c on c.id = e.chunk_id
                  join rag_documents d on d.id = c.document_id
                  where d.source = :task_source
                  group by d.id, d.source_id
                  order by dist asc
                  limit :k
                )
                select doc_id, task_id, dist from hits order by dist asc
                """
            ),
            {"qvec": qvec_lit, "k": int(cfg.top_tasks), "task_source": cfg.task_source},
        ).mappings().all()

    task_ids: List[int] = []
    for r in task_rows:
        try:
            task_ids.append(int(r["task_id"]))
        except Exception:
            pass

    debug = {"task_source": cfg.task_source, "task_ids": task_ids, "task_rows": [dict(r) for r in task_rows]}
    blocks: List[Dict[str, Any]] = []

    # 2) snapshots for those tasks
    if task_ids:
        with engine.connect() as conn:
            snap_rows = conn.execute(
                text(
                    """
                    select
                      c.id as chunk_id,
                      c.document_id,
                      c.chunk_index,
                      c.content,
                      c.meta as chunk_meta,
                      d.source,
                      d.title,
                      d.meta as doc_meta,
                      d.updated_at
                    from rag_documents d
                    join rag_chunks c on c.document_id = d.id
                    where d.source='sksp_item_snapshot'
                      and (d.meta->>'task_id')::bigint = any(:task_ids)
                    order by
                      (d.meta->>'task_id')::bigint asc,
                      d.updated_at desc nulls last,
                      c.chunk_index asc
                    """
                ),
                {"task_ids": task_ids},
            ).mappings().all()

        per_doc_cnt: Dict[int, int] = {}
        per_task_cnt: Dict[int, int] = {}

        for r in snap_rows:
            dm = r.get("doc_meta") or {}
            try:
                tid = int(dm.get("task_id"))
            except Exception:
                continue

            if per_task_cnt.get(tid, 0) >= cfg.per_task_snapshot_rows:
                continue

            doc_id = int(r["document_id"])
            if per_doc_cnt.get(doc_id, 0) >= cfg.per_file_row_cap:
                continue

            per_task_cnt[tid] = per_task_cnt.get(tid, 0) + 1
            per_doc_cnt[doc_id] = per_doc_cnt.get(doc_id, 0) + 1
            blocks.append(_row_to_block(r, score=None))

    # 3) keyword boost from snapshots: search in content OR meta::text (SKU/desc often sits in meta)
    boost_terms = _extract_boost_terms(query_text)[: cfg.keyword_boost_max_terms]
    if cfg.keyword_boost_rows and boost_terms:
        with engine.connect() as conn:
            for pats in boost_terms:
                conds = []
                params = {"qvec": qvec_lit, "k": int(cfg.keyword_boost_rows)}
                for i, p in enumerate(pats):
                    params[f"p{i}"] = f"%{p}%"
                    conds.append(f"(c.content ilike :p{i} or c.meta::text ilike :p{i} or d.title ilike :p{i})")
                where_any = " or ".join(conds)

                rows = conn.execute(
                    text(
                        f"""
                        select
                          c.id as chunk_id,
                          c.document_id,
                          c.chunk_index,
                          c.content,
                          c.meta as chunk_meta,
                          d.source,
                          d.title,
                          d.meta as doc_meta,
                          (e.embedding <-> cast(:qvec as vector)) as dist
                        from rag_embeddings e
                        join rag_chunks c on c.id = e.chunk_id
                        join rag_documents d on d.id = c.document_id
                        where d.source='sksp_item_snapshot'
                          and ({where_any})
                        order by dist asc
                        limit :k
                        """
                    ),
                    params,
                ).mappings().all()

                for r in rows:
                    dist = float(r["dist"])
                    blocks.append(_row_to_block(r, score=1.0 / (1.0 + dist)))

    # 4) global snapshot fallback
    if cfg.global_snapshot_fallback_rows and len(blocks) < 90:
        with engine.connect() as conn:
            extra = conn.execute(
                text(
                    """
                    select
                      c.id as chunk_id,
                      c.document_id,
                      c.chunk_index,
                      c.content,
                      c.meta as chunk_meta,
                      d.source,
                      d.title,
                      d.meta as doc_meta,
                      (e.embedding <-> cast(:qvec as vector)) as dist
                    from rag_embeddings e
                    join rag_chunks c on c.id = e.chunk_id
                    join rag_documents d on d.id = c.document_id
                    where d.source='sksp_item_snapshot'
                    order by dist asc
                    limit :k
                    """
                ),
                {"qvec": qvec_lit, "k": int(cfg.global_snapshot_fallback_rows)},
            ).mappings().all()

        for r in extra:
            dist = float(r["dist"])
            blocks.append(_row_to_block(r, score=1.0 / (1.0 + dist)))

    # 5) last resort legacy
    if cfg.global_item_fallback_rows and len(blocks) < 40:
        with engine.connect() as conn:
            extra = conn.execute(
                text(
                    """
                    select
                      c.id as chunk_id,
                      c.document_id,
                      c.chunk_index,
                      c.content,
                      c.meta as chunk_meta,
                      d.source,
                      d.title,
                      d.meta as doc_meta,
                      (e.embedding <-> cast(:qvec as vector)) as dist
                    from rag_embeddings e
                    join rag_chunks c on c.id = e.chunk_id
                    join rag_documents d on d.id = c.document_id
                    where d.source='sksp_item'
                    order by dist asc
                    limit :k
                    """
                ),
                {"qvec": qvec_lit, "k": int(cfg.global_item_fallback_rows)},
            ).mappings().all()
        for r in extra:
            dist = float(r["dist"])
            blocks.append(_row_to_block(r, score=1.0 / (1.0 + dist)))

    if cfg.dedup:
        blocks = _dedup_blocks(blocks)

    return {"context_blocks": blocks, "debug": debug}
