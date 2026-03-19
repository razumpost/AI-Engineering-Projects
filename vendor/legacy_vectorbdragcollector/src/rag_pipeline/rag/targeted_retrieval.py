from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.rag_pipeline.embeddings import Embedder
from src.rag_pipeline.patch_intent import PatchAction, CATEGORIES


def _vec_to_pgvector(vec: List[float]) -> str:
    return "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"


@dataclass(frozen=True)
class TargetedRetrievalConfig:
    supplier_price_k: int = 22
    snapshot_k: int = 22
    ilike_terms_cap: int = 6
    require_sku_hint: bool = True


def _mk_block(r: Dict[str, Any], *, score: Optional[float]) -> Dict[str, Any]:
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


def _extract_terms(action: PatchAction, cfg: TargetedRetrievalConfig) -> List[str]:
    base = CATEGORIES.get(action.category, [])
    terms: List[str] = []
    for t in (action.must_have_terms or []):
        if t and t not in terms:
            terms.append(t)
    for t in base:
        if t and t not in terms:
            terms.append(t)
    for tok in action.query.split():
        tok = tok.strip().strip(",.;:()[]").lower()
        if len(tok) >= 4 and tok not in terms:
            terms.append(tok)
    return terms[: cfg.ilike_terms_cap]


def _run_source_query(
    *,
    source: str,
    where_any: str,
    params: Dict[str, Any],
    k: int,
    engine: Engine,
    require_sku_hint: bool,
) -> List[Dict[str, Any]]:
    sku_filter = ""
    if require_sku_hint:
        sku_filter = """
          and (
            (c.meta ? 'sku' and coalesce(c.meta->>'sku','') <> '')
            or (c.meta ? 'article' and coalesce(c.meta->>'article','') <> '')
            or c.content ilike '%Артикул:%'
            or c.content ilike '%SKU:%'
          )
        """

    sql = text(
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
        where d.source = :src
          and ({where_any})
          {sku_filter}
        order by dist asc
        limit :k
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {**params, "src": source, "k": int(k)}).mappings().all()

    out = []
    for r in rows:
        dist = float(r["dist"])
        out.append(_mk_block(r, score=1.0 / (1.0 + dist)))
    return out


def targeted_retrieval_for_action(
    *,
    action: PatchAction,
    engine: Engine,
    embedder: Embedder,
    cfg: Optional[TargetedRetrievalConfig] = None,
) -> List[Dict[str, Any]]:
    cfg = cfg or TargetedRetrievalConfig()
    q = action.query.strip()
    if not q:
        return []

    qvec = embedder.embed_texts([q], is_query=True)[0]
    qvec_lit = _vec_to_pgvector(qvec)

    terms = _extract_terms(action, cfg)
    params: Dict[str, Any] = {"qvec": qvec_lit}
    conds = []
    for i, t in enumerate(terms):
        params[f"p{i}"] = f"%{t}%"
        conds.append(f"(c.content ilike :p{i} or c.meta::text ilike :p{i} or d.title ilike :p{i})")
    where_any = " or ".join(conds) if conds else "true"

    blocks: List[Dict[str, Any]] = []
    # supplier_price can also contain SKU; keep require_sku_hint=True by default
    blocks.extend(_run_source_query(source="supplier_price", where_any=where_any, params=params, k=cfg.supplier_price_k, engine=engine, require_sku_hint=cfg.require_sku_hint))
    blocks.extend(_run_source_query(source="sksp_item_snapshot", where_any=where_any, params=params, k=cfg.snapshot_k, engine=engine, require_sku_hint=cfg.require_sku_hint))
    return blocks


def targeted_retrieval_for_intent(
    *,
    actions: List[PatchAction],
    engine: Engine,
    embedder: Embedder,
    cfg: Optional[TargetedRetrievalConfig] = None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for a in actions:
        if a.action.lower() not in ("add", "replace", "update"):
            continue
        out.extend(targeted_retrieval_for_action(action=a, engine=engine, embedder=embedder, cfg=cfg))
    return out
