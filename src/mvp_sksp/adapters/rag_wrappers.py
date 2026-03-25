from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from decimal import Decimal
from functools import lru_cache
from typing import Any, Iterable

from ..domain.candidates import Candidate
from ..domain.spec import Evidence, EvidenceSource
from ..knowledge.loader import load_knowledge_map
from ..planning.plan_models import ClassifiedCandidate


@dataclass(frozen=True)
class RagHit:
    """Normalized retrieval hit.

    This wrapper exists to keep the rest of the pipeline deterministic while
    allowing multiple backends (vector, graph, hybrid) to supply candidates.
    """

    candidate: Candidate
    score: float
    evidence: Evidence


_PRICE_RE = re.compile(
    r"(?P<price>\d{1,3}(?:[ \u00A0]\d{3})*(?:[.,]\d{1,2})?)\s*(?P<cur>руб\.?|р\.?|₽|rub|rur|usd|\$|eur|€)?",
    re.IGNORECASE,
)


def _as_decimal(val: Any) -> Decimal | None:
    if val is None:
        return None
    if isinstance(val, Decimal):
        return val
    if isinstance(val, (int, float)):
        return Decimal(str(val))
    if isinstance(val, str):
        s = val.strip().replace("\u00A0", " ").replace(" ", "").replace(",", ".")
        if not s:
            return None
        try:
            return Decimal(s)
        except Exception:
            return None
    return None


def _extract_price(text: str) -> tuple[Decimal | None, str | None]:
    if not text:
        return None, None
    m = _PRICE_RE.search(text)
    if not m:
        return None, None
    price = _as_decimal(m.group("price"))
    cur = (m.group("cur") or "").strip().lower() or None
    if cur in {"руб", "руб.", "р", "р.", "₽", "rub", "rur"}:
        cur = "RUB"
    if cur in {"usd", "$"}:
        cur = "USD"
    if cur in {"eur", "€"}:
        cur = "EUR"
    return price, cur


def _norm_sku(sku: str | None) -> str | None:
    if not sku:
        return None
    s = sku.strip()
    if not s:
        return None
    return re.sub(r"\s+", "", s).casefold()


def _candidate_signature(c: Candidate) -> str:
    """Stable signature for merging hits from different KB sources."""
    parts = [
        _norm_sku(c.sku) or "",
        (c.manufacturer or "").strip().casefold(),
        (c.name or "").strip().casefold(),
        (c.category or "").strip().casefold(),
    ]
    return "|".join(parts).strip("|")


def _safe_text(s: str | None) -> str:
    return (s or "").strip()


def _merge_text(primary: str | None, fallback: str | None) -> str | None:
    p = _safe_text(primary)
    if p:
        return p
    f = _safe_text(fallback)
    return f or None


def _merge_evidence(a: Evidence, b: Evidence) -> Evidence:
    sources = list({*a.sources, *b.sources})
    notes = [n for n in (a.notes + b.notes) if _safe_text(n)]
    return Evidence(
        sources=sources,
        notes=notes,
        source_ts=max(a.source_ts or 0, b.source_ts or 0),
        source_type=a.source_type if (a.source_type == EvidenceSource.SUPPLIER_PRICE) else b.source_type,
    )


def _merge_candidate_fields(
    sksp: Candidate | None,
    supplier: Candidate | None,
) -> Candidate | None:
    """Merge candidate fields following required priorities.

    - price: supplier > sksp
    - description: sksp > supplier
    - do not invent missing fields
    """
    if not sksp and not supplier:
        return None
    base = sksp or supplier
    assert base is not None

    sksp_price = sksp.price if sksp else None
    sup_price = supplier.price if supplier else None
    sksp_cur = sksp.currency if sksp else None
    sup_cur = supplier.currency if supplier else None

    price = sup_price if sup_price is not None else sksp_price
    currency = sup_cur if sup_price is not None else sksp_cur

    desc = _merge_text(sksp.description if sksp else None, supplier.description if supplier else None)

    return Candidate(
        candidate_id=base.candidate_id,
        sku=_merge_text(sksp.sku if sksp else None, supplier.sku if supplier else None),
        manufacturer=_merge_text(sksp.manufacturer if sksp else None, supplier.manufacturer if supplier else None),
        name=_merge_text(sksp.name if sksp else None, supplier.name if supplier else None) or base.name,
        description=desc,
        category=_merge_text(sksp.category if sksp else None, supplier.category if supplier else None),
        price=price,
        currency=currency,
        unit=_merge_text(sksp.unit if sksp else None, supplier.unit if supplier else None),
        url=_merge_text(sksp.url if sksp else None, supplier.url if supplier else None),
        meta={**(supplier.meta or {}), **(sksp.meta or {})},
    )


def _merge_hits_by_signature(hits: list[RagHit]) -> list[RagHit]:
    by_sig: dict[str, list[RagHit]] = {}
    for h in hits:
        sig = _candidate_signature(h.candidate)
        by_sig.setdefault(sig, []).append(h)

    merged: list[RagHit] = []
    for sig, group in by_sig.items():
        if len(group) == 1:
            merged.append(group[0])
            continue

        supplier_hits = [h for h in group if h.evidence.source_type == EvidenceSource.SUPPLIER_PRICE]
        sksp_hits = [h for h in group if h.evidence.source_type == EvidenceSource.SKSP_SNAPSHOT]

        # pick best per source type by score
        best_supplier = max(supplier_hits, key=lambda x: x.score) if supplier_hits else None
        best_sksp = max(sksp_hits, key=lambda x: x.score) if sksp_hits else None

        merged_candidate = _merge_candidate_fields(
            sksp=best_sksp.candidate if best_sksp else None,
            supplier=best_supplier.candidate if best_supplier else None,
        )
        if not merged_candidate:
            continue

        best = max(group, key=lambda x: x.score)
        merged_evidence = best.evidence
        for h in group:
            if h is best:
                continue
            merged_evidence = _merge_evidence(merged_evidence, h.evidence)

        merged.append(
            RagHit(
                candidate=merged_candidate,
                score=max(h.score for h in group),
                evidence=merged_evidence,
            )
        )

    merged.sort(key=lambda x: x.score, reverse=True)
    return merged


@lru_cache(maxsize=1)
def _km():
    return load_knowledge_map()


def _evidence_from_meta(meta: dict[str, Any]) -> Evidence:
    st = int(meta.get("source_ts") or 0)
    src = meta.get("source_type") or ""
    if src == "supplier_price":
        stype = EvidenceSource.SUPPLIER_PRICE
    elif src == "sksp_snapshot":
        stype = EvidenceSource.SKSP_SNAPSHOT
    else:
        stype = EvidenceSource.UNKNOWN
    sources = []
    if meta.get("source_url"):
        sources.append(meta["source_url"])
    if meta.get("source_doc_id"):
        sources.append(str(meta["source_doc_id"]))
    return Evidence(
        sources=sources,
        notes=[str(meta.get("source_note") or "")] if meta.get("source_note") else [],
        source_ts=st,
        source_type=stype,
    )


def _to_candidate(raw: dict[str, Any]) -> Candidate:
    price = _as_decimal(raw.get("price"))
    currency = raw.get("currency")
    if price is None:
        p, cur = _extract_price(_safe_text(raw.get("text") or raw.get("name") or raw.get("description")))
        price = p
        currency = currency or cur

    meta = dict(raw.get("meta") or {})
    for k in ("source_ts", "source_type", "source_url", "source_doc_id", "source_note"):
        if k in raw:
            meta[k] = raw[k]

    return Candidate(
        candidate_id=str(raw.get("candidate_id") or raw.get("id") or uuid4_str()),
        sku=_safe_text(raw.get("sku")),
        manufacturer=_safe_text(raw.get("manufacturer")),
        name=_safe_text(raw.get("name") or raw.get("title") or raw.get("text") or "UNKNOWN"),
        description=_safe_text(raw.get("description")),
        category=_safe_text(raw.get("category")),
        price=price,
        currency=_safe_text(currency),
        unit=_safe_text(raw.get("unit")),
        url=_safe_text(raw.get("url")),
        meta=meta,
    )


def uuid4_str() -> str:
    import uuid

    return str(uuid.uuid4())


def _make_hit(raw: dict[str, Any], score: float) -> RagHit:
    cand = _to_candidate(raw)
    ev = _evidence_from_meta(cand.meta or {})
    return RagHit(candidate=cand, score=float(score), evidence=ev)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _graph_enabled() -> bool:
    return os.environ.get("GRAPH_ENABLED", "1").strip() not in {"0", "false", "False"}


def _kuzu_path() -> str | None:
    p = os.environ.get("KUZU_DB_PATH") or os.environ.get("GRAPH_DB_PATH")
    return p.strip() if p else None


def _try_query_kuzu(query: str, parameters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Best-effort Kuzu query without hard dependency."""
    db_path = _kuzu_path()
    if not db_path or not _graph_enabled():
        return []
    try:
        import kuzu  # type: ignore
    except Exception:
        return []

    try:
        db = kuzu.Database(db_path)
        conn = kuzu.Connection(db)
        res = conn.execute(query, parameters or {})
        rows: list[dict[str, Any]] = []
        while res.has_next():
            row = res.get_next()
            # Kuzu row supports dict conversion in newer versions; fall back otherwise.
            if isinstance(row, dict):
                rows.append(row)
            else:
                try:
                    rows.append(row.to_dict())  # type: ignore[attr-defined]
                except Exception:
                    # stringify row as last resort
                    rows.append({"_row": str(row)})
        return rows
    except Exception:
        return []


def _supplier_price_query(text: str, limit: int = 50) -> list[dict[str, Any]]:
    """Try to retrieve items from the *supplier chat price* subgraph.

    We search across multiple likely label/property combinations to be robust
    to ingest/schema drift.
    """
    q = """
    MATCH (d)
    WHERE
      (LABEL(d) IN ['PriceDoc', 'SupplierPriceDoc', 'Document', 'File'] OR LABEL(d) IS NOT NULL)
      AND (
        (d.kind IS NOT NULL AND lower(d.kind) CONTAINS 'price') OR
        (d.channel IS NOT NULL AND lower(d.channel) CONTAINS 'supplier') OR
        (d.title IS NOT NULL AND lower(d.title) CONTAINS 'прайс') OR
        (d.title IS NOT NULL AND lower(d.title) CONTAINS 'price')
      )
    WITH d
    MATCH (d)-[r]->(it)
    WHERE
      (LABEL(it) IN ['PriceItem', 'LineItem', 'Item', 'SkuItem'] OR LABEL(it) IS NOT NULL)
      AND (
        (it.text IS NOT NULL AND lower(it.text) CONTAINS lower($q)) OR
        (it.name IS NOT NULL AND lower(it.name) CONTAINS lower($q)) OR
        (it.description IS NOT NULL AND lower(it.description) CONTAINS lower($q)) OR
        (it.sku IS NOT NULL AND lower(it.sku) CONTAINS lower($q)) OR
        (it.manufacturer IS NOT NULL AND lower(it.manufacturer) CONTAINS lower($q))
      )
    RETURN
      it.sku AS sku,
      it.manufacturer AS manufacturer,
      coalesce(it.name, it.title, it.text) AS name,
      it.description AS description,
      it.category AS category,
      it.price AS price,
      it.currency AS currency,
      it.unit AS unit,
      it.url AS url,
      coalesce(d.updated_at, d.ts, d.created_at, 0) AS source_ts,
      'supplier_price' AS source_type,
      coalesce(d.url, d.link, d.source_url) AS source_url,
      coalesce(d.id, d.doc_id, d.file_id) AS source_doc_id,
      coalesce(d.title, d.name, 'supplier price') AS source_note
    ORDER BY source_ts DESC
    LIMIT $limit
    """
    return _try_query_kuzu(q, {"q": text, "limit": int(limit)})


def _sksp_snapshot_query(text: str, limit: int = 50) -> list[dict[str, Any]]:
    """Try to retrieve items from SKSP snapshots / tasks database ingested into graph."""
    q = """
    MATCH (d)
    WHERE
      (LABEL(d) IN ['SkspSnapshot', 'SKSP', 'Snapshot', 'Document', 'File'] OR LABEL(d) IS NOT NULL)
      AND (
        (d.kind IS NOT NULL AND lower(d.kind) CONTAINS 'sksp') OR
        (d.title IS NOT NULL AND lower(d.title) CONTAINS 'сксп') OR
        (d.title IS NOT NULL AND lower(d.title) CONTAINS 'sksp')
      )
    WITH d
    MATCH (d)-[r]->(it)
    WHERE
      (LABEL(it) IN ['SkspItem', 'LineItem', 'Item', 'SkuItem'] OR LABEL(it) IS NOT NULL)
      AND (
        (it.text IS NOT NULL AND lower(it.text) CONTAINS lower($q)) OR
        (it.name IS NOT NULL AND lower(it.name) CONTAINS lower($q)) OR
        (it.description IS NOT NULL AND lower(it.description) CONTAINS lower($q)) OR
        (it.sku IS NOT NULL AND lower(it.sku) CONTAINS lower($q)) OR
        (it.manufacturer IS NOT NULL AND lower(it.manufacturer) CONTAINS lower($q))
      )
    RETURN
      it.sku AS sku,
      it.manufacturer AS manufacturer,
      coalesce(it.name, it.title, it.text) AS name,
      it.description AS description,
      it.category AS category,
      it.price AS price,
      it.currency AS currency,
      it.unit AS unit,
      it.url AS url,
      coalesce(d.updated_at, d.ts, d.created_at, 0) AS source_ts,
      'sksp_snapshot' AS source_type,
      coalesce(d.url, d.link, d.source_url) AS source_url,
      coalesce(d.id, d.doc_id, d.file_id) AS source_doc_id,
      coalesce(d.title, d.name, 'sksp snapshot') AS source_note
    ORDER BY source_ts DESC
    LIMIT $limit
    """
    return _try_query_kuzu(q, {"q": text, "limit": int(limit)})


def retrieve_candidates_hybrid(query: str, limit: int = 50) -> list[RagHit]:
    """Hybrid retrieval: supplier prices first, then SKSP snapshots, then fallback to empty.

    This function is intentionally deterministic: ordering and merge priorities
    are fixed, so manager receives repeatable outputs.
    """
    hits: list[RagHit] = []

    for row in _supplier_price_query(query, limit=limit):
        hits.append(_make_hit(row, score=0.9))

    for row in _sksp_snapshot_query(query, limit=limit):
        hits.append(_make_hit(row, score=0.8))

    # Merge duplicates by signature, preserving priorities (price vs description).
    hits = _merge_hits_by_signature(hits)

    # Safety trim (keep deterministic ordering)
    return hits[: int(limit)]


def hits_to_classified_candidates(
    hits: list[RagHit],
) -> list[ClassifiedCandidate]:
    """Convert RagHit to ClassifiedCandidate for downstream normalization."""
    km = _km()
    out: list[ClassifiedCandidate] = []
    for h in hits:
        out.append(
            ClassifiedCandidate(
                candidate_id=h.candidate.candidate_id,
                sku=h.candidate.sku,
                manufacturer=h.candidate.manufacturer,
                name=h.candidate.name,
                description=h.candidate.description,
                category=h.candidate.category,
                price=h.candidate.price,
                currency=h.candidate.currency,
                unit=h.candidate.unit,
                url=h.candidate.url,
                score=h.score,
                meta=h.candidate.meta or {},
                evidence=h.evidence,
                # family is assigned later by classifier
                family=None,
                family_score=None,
                room_fit=None,
                notes=[],
            )
        )
    return out


def explain_hit(hit: RagHit) -> str:
    """Human-readable reason for why this item was selected (manager-facing)."""
    parts = []
    if hit.candidate.sku:
        parts.append(f"SKU: {hit.candidate.sku}")
    if hit.candidate.manufacturer:
        parts.append(f"бренд: {hit.candidate.manufacturer}")
    if hit.candidate.price is not None:
        cur = hit.candidate.currency or "RUB"
        parts.append(f"цена: {hit.candidate.price} {cur}")

    if hit.evidence.source_type == EvidenceSource.SUPPLIER_PRICE:
        parts.append("источник: прайс поставщика (приоритет по цене)")
    elif hit.evidence.source_type == EvidenceSource.SKSP_SNAPSHOT:
        parts.append("источник: прошлые СКСП (приоритет по описанию)")
    else:
        parts.append("источник: неизвестен")

    if hit.evidence.source_ts:
        parts.append(f"актуальность(ts): {hit.evidence.source_ts}")

    if hit.evidence.sources:
        parts.append(f"ref: {hit.evidence.sources[0]}")
    return " • ".join(parts)


def normalize_query_text(transcript_text: str) -> str:
    """Light normalization of transcript to create a robust retrieval prompt."""
    t = transcript_text.strip()
    t = re.sub(r"\s+", " ", t)
    return t[:2000]


def build_rag_query_from_requirements(req: dict[str, Any]) -> str:
    """Build a retrieval query string for hybrid retrieval.

    This is used after LLM parses transcript into structured requirements.
    """
    bits: list[str] = []
    room = (req.get("room_type") or "").strip()
    if room:
        bits.append(room)

    caps = req.get("caps") or {}
    for k, v in sorted(caps.items()):
        if v:
            bits.append(str(k))

    flags = req.get("flags") or {}
    for k, v in sorted(flags.items()):
        if v:
            bits.append(str(k))

    hints = req.get("hints") or []
    for h in hints:
        if isinstance(h, str) and h.strip():
            bits.append(h.strip())

    s = " ".join(bits)
    s = re.sub(r"\s+", " ", s).strip()
    return s or "сксп оборудование"


def retrieve_for_project(req: dict[str, Any], limit: int = 80) -> list[RagHit]:
    q = build_rag_query_from_requirements(req)
    return retrieve_candidates_hybrid(q, limit=limit)


def rank_and_trim(hits: list[RagHit], limit: int = 80) -> list[RagHit]:
    """Optional additional trimming stage (kept deterministic)."""
    # currently sorting done in merge
    return hits[: int(limit)]


def group_hits_by_family(
    classified: list[ClassifiedCandidate],
) -> dict[str, list[ClassifiedCandidate]]:
    fam_map: dict[str, list[ClassifiedCandidate]] = {}
    for c in classified:
        if not c.family:
            continue
        fam_map.setdefault(c.family, []).append(c)
    for fam in fam_map:
        fam_map[fam].sort(key=lambda x: float(x.score or 0.0), reverse=True)
    return fam_map


def pick_top_per_family(
    fam_map: dict[str, list[ClassifiedCandidate]],
    per_family: int = 5,
) -> dict[str, list[ClassifiedCandidate]]:
    return {k: v[: int(per_family)] for k, v in fam_map.items()}


def flatten_top_candidates(
    fam_map: dict[str, list[ClassifiedCandidate]],
) -> list[ClassifiedCandidate]:
    out: list[ClassifiedCandidate] = []
    for fam in sorted(fam_map.keys()):
        out.extend(fam_map[fam])
    return out


def debug_dump_hits(hits: list[RagHit], max_n: int = 20) -> str:
    lines = []
    for h in hits[: int(max_n)]:
        lines.append(f"{h.score:.3f} | {h.candidate.manufacturer or ''} | {h.candidate.sku or ''} | {h.candidate.name}")
        lines.append(f"  {explain_hit(h)}")
    return "\n".join(lines)


# Back-compat names (older parts of scaffold may import these).
retrieve_candidates = retrieve_candidates_hybrid