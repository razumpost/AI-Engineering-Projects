from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

_PRICE_RE = re.compile(r"(\d[\d\s\u00a0\u202f]{2,})\s*₽")
_WS_RE = re.compile(r"\s+")


def _norm(s: Any) -> str:
    return _WS_RE.sub(" ", str(s or "").replace("\u00a0", " ").replace("\u202f", " ")).strip().casefold()


def _digits_price(s: str) -> Optional[int]:
    s = (s or "").replace(" ", "").replace("\u00a0", "").replace("\u202f", "")
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


@dataclass(frozen=True)
class PriceEvidence:
    price_rub: Optional[int]
    source: str  # 'context' | 'db_price' | 'web_price'
    ref: str      # doc/file/url
    confidence: float = 1.0


@dataclass(frozen=True)
class PriceResolverConfig:
    """
    Универсальный резолвер цен:
    - db_price_sources: какие rag_documents.source считать прайсами (можно авто-определять)
    - allow_web: разрешить web fallback
    - web_domains: whitelist доменов
    - web_timeout_s: таймаут
    """
    db_price_sources: Tuple[str, ...] = ("supplier_price", "supplier_prices", "price", "prices", "supplier_chat_price")
    allow_web: bool = False
    web_domains: Tuple[str, ...] = ("ipvs.ru", "shk-s.ru", "nextouch.ru", "digis.ru", "auvix.ru", "eliteboard.ru", "avc.ru")
    web_timeout_s: int = 15
    user_agent: str = "Mozilla/5.0 (compatible; VectorBDRAGcollector/1.0; +local)"


class PriceResolver:
    def __init__(self, engine: Engine, cfg: Optional[PriceResolverConfig] = None):
        self.engine = engine
        self.cfg = cfg or PriceResolverConfig(
            allow_web=(os.getenv("ALLOW_WEB_PRICES") or "0").strip() in ("1", "true", "yes")
        )

    def _db_sources_present(self) -> List[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("select distinct source from rag_documents")).mappings().all()
        sources = [str(r["source"]) for r in rows]
        # heuristic: keep ones containing 'price' or 'supplier'
        picked = [s for s in sources if any(x in s for x in ("price", "supplier", "прайс", "prices"))]
        if picked:
            return picked
        return list(self.cfg.db_price_sources)

    def resolve_price(
        self,
        *,
        article: str,
        manufacturer: str = "",
        description: str = "",
        context_prices: Optional[Iterable[int]] = None,
    ) -> Optional[PriceEvidence]:
        """
        Return best evidence for price:
          1) if context_prices provided -> pick first valid
          2) search DB price sources by article
          3) optional web scraping by article+brand keywords (whitelist domains)
        """
        art = (article or "").strip()
        if not art:
            return None

        # 1) context already had a price (highest priority)
        if context_prices:
            for p in context_prices:
                if p and p > 0:
                    return PriceEvidence(price_rub=int(p), source="context", ref="context", confidence=1.0)

        # 2) DB price lookup
        ev = self._resolve_price_from_db(article=art)
        if ev:
            return ev

        # 3) Web fallback
        if self.cfg.allow_web:
            ev = self._resolve_price_from_web(article=art, manufacturer=manufacturer, description=description)
            if ev:
                return ev

        return None

    def _resolve_price_from_db(self, *, article: str) -> Optional[PriceEvidence]:
        sources = self._db_sources_present()

        # Strategy:
        # - look for chunks where meta contains sku/article equals our article
        # - else fallback to content ILIKE article
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    with candidates as (
                      select
                        d.id as doc_id,
                        d.source as doc_source,
                        d.title as title,
                        c.id as chunk_id,
                        c.content,
                        c.meta as meta
                      from rag_documents d
                      join rag_chunks c on c.document_id = d.id
                      where d.source = any(:sources)
                        and (
                          (c.meta ? 'sku' and c.meta->>'sku' = :article)
                          or (c.meta ? 'article' and c.meta->>'article' = :article)
                          or (c.content ilike ('%' || :article || '%'))
                        )
                      limit 200
                    )
                    select doc_id, doc_source, title, chunk_id, content, meta
                    from candidates
                    """
                ),
                {"sources": sources, "article": article},
            ).mappings().all()

        best: Optional[PriceEvidence] = None
        for r in row:
            meta = r.get("meta") or {}
            price = None

            # common keys
            for k in ("price_rub", "unit_price", "price", "Цена", "Цена_руб", "priceRub"):
                if isinstance(meta, dict) and k in meta:
                    price = _digits_price(str(meta.get(k)))
                    if price:
                        break

            if not price:
                # parse from content
                m = _PRICE_RE.search(str(r.get("content") or ""))
                if m:
                    price = _digits_price(m.group(1))

            if price and price > 0:
                cand = PriceEvidence(
                    price_rub=price,
                    source="db_price",
                    ref=f"{r.get('doc_source')}:{r.get('doc_id')}#{r.get('chunk_id')}",
                    confidence=0.9,
                )
                # choose minimal non-zero price as conservative
                if best is None or (cand.price_rub or 10**18) < (best.price_rub or 10**18):
                    best = cand

        return best

    def _resolve_price_from_web(self, *, article: str, manufacturer: str, description: str) -> Optional[PriceEvidence]:
        # universal lightweight web fallback: try search pages by hitting site internal search endpoints is too custom,
        # so we do simple: fetch a few likely URLs by pattern is not reliable.
        # Therefore, web fallback is intentionally conservative:
        # - only use if an env provides direct URL mapping or you already ingested vendor_catalog with URLs
        #
        # If vendor_catalog exists, we can lookup matching product URL by embedding or exact sku match.
        url = self._find_vendor_url_by_article(article)
        if not url:
            return None

        domain_ok = any(url.endswith(d) or (("//" + d) in url) or (("." + d + "/") in url) for d in self.cfg.web_domains)
        if not domain_ok:
            return None

        try:
            r = requests.get(url, headers={"User-Agent": self.cfg.user_agent}, timeout=self.cfg.web_timeout_s)
            if r.status_code >= 400:
                return None
            m = _PRICE_RE.search(r.text)
            if not m:
                return None
            price = _digits_price(m.group(1))
            if not price or price <= 0:
                return None
            return PriceEvidence(price_rub=price, source="web_price", ref=url, confidence=0.6)
        except Exception:
            return None

    def _find_vendor_url_by_article(self, article: str) -> Optional[str]:
        # Prefer DB vendor_catalog ingestion (source='vendor_catalog'), exact sku/article match in chunk_meta.
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    select
                      (c.meta->>'url') as url,
                      c.meta as meta
                    from rag_documents d
                    join rag_chunks c on c.document_id = d.id
                    where d.source='vendor_catalog'
                      and (
                        (c.meta ? 'sku' and c.meta->>'sku' = :article)
                        or (c.meta ? 'article' and c.meta->>'article' = :article)
                        or (c.content ilike ('%' || :article || '%'))
                      )
                    limit 10
                    """
                ),
                {"article": article},
            ).mappings().first()
        if not row:
            return None
        url = str(row.get("url") or "").strip()
        return url or None
