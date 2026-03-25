from __future__ import annotations

from typing import Any


def _price_maps(pool: Any) -> tuple[dict[str, float], dict[str, float]]:
    by_cid: dict[str, float] = {}
    by_sku: dict[str, float] = {}

    for it in list(getattr(pool, "items", []) or []):
        cid = getattr(it, "candidate_id", None)
        sku = str(getattr(it, "sku", "") or "").strip()
        try:
            price = float(getattr(it, "unit_price_rub", 0) or 0)
        except Exception:
            price = 0.0

        if cid:
            by_cid[str(cid)] = price
        if sku:
            by_sku[sku] = max(by_sku.get(sku, 0.0), price)

    return by_cid, by_sku


def _line_price(line: Any) -> float:
    for attr in ("unit_price_rub", "unit_price", "price_rub"):
        try:
            v = getattr(line, attr, None)
            if v not in (None, "", 0, 0.0):
                return float(v)
        except Exception:
            continue
    return 0.0


def validate_prices(spec: Any, source_pool: Any | None = None) -> list[str]:
    risks: list[str] = []
    by_cid, by_sku = _price_maps(source_pool) if source_pool is not None else ({}, {})

    for line in list(getattr(spec, "items", []) or []):
        sku = str(getattr(line, "sku", "") or "").strip()
        cid = getattr(line, "candidate_id", None)

        price = _line_price(line)

        if price <= 0 and cid is not None:
            price = float(by_cid.get(str(cid), 0.0) or 0.0)
            if price > 0:
                try:
                    setattr(line, "unit_price_rub", price)
                except Exception:
                    pass

        if price <= 0 and sku:
            price = float(by_sku.get(sku, 0.0) or 0.0)
            if price > 0:
                try:
                    setattr(line, "unit_price_rub", price)
                except Exception:
                    pass

        desc = " ".join([str(getattr(line, "name", "") or ""), str(getattr(line, "description", "") or "")]).casefold()
        is_consumable = any(k in desc for k in ["расход", "комплект", "работ", "монтаж", "услуг"])
        has_real_sku = sku not in ("", "-", "—")

        if price <= 0 and has_real_sku and not is_consumable and not sku.startswith("ph::"):
            risks.append(f"[price_missing] Цена уточняется: {sku}")

    if risks and hasattr(spec, "risks"):
        cur = list(getattr(spec, "risks", []) or [])
        seen = set(cur)
        for r in risks:
            if r not in seen:
                cur.append(r)
        setattr(spec, "risks", cur)

    return risks