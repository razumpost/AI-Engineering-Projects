from __future__ import annotations

from typing import Any


def _candidate_price_map(pool: Any) -> dict[str, float]:
    out: dict[str, float] = {}
    for it in list(getattr(pool, "items", []) or []):
        cid = getattr(it, "candidate_id", None)
        if not cid:
            continue
        try:
            out[cid] = float(getattr(it, "unit_price_rub", 0) or 0)
        except Exception:
            out[cid] = 0.0
    return out


def validate_prices(spec: Any, source_pool: Any | None = None) -> list[str]:
    risks: list[str] = []
    price_by_cid = _candidate_price_map(source_pool) if source_pool is not None else {}

    for line in list(getattr(spec, "items", []) or []):
        cid = getattr(line, "candidate_id", None)
        sku = str(getattr(line, "sku", "") or "").strip()

        # 1) take from line if present
        price = 0.0
        try:
            v = getattr(line, "unit_price_rub", None)
            if v not in (None, "", 0, 0.0):
                price = float(v)
        except Exception:
            price = 0.0

        # 2) fill from candidate pool if missing
        if price <= 0 and cid and cid in price_by_cid and price_by_cid[cid] > 0:
            price = float(price_by_cid[cid])
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