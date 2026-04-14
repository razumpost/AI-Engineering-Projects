from __future__ import annotations

from typing import Any


def _items(spec: Any) -> list[Any]:
    return list(getattr(spec, "items", []) or [])


def _line_name(line: Any) -> str:
    name = str(getattr(line, "name", "") or "").strip()
    if name:
        return name
    desc = str(getattr(line, "description", "") or "").strip()
    if desc:
        return desc
    sku = str(getattr(line, "sku", "") or "").strip()
    if sku:
        return sku
    return "Позиция"


def _to_float(v: Any) -> float | None:
    if v in (None, "", "-", "—"):
        return None
    try:
        f = float(v)
    except Exception:
        return None
    if f == 0:
        return None
    return f


def _line_price(line: Any) -> float | None:
    money = getattr(line, "unit_price", None)
    if money is not None:
        amount = getattr(money, "amount", None)
        f = _to_float(amount)
        if f is not None:
            return f

    for key in ("unit_price_rub", "unit_price", "price", "price_rub"):
        f = _to_float(getattr(line, key, None))
        if f is not None:
            return f

    meta = getattr(line, "meta", None)
    if isinstance(meta, dict):
        for key in ("unit_price_rub", "unit_price", "price", "price_rub"):
            f = _to_float(meta.get(key))
            if f is not None:
                return f

    return None


def validate_prices(spec: Any, source_pool: Any | None = None) -> list[str]:
    _ = source_pool

    risks: list[str] = []

    for line in _items(spec):
        if _line_price(line) is None:
            risks.append(f"[price_missing] Цена уточняется: {_line_name(line)}")

    deduped: list[str] = []
    seen: set[str] = set()
    for r in risks:
        if r not in seen:
            seen.add(r)
            deduped.append(r)

    if hasattr(spec, "risks"):
        cur = list(getattr(spec, "risks", []) or [])
        cur = [x for x in cur if not str(x).startswith("[price_missing]")]

        seen2 = set(cur)
        for r in deduped:
            if r not in seen2:
                cur.append(r)
                seen2.add(r)

        setattr(spec, "risks", cur)

    return deduped