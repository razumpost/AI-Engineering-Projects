from __future__ import annotations

import re
from decimal import Decimal
from typing import Optional

from .intent import PatchIntent


def _dec(s: str) -> Optional[Decimal]:
    s = (s or "").strip().replace(",", ".")
    if not s:
        return None
    try:
        return Decimal(s)
    except Exception:
        return None


_RE_REPLACE_BRAND = re.compile(r"^\s*(?:замени|поменяй)\s+бренд\s+(.+?)\s+(?:на)\s+(.+?)\s*$", re.IGNORECASE)
_RE_REPLACE = re.compile(r"^\s*(?:замени|поменяй)\s+(.+?)\s+(?:на|вместо)\s+(.+?)\s*$", re.IGNORECASE)
_RE_REMOVE = re.compile(r"^\s*(?:убери|удали|исключи)\s+(.+?)\s*$", re.IGNORECASE)
_RE_ADD = re.compile(r"^\s*(?:добавь|добавить)\s+(?:(\d+(?:[.,]\d+)?)\s+)?(.+?)\s*$", re.IGNORECASE)
_RE_SETQ = re.compile(
    r"^\s*(?:измени|поставь|установи)\s+(?:количество|qty)?\s*(.+?)\s*(?:на|=)\s*(\d+(?:[.,]\d+)?)\s*$",
    re.IGNORECASE,
)


def parse_patch_intent(text: str) -> PatchIntent:
    raw = text or ""
    t = raw.strip()

    m = _RE_REPLACE_BRAND.match(t)
    if m:
        return PatchIntent(action="replace_brand", raw=raw, target=m.group(1).strip(), replacement=m.group(2).strip())

    m = _RE_REPLACE.match(t)
    if m:
        return PatchIntent(action="replace", raw=raw, target=m.group(1).strip(), replacement=m.group(2).strip())

    m = _RE_REMOVE.match(t)
    if m:
        return PatchIntent(action="remove", raw=raw, target=m.group(1).strip())

    m = _RE_SETQ.match(t)
    if m:
        return PatchIntent(action="set_qty", raw=raw, target=m.group(1).strip(), qty=_dec(m.group(2)))

    m = _RE_ADD.match(t)
    if m:
        return PatchIntent(action="add", raw=raw, qty=_dec(m.group(1) or ""), target=m.group(2).strip())

    return PatchIntent(action="unknown", raw=raw)
