from __future__ import annotations

import datetime as dt
import functools
import os
import re
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import requests

_USD_HINT = re.compile(r"(\$|\busd\b|доллар|usd/|usd\s)", re.IGNORECASE)
_EUR_HINT = re.compile(r"(€|\beur\b|евро|eur/|eur\s)", re.IGNORECASE)
_CNY_HINT = re.compile(r"(¥|\bcny\b|юан|cny/|cny\s|rmb)", re.IGNORECASE)

_NUM = re.compile(r"(?<!\d)(\d+(?:[.,]\d+)?)(?!\d)")


@dataclass(frozen=True)
class FxRate:
    code: str
    value_rub: float  # RUB per 1 unit


def _msk_today() -> dt.date:
    # MSK is UTC+3; good enough for daily rate selection
    return (dt.datetime.utcnow() + dt.timedelta(hours=3)).date()


def detect_currency(text: str) -> Optional[str]:
    t = text or ""
    if _USD_HINT.search(t):
        return "USD"
    if _EUR_HINT.search(t):
        return "EUR"
    if _CNY_HINT.search(t):
        return "CNY"
    return None


def parse_price_number(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    m = _NUM.search(s.replace(" ", ""))
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except Exception:
        return None


def _cbr_xml_url(d: dt.date) -> str:
    # XML_daily.asp?date_req=DD/MM/YYYY
    return f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={d:%d/%m/%Y}"


def _fetch_cbr_rates_for_date(d: dt.date, timeout_s: int = 20) -> Dict[str, FxRate]:
    """
    Returns RUB per 1 unit for codes.
    """
    r = requests.get(_cbr_xml_url(d), timeout=timeout_s)
    r.raise_for_status()
    xml = r.text

    def _find(code: str) -> Optional[FxRate]:
        # Very small parser: find <CharCode>USD</CharCode> ... <Nominal>1</Nominal> ... <Value>91,1234</Value>
        # NOTE: CBR uses comma as decimal separator.
        pat = re.compile(
            rf"<CharCode>\s*{re.escape(code)}\s*</CharCode>.*?<Nominal>\s*(\d+)\s*</Nominal>.*?<Value>\s*([\d,]+)\s*</Value>",
            re.DOTALL,
        )
        m = pat.search(xml)
        if not m:
            return None
        nominal = int(m.group(1))
        value = float(m.group(2).replace(",", "."))
        return FxRate(code=code, value_rub=value / max(nominal, 1))

    out: Dict[str, FxRate] = {}
    for code in ("USD", "EUR", "CNY"):
        fr = _find(code)
        if fr:
            out[code] = fr
    return out


@functools.lru_cache(maxsize=8)
def get_cbr_rates(date_iso: str) -> Dict[str, FxRate]:
    d = dt.date.fromisoformat(date_iso)
    return _fetch_cbr_rates_for_date(d)


def get_best_rates() -> Tuple[str, Dict[str, FxRate]]:
    """
    Try today (MSK). If fail / missing, fallback to yesterday.
    """
    today = _msk_today()
    for d in (today, today - dt.timedelta(days=1)):
        try:
            rates = get_cbr_rates(d.isoformat())
            if rates:
                return d.isoformat(), rates
        except Exception:
            continue
    return today.isoformat(), {}
