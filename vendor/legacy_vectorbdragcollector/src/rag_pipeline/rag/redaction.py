# =========================
# FILE: src/rag_pipeline/rag/redaction.py
# =========================
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Tuple

from ..security import redact_sensitive


@dataclass(frozen=True)
class RedactionConfig:
    """
    Redaction config for any text that leaves the system (e.g. sent to YaGPT or shown to user).

    The goal is to suppress:
      - persons (names, Bitrix user tags)
      - phone numbers
      - INN
      - contract / agreement numbers
      - exact addresses (keep city/region when possible)
    """

    redact_persons: bool = True
    redact_phones: bool = True
    redact_inn: bool = True
    redact_contracts: bool = True
    redact_addresses: bool = True


_BITRIX_USER_TAG = re.compile(r"\[USER=\d+]\s*.*?\s*\[/USER]", flags=re.IGNORECASE | re.DOTALL)
_BITRIX_URL_TAG = re.compile(r"\[URL=.*?]\s*.*?\s*\[/URL]", flags=re.IGNORECASE | re.DOTALL)

# 2-3 capitalized Cyrillic words: "Иванов Иван Иванович" / "Иван Иванов"
_PERSON_RU = re.compile(
    r"(?<![A-Za-zА-Яа-яЁё])"
    r"(?:[А-ЯЁ][а-яё]{2,}\s+){1,2}[А-ЯЁ][а-яё]{2,}"
    r"(?![A-Za-zА-Яа-яЁё])"
)

# Contract/agreement numbers
_CONTRACT = re.compile(
    r"(?i)\b(?:договор|контракт|соглашение|доп\.?\s*соглашение|дс)\s*№?\s*"
    r"[A-ZА-Я0-9][A-ZА-Я0-9\/\-\._]{3,}\b"
)

# 6-digit postal codes
_POSTAL = re.compile(r"(?<!\d)\d{6}(?!\d)")

# "ул.", "д.", "корп.", "кв." parts: replace everything after city marker, but keep city/region token
_ADDRESS_MARKERS = re.compile(
    r"(?i)\b(?:ул\.|улица|просп\.|проспект|пр\-т|пер\.|переулок|шоссе|наб\.|набережная|дом|д\.|кв\.|квартира|офис|оф\.)\b"
)

_CITY_CAPTURE = re.compile(r"(?i)\b(?:г\.|город)\s*([А-ЯЁ][а-яё\-]+)\b")
_REGION_CAPTURE = re.compile(r"(?i)\b(?:обл\.|область|край|респ\.|республика)\s*([А-ЯЁ][а-яё\-]+)\b")


def redact_text(text: str, cfg: RedactionConfig | None = None) -> Tuple[str, int]:
    """
    Returns (redacted_text, redaction_count).
    """
    cfg = cfg or RedactionConfig()
    t = text or ""
    cnt = 0

    # Reuse generic redactor for phone/inn/etc (keeps it consistent project-wide)
    if cfg.redact_phones or cfg.redact_inn:
        before = t
        t = redact_sensitive(
            t,
            redact_phones=cfg.redact_phones,
            redact_inn=cfg.redact_inn,
            redact_cards=False,
            redact_emails=False,
        )
        if t != before:
            cnt += 1

    if cfg.redact_contracts:
        t2, k = _sub_count(_CONTRACT, "[CONTRACT_NUMBER]", t)
        t, cnt = t2, cnt + k

    if cfg.redact_addresses:
        t2, k = _redact_addresses_keep_city(t)
        t, cnt = t2, cnt + k
        t2, k = _sub_count(_POSTAL, "[POSTAL_CODE]", t)
        t, cnt = t2, cnt + k

    if cfg.redact_persons:
        t2, k = _sub_count(_BITRIX_USER_TAG, "[PERSON]", t)
        t, cnt = t2, cnt + k
        t2, k = _sub_count(_BITRIX_URL_TAG, "[LINK]", t)
        t, cnt = t2, cnt + k
        t2, k = _sub_count(_PERSON_RU, "[PERSON]", t)
        t, cnt = t2, cnt + k

    return t, cnt


def redact_many(texts: Iterable[str], cfg: RedactionConfig | None = None) -> Tuple[list[str], int]:
    out: list[str] = []
    total = 0
    for t in texts:
        r, k = redact_text(t, cfg=cfg)
        out.append(r)
        total += k
    return out, total


def _sub_count(pattern: re.Pattern[str], repl: str, text: str) -> Tuple[str, int]:
    n = 0

    def _r(_: re.Match[str]) -> str:
        nonlocal n
        n += 1
        return repl

    return pattern.sub(_r, text), n


def _redact_addresses_keep_city(text: str) -> Tuple[str, int]:
    """
    Best-effort address redaction: keep city/region tokens if present, remove street/building details.
    """
    if not text:
        return text, 0

    hits = 0
    t = text

    def _replace(match: re.Match[str]) -> str:
        nonlocal hits
        hits += 1
        chunk = match.group(0)
        city = _CITY_CAPTURE.search(chunk)
        region = _REGION_CAPTURE.search(chunk)
        keep = []
        if region:
            keep.append(region.group(0).strip())
        if city:
            keep.append(city.group(0).strip())
        keep_part = ", ".join(keep).strip()
        if keep_part:
            return keep_part + " [ADDRESS]"
        return "[ADDRESS]"

    # Heuristic: find long-ish substrings with address markers, and redact from marker to end-of-line.
    pattern = re.compile(r"(?m)^.*" + _ADDRESS_MARKERS.pattern + r".*$")
    t2 = pattern.sub(_replace, t)
    return t2, hits
