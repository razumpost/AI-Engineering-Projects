# src/bitrix_kp_collector/file_classifier.py
from __future__ import annotations

import re
from pathlib import Path

_KP_PAT = re.compile(r"\b(кп|коммерческ(ое|ий)\s+предлож(ение|.)|commercial\s+proposal)\b", re.IGNORECASE)
_SKSP_PAT = re.compile(r"\b(сксп|сквозн(ая|ой)\s+специфик(ация|ации)|сквозная\s+спека)\b", re.IGNORECASE)

_EXT_OK = {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt", ".rtf"}


def is_kp_candidate(name: str) -> bool:
    n = (name or "").strip()
    return bool(_KP_PAT.search(n))


def is_sksp_candidate(name: str) -> bool:
    n = (name or "").strip()
    return bool(_SKSP_PAT.search(n))


def is_supported_file(name: str) -> bool:
    ext = Path(name).suffix.lower()
    return ext in _EXT_OK
