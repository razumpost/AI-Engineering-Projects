from __future__ import annotations

"""Deterministic request->query rewrite stub."""

import re
from typing import Any, Dict, Tuple

_WS_RE = re.compile(r"\s+")


def rewrite_request_to_query(request_text: str, *, fail_closed: bool = True) -> Tuple[str, Dict[str, Any]]:
    clean = _WS_RE.sub(" ", (request_text or "").replace("\u00a0", " ")).strip()
    return clean, {"clean_request": clean}
