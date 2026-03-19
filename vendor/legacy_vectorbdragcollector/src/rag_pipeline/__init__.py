"""Compatibility shim for historic imports.

Allows:
    PYTHONPATH=. python -m rag_pipeline ...
and:
    from rag_pipeline.embeddings import Embedder
even if implementation lives in src/rag_pipeline.
"""
from __future__ import annotations

from pathlib import Path
import sys

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
_IMPL = _SRC / "rag_pipeline"

if _SRC.exists():
    src_str = str(_SRC)
    if src_str not in sys.path:
        sys.path.insert(0, src_str)

    if _IMPL.exists():
        try:
            __path__.append(str(_IMPL))  # type: ignore[name-defined]
        except Exception:
            pass
