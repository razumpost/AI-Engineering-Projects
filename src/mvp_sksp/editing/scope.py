from __future__ import annotations

from typing import Optional

from ..domain.ops import MatchSelector
from ..domain.spec import Spec
from .intent import PatchIntent
from .matching import resolve_single_line


def infer_scope_whitelist(spec: Spec, intent: PatchIntent) -> Optional[list[str]]:
    """
    replace/remove/set_qty/replace_brand:
      - match target in current spec
      - return that line's category as whitelist
    add/unknown:
      - no whitelist
    """
    if intent.action not in {"replace", "remove", "set_qty", "replace_brand"}:
        return None
    if not intent.target:
        return None

    sel = MatchSelector(category=None, contains=[intent.target])
    line_id = resolve_single_line(spec.items, sel)
    if not line_id:
        return None

    for it in spec.items:
        if it.line_id == line_id:
            cat = (it.category or "").strip()
            return [cat] if cat else None
    return None
