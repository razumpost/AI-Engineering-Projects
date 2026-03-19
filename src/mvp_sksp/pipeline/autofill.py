from __future__ import annotations

import re
from decimal import Decimal
from typing import Iterable, Optional

from ..domain.candidates import CandidatePool, CandidateItem
from ..domain.ops import PatchOperation, ItemRef
from ..domain.spec import Spec, norm_key


_RE_SEATS = re.compile(r"(?:на\s*)?(\d{1,3})\s*(?:мест|чел)", re.IGNORECASE)
_RE_COUNT_CAM = re.compile(r"(\d{1,2})\s*(?:камер|camera|ptz)", re.IGNORECASE)
_RE_MIC = re.compile(r"(микрофон|gooseneck|microphone|\bmic\b)", re.IGNORECASE)


def _extract_seat_count(text: str) -> Optional[int]:
    m = _RE_SEATS.search(text or "")
    if not m:
        return None
    try:
        v = int(m.group(1))
        return v if 1 <= v <= 200 else None
    except Exception:
        return None


def _extract_cam_count(text: str) -> Optional[int]:
    m = _RE_COUNT_CAM.search(text or "")
    if not m:
        return None
    try:
        v = int(m.group(1))
        return v if 1 <= v <= 20 else None
    except Exception:
        return None


def _candidate_quality_score(ci: CandidateItem) -> int:
    s = 0
    if ci.sku:
        s += 2
    if ci.manufacturer:
        s += 2
    if ci.unit_price_rub is not None:
        s += 1
    if ci.description and len(ci.description) > 10:
        s += 1
    return s


def _prefer_scopes_from_query(query: str, categories: set[str]) -> list[str]:
    q = (query or "").casefold()
    # НЕ "жёсткий набор", а мягкая подсказка: если явно ВКС/переговорная — приоритетим эти scope,
    # иначе берём самые частые по кандидатурам.
    prefer: list[str] = []
    if "вкс" in q or "переговор" in q or "conference" in q:
        for c in ["conference", "cameras", "signal_transport", "processing"]:
            if c in categories:
                prefer.append(c)
    if not prefer:
        prefer = []
    return prefer


def _qty_hint(ci: CandidateItem, *, seat_count: Optional[int], cam_count: Optional[int]) -> Decimal:
    # Никаких SKU-хардкодов. Только общие, прозрачные правила.
    name = f"{ci.name} {ci.description}".casefold()
    if ci.category == "cameras" and cam_count and ("камера" in name or "camera" in name or "ptz" in name):
        return Decimal(cam_count)
    if ci.category == "conference" and seat_count and _RE_MIC.search(name):
        # MVP правило: если это микрофон в переговорке — по местам (потом уточним/оптимизируем)
        return Decimal(seat_count)
    return Decimal(1)


def build_autofill_ops(
    *,
    spec: Spec,
    pool: CandidatePool,
    query_text: str,
    min_lines: int = 18,
    target_lines: int = 32,
    hard_cap: int = 70,
) -> list[PatchOperation]:
    """
    If LLM produced too few lines, auto-append a richer draft using precedent items.
    Uses:
      - candidate pool items (already from top tasks snapshots)
      - dedupe by item_key
      - light qty hints from query (seats/cameras) without SKU hardcodes
    """
    if len(spec.items) >= min_lines:
        return []

    seat_count = _extract_seat_count(query_text) or _extract_seat_count(spec.project_summary or "")
    cam_count = _extract_cam_count(query_text)

    existing_keys = {it.item_key for it in spec.items}
    categories = {ci.category for ci in pool.items if ci.category}
    prefer_scopes = _prefer_scopes_from_query(query_text, categories)

    # prioritize items that:
    # - have good fields
    # - belong to preferred scopes
    # - have evidence_task_ids
    def score(ci: CandidateItem) -> tuple[int, int]:
        base = _candidate_quality_score(ci)
        pref = 2 if ci.category in prefer_scopes else 0
        ev = 1 if ci.evidence_task_ids else 0
        return (pref + ev, base)

    candidates = sorted(pool.items, key=score, reverse=True)

    per_scope_min = 4 if prefer_scopes else 0
    per_scope_count: dict[str, int] = {}

    ops: list[PatchOperation] = []
    added = 0

    for ci in candidates:
        if len(spec.items) + added >= hard_cap:
            break

        # create item_key in the same way editor does: manufacturer+sku+name fallback
        mk = norm_key(ci.manufacturer or "")
        sk = norm_key(ci.sku or "")
        nk = norm_key(ci.name or ci.description or "")
        item_key = "|".join([mk, sk, nk])

        if item_key in existing_keys:
            continue

        # coverage: ensure we include at least per_scope_min items from each preferred scope
        if prefer_scopes and ci.category in prefer_scopes:
            per_scope_count.setdefault(ci.category, 0)

        if prefer_scopes and per_scope_min:
            # if some preferred scopes still underfilled, prioritize them
            underfilled = [s for s in prefer_scopes if per_scope_count.get(s, 0) < per_scope_min]
            if underfilled and ci.category not in underfilled:
                continue

        qty = _qty_hint(ci, seat_count=seat_count, cam_count=cam_count)

        ops.append(
            PatchOperation(
                op="add_line",
                category=ci.category,
                item=ItemRef(candidate_id=ci.candidate_id),
                qty=qty,
                reason="Автозаполнение по прецедентам (граф/Kuzu)",
                evidence_task_ids=list(ci.evidence_task_ids),
            )
        )

        existing_keys.add(item_key)
        added += 1
        if ci.category in prefer_scopes:
            per_scope_count[ci.category] = per_scope_count.get(ci.category, 0) + 1

        # stop when we reached target fullness
        if len(spec.items) + added >= target_lines:
            break

    # If still tiny (pool small), just return what we have
    return ops
