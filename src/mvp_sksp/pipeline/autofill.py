from __future__ import annotations

import re
from decimal import Decimal
from typing import Optional

from ..adapters.price_classifier import classify_price_item
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


def _candidate_scope(ci: CandidateItem) -> str:
    return classify_price_item(ci.name or "", ci.description or "")


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


def _prefer_scopes_from_query(query: str, scopes: set[str]) -> list[str]:
    q = (query or "").casefold()
    prefer: list[str] = []

    meeting_room_like = any(x in q for x in ["переговор", "conference", "meeting room"])
    wants_display = any(x in q for x in ["дисплей", "панель", "экран", "display", "monitor"])
    wants_camera = any(x in q for x in ["камера", "camera", "ptz"])
    wants_mic = any(x in q for x in ["микрофон", "microphone", "mic"])
    wants_audio = any(x in q for x in ["акуст", "speaker", "soundbar", "audio"])

    if meeting_room_like or wants_display or wants_camera or wants_mic:
        for c in ["display", "camera", "microphone", "audio", "controller"]:
            if c in scopes:
                prefer.append(c)

    return prefer


def _is_delegate_discussion_candidate(ci: CandidateItem) -> bool:
    blob = f"{ci.name or ''} {ci.description or ''} {ci.model or ''} {ci.sku or ''}".casefold()
    return any(
        x in blob
        for x in [
            "delegate",
            "chairman",
            "пульт делегата",
            "пульт председателя",
            "дискуссион",
            "discussion",
            "conference unit",
            "bosch dis",
            "taiden",
            "relacart",
            "televic",
        ]
    )


def _qty_hint(ci: CandidateItem, *, seat_count: Optional[int], cam_count: Optional[int]) -> Decimal:
    scope = _candidate_scope(ci)
    name = f"{ci.name} {ci.description}".casefold()

    if scope == "camera" and cam_count and ("камера" in name or "camera" in name or "ptz" in name):
        return Decimal(1)

    # Для обычной переговорки не размножаем микрофон по числу мест.
    # seat_count имеет смысл только для discussion/delegate-пультов.
    if scope == "microphone" and seat_count and _RE_MIC.search(name):
        if _is_delegate_discussion_candidate(ci):
            return Decimal(seat_count)
        return Decimal(1)

    return Decimal(1)


def _is_query_software_first(query_text: str) -> bool:
    q = (query_text or "").casefold()
    return any(x in q for x in ["signage", "digital signage", "cms", "spinetix", "player", "лиценз", "software"])


def build_autofill_ops(
    *,
    spec: Spec,
    pool: CandidatePool,
    query_text: str,
    min_lines: int = 18,
    target_lines: int = 32,
    hard_cap: int = 70,
) -> list[PatchOperation]:
    if len(spec.items) >= min_lines:
        return []

    seat_count = _extract_seat_count(query_text) or _extract_seat_count(spec.project_summary or "")
    cam_count = _extract_cam_count(query_text)

    existing_keys = {it.item_key for it in spec.items}
    scopes = {_candidate_scope(ci) for ci in pool.items}
    prefer_scopes = _prefer_scopes_from_query(query_text, scopes)

    software_first = _is_query_software_first(query_text)
    core_scopes = {"display", "camera", "microphone", "audio", "controller"}
    support_scopes = {"mount", "cable"}
    bad_scopes = {"software", "ops"} if not software_first else set()

    current_core_in_spec = 0
    for it in spec.items:
        blob = f"{getattr(it, 'name', '')} {getattr(it, 'description', '')}"
        sc = classify_price_item(blob, "")
        if sc in core_scopes:
            current_core_in_spec += 1

    def score(ci: CandidateItem) -> tuple[int, int, int]:
        base = _candidate_quality_score(ci)
        scope = _candidate_scope(ci)
        pref = 3 if scope in prefer_scopes else 0
        ev = 1 if ci.evidence_task_ids else 0

        scope_rank = 0
        if scope in core_scopes:
            scope_rank = 3
        elif scope in support_scopes:
            scope_rank = 1
        elif scope in bad_scopes:
            scope_rank = -5

        return (pref + ev + scope_rank, base, len(ci.evidence_task_ids or []))

    candidates = sorted(pool.items, key=score, reverse=True)

    per_scope_min = 1 if prefer_scopes else 0
    per_scope_count: dict[str, int] = {k: 0 for k in prefer_scopes}

    ops: list[PatchOperation] = []
    added = 0

    for ci in candidates:
        if len(spec.items) + added >= hard_cap:
            break

        mk = norm_key(ci.manufacturer or "")
        sk = norm_key(ci.sku or "")
        nk = norm_key(ci.name or ci.description or "")
        item_key = "|".join([mk, sk, nk])

        if item_key in existing_keys:
            continue

        scope = _candidate_scope(ci)

        if scope in bad_scopes:
            continue

        core_covered_total = current_core_in_spec + sum(v for k, v in per_scope_count.items() if k in core_scopes)
        if scope in support_scopes and core_covered_total < max(2, len([x for x in prefer_scopes if x in core_scopes])):
            continue

        if prefer_scopes and per_scope_min:
            underfilled = [s for s in prefer_scopes if per_scope_count.get(s, 0) < per_scope_min]
            if underfilled and scope not in underfilled:
                continue

        qty = _qty_hint(ci, seat_count=seat_count, cam_count=cam_count)

        if scope == "camera" and cam_count and qty > 1:
            qty = Decimal(1)

        ops.append(
            PatchOperation(
                op="add_line",
                category=scope,
                item=ItemRef(candidate_id=ci.candidate_id),
                qty=qty,
                reason="Осторожное автозаполнение по core scopes",
                evidence_task_ids=list(ci.evidence_task_ids),
            )
        )

        existing_keys.add(item_key)
        added += 1
        if scope in per_scope_count:
            per_scope_count[scope] = per_scope_count.get(scope, 0) + 1

        if prefer_scopes and all(per_scope_count.get(s, 0) >= per_scope_min for s in prefer_scopes):
            if len(ops) >= max(4, len(prefer_scopes) + 1):
                break

        if len(spec.items) + added >= target_lines:
            break

    return ops