from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any, Iterable

from ..domain.candidates import CandidatePool
from ..domain.ops import ItemRef, PatchOperation
from ..domain.spec import Spec
from ..editing.editor import apply_operations
from ..knowledge.models import ProjectRequirements
from ..planning.plan_models import TopologyDecision


def _new_spec_id() -> str:
    return f"sp_{uuid.uuid4().hex[:12]}"


def _unique_in_order(xs: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in xs:
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


def _as_decimal(v: Any, default: Decimal = Decimal("1")) -> Decimal:
    if v is None:
        return default
    try:
        if isinstance(v, Decimal):
            return v
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        s = str(v).strip().replace("\u00a0", " ").replace(" ", "").replace(",", ".")
        if not s:
            return default
        return Decimal(s)
    except Exception:
        return default


def _qty_from_candidate(ci: Any) -> Decimal:
    # Our deal_kuzu_retriever stores original row props in meta["props"]
    props = (getattr(ci, "meta", None) or {}).get("props") or {}
    q = props.get("qty")
    d = _as_decimal(q, default=Decimal("1"))
    return d if d > 0 else Decimal("1")


def _seed_from_existing_sksp(pool: CandidatePool, max_lines: int = 250) -> list[PatchOperation]:
    # Prefer items ingested from SKSP snapshot for this deal
    sksp_items = []
    other_items = []

    for ci in list(pool.items or []):
        ps = str(getattr(ci, "price_source", "") or "")
        cat = str(getattr(ci, "category", "") or "")
        if cat == "sksp" or ps == "sksp_xlsx_ingest":
            sksp_items.append(ci)
        else:
            other_items.append(ci)

    chosen = sksp_items if sksp_items else other_items
    chosen = chosen[: max_lines]

    ops: list[PatchOperation] = []
    for ci in chosen:
        qty = _qty_from_candidate(ci)
        ops.append(
            PatchOperation(
                op="add_line",
                category=str(getattr(ci, "category", "") or "") or None,
                item=ItemRef(candidate_id=ci.candidate_id),
                qty=qty,
                reason="Seed: existing deal SKSP snapshot" if sksp_items else "Seed: fallback from deal candidates",
                evidence_task_ids=list(getattr(ci, "evidence_task_ids", None) or []),
            )
        )
    return ops


def _role_qty(role_key: str, req: ProjectRequirements) -> Decimal:
    seat = int(req.caps.seat_count or 0)

    if role_key == "room_display_main":
        return Decimal("1")
    if role_key in {"room_camera_main", "room_camera_secondary"}:
        return Decimal("1")
    if role_key == "room_audio_capture":
        return Decimal(str(max(1, seat or 1)))
    if role_key == "room_audio_playback":
        return Decimal("2")
    if role_key in {"room_signal_switching", "room_byod_ingest", "room_usb_bridge_or_byod_gateway"}:
        return Decimal("1")
    if role_key == "room_cabling_and_accessories":
        return Decimal("1")
    return Decimal("1")


def seed_spec_from_role_candidates(
    *,
    request_text: str,
    pool: CandidatePool,
    role_candidates: dict[str, list[str]],
    requirements: ProjectRequirements,
    topology: TopologyDecision,
) -> Spec:
    """
    Deterministic baseline spec.

    Priority:
      1) If role_candidates present -> seed by roles (engine-style)
      2) Else -> seed from existing deal SKSP snapshot (deal-aware, no hallucination)
    """
    spec = Spec(spec_id=_new_spec_id(), project_title="СкСп", items=[])
    spec.project_summary = request_text
    spec.why_composition = [
        f"Seed: topology={getattr(topology, 'topology_key', 'unknown')}",
        "Seed построен детерминированно (без LLM), затем дополняется зависимостями и количествами.",
    ]

    ops: list[PatchOperation] = []

    if role_candidates:
        for role_key, cids in role_candidates.items():
            qty = _role_qty(role_key, requirements)
            for cid in _unique_in_order(cids):
                ops.append(
                    PatchOperation(
                        op="add_line",
                        category=None,
                        item=ItemRef(candidate_id=cid),
                        qty=qty,
                        reason=f"Seed: role={role_key}",
                        evidence_task_ids=[],
                    )
                )
    else:
        ops = _seed_from_existing_sksp(pool)

        if ops:
            spec.why_composition.append(
                "Role coverage пустой → использован последний сохранённый состав СкСп по сделке (из KB)."
            )

    rep = apply_operations(spec, ops, pool)
    if rep.warnings:
        spec.apply_warnings.extend(rep.warnings)
    if rep.errors:
        spec.apply_warnings.extend([f"seed_error: {e}" for e in rep.errors])

    used: set[int] = set()
    for it in spec.items:
        used.update(it.evidence.bitrix_task_ids)
    spec.used_bitrix_task_ids = sorted(used)

    return spec