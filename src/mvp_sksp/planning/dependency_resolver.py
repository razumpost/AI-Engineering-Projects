from __future__ import annotations

import uuid
from dataclasses import fields, is_dataclass
from typing import Any

from ..knowledge.audio_policy import audio_profile
from ..knowledge.models import ProjectRequirements
from ..normalization.candidate_classifier import classify_candidates
from .plan_models import TopologyDecision


def _pool_items(pool: Any) -> list[Any]:
    return list(getattr(pool, "items", []) or [])


def _line_class(spec: Any) -> type[Any] | None:
    items = list(getattr(spec, "items", []) or [])
    return items[0].__class__ if items else None


def _supported_keys(cls: type[Any]) -> set[str]:
    if hasattr(cls, "model_fields"):
        return set(getattr(cls, "model_fields").keys())
    if is_dataclass(cls):
        return {f.name for f in fields(cls)}
    return set(getattr(cls, "__annotations__", {}).keys())


def _make_line_id() -> str:
    return f"li_{uuid.uuid4().hex[:12]}"


def _stable_item_key(kind: str) -> str:
    # stable key => placeholders won't duplicate across multiple passes
    return f"placeholder::{kind}"


def _find_existing_placeholder(spec: Any, kind: str) -> Any | None:
    for line in list(getattr(spec, "items", []) or []):
        if str(getattr(line, "manufacturer", "") or "").strip() == "Уточнить":
            if str(getattr(line, "item_key", "") or "") == _stable_item_key(kind):
                return line
    return None


def _build_line_from_candidate(spec: Any, candidate: Any, qty: int, category: str) -> Any:
    cls = _line_class(spec)
    if cls is None:
        raise RuntimeError("Cannot create line: spec.items is empty")

    keys = _supported_keys(cls)
    ev = list(getattr(candidate, "evidence_task_ids", None) or [])

    payload: dict[str, Any] = {
        "category": category,
        "candidate_id": getattr(candidate, "candidate_id", None),
        "manufacturer": getattr(candidate, "manufacturer", "") or "",
        "sku": getattr(candidate, "sku", "") or "",
        "name": getattr(candidate, "name", "") or "",
        "description": getattr(candidate, "description", "") or getattr(candidate, "name", "") or "",
        "qty": qty,
        "unit_price_rub": getattr(candidate, "unit_price_rub", None),
    }
    if "line_id" in keys:
        payload["line_id"] = _make_line_id()
    if "item_key" in keys:
        # stable dedupe on manufacturer+sku
        m = (payload["manufacturer"] or "").strip().casefold()
        sku = (payload["sku"] or "").strip().casefold()
        payload["item_key"] = f"{m}::{sku}" if m and sku else f"cid::{payload.get('candidate_id')}"
    if "evidence" in keys:
        payload["evidence"] = {"bitrix_task_ids": ev}
    if "evidence_task_ids" in keys:
        payload["evidence_task_ids"] = ev

    filtered = {k: v for k, v in payload.items() if k in keys}
    if hasattr(cls, "model_validate"):
        return cls.model_validate(filtered)
    return cls(**filtered)


def _build_placeholder_line(spec: Any, *, kind: str, title: str, category: str, qty: int) -> Any:
    cls = _line_class(spec)
    if cls is None:
        raise RuntimeError("Cannot create placeholder: spec.items is empty")

    keys = _supported_keys(cls)
    payload: dict[str, Any] = {
        "category": category,
        "manufacturer": "Уточнить",
        "sku": "—",
        "name": title,
        "description": title,
        "qty": qty,
        "unit_price_rub": None,
    }
    if "line_id" in keys:
        payload["line_id"] = _make_line_id()
    if "item_key" in keys:
        payload["item_key"] = _stable_item_key(kind)
    if "candidate_id" in keys:
        payload["candidate_id"] = f"ph::{kind}"
    if "evidence" in keys:
        payload["evidence"] = {"bitrix_task_ids": []}
    if "evidence_task_ids" in keys:
        payload["evidence_task_ids"] = []

    filtered = {k: v for k, v in payload.items() if k in keys}
    if hasattr(cls, "model_validate"):
        return cls.model_validate(filtered)
    return cls(**filtered)


def _present_families(spec: Any, source_pool: Any) -> dict[str, int]:
    cls_by_id = {c.candidate_id: c for c in classify_candidates(_pool_items(source_pool))}
    fam_count: dict[str, int] = {}
    for line in list(getattr(spec, "items", []) or []):
        cid = getattr(line, "candidate_id", None)
        if not cid:
            continue
        c = cls_by_id.get(cid)
        if not c or not c.family:
            continue
        fam_count[c.family] = fam_count.get(c.family, 0) + 1
    return fam_count


def _best_candidate_for_family(source_pool: Any, family: str) -> Any | None:
    items = _pool_items(source_pool)
    cls_by_id = {c.candidate_id: c for c in classify_candidates(items)}

    best: tuple[float, Any] | None = None
    for it in items:
        cid = getattr(it, "candidate_id", None)
        if not cid:
            continue
        c = cls_by_id.get(cid)
        if not c or c.family != family:
            continue
        score = float(getattr(c, "family_confidence", 0.0) or 0.0) * 10.0
        if getattr(it, "manufacturer", None):
            score += 1.0
        if getattr(it, "sku", None):
            score += 1.0
        if getattr(it, "unit_price_rub", None) not in (None, "", 0, 0.0):
            score += 0.75
        ev = getattr(it, "evidence_task_ids", None) or []
        score += min(1.0, len(ev) * 0.2)
        if best is None or score > best[0]:
            best = (score, it)
    return best[1] if best else None


def resolve_dependencies(spec: Any, source_pool: Any, requirements: ProjectRequirements, topology: TopologyDecision) -> list[str]:
    warnings: list[str] = []

    fam = _present_families(spec, source_pool)

    # 1) Display must exist
    if requirements.room_type == "meeting_room" and not (fam.get("interactive_panel") or fam.get("display_panel")):
        cand = _best_candidate_for_family(source_pool, "interactive_panel") or _best_candidate_for_family(source_pool, "display_panel")
        if cand is not None:
            getattr(spec, "items").append(_build_line_from_candidate(spec, cand, qty=1, category="display"))
        else:
            if _find_existing_placeholder(spec, "display") is None:
                getattr(spec, "items").append(_build_placeholder_line(spec, kind="display", title="Панель/экран для переговорной, подобрать", category="display", qty=1))
            warnings.append("missing_dependency: display")

    fam = _present_families(spec, source_pool)

    # 2) Audio playback must exist (meeting_room => low-Z)
    profile = audio_profile(requirements)
    has_playback = bool(fam.get("soundbar") or fam.get("wall_speaker") or fam.get("ceiling_speaker") or fam.get("speakerphone"))
    if requirements.room_type == "meeting_room" and not has_playback:
        cand = _best_candidate_for_family(source_pool, "soundbar") or _best_candidate_for_family(source_pool, "wall_speaker") or _best_candidate_for_family(source_pool, "ceiling_speaker")
        if cand is not None and profile == "lowz":
            getattr(spec, "items").append(_build_line_from_candidate(spec, cand, qty=2 if "speaker" in (cand.description or "").casefold() else 1, category="conference"))
        else:
            if _find_existing_placeholder(spec, "audio_lowz") is None:
                getattr(spec, "items").append(_build_placeholder_line(spec, kind="audio_lowz", title="Акустика для переговорной (низкоомная), подобрать", category="conference", qty=2))
            warnings.append("missing_dependency: meeting_room_audio_lowz")

    fam = _present_families(spec, source_pool)

    # 3) Discussion system dependencies: if delegate exists -> chairman + central/controller
    has_delegate = bool(fam.get("delegate_unit"))
    if has_delegate and not fam.get("chairman_unit"):
        cand = _best_candidate_for_family(source_pool, "chairman_unit")
        if cand is not None:
            getattr(spec, "items").append(_build_line_from_candidate(spec, cand, qty=1, category="conference"))
        else:
            if _find_existing_placeholder(spec, "chairman") is None:
                getattr(spec, "items").append(_build_placeholder_line(spec, kind="chairman", title="Пульт председателя (1 шт), подобрать", category="conference", qty=1))
            warnings.append("missing_dependency: chairman_unit")

    fam = _present_families(spec, source_pool)

    if has_delegate and not (fam.get("conference_controller") or fam.get("conference_central_unit")):
        cand = _best_candidate_for_family(source_pool, "conference_controller") or _best_candidate_for_family(source_pool, "conference_central_unit")
        if cand is not None:
            getattr(spec, "items").append(_build_line_from_candidate(spec, cand, qty=1, category="conference"))
        else:
            if _find_existing_placeholder(spec, "central") is None:
                getattr(spec, "items").append(_build_placeholder_line(spec, kind="central", title="Центральный блок/контроллер конференц-системы (1 шт), подобрать", category="conference", qty=1))
            warnings.append("missing_dependency: conference_controller")

    fam = _present_families(spec, source_pool)

    # 4) BYOD: only real BYOD families; do NOT accept splitters
    if bool(getattr(requirements.flags, "byod", False)):
        has_byod = bool(fam.get("byod_usb_hdmi_gateway") or fam.get("byod_wireless_presentation") or fam.get("usb_c_dock"))
        if not has_byod:
            cand = (
                _best_candidate_for_family(source_pool, "byod_usb_hdmi_gateway")
                or _best_candidate_for_family(source_pool, "byod_wireless_presentation")
                or _best_candidate_for_family(source_pool, "usb_c_dock")
            )
            if cand is not None:
                getattr(spec, "items").append(_build_line_from_candidate(spec, cand, qty=1, category="signal_transport"))
            else:
                if _find_existing_placeholder(spec, "byod") is None:
                    getattr(spec, "items").append(_build_placeholder_line(spec, kind="byod", title="BYOD модуль (USB-C/HDMI), подобрать", category="signal_transport", qty=1))
                warnings.append("missing_dependency: byod_device")

    return warnings