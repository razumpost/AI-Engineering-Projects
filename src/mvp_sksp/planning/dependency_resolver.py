from __future__ import annotations

import uuid
from dataclasses import fields, is_dataclass
from typing import Any

from ..domain.spec import build_item_key, norm_text
from ..knowledge.models import ProjectRequirements
from ..normalization.candidate_classifier import classify_candidate, classify_candidates
from .plan_models import TopologyDecision


def _pool_items(pool: Any) -> list[Any]:
    return list(getattr(pool, "items", []) or [])


def _classified_by_id(pool: Any) -> dict[str, Any]:
    return {c.candidate_id: c for c in classify_candidates(_pool_items(pool))}


def _line_candidate_id(line: Any) -> str | None:
    cid = getattr(line, "candidate_id", None)
    if cid:
        return str(cid)
    meta = getattr(line, "meta", None)
    if isinstance(meta, dict) and meta.get("candidate_id"):
        return str(meta.get("candidate_id"))
    return None


def _line_family(line: Any, cls_by_id: dict[str, Any]) -> str | None:
    cid = _line_candidate_id(line)
    if cid and cid in cls_by_id and getattr(cls_by_id[cid], "family", None):
        return cls_by_id[cid].family

    class _LineLike:
        candidate_id = cid or "line"
        category = getattr(line, "category", None)
        sku = getattr(line, "sku", None)
        manufacturer = getattr(line, "manufacturer", None)
        name = getattr(line, "name", None) or getattr(line, "description", "") or ""
        description = getattr(line, "description", None)

    c = classify_candidate(_LineLike())
    return c.family


def _line_qty(line: Any) -> float:
    try:
        return float(getattr(line, "qty", 0) or 0)
    except Exception:
        return 0.0


def _supported_keys(cls: type[Any]) -> set[str]:
    if hasattr(cls, "model_fields"):
        return set(getattr(cls, "model_fields").keys())
    if is_dataclass(cls):
        return {f.name for f in fields(cls)}
    return set(getattr(cls, "__annotations__", {}).keys())


def _new_line_id() -> str:
    return f"li_{uuid.uuid4().hex[:12]}"


def _line_class(spec: Any) -> type[Any] | None:
    items = list(getattr(spec, "items", []) or [])
    if items:
        return items[0].__class__
    try:
        from ..domain.spec import LineItem as DomainLineItem
        return DomainLineItem
    except Exception:
        return None


def _build_line_from_candidate(spec: Any, candidate: Any, qty: int | float, category: str = "misc") -> Any:
    cls = _line_class(spec)
    if cls is None:
        raise RuntimeError("Cannot create new spec line: unknown line class")

    keys = _supported_keys(cls)

    manufacturer = getattr(candidate, "manufacturer", None)
    sku = getattr(candidate, "sku", None)
    model = getattr(candidate, "model", None)
    name = getattr(candidate, "name", None) or ""
    description = norm_text(getattr(candidate, "description", None) or name or "")

    payload: dict[str, Any] = {
        "category": category,
        "manufacturer": manufacturer,
        "sku": sku,
        "model": model,
        "name": name or description or "Item",
        "description": description or name or "Item",
        "unit": "шт",
        "qty": int(qty) if float(qty).is_integer() else float(qty),
    }

    if "line_id" in keys:
        payload["line_id"] = _new_line_id()
    if "item_key" in keys:
        payload["item_key"] = build_item_key(sku=sku, manufacturer=manufacturer, model=model, description=description)

    if "unit_price" in keys:
        payload["unit_price"] = candidate.money() if hasattr(candidate, "money") else None
    if "unit_price_rub" in keys:
        payload["unit_price_rub"] = getattr(candidate, "unit_price_rub", None)

    ev = list(getattr(candidate, "evidence_task_ids", None) or [])
    if "evidence" in keys:
        payload["evidence"] = {
            "bitrix_task_ids": ev,
            "supplier_item_ids": [],
            "retrieval_block_ids": [],
            "notes": [getattr(candidate, "price_source", None)] if getattr(candidate, "price_source", None) else [],
        }
    if "evidence_task_ids" in keys:
        payload["evidence_task_ids"] = ev

    if "meta" in keys:
        payload["meta"] = {"candidate_id": getattr(candidate, "candidate_id", None), "source": "dependency_resolver"}
    if "candidate_id" in keys:
        payload["candidate_id"] = getattr(candidate, "candidate_id", None)

    filtered = {k: v for k, v in payload.items() if k in keys}
    if hasattr(cls, "model_validate"):
        return cls.model_validate(filtered)
    return cls(**filtered)


def _present_families(spec: Any, source_pool: Any) -> dict[str, list[Any]]:
    cls_by_id = _classified_by_id(source_pool)
    by_family: dict[str, list[Any]] = {}
    for line in list(getattr(spec, "items", []) or []):
        fam = _line_family(line, cls_by_id)
        if not fam:
            continue
        by_family.setdefault(fam, []).append(line)
    return by_family


def _best_candidate_for_family(family: str, source_pool: Any) -> Any | None:
    cls_by_id = _classified_by_id(source_pool)
    best: tuple[float, Any] | None = None

    for item in _pool_items(source_pool):
        cid = getattr(item, "candidate_id", None)
        if not cid or str(cid) not in cls_by_id:
            continue
        cls = cls_by_id[str(cid)]
        if not cls or cls.family != family:
            continue

        score = float(getattr(cls, "family_confidence", 0.0) or 0.0) * 10.0
        score += 1.0 if getattr(item, "manufacturer", None) else 0.0
        score += 1.0 if getattr(item, "sku", None) else 0.0
        score += 0.75 if getattr(item, "unit_price_rub", None) not in (None, "", 0, 0.0) else 0.0
        score += 0.5 if getattr(item, "description", None) else 0.0
        ev = getattr(item, "evidence_task_ids", None) or []
        score += min(1.0, len(ev) * 0.2)

        if best is None or score > best[0]:
            best = (score, item)

    return best[1] if best else None


def _append_family_if_missing(
    spec: Any,
    source_pool: Any,
    present_families: dict[str, list[Any]],
    family: str,
    *,
    qty: int | float = 1,
    category: str = "misc",
) -> bool:
    if present_families.get(family):
        return False
    candidate = _best_candidate_for_family(family, source_pool)
    if candidate is None:
        return False
    line = _build_line_from_candidate(spec, candidate, qty=qty, category=category)
    getattr(spec, "items").append(line)
    present_families.setdefault(family, []).append(line)
    return True


def resolve_dependencies(
    spec: Any,
    source_pool: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
) -> list[str]:
    warnings: list[str] = []
    present_families = _present_families(spec, source_pool)

    has_delegate = bool(present_families.get("delegate_unit"))
    has_chairman = bool(present_families.get("chairman_unit"))
    has_discussion = has_delegate or has_chairman or topology.topology_key == "meeting_room_delegate_dsp"

    has_audio_processing = bool(present_families.get("dsp") or present_families.get("conference_controller"))
    has_speakers = bool(present_families.get("wall_speaker") or present_families.get("ceiling_speaker") or present_families.get("soundbar"))

    if has_discussion and not has_chairman:
        if not _append_family_if_missing(spec, source_pool, present_families, "chairman_unit", qty=1, category="conference"):
            warnings.append("missing_dependency: chairman_unit")

    if has_discussion and not present_families.get("conference_controller"):
        if not _append_family_if_missing(spec, source_pool, present_families, "conference_controller", qty=1, category="conference"):
            warnings.append("missing_dependency: conference_controller")

    if (has_discussion or has_speakers) and not has_audio_processing:
        if not _append_family_if_missing(spec, source_pool, present_families, "dsp", qty=1, category="conference"):
            warnings.append("missing_dependency: dsp")

    if present_families.get("wall_speaker"):
        needs_amp = False
        for line in present_families["wall_speaker"]:
            text = " ".join([str(getattr(line, "sku", "") or ""), str(getattr(line, "description", "") or ""), str(getattr(line, "name", "") or "")]).casefold()
            if "100v" in text or "трансляц" in text or "70v" in text or "speaker line" in text:
                needs_amp = True
                break

        if needs_amp and not present_families.get("amplifier"):
            if not _append_family_if_missing(spec, source_pool, present_families, "amplifier", qty=1, category="conference"):
                warnings.append("missing_dependency: amplifier")

    has_byod = bool(present_families.get("byod_usb_hdmi_gateway") or present_families.get("byod_wireless_presentation") or present_families.get("usb_c_dock"))
    has_switching = bool(
        present_families.get("presentation_switcher")
        or present_families.get("matrix_switcher")
        or present_families.get("simple_io_hub")
        or present_families.get("av_over_ip_tx")
        or present_families.get("av_over_ip_rx")
    )
    if has_byod and not has_switching:
        added = (
            _append_family_if_missing(spec, source_pool, present_families, "presentation_switcher", qty=1, category="signal_transport")
            or _append_family_if_missing(spec, source_pool, present_families, "simple_io_hub", qty=1, category="signal_transport")
            or _append_family_if_missing(spec, source_pool, present_families, "matrix_switcher", qty=1, category="signal_transport")
        )
        if not added:
            warnings.append("missing_dependency: switching_for_byod")

    has_any_cabling = bool(present_families.get("cable_cat") or present_families.get("cable_hdmi") or present_families.get("cable_usb") or present_families.get("adapters_kit"))
    if not has_any_cabling:
        added = (
            _append_family_if_missing(spec, source_pool, present_families, "cable_cat", qty=1, category="signal_transport")
            or _append_family_if_missing(spec, source_pool, present_families, "adapters_kit", qty=1, category="signal_transport")
        )
        if not added:
            warnings.append("missing_dependency: basic_cabling")

    _ = requirements
    return warnings