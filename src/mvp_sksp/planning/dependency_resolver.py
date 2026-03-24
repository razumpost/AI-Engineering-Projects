from __future__ import annotations

import uuid
from dataclasses import fields, is_dataclass
from typing import Any

from ..knowledge.audio_policy import audio_profile
from ..knowledge.models import ProjectRequirements
from .plan_models import TopologyDecision


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


def _make_item_key(prefix: str) -> str:
    return f"{prefix}::{uuid.uuid4().hex[:10]}"


def _build_placeholder_line(spec: Any, *, title: str, category: str, qty: int = 1) -> Any:
    cls = _line_class(spec)
    if cls is None:
        raise RuntimeError("Cannot create placeholder line: spec.items is empty")

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
        payload["item_key"] = _make_item_key("placeholder")
    if "candidate_id" in keys:
        payload["candidate_id"] = _make_item_key("ph")
    if "evidence" in keys:
        payload["evidence"] = {"bitrix_task_ids": []}
    if "evidence_task_ids" in keys:
        payload["evidence_task_ids"] = []

    filtered = {k: v for k, v in payload.items() if k in keys}
    if hasattr(cls, "model_validate"):
        return cls.model_validate(filtered)
    return cls(**filtered)


def resolve_dependencies(spec: Any, source_pool: Any, requirements: ProjectRequirements, topology: TopologyDecision) -> list[str]:
    warnings: list[str] = []
    items = list(getattr(spec, "items", []) or [])

    # Meeting-room audio policy: do not allow 100V
    if audio_profile(requirements) == "lowz":
        bad_100v = [x for x in items if ("100v" in (str(getattr(x, "description", "")) or "").casefold() or "70v" in (str(getattr(x, "description", "")) or "").casefold())]
        if bad_100v:
            # remove and add placeholder (if we have no clear lowZ candidate in pool)
            setattr(spec, "items", [x for x in items if x not in bad_100v])
            getattr(spec, "items").append(_build_placeholder_line(spec, title="Акустика для переговорной (низкоомная), подобрать", category="conference", qty=2))
            warnings.append("missing_dependency: meeting_room_audio_lowz")

    items = list(getattr(spec, "items", []) or [])

    # Remove videowall controller if slipped in (never BYOD in meeting room)
    if requirements.room_type == "meeting_room":
        bad_vw = [x for x in items if ("видеостен" in (str(getattr(x, "description", "")) or "").casefold() or "videowall" in (str(getattr(x, "description", "")) or "").casefold())]
        if bad_vw:
            setattr(spec, "items", [x for x in items if x not in bad_vw])
            warnings.append("[meeting_room] removed videowall controller items")

    # If BYOD required but no real byod device -> placeholder
    if bool(getattr(requirements.flags, "byod", False)):
        items = list(getattr(spec, "items", []) or [])
        has_byod = any("byod" in (str(getattr(x, "description", "")) or "").casefold() for x in items) or any("usb-c" in (str(getattr(x, "description", "")) or "").casefold() for x in items)
        if not has_byod:
            getattr(spec, "items").append(_build_placeholder_line(spec, title="BYOD модуль (USB-C/HDMI), подобрать", category="signal_transport", qty=1))
            warnings.append("missing_dependency: byod_device")

    # If switching role uncovered -> placeholder
    items = list(getattr(spec, "items", []) or [])
    has_switch = any("switch" in (str(getattr(x, "description", "")) or "").casefold() or "свитчер" in (str(getattr(x, "description", "")) or "").casefold() for x in items)
    if not has_switch:
        getattr(spec, "items").append(_build_placeholder_line(spec, title="Презентационный свитчер/хаб для переключения источников, подобрать", category="signal_transport", qty=1))
        warnings.append("missing_dependency: room_signal_switching")

    return warnings