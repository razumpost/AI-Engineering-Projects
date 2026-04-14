from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import uuid4

from ..knowledge.audio_policy import audio_profile
from ..knowledge.models import ProjectRequirements
from ..normalization.candidate_classifier import classify_candidates
from .plan_models import TopologyDecision


def _make_line_id() -> str:
    return f"li_{uuid4().hex[:12]}"


def _line_cls(spec: Any):
    items = list(getattr(spec, "items", []) or [])
    if items:
        return items[0].__class__
    return None


def _model_keys(cls: Any) -> set[str]:
    if cls is None:
        return set()

    if hasattr(cls, "model_fields"):
        return set(cls.model_fields.keys())

    if hasattr(cls, "__fields__"):
        return set(cls.__fields__.keys())

    ann = getattr(cls, "__annotations__", None)
    if isinstance(ann, dict):
        return set(ann.keys())

    return set()


def _instantiate_line(cls: Any, payload: dict[str, Any]) -> Any:
    if cls is None:
        return SimpleNamespace(**payload)

    if hasattr(cls, "model_validate"):
        return cls.model_validate(payload)

    return cls(**payload)


def _find_existing_placeholder(spec: Any, kind: str) -> Any | None:
    for line in list(getattr(spec, "items", []) or []):
        meta = getattr(line, "meta", None)
        if isinstance(meta, dict) and meta.get("placeholder_kind") == kind:
            return line
    return None


def _build_placeholder_line(
    spec: Any,
    *,
    kind: str,
    title: str,
    category: str,
    qty: int,
) -> Any:
    cls = _line_cls(spec)
    keys = _model_keys(cls)

    payload = {
        "line_id": _make_line_id(),
        "item_key": f"ph::{kind}",
        "candidate_id": None,
        "category": category,
        "manufacturer": "Уточнить",
        "sku": "—",
        "model": None,
        "name": title,
        "description": title,
        "qty": qty,
        "unit_price": None,
        "evidence_task_ids": [],
        "evidence": {"bitrix_task_ids": []},
        "meta": {
            "placeholder_kind": kind,
        },
    }

    if keys:
        payload = {k: v for k, v in payload.items() if k in keys}

    return _instantiate_line(cls, payload)


def _present_families(spec: Any) -> dict[str, int]:
    items = list(getattr(spec, "items", []) or [])
    classified = classify_candidates(items)

    out: dict[str, int] = {}
    for c in classified:
        if not c.family:
            continue
        out[c.family] = out.get(c.family, 0) + 1
    return out


def _ensure_placeholder(
    *,
    spec: Any,
    fam_present: dict[str, int],
    family: str,
    kind: str,
    title: str,
    category: str,
    qty: int,
    warnings: list[str],
) -> None:
    if fam_present.get(family):
        return

    if _find_existing_placeholder(spec, kind) is not None:
        return

    getattr(spec, "items").append(
        _build_placeholder_line(
            spec,
            kind=kind,
            title=title,
            category=category,
            qty=qty,
        )
    )
    fam_present[family] = fam_present.get(family, 0) + 1
    warnings.append(f"missing_dependency: {family}")


def _is_discussion_mode(spec: Any, topology: TopologyDecision) -> bool:
    fam_present = _present_families(spec)

    if topology.topology_key == "meeting_room_discussion_only":
        return True

    return any(
        fam_present.get(f)
        for f in [
            "delegate_unit",
            "chairman_unit",
            "discussion_central_unit",
            "power_supply_discussion",
            "discussion_dsp",
        ]
    )


def resolve_dependencies(
    spec: Any,
    source_pool: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
) -> list[str]:
    _ = source_pool
    warnings: list[str] = []

    fam_present = _present_families(spec)
    seat_count = int(requirements.caps.seat_count or 0)

    discussion_mode = _is_discussion_mode(spec, topology)

    if discussion_mode:
        _ensure_placeholder(
            spec=spec,
            fam_present=fam_present,
            family="chairman_unit",
            kind="chairman_unit",
            title="Пульт председателя дискуссионной системы, подобрать",
            category="conference",
            qty=1,
            warnings=warnings,
        )

        _ensure_placeholder(
            spec=spec,
            fam_present=fam_present,
            family="discussion_central_unit",
            kind="discussion_central_unit",
            title="Центральный блок дискуссионной системы, подобрать",
            category="conference",
            qty=1,
            warnings=warnings,
        )

        if seat_count > 20:
            _ensure_placeholder(
                spec=spec,
                fam_present=fam_present,
                family="power_supply_discussion",
                kind="power_supply_discussion",
                title="Блок питания / расширения дискуссионной системы, подобрать",
                category="conference",
                qty=1,
                warnings=warnings,
            )

        _ensure_placeholder(
            spec=spec,
            fam_present=fam_present,
            family="discussion_dsp",
            kind="discussion_dsp",
            title="Аудиопроцессор (DSP) для интеграции дискуссионной системы со звуком, подобрать",
            category="conference",
            qty=1,
            warnings=warnings,
        )

    if requirements.room_type == "meeting_room":
        has_playback = bool(
            fam_present.get("soundbar")
            or fam_present.get("wall_speaker")
            or fam_present.get("ceiling_speaker")
            or fam_present.get("speakerphone")
        )

        if not has_playback:
            profile = audio_profile(requirements)
            if profile == "lowz":
                if _find_existing_placeholder(spec, "audio_lowz") is None:
                    getattr(spec, "items").append(
                        _build_placeholder_line(
                            spec,
                            kind="audio_lowz",
                            title="Акустика для переговорной (низкоомная), подобрать",
                            category="conference",
                            qty=2,
                        )
                    )
                warnings.append("missing_dependency: meeting_room_audio_lowz")

    return warnings