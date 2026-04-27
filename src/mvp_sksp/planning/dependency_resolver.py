from __future__ import annotations

from decimal import Decimal
import re
from typing import Any
from uuid import uuid4

from ..domain.spec import LineItem
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
    # Единый контракт: placeholder-строки должны быть валидными LineItem.
    return LineItem


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
    if hasattr(cls, "model_validate"):
        return cls.model_validate(payload)

    if cls is None:
        return LineItem.model_validate(payload)

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


def _pool_items(pool: Any) -> list[Any]:
    return list(getattr(pool, "items", []) or [])


def _placeholder_kind(line: Any) -> str | None:
    meta = getattr(line, "meta", None)
    if isinstance(meta, dict):
        v = meta.get("placeholder_kind")
        if v:
            return str(v)
    return None


def _candidate_task_ids(ci: Any) -> list[int]:
    out: list[int] = []
    for v in list(getattr(ci, "evidence_task_ids", []) or []):
        try:
            out.append(int(v))
        except Exception:
            pass
    return out


def _candidate_text(ci: Any) -> str:
    return " ".join(
        [
            str(getattr(ci, "name", "") or ""),
            str(getattr(ci, "description", "") or ""),
            str(getattr(ci, "model", "") or ""),
            str(getattr(ci, "sku", "") or ""),
        ]
    ).casefold()


def _clean_supplier_tail(text: str) -> str:
    s = (text or "").strip()
    if "|" not in s:
        return s
    parts = [p.strip() for p in s.split("|")]
    if len(parts) < 3:
        return s
    numeric_tail = 0
    for p in parts[1:]:
        if re.fullmatch(r"[\d\s.,]+(?:дн(?:ей|я)?|дней)?", p.casefold()):
            numeric_tail += 1
    if numeric_tail >= 2:
        return parts[0]
    return s


def _sanitize_text(text: Any) -> str:
    s = str(text or "").replace("\u00a0", " ").strip()
    s = _clean_supplier_tail(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _candidate_unit_price(ci: Any) -> Decimal | None:
    raw = getattr(ci, "unit_price_rub", None)
    if raw is not None:
        try:
            return Decimal(str(raw))
        except Exception:
            pass
    meta = getattr(ci, "meta", None) or {}
    if isinstance(meta, dict):
        for key in ("unit_price_rub", "price_rub", "price"):
            if meta.get(key) is not None:
                try:
                    return Decimal(str(meta.get(key)))
                except Exception:
                    pass
    return None


def _candidate_score(ci: Any, confidence: float) -> float:
    score = float(confidence or 0.0) * 100.0
    if getattr(ci, "manufacturer", None):
        score += 8.0
    if getattr(ci, "sku", None):
        score += 8.0
    if getattr(ci, "unit_price_rub", None) is not None:
        score += 5.0
    if _candidate_task_ids(ci):
        score += 12.0
    if getattr(ci, "description", None):
        score += 2.0
    return score


def _role_text_bonus(role_kind: str, text: str) -> float:
    score = 0.0
    if role_kind == "videowall_panel":
        if any(x in text for x in ["videowall", "видеостен", "video wall", "lcd videowall"]):
            score += 20.0
        if any(x in text for x in ["panel", "панель", "narrow bezel", "ultra narrow bezel"]):
            score += 8.0
    elif role_kind == "videowall_controller":
        if _looks_controller_like(text):
            score += 60.0
        if _looks_matrix_like(text) and not _looks_controller_like(text):
            score -= 80.0
    elif role_kind == "matrix_switcher":
        if _looks_matrix_like(text):
            score += 60.0
        if _looks_controller_like(text) and not _looks_matrix_like(text):
            score -= 80.0
    elif role_kind == "videowall_mount":
        if any(x in text for x in ["mount", "pull-out", "bracket", "кронштейн", "каркас"]):
            score += 16.0
    elif role_kind == "cabling_av":
        if any(x in text for x in ["cable", "кабель", "hdmi", "displayport", "cat"]):
            score += 12.0
    return score


def _looks_controller_like(text: str) -> bool:
    return any(
        x in text
        for x in [
            "videowall controller",
            "video wall controller",
            "videowall processor",
            "video wall processor",
            "controller",
            "processor",
            "контроллер",
            "процессор",
        ]
    ) or bool(re.search(r"\bvdn\d+\s*x\s*\d+", text) or re.search(r"\bvdn\d+x\d+", text))


def _looks_matrix_like(text: str) -> bool:
    return any(
        x in text
        for x in [
            "matrix switcher",
            "hdmi matrix",
            "matrix",
            "switcher",
            "матрич",
            "коммутатор",
            "kramer",
            "vsm-",
        ]
    )


def _looks_cabling_like(text: str) -> bool:
    return any(
        x in text
        for x in [
            "cable",
            "кабель",
            "hdmi cable",
            "displayport cable",
            "patch cord",
            "патч-корд",
            "cat5",
            "cat6",
            "utp",
            "sftp",
            "connector",
            "adapter",
            "переходник",
            "разъем",
            "разъём",
        ]
    )


def _looks_non_product_row(text: str) -> bool:
    company_like = any(
        x in text
        for x in [
            "ооо ",
            " ооо",
            "зао ",
            " зао",
            "пао ",
            " пао",
            "ао ",
            " ао",
            "ип ",
            "llc",
            "ltd",
            "инжиниринг",
            "engineering",
        ]
    )
    contract_like = any(
        x in text
        for x in [
            "договор",
            "контрагент",
            "реестр",
            "акт ",
            "счет ",
            "счёт ",
            "contract",
            "counterparty",
            "registry",
        ]
    )
    sku_date_like = bool(re.search(r"\b20\d{2}[-./]\d{2}[-./]\d{2}\b", text))
    return company_like or contract_like or sku_date_like


def _candidate_has_product_grounding(ci: Any, text: str) -> bool:
    if _looks_non_product_row(text):
        return False
    manufacturer = _sanitize_text(getattr(ci, "manufacturer", ""))
    sku = _sanitize_text(getattr(ci, "sku", ""))
    model = _sanitize_text(getattr(ci, "model", ""))
    name = _sanitize_text(getattr(ci, "name", ""))
    has_identity = bool(
        (manufacturer and manufacturer != "Уточнить")
        or (sku and sku != "—")
        or model
        or name
    )
    return has_identity and bool(_candidate_task_ids(ci) or _candidate_unit_price(ci) is not None or sku or model)


def _candidate_eligible_for_role(role_kind: str, family: str | None, text: str) -> bool:
    if _looks_non_product_row(text):
        return False

    fam = str(family or "")
    if role_kind == "videowall_controller":
        return fam == "videowall_controller" or _looks_controller_like(text)
    if role_kind == "matrix_switcher":
        return fam == "matrix_switcher" or (_looks_matrix_like(text) and not _looks_controller_like(text))
    if role_kind == "cabling_av":
        return _looks_cabling_like(text) and not (_looks_controller_like(text) or _looks_matrix_like(text))
    if role_kind == "videowall_mount":
        return fam in {"videowall_mount", "mounting_kit"} and any(
            x in text for x in ["mount", "pull-out", "bracket", "кронштейн", "каркас", "frame"]
        )
    if role_kind == "videowall_panel":
        if fam == "videowall_panel":
            return True
        if fam == "display_panel":
            return any(x in text for x in ["videowall", "видеостен", "video wall", "narrow bezel", "lcd videowall"])
    return False


def _pick_best_candidate_for_role(
    source_pool: Any,
    role_kind: str,
    families: list[str],
    used_candidate_ids: set[str],
) -> Any | None:
    fam_set = set(families)
    best: tuple[float, Any] | None = None
    for cls, ci in zip(classify_candidates(_pool_items(source_pool)), _pool_items(source_pool)):
        cid = str(getattr(ci, "candidate_id", "") or "")
        if not cid or cid in used_candidate_ids:
            continue
        text = _candidate_text(ci)
        if not _candidate_has_product_grounding(ci, text):
            continue
        if not _candidate_eligible_for_role(role_kind, cls.family, text):
            continue
        # Text-first exceptions are deliberate for VDN/controller-like rows whose
        # family classifier may be too broad. Other roles stay family-gated.
        if cls.family not in fam_set and role_kind not in {"videowall_controller", "cabling_av"}:
            continue
        score = _candidate_score(ci, float(getattr(cls, "family_confidence", 0.0) or 0.0))
        score += _role_text_bonus(role_kind, text)
        if best is None or score > best[0]:
            best = (score, ci)
    return best[1] if best else None


def _ground_line_from_candidate(line: Any, ci: Any, role_kind: str) -> None:
    role_category = {
        "videowall_panel": "display",
        "videowall_mount": "signal_transport",
        "matrix_switcher": "signal_transport",
        "videowall_controller": "processing",
        "cabling_av": "signal_transport",
    }

    manufacturer = _sanitize_text(getattr(ci, "manufacturer", None) or getattr(line, "manufacturer", None))
    sku = _sanitize_text(getattr(ci, "sku", None) or getattr(line, "sku", None))
    model = _sanitize_text(getattr(ci, "model", None) or getattr(line, "model", None))
    name = _sanitize_text(getattr(ci, "name", None) or getattr(line, "name", None))
    description = _sanitize_text(getattr(ci, "description", None) or getattr(line, "description", None))
    if not name and description:
        name = description

    composed_parts = [x for x in [manufacturer, model or sku, name] if x and x not in {"—", "Уточнить"}]
    composed_description = " — ".join(composed_parts)
    if not description or re.fullmatch(r"[\d\s.,]+", description):
        description = composed_description or name or "Позиция"
    elif role_kind in {"matrix_switcher", "videowall_mount", "videowall_controller"} and composed_description:
        description = composed_description

    setattr(line, "manufacturer", manufacturer)
    setattr(line, "sku", sku)
    setattr(line, "model", model)
    setattr(line, "name", name or "Позиция")
    setattr(line, "description", description or name or "Позиция")
    if role_kind in role_category:
        setattr(line, "category", role_category[role_kind])

    unit_price = _candidate_unit_price(ci)
    if unit_price is not None:
        try:
            setattr(line, "unit_price", {"amount": Decimal(str(unit_price)), "currency": "RUB"})
        except Exception:
            pass

    ev = getattr(line, "evidence", None)
    task_ids = _candidate_task_ids(ci)
    if ev is not None and hasattr(ev, "bitrix_task_ids"):
        setattr(ev, "bitrix_task_ids", task_ids)
        notes = list(getattr(ev, "notes", []) or [])
        notes.append(f"grounded_from_candidate:{getattr(ci, 'candidate_id', '')}")
        setattr(ev, "notes", notes[-8:])

    meta = dict(getattr(line, "meta", {}) or {})
    meta.pop("placeholder_kind", None)
    meta["grounded_from_role"] = role_kind
    meta["candidate_id"] = str(getattr(ci, "candidate_id", "") or "")
    setattr(line, "meta", meta)
    setattr(line, "item_key", f"cid:{meta['candidate_id']}" if meta["candidate_id"] else getattr(line, "item_key", ""))


def _ground_videowall_placeholders(spec: Any, source_pool: Any, warnings: list[str]) -> None:
    if source_pool is None:
        return

    role_to_families: dict[str, list[str]] = {
        "videowall_panel": ["videowall_panel", "display_panel"],
        "videowall_controller": ["videowall_controller", "matrix_switcher"],
        "matrix_switcher": ["matrix_switcher", "videowall_controller"],
        "videowall_mount": ["videowall_mount", "mounting_kit"],
        "cabling_av": ["cabling_av", "power_accessories"],
    }
    used_candidate_ids: set[str] = set()

    for line in list(getattr(spec, "items", []) or []):
        role_kind = _placeholder_kind(line)
        if not role_kind or role_kind not in role_to_families:
            continue

        ci = _pick_best_candidate_for_role(source_pool, role_kind, role_to_families[role_kind], used_candidate_ids)
        if ci is None:
            continue

        cid = str(getattr(ci, "candidate_id", "") or "")
        if cid:
            used_candidate_ids.add(cid)
        _ground_line_from_candidate(line, ci, role_kind)
        warnings.append(f"placeholder_grounded:{role_kind}:{cid or 'unknown'}")


def _drop_redundant_videowall_placeholders(spec: Any) -> None:
    # Если роль уже закрыта grounded line, одноименный placeholder удаляем.
    closed_roles: set[str] = set()
    for line in list(getattr(spec, "items", []) or []):
        meta = dict(getattr(line, "meta", {}) or {})
        grounded_role = meta.get("grounded_from_role")
        if grounded_role:
            closed_roles.add(str(grounded_role))

    if not closed_roles:
        return

    kept: list[Any] = []
    for line in list(getattr(spec, "items", []) or []):
        role_kind = _placeholder_kind(line)
        if role_kind and role_kind in closed_roles:
            continue
        kept.append(line)
    setattr(spec, "items", kept)


def _line_grounded_role(line: Any) -> str | None:
    meta = getattr(line, "meta", None)
    if isinstance(meta, dict):
        role = meta.get("grounded_from_role")
        if role:
            return str(role)
    return None


def _role_from_family(family: str | None) -> str | None:
    fam_to_role = {
        "videowall_panel": "videowall_panel",
        "display_panel": "videowall_panel",
        "videowall_mount": "videowall_mount",
        "mounting_kit": "videowall_mount",
        "videowall_controller": "videowall_controller",
        "matrix_switcher": "matrix_switcher",
        "cabling_av": "cabling_av",
        "power_accessories": "cabling_av",
    }
    return fam_to_role.get(str(family or ""))


def _line_quality_score(line: Any) -> int:
    s = 0
    if _line_grounded_role(line):
        s += 100
    if getattr(line, "manufacturer", None) and str(getattr(line, "manufacturer", "")).strip() not in {"", "Уточнить"}:
        s += 20
    if getattr(line, "sku", None) and str(getattr(line, "sku", "")).strip() not in {"", "—"}:
        s += 20
    ev = getattr(line, "evidence", None)
    s += min(20, 5 * len(list(getattr(ev, "bitrix_task_ids", []) or [])))
    if getattr(line, "unit_price", None) is not None:
        s += 8
    return s


def _dedupe_videowall_roles(spec: Any, source_pool: Any) -> None:
    cls_by_id = {}
    try:
        cls_by_id = {c.candidate_id: c for c in classify_candidates(_pool_items(source_pool))}
    except Exception:
        cls_by_id = {}

    role_lines: dict[str, list[Any]] = {}
    pass_through: list[Any] = []
    for line in list(getattr(spec, "items", []) or []):
        role = _line_grounded_role(line)
        if not role:
            role = _placeholder_kind(line)
        if not role:
            cid = str((getattr(line, "meta", None) or {}).get("candidate_id") or "")
            fam = cls_by_id.get(cid).family if cid and cid in cls_by_id else None
            role = _role_from_family(fam)

        if role in {"videowall_panel", "videowall_mount", "videowall_controller", "matrix_switcher", "cabling_av"}:
            role_lines.setdefault(role, []).append(line)
        else:
            pass_through.append(line)

    for role, lines in role_lines.items():
        lines.sort(key=_line_quality_score, reverse=True)
        pass_through.append(lines[0])

    setattr(spec, "items", pass_through)


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


def _is_discussion_mode(spec: Any, topology: TopologyDecision, requirements: ProjectRequirements) -> bool:
    fam_present = _present_families(spec)

    if topology.topology_key == "meeting_room_discussion_only":
        return True

    if bool(requirements.flags.control) and int(requirements.caps.seat_count or 0) >= 8:
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


def _has_any_family(fam_present: dict[str, int], families: list[str]) -> bool:
    return any(fam_present.get(f) for f in families)


def _ensure_meeting_room_core_placeholders(
    *,
    spec: Any,
    fam_present: dict[str, int],
    requirements: ProjectRequirements,
    warnings: list[str],
) -> None:
    display_count = int(requirements.caps.display_count or 0)
    camera_count = int(requirements.caps.camera_count or 0)

    has_display = _has_any_family(fam_present, ["display_panel", "interactive_panel"])
    has_camera = _has_any_family(fam_present, ["ptz_camera", "fixed_conference_camera", "videobar"])
    has_audio_capture = _has_any_family(fam_present, ["tabletop_mic", "ceiling_mic_array", "speakerphone", "videobar"])
    has_switching = _has_any_family(
        fam_present,
        ["presentation_switcher", "matrix_switcher", "av_over_ip_tx", "av_over_ip_rx", "simple_io_hub", "byod_usb_hdmi_gateway", "usb_c_dock"],
    )

    if display_count > 0 and not has_display:
        _ensure_placeholder(
            spec=spec,
            fam_present=fam_present,
            family="display_panel",
            kind="room_display_main",
            title="Профессиональный дисплей для переговорной, подобрать",
            category="display",
            qty=max(1, display_count),
            warnings=warnings,
        )

    if camera_count > 0 and not has_camera:
        _ensure_placeholder(
            spec=spec,
            fam_present=fam_present,
            family="ptz_camera",
            kind="room_camera_main",
            title="Камера ВКС / PTZ для переговорной, подобрать",
            category="conference",
            qty=1,
            warnings=warnings,
        )

        if camera_count > 1:
            _ensure_placeholder(
                spec=spec,
                fam_present=fam_present,
                family="fixed_conference_camera",
                kind="room_camera_secondary",
                title="Дополнительная камера ВКС для переговорной, подобрать",
                category="conference",
                qty=max(1, camera_count - 1),
                warnings=warnings,
            )

    if not has_audio_capture:
        _ensure_placeholder(
            spec=spec,
            fam_present=fam_present,
            family="tabletop_mic",
            kind="room_audio_capture",
            title="Микрофонная подсистема переговорной, подобрать",
            category="conference",
            qty=1,
            warnings=warnings,
        )

    if not has_switching and (requirements.flags.presentation or requirements.flags.vks or display_count > 0):
        _ensure_placeholder(
            spec=spec,
            fam_present=fam_present,
            family="presentation_switcher",
            kind="room_signal_switching",
            title="Коммутатор / BYOD-шлюз / USB bridge для переговорной, подобрать",
            category="signal_transport",
            qty=1,
            warnings=warnings,
        )


def _is_videowall_mode(spec: Any, requirements: ProjectRequirements) -> bool:
    if requirements.room_type == "videowall":
        return True
    fam_present = _present_families(spec)
    return _has_any_family(
        fam_present,
        [
            "videowall_panel",
            "videowall_mount",
            "videowall_controller",
            "matrix_switcher",
        ],
    )


def _needs_videowall_matrix(requirements: ProjectRequirements) -> bool:
    # `many_av_io_ports` пока не сохранен в requirements.flags.
    # Консервативно включаем матричную коммутацию, если явно нужен control
    # или видеостена многопанельная.
    display_count = int(requirements.caps.display_count or 0)
    return bool(requirements.flags.control) or display_count >= 4


def _ensure_videowall_core_placeholders(
    *,
    spec: Any,
    fam_present: dict[str, int],
    requirements: ProjectRequirements,
    warnings: list[str],
) -> None:
    display_count = int(requirements.caps.display_count or 0)

    has_panel = _has_any_family(fam_present, ["videowall_panel"])
    has_mount = _has_any_family(fam_present, ["videowall_mount", "mounting_kit"])
    has_controller = _has_any_family(fam_present, ["videowall_controller"])
    has_matrix = _has_any_family(fam_present, ["matrix_switcher"])
    has_cabling = _has_any_family(fam_present, ["cabling_av", "power_accessories"])

    if not has_panel:
        _ensure_placeholder(
            spec=spec,
            fam_present=fam_present,
            family="videowall_panel",
            kind="videowall_panel",
            title="Панели видеостены (LCD videowall), подобрать",
            category="display",
            qty=max(1, display_count),
            warnings=warnings,
        )

    if not has_mount:
        _ensure_placeholder(
            spec=spec,
            fam_present=fam_present,
            family="videowall_mount",
            kind="videowall_mount",
            title="Крепление / каркас видеостены, подобрать",
            category="signal_transport",
            qty=1,
            warnings=warnings,
        )

    if not has_controller:
        _ensure_placeholder(
            spec=spec,
            fam_present=fam_present,
            family="videowall_controller",
            kind="videowall_controller",
            title="Контроллер / процессор видеостены, подобрать",
            category="processing",
            qty=1,
            warnings=warnings,
        )

    if _needs_videowall_matrix(requirements) and not has_matrix:
        _ensure_placeholder(
            spec=spec,
            fam_present=fam_present,
            family="matrix_switcher",
            kind="matrix_switcher",
            title="Матричный коммутатор видеостены, подобрать",
            category="signal_transport",
            qty=1,
            warnings=warnings,
        )

    if not has_cabling:
        _ensure_placeholder(
            spec=spec,
            fam_present=fam_present,
            family="cabling_av",
            kind="cabling_av",
            title="Коммутация и кабельная система видеостены, подобрать",
            category="signal_transport",
            qty=1,
            warnings=warnings,
        )


def resolve_dependencies(
    spec: Any,
    source_pool: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
) -> list[str]:
    warnings: list[str] = []

    fam_present = _present_families(spec)
    seat_count = int(requirements.caps.seat_count or 0)

    discussion_mode = _is_discussion_mode(spec, topology, requirements)
    videowall_mode = _is_videowall_mode(spec, requirements)

    if videowall_mode:
        _ensure_videowall_core_placeholders(
            spec=spec,
            fam_present=fam_present,
            requirements=requirements,
            warnings=warnings,
        )
        _ground_videowall_placeholders(spec, source_pool, warnings)
        _drop_redundant_videowall_placeholders(spec)
        _dedupe_videowall_roles(spec, source_pool)

    if discussion_mode:
        # ЖЕСТКО: для discussion baseline эти линии должны быть всегда, если нет placeholder_kind
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

    # ordinary meeting-room placeholders не должны добавляться в discussion-mode
    if requirements.room_type == "meeting_room" and not discussion_mode:
        _ensure_meeting_room_core_placeholders(
            spec=spec,
            fam_present=fam_present,
            requirements=requirements,
            warnings=warnings,
        )

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