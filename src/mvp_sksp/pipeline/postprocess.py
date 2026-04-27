from __future__ import annotations

import os
import re
from typing import Any, Sequence

from ..knowledge.models import ProjectRequirements
from ..planning.dependency_resolver import resolve_dependencies
from ..planning.plan_models import TopologyDecision
from ..planning.quantity_resolver import resolve_quantities
from ..planning.role_expander import ExpandedRole
from .explain_fallback import build_fallback_explanations
from .price_validator import validate_prices
from .spec_mapper import (
    _classified_by_id,
    _line_family,
    merge_duplicate_candidate_lines,
    normalize_categories,
    sort_spec_items,
)

__all__ = ["postprocess_spec"]


_VIDEOWALL_ROLES = {
    "videowall_panel",
    "videowall_mount",
    "videowall_controller",
    "matrix_switcher",
    "cabling_av",
}


def _line_meta(line: Any) -> dict[str, Any]:
    meta = getattr(line, "meta", None)
    return dict(meta) if isinstance(meta, dict) else {}


def _line_grounded_role(line: Any) -> str | None:
    role = _line_meta(line).get("grounded_from_role")
    return str(role) if role else None


def _line_placeholder_kind(line: Any) -> str | None:
    kind = _line_meta(line).get("placeholder_kind")
    return str(kind) if kind else None


def _line_candidate_id(line: Any) -> str:
    meta = _line_meta(line)
    cid = meta.get("candidate_id")
    if cid:
        return str(cid)
    cid2 = getattr(line, "candidate_id", None)
    return str(cid2 or "")


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


def _line_text(line: Any) -> str:
    return " ".join(
        [
            str(getattr(line, "name", "") or ""),
            str(getattr(line, "description", "") or ""),
            str(getattr(line, "model", "") or ""),
            str(getattr(line, "sku", "") or ""),
        ]
    ).casefold()


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
        for x in ["matrix switcher", "hdmi matrix", "matrix", "switcher", "матрич", "коммутатор", "kramer", "vsm-"]
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


def _correct_videowall_role(role: str | None, family: str | None, text: str) -> str | None:
    if _looks_non_product_row(text):
        return None

    fam_role = _role_from_family(family)

    if role == "cabling_av" and not _looks_cabling_like(text):
        if _looks_controller_like(text):
            return "videowall_controller"
        if _looks_matrix_like(text):
            return "matrix_switcher"
        return None

    if role == "videowall_controller" and _looks_matrix_like(text) and not _looks_controller_like(text):
        return "matrix_switcher"

    if role == "matrix_switcher" and _looks_controller_like(text) and not _looks_matrix_like(text):
        return "videowall_controller"

    return role or fam_role


def _resolve_videowall_role(line: Any, cls_by_id: dict[str, Any]) -> str | None:
    role = _line_grounded_role(line) or _line_placeholder_kind(line)
    cid = _line_candidate_id(line)
    family = cls_by_id.get(cid).family if cid and cid in cls_by_id else None
    text = _line_text(line)
    corrected = _correct_videowall_role(role if role in _VIDEOWALL_ROLES else None, family, text)
    return corrected if corrected in _VIDEOWALL_ROLES else None


def _line_evidence_count(line: Any) -> int:
    ev = getattr(line, "evidence", None)
    ids = list(getattr(ev, "bitrix_task_ids", []) or [])
    return len(ids)


def _line_quality_score(line: Any) -> int:
    score = 0
    if _line_grounded_role(line):
        score += 100
    man = str(getattr(line, "manufacturer", "") or "").strip()
    if man and man != "Уточнить":
        score += 20
    sku = str(getattr(line, "sku", "") or "").strip()
    if sku and sku != "—":
        score += 20
    score += min(20, 5 * _line_evidence_count(line))
    if getattr(line, "unit_price", None) is not None:
        score += 8
    return score


def _sanitize_supplier_tail(text: str) -> str:
    s = (text or "").strip()
    if "|" not in s:
        return s
    parts = [p.strip() for p in s.split("|")]
    if len(parts) < 3:
        return s
    numeric_tail = 0
    for p in parts[1:]:
        if re.fullmatch(r"[\d\s.,]+(?:дн(?:ей|я)?|days?)?", p.casefold()):
            numeric_tail += 1
    if numeric_tail >= 2:
        return parts[0]
    return s


def _sanitize_text(v: Any) -> str:
    s = str(v or "").replace("\u00a0", " ").strip()
    s = _sanitize_supplier_tail(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _looks_numeric_short(s: str) -> bool:
    t = (s or "").strip()
    if not t:
        return False
    return bool(re.fullmatch(r"[\d\s.,]+", t)) and len(t) <= 12


def _sanitize_line_text_fields(line: Any) -> None:
    raw_name = _sanitize_text(getattr(line, "name", ""))
    raw_desc = _sanitize_text(getattr(line, "description", ""))
    raw_model = _sanitize_text(getattr(line, "model", ""))

    best = raw_desc or raw_name or raw_model or "Позиция"
    if _looks_numeric_short(best):
        for alt in [raw_name, raw_desc, raw_model]:
            if alt and not _looks_numeric_short(alt):
                best = alt
                break
    if _looks_numeric_short(best):
        best = "Позиция, требуется уточнение"

    # Description should be the most human-readable field for exporter.
    setattr(line, "description", best)
    if not raw_name or _looks_numeric_short(raw_name):
        setattr(line, "name", best)
    else:
        setattr(line, "name", raw_name)


def _enforce_videowall_roles_and_debug(spec: Any, source_pool: Any) -> None:
    cls_by_id = _classified_by_id(source_pool)
    role_to_category = {
        "videowall_panel": "display",
        "videowall_mount": "signal_transport",
        "matrix_switcher": "signal_transport",
        "videowall_controller": "processing",
        "cabling_av": "signal_transport",
    }

    role_lines: dict[str, list[Any]] = {}
    passthrough: list[Any] = []
    for line in list(getattr(spec, "items", []) or []):
        original_role = _line_grounded_role(line) or _line_placeholder_kind(line)
        role = _resolve_videowall_role(line, cls_by_id)
        _sanitize_line_text_fields(line)
        if role in _VIDEOWALL_ROLES:
            meta = _line_meta(line)
            meta["grounded_from_role"] = role
            setattr(line, "meta", meta)
            setattr(line, "category", role_to_category[role])
            role_lines.setdefault(role, []).append(line)
        else:
            # Drop previously grounded videowall rows that are now rejected by
            # final product/role sanity checks. Placeholders remain if no valid
            # product candidate exists.
            if original_role in _VIDEOWALL_ROLES:
                continue
            passthrough.append(line)

    # Role-level dedupe: keep one best line per videowall role.
    for role, lines in role_lines.items():
        lines.sort(key=_line_quality_score, reverse=True)
        passthrough.append(lines[0])

    setattr(spec, "items", passthrough)

    if os.getenv("MVP_SKSP_DEBUG_VIDEOWALL", "").strip() == "1":
        print("[debug_videowall] final role/category/description before export")
        for line in list(getattr(spec, "items", []) or []):
            role = _resolve_videowall_role(line, cls_by_id)
            if role in _VIDEOWALL_ROLES:
                print(
                    f"- role={role} "
                    f"cid={_line_candidate_id(line)} "
                    f"category={getattr(line, 'category', None)} "
                    f"description={getattr(line, 'description', '')!r} "
                    f"quality={_line_quality_score(line)}"
                )


def _has_discussion_intent(requirements: ProjectRequirements, topology: TopologyDecision) -> bool:
    if topology.topology_key == "meeting_room_discussion_only":
        return True

    try:
        conf = dict(getattr(requirements, "confidence", {}) or {})
        if float(conf.get("discussion", 0.0) or 0.0) > 0:
            return True
    except Exception:
        pass

    seat_count = int(requirements.caps.seat_count or 0)
    camera_count = int(requirements.caps.camera_count or 0)
    display_count = int(requirements.caps.display_count or 0)

    if bool(requirements.flags.control) and seat_count >= 8 and camera_count == 0 and display_count == 0:
        return True

    return False


def _allowed_families(
    roles: Sequence[ExpandedRole],
    topology: TopologyDecision,
    requirements: ProjectRequirements,
) -> set[str]:
    allowed: set[str] = set()

    for role in roles:
        allowed.update(role.allowed_families or [])
        allowed.update(role.preferred_families or [])

    for fams in (topology.preferred_families or {}).values():
        allowed.update(fams or [])

    allowed.update(
        {
            "cabling_av",
            "mounting_kit",
            "power_accessories",
            "power_supply_discussion",
            "managed_switch",
            "poe_switch",
            "discussion_central_unit",
            "discussion_dsp",
            "conference_controller",
        }
    )

    if _has_discussion_intent(requirements, topology):
        allowed.update(
            {
                "delegate_unit",
                "chairman_unit",
                "discussion_central_unit",
                "discussion_dsp",
                "power_supply_discussion",
                "tabletop_mic",
                "ceiling_mic_array",
            }
        )

    return allowed


def _discussion_forbidden_families() -> set[str]:
    return {
        "display_panel",
        "interactive_panel",
        "projector",
        "projection_screen",
        "ptz_camera",
        "fixed_conference_camera",
        "videobar",
        "presentation_switcher",
        "matrix_switcher",
        "simple_io_hub",
        "byod_usb_hdmi_gateway",
        "byod_wireless_presentation",
        "usb_c_dock",
    }


def _prune_items_outside_allowed_families(
    spec: Any,
    source_pool: Any,
    roles: Sequence[ExpandedRole],
    topology: TopologyDecision,
    requirements: ProjectRequirements,
) -> list[str]:
    allowed = _allowed_families(roles, topology, requirements)
    cls_by_id = _classified_by_id(source_pool)

    discussion_intent = _has_discussion_intent(requirements, topology)
    discussion_blocked = _discussion_forbidden_families()

    items = list(getattr(spec, "items", []) or [])
    kept: list[Any] = []
    warnings: list[str] = []

    for line in items:
        fam = _line_family(line, cls_by_id)

        if not fam:
            kept.append(line)
            continue

        if discussion_intent and fam in discussion_blocked:
            warnings.append(
                f"Line dropped by discussion gate: family={fam}, sku={getattr(line, 'sku', '')}, name={getattr(line, 'name', '')}"
            )
            continue

        if fam not in allowed:
            warnings.append(
                f"Line dropped by family gate: family={fam}, sku={getattr(line, 'sku', '')}, name={getattr(line, 'name', '')}"
            )
            continue

        kept.append(line)

    setattr(spec, "items", kept)
    return warnings


def postprocess_spec(
    *,
    spec: Any,
    filtered_pool: Any,
    source_pool: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
    roles: Sequence[ExpandedRole],
) -> Any:
    _ = filtered_pool

    merge_duplicate_candidate_lines(spec)

    dep_warnings_1 = resolve_dependencies(spec, source_pool, requirements, topology)
    merge_duplicate_candidate_lines(spec)

    qty_warnings = resolve_quantities(spec, source_pool, requirements, topology)
    merge_duplicate_candidate_lines(spec)

    gate_warnings = _prune_items_outside_allowed_families(spec, source_pool, roles, topology, requirements)
    merge_duplicate_candidate_lines(spec)

    # после family gate ещё раз восстанавливаем обязательные dependency placeholder’ы
    dep_warnings_2 = resolve_dependencies(spec, source_pool, requirements, topology)
    merge_duplicate_candidate_lines(spec)

    if requirements.room_type == "videowall":
        _enforce_videowall_roles_and_debug(spec, source_pool)
        merge_duplicate_candidate_lines(spec)

    normalize_categories(spec, source_pool)
    sort_spec_items(spec)

    validate_prices(spec, source_pool)
    build_fallback_explanations(spec=spec, requirements=requirements, topology=topology)
    used_task_ids: set[int] = set()
    for it in list(getattr(spec, "items", []) or []):
        ev = getattr(it, "evidence", None)
        for tid in list(getattr(ev, "bitrix_task_ids", []) or []):
            try:
                used_task_ids.add(int(tid))
            except Exception:
                pass
    if hasattr(spec, "used_bitrix_task_ids"):
        setattr(spec, "used_bitrix_task_ids", sorted(used_task_ids))

    warnings = dep_warnings_1 + qty_warnings + gate_warnings + dep_warnings_2
    if warnings and hasattr(spec, "apply_warnings"):
        cur = list(getattr(spec, "apply_warnings", []) or [])
        seen = set(cur)
        for w in warnings:
            if w not in seen:
                cur.append(w)
                seen.add(w)
        setattr(spec, "apply_warnings", cur)

    return spec