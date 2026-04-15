from __future__ import annotations

import re
import uuid
from decimal import Decimal
from typing import Optional

from ..domain.candidates import CandidatePool
from ..domain.ops import ApplyReport, PatchOperation
from ..domain.spec import LineItem, Spec, build_item_key, norm_text
from .matching import resolve_single_line


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _find_line_index(spec: Spec, line_id: str) -> Optional[int]:
    for i, it in enumerate(spec.items):
        if it.line_id == line_id:
            return i
    return None


def _is_placeholder_line(it: LineItem) -> bool:
    meta = getattr(it, "meta", None) or {}
    return isinstance(meta, dict) and bool(meta.get("placeholder_kind"))


def _looks_like_discussion_delegate(line: LineItem) -> bool:
    blob = " ".join(
        [
            norm_text(getattr(line, "name", "") or ""),
            norm_text(getattr(line, "description", "") or ""),
            norm_text(getattr(line, "model", "") or ""),
            norm_text(getattr(line, "sku", "") or ""),
        ]
    ).casefold()

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


def _merge_duplicates(spec: Spec, report: ApplyReport) -> None:
    """
    ВАЖНО:
    - placeholder duplicate -> оставляем max(qty)
    - обычные AV позиции (display/camera/microphone/audio/controller) -> оставляем max(qty),
      а не суммируем, чтобы не раздувать строки из-за повторного add_line
    - discussion/delegate/cabling/support можно суммировать
    """
    by_key: dict[str, LineItem] = {}
    merged: list[LineItem] = []

    conservative_categories = {"display", "camera", "microphone", "audio", "conference", "controller", "signal_transport"}

    for it in spec.items:
        if it.item_key not in by_key:
            by_key[it.item_key] = it
            merged.append(it)
            continue

        prev = by_key[it.item_key]

        should_sum = False

        if _looks_like_discussion_delegate(prev) or _looks_like_discussion_delegate(it):
            should_sum = True
        elif (getattr(prev, "category", "") or "") in {"cable", "mount", "mounting", "power", "accessories"}:
            should_sum = True
        elif (getattr(it, "category", "") or "") in {"cable", "mount", "mounting", "power", "accessories"}:
            should_sum = True

        if _is_placeholder_line(prev) or _is_placeholder_line(it):
            should_sum = False

        if should_sum:
            prev.qty = prev.qty + it.qty
            report.warnings.append(f"Merged duplicate lines by item_key with SUM qty: {it.item_key}")
        else:
            prev.qty = max(prev.qty, it.qty)
            report.warnings.append(f"Merged duplicate lines by item_key with MAX qty: {it.item_key}")

            # если у старой строки пустее данные, аккуратно обогащаем
            if not prev.name and it.name:
                prev.name = it.name
            if not prev.description and it.description:
                prev.description = it.description
            if prev.unit_price is None and it.unit_price is not None:
                prev.unit_price = it.unit_price

            prev_evidence = getattr(prev, "evidence", None) or {}
            it_evidence = getattr(it, "evidence", None) or {}
            if isinstance(prev_evidence, dict) and isinstance(it_evidence, dict):
                prev_ids = list(prev_evidence.get("bitrix_task_ids", []) or [])
                it_ids = list(it_evidence.get("bitrix_task_ids", []) or [])
                prev_evidence["bitrix_task_ids"] = list(dict.fromkeys(prev_ids + it_ids))
                prev.evidence = prev_evidence

    spec.items = merged


def _looks_like_company_name(name: str) -> bool:
    t = norm_text(name).casefold()
    if not t:
        return False

    if any(t.startswith(x) for x in ["ооо ", "ао ", "пао ", "зао ", "ип ", 'ооо "', 'ао "', 'пао "', 'зао "']):
        return True

    if "регионком" in t:
        return True

    if "договор" in t or "контракт" in t:
        return True

    return False


def _looks_like_product_fragment(s: str) -> bool:
    t = norm_text(s).casefold()
    if not t:
        return False

    product_needles = [
        "clockaudio",
        "microphone",
        "микрофон",
        "camera",
        "камера",
        "ptz",
        "display",
        "дисплей",
        "панель",
        "экран",
        "speaker",
        "акуст",
        "soundbar",
        "switcher",
        "matrix",
        "коммутатор",
        "mount kit",
        "кронштейн",
        "стойка",
        "тележка",
        "dsp",
        "processor",
        "процессор",
        "byod",
        "dock",
    ]
    if any(x in t for x in product_needles):
        return True

    if re.search(r"[a-zа-я].*\d|\d.*[a-zа-я]", t, re.IGNORECASE):
        return True

    return False


def _best_product_fragment(name: str, desc: str) -> tuple[str, str]:
    clean_name = norm_text(name)
    clean_desc = norm_text(desc)

    parts = [norm_text(x) for x in re.split(r"\|", clean_desc) if norm_text(x)]
    product_parts = [p for p in parts if _looks_like_product_fragment(p)]

    best_part = product_parts[-1] if product_parts else ""

    final_name = clean_name
    final_desc = clean_desc

    if best_part:
        final_desc = best_part

        if _looks_like_company_name(clean_name) or clean_name == clean_desc or len(clean_name) <= 4:
            final_name = best_part

    if not best_part and _looks_like_company_name(clean_name):
        final_name = clean_desc

    return final_name or clean_name, final_desc or clean_desc or clean_name


def _candidate_to_line(pool: CandidatePool, candidate_id: str, *, category: str, qty: Decimal) -> LineItem:
    ci = pool.by_id().get(candidate_id)
    if not ci:
        raise ValueError(f"Unknown candidate_id={candidate_id}")

    raw_name = norm_text(ci.name or "")
    raw_desc = norm_text(ci.description or ci.name or "")
    pretty_name, pretty_desc = _best_product_fragment(raw_name, raw_desc)

    desc = norm_text(pretty_desc or pretty_name)
    item_key = build_item_key(sku=ci.sku, manufacturer=ci.manufacturer, model=ci.model, description=desc)

    return LineItem(
        line_id=_new_id("li"),
        category=category or ci.category,
        sku=ci.sku,
        manufacturer=ci.manufacturer,
        model=ci.model,
        name=pretty_name or ci.name,
        description=desc,
        qty=qty,
        unit_price=ci.money(),
        item_key=item_key,
        evidence={
            "bitrix_task_ids": list(ci.evidence_task_ids),
            "supplier_item_ids": [],
            "retrieval_block_ids": [],
            "notes": [ci.price_source] if ci.price_source else [],
        },
        flags={},
        meta={},
    )


def apply_operations(spec: Spec, ops: list[PatchOperation], pool: CandidatePool) -> ApplyReport:
    report = ApplyReport()
    for op in ops:
        try:
            if op.op == "add_line":
                if not op.item or not op.item.candidate_id:
                    report.errors.append("add_line: missing item.candidate_id")
                    report.skipped_ops += 1
                    continue
                qty = op.qty if op.qty is not None else Decimal("1")
                line = _candidate_to_line(pool, op.item.candidate_id, category=op.category or "", qty=qty)
                spec.items.append(line)
                report.applied_ops += 1

            elif op.op == "replace_line":
                if not op.item or not op.item.candidate_id:
                    report.errors.append("replace_line: missing item.candidate_id")
                    report.skipped_ops += 1
                    continue
                if not op.target:
                    report.errors.append("replace_line: missing target")
                    report.skipped_ops += 1
                    continue

                line_id = op.target.line_id
                if not line_id and op.target.match:
                    line_id = resolve_single_line(spec.items, op.target.match)
                if not line_id:
                    report.errors.append("replace_line: target not resolved")
                    report.skipped_ops += 1
                    continue

                idx = _find_line_index(spec, line_id)
                if idx is None:
                    report.errors.append(f"replace_line: line_id not found: {line_id}")
                    report.skipped_ops += 1
                    continue

                qty = op.qty if op.qty is not None else spec.items[idx].qty
                new_line = _candidate_to_line(
                    pool,
                    op.item.candidate_id,
                    category=op.category or spec.items[idx].category,
                    qty=qty,
                )
                new_line.line_id = spec.items[idx].line_id
                spec.items[idx] = new_line
                report.applied_ops += 1

            elif op.op == "remove_line":
                if not op.target:
                    report.errors.append("remove_line: missing target")
                    report.skipped_ops += 1
                    continue
                line_id = op.target.line_id
                if not line_id and op.target.match:
                    line_id = resolve_single_line(spec.items, op.target.match)
                if not line_id:
                    report.errors.append("remove_line: target not resolved")
                    report.skipped_ops += 1
                    continue
                idx = _find_line_index(spec, line_id)
                if idx is None:
                    report.errors.append(f"remove_line: line_id not found: {line_id}")
                    report.skipped_ops += 1
                    continue
                spec.items.pop(idx)
                report.applied_ops += 1

            elif op.op == "set_qty":
                if not op.target or op.qty is None:
                    report.errors.append("set_qty: missing target or qty")
                    report.skipped_ops += 1
                    continue
                line_id = op.target.line_id
                if not line_id and op.target.match:
                    line_id = resolve_single_line(spec.items, op.target.match)
                if not line_id:
                    report.errors.append("set_qty: target not resolved")
                    report.skipped_ops += 1
                    continue
                idx = _find_line_index(spec, line_id)
                if idx is None:
                    report.errors.append(f"set_qty: line_id not found: {line_id}")
                    report.skipped_ops += 1
                    continue
                spec.items[idx].qty = op.qty
                report.applied_ops += 1

            elif op.op == "replace_brand":
                report.errors.append("replace_brand: not implemented in scaffold (use replace_line per item)")
                report.skipped_ops += 1

            else:
                report.errors.append(f"Unknown op: {op.op}")
                report.skipped_ops += 1

        except Exception as e:
            report.errors.append(f"Op failed ({op.op}): {e}")
            report.skipped_ops += 1

    _merge_duplicates(spec, report)
    spec.touch()
    return report