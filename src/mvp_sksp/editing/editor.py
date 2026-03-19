from __future__ import annotations

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


def _merge_duplicates(spec: Spec, report: ApplyReport) -> None:
    by_key: dict[str, LineItem] = {}
    merged: list[LineItem] = []
    for it in spec.items:
        if it.item_key in by_key:
            prev = by_key[it.item_key]
            prev.qty = prev.qty + it.qty
            report.warnings.append(f"Merged duplicate lines by item_key: {it.item_key}")
            continue
        by_key[it.item_key] = it
        merged.append(it)
    spec.items = merged


def _candidate_to_line(pool: CandidatePool, candidate_id: str, *, category: str, qty: Decimal) -> LineItem:
    ci = pool.by_id().get(candidate_id)
    if not ci:
        raise ValueError(f"Unknown candidate_id={candidate_id}")

    desc = norm_text(ci.description or ci.name)
    item_key = build_item_key(sku=ci.sku, manufacturer=ci.manufacturer, model=ci.model, description=desc)

    return LineItem(
        line_id=_new_id("li"),
        category=category or ci.category,
        sku=ci.sku,
        manufacturer=ci.manufacturer,
        model=ci.model,
        name=ci.name,
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
