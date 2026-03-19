from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from ..domain.candidates import CandidatePool
from ..domain.spec import Spec, norm_key


@dataclass(frozen=True)
class ValidationReport:
    ok: bool
    errors: list[str]
    warnings: list[str]


def _is_suspicious_price(amount: Decimal) -> bool:
    return amount <= Decimal("0") or amount > Decimal("50000000")


def validate_and_fix(spec: Spec, pool: CandidatePool) -> ValidationReport:
    errors: list[str] = []
    warnings: list[str] = []

    seen = set()
    for it in spec.items:
        if it.line_id in seen:
            errors.append(f"Duplicate line_id: {it.line_id}")
        seen.add(it.line_id)

        missing: list[str] = []
        if not it.manufacturer or not norm_key(it.manufacturer):
            missing.append("manufacturer")
        if not it.sku or not norm_key(it.sku):
            missing.append("sku")
        if it.unit_price is None:
            missing.append("unit_price")
        if missing:
            it.flags.needs_clarification = True
            it.flags.missing_fields = missing
            warnings.append(f"Line needs clarification ({it.line_id}): missing={missing}")

        if it.qty <= Decimal("0"):
            errors.append(f"Invalid qty ({it.line_id}): {it.qty}")

        if it.unit_price is not None and _is_suspicious_price(it.unit_price.amount):
            it.flags.suspicious_price = True
            warnings.append(f"Suspicious price ({it.line_id}): {it.unit_price.amount}")

        if not it.evidence.bitrix_task_ids:
            warnings.append(f"No Bitrix evidence for line ({it.line_id})")

    return ValidationReport(ok=(len(errors) == 0), errors=errors, warnings=warnings)
