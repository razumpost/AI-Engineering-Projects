from __future__ import annotations

from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, Field


class Candidate(BaseModel):
    """Atomic KB item candidate (retrieved from SKSP/prices/tasks etc.)."""

    candidate_id: str
    sku: Optional[str] = None
    manufacturer: Optional[str] = None
    name: str
    description: Optional[str] = None
    category: Optional[str] = None

    price: Optional[Decimal] = None
    currency: Optional[str] = None
    unit: Optional[str] = None
    url: Optional[str] = None

    meta: dict[str, Any] = Field(default_factory=dict)


class CandidateMergePolicy(BaseModel):
    """Merge policy for combining candidates from multiple KB sources."""

    price_priority: list[str] = Field(default_factory=lambda: ["supplier_price", "sksp_snapshot"])
    description_priority: list[str] = Field(default_factory=lambda: ["sksp_snapshot", "supplier_price"])


def merge_candidate_fields(
    primary: Candidate,
    secondary: Candidate,
    primary_source: str,
    secondary_source: str,
    policy: CandidateMergePolicy | None = None,
) -> Candidate:
    """Merge `primary` + `secondary` into one Candidate following project requirements.

    Requirements:
      - price: prioritize supplier chat price items
      - description: prioritize SKSP snapshots
      - never invent missing data; only select from existing fields
    """
    pol = policy or CandidateMergePolicy()

    def _pick_by_priority(
        field_name: str,
        a: Any,
        b: Any,
        prio: list[str],
    ) -> Any:
        if a is not None and (primary_source in prio) and (secondary_source in prio):
            return a if prio.index(primary_source) <= prio.index(secondary_source) else b
        if a is not None:
            return a
        return b

    # price and currency are linked
    price = _pick_by_priority("price", primary.price, secondary.price, pol.price_priority)
    currency = primary.currency if price == primary.price else secondary.currency
    if price is None:
        currency = primary.currency or secondary.currency

    description = _pick_by_priority(
        "description",
        primary.description,
        secondary.description,
        pol.description_priority,
    )

    return Candidate(
        candidate_id=primary.candidate_id,
        sku=primary.sku or secondary.sku,
        manufacturer=primary.manufacturer or secondary.manufacturer,
        name=primary.name or secondary.name,
        description=description,
        category=primary.category or secondary.category,
        price=price,
        currency=currency,
        unit=primary.unit or secondary.unit,
        url=primary.url or secondary.url,
        meta={**(secondary.meta or {}), **(primary.meta or {})},
    )