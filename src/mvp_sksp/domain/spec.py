from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, ConfigDict


def _now() -> datetime:
    return datetime.now(timezone.utc)


def norm_text(s: str) -> str:
    return " ".join((s or "").replace("\u00a0", " ").split()).strip()


def norm_key(s: str) -> str:
    return norm_text(s).casefold()


class Money(BaseModel):
    """Normalized money value for pricing in spec."""
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(..., ge=Decimal("0"))
    currency: Literal["RUB"] = "RUB"


class Evidence(BaseModel):
    """Evidence links for why an item/qty/price was chosen."""
    model_config = ConfigDict(extra="forbid")

    bitrix_task_ids: list[int] = Field(default_factory=list)
    supplier_item_ids: list[str] = Field(default_factory=list)
    retrieval_block_ids: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class LineFlags(BaseModel):
    model_config = ConfigDict(extra="forbid")

    needs_clarification: bool = False
    suspicious_price: bool = False
    missing_fields: list[str] = Field(default_factory=list)


class LineItem(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    line_id: str
    category: str

    sku: Optional[str] = None
    manufacturer: Optional[str] = None
    model: Optional[str] = None

    name: str
    description: str

    unit: str = "шт"
    qty: Decimal = Field(..., gt=Decimal("0"))

    unit_price: Optional[Money] = None

    item_key: str
    evidence: Evidence = Field(default_factory=Evidence)
    flags: LineFlags = Field(default_factory=LineFlags)

    reasoning: str = ""
    meta: dict[str, Any] = Field(default_factory=dict)


class Spec(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    spec_id: str
    project_title: str = "СкСп"

    project_summary: str = ""
    why_composition: list[str] = Field(default_factory=list)
    why_qty_and_price: list[str] = Field(default_factory=list)

    apply_warnings: list[str] = Field(default_factory=list)
    validation_warnings: list[str] = Field(default_factory=list)

    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)

    items: list[LineItem] = Field(default_factory=list)

    assumptions: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    manager_questions: list[str] = Field(default_factory=list)

    used_bitrix_task_ids: list[int] = Field(default_factory=list)

    def touch(self) -> None:
        self.updated_at = _now()


def build_item_key(*, sku: Optional[str], manufacturer: Optional[str], model: Optional[str], description: str) -> str:
    if sku and norm_key(sku):
        return f"sku:{norm_key(sku)}"
    mm = ":".join([norm_key(manufacturer or ""), norm_key(model or "")]).strip(":")
    if mm and mm != ":":
        return f"mm:{mm}"
    return f"desc:{norm_key(description)}"
