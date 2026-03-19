from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, ConfigDict


class MatchSelector(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category: Optional[str] = None
    contains: list[str] = Field(default_factory=list)


class TargetSelector(BaseModel):
    model_config = ConfigDict(extra="forbid")

    line_id: Optional[str] = None
    match: Optional[MatchSelector] = None


class ItemRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_id: Optional[str] = None
    sku: Optional[str] = None
    manufacturer: Optional[str] = None
    model: Optional[str] = None
    description: Optional[str] = None


OpType = Literal["add_line", "replace_line", "remove_line", "set_qty", "replace_brand"]


class PatchOperation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: OpType
    category: Optional[str] = None

    target: Optional[TargetSelector] = None
    item: Optional[ItemRef] = None

    qty: Optional[Decimal] = None
    explicit_add: bool = False

    reason: str = ""
    evidence_task_ids: list[int] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)


class ApplyReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    applied_ops: int = 0
    skipped_ops: int = 0
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
