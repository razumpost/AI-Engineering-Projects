from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


__all__ = [
    "Priority",
    "OpType",
    "ItemRef",
    "TargetRef",
    "PatchOperation",
    "BriefConstraints",
    "Brief",
    "FollowUpQuestion",
    "UsedEvidence",
    "SkspLLMResponse",
]

Priority = Literal["high", "medium", "low"]
OpType = Literal["add_line", "replace_line", "remove_line", "set_qty", "replace_brand"]


class ItemRef(BaseModel):
    model_config = ConfigDict(extra="forbid")
    candidate_id: str


class TargetRef(BaseModel):
    model_config = ConfigDict(extra="forbid")
    line_id: str


class PatchOperation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: OpType
    category: Optional[str] = None

    item: Optional[ItemRef] = None
    target: Optional[TargetRef] = None

    qty: Optional[Decimal] = None
    reason: str = ""
    evidence_task_ids: list[int] = Field(default_factory=list)


class BriefConstraints(BaseModel):
    # DeepSeek/YandexGPT may output extra keys or strings -> keep tolerant
    model_config = ConfigDict(extra="allow")

    budget: Optional[str] = None
    timeline: Optional[str] = None
    brand_preferences: Optional[str] = None


class Brief(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_summary: str = ""
    constraints: BriefConstraints = Field(default_factory=BriefConstraints)


class FollowUpQuestion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str
    priority: Priority = "medium"


class UsedEvidence(BaseModel):
    """
    Orchestrator expects .bitrix_task_ids attribute.
    """
    model_config = ConfigDict(extra="allow")

    bitrix_task_ids: list[int] = Field(default_factory=list)
    candidate_item_ids: list[str] = Field(default_factory=list)


class SkspLLMResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str = "sksp.v1"
    mode: Literal["compose", "patch"] = "compose"

    brief: Brief = Field(default_factory=Brief)

    used_evidence: UsedEvidence = Field(default_factory=UsedEvidence)
    operations: list[PatchOperation] = Field(default_factory=list)

    explanations: dict[str, list[str]] = Field(default_factory=dict)
    followup_questions: list[FollowUpQuestion] = Field(default_factory=list)

    assumptions: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)