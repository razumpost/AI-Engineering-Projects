from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, ConfigDict

from .ops import PatchOperation


class BriefConstraints(BaseModel):
    model_config = ConfigDict(extra="forbid")

    display_layout: Optional[str] = None
    budget_level: Optional[Literal["econom", "mid", "premium"]] = None
    deadline: Optional[str] = None

    must_have: list[str] = Field(default_factory=list)
    must_not_have: list[str] = Field(default_factory=list)


class Brief(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_summary: str
    constraints: BriefConstraints = Field(default_factory=BriefConstraints)


class UsedEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bitrix_task_ids: list[int] = Field(default_factory=list)
    candidate_item_ids: list[str] = Field(default_factory=list)


class FollowUpQuestion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    priority: Literal['high','medium','low'] = 'medium'
    question: str
    why: str = ""
    answer_format: Literal["choice", "free_text", "number", "date"] = "free_text"
    options: list[str] = Field(default_factory=list)


class SkspLLMResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal["sksp.v1"] = "sksp.v1"
    mode: Literal["compose", "patch"]

    brief: Brief
    used_evidence: UsedEvidence = Field(default_factory=UsedEvidence)

    operations: list[PatchOperation] = Field(default_factory=list)

    explanations: dict[str, list[str]] = Field(default_factory=dict)
    followup_questions: list[FollowUpQuestion] = Field(default_factory=list)

    assumptions: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
