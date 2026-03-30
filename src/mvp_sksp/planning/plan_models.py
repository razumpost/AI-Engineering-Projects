from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

# Backward-compat alias: some modules import LineItemPlan from this module.
# We map it to the canonical domain LineItem model used by Spec.items.
from ..domain.spec import LineItem as LineItemPlan  # noqa: F401


class ClassifiedCandidate(BaseModel):
    candidate_id: str
    family: str | None = None
    family_confidence: float = 0.0
    capabilities: list[str] = Field(default_factory=list)
    interfaces: list[str] = Field(default_factory=list)
    room_fit: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class TopologyDecision(BaseModel):
    topology_key: str
    score: float
    reason: str
    required_roles: list[str] = Field(default_factory=list)
    optional_roles: list[str] = Field(default_factory=list)
    preferred_families: dict[str, list[str]] = Field(default_factory=dict)


class PlannedSelection(BaseModel):
    role_key: str
    family: str
    candidate_id: str
    qty: int | float | None = None
    source: Literal["role_coverage", "dependency", "manual", "topology_template"] = "role_coverage"
    required: bool = True
    reason: str = ""
    evidence_task_ids: list[int] = Field(default_factory=list)


class ProjectPlan(BaseModel):
    topology: TopologyDecision
    selections: list[PlannedSelection] = Field(default_factory=list)
    uncovered_roles: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)