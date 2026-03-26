from __future__ import annotations

from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, Field, ConfigDict

from .spec import Money, norm_key


class CandidateItem(BaseModel):
    """Candidate equipment item retrieved from suppliers/tasks/previous SKSP."""
    model_config = ConfigDict(extra="forbid")

    candidate_id: str
    category: str

    sku: Optional[str] = None
    manufacturer: Optional[str] = None
    model: Optional[str] = None

    name: str
    description: str

    unit_price_rub: Optional[Decimal] = None
    price_source: Optional[str] = None

    evidence_task_ids: list[int] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)

    def money(self) -> Optional[Money]:
        if self.unit_price_rub is None:
            return None
        return Money(amount=Decimal(str(self.unit_price_rub)), currency="RUB")

    def signature(self) -> str:
        if self.sku and norm_key(self.sku):
            return f"sku:{norm_key(self.sku)}"
        mm = ":".join([norm_key(self.manufacturer or ""), norm_key(self.model or "")]).strip(":")
        if mm and mm != ":":
            return f"mm:{mm}"
        return f"desc:{norm_key((self.name or '') + ' ' + (self.description or ''))}"


class CandidateTask(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: int
    title: str
    url: str
    similarity: float = 0.0
    snippet: str = ""


class CandidatePool(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[CandidateItem] = Field(default_factory=list)
    tasks: list[CandidateTask] = Field(default_factory=list)

    def by_id(self) -> dict[str, CandidateItem]:
        return {c.candidate_id: c for c in self.items}

    def merge(self, other: "CandidatePool") -> "CandidatePool":
        """Merge two pools; keeps unique items by candidate_id."""
        by_id = self.by_id()
        for c in other.items:
            if c.candidate_id not in by_id:
                by_id[c.candidate_id] = c

        tasks = {t.task_id: t for t in self.tasks}
        for t in other.tasks:
            if t.task_id not in tasks:
                tasks[t.task_id] = t

        return CandidatePool(items=list(by_id.values()), tasks=list(tasks.values()))