from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class RoomTypeDef(BaseModel):
    key: str
    title: str
    default_roles: list[str] = Field(default_factory=list)
    default_capabilities: list[str] = Field(default_factory=list)


class CapabilityDef(BaseModel):
    key: str
    title: str
    adds_roles: list[str] = Field(default_factory=list)
    excludes_roles: list[str] = Field(default_factory=list)


class RoleDef(BaseModel):
    key: str
    title: str
    allowed_families: list[str] = Field(default_factory=list)
    required: bool = True
    qty_rule: str = "one"
    notes: list[str] = Field(default_factory=list)


class FamilyDef(BaseModel):
    key: str
    title: str
    categories: list[str] = Field(default_factory=list)
    capabilities: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    interfaces: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class DependencyRule(BaseModel):
    family: str
    requires_all: list[str] = Field(default_factory=list)
    requires_any: list[list[str]] = Field(default_factory=list)
    optional_with: list[str] = Field(default_factory=list)
    incompatible_with: list[str] = Field(default_factory=list)
    recommended_with: list[str] = Field(default_factory=list)


class QuantityRule(BaseModel):
    family: str
    rule: Literal[
        "one",
        "one_per_room",
        "same_as_camera_count",
        "same_as_display_count",
        "same_as_seat_count",
        "seat_count_minus_one",
        "one_per_display",
        "one_per_camera",
        "custom",
    ]
    min_value: int | None = None
    max_value: int | None = None
    notes: list[str] = Field(default_factory=list)


class TopologyRolesDef(BaseModel):
    required: list[str] = Field(default_factory=list)
    optional: list[str] = Field(default_factory=list)


class TopologyPatternDef(BaseModel):
    key: str
    title: str
    room_types: list[str] = Field(default_factory=list)
    requires_flags: list[str] = Field(default_factory=list)
    optional_flags: list[str] = Field(default_factory=list)
    preferred_when: dict[str, Any] = Field(default_factory=dict)
    roles: TopologyRolesDef = Field(default_factory=TopologyRolesDef)
    preferred_families: dict[str, list[str]] = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list)


class ConflictRuleDef(BaseModel):
    key: str
    conflicts_with: list[str] = Field(default_factory=list)
    allowed_room_types: list[str] = Field(default_factory=list)
    forbidden_room_types: list[str] = Field(default_factory=list)
    unless: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class KnowledgeMap(BaseModel):
    room_types: dict[str, RoomTypeDef]
    capabilities: dict[str, CapabilityDef]
    roles: dict[str, RoleDef]
    families: dict[str, FamilyDef]
    dependency_rules: dict[str, DependencyRule]
    quantity_rules: dict[str, QuantityRule]
    topology_patterns: dict[str, TopologyPatternDef]
    conflict_rules: dict[str, ConflictRuleDef]


class RequirementCaps(BaseModel):
    seat_count: int | None = None
    room_count: int | None = None
    camera_count: int | None = None
    display_count: int | None = None


class RequirementFlags(BaseModel):
    vks: bool = False
    byod: bool = False
    presentation: bool = False
    recording: bool = False
    streaming: bool = False
    speech_reinforcement: bool = False
    control: bool = False


class RequirementExclusions(BaseModel):
    led: bool = False
    projector: bool = False
    operator_room: bool = False


class ProjectRequirements(BaseModel):
    room_type: str
    caps: RequirementCaps = Field(default_factory=RequirementCaps)
    flags: RequirementFlags = Field(default_factory=RequirementFlags)
    exclusions: RequirementExclusions = Field(default_factory=RequirementExclusions)
    confidence: dict[str, float] = Field(default_factory=dict)