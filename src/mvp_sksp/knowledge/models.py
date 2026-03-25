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


class FamilySignatureDef(BaseModel):
    """Deterministic gates for family matching (exploitation-safe).

    - must_have: if non-empty, at least one token/phrase must be present in item text
    - must_not: if any token/phrase present, family is rejected
    - strong_keywords: additional high-signal tokens/phrases (boost score)
    """

    must_have: list[str] = Field(default_factory=list)
    must_not: list[str] = Field(default_factory=list)
    strong_keywords: list[str] = Field(default_factory=list)


class FamilyDef(BaseModel):
    key: str
    title: str
    categories: list[str] = Field(default_factory=list)
    capabilities: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    interfaces: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

    # KB extensions (editable via families_kb.yaml overrides)
    tags: dict[str, list[str]] = Field(default_factory=dict)
    signature: FamilySignatureDef = Field(default_factory=FamilySignatureDef)


class DependencyRule(BaseModel):
    """Dependency constraints between equipment families.

    The YAML supports both:
    - requires_all: required families (must all exist)
    - requires_any: list of alternative groups; for each group at least one family must exist
      example: requires_any: [[cable_hdmi, cable_usb], [mounting_kit]]
    """

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
    room_types: dict[str, RoomTypeDef] = Field(default_factory=dict)
    capabilities: dict[str, CapabilityDef] = Field(default_factory=dict)
    roles: dict[str, RoleDef] = Field(default_factory=dict)
    families: dict[str, FamilyDef] = Field(default_factory=dict)
    dependency_rules: dict[str, DependencyRule] = Field(default_factory=dict)
    quantity_rules: dict[str, QuantityRule] = Field(default_factory=dict)
    topology_patterns: dict[str, TopologyPatternDef] = Field(default_factory=dict)
    conflict_rules: dict[str, ConflictRuleDef] = Field(default_factory=dict)


class RequirementCaps(BaseModel):
    video: bool = False
    audio: bool = False
    vc: bool = False
    presentation: bool = False
    recording: bool = False
    streaming: bool = False
    interpretation: bool = False
    simultaneous_translation: bool = False


class RequirementFlags(BaseModel):
    led: bool = False
    projector: bool = False
    videowall: bool = False
    operator_room: bool = False
    auditorium: bool = False
    discussion: bool = False
    control: bool = False


class RequirementExclusions(BaseModel):
    led: bool = False
    projector: bool = False
    operator_room: bool = False


class ProjectRequirements(BaseModel):
    """Parsed requirements from request/transcript.

    This object intentionally stays *small and stable* — everything else is derived
    deterministically (role plan, topology, quantities, dependencies).
    """

    room_type: str = "meeting_room"
    caps: RequirementCaps = Field(default_factory=RequirementCaps)
    flags: RequirementFlags = Field(default_factory=RequirementFlags)
    exclusions: RequirementExclusions = Field(default_factory=RequirementExclusions)

    # field -> [0..1] confidence, used only for explanations / follow-up gating.
    confidence: dict[str, float] = Field(default_factory=dict)