from __future__ import annotations

from pathlib import Path

import yaml

from .models import (
    CapabilityDef,
    ConflictRuleDef,
    DependencyRule,
    FamilyDef,
    KnowledgeMap,
    QuantityRule,
    RoleDef,
    RoomTypeDef,
    TopologyPatternDef,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _ontology_dir() -> Path:
    return _repo_root() / "src" / "mvp_sksp" / "knowledge" / "ontology"


def _load_yaml(name: str) -> dict:
    path = _ontology_dir() / name
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a mapping")
    return data


def _merge_dicts(base: dict, extra: dict) -> dict:
    out = dict(base)
    for key, value in extra.items():
        if key in out and isinstance(out[key], dict) and isinstance(value, dict):
            out[key] = {**out[key], **value}
        else:
            out[key] = value
    return out


def load_knowledge_map() -> KnowledgeMap:
    room_types_raw = _load_yaml("room_types.yaml").get("room_types", {})
    capabilities_raw = _load_yaml("capabilities.yaml").get("capabilities", {})
    roles_raw = _load_yaml("roles.yaml").get("roles", {})

    families_raw = _load_yaml("families.yaml").get("families", {})
    extra_families_path = _ontology_dir() / "families_step8.yaml"
    if extra_families_path.exists():
        extra_families_raw = _load_yaml("families_step8.yaml").get("families", {})
        families_raw = _merge_dicts(families_raw, extra_families_raw)

    dependency_rules_raw = _load_yaml("dependency_rules.yaml").get("dependency_rules", {})
    quantity_rules_raw = _load_yaml("quantity_rules.yaml").get("quantity_rules", {})

    topology_patterns_raw = _load_yaml("topology_patterns.yaml").get("topology_patterns", {})
    conflict_rules_raw = _load_yaml("conflict_rules.yaml").get("conflict_rules", {})

    return KnowledgeMap(
        room_types={k: RoomTypeDef(key=k, **v) for k, v in room_types_raw.items()},
        capabilities={k: CapabilityDef(key=k, **v) for k, v in capabilities_raw.items()},
        roles={k: RoleDef(key=k, **v) for k, v in roles_raw.items()},
        families={k: FamilyDef(key=k, **v) for k, v in families_raw.items()},
        dependency_rules={k: DependencyRule(family=k, **v) for k, v in dependency_rules_raw.items()},
        quantity_rules={k: QuantityRule(family=k, **v) for k, v in quantity_rules_raw.items()},
        topology_patterns={k: TopologyPatternDef(key=k, **v) for k, v in topology_patterns_raw.items()},
        conflict_rules={k: ConflictRuleDef(key=k, **v) for k, v in conflict_rules_raw.items()},
    )