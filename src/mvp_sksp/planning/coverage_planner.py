from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable


@dataclass
class CoverageFilterResult:
    filtered_pool: Any
    selected_items: list[Any]
    selected_candidate_ids: list[str]
    required_families: list[str]
    required_roles: list[str]
    total_candidates_before: int
    total_candidates_after: int
    role_debug: list[dict[str, Any]] = field(default_factory=list)


def _obj_get(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _meta_get(obj: Any, key: str, default: Any = None) -> Any:
    meta = _obj_get(obj, "meta", None)
    if isinstance(meta, dict):
        return meta.get(key, default)
    return default


def _norm(v: Any) -> str:
    return str(v or "").strip().casefold()


def _candidate_family(c: Any) -> str:
    for key in ("family", "category"):
        v = _obj_get(c, key, None)
        if v:
            return _norm(v)

    for key in ("family", "classified_category", "graph_family", "equipment_family", "category"):
        v = _meta_get(c, key, None)
        if v:
            return _norm(v)

    role = _norm(_obj_get(c, "role", None) or _meta_get(c, "role", None))
    if role:
        return role

    return ""


def _candidate_role(c: Any) -> str:
    role = _obj_get(c, "role", None) or _meta_get(c, "role", None)
    if role:
        return _norm(role)

    fam = _candidate_family(c)
    if fam:
        return fam

    return ""


def _candidate_score(c: Any) -> float:
    meta_score = _meta_get(c, "score", None)
    if meta_score is not None:
        try:
            return float(meta_score)
        except Exception:
            pass

    for key in ("score", "similarity"):
        v = _obj_get(c, key, None)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass

    return 0.0


def _candidate_id(c: Any) -> str:
    cid = _obj_get(c, "candidate_id", None)
    if cid:
        return str(cid)
    return ""


def _iter_candidates(items: Iterable[Any]) -> list[Any]:
    return list(items or [])


def _extract_required_families(requirements: Any) -> list[str]:
    out: list[str] = []

    if not requirements:
        return out

    try:
        if isinstance(requirements, list):
            for r in requirements:
                if isinstance(r, dict):
                    fam = (
                        r.get("family")
                        or r.get("category")
                        or r.get("graph_family")
                        or r.get("equipment_family")
                    )
                    fam = _norm(fam)
                    if fam:
                        out.append(fam)
                else:
                    fam = _norm(_obj_get(r, "family", None) or _obj_get(r, "category", None))
                    if fam:
                        out.append(fam)
        elif isinstance(requirements, dict):
            fams = requirements.get("families")
            if isinstance(fams, list):
                for f in fams:
                    ff = _norm(f)
                    if ff:
                        out.append(ff)
        else:
            fam = _norm(_obj_get(requirements, "family", None) or _obj_get(requirements, "category", None))
            if fam:
                out.append(fam)
    except Exception:
        return []

    seen: set[str] = set()
    deduped: list[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            deduped.append(x)
    return deduped


def _extract_required_roles(roles: Any) -> list[str]:
    out: list[str] = []

    if not roles:
        return out

    try:
        if isinstance(roles, list):
            for r in roles:
                rr = _norm(r)
                if rr:
                    out.append(rr)
        elif isinstance(roles, dict):
            vals = roles.get("roles")
            if isinstance(vals, list):
                for r in vals:
                    rr = _norm(r)
                    if rr:
                        out.append(rr)
        else:
            rr = _norm(roles)
            if rr:
                out.append(rr)
    except Exception:
        return []

    seen: set[str] = set()
    deduped: list[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            deduped.append(x)
    return deduped


def _update_pool_items(pool: Any, items: list[Any], tasks: Any) -> Any:
    if hasattr(pool, "model_copy"):
        try:
            return pool.model_copy(update={"items": items, "tasks": tasks})
        except Exception:
            pass

    if isinstance(pool, dict):
        out = dict(pool)
        out["items"] = items
        out["tasks"] = tasks
        return out

    try:
        pool.items = items
        pool.tasks = tasks
        return pool
    except Exception:
        return pool


def _make_role_debug(
    items: list[Any],
    wanted_families: set[str],
    wanted_roles: set[str],
) -> list[dict[str, Any]]:
    debug_rows: list[dict[str, Any]] = []
    for c in items:
        debug_rows.append(
            {
                "candidate_id": _candidate_id(c),
                "family": _candidate_family(c),
                "role": _candidate_role(c),
                "score": _candidate_score(c),
                "matched_family": _candidate_family(c) in wanted_families if wanted_families else False,
                "matched_role": _candidate_role(c) in wanted_roles if wanted_roles else False,
            }
        )
    return debug_rows


def build_filtered_pool_for_coverage(
    pool: Any,
    *,
    requirements: Any | None = None,
    topology: Any | None = None,
    roles: Any | None = None,
    required_families: list[str] | None = None,
    per_family_limit: int = 8,
    total_limit: int = 60,
) -> CoverageFilterResult:
    items = _iter_candidates(_obj_get(pool, "items", []))
    tasks = _obj_get(pool, "tasks", [])
    before_count = len(items)

    if not required_families and requirements:
        required_families = _extract_required_families(requirements)

    wanted_families = {_norm(x) for x in (required_families or []) if _norm(x)}
    wanted_roles = set(_extract_required_roles(roles))

    if not items or (not wanted_families and not wanted_roles):
        return CoverageFilterResult(
            filtered_pool=pool,
            selected_items=items,
            selected_candidate_ids=[_candidate_id(x) for x in items if _candidate_id(x)],
            required_families=sorted(wanted_families),
            required_roles=sorted(wanted_roles),
            total_candidates_before=before_count,
            total_candidates_after=len(items),
            role_debug=_make_role_debug(items, wanted_families, wanted_roles),
        )

    buckets_family: dict[str, list[Any]] = {}
    buckets_role: dict[str, list[Any]] = {}
    fallback: list[Any] = []

    for c in items:
        fam = _candidate_family(c)
        role = _candidate_role(c)

        if fam:
            buckets_family.setdefault(fam, []).append(c)
        if role:
            buckets_role.setdefault(role, []).append(c)
        if not fam and not role:
            fallback.append(c)

    for fam_items in buckets_family.values():
        fam_items.sort(key=_candidate_score, reverse=True)
    for role_items in buckets_role.values():
        role_items.sort(key=_candidate_score, reverse=True)

    selected: list[Any] = []
    seen_ids: set[str] = set()

    for fam in wanted_families:
        fam_items = buckets_family.get(fam, [])
        taken = 0
        for c in fam_items:
            cid = _candidate_id(c)
            if cid and cid in seen_ids:
                continue
            if cid:
                seen_ids.add(cid)
            selected.append(c)
            taken += 1
            if taken >= per_family_limit:
                break

    for role in wanted_roles:
        role_items = buckets_role.get(role, [])
        taken = 0
        for c in role_items:
            cid = _candidate_id(c)
            if cid and cid in seen_ids:
                continue
            if cid:
                seen_ids.add(cid)
            selected.append(c)
            taken += 1
            if taken >= per_family_limit:
                break

    remainder: list[Any] = []
    for fam_items in buckets_family.values():
        for c in fam_items:
            cid = _candidate_id(c)
            if cid and cid in seen_ids:
                continue
            remainder.append(c)

    for role_items in buckets_role.values():
        for c in role_items:
            cid = _candidate_id(c)
            if cid and cid in seen_ids:
                continue
            remainder.append(c)

    remainder.extend(fallback)
    remainder.sort(key=_candidate_score, reverse=True)

    for c in remainder:
        if len(selected) >= total_limit:
            break
        cid = _candidate_id(c)
        if cid and cid in seen_ids:
            continue
        if cid:
            seen_ids.add(cid)
        selected.append(c)

    filtered_pool = _update_pool_items(pool, selected, tasks)

    return CoverageFilterResult(
        filtered_pool=filtered_pool,
        selected_items=selected,
        selected_candidate_ids=[_candidate_id(x) for x in selected if _candidate_id(x)],
        required_families=sorted(wanted_families),
        required_roles=sorted(wanted_roles),
        total_candidates_before=before_count,
        total_candidates_after=len(selected),
        role_debug=_make_role_debug(selected, wanted_families, wanted_roles),
    )