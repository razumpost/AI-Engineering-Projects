from __future__ import annotations

import uuid
from dataclasses import fields, is_dataclass
from typing import Any

from ..knowledge.loader import load_knowledge_map
from ..knowledge.models import DependencyRule
from .plan_models import ClassifiedCandidate, LineItemPlan


def _km():
    return load_knowledge_map()


def _has_family(plan: list[LineItemPlan], family: str) -> bool:
    return any(li.family == family for li in plan)


def _pick_best_candidate(cands: list[ClassifiedCandidate]) -> ClassifiedCandidate | None:
    if not cands:
        return None
    cands = sorted(cands, key=lambda c: float(c.score or 0.0), reverse=True)
    return cands[0]


def _ensure_family(
    plan: list[LineItemPlan],
    family: str,
    candidates_by_family: dict[str, list[ClassifiedCandidate]],
    reason: str,
    qty: int = 1,
) -> None:
    if _has_family(plan, family):
        return
    best = _pick_best_candidate(candidates_by_family.get(family, []))
    if not best:
        plan.append(
            LineItemPlan(
                line_id=str(uuid.uuid4()),
                family=family,
                qty=qty,
                sku=None,
                manufacturer=None,
                name=f"[TBD] {family}",
                description=None,
                unit=None,
                price=None,
                currency=None,
                url=None,
                evidence=None,
                notes=[reason, "нет точного совпадения в KB — требуется подобрать вручную/уточнить"],
                meta={"autofill": True},
            )
        )
        return

    plan.append(
        LineItemPlan(
            line_id=str(uuid.uuid4()),
            family=family,
            qty=qty,
            sku=best.sku,
            manufacturer=best.manufacturer,
            name=best.name,
            description=best.description,
            unit=best.unit,
            price=best.price,
            currency=best.currency,
            url=best.url,
            evidence=best.evidence,
            notes=[reason],
            meta={**(best.meta or {}), "autofill": True},
        )
    )


def _apply_requires_all(
    plan: list[LineItemPlan],
    rule: DependencyRule,
    candidates_by_family: dict[str, list[ClassifiedCandidate]],
) -> None:
    for dep in rule.requires_all:
        _ensure_family(
            plan,
            dep,
            candidates_by_family,
            reason=f"добавлено по зависимости: {rule.family} требует {dep}",
            qty=1,
        )


def _apply_requires_any(
    plan: list[LineItemPlan],
    rule: DependencyRule,
    candidates_by_family: dict[str, list[ClassifiedCandidate]],
) -> None:
    for group in rule.requires_any:
        # group is a set of alternatives, at least one should exist.
        if any(_has_family(plan, fam) for fam in group):
            continue
        # pick the best available candidate among group families
        best_fam = None
        best_score = -1.0
        for fam in group:
            c = _pick_best_candidate(candidates_by_family.get(fam, []))
            s = float(c.score or 0.0) if c else -1.0
            if s > best_score:
                best_score = s
                best_fam = fam
        if best_fam is None:
            # fallback to first as placeholder
            best_fam = group[0] if group else None
        if best_fam:
            _ensure_family(
                plan,
                best_fam,
                candidates_by_family,
                reason=f"добавлено по зависимости: {rule.family} требует один из {group}",
                qty=1,
            )


def _apply_optional_with(
    plan: list[LineItemPlan],
    rule: DependencyRule,
    candidates_by_family: dict[str, list[ClassifiedCandidate]],
) -> None:
    for fam in rule.optional_with:
        # add only if we have good candidate
        best = _pick_best_candidate(candidates_by_family.get(fam, []))
        if not best:
            continue
        if float(best.score or 0.0) < 0.45:
            continue
        _ensure_family(
            plan,
            fam,
            candidates_by_family,
            reason=f"опционально добавлено: вместе с {rule.family}",
            qty=1,
        )


def _apply_incompatible(
    plan: list[LineItemPlan],
    rule: DependencyRule,
) -> None:
    if not rule.incompatible_with:
        return
    if not _has_family(plan, rule.family):
        return
    for bad in rule.incompatible_with:
        for li in list(plan):
            if li.family == bad:
                li.notes.append(f"конфликт: {rule.family} несовместимо с {bad} — проверить выбор")
                li.meta = dict(li.meta or {})
                li.meta["conflict"] = True


def _apply_recommended(
    plan: list[LineItemPlan],
    rule: DependencyRule,
    candidates_by_family: dict[str, list[ClassifiedCandidate]],
) -> None:
    for fam in rule.recommended_with:
        best = _pick_best_candidate(candidates_by_family.get(fam, []))
        if not best:
            continue
        if float(best.score or 0.0) < 0.60:
            continue
        _ensure_family(
            plan,
            fam,
            candidates_by_family,
            reason=f"рекомендуется вместе с {rule.family}",
            qty=1,
        )


def _needs_amp_for_100v(plan: list[LineItemPlan]) -> bool:
    has_100v = any((li.meta or {}).get("tag_100v") or (li.family == "speaker_100v") for li in plan)
    if not has_100v:
        return False
    return not _has_family(plan, "amplifier")


def _needs_dsp_for_discussion(plan: list[LineItemPlan]) -> bool:
    has_discussion = _has_family(plan, "discussion_system") or any("дискус" in (li.name or "").casefold() for li in plan)
    if not has_discussion:
        return False
    return not _has_family(plan, "dsp_audio")


def resolve_dependencies(
    base_plan: list[LineItemPlan],
    candidates_by_family: dict[str, list[ClassifiedCandidate]],
) -> list[LineItemPlan]:
    """Apply dependency rules from knowledge base + minimal safety rules."""
    km = _km()
    plan = list(base_plan)

    # apply KB rules for each family present in plan (and recursively for newly added)
    changed = True
    while changed:
        changed = False
        present = {li.family for li in plan if li.family}

        for fam in sorted(present):
            rule = km.dependency_rules.get(fam)
            if not rule:
                continue

            before = len(plan)
            _apply_requires_all(plan, rule, candidates_by_family)
            _apply_requires_any(plan, rule, candidates_by_family)
            _apply_optional_with(plan, rule, candidates_by_family)
            _apply_recommended(plan, rule, candidates_by_family)
            _apply_incompatible(plan, rule)
            if len(plan) != before:
                changed = True

        # minimal deterministic safety heuristics:
        if _needs_amp_for_100v(plan):
            before = len(plan)
            _ensure_family(
                plan,
                "amplifier",
                candidates_by_family,
                reason="добавлено по эвристике: 100V акустика требует усилитель/линейный усилитель",
                qty=1,
            )
            changed = changed or (len(plan) != before)

        if _needs_dsp_for_discussion(plan):
            before = len(plan)
            _ensure_family(
                plan,
                "dsp_audio",
                candidates_by_family,
                reason="добавлено по эвристике: дискуссионная система обычно требует DSP/микшер",
                qty=1,
            )
            changed = changed or (len(plan) != before)

    return plan