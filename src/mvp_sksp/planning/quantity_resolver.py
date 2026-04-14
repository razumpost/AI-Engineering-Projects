from __future__ import annotations

from typing import Any

from ..knowledge.models import ProjectRequirements
from ..normalization.candidate_classifier import classify_candidate
from .plan_models import TopologyDecision


def _line_key(line: Any) -> str:
    for key in ("item_key", "line_id", "candidate_id"):
        val = getattr(line, key, None)
        if val:
            return str(val)
    return "::".join(
        [
            str(getattr(line, "manufacturer", "") or ""),
            str(getattr(line, "sku", "") or ""),
            str(getattr(line, "model", "") or ""),
            str(getattr(line, "name", "") or ""),
        ]
    ).casefold()


def _line_qty(line: Any) -> float:
    try:
        return float(getattr(line, "qty", 0) or 0)
    except Exception:
        return 0.0


def _set_line_qty(line: Any, qty: int | float) -> None:
    try:
        setattr(line, "qty", qty)
    except Exception:
        pass


def _drop_lines(spec: Any, lines: list[Any]) -> None:
    drop_keys = {_line_key(x) for x in lines}
    items = [x for x in list(getattr(spec, "items", []) or []) if _line_key(x) not in drop_keys]
    setattr(spec, "items", items)


def _family_lines(spec: Any) -> dict[str, list[Any]]:
    items = list(getattr(spec, "items", []) or [])
    out: dict[str, list[Any]] = {}

    for line in items:
        fam = classify_candidate(line).family
        if not fam:
            continue
        out.setdefault(fam, []).append(line)

    return out


def _pick_best_line(lines: list[Any]) -> Any:
    def score(line: Any) -> tuple[float, float]:
        price = 0.0
        try:
            price = float(getattr(line, "unit_price_rub", 0) or 0)
        except Exception:
            price = 0.0
        desc_len = float(len(str(getattr(line, "description", "") or "")))
        return (1.0 if price > 0 else 0.0, desc_len)

    return sorted(lines, key=score, reverse=True)[0]


def _keep_one(spec: Any, fam: dict[str, list[Any]], family: str) -> dict[str, list[Any]]:
    lines = fam.get(family, [])
    if not lines:
        return fam

    best = _pick_best_line(lines)
    _drop_lines(spec, [x for x in lines if _line_key(x) != _line_key(best)])
    return _family_lines(spec)


def resolve_quantities(
    spec: Any,
    source_pool: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
) -> list[str]:
    _ = source_pool
    _ = topology

    warnings: list[str] = []
    seat_count = int(requirements.caps.seat_count or 0)

    fam = _family_lines(spec)

    for family in [
        "delegate_unit",
        "chairman_unit",
        "discussion_central_unit",
        "power_supply_discussion",
        "discussion_dsp",
        "wall_speaker",
        "ceiling_speaker",
        "soundbar",
    ]:
        fam = _keep_one(spec, fam, family)

    chairman_present = bool(fam.get("chairman_unit"))

    # delegate count:
    # если есть отдельный председатель, считаем места как total seats incl chairman
    # => delegates = seats - 1
    delegate_lines = fam.get("delegate_unit", [])
    if delegate_lines:
        best = _pick_best_line(delegate_lines)
        if seat_count > 0:
            desired = max(1, seat_count - 1) if chairman_present else seat_count
        else:
            desired = max(1, int(round(_line_qty(best) or 1)))
        _set_line_qty(best, desired)
        warnings.append(f"Quantity resolver: delegate_unit qty set to {desired}")
        fam = _family_lines(spec)

    chairman_lines = fam.get("chairman_unit", [])
    if chairman_lines:
        best = _pick_best_line(chairman_lines)
        _set_line_qty(best, 1)
        warnings.append("Quantity resolver: chairman_unit qty set to 1")
        fam = _family_lines(spec)

    central_lines = fam.get("discussion_central_unit", [])
    if central_lines:
        best = _pick_best_line(central_lines)
        _set_line_qty(best, 1)
        warnings.append("Quantity resolver: discussion_central_unit qty set to 1")
        fam = _family_lines(spec)

    psu_lines = fam.get("power_supply_discussion", [])
    if psu_lines:
        best = _pick_best_line(psu_lines)
        desired = 1 if seat_count > 20 else max(1, int(round(_line_qty(best) or 1)))
        _set_line_qty(best, desired)
        warnings.append(f"Quantity resolver: power_supply_discussion qty set to {desired}")
        fam = _family_lines(spec)

    dsp_lines = fam.get("discussion_dsp", [])
    if dsp_lines:
        best = _pick_best_line(dsp_lines)
        _set_line_qty(best, 1)
        warnings.append("Quantity resolver: discussion_dsp qty set to 1")
        fam = _family_lines(spec)

    if fam.get("soundbar"):
        best = _pick_best_line(fam["soundbar"])
        _set_line_qty(best, 1)
        fam = _family_lines(spec)

    for family in ("wall_speaker", "ceiling_speaker"):
        if fam.get(family):
            best = _pick_best_line(fam[family])
            if _line_qty(best) < 2:
                _set_line_qty(best, 2)
            fam = _family_lines(spec)

    items = [x for x in list(getattr(spec, "items", []) or []) if _line_qty(x) > 0]
    setattr(spec, "items", items)

    return warnings