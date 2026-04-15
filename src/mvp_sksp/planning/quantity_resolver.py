from __future__ import annotations

from typing import Any

from ..knowledge.models import ProjectRequirements
from ..normalization.candidate_classifier import classify_candidates
from .plan_models import TopologyDecision


def _line_key(line: Any) -> str:
    return str(getattr(line, "line_id", "") or id(line))


def _drop_lines(spec: Any, lines: list[Any]) -> None:
    drop_keys = {_line_key(x) for x in lines}
    items = [x for x in list(getattr(spec, "items", []) or []) if _line_key(x) not in drop_keys]
    setattr(spec, "items", items)


def _line_placeholder_kind(line: Any) -> str | None:
    meta = getattr(line, "meta", None)
    if isinstance(meta, dict) and meta.get("placeholder_kind"):
        return str(meta.get("placeholder_kind"))
    return None


def _line_desc_len(line: Any) -> int:
    return len(
        (
            str(getattr(line, "name", "") or "")
            + " "
            + str(getattr(line, "description", "") or "")
            + " "
            + str(getattr(line, "model", "") or "")
            + " "
            + str(getattr(line, "sku", "") or "")
        ).strip()
    )


def _line_price_score(line: Any) -> float:
    # unit_price_rub бывает у candidate-like строк, unit_price.amount — у LineItem
    try:
        p = getattr(line, "unit_price_rub", None)
        if p not in (None, "", 0, 0.0):
            return 1.0
    except Exception:
        pass

    try:
        money = getattr(line, "unit_price", None)
        amount = getattr(money, "amount", None) if money is not None else None
        if amount not in (None, "", 0, 0.0):
            return 1.0
    except Exception:
        pass

    return 0.0


def _classify_spec_lines(spec: Any) -> dict[str, list[Any]]:
    items = list(getattr(spec, "items", []) or [])
    classes = classify_candidates(items)

    fam: dict[str, list[Any]] = {}
    for line, cls in zip(items, classes):
        family = getattr(cls, "family", None)
        if not family:
            continue
        fam.setdefault(str(family), []).append(line)
    return fam


def _prefer_placeholder(lines: list[Any], placeholder_kind: str) -> Any:
    # Для critical placeholder’ов discussion ветки сначала держим именно placeholder,
    # а не позволяем “лучшему” шумному line вытеснить его.
    for line in lines:
        if _line_placeholder_kind(line) == placeholder_kind:
            return line

    # Иначе — самая “качественная” строка: цена > длина описания
    def score(line: Any) -> tuple[float, int]:
        return (_line_price_score(line), _line_desc_len(line))

    return sorted(lines, key=score, reverse=True)[0]


def _set_qty(line: Any, qty: int) -> None:
    try:
        setattr(line, "qty", qty)
    except Exception:
        pass


def _dedupe_family_keep_best(spec: Any, lines: list[Any], best: Any) -> None:
    to_drop = [x for x in lines if _line_key(x) != _line_key(best)]
    if to_drop:
        _drop_lines(spec, to_drop)


def _has_discussion_intent(requirements: ProjectRequirements, topology: TopologyDecision) -> bool:
    if topology.topology_key == "meeting_room_discussion_only":
        return True

    if bool(requirements.flags.control) and int(requirements.caps.seat_count or 0) >= 8:
        return True

    return False


def resolve_quantities(
    spec: Any,
    source_pool: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
) -> list[str]:
    _ = source_pool
    warnings: list[str] = []

    fam = _classify_spec_lines(spec)
    seat_count = int(requirements.caps.seat_count or 0)
    discussion_mode = _has_discussion_intent(requirements, topology)

    # ------------------------------------------------------------
    # DISCUSSION MODE
    # ------------------------------------------------------------
    if discussion_mode:
        delegate_lines = fam.get("delegate_unit", [])
        chairman_lines = fam.get("chairman_unit", [])
        central_lines = fam.get("discussion_central_unit", [])
        dsp_lines = fam.get("discussion_dsp", [])
        psu_lines = fam.get("power_supply_discussion", [])

        chairman_present = bool(chairman_lines)

        if delegate_lines:
            best = _prefer_placeholder(delegate_lines, "delegate_unit")
            desired = max(1, seat_count - 1) if chairman_present and seat_count > 0 else max(1, seat_count or 1)
            _set_qty(best, desired)
            _dedupe_family_keep_best(spec, delegate_lines, best)
            warnings.append(f"Quantity resolver: delegate_unit qty set to {desired}")

        if chairman_lines:
            best = _prefer_placeholder(chairman_lines, "chairman_unit")
            _set_qty(best, 1)
            _dedupe_family_keep_best(spec, chairman_lines, best)
            warnings.append("Quantity resolver: chairman_unit qty set to 1")

        if central_lines:
            best = _prefer_placeholder(central_lines, "discussion_central_unit")
            _set_qty(best, 1)
            _dedupe_family_keep_best(spec, central_lines, best)
            warnings.append("Quantity resolver: discussion_central_unit qty set to 1")

        if dsp_lines:
            best = _prefer_placeholder(dsp_lines, "discussion_dsp")
            _set_qty(best, 1)
            _dedupe_family_keep_best(spec, dsp_lines, best)
            warnings.append("Quantity resolver: discussion_dsp qty set to 1")

        if psu_lines:
            best = _prefer_placeholder(psu_lines, "power_supply_discussion")
            desired = 1 if seat_count > 20 else max(1, int(round(float(getattr(best, "qty", 1) or 1))))
            _set_qty(best, desired)
            _dedupe_family_keep_best(spec, psu_lines, best)
            warnings.append(f"Quantity resolver: power_supply_discussion qty set to {desired}")

        return warnings

    # ------------------------------------------------------------
    # ORDINARY MEETING ROOM
    # ------------------------------------------------------------
    camera_main = fam.get("ptz_camera", []) + fam.get("fixed_conference_camera", []) + fam.get("videobar", [])
    audio_capture = fam.get("tabletop_mic", []) + fam.get("ceiling_mic_array", []) + fam.get("speakerphone", [])
    audio_playback = fam.get("soundbar", []) + fam.get("wall_speaker", []) + fam.get("ceiling_speaker", [])

    if camera_main:
        # ordinary meeting room: реальные камеры не размножаем здесь, по одной на line
        best = sorted(camera_main, key=lambda x: (_line_price_score(x), _line_desc_len(x)), reverse=True)[0]
        _set_qty(best, max(1, int(getattr(best, "qty", 1) or 1)))
        warnings.append("Quantity resolver: ordinary meeting room camera qty normalized")

    if audio_capture:
        # ordinary meeting room: не тащим qty микрофона по seat_count
        for line in audio_capture:
            if _line_placeholder_kind(line):
                continue
            _set_qty(line, 1)
        warnings.append("Quantity resolver: ordinary meeting room audio_capture qty normalized to 1 for real lines")

    if audio_playback:
        for line in audio_playback:
            if _line_placeholder_kind(line):
                continue
            cur = int(round(float(getattr(line, "qty", 1) or 1)))
            _set_qty(line, max(1, cur))
        warnings.append("Quantity resolver: ordinary meeting room audio_playback qty normalized")

    return warnings