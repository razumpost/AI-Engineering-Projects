from __future__ import annotations

from decimal import Decimal
from typing import Any

from ..normalization.candidate_classifier import classify_candidate, classify_candidates


_CATEGORY_ORDER = {
    "display": 0,
    "cameras": 1,
    "conference": 2,
    "signal_transport": 3,
    "processing": 4,
    "misc": 5,
}

_FAMILY_TO_CATEGORY = {
    "ptz_camera": "cameras",
    "fixed_conference_camera": "cameras",
    "videobar": "conference",
    "display_panel": "display",
    "interactive_panel": "display",
    "projector": "display",
    "projection_screen": "display",
    "videowall_panel": "display",
    "led_cabinet": "display",
    "delegate_unit": "conference",
    "chairman_unit": "conference",
    "tabletop_mic": "conference",
    "ceiling_mic_array": "conference",
    "speakerphone": "conference",
    "conference_controller": "conference",
    "discussion_central_unit": "conference",
    "discussion_dsp": "conference",
    "dsp": "conference",
    "usb_dsp_bridge": "conference",
    "wall_speaker": "conference",
    "ceiling_speaker": "conference",
    "soundbar": "conference",
    "amplifier": "conference",
    "power_supply_discussion": "conference",
    "byod_wireless_presentation": "signal_transport",
    "byod_usb_hdmi_gateway": "signal_transport",
    "usb_c_dock": "signal_transport",
    "matrix_switcher": "signal_transport",
    "presentation_switcher": "signal_transport",
    "av_over_ip_tx": "signal_transport",
    "av_over_ip_rx": "signal_transport",
    "simple_io_hub": "signal_transport",
    "managed_switch": "signal_transport",
    "poe_switch": "signal_transport",
    "cable_hdmi": "signal_transport",
    "cable_usb": "signal_transport",
    "cable_cat": "signal_transport",
    "cabling_av": "signal_transport",
    "adapters_kit": "signal_transport",
    "mounting_kit": "signal_transport",
    "power_accessories": "signal_transport",
    "control_processor": "processing",
    "touch_panel": "processing",
    "keypad_controller": "processing",
    "video_mixer": "processing",
    "operator_monitor": "processing",
    "recorder": "processing",
    "stream_encoder": "processing",
}


def _pool_items(pool: Any) -> list[Any]:
    return list(getattr(pool, "items", []) or [])


def _classified_by_id(pool: Any) -> dict[str, Any]:
    return {c.candidate_id: c for c in classify_candidates(_pool_items(pool))}


def _line_candidate_id(line: Any) -> str | None:
    cid = getattr(line, "candidate_id", None)
    if cid:
        return str(cid)
    meta = getattr(line, "meta", None)
    if isinstance(meta, dict) and meta.get("candidate_id"):
        return str(meta.get("candidate_id"))
    return None


def _line_qty_value(line: Any) -> Decimal:
    q = getattr(line, "qty", None)
    if isinstance(q, Decimal):
        return q
    try:
        return Decimal(str(q or 0))
    except Exception:
        return Decimal("0")


def _line_family(line: Any, cls_by_id: dict[str, Any]) -> str | None:
    cid = _line_candidate_id(line)
    if cid and cid in cls_by_id and getattr(cls_by_id[cid], "family", None):
        return cls_by_id[cid].family

    class _LineLike:
        candidate_id = cid or "line"
        category = getattr(line, "category", None)
        sku = getattr(line, "sku", None)
        manufacturer = getattr(line, "manufacturer", None)
        model = getattr(line, "model", None)
        name = getattr(line, "name", None) or getattr(line, "description", "") or ""
        description = getattr(line, "description", None)

    c = classify_candidate(_LineLike())
    return c.family


def merge_duplicate_candidate_lines(spec: Any) -> None:
    items = list(getattr(spec, "items", []) or [])
    out: list[Any] = []

    by_key: dict[tuple[str, str, str], Any] = {}
    for line in items:
        cid = _line_candidate_id(line)
        if cid:
            key = ("cid", cid, "")
        else:
            key = ("sku", str(getattr(line, "manufacturer", "") or ""), str(getattr(line, "sku", "") or ""))

        if key not in by_key:
            by_key[key] = line
            out.append(line)
            continue

        existing = by_key[key]
        setattr(existing, "qty", _line_qty_value(existing) + _line_qty_value(line))

    setattr(spec, "items", out)


def normalize_categories(spec: Any, source_pool: Any) -> None:
    cls_by_id = _classified_by_id(source_pool)

    for line in list(getattr(spec, "items", []) or []):
        fam = _line_family(line, cls_by_id)
        if not fam:
            continue
        category = _FAMILY_TO_CATEGORY.get(fam, "misc")
        setattr(line, "category", category)


def sort_spec_items(spec: Any) -> None:
    items = list(getattr(spec, "items", []) or [])

    def sort_key(line: Any) -> tuple[int, str, str]:
        category = str(getattr(line, "category", "misc") or "misc")
        manufacturer = str(getattr(line, "manufacturer", "") or "")
        sku = str(getattr(line, "sku", "") or "")
        return (_CATEGORY_ORDER.get(category, 99), manufacturer.casefold(), sku.casefold())

    items.sort(key=sort_key)
    setattr(spec, "items", items)