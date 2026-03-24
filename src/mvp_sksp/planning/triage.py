from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from ..knowledge.loader import load_triage_rules
from ..knowledge.models import ProjectRequirements
from ..normalization.candidate_classifier import classify_candidate
from .plan_models import TopologyDecision
from .role_expander import ExpandedRole


@dataclass(frozen=True)
class TriageIssue:
    code: str
    severity: str  # "high" | "medium" | "low"
    message: str
    refs: list[str]


def _line_text(line: Any) -> str:
    return " ".join(
        [
            str(getattr(line, "category", "") or ""),
            str(getattr(line, "manufacturer", "") or ""),
            str(getattr(line, "sku", "") or ""),
            str(getattr(line, "name", "") or ""),
            str(getattr(line, "description", "") or ""),
        ]
    ).casefold()


def _line_ref(line: Any) -> str:
    return (
        str(getattr(line, "line_id", None) or "")
        or str(getattr(line, "sku", None) or "")
        or str(getattr(line, "name", None) or "")
        or "line"
    )


def _line_qty(line: Any) -> int:
    try:
        return int(getattr(line, "qty", 0) or 0)
    except Exception:
        return 0


def _line_family(line: Any) -> str | None:
    class _LineLike:
        candidate_id = str(getattr(line, "line_id", None) or getattr(line, "sku", None) or "line")
        category = getattr(line, "category", None)
        sku = getattr(line, "sku", None)
        manufacturer = getattr(line, "manufacturer", None)
        name = getattr(line, "name", None) or getattr(line, "description", "") or ""
        description = getattr(line, "description", None)

    return classify_candidate(_LineLike()).family


def run_triage(
    *,
    spec: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
    roles: Sequence[ExpandedRole],
) -> list[TriageIssue]:
    """KB-driven triage that flags likely systemic issues (compact)."""
    cfg = load_triage_rules() or {}
    kw = cfg.get("keywords", {}) if isinstance(cfg.get("keywords", {}), dict) else {}
    fam = cfg.get("families", {}) if isinstance(cfg.get("families", {}), dict) else {}

    audio_100v = [str(x).casefold() for x in (kw.get("audio_100v") or [])]
    videowall_kw = [str(x).casefold() for x in (kw.get("videowall") or [])]
    amp_any_kw = [str(x).casefold() for x in (kw.get("amp_any") or ["усилитель", "amplifier"])]

    speaker_families = set(fam.get("speakers") or ["wall_speaker", "ceiling_speaker"])
    amp_families = set(fam.get("amplifiers") or ["amplifier"])
    byod_families = set(fam.get("byod") or ["byod_usb_hdmi_gateway", "byod_wireless_presentation", "usb_c_dock"])

    issues: list[TriageIssue] = []
    items = list(getattr(spec, "items", []) or [])

    present_families: dict[str, list[Any]] = {}
    for it in items:
        f = _line_family(it)
        if f:
            present_families.setdefault(f, []).append(it)

    # 1) Missing required role coverage
    present_family_keys = set(present_families.keys())
    for r in roles:
        if not getattr(r, "required", True):
            continue
        allowed = set(getattr(r, "allowed_families", []) or [])
        if allowed and not (present_family_keys & allowed):
            issues.append(
                TriageIssue(
                    code="missing_required_role",
                    severity="high",
                    message=f"Нет покрытия обязательной роли {r.role_key}",
                    refs=[r.role_key],
                )
            )

    # 2) 100V speaker requires 100V amp (robust: amp detected by family OR by keywords)
    speakers_100v = []
    for sf in speaker_families:
        for it in present_families.get(sf, []):
            t = _line_text(it)
            if any(k in t for k in audio_100v):
                speakers_100v.append(it)

    if speakers_100v:
        amps: list[Any] = []
        amps_100v: list[Any] = []

        # 2.1 family-based amps
        for af in amp_families:
            for it in present_families.get(af, []):
                amps.append(it)
                t = _line_text(it)
                if any(k in t for k in audio_100v):
                    amps_100v.append(it)

        # 2.2 keyword fallback (covers early-stage classifier misses)
        if not amps:
            for it in items:
                t = _line_text(it)
                if any(k in t for k in amp_any_kw):
                    amps.append(it)
                    if any(k in t for k in audio_100v):
                        amps_100v.append(it)

        if not amps:
            issues.append(
                TriageIssue(
                    code="missing_100v_amplifier",
                    severity="high",
                    message="Обнаружена 100V акустика, но усилитель не найден",
                    refs=[_line_ref(x) for x in speakers_100v[:3]],
                )
            )
        elif not amps_100v:
            issues.append(
                TriageIssue(
                    code="incompatible_amp_for_100v_speakers",
                    severity="high",
                    message="Обнаружена 100V акустика, но усилитель не выглядит как 100V (возможна несовместимость)",
                    refs=[_line_ref(x) for x in (speakers_100v[:2] + amps[:2])],
                )
            )

    # 3) BYOD false-positive: videowall controller-like text in BYOD families
    for bf in byod_families:
        for it in present_families.get(bf, []):
            t = _line_text(it)
            if any(k in t for k in videowall_kw):
                issues.append(
                    TriageIssue(
                        code="byod_false_positive_videowall",
                        severity="high",
                        message="BYOD-кандидат содержит признаки видеостены (проверь family/signature)",
                        refs=[_line_ref(it)],
                    )
                )

    # 4) Suspicious quantities
    if requirements.room_type == "meeting_room":
        for fkey in ["interactive_panel", "display_panel", "projector"]:
            for it in present_families.get(fkey, []):
                if _line_qty(it) > 1:
                    issues.append(
                        TriageIssue(
                            code="suspicious_qty_display",
                            severity="medium",
                            message="Для переговорной обычно 1 основное устройство отображения",
                            refs=[_line_ref(it)],
                        )
                    )

    cam_req = int(requirements.caps.camera_count or 0)
    if cam_req > 0:
        cam_total = 0
        for fkey in ["ptz_camera", "fixed_conference_camera", "videobar"]:
            for it in present_families.get(fkey, []):
                cam_total += _line_qty(it)
        if cam_total > int(cam_req * 1.5):
            issues.append(
                TriageIssue(
                    code="suspicious_qty_cameras",
                    severity="medium",
                    message=f"Камер больше ожидаемого (requested={cam_req}, total={cam_total})",
                    refs=[],
                )
            )

    _ = topology
    return issues