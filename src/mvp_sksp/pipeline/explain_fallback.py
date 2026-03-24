from __future__ import annotations

from typing import Any

from ..knowledge.models import ProjectRequirements
from ..planning.plan_models import TopologyDecision


def _ensure_list(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if isinstance(v, str):
        return [v.strip()] if v.strip() else []
    return [str(v).strip()] if str(v).strip() else []


def build_fallback_explanations(*, spec: Any, requirements: ProjectRequirements, topology: TopologyDecision) -> None:
    why_comp = _ensure_list(getattr(spec, "why_composition", None))
    why_qty = _ensure_list(getattr(spec, "why_qty_and_price", None))

    seat = int(requirements.caps.seat_count or 0)
    cam = int(requirements.caps.camera_count or 1)

    if not why_comp:
        why_comp = [
            f"Конфигурация выбрана по topology: {topology.topology_key}.",
            "Сначала закрыты обязательные роли: панель/экран, камеры, аудио (захват/воспроизведение), коммутация, кабельная часть.",
            "Позиции взяты из похожих задач Bitrix (ссылки в строках).",
        ]

    if not why_qty:
        why_qty = [
            f"Камеры: {cam} шт — как в запросе.",
            f"Микрофоны: {max(1, seat - 1) if seat else 'по роли'} шт — под участников + 1 председатель.",
            "Акустика: минимум 2 точки в переговорной для равномерного покрытия.",
        ]
        if bool(getattr(requirements.flags, "byod", False)):
            why_qty.append("BYOD: 1 узел подключения, чтобы подключать ноутбук участника без перекоммутации.")

    setattr(spec, "why_composition", why_comp)
    setattr(spec, "why_qty_and_price", why_qty)

    qs = _ensure_list(getattr(spec, "manager_questions", None))
    add_q = [
        "Какое расстояние от стола подключений до панели/экрана? Если до ~10 м — можно упростить коммутацию.",
        "ВКС будет через ноутбук (BYOD) или нужен отдельный ПК/кодек в комнате?",
        "Нужна ли запись/трансляция встреч или только онлайн-участие?",
    ]
    seen = set(qs)
    for q in add_q:
        if q not in seen:
            qs.append(q)
            seen.add(q)
    setattr(spec, "manager_questions", qs)