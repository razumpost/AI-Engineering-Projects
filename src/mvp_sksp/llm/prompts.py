from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Sequence

from ..domain.candidates import CandidatePool
from ..domain.spec import Spec
from ..knowledge.models import ProjectRequirements
from ..planning.plan_models import TopologyDecision
from ..planning.role_expander import ExpandedRole


@dataclass(frozen=True)
class PromptBundle:
    system: str
    user: str


_SCHEMA_EXAMPLE = {
    "version": "sksp.v1",
    "mode": "compose",
    "brief": {"project_summary": "…", "constraints": {}},
    "used_evidence": {"bitrix_task_ids": [123], "candidate_item_ids": ["ci_..."]},
    "operations": [
        {
            "op": "add_line",
            "category": "conference",
            "item": {"candidate_id": "ci_graph_79004_HD-PTZ430HSU3-W"},
            "qty": 2,
            "reason": "Закрывает обязательную роль выбранной topology",
            "evidence_task_ids": [79004],
        }
    ],
    "explanations": {
        "why_composition": ["Сначала закрыты обязательные роли выбранной topology."],
        "why_qty_and_price": ["Количество выведено из caps и quantity rules."],
    },
    "followup_questions": [{"question": "Нужна ли запись/трансляция?", "priority": "medium"}],
    "assumptions": ["…"],
    "risks": ["…"],
}


def _role_plan_dump(roles: Sequence[ExpandedRole]) -> list[dict]:
    return [asdict(r) for r in roles]


def compose_prompt(
    user_request: str,
    pool: CandidatePool,
    requirements: ProjectRequirements,
    roles: Sequence[ExpandedRole],
    topology: TopologyDecision,
    role_candidates: dict[str, list[str]],
) -> PromptBundle:
    system = (
        "Ты проектный помощник по спецификации (СкСп).\n"
        "Верни ТОЛЬКО валидный JSON по контракту sksp.v1.\n"
        "Главное правило: сначала покрой обязательные роли из TopologyDecision и RolePlan.\n"
        "ВАЖНО:\n"
        "- operations — список ОПЕРАЦИЙ; у каждой операции ОБЯЗАТЕЛЬНО поле op\n"
        "- Используй ТОЛЬКО candidate_id из CandidatePool.\n"
        "- Если не хватает данных — сначала собери максимально полную СкСп из доступного, "
        "а вопросы задавай только если без них нельзя выбрать тип/количество.\n"
        "- Вопросы должны быть менеджерские (без VLAN/IGMP/EDID/HDCP/битрейтов).\n"
    )

    user = (
        f"USER_REQUEST:\n{user_request}\n\n"
        f"REQUIREMENTS:\n{requirements.model_dump(mode='json')}\n\n"
        f"TOPOLOGY:\n{topology.model_dump(mode='json')}\n\n"
        f"ROLE_PLAN:\n{_role_plan_dump(roles)}\n\n"
        f"ROLE_CANDIDATES (лучшее для покрытия ролей):\n{role_candidates}\n\n"
        "CANDIDATE_POOL (items/tasks):\n"
        f"items={len(pool.items)} tasks={len(pool.tasks)}\n"
        "Схема ответа (пример):\n"
        f"{_SCHEMA_EXAMPLE}\n\n"
        "Требования к operations:\n"
        "- для add_line: op='add_line', item.candidate_id обязателен\n"
        "- для replace_line: op='replace_line', target.line_id обязателен\n"
        "- qty должно быть числом > 0\n"
        "- evidence_task_ids должны быть реальными id задач Bitrix (если есть)\n"
    )

    return PromptBundle(system=system, user=user)


def patch_prompt(patch_text: str, spec: Spec, pool: CandidatePool) -> PromptBundle:
    system = (
        "Ты редактор СкСп.\n"
        "Верни ТОЛЬКО валидный JSON по контракту sksp.v1.\n"
        "Не создавай дубликаты: replace_line вместо add_line, если пользователь сказал 'замени'.\n"
    )
    user = (
        f"PATCH_TEXT:\n{patch_text}\n\n"
        f"CURRENT_SPEC:\n{spec.model_dump(mode='json')}\n\n"
        f"CANDIDATE_POOL:\nitems={len(pool.items)} tasks={len(pool.tasks)}\n\n"
        "Верни operations для правки, не пересобирай всё заново."
    )
    return PromptBundle(system=system, user=user)