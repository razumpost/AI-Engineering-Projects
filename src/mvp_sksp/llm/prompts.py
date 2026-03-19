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
        "CandidatePool уже отфильтрован по topology, room_type и allowed_families.\n"
        "Главное правило: сначала покрой обязательные роли из TopologyDecision и RolePlan, затем добери полезные вторичные позиции.\n"
        "ЖЁСТКИЕ ОГРАНИЧЕНИЯ:\n"
        "- operations — список ОПЕРАЦИЙ; у каждой операции ОБЯЗАТЕЛЬНО поле op\n"
        "- использовать можно ТОЛЬКО candidate_id из CandidatePool\n"
        "- для каждой роли используй ТОЛЬКО candidate_id из RoleCandidates для этой роли\n"
        "- не используй display/panel candidates как камеры\n"
        "- не используй projection_screen/LED/video_mixer в meeting_room\n"
        "- не собирай параллельно несколько альтернативных topology в одной СкСп\n"
        "- не дублируй строки\n"
        "- followup_questions — список объектов: {question: string, priority: high|medium|low}\n"
    )

    user = (
        "Сформируй максимально полный первый черновик СкСп.\n"
        "Приоритет принятия решения:\n"
        "1) закрыть required roles из TopologyDecision\n"
        "2) учесть RolePlan и preferred_families\n"
        "3) использовать candidate_id из RoleCandidates\n"
        "4) не добавлять альтернативные стеки оборудования\n"
        "5) followup_questions только если после заполнения остаются критичные неопределённости\n\n"
        f"UserRequest:\n{user_request}\n\n"
        f"ProjectRequirements(JSON):\n{requirements.model_dump(mode='json')}\n\n"
        f"TopologyDecision(JSON):\n{topology.model_dump(mode='json')}\n\n"
        f"RolePlan(JSON):\n{_role_plan_dump(roles)}\n\n"
        f"RoleCandidates(JSON):\n{role_candidates}\n\n"
        f"CandidatePool(JSON):\n{pool.model_dump(mode='json')}\n\n"
        f"JSON пример:\n{_SCHEMA_EXAMPLE}\n"
    )
    return PromptBundle(system=system, user=user)


def patch_prompt(patch_text: str, current_spec: Spec, pool: CandidatePool) -> PromptBundle:
    system = (
        "Ты проектный помощник по правкам СкСп.\n"
        "Верни ТОЛЬКО валидный JSON по контракту sksp.v1.\n"
        "ВАЖНО:\n"
        "- operations — список ОПЕРАЦИЙ; у каждой операции ОБЯЗАТЕЛЬНО поле op\n"
        "- если пользователь пишет 'замени' — используй replace_line (НЕ add_line)\n"
        "- followup_questions — список объектов: {question: string, priority: high|medium|low}\n"
    )

    user = (
        "Применить правку к текущей спецификации через operations.\n"
        "Если неоднозначно — operations=[] и 1-3 вопроса.\n\n"
        f"PatchText:\n{patch_text}\n\n"
        f"CurrentSpec(JSON):\n{current_spec.model_dump(mode='json')}\n\n"
        f"CandidatePool(JSON):\n{pool.model_dump(mode='json')}\n\n"
        f"JSON пример:\n{_SCHEMA_EXAMPLE}\n"
    )
    return PromptBundle(system=system, user=user)
