from __future__ import annotations

from typing import Any

from ..knowledge.models import ProjectRequirements
from ..llm.client import ChatCompletionClient, extract_json_object
from ..planning.plan_models import TopologyDecision


def _top_items_brief(spec: Any, limit: int = 18) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for it in list(getattr(spec, "items", []) or [])[:limit]:
        out.append(
            {
                "category": getattr(it, "category", None),
                "manufacturer": getattr(it, "manufacturer", None),
                "sku": getattr(it, "sku", None),
                "name": getattr(it, "name", None),
                "qty": getattr(it, "qty", None),
                "unit_price_rub": getattr(it, "unit_price_rub", None),
                "bitrix_task_ids": list(getattr(getattr(it, "evidence", None), "bitrix_task_ids", []) or []),
            }
        )
    return out


def try_llm_explain(
    *,
    llm: ChatCompletionClient,
    spec: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
) -> None:
    """
    B) Small separate LLM pass ONLY for explanations/questions.
    Never edits spec lines. If fails -> do nothing (fallback already exists).
    """
    payload = {
        "request_summary": getattr(spec, "project_summary", "") or "",
        "room_type": requirements.room_type,
        "caps": requirements.caps.model_dump(mode="json"),
        "flags": requirements.flags.model_dump(mode="json"),
        "topology": topology.model_dump(mode="json"),
        "items": _top_items_brief(spec, limit=20),
    }

    system = (
        "Ты менеджерский ассистент по СкСп.\n"
        "Тебе нельзя менять состав позиций.\n"
        "Сформируй понятные объяснения (без инженерного жаргона) и вопросы.\n"
        "Верни ТОЛЬКО JSON:\n"
        "{"
        '"why_composition":[...],'
        '"why_qty_and_price":[...],'
        '"manager_questions":[...],'
        '"assumptions":[...],'
        '"risks":[...]'
        "}\n"
    )
    user = f"DATA:\n{payload}\n"

    text = llm.complete([{"role": "system", "text": system}, {"role": "user", "text": user}])
    data = extract_json_object(text)

    why_comp = list(data.get("why_composition", []) or [])
    why_qty = list(data.get("why_qty_and_price", []) or [])
    qs = list(data.get("manager_questions", []) or [])
    ass = list(data.get("assumptions", []) or [])
    risks = list(data.get("risks", []) or [])

    if why_comp:
        setattr(spec, "why_composition", [str(x) for x in why_comp if str(x).strip()])
    if why_qty:
        setattr(spec, "why_qty_and_price", [str(x) for x in why_qty if str(x).strip()])
    if qs:
        setattr(spec, "manager_questions", [str(x) for x in qs if str(x).strip()])

    if ass and hasattr(spec, "assumptions"):
        cur = list(getattr(spec, "assumptions", []) or [])
        for x in ass:
            s = str(x).strip()
            if s and s not in cur:
                cur.append(s)
        setattr(spec, "assumptions", cur)

    if risks and hasattr(spec, "risks"):
        cur = list(getattr(spec, "risks", []) or [])
        for x in risks:
            s = str(x).strip()
            if s and s not in cur:
                cur.append(s)
        setattr(spec, "risks", cur)