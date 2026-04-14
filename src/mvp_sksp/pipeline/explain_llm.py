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


def _as_clean_list(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    return [s] if s else []


def _append_unique(base: list[str], extra: list[str], *, reject_prefixes: tuple[str, ...] = ()) -> list[str]:
    out = list(base or [])
    seen = set(out)

    for x in extra:
        s = str(x).strip()
        if not s:
            continue
        if reject_prefixes and any(s.startswith(p) for p in reject_prefixes):
            continue
        if s not in seen:
            out.append(s)
            seen.add(s)

    return out


def try_llm_explain(
    *,
    llm: ChatCompletionClient,
    spec: Any,
    requirements: ProjectRequirements,
    topology: TopologyDecision,
) -> None:
    """
    Small separate LLM pass ONLY for explanations/questions.
    Never edits spec lines.

    ВАЖНО:
    - не перезаписывает already-grounded fallback explanations
    - не добавляет LLM-generated [price_missing] risks
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

    why_comp = _as_clean_list(data.get("why_composition", []))
    why_qty = _as_clean_list(data.get("why_qty_and_price", []))
    qs = _as_clean_list(data.get("manager_questions", []))
    ass = _as_clean_list(data.get("assumptions", []))
    risks = _as_clean_list(data.get("risks", []))

    # Никогда не доверяем LLM price_missing — это должен решать validator/fallback.
    risks = [x for x in risks if not x.startswith("[price_missing]")]

    current_why_comp = list(getattr(spec, "why_composition", []) or [])
    current_why_qty = list(getattr(spec, "why_qty_and_price", []) or [])
    current_qs = list(getattr(spec, "manager_questions", []) or [])
    current_ass = list(getattr(spec, "assumptions", []) or [])
    current_risks = list(getattr(spec, "risks", []) or [])

    # Fallback explanations grounded in final spec > LLM. Заполняем только если пусто.
    if not current_why_comp and why_comp:
        setattr(spec, "why_composition", why_comp)

    if not current_why_qty and why_qty:
        setattr(spec, "why_qty_and_price", why_qty)

    # Вопросы/допущения/риски можно только аккуратно дополнять.
    if qs:
        setattr(spec, "manager_questions", _append_unique(current_qs, qs))

    if hasattr(spec, "assumptions"):
        setattr(spec, "assumptions", _append_unique(current_ass, ass))

    if hasattr(spec, "risks"):
        setattr(spec, "risks", _append_unique(current_risks, risks, reject_prefixes=("[price_missing]",)))