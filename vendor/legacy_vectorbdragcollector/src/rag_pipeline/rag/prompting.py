# =========================
# File: src/rag_pipeline/rag/prompting.py
# =========================
from __future__ import annotations

import json
from typing import Dict, List, Optional

from .intent import RagIntent


def build_messages(question: str, context_blocks: List[Dict], intent: RagIntent) -> List[Dict[str, str]]:
    """Default RAG answering prompt (non-СКСП)."""
    system = (
        "Ты — помощник, отвечающий по внутренней базе знаний компании.\n"
        "ЖЕЛЕЗНЫЕ правила:\n"
        "1) Отвечай ТОЛЬКО по предоставленному контексту. Никаких догадок.\n"
        "2) Если в контексте нет данных — прямо скажи, чего не хватает.\n"
        "3) Каждый факт/цифра/цена должны иметь цитату вида [SRC=... chunk=...].\n"
        "4) Не смешивай поставщиков: если не ясно кто дал цену — так и напиши.\n"
        "5) Не придумывай НДС/валюту/сроки, если этого нет в цитате.\n"
    )

    if intent.name == "price":
        system += (
            "\nФормат ответа для цен:\n"
            "- Дай таблицу Markdown с колонками: Позиция | Цена | Валюта/НДС (если есть) | Срок/условия (если есть) | Источник.\n"
            "- Включи ВСЕ найденные цены/предложения из контекста (не одну).\n"
            "- Если в чанке несколько строк прайса/КП — вытащи ключевые позиции, не теряй цифры.\n"
        )

    ctx_lines: List[str] = []
    for i, b in enumerate(context_blocks, 1):
        src = b.get("src", "unknown")
        chunk = b.get("chunk", b.get("chunk_id", ""))
        doc_type = b.get("doc_type") or b.get("meta", {}).get("doc_type") or ""
        prefix = f"### CONTEXT {i}\n[SRC={src} chunk={chunk} doc_type={doc_type}]\n"
        ctx_lines.append(prefix + (b.get("text") or "") + "\n")

    user = (
        "Контекст:\n"
        + "\n".join(ctx_lines)
        + "\nВопрос:\n"
        + (question or "")
        + "\n\nСформируй ответ строго по контексту и укажи источники."
    )

    return [
        {"role": "system", "text": system},
        {"role": "user", "text": user},
    ]


def build_planner_messages(user_request: str, *, known_doc_types: Optional[List[str]] = None) -> List[Dict[str, str]]:
    """Planner: turn a user request into retrieval queries + doc_type needs.

    Output MUST be strict JSON (no markdown), schema:

    {
      "retrieval_queries": ["..."],
      "need_doc_types": ["..."],
      "notes": "optional"
    }
    """
    known = known_doc_types or [
        "price_list",
        "vendor_kp",
        "sksps",
        "customer_kp_snapshot",
        "bitrix_task",
        "bitrix_file",
        "bitrix_call",
        "bitrix_chat",
    ]

    system = (
        "Ты — планировщик запросов к RAG-поиску по внутренней базе.\n"
        "Твоя задача: по запросу пользователя предложить 3–8 поисковых запросов для векторного поиска,\n"
        "и указать какие типы источников нужны (need_doc_types).\n"
        "ВАЖНО: в запросах НЕЛЬЗЯ использовать персональные данные (имена, телефоны, ИНН, точные адреса, номера договоров).\n"
        "Если пользователь их дал — игнорируй их и переформулируй нейтрально.\n"
        "Отвечай ТОЛЬКО валидным JSON, без комментариев и без markdown."
    )

    user = (
        "Запрос пользователя:\n"
        f"{user_request}\n\n"
        "Доступные/ожидаемые типы источников (пример):\n"
        f"{json.dumps(known, ensure_ascii=False)}\n\n"
        "Сгенерируй JSON по схеме: "
        '{"retrieval_queries":[...],"need_doc_types":[...],"notes":"..."}'
    )

    return [{"role": "system", "text": system}, {"role": "user", "text": user}]


def build_sksps_generator_messages(
    user_request: str,
    *,
    context_blocks: List[Dict],
    sksps_columns: List[str],
) -> List[Dict[str, str]]:
    """Generator: produce СкСп JSON, fill with equipment selection and questions.

    Output MUST be strict JSON (no markdown). The model MUST NOT include personal data.
    """

    system = (
        "Ты — инженер-проектировщик, который готовит 'СкСп' по шаблону компании на основе найденных материалов.\n"
        "Соблюдай правила:\n"
        "1) Используй ТОЛЬКО факты/цены/сроки, которые есть в контексте.\n"
        "2) НЕЛЬЗЯ выводить наружу персональные данные: имена, телефоны, ИНН, точные адреса (улица/дом/кв), номера договоров/контрактов.\n"
        "   - Адрес оставляй только как город/регион (если в контексте есть).\n"
        "3) Каждый числовой факт (цены/сроки) должен иметь ссылку на источник в поле 'citations' у строки.\n"
        "4) Если данных не хватает — добавь уточняющие вопросы в 'questions' и пометь позиции как 'TBD'.\n"
        "5) Не выдумывай модели оборудования и количества — если в контексте нет, ставь разумные placeholders и задавай вопросы.\n"
        "6) Выход: ТОЛЬКО валидный JSON. Без Markdown, без текста вокруг."
    )

    # Context formatting: compact, but with citations handle
    ctx_lines: List[str] = []
    for i, b in enumerate(context_blocks, 1):
        src = b.get("src", "unknown")
        chunk = b.get("chunk", b.get("chunk_id", ""))
        doc_type = b.get("doc_type") or b.get("meta", {}).get("doc_type") or ""
        prefix = f"[SRC={src} chunk={chunk} doc_type={doc_type}]"
        ctx_lines.append(f"### CONTEXT {i} {prefix}\n{(b.get('text') or '').strip()}\n")

    schema_hint = {
        "project": {
            "goal": "string",
            "location_city": "string|null",
            "location_region": "string|null",
            "assumptions": ["string"],
            "constraints": ["string"],
        },
        "questions": ["string"],
        "items": [
            {
                "manufacturer": "string",
                "article": "string",
                "description": "string",
                "qty": 1,
                "unit_price_rub": 0,
                "rrp_unit_price_rub": 0,
                "cost_unit_price_rub": 0,
                "delivery_term": "string",
                "supplier_delivery_term": "string",
                "supplier": "string",
                "registration_status": "string",
                "payment_terms": "string",
                "link": "string",
                "comment": "string",
                "citations": ["SRC=... chunk=..."],
            }
        ],
    }

    user = (
        "Шаблон/колонки СкСп (ориентир):\n"
        + json.dumps(sksps_columns, ensure_ascii=False)
        + "\n\n"
        "Контекст (источники):\n"
        + "\n".join(ctx_lines)
        + "\nЗапрос пользователя:\n"
        + (user_request or "")
        + "\n\n"
        "Сгенерируй JSON по схеме-подсказке (ключи можно расширять, но 'items' обязателен):\n"
        + json.dumps(schema_hint, ensure_ascii=False)
    )

    return [{"role": "system", "text": system}, {"role": "user", "text": user}]
