# File: src/rag_pipeline/patch_intent.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol


class ChatCompletionClient(Protocol):
    def complete(self, messages: List[Dict[str, str]], *, temperature: float = 0.1, max_tokens: int = 900) -> str: ...


_WS_RE = re.compile(r"\s+")


def _norm(s: str) -> str:
    return _WS_RE.sub(" ", (s or "").replace("\u00a0", " ")).strip()


@dataclass(frozen=True)
class PatchAction:
    action: str  # add|replace|remove|update
    category: str
    query: str
    qty: Optional[float] = None
    constraints: Dict[str, Any] = field(default_factory=dict)
    must_have_terms: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class PatchIntent:
    raw: str
    actions: List[PatchAction]
    notes: str = ""


CATEGORIES: Dict[str, List[str]] = {
    "display": ["панел", "интерактив", "диспле", "экран", "видеостен", "videowall", "lcd", "led"],
    "processing": ["видеоконтроллер", "контроллер", "видеопроцессор", "процессор", "controller", "processor", "novastar", "vx", "lvp", "mctrl", "splicer"],
    "mounting": ["креплен", "кроншт", "рама", "стойк", "настенн", "мобильн", "vesa"],
    "signal_transport": ["передач", "удлин", "hdbaset", "extender", "hdmi over", "tx", "rx", "оптик", "sdi", "usb extender", "hdmi"],
    "switching": ["матриц", "коммутатор", "switch", "kvm", "hdmi switch", "splitter"],
    "sources": ["плеер", "мини-пк", "pc", "медиасервер", "источник"],
    "audio_output": ["акуст", "саундбар", "видеобар", "усил", "колонк", "speakerphone", "videobar"],
    "audio_processing": ["dsp", "микшер", "процессор звука"],
    "mics": ["микроф", "массив", "петлич", "ручн"],
    "conference": ["конгресс", "делегат", "председател", "пульт", "conference system", "central unit", "delegate", "dante"],
    "cameras": ["камер", "ptz", "пэтэз", "вкс"],
    "control": ["управлен", "crestron", "amx", "контроллер управлен"],
    "network": ["сеть", "poe", "wifi", "маршрутизатор"],
    "power": ["ибп", "pdu", "питан", "блок питан"],
    "cabling": ["кабел", "патч", "разъем", "hdmi кабель", "usb кабель"],
    "racks": ["стойка 19", "шкаф", "рэков"],
    "accessories": ["адаптер", "переходник", "расходник", "крепеж", "короб"],
    "software": ["по", "cms", "контент", "лиценз"],
    "services": ["монтаж", "пусконалад", "пнр", "проектирован"],
}

_INTENT_PROMPT = """Ты — ИИ-инженер. Извлеки НАМЕРЕНИЕ менеджера из PATCH_FROM_MANAGER.

Правила:
- Верни JSON строго по схеме. Без Markdown.
- action: add|replace|remove|update.
  - replace: "замени/земени/поменяй/подешевле/не такой/а не/давай X а не Y/не Y"
- vendor_pref: если в тексте есть "<vendor> контроллер/камера/видеобар/удлинитель".

СХЕМА:
{
  "actions": [
    {
      "action": "add|replace|remove|update",
      "category": "<CATEGORIES key>",
      "query": "<короткий запрос для поиска>",
      "qty": null,
      "constraints": {"vendor_pref": "", "budget": ""},
      "must_have_terms": []
    }
  ],
  "notes": "short"
}

CATEGORIES:
{categories}

PATCH_FROM_MANAGER:
{patch}
"""

_NUM = re.compile(r"(?<!\d)(\d+(?:[\.,]\d+)?)(?!\d)")
_QTY_WORDS = re.compile(r"(шт|штук|комплект|компл|пара|набор)", re.IGNORECASE)
_SPLIT = re.compile(r"\s*(?:;|,|\s+и\s+|\s+а\s+|\s+плюс\s+)\s*", re.IGNORECASE)
_VENDOR_HINT = re.compile(r"(?:бренд|поставщик|вендор)\s*[-:]*\s*([a-zA-Zа-яА-ЯёЁ0-9\-]+)", re.IGNORECASE)
_VENDOR_DEVICE = re.compile(
    r"(?:\bдавай\b|\bдвай\b|\bхочу\b|\bнужен\b|\bнужна\b|\bнужно\b|\bзамени\b|\bземени\b|\bпоменяй\b)\s+([a-zа-яё0-9\-]{2,})\s+(контроллер|controller|камера|ptz|видеобар|videobar|удлинитель|extender)\b",
    re.IGNORECASE,
)


def _guess_action(low: str) -> str:
    if any(x in low for x in ("убери", "удали", "удалить", "исключи")):
        return "remove"
    if "а не" in low or re.search(r"\bне\b\s+[a-zа-яё0-9\-]{3,}", low):
        return "replace"
    if any(x in low for x in ("замени", "земени", "заменить", "поменяй", "поменять", "подешевле", "дешевле", "не дорог")):
        return "replace"
    if any(x in low for x in ("обнови", "исправь", "поправь", "уточни")):
        return "update"
    if any(x in low for x in ("добавь", "добавить", "нужен", "нужна", "нужно")):
        return "add"
    return "add"


def _guess_category(low: str) -> str:
    """Heuristic category for edit text.

    Priority matters: phrases like 'крепление для видеостены' must resolve to 'mounting'
    (not 'display') to avoid re-adding panels when user edits mounting hardware.
    """
    priority = (
        "mounting",
        "signal_transport",
        "processing",
        "switching",
        "cabling",
        "accessories",
        "display",
    )
    for cat in priority:
        kws = CATEGORIES.get(cat, [])
        if any(k in low for k in kws):
            return cat
    # fallback: original order
    for cat, kws in CATEGORIES.items():
        if any(k in low for k in kws):
            return cat
    return "accessories"


def _guess_vendor_pref(text: str) -> str:
    t = _norm(text)
    m = _VENDOR_HINT.search(t)
    if m:
        return m.group(1).strip()
    m2 = _VENDOR_DEVICE.search(t)
    if m2:
        return m2.group(1).strip()
    return ""


def _guess_budget(low: str) -> str:
    if any(x in low for x in ("не дорог", "подешевле", "дешевле", "бюджет", "эконом")):
        return "low"
    if any(x in low for x in ("премиум", "топ", "лучше", "дороже")):
        return "high"
    return ""


def _fallback_intent(patch: str) -> PatchIntent:
    p = _norm(patch)
    low = p.casefold()

    vendor_pref = _guess_vendor_pref(p)
    budget = _guess_budget(low)

    parts = [x for x in _SPLIT.split(p) if x.strip()]
    actions: List[PatchAction] = []

    for part in parts:
        pl = part.casefold()
        action = _guess_action(pl)
        category = _guess_category(pl)

        qty = None
        m = _NUM.search(pl)
        if m and _QTY_WORDS.search(pl):
            try:
                qty = float(m.group(1).replace(",", "."))
            except Exception:
                qty = None

        constraints: Dict[str, Any] = {}
        if vendor_pref:
            constraints["vendor_pref"] = vendor_pref
        if budget:
            constraints["budget"] = budget

        mh = [vendor_pref] if vendor_pref else []
        actions.append(
            PatchAction(
                action=action,
                category=category,
                query=part[:220],
                qty=qty,
                constraints=constraints,
                must_have_terms=mh,
            )
        )

    if not actions:
        actions = [PatchAction(action=_guess_action(low), category=_guess_category(low), query=p[:220], qty=None, constraints={}, must_have_terms=[])]
    return PatchIntent(raw=patch, actions=actions, notes="fallback")


def extract_patch_intent(patch: str, llm: Optional[ChatCompletionClient]) -> PatchIntent:
    patch = _norm(patch)
    if not patch:
        return PatchIntent(raw="", actions=[], notes="empty")

    if llm is None:
        return _fallback_intent(patch)

    prompt = _INTENT_PROMPT.format(categories=json.dumps(CATEGORIES, ensure_ascii=False), patch=patch)

    try:
        raw = llm.complete(
            [{"role": "system", "text": "Выводи только валидный JSON."}, {"role": "user", "text": prompt}],
            temperature=0.1,
            max_tokens=900,
        )
        try:
            obj = json.loads(raw)
        except Exception:
            m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
            if not m:
                return _fallback_intent(patch)
            obj = json.loads(m.group(0))

        actions: List[PatchAction] = []
        for a in obj.get("actions") or []:
            if not isinstance(a, dict):
                continue
            actions.append(
                PatchAction(
                    action=str(a.get("action") or "add"),
                    category=str(a.get("category") or "accessories"),
                    query=str(a.get("query") or patch)[:300],
                    qty=a.get("qty", None),
                    constraints=dict(a.get("constraints") or {}),
                    must_have_terms=[str(x) for x in (a.get("must_have_terms") or []) if str(x).strip()],
                )
            )
        if not actions:
            return _fallback_intent(patch)
        return PatchIntent(raw=patch, actions=actions, notes=str(obj.get("notes") or "").strip())
    except Exception:
        return _fallback_intent(patch)