# File: src/rag_pipeline/security.py
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Iterable, Tuple


@dataclass(frozen=True)
class ExternalSafetyConfig:
    # включить редактирование вообще
    redact: bool = os.getenv("EXTERNAL_REDACT", os.getenv("RAG_REDACT", "0")).strip() in ("1", "true", "True")
    # fail-closed: если после редактирования всё равно видим “опасные” паттерны — не отдаём наружу
    fail_closed: bool = os.getenv("EXTERNAL_FAIL_CLOSED", "0").strip() in ("1", "true", "True")
    # редактировать также и выход YaGPT (на всякий)
    redact_output: bool = os.getenv("EXTERNAL_REDACT_OUTPUT", "0").strip() in ("1", "true", "True")

    # режим: "strict" (жёстче) / "lite"
    mode: str = os.getenv("EXTERNAL_REDACT_MODE", "strict").strip().lower()


# --- patterns ---
PHONE_RE = re.compile(r"(?<!\d)(?:\+7|8)\s*\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}(?!\d)")
INN_RE = re.compile(r"\b(?:инн|ИНН)\s*[:№]?\s*\d{10,12}\b")
CONTRACT_RE = re.compile(r"\b(?:договор|контракт|сч[её]т|счет|спецификац(?:ия|ии)|кп|к/п)\s*[:№]?\s*[A-Za-zА-Яа-я0-9\-\/]{3,}\b", re.IGNORECASE)

# адреса (упрощённо): индекс/улица/дом/корпус/кв
POST_INDEX_RE = re.compile(r"\b\d{6}\b")
STREET_RE = re.compile(r"\b(ул\.|улица|пр-кт|проспект|пер\.|переулок|шоссе|наб\.|набережная|пл\.|площадь)\b", re.IGNORECASE)
HOUSE_RE = re.compile(r"\b(д\.|дом|корп\.|корпус|стр\.|строение|кв\.|квартира)\s*\d+[A-Za-zА-Яа-я0-9\-\/]*\b", re.IGNORECASE)

# простая маска организаций (контрагенты): ООО "Ромашка", АО Ромашка, ИП Иванов и т.п.
ORG_RE = re.compile(r"\b(ооо|зао|оао|пао|ао|ип|гуп|муп)\s+([\"«][^\"»]{2,80}[\"»]|[A-Za-zА-Яа-я0-9\.\-]{2,80})", re.IGNORECASE)

# “похоже на ФИО”: Три слова с заглавных (очень грубо)
FIO_RE = re.compile(r"\b[А-ЯЁ][а-яё]+[\s\-]+[А-ЯЁ][а-яё]+[\s\-]+[А-ЯЁ][а-яё]+\b")

# allowlist брендов, чтобы не редактировать “Logitech”, “Yealink” и т.п.
_DEFAULT_BRANDS = ("yealink", "logitech", "poly", "cisco", "trueconf", "hikvision", "dahua", "avaya", "panasonic", "samsung", "lg")
BRANDS_ALLOW = tuple(x.strip().lower() for x in os.getenv("EXTERNAL_BRANDS_ALLOW", ",".join(_DEFAULT_BRANDS)).split(",") if x.strip())


def _mask_org(m: re.Match) -> str:
    full = m.group(0)
    low = full.lower()
    if any(b in low for b in BRANDS_ALLOW):
        return full
    return "<org>"


def redact_sensitive(text: str, mode: str = "strict") -> str:
    t = text or ""

    # телефоны/инн/договоры
    t = PHONE_RE.sub("<phone>", t)
    t = INN_RE.sub("<inn>", t)
    t = CONTRACT_RE.sub("<contract>", t)

    # контрагенты
    t = ORG_RE.sub(_mask_org, t)

    # ФИО
    t = FIO_RE.sub("<person>", t)

    # адрес: оставим город/регион, но уберём улицу/дом/индекс
    # 1) индекс -> <zip>
    t = POST_INDEX_RE.sub("<zip>", t)
    # 2) улица/дом
    if mode == "strict":
        t = STREET_RE.sub("<street>", t)
        t = HOUSE_RE.sub("<house>", t)

    return t


def safe_for_external(text: str, cfg: ExternalSafetyConfig) -> str:
    """
    1) Редактируем
    2) Если fail_closed=1 и после редактирования остались “опасные” паттерны — возвращаем пусто (или можно raise)
    """
    if not cfg.redact:
        return text or ""

    red = redact_sensitive(text or "", mode=cfg.mode)

    if cfg.fail_closed:
        # если после редактирования всё равно находится телефон/инн — значит что-то пошло не так
        leftovers = []
        if PHONE_RE.search(red):
            leftovers.append("phone")
        if INN_RE.search(red):
            leftovers.append("inn")
        if FIO_RE.search(red):
            leftovers.append("fio")
        if leftovers:
            # fail-closed: ничего наружу
            return ""

    return red
