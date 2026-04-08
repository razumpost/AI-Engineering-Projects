from __future__ import annotations

import json
import os
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv  # type: ignore
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ..domain.candidates import CandidateItem, CandidatePool


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _load_env() -> None:
    load_dotenv(dotenv_path=str(_repo_root() / ".env"), override=False)


def _safe_json(v: Any) -> dict[str, Any]:
    if isinstance(v, dict):
        return v
    if isinstance(v, str) and v.strip():
        try:
            obj = json.loads(v)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _to_decimal(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    try:
        if isinstance(v, Decimal):
            return v
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        s = str(v).strip().replace("\u00a0", " ")
        s = s.replace("RUB", "").replace("руб", "").replace("₽", "")
        s = s.replace(" ", "").replace(",", ".")
        m = re.search(r"(-?\d+(?:\.\d+)?)", s)
        if not m:
            return None
        return Decimal(m.group(1))
    except (InvalidOperation, Exception):
        return None


def _norm_text(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip().replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s


def _norm_fold(v: Any) -> str:
    return _norm_text(v).casefold()


def _normalize_code_like(v: Any) -> str | None:
    s = _norm_text(v).upper()
    if not s:
        return None
    s = s.replace(" ", "")
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"[^A-Z0-9._/\-+]", "", s)
    if len(s) < 3:
        return None
    return s


def _extract_terms(text: str, limit: int = 8) -> list[str]:
    terms = [t for t in re.split(r"[^\wА-Яа-я]+", (text or "").casefold()) if len(t) >= 4]
    out: list[str] = []
    seen = set()
    for t in terms:
        if t not in seen:
            seen.add(t)
            out.append(t)
        if len(out) >= limit:
            break
    return out


def _extract_code_terms(text: str, limit: int = 8) -> list[str]:
    toks = re.findall(r"[A-Za-z0-9][A-Za-z0-9._/\-+]{2,}", text or "")
    out: list[str] = []
    seen = set()
    for t in toks:
        nt = _normalize_code_like(t)
        if not nt:
            continue
        if nt not in seen:
            seen.add(nt)
            out.append(nt)
        if len(out) >= limit:
            break
    return out


def _guess_family(name: str, description: str | None = None, vendor: str | None = None, model: str | None = None) -> str:
    blob = " ".join(
        [
            _norm_fold(name),
            _norm_fold(description),
            _norm_fold(vendor),
            _norm_fold(model),
        ]
    )

    # ВАЖНО:
    # mount/cable/software должны идти РАНЬШЕ display,
    # иначе "videoconferencing trolley for two displays" будет определяться как display.
    ordered_rules: list[tuple[str, list[str]]] = [
        (
            "mount",
            [
                "trolley", "тележка", "mobile stand", "wall mount", "ceiling mount",
                "pull-out wall mount", "bracket", "mount", "стойка", "кронштейн",
                "напольная стойка", "потолочный кронштейн",
            ],
        ),
        (
            "cable",
            [
                "cable", "кабель", "hdmi", "usb", "displayport", "vga", "xlr",
                "cat6", "cat.6", "hdbaset",
            ],
        ),
        (
            "software",
            [
                "software", "лиценз", "license", "smart player", "player license",
                "spinetix", "elementi", "cms", "content management", "signage software",
                "digital signage", "медиаплеер по",
            ],
        ),
        (
            "ops",
            [
                "ops", "slot pc", "ops pc", "ops-пк",
            ],
        ),
        (
            "camera",
            [
                "conference camera", "usb camera", "ptz", "webcam", "videobar",
                "camera", "камера", "hd camera",
            ],
        ),
        (
            "microphone",
            [
                "conference mic", "table microphone", "ceiling microphone",
                "microphone", "микрофон", "delegate unit", "chairman unit",
                "пульт делегата", "пульт председателя", "beamforming",
            ],
        ),
        (
            "audio",
            [
                "speakerphone", "soundbar", "speaker", "акуст", "audio",
                "громкоговор", "amplifier", "усилитель",
            ],
        ),
        (
            "controller",
            [
                "discussion system", "conference system core", "central unit",
                "центральный блок", "audio processor", "dsp", "matrix", "switcher",
                "controller", "процессор", "atem", "видеомикшер",
            ],
        ),
        (
            "display",
            [
                "smart display", "interactive display", "interactive panel",
                "professional display", "videowall", "display", "дисплей",
                "панель", "экран", "monitor", "lcd", "led", "eliteboard",
                "nextpanel", "edflat",
            ],
        ),
    ]

    for family, needles in ordered_rules:
        if any(n in blob for n in needles):
            return family

    return "other"


def _is_non_product_noise(name: str, description: str | None = None) -> bool:
    blob = " ".join([_norm_fold(name), _norm_fold(description)])
    bad = [
        "психографический",
        "портрет ца",
        "привычки",
        "анализ",
        "маркетинг",
        "рейтинг клиентов",
        "критерии оценки",
        "веб ресурсы",
        "типовой портрет",
    ]
    return any(x in blob for x in bad)


def _query_context_flags(text_query: str) -> dict[str, bool]:
    q = _norm_fold(text_query)
    return {
        "meeting_room": any(x in q for x in ["переговор", "conference", "meeting room"]),
        "camera": any(x in q for x in ["камера", "camera", "ptz"]),
        "display": any(x in q for x in ["дисплей", "display", "панель", "экран", "screen"]),
        "microphone": any(x in q for x in ["микрофон", "microphone", "mic"]),
        "audio": any(x in q for x in ["акуст", "speaker", "audio", "soundbar", "speakerphone"]),
        "software": any(x in q for x in ["software", "лиценз", "license", "cms", "по "]),
        "player": any(x in q for x in ["player", "smart player", "spinetix", "signage", "elementi"]),
    }


def _family_bonus(family: str, flags: dict[str, bool], evidence: dict[str, Any]) -> float:
    bonus = 0.0

    software_first = flags["software"] or flags["player"]

    if flags["meeting_room"]:
        if family in {"camera", "display", "microphone", "audio", "controller"}:
            bonus += 4.0
        elif family == "software":
            bonus += 0.5
        elif family in {"mount", "cable"}:
            bonus -= 1.0
        elif family == "other":
            bonus -= 3.0

    if flags["camera"] and family == "camera":
        bonus += 4.0
    if flags["display"] and family == "display":
        bonus += 4.0
    if flags["microphone"] and family == "microphone":
        bonus += 4.0
    if flags["audio"] and family in {"audio", "controller"}:
        bonus += 2.5

    if family == "software":
        desc_blob = " ".join(
            [
                _norm_fold(evidence.get("name")),
                _norm_fold(evidence.get("description")),
                _norm_fold(evidence.get("model")),
            ]
        )

        if software_first:
            bonus += 8.0

        if any(x in desc_blob for x in ["player", "spinetix", "elementi", "cms", "signage"]):
            bonus += 4.0

        if flags["meeting_room"] and not software_first:
            bonus -= 2.0

    # Если запрос software-first, но кандидат НЕ software,
    # то дисплеи и прочее железо должны уходить вниз.
    if software_first and family != "software":
        if family == "display":
            bonus -= 4.0
        elif family in {"mount", "cable"}:
            bonus -= 5.0
        else:
            bonus -= 2.0

    return bonus


class PriceLayerStore:
    def __init__(self, dsn: Optional[str] = None) -> None:
        _load_env()
        dsn = (dsn or os.getenv("DATABASE_URL") or os.getenv("DB_DSN") or "").strip()
        if not dsn:
            raise RuntimeError("DATABASE_URL/DB_DSN is empty")
        self.engine: Engine = create_engine(dsn, pool_pre_ping=True)

    def _row_to_candidate(self, row: dict[str, Any], *, score: float = 0.0) -> CandidateItem:
        evidence = _safe_json(row.get("evidence_json"))
        vendor = evidence.get("vendor") or row.get("vendor_norm")
        sku = evidence.get("sku") or row.get("sku_norm")
        model = evidence.get("model") or row.get("model_norm")
        name = evidence.get("name") or row.get("name_norm") or row.get("identity_key") or "UNKNOWN"
        desc = evidence.get("description") or name

        source_kind = str(row.get("source_kind") or "")
        family = _guess_family(
            name=str(name),
            description=str(desc) if desc else None,
            vendor=str(vendor) if vendor else None,
            model=str(model) if model else None,
        )

        category = "supplier_price" if source_kind == "supplier_price" else "sksp_price"

        return CandidateItem(
            candidate_id=f"price:{row['identity_key']}",
            category=category,
            sku=str(sku).strip() if sku else None,
            manufacturer=str(vendor).strip() if vendor else None,
            model=str(model).strip() if model else None,
            name=str(name),
            description=str(desc),
            unit_price_rub=_to_decimal(row.get("unit_price")),
            price_source=f"latest_price_candidates:{source_kind}",
            evidence_task_ids=[int(row["task_id"])] if row.get("task_id") else [],
            meta={
                "identity_key": row.get("identity_key"),
                "source_kind": source_kind,
                "workbook_name": row.get("workbook_name"),
                "sheet_name": row.get("sheet_name"),
                "row_no": row.get("row_no"),
                "score": score,
                "family": family,
                "evidence_json": evidence,
            },
        )

    def _best_price_match_for_candidate(self, item: CandidateItem) -> CandidateItem:
        sku_norm = _normalize_code_like(item.sku)
        model_norm = _normalize_code_like(item.model)
        vendor_norm = _norm_fold(item.manufacturer) if item.manufacturer else None
        name_norm = _norm_fold(item.name)

        sql = text(
            """
            select
              l.identity_key,
              l.chosen_sksp_item_id,
              l.source_kind,
              l.vendor_norm,
              l.sku_norm,
              l.model_norm,
              l.name_norm,
              l.unit_price,
              l.currency,
              l.file_doc_id,
              l.file_id,
              l.task_id,
              l.workbook_name,
              l.sheet_name,
              l.row_no,
              l.evidence_json
            from latest_price_candidates l
            where
              l.source_kind in ('supplier_price', 'sksp')
              and (
                (:sku_norm is not null and l.sku_norm = :sku_norm)
                or (:vendor_norm is not null and :model_norm is not null and l.vendor_norm = :vendor_norm and l.model_norm = :model_norm)
                or (:model_norm is not null and l.model_norm = :model_norm)
                or (:name_norm is not null and l.name_norm ilike ('%' || :name_norm || '%'))
              )
            order by
              case when :sku_norm is not null and l.sku_norm = :sku_norm then 1 else 9 end,
              case when :vendor_norm is not null and :model_norm is not null and l.vendor_norm = :vendor_norm and l.model_norm = :model_norm then 1 else 9 end,
              case when :model_norm is not null and l.model_norm = :model_norm then 1 else 9 end,
              case when :name_norm is not null and l.name_norm ilike ('%' || :name_norm || '%') then 1 else 9 end,
              case when l.source_kind = 'supplier_price' and l.unit_price is not null then 1
                   when l.source_kind = 'sksp' and l.unit_price is not null then 2
                   when l.source_kind = 'supplier_price' then 3
                   when l.source_kind = 'sksp' then 4
                   else 9 end,
              l.file_doc_id desc
            limit 1
            """
        )

        with self.engine.begin() as conn:
            row = conn.execute(
                sql,
                {
                    "sku_norm": sku_norm,
                    "vendor_norm": vendor_norm,
                    "model_norm": model_norm,
                    "name_norm": name_norm if len(name_norm) >= 4 else None,
                },
            ).mappings().first()

        if not row:
            return item

        price = _to_decimal(row.get("unit_price"))
        evidence = _safe_json(row.get("evidence_json"))
        merged_task_ids = list(item.evidence_task_ids or [])
        if row.get("task_id"):
            try:
                tid = int(row["task_id"])
                if tid not in merged_task_ids:
                    merged_task_ids.append(tid)
            except Exception:
                pass

        meta = dict(item.meta or {})
        meta["price_match"] = {
            "identity_key": row.get("identity_key"),
            "source_kind": row.get("source_kind"),
            "workbook_name": row.get("workbook_name"),
            "sheet_name": row.get("sheet_name"),
            "row_no": row.get("row_no"),
            "evidence_json": evidence,
        }

        update = {
            "unit_price_rub": price if price is not None else item.unit_price_rub,
            "price_source": f"latest_price_candidates:{row['source_kind']}" if row.get("source_kind") else item.price_source,
            "evidence_task_ids": merged_task_ids,
            "meta": meta,
        }

        if not item.sku and (evidence.get("sku") or row.get("sku_norm")):
            update["sku"] = str(evidence.get("sku") or row.get("sku_norm"))
        if not item.model and (evidence.get("model") or row.get("model_norm")):
            update["model"] = str(evidence.get("model") or row.get("model_norm"))
        if not item.manufacturer and (evidence.get("vendor") or row.get("vendor_norm")):
            update["manufacturer"] = str(evidence.get("vendor") or row.get("vendor_norm"))

        return item.model_copy(update=update)

    def enrich_pool_prices(self, pool: CandidatePool) -> CandidatePool:
        out_items: list[CandidateItem] = []
        for item in pool.items:
            if item.unit_price_rub is not None and (item.price_source or "").strip():
                out_items.append(item)
            else:
                out_items.append(self._best_price_match_for_candidate(item))
        return CandidatePool(items=out_items, tasks=pool.tasks)

    def search_price_candidates(self, text_query: str, limit: int = 20) -> CandidatePool:
        text_query = (text_query or "").strip()
        if not text_query:
            return CandidatePool(items=[], tasks=[])

        terms = _extract_terms(text_query, limit=8)
        code_terms = _extract_code_terms(text_query, limit=8)
        flags = _query_context_flags(text_query)

        if not terms and not code_terms:
            return CandidatePool(items=[], tasks=[])

        conds = []
        params: dict[str, Any] = {"limit": int(max(limit * 12, 80))}
        i = 0

        for term in terms:
            key = f"t{i}"
            params[key] = f"%{term}%"
            conds.append(
                f"""(
                    coalesce(l.name_norm,'') ilike :{key}
                    or coalesce(l.vendor_norm,'') ilike :{key}
                    or cast(l.evidence_json as text) ilike :{key}
                )"""
            )
            i += 1

        for code in code_terms:
            key = f"c{i}"
            params[key] = code
            conds.append(
                f"""(
                    l.sku_norm = :{key}
                    or l.model_norm = :{key}
                    or cast(l.evidence_json as text) ilike ('%' || :{key} || '%')
                )"""
            )
            i += 1

        where_sql = " or ".join(conds) if conds else "false"

        sql = text(
            f"""
            select
              l.identity_key,
              l.chosen_sksp_item_id,
              l.source_kind,
              l.vendor_norm,
              l.sku_norm,
              l.model_norm,
              l.name_norm,
              l.unit_price,
              l.currency,
              l.file_doc_id,
              l.file_id,
              l.task_id,
              l.workbook_name,
              l.sheet_name,
              l.row_no,
              l.evidence_json
            from latest_price_candidates l
            where {where_sql}
            order by
              case when l.source_kind = 'supplier_price' and l.unit_price is not null then 1
                   when l.source_kind = 'sksp' and l.unit_price is not null then 2
                   when l.source_kind = 'supplier_price' then 3
                   when l.source_kind = 'sksp' then 4
                   else 9 end,
              l.file_doc_id desc
            limit :limit
            """
        )

        with self.engine.begin() as conn:
            rows = conn.execute(sql, params).mappings().all()

        ranked: list[tuple[float, dict[str, Any]]] = []
        terms_fold = [_norm_fold(t) for t in terms]
        codes_fold = [_normalize_code_like(t) for t in code_terms if _normalize_code_like(t)]

        for r in rows:
            evidence = _safe_json(r.get("evidence_json"))
            source_kind = str(r.get("source_kind") or "")
            name = str(evidence.get("name") or r.get("name_norm") or "")
            desc = str(evidence.get("description") or "")
            vendor = str(evidence.get("vendor") or r.get("vendor_norm") or "")
            model = str(evidence.get("model") or r.get("model_norm") or "")

            family = _guess_family(name=name, description=desc, vendor=vendor, model=model)

            if _is_non_product_noise(name, desc):
                continue

            score = 0.0
            blob = " ".join(
                [
                    _norm_fold(r.get("vendor_norm")),
                    _norm_fold(r.get("sku_norm")),
                    _norm_fold(r.get("model_norm")),
                    _norm_fold(r.get("name_norm")),
                    _norm_fold(evidence.get("name")),
                    _norm_fold(evidence.get("description")),
                ]
            )

            for t in terms_fold:
                if t and t in blob:
                    score += 1.0

            for c in codes_fold:
                if c and (c == r.get("sku_norm") or c == r.get("model_norm") or c in blob.upper()):
                    score += 4.0

            if source_kind == "supplier_price":
                score += 1.0
            elif source_kind == "sksp":
                score += 0.5
            elif source_kind == "other_excel":
                score -= 6.0
            else:
                score -= 4.0

            if r.get("unit_price") is not None:
                score += 0.5

            score += _family_bonus(family, flags, evidence)

            if family == "mount":
                score -= 6.0

            if flags["meeting_room"] and family in {"mount", "cable"}:
                score -= 3.0

            ranked.append((score, dict(r)))

        ranked.sort(key=lambda x: x[0], reverse=True)

        items: list[CandidateItem] = []
        seen = set()
        for score, row in ranked:
            cid = f"price:{row['identity_key']}"
            if cid in seen:
                continue
            seen.add(cid)
            items.append(self._row_to_candidate(row, score=score))
            if len(items) >= limit:
                break

        return CandidatePool(items=items, tasks=[])