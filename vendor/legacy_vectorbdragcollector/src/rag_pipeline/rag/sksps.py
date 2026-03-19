# File: src/rag_pipeline/rag/sksps.py
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import openpyxl

from ..embeddings import Embedder
from ..retrieval import RetrievalConfig, search as retrieval_search
from ..security import ExternalSafetyConfig, safe_for_external
from .yandex_gpt_client import YandexGPTClient


NEGATIVE_DOMAINS = [
    "пожарная сигнализация",
    "пожарка",
    "апс",
    "соуэ",
    "скуд",
    "контроль доступа",
    "турникет",
    "шлагбаум",
]


COMMERCIAL_TITLE_KEYWORDS = [
    "прайс",
    "price",
    "коммерчес",
    "кп",
    "смет",
    "спецификац",
    "счет",
    "счёт",
    "proposal",
    "quotation",
]

COMMERCIAL_MARKERS = [
    "₽",
    "руб",
    "rur",
    "rub",
    "usd",
    "eur",
    "$",
    "€",
    "у.е",
    "р.",
    "цена",
    "стоимость",
    "итого",
    "сумма",
    "всего",
    "qty",
    "кол-во",
    "количество",
    "арт",
    "артик",
    "sku",
    "код",
    "model",
    "модель",
    "pn",
    "part",
]

PRICE_NUMBER_RE = re.compile(
    r"(?<!\d)(?:\d{1,3}(?:[\s\u00A0\u202F]\d{3})+|\d{4,9})(?:[.,]\d{1,2})?(?!\d)"
)
SKU_LIKE_RE = re.compile(r"\b[A-Z0-9]{2,}[A-Z0-9\-_\/]{2,}\b", flags=re.IGNORECASE)

FX_CURRENCY_RE = re.compile(r"(?i)(\bUSD\b|\bEUR\b|\$|€|\bUS\b)")
RUB_CURRENCY_RE = re.compile(r"(?i)(₽|руб\.?|рублей|\bRUB\b|\bRUR\b|р\.)")


@dataclass
class SkspsItem:
    manufacturer: str
    sku: str
    description: str
    unit_price_rub: float
    qty: int
    delivery_time: str = ""
    comment: str = ""
    url: str = ""


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _fill_xlsx(template_path: Path, out_path: Path, items: List[SkspsItem]) -> None:
    wb = openpyxl.load_workbook(template_path)
    ws = wb["СкСп"] if "СкСп" in wb.sheetnames else wb.active

    header_row = 1
    start_row = header_row + 1

    for i, it in enumerate(items, start=1):
        r = start_row + (i - 1)
        ws.cell(r, 1).value = i
        ws.cell(r, 2).value = it.manufacturer
        ws.cell(r, 3).value = it.sku
        ws.cell(r, 4).value = it.description
        ws.cell(r, 5).value = float(it.unit_price_rub)
        ws.cell(r, 6).value = int(it.qty)
        ws.cell(r, 7).value = float(it.unit_price_rub) * int(it.qty)
        ws.cell(r, 8).value = it.delivery_time
        ws.cell(r, 9).value = it.comment
        ws.cell(r, 10).value = it.url

    wb.save(out_path)


def _norm_text(s: str) -> str:
    return (
        (s or "")
        .replace("\u00A0", " ")
        .replace("\u202F", " ")
        .replace("\t", " ")
        .strip()
    )


def _is_negative_domain(text: str) -> bool:
    t = (text or "").lower()
    return any(bad in t for bad in NEGATIVE_DOMAINS)


def _looks_commercial_line(s: str) -> bool:
    s0 = _norm_text(s)
    if not s0:
        return False
    s_low = s0.lower()

    if _is_negative_domain(s_low):
        return False

    if any(m in s_low for m in COMMERCIAL_MARKERS):
        if PRICE_NUMBER_RE.search(s0) or re.search(r"\d", s0):
            return True

    if (";" in s0 or "|" in s0 or "\t" in (s or "")) and re.search(r"\d", s0):
        return True

    if PRICE_NUMBER_RE.search(s0) and (
        SKU_LIKE_RE.search(s0) or "кам" in s_low or "микроф" in s_low
    ):
        return True

    return False


def _commercial_score_hit(hit: Dict[str, Any]) -> float:
    title = (hit.get("title") or "").lower()
    content = _norm_text(hit.get("content") or "")
    c_low = content.lower()

    if _is_negative_domain(title) or _is_negative_domain(c_low):
        return -1.0

    score = 0.0

    if any(k in title for k in COMMERCIAL_TITLE_KEYWORDS):
        score += 4.0
    if any(k in c_low for k in ("прайс", "коммерчес", "кп", "смет", "итого", "сумма", "всего")):
        score += 2.5

    marker_hits = sum(1 for k in COMMERCIAL_MARKERS if k in c_low)
    score += min(3.0, marker_hits * 0.35)

    prices = PRICE_NUMBER_RE.findall(content)
    if prices:
        score += min(4.0, 1.5 + 0.4 * len(prices))

    if "\t" in (hit.get("content") or ""):
        score += 1.0
    if len(re.findall(r"\s{2,}", content)) > 6:
        score += 0.7

    dist = hit.get("dist")
    if isinstance(dist, (int, float)):
        score += max(0.0, 1.0 - float(dist))

    return score


def _dedup(hs: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for h in hs:
        cid = h.get("chunk_id")
        if cid in seen:
            continue
        seen.add(cid)
        out.append(h)
    return out


def _extract_commercial_snippets(hits: List[Dict[str, Any]], max_lines: int = 220) -> str:
    lines: List[str] = []
    for h in hits:
        title = (h.get("title") or "").strip()
        chunk_id = h.get("chunk_id")
        txt = (h.get("content") or "").strip()
        if not txt:
            continue

        for ln_i, ln in enumerate(txt.splitlines(), start=1):
            if _looks_commercial_line(ln):
                ln2 = _norm_text(ln)
                if ln2:
                    prefix = f"[CL|chunk={chunk_id}|{title}|L{ln_i}] "
                    lines.append(prefix + ln2)
                    if len(lines) >= max_lines:
                        return "\n".join(lines)

    return "\n".join(lines)


def _build_context(
    hits: List[Dict[str, Any]],
    max_chars: int = 14000,
    max_chunks: Optional[int] = None,
) -> str:
    parts: List[str] = []
    used = 0
    for i, h in enumerate(hits, start=1):
        if max_chunks is not None and i > max_chunks:
            break
        txt = (h.get("content") or "").strip()
        if not txt:
            continue
        title = (h.get("title") or "").strip()
        dist = h.get("dist")
        dist_s = f"{dist:.3f}" if isinstance(dist, (int, float)) else "NA"
        block = (
            f"[{i}] {h.get('source')}:{h.get('source_id')} chunk={h.get('chunk_id')} dist={dist_s} title={title}\n"
            f"{txt}\n"
        )
        if used + len(block) > max_chars:
            break
        parts.append(block)
        used += len(block)
    return "\n".join(parts).strip()


def _simple_planner(user_request: str) -> Dict[str, Any]:
    q = user_request.lower()
    tags: List[str] = []
    if "вкс" in q or "переговор" in q or "videoconf" in q:
        tags.append("VKS")
    if "панел" in q or "диспле" in q or "экран" in q:
        tags.append("Display/Panel")
    if "камера" in q or "ptz" in q:
        tags.append("Camera")
    if "микроф" in q or "аудио" in q or "акуст" in q or "спикерфон" in q:
        tags.append("Audio")
    if "скс" in q or "сеть" in q or "ethernet" in q:
        tags.append("SKS/Network")
    if "монтаж" in q or "пнр" in q or "пуск" in q:
        tags.append("Installation")

    only_sources = ["bitrix_file", "bitrix_task"]

    tech_queries: List[str] = []
    if "VKS" in tags:
        tech_queries += [
            "ТЗ ВКС переговорная 8 10 человек PTZ камера микрофоны акустика BYOD",
            "состав оборудования ВКС переговорная 10 человек камера микрофонный массив спикерфон",
            "спецификация ВКС переговорная камера микрофоны акустика панель управление монтаж ПНР",
        ]
    else:
        tech_queries += [user_request]

    price_queries: List[str] = []
    if "VKS" in tags:
        price_queries += [
            "прайс ВКС камера PTZ микрофонный массив спикерфон кодек цена USD RUB",
            "КП ВКС переговорная комплект цена USD RUB",
            "цена переговорная ВКС 10 человек комплект USD RUB",
            "прайс конференц система микрофоны переговорная цена USD RUB",
            "прайс панель дисплей цена USD RUB",
            "прайс монтаж ПНР ВКС стоимость",
        ]

    example_queries: List[str] = []
    if "VKS" in tags:
        example_queries += [
            "СкСп ВКС переговорная 10 человек камера микрофоны спикерфон",
            "смета СкСп ВКС переговорная монтаж ПНР",
        ]

    queries = (tech_queries + example_queries + price_queries + [user_request])[:12]
    return {"tags": tags, "queries": queries, "only_sources": only_sources}


def _extract_json_from_text(s: str) -> str:
    s = s.strip()
    s = re.sub(r"^```(?:json)?\s*", "", s)
    s = re.sub(r"\s*```$", "", s)
    m = re.search(r"\{.*\}", s, flags=re.DOTALL)
    return m.group(0).strip() if m else s


def _normalize_items(raw_items: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(raw_items, list):
        return out

    for it in raw_items:
        if not isinstance(it, dict):
            continue

        manufacturer = str(it.get("manufacturer") or "").strip()
        sku = str(it.get("sku") or "").strip()
        description = str(it.get("description") or "").strip()
        delivery_time = str(it.get("delivery_time") or "").strip()
        comment = str(it.get("comment") or "").strip()
        url = str(it.get("url") or "").strip()

        for key, val in (("manufacturer", manufacturer), ("sku", sku), ("description", description)):
            if val.lower() in {"неизвестно", "unknown", "н/д", "нет данных"}:
                if key == "description":
                    description = ""
                elif key == "manufacturer":
                    manufacturer = "(TBD)"
                else:
                    sku = "(TBD)"

        manufacturer = manufacturer or "(TBD)"
        sku = sku or "(TBD)"
        description = description or "(TBD)"

        qty_raw = it.get("qty", 1)
        qty = 1
        if isinstance(qty_raw, (int, float)) and int(qty_raw) > 0:
            qty = int(qty_raw)
        elif qty_raw is not None:
            comment = (comment + f" | qty_note={qty_raw}").strip(" |")

        price_raw = it.get("unit_price_rub", 0.0)
        price = 0.0
        if isinstance(price_raw, (int, float)):
            price = float(price_raw)
        else:
            raw_s = _norm_text(str(price_raw))
            is_fx = bool(FX_CURRENCY_RE.search(raw_s))

            s_clean = RUB_CURRENCY_RE.sub("", raw_s)
            s_clean = s_clean.replace(" ", "")

            m = re.search(r"\d+(?:[.,]\d+)?", s_clean)
            if m and not is_fx:
                price = float(m.group(0).replace(",", "."))
            else:
                price = 0.0
                if is_fx:
                    comment = (comment + f" | fx_price_seen={raw_s}").strip(" |")

        out.append(
            {
                "manufacturer": manufacturer,
                "sku": sku,
                "description": description,
                "unit_price_rub": price,
                "qty": qty,
                "delivery_time": delivery_time,
                "comment": comment,
                "url": url,
            }
        )
    return out


def _yagpt_generate_items(user_request: str, context: str) -> Dict[str, Any]:
    client = YandexGPTClient()

    system = {
        "role": "system",
        "text": """Ты помощник инженера и сметчика. Задача: сформировать позиции СкСп (оборудование и работы) СТРОГО по КОНТЕКСТУ.

КРИТИЧНО:
1) НЕЛЬЗЯ писать 'Неизвестно' / 'unknown' / 'нет данных'. Если нет точных данных — оставь пустую строку или '(TBD)', а unit_price_rub=0.
2) Если в контексте есть SKU/код/артикул/цена — ТЫ ОБЯЗАН(А) использовать их. НЕЛЬЗЯ игнорировать строки вида [CL|...].
3) Если в item есть '(TBD)' или unit_price_rub=0 — ОБЯЗАТЕЛЬНО добавь вопрос в questions (что именно нужно уточнить).
4) Мы НЕ занимаемся пожарной сигнализацией и СКУД — исключи эти темы из items и questions.
5) Если цена в контексте указана НЕ в RUB (например USD/EUR/$/€):
   - НЕ конвертируй сам(а), если в контексте нет явного курса/правила пересчета.
   - unit_price_rub поставь 0.
   - исходную цену и валюту запиши в comment (например: 'fx: 1580 USD from [CL|...]').
   - добавь вопрос: 'Нужен курс/правило пересчёта USD/EUR в RUB и дата курса'.

Верни СТРОГО JSON (без Markdown/комментариев), формат:
{
  "items": [
    {
      "manufacturer": str,
      "sku": str,
      "description": str,
      "unit_price_rub": number,
      "qty": integer,
      "delivery_time": str,
      "comment": str,
      "url": str
    }
  ],
  "questions": [str, ...],
  "rationale": str
}
""",
    }
    user = {
        "role": "user",
        "text": f"""Запрос:
{user_request}

Контекст (сначала коммерция, потом ТЗ/примеры):
{context}

Правила:
- Не добавляй позиции по пожарке/СКУД.
- Если qty неизвестно — ставь 1, пояснение в comment, и вопрос в questions.
- Если цена неизвестна — unit_price_rub=0, и вопрос в questions.
- Если берешь цену/SKU из контекста, добавь ссылку в comment (например: 'from [CL|chunk=...|...]').
""",
    }

    text = client.complete([system, user], temperature=0.05, max_tokens=1600)
    extracted = _extract_json_from_text(text)

    try:
        obj = json.loads(extracted)
    except Exception as e:
        raise RuntimeError(
            f"YaGPT returned non-JSON. First 1200 chars:\n{extracted[:1200]}"
        ) from e

    items = obj.get("items", [])
    filtered_items = []
    for it in items if isinstance(items, list) else []:
        if not isinstance(it, dict):
            continue
        desc = str(it.get("description") or "")
        if _is_negative_domain(desc):
            continue
        filtered_items.append(it)
    obj["items"] = filtered_items

    if not isinstance(obj.get("questions"), list):
        obj["questions"] = []

    return obj


def _retrieve_many(
    queries: Sequence[str],
    *,
    cfg: RetrievalConfig,
    embedder: Embedder,
    top_k: int,
    doc_types: Optional[Sequence[str]],
) -> List[Dict[str, Any]]:
    hits: List[Dict[str, Any]] = []
    for q in queries:
        hits.extend(
            retrieval_search(q, cfg=cfg, embedder=embedder, top_k=top_k, doc_types=doc_types)
        )
    return _dedup(hits)


def _pick_best_commercial(hits: List[Dict[str, Any]], limit: int = 32) -> List[Dict[str, Any]]:
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for h in hits:
        s = _commercial_score_hit(h)
        if s > 0:
            scored.append((s, h))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [h for _, h in scored[:limit]]


def _debug_dump_commercial(out_dir: Path, candidates: List[Dict[str, Any]], picked: List[Dict[str, Any]]) -> None:
    scored = []
    for h in candidates:
        scored.append(
            {
                "score": _commercial_score_hit(h),
                "dist": h.get("dist"),
                "title": h.get("title"),
                "source": h.get("source"),
                "source_id": h.get("source_id"),
                "chunk_id": h.get("chunk_id"),
                "content_head": _norm_text((h.get("content") or "")[:350]),
            }
        )
    scored.sort(key=lambda x: x["score"], reverse=True)

    payload = {
        "candidates_scored_top100": scored[:100],
        "picked_chunk_ids": [h.get("chunk_id") for h in picked],
    }
    _write_json(out_dir / "commercial.debug.json", payload)

    print("\n=== DEBUG COMMERCIAL TOP-20 ===")
    for i, row in enumerate(scored[:20], start=1):
        title = row.get("title") or ""
        head = row.get("content_head") or ""
        dist = row.get("dist")
        dist_s = f"{dist:.3f}" if isinstance(dist, (int, float)) else "NA"
        print(f"[{i:02d}] score={row['score']:.2f} dist={dist_s} chunk={row['chunk_id']} title={title}")
        print(f"     {head[:220]}")
    print("Wrote:", out_dir / "commercial.debug.json")


def generate_sksps(
    user_request: str,
    *,
    template_path: Path,
    out_dir: Path,
    dry_run: bool,
    verbose: bool = False,
    debug_commercial: bool = False,
) -> Dict[str, Any]:
    _ensure_dir(out_dir)

    planner = _simple_planner(user_request)
    if verbose:
        print(f"planner.tags={planner['tags']}")
        print(f"planner.queries={planner['queries']}")
        print(f"planner.only_sources={planner['only_sources']}")

    embedder = Embedder.from_env()

    cfg = RetrievalConfig()
    cfg.only_sources = list(planner["only_sources"])
    if len(cfg.only_sources) == 1:
        cfg.only_source = cfg.only_sources[0]

    commercial_candidates = _retrieve_many(
        planner["queries"],
        cfg=cfg,
        embedder=embedder,
        top_k=90,
        doc_types=["price_list", "vendor_kp"],
    )

    if len(commercial_candidates) < 12:
        commercial_candidates = _retrieve_many(
            planner["queries"],
            cfg=cfg,
            embedder=embedder,
            top_k=110,
            doc_types=None,
        )

    commercial_hits = _pick_best_commercial(commercial_candidates, limit=32)

    if debug_commercial:
        _debug_dump_commercial(out_dir, commercial_candidates, commercial_hits)

    commercial_lines = _extract_commercial_snippets(commercial_hits, max_lines=220)
    commercial_raw = _build_context(commercial_hits, max_chars=9000, max_chunks=12)

    tech_hits = _retrieve_many(
        planner["queries"],
        cfg=cfg,
        embedder=embedder,
        top_k=16,
        doc_types=["sksps", "task_full"],
    )
    tech_context = _build_context(tech_hits, max_chars=11000, max_chunks=18)

    merged_context = ""
    if commercial_lines.strip():
        merged_context += "=== COMMERCIAL LINES (prefer these for SKU/price) ===\n"
        merged_context += commercial_lines.strip() + "\n\n"
    if commercial_raw.strip():
        merged_context += "=== COMMERCIAL CHUNKS (raw excerpts, may include tables without currency) ===\n"
        merged_context += commercial_raw.strip() + "\n\n"
    merged_context += "=== TECHNICAL CONTEXT (TЗ / прошлые СкСп / задачи) ===\n"
    merged_context += tech_context.strip()

    safety = ExternalSafetyConfig()
    safe_context = safe_for_external(merged_context, safety) if safety.redact else merged_context

    (out_dir / "result.context.txt").write_text(safe_context or "", encoding="utf-8")

    if dry_run:
        items = [
            SkspsItem(
                manufacturer="(TBD)",
                sku="(TBD)",
                description="(dry-run) ВКС комплект + монтаж/ПНР. Подключи YaGPT чтобы заполнить из прайсов/КП.",
                unit_price_rub=0.0,
                qty=1,
                delivery_time="(TBD)",
                comment="Контекст собран, см. result.context.txt",
                url="",
            )
        ]
        result = {
            "mode": "dry_run",
            "request": user_request,
            "planner": planner,
            "items": [it.__dict__ for it in items],
            "questions": ["Уточните город/регион (без точного адреса) и бюджет/бренды (если есть)."],
        }
    else:
        obj = _yagpt_generate_items(user_request, safe_context or "")
        norm_items = _normalize_items(obj.get("items"))
        result = {
            "mode": "yagpt",
            "request": user_request,
            "planner": planner,
            "items": norm_items,
            "questions": obj.get("questions", []),
            "rationale": obj.get("rationale", ""),
        }

    _write_json(out_dir / "result.json", result)

    items_for_xlsx = [SkspsItem(**it) for it in result["items"]]
    _fill_xlsx(template_path, out_dir / "result.xlsx", items_for_xlsx)

    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("request", type=str, help="User request (free text)")
    ap.add_argument("--template", required=True, help="Path to СкСп XLSX template")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--dry-run", action="store_true", help="Do not call YaGPT; still produce JSON+XLSX")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument(
        "--debug-commercial",
        action="store_true",
        help="Print top commercial chunks and write commercial.debug.json",
    )
    args = ap.parse_args()

    res = generate_sksps(
        args.request,
        template_path=Path(args.template),
        out_dir=Path(args.out_dir),
        dry_run=bool(args.dry_run),
        verbose=bool(args.verbose),
        debug_commercial=bool(args.debug_commercial),
    )
    print(f"OK: wrote {Path(args.out_dir) / 'result.xlsx'}")
    if res.get("questions"):
        print("QUESTIONS:", res["questions"])


if __name__ == "__main__":
    main()
