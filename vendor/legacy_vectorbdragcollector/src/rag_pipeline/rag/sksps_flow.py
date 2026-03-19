# =========================
# File: src/rag_pipeline/rag/sksps_flow.py
# =========================
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from openpyxl import load_workbook

from ..security import ExternalSafetyConfig, detect_sensitive_markers, redact_sensitive
from .prompting import build_planner_messages, build_sksps_generator_messages
from .render_sksps import render_sksps_json_to_xlsx
from .retriever_pgvector import PgVectorRetriever
from .yandex_gpt_client import YandexGPTClient


_JSON_OBJ_RE = re.compile(r"\{.*\}", re.DOTALL)


@dataclass(frozen=True)
class PlannerResult:
    retrieval_queries: List[str]
    need_doc_types: List[str]
    notes: str = ""


def load_sksps_columns(template_path: str, *, sheet_name: str = "СкСп", header_row: int = 1) -> List[str]:
    wb = load_workbook(template_path, read_only=True, data_only=True)
    if sheet_name not in wb.sheetnames:
        raise RuntimeError(f"Template missing sheet {sheet_name!r}. Has: {wb.sheetnames}")
    ws = wb[sheet_name]
    cols: List[str] = []
    for cell in ws[header_row]:
        v = (cell.value or "")
        v = str(v).strip()
        if not v:
            break
        cols.append(v)
    return cols


def _extract_json(text: str) -> Dict[str, Any]:
    s = (text or "").strip()
    if not s:
        raise ValueError("empty response")
    # Fast path
    try:
        return json.loads(s)
    except Exception:
        pass
    # Try to find the first {...} block
    m = _JSON_OBJ_RE.search(s)
    if not m:
        raise ValueError("no JSON object found in response")
    return json.loads(m.group(0))


def run_planner(llm: YandexGPTClient, user_request: str, *, known_doc_types: Optional[List[str]] = None) -> PlannerResult:
    msgs = build_planner_messages(user_request, known_doc_types=known_doc_types)
    txt = llm.complete(msgs, temperature=0.1, max_tokens=700)
    obj = _extract_json(txt)

    queries = [q.strip() for q in (obj.get("retrieval_queries") or []) if isinstance(q, str) and q.strip()]
    need = [d.strip() for d in (obj.get("need_doc_types") or []) if isinstance(d, str) and d.strip()]
    notes = str(obj.get("notes") or "").strip()

    if not queries:
        queries = [user_request.strip()]

    return PlannerResult(retrieval_queries=queries[:12], need_doc_types=need[:20], notes=notes)


def retrieve_context(
    retriever: PgVectorRetriever,
    queries: List[str],
    *,
    top_k: int = 12,
    doc_types: Optional[List[str]] = None,
    per_src_limit: int = 3,
) -> List[Dict[str, Any]]:
    """Retrieve and dedupe chunks across multiple queries."""
    seen = set()
    out: List[Dict[str, Any]] = []
    for q in queries:
        hits = retriever.search(q, top_k=top_k, doc_types=doc_types, per_src_limit=per_src_limit)
        for h in hits:
            cid = h.get("chunk_id") or h.get("chunk") or h.get("id")
            if cid in seen:
                continue
            seen.add(cid)
            out.append(h)
    return out


def generate_sksps_json(
    llm: YandexGPTClient,
    user_request: str,
    *,
    context_blocks: List[Dict[str, Any]],
    sksps_columns: List[str],
) -> Dict[str, Any]:
    msgs = build_sksps_generator_messages(
        user_request,
        context_blocks=context_blocks,
        sksps_columns=sksps_columns,
    )
    txt = llm.complete(msgs, temperature=0.2, max_tokens=2200)
    obj = _extract_json(txt)

    # Final safety sweep: if something slipped, redact it in-place (fail closed is enforced on outbound already)
    dumped = json.dumps(obj, ensure_ascii=False)
    is_sensitive, reason = detect_sensitive_markers(dumped)
    if is_sensitive:
        # keep structure, redact string values
        obj = _deep_redact(obj)
        obj["_safety_note"] = f"Redacted leaked marker: {reason}"
    return obj


def _deep_redact(x: Any) -> Any:
    if isinstance(x, str):
        return redact_sensitive(x, mode=ExternalSafetyConfig().mode)
    if isinstance(x, list):
        return [_deep_redact(i) for i in x]
    if isinstance(x, dict):
        return {k: _deep_redact(v) for k, v in x.items()}
    return x


def run_sksps_pipeline(
    user_request: str,
    *,
    template_path: str,
    out_xlsx: str,
    out_json: Optional[str] = None,
    top_k: int = 12,
    per_src_limit: int = 3,
    doc_types: Optional[List[str]] = None,
    dry_run: bool = False,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """End-to-end: planner -> retrieval -> generator -> xlsx."""
    if not os.path.exists(template_path):
        raise FileNotFoundError(template_path)

    sksps_columns = load_sksps_columns(template_path)

    retriever = PgVectorRetriever()

    # Planner is optional; if dry_run, keep queries simple
    if dry_run:
        queries = [user_request]
        planner_need: List[str] = []
    else:
        llm = YandexGPTClient()
        plan = run_planner(llm, user_request)
        queries = plan.retrieval_queries
        planner_need = plan.need_doc_types

    effective_doc_types = doc_types or (planner_need or None)

    hits = retrieve_context(
        retriever,
        queries,
        top_k=top_k,
        doc_types=effective_doc_types,
        per_src_limit=per_src_limit,
    )

    if dry_run:
        # Produce a minimal stub JSON so you can render and inspect pipeline wiring.
        obj = {
            "project": {"goal": user_request, "location_city": None, "location_region": None, "assumptions": [], "constraints": []},
            "questions": ["TBD: уточните требования (кол-во точек, длины трасс, состав оборудования)."],
            "items": [],
        }
    else:
        llm = YandexGPTClient()
        obj = generate_sksps_json(llm, user_request, context_blocks=hits, sksps_columns=sksps_columns)

    if out_json:
        Path(out_json).parent.mkdir(parents=True, exist_ok=True)
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

    render_sksps_json_to_xlsx(obj, template_path=template_path, out_path=out_xlsx)
    return obj, hits
