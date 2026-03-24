from __future__ import annotations

import uuid
from typing import Any

from pydantic import ValidationError

from ..domain.candidates import CandidatePool
from ..domain.llm_contract import SkspLLMResponse
from ..domain.ops import MatchSelector, PatchOperation, TargetSelector
from ..domain.spec import Spec
from ..editing.editor import apply_operations
from ..editing.matching import resolve_single_line
from ..editing.parser import parse_patch_intent
from ..llm.client import ChatCompletionClient, extract_json_object
from ..persistence.snapshot_store import SnapshotPaths, load_last_valid, save_iter, save_text, update_last_valid
from ..validation.validator import validate_and_fix
from .autofill import build_autofill_ops


_BANNED_MANAGER_TOKENS = ["vlan", "igmp", "edid", "hdcp", "genlock", "pixel pitch", "nits", "poe", "битрейт"]


def _new_spec_id() -> str:
    return f"sp_{uuid.uuid4().hex[:12]}"


def _filter_manager_questions(qs: list[str]) -> list[str]:
    out: list[str] = []
    for q in qs:
        low = (q or "").casefold()
        if any(t in low for t in _BANNED_MANAGER_TOKENS):
            continue
        out.append(q)
    return out


def _as_str_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if isinstance(v, str):
        return [v.strip()] if v.strip() else []
    return [str(v).strip()] if str(v).strip() else []


def _normalize_explanations(expl):
    if isinstance(expl, dict):
        out = {}
        for k, v in expl.items():
            out[str(k)] = _as_str_list(v)
        return out
    if isinstance(expl, list):
        return {"why_composition": _as_str_list(expl), "why_qty_and_price": []}
    if isinstance(expl, str):
        return {"why_composition": [expl], "why_qty_and_price": []}
    return {"why_composition": [], "why_qty_and_price": []}


def _as_str_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if isinstance(v, str):
        return [v.strip()] if v.strip() else []
    return [str(v).strip()] if str(v).strip() else []


def _normalize_explanations(expl):
    if isinstance(expl, dict):
        out = {}
        for k, v in expl.items():
            out[str(k)] = _as_str_list(v)
        return out
    if isinstance(expl, list):
        return {"why_composition": _as_str_list(expl), "why_qty_and_price": []}
    if isinstance(expl, str):
        return {"why_composition": [expl], "why_qty_and_price": []}
    return {"why_composition": [], "why_qty_and_price": []}


def _apply_llm_meta(spec: Spec, resp: SkspLLMResponse) -> None:
    spec.project_summary = getattr(resp.brief, "project_summary", "") or spec.project_summary

    ex = _normalize_explanations(getattr(resp, "explanations", None))
    spec.why_composition = list(ex.get("why_composition", []))
    spec.why_qty_and_price = list(ex.get("why_qty_and_price", []))

    spec.assumptions = _as_str_list(getattr(resp, "assumptions", []))
    spec.risks = _as_str_list(getattr(resp, "risks", []))

    fqs = getattr(resp, "followup_questions", []) or []
    qs = []
    for q in fqs:
        if isinstance(q, dict):
            qs.append(str(q.get("question", "")).strip())
        else:
            qs.append(str(getattr(q, "question", q)).strip())
    spec.manager_questions = _filter_manager_questions([x for x in qs if x])


def _restrict_ops_to_pool(ops: list[PatchOperation], pool: CandidatePool) -> list[PatchOperation]:
    allowed = set(pool.by_id().keys())
    out: list[PatchOperation] = []
    for op in ops:
        cid = op.item.candidate_id if op.item and op.item.candidate_id else None
        if op.op in {"add_line", "replace_line"} and cid and cid not in allowed:
            continue
        out.append(op)
    return out


def _coerce_llm_obj(obj: dict[str, Any]) -> dict[str, Any]:
    d = dict(obj)
    if "version" not in d:
        d["version"] = "sksp.v1"
    if "mode" not in d:
        d["mode"] = "compose"
    if "brief" not in d:
        d["brief"] = {"project_summary": d.get("project_summary", ""), "constraints": {}}
    if "used_evidence" not in d:
        d["used_evidence"] = {"bitrix_task_ids": d.get("bitrix_task_ids", []), "candidate_item_ids": []}
    if "explanations" not in d:
        d["explanations"] = {"why_composition": d.get("why_composition", []), "why_qty_and_price": d.get("why_qty_and_price", [])}

    fq = d.get("followup_questions")
    if isinstance(fq, list) and fq and isinstance(fq[0], str):
        d["followup_questions"] = [{"question": x.strip(), "priority": "medium"} for x in fq if str(x).strip()]
    elif isinstance(fq, list) and fq and isinstance(fq[0], dict):
        fixed = []
        for q in fq:
            if isinstance(q, dict) and "question" in q and "priority" not in q:
                fixed.append({**q, "priority": "medium"})
            else:
                fixed.append(q)
        d["followup_questions"] = fixed

    ops = d.get("operations")
    if isinstance(ops, list) and ops and all(isinstance(x, dict) for x in ops):
        if all(("op" not in x) and ("candidate_id" in x) for x in ops):
            new_ops: list[dict[str, Any]] = []
            for x in ops:
                cid = x.get("candidate_id")
                if not cid:
                    continue
                qty = x.get("qty") or x.get("quantity") or 1
                cat = x.get("category") or x.get("scope") or "misc"
                ev = x.get("evidence_task_ids") or x.get("bitrix_task_ids") or []
                new_ops.append(
                    {
                        "op": "add_line",
                        "category": cat,
                        "item": {"candidate_id": cid},
                        "qty": qty,
                        "reason": x.get("reason") or "",
                        "evidence_task_ids": ev,
                    }
                )
            d["operations"] = new_ops

    return d


def _parse_llm_response(raw: str) -> SkspLLMResponse:
    obj = extract_json_object(raw)
    try:
        return SkspLLMResponse.model_validate(obj)
    except ValidationError:
        return SkspLLMResponse.model_validate(_coerce_llm_obj(obj))


def _coerce_ops_for_replace_intent(spec: Spec, resp: SkspLLMResponse, patch_text: str) -> None:
    intent = parse_patch_intent(patch_text)
    if intent.action != "replace" or not intent.target:
        return

    has_replace = any(op.op == "replace_line" for op in resp.operations)
    if has_replace:
        resp.operations = [op for op in resp.operations if not (op.op == "add_line" and not op.explicit_add)]
        return

    adds = [op for op in resp.operations if op.op == "add_line" and op.item and op.item.candidate_id]
    if len(adds) != 1:
        resp.operations = []
        spec.manager_questions = _filter_manager_questions(
            spec.manager_questions + [f"Что именно заменить в спецификации: '{intent.target}'? Укажи строку/SKU/производителя."]
        )
        return

    sel = MatchSelector(category=None, contains=[intent.target])
    line_id = resolve_single_line(spec.items, sel)
    if not line_id:
        resp.operations = []
        spec.manager_questions = _filter_manager_questions(spec.manager_questions + [f"Не нашёл в спецификации '{intent.target}'. Что именно заменить?"])
        return

    add = adds[0]
    resp.operations = [
        PatchOperation(
            op="replace_line",
            category=add.category,
            target=TargetSelector(line_id=line_id, match=None),
            item=add.item,
            qty=add.qty,
            explicit_add=False,
            reason=add.reason,
            evidence_task_ids=add.evidence_task_ids,
            meta=add.meta,
        )
    ]


def compose(*, llm: ChatCompletionClient, run: SnapshotPaths, system: str, user: str, pool: CandidatePool, request_text: str) -> Spec:
    spec = Spec(spec_id=_new_spec_id(), project_title="СкСп", items=[])

    raw = llm.complete([{"role": "system", "text": system}, {"role": "user", "text": user}])
    save_text(run, "llm_compose_raw", raw)

    resp = _parse_llm_response(raw)
    _apply_llm_meta(spec, resp)

    arep = apply_operations(spec, resp.operations, pool)
    spec.apply_warnings = list(arep.warnings)
    save_iter(run, "compose_draft", spec)

    # Auto-fill if draft is too small
    fill_ops = build_autofill_ops(spec=spec, pool=pool, query_text=request_text)
    if fill_ops:
        arep2 = apply_operations(spec, fill_ops, pool)
        spec.apply_warnings.extend(list(arep2.warnings))
        save_iter(run, "compose_autofilled", spec)

    vrep = validate_and_fix(spec, pool)
    if not vrep.ok:
        raise RuntimeError(f"Validation failed. Errors={vrep.errors}")
    spec.validation_warnings = list(vrep.warnings)

    used = set(resp.used_evidence.bitrix_task_ids)
    for it in spec.items:
        used.update(it.evidence.bitrix_task_ids)
    spec.used_bitrix_task_ids = sorted(used)

    update_last_valid(run, spec)
    save_iter(run, "compose_validated", spec)
    return spec


def patch(*, llm: ChatCompletionClient, run: SnapshotPaths, system: str, user: str, pool: CandidatePool, patch_text: str) -> Spec:
    base = load_last_valid(run) or Spec(spec_id=_new_spec_id(), project_title="СкСп", items=[])

    raw = llm.complete([{"role": "system", "text": system}, {"role": "user", "text": user}])
    save_text(run, "llm_patch_raw", raw)

    resp = _parse_llm_response(raw)
    _apply_llm_meta(base, resp)

    _coerce_ops_for_replace_intent(base, resp, patch_text)

    arep = apply_operations(base, resp.operations, pool)
    base.apply_warnings = list(arep.warnings)
    save_iter(run, "patch_draft", base)

    vrep = validate_and_fix(base, pool)
    if not vrep.ok:
        prev = load_last_valid(run)
        if prev:
            return prev
        raise RuntimeError(f"Validation failed, no last_valid. Errors={vrep.errors}")
    base.validation_warnings = list(vrep.warnings)

    used = set(resp.used_evidence.bitrix_task_ids)
    for it in base.items:
        used.update(it.evidence.bitrix_task_ids)
    base.used_bitrix_task_ids = sorted(used)

    update_last_valid(run, base)
    save_iter(run, "patch_validated", base)
    return base
