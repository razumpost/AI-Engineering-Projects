from __future__ import annotations

"""
Deterministic graph-first helper (no hallucinations).

Used in 2 places:
1) retrieval rerank (task_ids) by scopes present in latest snapshot
2) patch resolve for:
   - replace X -> Y: enforce replacement in scope
   - add N extenders: strict qty + canonical SKU merge
   - delegate/chairman: central unit check -> add engineering question

Graph is populated ONLY by scripts/cognee_ingest.py (facts from DB).
"""

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .cognee_client import CogneeClient


@dataclass(frozen=True)
class EnrichedQuery:
    scopes: Tuple[str, ...]


class ScopeClassifier:
    _scope_keywords: Dict[str, Tuple[str, ...]] = {
        "processing": (
            "контроллер",
            "видеоконтроллер",
            "процессор",
            "controller",
            "processor",
            "videon",
            "novastar",
            "vx",
            "lvp",
        ),
        "display": ("видеостен", "панел", "диспле", "экран", "videowall", "led", "lcd", '55"'),
        "cameras": ("ptz", "камера", "camera", "zoom", "lens", "tracking", "autoframe"),
        "videobar": ("видеобар", "videobar", "soundbar", "speakerphone", "all-in-one", "all in one"),
        "conference": (
            "конференц",
            "делегат",
            "председател",
            "chairman",
            "delegate",
            "пульт",
            "central unit",
            "base unit",
        ),
        "signal_transport": ("hdbaset", "extender", "удлин", "hdmi", "hdmi over", "tx", "rx", "передатчик", "приемник"),
    }

    def scopes_for_text(self, text: str) -> Tuple[str, ...]:
        t = (text or "").casefold()
        out: List[str] = []
        for scope, kws in self._scope_keywords.items():
            if any(k.casefold() in t for k in kws):
                out.append(scope)
        return tuple(out)


class PatchParser:
    _re_replace = re.compile(r"(замени|заменить|replace)\s+(?P<frm>.+?)\s+(на|with)\s+(?P<to>.+)$", re.IGNORECASE)
    _re_add = re.compile(r"(добавь|add)\s+(?:(ещ[её])\s+)?(?P<qty>\d+)\s*(?P<what>.+)$", re.IGNORECASE)

    def parse_replace(self, text: str) -> Optional[Tuple[str, str]]:
        m = self._re_replace.search(text or "")
        if not m:
            return None
        return m.group("frm").strip(), m.group("to").strip()

    def parse_add(self, text: str) -> Optional[Tuple[int, str]]:
        m = self._re_add.search(text or "")
        if not m:
            return None
        return int(m.group("qty")), (m.group("what") or "").strip()

    def mentions_conference(self, text: str) -> bool:
        t = (text or "").casefold()
        return any(k in t for k in ("делегат", "председател", "delegate", "chairman"))


class GraphAdapter:
    def __init__(self, client: Optional[CogneeClient] = None) -> None:
        self.client = client or CogneeClient()
        self.scopes = ScopeClassifier()
        self.parser = PatchParser()

    @property
    def enabled(self) -> bool:
        return self.client.enabled

    def enrich_query(self, query_text: str) -> EnrichedQuery:
        return EnrichedQuery(scopes=self.scopes.scopes_for_text(query_text))

    def rerank_task_ids(self, task_ids: Sequence[int], enriched: EnrichedQuery) -> List[int]:
        if not self.enabled or not task_ids or not enriched.scopes:
            return list(task_ids)

        ids_lit = ",".join(str(int(x)) for x in task_ids[:80])
        q = f"""
        MATCH (t:Task)
        WHERE t.task_id IN [{ids_lit}]
        OPTIONAL MATCH (t)-[:has_snapshot]->(s:Snapshot)
        WITH t, max(s.updated_at_ts) as ts
        OPTIONAL MATCH (t)-[:has_snapshot]->(s2:Snapshot)
        WHERE s2.updated_at_ts = ts
        OPTIONAL MATCH (s2)-[:has_item]->(:Item)-[:in_scope]->(sc:Scope)
        RETURN t.task_id as task_id, ts as ts, collect(DISTINCT sc.name) as scopes
        """
        rows = self.client.run_cypher_sync(q, top_k=400)

        scored: Dict[int, int] = {}
        for r in rows:
            tid = _pick(r, "task_id")
            if tid is None:
                continue
            try:
                tid_i = int(tid)
            except Exception:
                continue

            ts = _pick(r, "ts")
            try:
                ts_i = int(ts) if ts is not None else 0
            except Exception:
                ts_i = 0

            row_scopes = _pick(r, "scopes") or []
            if isinstance(row_scopes, str):
                row_scopes = [row_scopes]
            row_scopes = [str(x) for x in row_scopes if x]

            matches = sum(1 for s in enriched.scopes if s in row_scopes)
            scored[tid_i] = matches * 10_000_000_000 + ts_i

        return sorted(list(task_ids), key=lambda x: scored.get(int(x), 0), reverse=True)

    def resolve_patch(self, *, primary_task_id: int, patch_text: str) -> Dict[str, Any]:
        if not self.enabled or not patch_text or not primary_task_id:
            return {}

        out: Dict[str, Any] = {}
        rep = self._resolve_replace(primary_task_id, patch_text)
        if rep:
            out["replace"] = rep
        add = self._resolve_add(patch_text)
        if add:
            out["add"] = add
        conf = self._resolve_conference(primary_task_id, patch_text)
        if conf:
            out["conference"] = conf
        return out

    def _resolve_replace(self, task_id: int, patch_text: str) -> Optional[Dict[str, Any]]:
        parsed = self.parser.parse_replace(patch_text)
        if not parsed:
            return None
        _, to = parsed
        scopes = self.scopes.scopes_for_text(patch_text) or ("processing",)
        scope = scopes[0]

        current = self._task_items(task_id, scope=scope)
        old_skus = [str(it["sku"]) for it in current if it.get("sku")]

        cand = self._find_best_item(term=to, scope=scope)
        if not cand:
            return {"scope": scope, "old_skus": old_skus, "new_sku": None, "reason": "no_candidate_for_to_term"}

        return {
            "scope": scope,
            "old_skus": old_skus,
            "new_sku": cand.get("sku"),
            "new_hint": {"manufacturer": cand.get("manufacturer"), "desc": cand.get("desc")},
            "reason": None,
        }

    def _resolve_add(self, patch_text: str) -> Optional[Dict[str, Any]]:
        parsed = self.parser.parse_add(patch_text)
        if not parsed:
            return None
        qty, what = parsed

        scopes = self.scopes.scopes_for_text(patch_text) or self.scopes.scopes_for_text(what) or ("signal_transport",)
        scope = scopes[0]

        cand = self._find_best_item(term=what, scope=scope)
        if not cand:
            return {"scope": scope, "sku": None, "qty": int(qty), "reason": "no_candidate_item"}

        return {
            "scope": scope,
            "sku": cand.get("sku"),
            "qty": int(qty),
            "hint": {"manufacturer": cand.get("manufacturer"), "desc": cand.get("desc")},
        }

    def _resolve_conference(self, task_id: int, patch_text: str) -> Optional[Dict[str, Any]]:
        if not self.parser.mentions_conference(patch_text):
            return None
        if self._task_has_central_unit(task_id):
            return None

        add_placeholder = os.getenv("COGNEE_PLACEHOLDERS", "0").strip() == "1"
        return {
            "needs_central_unit": True,
            "question": "Нужен central unit/processor конференц-системы (совместимость с пультами делегатов/председателя). Уточните модель/серии.",
            "add_placeholder": add_placeholder,
        }

    def _task_items(self, task_id: int, *, scope: Optional[str]) -> List[Dict[str, Any]]:
        scope_match = ""
        if scope:
            scope_match = f'MATCH (i)-[:in_scope]->(sc:Scope {{name: "{_esc(scope)}"}})'

        q = f"""
        MATCH (t:Task {{task_id: {int(task_id)} }})-[:has_snapshot]->(s:Snapshot)
        WITH s ORDER BY s.updated_at_ts DESC LIMIT 1
        MATCH (s)-[:has_item]->(i:Item)
        {scope_match}
        RETURN i.sku as sku, i.manufacturer as manufacturer, i.desc as desc
        """
        rows = self.client.run_cypher_sync(q, top_k=400)

        out: List[Dict[str, Any]] = []
        for r in rows:
            sku = _pick(r, "sku")
            if isinstance(sku, str) and sku.strip():
                out.append({"sku": sku.strip(), "manufacturer": _pick(r, "manufacturer"), "desc": _pick(r, "desc")})
        return out

    def _find_best_item(self, *, term: str, scope: Optional[str]) -> Optional[Dict[str, Any]]:
        term_l = (term or "").casefold().strip()
        if not term_l:
            return None

        scope_match = ""
        if scope:
            scope_match = f'MATCH (i)-[:in_scope]->(sc:Scope {{name: "{_esc(scope)}"}})'

        q = f"""
        MATCH (s:Snapshot)-[:has_item]->(i:Item)
        {scope_match}
        WHERE
          (i.sku IS NOT NULL AND lower(i.sku) CONTAINS "{_esc(term_l)}")
          OR (i.manufacturer IS NOT NULL AND lower(i.manufacturer) CONTAINS "{_esc(term_l)}")
          OR (i.desc IS NOT NULL AND lower(i.desc) CONTAINS "{_esc(term_l)}")
        RETURN i.sku as sku, i.manufacturer as manufacturer, i.desc as desc, s.updated_at_ts as ts
        ORDER BY ts DESC
        LIMIT 10
        """
        rows = self.client.run_cypher_sync(q, top_k=20)
        for r in rows:
            sku = _pick(r, "sku")
            if isinstance(sku, str) and sku.strip():
                return {"sku": sku.strip(), "manufacturer": _pick(r, "manufacturer"), "desc": _pick(r, "desc")}
        return None

    def _task_has_central_unit(self, task_id: int) -> bool:
        items = self._task_items(task_id, scope="conference")
        for it in items:
            blob = f"{it.get('sku','')} {it.get('manufacturer','')} {it.get('desc','')}".casefold()
            if any(k in blob for k in ("central unit", "центральн", "processor", "процессор", "base unit")):
                return True
        return False


def _pick(row: Any, key: str) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key)
    try:
        return getattr(row, key)
    except Exception:
        return None


def _esc(s: str) -> str:
    return (s or "").replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ").strip()
