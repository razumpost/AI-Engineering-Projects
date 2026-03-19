#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CogneeClient: cypher-reads via kuzu напрямую из локальной БД Cognee.

Ключевое:
- БД kuzu у Cognee у тебя — ФАЙЛ:
    SYSTEM_ROOT_DIRECTORY/databases/cognee_graph_kuzu
- Внутри kuzu схема Cognee сейчас:
    Node(id, name, type, created_at, updated_at, properties[JSON-string])
    EDGE(relationship_name, created_at, updated_at, properties[JSON-string])

Задача:
- Выполнять Cypher чтения без cognee.search (который в 0.5.x упирается в users/principals).
- Поддержать legacy-запросы вида (:Task)-[:has_snapshot]->(:Snapshot) и i.desc CONTAINS "..."
  переводом в Node/EDGE и поиском по Node.properties.

ENV:
- COGNEE_ENABLED=1
- ALLOW_CYPHER_QUERY=true
- SYSTEM_ROOT_DIRECTORY=/abs/path/to/.cognee_system (по умолчанию ./ .cognee_system)
- COGNEE_CYPHER_DEBUG=1 (опционально)
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class CogneeConfig:
    enabled: bool
    allow_cypher: bool
    system_root: Path
    debug: bool
    legacy_compat: bool

    @staticmethod
    def from_env() -> "CogneeConfig":
        enabled = os.getenv("COGNEE_ENABLED", "0").strip() == "1"
        allow_cypher = _env_bool("ALLOW_CYPHER_QUERY", "0")
        debug = _env_bool("COGNEE_CYPHER_DEBUG", "0")
        legacy_compat = _env_bool("COGNEE_LEGACY_CYPHER_COMPAT", "1")

        root = os.getenv("SYSTEM_ROOT_DIRECTORY", "").strip()
        if not root:
            root = str((_repo_root() / ".cognee_system").resolve())
        rootp = Path(root).expanduser().resolve()
        return CogneeConfig(
            enabled=enabled,
            allow_cypher=allow_cypher,
            system_root=rootp,
            debug=debug,
            legacy_compat=legacy_compat,
        )


def _cypher_literal(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    s = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def _apply_params(query: str, params: Optional[Dict[str, Any]]) -> str:
    if not params:
        return query
    out = query
    for k, v in params.items():
        out = out.replace(f"${k}", _cypher_literal(v))
    return out


def _looks_like_json_obj(s: str) -> bool:
    s = s.strip()
    return s.startswith("{") and s.endswith("}")


def _expand_json_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Если в строке есть колонка props/properties/.. с JSON-строкой — распакуем и расплющим в корень.
    Плюс: если ключ сам был 'props'/'properties' — удаляем его чтобы не мешал.
    """
    out = dict(row)
    keys = list(out.keys())
    for k in keys:
        v = out.get(k)
        if isinstance(v, str) and _looks_like_json_obj(v):
            try:
                d = json.loads(v)
            except Exception:
                continue
            if isinstance(d, dict):
                # подмешиваем
                for dk, dv in d.items():
                    if dk not in out or out[dk] in (None, "", v):
                        out[dk] = dv
                # часто удобнее удалить исходный json-столбец
                if k in ("props", "properties") or k.endswith(".properties"):
                    out.pop(k, None)
    return out


class CogneeClient:
    def __init__(self) -> None:
        self.cfg = CogneeConfig.from_env()
        self.enabled = bool(self.cfg.enabled)
        self._kuzu: Any = None
        self._db: Any = None
        self._conn: Any = None

    # ---------- kuzu low-level ----------
    def _open_kuzu(self) -> Tuple[Any, Any]:
        if self._conn is not None:
            return self._db, self._conn
        try:
            import kuzu  # type: ignore
        except Exception as e:
            raise RuntimeError("kuzu-python not installed") from e

        db_file = (self.cfg.system_root / "databases" / "cognee_graph_kuzu").resolve()
        if not db_file.exists() or not db_file.is_file():
            raise FileNotFoundError(f"kuzu db file not found: {db_file}")

        if self.cfg.debug:
            print(f"[cognee_client] opening kuzu db: {db_file}")

        self._kuzu = kuzu
        self._db = kuzu.Database(str(db_file))
        self._conn = kuzu.Connection(self._db)
        return self._db, self._conn

    # ---------- legacy cypher compat ----------
    def _normalize_contains_literals(self, cypher: str) -> str:
        """
        1) CONTAINS "xxx" -> CONTAINS 'xxx'
        2) если в запросе есть `.properties` (ищем по JSON-строке) и literal содержит кириллицу —
           заменяем literal на \\uXXXX форму, чтобы матчить JSON-escapes в properties.
        """
        has_props = ".properties" in cypher

        def _fix_lit(lit: str) -> str:
            # если ищем по JSON-строке — кириллицу превращаем в \\uXXXX (двойной слэш)
            if has_props and any(ord(ch) > 127 for ch in lit):
                esc = lit.encode("unicode_escape").decode("ascii")   # \u043a\u0440...
                esc = esc.replace("\\u", "\\\\u")                    # \\u043a\\u0440...
                return esc
            return lit

        def repl_dq(m: re.Match) -> str:
            lit = m.group(1)
            lit = lit.replace("\\", "\\\\").replace("'", "\\'")
            return "CONTAINS '" + _fix_lit(lit) + "'"

        def repl_sq(m: re.Match) -> str:
            lit = m.group(1)
            lit = lit.replace("\\", "\\\\").replace("'", "\\'")
            return "CONTAINS '" + _fix_lit(lit) + "'"

        cypher = re.sub(r'(?is)\bCONTAINS\s+"([^"]*)"', repl_dq, cypher)
        cypher = re.sub(r"(?is)\bCONTAINS\s+'([^']*)'", repl_sq, cypher)
        return cypher

    def _legacy_to_node_edge(self, cypher: str) -> str:
        """
        Поддержка запросов, которые пишутся как будто в графе есть Task/Snapshot/Item/Scope и отношения has_item/...
        Переводим в Node/EDGE + фильтры.
        """
        if not self.cfg.legacy_compat:
            return cypher

        original = cypher

        # отношения
        rel_map = {
            "has_snapshot": "r_has_snapshot",
            "has_item": "r_has_item",
            "in_scope": "r_in_scope",
        }
        rel_filters: List[str] = []
        for rel, var in rel_map.items():
            # -[:rel]->  => -[var:EDGE]->
            cypher, n = re.subn(rf"(?is)-\s*\[:\s*{rel}\s*\]\s*->", rf"-[{var}:EDGE]->", cypher)
            if n:
                rel_filters.append(f"{var}.relationship_name='{rel}'")

        # Scope {name:"x"}  -> Node + фильтр по type/name
        scope_filters: List[str] = []
        def repl_scope(m: re.Match) -> str:
            alias = m.group("a")
            name_lit = m.group("n")
            # нормализуем в single quotes
            if name_lit.startswith('"') and name_lit.endswith('"'):
                name = name_lit[1:-1]
            elif name_lit.startswith("'") and name_lit.endswith("'"):
                name = name_lit[1:-1]
            else:
                name = name_lit
            name = name.replace("\\", "\\\\").replace("'", "\\'")
            scope_filters.append(f"{alias}.type='Scope'")
            scope_filters.append(f"{alias}.name='{name}'")
            return f"({alias}:Node)"
        cypher = re.sub(r'(?is)\((?P<a>\w+)\s*:\s*Scope\s*\{\s*name\s*:\s*(?P<n>"[^"]*"|\'[^\']*\')\s*\}\s*\)', repl_scope, cypher)

        # метки Task/Snapshot/Item/Scope -> Node + фильтр alias.type='X'
        type_filters: List[str] = []
        for label in ("Task", "Snapshot", "Item", "Scope"):
            # (x:Label  -> (x:Node
            for m in re.finditer(rf"(?is)\((\w+)\s*:\s*{label}\b", cypher):
                alias = m.group(1)
                type_filters.append(f"{alias}.type='{label}'")
            cypher = re.sub(rf"(?is)\((\w+)\s*:\s*{label}\b", r"(\1:Node", cypher)

        # поля sku/manufacturer/desc/task_id/... в WHERE обычно используются как lower(i.desc) ...
        # переводим их на поиск по JSON-строке i.properties
        cypher = re.sub(r"(?is)\blower\(\s*(\w+)\.(sku|manufacturer|desc|task_id|updated_at_iso|updated_at_ts)\s*\)", r"lower(\1.properties)", cypher)
        cypher = re.sub(r"(?is)\b(\w+)\.(sku|manufacturer|desc|task_id|updated_at_iso|updated_at_ts)\s+IS\s+NOT\s+NULL\b", r"\1.properties IS NOT NULL", cypher)

        # теперь добавим фильтры в WHERE
        filters = [*type_filters, *rel_filters, *scope_filters]
        if filters:
            fexpr = " AND ".join(filters)
            m_where = re.search(r"(?is)\bWHERE\b", cypher)
            if m_where:
                # вставляем сразу после WHERE
                cypher = re.sub(r"(?is)\bWHERE\b", f"WHERE {fexpr} AND ", cypher, count=1)
            else:
                # вставляем перед RETURN/WITH/ORDER BY/LIMIT
                m_ins = re.search(r"(?is)\bRETURN\b|\bWITH\b|\bORDER\s+BY\b|\bLIMIT\b", cypher)
                if m_ins:
                    pos = m_ins.start()
                    cypher = cypher[:pos] + f" WHERE {fexpr} " + cypher[pos:]
                else:
                    cypher = cypher + f" WHERE {fexpr}"

        # нормализуем CONTAINS (после подмены на .properties)
        cypher = self._normalize_contains_literals(cypher)

        if self.cfg.debug and original != cypher:
            print("[cognee_client] legacy->kuzu rewrite applied")

        return cypher

    # ---------- public API ----------
    def run_cypher_sync(
        self,
        cypher: str,
        top_k: int = 200,
        expand_properties: bool = True,
        params: Optional[Dict[str, Any]] = None,
        **_kw: Any,
    ) -> List[Dict[str, Any]]:
        if not self.enabled or not self.cfg.allow_cypher:
            return []

        cypher = _apply_params(cypher, params)
        cypher = self._legacy_to_node_edge(cypher)

        try:
            _, conn = self._open_kuzu()
            res = conn.execute(cypher)
            cols = list(res.get_column_names())
            out: List[Dict[str, Any]] = []
            while res.has_next() and len(out) < int(top_k):
                row = dict(zip(cols, res.get_next()))
                if expand_properties:
                    row = _expand_json_row(row)
                out.append(row)
            return out
        except Exception as e:
            if self.cfg.debug:
                print(f"[cognee_client] cypher failed: {e}")
                print("[cognee_client] cypher was:\n", cypher)
            return []

    # Утилиты (удобно для тестов/CLI)
    def get_task_node_id(self, task_id: int) -> Optional[str]:
        rows = self.run_cypher_sync(
            "MATCH (t:Node) WHERE t.type='Task' RETURN t.id AS id, t.properties AS props",
            top_k=5000,
        )
        for r in rows:
            try:
                if int(r.get("task_id") or -1) == int(task_id):
                    return str(r["id"])
            except Exception:
                continue
        return None

    def get_latest_snapshot(self, task_id: int) -> Optional[Dict[str, Any]]:
        tid = self.get_task_node_id(task_id)
        if not tid:
            return None
        rows = self.run_cypher_sync(
            "MATCH (t:Node)-[r:EDGE]->(s:Node) "
            "WHERE t.id=$tid AND r.relationship_name='has_snapshot' AND s.type='Snapshot' "
            "RETURN s.id AS sid, s.properties AS props",
            params={"tid": tid},
            top_k=5000,
        )
        if not rows:
            return None
        rows.sort(key=lambda x: int(x.get("updated_at_ts") or 0), reverse=True)
        return rows[0]

    def get_latest_snapshot_items(self, task_id: int, limit: int = 20, with_scopes: bool = True) -> List[Dict[str, Any]]:
        snap = self.get_latest_snapshot(task_id)
        if not snap:
            return []
        sid = snap["sid"]
        items = self.run_cypher_sync(
            "MATCH (s:Node)-[r:EDGE]->(i:Node) "
            "WHERE s.id=$sid AND r.relationship_name='has_item' AND i.type='Item' "
            "RETURN i.id AS iid, i.properties AS props",
            params={"sid": sid},
            top_k=max(50, int(limit) * 5),
        )[: int(limit)]

        if not with_scopes or not items:
            return items

        # подтягиваем scope пачкой (по одному запросу на item — норм для limit<=50)
        for it in items:
            iid = it.get("iid")
            if not iid:
                continue
            sc = self.run_cypher_sync(
                "MATCH (i:Node)-[r:EDGE]->(sc:Node) "
                "WHERE i.id=$iid AND r.relationship_name='in_scope' AND sc.type='Scope' "
                "RETURN sc.name AS scope LIMIT 1",
                params={"iid": iid},
                top_k=1,
                expand_properties=False,
            )
            it["scope"] = sc[0]["scope"] if sc else None
        return items


### COGNEE_CYPHER_NORMALIZE_LITERALS_V1 ###
import re as _re

def _lit_anchor(text: str) -> str:
    low = (text or "").lower()
    # латинские якоря (для hdmi и т.п. достаточно этого)
    for tok in ("hdmi", "hdbaset", "sdi", "sdvoe", "usb", "dp", "displayport", "lan", "ethernet", "4k", "1080"):
        if tok in low:
            return tok
    # кириллица -> \uXXXX, чтобы матчить JSON-escape в Node.properties
    if any(ord(ch) > 127 for ch in low):
        esc = low.encode("unicode_escape").decode("ascii")  # \u043a...
        esc = esc.replace("\\u", "\\\\u")                   # \\u043a...
        return esc
    return low.strip()

def _normalize_cypher_literals(cypher: str) -> str:
    # CONTAINS "..." -> CONTAINS '...'
    def repl_contains_dq(m):
        return "CONTAINS '" + _lit_anchor(m.group(1)).replace("'", "\\'") + "'"
    cypher = _re.sub(r'(?is)\bCONTAINS\s+"([^"]*)"', repl_contains_dq, cypher)

    # CONTAINS '...' -> якорим/нормализуем
    def repl_contains_sq(m):
        return "CONTAINS '" + _lit_anchor(m.group(1)).replace("'", "\\'") + "'"
    cypher = _re.sub(r"(?is)\bCONTAINS\s+'([^']*)'", repl_contains_sq, cypher)

    # {name: "x"} и = "x" -> одинарные кавычки
    cypher = _re.sub(r'(?is):\s*"([^"]*)"', lambda m: ": '" + m.group(1).replace("'", "\\'") + "'", cypher)
    cypher = _re.sub(r'(?is)=\s*"([^"]*)"', lambda m: "= '" + m.group(1).replace("'", "\\'") + "'", cypher)
    return cypher

try:
    _orig_run = CogneeClient.run_cypher_sync  # type: ignore[name-defined]
    def run_cypher_sync(self, cypher: str, *args, **kwargs):  # type: ignore[override]
        return _orig_run(self, _normalize_cypher_literals(cypher), *args, **kwargs)
    CogneeClient.run_cypher_sync = run_cypher_sync  # type: ignore[name-defined]
except Exception:
    pass
