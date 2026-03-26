from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional

import kuzu
from dotenv import load_dotenv  # type: ignore
from sqlalchemy import bindparam, create_engine, text

from ..domain.candidates import CandidateItem, CandidatePool, CandidateTask
from .bitrix_links import task_url


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _load_env() -> None:
    load_dotenv(dotenv_path=str(_repo_root() / ".env"), override=False)


def _safe_json(v: Any) -> dict[str, Any]:
    if isinstance(v, dict):
        return v
    if isinstance(v, str) and v.strip():
        try:
            return json.loads(v)
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
        m = re.search(r"(\d+(?:\.\d+)?)", s)
        if not m:
            return None
        return Decimal(m.group(1))
    except (InvalidOperation, Exception):
        return None


def _make_engine():
    _load_env()
    dsn = (os.getenv("DATABASE_URL") or os.getenv("DB_DSN") or "").strip()
    if not dsn:
        return None
    return create_engine(dsn, pool_pre_ping=True)


def _keyword_tasks_postgres(engine, query: str, limit: int = 30) -> list[CandidateTask]:
    # optional: use Postgres only for nicer titles/urls; items come from Kuzu
    terms = [t for t in re.split(r"[^\wА-Яа-я]+", (query or "").lower()) if len(t) >= 4][:8]
    if not terms:
        return []

    params: dict[str, Any] = {"limit": int(limit), "sources": ["bitrix_task", "task"]}
    conds = []
    for i, t in enumerate(terms):
        params[f"p{i}"] = f"%{t}%"
        p = f":p{i}"
        conds.append(f"(d.title ILIKE {p} OR CAST(d.meta AS TEXT) ILIKE {p})")
    where_any = " OR ".join(conds)

    sql = (
        text(
            f"""
            SELECT d.source_id, d.title, d.meta
            FROM rag_documents d
            WHERE d.source IN :sources
              AND ({where_any})
            ORDER BY d.id DESC
            LIMIT :limit
            """
        )
        .bindparams(bindparam("sources", expanding=True))
    )

    out: list[CandidateTask] = []
    with engine.connect() as conn:
        rows = conn.execute(sql, params).mappings().all()
        for r in rows:
            meta = _safe_json(r.get("meta"))
            sid = r.get("source_id")
            try:
                tid = int(sid)
            except Exception:
                continue
            title = r.get("title") or meta.get("title") or f"Task {tid}"
            out.append(
                CandidateTask(
                    task_id=tid,
                    title=title,
                    url=task_url(tid),
                    similarity=0.0,
                    snippet="",
                )
            )
    return out


@dataclass(frozen=True)
class GraphTask:
    task_id: int
    title: str
    url: str
    index_text: str


class KuzuGraph:
    def __init__(self) -> None:
        _load_env()
        sys_root = (os.getenv("SYSTEM_ROOT_DIRECTORY") or "").strip()
        if not sys_root:
            raise RuntimeError("SYSTEM_ROOT_DIRECTORY is not set")
        self.path = str(Path(sys_root) / "databases" / "cognee_graph_kuzu")
        self.db = kuzu.Database(self.path)
        self.conn = kuzu.Connection(self.db)

    def _query(self, cypher: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        res = self.conn.execute(cypher, params or {})
        cols = list(res.get_column_names())
        out: list[dict[str, Any]] = []
        while res.has_next():
            row = res.get_next()
            out.append(dict(zip(cols, row)))
        return out

    def get_tasks(self, limit: int = 250) -> list[GraphTask]:
        rows = self._query(
            """
            MATCH (n:Node)
            WHERE n.type = 'Task'
            RETURN n.id AS id, n.name AS name, n.properties AS props
            LIMIT $limit
            """,
            {"limit": int(limit)},
        )
        out: list[GraphTask] = []
        for r in rows:
            props = _safe_json(r.get("props"))
            tid = int(props.get("task_id") or r.get("id") or 0)
            title = props.get("title") or r.get("name") or f"Task {tid}"
            idx = ""
            try:
                idx = (props.get("metadata") or {}).get("index_text") or ""
            except Exception:
                idx = ""
            out.append(GraphTask(task_id=tid, title=title, url=task_url(tid), index_text=str(idx)))
        return out

    def get_snapshots_for_task(self, task_id: int, limit: int = 40) -> list[dict[str, Any]]:
        return self._query(
            """
            MATCH (t:Node)-[:HAS]->(s:Node)
            WHERE t.type='Task' AND s.type='Snapshot' AND t.properties CONTAINS $tid
            RETURN s.id AS id, s.name AS name, s.properties AS props
            LIMIT $limit
            """,
            {"tid": str(task_id), "limit": int(limit)},
        )

    def get_items_for_snapshot(self, snapshot_id: str, limit: int = 800) -> list[dict[str, Any]]:
        return self._query(
            """
            MATCH (s:Node)-[:HAS]->(i:Node)
            WHERE s.type='Snapshot' AND i.type='Item' AND s.id = $sid
            RETURN i.id AS id, i.name AS name, i.properties AS props
            LIMIT $limit
            """,
            {"sid": snapshot_id, "limit": int(limit)},
        )


def _pick_best_graph_tasks(tasks: list[GraphTask], query: str, k: int = 12) -> list[GraphTask]:
    q = (query or "").casefold().strip()
    if not q:
        return tasks[:k]
    toks = [t for t in re.split(r"[^\wА-Яа-я]+", q) if len(t) >= 4][:10]
    if not toks:
        return tasks[:k]

    scored: list[tuple[float, GraphTask]] = []
    for t in tasks:
        txt = (t.index_text or "").casefold()
        s = 0.0
        for tok in toks:
            if tok in txt:
                s += 1.0
        scored.append((s, t))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [t for s, t in scored[:k]]


def retrieve_candidates(text: str, scope_whitelist: Optional[list[str]] = None) -> CandidatePool:
    """
    Kuzu-direct retrieval:
    - tasks + snapshots/items from Kuzu Node/EDGE
    - optional Postgres keyword tasks to enrich titles/urls
    - optional scope_whitelist filters items by category/scope (for patch-mode)
    """
    _load_env()
    debug = os.getenv("MVP_SKSP_DEBUG") == "1"

    whitelist_set = set(scope_whitelist) if scope_whitelist else None

    g = KuzuGraph()
    graph_tasks = g.get_tasks(limit=250)
    picked = _pick_best_graph_tasks(graph_tasks, text, k=12)

    if debug:
        print("[debug] graph tasks total:", len(graph_tasks), "picked:", len(picked))
        print("[debug] kuzu path:", g.path)
        if whitelist_set:
            print("[debug] scope_whitelist:", whitelist_set)

    items: list[CandidateItem] = []
    for gt in picked:
        snaps = g.get_snapshots_for_task(gt.task_id, limit=40)
        for s in snaps:
            sid = str(s.get("id"))
            iprops = _safe_json(s.get("props"))
            scope = (iprops.get("scope") or iprops.get("category") or "").strip() or "unknown"
            if whitelist_set and scope not in whitelist_set:
                continue

            snap_items = g.get_items_for_snapshot(sid, limit=800)
            for it in snap_items:
                props = _safe_json(it.get("props"))
                sku = (props.get("sku") or props.get("article") or props.get("арт") or None)
                mfr = (props.get("manufacturer") or props.get("vendor") or None)
                name = str(props.get("name") or it.get("name") or "")
                desc = str(props.get("description") or props.get("desc") or "")

                price = _to_decimal(props.get("price") or props.get("unit_price") or props.get("unit_price_rub"))

                cid = f"kuzu:{gt.task_id}:{sid}:{it.get('id')}"
                items.append(
                    CandidateItem(
                        candidate_id=cid,
                        category=scope,
                        sku=str(sku).strip() if sku else None,
                        manufacturer=str(mfr).strip() if mfr else None,
                        model=None,
                        name=name or (desc[:80] if desc else "UNKNOWN"),
                        description=desc or name or "",
                        unit_price_rub=price,
                        price_source="kuzu_graph",
                        evidence_task_ids=[gt.task_id],
                        meta={"task_id": gt.task_id, "snapshot_id": sid, "scope": scope},
                    )
                )

    tasks: list[CandidateTask] = []
    existing = set()
    for gt in picked:
        tasks.append(CandidateTask(task_id=gt.task_id, title=gt.title, url=gt.url, similarity=0.0, snippet=""))
        existing.add(gt.task_id)

    eng = _make_engine()
    if eng is not None:
        for t in _keyword_tasks_postgres(eng, text, limit=30):
            if t.task_id not in existing:
                tasks.append(t)
                existing.add(t.task_id)

    return CandidatePool(items=items, tasks=tasks)