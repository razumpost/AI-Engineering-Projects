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
            ORDER BY d.updated_at DESC NULLS LAST
            LIMIT :limit
            """
        )
        .bindparams(bindparam("sources", expanding=True))
    )

    out: list[CandidateTask] = []
    with engine.connect() as conn:
        rows = conn.execute(sql, params).mappings().all()

    for r in rows:
        try:
            tid = int(r["source_id"])
        except Exception:
            continue
        meta = _safe_json(r.get("meta"))
        url = None
        raw = meta.get("raw")
        if isinstance(raw, dict):
            url = raw.get("url") or raw.get("link")
        url = url or meta.get("url") or meta.get("link") or task_url(tid)
        out.append(
            CandidateTask(
                task_id=tid,
                title=str(r.get("title") or f"Task #{tid}"),
                url=str(url),
                similarity=0.0,
                snippet="",
            )
        )
    return out


@dataclass(frozen=True)
class GraphTask:
    task_id: int
    node_id: str
    title: str
    url: str
    props_text: str


class KuzuGraph:
    def __init__(self) -> None:
        _load_env()
        root = os.getenv("SYSTEM_ROOT_DIRECTORY") or ""
        if not root:
            raise RuntimeError("SYSTEM_ROOT_DIRECTORY is not set")
        p = Path(root) / "databases" / "cognee_graph_kuzu"
        if not p.exists():
            raise RuntimeError(f"Kuzu graph file not found: {p}")
        self.path = p

        self.db = kuzu.Database(str(p))
        self.conn = kuzu.Connection(self.db)

    def fetch(self, query: str) -> list[dict[str, Any]]:
        res = self.conn.execute(query)
        cols = res.get_column_names()
        out: list[dict[str, Any]] = []
        while res.has_next():
            row = res.get_next()
            out.append({cols[i]: row[i] for i in range(len(cols))})
        return out

    def get_tasks(self, limit: int = 250) -> list[GraphTask]:
        rows = self.fetch(
            f"""
            MATCH (t:Node)
            WHERE t.type='Task'
            RETURN t.id AS id, t.name AS name, t.properties AS props
            LIMIT {int(limit)}
            """
        )
        out: list[GraphTask] = []
        for r in rows:
            props_raw = r.get("props") or ""
            props_text = props_raw if isinstance(props_raw, str) else json.dumps(props_raw, ensure_ascii=False)
            props = _safe_json(props_raw)
            tid = props.get("task_id")
            try:
                tid = int(tid)
            except Exception:
                continue
            title = str(props.get("title") or props.get("name") or r.get("name") or f"Task #{tid}")
            url = str(props.get("url") or props.get("link") or task_url(tid))
            out.append(
                GraphTask(
                    task_id=tid,
                    node_id=str(r.get("id")),
                    title=title,
                    url=url,
                    props_text=props_text.lower(),
                )
            )
        return out

    def get_latest_snapshot_id(self, task_node_id: str) -> Optional[str]:
        rows = self.fetch(
            f"""
            MATCH (t:Node)-[e]->(s:Node)
            WHERE t.id='{task_node_id}'
              AND e.relationship_name='has_snapshot'
              AND s.type='Snapshot'
            RETURN s.id AS sid, s.properties AS props
            LIMIT 50
            """
        )
        if not rows:
            return None

        best_sid: Optional[str] = None
        best_ts = -1
        for r in rows:
            sid = str(r.get("sid"))
            props = _safe_json(r.get("props"))
            ts = props.get("updated_at_ts")
            try:
                ts_i = int(ts)
            except Exception:
                ts_i = -1
            if ts_i > best_ts:
                best_ts = ts_i
                best_sid = sid
        return best_sid or str(rows[0].get("sid"))

    def get_snapshot_items(self, snapshot_id: str, limit: int = 220) -> list[dict[str, Any]]:
        rows = self.fetch(
            f"""
            MATCH (s:Node)-[e1]->(i:Node)
            WHERE s.id='{snapshot_id}'
              AND e1.relationship_name='has_item'
              AND i.type='Item'
            OPTIONAL MATCH (i)-[e2]->(sc:Node)
            WHERE e2.relationship_name='in_scope'
              AND sc.type='Scope'
            RETURN i.id AS iid,
                   i.properties AS iprops,
                   sc.name AS scope_name,
                   sc.properties AS scprops
            LIMIT {int(limit)}
            """
        )
        out: list[dict[str, Any]] = []
        for r in rows:
            ip = _safe_json(r.get("iprops"))
            scp = _safe_json(r.get("scprops"))
            scope_name = r.get("scope_name")
            scope = ""
            if isinstance(scope_name, str):
                scope = scope_name.strip()
            if not scope:
                scope = str(scp.get("scope") or scp.get("name") or "").strip()

            out.append(
                {
                    "sku": ip.get("sku"),
                    "manufacturer": ip.get("manufacturer"),
                    "desc": ip.get("desc") or ip.get("description") or ip.get("name"),
                    "unit_price_rub": ip.get("unit_price_rub"),
                    "scope": scope or "misc",
                }
            )
        return out


def _pick_best_graph_tasks(tasks: list[GraphTask], query: str, k: int = 12) -> list[GraphTask]:
    toks = [t for t in re.split(r"[^\wА-Яа-я]+", (query or "").lower()) if len(t) >= 4][:10]
    if not toks:
        return tasks[:k]
    scored: list[tuple[int, GraphTask]] = []
    for t in tasks:
        score = sum(1 for x in toks if x in t.props_text)
        scored.append((score, t))
    scored.sort(key=lambda x: x[0], reverse=True)
    best = [t for s, t in scored if s > 0][:k]
    if not best:
        best = [t for _, t in scored][:k]
    return best


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
            print("[debug] scope_whitelist:", sorted(whitelist_set))

    items: list[CandidateItem] = []
    for gt in picked:
        sid = g.get_latest_snapshot_id(gt.node_id)
        if not sid:
            if debug:
                print("[debug] task", gt.task_id, "no snapshot")
            continue

        rows = g.get_snapshot_items(sid, limit=220)
        if debug:
            print("[debug] task", gt.task_id, "snap", sid, "items", len(rows))

        for idx, it in enumerate(rows):
            scope = (str(it.get("scope") or "").strip() or "misc")
            if whitelist_set and scope not in whitelist_set:
                continue

            sku = (str(it.get("sku") or "").strip() or None)
            mfr = (str(it.get("manufacturer") or "").strip() or None)
            desc = (str(it.get("desc") or "").strip())
            price = _to_decimal(it.get("unit_price_rub"))

            key = sku or f"row{idx}"
            cid = f"ci_graph_{gt.task_id}_{key}"
            name = desc or sku or "Позиция"

            items.append(
                CandidateItem(
                    candidate_id=cid,
                    category=scope,
                    sku=sku,
                    manufacturer=mfr,
                    model=None,
                    name=name,
                    description=desc or name,
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
