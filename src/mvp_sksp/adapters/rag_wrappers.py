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


_REL_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


def _resolve_kuzu_db_path() -> Path:
    """
    IMPORTANT: in your environment Kuzu expects a FILE path (not a directory).

    Priority:
      1) KUZU_DB_PATH (file)
      2) SYSTEM_ROOT_DIRECTORY/.cognee_system/databases/cognee_graph_kuzu (file) if needed
      3) SYSTEM_ROOT_DIRECTORY/databases/cognee_graph_kuzu if SYSTEM_ROOT_DIRECTORY already is .cognee_system
    """
    _load_env()

    explicit = (os.getenv("KUZU_DB_PATH") or "").strip()
    if explicit:
        p = Path(explicit).expanduser().resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    sys_root = (os.getenv("SYSTEM_ROOT_DIRECTORY") or "").strip()
    if not sys_root:
        raise RuntimeError("KUZU_DB_PATH is empty and SYSTEM_ROOT_DIRECTORY is not set")

    sp = Path(sys_root).expanduser().resolve()
    if sp.name == ".cognee_system":
        p = sp / "databases" / "cognee_graph_kuzu"
    else:
        p = sp / ".cognee_system" / "databases" / "cognee_graph_kuzu"

    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _show_tables(conn: kuzu.Connection) -> set[str]:
    try:
        res = conn.execute("CALL show_tables() RETURN *")
    except Exception:
        return set()

    cols = list(res.get_column_names())
    name_idx = None
    for i, c in enumerate(cols):
        if str(c).lower() in {"name", "table_name", "tablename"}:
            name_idx = i
            break
    if name_idx is None:
        return set()

    names: set[str] = set()
    while res.has_next():
        row = res.get_next()
        try:
            names.add(str(row[name_idx]))
        except Exception:
            continue
    return names


def _ensure_schema(conn: kuzu.Connection) -> None:
    names = _show_tables(conn)

    if "Node" not in names:
        try:
            conn.execute(
                """
                CREATE NODE TABLE Node(
                  id STRING,
                  type STRING,
                  name STRING,
                  properties STRING,
                  PRIMARY KEY(id)
                )
                """
            )
        except RuntimeError as e:
            if "already exists" not in str(e).lower():
                raise

    names = _show_tables(conn)

    if "EDGE" not in names:
        try:
            conn.execute("CREATE REL TABLE EDGE(FROM Node TO Node)")
        except RuntimeError as e:
            if "already exists" not in str(e).lower():
                raise


@dataclass(frozen=True)
class GraphTask:
    task_id: int
    title: str
    url: str
    index_text: str


class KuzuGraph:
    def __init__(self) -> None:
        self.db_path = _resolve_kuzu_db_path()
        self.db = kuzu.Database(str(self.db_path))
        self.conn = kuzu.Connection(self.db)
        _ensure_schema(self.conn)
        self.rel_tables = self._discover_relationship_tables()

    def _query(self, cypher: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        res = self.conn.execute(cypher, params or {})
        cols = list(res.get_column_names())
        out: list[dict[str, Any]] = []
        while res.has_next():
            row = res.get_next()
            out.append(dict(zip(cols, row)))
        return out

    def _discover_relationship_tables(self) -> list[str]:
        names = _show_tables(self.conn)
        rels = []
        for n in names:
            if _REL_NAME_RE.match(n):
                # in your schema only EDGE exists, but keep it generic
                if n.upper() in {"EDGE", "HAS", "CONTAINS", "LINKS", "REL"} or n == "EDGE":
                    rels.append(n)
        if "EDGE" in names and "EDGE" not in rels:
            rels.insert(0, "EDGE")
        return rels or ["EDGE"]

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
            tid = None
            try:
                tid = int(props.get("task_id")) if props.get("task_id") else None
            except Exception:
                tid = None
            if tid is None:
                # try parse id like task:80330
                rid = str(r.get("id") or "")
                if rid.startswith("task:"):
                    try:
                        tid = int(rid.split(":", 1)[1])
                    except Exception:
                        tid = 0
                else:
                    tid = 0

            title = props.get("title") or r.get("name") or f"Task {tid}"
            idx = str(props.get("index_text") or "")
            out.append(GraphTask(task_id=int(tid), title=str(title), url=task_url(int(tid)), index_text=idx))
        return out

    def _try_rel_match(self, src_type: str, dst_type: str, src_contains: str, limit: int) -> list[dict[str, Any]]:
        for rel in self.rel_tables:
            if not _REL_NAME_RE.match(rel):
                continue

            rows = self._query(
                f"""
                MATCH (a:Node)-[:{rel}]->(b:Node)
                WHERE a.type=$src_type AND b.type=$dst_type AND a.properties CONTAINS $needle
                RETURN b.id AS id, b.name AS name, b.properties AS props
                LIMIT $limit
                """,
                {"src_type": src_type, "dst_type": dst_type, "needle": src_contains, "limit": int(limit)},
            )
            if rows:
                return rows

            rows = self._query(
                f"""
                MATCH (b:Node)-[:{rel}]->(a:Node)
                WHERE a.type=$src_type AND b.type=$dst_type AND a.properties CONTAINS $needle
                RETURN b.id AS id, b.name AS name, b.properties AS props
                LIMIT $limit
                """,
                {"src_type": src_type, "dst_type": dst_type, "needle": src_contains, "limit": int(limit)},
            )
            if rows:
                return rows
        return []

    def get_snapshots_for_task(self, task_id: int, limit: int = 40) -> list[dict[str, Any]]:
        tid = str(task_id)
        rows = self._try_rel_match("Task", "Snapshot", tid, limit)
        if rows:
            return rows

        # fallback: by snapshot properties containing task id
        return self._query(
            """
            MATCH (s:Node)
            WHERE s.type='Snapshot' AND s.properties CONTAINS $tid
            RETURN s.id AS id, s.name AS name, s.properties AS props
            LIMIT $limit
            """,
            {"tid": tid, "limit": int(limit)},
        )

    def get_items_for_snapshot(self, snapshot_id: str, task_id: int | None = None, limit: int = 800) -> list[dict[str, Any]]:
        sid = str(snapshot_id)
        rows = self._try_rel_match("Snapshot", "Item", sid, limit)
        if rows:
            return rows

        # fallback: by item properties containing snapshot id
        rows = self._query(
            """
            MATCH (i:Node)
            WHERE i.type='Item' AND i.properties CONTAINS $sid
            RETURN i.id AS id, i.name AS name, i.properties AS props
            LIMIT $limit
            """,
            {"sid": sid, "limit": int(limit)},
        )
        if rows:
            return rows

        # last fallback: by task id
        if task_id is not None:
            return self._query(
                """
                MATCH (i:Node)
                WHERE i.type='Item' AND i.properties CONTAINS $tid
                RETURN i.id AS id, i.name AS name, i.properties AS props
                LIMIT $limit
                """,
                {"tid": str(task_id), "limit": int(limit)},
            )
        return []


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
    """Global-ish retrieval from Kuzu (for older pipeline paths).

    NOTE: deal-aware retrieval lives in deal_kuzu_retriever.py.
    This function exists for compatibility and "global fallback".
    """
    _load_env()
    whitelist_set = set(scope_whitelist) if scope_whitelist else None

    g = KuzuGraph()
    graph_tasks = g.get_tasks(limit=250)
    picked = _pick_best_graph_tasks(graph_tasks, text, k=12)

    items: list[CandidateItem] = []
    for gt in picked:
        snaps = g.get_snapshots_for_task(gt.task_id, limit=40)
        for s in snaps:
            sid = str(s.get("id"))
            sprops = _safe_json(s.get("props"))
            scope = (sprops.get("scope") or sprops.get("category") or "unknown").strip()
            if whitelist_set and scope not in whitelist_set:
                continue

            snap_items = g.get_items_for_snapshot(sid, task_id=gt.task_id, limit=2000)
            for it in snap_items:
                props = _safe_json(it.get("props"))
                sku = props.get("sku") or props.get("article") or props.get("Артикул") or props.get("арт") or None
                mfr = props.get("manufacturer") or props.get("vendor") or None
                name = str(props.get("name") or it.get("name") or "")
                desc = str(props.get("desc") or props.get("description") or "")

                price = _to_decimal(props.get("unit_price_rub") or props.get("price") or props.get("unit_price"))

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
                        price_source=str(props.get("price_source") or "kuzu_graph"),
                        evidence_task_ids=[gt.task_id],
                        meta={"task_id": gt.task_id, "snapshot_id": sid},
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