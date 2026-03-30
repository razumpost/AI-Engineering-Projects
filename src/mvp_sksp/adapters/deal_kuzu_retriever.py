from __future__ import annotations

import json
import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import kuzu
from dotenv import load_dotenv  # type: ignore

from ..domain.candidates import CandidateItem, CandidatePool, CandidateTask


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
        s = str(v).strip().replace("\u00a0", " ").replace(" ", "").replace(",", ".")
        return Decimal(s)
    except Exception:
        return None


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
class KuzuItemRow:
    node_id: str
    props: dict[str, Any]


class KuzuDealRetriever:
    """Deal-aware KB retriever based on Kuzu (Node + EDGE).

    IMPORTANT: in your setup KUZU_DB_PATH points to a FILE (not a directory).
    """

    def __init__(self, kuzu_db_path: Optional[str] = None) -> None:
        _load_env()

        explicit = (kuzu_db_path or os.getenv("KUZU_DB_PATH") or "").strip()
        if explicit:
            self.db_path = Path(explicit).expanduser().resolve()
        else:
            sys_root = (os.getenv("SYSTEM_ROOT_DIRECTORY") or "").strip()
            if not sys_root:
                raise RuntimeError("SYSTEM_ROOT_DIRECTORY is not set and KUZU_DB_PATH is empty")
            self.db_path = (Path(sys_root) / "databases" / "cognee_graph_kuzu").resolve()

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.db = kuzu.Database(str(self.db_path))
        self.conn = kuzu.Connection(self.db)
        _ensure_schema(self.conn)

    def _query(self, cypher: str, params: Optional[dict[str, Any]] = None) -> list[dict[str, Any]]:
        res = self.conn.execute(cypher, params or {})
        cols = list(res.get_column_names())
        out: list[dict[str, Any]] = []
        while res.has_next():
            row = res.get_next()
            out.append(dict(zip(cols, row)))
        return out

    def _neighbors_by_edge(self, node_id: str, want_type: str, limit: int) -> list[str]:
        out: list[str] = []

        rows1 = self._query(
            """
            MATCH (a:Node)-[:EDGE]->(b:Node)
            WHERE a.id=$id AND b.type=$t
            RETURN b.id AS id
            LIMIT $limit
            """,
            {"id": node_id, "t": want_type, "limit": int(limit)},
        )
        out += [str(r["id"]) for r in rows1 if r.get("id")]

        rows2 = self._query(
            """
            MATCH (b:Node)-[:EDGE]->(a:Node)
            WHERE a.id=$id AND b.type=$t
            RETURN b.id AS id
            LIMIT $limit
            """,
            {"id": node_id, "t": want_type, "limit": int(limit)},
        )
        out += [str(r["id"]) for r in rows2 if r.get("id")]

        seen = set()
        uniq: list[str] = []
        for x in out:
            if x not in seen:
                seen.add(x)
                uniq.append(x)
        return uniq

    def _items_for_snapshot(self, snapshot_id: str, limit: int) -> list[KuzuItemRow]:
        props_list: list[KuzuItemRow] = []

        rows1 = self._query(
            """
            MATCH (s:Node)-[:EDGE]->(i:Node)
            WHERE s.id=$sid AND i.type='Item'
            RETURN i.id AS id, i.properties AS props
            LIMIT $limit
            """,
            {"sid": snapshot_id, "limit": int(limit)},
        )
        for r in rows1:
            props_list.append(KuzuItemRow(node_id=str(r["id"]), props=_safe_json(r.get("props"))))

        rows2 = self._query(
            """
            MATCH (i:Node)-[:EDGE]->(s:Node)
            WHERE s.id=$sid AND i.type='Item'
            RETURN i.id AS id, i.properties AS props
            LIMIT $limit
            """,
            {"sid": snapshot_id, "limit": int(limit)},
        )
        for r in rows2:
            props_list.append(KuzuItemRow(node_id=str(r["id"]), props=_safe_json(r.get("props"))))

        return props_list

    def _sksp_snapshot_ids_for_deal(self, deal_id: str, limit: int = 80) -> list[str]:
        prefix = f"sksp_snapshot:{deal_id}:"
        rows = self._query(
            """
            MATCH (s:Node)
            WHERE s.type='Snapshot' AND s.id CONTAINS $pfx
            RETURN s.id AS id
            LIMIT $limit
            """,
            {"pfx": prefix, "limit": int(limit)},
        )
        return [str(r["id"]) for r in rows if r.get("id")]

    def _task_node_ids(self, task_id: int, limit: int = 10) -> list[str]:
        out: list[str] = []

        tid_node = f"task:{task_id}"
        rows = self._query(
            """
            MATCH (t:Node)
            WHERE t.id=$id
            RETURN t.id AS id
            LIMIT 1
            """,
            {"id": tid_node},
        )
        out += [str(r["id"]) for r in rows if r.get("id")]

        rows2 = self._query(
            """
            MATCH (t:Node)
            WHERE t.type='Task' AND t.properties CONTAINS $tid
            RETURN t.id AS id
            LIMIT $limit
            """,
            {"tid": str(task_id), "limit": int(limit)},
        )
        out += [str(r["id"]) for r in rows2 if r.get("id")]

        seen = set()
        uniq: list[str] = []
        for x in out:
            if x not in seen:
                seen.add(x)
                uniq.append(x)
        return uniq

    def _candidate_from_item(
        self,
        item: KuzuItemRow,
        *,
        category: str,
        evidence_task_ids: list[int],
        default_price_source: str,
    ) -> CandidateItem:
        p = item.props or {}
        sku = p.get("sku") or p.get("SKU") or p.get("article") or p.get("Артикул") or p.get("арт")
        manufacturer = p.get("manufacturer") or p.get("vendor") or p.get("brand")
        desc = p.get("desc") or p.get("description") or p.get("name") or ""
        name = p.get("name") or p.get("title") or (desc[:80] if desc else "") or "UNKNOWN"
        unit_price = _to_decimal(p.get("unit_price_rub") or p.get("price") or p.get("unit_price"))

        return CandidateItem(
            candidate_id=f"kuzu:{item.node_id}",
            category=category,
            sku=str(sku).strip() if sku else None,
            manufacturer=str(manufacturer).strip() if manufacturer else None,
            model=None,
            name=str(name),
            description=str(desc),
            unit_price_rub=unit_price,
            price_source=str(p.get("price_source") or default_price_source),
            evidence_task_ids=list(evidence_task_ids),
            meta={"kuzu_node_id": item.node_id, "props": p},
        )

    def retrieve_for_deal(
        self,
        deal_id: str,
        *,
        tasks: list[CandidateTask],
        limit_items_per_snapshot: int = 10000,
    ) -> CandidatePool:
        items: list[CandidateItem] = []
        task_ids = [t.task_id for t in tasks]

        snap_ids = self._sksp_snapshot_ids_for_deal(deal_id, limit=80)
        if snap_ids:
            for sid in snap_ids:
                for it in self._items_for_snapshot(sid, limit=int(limit_items_per_snapshot)):
                    items.append(
                        self._candidate_from_item(
                            it,
                            category="sksp",
                            evidence_task_ids=task_ids,
                            default_price_source="sksp_xlsx_ingest",
                        )
                    )
            return CandidatePool(items=items, tasks=tasks)

        for t in tasks:
            for tid_node in self._task_node_ids(t.task_id):
                snap_ids2 = self._neighbors_by_edge(tid_node, want_type="Snapshot", limit=120)
                for sid in snap_ids2:
                    for it in self._items_for_snapshot(sid, limit=int(limit_items_per_snapshot)):
                        items.append(
                            self._candidate_from_item(
                                it,
                                category="kuzu_graph",
                                evidence_task_ids=[t.task_id],
                                default_price_source="kuzu_graph",
                            )
                        )

        return CandidatePool(items=items, tasks=tasks)