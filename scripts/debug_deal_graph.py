from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import kuzu
from dotenv import load_dotenv  # type: ignore
from sqlalchemy import create_engine, text


def _safe_json(v: Any) -> dict[str, Any]:
    if isinstance(v, dict):
        return v
    if isinstance(v, str) and v.strip():
        try:
            return json.loads(v)
        except Exception:
            return {}
    return {}


def _query(conn: kuzu.Connection, cypher: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    res = conn.execute(cypher, params)
    cols = list(res.get_column_names())
    out: list[dict[str, Any]] = []
    while res.has_next():
        row = res.get_next()
        out.append(dict(zip(cols, row)))
    return out


def _kuzu_conn_from_env() -> tuple[kuzu.Connection, Path]:
    load_dotenv(".env", override=False)
    sys_root = (os.getenv("SYSTEM_ROOT_DIRECTORY") or "").strip()
    if not sys_root:
        raise SystemExit("SYSTEM_ROOT_DIRECTORY is not set")
    db_path = Path(sys_root) / "databases" / "cognee_graph_kuzu"
    db = kuzu.Database(str(db_path))
    return kuzu.Connection(db), db_path


def _task_ids_for_deal(engine, deal_id: str) -> list[int]:
    q = text(
        """
        SELECT id
        FROM tasks
        WHERE raw::text ILIKE :dtag
           OR title ILIKE :dlike
        ORDER BY id DESC
        """
    )
    with engine.connect() as c:
        rows = c.execute(q, {"dtag": f"%D_{deal_id}%", "dlike": f"%{deal_id}%"}).mappings().all()
    out = []
    for r in rows:
        try:
            out.append(int(r["id"]))
        except Exception:
            pass
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--deal-id", required=True)
    ap.add_argument("--max-items", type=int, default=80)
    args = ap.parse_args()

    dsn = os.environ.get("DB_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DB_DSN/DATABASE_URL empty (set -a; source .env; set +a)")
    eng = create_engine(dsn, future=True, pool_pre_ping=True)

    conn, db_path = _kuzu_conn_from_env()
    print("kuzu_db_path:", db_path)

    task_ids = _task_ids_for_deal(eng, args.deal_id)
    print("deal_id:", args.deal_id, "task_ids:", task_ids)

    for tid in task_ids:
        task_nodes = _query(
            conn,
            """
            MATCH (t:Node)
            WHERE t.type='Task' AND t.properties CONTAINS $tid
            RETURN t.id AS id, left(t.properties, 400) AS props
            LIMIT 5
            """,
            {"tid": str(tid)},
        )
        print("\n--- task_id", tid, "task_nodes:", len(task_nodes), "---")
        for tn in task_nodes:
            tnode_id = tn["id"]
            print("TaskNode:", tnode_id, "props:", tn["props"])

            snaps = _query(
                conn,
                """
                MATCH (a:Node)-[:EDGE]->(b:Node)
                WHERE a.id=$id AND b.type='Snapshot'
                RETURN b.id AS sid
                LIMIT 30
                """,
                {"id": tnode_id},
            )
            if not snaps:
                snaps = _query(
                    conn,
                    """
                    MATCH (b:Node)-[:EDGE]->(a:Node)
                    WHERE a.id=$id AND b.type='Snapshot'
                    RETURN b.id AS sid
                    LIMIT 30
                    """,
                    {"id": tnode_id},
                )
            print("snapshots:", len(snaps))

            shown = 0
            for s in snaps:
                sid = s["sid"]
                items = _query(
                    conn,
                    """
                    MATCH (s:Node)-[:EDGE]->(i:Node)
                    WHERE s.id=$sid AND i.type='Item'
                    RETURN left(i.properties, 800) AS props
                    LIMIT $lim
                    """,
                    {"sid": sid, "lim": int(args.max_items)},
                )
                if not items:
                    items = _query(
                        conn,
                        """
                        MATCH (i:Node)-[:EDGE]->(s:Node)
                        WHERE s.id=$sid AND i.type='Item'
                        RETURN left(i.properties, 800) AS props
                        LIMIT $lim
                        """,
                        {"sid": sid, "lim": int(args.max_items)},
                    )

                if items:
                    print("snapshot", sid, "items:", len(items))
                    for it in items[:10]:
                        props = _safe_json(it["props"])
                        print("  sku:", props.get("sku"), "mfr:", props.get("manufacturer"), "desc_head:", str(props.get("desc") or "")[:80])
                        shown += 1
                        if shown >= 12:
                            return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())