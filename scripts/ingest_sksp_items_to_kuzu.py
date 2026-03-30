from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import kuzu
from dotenv import load_dotenv  # type: ignore
from sqlalchemy import create_engine, text


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if ln:
                rows.append(json.loads(ln))
    return rows


def _norm_s(s: Any) -> str:
    return str(s or "").strip()


def _node_props(**kwargs: Any) -> str:
    return json.dumps(kwargs, ensure_ascii=False, default=str)


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
            # tolerate "already exists" if racing
            if "already exists" not in str(e).lower():
                raise

    names = _show_tables(conn)

    if "EDGE" not in names:
        try:
            conn.execute("CREATE REL TABLE EDGE(FROM Node TO Node)")
        except RuntimeError as e:
            if "already exists" not in str(e).lower():
                raise


def _kuzu_conn_from_env() -> tuple[kuzu.Connection, Path]:
    load_dotenv(".env", override=False)

    explicit = (os.getenv("KUZU_DB_PATH") or "").strip()
    if explicit:
        db_path = Path(explicit).expanduser().resolve()
    else:
        sys_root = (os.getenv("SYSTEM_ROOT_DIRECTORY") or "").strip()
        if not sys_root:
            raise SystemExit("SYSTEM_ROOT_DIRECTORY is not set and KUZU_DB_PATH is empty")
        db_path = (Path(sys_root) / "databases" / "cognee_graph_kuzu").resolve()

    # IMPORTANT: in your env Kuzu expects a FILE path, not a directory.
    db_path.parent.mkdir(parents=True, exist_ok=True)

    db = kuzu.Database(str(db_path))
    conn = kuzu.Connection(db)
    _ensure_schema(conn)
    return conn, db_path


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

    out: list[int] = []
    for r in rows:
        try:
            out.append(int(r["id"]))
        except Exception:
            pass
    return out


def _merge_node(conn: kuzu.Connection, node_id: str, node_type: str, name: str, props_json: str) -> None:
    conn.execute(
        """
        MERGE (n:Node {id: $id})
        SET n.type = $type,
            n.name = $name,
            n.properties = $props
        """,
        {"id": node_id, "type": node_type, "name": name, "props": props_json},
    )


def _create_edge(conn: kuzu.Connection, src_id: str, dst_id: str) -> None:
    conn.execute(
        """
        MATCH (a:Node {id: $a}), (b:Node {id: $b})
        CREATE (a)-[:EDGE]->(b)
        """,
        {"a": src_id, "b": dst_id},
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--items", required=True, help="tmp/items_all_norm.jsonl")
    ap.add_argument("--limit-deals", type=int, default=0)
    ap.add_argument("--limit-items", type=int, default=0)
    args = ap.parse_args()

    dsn = os.environ.get("DB_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DB_DSN/DATABASE_URL empty (set -a; source .env; set +a)")

    eng = create_engine(dsn, future=True, pool_pre_ping=True)
    conn, db_path = _kuzu_conn_from_env()
    print("kuzu_db_path:", db_path)

    rows = _read_jsonl(Path(args.items).expanduser().resolve())
    if args.limit_items and args.limit_items > 0:
        rows = rows[: int(args.limit_items)]

    by_snapshot: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in rows:
        deal_id = _norm_s(r.get("deal_id"))
        file_id = _norm_s(r.get("sksp_file_id"))
        if not deal_id or not file_id:
            continue
        by_snapshot.setdefault((deal_id, file_id), []).append(r)

    deals = sorted({d for d, _ in by_snapshot.keys()})
    if args.limit_deals and args.limit_deals > 0:
        deals = deals[: int(args.limit_deals)]

    ingested_items = 0
    ingested_snaps = 0
    ingested_tasks = 0
    edges = 0

    for deal_id in deals:
        snap_keys = [k for k in by_snapshot.keys() if k[0] == deal_id]
        task_ids = _task_ids_for_deal(eng, deal_id)
        if not task_ids:
            continue

        task_node_ids: list[str] = []
        for tid in task_ids:
            tid_node = f"task:{tid}"
            task_node_ids.append(tid_node)
            _merge_node(
                conn,
                node_id=tid_node,
                node_type="Task",
                name=f"Task {tid}",
                props_json=_node_props(task_id=tid, deal_id=deal_id, index_text=f"task {tid} deal {deal_id}"),
            )
            ingested_tasks += 1

        for _, file_id in snap_keys:
            snap_id = f"sksp_snapshot:{deal_id}:{file_id}"
            _merge_node(
                conn,
                node_id=snap_id,
                node_type="Snapshot",
                name=f"SKSP Snapshot deal={deal_id} file={file_id}",
                props_json=_node_props(deal_id=deal_id, sksp_file_id=file_id, source="sksp_xlsx"),
            )
            ingested_snaps += 1

            for tid_node in task_node_ids:
                _create_edge(conn, tid_node, snap_id)
                edges += 1

            for r in by_snapshot[(deal_id, file_id)]:
                sheet = _norm_s(r.get("sheet"))
                row = _norm_s(r.get("row"))
                item_id = f"sksp_item:{deal_id}:{file_id}:{sheet}:{row}"

                sku = _norm_s(r.get("sku"))
                vendor = _norm_s(r.get("vendor"))
                desc = _norm_s(r.get("description"))
                name = _norm_s(r.get("name"))
                price = r.get("price")
                qty = r.get("qty")
                url = _norm_s(r.get("url"))

                index_text = " ".join([p for p in [sku, vendor, name, desc] if p]).strip()

                props = _node_props(
                    deal_id=deal_id,
                    sksp_file_id=file_id,
                    sheet=sheet,
                    row=row,
                    sku=sku or None,
                    manufacturer=vendor or None,
                    desc=desc or None,
                    unit_price_rub=float(price) if price not in (None, "") else None,
                    qty=float(qty) if qty not in (None, "") else None,
                    url=url or None,
                    index_text=index_text,
                    price_source="sksp_xlsx_ingest",
                )

                _merge_node(
                    conn,
                    node_id=item_id,
                    node_type="Item",
                    name=name or (desc[:60] if desc else (sku or "Item")),
                    props_json=props,
                )
                ingested_items += 1

                _create_edge(conn, snap_id, item_id)
                edges += 1

    print(f"done: tasks={ingested_tasks} snapshots={ingested_snaps} items={ingested_items} edges_created={edges}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())