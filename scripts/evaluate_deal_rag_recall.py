from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
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


def _norm_sku(s: str | None) -> str | None:
    if not s:
        return None
    t = s.strip().replace(" ", "")
    return t.casefold() if t else None


def _safe_json(v: Any) -> dict[str, Any]:
    if isinstance(v, dict):
        return v
    if isinstance(v, str) and v.strip():
        try:
            return json.loads(v)
        except Exception:
            return {}
    return {}


def _collect_gt_skus(items: list[dict[str, Any]]) -> dict[str, set[str]]:
    gt: dict[str, set[str]] = defaultdict(set)
    for it in items:
        deal_id = str(it.get("deal_id") or "").strip()
        sku = _norm_sku(str(it.get("sku") or "").strip())
        if deal_id and sku:
            gt[deal_id].add(sku)
    return gt


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


def _kuzu_conn_from_env() -> tuple[kuzu.Connection, Path]:
    load_dotenv(".env", override=False)
    sys_root = (os.getenv("SYSTEM_ROOT_DIRECTORY") or "").strip()
    if not sys_root:
        raise SystemExit("SYSTEM_ROOT_DIRECTORY is not set")
    db_path = Path(sys_root) / "databases" / "cognee_graph_kuzu"
    db = kuzu.Database(str(db_path))
    return kuzu.Connection(db), db_path


def _query(conn: kuzu.Connection, cypher: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    res = conn.execute(cypher, params)
    cols = list(res.get_column_names())
    out: list[dict[str, Any]] = []
    while res.has_next():
        row = res.get_next()
        out.append(dict(zip(cols, row)))
    return out


def _extract_sku_from_props(props: dict[str, Any]) -> str | None:
    for key in ("sku", "SKU", "article", "Артикул", "арт", "artikul", "partnumber", "pn"):
        if key in props and props[key]:
            return str(props[key])
    return None


def _find_task_node_ids(conn: kuzu.Connection, task_id: int, limit: int = 10) -> list[str]:
    # Task Node может хранить task_id внутри properties
    rows = _query(
        conn,
        """
        MATCH (t:Node)
        WHERE t.type='Task' AND t.properties CONTAINS $tid
        RETURN t.id AS id
        LIMIT $limit
        """,
        {"tid": str(task_id), "limit": int(limit)},
    )
    return [str(r["id"]) for r in rows if r.get("id")]


def _neighbors_by_edge(conn: kuzu.Connection, node_id: str, want_type: str, limit: int) -> list[str]:
    # Пробуем обе стороны, потому что направление EDGE может быть любым
    out: list[str] = []

    rows1 = _query(
        conn,
        """
        MATCH (a:Node)-[:EDGE]->(b:Node)
        WHERE a.id=$id AND b.type=$t
        RETURN b.id AS id
        LIMIT $limit
        """,
        {"id": node_id, "t": want_type, "limit": int(limit)},
    )
    out += [str(r["id"]) for r in rows1 if r.get("id")]

    rows2 = _query(
        conn,
        """
        MATCH (b:Node)-[:EDGE]->(a:Node)
        WHERE a.id=$id AND b.type=$t
        RETURN b.id AS id
        LIMIT $limit
        """,
        {"id": node_id, "t": want_type, "limit": int(limit)},
    )
    out += [str(r["id"]) for r in rows2 if r.get("id")]

    # unique preserve order
    seen = set()
    uniq: list[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def _items_props_for_snapshot(conn: kuzu.Connection, snapshot_id: str, limit: int) -> list[dict[str, Any]]:
    props_list: list[dict[str, Any]] = []

    rows1 = _query(
        conn,
        """
        MATCH (s:Node)-[:EDGE]->(i:Node)
        WHERE s.id=$sid AND i.type='Item'
        RETURN i.properties AS props
        LIMIT $limit
        """,
        {"sid": snapshot_id, "limit": int(limit)},
    )
    props_list += [_safe_json(r.get("props")) for r in rows1]

    rows2 = _query(
        conn,
        """
        MATCH (i:Node)-[:EDGE]->(s:Node)
        WHERE s.id=$sid AND i.type='Item'
        RETURN i.properties AS props
        LIMIT $limit
        """,
        {"sid": snapshot_id, "limit": int(limit)},
    )
    props_list += [_safe_json(r.get("props")) for r in rows2]

    return props_list


def _skus_from_kuzu_for_deal(conn: kuzu.Connection, task_ids: list[int], limit_items_per_snap: int = 4000) -> set[str]:
    skus: set[str] = set()

    for tid in task_ids:
        task_nodes = _find_task_node_ids(conn, tid, limit=10)
        for tn in task_nodes:
            snap_ids = _neighbors_by_edge(conn, tn, want_type="Snapshot", limit=60)
            for sid in snap_ids:
                props_list = _items_props_for_snapshot(conn, sid, limit=int(limit_items_per_snap))
                for props in props_list:
                    sku = _extract_sku_from_props(props)
                    ns = _norm_sku(sku) if sku else None
                    if ns and ns != "-" and ns != "—":
                        skus.add(ns)

    return skus


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs", required=True)
    ap.add_argument("--items", required=True)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--limit-items-per-snapshot", type=int, default=4000)
    args = ap.parse_args()

    dsn = os.environ.get("DB_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DB_DSN/DATABASE_URL empty (set -a; source .env; set +a)")

    eng = create_engine(dsn, future=True, pool_pre_ping=True)
    conn, db_path = _kuzu_conn_from_env()
    print("kuzu_db_path:", db_path)

    pairs = _read_jsonl(Path(args.pairs).expanduser().resolve())
    items = _read_jsonl(Path(args.items).expanduser().resolve())
    gt = _collect_gt_skus(items)

    results: list[dict[str, Any]] = []
    for p in pairs:
        deal_id = str(p.get("deal_id") or "").strip()
        if not deal_id:
            continue

        gt_skus = gt.get(deal_id, set())
        if not gt_skus:
            continue

        task_ids = _task_ids_for_deal(eng, deal_id)
        if not task_ids:
            continue

        retrieved = _skus_from_kuzu_for_deal(
            conn, task_ids, limit_items_per_snap=int(args.limit_items_per_snapshot)
        )
        hit = len(gt_skus & retrieved)
        rec = hit / max(1, len(gt_skus))

        results.append({"deal_id": deal_id, "gt_cnt": len(gt_skus), "task_cnt": len(task_ids), "hit_cnt": hit, "recall": rec})

        if args.limit and len(results) >= int(args.limit):
            break

    if not results:
        print("no results")
        return 0

    avg = sum(r["recall"] for r in results) / len(results)
    results.sort(key=lambda r: r["recall"])

    print(f"deals_eval={len(results)} deal_routed_recall avg={avg:.3f}")
    print("worst 10:")
    for r in results[:10]:
        print(json.dumps(r, ensure_ascii=False))
    print("best 5:")
    for r in results[-5:]:
        print(json.dumps(r, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())