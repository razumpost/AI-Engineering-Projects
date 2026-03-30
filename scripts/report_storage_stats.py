from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import kuzu
from dotenv import load_dotenv  # type: ignore
from sqlalchemy import create_engine, text


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _du_bytes(path: Path) -> int:
    # GNU coreutils: du -sb
    try:
        out = subprocess.check_output(["du", "-sb", str(path)], text=True).strip()
        return int(out.split()[0])
    except Exception:
        # fallback: du -sk
        out = subprocess.check_output(["du", "-sk", str(path)], text=True).strip()
        return int(out.split()[0]) * 1024


def _pg_one(eng, sql: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    with eng.connect() as c:
        row = c.execute(text(sql), params or {}).mappings().first()
    return dict(row) if row else {}


def _pg_all(eng, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    with eng.connect() as c:
        rows = c.execute(text(sql), params or {}).mappings().all()
    return [dict(r) for r in rows]


def _kuzu_stats(db_path: Path) -> dict[str, Any]:
    db = kuzu.Database(str(db_path))
    conn = kuzu.Connection(db)

    res = conn.execute("MATCH (n:Node) RETURN count(*) AS cnt")
    nodes = int(res.get_next()[0])

    res = conn.execute("MATCH ()-[r]->() RETURN count(*) AS cnt")
    edges = int(res.get_next()[0])

    types: list[dict[str, Any]] = []
    res = conn.execute("MATCH (n:Node) RETURN n.type AS type, count(*) AS cnt ORDER BY cnt DESC LIMIT 50")
    while res.has_next():
        t, cnt = res.get_next()
        types.append({"type": t, "cnt": int(cnt)})

    return {"nodes": nodes, "edges": edges, "types": types}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./tmp/storage_report.json")
    args = ap.parse_args()

    load_dotenv(".env", override=False)

    db_dsn = _env("DB_DSN") or _env("DATABASE_URL")
    if not db_dsn:
        raise SystemExit("DB_DSN/DATABASE_URL empty (set -a; source .env; set +a)")

    sys_root = _env("SYSTEM_ROOT_DIRECTORY")
    if not sys_root:
        raise SystemExit("SYSTEM_ROOT_DIRECTORY empty")

    collector_root = Path(sys_root).expanduser().resolve().parent
    downloads_dir = collector_root / (_env("DOWNLOAD_DIR", "downloads"))
    cognee_dir = Path(sys_root).expanduser().resolve()
    kuzu_dir = cognee_dir / "databases" / "cognee_graph_kuzu"

    eng = create_engine(db_dsn, future=True, pool_pre_ping=True)

    report: dict[str, Any] = {"postgres": {}, "kuzu": {}, "disk": {}}

    # Postgres sizes
    report["postgres"]["db"] = _pg_one(
        eng,
        "SELECT current_database() AS db, pg_database_size(current_database()) AS bytes",
    )

    report["postgres"]["tables_top"] = _pg_all(
        eng,
        """
        SELECT
          relname AS table,
          pg_total_relation_size(relid) AS total_bytes,
          pg_relation_size(relid) AS data_bytes,
          pg_indexes_size(relid) AS index_bytes,
          (pg_total_relation_size(relid) - pg_relation_size(relid) - pg_indexes_size(relid)) AS toast_bytes,
          n_live_tup
        FROM pg_stat_user_tables
        ORDER BY pg_total_relation_size(relid) DESC
        LIMIT 30
        """,
    )

    # Key table completeness
    report["postgres"]["rag_chunks_completeness"] = _pg_one(
        eng,
        """
        SELECT
          count(*) AS rows,
          count(content) AS content_not_null,
          round(100.0*count(content)/nullif(count(*),0),2) AS content_pct,
          count(meta) AS meta_not_null,
          round(100.0*count(meta)/nullif(count(*),0),2) AS meta_pct,
          count(nullif(meta::jsonb->>'path','')) AS path_not_empty,
          count(nullif(meta::jsonb->>'deal_id','')) AS deal_id_not_empty,
          count(nullif(meta::jsonb->>'activity_id','')) AS activity_id_not_empty,
          count(*) FILTER (WHERE (meta::jsonb->>'path') ILIKE '%calls_transcripts%') AS call_chunks
        FROM rag_chunks
        """,
    )

    report["postgres"]["files_completeness"] = _pg_one(
        eng,
        """
        SELECT
          count(*) AS files,
          count(name) AS name_not_null,
          count(local_path) AS local_path_not_null,
          count(download_url) AS download_url_not_null
        FROM files
        """,
    )

    report["postgres"]["tasks_completeness"] = _pg_one(
        eng,
        """
        SELECT
          count(*) AS tasks,
          count(title) AS title_not_null,
          count(raw) AS raw_not_null,
          count(*) FILTER (WHERE raw::text ILIKE '%D_%') AS tasks_with_deal_tag
        FROM tasks
        """,
    )

    # vectorization footprint
    report["postgres"]["vector_tables"] = {}
    for tname in ["rag_embeddings", "rag_chunks", "rag_documents", "rag_engrams", "rag_engram_refs"]:
        try:
            report["postgres"]["vector_tables"][tname] = _pg_one(
                eng,
                f"""
                SELECT
                  '{tname}' AS table,
                  pg_total_relation_size('{tname}'::regclass) AS total_bytes,
                  pg_relation_size('{tname}'::regclass) AS data_bytes,
                  pg_indexes_size('{tname}'::regclass) AS index_bytes
                """,
            )
        except Exception as e:
            report["postgres"]["vector_tables"][tname] = {"table": tname, "error": str(e)}

    # Kuzu stats + disk sizes
    report["kuzu"]["path"] = str(kuzu_dir)
    report["kuzu"]["disk_bytes"] = _du_bytes(kuzu_dir) if kuzu_dir.exists() else 0
    report["kuzu"]["graph"] = _kuzu_stats(kuzu_dir) if kuzu_dir.exists() else {}

    report["disk"]["collector_root"] = str(collector_root)
    report["disk"]["downloads_dir"] = str(downloads_dir)
    report["disk"]["downloads_bytes"] = _du_bytes(downloads_dir) if downloads_dir.exists() else 0
    report["disk"]["cognee_system_dir"] = str(cognee_dir)
    report["disk"]["cognee_system_bytes"] = _du_bytes(cognee_dir) if cognee_dir.exists() else 0

    out_path = Path(args.out).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    print(f"written: {out_path}")
    print("postgres db bytes:", report["postgres"]["db"].get("bytes"))
    print("kuzu disk bytes:", report["kuzu"].get("disk_bytes"))
    print("downloads bytes:", report["disk"].get("downloads_bytes"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())