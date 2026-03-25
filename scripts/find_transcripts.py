# scripts/find_transcripts.py
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None


DEFAULT_NEEDLES = [
    "расшифров",
    "стенограм",
    "транскрип",
    "transcript",
    "transcription",
    "speech",
    "stt",
    "whisper",
    "deepgram",
    "assembly",
    "recording",
    "call",
    ".vtt",
    ".srt",
    ".mp3",
    ".wav",
    ".m4a",
]

DEFAULT_COL_PATTERNS = [
    "transcript",
    "transcription",
    "stt",
    "speech",
    "whisper",
    "audio",
    "record",
    "call",
    "dialog",
    "message",
    "body",
    "content",
    "text",
    "raw",
    "payload",
    "data",
]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_env() -> None:
    if load_dotenv is None:
        return
    env_path = _repo_root() / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=str(env_path), override=False)


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return (v or default).strip()


def _excerpt(text_value: str, needle: str, radius: int = 140) -> str:
    t = (text_value or "").replace("\n", " ")
    if not t.strip():
        return ""
    low = t.lower()
    idx = low.find((needle or "").lower())
    if idx < 0:
        return t[: radius * 2].strip()
    start = max(0, idx - radius)
    end = min(len(t), idx + len(needle) + radius)
    return t[start:end].strip()


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        return str(obj)


def _quote_ident(ident: str) -> str:
    # minimal safe quoting for identifiers (schema-derived)
    return '"' + ident.replace('"', '""') + '"'


@dataclass(frozen=True)
class PgHit:
    source: str
    table: str
    column: str
    row_id: str
    created_at: str | None
    title: str | None
    excerpt: str


class PostgresSearcher:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def _run(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params or {}).mappings().all()
            return [dict(r) for r in rows]

    def list_tables(self, limit: int = 80) -> list[dict[str, Any]]:
        # pg_stat_user_tables gives rough row counts (fast)
        sql = """
        SELECT
          relname AS table,
          n_live_tup::bigint AS approx_rows,
          last_vacuum::text AS last_vacuum,
          last_analyze::text AS last_analyze
        FROM pg_stat_user_tables
        ORDER BY n_live_tup DESC NULLS LAST
        LIMIT :limit
        """
        return self._run(sql, {"limit": int(limit)})

    def list_columns(self, like_patterns: list[str]) -> list[dict[str, Any]]:
        likes = " OR ".join([f"c.column_name ILIKE :p{i}" for i in range(len(like_patterns))]) or "TRUE"
        params = {f"p{i}": f"%{p}%" for i, p in enumerate(like_patterns)}
        sql = f"""
        SELECT
          c.table_name,
          c.column_name,
          c.data_type
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
          AND ({likes})
        ORDER BY c.table_name, c.column_name
        """
        return self._run(sql, params)

    def table_columns_map(self) -> dict[str, set[str]]:
        sql = """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema='public'
        """
        rows = self._run(sql)
        m: dict[str, set[str]] = {}
        for r in rows:
            m.setdefault(r["table_name"], set()).add(r["column_name"])
        return m

    def search_anywhere(
        self,
        needle: str,
        limit_per_column: int = 10,
        max_columns: int = 200,
        only_tables: set[str] | None = None,
    ) -> list[PgHit]:
        # Find candidate columns (text/varchar/json/jsonb) in public schema
        sql_cols = """
        SELECT
          c.table_name,
          c.column_name,
          c.data_type
        FROM information_schema.columns c
        WHERE c.table_schema='public'
          AND c.data_type IN ('text', 'character varying', 'json', 'jsonb')
        ORDER BY
          (CASE
             WHEN c.column_name ILIKE '%transcript%' THEN 0
             WHEN c.column_name ILIKE '%speech%' THEN 1
             WHEN c.column_name ILIKE '%stt%' THEN 2
             WHEN c.column_name ILIKE '%body%' THEN 3
             WHEN c.column_name ILIKE '%content%' THEN 4
             WHEN c.column_name ILIKE '%raw%' THEN 5
             ELSE 10
           END),
          c.table_name,
          c.column_name
        """
        cols = self._run(sql_cols)
        if only_tables:
            cols = [c for c in cols if c["table_name"] in only_tables]
        cols = cols[: int(max_columns)]

        colmap = self.table_columns_map()
        out: list[PgHit] = []

        for c in cols:
            table = c["table_name"]
            col = c["column_name"]
            if only_tables and table not in only_tables:
                continue

            cols_set = colmap.get(table, set())
            id_col = "id" if "id" in cols_set else None
            created_col = "created_at" if "created_at" in cols_set else None
            title_col = "title" if "title" in cols_set else None
            name_col = "name" if "name" in cols_set else None

            select_id = _quote_ident(id_col) if id_col else "ctid::text"
            select_created = _quote_ident(created_col) + "::text" if created_col else "NULL::text"
            select_title = (
                _quote_ident(title_col)
                if title_col
                else (_quote_ident(name_col) if name_col else "NULL::text")
            )

            q = f"""
            SELECT
              {select_id} AS row_id,
              {select_created} AS created_at,
              {select_title}::text AS title,
              left({_quote_ident(col)}::text, 8000) AS blob
            FROM {_quote_ident(table)}
            WHERE {_quote_ident(col)}::text ILIKE :likeq
            ORDER BY {(_quote_ident(created_col) if created_col else "1")} DESC NULLS LAST
            LIMIT :lim
            """
            try:
                rows = self._run(q, {"likeq": f"%{needle}%", "lim": int(limit_per_column)})
            except Exception:
                continue

            for r in rows:
                blob = (r.get("blob") or "")
                out.append(
                    PgHit(
                        source="pg",
                        table=table,
                        column=col,
                        row_id=str(r.get("row_id")),
                        created_at=r.get("created_at"),
                        title=r.get("title"),
                        excerpt=_excerpt(blob, needle),
                    )
                )

        # stable ordering: newest-ish first if created_at parsable; else keep insertion
        return out


def _graph_enabled() -> bool:
    return os.environ.get("GRAPH_ENABLED", "1").strip() not in {"0", "false", "False"}


def _default_kuzu_path() -> str | None:
    p = os.environ.get("KUZU_DB_PATH") or os.environ.get("GRAPH_DB_PATH")
    if p:
        return p.strip()
    sys_root = _env("SYSTEM_ROOT_DIRECTORY")
    if sys_root:
        return str(Path(sys_root) / "databases" / "cognee_graph_kuzu")
    return None


def _unicode_escape_anchor(s: str) -> str:
    low = (s or "").lower()
    if any(ord(ch) > 127 for ch in low):
        esc = low.encode("unicode_escape").decode("ascii")
        return esc.replace("\\u", "\\\\u")
    return low.strip()


@dataclass(frozen=True)
class KuzuHit:
    node_id: str
    node_type: str | None
    name: str | None
    updated_at: str | None
    excerpt: str


class KuzuSearcher:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        try:
            import kuzu  # type: ignore
        except Exception as e:
            raise RuntimeError("kuzu-python not installed") from e
        self.kuzu = kuzu
        self.db = self.kuzu.Database(str(self.db_path))
        self.conn = self.kuzu.Connection(self.db)

    def _query(self, cypher: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        res = self.conn.execute(cypher, params or {})
        cols = list(res.get_column_names())
        out: list[dict[str, Any]] = []
        while res.has_next():
            row = res.get_next()
            out.append(dict(zip(cols, row)))
        return out

    def list_top_types(self, limit: int = 25) -> list[dict[str, Any]]:
        q = """
        MATCH (n:Node)
        RETURN n.type AS type, count(*) AS cnt
        ORDER BY cnt DESC
        LIMIT $limit
        """
        return self._query(q, {"limit": int(limit)})

    def sample_properties(self, node_type: str, limit: int = 2) -> list[dict[str, Any]]:
        q = """
        MATCH (n:Node)
        WHERE n.type = $t
        RETURN n.id AS id, n.name AS name, n.updated_at AS updated_at, left(n.properties, 4000) AS props
        LIMIT $limit
        """
        return self._query(q, {"t": node_type, "limit": int(limit)})

    def search_nodes(self, needle: str, limit: int, types: list[str] | None = None) -> list[KuzuHit]:
        anchored = _unicode_escape_anchor(needle)
        type_filter = ""
        params: dict[str, Any] = {"q": needle, "anchored": anchored, "limit": int(limit)}
        if types:
            type_filter = "AND n.type IN $types"
            params["types"] = types

        q = f"""
        MATCH (n:Node)
        WHERE (
          (n.type IS NOT NULL AND lower(n.type) CONTAINS lower($q))
          OR (n.name IS NOT NULL AND lower(n.name) CONTAINS lower($q))
          OR (n.properties IS NOT NULL AND lower(n.properties) CONTAINS lower($q))
          OR (n.properties IS NOT NULL AND n.properties CONTAINS $anchored)
        )
        {type_filter}
        RETURN
          n.id AS id,
          n.type AS type,
          n.name AS name,
          n.updated_at AS updated_at,
          left(n.properties, 8000) AS props
        LIMIT $limit
        """
        rows = self._query(q, params)
        out: list[KuzuHit] = []
        for r in rows:
            props = str(r.get("props") or "")
            out.append(
                KuzuHit(
                    node_id=str(r.get("id")),
                    node_type=r.get("type"),
                    name=r.get("name"),
                    updated_at=str(r.get("updated_at") or "") or None,
                    excerpt=_excerpt(props, needle) or _excerpt(props, anchored),
                )
            )
        return out


def _print_pg_hits(hits: list[PgHit], max_n: int = 120) -> None:
    for h in hits[:max_n]:
        print(f"[PG] {h.table}.{h.column} row_id={h.row_id} created_at={h.created_at} title={h.title}")
        if h.excerpt:
            print(f"  {h.excerpt}")
        print()


def _print_kuzu_hits(hits: list[KuzuHit], max_n: int = 120) -> None:
    for h in hits[:max_n]:
        print(f"[KUZU] id={h.node_id} type={h.node_type} name={h.name} updated_at={h.updated_at}")
        if h.excerpt:
            print(f"  {h.excerpt}")
        print()


def main() -> int:
    _load_env()

    ap = argparse.ArgumentParser()
    ap.add_argument("--q", default="", help="Поисковая строка. Если пусто — дефолтные needle’ы.")
    ap.add_argument("--limit", type=int, default=30, help="Лимит выдачи на режим/источник.")
    ap.add_argument("--mode", choices=["pg", "kuzu", "both"], default="both")

    # PG
    ap.add_argument("--pg-introspect", action="store_true", help="Показать таблицы + колонки по паттернам и выйти.")
    ap.add_argument("--pg-scan", action="store_true", help="Широкий поиск по всем text/varchar/json(b) колонкам.")
    ap.add_argument("--pg-limit-per-col", type=int, default=6, help="Лимит совпадений на колонку при pg-scan.")
    ap.add_argument("--pg-max-cols", type=int, default=220, help="Ограничить число колонок при pg-scan.")
    ap.add_argument("--pg-only-tables", default="", help="CSV таблиц: искать только в них.")

    # Kuzu
    ap.add_argument("--kuzu-db", default="", help="Путь до Kuzu DB (иначе берём из env/дефолта).")
    ap.add_argument("--list-kuzu-types", action="store_true", help="Топ Node.type и выйти.")
    ap.add_argument("--kuzu-sample", action="store_true", help="Показать пример properties для Task/Snapshot.")
    ap.add_argument("--kuzu-types", default="Task,Snapshot", help="CSV типов Node.type для поиска (default Task,Snapshot).")

    args = ap.parse_args()

    needles = [args.q.strip()] if args.q.strip() else list(DEFAULT_NEEDLES)
    only_tables = {t.strip() for t in args.pg_only_tables.split(",") if t.strip()} or None

    # --- Postgres ---
    if args.mode in {"pg", "both"}:
        db_url = _env("DATABASE_URL")
        if not db_url:
            print("DATABASE_URL пуст — PG пропущен.", file=sys.stderr)
        else:
            try:
                eng = create_engine(db_url, future=True, pool_pre_ping=True)
                pg = PostgresSearcher(eng)

                if args.pg_introspect:
                    print("=== PG tables (approx rows) ===")
                    print(_safe_json(pg.list_tables()))
                    print("\n=== PG columns matching patterns ===")
                    print(_safe_json(pg.list_columns(DEFAULT_COL_PATTERNS)))
                    return 0

                if args.pg_scan:
                    for n in needles:
                        print("=" * 88)
                        print(f"PG scan: {n!r}")
                        hits = pg.search_anywhere(
                            n,
                            limit_per_column=args.pg_limit_per_col,
                            max_columns=args.pg_max_cols,
                            only_tables=only_tables,
                        )
                        _print_pg_hits(hits, max_n=args.limit)
                    # If pg-scan is used, don't also do other PG logic
                else:
                    # Minimal (old behavior) removed intentionally — pg-scan is now the main path.
                    pass

            except SQLAlchemyError as e:
                print(f"Postgres error: {e}", file=sys.stderr)

    # --- Kuzu ---
    if args.mode in {"kuzu", "both"}:
        kuzu_db = args.kuzu_db.strip() or (_default_kuzu_path() or "")
        if not kuzu_db:
            print("Kuzu path пуст — Kuzu пропущен.", file=sys.stderr)
        else:
            db_path = Path(kuzu_db).expanduser().resolve()
            if not db_path.exists():
                print(f"Kuzu DB не найден: {db_path}", file=sys.stderr)
            else:
                try:
                    kz = KuzuSearcher(db_path)

                    if args.list_kuzu_types:
                        print(_safe_json(kz.list_top_types()))
                        return 0

                    if args.kuzu_sample:
                        for t in ["Task", "Snapshot"]:
                            print("=" * 88)
                            print(f"KUZU sample type={t}")
                            print(_safe_json(kz.sample_properties(t, limit=2)))
                        return 0

                    types = [t.strip() for t in args.kuzu_types.split(",") if t.strip()] or None

                    for n in needles:
                        print("=" * 88)
                        print(f"KUZU search: {n!r} types={types}")
                        hits = kz.search_nodes(n, limit=args.limit, types=types)
                        _print_kuzu_hits(hits, max_n=args.limit)

                except Exception as e:
                    print(f"Kuzu error: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())