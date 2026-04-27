from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

import kuzu
from dotenv import load_dotenv  # type: ignore

from src.mvp_sksp.domain.equipment_graph import FAMILIES, RELATIONS


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_env() -> None:
    load_dotenv(dotenv_path=str(_repo_root() / ".env"), override=False)


def _default_db_path() -> str:
    return os.getenv("KUZU_DB_PATH", str(_repo_root() / ".cache" / "kuzu_equipment"))


def _q(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _show_tables(conn: kuzu.Connection) -> set[str]:
    try:
        res = conn.execute("CALL show_tables() RETURN *")
    except Exception:
        return set()

    cols = [str(c) for c in res.get_column_names()]
    name_idx = None
    for i, col in enumerate(cols):
        if col.lower() in {"name", "table_name", "tablename"}:
            name_idx = i
            break
    if name_idx is None:
        return set()

    out: set[str] = set()
    while res.has_next():
        row = res.get_next()
        try:
            out.add(str(row[name_idx]))
        except Exception:
            continue
    return out


def _drop_table_if_exists(conn: kuzu.Connection, table_name: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name or ""):
        return
    conn.execute(f"DROP TABLE IF EXISTS {table_name};")


def _drop_old_tables(conn: kuzu.Connection) -> None:
    # 1) Drop known/expected relationship tables for equipment graph domain.
    relation_tables = {str(rel.rel_type).strip().upper() for rel in RELATIONS}
    relation_tables.update({"REQUIRES", "OPTIONAL_WITH", "COMPATIBLE_WITH", "ALTERNATIVE_TO"})

    existing = _show_tables(conn)
    for rel_table in sorted(relation_tables):
        if not existing or rel_table in existing:
            _drop_table_if_exists(conn, rel_table)

    # 2) Drop Family. If there are unexpected relation tables referencing Family,
    # Kuzu error includes their names; iteratively drop and retry.
    for _ in range(12):
        try:
            _drop_table_if_exists(conn, "Family")
            return
        except RuntimeError as e:
            msg = str(e)
            m = re.search(r"relationship table\s+([A-Za-z_][A-Za-z0-9_]*)", msg, flags=re.IGNORECASE)
            if not m:
                raise
            _drop_table_if_exists(conn, m.group(1))

    raise RuntimeError("Failed to drop Family after dependency cleanup retries.")


def _create_schema(conn: kuzu.Connection) -> None:
    conn.execute(
        """
        CREATE NODE TABLE Family(
            family_id STRING,
            name STRING,
            kind STRING,
            description STRING,
            PRIMARY KEY(family_id)
        );
        """
    )
    conn.execute(
        """
        CREATE REL TABLE REQUIRES(
            FROM Family TO Family,
            condition_key STRING,
            rationale STRING,
            priority INT64
        );
        """
    )
    conn.execute(
        """
        CREATE REL TABLE OPTIONAL_WITH(
            FROM Family TO Family,
            condition_key STRING,
            rationale STRING,
            priority INT64
        );
        """
    )


def _insert_families(conn: kuzu.Connection) -> None:
    for fam in FAMILIES:
        conn.execute(
            f'''
            CREATE (f:Family {{
                family_id: "{_q(fam.family_id)}",
                name: "{_q(fam.name)}",
                kind: "{_q(fam.kind)}",
                description: "{_q(fam.description)}"
            }});
            '''
        )


def _insert_relations(conn: kuzu.Connection) -> None:
    for rel in RELATIONS:
        rel_type = rel.rel_type.strip().upper()
        if rel_type not in {"REQUIRES", "OPTIONAL_WITH"}:
            continue

        conn.execute(
            f'''
            MATCH (a:Family {{family_id: "{_q(rel.src_family)}"}}), (b:Family {{family_id: "{_q(rel.dst_family)}"}})
            CREATE (a)-[:{rel_type} {{
                condition_key: "{_q(rel.condition_key or "always")}",
                rationale: "{_q(rel.rationale)}",
                priority: {int(rel.priority)}
            }}]->(b);
            '''
        )


def rebuild_graph(db_path: str) -> None:
    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)

    _drop_old_tables(conn)
    _create_schema(conn)
    _insert_families(conn)
    _insert_relations(conn)


def main() -> int:
    _load_env()

    ap = argparse.ArgumentParser(description="Rebuild equipment graph in Kuzu from domain definitions.")
    ap.add_argument("--db-path", default=_default_db_path(), help="Path to Kuzu DB")
    args = ap.parse_args()

    db_path = str(Path(args.db_path).expanduser().resolve())
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    rebuild_graph(db_path)
    print(f"[init_equipment_graph] rebuilt from src.mvp_sksp.domain.equipment_graph into: {db_path}")
    print(f"[init_equipment_graph] families={len(FAMILIES)} relations={len(RELATIONS)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
