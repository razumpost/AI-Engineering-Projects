from __future__ import annotations

import argparse
import os
from collections import deque
from pathlib import Path
from typing import Any

import kuzu
from dotenv import load_dotenv  # type: ignore

from src.mvp_sksp.domain.equipment_graph import (
    condition_matches,
    derive_request_flags,
    infer_seed_families,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_env() -> None:
    load_dotenv(dotenv_path=str(_repo_root() / ".env"), override=False)


def _db_path() -> str:
    return os.getenv("KUZU_DB_PATH", str(_repo_root() / ".cache" / "kuzu_equipment"))


def _open_conn() -> kuzu.Connection:
    db = kuzu.Database(_db_path())
    return kuzu.Connection(db)


def _query_family(conn: kuzu.Connection, family_id: str) -> dict[str, Any] | None:
    result = conn.execute(
        f'''
        MATCH (f:Family {{family_id: "{family_id}"}})
        RETURN f.family_id, f.name, f.kind, f.description;
        '''
    )

    if not result.has_next():
        return None

    row = result.get_next()
    return {
        "family_id": row[0],
        "name": row[1],
        "kind": row[2],
        "description": row[3],
    }


def _query_edges(conn: kuzu.Connection, src_family: str) -> list[dict[str, Any]]:
    query = f'''
    MATCH (a:Family {{family_id: "{src_family}"}})-[r]->(b:Family)
    RETURN label(r), b.family_id, b.name, b.kind, r.condition_key, r.rationale, r.priority
    ORDER BY r.priority ASC;
    '''
    result = conn.execute(query)

    rows: list[dict[str, Any]] = []
    while result.has_next():
        row = result.get_next()
        rows.append(
            {
                "rel_type": row[0],
                "dst_family": row[1],
                "dst_name": row[2],
                "dst_kind": row[3],
                "condition_key": row[4],
                "rationale": row[5],
                "priority": row[6],
            }
        )
    return rows


def expand_graph(request_text: str) -> dict[str, Any]:
    flags = derive_request_flags(request_text)
    seed_families = infer_seed_families(request_text)

    conn = _open_conn()

    visited: set[str] = set()
    queue = deque(seed_families)

    added: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    while queue:
        current = queue.popleft()
        if current in visited:
            continue
        visited.add(current)

        edges = _query_edges(conn, current)
        for edge in edges:
            rel_type = edge["rel_type"]
            dst = edge["dst_family"]
            cond = edge["condition_key"] or "always"

            if rel_type == "REQUIRES":
                matched = condition_matches(cond, flags)
            elif rel_type == "OPTIONAL_WITH":
                matched = condition_matches(cond, flags)
            else:
                matched = False

            if matched:
                if dst not in visited:
                    queue.append(dst)
                added.append(
                    {
                        "from_family": current,
                        "rel_type": rel_type,
                        "to_family": dst,
                        "condition_key": cond,
                        "rationale": edge["rationale"],
                        "priority": edge["priority"],
                    }
                )
            else:
                skipped.append(
                    {
                        "from_family": current,
                        "rel_type": rel_type,
                        "to_family": dst,
                        "condition_key": cond,
                        "rationale": edge["rationale"],
                        "priority": edge["priority"],
                    }
                )

    resolved_families = []
    for fid in visited:
        fam = _query_family(conn, fid)
        if fam:
            resolved_families.append(fam)

    resolved_families.sort(key=lambda x: x["family_id"])
    added.sort(key=lambda x: (x["priority"], x["from_family"], x["to_family"]))
    skipped.sort(key=lambda x: (x["priority"], x["from_family"], x["to_family"]))

    return {
        "request_text": request_text,
        "flags": flags,
        "seed_families": seed_families,
        "resolved_families": resolved_families,
        "added_edges": added,
        "skipped_edges": skipped,
    }


def main() -> int:
    _load_env()

    ap = argparse.ArgumentParser()
    ap.add_argument("--request", required=True, help="Свободный текст запроса")
    args = ap.parse_args()

    data = expand_graph(args.request)

    print("[debug_graph_expansion] request")
    print(data["request_text"])
    print()

    print("[debug_graph_expansion] flags")
    for k, v in data["flags"].items():
        print(f"- {k}={v}")
    print()

    print("[debug_graph_expansion] seed_families")
    for x in data["seed_families"]:
        print(f"- {x}")
    print()

    print("[debug_graph_expansion] resolved_families")
    for fam in data["resolved_families"]:
        print(f"- {fam['family_id']} | {fam['kind']} | {fam['name']}")
    print()

    print("[debug_graph_expansion] added_edges")
    for e in data["added_edges"]:
        print(
            f"- {e['from_family']} --{e['rel_type']}[{e['condition_key']}]--> {e['to_family']} | {e['rationale']}"
        )
    print()

    print("[debug_graph_expansion] skipped_edges")
    for e in data["skipped_edges"]:
        print(
            f"- {e['from_family']} --{e['rel_type']}[{e['condition_key']}]--> {e['to_family']} | {e['rationale']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())