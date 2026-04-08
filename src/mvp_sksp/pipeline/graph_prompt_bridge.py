from __future__ import annotations

import os
from collections import deque
from pathlib import Path
from typing import Any

import kuzu
from dotenv import load_dotenv  # type: ignore

from ..domain.equipment_graph import (
    condition_matches,
    derive_request_flags,
    infer_seed_families,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


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
    _load_env()

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

            if rel_type in {"REQUIRES", "OPTIONAL_WITH"}:
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


def _compact_family_line(fam: dict[str, Any]) -> str:
    return f"{fam['family_id']} ({fam['kind']})"


def render_graph_hint(request_text: str, graph_data: dict[str, Any] | None = None) -> str:
    data = graph_data or expand_graph(request_text)

    family_lines = [_compact_family_line(f) for f in data["resolved_families"]]
    added_lines = []
    for edge in data["added_edges"]:
        added_lines.append(
            f"{edge['from_family']} -> {edge['to_family']} [{edge['rel_type']}; {edge['condition_key']}]"
        )

    blocks = [
        "[ENGINEERING_GRAPH_CONTEXT]",
        "Ниже инженерные family и зависимости, автоматически выведенные из запроса.",
        "Используй их как жёсткие/приоритетные семена для состава решения.",
        "",
        "Resolved families:",
        *[f"- {line}" for line in family_lines],
        "",
        "Auto-added dependencies:",
        *[f"- {line}" for line in added_lines],
        "",
        "Rules:",
        "- Не игнорируй критические family из списка Resolved families.",
        "- Если family попала через REQUIRES/OPTIONAL_WITH и логически обязательна по запросу, учти её в проекте.",
        "- Для discussion/delegate systems учитывай central unit, DSP, питание/расширение и кабельную инфраструктуру.",
        "- Для meeting room учитывай display, camera, microphone, mount, cabling и при необходимости DSP/controller.",
        "[/ENGINEERING_GRAPH_CONTEXT]",
    ]
    return "\n".join(blocks)


def augment_transcript_with_graph(request_text: str) -> tuple[str, dict[str, Any]]:
    data = expand_graph(request_text)
    hint = render_graph_hint(request_text, data)
    augmented = f"{hint}\n\n[USER_REQUEST]\n{request_text}\n[/USER_REQUEST]"
    return augmented, data