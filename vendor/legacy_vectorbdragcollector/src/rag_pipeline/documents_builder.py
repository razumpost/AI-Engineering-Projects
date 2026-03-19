from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from .embeddings import Embedder


@dataclass
class BuildConfig:
    include_tasks: bool = os.getenv("RAG_INCLUDE_TASKS", "1") not in ("0", "false", "False")
    include_files: bool = os.getenv("RAG_INCLUDE_FILES", "1") not in ("0", "false", "False")
    include_chats: bool = os.getenv("RAG_INCLUDE_CHATS", "1") not in ("0", "false", "False")

    chunk_chars: int = int(os.getenv("RAG_CHUNK_CHARS", "1400"))
    chunk_overlap: int = int(os.getenv("RAG_CHUNK_OVERLAP", "200"))

    # Для файлов, чтобы не убиться на огромных xlsx/pdf:
    file_max_chars: int = int(os.getenv("RAG_FILE_MAX_CHARS", "200000"))  # 200k символов на файл
    xlsx_max_rows_per_sheet: int = int(os.getenv("RAG_XLSX_MAX_ROWS_PER_SHEET", "300"))
    xlsx_max_sheets: int = int(os.getenv("RAG_XLSX_MAX_SHEETS", "6"))

    incremental: bool = os.getenv("INCREMENTAL", "0") not in ("0", "false", "False")


@dataclass
class BuildStats:
    documents: int = 0
    chunks: int = 0
    embeddings: int = 0


def _jsonb(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def _chunk_text(text_s: str, *, size: int, overlap: int) -> List[str]:
    t = (text_s or "").strip()
    if not t:
        return []
    if size <= 0:
        return [t]
    if overlap < 0:
        overlap = 0

    out: List[str] = []
    i = 0
    n = len(t)
    step = max(1, size - overlap)

    while i < n:
        out.append(t[i : i + size])
        i += step

    return out


def build_all(conn: Connection, *, embedder: Embedder, cfg: BuildConfig) -> BuildStats:
    stats = BuildStats()

    if cfg.include_tasks:
        s = build_chunks_for_tasks(conn, embedder=embedder, cfg=cfg)
        stats.documents += s.documents
        stats.chunks += s.chunks
        stats.embeddings += s.embeddings

    if cfg.include_files:
        s = build_chunks_for_files(conn, embedder=embedder, cfg=cfg)
        stats.documents += s.documents
        stats.chunks += s.chunks
        stats.embeddings += s.embeddings

    if cfg.include_chats:
        s = build_chunks_for_chats(conn, embedder=embedder, cfg=cfg)
        stats.documents += s.documents
        stats.chunks += s.chunks
        stats.embeddings += s.embeddings

    return stats


def _has_col(conn: Connection, table: str, col: str) -> bool:
    insp = inspect(conn)
    try:
        cols = {c["name"] for c in insp.get_columns(table)}
        return col in cols
    except Exception:
        return False


def _upsert_document(
    conn: Connection,
    *,
    source: str,
    source_id: str,
    title: Optional[str],
    created_at: Optional[datetime],
    updated_at: Optional[datetime],
    meta: Dict[str, Any],
) -> int:
    row = conn.execute(
        text(
            """
            insert into rag_documents(source, source_id, title, created_at, updated_at, meta)
            values (:source, :sid, :title, :created_at, :updated_at, cast(:meta as jsonb))
            on conflict (source, source_id) do update set
              title = excluded.title,
              created_at = excluded.created_at,
              updated_at = excluded.updated_at,
              meta = excluded.meta
            returning id;
            """
        ),
        {
            "source": source,
            "sid": str(source_id),
            "title": title,
            "created_at": created_at,
            "updated_at": updated_at,
            "meta": _jsonb(meta),
        },
    ).scalar_one()
    return int(row)


def _upsert_chunk(conn: Connection, *, doc_id: int, chunk_index: int, content: str, meta: Dict[str, Any]) -> int:
    chunk_id = conn.execute(
        text(
            """
            insert into rag_chunks(document_id, chunk_index, content, meta)
            values (:doc_id, :chunk_index, :content, cast(:meta as jsonb))
            on conflict (document_id, chunk_index) do update set
              content = excluded.content,
              meta = excluded.meta
            returning id;
            """
        ),
        {"doc_id": doc_id, "chunk_index": int(chunk_index), "content": content, "meta": _jsonb(meta)},
    ).scalar_one()
    return int(chunk_id)


def _upsert_embedding(conn: Connection, *, chunk_id: int, vec_str: str) -> None:
    # ВАЖНО: используем CAST(:vec AS vector), а не :vec::vector (иначе psycopg2 может не распарсить bind)
    conn.execute(
        text(
            """
            insert into rag_embeddings(chunk_id, embedding)
            values (:cid, cast(:vec as vector))
            on conflict (chunk_id) do update set embedding = excluded.embedding;
            """
        ),
        {"cid": int(chunk_id), "vec": vec_str},
    )


# -------------------- TASKS --------------------

def build_chunks_for_tasks(conn: Connection, *, embedder: Embedder, cfg: BuildConfig) -> BuildStats:
    stats = BuildStats()

    title_col = "title" if _has_col(conn, "tasks", "title") else ("name" if _has_col(conn, "tasks", "name") else None)
    desc_col = "description" if _has_col(conn, "tasks", "description") else None
    raw_col = "raw" if _has_col(conn, "tasks", "raw") else None

    created_col = "created_at" if _has_col(conn, "tasks", "created_at") else None
    updated_col = "updated_at" if _has_col(conn, "tasks", "updated_at") else None

    if title_col is None:
        return stats

    q = f"select id, {title_col} as title"
    q += f", {desc_col} as description" if desc_col else ", null as description"
    q += f", {created_col} as created_at" if created_col else ", null::timestamptz as created_at"
    q += f", {updated_col} as updated_at" if updated_col else ", null::timestamptz as updated_at"
    q += f", {raw_col} as raw" if raw_col else ", null as raw"
    q += " from tasks order by id asc"

    tasks = conn.execute(text(q)).mappings().all()

    # comments (если таблица есть)
    comments_exist = True
    try:
        inspect(conn).get_columns("task_comments")
    except Exception:
        comments_exist = False

    comments_by_task: Dict[int, List[str]] = {}
    if comments_exist:
        body_col = "body" if _has_col(conn, "task_comments", "body") else None
        if body_col:
            for r in conn.execute(text(f"select task_id, {body_col} as body from task_comments order by id asc")).mappings():
                tid = int(r["task_id"])
                b = (r["body"] or "").strip()
                if not b:
                    continue
                comments_by_task.setdefault(tid, []).append(b)

    for t in tasks:
        tid = int(t["id"])
        title = (t["title"] or "").strip()

        parts: List[str] = [f"TASK {tid}", title]
        if t.get("description"):
            parts.append(str(t["description"]))
        if tid in comments_by_task:
            parts.append("\n".join(comments_by_task[tid]))

        content = "\n\n".join([p for p in parts if p])

        doc_id = _upsert_document(
            conn,
            source="bitrix_task",
            source_id=str(tid),
            title=title,
            created_at=t.get("created_at"),
            updated_at=t.get("updated_at"),
            meta={"task_id": tid, "raw": t.get("raw")},
        )
        stats.documents += 1

        chunks = _chunk_text(content, size=cfg.chunk_chars, overlap=cfg.chunk_overlap)
        if not chunks:
            continue

        embs = embedder.embed_texts(chunks, is_query=False)
        for idx, ch in enumerate(chunks):
            chunk_id = _upsert_chunk(conn, doc_id=doc_id, chunk_index=idx, content=ch, meta={"kind": "task_text"})
            _upsert_embedding(conn, chunk_id=chunk_id, vec_str=embedder.to_pgvector(embs[idx]))
            stats.chunks += 1
            stats.embeddings += 1

    return stats


# -------------------- FILES --------------------

def _read_docx(path: Path) -> str:
    try:
        import docx  # python-docx
        d = docx.Document(str(path))
        return "\n".join([p.text for p in d.paragraphs if p.text])
    except Exception:
        return ""


def _read_pptx(path: Path) -> str:
    try:
        from pptx import Presentation
        prs = Presentation(str(path))
        texts: List[str] = []
        for slide in prs.slides:
            for shape in slide.shapes:
                if hasattr(shape, "text"):
                    txt = (shape.text or "").strip()
                    if txt:
                        texts.append(txt)
        return "\n".join(texts)
    except Exception:
        return ""


def _read_pdf(path: Path) -> str:
    try:
        from pypdf import PdfReader
        r = PdfReader(str(path))
        texts: List[str] = []
        for p in r.pages:
            t = (p.extract_text() or "").strip()
            if t:
                texts.append(t)
        return "\n".join(texts)
    except Exception:
        return ""


def _read_xlsx(path: Path, *, max_rows_per_sheet: int, max_sheets: int) -> str:
    try:
        from openpyxl import load_workbook
        wb = load_workbook(str(path), read_only=True, data_only=True)
        texts: List[str] = []
        for ws in wb.worksheets[:max_sheets]:
            texts.append(f"[SHEET] {ws.title}")
            rows = 0
            for row in ws.iter_rows(values_only=True):
                if rows >= max_rows_per_sheet:
                    break
                vals = []
                for v in row:
                    if v is None:
                        continue
                    s = str(v).strip()
                    if s:
                        vals.append(s)
                if vals:
                    texts.append(" | ".join(vals))
                rows += 1
        return "\n".join(texts)
    except Exception:
        return ""


def _read_file_text(path: Path, cfg: BuildConfig) -> str:
    suf = path.suffix.lower()
    if suf == ".docx":
        return _read_docx(path)
    if suf == ".pptx":
        return _read_pptx(path)
    if suf == ".pdf":
        return _read_pdf(path)
    if suf in (".xlsx",):
        return _read_xlsx(path, max_rows_per_sheet=cfg.xlsx_max_rows_per_sheet, max_sheets=cfg.xlsx_max_sheets)
    return ""


def build_chunks_for_files(conn: Connection, *, embedder: Embedder, cfg: BuildConfig) -> BuildStats:
    stats = BuildStats()

    updated_expr = "updated_at" if _has_col(conn, "files", "updated_at") else "null::timestamptz as updated_at"
    raw_expr = "raw" if _has_col(conn, "files", "raw") else "null as raw"
    size_expr = "size" if _has_col(conn, "files", "size") else "null as size"
    download_expr = "download_url" if _has_col(conn, "files", "download_url") else "null as download_url"

    rows = conn.execute(
        text(
            f"""
            select id, name, local_path, {updated_expr}, {size_expr}, {download_expr}, {raw_expr}
            from files
            where local_path is not null
            order by id asc
            """
        )
    ).mappings().all()

    for r in rows:
        fid = int(r["id"])
        name = (r.get("name") or "").strip()
        local_path = r.get("local_path")
        if not local_path:
            continue

        p = Path(str(local_path))
        if not p.exists():
            continue

        extracted = _read_file_text(p, cfg)
        base_text = f"FILE {fid}\n{name}\n{p.name}\n{p.as_posix()}"
        text_s = base_text + ("\n\n" + extracted if extracted else "")

        if len(text_s) > cfg.file_max_chars:
            text_s = text_s[: cfg.file_max_chars]

        doc_id = _upsert_document(
            conn,
            source="bitrix_file",
            source_id=str(fid),
            title=name or p.name,
            created_at=None,
            updated_at=r.get("updated_at"),
            meta={
                "file_id": fid,
                "name": name,
                "path": str(local_path),
                "size": r.get("size"),
                "download_url": r.get("download_url"),
                "raw": r.get("raw"),
            },
        )
        stats.documents += 1

        chunks = _chunk_text(text_s, size=cfg.chunk_chars, overlap=cfg.chunk_overlap)
        if not chunks:
            continue

        embs = embedder.embed_texts(chunks, is_query=False)
        for idx, ch in enumerate(chunks):
            chunk_id = _upsert_chunk(conn, doc_id=doc_id, chunk_index=idx, content=ch, meta={"kind": "file_text"})
            _upsert_embedding(conn, chunk_id=chunk_id, vec_str=embedder.to_pgvector(embs[idx]))
            stats.chunks += 1
            stats.embeddings += 1

    return stats


# -------------------- CHATS --------------------

def build_chunks_for_chats(conn: Connection, *, embedder: Embedder, cfg: BuildConfig) -> BuildStats:
    stats = BuildStats()

    try:
        inspect(conn).get_columns("chat_messages")
    except Exception:
        return stats

    body_col = "body" if _has_col(conn, "chat_messages", "body") else None
    raw_col = "raw" if _has_col(conn, "chat_messages", "raw") else None
    created_col = "created_at" if _has_col(conn, "chat_messages", "created_at") else None

    if body_col is None:
        return stats

    q = f"select dialog_id, id as msg_id, {body_col} as body"
    q += f", {created_col} as created_at" if created_col else ", null::timestamptz as created_at"
    q += f", {raw_col} as raw" if raw_col else ", null as raw"
    q += " from chat_messages order by dialog_id asc, id asc"

    rows = conn.execute(text(q)).mappings().all()

    by_dialog: Dict[str, List[str]] = {}
    for r in rows:
        did = str(r["dialog_id"])
        body = (r.get("body") or "").strip()
        if not body:
            continue
        by_dialog.setdefault(did, []).append(body)

    for did, msgs in by_dialog.items():
        content = f"CHAT {did}\n\n" + "\n\n".join(msgs)

        doc_id = _upsert_document(
            conn,
            source="bitrix_chat",
            source_id=did,
            title=f"Chat {did}",
            created_at=None,
            updated_at=None,
            meta={"dialog_id": did, "messages": len(msgs)},
        )
        stats.documents += 1

        chunks = _chunk_text(content, size=cfg.chunk_chars, overlap=cfg.chunk_overlap)
        if not chunks:
            continue

        embs = embedder.embed_texts(chunks, is_query=False)
        for idx, ch in enumerate(chunks):
            chunk_id = _upsert_chunk(conn, doc_id=doc_id, chunk_index=idx, content=ch, meta={"kind": "chat_text"})
            _upsert_embedding(conn, chunk_id=chunk_id, vec_str=embedder.to_pgvector(embs[idx]))
            stats.chunks += 1
            stats.embeddings += 1

    return stats
