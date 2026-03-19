from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from src.bitrix_kp_collector.bitrix_client import BitrixClient
from src.bitrix_kp_collector.config import Settings
from src.core.database import init_db, get_engine, get_session
from src.core.models import Base, Task, TaskComment, File, TaskFile, TaskSnapshot


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def _safe_int(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None else None
    except Exception:
        return None


def _parse_dt(s: Any) -> Optional[datetime]:
    if not s:
        return None
    if isinstance(s, datetime):
        return s
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _ensure_schema_migrations() -> None:
    """
    Без alembic, но безопасно:
    - добавляем недостающие колонки (IF NOT EXISTS)
    - создаём уникальные индексы (НЕ трогаем PK, чтобы не ловить multiple primary keys)
    """
    eng = get_engine()
    with eng.begin() as conn:
        # task_snapshots columns (на случай старой схемы)
        conn.execute(
            text(
                """
                alter table if exists task_snapshots
                  add column if not exists last_task_updated_at timestamp;
                alter table if exists task_snapshots
                  add column if not exists last_comments_hash varchar(40);
                alter table if exists task_snapshots
                  add column if not exists last_files_hash varchar(40);
                """
            )
        )

        # chat_files uniqueness (PK НЕ меняем)
        conn.execute(
            text(
                """
                do $$
                begin
                  if to_regclass('public.chat_files') is not null then
                    create unique index if not exists ux_chat_files_triplet
                      on chat_files(dialog_id, message_id, file_id);
                  end if;
                end $$;
                """
            )
        )

        # полезные индексы для скорости
        conn.execute(
            text(
                """
                create index if not exists ix_task_comments_task_id on task_comments(task_id);
                create index if not exists ix_task_files_task_id on task_files(task_id);
                create index if not exists ix_files_local_path on files(local_path);
                do $$
                begin
                  if to_regclass('public.chat_messages') is not null then
                    create index if not exists ix_chat_messages_dialog on chat_messages(dialog_id);
                  end if;
                end $$;
                """
            )
        )


class TaskFingerprintCollector:
    """
    Сборщик задач+комментов+файлов в Postgres.

    ВАЖНО:
    - snapshot: сравнение updated_at + hash комментов + hash файлов
    - файлы: id = attachedObjectId (как в tasks UF_TASK_WEBDAV_FILES)
    """

    def __init__(self, settings: Settings):
        self.settings = settings

        init_db(settings.db_url, statement_timeout_ms=settings.db_statement_timeout_ms)

        engine = get_engine()
        Base.metadata.create_all(bind=engine)
        _ensure_schema_migrations()

        self.client = BitrixClient(
            settings.webhook_primary,
            settings.webhook_secondary,
            max_retries=settings.max_retries,
            base_sleep=settings.retry_backoff_s,
            timeout=settings.timeout_s,
            connect_timeout=settings.connect_timeout_s,
            ignore_env_proxies=settings.ignore_env_proxies,
        )

        self.download_tasks_dir = (settings.download_dir / "tasks")
        self.download_tasks_dir.mkdir(parents=True, exist_ok=True)

    def collect_for_all_tasks(self) -> None:
        print(f"[Collector] === ALL TASKS since {self.settings.min_created_date.isoformat()} ===")

        if self.settings.single_task_id:
            tid = int(self.settings.single_task_id)
            tasks = self.client.list_tasks(self.settings.min_created_date.isoformat())
            tasks = [t for t in tasks if int(self._task_id(t) or -1) == tid]
        else:
            tasks = self.client.list_tasks(self.settings.min_created_date.isoformat())

        with get_session() as db:
            for t in tasks:
                task_id = self._task_id(t)
                if task_id is None:
                    continue
                print(f"\n[Collector] === TASK {task_id} ===")
                self.collect_for_task(db, task_id, t)

    def _task_id(self, t: Any) -> Optional[int]:
        if isinstance(t, dict):
            tid = t.get("ID") or t.get("id") or (t.get("task") or {}).get("id")
            return int(tid) if tid is not None else None
        return None

    def collect_for_task(self, db: Session, task_id: int, task_data: Dict[str, Any]) -> None:
        db_task = db.get(Task, task_id)
        if db_task is None:
            db_task = Task(id=task_id)
            db.add(db_task)

        db_task.title = task_data.get("TITLE") or task_data.get("title")
        db_task.created_at = _parse_dt(task_data.get("CREATED_DATE") or task_data.get("createdDate"))
        db_task.updated_at = _parse_dt(task_data.get("CHANGED_DATE") or task_data.get("changedDate"))
        db_task.raw = task_data

        snap = db.get(TaskSnapshot, task_id)
        if snap is None:
            snap = TaskSnapshot(task_id=task_id)
            db.add(snap)

        comments: List[Dict[str, Any]] = []
        if self.settings.include_task_chat:
            comments = self.client.get_comments(task_id)

        attached_ids = self._extract_attached_ids(task_data)

        comments_hash = _sha1(
            str([(c.get("ID"), c.get("POST_DATE"), (c.get("POST_MESSAGE") or "")) for c in comments])
        )
        files_hash = _sha1(str(attached_ids))

        if (
            snap.last_task_updated_at == db_task.updated_at
            and snap.last_comments_hash == comments_hash
            and snap.last_files_hash == files_hash
        ):
            return

        if self.settings.include_task_chat and comments:
            self._upsert_comments(db, task_id, comments)

        if attached_ids:
            meta = self.client.get_attached_objects(attached_ids)
            self._upsert_and_download_files(db, task_id, meta, comment_id=None)

        snap.updated_at = datetime.utcnow()
        snap.last_task_updated_at = db_task.updated_at
        snap.last_comments_hash = comments_hash
        snap.last_files_hash = files_hash

        db.commit()

    def _extract_attached_ids(self, task_data: Dict[str, Any]) -> List[int]:
        v = task_data.get("UF_TASK_WEBDAV_FILES")
        if v is None:
            v = task_data.get("ufTaskWebdavFiles")
        if isinstance(v, list):
            out: List[int] = []
            for x in v:
                try:
                    out.append(int(x))
                except Exception:
                    pass
            return out
        return []

    def _upsert_comments(self, db: Session, task_id: int, comments: List[Dict[str, Any]]) -> None:
        for c in comments:
            cid = _safe_int(c.get("ID"))
            if cid is None:
                continue

            row = db.get(TaskComment, cid)
            if row is None:
                row = TaskComment(id=cid, task_id=task_id, created_at=datetime.utcnow(), body="[EMPTY]")
                db.add(row)

            row.task_id = task_id
            row.author_id = _safe_int(c.get("AUTHOR_ID"))
            row.author_name = c.get("AUTHOR_NAME")
            row.created_at = _parse_dt(c.get("POST_DATE")) or row.created_at

            body = (c.get("POST_MESSAGE") or "").strip()
            row.body = body if body else "[EMPTY]"
            row.raw = c

    def _upsert_and_download_files(
        self, db: Session, task_id: int, meta: Dict[int, Dict[str, Any]], comment_id: Optional[int]
    ) -> None:
        from pathlib import Path
        import requests

        for attached_id, m in meta.items():
            name = m.get("NAME") or (m.get("OBJECT") or {}).get("NAME")
            size = m.get("SIZE") or (m.get("OBJECT") or {}).get("SIZE")
            download_url = m.get("DOWNLOAD_URL") or m.get("downloadUrl") or (m.get("FILE") or {}).get("DOWNLOAD_URL")

            db_file = db.get(File, int(attached_id))
            if db_file is None:
                db_file = File(id=int(attached_id))
                db.add(db_file)

            db_file.name = name
            db_file.size = _safe_int(size)
            db_file.download_url = download_url
            db_file.raw = m

            exists = db.execute(
                select(TaskFile).where(
                    TaskFile.task_id == task_id,
                    TaskFile.file_id == int(attached_id),
                    TaskFile.comment_id == comment_id,
                )
            ).scalar_one_or_none()

            if exists is None:
                db.add(TaskFile(task_id=task_id, file_id=int(attached_id), comment_id=comment_id))

            if download_url:
                url = self.client.absolutize(download_url)
                safe_name = (name or f"{attached_id}").replace("/", "_").replace("\\", "_")
                out_path = self.download_tasks_dir / f"{attached_id}_{safe_name}"

                if out_path.exists() and out_path.stat().st_size > 0:
                    db_file.local_path = str(out_path)
                    continue

                try:
                    with requests.get(url, stream=True, timeout=(20, 240)) as r:
                        r.raise_for_status()
                        with open(out_path, "wb") as f:
                            for chunk in r.iter_content(chunk_size=1024 * 256):
                                if chunk:
                                    f.write(chunk)
                    db_file.local_path = str(out_path)
                    print(f"[Collector] downloaded: {out_path.name}")
                except Exception as e:
                    print(f"[Collector] download failed: {safe_name} -> {e}")
                    try:
                        if out_path.exists() and out_path.stat().st_size == 0:
                            out_path.unlink()
                    except Exception:
                        pass
