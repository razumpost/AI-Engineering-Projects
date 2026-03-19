# src/bitrix_kp_collector/supplier_chat_collector.py
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.bitrix_kp_collector.bitrix_client import BitrixClient
from src.core.models import ChatMessage, ChatFile, File


def _safe_int(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None else None
    except Exception:
        return None


def _parse_dt(x: Any) -> Optional[datetime]:
    if not x:
        return None
    if isinstance(x, datetime):
        return x
    s = str(x)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


class SupplierChatCollector:
    """
    Сборщик IM-диалогов (чатов поставщиков):

    - тянет сообщения чата (dialog_id вида chat85378)
    - извлекает file ids из сообщений (как получится)
    - пытается получить метаданные по id:
        1) disk.attachedObject.get  -> id как есть (положительный)
        2) disk.file.get            -> сохраняем в files.id = -disk_id (чтобы не было коллизий)
    - upsert в таблицу files
    - скачивает файлы в downloads/chats/<dialog_id>/
    """

    def __init__(self, client: BitrixClient, download_dir: str):
        self.client = client
        self.download_dir = download_dir

        self.page_size = int(os.getenv("SUPPLIER_CHAT_PAGE_SIZE", "50"))
        self.limit = int(os.getenv("SUPPLIER_CHAT_LIMIT", "5000"))

        self.timeout_s = float(os.getenv("BITRIX_DOWNLOAD_TIMEOUT_S", "120"))
        self.max_mb = int(os.getenv("BITRIX_DOWNLOAD_MAX_MB", "200"))

    def collect_dialog(self, db: Session, dialog_id: str) -> None:
        print(f"[SupplierChat] === DIALOG {dialog_id} ===")

        msgs = self._fetch_messages(dialog_id, limit=self.limit)
        if not msgs:
            print("[SupplierChat] no messages")
            return

        raw_file_ids: Set[int] = set()

        for m in msgs:
            mid = _safe_int(m.get("id") or m.get("ID"))
            if mid is None:
                continue

            row = db.get(ChatMessage, mid)
            if row is None:
                row = ChatMessage(id=mid, dialog_id=dialog_id, body="")
                db.add(row)

            row.dialog_id = dialog_id
            row.author_id = _safe_int(m.get("authorId") or m.get("AUTHOR_ID") or m.get("fromUserId"))
            row.created_at = _parse_dt(m.get("date") or m.get("DATE_CREATE") or m.get("POST_DATE"))
            row.body = str(m.get("text") or m.get("TEXT") or m.get("message") or "")
            row.raw = m

            # фиксируем сообщение, чтобы FK на message_id не падал
            db.flush()

            for fid in self._extract_file_ids(m):
                raw_file_ids.add(fid)

                # В chat_files храним dialog_id + message_id + file_id (file_id может стать отрицательным позже)
                exists = db.execute(
                    select(ChatFile).where(
                        ChatFile.dialog_id == dialog_id,
                        ChatFile.message_id == mid,
                        ChatFile.file_id == fid,
                    )
                ).scalar_one_or_none()
                if exists is None:
                    db.add(ChatFile(dialog_id=dialog_id, message_id=mid, file_id=fid))

        db.flush()

        if not raw_file_ids:
            db.commit()
            print("[SupplierChat] no file ids in dialog")
            return

        raw_ids_sorted = sorted(raw_file_ids)

        # 1) пробуем как attachedObject ids
        meta_attached = self.client.get_attached_objects(raw_ids_sorted, batch_size=50)

        # 2) пробуем как disk file ids
        meta_disk = self.client.get_disk_files(raw_ids_sorted, batch_size=50)

        # Собираем canonical meta:
        # - если нашлось в attached -> canonical_id = +fid
        # - если нашлось в disk.file -> canonical_id = -fid (чтобы не конфликтовать с attached ids)
        canonical_meta: Dict[int, Dict[str, Any]] = {}
        remap_to_canonical: Dict[int, int] = {}

        for fid in raw_ids_sorted:
            if fid in meta_attached and meta_attached[fid]:
                canonical_id = int(fid)
                canonical_meta[canonical_id] = meta_attached[fid]
                remap_to_canonical[fid] = canonical_id
            elif fid in meta_disk and meta_disk[fid]:
                canonical_id = -int(fid)
                canonical_meta[canonical_id] = meta_disk[fid]
                remap_to_canonical[fid] = canonical_id

        if not canonical_meta:
            db.commit()
            print("[SupplierChat] file ids found, but no meta from attachedObject nor disk.file")
            return

        # Перепривязываем chat_files.file_id на canonical_id (особенно важно для disk.file -> отрицательные)
        # (делаем это аккуратно, чтобы не плодить дублей)
        for old_fid, new_fid in remap_to_canonical.items():
            if old_fid == new_fid:
                continue

            # если строка с old_fid есть — обновим на new_fid, но только если такой уже нет
            rows = db.execute(
                select(ChatFile).where(ChatFile.dialog_id == dialog_id, ChatFile.file_id == old_fid)
            ).scalars().all()

            for r in rows:
                exists_new = db.execute(
                    select(ChatFile).where(
                        ChatFile.dialog_id == r.dialog_id,
                        ChatFile.message_id == r.message_id,
                        ChatFile.file_id == new_fid,
                    )
                ).scalar_one_or_none()

                if exists_new is None:
                    r.file_id = new_fid
                else:
                    # дубль — удаляем старую
                    db.delete(r)

        db.flush()

        self._upsert_files(db, canonical_meta)
        self._download_files(db, canonical_meta, dialog_id=dialog_id)

        db.commit()

    # -------- Bitrix IM --------

    def _fetch_messages(self, dialog_id: str, limit: int) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        last_id: Optional[int] = None

        while True:
            need = min(self.page_size, max(0, limit - len(out)))
            if need <= 0:
                break

            page = self._call_messages_api(dialog_id, need, last_id)
            if not page:
                break

            new_ids = [(_safe_int(x.get("id") or x.get("ID"))) for x in page]
            new_ids = [x for x in new_ids if x is not None]
            if last_id is not None and new_ids and max(new_ids) <= last_id:
                break

            out.extend(page)
            if new_ids:
                last_id = max(new_ids)

            if len(page) < need:
                break

        return out[:limit]

    def _call_messages_api(self, dialog_id: str, limit: int, last_id: Optional[int]) -> List[Dict[str, Any]]:
        params = {"DIALOG_ID": dialog_id, "LIMIT": int(limit)}
        if last_id is not None:
            params["LAST_ID"] = int(last_id)

        # 1) im.dialog.messages.get
        try:
            raw = self.client.call_raw("im.dialog.messages.get", params)
            return self._normalize_messages(raw)
        except Exception:
            pass

        # 2) fallback: im.message.get
        try:
            raw = self.client.call_raw("im.message.get", params)
            return self._normalize_messages(raw)
        except Exception:
            return []

    def _normalize_messages(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        res = raw.get("result") if isinstance(raw, dict) else None
        if isinstance(res, list):
            return [x for x in res if isinstance(x, dict)]
        if isinstance(res, dict):
            for k in ("messages", "items", "list"):
                v = res.get(k)
                if isinstance(v, list):
                    return [x for x in v if isinstance(x, dict)]
        return []

    # -------- File extraction --------

    def _extract_file_ids(self, msg: Dict[str, Any]) -> List[int]:
        out: List[int] = []

        # встречаются варианты
        for k in ("FILES", "files", "FILE_ID", "fileId", "diskFileId", "DISK_FILE_ID"):
            v = msg.get(k)
            if isinstance(v, list):
                for x in v:
                    i = _safe_int(x)
                    if i is not None:
                        out.append(i)
            else:
                i = _safe_int(v)
                if i is not None:
                    out.append(i)

        att = msg.get("ATTACH") or msg.get("attach")
        if isinstance(att, list):
            for a in att:
                if isinstance(a, dict):
                    fid = _safe_int(
                        a.get("ATTACHED_ID")
                        or a.get("attachedId")
                        or a.get("FILE_ID")
                        or a.get("fileId")
                        or a.get("DISK_FILE_ID")
                        or a.get("diskFileId")
                        or a.get("id")
                    )
                    if fid is not None:
                        out.append(fid)

        params = msg.get("params") or msg.get("PARAMS")
        if isinstance(params, dict):
            for k in ("ATTACHED_ID", "attachedId", "FILE_ID", "fileId", "DISK_FILE_ID", "diskFileId", "FILES"):
                v = params.get(k)
                if isinstance(v, list):
                    for x in v:
                        i = _safe_int(x)
                        if i is not None:
                            out.append(i)
                else:
                    i = _safe_int(v)
                    if i is not None:
                        out.append(i)

        return sorted(set(out))

    def _upsert_files(self, db: Session, meta: Dict[int, Dict[str, Any]]) -> None:
        """
        meta keys are canonical ids:
          +id -> attachedObject id
          -id -> disk file id (stored as negative to avoid collisions)
        """
        for canonical_id, m in meta.items():
            # нормализуем как получится
            name = (
                m.get("NAME")
                or m.get("name")
                or (m.get("OBJECT") or {}).get("NAME")
                or (m.get("FILE") or {}).get("NAME")
            )
            size = (
                m.get("SIZE")
                or m.get("size")
                or (m.get("OBJECT") or {}).get("SIZE")
                or (m.get("FILE") or {}).get("SIZE")
            )
            download_url = (
                m.get("DOWNLOAD_URL")
                or m.get("downloadUrl")
                or (m.get("FILE") or {}).get("DOWNLOAD_URL")
                or (m.get("FILE") or {}).get("downloadUrl")
            )

            f = db.get(File, int(canonical_id))
            if f is None:
                f = File(id=int(canonical_id))
                db.add(f)

            if name:
                f.name = str(name)
            f.size = _safe_int(size)
            if download_url:
                f.download_url = str(download_url)
            f.raw = m

        db.flush()

    def _download_files(self, db: Session, meta: Dict[int, Dict[str, Any]], dialog_id: str) -> None:
        base = Path(self.download_dir) / "chats" / dialog_id
        base.mkdir(parents=True, exist_ok=True)

        for canonical_id in sorted(meta.keys()):
            f = db.get(File, int(canonical_id))
            if not f:
                continue
            if f.local_path:
                continue
            if not f.download_url:
                continue

            safe_name = (f.name or f"file_{canonical_id}").replace("/", "_").replace("\\", "_")
            out_path = base / f"{canonical_id}_{safe_name}"

            if f.size and f.size > self.max_mb * 1024 * 1024:
                print(f"[SupplierChat] skip too large: id={canonical_id} size={f.size}")
                continue

            url = self.client.absolutize(f.download_url)
            try:
                r = requests.get(url, stream=True, timeout=self.timeout_s)
                r.raise_for_status()
                with open(out_path, "wb") as w:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            w.write(chunk)
                f.local_path = str(out_path)
                db.flush()
                print(f"[SupplierChat] downloaded: {out_path.name}")
            except Exception as e:
                print(f"[SupplierChat] download failed id={canonical_id}: {e}")
