# src/bitrix_kp_collector/call_recordings.py
from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests

from .bitrix_client import BitrixClient


@dataclass(frozen=True)
class RecordingRef:
    deal_id: int
    activity_id: int
    idx: int

    # FILES[].id may be: attachedObject id OR disk file id OR "fileId" for crm_show_file.php
    file_id: Optional[int]
    attached_id: Optional[int]
    disk_file_id: Optional[int]

    file_name: str
    download_url: Optional[str]
    fallback_url: Optional[str]

    subject: Optional[str] = None
    start_time: Optional[str] = None
    direction: Optional[str] = None
    phone: Optional[str] = None

    @property
    def rec_index(self) -> int:
        return int(self.idx)

    @property
    def url(self) -> Optional[str]:
        return self.download_url or self.fallback_url

    @property
    def meta(self) -> Dict[str, Any]:
        return {
            "file_id": self.file_id,
            "attached_id": self.attached_id,
            "disk_file_id": self.disk_file_id,
            "fallback_url": self.fallback_url,
            "subject": self.subject,
            "start_time": self.start_time,
            "direction": self.direction,
            "phone": self.phone,
        }


class BitrixCallExtractor:
    """
    Extract call recordings from CRM activities and download audio.
    - Writes via .part and validates first bytes / content-type to avoid HTML saved as mp3.
    - Resolves URL via disk.* when possible (webhook-friendly).
    """

    def __init__(
        self,
        client: BitrixClient,
        *,
        out_dir: str | Path,
        timeout_s: int = 120,
        connect_timeout_s: int = 15,
        max_retries: int = 8,
        retry_backoff_s: float = 1.2,
    ) -> None:
        self.client = client
        self.out_dir = Path(out_dir)
        self.timeout_s = int(timeout_s)
        self.connect_timeout_s = int(connect_timeout_s)
        self.max_retries = int(max_retries)
        self.retry_backoff_s = float(retry_backoff_s)

    @staticmethod
    def _safe_int(x: Any) -> Optional[int]:
        try:
            if x is None:
                return None
            return int(x)
        except Exception:
            return None

    @staticmethod
    def _looks_like_text_payload(b: bytes) -> bool:
        s = (b or b"").lstrip().lower()
        return (
            s.startswith(b"<html")
            or s.startswith(b"<!doctype")
            or s.startswith(b"<head")
            or s.startswith(b"<body")
            or s.startswith(b"{")
            or s.startswith(b"[")
            or b"<html" in s[:512]
            or b"<!doctype" in s[:512]
        )

    def list_deal_call_activities(self, deal_id: int) -> List[Dict[str, Any]]:
        raw = self.client.call_raw("crm.activity.list", {"filter": {"OWNER_ID": int(deal_id), "TYPE_ID": 2}})
        res = raw.get("result") if isinstance(raw, dict) else None
        return res if isinstance(res, list) else []

    def _abs_url(self, url: str) -> str:
        return self.client.absolutize(url)

    @staticmethod
    def _parse_file_id_from_url(url: Optional[str]) -> Optional[int]:
        if not url:
            return None
        try:
            q = parse_qs(urlparse(url).query)
            v = q.get("fileId") or q.get("fileID") or q.get("FILEID")
            if not v:
                return None
            return int(v[0])
        except Exception:
            return None

    def _resolve_attached_object(self, attached_object_id: int) -> Tuple[Optional[int], Optional[str]]:
        resp = self.client.call_raw("disk.attachedObject.get", {"id": int(attached_object_id)})
        obj = (resp.get("result") or {}) if isinstance(resp, dict) else {}
        disk_file_id = self._safe_int(obj.get("OBJECT_ID") or obj.get("objectId"))
        download_url = obj.get("DOWNLOAD_URL") or obj.get("DOWNLOAD_URL_SHORT") or obj.get("DETAIL_URL")
        return disk_file_id, download_url

    def _resolve_disk_file(self, disk_file_id: int) -> Tuple[Optional[str], Optional[str]]:
        resp = self.client.call_raw("disk.file.get", {"id": int(disk_file_id)})
        obj = (resp.get("result") or {}) if isinstance(resp, dict) else {}
        download_url = obj.get("DOWNLOAD_URL") or obj.get("DOWNLOAD_URL_SHORT") or obj.get("DETAIL_URL")
        name = obj.get("NAME") or obj.get("ORIGINAL_NAME")
        return download_url, name

    def _download_binary(self, url: str, dst: Path) -> None:
        url = self._abs_url(url)
        dst.parent.mkdir(parents=True, exist_ok=True)

        last_err: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            tmp = dst.with_suffix(dst.suffix + ".part")
            try:
                if tmp.exists():
                    tmp.unlink(missing_ok=True)

                r = requests.get(
                    url,
                    timeout=(self.connect_timeout_s, self.timeout_s),
                    stream=True,
                    headers={"User-Agent": "VectorBDRAGcollector/1.0"},
                    allow_redirects=True,
                )

                ct = (r.headers.get("content-type") or "").lower()
                if r.status_code >= 400:
                    raise RuntimeError(f"HTTP {r.status_code} content-type={ct}")

                first = r.raw.read(4096, decode_content=True) or b""
                if self._looks_like_text_payload(first) or ("text/" in ct) or ("application/json" in ct) or ("application/xml" in ct):
                    preview = first[:400].decode("utf-8", errors="replace")
                    raise RuntimeError(f"Got HTML/JSON instead of media (ct={ct}) preview={preview!r}")

                with open(tmp, "wb") as f:
                    f.write(first)
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

                os.replace(tmp, dst)

                if dst.stat().st_size < 1024:
                    with open(dst, "rb") as f:
                        head = f.read(512)
                    if self._looks_like_text_payload(head):
                        preview = head.decode("utf-8", errors="replace")
                        dst.unlink(missing_ok=True)
                        raise RuntimeError(f"Downloaded tiny non-media payload (ct={ct}) preview={preview!r}")

                return
            except Exception as e:
                last_err = e
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
                sleep_s = min(30.0, (self.retry_backoff_s ** attempt) + 0.1)
                time.sleep(sleep_s)

        raise RuntimeError(f"Failed to download after {self.max_retries} retries: {url} err={last_err!r}")

    def iter_recordings_for_deal(self, deal_id: int) -> Iterable[RecordingRef]:
        acts = self.list_deal_call_activities(int(deal_id))
        for act in acts:
            act_id = self._safe_int(act.get("ID")) or 0
            files = act.get("FILES") or []
            subject = act.get("SUBJECT")
            start_time = act.get("START_TIME")
            direction = act.get("DIRECTION")
            phone = None
            try:
                phone = act.get("COMMUNICATIONS") and act["COMMUNICATIONS"][0].get("VALUE")
            except Exception:
                phone = None

            if not isinstance(files, list) or not files:
                continue

            for idx, f in enumerate(files):
                f = f or {}
                raw_id = self._safe_int(f.get("id") or f.get("ID"))
                fallback_url = f.get("url") or f.get("URL")

                file_id = self._parse_file_id_from_url(fallback_url)
                attached_id: Optional[int] = raw_id
                disk_file_id: Optional[int] = None
                download_url: Optional[str] = None
                file_name = f"{act_id}_{idx}.mp3"

                if raw_id:
                    try:
                        disk_file_id, download_url = self._resolve_attached_object(raw_id)
                    except Exception:
                        # Not an attachedObject id -> treat as disk file id
                        disk_file_id = raw_id

                if not disk_file_id and file_id:
                    disk_file_id = file_id

                if disk_file_id and not download_url:
                    try:
                        download_url2, name2 = self._resolve_disk_file(disk_file_id)
                        download_url = download_url2 or download_url
                        if name2:
                            safe_name = re.sub(r"[^\w\.\-]+", "_", str(name2))
                            if safe_name.lower().endswith((".mp3", ".wav", ".m4a", ".ogg")):
                                file_name = safe_name
                    except Exception:
                        pass

                yield RecordingRef(
                    deal_id=int(deal_id),
                    activity_id=int(act_id),
                    idx=int(idx),
                    file_id=file_id,
                    attached_id=attached_id,
                    disk_file_id=disk_file_id,
                    file_name=file_name,
                    download_url=download_url or None,
                    fallback_url=str(fallback_url).strip() if fallback_url else None,
                    subject=str(subject) if subject is not None else None,
                    start_time=str(start_time) if start_time is not None else None,
                    direction=str(direction) if direction is not None else None,
                    phone=str(phone) if phone is not None else None,
                )

    def download_recording(self, rec: RecordingRef) -> Path:
        if not rec.url:
            raise RuntimeError(
                f"No url for deal={rec.deal_id} activity={rec.activity_id} idx={rec.idx} file_id={rec.file_id} attached_id={rec.attached_id} disk_file_id={rec.disk_file_id}"
            )
        dst = self.out_dir / "calls" / str(rec.deal_id) / str(rec.activity_id) / rec.file_name
        self._download_binary(rec.url, dst)
        return dst
