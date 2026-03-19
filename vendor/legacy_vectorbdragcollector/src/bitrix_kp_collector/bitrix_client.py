# src/bitrix_kp_collector/bitrix_client.py
from __future__ import annotations

import time
import json
from dataclasses import dataclass
from typing import Optional, Any, Dict, Tuple
from urllib.parse import urljoin, urlparse

import requests


@dataclass
class BitrixClientConfig:
    webhook_primary: str
    webhook_secondary: Optional[str] = None

    timeout_s: float = 60.0
    connect_timeout_s: float = 15.0

    max_retries: int = 8
    retry_backoff_s: float = 0.7

    # для скачивания файлов — отдельный таймаут (часто больше, чем REST)
    download_timeout_s: float = 180.0

    # не брать прокси из env (часто ломает доступ к bitrix)
    ignore_env_proxies: bool = True


class BitrixClient:
    """
    Bitrix REST client с fallback на второй webhook при 401/403.
    Плюс: безопасное скачивание disk attached-object через REST (disk.attachedObject.get / disk.file.get),
    чтобы fallback реально работал и не упираться в "Bad permission" на прямых disk/uf.php ссылках.
    """

    def __init__(self, cfg: BitrixClientConfig):
        self.webhook_primary = cfg.webhook_primary.rstrip("/") + "/"
        self.webhook_secondary = (cfg.webhook_secondary.rstrip("/") + "/" if cfg.webhook_secondary else None)

        self.timeout_s = float(cfg.timeout_s)
        self.connect_timeout_s = float(cfg.connect_timeout_s)
        self.max_retries = int(cfg.max_retries)
        self.retry_backoff_s = float(cfg.retry_backoff_s)
        self.download_timeout_s = float(cfg.download_timeout_s)
        self.ignore_env_proxies = bool(cfg.ignore_env_proxies)

        self._sess = requests.Session()
        if self.ignore_env_proxies:
            self._sess.trust_env = False

        # домен берём из primary webhook
        pr = urlparse(self.webhook_primary)
        self._base_origin = f"{pr.scheme}://{pr.netloc}"

    def absolutize(self, maybe_relative_url: str) -> str:
        if not maybe_relative_url:
            return maybe_relative_url
        if maybe_relative_url.startswith("http://") or maybe_relative_url.startswith("https://"):
            return maybe_relative_url
        return urljoin(self._base_origin, maybe_relative_url)

    def _other_webhook(self, cur: str) -> Optional[str]:
        if not self.webhook_secondary:
            return None
        if cur.rstrip("/") == self.webhook_primary.rstrip("/"):
            return self.webhook_secondary
        return self.webhook_primary

    def _call_raw_once(self, webhook: str, method: str, params: Optional[dict]) -> dict:
        url = webhook + method
        timeout = (self.connect_timeout_s, self.timeout_s)

        for attempt in range(self.max_retries + 1):
            r = self._sess.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                time.sleep(self.retry_backoff_s * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()

        # если дошли сюда — последний ответ был 429 и retries закончились
        r.raise_for_status()
        return {}

    def _call_raw_starting_from(self, start_webhook: str, method: str, params: Optional[dict]) -> Tuple[dict, str]:
        """
        Как call_raw, но возвращает (data, used_webhook).
        Нужно, чтобы уметь принудительно "перегенерировать" download URL другим webhook'ом.
        """
        webhook = start_webhook
        tried = set()

        while True:
            tried.add(webhook)
            try:
                data = self._call_raw_once(webhook, method, params)
                return data, webhook
            except requests.HTTPError as e:
                resp = getattr(e, "response", None)
                status = resp.status_code if resp is not None else None

                # 401/403 -> пробуем другой webhook (если есть)
                if status in (401, 403):
                    other = self._other_webhook(webhook)
                    if other and other not in tried:
                        webhook = other
                        continue
                raise

    def call_raw(self, method: str, params: Optional[dict] = None) -> dict:
        """
        Обычный REST вызов с fallback на второй webhook при 401/403.
        """
        data, _used = self._call_raw_starting_from(self.webhook_primary, method, params)
        return data

    # -------------------------
    # Disk helpers (важное!)
    # -------------------------

    def get_attached_object(self, attached_id: int, start_webhook: Optional[str] = None) -> Tuple[dict, str]:
        """
        Возвращает (attached_meta, used_webhook).
        """
        params = {"id": int(attached_id)}
        start = start_webhook or self.webhook_primary
        data, used = self._call_raw_starting_from(start, "disk.attachedObject.get", params)
        return (data.get("result") or {}), used

    def get_disk_file(self, file_id: int, start_webhook: Optional[str] = None) -> Tuple[dict, str]:
        """
        Возвращает (file_meta, used_webhook).
        """
        params = {"id": int(file_id)}
        start = start_webhook or self.webhook_primary
        data, used = self._call_raw_starting_from(start, "disk.file.get", params)
        return (data.get("result") or {}), used

    @staticmethod
    def _extract_download_url(meta: dict) -> Optional[str]:
        """
        Bitrix иногда по-разному называет поле. Достаём максимально терпимо.
        """
        if not meta:
            return None
        for k in ("DOWNLOAD_URL", "DOWNLOAD_URL_PUBLIC", "LINK_DOWNLOAD", "DOWNLOAD_URL_SHORT", "DOWNLOAD_URL_INTERNAL"):
            v = meta.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        # иногда лежит в nested
        for k in ("LINKS", "links"):
            v = meta.get(k)
            if isinstance(v, dict):
                for kk in ("DOWNLOAD", "download", "DOWNLOAD_URL"):
                    vv = v.get(kk)
                    if isinstance(vv, str) and vv.strip():
                        return vv.strip()
        return None

    def download_url(self, url: str, dst_path: str) -> None:
        """
        Скачивает по URL и сохраняет в dst_path.
        Не логирует URL (чтобы не светить auth/query).
        """
        url = self.absolutize(url)
        timeout = (self.connect_timeout_s, self.download_timeout_s)

        r = self._sess.get(url, stream=True, timeout=timeout)
        r.raise_for_status()

        with open(dst_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 128):
                if chunk:
                    f.write(chunk)

    def download_attached_object(self, attached_id: int, dst_path: str) -> dict:
        """
        “Продовый” путь: attachedId -> REST meta -> download URL -> download.
        Если скачивание по URL дало 401/403 — перегенерим meta другим webhook’ом и пробуем снова.
        Возвращает meta (attached/file), которое использовали.
        """
        # 1) сначала пробуем начиная с primary
        start = self.webhook_primary
        attempts = [start]
        if self.webhook_secondary:
            attempts.append(self.webhook_secondary)

        last_err: Optional[Exception] = None

        for start_webhook in attempts:
            try:
                attached_meta, used_webhook = self.get_attached_object(attached_id, start_webhook=start_webhook)
                url = self._extract_download_url(attached_meta)

                # если attached не даёт URL — пробуем через disk.file.get
                if not url:
                    # в attachedObject.get обычно есть OBJECT_ID (id disk файла)
                    file_id = attached_meta.get("OBJECT_ID") or attached_meta.get("FILE_ID")
                    if file_id:
                        file_meta, used_webhook2 = self.get_disk_file(int(file_id), start_webhook=used_webhook)
                        url = self._extract_download_url(file_meta)
                        if url:
                            url = self.absolutize(url)
                            self.download_url(url, dst_path)
                            self._assert_not_bitrix_json_error(dst_path)
                            return {"attached": attached_meta, "file": file_meta, "used_webhook": used_webhook2}

                if not url:
                    raise RuntimeError(f"Cannot find download url for attached_id={attached_id}")

                url = self.absolutize(url)
                self.download_url(url, dst_path)
                self._assert_not_bitrix_json_error(dst_path)
                return {"attached": attached_meta, "used_webhook": used_webhook}

            except Exception as e:
                last_err = e
                continue

        raise RuntimeError(f"Failed to download attached_id={attached_id}. Last error: {last_err}")

    @staticmethod
    def _assert_not_bitrix_json_error(path: str) -> None:
        """
        Bitrix иногда вместо файла отдаёт JSON с ошибкой (как у вас).
        Детектим и падаем, чтобы fallback мог попробовать другой webhook.
        """
        try:
            with open(path, "rb") as f:
                head = f.read(200)
        except Exception:
            return

        # если это XLSX — это ZIP, начинается с PK
        if head.startswith(b"PK\x03\x04"):
            return

        # если это json с ошибкой — ловим
        if head.startswith(b"{") and b"\"status\"" in head and b"error" in head:
            try:
                txt = head.decode("utf-8", "replace")
            except Exception:
                txt = str(head)
            raise RuntimeError(f"Downloaded file is Bitrix JSON error, not a real file. Head: {txt[:200]}")
