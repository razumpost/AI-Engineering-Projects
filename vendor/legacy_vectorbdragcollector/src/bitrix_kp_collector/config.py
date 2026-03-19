# src/bitrix_kp_collector/config.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from pydantic_settings import BaseSettings, SettingsConfigDict


class _Env(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    BITRIX_WEBHOOK_URL: str
    BITRIX_WEBHOOK_URL_2: Optional[str] = None

    BITRIX_TIMEOUT_S: int = 120
    BITRIX_CONNECT_TIMEOUT_S: int = 15
    BITRIX_MAX_RETRIES: int = 12
    BITRIX_RETRY_BACKOFF_S: float = 0.7
    BITRIX_IGNORE_ENV_PROXIES: int = 1

    SINCE_DT: str
    SINGLE_TASK_ID: Optional[int] = None

    INCLUDE_TASK_CHAT: bool = True
    CHAT_LIMIT: int = 10000
    CHAT_MAX_PAGES: int = 50

    # IM chats
    INCLUDE_IM_CHATS: bool = False
    CHAT_DIALOG_IDS: Optional[str] = None  # "chat85378,chat123"
    IM_CHAT_LIMIT: int = 50
    IM_CHAT_MAX_PAGES: int = 200

    DATABASE_URL: str
    DB_STATEMENT_TIMEOUT_MS: int = 0

    DOWNLOAD_DIR: str = "downloads"


@dataclass(frozen=True)
class Settings:
    webhook_primary: str
    webhook_secondary: Optional[str]

    timeout_s: int
    connect_timeout_s: int
    max_retries: int
    retry_backoff_s: float
    ignore_env_proxies: bool

    min_created_date: datetime
    single_task_id: Optional[int]

    include_task_chat: bool
    chat_limit: int
    chat_max_pages: int

    include_im_chats: bool
    chat_dialog_ids: List[str]
    im_chat_limit: int
    im_chat_max_pages: int

    db_url: str
    db_statement_timeout_ms: int

    download_dir: Path

    @staticmethod
    def from_env() -> "Settings":
        e = _Env()
        since = datetime.fromisoformat(e.SINCE_DT.replace("Z", "+00:00"))

        # parse chat ids
        ids: List[str] = []
        if e.CHAT_DIALOG_IDS:
            ids = [x.strip() for x in e.CHAT_DIALOG_IDS.split(",") if x.strip()]

        return Settings(
            webhook_primary=e.BITRIX_WEBHOOK_URL,
            webhook_secondary=e.BITRIX_WEBHOOK_URL_2,
            timeout_s=e.BITRIX_TIMEOUT_S,
            connect_timeout_s=e.BITRIX_CONNECT_TIMEOUT_S,
            max_retries=e.BITRIX_MAX_RETRIES,
            retry_backoff_s=float(e.BITRIX_RETRY_BACKOFF_S),
            ignore_env_proxies=bool(e.BITRIX_IGNORE_ENV_PROXIES),
            min_created_date=since,
            single_task_id=e.SINGLE_TASK_ID,
            include_task_chat=bool(e.INCLUDE_TASK_CHAT),
            chat_limit=int(e.CHAT_LIMIT),
            chat_max_pages=int(e.CHAT_MAX_PAGES),
            include_im_chats=bool(e.INCLUDE_IM_CHATS),
            chat_dialog_ids=ids,
            im_chat_limit=int(e.IM_CHAT_LIMIT),
            im_chat_max_pages=int(e.IM_CHAT_MAX_PAGES),
            db_url=e.DATABASE_URL,
            db_statement_timeout_ms=int(e.DB_STATEMENT_TIMEOUT_MS),
            download_dir=Path(e.DOWNLOAD_DIR),
        )
