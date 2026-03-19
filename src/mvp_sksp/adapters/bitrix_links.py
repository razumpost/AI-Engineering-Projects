from __future__ import annotations

import os
from typing import Optional


def task_url(task_id: int, settings: Optional[object] = None) -> str:
    """Build Bitrix task URL; uses Settings.bitrix_base_url if present, else env BITRIX_BASE_URL."""
    base = None
    if settings is not None:
        base = getattr(settings, "bitrix_base_url", None)
    base = base or os.getenv("BITRIX_BASE_URL", "").rstrip("/")
    if not base:
        return f"bitrix://task/{task_id}"
    return f"{base}/company/personal/user/1/tasks/task/view/{task_id}/"
