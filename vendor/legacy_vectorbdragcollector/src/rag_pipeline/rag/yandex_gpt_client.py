# File: src/rag_pipeline/rag/yandex_gpt_client.py
from __future__ import annotations

import os
from typing import Dict, List

import requests

from ..security import ExternalSafetyConfig, redact_sensitive, safe_for_external


class YandexGPTClient:
    """
    Minimal YaGPT wrapper with outbound safety redaction.

    Supports env:
      - YA_API_KEY or YANDEX_API_KEY
      - YA_FOLDER_ID or YANDEX_FOLDER_ID
      - YA_GPT_MODEL / YANDEX_GPT_MODEL (default: yandexgpt-lite)
    """

    def __init__(self) -> None:
        self.api_key = os.getenv("YA_API_KEY") or os.getenv("YANDEX_API_KEY") or ""
        self.folder_id = os.getenv("YA_FOLDER_ID") or os.getenv("YANDEX_FOLDER_ID") or ""
        self.model = os.getenv("YA_GPT_MODEL") or os.getenv("YANDEX_GPT_MODEL") or os.getenv("YANDEX_GPT_MODEL", "yandexgpt-lite")
        self._cfg = ExternalSafetyConfig()

        if not self.api_key or not self.folder_id:
            raise RuntimeError("Missing YA_API_KEY/YA_FOLDER_ID (or YANDEX_API_KEY/YANDEX_FOLDER_ID)")

    def complete(self, messages: List[Dict], temperature: float = 0.2, max_tokens: int = 1200) -> str:
        url = os.getenv("YA_GPT_ENDPOINT", "https://llm.api.cloud.yandex.net/foundationModels/v1/completion")
        headers = {
            "Authorization": f"Api-Key {self.api_key}",
            "Content-Type": "application/json",
        }

        safe_messages: List[Dict] = []
        for m in messages:
            txt = m.get("text", "") if isinstance(m, dict) else ""
            safe_txt = safe_for_external(txt, self._cfg) if self._cfg.redact else (txt or "")
            safe_messages.append({"role": m.get("role"), "text": safe_txt})

        payload = {
            "modelUri": f"gpt://{self.folder_id}/{self.model}",
            "completionOptions": {
                "stream": False,
                "temperature": float(temperature),
                "maxTokens": int(max_tokens),
            },
            "messages": safe_messages,
        }

        r = requests.post(url, headers=headers, json=payload, timeout=120)
        r.raise_for_status()
        data = r.json()

        text = data["result"]["alternatives"][0]["message"]["text"]
        if self._cfg.redact_output:
            text = redact_sensitive(text, mode=self._cfg.mode)
        return text
