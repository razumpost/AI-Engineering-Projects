from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Protocol

import requests


class ChatCompletionClient(Protocol):
    def complete(self, messages: list[dict[str, str]]) -> str: ...


_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def extract_json_object(text: str) -> dict[str, Any]:
    """
    Extract first JSON object from a string.
    LLM must return JSON-only, but we guard against wrappers.
    """
    if not text:
        raise ValueError("Empty LLM output")
    t = text.strip()
    if t.startswith("{") and t.endswith("}"):
        return json.loads(t)
    m = _JSON_RE.search(t)
    if not m:
        raise ValueError("No JSON object found in LLM output")
    return json.loads(m.group(0))


@dataclass(frozen=True)
class YandexFMConfig:
    endpoint: str
    model_uri: str
    api_key: str = ""
    iam_token: str = ""
    folder_id: str = ""
    timeout_s: float = 240.0
    connect_timeout_s: float = 30.0
    max_retries: int = 3
    temperature: float = 0.2
    max_tokens: int = 2000


class YandexFM(ChatCompletionClient):
    """
    Yandex AI Studio TextGeneration.Completion client.

    Endpoint: https://llm.api.cloud.yandex.net/foundationModels/v1/completion  (sync)
    Body keys: modelUri, completionOptions{stream,temperature,maxTokens}, messages[{role,text}]
    """
    def __init__(self, cfg: YandexFMConfig) -> None:
        self.cfg = cfg

    def _build_url(self) -> str:
        ep = (self.cfg.endpoint or "").rstrip("/")
        path = "/foundationModels/v1/completion"
        return ep if ep.endswith(path) else (ep + path)

    def _headers(self) -> dict[str, str]:
        h = {"Content-Type": "application/json"}

        # Prefer Api-Key if provided; otherwise Bearer IAM token.
        if self.cfg.api_key:
            h["Authorization"] = f"Api-Key {self.cfg.api_key}"
        elif self.cfg.iam_token:
            h["Authorization"] = f"Bearer {self.cfg.iam_token}"
        else:
            # Yandex returns 401/400; we still raise with body.
            h["Authorization"] = "Api-Key "

        # Optional but often helpful; harmless if extra.
        if self.cfg.folder_id:
            h["x-folder-id"] = self.cfg.folder_id

        # Disable data logging (optional)
        h["x-data-logging-enabled"] = "false"
        return h

    def _payload(self, messages: list[dict[str, str]]) -> dict[str, Any]:
        # Normalize roles/text to expected schema
        norm_msgs = []
        for m in messages:
            role = (m.get("role") or "user").strip()
            text = (m.get("text") or "").strip()
            norm_msgs.append({"role": role, "text": text})

        return {
            "modelUri": self.cfg.model_uri,
            "completionOptions": {
                "stream": False,
                "temperature": float(self.cfg.temperature),
                # docs show string; int also works in many clients, but string is safest
                "maxTokens": str(int(self.cfg.max_tokens)),
            },
            "messages": norm_msgs,
        }

    def complete(self, messages: list[dict[str, str]]) -> str:
        url = self._build_url()
        payload = self._payload(messages)
        headers = self._headers()

        last_err: Exception | None = None
        for _ in range(max(1, int(self.cfg.max_retries))):
            try:
                r = requests.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=(float(self.cfg.connect_timeout_s), float(self.cfg.timeout_s)),
                )
                if r.status_code >= 400:
                    # IMPORTANT: show server body (the actual reason for 400)
                    raise RuntimeError(
                        f"Yandex LLM HTTP {r.status_code}: {r.text[:2000]}"
                    )

                data = r.json()
                return (
                    data["result"]["alternatives"][0]["message"]["text"]
                )
            except Exception as e:
                last_err = e

        raise RuntimeError(f"LLM call failed: {last_err}")
