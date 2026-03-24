from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Any, Protocol

import requests


class ChatCompletionClient(Protocol):
    def complete(self, messages: list[dict[str, Any]]) -> str: ...


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL | re.IGNORECASE)


def extract_json_object(text: str) -> dict[str, Any]:
    """
    Extract first JSON object from an LLM output.
    Returns parsed dict (not a string) to keep orchestrator stable.
    """
    if not text or not str(text).strip():
        raise ValueError("Empty LLM output")

    t = str(text).strip()

    m = _JSON_FENCE_RE.search(t)
    if m:
        return json.loads(m.group(1))

    start = t.find("{")
    if start < 0:
        raise ValueError("No JSON object found in LLM output")

    dec = json.JSONDecoder()
    obj, _end = dec.raw_decode(t[start:])

    if not isinstance(obj, dict):
        raise ValueError(f"Expected JSON object(dict), got {type(obj).__name__}")
    return obj


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
    Supports two backends depending on endpoint:

    1) OpenAI-compatible (DeepSeek via YC AI Studio):
       endpoint = https://llm.api.cloud.yandex.net/v1
       POST /chat/completions
       model = gpt://<folder_id>/deepseek-v32/latest

    2) Legacy FoundationModels:
       endpoint = https://llm.api.cloud.yandex.net/foundationModels/v1/completion
       POST /foundationModels/v1/completion
       modelUri = gpt://<folder_id>/yandexgpt/latest
    """

    def __init__(self, cfg: YandexFMConfig) -> None:
        self.cfg = cfg

    def complete(self, messages: list[dict[str, Any]]) -> str:
        last_err: Exception | None = None
        for attempt in range(max(1, int(self.cfg.max_retries))):
            try:
                if "/foundationModels/" in (self.cfg.endpoint or ""):
                    return self._complete_foundation_models(messages)
                return self._complete_openai_compat(messages)
            except Exception as e:
                last_err = e
                time.sleep(0.6 * (attempt + 1))
        raise RuntimeError(f"LLM call failed: {last_err}")

    # ---------------- OpenAI-compatible ----------------

    def _complete_openai_compat(self, messages: list[dict[str, Any]]) -> str:
        base = (self.cfg.endpoint or "").rstrip("/")
        url = f"{base}/chat/completions"

        payload = {
            "model": self.cfg.model_uri,
            "messages": [self._to_openai_msg(m) for m in messages],
            "temperature": float(self.cfg.temperature),
        }

        # Try Bearer first (works for many YC setups), then Api-Key fallback.
        r = requests.post(
            url,
            headers=self._headers_bearer(),
            json=payload,
            timeout=(float(self.cfg.connect_timeout_s), float(self.cfg.timeout_s)),
        )
        if r.status_code in (401, 403):
            r = requests.post(
                url,
                headers=self._headers_api_key(),
                json=payload,
                timeout=(float(self.cfg.connect_timeout_s), float(self.cfg.timeout_s)),
            )

        if r.status_code >= 400:
            raise RuntimeError(f"Yandex OpenAI-compat HTTP {r.status_code}: {r.text}")

        data = r.json()
        try:
            return data["choices"][0]["message"]["content"]
        except Exception:
            raise RuntimeError(f"Unexpected OpenAI-compat response: {json.dumps(data, ensure_ascii=False)[:2000]}")

    def _to_openai_msg(self, m: dict[str, Any]) -> dict[str, Any]:
        role = m.get("role", "user")
        content = m.get("content")
        if content is None:
            content = m.get("text", "")
        return {"role": role, "content": content}

    def _headers_bearer(self) -> dict[str, str]:
        h = {"Content-Type": "application/json"}
        token = (self.cfg.api_key or "").strip()
        iam = (self.cfg.iam_token or "").strip()
        if token:
            h["Authorization"] = f"Bearer {token}"
        elif iam:
            h["Authorization"] = f"Bearer {iam}"
        return h

    def _headers_api_key(self) -> dict[str, str]:
        h = {"Content-Type": "application/json"}
        token = (self.cfg.api_key or "").strip()
        iam = (self.cfg.iam_token or "").strip()
        if token:
            h["Authorization"] = f"Api-Key {token}"
        elif iam:
            h["Authorization"] = f"Bearer {iam}"
        return h

    # ---------------- Legacy FoundationModels ----------------

    def _complete_foundation_models(self, messages: list[dict[str, Any]]) -> str:
        url = (self.cfg.endpoint or "").rstrip("/")

        payload = {
            "modelUri": self.cfg.model_uri,
            "completionOptions": {
                "stream": False,
                "temperature": float(self.cfg.temperature),
                "maxTokens": int(self.cfg.max_tokens),
            },
            "messages": [
                {"role": m.get("role", "user"), "text": m.get("text") or m.get("content", "")} for m in messages
            ],
        }

        r = requests.post(
            url,
            headers=self._headers_api_key(),
            json=payload,
            timeout=(float(self.cfg.connect_timeout_s), float(self.cfg.timeout_s)),
        )
        if r.status_code >= 400:
            raise RuntimeError(f"Yandex FoundationModels HTTP {r.status_code}: {r.text}")

        data = r.json()
        try:
            return data["result"]["alternatives"][0]["message"]["text"]
        except Exception:
            raise RuntimeError(f"Unexpected FoundationModels response: {json.dumps(data, ensure_ascii=False)[:2000]}")