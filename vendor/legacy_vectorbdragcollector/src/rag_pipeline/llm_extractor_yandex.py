# File: src/rag_pipeline/llm_extractor_yandex.py
import json
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


@dataclass(frozen=True)
class YandexJsonExtractorConfig:
    """
    Env:
      - YC_FOLDER_ID (or YANDEX_FOLDER_ID / YA_FOLDER_ID)
      - YC_API_KEY   (or YANDEX_API_KEY / YA_API_KEY) OR YC_IAM_TOKEN
      - YC_FM_ENDPOINT (or YA_GPT_ENDPOINT)
      - YC_FM_MODEL_URI (default: gpt://<folder>/yandexgpt/latest)
      - YC_TIMEOUT_S / YC_MAX_RETRIES / YC_RETRY_BACKOFF
    """
    folder_id: str
    api_key: str
    iam_token: str
    endpoint: str
    model_uri: str
    timeout_s: float
    max_retries: int
    retry_backoff: float

    @property
    def is_configured(self) -> bool:
        return bool(self.endpoint and self.model_uri and (self.api_key or self.iam_token))

    @staticmethod
    def from_env() -> "YandexJsonExtractorConfig":
        folder_id = (os.getenv("YC_FOLDER_ID") or "").strip()
        api_key = (os.getenv("YC_API_KEY") or "").strip()
        iam_token = (os.getenv("YC_IAM_TOKEN") or "").strip()

        endpoint = (os.getenv("YC_FM_ENDPOINT") or "").strip()
        model_uri = (os.getenv("YC_FM_MODEL_URI") or "").strip()

        if not folder_id:
            folder_id = (os.getenv("YANDEX_FOLDER_ID") or os.getenv("YA_FOLDER_ID") or "").strip()
        if not api_key:
            api_key = (os.getenv("YANDEX_API_KEY") or os.getenv("YA_API_KEY") or "").strip()
        if not endpoint:
            endpoint = (os.getenv("YA_GPT_ENDPOINT") or "https://llm.api.cloud.yandex.net/foundationModels/v1/completion").strip()

        if not model_uri and folder_id:
            model_uri = f"gpt://{folder_id}/yandexgpt/latest"

        timeout_s = float(os.getenv("YC_TIMEOUT_S", os.getenv("YANDEX_TIMEOUT_S", "40")))
        max_retries = int(os.getenv("YC_MAX_RETRIES", os.getenv("YANDEX_MAX_RETRIES", "4")))
        retry_backoff = float(os.getenv("YC_RETRY_BACKOFF", os.getenv("YANDEX_RETRY_BACKOFF", "1.3")))

        return YandexJsonExtractorConfig(
            folder_id=folder_id,
            api_key=api_key,
            iam_token=iam_token,
            endpoint=endpoint,
            model_uri=model_uri,
            timeout_s=timeout_s,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
        )


class YandexJsonExtractor:
    """
    JSON-only extraction via Yandex completion API.

    Hard rules (enforced via prompt):
      - Return STRICT JSON, no markdown
      - unit_price must be null unless price is explicitly present in rag_context
      - Do not output string "null" (use JSON null)
      - Respect INTENT if present (SOFTWARE/VIDEOWALL/GENERAL)
    """

    def __init__(self, cfg: YandexJsonExtractorConfig) -> None:
        if not cfg.is_configured:
            raise RuntimeError("YandexJsonExtractorConfig is not configured (missing creds/model)")
        self.cfg = cfg

    def generate_sksp(
        self,
        *,
        transcript: str,
        rag_context: str,
        max_items: int = 60,
        max_questions: int = 8,
    ) -> Dict[str, Any]:
        schema = {
            "type": "object",
            "properties": {
                "sksp_title": {"type": "string"},
                "items": {
                    "type": "array",
                    "maxItems": max_items,
                    "items": {
                        "type": "object",
                        "properties": {
                            "group": {"type": "string"},
                            "vendor": {"type": "string"},
                            "article": {"type": "string"},
                            "description": {"type": "string"},
                            "qty": {"type": "number"},
                            "unit_price": {"type": ["number", "null"]},
                            "delivery": {"type": "string"},
                            "comment": {"type": "string"},
                            "link": {"type": "string"},
                            "supplier": {"type": "string"},
                            "registration": {"type": "string"},
                            "payment_terms": {"type": "string"},
                        },
                        "required": ["description", "qty", "unit_price"],
                        "additionalProperties": False,
                    },
                },
                "questions": {"type": "array", "items": {"type": "string"}, "maxItems": max_questions},
            },
            "required": ["items", "questions"],
            "additionalProperties": False,
        }

        system = (
            "Ты — ассистент пресейла AV/IT. "
            "Верни СТРОГО JSON без Markdown, без пояснений, без комментариев вне JSON. "
            "Нельзя писать строку \"null\" — только JSON null. "
            "Цены/артикулы/поставщиков НЕ выдумывать: брать только из rag_context. "
            "Если цены нет в rag_context — unit_price=null. "
            "Если INTENT=SOFTWARE — НЕ включать видеопанели/крепления/видеостены (только ПО/сервер/плееры/лицензии). "
            "Если INTENT=VIDEOWALL — можно включать панели/контроллер/крепления. "
            "items должны быть короткими и практичными, 5..25 позиций для MVP."
        )

        user = (
            f"jsonSchema: {json.dumps(schema, ensure_ascii=False)}\n\n"
            f"TRANSCRIPT:\n{transcript}\n\n"
            f"RAG_CONTEXT:\n{rag_context}\n\n"
            "Сформируй items и questions. "
            "В questions задавай уточнения только по сути (размер/модель/лицензии/топология/контент/сроки/бюджет)."
        )

        payload = {
            "modelUri": self.cfg.model_uri,
            "completionOptions": {"stream": False, "temperature": 0.15, "maxTokens": "2600"},
            "messages": [{"role": "system", "text": system}, {"role": "user", "text": user}],
        }

        raw = self._post(payload)
        obj = _parse_json_object(raw)
        _validate_sksp(obj, max_items=max_items, max_questions=max_questions)
        return obj

    def _post(self, payload: Dict[str, Any]) -> str:
        headers = {"Content-Type": "application/json"}
        if self.cfg.api_key:
            headers["Authorization"] = f"Api-Key {self.cfg.api_key}"
        else:
            headers["Authorization"] = f"Bearer {self.cfg.iam_token}"

        last_err: Optional[Exception] = None
        for attempt in range(self.cfg.max_retries + 1):
            try:
                resp = requests.post(
                    self.cfg.endpoint,
                    headers=headers,
                    data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                    timeout=self.cfg.timeout_s,
                )
                if resp.status_code >= 400:
                    body = resp.text[:2000]
                    if resp.status_code in {400, 401, 403}:
                        raise RuntimeError(f"Yandex completion HTTP {resp.status_code}: {body}")
                    raise requests.HTTPError(f"HTTP {resp.status_code}: {body}")

                data = resp.json()
                return _extract_text(data)

            except Exception as e:
                last_err = e
                if attempt >= self.cfg.max_retries:
                    break
                time.sleep(min(30.0, (self.cfg.retry_backoff**attempt) + random.random()))

        raise RuntimeError(f"Yandex completion failed after retries: {last_err}") from last_err


def _extract_text(j: Dict[str, Any]) -> str:
    for path in [
        ("result", "alternatives", 0, "message", "text"),
        ("alternatives", 0, "message", "text"),
        ("result", "text"),
        ("text",),
    ]:
        cur: Any = j
        ok = True
        for k in path:
            if isinstance(k, int):
                if not (isinstance(cur, list) and 0 <= k < len(cur)):
                    ok = False
                    break
                cur = cur[k]
            else:
                if not (isinstance(cur, dict) and k in cur):
                    ok = False
                    break
                cur = cur[k]
        if ok and isinstance(cur, str):
            return cur
    return json.dumps(j, ensure_ascii=False)


def _parse_json_object(text: str) -> Dict[str, Any]:
    s = (text or "").strip()
    s = s.strip("`").strip()
    if s.lower().startswith("json"):
        s = s[4:].strip()

    l = s.find("{")
    r = s.rfind("}")
    if l != -1 and r != -1 and r > l:
        s = s[l : r + 1]

    obj = json.loads(s)
    if not isinstance(obj, dict):
        raise RuntimeError("Extractor returned non-object JSON")
    return obj


def _validate_sksp(obj: Dict[str, Any], *, max_items: int, max_questions: int) -> None:
    items = obj.get("items")
    if not isinstance(items, list):
        raise RuntimeError("JSON must include 'items' array")
    if len(items) > max_items:
        obj["items"] = items[:max_items]

    qs = obj.get("questions")
    if not isinstance(qs, list):
        obj["questions"] = []
    elif len(qs) > max_questions:
        obj["questions"] = qs[:max_questions]