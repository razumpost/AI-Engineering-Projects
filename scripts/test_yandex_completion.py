from __future__ import annotations

import json
import os
import sys
import urllib.request
from pathlib import Path

from dotenv import load_dotenv  # type: ignore


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_env() -> None:
    load_dotenv(dotenv_path=str(_repo_root() / ".env"), override=False)


def _env(name: str) -> str:
    v = (os.getenv(name) or "").strip()
    return v


def main() -> int:
    _load_env()

    endpoint = _env("YANDEX_FM_ENDPOINT") or _env("YC_FM_ENDPOINT")
    model_uri = _env("YANDEX_FM_MODEL_URI") or _env("YC_FM_MODEL_URI") or _env("YANDEX_GPT_MODEL")
    api_key = _env("YANDEX_FM_API_KEY") or _env("YC_API_KEY") or _env("YANDEX_API_KEY")
    iam_token = _env("YANDEX_FM_IAM_TOKEN") or _env("YC_IAM_TOKEN")
    folder_id = _env("YANDEX_FOLDER_ID") or _env("YC_FOLDER_ID")

    if not endpoint:
        print("ERROR: YANDEX_FM_ENDPOINT is empty")
        return 2
    if not model_uri:
        print("ERROR: YANDEX_FM_MODEL_URI (or YANDEX_GPT_MODEL) is empty")
        return 2
    if not (api_key or iam_token):
        print("ERROR: YANDEX_FM_API_KEY/YC_API_KEY or YANDEX_FM_IAM_TOKEN is empty")
        return 2

    headers = {"Content-Type": "application/json"}
    if iam_token:
        headers["Authorization"] = f"Bearer {iam_token}"
    else:
        headers["Authorization"] = f"Api-Key {api_key}"

    payload = {
        "modelUri": model_uri,
        "completionOptions": {"stream": False, "temperature": 0.2, "maxTokens": 96},
        "messages": [
            {"role": "system", "text": "Ты тестовый ассистент. Отвечай максимально коротко."},
            {"role": "user", "text": "Ответь одним словом: ОК"},
        ],
    }

    req = urllib.request.Request(
        endpoint,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )

    print("endpoint:", endpoint)
    print("modelUri:", model_uri)
    print("folder_id:", folder_id or "(empty)")
    print("auth:", "IAM_TOKEN" if iam_token else "API_KEY")

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print("HTTP", e.code, body)
        return 1
    except Exception as e:
        print("ERROR:", type(e).__name__, e)
        return 1

    try:
        data = json.loads(body)
    except Exception:
        print("RAW:", body[:2000])
        return 1

    # Most common response shape:
    # {"result":{"alternatives":[{"message":{"role":"assistant","text":"..."}}]}}
    text = None
    try:
        text = data["result"]["alternatives"][0]["message"]["text"]
    except Exception:
        pass

    print("\n--- response text ---")
    print(text if text is not None else json.dumps(data, ensure_ascii=False, indent=2)[:2000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())