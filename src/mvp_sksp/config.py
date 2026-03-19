from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _repo_root() -> Path:
    # /repo/src/mvp_sksp/config.py -> parents[2] = /repo
    return Path(__file__).resolve().parents[2]


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        if not k:
            continue
        v = v.strip().strip('"').strip("'")
        if "#" in v and not (v.startswith("http://") or v.startswith("https://")):
            v = v.split("#", 1)[0].strip()
        if k not in out or (not out[k] and v):
            out[k] = v
    return out


_ENV = _read_env_file(_repo_root() / ".env")


def _get(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip():
            return str(v).strip()
    for n in names:
        v = _ENV.get(n)
        if v is not None and str(v).strip():
            return str(v).strip()
    return default


def _get_int(*names: str, default: int) -> int:
    v = _get(*names, default="")
    try:
        return int(v) if v else default
    except Exception:
        return default


def _sanitize_model_uri(value: str, folder_id: str) -> str:
    v = (value or "").strip().strip('"').strip("'")
    if "gpt://" in v:
        v = v[v.index("gpt://") :].strip().strip('"').strip("'")
    if (not v or v in {"yandexgpt-latest", "yandexgpt"}) and folder_id:
        v = f"gpt://{folder_id}/yandexgpt/latest"
    return v


@dataclass(frozen=True)
class Settings:
    run_dir: str = _get("RUN_DIR", "DOWNLOAD_DIR", default="downloads/mvp_runs")
    bitrix_base_url: str = _get("BITRIX_BASE_URL", default="").rstrip("/")

    yandex_folder_id: str = _get("YANDEX_FOLDER_ID", "YC_FOLDER_ID", "YA_FOLDER_ID", default="")
    yandex_fm_endpoint: str = _get(
        "YANDEX_FM_ENDPOINT",
        "YC_FM_ENDPOINT",
        "YA_GPT_ENDPOINT",
        default="https://llm.api.cloud.yandex.net/foundationModels/v1/completion",
    )

    _raw_model_uri: str = _get("YANDEX_FM_MODEL_URI", "YC_FM_MODEL_URI", "YANDEX_GPT_MODEL", default="")
    yandex_fm_model_uri: str = _sanitize_model_uri(_raw_model_uri, yandex_folder_id)

    yandex_fm_api_key: str = _get("YANDEX_FM_API_KEY", "YC_API_KEY", "YANDEX_API_KEY", default="")
    yandex_fm_iam_token: str = _get("YANDEX_FM_IAM_TOKEN", "YC_IAM_TOKEN", default="")

    llm_timeout_s: float = float(_get("YC_FM_TIMEOUT_S", "YANDEX_FM_TIMEOUT_S", default="240") or "240")
    llm_connect_timeout_s: float = float(_get("YC_FM_CONNECT_TIMEOUT_S", "YANDEX_FM_CONNECT_TIMEOUT_S", default="30") or "30")
    llm_max_retries: int = _get_int("YC_FM_MAX_RETRIES", "YANDEX_FM_MAX_RETRIES", default=3)
