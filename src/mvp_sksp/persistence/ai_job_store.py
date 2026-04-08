# src/mvp_sksp/persistence/ai_job_store.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _load_env_file() -> dict[str, str]:
    env_path = _repo_root() / ".env"
    if not env_path.exists():
        return {}
    out: dict[str, str] = {}
    for raw in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


_ENV = _load_env_file()


def _get_env(*names: str) -> str:
    for n in names:
        v = os.getenv(n)
        if v and str(v).strip():
            return str(v).strip()
    for n in names:
        v = _ENV.get(n)
        if v and str(v).strip():
            return str(v).strip()
    return ""


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


@dataclass(frozen=True)
class JobRef:
    job_id: int
    iteration_id: int


class AIJobStore:
    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = (dsn or _get_env("DATABASE_URL", "DB_DSN")).strip()
        if not self._dsn:
            raise RuntimeError("DATABASE_URL/DB_DSN is empty. Run: set -a; source .env; set +a")
        self._engine: Engine = create_engine(self._dsn, future=True, pool_pre_ping=True)

    @property
    def engine(self) -> Engine:
        return self._engine

    def init_schema(self) -> None:
        ddl = [
            """
            create table if not exists ai_jobs (
              id bigserial primary key,
              deal_id text not null,
              activity_id text null,
              manager_id text null,
              status text not null default 'created',
              transcript_text text null,
              transcript_meta jsonb not null default '{}'::jsonb,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            )
            """,
            """
            create index if not exists ix_ai_jobs_deal_id on ai_jobs(deal_id)
            """,
            """
            create table if not exists ai_iterations (
              id bigserial primary key,
              job_id bigint not null references ai_jobs(id) on delete cascade,
              iteration_no integer not null,
              status text not null default 'draft',
              request_text text null,
              requirements_json jsonb not null default '{}'::jsonb,
              confidence_map jsonb not null default '{}'::jsonb,
              topology_json jsonb not null default '{}'::jsonb,
              summary_json jsonb not null default '{}'::jsonb,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now(),
              unique(job_id, iteration_no)
            )
            """,
            """
            create index if not exists ix_ai_iterations_job_id on ai_iterations(job_id)
            """,
            """
            create table if not exists ai_artifacts (
              id bigserial primary key,
              iteration_id bigint not null references ai_iterations(id) on delete cascade,
              artifact_type text not null,
              path text not null,
              mime_type text null,
              payload_json jsonb not null default '{}'::jsonb,
              created_at timestamptz not null default now()
            )
            """,
            """
            create index if not exists ix_ai_artifacts_iteration_id on ai_artifacts(iteration_id)
            """,
            """
            create table if not exists ai_evidence (
              id bigserial primary key,
              iteration_id bigint not null references ai_iterations(id) on delete cascade,
              line_id text not null,
              line_no integer null,
              source_kind text not null,
              source_ref text null,
              vendor text null,
              sku text null,
              model text null,
              path text null,
              evidence_date timestamptz null,
              payload_json jsonb not null default '{}'::jsonb,
              created_at timestamptz not null default now()
            )
            """,
            """
            create index if not exists ix_ai_evidence_iteration_id on ai_evidence(iteration_id)
            """,
            """
            create index if not exists ix_ai_evidence_line_id on ai_evidence(line_id)
            """,
        ]
        with self._engine.begin() as conn:
            for stmt in ddl:
                conn.execute(text(stmt))

    def create_job(
        self,
        *,
        deal_id: str,
        activity_id: str | None,
        manager_id: str | None,
        transcript_text: str,
        transcript_meta: dict[str, Any],
        status: str = "created",
    ) -> int:
        sql = text(
            """
            insert into ai_jobs(
              deal_id, activity_id, manager_id, status, transcript_text, transcript_meta
            )
            values(
              :deal_id, :activity_id, :manager_id, :status, :transcript_text, cast(:transcript_meta as jsonb)
            )
            returning id
            """
        )
        with self._engine.begin() as conn:
            return int(
                conn.execute(
                    sql,
                    {
                        "deal_id": str(deal_id),
                        "activity_id": activity_id,
                        "manager_id": manager_id,
                        "status": status,
                        "transcript_text": transcript_text,
                        "transcript_meta": _json_dumps(transcript_meta),
                    },
                ).scalar_one()
            )

    def update_job_status(self, job_id: int, status: str) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    """
                    update ai_jobs
                    set status = :status, updated_at = now()
                    where id = :job_id
                    """
                ),
                {"job_id": int(job_id), "status": status},
            )

    def next_iteration_no(self, job_id: int) -> int:
        with self._engine.begin() as conn:
            v = conn.execute(
                text("select coalesce(max(iteration_no), 0) + 1 from ai_iterations where job_id = :job_id"),
                {"job_id": int(job_id)},
            ).scalar_one()
        return int(v)

    def create_iteration(
        self,
        *,
        job_id: int,
        request_text: str,
        requirements_json: dict[str, Any],
        confidence_map: dict[str, Any],
        topology_json: dict[str, Any],
        summary_json: dict[str, Any],
        status: str = "draft",
    ) -> int:
        iteration_no = self.next_iteration_no(job_id)
        sql = text(
            """
            insert into ai_iterations(
              job_id, iteration_no, status, request_text, requirements_json, confidence_map, topology_json, summary_json
            )
            values(
              :job_id, :iteration_no, :status, :request_text,
              cast(:requirements_json as jsonb),
              cast(:confidence_map as jsonb),
              cast(:topology_json as jsonb),
              cast(:summary_json as jsonb)
            )
            returning id
            """
        )
        with self._engine.begin() as conn:
            return int(
                conn.execute(
                    sql,
                    {
                        "job_id": int(job_id),
                        "iteration_no": int(iteration_no),
                        "status": status,
                        "request_text": request_text,
                        "requirements_json": _json_dumps(requirements_json),
                        "confidence_map": _json_dumps(confidence_map),
                        "topology_json": _json_dumps(topology_json),
                        "summary_json": _json_dumps(summary_json),
                    },
                ).scalar_one()
            )

    def update_iteration_status(self, iteration_id: int, status: str) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    """
                    update ai_iterations
                    set status = :status, updated_at = now()
                    where id = :iteration_id
                    """
                ),
                {"iteration_id": int(iteration_id), "status": status},
            )

    def add_artifact(
        self,
        *,
        iteration_id: int,
        artifact_type: str,
        path: str,
        mime_type: str | None = None,
        payload_json: Optional[dict[str, Any]] = None,
    ) -> int:
        sql = text(
            """
            insert into ai_artifacts(iteration_id, artifact_type, path, mime_type, payload_json)
            values(:iteration_id, :artifact_type, :path, :mime_type, cast(:payload_json as jsonb))
            returning id
            """
        )
        with self._engine.begin() as conn:
            return int(
                conn.execute(
                    sql,
                    {
                        "iteration_id": int(iteration_id),
                        "artifact_type": artifact_type,
                        "path": path,
                        "mime_type": mime_type,
                        "payload_json": _json_dumps(payload_json or {}),
                    },
                ).scalar_one()
            )

    def add_evidence(
        self,
        *,
        iteration_id: int,
        line_id: str,
        line_no: int | None,
        source_kind: str,
        source_ref: str | None,
        vendor: str | None,
        sku: str | None,
        model: str | None,
        path: str | None,
        evidence_date: str | None,
        payload_json: Optional[dict[str, Any]] = None,
    ) -> int:
        sql = text(
            """
            insert into ai_evidence(
              iteration_id, line_id, line_no, source_kind, source_ref, vendor, sku, model, path, evidence_date, payload_json
            )
            values(
              :iteration_id, :line_id, :line_no, :source_kind, :source_ref, :vendor, :sku, :model, :path,
              :evidence_date, cast(:payload_json as jsonb)
            )
            returning id
            """
        )
        with self._engine.begin() as conn:
            return int(
                conn.execute(
                    sql,
                    {
                        "iteration_id": int(iteration_id),
                        "line_id": line_id,
                        "line_no": line_no,
                        "source_kind": source_kind,
                        "source_ref": source_ref,
                        "vendor": vendor,
                        "sku": sku,
                        "model": model,
                        "path": path,
                        "evidence_date": evidence_date,
                        "payload_json": _json_dumps(payload_json or {}),
                    },
                ).scalar_one()
            )