# src/mvp_sksp/services/engineer_service.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv  # type: ignore

from ..adapters.deal_postgres_store import PostgresDealStore
from ..config import Settings
from ..llm.client import YandexFM, YandexFMConfig
from ..llm.prompts import compose_prompt
from ..persistence.ai_job_store import AIJobStore
from ..persistence.snapshot_store import SnapshotPaths, make_run_paths, save_iter, save_text, update_last_valid
from ..pipeline.deal_retrieval import build_candidate_pool_for_deal
from ..pipeline.draft_seed import seed_spec_from_role_candidates
from ..pipeline.export import export_xlsx, render_markdown
from ..pipeline.orchestrator import compose
from ..pipeline.postprocess import postprocess_spec
from ..planning.coverage_planner import build_filtered_pool_for_coverage
from ..planning.requirements import parse_requirements
from ..planning.role_expander import expand_required_roles
from ..planning.topology_selector import select_topology


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _load_env() -> None:
    load_dotenv(dotenv_path=str(_repo_root() / ".env"), override=False)


def _now_tag() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _safe_json(obj: Any) -> dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    try:
        if hasattr(obj, "model_dump"):
            return obj.model_dump(mode="json")  # type: ignore[no-any-return]
    except Exception:
        pass
    return {"value": obj}


@dataclass(frozen=True)
class EngineerRunResult:
    job_id: int
    iteration_id: int
    run_dir: str
    markdown_path: str
    xlsx_path: str
    requirements_json_path: str
    evidence_json_path: str


class EngineerService:
    def __init__(
        self,
        *,
        settings: Optional[Settings] = None,
        ai_store: Optional[AIJobStore] = None,
        deal_store: Optional[PostgresDealStore] = None,
    ) -> None:
        _load_env()
        self.settings = settings or Settings()
        self.ai_store = ai_store or AIJobStore()
        self.ai_store.init_schema()
        self.deal_store = deal_store or PostgresDealStore()

    def _load_transcript(
        self,
        *,
        deal_id: str,
        activity_id: str | None = None,
        transcript_text: str | None = None,
        transcript_file: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        if transcript_text and transcript_text.strip():
            return transcript_text.strip(), {"source": "inline", "deal_id": deal_id, "activity_id": activity_id}

        if transcript_file:
            p = Path(transcript_file).expanduser().resolve()
            return p.read_text(encoding="utf-8", errors="replace").strip(), {
                "source": "file",
                "deal_id": deal_id,
                "activity_id": activity_id,
                "path": str(p),
            }

        text, meta = self.deal_store.get_best_transcript_for_deal(deal_id, activity_id=activity_id)
        meta["source"] = "db"
        return text, meta

    def _make_llm(self) -> YandexFM:
        s = self.settings
        return YandexFM(
            YandexFMConfig(
                endpoint=s.yandex_fm_endpoint,
                model_uri=s.yandex_fm_model_uri,
                api_key=s.yandex_fm_api_key,
                iam_token=s.yandex_fm_iam_token,
                folder_id=s.yandex_folder_id,
                timeout_s=float(s.llm_timeout_s),
                connect_timeout_s=float(s.llm_connect_timeout_s),
                max_retries=int(s.llm_max_retries),
            )
        )

    def _make_run_paths(self, run_dir: str | None, deal_id: str, job_id: int) -> SnapshotPaths:
        base_dir = Path(run_dir or self.settings.run_dir).expanduser()
        full_dir = base_dir / f"deal_{deal_id}" / f"job_{job_id}_{_now_tag()}"
        return make_run_paths(str(full_dir))

    def _write_text_artifact(self, path: Path, text_value: str) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text_value or "", encoding="utf-8")
        return path

    def _write_json_artifact(self, path: Path, obj: Any) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return path

    def _candidate_index(self, pool) -> dict[str, Any]:
        return {c.candidate_id: c for c in getattr(pool, "items", []) or []}

    def _find_candidate_for_line(self, line, pool_items_by_id: dict[str, Any]) -> Any | None:
        meta = getattr(line, "meta", None)
        if isinstance(meta, dict) and meta.get("candidate_id"):
            return pool_items_by_id.get(str(meta["candidate_id"]))

        sku = (getattr(line, "sku", None) or "").strip()
        manufacturer = (getattr(line, "manufacturer", None) or "").strip().casefold()
        model = (getattr(line, "model", None) or "").strip().casefold()

        for c in pool_items_by_id.values():
            if sku and (getattr(c, "sku", None) or "").strip() == sku:
                return c

        for c in pool_items_by_id.values():
            c_man = (getattr(c, "manufacturer", None) or "").strip().casefold()
            c_mod = (getattr(c, "model", None) or "").strip().casefold()
            if manufacturer and model and manufacturer == c_man and model == c_mod:
                return c

        desc = ((getattr(line, "description", None) or "") + " " + (getattr(line, "name", None) or "")).casefold()
        for c in pool_items_by_id.values():
            c_desc = ((getattr(c, "description", None) or "") + " " + (getattr(c, "name", None) or "")).casefold()
            if desc and c_desc and desc[:120] == c_desc[:120]:
                return c

        return None

    def _persist_evidence(self, *, iteration_id: int, spec, pool) -> None:
        pool_items_by_id = self._candidate_index(pool)

        for idx, line in enumerate(getattr(spec, "items", []) or [], start=1):
            candidate = self._find_candidate_for_line(line, pool_items_by_id)
            notes = list(getattr(getattr(line, "evidence", None), "notes", []) or [])
            task_ids = list(getattr(getattr(line, "evidence", None), "bitrix_task_ids", []) or [])

            # 1) evidence rows from Bitrix task ids
            for tid in task_ids:
                self.ai_store.add_evidence(
                    iteration_id=iteration_id,
                    line_id=str(getattr(line, "line_id", f"line_{idx}")),
                    line_no=idx,
                    source_kind="task",
                    source_ref=f"bitrix_task:{tid}",
                    vendor=getattr(line, "manufacturer", None),
                    sku=getattr(line, "sku", None),
                    model=getattr(line, "model", None),
                    path=(candidate.meta.get("path") if candidate and isinstance(candidate.meta, dict) else None),
                    evidence_date=None,
                    payload_json={
                        "reasoning": getattr(line, "reasoning", ""),
                        "candidate_id": (candidate.candidate_id if candidate else None),
                        "price_source": (candidate.price_source if candidate else None),
                    },
                )

            # 2) supplier / sksp / generic candidate evidence
            if candidate is not None:
                meta = getattr(candidate, "meta", {}) or {}
                source_kind = "candidate"
                source_ref = str(candidate.candidate_id)

                price_source = (getattr(candidate, "price_source", None) or "").strip().casefold()
                if "supplier" in price_source or "price" in price_source or "прайс" in price_source:
                    source_kind = "supplier_price"
                elif "sksp" in price_source or "сксп" in price_source:
                    source_kind = "sksp_fallback"

                self.ai_store.add_evidence(
                    iteration_id=iteration_id,
                    line_id=str(getattr(line, "line_id", f"line_{idx}")),
                    line_no=idx,
                    source_kind=source_kind,
                    source_ref=source_ref,
                    vendor=getattr(candidate, "manufacturer", None),
                    sku=getattr(candidate, "sku", None),
                    model=getattr(candidate, "model", None),
                    path=(meta.get("path") or meta.get("local_path")),
                    evidence_date=str(meta.get("date") or meta.get("updated_at") or "") or None,
                    payload_json={
                        "candidate_meta": meta,
                        "line_notes": notes,
                        "line_reasoning": getattr(line, "reasoning", ""),
                        "unit_price_rub": (
                            str(getattr(candidate, "unit_price_rub", None))
                            if getattr(candidate, "unit_price_rub", None) is not None
                            else None
                        ),
                        "price_source": getattr(candidate, "price_source", None),
                    },
                )
            else:
                # 3) at least one row so every line has evidence
                self.ai_store.add_evidence(
                    iteration_id=iteration_id,
                    line_id=str(getattr(line, "line_id", f"line_{idx}")),
                    line_no=idx,
                    source_kind="line_fallback",
                    source_ref=None,
                    vendor=getattr(line, "manufacturer", None),
                    sku=getattr(line, "sku", None),
                    model=getattr(line, "model", None),
                    path=None,
                    evidence_date=None,
                    payload_json={
                        "line_notes": notes,
                        "line_reasoning": getattr(line, "reasoning", ""),
                        "used_bitrix_task_ids": task_ids,
                    },
                )

    def run(
        self,
        *,
        deal_id: str,
        activity_id: str | None = None,
        transcript_text: str | None = None,
        transcript_file: str | None = None,
        manager_id: str | None = None,
        run_dir: str | None = None,
        include_global: bool = True,
    ) -> EngineerRunResult:
        transcript, transcript_meta = self._load_transcript(
            deal_id=deal_id,
            activity_id=activity_id,
            transcript_text=transcript_text,
            transcript_file=transcript_file,
        )
        if not transcript.strip():
            raise RuntimeError(f"Empty transcript for deal_id={deal_id}")

        job_id = self.ai_store.create_job(
            deal_id=str(deal_id),
            activity_id=activity_id,
            manager_id=manager_id,
            transcript_text=transcript,
            transcript_meta=transcript_meta,
            status="running",
        )

        run = self._make_run_paths(run_dir, str(deal_id), job_id)
        save_text(run, "transcript", transcript)

        requirements = parse_requirements(transcript)
        topology = select_topology(requirements)
        role_plan = expand_required_roles(requirements)

        raw_pool = build_candidate_pool_for_deal(
            str(deal_id),
            transcript,
            current_spec=None,
            mode="compose",
            include_global=include_global,
        )

        coverage = build_filtered_pool_for_coverage(
            pool=raw_pool,
            requirements=requirements,
            topology=topology,
            roles=role_plan,
        )

        pool = coverage.filtered_pool
        role_candidates = {
            d.role_key: d.selected_candidate_ids
            for d in coverage.role_debug
            if d.selected_candidate_ids
        }

        summary_json = {
            "raw_pool_items": len(raw_pool.items),
            "raw_pool_tasks": len(raw_pool.tasks),
            "filtered_pool_items": len(pool.items),
            "filtered_pool_tasks": len(pool.tasks),
            "kept_candidate_ids": coverage.kept_candidate_ids,
            "coverage_warnings": coverage.warnings,
            "role_candidates": role_candidates,
        }

        iteration_id = self.ai_store.create_iteration(
            job_id=job_id,
            request_text=transcript,
            requirements_json=requirements.model_dump(mode="json"),
            confidence_map=dict(requirements.confidence),
            topology_json=topology.model_dump(mode="json"),
            summary_json=summary_json,
            status="running",
        )

        self._write_json_artifact(run.run_dir / "requirements.json", requirements.model_dump(mode="json"))
        self._write_json_artifact(run.run_dir / "coverage.json", summary_json)

        llm = self._make_llm()
        prompt_bundle = compose_prompt(
            transcript,
            pool,
            requirements=requirements,
            roles=role_plan,
            topology=topology,
            role_candidates=role_candidates,
        )

        seed_spec = seed_spec_from_role_candidates(
            request_text=transcript,
            pool=pool,
            role_candidates=role_candidates,
            requirements=requirements,
            topology=topology,
        )

        try:
            spec = compose(
                llm=llm,
                run=run,
                system=prompt_bundle.system,
                user=prompt_bundle.user,
                pool=pool,
                request_text=transcript,
            )
        except Exception as e:
            save_text(run, "compose_exception", str(e))
            spec = seed_spec

        spec = postprocess_spec(
            spec=spec,
            filtered_pool=pool,
            source_pool=raw_pool,
            requirements=requirements,
            topology=topology,
            roles=role_plan,
        )
        update_last_valid(run, spec)
        save_iter(run, "final_spec", spec)

        md_text = render_markdown(spec, pool=pool, settings=self.settings)
        md_path = self._write_text_artifact(run.run_dir / "explain.md", md_text)
        xlsx_path = export_xlsx(spec, run.run_dir / "sksp.xlsx", pool=pool, settings=self.settings)
        req_json_path = self._write_json_artifact(run.run_dir / "requirements.json", requirements.model_dump(mode="json"))

        evidence_summary = {
            "used_bitrix_task_ids": list(getattr(spec, "used_bitrix_task_ids", []) or []),
            "lines": [
                {
                    "line_id": getattr(line, "line_id", None),
                    "manufacturer": getattr(line, "manufacturer", None),
                    "sku": getattr(line, "sku", None),
                    "model": getattr(line, "model", None),
                    "name": getattr(line, "name", None),
                    "task_ids": list(getattr(getattr(line, "evidence", None), "bitrix_task_ids", []) or []),
                    "notes": list(getattr(getattr(line, "evidence", None), "notes", []) or []),
                }
                for line in getattr(spec, "items", []) or []
            ],
        }
        evidence_json_path = self._write_json_artifact(run.run_dir / "evidence.json", evidence_summary)

        self.ai_store.add_artifact(
            iteration_id=iteration_id,
            artifact_type="sksp_xlsx",
            path=str(xlsx_path),
            mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            payload_json={"deal_id": deal_id},
        )
        self.ai_store.add_artifact(
            iteration_id=iteration_id,
            artifact_type="explain_md",
            path=str(md_path),
            mime_type="text/markdown",
            payload_json={"deal_id": deal_id},
        )
        self.ai_store.add_artifact(
            iteration_id=iteration_id,
            artifact_type="requirements_json",
            path=str(req_json_path),
            mime_type="application/json",
            payload_json={"deal_id": deal_id},
        )
        self.ai_store.add_artifact(
            iteration_id=iteration_id,
            artifact_type="evidence_json",
            path=str(evidence_json_path),
            mime_type="application/json",
            payload_json={"deal_id": deal_id},
        )

        self._persist_evidence(iteration_id=iteration_id, spec=spec, pool=pool)

        self.ai_store.update_iteration_status(iteration_id, "completed")
        self.ai_store.update_job_status(job_id, "completed")

        return EngineerRunResult(
            job_id=job_id,
            iteration_id=iteration_id,
            run_dir=str(run.run_dir),
            markdown_path=str(md_path),
            xlsx_path=str(xlsx_path),
            requirements_json_path=str(req_json_path),
            evidence_json_path=str(evidence_json_path),
        )