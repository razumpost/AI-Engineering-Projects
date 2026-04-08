from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from dotenv import load_dotenv  # type: ignore

from src.mvp_sksp.pipeline.graph_prompt_bridge import augment_transcript_with_graph
from src.mvp_sksp.services.engineer_service import EngineerService


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_env() -> None:
    load_dotenv(dotenv_path=str(_repo_root() / ".env"), override=False)


def _to_plain(obj: Any) -> Any:
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "__dict__"):
        return dict(obj.__dict__)
    return obj


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--deal-id", required=True, help="ID сделки")
    ap.add_argument("--transcript-text", required=True, help="Свободный текст запроса/транскрипта")
    ap.add_argument("--no-graph", action="store_true", help="Не использовать graph expansion")
    return ap.parse_args()


def main() -> int:
    _load_env()
    args = parse_args()

    transcript_text = args.transcript_text
    graph_data: dict[str, Any] | None = None

    if not args.no_graph:
        transcript_text, graph_data = augment_transcript_with_graph(args.transcript_text)

    svc = EngineerService()
    result = svc.run(
        deal_id=str(args.deal_id),
        transcript_text=transcript_text,
    )

    payload = _to_plain(result)

    if isinstance(payload, dict) and graph_data:
        run_dir = payload.get("run_dir")
        if run_dir:
            run_dir_path = Path(str(run_dir))
            run_dir_path.mkdir(parents=True, exist_ok=True)

            graph_path = run_dir_path / "graph_context.json"
            augmented_path = run_dir_path / "augmented_request.txt"

            graph_path.write_text(
                json.dumps(graph_data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            augmented_path.write_text(transcript_text, encoding="utf-8")

            payload["graph_context_path"] = str(graph_path)
            payload["augmented_request_path"] = str(augmented_path)
            payload["graph_seed_families"] = graph_data.get("seed_families", [])
            payload["graph_resolved_families"] = [
                f["family_id"] for f in graph_data.get("resolved_families", [])
            ]

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())