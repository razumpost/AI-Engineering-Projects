from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ..domain.spec import Spec


@dataclass(frozen=True)
class SnapshotPaths:
    run_dir: Path
    last_valid_path: Path


def _atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def make_run_paths(base_dir: str) -> SnapshotPaths:
    run_dir = Path(base_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    return SnapshotPaths(run_dir=run_dir, last_valid_path=run_dir / "last_valid.json")


def save_iter(run: SnapshotPaths, name: str, spec: Spec) -> Path:
    path = run.run_dir / f"{name}.json"
    _atomic_write_json(path, spec.model_dump(mode="json"))
    return path


def save_text(run: SnapshotPaths, name: str, text: str) -> Path:
    """Best-effort debug artifact; never overwrites last_valid."""
    run.run_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = run.run_dir / f"{name}_{ts}.txt"
    path.write_text(text or "", encoding="utf-8")
    return path


def load_last_valid(run: SnapshotPaths) -> Spec | None:
    if not run.last_valid_path.exists():
        return None
    data = json.loads(run.last_valid_path.read_text(encoding="utf-8"))
    return Spec.model_validate(data)


def update_last_valid(run: SnapshotPaths, spec: Spec) -> None:
    _atomic_write_json(run.last_valid_path, spec.model_dump(mode="json"))
