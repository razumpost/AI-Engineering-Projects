# src/bitrix_kp_collector/storage.py
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path


@dataclass(frozen=True)
class StoredFile:
    path: Path
    sha256_hex: str
    size_bytes: int


def compute_sha256(path: Path) -> str:
    h = sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_stored_file(path: Path) -> StoredFile:
    stat = path.stat()
    return StoredFile(path=path, sha256_hex=compute_sha256(path), size_bytes=stat.st_size)
