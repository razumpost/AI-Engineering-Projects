from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


_URL_RE = re.compile(r"^https?://", re.IGNORECASE)
_NUM_RE = re.compile(r"^\d+([.,]\d+)?$")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if ln:
                rows.append(json.loads(ln))
    return rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")


def _first_line(text: str | None) -> str | None:
    if not text:
        return None
    t = text.strip()
    if not t:
        return None
    return t.splitlines()[0].strip() or None


def normalize_item(it: dict[str, Any]) -> dict[str, Any]:
    it = dict(it)

    name = str(it.get("name") or "").strip()
    desc = str(it.get("description") or "").strip()
    sku = str(it.get("sku") or "").strip()
    vendor = str(it.get("vendor") or "").strip()

    # 1) name is URL -> move to url
    if name and _URL_RE.match(name):
        it["url"] = name
        # build a reasonable name
        fallback = " ".join([p for p in [vendor, sku] if p]).strip()
        if not fallback:
            fallback = _first_line(desc) or "UNKNOWN"
        it["name"] = fallback

    # 2) unit sanity: if unit looks numeric (often price) -> drop
    unit = str(it.get("unit") or "").strip()
    if unit and _NUM_RE.match(unit):
        it["unit"] = None

    # 3) lightweight normalization for numeric strings
    for k in ("qty", "price", "sum"):
        v = it.get(k)
        if v is None:
            continue
        s = str(v).strip().replace("\u00A0", " ").replace(" ", "").replace(",", ".")
        it[k] = s if s else None

    return it


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input items jsonl")
    ap.add_argument("--out", required=True, help="Output normalized items jsonl")
    args = ap.parse_args()

    inp = Path(args.inp).expanduser().resolve()
    out = Path(args.out).expanduser().resolve()

    rows = _read_jsonl(inp)
    norm = [normalize_item(r) for r in rows]
    _write_jsonl(out, norm)
    print(f"items={len(norm)} -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())