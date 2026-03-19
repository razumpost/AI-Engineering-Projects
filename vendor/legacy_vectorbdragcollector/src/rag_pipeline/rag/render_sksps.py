# =========================
# File: src/rag_pipeline/rag/render_sksps.py
# =========================
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict

from .sksps_writer import items_from_json, write_sksps_from_items


def _default_template() -> str:
    # Keep old default, but make it overridable.
    return os.getenv("SKSPS_TEMPLATE_PATH", "assets/templates/sksps_template.xlsx")


def render_sksps_json_to_xlsx(
    sksps_json: Dict[str, Any],
    *,
    template_path: str,
    out_path: str,
) -> None:
    items = items_from_json(sksps_json)
    if not items:
        raise RuntimeError("No items found in СкСп JSON. Expected obj.items or obj.sections[*].items")

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    write_sksps_from_items(template_path=template_path, out_path=out_path, items=items)


def main() -> None:
    ap = argparse.ArgumentParser(description="Render СкСп JSON -> Excel (xlsx) using a template.")
    ap.add_argument("--json", required=True, help="Path to СкСп JSON file")
    ap.add_argument("--template", default=_default_template(), help="Path to СкСп template .xlsx")
    ap.add_argument("--out", required=True, help="Output .xlsx path")
    args = ap.parse_args()

    if not os.path.exists(args.template):
        raise SystemExit(
            f"Template not found: {args.template}. "
            f"Put your template at assets/templates/sksps_template.xlsx or pass --template /path/to/template.xlsx "
            f"(or export SKSPS_TEMPLATE_PATH)."
        )

    with open(args.json, "r", encoding="utf-8") as f:
        obj = json.load(f)

    render_sksps_json_to_xlsx(obj, template_path=args.template, out_path=args.out)
    print(f"OK: wrote {args.out}")


if __name__ == "__main__":
    main()