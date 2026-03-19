from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(*args: str) -> None:
    subprocess.run([sys.executable, *args], cwd=ROOT, check=True)


def main() -> int:
    run("-m", "pip", "install", "-U", "pip")
    run("-m", "pip", "install", "-e", ".[dev]")
    print("Bootstrap complete.")
    print("Next:")
    print("  python scripts/smoke_test.py")
    print('  python scripts/mvp_sksp_cli.py --request "переговорная на 12 мест под ВКС" --interactive')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
