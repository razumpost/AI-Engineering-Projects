from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _run_case(deal_id: str, text: str) -> dict:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "mvp_engineer_run.py"),
        "--deal-id",
        str(deal_id),
        "--transcript-text",
        text,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
    if proc.returncode != 0:
        raise RuntimeError(
            f"Run failed for case:\n{text}\n\nSTDOUT:\n{proc.stdout}\n\nSTDERR:\n{proc.stderr}"
        )

    stdout = proc.stdout.strip()
    start = stdout.find("{")
    end = stdout.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise RuntimeError(f"Cannot parse JSON result from stdout:\n{stdout}")

    payload = json.loads(stdout[start : end + 1])

    run_dir = Path(payload["run_dir"])
    explain_path = Path(payload["markdown_path"])
    evidence_path = Path(payload["evidence_json_path"])
    requirements_path = Path(payload["requirements_json_path"])

    explain = explain_path.read_text(encoding="utf-8")
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    requirements = json.loads(requirements_path.read_text(encoding="utf-8"))

    return {
        "result": payload,
        "run_dir": run_dir,
        "explain": explain,
        "evidence": evidence,
        "requirements": requirements,
    }


def _line_names(evidence: dict) -> list[str]:
    return [str(x.get("name") or "") for x in evidence.get("lines", [])]


def _assert_contains_any(haystack: list[str], needles: list[str], label: str) -> None:
    if not any(any(n in h for n in needles) for h in haystack):
        raise AssertionError(f"{label}: none of {needles!r} found in lines={haystack!r}")


def _assert_not_contains_any(haystack: list[str], needles: list[str], label: str) -> None:
    bad = [h for h in haystack if any(n in h for n in needles)]
    if bad:
        raise AssertionError(f"{label}: forbidden matches found: {bad!r}")


def _assert_seat_count(req: dict, expected: int) -> None:
    caps = req.get("caps", {})
    got = caps.get("seat_count")
    if got != expected:
        raise AssertionError(f"seat_count mismatch: expected {expected}, got {got}")


def _assert_meeting_caps(req: dict, *, seat_count: int, camera_count: int, display_count: int) -> None:
    caps = req.get("caps", {})
    if caps.get("seat_count") != seat_count:
        raise AssertionError(f"seat_count mismatch: expected {seat_count}, got {caps.get('seat_count')}")
    if caps.get("camera_count") != camera_count:
        raise AssertionError(f"camera_count mismatch: expected {camera_count}, got {caps.get('camera_count')}")
    if caps.get("display_count") != display_count:
        raise AssertionError(f"display_count mismatch: expected {display_count}, got {caps.get('display_count')}")


def check_discussion_case(case: dict) -> None:
    req = case["requirements"]
    names = _line_names(case["evidence"])
    explain = case["explain"]

    # Для discussion smoke фиксируем только инварианты результата.
    # parser-эвристики вроде camera_count могут плавать и не должны валить baseline,
    # пока итоговый discussion skeleton остаётся корректным.
    _assert_seat_count(req, 24)

    _assert_contains_any(names, ["Пульт председателя"], "discussion chairman placeholder")
    _assert_contains_any(names, ["Центральный блок"], "discussion central unit placeholder")
    _assert_contains_any(names, ["Аудиопроцессор", "DSP"], "discussion dsp placeholder")

    _assert_not_contains_any(
        names,
        [
            "Профессиональный дисплей для переговорной",
            "Камера ВКС / PTZ для переговорной",
            "Дополнительная камера ВКС для переговорной",
            "Коммутатор / BYOD-шлюз / USB bridge для переговорной",
        ],
        "discussion should not leak ordinary meeting-room placeholders",
    )

    if "пульт" not in explain.casefold() and "дискус" not in explain.casefold():
        raise AssertionError("discussion explain does not mention discussion/pult logic")


def check_meeting_room_case(case: dict) -> None:
    req = case["requirements"]
    names = _line_names(case["evidence"])
    explain = case["explain"]

    _assert_meeting_caps(req, seat_count=20, camera_count=2, display_count=1)

    _assert_contains_any(names, ["Профессиональный дисплей для переговорной"], "meeting room display placeholder")
    _assert_contains_any(names, ["Камера ВКС / PTZ для переговорной"], "meeting room camera placeholder")
    _assert_contains_any(names, ["Дополнительная камера ВКС для переговорной"], "meeting room 2nd camera placeholder")
    _assert_contains_any(names, ["Акустика для переговорной"], "meeting room playback placeholder")
    _assert_contains_any(names, ["Коммутатор / BYOD-шлюз / USB bridge"], "meeting room switching placeholder")
    _assert_contains_any(names, ["Clockaudio"], "meeting room real microphone line")

    _assert_not_contains_any(
        names,
        [
            "ACC-CF-EH5C",
            "ACC-CR-EH5C",
            "NMP711-P10",
            "49WEC-CB",
            "VSD242-BKA-EU0",
            '23.6"',
            'ООО "РегионКом"',
        ],
        "meeting room forbidden junk",
    )

    if "Аудиовоспроизведение не найдено" not in explain:
        raise AssertionError("meeting room explain should reflect fail-closed playback placeholder state")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--deal-id", default="17372")
    args = parser.parse_args()

    discussion_query = "пульты делегатов, пульт председателя, 24 места, нужен центральный блок и интеграция со звуком"
    meeting_query = "переговорная на 20 мест, две камеры, дисплей 75, микрофоны"

    print("=== discussion_case ===")
    discussion = _run_case(args.deal_id, discussion_query)
    check_discussion_case(discussion)
    print("OK:", discussion["run_dir"])

    print("=== meeting_room_case ===")
    meeting = _run_case(args.deal_id, meeting_query)
    check_meeting_room_case(meeting)
    print("OK:", meeting["run_dir"])

    print("\nAll regression smoke checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())