"""
atlas_sync_if_zero.py
---------------------
Auto-refresh helper:
- If local review/dashboard signals look empty or suspiciously zero, force-sync
  from Google Drive and rebuild artifacts.
- Otherwise, rebuild locally (and still allow auto-sync logic in the main script).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict


def _read_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _status_counts(index_payload: dict) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    episodes = index_payload.get("episodes", [])
    if not isinstance(episodes, list):
        return counts
    for ep in episodes:
        if not isinstance(ep, dict):
            continue
        st = str(ep.get("review_status", "unknown") or "unknown").strip().lower()
        counts[st] = counts.get(st, 0) + 1
    return counts


def _file_nonempty(path: Path) -> bool:
    return path.exists() and path.is_file() and path.stat().st_size > 0


def _count_json_files(path: Path) -> int:
    if not path.exists() or not path.is_dir():
        return 0
    return sum(1 for _ in path.glob("*.json"))


def _coverage_points(outputs_dir: Path, index_payload: dict | None) -> tuple[int, Dict[str, int | bool]]:
    usage_ok = _file_nonempty(outputs_dir / "gemini_usage.jsonl")
    transitions_ok = _file_nonempty(outputs_dir / "training_feedback" / "live" / "t4_transitions_history.jsonl")
    lessons_ok = _file_nonempty(outputs_dir / "training_feedback" / "live" / "alignment_lessons_history.jsonl")
    task_state_count = _count_json_files(outputs_dir / ".task_state") + _count_json_files(outputs_dir / "task_state")
    review_index_ok = bool(isinstance(index_payload, dict))
    episodes_count = 0
    if isinstance(index_payload, dict):
        eps = index_payload.get("episodes", [])
        if isinstance(eps, list):
            episodes_count = len(eps)
    episode_signal_ok = episodes_count > 0

    points = sum(
        [
            1 if usage_ok else 0,
            1 if transitions_ok else 0,
            1 if lessons_ok else 0,
            1 if task_state_count > 0 else 0,
            1 if review_index_ok else 0,
            1 if episode_signal_ok else 0,
        ]
    )
    details: Dict[str, int | bool] = {
        "usage_ok": usage_ok,
        "transitions_ok": transitions_ok,
        "lessons_ok": lessons_ok,
        "task_state_count": task_state_count,
        "review_index_ok": review_index_ok,
        "episodes_count": episodes_count,
    }
    return points, details


def _looks_empty(outputs_dir: Path) -> tuple[bool, str]:
    index_path = outputs_dir / "episodes_review_index.json"
    dashboard_path = outputs_dir / "atlas_dashboard.html"

    if not index_path.exists():
        return True, "missing review index"
    if not dashboard_path.exists():
        return True, "missing dashboard"

    payload = _read_json(index_path)
    if not isinstance(payload, dict):
        return True, "invalid review index json"

    coverage_points, details = _coverage_points(outputs_dir, payload)
    # Strong fallback: if core coverage is weak, force sync even if index exists.
    if coverage_points < 4:
        return True, f"weak coverage ({coverage_points}) details={details}"

    episodes = payload.get("episodes", [])
    if not isinstance(episodes, list) or len(episodes) == 0:
        return True, "index has zero episodes"

    counts = _status_counts(payload)
    signal = (
        counts.get("submitted", 0)
        + counts.get("disputed", 0)
        + counts.get("policy_fail", 0)
        + counts.get("error", 0)
        + counts.get("labeled_not_submitted", 0)
    )
    if signal == 0:
        return True, "all key status buckets are zero"

    return False, f"signals look valid coverage={coverage_points}"


def _run(cmd: list[str]) -> None:
    print("[sync-if-zero] run:", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Auto sync from Drive if local review indicators look empty.")
    parser.add_argument("--outputs-dir", default="outputs")
    parser.add_argument("--drive-link", required=True, help="Google Drive folder link: .../folders/<ID>?...")
    parser.add_argument("--remote", default="gdrive")
    parser.add_argument("--atlas-state", default=".state/atlas_auth.json")
    parser.add_argument("--probe-timeout-sec", type=float, default=0.8)
    parser.add_argument("--only-status", default="disputed,policy_fail,error,labeled_not_submitted,unknown")
    parser.add_argument("--build-power-queue", action="store_true", default=True)
    parser.add_argument("--video-dir", default=r"D:\atlas video")
    parser.add_argument("--gemini-chat-url", default="https://gemini.google.com/app/b3006ba9f325b55c")
    args = parser.parse_args()

    outputs_dir = Path(args.outputs_dir).resolve()
    outputs_dir.mkdir(parents=True, exist_ok=True)

    should_force_sync, reason = _looks_empty(outputs_dir)
    print(f"[sync-if-zero] decision: force_sync={should_force_sync} reason={reason}")

    app_dir = Path(__file__).resolve().parent
    auto_sync_script = app_dir / "atlas_auto_sync_and_rebuild.py"

    cmd = [
        sys.executable,
        str(auto_sync_script),
        "--outputs-dir",
        str(outputs_dir),
        "--drive-link",
        str(args.drive_link),
        "--remote",
        str(args.remote),
        "--probe-atlas-status",
        "auto",
        "--atlas-state",
        str(Path(args.atlas_state).resolve()),
        "--probe-timeout-sec",
        str(float(args.probe_timeout_sec)),
        "--only-status",
        str(args.only_status),
        "--video-dir",
        str(args.video_dir),
        "--gemini-chat-url",
        str(args.gemini_chat_url),
    ]
    if args.build_power_queue:
        cmd.append("--build-power-queue")
    if should_force_sync:
        cmd.append("--force-sync")

    _run(cmd)
    print("[sync-if-zero] done.")


if __name__ == "__main__":
    main()
