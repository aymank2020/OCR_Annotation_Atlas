"""
atlas_auto_sync_and_rebuild.py
------------------------------
Automatic recovery workflow for cases where local outputs are empty/stale while
the real data lives on Google Drive.

What it does:
1) Checks local outputs coverage.
2) If coverage is weak and Drive source is provided, pulls metadata from Drive via rclone.
3) Rebuilds:
   - episodes_review_index.json
   - atlas_dashboard.html
   - atlas_review_viewer.html
   - chat_reviews/*
   - power_automate_queue.csv (optional)
4) Optionally uploads generated artifacts back to Drive.

Example:
  python atlas_auto_sync_and_rebuild.py ^
    --outputs-dir outputs ^
    --drive-link "https://drive.google.com/drive/folders/XXXX?usp=sharing" ^
    --remote gdrive ^
    --build-power-queue ^
    --video-dir "D:\\atlas video"
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple


def _log(msg: str) -> None:
    print(f"[auto-sync] {msg}")


def _run(cmd: List[str], cwd: Optional[Path] = None) -> None:
    _log("run: " + " ".join(cmd))
    subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True)


def _extract_folder_id(link: str) -> Optional[str]:
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", link)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", link)
    if m:
        return m.group(1)
    return None


def _count_json_files(path: Path) -> int:
    if not path.exists() or not path.is_dir():
        return 0
    return sum(1 for _ in path.glob("*.json"))


def _file_nonempty(path: Path) -> bool:
    return path.exists() and path.is_file() and path.stat().st_size > 0


def _detect_coverage(outputs_dir: Path) -> Tuple[int, dict]:
    usage_ok = _file_nonempty(outputs_dir / "gemini_usage.jsonl")
    transitions_ok = _file_nonempty(outputs_dir / "training_feedback" / "live" / "t4_transitions_history.jsonl")
    lessons_ok = _file_nonempty(outputs_dir / "training_feedback" / "live" / "alignment_lessons_history.jsonl")
    task_state_count = (
        _count_json_files(outputs_dir / ".task_state")
        + _count_json_files(outputs_dir / "task_state")
        + _count_json_files(outputs_dir.parent / ".task_state")
        + _count_json_files(outputs_dir.parent / "task_state")
    )
    review_index_ok = _file_nonempty(outputs_dir / "episodes_review_index.json")
    coverage_points = sum(
        [
            1 if usage_ok else 0,
            1 if transitions_ok else 0,
            1 if lessons_ok else 0,
            1 if task_state_count > 0 else 0,
            1 if review_index_ok else 0,
        ]
    )
    details = {
        "usage_ok": usage_ok,
        "transitions_ok": transitions_ok,
        "lessons_ok": lessons_ok,
        "task_state_count": task_state_count,
        "review_index_ok": review_index_ok,
    }
    return coverage_points, details


def _rclone_copy_metadata(
    remote_name: str,
    drive_path: str,
    drive_root_folder_id: Optional[str],
    dest: Path,
    include_video: bool,
) -> None:
    if shutil.which("rclone") is None:
        raise RuntimeError("rclone is required but not found in PATH")

    src = f"{remote_name}:{drive_path}" if drive_path else f"{remote_name}:"
    base_cmd = ["rclone", "copy", src, str(dest), "--create-empty-src-dirs", "--progress", "--checkers", "8", "--transfers", "4"]
    if drive_root_folder_id:
        base_cmd.extend(["--drive-root-folder-id", drive_root_folder_id])

    # metadata first (exclude large video blobs)
    cmd = list(base_cmd)
    cmd.extend(
        [
            "--exclude", "*.mp4",
            "--exclude", "*.mov",
            "--exclude", "*.webm",
            "--exclude", "*.mkv",
            "--exclude", "*.avi",
        ]
    )
    _run(cmd)

    if include_video:
        cmd2 = list(base_cmd)
        cmd2.extend(["--include", "video_*.mp4", "--include", "**/video_*.mp4"])
        _run(cmd2)


def _rclone_upload_results(
    remote_name: str,
    drive_path: str,
    drive_root_folder_id: Optional[str],
    files_to_upload: List[Path],
    chat_reviews_dir: Optional[Path],
) -> None:
    if shutil.which("rclone") is None:
        raise RuntimeError("rclone is required but not found in PATH")

    dest = f"{remote_name}:{drive_path}" if drive_path else f"{remote_name}:"
    args = ["--progress"]
    if drive_root_folder_id:
        args.extend(["--drive-root-folder-id", drive_root_folder_id])

    for f in files_to_upload:
        if f.exists():
            _run(["rclone", "copy", str(f), dest, *args])
    if chat_reviews_dir and chat_reviews_dir.exists():
        _run(["rclone", "copy", str(chat_reviews_dir), f"{dest.rstrip('/')}/chat_reviews", *args])


def _resolve_effective_outputs(snapshot_dir: Path) -> Path:
    if (snapshot_dir / "outputs").exists():
        return snapshot_dir / "outputs"
    return snapshot_dir


def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst, ignore_errors=True)
    shutil.copytree(src, dst)


def _publish_artifacts_to_outputs(
    effective_outputs: Path,
    outputs_dir: Path,
    index_path: Path,
    dashboard_path: Path,
    viewer_path: Path,
    queue_path: Path,
    chat_dir: Path,
) -> None:
    outputs_dir.mkdir(parents=True, exist_ok=True)
    for src in (index_path, dashboard_path, viewer_path, queue_path):
        if src.exists():
            shutil.copy2(src, outputs_dir / src.name)
    if chat_dir.exists():
        _copy_tree(chat_dir, outputs_dir / "chat_reviews")


def main() -> None:
    parser = argparse.ArgumentParser(description="Auto sync outputs from Drive (if needed) and rebuild dashboard/review artifacts.")
    parser.add_argument("--outputs-dir", default="outputs", help="Local outputs directory")
    parser.add_argument("--work-dir", default="", help="Temp working folder for Drive snapshot")
    parser.add_argument("--auto-sync-if-empty", action="store_true", default=True, help="Sync from Drive if local coverage is weak")
    parser.add_argument("--force-sync", action="store_true", help="Always sync from Drive when drive source is provided")

    parser.add_argument("--drive-link", default="", help="Google Drive folder link")
    parser.add_argument("--drive-path", default="", help="rclone path under remote (alternative to --drive-link)")
    parser.add_argument("--remote", default=os.environ.get("RCLONE_REMOTE", "gdrive"), help="rclone remote name")
    parser.add_argument("--include-video", action="store_true", help="Include video files in pull step")
    parser.add_argument("--upload-results", action="store_true", help="Upload generated artifacts back to Drive")
    parser.add_argument("--publish-to-project-outputs", dest="publish_to_project_outputs", action="store_true", default=True)
    parser.add_argument("--no-publish-to-project-outputs", dest="publish_to_project_outputs", action="store_false")

    parser.add_argument("--probe-atlas-status", default="auto", choices=["off", "auto", "on"])
    parser.add_argument("--atlas-state", default=".state/atlas_auth.json", help="Path to atlas_auth.json for probe cookies")
    parser.add_argument("--probe-timeout-sec", type=float, default=0.8)

    parser.add_argument("--only-status", default="disputed,policy_fail,error,labeled_not_submitted,unknown")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--build-power-queue", action="store_true", help="Also generate outputs/power_automate_queue.csv")
    parser.add_argument("--video-dir", default=r"D:\atlas video", help="Video directory used by power queue")
    parser.add_argument("--gemini-chat-url", default="https://gemini.google.com/app/b3006ba9f325b55c")
    args = parser.parse_args()

    app_dir = Path(__file__).resolve().parent
    py = sys.executable
    outputs_dir = Path(args.outputs_dir).resolve()
    outputs_dir.mkdir(parents=True, exist_ok=True)

    local_points, local_cov = _detect_coverage(outputs_dir)
    _log(f"local coverage points={local_points}/5 details={local_cov}")

    drive_root_id: Optional[str] = None
    drive_path = str(args.drive_path or "").strip()
    if args.drive_link:
        drive_root_id = _extract_folder_id(args.drive_link)
        if not drive_root_id:
            raise SystemExit("Could not extract folder id from --drive-link")
        drive_path = ""

    can_sync = bool(args.drive_link or args.drive_path)
    do_sync = False
    if args.force_sync and can_sync:
        do_sync = True
    elif args.auto_sync_if_empty and can_sync and local_points < 2:
        do_sync = True

    effective_outputs = outputs_dir
    snapshot_dir = None
    if do_sync:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        base_work = Path(args.work_dir).resolve() if args.work_dir else (Path("/tmp") / f"atlas_drive_snapshot_{ts}")
        snapshot_dir = base_work
        if snapshot_dir.exists():
            shutil.rmtree(snapshot_dir, ignore_errors=True)
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        _log(f"syncing from drive into: {snapshot_dir}")
        _rclone_copy_metadata(
            remote_name=args.remote,
            drive_path=drive_path,
            drive_root_folder_id=drive_root_id,
            dest=snapshot_dir,
            include_video=bool(args.include_video),
        )
        effective_outputs = _resolve_effective_outputs(snapshot_dir)
        points_after_sync, cov_after_sync = _detect_coverage(effective_outputs)
        _log(f"snapshot coverage points={points_after_sync}/5 details={cov_after_sync}")
    else:
        _log("skip sync: using local outputs directly")

    # Build artifacts
    index_path = effective_outputs / "episodes_review_index.json"
    dashboard_path = effective_outputs / "atlas_dashboard.html"
    viewer_path = effective_outputs / "atlas_review_viewer.html"
    chat_dir = effective_outputs / "chat_reviews"
    queue_path = effective_outputs / "power_automate_queue.csv"

    _run(
        [
            py,
            str(app_dir / "atlas_review_builder.py"),
            "--outputs-dir",
            str(effective_outputs),
            "--out",
            str(index_path),
            "--probe-atlas-status",
            args.probe_atlas_status,
            "--atlas-state",
            str(Path(args.atlas_state).resolve()),
            "--probe-timeout-sec",
            str(float(args.probe_timeout_sec)),
        ]
    )
    _run([py, str(app_dir / "atlas_dashboard_gen.py"), "--outputs-dir", str(effective_outputs)])
    _run([py, str(app_dir / "atlas_review_viewer_gen.py"), "--index", str(index_path), "--out", str(viewer_path)])
    _run(
        [
            py,
            str(app_dir / "atlas_chat_exporter.py"),
            "--index",
            str(index_path),
            "--out-dir",
            str(chat_dir),
            "--only-status",
            str(args.only_status),
            "--limit",
            str(max(0, int(args.limit))),
        ]
    )

    if args.build_power_queue:
        _run(
            [
                py,
                str(app_dir / "atlas_power_automate_queue.py"),
                "--index",
                str(index_path),
                "--chat-reviews-dir",
                str(chat_dir),
                "--out-csv",
                str(queue_path),
                "--video-dir",
                str(args.video_dir),
                "--statuses",
                str(args.only_status),
                "--gemini-chat-url",
                str(args.gemini_chat_url),
            ]
        )

    if bool(args.publish_to_project_outputs):
        if effective_outputs.resolve() != outputs_dir.resolve():
            _log("publishing artifacts into project outputs/")
            _publish_artifacts_to_outputs(
                effective_outputs=effective_outputs,
                outputs_dir=outputs_dir,
                index_path=index_path,
                dashboard_path=dashboard_path,
                viewer_path=viewer_path,
                queue_path=queue_path,
                chat_dir=chat_dir,
            )
        else:
            _log("publish skipped: effective outputs already points to project outputs/")

    # Optional upload back to Drive.
    if args.upload_results and can_sync:
        _log("uploading generated artifacts back to Drive")
        _rclone_upload_results(
            remote_name=args.remote,
            drive_path=drive_path,
            drive_root_folder_id=drive_root_id,
            files_to_upload=[dashboard_path, index_path, viewer_path, queue_path],
            chat_reviews_dir=chat_dir,
        )

    _log("done")
    _log(f"effective_outputs: {effective_outputs}")
    _log(f"dashboard: {dashboard_path}")
    _log(f"review_index: {index_path}")
    _log(f"review_viewer: {viewer_path}")
    _log(f"chat_reviews: {chat_dir}")
    if args.build_power_queue:
        _log(f"power_queue: {queue_path}")


if __name__ == "__main__":
    main()
