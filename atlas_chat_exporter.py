"""
Export episode review packages for manual AI chat auditing.

Input:
  - episodes_review_index.json (from atlas_review_builder.py)

Output:
  - chat_reviews/<episode_id>/chat_prompt.txt
  - chat_reviews/<episode_id>/episode_meta.json
  - optional copied video file
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, List


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _segments_to_text(data: Any) -> str:
    if not data:
        return ""
    if isinstance(data, str):
        return data.strip()
    if isinstance(data, dict):
        if isinstance(data.get("segments"), list):
            lines: List[str] = []
            for s in data["segments"]:
                start = s.get("start_sec", s.get("start", ""))
                end = s.get("end_sec", s.get("end", ""))
                label = s.get("label", "")
                lines.append(f"{start} -> {end} | {label}")
            return "\n".join(lines).strip()
        return json.dumps(data, ensure_ascii=False, indent=2)
    if isinstance(data, list):
        lines = []
        for s in data:
            if isinstance(s, dict):
                start = s.get("start_sec", s.get("start", ""))
                end = s.get("end_sec", s.get("end", ""))
                label = s.get("label", "")
                lines.append(f"{start} -> {end} | {label}")
            else:
                lines.append(str(s))
        return "\n".join(lines).strip()
    return str(data).strip()


def _build_prompt(entry: Dict[str, Any]) -> str:
    tier2 = _segments_to_text(entry.get("tier2_text") or entry.get("tier2"))
    tier3 = _segments_to_text(entry.get("tier3_text") or entry.get("tier3"))
    validation = entry.get("validation") or {}
    disputes = entry.get("disputes") or []

    validation_summary = {
        "ok": validation.get("ok"),
        "episode_errors": validation.get("episode_errors", []),
        "episode_warnings": validation.get("episode_warnings", []),
        "major_fail_triggers": validation.get("major_fail_triggers", []),
        "device_class_conflicts": validation.get("device_class_conflicts", []),
    }

    return f"""I need a strict Atlas Tier-4 audit for this episode.

Episode ID: {entry.get("episode_id", "")}
Atlas URL: {entry.get("atlas_url", "")}
Current status: {entry.get("review_status", "unknown")}

[Tier2 - before AI update]
{tier2 or "(missing)"}

[Tier3 - after AI update]
{tier3 or "(missing)"}

[Validator summary]
{json.dumps(validation_summary, ensure_ascii=False, indent=2)}

[Disputes summary]
Count: {len(disputes)}
Sample:
{json.dumps(disputes[:3], ensure_ascii=False, indent=2)}

Please evaluate:
1) Which is better: Tier2 or Tier3?
2) Is Tier3 submit-safe according to Atlas policy?
3) List exact segment-level issues with rule names.
4) Provide corrected labels for failing segments.
5) Final verdict: PASS / FAIL with confidence.
"""


def _status_allowed(status: str, filters: Iterable[str]) -> bool:
    if not filters:
        return True
    return status in filters


def export_chat_packages(
    index_path: Path,
    out_dir: Path,
    only_status: List[str],
    limit: int,
    copy_video: bool,
) -> Dict[str, int]:
    payload = _load_json(index_path)
    episodes = payload.get("episodes", []) if isinstance(payload, dict) else []
    if not isinstance(episodes, list):
        episodes = []

    out_dir.mkdir(parents=True, exist_ok=True)
    exported = 0
    skipped = 0
    copied_videos = 0

    for ep in episodes:
        if not isinstance(ep, dict):
            continue
        status = str(ep.get("review_status", "unknown"))
        if not _status_allowed(status, only_status):
            skipped += 1
            continue
        if limit > 0 and exported >= limit:
            break

        eid = str(ep.get("episode_id", "")).strip()
        if not eid:
            skipped += 1
            continue

        ep_dir = out_dir / eid
        ep_dir.mkdir(parents=True, exist_ok=True)

        prompt = _build_prompt(ep)
        (ep_dir / "chat_prompt.txt").write_text(prompt, encoding="utf-8")
        (ep_dir / "episode_meta.json").write_text(
            json.dumps(ep, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (ep_dir / "atlas_url.txt").write_text(str(ep.get("atlas_url", "")), encoding="utf-8")

        if copy_video:
            video_path = str(ep.get("video_path") or "").strip()
            if video_path:
                src = Path(video_path)
                if src.exists() and src.is_file():
                    dst = ep_dir / src.name
                    try:
                        shutil.copy2(src, dst)
                        copied_videos += 1
                    except Exception:
                        pass

        exported += 1

    return {
        "exported": exported,
        "skipped": skipped,
        "copied_videos": copied_videos,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Export chat review packages from episodes_review_index.json")
    parser.add_argument(
        "--index",
        default="episodes_review_index.json",
        help="Path to episodes_review_index.json",
    )
    parser.add_argument(
        "--out-dir",
        default="chat_reviews",
        help="Output folder for per-episode chat packages",
    )
    parser.add_argument(
        "--only-status",
        default="",
        help="Comma-separated statuses to include (e.g. disputed,policy_fail,error)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Max episodes to export (0 = all)")
    parser.add_argument(
        "--copy-video",
        action="store_true",
        help="Copy episode video into each package folder if available",
    )
    args = parser.parse_args()

    statuses = [s.strip() for s in args.only_status.split(",") if s.strip()]
    stats = export_chat_packages(
        index_path=Path(args.index).resolve(),
        out_dir=Path(args.out_dir).resolve(),
        only_status=statuses,
        limit=max(0, int(args.limit)),
        copy_video=bool(args.copy_video),
    )

    print(f"[chat-exporter] exported: {stats['exported']}")
    print(f"[chat-exporter] skipped: {stats['skipped']}")
    print(f"[chat-exporter] copied_videos: {stats['copied_videos']}")
    print(f"[chat-exporter] out_dir: {Path(args.out_dir).resolve()}")


if __name__ == "__main__":
    main()
