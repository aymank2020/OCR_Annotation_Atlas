"""
Build a CSV queue for Power Automate Desktop:
- episode id
- local video path
- chat prompt path
- chat prompt text (single-line escaped)
- target Gemini chat URL
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, List


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def build_queue(
    index_path: Path,
    chat_reviews_dir: Path,
    out_csv: Path,
    video_dir: Path,
    statuses: List[str],
    limit: int,
    gemini_chat_url: str,
) -> int:
    payload = _load_json(index_path)
    episodes = payload.get("episodes", []) if isinstance(payload, dict) else []
    if not isinstance(episodes, list):
        episodes = []

    statuses_set = {s.strip().lower() for s in statuses if s.strip()}
    rows: List[Dict[str, str]] = []

    for ep in episodes:
        if not isinstance(ep, dict):
            continue
        eid = str(ep.get("episode_id") or "").strip().lower()
        if not eid:
            continue
        st = str(ep.get("review_status") or "").strip().lower()
        if statuses_set and st not in statuses_set:
            continue

        prompt_path = chat_reviews_dir / eid / "chat_prompt.txt"
        prompt = _load_text(prompt_path)
        if prompt:
            prompt = prompt.rstrip() + "\n\nمهم: اكتب الإجابة النهائية بالكامل باللغة العربية."

        video_path = video_dir / f"video_{eid}.mp4"
        rows.append(
            {
                "episode_id": eid,
                "review_status": st,
                "atlas_url": str(ep.get("atlas_url") or ep.get("open_url") or ""),
                "video_path": str(video_path),
                "prompt_path": str(prompt_path),
                "prompt_text": prompt,
                "gemini_chat_url": gemini_chat_url,
            }
        )
        if limit > 0 and len(rows) >= limit:
            break

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "episode_id",
                "review_status",
                "atlas_url",
                "video_path",
                "prompt_path",
                "prompt_text",
                "gemini_chat_url",
            ],
        )
        w.writeheader()
        for row in rows:
            w.writerow(row)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Power Automate queue CSV from review index.")
    parser.add_argument("--index", default="outputs/episodes_review_index.json")
    parser.add_argument("--chat-reviews-dir", default="outputs/chat_reviews")
    parser.add_argument("--out-csv", default="outputs/power_automate_queue.csv")
    parser.add_argument("--video-dir", default=r"D:\atlas video")
    parser.add_argument(
        "--statuses",
        default="disputed,policy_fail,error,labeled_not_submitted,unknown",
        help="Comma-separated statuses",
    )
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--gemini-chat-url", default="https://gemini.google.com/app/b3006ba9f325b55c")
    args = parser.parse_args()

    n = build_queue(
        index_path=Path(args.index),
        chat_reviews_dir=Path(args.chat_reviews_dir),
        out_csv=Path(args.out_csv),
        video_dir=Path(args.video_dir),
        statuses=[x.strip() for x in str(args.statuses).split(",") if x.strip()],
        limit=max(0, int(args.limit)),
        gemini_chat_url=str(args.gemini_chat_url),
    )
    print(f"[power-queue] rows: {n}")
    print(f"[power-queue] file: {Path(args.out_csv).resolve()}")


if __name__ == "__main__":
    main()

