"""
Build a CSV queue for Power Automate Desktop:
- episode id
- local video path
- chat prompt path
- chat prompt text
- target Gemini chat URL
- optional previous Gemini eval metadata
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


def _load_eval_map(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        payload = _load_json(path)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    raw = payload.get("evaluations")
    if not isinstance(raw, dict):
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for eid, item in raw.items():
        if isinstance(item, dict):
            out[str(eid or "").strip().lower()] = item
    return out


def build_queue(
    index_path: Path,
    chat_reviews_dir: Path,
    out_csv: Path,
    video_dir: Path,
    eval_json: Path,
    statuses: List[str],
    limit: int,
    gemini_chat_url: str,
    skip_reviewed: bool,
) -> Dict[str, int]:
    payload = _load_json(index_path)
    episodes = payload.get("episodes", []) if isinstance(payload, dict) else []
    if not isinstance(episodes, list):
        episodes = []

    eval_map = _load_eval_map(eval_json)
    statuses_set = {s.strip().lower() for s in statuses if s.strip()}
    rows: List[Dict[str, str]] = []
    skipped_reviewed = 0

    for ep in episodes:
        if not isinstance(ep, dict):
            continue
        eid = str(ep.get("episode_id") or "").strip().lower()
        if not eid:
            continue
        st = str(ep.get("review_status") or "").strip().lower()
        if statuses_set and st not in statuses_set:
            continue

        existing_eval = eval_map.get(eid)
        if skip_reviewed and existing_eval is not None:
            skipped_reviewed += 1
            continue

        prompt_path = chat_reviews_dir / eid / "chat_prompt.txt"
        prompt = _load_text(prompt_path)
        if prompt:
            prompt = prompt.rstrip() + "\n\nImportant: write the final answer fully in Arabic."

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
                "eval_score": str((existing_eval or {}).get("score_pct", "")),
                "eval_updated_at_utc": str((existing_eval or {}).get("updated_at_utc", "")),
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
                "eval_score",
                "eval_updated_at_utc",
            ],
        )
        w.writeheader()
        for row in rows:
            w.writerow(row)

    return {
        "rows": len(rows),
        "skipped_reviewed": skipped_reviewed,
        "episodes_total": len(episodes),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Power Automate queue CSV from review index.")
    parser.add_argument("--index", default="outputs/episodes_review_index.json")
    parser.add_argument("--chat-reviews-dir", default="outputs/chat_reviews")
    parser.add_argument("--out-csv", default="outputs/power_automate_queue.csv")
    parser.add_argument("--video-dir", default=r"D:\atlas video")
    parser.add_argument("--eval-json", default="outputs/gemini_chat_evaluations.json")
    parser.add_argument(
        "--statuses",
        default="disputed,policy_fail,error,labeled_not_submitted,unknown",
        help="Comma-separated statuses",
    )
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--gemini-chat-url", default="https://gemini.google.com/app/b3006ba9f325b55c")
    parser.add_argument("--skip-reviewed", dest="skip_reviewed", action="store_true", default=True)
    parser.add_argument("--no-skip-reviewed", dest="skip_reviewed", action="store_false")
    args = parser.parse_args()

    stats = build_queue(
        index_path=Path(args.index),
        chat_reviews_dir=Path(args.chat_reviews_dir),
        out_csv=Path(args.out_csv),
        video_dir=Path(args.video_dir),
        eval_json=Path(args.eval_json),
        statuses=[x.strip() for x in str(args.statuses).split(",") if x.strip()],
        limit=max(0, int(args.limit)),
        gemini_chat_url=str(args.gemini_chat_url),
        skip_reviewed=bool(args.skip_reviewed),
    )
    print(f"[power-queue] rows: {stats['rows']}")
    print(f"[power-queue] skipped_reviewed: {stats['skipped_reviewed']}")
    print(f"[power-queue] episodes_total: {stats['episodes_total']}")
    print(f"[power-queue] file: {Path(args.out_csv).resolve()}")


if __name__ == "__main__":
    main()

