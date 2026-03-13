"""
Store/merge Gemini Chat evaluation results by episode.

This script is intended for Power Automate / external automation pipelines.
It updates outputs/gemini_chat_evaluations.json with a per-episode record.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _parse_score(text: str) -> Optional[int]:
    src = str(text or "")
    m = re.search(
        r"(?:score|confidence|accuracy|quality|percent|نسبة|تقييم)\s*[:=\-]?\s*(\d{1,3})\s*%?",
        src,
        flags=re.I,
    ) or re.search(r"(\d{1,3})\s*%", src)
    if not m:
        return None
    try:
        n = int(m.group(1))
    except Exception:
        return None
    return max(0, min(100, n))


def _write_episode_chat_text_file(outputs_dir: Path, episode_id: str, text: str) -> Optional[Path]:
    eid = str(episode_id or "").strip().lower()
    if not eid:
        return None
    target_dir = outputs_dir / "chat_reviews" / eid
    target_dir.mkdir(parents=True, exist_ok=True)
    out_path = target_dir / f"text_{eid}_chat.txt"
    out_path.write_text(str(text or ""), encoding="utf-8")
    return out_path


def upsert_evaluation(
    outputs_dir: Path,
    episode_id: str,
    text: str,
    score_pct: Optional[int],
    source: str,
) -> Path:
    outputs_dir.mkdir(parents=True, exist_ok=True)
    path = outputs_dir / "gemini_chat_evaluations.json"
    payload = _load_json(path, default={})
    if not isinstance(payload, dict):
        payload = {}

    evaluations = payload.get("evaluations")
    if not isinstance(evaluations, dict):
        evaluations = {}

    eid = str(episode_id or "").strip().lower()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    resolved_score = _parse_score(text) if score_pct is None else max(0, min(100, int(score_pct)))
    evaluations[eid] = {
        "episode_id": eid,
        "text": text,
        "score_pct": resolved_score,
        "source": source,
        "updated_at_utc": now,
    }

    out = {
        "generated_at_utc": now,
        "source": "atlas_eval_store",
        "evaluations": evaluations,
    }
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        _write_episode_chat_text_file(outputs_dir, eid, text)
    except Exception:
        pass
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Upsert episode evaluation into gemini_chat_evaluations.json")
    parser.add_argument("--outputs-dir", default="outputs", help="Outputs directory")
    parser.add_argument("--episode-id", required=True, help="Atlas episode id")
    parser.add_argument("--text", default="", help="Evaluation text")
    parser.add_argument("--text-file", default="", help="Path to text file containing evaluation")
    parser.add_argument("--score-pct", type=int, default=None, help="Optional numeric score 0..100")
    parser.add_argument("--source", default="power_automate", help="Evaluation source label")
    args = parser.parse_args()

    text = str(args.text or "")
    if args.text_file:
        p = Path(args.text_file)
        if p.exists():
            text = p.read_text(encoding="utf-8", errors="replace")

    out = upsert_evaluation(
        outputs_dir=Path(args.outputs_dir),
        episode_id=str(args.episode_id),
        text=text,
        score_pct=args.score_pct,
        source=str(args.source or "power_automate"),
    )
    print(f"[eval-store] updated: {out}")
    eid = str(args.episode_id or "").strip().lower()
    if eid:
        chat_file = Path(args.outputs_dir) / "chat_reviews" / eid / f"text_{eid}_chat.txt"
        print(f"[eval-store] chat_text_file: {chat_file.resolve()}")


if __name__ == "__main__":
    main()

