"""
Build a single review index for all Atlas episodes from outputs/.

The index is intended for manual/AI audit loops (ChatGPT/Claude/Gemini)
to compare Tier2 vs Tier3 and inspect validation/disputes per episode.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


EPISODE_ID_PATTERN = re.compile(r"([0-9a-f]{24})", re.IGNORECASE)


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _extract_episode_id(text: str) -> Optional[str]:
    m = EPISODE_ID_PATTERN.search(text or "")
    return m.group(1).lower() if m else None


def _pick_first(paths: List[Path]) -> Optional[str]:
    if not paths:
        return None
    return str(sorted(paths)[0])


def _find_task_state_dirs(outputs_dir: Path) -> List[Path]:
    candidates = [
        outputs_dir / ".task_state",
        outputs_dir / "task_state",
        outputs_dir.parent / ".task_state",
        outputs_dir.parent / "task_state",
    ]
    return [p for p in candidates if p.exists() and p.is_dir()]


def _collect_episode_files(outputs_dir: Path) -> Dict[str, Dict[str, List[Path]]]:
    """
    Build a file map by episode id.
    file_map[eid][bucket] = [paths...]
    """
    file_map: Dict[str, Dict[str, List[Path]]] = defaultdict(lambda: defaultdict(list))

    # Targeted buckets from outputs root / nested folders.
    for path in outputs_dir.rglob("*"):
        if not path.is_file():
            continue
        eid = _extract_episode_id(path.name)
        if not eid:
            continue
        name_l = path.name.lower()
        rel = path.relative_to(outputs_dir)

        if name_l.startswith("video_") and path.suffix.lower() in {".mp4", ".webm", ".mov"}:
            file_map[eid]["videos"].append(path)
        if name_l.startswith("text_") and name_l.endswith("_current.txt"):
            file_map[eid]["tier2_text"].append(path)
        if name_l.startswith("text_") and name_l.endswith("_update.txt"):
            file_map[eid]["tier3_text"].append(path)
        if name_l.startswith("validation_") and path.suffix.lower() == ".json":
            file_map[eid]["validation"].append(path)
        if name_l.startswith("labels_") and path.suffix.lower() == ".json":
            file_map[eid]["labels"].append(path)
        if name_l.startswith("segments_") and path.suffix.lower() == ".json":
            file_map[eid]["segments"].append(path)
        if name_l.startswith("prompt_") and path.suffix.lower() == ".txt":
            file_map[eid]["prompts"].append(path)

        # Keep a bounded list of related files per episode.
        if len(file_map[eid]["related"]) < 200:
            file_map[eid]["related"].append(outputs_dir / rel)

    # Look into training_feedback runs/episodes/<episode_id>/ for tier files.
    runs_root = outputs_dir / "training_feedback" / "runs"
    if runs_root.exists():
        for ep_dir in runs_root.glob("*/episodes/*"):
            if not ep_dir.is_dir():
                continue
            eid = _extract_episode_id(ep_dir.name)
            if not eid:
                continue
            for p in ep_dir.glob("*.json"):
                name_l = p.name.lower()
                if "tier2" in name_l or "draft" in name_l:
                    file_map[eid]["tier2_json"].append(p)
                elif "tier3" in name_l or "final" in name_l or "repaired" in name_l:
                    file_map[eid]["tier3_json"].append(p)
                elif "valid" in name_l:
                    file_map[eid]["validation"].append(p)
                if len(file_map[eid]["related"]) < 200:
                    file_map[eid]["related"].append(p)

    return file_map


def _load_task_states(outputs_dir: Path) -> Dict[str, Dict[str, Any]]:
    states: Dict[str, Dict[str, Any]] = {}
    for state_dir in _find_task_state_dirs(outputs_dir):
        for f in state_dir.glob("*.json"):
            payload = _load_json(f)
            if not isinstance(payload, dict):
                continue
            eid = _extract_episode_id(f.stem) or f.stem.lower()
            states[eid] = payload
    return states


def _load_disputes(outputs_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    by_ep: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    live = outputs_dir / "training_feedback" / "live" / "t4_transitions_history.jsonl"
    for row in _load_jsonl(live):
        eid = str(row.get("episode_id") or "").lower().strip()
        if eid:
            by_ep[eid].append(row)
    return by_ep


def _load_usage(outputs_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    by_ep: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    usage_rows = _load_jsonl(outputs_dir / "gemini_usage.jsonl")
    for row in usage_rows:
        eid = str(row.get("episode_id") or row.get("task_id") or "").lower().strip()
        if eid:
            by_ep[eid].append(row)
    return by_ep


def _calc_status(state: Dict[str, Any], disputes_count: int, has_tier3: bool) -> str:
    submitted = bool(state.get("episode_submitted") or state.get("submitted"))
    labels_applied = bool(state.get("labels_applied"))
    policy_ok = state.get("validation_ok")
    has_error = bool(state.get("last_error") or state.get("has_error"))

    if disputes_count > 0:
        return "disputed"
    if policy_ok is False:
        return "policy_fail"
    if has_error:
        return "error"
    if submitted:
        return "submitted"
    if labels_applied or has_tier3:
        return "labeled_not_submitted"
    return "unknown"


def _total_cost(rows: List[Dict[str, Any]]) -> float:
    return round(sum(float(r.get("estimated_cost_usd", 0) or 0.0) for r in rows), 6)


def build_index(outputs_dir: Path) -> Dict[str, Any]:
    outputs_dir = outputs_dir.resolve()
    print(f"[review-builder] outputs_dir={outputs_dir}")

    file_map = _collect_episode_files(outputs_dir)
    states = _load_task_states(outputs_dir)
    disputes = _load_disputes(outputs_dir)
    usage = _load_usage(outputs_dir)

    all_ids = set(file_map.keys()) | set(states.keys()) | set(disputes.keys()) | set(usage.keys())
    print(f"[review-builder] discovered episode ids: {len(all_ids)}")

    episodes: List[Dict[str, Any]] = []
    for eid in sorted(all_ids):
        files = file_map.get(eid, {})
        state = states.get(eid, {})
        ep_disputes = disputes.get(eid, [])
        ep_usage = usage.get(eid, [])

        tier2_text_path = _pick_first(files.get("tier2_text", []))
        tier3_text_path = _pick_first(files.get("tier3_text", []))
        validation_path = _pick_first(files.get("validation", []))

        tier2_text = Path(tier2_text_path).read_text(encoding="utf-8", errors="replace").strip() if tier2_text_path else ""
        tier3_text = Path(tier3_text_path).read_text(encoding="utf-8", errors="replace").strip() if tier3_text_path else ""

        tier2_json = _load_json(Path(_pick_first(files.get("tier2_json", [])))) if files.get("tier2_json") else None
        tier3_json = _load_json(Path(_pick_first(files.get("tier3_json", [])))) if files.get("tier3_json") else None
        validation = _load_json(Path(validation_path)) if validation_path else None

        status = _calc_status(state, len(ep_disputes), bool(tier3_text or tier3_json))
        episode_entry = {
            "episode_id": eid,
            "atlas_url": f"https://audit.atlascapture.io/tasks/room/normal/label/{eid}",
            "review_status": status,
            "video_path": _pick_first(files.get("videos", [])),
            "tier2_text_path": tier2_text_path,
            "tier3_text_path": tier3_text_path,
            "tier2_text": tier2_text,
            "tier3_text": tier3_text,
            "tier2": tier2_json,
            "tier3": tier3_json,
            "validation_path": validation_path,
            "validation": validation,
            "task_state": state,
            "disputes_count": len(ep_disputes),
            "disputes": ep_disputes,
            "usage": ep_usage,
            "total_cost_usd": _total_cost(ep_usage),
            "related_files": [str(p) for p in files.get("related", [])],
        }
        episodes.append(episode_entry)

    priority = {
        "disputed": 0,
        "policy_fail": 1,
        "error": 2,
        "labeled_not_submitted": 3,
        "submitted": 4,
        "unknown": 5,
    }
    episodes.sort(key=lambda x: (priority.get(x["review_status"], 99), x["episode_id"]))

    counts = Counter(ep["review_status"] for ep in episodes)
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "outputs_dir": str(outputs_dir),
        "total": len(episodes),
        "status_counts": dict(counts),
        "episodes": episodes,
    }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Build episodes review index for Atlas QA re-audit.")
    parser.add_argument("--outputs-dir", default="outputs", help="Path to outputs directory")
    parser.add_argument(
        "--out",
        default="episodes_review_index.json",
        help="Output JSON file path (recommended: outputs/episodes_review_index.json)",
    )
    args = parser.parse_args()

    payload = build_index(Path(args.outputs_dir))
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[review-builder] wrote: {out_path}")
    print(f"[review-builder] total episodes: {payload['total']}")
    for status, count in sorted(payload["status_counts"].items()):
        print(f"  - {status}: {count}")


if __name__ == "__main__":
    main()
