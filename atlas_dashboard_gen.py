"""
atlas_dashboard_gen.py
â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
Reads outputs/ directory and generates a standalone HTML operations dashboard.

Usage:
    python atlas_dashboard_gen.py                          # uses ./outputs
    python atlas_dashboard_gen.py --outputs-dir /path/to/outputs
    python atlas_dashboard_gen.py --open                   # auto-open in browser

Reads:
    outputs/gemini_usage.jsonl       â†’ cost & token metrics per request
    outputs/.task_state/*.json       â†’ per-episode state (submitted, errors, etc.)
    outputs/training_feedback/live/t4_transitions_history.jsonl  â†’ disputes
    outputs/training_feedback/live/alignment_lessons_history.jsonl

Writes:
    outputs/atlas_dashboard.html
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import webbrowser
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Data loaders
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows


def _load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


EPISODE_ID_PATTERN = re.compile(r"([0-9a-f]{24})", re.IGNORECASE)


def _extract_episode_id(text: str) -> Optional[str]:
    m = EPISODE_ID_PATTERN.search(text or "")
    return m.group(1).lower() if m else None


def _merge_state_dicts(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if not items:
        return merged
    for d in items:
        if isinstance(d, dict):
            merged.update(d)

    merged["episode_submitted"] = any(bool(d.get("episode_submitted") or d.get("submitted")) for d in items)
    merged["submitted"] = merged["episode_submitted"]
    merged["labels_applied"] = any(bool(d.get("labels_applied")) for d in items)
    merged["labels_ready"] = any(bool(d.get("labels_ready")) for d in items)
    merged["has_error"] = any(bool(d.get("last_error") or d.get("has_error")) for d in items)

    vals = [d.get("validation_ok") for d in items if d.get("validation_ok") is not None]
    if vals:
        merged["validation_ok"] = any(bool(v) for v in vals)
    return merged


def load_usage(outputs_dir: Path) -> List[Dict[str, Any]]:
    return _load_jsonl(outputs_dir / "gemini_usage.jsonl")


def load_review_index(outputs_dir: Path) -> Dict[str, Any]:
    p = outputs_dir / "episodes_review_index.json"
    data = _load_json(p, default={})
    return data if isinstance(data, dict) else {}


def load_task_states(outputs_dir: Path) -> List[Dict[str, Any]]:
    by_episode: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    # Source 1: legacy .task_state/task_state folders
    candidates = [
        outputs_dir / ".task_state",
        outputs_dir / "task_state",
        outputs_dir.parent / ".task_state",
        outputs_dir.parent / "task_state",
    ]
    state_dir: Optional[Path] = None
    for candidate in candidates:
        if candidate.exists() and any(candidate.glob("*.json")):
            state_dir = candidate
            break
    if state_dir is None:
        for candidate in candidates:
            if candidate.exists():
                state_dir = candidate
                break
    if state_dir is not None and state_dir.exists():
        for f in sorted(state_dir.glob("*.json")):
            data = _load_json(f)
            if not isinstance(data, dict):
                continue
            eid = _extract_episode_id(f.stem) or str(f.stem).lower()
            data.setdefault("_file", f.stem)
            data["_state_source"] = "legacy_task_state"
            data["_episode_id"] = eid
            by_episode[eid].append(data)

    # Source 2: training_feedback runs snapshots (matched_outputs/task_state_*.json)
    runs_root = outputs_dir / "training_feedback" / "runs"
    if runs_root.exists():
        for f in runs_root.rglob("task_state_*.json"):
            data = _load_json(f)
            if not isinstance(data, dict):
                continue
            eid = _extract_episode_id(f.name) or _extract_episode_id(str(data.get("video_path", "")))
            if not eid:
                continue
            data["_file"] = str(f.name)
            data["_state_source"] = "runs_task_state"
            data["_episode_id"] = eid
            by_episode[eid].append(data)

    # Source 3: episodes_review_index.json (if it includes task_state)
    idx = load_review_index(outputs_dir)
    episodes = idx.get("episodes", []) if isinstance(idx, dict) else []
    if isinstance(episodes, list):
        for ep in episodes:
            if not isinstance(ep, dict):
                continue
            eid = str(ep.get("episode_id", "")).strip().lower()
            st = ep.get("task_state")
            if not eid or not isinstance(st, dict):
                continue
            st = dict(st)
            st["_state_source"] = "review_index"
            st["_episode_id"] = eid
            by_episode[eid].append(st)

    # Merge all snapshots per episode
    merged_states: List[Dict[str, Any]] = []
    for eid in sorted(by_episode.keys()):
        merged = _merge_state_dicts(by_episode[eid])
        merged["_episode_id"] = eid
        merged["_snapshots_count"] = len(by_episode[eid])
        merged_states.append(merged)

    return merged_states


def _flatten_transition_rows(node: Any, out: List[Dict[str, Any]]) -> None:
    if isinstance(node, dict):
        # Candidate row: includes any transition-like marker.
        if any(
            k in node
            for k in (
                "dispute_bucket",
                "status",
                "episode_id",
                "task_id",
                "original_labels",
                "corrected_labels",
                "labels_before",
                "labels_after",
                "validator_errors",
                "resolution_notes",
            )
        ):
            out.append(node)
        for v in node.values():
            _flatten_transition_rows(v, out)
    elif isinstance(node, list):
        for v in node:
            _flatten_transition_rows(v, out)


def load_transitions(outputs_dir: Path) -> List[Dict[str, Any]]:
    live_path = outputs_dir / "training_feedback" / "live" / "t4_transitions_history.jsonl"
    rows = _load_jsonl(live_path)
    if rows:
        return rows

    # Fallback: aggregate from runs/*/t4_transitions.json
    rows = []
    runs_glob = outputs_dir / "training_feedback" / "runs"
    for f in sorted(runs_glob.glob("*/t4_transitions.json")):
        data = _load_json(f)
        if data is None:
            continue
        _flatten_transition_rows(data, rows)
    if rows:
        return rows

    # Fallback 2: synthesize dispute rows from episodes_review_index.json
    idx = load_review_index(outputs_dir)
    episodes = idx.get("episodes", []) if isinstance(idx, dict) else []
    generated_at = str(idx.get("generated_at_utc", "") or "")
    if isinstance(episodes, list):
        for ep in episodes:
            if not isinstance(ep, dict):
                continue
            disputes_count = int(ep.get("disputes_count") or 0)
            if disputes_count <= 0:
                continue
            eid = str(ep.get("episode_id") or "").strip()
            if not eid:
                continue
            rows.append(
                {
                    "episode_id": eid,
                    "status": "disputed",
                    "dispute_bucket": "disputed",
                    "ts_utc": generated_at,
                    "_source": "review_index",
                }
            )
    return rows


def load_lessons(outputs_dir: Path) -> List[Dict[str, Any]]:
    return _load_jsonl(
        outputs_dir / "training_feedback" / "live" / "alignment_lessons_history.jsonl"
    )


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Metric computation
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def compute_cost_metrics(usage: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not usage:
        return {
            "total_cost_usd": 0.0, "total_requests": 0,
            "total_input_tokens": 0, "total_output_tokens": 0,
            "by_day": [], "by_model": {}, "avg_cost_per_request": 0.0,
        }

    total_cost = 0.0
    total_input = 0
    total_output = 0
    by_day: Dict[str, float] = defaultdict(float)
    by_model: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"requests": 0, "cost": 0.0, "tokens": 0})

    for row in usage:
        cost = float(row.get("estimated_cost_usd", 0) or 0)
        inp = int(row.get("prompt_tokens", 0) or 0)
        out = int(row.get("output_tokens", 0) or 0)
        model = str(row.get("model", "unknown") or "unknown").strip()
        ts = str(row.get("ts_utc", "") or "")
        day = ts[:10] if len(ts) >= 10 else "unknown"

        total_cost += cost
        total_input += inp
        total_output += out
        by_day[day] += cost
        by_model[model]["requests"] += 1
        by_model[model]["cost"] += cost
        by_model[model]["tokens"] += inp + out

    sorted_days = sorted(by_day.items())
    return {
        "total_cost_usd": round(total_cost, 6),
        "total_requests": len(usage),
        "total_input_tokens": total_input,
        "total_output_tokens": total_output,
        "avg_cost_per_request": round(total_cost / len(usage), 6) if usage else 0.0,
        "by_day": [{"date": d, "cost": round(c, 6)} for d, c in sorted_days],
        "by_model": {
            m: {"requests": v["requests"], "cost": round(v["cost"], 6), "tokens": v["tokens"]}
            for m, v in by_model.items()
        },
    }


def compute_episode_metrics(states: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not states:
        return {
            "total": 0, "submitted": 0, "labels_applied": 0,
            "labels_ready": 0, "has_error": 0,
            "policy_passed": 0, "policy_failed": 0,
            "submit_rate_pct": 0.0,
            "policy_pass_rate_pct": 0.0,
        }
    total = len(states)
    submitted = sum(1 for s in states if s.get("episode_submitted") or s.get("submitted"))
    applied = sum(1 for s in states if s.get("labels_applied"))
    ready = sum(1 for s in states if s.get("labels_ready"))
    has_error = sum(1 for s in states if s.get("last_error") or s.get("has_error"))
    policy_ok = sum(1 for s in states if s.get("validation_ok") is True)
    policy_fail = sum(1 for s in states if s.get("validation_ok") is False)
    return {
        "total": total,
        "submitted": submitted,
        "labels_applied": applied,
        "labels_ready": ready,
        "has_error": has_error,
        "policy_passed": policy_ok,
        "policy_failed": policy_fail,
        "submit_rate_pct": round(submitted / total * 100, 1) if total else 0.0,
        "policy_pass_rate_pct": round(policy_ok / (policy_ok + policy_fail) * 100, 1)
            if (policy_ok + policy_fail) > 0 else 0.0,
    }


def compute_episode_metrics_from_review_index(episodes: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not episodes:
        return compute_episode_metrics([])

    total = len(episodes)
    submitted = 0
    applied = 0
    ready = 0
    has_error = 0
    policy_ok = 0
    policy_fail = 0

    for ep in episodes:
        if not isinstance(ep, dict):
            continue
        status = str(ep.get("review_status") or "").strip().lower()
        if status in {"submitted", "disputed"}:
            submitted += 1

        st = ep.get("task_state")
        if isinstance(st, dict):
            if st.get("labels_applied"):
                applied += 1
            if st.get("labels_ready"):
                ready += 1
            if st.get("last_error") or st.get("has_error"):
                has_error += 1
            if st.get("validation_ok") is True:
                policy_ok += 1
            elif st.get("validation_ok") is False:
                policy_fail += 1
            continue

        # Fallback to validation object when task_state is unavailable.
        v = ep.get("validation")
        if isinstance(v, dict):
            if v.get("ok") is True:
                policy_ok += 1
            elif v.get("ok") is False:
                policy_fail += 1

    return {
        "total": total,
        "submitted": submitted,
        "labels_applied": applied,
        "labels_ready": ready,
        "has_error": has_error,
        "policy_passed": policy_ok,
        "policy_failed": policy_fail,
        "submit_rate_pct": round(submitted / total * 100, 1) if total else 0.0,
        "policy_pass_rate_pct": round(policy_ok / (policy_ok + policy_fail) * 100, 1)
        if (policy_ok + policy_fail) > 0
        else 0.0,
    }


def compute_dispute_metrics(transitions: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not transitions:
        return {"total_disputes": 0, "by_bucket": {}, "recent": []}
    by_bucket: Dict[str, int] = defaultdict(int)
    for t in transitions:
        bucket = str(t.get("dispute_bucket") or t.get("status") or "unknown")
        by_bucket[bucket] += 1

    recent = sorted(
        transitions,
        key=lambda x: str(x.get("ts_utc") or x.get("timestamp") or ""),
        reverse=True
    )[:10]

    return {
        "total_disputes": len(transitions),
        "by_bucket": dict(by_bucket),
        "recent": recent,
    }


def _lesson_ts(row: Dict[str, Any]) -> str:
    for key in ("ts_utc", "timestamp", "generated_at"):
        v = str(row.get(key, "") or "").strip()
        if v:
            return v
    return ""


def _lesson_text(row: Dict[str, Any]) -> str:
    def _parse_maybe_json(value: Any) -> Any:
        if isinstance(value, dict) or isinstance(value, list):
            return value
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        if not (text.startswith("{") or text.startswith("[")):
            return None
        try:
            return json.loads(text)
        except Exception:
            return None

    parsed = _parse_maybe_json(row.get("parsed"))
    if isinstance(parsed, dict):
        updates = parsed.get("global_policy_updates")
        if isinstance(updates, list):
            updates = [str(x).strip() for x in updates if str(x).strip()]
            if updates:
                return " | ".join(updates[:3])

        patterns = parsed.get("top_failure_patterns")
        if isinstance(patterns, list):
            pretty_patterns: List[str] = []
            for p in patterns:
                if isinstance(p, dict):
                    text = str(
                        p.get("pattern")
                        or p.get("summary")
                        or p.get("issue")
                        or p.get("name")
                        or ""
                    ).strip()
                    if not text:
                        text = json.dumps(p, ensure_ascii=False)
                else:
                    text = str(p).strip()
                if text:
                    pretty_patterns.append(text)
            if pretty_patterns:
                return "Top patterns: " + " | ".join(pretty_patterns[:3])

        checklist = parsed.get("reviewer_checklist")
        if isinstance(checklist, list):
            checklist = [str(x).strip() for x in checklist if str(x).strip()]
            if checklist:
                return "Checklist: " + " | ".join(checklist[:3])

        bucket = parsed.get("bucket_summary")
        if isinstance(bucket, dict) and bucket:
            disputed = int(bucket.get("Disputed", 0) or 0)
            awaiting = int(bucket.get("Awaiting T2", 0) or 0)
            both_ok = int(bucket.get("Both OK", 0) or 0)
            episodes = int(row.get("episodes_in_dataset", 0) or (disputed + awaiting + both_ok))
            return (
                f"Dataset episodes: {episodes} | "
                f"Disputed: {disputed}, Awaiting T2: {awaiting}, Both OK: {both_ok}"
            )
        verdicts = parsed.get("episode_verdicts")
        if isinstance(verdicts, list) and verdicts:
            return f"Episode verdicts analyzed: {len(verdicts)}"

    for key in ("lesson_text", "lesson", "summary", "insight", "title", "message"):
        v = str(row.get(key, "") or "").strip()
        if v:
            maybe = _parse_maybe_json(v)
            if isinstance(maybe, dict):
                if "summary" in maybe and str(maybe.get("summary", "")).strip():
                    return str(maybe.get("summary")).strip()
                if "message" in maybe and str(maybe.get("message", "")).strip():
                    return str(maybe.get("message")).strip()
                if "lesson" in maybe and str(maybe.get("lesson", "")).strip():
                    return str(maybe.get("lesson")).strip()
                if "parsed" in maybe:
                    nested = _lesson_text({"parsed": maybe.get("parsed")})
                    if nested and "Lesson snapshot generated" not in nested:
                        return nested
            return v

    run_dir = str(row.get("run_dir", "") or "").strip()
    if run_dir:
        return f"Lesson snapshot generated from {Path(run_dir).name}"
    return "Lesson snapshot generated."


def compute_lesson_metrics(lessons: List[Dict[str, Any]]) -> Dict[str, Any]:
    sorted_rows = sorted(
        lessons,
        key=lambda x: _lesson_ts(x),
        reverse=True,
    )[:5]
    recent = []
    for row in sorted_rows:
        recent.append(
            {
                "display_text": _lesson_text(row),
                "display_ts": _lesson_ts(row),
            }
        )
    return {
        "total_lessons": len(lessons),
        "recent": recent,
    }


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# HTML generation
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en" dir="ltr">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Atlas Pipeline Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Cairo:wght@400;600;700&display=swap');

  :root {
    --bg: #0d0f14;
    --bg2: #141720;
    --bg3: #1c2030;
    --border: #2a2f42;
    --accent: #00e5c8;
    --accent2: #7c6fff;
    --accent3: #ff6b6b;
    --accent4: #ffd166;
    --text: #e2e8f0;
    --text2: #8892a4;
    --mono: 'IBM Plex Mono', monospace;
    --sans: 'Cairo', sans-serif;
  }

  * { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--sans);
    min-height: 100vh;
    padding: 0 0 60px;
  }

  .topbar {
    background: var(--bg2);
    border-bottom: 1px solid var(--border);
    padding: 18px 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: sticky;
    top: 0;
    z-index: 100;
  }

  .topbar-title {
    font-size: 1.1rem;
    font-weight: 700;
    color: var(--accent);
    letter-spacing: 0.04em;
    font-family: var(--mono);
  }

  .topbar-subtitle { font-size: 0.78rem; color: var(--text2); font-family: var(--mono); }

  .badge {
    background: var(--bg3);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 4px 12px;
    font-size: 0.72rem;
    font-family: var(--mono);
    color: var(--accent);
  }

  main { max-width: 1400px; margin: 0 auto; padding: 32px 24px 0; }

  .section-label {
    font-size: 0.7rem;
    font-family: var(--mono);
    color: var(--text2);
    text-transform: uppercase;
    letter-spacing: 0.12em;
    margin-bottom: 14px;
    padding-bottom: 6px;
    border-bottom: 1px solid var(--border);
  }

  /* KPI Grid */
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: 14px;
    margin-bottom: 36px;
  }

  .kpi-card {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px 18px 16px;
    position: relative;
    overflow: hidden;
    transition: border-color 0.2s;
  }

  .kpi-card:hover { border-color: var(--accent); }

  .kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: var(--accent-color, var(--accent));
  }

  .kpi-label {
    font-size: 0.7rem;
    color: var(--text2);
    font-family: var(--mono);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 10px;
  }

  .kpi-value {
    font-size: 2rem;
    font-weight: 700;
    font-family: var(--mono);
    color: var(--accent-color, var(--accent));
    line-height: 1;
  }

  .kpi-sub {
    font-size: 0.68rem;
    color: var(--text2);
    margin-top: 6px;
    font-family: var(--mono);
  }

  /* Charts row */
  .charts-row {
    display: grid;
    grid-template-columns: 2fr 1fr;
    gap: 16px;
    margin-bottom: 36px;
  }

  @media (max-width: 900px) {
    .charts-row { grid-template-columns: 1fr; }
  }

  .chart-card {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px;
  }

  .chart-title {
    font-size: 0.78rem;
    font-family: var(--mono);
    color: var(--text2);
    margin-bottom: 16px;
    text-transform: uppercase;
    letter-spacing: 0.08em;
  }

  .chart-wrap { position: relative; height: 200px; }

  /* Two column layout */
  .two-col {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    margin-bottom: 36px;
  }

  @media (max-width: 780px) {
    .two-col { grid-template-columns: 1fr; }
  }

  .panel {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px;
  }

  .panel-title {
    font-size: 0.75rem;
    font-family: var(--mono);
    color: var(--text2);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 14px;
    padding-bottom: 10px;
    border-bottom: 1px solid var(--border);
  }

  /* Table */
  .data-table { width: 100%; border-collapse: collapse; font-size: 0.78rem; }

  .data-table th {
    text-align: left;
    padding: 8px 10px;
    color: var(--text2);
    font-family: var(--mono);
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    border-bottom: 1px solid var(--border);
  }

  .data-table td {
    padding: 9px 10px;
    border-bottom: 1px solid var(--border);
    color: var(--text);
    font-family: var(--mono);
    font-size: 0.75rem;
  }

  .data-table tr:last-child td { border-bottom: none; }
  .data-table tr:hover td { background: var(--bg3); }

  /* Status dots */
  .dot {
    display: inline-block;
    width: 7px; height: 7px;
    border-radius: 50%;
    margin-left: 6px;
    vertical-align: middle;
  }
  .dot-green { background: #22c55e; box-shadow: 0 0 6px #22c55e80; }
  .dot-red   { background: var(--accent3); box-shadow: 0 0 6px #ff6b6b80; }
  .dot-yellow{ background: var(--accent4); }
  .dot-blue  { background: var(--accent2); }

  /* Progress bar */
  .progress-wrap { background: var(--bg3); border-radius: 4px; height: 6px; overflow: hidden; margin-top: 4px; }
  .progress-fill { height: 100%; border-radius: 4px; background: var(--accent); transition: width 0.6s ease; }

  /* Model pills */
  .pill {
    display: inline-block;
    background: var(--bg3);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 2px 8px;
    font-family: var(--mono);
    font-size: 0.68rem;
    color: var(--accent2);
  }

  /* Lesson card */
  .lesson-card {
    background: var(--bg3);
    border: 1px solid var(--border);
    border-right: 3px solid var(--accent2);
    border-radius: 6px;
    padding: 12px 14px;
    margin-bottom: 10px;
    font-size: 0.76rem;
    line-height: 1.5;
  }

  .lesson-ts {
    font-family: var(--mono);
    font-size: 0.65rem;
    color: var(--text2);
    margin-top: 6px;
  }

  /* No data */
  .no-data {
    color: var(--text2);
    font-family: var(--mono);
    font-size: 0.76rem;
    text-align: center;
    padding: 30px;
    opacity: 0.6;
  }

  .notice {
    background: #1b2130;
    border: 1px solid #30405f;
    border-right: 4px solid var(--accent4);
    border-radius: 8px;
    padding: 12px 14px;
    margin-bottom: 18px;
    font-family: var(--mono);
    font-size: 0.72rem;
    color: var(--text);
    line-height: 1.6;
  }

  .completeness-panel {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 14px;
    margin-bottom: 20px;
  }
  .completeness-title {
    font-size: 0.75rem;
    font-family: var(--mono);
    color: var(--text2);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 10px;
  }
  .completeness-row {
    margin-bottom: 10px;
  }
  .completeness-row:last-child {
    margin-bottom: 0;
  }
  .completeness-label {
    display: flex;
    justify-content: space-between;
    font-family: var(--mono);
    font-size: 0.68rem;
    color: var(--text2);
    margin-bottom: 4px;
  }

  /* Footer */
  .footer {
    margin-top: 50px;
    padding: 18px 32px;
    border-top: 1px solid var(--border);
    font-family: var(--mono);
    font-size: 0.65rem;
    color: var(--text2);
    text-align: center;
  }
</style>
</head>
<body>

<div class="topbar">
  <div>
    <div class="topbar-title">Atlas Pipeline Dashboard | لوحة متابعة أطلس</div>
    <div class="topbar-subtitle">OCR_Annotation_Atlas · aymank2020</div>
  </div>
  <div class="badge">Generated | وقت الإنشاء: __GENERATED_AT__</div>
</div>

<main>
  <div id="coverage-note"></div>
  <div id="completeness-panel" class="completeness-panel"></div>

  <!-- KPIs -->
  <div class="section-label">Key Metrics | مؤشرات الأداء</div>
  <div class="kpi-grid" id="kpi-grid"></div>

  <!-- Cost chart + Model breakdown -->
  <div class="section-label">Cost & Resources | التكلفة والموارد</div>
  <div class="charts-row">
    <div class="chart-card">
      <div class="chart-title">Daily Cost (USD) | التكلفة اليومية</div>
      <div class="chart-wrap"><canvas id="costChart"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-title">Model Distribution | توزيع النماذج</div>
      <div class="chart-wrap"><canvas id="modelChart"></canvas></div>
    </div>
  </div>

  <!-- Episode status + Policy gate -->
  <div class="section-label">Episodes & Policy Gate</div>
  <div class="two-col">

    <div class="panel">
      <div class="panel-title">Episode Status | حالة الحلقات</div>
      <div id="episode-panel"></div>
    </div>

    <div class="panel">
      <div class="panel-title">Model Usage by Request | استخدام النماذج حسب الطلب</div>
      <div id="model-table-panel"></div>
    </div>

  </div>

  <!-- Disputes + Lessons -->
  <div class="section-label">Continuous Learning | التعلم المستمر</div>
  <div class="two-col">

    <div class="panel">
      <div class="panel-title">Disputes / T4 Transitions | النزاعات <span id="dispute-count" class="pill"></span></div>
      <div id="dispute-panel"></div>
    </div>

    <div class="panel">
      <div class="panel-title">Latest Lessons | آخر الدروس</div>
      <div id="lessons-panel"></div>
    </div>

  </div>

</main>

<div class="footer">
  Atlas Pipeline Dashboard · Auto-generated · Data from outputs/ directory
</div>

<script>
const DATA = __JSON_DATA__;

function asPercentOrNA(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "N/A";
  return `${Number(v).toFixed(1)}%`;
}

function asWidth(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return 0;
  return Math.max(0, Math.min(100, Number(v)));
}

// Data coverage notice
const coverageMsgs = [];
if (!DATA.coverage.has_task_state) {
  coverageMsgs.push("Episode status source not found (.task_state, runs/*/matched_outputs/task_state_*.json, or episodes_review_index.json) | مصدر حالة الحلقات غير موجود");
}
if (DATA.coverage.has_runs_task_state_files && DATA.episodes.total > 0) {
  coverageMsgs.push("Episode status loaded from runs snapshots | تم تحميل حالة الحلقات من لقطات runs");
}
if (!DATA.coverage.has_live_transitions && DATA.coverage.has_runs_transition_files) {
  coverageMsgs.push("Using fallback source runs/*/t4_transitions.json | استخدام مصدر احتياطي من ملفات runs");
}
if (DATA.coverage.has_runs_transition_files && DATA.disputes.total_disputes === 0) {
  coverageMsgs.push("Transition files exist but contain no dispute rows yet | الملفات موجودة لكن بدون حالات نزاع");
}
if (DATA.cost.total_requests > 0 && DATA.episodes.total === 0) {
  coverageMsgs.push("Requests exist but episode states are zero (likely dry_run mode or missing task_state files) | توجد طلبات لكن حالات الحلقات صفر (غالباً وضع dry_run أو ملفات task_state غير موجودة)");
}
if (coverageMsgs.length > 0) {
  const note = document.getElementById("coverage-note");
  note.className = "notice";
  note.innerHTML = coverageMsgs.map(m => `- ${m}`).join("<br/>");
}

// Data completeness panel
const c = DATA.coverage || {};
const sRows = c.source_rows || {};
const sAvail = c.source_available || {};
const cPanel = document.getElementById("completeness-panel");
const sources = [
  { key: "usage", label: "Usage Log | سجل الاستخدام" },
  { key: "task_state", label: "Episode State | حالة الحلقات" },
  { key: "transitions", label: "Transitions | التحولات" },
  { key: "lessons", label: "Lessons | الدروس" },
  { key: "review_index", label: "Review Index | فهرس المراجعة" },
];
cPanel.innerHTML = `
  <div class="completeness-title">Data Completeness | اكتمال البيانات (${asPercentOrNA(c.completeness_pct)})</div>
  ${sources.map(src => {
    const ok = !!sAvail[src.key];
    const pct = ok ? 100 : 0;
    const count = Number(sRows[src.key] || 0);
    return `
      <div class="completeness-row">
        <div class="completeness-label">
          <span>${src.label}</span>
          <span>${ok ? "Available | متاح" : "Missing | غير متاح"} (${count})</span>
        </div>
        <div class="progress-wrap">
          <div class="progress-fill" style="width:${pct}%;background:${ok ? "var(--accent)" : "var(--accent3)"}"></div>
        </div>
      </div>
    `;
  }).join("")}
`;

// KPIs
const kpis = [
  {
    label: "Total Cost (USD) | إجمالي التكلفة",
    value: "$" + DATA.cost.total_cost_usd.toFixed(4),
    sub: DATA.cost.total_requests + " requests | طلب",
    color: "#00e5c8"
  },
  {
    label: "Cost / Request | تكلفة الطلب",
    value: "$" + DATA.cost.avg_cost_per_request.toFixed(6),
    sub: "avg per API call | متوسط لكل استدعاء",
    color: "#00e5c8"
  },
  {
    label: "Input Tokens | توكنات الإدخال",
    value: (DATA.cost.total_input_tokens / 1000).toFixed(1) + "K",
    sub: "prompt tokens total | إجمالي توكنات البرومبت",
    color: "#7c6fff"
  },
  {
    label: "Output Tokens | توكنات الإخراج",
    value: (DATA.cost.total_output_tokens / 1000).toFixed(1) + "K",
    sub: "candidate tokens total | إجمالي توكنات الناتج",
    color: "#7c6fff"
  },
  {
    label: "Episodes | الحلقات",
    value: DATA.episodes.total,
    sub: DATA.episodes.submitted + " submitted | تم الإرسال",
    color: "#ffd166"
  },
  {
    label: "Submit Rate | معدل الإرسال",
    value: DATA.episodes.submit_rate_pct + "%",
    sub: DATA.episodes.submitted + "/" + DATA.episodes.total,
    color: "#22c55e"
  },
  {
    label: "Policy Pass Rate | معدل نجاح السياسة",
    value: asPercentOrNA(DATA.episodes.policy_pass_rate_pct),
    sub: DATA.episodes.policy_passed + " passed / " + DATA.episodes.policy_failed + " failed",
    color: "#22c55e"
  },
  {
    label: "Disputes | النزاعات",
    value: DATA.disputes.total_disputes,
    sub: "T4 transitions captured | حالات T4 الملتقطة",
    color: "#ff6b6b"
  },
];

const grid = document.getElementById("kpi-grid");
kpis.forEach(k => {
  const card = document.createElement("div");
  card.className = "kpi-card";
  card.style.setProperty("--accent-color", k.color);
  card.innerHTML = `
    <div class="kpi-label">${k.label}</div>
    <div class="kpi-value">${k.value}</div>
    <div class="kpi-sub">${k.sub}</div>
  `;
  grid.appendChild(card);
});

// Cost chart
const dayData = DATA.cost.by_day;
if (dayData.length > 0) {
  const ctx = document.getElementById("costChart").getContext("2d");
  new Chart(ctx, {
    type: "bar",
    data: {
      labels: dayData.map(d => d.date),
      datasets: [{
        label: "Cost USD",
        data: dayData.map(d => d.cost),
        backgroundColor: "#00e5c830",
        borderColor: "#00e5c8",
        borderWidth: 1.5,
        borderRadius: 4,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: "#8892a4", font: { family: "IBM Plex Mono", size: 10 } }, grid: { color: "#2a2f42" } },
        y: { ticks: { color: "#8892a4", font: { family: "IBM Plex Mono", size: 10 } }, grid: { color: "#2a2f42" } }
      }
    }
  });
} else {
  document.getElementById("costChart").parentElement.innerHTML = '<div class="no-data">No cost data yet | لا توجد بيانات تكلفة بعد</div>';
}

// Model pie
const modelData = DATA.cost.by_model;
const modelKeys = Object.keys(modelData);
if (modelKeys.length > 0) {
  const ctx2 = document.getElementById("modelChart").getContext("2d");
  const palette = ["#00e5c8","#7c6fff","#ffd166","#ff6b6b","#22c55e","#38bdf8"];
  new Chart(ctx2, {
    type: "doughnut",
    data: {
      labels: modelKeys.map(k => k.replace("gemini-","g-")),
      datasets: [{
        data: modelKeys.map(k => modelData[k].requests),
        backgroundColor: palette.slice(0, modelKeys.length),
        borderColor: "#141720",
        borderWidth: 2,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: {
          position: "bottom",
          labels: { color: "#8892a4", font: { family: "IBM Plex Mono", size: 10 }, padding: 10 }
        }
      }
    }
  });
} else {
  document.getElementById("modelChart").parentElement.innerHTML = '<div class="no-data">No data available | لا توجد بيانات</div>';
}

// Episode panel
const ep = DATA.episodes;
const epPanel = document.getElementById("episode-panel");
const epRows = [
  ["Submitted | تم الإرسال",      ep.submitted,      "dot-green"],
  ["Labels Applied | تم تطبيق التسميات", ep.labels_applied, "dot-green"],
  ["Labels Ready | التسميات جاهزة",   ep.labels_ready,   "dot-blue"],
  ["Policy Passed | سياسة ناجحة",  ep.policy_passed,  "dot-green"],
  ["Policy Failed | سياسة فاشلة",  ep.policy_failed,  "dot-red"],
  ["Has Error | يوجد خطأ",      ep.has_error,      "dot-red"],
];
epPanel.innerHTML = epRows.map(([label, val, dot]) => `
  <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border)">
    <span style="font-family:var(--mono);font-size:0.75rem;color:var(--text2)">${label}</span>
    <span style="font-family:var(--mono);font-size:0.8rem">
      <span class="dot ${dot}"></span>${val}
    </span>
  </div>
`).join("") + `
  <div style="margin-top:16px">
    <div style="font-family:var(--mono);font-size:0.68rem;color:var(--text2);margin-bottom:6px">Submit Rate | معدل الإرسال: ${ep.submit_rate_pct}%</div>
    <div class="progress-wrap"><div class="progress-fill" style="width:${ep.submit_rate_pct}%"></div></div>
    <div style="font-family:var(--mono);font-size:0.68rem;color:var(--text2);margin-top:10px;margin-bottom:6px">Policy Pass | نجاح السياسة: ${asPercentOrNA(ep.policy_pass_rate_pct)}</div>
    <div class="progress-wrap"><div class="progress-fill" style="width:${asWidth(ep.policy_pass_rate_pct)}%;background:var(--accent4)"></div></div>
  </div>
`;

// Model table
const mtPanel = document.getElementById("model-table-panel");
if (modelKeys.length > 0) {
  mtPanel.innerHTML = `
    <table class="data-table">
      <thead><tr>
        <th>Model | النموذج</th><th>Requests | الطلبات</th><th>Cost $ | التكلفة</th><th>Tokens | التوكنز</th>
      </tr></thead>
      <tbody>
        ${modelKeys.map(k => `<tr>
          <td><span class="pill">${k.replace("gemini-","g-")}</span></td>
          <td>${modelData[k].requests}</td>
          <td>${modelData[k].cost.toFixed(6)}</td>
          <td>${(modelData[k].tokens/1000).toFixed(1)}K</td>
        </tr>`).join("")}
      </tbody>
    </table>
  `;
} else {
  mtPanel.innerHTML = '<div class="no-data">No data available | لا توجد بيانات</div>';
}

// Disputes
document.getElementById("dispute-count").textContent = DATA.disputes.total_disputes;
const dispPanel = document.getElementById("dispute-panel");
if (DATA.disputes.recent.length > 0) {
  const bucketColors = { disputed: "#ff6b6b", awaiting_t2: "#ffd166", both_ok: "#22c55e" };
  dispPanel.innerHTML = `
    <table class="data-table">
      <thead><tr><th>Episode | الحلقة</th><th>Status | الحالة</th><th>Date | التاريخ</th></tr></thead>
      <tbody>
        ${DATA.disputes.recent.map(t => {
          const bucket = t.dispute_bucket || t.status || "?";
          const col = bucketColors[bucket] || "#8892a4";
          const ts = (t.ts_utc || t.timestamp || "").substring(0,10);
          const eid = t.episode_id || t._file || "—";
          return `<tr>
            <td style="font-size:0.68rem">${eid.substring(0,16)}</td>
            <td><span style="color:${col};font-family:var(--mono);font-size:0.68rem">${bucket}</span></td>
            <td style="font-size:0.68rem">${ts}</td>
          </tr>`;
        }).join("")}
      </tbody>
    </table>
  `;
} else {
  dispPanel.innerHTML = '<div class="no-data">No disputes yet | لا توجد نزاعات بعد</div>';
}

// Lessons
const lessPanel = document.getElementById("lessons-panel");
if (DATA.lessons.recent.length > 0) {
  lessPanel.innerHTML = DATA.lessons.recent.map(l => {
    const text = l.display_text || "Lesson snapshot generated.";
    const ts = (l.display_ts || "").toString().substring(0,19);
    return `
      <div class="lesson-card">
        ${text.substring(0,300)}${text.length > 300 ? "…" : ""}
        <div class="lesson-ts">${ts}</div>
      </div>
    `;
  }).join("");
} else {
  lessPanel.innerHTML = '<div class="no-data">No lessons yet | لا توجد دروس بعد</div>';
}
</script>
</body>
</html>
"""


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Main
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def generate_dashboard(outputs_dir: Path, open_browser: bool = False) -> Path:
    print(f"[dashboard] reading outputs from: {outputs_dir}")

    usage = load_usage(outputs_dir)
    states = load_task_states(outputs_dir)
    transitions = load_transitions(outputs_dir)
    lessons = load_lessons(outputs_dir)
    review_index = load_review_index(outputs_dir)

    task_state_candidates = [
        outputs_dir / ".task_state",
        outputs_dir / "task_state",
        outputs_dir.parent / ".task_state",
        outputs_dir.parent / "task_state",
    ]
    task_state_dir_exists = any(p.exists() and p.is_dir() for p in task_state_candidates)
    runs_root = outputs_dir / "training_feedback" / "runs"
    has_runs_task_state_files = any(runs_root.rglob("task_state_*.json")) if runs_root.exists() else False
    live_transitions_path = outputs_dir / "training_feedback" / "live" / "t4_transitions_history.jsonl"
    has_live_transitions = bool(live_transitions_path.exists() and live_transitions_path.stat().st_size > 0)
    has_runs_transition_files = any(runs_root.glob("*/t4_transitions.json")) if runs_root.exists() else False
    has_review_index = isinstance(review_index, dict) and isinstance(review_index.get("episodes"), list)

    source_rows = {
        "usage": len(usage),
        "task_state": len(states),
        "transitions": len(transitions),
        "lessons": len(lessons),
        "review_index": len(review_index.get("episodes", [])) if has_review_index else 0,
    }
    source_available = {
        "usage": source_rows["usage"] > 0,
        "task_state": source_rows["task_state"] > 0,
        "transitions": source_rows["transitions"] > 0,
        "lessons": source_rows["lessons"] > 0,
        "review_index": source_rows["review_index"] > 0,
    }
    completeness_pct = round(sum(1 for v in source_available.values() if v) / len(source_available) * 100, 1)

    coverage = {
        "has_task_state": source_rows["task_state"] > 0,
        "task_state_dir_exists": task_state_dir_exists,
        "has_runs_task_state_files": has_runs_task_state_files,
        "has_live_transitions": has_live_transitions,
        "has_runs_transition_files": has_runs_transition_files,
        "has_review_index": has_review_index,
        "source_rows": source_rows,
        "source_available": source_available,
        "completeness_pct": completeness_pct,
    }

    print(f"[dashboard] usage_rows={len(usage)} task_states={len(states)} "
          f"transitions={len(transitions)} lessons={len(lessons)}")

    data = {
        "cost": compute_cost_metrics(usage),
        "episodes": compute_episode_metrics_from_review_index(review_index.get("episodes", []))
        if has_review_index
        else compute_episode_metrics(states),
        "disputes": compute_dispute_metrics(transitions),
        "lessons": compute_lesson_metrics(lessons),
        "coverage": coverage,
    }

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = HTML_TEMPLATE.replace("__JSON_DATA__", json.dumps(data, ensure_ascii=False))
    html = html.replace("__GENERATED_AT__", generated_at)

    out_path = outputs_dir / "atlas_dashboard.html"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"[dashboard] saved: {out_path}")

    if open_browser:
        webbrowser.open(out_path.as_uri())

    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Atlas Pipeline Dashboard")
    parser.add_argument("--outputs-dir", default="outputs", help="Path to outputs directory")
    parser.add_argument("--open", action="store_true", help="Open dashboard in browser after generating")
    args = parser.parse_args()
    generate_dashboard(Path(args.outputs_dir), open_browser=args.open)


if __name__ == "__main__":
    main()

