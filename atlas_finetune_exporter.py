"""
atlas_finetune_exporter.py
──────────────────────────
Reads accumulated disputes, T4 transitions, and alignment lessons from
atlas_feedback_training_export.py outputs and converts them into
fine-tuning JSONL files for Gemini (supervised) or OpenAI (chat completion).

Usage:
    python atlas_finetune_exporter.py                          # uses ./outputs
    python atlas_finetune_exporter.py --outputs-dir /path/to/outputs
    python atlas_finetune_exporter.py --format openai          # default: both
    python atlas_finetune_exporter.py --min-quality 0.7       # skip low confidence
    python atlas_finetune_exporter.py --dry-run                # print stats only

Reads:
    outputs/training_feedback/live/t4_transitions_history.jsonl
    outputs/training_feedback/live/alignment_lessons_history.jsonl
    outputs/training_feedback/runs/*/t4_transitions.json
    outputs/*.json  (final annotation outputs with validation reports)

Writes:
    outputs/finetune/
        atlas_finetune_openai.jsonl       → OpenAI chat fine-tuning format
        atlas_finetune_gemini.jsonl       → Gemini supervised fine-tuning format
        atlas_finetune_summary.json       → stats + sample review
        atlas_finetune_review.html        → human review page
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ──────────────────────────────────────────────────────────────────────────────
# System prompt for fine-tuning (mirrors prompts.py VIDEO_ANNOTATION_PROMPT)
# ──────────────────────────────────────────────────────────────────────────────

FINETUNE_SYSTEM_PROMPT = """You are an Atlas Standard Tier-3 labeling assistant for egocentric hand-action annotation.

Rules (strictly enforced):
- one segment = one continuous hand-object interaction toward one goal
- imperative voice only, no -ing verb starts
- forbidden verbs: inspect, check, reach, examine (reach allowed only at truncated video end)
- forbidden narrative words: then, another, continue, next, again
- no numerals in labels, no articles (a/an/the)
- max 2 atomic actions per label (one comma or one "and")
- No Action: only when hands touch nothing or ego is idle — never mixed with action
- place must include location (place box on shelf, not: place box)
- attach every verb to its object
- dense/coarse never mixed in one segment
- never hallucinate unseen actions or objects

Output strict JSON only. First field must be step_by_step_reasoning (max 2 sentences), then segments."""


# ──────────────────────────────────────────────────────────────────────────────
# Loaders
# ──────────────────────────────────────────────────────────────────────────────

def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
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


def load_all_transitions(outputs_dir: Path) -> List[Dict[str, Any]]:
    """Load T4 transitions from live file + all per-run files (dedup by episode_id)."""
    seen: set = set()
    rows: List[Dict[str, Any]] = []

    # Live file (most up to date)
    live_path = outputs_dir / "training_feedback" / "live" / "t4_transitions_history.jsonl"
    for row in _load_jsonl(live_path):
        eid = str(row.get("episode_id", "")).strip()
        if eid and eid not in seen:
            seen.add(eid)
            rows.append(row)

    # Per-run snapshots (may add older episodes not in live)
    runs_dir = outputs_dir / "training_feedback" / "runs"
    if runs_dir.exists():
        for run_dir in sorted(runs_dir.iterdir()):
            run_file = run_dir / "t4_transitions.json"
            data = _load_json(run_file, default=[])
            if isinstance(data, list):
                for row in data:
                    eid = str(row.get("episode_id", "")).strip()
                    if eid and eid not in seen:
                        seen.add(eid)
                        rows.append(row)

    return rows


def load_final_annotations(outputs_dir: Path) -> List[Dict[str, Any]]:
    """Load _final.json files — these are validated annotation outputs."""
    results: List[Dict[str, Any]] = []
    for f in sorted(outputs_dir.glob("*_final.json")):
        data = _load_json(f)
        if isinstance(data, dict) and "segments" in data:
            data.setdefault("_source_file", f.name)
            results.append(data)
    return results


def load_alignment_lessons(outputs_dir: Path) -> List[Dict[str, Any]]:
    return _load_jsonl(
        outputs_dir / "training_feedback" / "live" / "alignment_lessons_history.jsonl"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Sample builders
# ──────────────────────────────────────────────────────────────────────────────

def _segments_to_text_table(segments: List[Dict[str, Any]]) -> str:
    """Convert segment list to readable text for the user prompt."""
    lines: List[str] = []
    for seg in segments:
        idx = seg.get("segment_index", "?")
        start = seg.get("start_sec", 0)
        end = seg.get("end_sec", 0)
        label = seg.get("label", "?")
        gran = seg.get("granularity", "?")
        lines.append(f"[{idx}] {start:.1f}s-{end:.1f}s | {gran} | {label}")
    return "\n".join(lines)


def _build_sample_from_transition(
    transition: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Build one fine-tuning sample from a T4 dispute transition.

    A transition contains:
      episode_id, original_labels, corrected_labels (or resolved_labels),
      dispute_bucket, resolution_notes, validator_errors, etc.
    """
    eid = str(transition.get("episode_id", "")).strip()
    original = transition.get("original_labels") or transition.get("labels_before") or []
    corrected = transition.get("corrected_labels") or transition.get("labels_after") or transition.get("resolved_labels") or []

    if not original or not corrected:
        return None

    # Build the user prompt: "here are the draft segments, fix them"
    if isinstance(original, list) and isinstance(original[0], dict):
        draft_text = _segments_to_text_table(original)
    else:
        draft_text = "\n".join(str(x) for x in original)

    # Build the ideal response JSON
    if isinstance(corrected, list) and isinstance(corrected[0], dict):
        # Wrap in expected schema shape
        ideal_response = {
            "step_by_step_reasoning": transition.get("resolution_notes") or
                "Reviewed video evidence and corrected labels per Atlas Tier-3 policy.",
            "operations": [],
            "segments": corrected,
        }
    else:
        # Corrected labels are plain strings — build minimal response
        ideal_response = {
            "step_by_step_reasoning": "Applied policy corrections from T4 dispute resolution.",
            "operations": [],
            "segments": [{"label": str(c)} for c in corrected],
        }

    notes = str(transition.get("resolution_notes") or transition.get("dispute_notes") or "")
    errors = transition.get("validator_errors") or []
    errors_text = "\n".join(f"- {e}" for e in errors) if errors else ""

    user_msg_parts = [
        f"Episode: {eid}",
        "",
        "Draft segments (may contain errors):",
        draft_text,
    ]
    if errors_text:
        user_msg_parts += ["", "Validator errors detected:", errors_text]
    if notes:
        user_msg_parts += ["", f"Resolution context: {notes}"]
    user_msg_parts += ["", "Correct the labels and return strict JSON."]

    return {
        "_meta": {
            "source": "t4_dispute",
            "episode_id": eid,
            "dispute_bucket": transition.get("dispute_bucket", ""),
            "ts_utc": transition.get("ts_utc") or transition.get("timestamp", ""),
        },
        "system": FINETUNE_SYSTEM_PROMPT,
        "user": "\n".join(user_msg_parts),
        "assistant": json.dumps(ideal_response, ensure_ascii=False, indent=2),
    }


def _build_sample_from_final(
    annotation: Dict[str, Any],
    min_confidence: float = 0.0,
) -> Optional[Dict[str, Any]]:
    """
    Build a fine-tuning sample from a *validated + submitted* final annotation.
    These are positive examples: input = raw segments, output = corrected annotation.
    """
    segments = annotation.get("segments") or []
    if not segments:
        return None

    # Filter by confidence
    if min_confidence > 0:
        confidences = [float(s.get("confidence", 1.0) or 1.0) for s in segments]
        avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
        if avg_conf < min_confidence:
            return None

    eid = str(annotation.get("episode_id") or annotation.get("_source_file", "")).strip()

    # Simulate "draft" by using employee-style labels with no corrections
    draft_text = _segments_to_text_table(segments)

    ideal_response = {
        "step_by_step_reasoning": annotation.get("step_by_step_reasoning") or
            "Analyzed video chronologically and applied Tier-3 policy.",
        "operations": [],
        "segments": [
            {
                "segment_index": s.get("segment_index", i + 1),
                "start_sec": s.get("start_sec", 0),
                "end_sec": s.get("end_sec", 0),
                "duration_sec": s.get("duration_sec", 0),
                "label": s.get("label", ""),
                "granularity": s.get("granularity", "coarse"),
                "confidence": s.get("confidence", 1.0),
            }
            for i, s in enumerate(segments)
        ],
    }

    user_msg = (
        f"Episode: {eid}\n\n"
        "Review these segments and return corrected annotation as strict JSON:\n"
        + draft_text
    )

    return {
        "_meta": {
            "source": "validated_final",
            "episode_id": eid,
            "segment_count": len(segments),
        },
        "system": FINETUNE_SYSTEM_PROMPT,
        "user": user_msg,
        "assistant": json.dumps(ideal_response, ensure_ascii=False),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Format converters
# ──────────────────────────────────────────────────────────────────────────────

def to_openai_format(sample: Dict[str, Any]) -> Dict[str, Any]:
    """OpenAI chat fine-tuning format."""
    return {
        "messages": [
            {"role": "system", "content": sample["system"]},
            {"role": "user",   "content": sample["user"]},
            {"role": "assistant", "content": sample["assistant"]},
        ]
    }


def to_gemini_format(sample: Dict[str, Any]) -> Dict[str, Any]:
    """Gemini supervised fine-tuning format (text-in / text-out)."""
    return {
        "input_text": sample["system"] + "\n\n" + sample["user"],
        "output_text": sample["assistant"],
    }


# ──────────────────────────────────────────────────────────────────────────────
# Quality filter
# ──────────────────────────────────────────────────────────────────────────────

def _is_quality_sample(sample: Dict[str, Any], min_assistant_len: int = 50) -> bool:
    """Basic quality gate — skip empty/malformed samples."""
    if not sample.get("user") or not sample.get("assistant"):
        return False
    if len(sample["assistant"]) < min_assistant_len:
        return False
    # Try to parse the assistant JSON
    try:
        parsed = json.loads(sample["assistant"])
        if not isinstance(parsed, dict):
            return False
        if "segments" not in parsed:
            return False
        if not parsed["segments"]:
            return False
    except Exception:
        return False
    return True


# ──────────────────────────────────────────────────────────────────────────────
# HTML review page
# ──────────────────────────────────────────────────────────────────────────────

def build_review_html(samples: List[Dict[str, Any]], stats: Dict[str, Any]) -> str:
    sample_cards = ""
    for i, s in enumerate(samples[:50], 1):
        meta = s.get("_meta", {})
        source = meta.get("source", "?")
        eid = meta.get("episode_id", "?")
        bucket = meta.get("dispute_bucket", "")
        user_preview = s["user"][:400].replace("<", "&lt;").replace(">", "&gt;")
        asst_preview = s["assistant"][:600].replace("<", "&lt;").replace(">", "&gt;")
        badge_color = "#ff6b6b" if source == "t4_dispute" else "#00e5c8"
        sample_cards += f"""
        <div class="sample-card">
          <div class="sample-header">
            <span class="sample-num">#{i}</span>
            <span class="badge" style="background:{badge_color}20;color:{badge_color};border-color:{badge_color}40">{source}</span>
            <span class="eid">{eid[:40]}</span>
            {'<span class="bucket">' + bucket + '</span>' if bucket else ''}
          </div>
          <div class="sample-body">
            <div class="col">
              <div class="col-label">USER PROMPT</div>
              <pre class="code">{user_preview}{"..." if len(s["user"]) > 400 else ""}</pre>
            </div>
            <div class="col">
              <div class="col-label">IDEAL RESPONSE</div>
              <pre class="code">{asst_preview}{"..." if len(s["assistant"]) > 600 else ""}</pre>
            </div>
          </div>
        </div>
        """

    return f"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Atlas Fine-tuning Dataset Review</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Cairo:wght@400;600;700&display=swap');
  :root {{
    --bg:#0d0f14;--bg2:#141720;--bg3:#1c2030;--border:#2a2f42;
    --accent:#00e5c8;--accent2:#7c6fff;--accent3:#ff6b6b;
    --text:#e2e8f0;--text2:#8892a4;
    --mono:'IBM Plex Mono',monospace;--sans:'Cairo',sans-serif;
  }}
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:var(--bg);color:var(--text);font-family:var(--sans);padding:0 0 60px}}
  .topbar{{background:var(--bg2);border-bottom:1px solid var(--border);padding:16px 32px;display:flex;align-items:center;justify-content:space-between}}
  .topbar-title{{font-family:var(--mono);font-size:1rem;color:var(--accent);font-weight:700}}
  .topbar-sub{{font-family:var(--mono);font-size:0.72rem;color:var(--text2)}}
  main{{max-width:1400px;margin:0 auto;padding:28px 24px 0}}
  .stats-row{{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:28px}}
  .stat{{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:14px 20px;font-family:var(--mono)}}
  .stat-v{{font-size:1.6rem;color:var(--accent);font-weight:700}}
  .stat-l{{font-size:0.65rem;color:var(--text2);text-transform:uppercase;letter-spacing:.08em;margin-top:4px}}
  .sample-card{{background:var(--bg2);border:1px solid var(--border);border-radius:10px;margin-bottom:16px;overflow:hidden}}
  .sample-header{{padding:12px 18px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px;background:var(--bg3)}}
  .sample-num{{font-family:var(--mono);font-size:0.72rem;color:var(--text2)}}
  .badge{{border:1px solid;border-radius:4px;padding:2px 8px;font-family:var(--mono);font-size:0.68rem}}
  .eid{{font-family:var(--mono);font-size:0.7rem;color:var(--text2)}}
  .bucket{{font-family:var(--mono);font-size:0.68rem;color:#ffd166;background:#ffd16620;border:1px solid #ffd16640;border-radius:4px;padding:2px 8px}}
  .sample-body{{display:grid;grid-template-columns:1fr 1fr;gap:0}}
  .col{{padding:16px 18px;border-left:1px solid var(--border)}}
  .col:first-child{{border-left:none}}
  .col-label{{font-family:var(--mono);font-size:0.62rem;color:var(--text2);text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px}}
  .code{{font-family:var(--mono);font-size:0.69rem;white-space:pre-wrap;word-break:break-all;color:var(--text);line-height:1.5;max-height:220px;overflow:auto}}
  @media(max-width:700px){{.sample-body{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<div class="topbar">
  <div>
    <div class="topbar-title">▸ Fine-tuning Dataset Review</div>
    <div class="topbar-sub">Atlas OCR Annotation Pipeline · Human Review Interface</div>
  </div>
  <div style="font-family:var(--mono);font-size:0.68rem;color:var(--text2)">{datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}</div>
</div>
<main>
  <div class="stats-row">
    <div class="stat"><div class="stat-v">{stats["total_samples"]}</div><div class="stat-l">Total Samples</div></div>
    <div class="stat"><div class="stat-v">{stats["from_disputes"]}</div><div class="stat-l">From Disputes</div></div>
    <div class="stat"><div class="stat-v">{stats["from_validated"]}</div><div class="stat-l">From Validated</div></div>
    <div class="stat"><div class="stat-v">{stats["skipped_quality"]}</div><div class="stat-l">Skipped (Quality)</div></div>
  </div>
  <p style="font-family:var(--mono);font-size:0.72rem;color:var(--text2);margin-bottom:20px">
    Showing first 50 samples of {stats["total_samples"]} total. Review before uploading to fine-tuning API.
  </p>
  {sample_cards}
</main>
</body>
</html>"""


# ──────────────────────────────────────────────────────────────────────────────
# Main export
# ──────────────────────────────────────────────────────────────────────────────

def export(
    outputs_dir: Path,
    fmt: str = "both",
    min_confidence: float = 0.0,
    dry_run: bool = False,
) -> Dict[str, Any]:

    print(f"[finetune] reading from: {outputs_dir}")

    transitions = load_all_transitions(outputs_dir)
    finals = load_final_annotations(outputs_dir)
    lessons = load_alignment_lessons(outputs_dir)

    print(f"[finetune] found: transitions={len(transitions)} finals={len(finals)} lessons={len(lessons)}")

    samples: List[Dict[str, Any]] = []
    skipped_quality = 0

    # ── From T4 disputes (highest learning signal) ──
    for t in transitions:
        s = _build_sample_from_transition(t)
        if s is None:
            skipped_quality += 1
            continue
        if not _is_quality_sample(s):
            skipped_quality += 1
            continue
        samples.append(s)

    from_disputes = len(samples)
    print(f"[finetune] samples from disputes: {from_disputes}")

    # ── From validated final annotations (positive examples) ──
    for ann in finals:
        s = _build_sample_from_final(ann, min_confidence=min_confidence)
        if s is None:
            skipped_quality += 1
            continue
        if not _is_quality_sample(s):
            skipped_quality += 1
            continue
        samples.append(s)

    from_validated = len(samples) - from_disputes
    print(f"[finetune] samples from validated finals: {from_validated}")
    print(f"[finetune] skipped (quality gate): {skipped_quality}")
    print(f"[finetune] total samples: {len(samples)}")

    stats = {
        "total_samples": len(samples),
        "from_disputes": from_disputes,
        "from_validated": from_validated,
        "skipped_quality": skipped_quality,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "outputs_dir": str(outputs_dir),
        "min_confidence_filter": min_confidence,
    }

    if dry_run:
        print("[finetune] dry-run mode — no files written.")
        print(json.dumps(stats, indent=2))
        return stats

    out_dir = outputs_dir / "finetune"
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── OpenAI JSONL ──
    if fmt in ("openai", "both"):
        openai_path = out_dir / "atlas_finetune_openai.jsonl"
        with openai_path.open("w", encoding="utf-8") as f:
            for s in samples:
                f.write(json.dumps(to_openai_format(s), ensure_ascii=False) + "\n")
        print(f"[finetune] ✓ OpenAI JSONL: {openai_path} ({len(samples)} samples)")

    # ── Gemini JSONL ──
    if fmt in ("gemini", "both"):
        gemini_path = out_dir / "atlas_finetune_gemini.jsonl"
        with gemini_path.open("w", encoding="utf-8") as f:
            for s in samples:
                f.write(json.dumps(to_gemini_format(s), ensure_ascii=False) + "\n")
        print(f"[finetune] ✓ Gemini JSONL: {gemini_path} ({len(samples)} samples)")

    # ── Summary JSON ──
    summary_path = out_dir / "atlas_finetune_summary.json"
    summary_path.write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")

    # ── HTML review page ──
    review_html = build_review_html(samples, stats)
    review_path = out_dir / "atlas_finetune_review.html"
    review_path.write_text(review_html, encoding="utf-8")
    print(f"[finetune] ✓ Review HTML: {review_path}")

    print(f"\n[finetune] ── Export complete ──────────────────────────")
    print(f"  Total samples   : {len(samples)}")
    print(f"  From disputes   : {from_disputes}  ← highest learning signal")
    print(f"  From validated  : {from_validated}")
    print(f"  Skipped         : {skipped_quality}")
    print(f"  Output dir      : {out_dir}")
    print(f"\n  Next steps:")
    print(f"  1. Review: {out_dir}/atlas_finetune_review.html")
    print(f"  2. Upload OpenAI: openai api fine_tuning.jobs.create --training-file {out_dir}/atlas_finetune_openai.jsonl --model gpt-4o-mini")
    print(f"  3. Upload Gemini: Use Vertex AI > Supervised fine-tuning > Upload {out_dir}/atlas_finetune_gemini.jsonl")

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Atlas fine-tuning dataset from disputes and validated outputs")
    parser.add_argument("--outputs-dir", default="outputs", help="Path to outputs directory")
    parser.add_argument("--format", choices=["openai", "gemini", "both"], default="both")
    parser.add_argument("--min-quality", type=float, default=0.0,
                        help="Minimum avg segment confidence to include validated finals (0.0–1.0)")
    parser.add_argument("--dry-run", action="store_true", help="Print stats only, don't write files")
    args = parser.parse_args()
    export(
        outputs_dir=Path(args.outputs_dir),
        fmt=args.format,
        min_confidence=args.min_quality,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
