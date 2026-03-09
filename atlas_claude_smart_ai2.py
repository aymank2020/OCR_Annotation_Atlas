"""
Atlas Capture - Policy-Aware AI Annotator (Claude Vision)
==========================================================

Production-oriented annotator that applies the latest Atlas policy constraints:
- Treat "gripper" as an extension of ego hand
- "No Action" for any no-contact period (including reset posture)
- Segment boundaries by hand/gripper engagement-disengagement
- Hard caps: segment <= 60 seconds, label <= 20 words

The pipeline:
1) Open local video OR download from Atlas URL
2) Extract motion-aware + uniform frames
3) Three Claude passes:
   - Pass 0: object map
   - Pass 1: hand/gripper timeline
   - Pass 2: final annotations JSON
4) Strict post-processing + validation
5) Save JSON + CSV
"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import re
import tempfile
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


MAX_SEGMENT_SECONDS = 60.0
MAX_LABEL_WORDS = 20
MIN_SEGMENT_SECONDS = 0.1

FORBIDDEN_VERBS = ("inspect", "check", "reach", "examine", "continue")
DISALLOWED_TOOL_TERMS = (
    "mechanical arm",
    "robotic arm",
    "robot arm",
    "manipulator",
    "robot gripper",
    "claw arm",
)

PLACE_PREPOSITIONS = ("on", "in", "into", "onto", "at", "to", "inside", "under", "over")

DEFAULT_HEADERS = {
    "accept": "*/*",
    "range": "bytes=0-",
    "referer": "https://audit.atlascapture.io/",
    "sec-fetch-dest": "video",
    "sec-fetch-mode": "no-cors",
    "sec-fetch-site": "same-site",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}

DEFAULT_MODEL = "claude-3-5-sonnet-20241022"


SYSTEM_PROMPT = f"""You are an expert Atlas Capture egocentric annotation specialist.

Follow these non-negotiable rules exactly:

1) GRIPPER MENTAL MODEL
- Treat the gripper as a direct extension of ego hand.
- Usually do NOT mention the tool in labels.
- If unavoidable, use only the term "gripper".
- Never use terms like "mechanical arm" or "robotic arm".

2) OBJECT ACCURACY
- Prioritize accuracy over specificity.
- If uncertain after careful inspection, use a safe general noun: tool, container, item.
- Never guess object identity.

3) WHAT TO LABEL
- Label only hand/gripper-object interactions.
- No interaction -> label "No Action".
- Reset posture with no object interaction is also "No Action".

4) SEGMENT BOUNDARIES
- Start: when hands begin moving toward or touching the primary object.
- End: exact moment hands release/disengage.
- Split at disengagement or clear goal change.

5) GRANULARITY
- For many rapid micro-interactions, prefer coarse labels to preserve accuracy.
- Dense labels are allowed only when clearly verifiable from evidence.

6) LABEL FORMAT
- Imperative voice only (e.g., "pick up block", "place cup on table").
- Forbidden verbs: inspect, check, reach, examine, continue.
- No numerals.
- "place" must include location.
- "No Action" must be standalone.

7) HARD LIMITS
- Each segment duration must be <= {MAX_SEGMENT_SECONDS:.0f} seconds.
- Each label must be <= {MAX_LABEL_WORDS} words.

Return ONLY valid JSON:
{{
  "episode_description": "task summary from hand-action perspective",
  "segments": [
    {{
      "start": 0.0,
      "end": 3.2,
      "hand_contact": "both hands touching box",
      "label": "pick up box",
      "type": "coarse",
      "confidence": "high"
    }}
  ]
}}
"""


@dataclass
class LabelIssue:
    severity: str  # "error" | "warning"
    message: str


def _require_module(module_name: str, pip_name: Optional[str] = None):
    try:
        return __import__(module_name)
    except ImportError as exc:
        package = pip_name or module_name
        raise RuntimeError(
            f"Missing dependency '{module_name}'. Install it with: pip install {package}"
        ) from exc


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _fmt_time(seconds: float) -> str:
    minutes = int(seconds // 60)
    rem = seconds % 60
    return f"{minutes}:{rem:04.1f}"


def load_headers(headers_json_path: Optional[str]) -> Dict[str, str]:
    headers = dict(DEFAULT_HEADERS)
    if not headers_json_path:
        return headers

    with open(headers_json_path, "r", encoding="utf-8") as f:
        loaded = json.load(f)

    if not isinstance(loaded, dict):
        raise ValueError("headers JSON must be an object/dict")

    for key, value in loaded.items():
        headers[str(key).strip().lower()] = str(value)
    return headers


def download_video(video_url: str, headers: Dict[str, str], timeout_sec: int = 180) -> str:
    requests = _require_module("requests")

    print("\n[1/5] Downloading video...")
    response = requests.get(video_url, headers=headers, stream=True, timeout=timeout_sec)
    response.raise_for_status()

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    total = int(response.headers.get("Content-Length", 0))
    downloaded = 0

    for chunk in response.iter_content(chunk_size=512 * 1024):
        if not chunk:
            continue
        tmp.write(chunk)
        downloaded += len(chunk)
        if total:
            pct = 100.0 * downloaded / total
            print(
                f"\r   {pct:5.1f}%  {downloaded/1048576:.1f}MB / {total/1048576:.1f}MB",
                end="",
                flush=True,
            )

    tmp.close()
    print(f"\n   Downloaded {downloaded/1048576:.1f}MB -> {tmp.name}")
    return tmp.name


def _sample_frame_changes(video_path: str, duration: float, max_samples: int = 120) -> List[Tuple[float, float]]:
    cv2 = _require_module("cv2", "opencv-python")
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return []

    step = max(0.4, duration / max(15, max_samples))
    times = []
    t = min(0.5, max(0.0, duration / 2.0))
    while t < duration:
        times.append(t)
        t += step

    changes: List[Tuple[float, float]] = []
    prev_gray = None

    for ts in times:
        cap.set(cv2.CAP_PROP_POS_MSEC, ts * 1000.0)
        ok, frame = cap.read()
        if not ok:
            continue
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        if prev_gray is not None:
            diff = float(cv2.absdiff(gray, prev_gray).mean())
            changes.append((diff, ts))
        prev_gray = gray

    cap.release()
    return changes


def extract_frames(video_path: str, max_frames: int = 45) -> Tuple[List[Dict[str, Any]], float]:
    cv2 = _require_module("cv2", "opencv-python")

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video: {video_path}")

    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    duration = frame_count / fps if frame_count > 0 else 0.0
    cap.release()

    if duration <= 0:
        raise RuntimeError("Could not determine video duration")

    max_frames = max(8, min(90, int(max_frames)))
    print(f"\n[2/5] Extracting frames from {duration:.1f}s video (target={max_frames})...")

    uniform_count = max(4, int(max_frames * 0.65))
    change_count = max_frames - uniform_count

    uniform_times = []
    for i in range(uniform_count):
        t = (duration * (i + 1)) / (uniform_count + 1)
        uniform_times.append(round(t, 2))

    changes = _sample_frame_changes(video_path, duration, max_samples=max_frames * 3)
    changes_sorted = sorted(changes, key=lambda x: x[0], reverse=True)
    change_times = [round(ts, 2) for _, ts in changes_sorted[: change_count * 3]]

    all_times = sorted(set(uniform_times + change_times))
    if not all_times:
        all_times = uniform_times

    if len(all_times) > max_frames:
        idxs = {
            int(round(i * (len(all_times) - 1) / (max_frames - 1)))
            for i in range(max_frames)
        }
        selected_times = [all_times[i] for i in sorted(idxs)]
    else:
        selected_times = all_times

    cap = cv2.VideoCapture(video_path)
    frames: List[Dict[str, Any]] = []
    for i, ts in enumerate(selected_times, start=1):
        cap.set(cv2.CAP_PROP_POS_MSEC, ts * 1000.0)
        ok, frame = cap.read()
        if not ok:
            continue
        h, w = frame.shape[:2]
        if w > 768:
            new_w = 768
            new_h = int(h * new_w / w)
            frame = cv2.resize(frame, (new_w, new_h))

        ok2, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        if not ok2:
            continue

        frames.append({"time": float(ts), "b64": base64.b64encode(buffer).decode("ascii")})
        print(f"\r   extracted {len(frames)}/{len(selected_times)}", end="", flush=True)

    cap.release()
    print(f"\n   Ready: {len(frames)} frames")
    return frames, duration


def _anthropic_client(api_key: str):
    anthropic = _require_module("anthropic")
    return anthropic.Anthropic(api_key=api_key)


def _call_claude(
    client: Any,
    model: str,
    user_content: Sequence[Dict[str, Any]],
    system_prompt: Optional[str] = None,
    max_tokens: int = 2000,
) -> Tuple[str, Dict[str, int]]:
    kwargs: Dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": list(user_content)}],
    }
    if system_prompt:
        kwargs["system"] = system_prompt

    response = client.messages.create(**kwargs)
    text_parts = []
    for block in getattr(response, "content", []):
        if getattr(block, "type", "") == "text":
            text_parts.append(getattr(block, "text", ""))
    text = "\n".join(text_parts).strip()

    usage = getattr(response, "usage", None)
    usage_dict = {
        "input_tokens": int(getattr(usage, "input_tokens", 0) or 0),
        "output_tokens": int(getattr(usage, "output_tokens", 0) or 0),
    }
    return text, usage_dict


def _content_with_frames(frames: Sequence[Dict[str, Any]], note_every: int = 4) -> List[Dict[str, Any]]:
    content: List[Dict[str, Any]] = []
    for i, frame in enumerate(frames):
        content.append(
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/jpeg",
                    "data": frame["b64"],
                },
            }
        )
        if i % max(1, note_every) == 0:
            content.append({"type": "text", "text": f"t={frame['time']:.1f}s"})
    return content


def build_object_map(client: Any, model: str, frames: Sequence[Dict[str, Any]]) -> str:
    sample = []
    if frames:
        n = len(frames)
        idxs = list(range(min(6, n)))
        idxs += list(range(max(0, n // 2 - 2), min(n, n // 2 + 2)))
        idxs += list(range(max(0, n - 6), n))
        seen = sorted(set(idxs))
        sample = [frames[i] for i in seen]

    content: List[Dict[str, Any]] = [
        {
            "type": "text",
            "text": (
                "Build an OBJECT MAP for this egocentric task. "
                "List each likely object identity and how it appears across orientations.\n"
                "Important: if an object flips/rotates, keep it as the same object unless "
                "evidence proves otherwise.\n"
                "If uncertain, use a safe general noun (tool/container/item).\n\n"
                "Output plain text in this structure:\n"
                "OBJECT MAP:\n"
                "[1] NAME: ...\n"
                "    FRONT/PRIMARY VIEW: ...\n"
                "    BACK/ALT VIEW: ...\n"
                "    NOTES: ...\n"
            ),
        }
    ]
    content.extend(_content_with_frames(sample, note_every=2))
    content.append({"type": "text", "text": "Now produce the complete object map."})

    text, _ = _call_claude(client, model, content, max_tokens=1200)
    return text.strip()


def build_hand_timeline(
    client: Any,
    model: str,
    frames: Sequence[Dict[str, Any]],
    duration: float,
    object_map: str,
) -> str:
    content: List[Dict[str, Any]] = [
        {
            "type": "text",
            "text": (
                f"Video duration: {duration:.1f}s\n\n"
                "Use this OBJECT MAP while tracking hand/gripper interactions:\n"
                f"{object_map}\n\n"
                "Build a precise interaction timeline:\n"
                "- identify active hand/gripper\n"
                "- identify touched object\n"
                "- describe interaction intent (action)\n"
                "- include approximate time windows\n"
                "No object contact periods should be explicitly marked as No Action windows.\n"
            ),
        }
    ]
    content.extend(_content_with_frames(frames, note_every=4))
    content.append(
        {
            "type": "text",
            "text": "Return a chronological plain-text timeline with clear time ranges.",
        }
    )

    text, _ = _call_claude(client, model, content, max_tokens=1800)
    return text.strip()


def generate_raw_annotations(
    client: Any,
    model: str,
    frames: Sequence[Dict[str, Any]],
    duration: float,
    object_map: str,
    timeline: str,
) -> Dict[str, Any]:
    content: List[Dict[str, Any]] = [
        {
            "type": "text",
            "text": (
                f"Video duration: {duration:.1f}s.\n\n"
                "You already have:\n"
                f"OBJECT MAP:\n{object_map}\n\n"
                f"HAND/GRIPPER TIMELINE:\n{timeline}\n\n"
                "Generate final segment annotations now. "
                "Must obey segment and label limits. "
                "Return ONLY valid JSON."
            ),
        }
    ]
    content.extend(_content_with_frames(frames[::2] if len(frames) > 24 else frames, note_every=4))

    raw_text, usage = _call_claude(
        client=client,
        model=model,
        user_content=content,
        system_prompt=SYSTEM_PROMPT,
        max_tokens=3500,
    )
    result = parse_json_object(raw_text)
    result["_usage"] = usage
    return result


def parse_json_object(raw_text: str) -> Dict[str, Any]:
    clean = re.sub(r"```json|```", "", raw_text or "").strip()
    start = clean.find("{")
    end = clean.rfind("}")
    if start < 0 or end < 0 or end <= start:
        raise ValueError(f"Could not find JSON object in model output:\n{raw_text[:800]}")

    candidate = clean[start : end + 1]
    try:
        return json.loads(candidate)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Could not parse JSON from model output:\n{candidate[:1200]}") from exc


def normalize_label(raw_label: Any) -> str:
    label = str(raw_label or "").strip()
    label = re.sub(r"\s+", " ", label)
    label = label.strip(" ,.;")

    for term in DISALLOWED_TOOL_TERMS:
        label = re.sub(rf"\b{re.escape(term)}\b", "gripper", label, flags=re.IGNORECASE)

    label = re.sub(r"\s*,\s*", ", ", label)
    label = re.sub(r"\s+and\s+", " and ", label, flags=re.IGNORECASE)
    label = re.sub(r"\s+", " ", label).strip(" ,.;")

    if label.lower() == "no action":
        return "No Action"
    return label.lower()


def _label_word_count(label: str) -> int:
    return len([w for w in label.split() if w.strip()])


def autofix_label(label: str, preserve_reach: bool = False) -> Tuple[str, List[LabelIssue]]:
    issues: List[LabelIssue] = []
    fixed = normalize_label(label)

    if not fixed:
        return "No Action", [LabelIssue("warning", "Empty label replaced with No Action")]

    if fixed != "No Action":
        if not preserve_reach:
            fixed = re.sub(r"\breach for\b", "pick up", fixed)
            fixed = re.sub(r"\breach\b", "pick up", fixed)
        fixed = re.sub(r"\binspect\b", "adjust", fixed)
        fixed = re.sub(r"\bcheck\b", "adjust", fixed)
        fixed = re.sub(r"\bexamine\b", "adjust", fixed)
        fixed = re.sub(r"\bcontinue\b", "", fixed)
        fixed = re.sub(r"\s+", " ", fixed).strip(" ,.;")

        if re.search(r"\bno action\b", fixed, flags=re.IGNORECASE):
            fixed = "No Action"
            issues.append(LabelIssue("warning", 'Mixed "No Action" converted to standalone No Action'))

        fixed = re.sub(r"\brobot(ic)?\s+arm\b", "gripper", fixed)
        fixed = re.sub(r"\bmechanical\s+arm\b", "gripper", fixed)

        if re.search(r"\bplace\b", fixed):
            place_ok = any(re.search(rf"\b{prep}\b", fixed) for prep in PLACE_PREPOSITIONS)
            if not place_ok:
                fixed = f"{fixed} on surface"
                issues.append(LabelIssue("warning", 'Added generic location for "place"'))

        if re.search(r"\b\d+\b", fixed):
            fixed = re.sub(r"\b\d+\b", "", fixed)
            fixed = re.sub(r"\s+", " ", fixed).strip(" ,.;")
            issues.append(LabelIssue("warning", "Removed numeral tokens"))

    if fixed != "No Action":
        words = fixed.split()
        if len(words) > MAX_LABEL_WORDS:
            # Prefer clause-safe trimming before hard word truncation.
            if "," in fixed:
                fixed = fixed.split(",", 1)[0].strip()
                issues.append(
                    LabelIssue(
                        "warning",
                        f"Label exceeded {MAX_LABEL_WORDS} words. Kept only the first clause to maintain safety.",
                    )
                )
            else:
                fixed = " ".join(words[:MAX_LABEL_WORDS]).strip(" ,.;")
                issues.append(
                    LabelIssue(
                        "warning",
                        f"Trimmed label forcefully to {MAX_LABEL_WORDS} words (policy cap).",
                    )
                )

    if fixed != "No Action" and _label_word_count(fixed) < 2:
        fixed = "handle item"
        issues.append(LabelIssue("warning", "Too-short label replaced with generic safe label"))

    if not fixed:
        fixed = "No Action"
        issues.append(LabelIssue("warning", "Unusable label replaced with No Action"))

    return fixed, issues


def validate_label(label: str) -> List[LabelIssue]:
    issues: List[LabelIssue] = []
    l = normalize_label(label)

    if not l:
        return [LabelIssue("error", "Empty label")]

    if l == "No Action":
        return []

    word_count = _label_word_count(l)
    if word_count < 2:
        issues.append(LabelIssue("error", "Too short: label needs at least two words"))
    if word_count > MAX_LABEL_WORDS:
        issues.append(LabelIssue("error", f"Too long: label exceeds {MAX_LABEL_WORDS} words"))

    for v in FORBIDDEN_VERBS:
        if re.search(rf"\b{v}\b", l):
            issues.append(LabelIssue("error", f'Forbidden verb "{v}"'))

    if re.search(r"\b\d+\b", l):
        issues.append(LabelIssue("error", "Numerals are not allowed"))

    if re.search(r"\bno action\b", l, flags=re.IGNORECASE) and l != "no action":
        issues.append(LabelIssue("error", '"No Action" must be standalone'))

    for term in DISALLOWED_TOOL_TERMS:
        if re.search(rf"\b{re.escape(term)}\b", l):
            issues.append(LabelIssue("error", f'Disallowed tool term "{term}" (use "gripper" only if unavoidable)'))

    if re.search(r"\bplace\b", l):
        place_ok = any(re.search(rf"\b{prep}\b", l) for prep in PLACE_PREPOSITIONS)
        if not place_ok:
            issues.append(LabelIssue("warning", '"place" should include location'))

    if re.search(r"\b\w+ing\b", l):
        issues.append(LabelIssue("warning", "Contains -ing form; imperative is preferred"))

    return issues


def _clean_segment_type(raw_type: Any) -> str:
    t = str(raw_type or "").strip().lower()
    return t if t in {"coarse", "dense"} else "coarse"


def _clean_confidence(raw_conf: Any) -> str:
    c = str(raw_conf or "").strip().lower()
    return c if c in {"low", "medium", "high"} else "medium"


def _make_no_action_segment(start: float, end: float) -> Dict[str, Any]:
    return {
        "start": round(start, 1),
        "end": round(end, 1),
        "hand_contact": "no hand-object contact",
        "label": "No Action",
        "type": "coarse",
        "confidence": "medium",
        "issues": [],
    }


def normalize_segments(raw_segments: Any, duration: float) -> List[Dict[str, Any]]:
    candidates = []
    if isinstance(raw_segments, list):
        for i, item in enumerate(raw_segments):
            if not isinstance(item, dict):
                continue
            start = _to_float(item.get("start"), 0.0)
            end = _to_float(item.get("end"), start + MIN_SEGMENT_SECONDS)
            hand_contact = str(item.get("hand_contact", "")).strip()
            label = str(item.get("label", "")).strip()

            candidates.append(
                {
                    "source_index": i,
                    "start": start,
                    "end": end,
                    "hand_contact": hand_contact,
                    "label": label,
                    "type": _clean_segment_type(item.get("type")),
                    "confidence": _clean_confidence(item.get("confidence")),
                }
            )

    if not candidates:
        return [_make_no_action_segment(0.0, max(MIN_SEGMENT_SECONDS, duration))]

    clamped = []
    for seg in candidates:
        s = max(0.0, min(duration, seg["start"]))
        e = max(0.0, min(duration, seg["end"]))
        if e <= s:
            e = min(duration, s + MIN_SEGMENT_SECONDS)
        if e <= s:
            continue
        seg2 = dict(seg)
        seg2["start"] = s
        seg2["end"] = e
        clamped.append(seg2)

    clamped.sort(key=lambda x: (x["start"], x["end"]))
    if not clamped:
        return [_make_no_action_segment(0.0, max(MIN_SEGMENT_SECONDS, duration))]

    stitched: List[Dict[str, Any]] = []
    cursor = 0.0
    for seg in clamped:
        s = max(cursor, seg["start"])
        e = max(s + MIN_SEGMENT_SECONDS, seg["end"])
        e = min(e, duration)
        if s - cursor > 0.25:
            stitched.append(_make_no_action_segment(cursor, s))
        if e - s >= MIN_SEGMENT_SECONDS:
            seg2 = dict(seg)
            seg2["start"] = s
            seg2["end"] = e
            stitched.append(seg2)
            cursor = e

    if duration - cursor > 0.25:
        stitched.append(_make_no_action_segment(cursor, duration))

    final_segments: List[Dict[str, Any]] = []
    for seg_idx, seg in enumerate(stitched):
        start = seg["start"]
        end = seg["end"]
        chunks = []
        while end - start > MAX_SEGMENT_SECONDS + 1e-6:
            split_end = min(end, start + MAX_SEGMENT_SECONDS)
            chunks.append((start, split_end))
            start = split_end
        if end - start >= MIN_SEGMENT_SECONDS:
            chunks.append((start, end))

        is_last_segment = seg_idx == len(stitched) - 1
        for chunk_idx, (s, e) in enumerate(chunks):
            is_last_chunk = chunk_idx == len(chunks) - 1
            raw_seg_label = str(seg.get("label", ""))
            tail_sec = max(0.0, duration - e)
            preserve_reach = bool(
                is_last_segment
                and is_last_chunk
                and tail_sec <= 0.35
                and re.search(r"\breach\b", raw_seg_label, re.IGNORECASE)
            )
            fixed_label, autofix_issues = autofix_label(raw_seg_label, preserve_reach=preserve_reach)
            issues = autofix_issues + validate_label(fixed_label)
            final_segments.append(
                {
                    "start": round(s, 1),
                    "end": round(e, 1),
                    "hand_contact": seg.get("hand_contact", ""),
                    "label": fixed_label,
                    "type": _clean_segment_type(seg.get("type")),
                    "confidence": _clean_confidence(seg.get("confidence")),
                    "issues": [f"{it.severity}: {it.message}" for it in issues],
                }
            )

    sanitized: List[Dict[str, Any]] = []
    cursor = 0.0
    for seg in sorted(final_segments, key=lambda x: (x["start"], x["end"])):
        s = max(cursor, _to_float(seg.get("start"), cursor))
        e = min(duration, _to_float(seg.get("end"), s + MIN_SEGMENT_SECONDS))
        if e - s < MIN_SEGMENT_SECONDS:
            continue
        seg2 = dict(seg)
        seg2["start"] = round(s, 1)
        seg2["end"] = round(e, 1)
        sanitized.append(seg2)
        cursor = e

    if not sanitized:
        sanitized = [_make_no_action_segment(0.0, max(MIN_SEGMENT_SECONDS, duration))]

    if duration - sanitized[-1]["end"] > 0.25:
        sanitized.append(_make_no_action_segment(sanitized[-1]["end"], duration))

    for seg in sanitized:
        if normalize_label(seg["label"]) == "No Action":
            seg["label"] = "No Action"

    return sanitized


def build_quality_report(segments: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(segments)
    no_action_count = sum(1 for s in segments if s.get("label") == "No Action")
    max_dur = max((s["end"] - s["start"]) for s in segments) if segments else 0.0
    max_words = max((_label_word_count(str(s.get("label", ""))) for s in segments), default=0)
    warnings = sum(
        1
        for s in segments
        for issue in s.get("issues", [])
        if isinstance(issue, str) and issue.startswith("warning:")
    )
    errors = sum(
        1
        for s in segments
        for issue in s.get("issues", [])
        if isinstance(issue, str) and issue.startswith("error:")
    )
    return {
        "segments_total": total,
        "segments_no_action": no_action_count,
        "max_segment_duration_sec": round(max_dur, 2),
        "max_label_words": max_words,
        "warnings": warnings,
        "errors": errors,
    }


def postprocess_result(raw_result: Dict[str, Any], duration: float) -> Dict[str, Any]:
    episode_description = str(raw_result.get("episode_description", "")).strip()
    segments = normalize_segments(raw_result.get("segments"), duration)
    quality = build_quality_report(segments)
    return {
        "episode_description": episode_description or "egocentric hand-object interaction task",
        "policy_version": "atlas-gripper-2026-02",
        "segments": segments,
        "quality_report": quality,
    }


def save_outputs(result: Dict[str, Any], output_prefix: str) -> Tuple[str, str]:
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    json_path = f"{output_prefix}_{timestamp}.json"
    csv_path = f"{output_prefix}_{timestamp}.csv"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "seg_num",
                "start",
                "end",
                "duration",
                "hand_contact",
                "label",
                "type",
                "confidence",
                "issues",
            ]
        )
        for i, seg in enumerate(result.get("segments", []), start=1):
            writer.writerow(
                [
                    i,
                    seg.get("start", 0.0),
                    seg.get("end", 0.0),
                    round(_to_float(seg.get("end"), 0.0) - _to_float(seg.get("start"), 0.0), 1),
                    seg.get("hand_contact", ""),
                    seg.get("label", ""),
                    seg.get("type", ""),
                    seg.get("confidence", ""),
                    "; ".join(seg.get("issues", [])),
                ]
            )

    return json_path, csv_path


def print_summary(result: Dict[str, Any], duration: float, usage: Optional[Dict[str, int]]) -> None:
    segments = result.get("segments", [])
    quality = result.get("quality_report", {})

    print("\n" + "=" * 72)
    print(f"Task: {result.get('episode_description', '')}")
    print(f"Duration: {duration:.1f}s | Segments: {len(segments)}")
    print("=" * 72)

    for i, seg in enumerate(segments, start=1):
        start = _to_float(seg.get("start"), 0.0)
        end = _to_float(seg.get("end"), 0.0)
        dur = end - start
        label = str(seg.get("label", ""))
        seg_type = seg.get("type", "")
        conf = seg.get("confidence", "")
        print(f"[{i:02d}] {_fmt_time(start)} -> {_fmt_time(end)} ({dur:.1f}s) [{seg_type}/{conf}]")
        print(f"     {label}")
        if seg.get("issues"):
            print(f"     issues: {' | '.join(seg['issues'])}")

    print("\nQuality report:")
    print(json.dumps(quality, indent=2))

    if usage:
        in_tok = int(usage.get("input_tokens", 0))
        out_tok = int(usage.get("output_tokens", 0))
        print(f"\nClaude usage: in={in_tok:,} out={out_tok:,}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Atlas policy-aware AI annotator (Claude)")
    parser.add_argument("--video-url", default="", help="Atlas video URL with token")
    parser.add_argument("--video-file", default="", help="Local video path")
    parser.add_argument("--api-key", default="", help="Anthropic API key")
    parser.add_argument("--headers-json", default="", help="Optional JSON file containing request headers")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"Claude model (default: {DEFAULT_MODEL})")
    parser.add_argument("--max-frames", type=int, default=45, help="Target number of extracted frames")
    parser.add_argument("--output-prefix", default="annotations", help="Output file prefix")
    parser.add_argument(
        "--skip-object-map",
        action="store_true",
        help="Skip pass0 object map and use a generic fallback map",
    )
    return parser.parse_args()


def run_pipeline(args: argparse.Namespace) -> Tuple[Dict[str, Any], float]:
    api_key = args.api_key.strip() or os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        api_key = input("Anthropic API key: ").strip()
    if not api_key:
        raise RuntimeError("Anthropic API key is required")

    video_file = args.video_file.strip()
    temp_download_path = None

    if video_file:
        if not os.path.exists(video_file):
            raise FileNotFoundError(f"Video file not found: {video_file}")
    else:
        video_url = args.video_url.strip()
        if not video_url:
            video_url = input("Atlas video URL: ").strip()
        if not video_url:
            raise RuntimeError("Either --video-file or --video-url is required")
        headers = load_headers(args.headers_json.strip() or None)
        temp_download_path = download_video(video_url, headers)
        video_file = temp_download_path

    try:
        frames, duration = extract_frames(video_file, max_frames=args.max_frames)
        if not frames:
            raise RuntimeError("Frame extraction returned zero frames")

        client = _anthropic_client(api_key)

        print("\n[3/5] Pass 0: object map")
        if args.skip_object_map:
            object_map = "OBJECT MAP:\n[1] NAME: primary object\n[2] NAME: tool\n[3] NAME: container"
        else:
            object_map = build_object_map(client, args.model, frames)

        print("\n[4/5] Pass 1: hand/gripper timeline")
        timeline = build_hand_timeline(client, args.model, frames, duration, object_map)

        print("\n[5/5] Pass 2: final annotations")
        raw_result = generate_raw_annotations(client, args.model, frames, duration, object_map, timeline)

        usage = raw_result.pop("_usage", {})
        result = postprocess_result(raw_result, duration)
        result["model"] = args.model
        result["frame_count"] = len(frames)
        result["generated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        result["usage"] = usage

        print_summary(result, duration, usage)
        return result, duration
    finally:
        if temp_download_path and os.path.exists(temp_download_path):
            os.remove(temp_download_path)
            print(f"\nTemp file removed: {temp_download_path}")


def main() -> None:
    args = parse_args()
    result, _ = run_pipeline(args)
    json_path, csv_path = save_outputs(result, args.output_prefix)
    print(f"\nSaved JSON: {json_path}")
    print(f"Saved CSV:  {csv_path}")


if __name__ == "__main__":
    main()
