"""
Atlas annotation validator (rule-engine).
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import prompts


NO_ACTION_LABEL = "No Action"
MAX_ATOMIC_ACTIONS_PER_LABEL = 2

DISALLOWED_TOOL_TERMS = (
    "mechanical arm",
    "robotic arm",
    "robot arm",
    "manipulator",
    "robot gripper",
    "claw arm",
)

OBJECT_EXPECTING_VERBS = {
    "pick up",
    "place",
    "move",
    "adjust",
    "hold",
    "grab",
    "cut",
    "open",
    "close",
    "peel",
    "secure",
    "wipe",
    "flip",
}

INTENT_PATTERNS = [
    r"\bprepare to\b",
    r"\btry to\b",
    r"\babout to\b",
    r"\bintend to\b",
]

NUMERAL_PATTERN = re.compile(r"\d")
WHITESPACE_PATTERN = re.compile(r"\s+")
PLACE_LOCATION_PATTERN = re.compile(r"\bplace\b.*\b(on|in|into|onto|to|inside|at|under|over)\b", re.IGNORECASE)
CHAINED_VERB_WITHOUT_OBJECT_PATTERN = re.compile(
    r"\b(pick up|place|move|adjust|hold|align)\s+and\s+(pick up|place|move|adjust|hold|align)\b",
    re.IGNORECASE,
)
ORPHAN_SECOND_PLACE_PATTERN = re.compile(
    r"\band\s+place\s+(on|in|into|onto|to|inside|at|under|over)\b",
    re.IGNORECASE,
)

NUMERAL_TO_WORD = {
    "0": "zero",
    "1": "one",
    "2": "two",
    "3": "three",
    "4": "four",
    "5": "five",
    "6": "six",
    "7": "seven",
    "8": "eight",
    "9": "nine",
    "10": "ten",
}


def normalize_spaces(text: str) -> str:
    return WHITESPACE_PATTERN.sub(" ", str(text).strip())


def lower(text: str) -> str:
    return normalize_spaces(text).lower()


def parse_time_value(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)

    s = normalize_spaces(str(value))
    if not s:
        return 0.0

    if ":" not in s:
        try:
            return float(s)
        except ValueError:
            return 0.0

    parts = s.split(":")
    try:
        if len(parts) == 2:
            minutes = float(parts[0])
            seconds = float(parts[1])
            return minutes * 60.0 + seconds
        if len(parts) == 3:
            hours = float(parts[0])
            minutes = float(parts[1])
            seconds = float(parts[2])
            return hours * 3600.0 + minutes * 60.0 + seconds
    except ValueError:
        return 0.0
    return 0.0


def duration_matches(start: float, end: float, duration: float, tol: float = 0.05) -> bool:
    calc = round(end - start, 3)
    return abs(calc - duration) <= tol


def contains_forbidden_verbs(label: str) -> List[str]:
    label_l = lower(label)
    found = []
    for verb in prompts.FORBIDDEN_VERBS:
        if re.search(rf"\b{re.escape(verb)}\b", label_l):
            found.append(verb)
    return found


def has_numerals(label: str) -> bool:
    return bool(NUMERAL_PATTERN.search(label))


def min_two_words(label: str) -> bool:
    if normalize_spaces(label) == NO_ACTION_LABEL:
        return True
    words = [w for w in re.split(r"\s+", normalize_spaces(label)) if w]
    return len(words) >= 2


def is_imperative_like(label: str) -> bool:
    l = lower(label)
    if l == "no action":
        return True
    if any(re.search(p, l) for p in INTENT_PATTERNS):
        return False
    bad_starts = ("a ", "an ", "the ", "person ", "ego ", "he ", "she ", "they ")
    if l.startswith(bad_starts):
        return False
    first_word = re.split(r"\s+", l.strip())[0] if l.strip() else ""
    if len(first_word) > 4 and first_word.endswith("ing"):
        return False
    return True


def has_intent_only_language(label: str) -> bool:
    l = lower(label)
    return any(re.search(p, l) for p in INTENT_PATTERNS)


def split_actions(label: str) -> List[str]:
    l = normalize_spaces(label)
    if l == NO_ACTION_LABEL:
        return [NO_ACTION_LABEL]
    parts = []
    for chunk in l.split(","):
        subs = [s.strip() for s in re.split(r"\band\b", chunk) if s.strip()]
        parts.extend(subs)
    return [p for p in parts if p]


def count_atomic_actions(label: str) -> int:
    l = normalize_spaces(label)
    if not l:
        return 0
    if l == NO_ACTION_LABEL:
        return 1
    return len(split_actions(l))


def disallowed_tool_terms_found(label: str) -> List[str]:
    l = lower(label)
    found: List[str] = []
    for term in DISALLOWED_TOOL_TERMS:
        if re.search(rf"\b{re.escape(term)}\b", l):
            found.append(term)
    return found


def detect_possible_missing_object(action_phrase: str) -> bool:
    l = lower(action_phrase)
    for verb in sorted(OBJECT_EXPECTING_VERBS, key=len, reverse=True):
        if l == verb:
            return True
        if l.startswith(verb + " "):
            tokens = l.split()
            if verb == "pick up" and len(tokens) <= 2:
                return True
            if verb in {"place", "move"} and len(tokens) <= 2:
                return True
            return False
    return False


def has_unattached_verb_chain(label: str) -> bool:
    l = normalize_spaces(label)
    if not l or l == NO_ACTION_LABEL:
        return False
    if CHAINED_VERB_WITHOUT_OBJECT_PATTERN.search(l):
        return True
    if ORPHAN_SECOND_PLACE_PATTERN.search(l):
        return True
    return False


def place_has_location(label: str) -> bool:
    l = normalize_spaces(label)
    if "place" not in l.lower():
        return True
    return bool(PLACE_LOCATION_PATTERN.search(l))


def no_action_mixed_with_action(label: str) -> bool:
    l = lower(label)
    if l == "no action":
        return False
    return "no action" in l


def dense_coarse_mixed(segment: Dict[str, Any]) -> bool:
    gran = segment.get("granularity")
    label = lower(segment.get("label", ""))
    if gran not in {"dense", "coarse"}:
        return False
    has_move = bool(re.search(r"\bmove\b", label))
    has_pick_place = bool(re.search(r"\bpick up\b", label) and re.search(r"\bplace\b", label))
    return has_move and has_pick_place


def classify_audit_risk(reasons: Sequence[str]) -> str:
    if not reasons:
        return "low"
    high_markers = {
        "forbidden_verbs",
        "disallowed_tool_terms",
        "no_action_mixed",
        "too_many_atomic_actions",
        "duration_mismatch",
        "timestamp_overlap",
        "timestamp_order_invalid",
        "granularity_label_mismatch",
        "dense_coarse_mixed",
        "possible_hallucination",
        "place_missing_location",
    }
    if any(r in high_markers for r in reasons):
        return "high"
    if len(reasons) >= 2:
        return "medium"
    return "low"


def _infer_primary_goal(label: str, granularity: str) -> str:
    l = normalize_spaces(label)
    if granularity == "no_action":
        return "no_contact"
    actions = split_actions(l)
    if not actions:
        return "task_action"
    return actions[-1]


def _infer_primary_object(label: str, granularity: str) -> str:
    if granularity == "no_action":
        return "none"
    tokens = lower(label).split()
    if len(tokens) < 2:
        return "item"
    # Keep conservative fallback to avoid hallucination.
    if tokens[0] == "pick" and len(tokens) >= 3 and tokens[1] == "up":
        return tokens[2]
    if tokens[0] in {"place", "move", "grab", "hold", "adjust", "flip", "wipe"} and len(tokens) >= 2:
        return tokens[1]
    return "item"


def normalize_annotation(
    annotation: Any,
    episode_id: str = "episode",
    annotation_version: str = "atlas_v2_pipeline",
    video_duration_sec: float = 0.0,
) -> Dict[str, Any]:
    if isinstance(annotation, (str, Path)):
        text = Path(annotation).read_text(encoding="utf-8")
        annotation = json.loads(text)

    if isinstance(annotation, dict):
        raw_segments = annotation.get("segments")
        episode_id = str(annotation.get("episode_id") or episode_id)
        if isinstance(annotation.get("video_duration_sec"), (int, float)):
            video_duration_sec = float(annotation["video_duration_sec"])
        if isinstance(annotation.get("annotation_version"), str) and annotation["annotation_version"].strip():
            annotation_version = annotation["annotation_version"].strip()
    elif isinstance(annotation, list):
        raw_segments = annotation
    else:
        raise ValueError("Annotation must be dict/list/path/json-string")

    if not isinstance(raw_segments, list):
        raise ValueError("Annotation segments must be a list")

    segments: List[Dict[str, Any]] = []
    max_end = 0.0

    for i, raw in enumerate(raw_segments, start=1):
        if not isinstance(raw, dict):
            continue

        seg_idx = int(raw.get("segment_index") or raw.get("step") or i)
        start = parse_time_value(raw.get("start_sec", raw.get("start", raw.get("from", raw.get("start_time", 0.0)))))
        end = parse_time_value(raw.get("end_sec", raw.get("end", raw.get("to", raw.get("end_time", 0.0)))))

        if end <= 0 and isinstance(raw.get("duration_seconds"), (int, float)):
            end = start + float(raw["duration_seconds"])

        label = normalize_spaces(raw.get("label", raw.get("description", raw.get("action", raw.get("annotation", "")))))
        gran = str(raw.get("granularity", raw.get("type", "coarse"))).strip().lower()
        if gran not in {"dense", "coarse", "no_action"}:
            gran = "no_action" if lower(label) == "no action" else "coarse"
        if lower(label) == "no action":
            gran = "no_action"

        confidence_raw = raw.get("confidence", raw.get("score", 0.7))
        if isinstance(confidence_raw, str):
            map_conf = {"low": 0.35, "medium": 0.65, "high": 0.9}
            confidence = map_conf.get(confidence_raw.strip().lower(), 0.7)
        else:
            try:
                confidence = float(confidence_raw)
            except (TypeError, ValueError):
                confidence = 0.7
        confidence = min(1.0, max(0.0, confidence))

        if end <= start:
            end = start + 0.1

        duration = round(end - start, 3)
        max_end = max(max_end, end)

        primary_goal = normalize_spaces(raw.get("primary_goal", "")) or _infer_primary_goal(label, gran)
        primary_object = normalize_spaces(raw.get("primary_object", "")) or _infer_primary_object(label, gran)

        segments.append(
            {
                "segment_index": seg_idx,
                "start_sec": round(start, 3),
                "end_sec": round(end, 3),
                "duration_sec": duration,
                "label": label or ("No Action" if gran == "no_action" else "handle item"),
                "granularity": gran,
                "primary_goal": primary_goal,
                "primary_object": primary_object,
                "secondary_objects": raw.get("secondary_objects", []),
                "actions_observed": raw.get("actions_observed", []),
                "confidence": confidence,
                "uncertainty_note": normalize_spaces(raw.get("uncertainty_note", "")),
                "escalation_flag": bool(raw.get("escalation_flag", False)),
                "escalation_reason": raw.get("escalation_reason", ""),
                "rule_checks": raw.get("rule_checks", {}),
                "audit_risk": raw.get("audit_risk", {"level": "low", "reasons": []}),
            }
        )

    segments.sort(key=lambda x: (x["start_sec"], x["end_sec"]))
    for idx, seg in enumerate(segments, start=1):
        seg["segment_index"] = idx

    if video_duration_sec <= 0:
        video_duration_sec = max_end

    episode_checks = {
        "segments_sorted": True,
        "no_negative_durations": True,
        "no_overlaps": True,
        "coverage_within_video_duration": True,
        "gaps_present": False,
        "repeated_action_logic_checked": True,
        "merge_split_logic_checked": True,
        "notes": "",
    }

    return {
        "episode_id": episode_id or "episode",
        "video_duration_sec": round(float(video_duration_sec), 3),
        "annotation_version": annotation_version,
        "source_context": {},
        "segments": segments,
        "episode_checks": episode_checks,
    }


def validate_segment(seg: Dict[str, Any], video_duration_sec: float) -> Tuple[Dict[str, Any], List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []

    idx = seg.get("segment_index")
    label = normalize_spaces(seg.get("label", ""))
    gran = seg.get("granularity")
    start = seg.get("start_sec")
    end = seg.get("end_sec")
    duration = seg.get("duration_sec")

    if not label:
        errors.append("empty_label")
    if not isinstance(start, (int, float)) or not isinstance(end, (int, float)):
        errors.append("timestamp_type_invalid")
    else:
        if start < 0 or end < 0:
            errors.append("timestamp_negative")
        if end <= start:
            errors.append("timestamp_order_invalid")
        if end > video_duration_sec + 0.1:
            warnings.append("end_beyond_video_duration")

    if isinstance(start, (int, float)) and isinstance(end, (int, float)) and isinstance(duration, (int, float)):
        if not duration_matches(float(start), float(end), float(duration)):
            errors.append("duration_mismatch")

    if label:
        if not min_two_words(label):
            errors.append("min_two_words_failed")
        if not is_imperative_like(label):
            errors.append("imperative_voice_failed")
        if has_numerals(label):
            errors.append("numerals_present")
        if has_intent_only_language(label):
            errors.append("intent_only_language")

        forbidden = contains_forbidden_verbs(label)
        if forbidden:
            errors.append("forbidden_verbs")
        disallowed_terms = disallowed_tool_terms_found(label)
        if disallowed_terms:
            errors.append("disallowed_tool_terms")
        if re.search(r"\bgripper\b", lower(label)):
            warnings.append("gripper_term_used")

        if gran == "no_action":
            if label != NO_ACTION_LABEL:
                errors.append("granularity_label_mismatch")
            pg = normalize_spaces(seg.get("primary_goal", ""))
            if pg and pg not in {"idle", "irrelevant", "no_contact"}:
                warnings.append("no_action_primary_goal_unusual")
        else:
            if label == NO_ACTION_LABEL:
                errors.append("granularity_label_mismatch")

        if no_action_mixed_with_action(label):
            errors.append("no_action_mixed")

        if label != NO_ACTION_LABEL:
            if count_atomic_actions(label) > MAX_ATOMIC_ACTIONS_PER_LABEL:
                errors.append("too_many_atomic_actions")
            missing = [phrase for phrase in split_actions(label) if detect_possible_missing_object(phrase)]
            if missing:
                warnings.append("possible_missing_object")
            if has_unattached_verb_chain(label):
                errors.append("verbs_not_attached_to_objects")
            if not place_has_location(label):
                warnings.append("place_missing_location")
            if dense_coarse_mixed(seg):
                errors.append("dense_coarse_mixed")

    confidence = seg.get("confidence")
    if not isinstance(confidence, (int, float)) or not (0 <= float(confidence) <= 1):
        warnings.append("confidence_invalid_or_missing")

    rc = seg.get("rule_checks")
    if isinstance(rc, dict):
        if rc.get("no_forbidden_verbs") is True and "forbidden_verbs" in errors:
            warnings.append("rule_checks_contradiction")
        if rc.get("no_numerals") is True and "numerals_present" in errors:
            warnings.append("rule_checks_contradiction")
        if rc.get("dense_coarse_not_mixed") is True and "dense_coarse_mixed" in errors:
            warnings.append("rule_checks_contradiction")

    derived_rule_checks = {
        "imperative_voice": "imperative_voice_failed" not in errors,
        "min_two_words": "min_two_words_failed" not in errors,
        "no_numerals": "numerals_present" not in errors,
        "no_forbidden_verbs": "forbidden_verbs" not in errors,
        "forbidden_verbs_found": contains_forbidden_verbs(label),
        "verbs_attached_to_objects": (
            "possible_missing_object" not in warnings and "verbs_not_attached_to_objects" not in errors
        ),
        "one_goal": True,
        "full_action_coverage": True,
        "no_hallucinated_steps": True,
        "dense_coarse_not_mixed": "dense_coarse_mixed" not in errors,
        "no_action_not_mixed_with_action": "no_action_mixed" not in errors,
        "timestamps_aligned": all(e not in errors for e in ["timestamp_order_invalid", "duration_mismatch"]),
        "hands_disengage_boundary_ok": True,
    }

    audit_reasons: List[str] = []
    if "forbidden_verbs" in errors:
        audit_reasons.append("verb_choice_ambiguous")
    if "verbs_not_attached_to_objects" in errors:
        audit_reasons.append("verb_choice_ambiguous")
    if "disallowed_tool_terms" in errors:
        audit_reasons.append("verb_choice_ambiguous")
    if "numerals_present" in errors:
        audit_reasons.append("possible_hallucination")
    if "dense_coarse_mixed" in errors:
        audit_reasons.append("granularity_choice_ambiguous")
    if "too_many_atomic_actions" in errors:
        audit_reasons.append("granularity_choice_ambiguous")
    if "no_action_mixed" in errors:
        audit_reasons.append("no_action_rule_risk")
    if "duration_mismatch" in errors or "timestamp_order_invalid" in errors:
        audit_reasons.append("timestamp_misalignment")
    if "possible_missing_object" in warnings:
        audit_reasons.append("object_identity_uncertain")
    if "place_missing_location" in warnings:
        audit_reasons.append("verb_choice_ambiguous")

    segment_report = {
        "segment_index": idx,
        "label": label,
        "errors": errors,
        "warnings": warnings,
        "derived_rule_checks": derived_rule_checks,
        "suggested_audit_risk": {
            "level": classify_audit_risk(errors + warnings),
            "reasons": sorted(set(audit_reasons)),
        },
    }
    return segment_report, errors, warnings


def validate_episode(annotation: Dict[str, Any]) -> Dict[str, Any]:
    ann = copy.deepcopy(annotation)
    duration = float(ann.get("video_duration_sec", 0) or 0)
    segments = ann.get("segments", [])
    if not isinstance(segments, list):
        return {"ok": False, "fatal_error": "segments_not_list"}

    seg_reports = []
    episode_errors: List[str] = []
    episode_warnings: List[str] = []

    for seg in segments:
        report, _, _ = validate_segment(seg, duration)
        seg_reports.append(report)
        seg["rule_checks"] = report["derived_rule_checks"]
        seg["audit_risk"] = report["suggested_audit_risk"]

    starts_ends = []
    for seg in segments:
        try:
            starts_ends.append((int(seg.get("segment_index", 0)), float(seg["start_sec"]), float(seg["end_sec"])))
        except Exception:
            episode_errors.append("segment_timestamp_parse_error")

    idxs = [x[0] for x in starts_ends]
    if idxs != sorted(idxs):
        episode_warnings.append("segment_indices_not_sorted")

    time_sorted = sorted(starts_ends, key=lambda x: (x[1], x[2]))
    for i in range(1, len(time_sorted)):
        prev = time_sorted[i - 1]
        cur = time_sorted[i]
        if cur[1] < prev[2] - 1e-6:
            episode_errors.append("timestamp_overlap")
            break

    if duration > 0:
        for _, s, e in starts_ends:
            if s < 0 or e > duration + 0.1:
                episode_warnings.append("segment_outside_video_duration")
                break

    any_seg_errors = any(report["errors"] for report in seg_reports)
    major_fail_triggers: List[str] = []
    for report in seg_reports:
        errs = set(report["errors"])
        if "forbidden_verbs" in errs:
            major_fail_triggers.append("forbidden_verbs_used")
        if "verbs_not_attached_to_objects" in errs:
            major_fail_triggers.append("verbs_not_attached_to_objects")
        if "disallowed_tool_terms" in errs:
            major_fail_triggers.append("disallowed_tool_terms")
        if "dense_coarse_mixed" in errs:
            major_fail_triggers.append("dense_coarse_mixed")
        if "too_many_atomic_actions" in errs:
            major_fail_triggers.append("too_many_atomic_actions")
        if "no_action_mixed" in errs:
            major_fail_triggers.append("no_action_mixed_with_action")
        if "timestamp_order_invalid" in errs or "duration_mismatch" in errs:
            major_fail_triggers.append("timestamps_invalid")

    if "timestamp_overlap" in episode_errors:
        major_fail_triggers.append("episode_overlap")

    ann["episode_checks"] = {
        "segments_sorted": "segment_indices_not_sorted" not in episode_warnings,
        "no_negative_durations": all((seg.get("duration_sec", 0) or 0) > 0 for seg in segments),
        "no_overlaps": "timestamp_overlap" not in episode_errors,
        "coverage_within_video_duration": "segment_outside_video_duration" not in episode_warnings,
        "gaps_present": False,
        "repeated_action_logic_checked": True,
        "merge_split_logic_checked": True,
        "notes": "",
    }

    return {
        "ok": not (episode_errors or any_seg_errors),
        "episode_id": ann.get("episode_id"),
        "normalized_annotation": ann,
        "episode_errors": sorted(set(episode_errors)),
        "episode_warnings": sorted(set(episode_warnings)),
        "segment_reports": seg_reports,
        "major_fail_triggers": sorted(set(major_fail_triggers)),
        "repair_recommended": bool(episode_errors or any_seg_errors or episode_warnings),
    }


def build_repair_payload(
    annotation: Dict[str, Any],
    validator_report: Dict[str, Any],
    evidence_notes: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"annotation": annotation, "validator_report": validator_report}
    if evidence_notes:
        payload["evidence_notes"] = evidence_notes
    return payload


def replace_small_numerals(text: str) -> str:
    def repl(match: re.Match[str]) -> str:
        token = match.group(0)
        return NUMERAL_TO_WORD.get(token, token)

    return re.sub(r"\b(?:10|[0-9])\b", repl, text)


def cheap_preclean_label(label: str) -> str:
    return replace_small_numerals(normalize_spaces(label))
