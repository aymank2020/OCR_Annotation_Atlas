"""
Batch runner for multi-way compare across episodes from episodes_review_index.json.

For each episode:
- resolve Tier2/API/Chat/Vertex-Chat/video inputs
- run atlas_triplet_compare
- persist per-episode result JSON
- optionally upsert summary into outputs/gemini_chat_evaluations.json
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from atlas_eval_store import upsert_evaluation
from atlas_triplet_compare import (
    generate_gemini_chat_timed_labels,
    parse_timed_segments_payload,
    parse_timed_segments_text,
    run_triplet_compare,
    segments_to_timed_text,
)


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _parse_status_filter(text: str) -> set[str]:
    raw = [s.strip().lower() for s in str(text or "").split(",")]
    return {s for s in raw if s}


def _as_path(value: Any) -> Optional[Path]:
    raw = str(value or "").strip()
    if not raw:
        return None
    p = Path(raw)
    if p.exists():
        return p.resolve()
    return None


def _is_existing_file_path(value: Any) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    p = Path(raw)
    try:
        return p.exists() and p.is_file()
    except Exception:
        return False


def _path_str_if_file(path: Optional[Path]) -> str:
    if path is None:
        return ""
    try:
        if path.exists() and path.is_file():
            return str(path)
    except Exception:
        return ""
    return ""


def _episode_related_paths(ep: Dict[str, Any]) -> List[Path]:
    out: List[Path] = []
    raw_related = ep.get("related_files")
    if not isinstance(raw_related, list):
        raw_related = ep.get("related_paths")
    for item in raw_related if isinstance(raw_related, list) else []:
        p = _as_path(item)
        if p is not None:
            out.append(p)
    return out


def _pick_by_name(paths: List[Path], exact_name: str) -> Optional[Path]:
    target = exact_name.lower()
    for p in paths:
        if p.name.lower() == target:
            return p
    return None


def _pick_first_existing(paths: List[Path], predicate) -> Optional[Path]:
    for p in paths:
        try:
            if predicate(p):
                return p
        except Exception:
            continue
    return None


def _load_eval_map(outputs_dir: Path) -> Dict[str, Dict[str, Any]]:
    payload = _load_json(outputs_dir / "gemini_chat_evaluations.json", default={})
    raw = payload.get("evaluations") if isinstance(payload, dict) else {}
    out: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw, dict):
        for eid, rec in raw.items():
            if isinstance(rec, dict):
                out[str(eid or "").strip().lower()] = rec
    return out


def _text_has_timed_segments(text: str) -> bool:
    return bool(parse_timed_segments_text(str(text or "")))


def _path_has_timed_segments(path: Path) -> bool:
    try:
        if not path.exists() or not path.is_file():
            return False
        raw = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return False
    if path.suffix.lower() == ".json":
        try:
            payload = json.loads(raw)
            if parse_timed_segments_payload(payload):
                return True
        except Exception:
            pass
    return _text_has_timed_segments(raw)


def _ensure_chat_text_from_eval(
    outputs_dir: Path,
    episode_id: str,
    eval_map: Dict[str, Dict[str, Any]],
) -> Optional[Path]:
    eid = str(episode_id or "").strip().lower()
    if not eid:
        return None
    rec = eval_map.get(eid) or {}
    text = str(rec.get("text") or "").strip()
    if not text:
        return None
    # Do not pollute timed-chat slots with compare summaries.
    if not _text_has_timed_segments(text):
        return None
    target_dir = outputs_dir / "chat_reviews" / eid
    target_dir.mkdir(parents=True, exist_ok=True)
    out_path = target_dir / f"text_{eid}_chat.txt"
    out_path.write_text(text, encoding="utf-8")
    return out_path


def _ensure_upload_opt_video(outputs_dir: Path, episode_id: str, video_main: Path) -> Optional[Path]:
    eid = str(episode_id or "").strip().lower()
    if not eid:
        return None
    if not video_main.exists() or not video_main.is_file():
        return None
    target = outputs_dir / f"video_{eid}_upload_opt.mp4"
    if target.exists() and target.stat().st_size > 0:
        return target
    if shutil.which("ffmpeg") is None:
        return None
    target.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(video_main),
        "-vf",
        "scale='min(960,iw)':-2:flags=lanczos,fps=8",
        "-an",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "30",
        "-movflags",
        "+faststart",
        str(target),
    ]
    try:
        subprocess.run(cmd, check=True)
    except Exception:
        return None
    if target.exists() and target.stat().st_size > 0:
        return target
    return None


def _ensure_api_text_from_labels(outputs_dir: Path, episode_id: str, labels_path: str) -> Optional[Path]:
    eid = str(episode_id or "").strip().lower()
    src_raw = str(labels_path or "").strip()
    if not eid or not src_raw:
        return None
    src = Path(src_raw)
    if not src.exists() or not src.is_file():
        return None
    payload = _load_json(src, default=None)
    segments = parse_timed_segments_payload(payload)
    if not segments:
        return None
    text = segments_to_timed_text(segments)
    if not text:
        return None
    out_path = outputs_dir / f"text_{eid}_update.txt"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text + "\n", encoding="utf-8")
    return out_path


def _ensure_api_text_from_chat(outputs_dir: Path, episode_id: str, chat_path: str) -> Optional[Path]:
    eid = str(episode_id or "").strip().lower()
    src_raw = str(chat_path or "").strip()
    if not eid or not src_raw:
        return None
    src = Path(src_raw)
    if not src.exists() or not src.is_file():
        return None
    raw = src.read_text(encoding="utf-8", errors="replace")
    segments = parse_timed_segments_text(raw)
    if not segments:
        return None
    text = segments_to_timed_text(segments)
    if not text:
        return None
    out_path = outputs_dir / f"text_{eid}_update.txt"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text + "\n", encoding="utf-8")
    return out_path


def _pick_episode_inputs(
    ep: Dict[str, Any],
    outputs_dir: Path,
    eval_map: Dict[str, Dict[str, Any]],
    write_chat_from_evals: bool,
) -> Dict[str, Any]:
    eid = str(ep.get("episode_id") or "").strip().lower()
    rel = _episode_related_paths(ep)

    video_main = _pick_by_name(rel, f"video_{eid}.mp4")
    if video_main is None:
        video_main = _as_path(ep.get("video_path"))
    if video_main is None:
        video_main = _pick_first_existing(
            rel,
            lambda p: p.suffix.lower() in {".mp4", ".mov", ".webm", ".mkv"} and p.name.lower().startswith("video_"),
        )

    video_limit = _pick_by_name(rel, f"video_{eid}_upload_opt.mp4")
    if video_limit is None:
        video_limit = _pick_first_existing(
            rel,
            lambda p: p.suffix.lower() in {".mp4", ".mov", ".webm", ".mkv"}
            and p.name.lower().startswith(f"video_{eid}")
            and "upload_opt" in p.name.lower(),
        )

    tier2_path = _as_path(ep.get("tier2_text_path"))
    if tier2_path is None:
        tier2_path = _pick_by_name(rel, f"text_{eid}_current.txt")
    if tier2_path is None:
        tier2_path = _pick_first_existing(
            rel,
            lambda p: p.name.lower().startswith(f"text_{eid}")
            and p.suffix.lower() in {".txt", ".json"}
            and ("current" in p.name.lower() or "tier2" in p.name.lower()),
        )

    api_path = _as_path(ep.get("tier3_text_path"))
    if api_path is None:
        api_path = _pick_by_name(rel, f"text_{eid}_update.txt")
    if api_path is None:
        api_path = _pick_first_existing(
            rel,
            lambda p: p.name.lower().startswith(f"text_{eid}")
            and p.suffix.lower() in {".txt", ".json"}
            and (
                "update" in p.name.lower()
                or "tier3" in p.name.lower()
                or "api" in p.name.lower()
            ),
        )

    task_state_path = _pick_by_name(rel, f"task_state_{eid}.json")
    if task_state_path is None:
        task_state_path = _pick_first_existing(rel, lambda p: p.name.lower().startswith("task_state_") and p.suffix.lower() == ".json")

    labels_path = _pick_by_name(rel, f"labels_{eid}.json")
    if labels_path is None:
        labels_path = _pick_first_existing(rel, lambda p: p.name.lower().startswith("labels_") and p.suffix.lower() == ".json")

    chat_path = outputs_dir / "chat_reviews" / eid / f"text_{eid}_chat.txt"
    if not chat_path.exists():
        chat_path = _pick_by_name(rel, f"text_{eid}_chat.txt") or chat_path
    if not chat_path.exists() and write_chat_from_evals:
        generated = _ensure_chat_text_from_eval(outputs_dir, eid, eval_map)
        if generated is not None:
            chat_path = generated

    vertex_chat_path = outputs_dir / "vertex_chat_reviews" / eid / f"text_{eid}_vertex_chat.txt"
    if not vertex_chat_path.exists():
        vertex_chat_path = _pick_by_name(rel, f"text_{eid}_vertex_chat.txt") or vertex_chat_path
    if not vertex_chat_path.exists():
        vertex_chat_path = _pick_first_existing(
            rel,
            lambda p: p.name.lower().startswith(f"text_{eid}")
            and p.suffix.lower() in {".txt", ".json"}
            and ("vertex_chat" in p.name.lower() or "vertex" in p.name.lower()),
        ) or vertex_chat_path

    return {
        "episode_id": eid,
        "video_path": _path_str_if_file(video_main),
        "video_path_limit": _path_str_if_file(video_limit),
        "tier2_path": _path_str_if_file(tier2_path),
        "api_path": _path_str_if_file(api_path),
        "chat_path": str(chat_path) if chat_path.exists() and chat_path.is_file() else "",
        "vertex_chat_path": str(vertex_chat_path) if vertex_chat_path.exists() and vertex_chat_path.is_file() else "",
        "task_state_path": _path_str_if_file(task_state_path),
        "labels_path": _path_str_if_file(labels_path),
    }


def _ensure_chat_timed_from_video(
    *,
    config: str,
    outputs_dir: Path,
    cache_dir: Path,
    remote: str,
    chat_timed_model: str,
    episode_id: str,
    video_path: str,
    video_path_limit: str,
) -> Optional[str]:
    eid = str(episode_id or "").strip().lower()
    main_video_raw = str(video_path or "").strip()
    if not eid or not main_video_raw:
        return None
    main_video = Path(main_video_raw)
    if not main_video.exists() or not main_video.is_file():
        return None
    limit_video_raw = str(video_path_limit or "").strip()
    limit_video = Path(limit_video_raw) if limit_video_raw else None
    if limit_video is not None and (not limit_video.exists() or not limit_video.is_file()):
        limit_video = None
    if limit_video is None:
        generated_limit = _ensure_upload_opt_video(outputs_dir, eid, main_video)
        if generated_limit is not None:
            limit_video = generated_limit

    chat_dir = outputs_dir / "chat_reviews" / eid
    chat_dir.mkdir(parents=True, exist_ok=True)
    chat_txt = chat_dir / f"text_{eid}_chat.txt"
    chat_json = chat_dir / f"labels_{eid}.json"

    result = generate_gemini_chat_timed_labels(
        config_path=config,
        video_path=str(main_video),
        video_path_limit=str(limit_video) if limit_video is not None else "",
        remote=remote,
        cache_dir=str(cache_dir / eid / "chat_timed"),
        model=chat_timed_model,
        out_txt=str(chat_txt),
        out_json=str(chat_json),
        episode_id=eid,
        prompt_scope="timed_labels",
    )
    out_txt = str(result.get("out_txt") or "").strip()
    if out_txt and Path(out_txt).exists():
        return out_txt
    if chat_txt.exists():
        return str(chat_txt)
    return None


def _ensure_vertex_chat_from_video(
    *,
    config: str,
    outputs_dir: Path,
    cache_dir: Path,
    remote: str,
    vertex_chat_model: str,
    episode_id: str,
    video_path: str,
    video_path_limit: str,
) -> Optional[str]:
    eid = str(episode_id or "").strip().lower()
    main_video_raw = str(video_path or "").strip()
    if not eid or not main_video_raw:
        return None
    main_video = Path(main_video_raw)
    if not main_video.exists() or not main_video.is_file():
        return None
    limit_video_raw = str(video_path_limit or "").strip()
    limit_video = Path(limit_video_raw) if limit_video_raw else None
    if limit_video is not None and (not limit_video.exists() or not limit_video.is_file()):
        limit_video = None
    if limit_video is None:
        generated_limit = _ensure_upload_opt_video(outputs_dir, eid, main_video)
        if generated_limit is not None:
            limit_video = generated_limit

    vertex_dir = outputs_dir / "vertex_chat_reviews" / eid
    vertex_dir.mkdir(parents=True, exist_ok=True)
    vertex_txt = vertex_dir / f"text_{eid}_vertex_chat.txt"
    vertex_json = vertex_dir / f"labels_{eid}_vertex_chat.json"

    result = generate_gemini_chat_timed_labels(
        config_path=config,
        video_path=str(main_video),
        video_path_limit=str(limit_video) if limit_video is not None else "",
        remote=remote,
        cache_dir=str(cache_dir / eid / "vertex_chat_timed"),
        model=vertex_chat_model,
        out_txt=str(vertex_txt),
        out_json=str(vertex_json),
        episode_id=eid,
        prompt_scope="vertex_chat",
    )
    out_txt = str(result.get("out_txt") or "").strip()
    if out_txt and Path(out_txt).exists():
        return out_txt
    if vertex_txt.exists():
        return str(vertex_txt)
    return None


def _ensure_api_update_from_video(
    *,
    config: str,
    outputs_dir: Path,
    cache_dir: Path,
    remote: str,
    api_model: str,
    episode_id: str,
    video_path: str,
    video_path_limit: str,
) -> Optional[Path]:
    eid = str(episode_id or "").strip().lower()
    main_video_raw = str(video_path or "").strip()
    if not eid or not main_video_raw:
        return None
    main_video = Path(main_video_raw)
    if not main_video.exists() or not main_video.is_file():
        return None

    limit_video_raw = str(video_path_limit or "").strip()
    limit_video = Path(limit_video_raw) if limit_video_raw else None
    if limit_video is not None and (not limit_video.exists() or not limit_video.is_file()):
        limit_video = None
    if limit_video is None:
        generated_limit = _ensure_upload_opt_video(outputs_dir, eid, main_video)
        if generated_limit is not None:
            limit_video = generated_limit

    api_dir = outputs_dir / "api_reviews" / eid
    api_dir.mkdir(parents=True, exist_ok=True)
    api_txt = api_dir / f"text_{eid}_api.txt"
    api_json = api_dir / f"labels_{eid}_api.json"

    result = generate_gemini_chat_timed_labels(
        config_path=config,
        video_path=str(main_video),
        video_path_limit=str(limit_video) if limit_video is not None else "",
        remote=remote,
        cache_dir=str(cache_dir / eid / "api_timed"),
        model=api_model,
        out_txt=str(api_txt),
        out_json=str(api_json),
        episode_id=eid,
        prompt_scope="api_update",
    )
    out_txt_raw = str(result.get("out_txt") or "").strip()
    out_txt_path = Path(out_txt_raw) if out_txt_raw else api_txt
    if not out_txt_path.exists() or not out_txt_path.is_file():
        return None

    rebuilt = _ensure_api_text_from_chat(outputs_dir, eid, str(out_txt_path))
    if rebuilt is not None:
        return rebuilt

    # Fallback: accept already-timed plain text as API update.
    raw = out_txt_path.read_text(encoding="utf-8", errors="replace")
    segments = parse_timed_segments_text(raw)
    if not segments:
        return None
    text = segments_to_timed_text(segments)
    if not text:
        return None
    out_path = outputs_dir / f"text_{eid}_update.txt"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text + "\n", encoding="utf-8")
    return out_path


def _ensure_tier2_placeholder(outputs_dir: Path, episode_id: str) -> Optional[Path]:
    eid = str(episode_id or "").strip().lower()
    if not eid:
        return None
    out_dir = outputs_dir / "triplet_fallback"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"text_{eid}_tier2_missing.txt"
    text = (
        "Tier2 source is unavailable for this episode.\n"
        "Treat tier2 as unavailable: score tier2=0, never choose tier2 as winner.\n"
    )
    out_path.write_text(text, encoding="utf-8")
    return out_path


def _safe_score(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        n = float(value)
        if n != n:
            return None
        return max(0.0, min(100.0, n))
    except Exception:
        return None


def _derive_eval_score(payload: Dict[str, Any]) -> Optional[int]:
    judge = payload.get("judge_result")
    if not isinstance(judge, dict):
        return None

    winner = str(judge.get("winner") or "").strip().lower()
    scores = judge.get("scores")
    score: Optional[float] = None
    if isinstance(scores, dict):
        score = _safe_score(scores.get(winner))

    if score is None:
        if winner == "api":
            score = 100.0
        elif winner == "chat":
            score = 95.0
        elif winner == "vertex_chat":
            score = 97.0
        elif winner == "tier2":
            score = 80.0
        elif winner == "none":
            score = 0.0

    hall = judge.get("hallucination")
    if isinstance(hall, dict) and winner in hall and bool(hall.get(winner)):
        score = min(60.0, score or 60.0)
    return int(round(score)) if score is not None else None


def _build_eval_text(payload: Dict[str, Any]) -> str:
    judge = payload.get("judge_result")
    if not isinstance(judge, dict):
        return str(payload.get("judge_raw_text") or "").strip()
    winner = str(judge.get("winner") or "").strip().lower() or "unknown"
    submit_safe = str(judge.get("submit_safe_solution") or "").strip().lower() or "unknown"
    reason = str(judge.get("best_reason_short") or "").strip()
    if not reason:
        reason = str(judge.get("final_recommendation") or "").strip()
    issues = judge.get("major_issues")
    issue_counts: Dict[str, int] = {}
    if isinstance(issues, dict):
        for k, v in issues.items():
            if isinstance(v, list):
                issue_counts[str(k)] = len(v)
    parts = [
        f"winner={winner}",
        f"submit_safe_solution={submit_safe}",
    ]
    if reason:
        parts.append(f"reason={reason}")
    if issue_counts:
        parts.append(f"issues={json.dumps(issue_counts, ensure_ascii=False)}")
    return "; ".join(parts)


def _skip_eval_update(existing: Optional[Dict[str, Any]], overwrite: bool, source: str) -> bool:
    if overwrite or not existing:
        return False
    text = str(existing.get("text") or "").strip()
    old_source = str(existing.get("source") or "").strip().lower()
    if not text:
        return False
    if old_source in {source.lower(), "triplet_compare_batch"}:
        return False
    return True


def run_batch(
    *,
    config: str,
    outputs_dir: Path,
    index_path: Path,
    remote: str,
    cache_dir: Path,
    results_dir: Path,
    results_jsonl: Path,
    model: str,
    compare_model: str,
    chat_timed_model: str,
    vertex_chat_model: str,
    api_update_model: str,
    only_status: str,
    limit: int,
    require_chat_path: bool,
    write_chat_from_evals: bool,
    generate_chat_timed_missing: bool,
    generate_vertex_chat_missing: bool,
    regenerate_api_missing: bool,
    regenerate_api_from_video: bool,
    allow_missing_tier2: bool,
    update_evals: bool,
    overwrite_evals: bool,
    source: str,
    skip_existing_results: bool,
) -> Dict[str, Any]:
    payload = _load_json(index_path, default={})
    episodes = payload.get("episodes", []) if isinstance(payload, dict) else []
    if not isinstance(episodes, list):
        episodes = []

    outputs_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)
    results_jsonl.parent.mkdir(parents=True, exist_ok=True)
    results_jsonl.write_text("", encoding="utf-8")

    status_filter = _parse_status_filter(only_status)
    eval_map = _load_eval_map(outputs_dir)

    summaries: List[Dict[str, Any]] = []
    done = 0
    ok = 0
    skipped = 0
    errors = 0
    eval_updated = 0
    eval_skipped = 0
    effective_compare_model = str(compare_model or model or "").strip()
    effective_chat_timed_model = str(chat_timed_model or model or "").strip()
    effective_vertex_chat_model = str(vertex_chat_model or model or "").strip()
    effective_api_update_model = str(api_update_model or model or "").strip()

    def _append_result_row(row: Dict[str, Any]) -> None:
        with results_jsonl.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            f.flush()

    for ep in episodes:
        if not isinstance(ep, dict):
            continue
        eid = str(ep.get("episode_id") or "").strip().lower()
        if not eid:
            continue
        st = str(ep.get("review_status") or "").strip().lower()
        if status_filter and st not in status_filter:
            continue
        if limit > 0 and done >= limit:
            break
        done += 1
        print(f"[triplet-batch] start {done} episode={eid} status={st}")

        row: Dict[str, Any] = {
            "episode_id": eid,
            "review_status": st,
            "ok": False,
            "skipped": False,
            "reason": "",
            "result_path": "",
            "winner": "",
            "score_pct": None,
        }

        inputs = _pick_episode_inputs(ep, outputs_dir, eval_map, write_chat_from_evals)

        # Optional fallback mode: continue even when tier2 source is missing.
        if allow_missing_tier2 and not _is_existing_file_path(inputs.get("tier2_path")):
            tier2_placeholder = _ensure_tier2_placeholder(outputs_dir, eid)
            if tier2_placeholder is not None:
                inputs["tier2_path"] = str(tier2_placeholder)
                print(f"[triplet-batch] info episode={eid} using_tier2_placeholder={tier2_placeholder}")

        # Fast fail on non-recoverable prerequisites to avoid expensive video processing.
        hard_missing: List[str] = []
        for key in ("video_path",):
            if not _is_existing_file_path(inputs.get(key)):
                hard_missing.append(key)
        if not allow_missing_tier2 and not _is_existing_file_path(inputs.get("tier2_path")):
            hard_missing.append("tier2_path")
        if hard_missing:
            row["skipped"] = True
            row["reason"] = f"missing_inputs: {', '.join(sorted(set(hard_missing)))}"
            summaries.append(row)
            _append_result_row(row)
            skipped += 1
            print(f"[triplet-batch] skip episode={eid} reason={row['reason']}")
            continue

        # Reject non-timed chat placeholders (legacy compare summaries).
        chat_candidate = str(inputs.get("chat_path") or "").strip()
        if chat_candidate:
            cp = Path(chat_candidate)
            if (not cp.exists()) or (not cp.is_file()) or (not _path_has_timed_segments(cp)):
                inputs["chat_path"] = ""

        # Reject non-timed vertex chat placeholders.
        vertex_chat_candidate = str(inputs.get("vertex_chat_path") or "").strip()
        if vertex_chat_candidate:
            vp = Path(vertex_chat_candidate)
            if (not vp.exists()) or (not vp.is_file()) or (not _path_has_timed_segments(vp)):
                inputs["vertex_chat_path"] = ""

        # Ensure Gemini Chat timed labels exist by calling Gemini with video.
        if generate_chat_timed_missing and not _is_existing_file_path(inputs.get("chat_path")):
            try:
                generated_chat = _ensure_chat_timed_from_video(
                    config=config,
                    outputs_dir=outputs_dir,
                    cache_dir=cache_dir,
                    remote=remote,
                    chat_timed_model=effective_chat_timed_model,
                    episode_id=eid,
                    video_path=str(inputs.get("video_path") or ""),
                    video_path_limit=str(inputs.get("video_path_limit") or ""),
                )
                if generated_chat:
                    inputs["chat_path"] = generated_chat
                    print(f"[triplet-batch] info episode={eid} generated_chat_timed={generated_chat}")
            except Exception as exc:
                print(f"[triplet-batch] warn episode={eid} chat_timed_generation_failed={exc}")

        # Ensure Vertex Chat timed labels exist by calling Vertex with video.
        if generate_vertex_chat_missing and not _is_existing_file_path(inputs.get("vertex_chat_path")):
            try:
                generated_vertex_chat = _ensure_vertex_chat_from_video(
                    config=config,
                    outputs_dir=outputs_dir,
                    cache_dir=cache_dir,
                    remote=remote,
                    vertex_chat_model=effective_vertex_chat_model,
                    episode_id=eid,
                    video_path=str(inputs.get("video_path") or ""),
                    video_path_limit=str(inputs.get("video_path_limit") or ""),
                )
                if generated_vertex_chat:
                    inputs["vertex_chat_path"] = generated_vertex_chat
                    print(f"[triplet-batch] info episode={eid} generated_vertex_chat={generated_vertex_chat}")
            except Exception as exc:
                print(f"[triplet-batch] warn episode={eid} vertex_chat_generation_failed={exc}")

        # Rebuild missing API update text when absent.
        if regenerate_api_missing and not _is_existing_file_path(inputs.get("api_path")):
            rebuilt_api: Optional[Path] = None
            if regenerate_api_from_video:
                try:
                    rebuilt_api = _ensure_api_update_from_video(
                        config=config,
                        outputs_dir=outputs_dir,
                        cache_dir=cache_dir,
                        remote=remote,
                        api_model=effective_api_update_model or effective_chat_timed_model,
                        episode_id=eid,
                        video_path=str(inputs.get("video_path") or ""),
                        video_path_limit=str(inputs.get("video_path_limit") or ""),
                    )
                except Exception as exc:
                    print(f"[triplet-batch] warn episode={eid} api_regen_from_video_failed={exc}")

            if rebuilt_api is None:
                rebuilt_api = _ensure_api_text_from_labels(outputs_dir, eid, str(inputs.get("labels_path") or ""))
            if rebuilt_api is None:
                rebuilt_api = _ensure_api_text_from_chat(outputs_dir, eid, str(inputs.get("chat_path") or ""))
            if rebuilt_api is None and generate_chat_timed_missing:
                try:
                    generated_chat = str(inputs.get("chat_path") or "").strip()
                    if not generated_chat:
                        generated_chat = _ensure_chat_timed_from_video(
                            config=config,
                            outputs_dir=outputs_dir,
                            cache_dir=cache_dir,
                            remote=remote,
                            chat_timed_model=effective_chat_timed_model,
                            episode_id=eid,
                            video_path=str(inputs.get("video_path") or ""),
                            video_path_limit=str(inputs.get("video_path_limit") or ""),
                        ) or ""
                    if generated_chat:
                        rebuilt_api = _ensure_api_text_from_chat(outputs_dir, eid, generated_chat)
                except Exception as exc:
                    print(f"[triplet-batch] warn episode={eid} api_rebuild_from_chat_failed={exc}")
            if rebuilt_api is not None:
                inputs["api_path"] = str(rebuilt_api)
                print(f"[triplet-batch] info episode={eid} regenerated_api_update={rebuilt_api}")

        missing: List[str] = []
        required_keys = ["video_path", "api_path"]
        required_keys.append("vertex_chat_path")
        if not allow_missing_tier2:
            required_keys.append("tier2_path")
        for key in required_keys:
            if not _is_existing_file_path(inputs.get(key)):
                missing.append(key)
        if require_chat_path and not _is_existing_file_path(inputs.get("chat_path")):
            missing.append("chat_path")
        if missing:
            row["skipped"] = True
            row["reason"] = f"missing_inputs: {', '.join(sorted(set(missing)))}"
            summaries.append(row)
            _append_result_row(row)
            skipped += 1
            print(f"[triplet-batch] skip episode={eid} reason={row['reason']}")
            continue

        out_path = results_dir / f"triplet_compare_{eid}.json"
        row["result_path"] = str(out_path)
        payload_obj: Optional[Dict[str, Any]] = None

        if skip_existing_results and out_path.exists():
            payload_obj = _load_json(out_path, default=None)
            if isinstance(payload_obj, dict):
                row["reason"] = "cached_result"
            else:
                payload_obj = None

        if payload_obj is None:
            try:
                payload_obj = run_triplet_compare(
                    config_path=config,
                    video_path=inputs["video_path"],
                    video_path_limit=inputs.get("video_path_limit", ""),
                    tier2_path=inputs["tier2_path"],
                    api_path=inputs["api_path"],
                    chat_path=inputs.get("chat_path", ""),
                    vertex_chat_path=inputs.get("vertex_chat_path", ""),
                    task_state_path=inputs.get("task_state_path", ""),
                    labels_path=inputs.get("labels_path", ""),
                    remote=remote,
                    cache_dir=str(cache_dir / eid),
                    model=effective_compare_model,
                    out=str(out_path),
                )
            except Exception as exc:
                row["reason"] = f"triplet_error: {exc}"
                summaries.append(row)
                _append_result_row(row)
                errors += 1
                print(f"[triplet-batch] error episode={eid} reason={row['reason']}")
                continue

        judge = payload_obj.get("judge_result", {}) if isinstance(payload_obj, dict) else {}
        winner = str(judge.get("winner") if isinstance(judge, dict) else "").strip().lower()
        row["winner"] = winner
        row["ok"] = True
        row["reason"] = row["reason"] or "ok"
        score_pct = _derive_eval_score(payload_obj if isinstance(payload_obj, dict) else {})
        row["score_pct"] = score_pct
        summaries.append(row)
        _append_result_row(row)
        ok += 1
        print(
            f"[triplet-batch] ok episode={eid} winner={row.get('winner') or 'unknown'} "
            f"score={row.get('score_pct')}"
        )

        if update_evals:
            existing = eval_map.get(eid)
            if _skip_eval_update(existing, overwrite=overwrite_evals, source=source):
                eval_skipped += 1
            else:
                eval_text = _build_eval_text(payload_obj if isinstance(payload_obj, dict) else {})
                upsert_evaluation(
                    outputs_dir=outputs_dir,
                    episode_id=eid,
                    text=eval_text,
                    score_pct=score_pct,
                    source=source,
                )
                eval_updated += 1
                eval_map[eid] = {
                    "episode_id": eid,
                    "text": eval_text,
                    "score_pct": score_pct,
                    "source": source,
                    "updated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "index_path": str(index_path),
        "results_jsonl": str(results_jsonl),
        "results_dir": str(results_dir),
        "processed": done,
        "ok": ok,
        "skipped": skipped,
        "errors": errors,
        "eval_updated": eval_updated,
        "eval_skipped": eval_skipped,
    }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch 4-way compare across episodes_review_index.json")
    parser.add_argument("--config", default="sample_web_auto_solver_vps.yaml")
    parser.add_argument("--outputs-dir", default="outputs")
    parser.add_argument("--index", default="outputs/episodes_review_index.json")
    parser.add_argument("--remote", default=os.environ.get("RCLONE_REMOTE", "gdrive"))
    parser.add_argument("--cache-dir", default="tmp/triplet_batch_cache")
    parser.add_argument("--results-dir", default="outputs/triplet_compare")
    parser.add_argument("--results-jsonl", default="outputs/triplet_compare_results.jsonl")
    parser.add_argument("--model", default="gemini-3.1-pro-preview")
    parser.add_argument("--compare-model", default="", help="Model for triplet compare judge (defaults to --model)")
    parser.add_argument("--chat-timed-model", default="", help="Model for generating chat timed labels (defaults to --model)")
    parser.add_argument("--vertex-chat-model", default="", help="Model for generating vertex chat timed labels (defaults to --model)")
    parser.add_argument(
        "--api-update-model",
        default="",
        help="Model for regenerating missing API update from video (defaults to --model)",
    )
    parser.add_argument("--only-status", default="", help="Comma-separated review_status filter (empty = all)")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--require-chat-path", action="store_true")
    parser.add_argument("--write-chat-from-evals", dest="write_chat_from_evals", action="store_true", default=False)
    parser.add_argument("--no-write-chat-from-evals", dest="write_chat_from_evals", action="store_false")
    parser.add_argument("--generate-chat-timed-missing", dest="generate_chat_timed_missing", action="store_true", default=True)
    parser.add_argument("--no-generate-chat-timed-missing", dest="generate_chat_timed_missing", action="store_false")
    parser.add_argument("--generate-vertex-chat-missing", dest="generate_vertex_chat_missing", action="store_true", default=True)
    parser.add_argument("--no-generate-vertex-chat-missing", dest="generate_vertex_chat_missing", action="store_false")
    parser.add_argument("--regenerate-api-missing", dest="regenerate_api_missing", action="store_true", default=True)
    parser.add_argument("--no-regenerate-api-missing", dest="regenerate_api_missing", action="store_false")
    parser.add_argument("--regenerate-api-from-video", dest="regenerate_api_from_video", action="store_true", default=True)
    parser.add_argument("--no-regenerate-api-from-video", dest="regenerate_api_from_video", action="store_false")
    parser.add_argument("--allow-missing-tier2", dest="allow_missing_tier2", action="store_true", default=False)
    parser.add_argument("--no-allow-missing-tier2", dest="allow_missing_tier2", action="store_false")
    parser.add_argument("--update-evals", dest="update_evals", action="store_true", default=True)
    parser.add_argument("--no-update-evals", dest="update_evals", action="store_false")
    parser.add_argument("--overwrite-evals", action="store_true", default=False)
    parser.add_argument("--source", default="triplet_compare_batch")
    parser.add_argument("--skip-existing-results", dest="skip_existing_results", action="store_true", default=True)
    parser.add_argument("--no-skip-existing-results", dest="skip_existing_results", action="store_false")
    args = parser.parse_args()

    summary = run_batch(
        config=str(args.config),
        outputs_dir=Path(args.outputs_dir).resolve(),
        index_path=Path(args.index).resolve(),
        remote=str(args.remote),
        cache_dir=Path(args.cache_dir).resolve(),
        results_dir=Path(args.results_dir).resolve(),
        results_jsonl=Path(args.results_jsonl).resolve(),
        model=str(args.model),
        compare_model=str(args.compare_model),
        chat_timed_model=str(args.chat_timed_model),
        vertex_chat_model=str(args.vertex_chat_model),
        api_update_model=str(args.api_update_model),
        only_status=str(args.only_status),
        limit=max(0, int(args.limit)),
        require_chat_path=bool(args.require_chat_path),
        write_chat_from_evals=bool(args.write_chat_from_evals),
        generate_chat_timed_missing=bool(args.generate_chat_timed_missing),
        generate_vertex_chat_missing=bool(args.generate_vertex_chat_missing),
        regenerate_api_missing=bool(args.regenerate_api_missing),
        regenerate_api_from_video=bool(args.regenerate_api_from_video),
        allow_missing_tier2=bool(args.allow_missing_tier2),
        update_evals=bool(args.update_evals),
        overwrite_evals=bool(args.overwrite_evals),
        source=str(args.source or "triplet_compare_batch"),
        skip_existing_results=bool(args.skip_existing_results),
    )
    print(f"[triplet-batch] processed={summary['processed']} ok={summary['ok']} skipped={summary['skipped']} errors={summary['errors']}")
    print(f"[triplet-batch] eval_updated={summary['eval_updated']} eval_skipped={summary['eval_skipped']}")
    print(f"[triplet-batch] results_dir={summary['results_dir']}")
    print(f"[triplet-batch] results_jsonl={summary['results_jsonl']}")


if __name__ == "__main__":
    main()
