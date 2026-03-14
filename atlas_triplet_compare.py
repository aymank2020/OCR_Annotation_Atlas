"""
Compare 3 candidate solutions (Tier2 / Gemini API / Gemini Chat) against up to 2 videos.

Supports:
- Local file paths
- Google Drive folder-link + filename references, e.g.
  https://drive.google.com/drive/folders/<FOLDER_ID>?usp=sharing\\video_x.mp4

Auth modes:
- gemini.auth_mode: api_key   (GEMINI_API_KEY / GOOGLE_API_KEY)
- gemini.auth_mode: vertex_ai (Service Account + Vertex endpoint)
- gemini.auth_mode: chat_web  (Playwright on gemini.google.com, no API billing)
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml


def _load_dotenv(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = str(raw or "").strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            key = str(k or "").strip()
            if not key:
                continue
            out[key] = str(v or "").strip().strip('"').strip("'")
    except Exception:
        return {}
    return out


def _read_secret(name: str, dotenv: Dict[str, str]) -> str:
    env_v = str(os.environ.get(name, "") or "").strip()
    if env_v:
        return env_v
    return str(dotenv.get(name, "") or "").strip()


def _first_non_empty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _join_text_blocks(*values: Any) -> str:
    out: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if text:
            out.append(text)
    return "\n\n".join(out).strip()


def _read_optional_text_file(path_value: Any, *, base_dir: Path) -> str:
    raw = str(path_value or "").strip()
    if not raw:
        return ""
    path = Path(raw)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    if not path.exists() or not path.is_file():
        return ""
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _build_prompt_context(gem_cfg: Dict[str, Any], *, cfg_dir: Path, scope: str) -> str:
    if bool(gem_cfg.get("use_vertex_cached_context_only", False)):
        return ""
    scope_norm = str(scope or "").strip().lower()
    return _join_text_blocks(
        gem_cfg.get("context_text", ""),
        _read_optional_text_file(gem_cfg.get("context_file", ""), base_dir=cfg_dir),
        gem_cfg.get(f"{scope_norm}_context_text", ""),
        _read_optional_text_file(gem_cfg.get(f"{scope_norm}_context_file", ""), base_dir=cfg_dir),
    )


def _normalize_auth_mode(raw: str) -> str:
    mode = str(raw or "").strip().lower()
    aliases = {
        "": "api_key",
        "api": "api_key",
        "apikey": "api_key",
        "api_key": "api_key",
        "google_api_key": "api_key",
        "vertex": "vertex_ai",
        "vertexai": "vertex_ai",
        "vertex_ai": "vertex_ai",
        "service_account": "vertex_ai",
        "chat": "chat_web",
        "chat_web": "chat_web",
        "gemini_chat": "chat_web",
        "gemini_web": "chat_web",
        "playwright": "chat_web",
    }
    return aliases.get(mode, "api_key")


def _extract_folder_id(link: str) -> Optional[str]:
    src = str(link or "")
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", src)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", src)
    if m:
        return m.group(1)
    return None


def _parse_drive_folder_file_ref(ref: str) -> Optional[Tuple[str, str]]:
    src = str(ref or "").strip()
    if "drive.google.com/drive/folders/" not in src:
        return None
    folder_id = _extract_folder_id(src)
    if not folder_id:
        return None

    normalized = src.replace("\\", "/")
    last = normalized.rsplit("/", 1)[-1]
    filename = last.split("?", 1)[0].strip()
    if not filename or "." not in filename:
        return None
    return folder_id, filename


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def _download_from_drive_ref(
    ref: str,
    out_dir: Path,
    remote: str,
) -> Path:
    parsed = _parse_drive_folder_file_ref(ref)
    if not parsed:
        raise RuntimeError(f"Unsupported Drive reference format: {ref}")
    if shutil.which("rclone") is None:
        raise RuntimeError("rclone is required for Drive references but was not found in PATH.")

    folder_id, filename = parsed
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "rclone",
        "copy",
        f"{remote}:",
        str(out_dir),
        "--drive-root-folder-id",
        folder_id,
        "--include",
        filename,
        "--checkers",
        "8",
        "--transfers",
        "4",
        "--progress",
    ]
    _run(cmd)
    found = list(out_dir.rglob(filename))
    if not found:
        raise RuntimeError(f"Drive download completed but file was not found locally: {filename}")
    return found[0]


def _resolve_input_path(ref: str, cache_dir: Path, remote: str) -> Path:
    raw = str(ref or "").strip()
    if not raw:
        raise RuntimeError("Empty input reference.")
    p = Path(raw)
    if p.exists():
        return p.resolve()
    if "drive.google.com/drive/folders/" in raw:
        return _download_from_drive_ref(raw, cache_dir, remote).resolve()
    raise RuntimeError(f"Input path is not local file and not supported Drive reference: {raw}")


def _load_text_or_json(path: Path) -> str:
    if not path.exists():
        return ""
    raw = path.read_text(encoding="utf-8", errors="replace")
    if path.suffix.lower() != ".json":
        return raw
    try:
        payload = json.loads(raw)
    except Exception:
        return raw
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _compress_video_for_inline(src: Path, out_dir: Path) -> Optional[Path]:
    if shutil.which("ffmpeg") is None:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    dst = out_dir / f"{src.stem}_inline.mp4"
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(src),
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
        str(dst),
    ]
    try:
        _run(cmd)
    except Exception:
        return None
    if not dst.exists() or dst.stat().st_size <= 0:
        return None
    return dst


def _video_part_for_inline(
    path: Path,
    max_inline_mb: Optional[float] = None,
    cache_dir: Optional[Path] = None,
    **kwargs: Any,
) -> Tuple[Optional[Dict[str, Any]], str]:
    # Backward-compatible kwargs support:
    # - old callsites may pass max_mb=...
    # - newer callsites pass max_inline_mb=...
    max_mb_raw = kwargs.pop("max_mb", None)
    max_mb = float(max_mb_raw if max_mb_raw is not None else (max_inline_mb if max_inline_mb is not None else 20.0))
    if cache_dir is None:
        cache_dir = Path(tempfile.gettempdir()) / "triplet_inline_cache"
    src = path
    size_mb = src.stat().st_size / (1024 * 1024)
    if size_mb > max_mb:
        compressed = _compress_video_for_inline(src, cache_dir)
        if compressed is None:
            return None, f"{src.name}: skipped (size {size_mb:.1f} MB > {max_mb:.1f} MB and ffmpeg unavailable/failed)"
        src = compressed
        size_mb = src.stat().st_size / (1024 * 1024)
    if size_mb > max_mb:
        return None, f"{src.name}: skipped after compression ({size_mb:.1f} MB > {max_mb:.1f} MB)"
    data = base64.b64encode(src.read_bytes()).decode("ascii")
    return {"inline_data": {"mime_type": "video/mp4", "data": data}}, f"{src.name}: attached ({size_mb:.1f} MB)"


def _extract_text_from_response_json(data: Dict[str, Any]) -> str:
    for cand in data.get("candidates", []):
        content = cand.get("content", {})
        for part in content.get("parts", []):
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                return text.strip()
    return ""


def _clean_json_text(text: str) -> str:
    clean = re.sub(r"```json|```", "", str(text or ""), flags=re.IGNORECASE).strip()
    start = clean.find("{")
    end = clean.rfind("}")
    if start >= 0 and end > start:
        return clean[start : end + 1]
    return clean


def _parse_time_like_to_sec(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        sec = float(value)
        if sec >= 0:
            return sec
        return None
    src = str(value or "").strip()
    if not src:
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?", src):
        try:
            sec = float(src)
            if sec >= 0:
                return sec
        except Exception:
            return None
        return None
    parts = src.split(":")
    if len(parts) not in {2, 3}:
        return None
    nums: List[float] = []
    for part in parts:
        if not re.fullmatch(r"\d+(?:\.\d+)?", part.strip()):
            return None
        try:
            nums.append(float(part.strip()))
        except Exception:
            return None
    if len(nums) == 2:
        mm, ss = nums
        return mm * 60.0 + ss
    hh, mm, ss = nums
    return hh * 3600.0 + mm * 60.0 + ss


def _format_time_sec(value: float) -> str:
    txt = f"{float(value):.3f}".rstrip("0").rstrip(".")
    if "." not in txt:
        txt += ".0"
    return txt


def _segment_from_obj(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    start_raw = (
        item.get("start_sec")
        if isinstance(item, dict)
        else None
    )
    if start_raw is None:
        start_raw = item.get("start") if isinstance(item, dict) else None
    if start_raw is None:
        start_raw = item.get("start_time") if isinstance(item, dict) else None
    if start_raw is None:
        start_raw = item.get("from") if isinstance(item, dict) else None
    if start_raw is None:
        start_raw = item.get("t0") if isinstance(item, dict) else None

    end_raw = (
        item.get("end_sec")
        if isinstance(item, dict)
        else None
    )
    if end_raw is None:
        end_raw = item.get("end") if isinstance(item, dict) else None
    if end_raw is None:
        end_raw = item.get("end_time") if isinstance(item, dict) else None
    if end_raw is None:
        end_raw = item.get("to") if isinstance(item, dict) else None
    if end_raw is None:
        end_raw = item.get("t1") if isinstance(item, dict) else None

    label_raw: Any = ""
    if isinstance(item, dict):
        label_raw = item.get("label")
        if label_raw in (None, ""):
            label_raw = item.get("action")
        if label_raw in (None, ""):
            label_raw = item.get("text")
        if label_raw in (None, "") and isinstance(item.get("actions"), list):
            label_raw = ", ".join(str(x or "").strip() for x in item.get("actions", []) if str(x or "").strip())
    label = str(label_raw or "").strip()

    a = _parse_time_like_to_sec(start_raw)
    b = _parse_time_like_to_sec(end_raw)
    if a is None or b is None:
        return None
    if b <= a:
        return None
    return {"start_sec": a, "end_sec": b, "label": label}


def parse_timed_segments_payload(payload: Any) -> List[Dict[str, Any]]:
    obj = payload
    if isinstance(obj, dict):
        if isinstance(obj.get("segments"), list):
            obj = obj.get("segments")
        elif isinstance(obj.get("labels"), list):
            obj = obj.get("labels")
        elif isinstance(obj.get("data"), dict) and isinstance(obj.get("data", {}).get("segments"), list):
            obj = obj.get("data", {}).get("segments")
    if not isinstance(obj, list):
        return []

    out: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str, str]] = set()
    for item in obj:
        if not isinstance(item, dict):
            continue
        seg = _segment_from_obj(item)
        if not seg:
            continue
        key = (
            _format_time_sec(seg["start_sec"]),
            _format_time_sec(seg["end_sec"]),
            str(seg.get("label") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(seg)
    out.sort(key=lambda s: (float(s.get("start_sec", 0.0)), float(s.get("end_sec", 0.0))))
    return out


def parse_timed_segments_text(raw_text: str) -> List[Dict[str, Any]]:
    src = str(raw_text or "")
    if not src.strip():
        return []

    # JSON first (most reliable).
    try:
        parsed = json.loads(_clean_json_text(src))
        segs = parse_timed_segments_payload(parsed)
        if segs:
            return segs
    except Exception:
        pass

    out: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str, str]] = set()
    lines = [ln.strip() for ln in src.replace("\r", "\n").split("\n") if str(ln or "").strip()]
    for line in lines:
        # Tab-separated forms:
        # 1\t0.0\t4.2\tlabel
        # 0.0\t4.2\tlabel
        parts = [p.strip() for p in re.split(r"\t+", line) if p.strip()]
        candidates: List[Tuple[str, str, str]] = []
        if len(parts) >= 4:
            candidates.append((parts[1], parts[2], " ".join(parts[3:]).strip()))
        if len(parts) >= 3:
            candidates.append((parts[0], parts[1], " ".join(parts[2:]).strip()))

        # Markdown table-like row:
        # | 1 | 0.0 | 4.2 | label |
        pipe_parts = [p.strip() for p in line.split("|") if p.strip()]
        if len(pipe_parts) >= 4:
            candidates.append((pipe_parts[1], pipe_parts[2], " ".join(pipe_parts[3:]).strip()))
        elif len(pipe_parts) >= 3:
            candidates.append((pipe_parts[0], pipe_parts[1], " ".join(pipe_parts[2:]).strip()))

        # Time range in free text:
        # 00:00.0 -> 00:04.2 label
        m = re.search(
            r"(?P<a>\d{1,2}:\d{1,2}(?::\d{1,2})?(?:\.\d+)?)\s*(?:->|=>|[-–—])\s*(?P<b>\d{1,2}:\d{1,2}(?::\d{1,2})?(?:\.\d+)?)",
            line,
        )
        if m:
            end_idx = m.end("b")
            candidates.append((m.group("a"), m.group("b"), line[end_idx:].strip()))

        for a_raw, b_raw, label_raw in candidates:
            a = _parse_time_like_to_sec(a_raw)
            b = _parse_time_like_to_sec(b_raw)
            if a is None or b is None or b <= a:
                continue
            label = str(label_raw or "").strip()
            key = (_format_time_sec(a), _format_time_sec(b), label)
            if key in seen:
                continue
            seen.add(key)
            out.append({"start_sec": a, "end_sec": b, "label": label})
            break
    out.sort(key=lambda s: (float(s.get("start_sec", 0.0)), float(s.get("end_sec", 0.0))))
    return out


def segments_to_timed_text(segments: List[Dict[str, Any]]) -> str:
    normalized = parse_timed_segments_payload({"segments": segments})
    lines: List[str] = []
    for idx, seg in enumerate(normalized, 1):
        label = str(seg.get("label") or "").strip() or "No Action"
        lines.append(
            f"{idx}\t{_format_time_sec(float(seg['start_sec']))}\t{_format_time_sec(float(seg['end_sec']))}\t{label}"
        )
    return "\n".join(lines).strip()


def _timed_labels_response_schema() -> Dict[str, Any]:
    return {
        "type": "OBJECT",
        "properties": {
            "segments": {
                "type": "ARRAY",
                "items": {
                    "type": "OBJECT",
                    "properties": {
                        "start_sec": {"type": "NUMBER"},
                        "end_sec": {"type": "NUMBER"},
                        "label": {"type": "STRING"},
                    },
                    "required": ["start_sec", "end_sec", "label"],
                },
            }
        },
        "required": ["segments"],
    }


def _triplet_compare_response_schema(include_thought_process: bool) -> Dict[str, Any]:
    props: Dict[str, Any] = {
        "winner": {"type": "STRING", "enum": ["tier2", "api", "chat", "none"]},
        "submit_safe_solution": {"type": "STRING", "enum": ["tier2", "api", "chat", "none"]},
        "scores": {
            "type": "OBJECT",
            "properties": {
                "tier2": {"type": "INTEGER"},
                "api": {"type": "INTEGER"},
                "chat": {"type": "INTEGER"},
            },
            "required": ["tier2", "api", "chat"],
        },
        "hallucination": {
            "type": "OBJECT",
            "properties": {
                "tier2": {"type": "BOOLEAN"},
                "api": {"type": "BOOLEAN"},
                "chat": {"type": "BOOLEAN"},
            },
            "required": ["tier2", "api", "chat"],
        },
        "major_issues": {
            "type": "OBJECT",
            "properties": {
                "tier2": {"type": "ARRAY", "items": {"type": "STRING"}},
                "api": {"type": "ARRAY", "items": {"type": "STRING"}},
                "chat": {"type": "ARRAY", "items": {"type": "STRING"}},
            },
            "required": ["tier2", "api", "chat"],
        },
        "best_reason_short": {"type": "STRING"},
        "final_recommendation": {"type": "STRING"},
    }
    required = [
        "winner",
        "submit_safe_solution",
        "scores",
        "hallucination",
        "major_issues",
        "best_reason_short",
        "final_recommendation",
    ]
    if include_thought_process:
        props["thought_process"] = {"type": "STRING"}
        required.insert(0, "thought_process")
    return {"type": "OBJECT", "properties": props, "required": required}


def _validate_triplet_judge_result(parsed: Any, *, require_thought_process: bool) -> Dict[str, Any]:
    if not isinstance(parsed, dict):
        raise RuntimeError("judge_result is not a JSON object.")
    allowed = {"tier2", "api", "chat", "none"}
    out: Dict[str, Any] = {}

    thought_process = str(parsed.get("thought_process") or "").strip()
    if require_thought_process and not thought_process:
        raise RuntimeError("Missing thought_process.")
    if thought_process:
        out["thought_process"] = thought_process

    winner = str(parsed.get("winner") or "").strip().lower()
    submit_safe = str(parsed.get("submit_safe_solution") or "").strip().lower()
    if winner not in allowed:
        raise RuntimeError(f"Invalid winner={winner!r}")
    if submit_safe not in allowed:
        raise RuntimeError(f"Invalid submit_safe_solution={submit_safe!r}")
    out["winner"] = winner
    out["submit_safe_solution"] = submit_safe

    scores = parsed.get("scores")
    if not isinstance(scores, dict):
        raise RuntimeError("scores must be object.")
    score_out: Dict[str, int] = {}
    for key in ("tier2", "api", "chat"):
        raw = scores.get(key)
        if raw is None:
            raise RuntimeError(f"scores.{key} missing.")
        try:
            score_out[key] = max(0, min(100, int(float(raw))))
        except Exception as exc:
            raise RuntimeError(f"scores.{key} invalid numeric value.") from exc
    out["scores"] = score_out

    hallucination = parsed.get("hallucination")
    if not isinstance(hallucination, dict):
        raise RuntimeError("hallucination must be object.")
    out["hallucination"] = {k: bool(hallucination.get(k, False)) for k in ("tier2", "api", "chat")}

    major_issues = parsed.get("major_issues")
    if not isinstance(major_issues, dict):
        raise RuntimeError("major_issues must be object.")
    issues_out: Dict[str, List[str]] = {}
    for key in ("tier2", "api", "chat"):
        raw_list = major_issues.get(key, [])
        if raw_list is None:
            raw_list = []
        if not isinstance(raw_list, list):
            raise RuntimeError(f"major_issues.{key} must be array.")
        issues_out[key] = [str(v).strip() for v in raw_list if str(v).strip()]
    out["major_issues"] = issues_out

    out["best_reason_short"] = str(parsed.get("best_reason_short") or "").strip()
    out["final_recommendation"] = str(parsed.get("final_recommendation") or "").strip()
    return out


def _translate_payload_for_vertex(value: Any) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            mapped = key
            if key == "inline_data":
                mapped = "inlineData"
            elif key == "mime_type":
                mapped = "mimeType"
            out[mapped] = _translate_payload_for_vertex(item)
        return out
    if isinstance(value, list):
        return [_translate_payload_for_vertex(item) for item in value]
    return value


def _selector_variants(expr: str) -> List[str]:
    out: List[str] = []
    for part in str(expr or "").split("||"):
        candidate = str(part or "").strip()
        if candidate:
            out.append(candidate)
    return out


def _first_visible_locator(page: Any, selector_expr: str, timeout_ms: int = 15000) -> Optional[Any]:
    variants = _selector_variants(selector_expr)
    if not variants:
        return None
    per_variant = max(500, int(timeout_ms / max(1, len(variants))))
    for sel in variants:
        try:
            loc = page.locator(sel).first
            loc.wait_for(state="visible", timeout=per_variant)
            return loc
        except Exception:
            continue
    return None


def _fill_chat_input(input_box: Any, text: str, page: Any) -> None:
    payload = str(text or "")
    try:
        input_box.fill(payload)
        return
    except Exception:
        pass
    try:
        input_box.click()
        page.keyboard.press("Control+A")
        page.keyboard.type(payload)
        return
    except Exception:
        pass
    input_box.click()
    page.keyboard.type(payload)


def _extract_latest_chat_response_text(page: Any) -> str:
    selectors = [
        "message-content",
        "model-response",
        "div.markdown",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel)
            count = loc.count()
        except Exception:
            continue
        if count <= 0:
            continue
        for idx in range(count - 1, -1, -1):
            try:
                txt = str(loc.nth(idx).inner_text() or "").strip()
            except Exception:
                continue
            if txt:
                return txt
    return ""


def _wait_for_new_chat_response_text(page: Any, baseline_text: str, timeout_sec: float) -> str:
    deadline = time.time() + max(10.0, float(timeout_sec))
    last_text = ""
    stable_count = 0
    while time.time() < deadline:
        try:
            txt = _extract_latest_chat_response_text(page)
        except Exception:
            txt = ""
        if txt and txt != baseline_text:
            if txt == last_text:
                stable_count += 1
            else:
                stable_count = 0
                last_text = txt
            # Wait a bit for streaming to settle.
            if stable_count >= 2:
                return txt
        try:
            page.wait_for_timeout(1000)
        except Exception:
            time.sleep(1.0)
    return last_text if last_text and last_text != baseline_text else ""


def _call_gemini_compare_chat_web(
    *,
    cfg: Dict[str, Any],
    prompt: str,
    video_a: Optional[Path],
    video_b: Optional[Path],
) -> Dict[str, Any]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise RuntimeError("Playwright is required for chat_web mode.") from exc

    gem = cfg.get("gemini", {}) if isinstance(cfg.get("gemini"), dict) else {}
    chat_url = str(gem.get("chat_web_url", "https://gemini.google.com/app") or "").strip() or "https://gemini.google.com/app"
    headless = bool(gem.get("chat_web_headless", True))
    timeout_sec = max(20.0, float(gem.get("chat_web_timeout_sec", 180) or 180))
    max_upload_mb = max(50.0, float(gem.get("chat_web_max_upload_mb", 2048) or 2048))
    attach_secondary = bool(gem.get("chat_web_attach_secondary_video", False))
    input_sel = str(gem.get("chat_web_input_selector", 'div[contenteditable="true"] || textarea') or "").strip()
    send_sel = str(
        gem.get(
            "chat_web_send_selector",
            'button[aria-label*="Send" i] || button:has-text("Send") || button:has-text("Run")',
        )
        or ""
    ).strip()
    file_input_sel = str(gem.get("chat_web_file_input_selector", 'input[type="file"]') or "").strip()
    attach_button_sel = str(
        gem.get(
            "chat_web_attach_button_selector",
            'button[aria-label*="Add files" i] || button[aria-label*="Upload" i] || button:has-text("Add files") || button:has-text("Upload")',
        )
        or ""
    ).strip()
    channel = str(gem.get("chat_web_channel", "") or "").strip()
    storage_state = str(gem.get("chat_web_storage_state", "") or "").strip()
    user_data_dir = str(gem.get("chat_web_user_data_dir", "") or "").strip()
    raw_args = gem.get("chat_web_launch_args", [])
    launch_args: List[str] = []
    if isinstance(raw_args, list):
        for item in raw_args:
            val = str(item or "").strip()
            if val:
                launch_args.append(val)
    # Running browser as root on Linux requires --no-sandbox.
    try:
        if hasattr(os, "geteuid") and int(os.geteuid()) == 0:
            if "--no-sandbox" not in launch_args:
                launch_args.append("--no-sandbox")
            if "--disable-dev-shm-usage" not in launch_args:
                launch_args.append("--disable-dev-shm-usage")
    except Exception:
        pass

    attach_candidates: List[Path] = []
    if video_a is not None and video_a.exists():
        attach_candidates.append(video_a)
    if attach_secondary and video_b is not None and video_b.exists():
        attach_candidates.append(video_b)

    attach_notes: List[str] = []
    raw_text = ""

    with sync_playwright() as pw:
        context = None
        browser = None
        page = None
        try:
            if user_data_dir:
                launch_kwargs: Dict[str, Any] = {
                    "user_data_dir": user_data_dir,
                    "headless": headless,
                }
                if channel:
                    launch_kwargs["channel"] = channel
                if launch_args:
                    launch_kwargs["args"] = launch_args
                context = pw.chromium.launch_persistent_context(**launch_kwargs)
            else:
                launch_kwargs = {"headless": headless}
                if channel:
                    launch_kwargs["channel"] = channel
                if launch_args:
                    launch_kwargs["args"] = launch_args
                browser = pw.chromium.launch(**launch_kwargs)
                context_kwargs: Dict[str, Any] = {}
                if storage_state and Path(storage_state).exists():
                    context_kwargs["storage_state"] = storage_state
                context = browser.new_context(**context_kwargs)

            page = context.new_page()
            page.goto(chat_url, wait_until="domcontentloaded", timeout=60000)

            chat_box = _first_visible_locator(page, input_sel, timeout_ms=30000)
            if chat_box is None:
                raise RuntimeError("Gemini chat input not visible. Login/session is likely missing.")

            if attach_button_sel:
                btn = _first_visible_locator(page, attach_button_sel, timeout_ms=3000)
                if btn is not None:
                    try:
                        btn.click(timeout=2000)
                        page.wait_for_timeout(400)
                    except Exception:
                        pass

            file_input = _first_visible_locator(page, file_input_sel, timeout_ms=5000)
            for vid in attach_candidates:
                size_mb = float(vid.stat().st_size) / (1024 * 1024)
                if size_mb > max_upload_mb:
                    attach_notes.append(f"{vid.name}: skipped ({size_mb:.1f} MB > {max_upload_mb:.1f} MB)")
                    continue
                if file_input is None:
                    attach_notes.append(f"{vid.name}: skipped (file input not found)")
                    continue
                try:
                    file_input.set_input_files(str(vid))
                    page.wait_for_timeout(1500)
                    attach_notes.append(f"{vid.name}: attached ({size_mb:.1f} MB)")
                except Exception as exc:
                    attach_notes.append(f"{vid.name}: attach_failed ({exc})")

            baseline_text = _extract_latest_chat_response_text(page)
            _fill_chat_input(chat_box, prompt, page)

            sent = False
            try:
                chat_box.press("Enter", timeout=1200)
                sent = True
            except Exception:
                sent = False
            if not sent:
                try:
                    page.keyboard.press("Enter")
                    sent = True
                except Exception:
                    sent = False
            if not sent and send_sel:
                send_btn = _first_visible_locator(page, send_sel, timeout_ms=2500)
                if send_btn is not None:
                    try:
                        send_btn.click(timeout=1500)
                        sent = True
                    except Exception:
                        sent = False
            if not sent:
                raise RuntimeError("Could not send prompt in Gemini chat (Enter/send button failed).")

            raw_text = _wait_for_new_chat_response_text(page, baseline_text=baseline_text, timeout_sec=timeout_sec)
            if not raw_text:
                raise RuntimeError("Timed out waiting for Gemini chat response.")
        finally:
            try:
                if page is not None:
                    page.close()
            except Exception:
                pass
            try:
                if context is not None:
                    context.close()
            except Exception:
                pass
            if browser is not None:
                try:
                    browser.close()
                except Exception:
                    pass

    try:
        parsed = json.loads(_clean_json_text(raw_text))
    except Exception:
        parsed = {"raw_text": raw_text}
    return {
        "parsed": parsed,
        "raw_text": raw_text,
        "attach_notes": attach_notes,
        "usage": {},
    }


def _vertex_access_token(credentials_path: Path) -> str:
    try:
        from google.auth.transport.requests import Request as GoogleAuthRequest
        from google.oauth2 import service_account
    except Exception as exc:
        raise RuntimeError("google-auth package is required for vertex_ai mode.") from exc

    creds = service_account.Credentials.from_service_account_file(
        str(credentials_path),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    req = GoogleAuthRequest()
    creds.refresh(req)
    token = str(getattr(creds, "token", "") or "").strip()
    if not token:
        raise RuntimeError("Could not obtain Vertex access token.")
    return token


def _call_gemini_compare(
    cfg: Dict[str, Any],
    dotenv: Dict[str, str],
    model: str,
    prompt: str,
    video_a: Optional[Path],
    video_b: Optional[Path],
    cache_dir: Path,
    response_schema: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    gem = cfg.get("gemini", {}) if isinstance(cfg.get("gemini"), dict) else {}
    auth_mode = _normalize_auth_mode(gem.get("auth_mode", "api_key"))
    vertex_cached_content_name = str(
        gem.get("vertex_cached_content_name", "")
        or _read_secret("VERTEX_CACHED_CONTENT_NAME", dotenv)
    ).strip()
    if auth_mode == "chat_web":
        return _call_gemini_compare_chat_web(
            cfg=cfg,
            prompt=prompt,
            video_a=video_a,
            video_b=video_b,
        )

    max_inline_mb = float(gem.get("max_inline_video_mb", 20.0) or 20.0)
    connect_timeout_sec = max(5, int(gem.get("connect_timeout_sec", 30) or 30))
    request_timeout_sec = max(30, int(gem.get("request_timeout_sec", 420) or 420))

    parts: list[Dict[str, Any]] = [{"text": prompt}]
    attach_notes: list[str] = []
    for vid in [video_a, video_b]:
        if vid is None:
            continue
        part, note = _video_part_for_inline(vid, max_inline_mb=max_inline_mb, cache_dir=cache_dir)
        attach_notes.append(note)
        if part is not None:
            parts.append(part)

    generation_cfg = {
        "temperature": float(gem.get("temperature", 0.0) or 0.0),
        "responseMimeType": "application/json",
        "candidateCount": 1,
    }
    if isinstance(response_schema, dict) and response_schema:
        generation_cfg["responseSchema"] = response_schema
    payload: Dict[str, Any] = {
        "contents": [{"role": "user", "parts": parts}],
        "generationConfig": generation_cfg,
    }

    system_instruction = str(gem.get("system_instruction_text", "") or "").strip()
    if system_instruction:
        payload["systemInstruction"] = {"parts": [{"text": system_instruction}]}

    headers: Dict[str, str]
    url: str
    payload_to_send: Dict[str, Any] = payload

    if auth_mode == "vertex_ai":
        project = str(gem.get("vertex_project", "") or "").strip() or _read_secret(
            "GOOGLE_CLOUD_PROJECT", dotenv
        )
        location = str(gem.get("vertex_location", "") or "").strip() or _read_secret(
            "GOOGLE_CLOUD_LOCATION", dotenv
        ) or "us-central1"
        cred_path_raw = str(gem.get("vertex_credentials_path", "") or "").strip() or _read_secret(
            "GOOGLE_APPLICATION_CREDENTIALS", dotenv
        )
        if not project:
            raise RuntimeError("Missing Vertex project (gemini.vertex_project / GOOGLE_CLOUD_PROJECT).")
        if not cred_path_raw:
            raise RuntimeError(
                "Missing Vertex credentials path (gemini.vertex_credentials_path / GOOGLE_APPLICATION_CREDENTIALS)."
            )
        cred_path = Path(cred_path_raw)
        if not cred_path.exists():
            raise RuntimeError(f"Vertex credentials file not found: {cred_path}")
        token = _vertex_access_token(cred_path)
        model_path = model if "/" in model else f"publishers/google/models/{model}"
        url = (
            f"https://{location}-aiplatform.googleapis.com/v1/projects/{project}/locations/{location}/"
            f"{model_path}:generateContent"
        )
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
        payload_to_send = _translate_payload_for_vertex(payload)
        if vertex_cached_content_name:
            payload_to_send["cachedContent"] = vertex_cached_content_name
            if bool(gem.get("vertex_cached_content_strip_system_instruction", True)):
                payload_to_send.pop("systemInstruction", None)
    else:
        api_key = str(gem.get("api_key", "") or "").strip()
        if not api_key:
            api_key = _read_secret("GEMINI_API_KEY", dotenv) or _read_secret("GOOGLE_API_KEY", dotenv)
        if not api_key:
            raise RuntimeError("Missing Gemini API key (GEMINI_API_KEY / GOOGLE_API_KEY).")
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        headers = {"Content-Type": "application/json", "X-goog-api-key": api_key}

    resp = requests.post(
        url,
        headers=headers,
        json=payload_to_send,
        timeout=(connect_timeout_sec, request_timeout_sec),
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Gemini compare request failed HTTP {resp.status_code}: {resp.text[:800]}")
    data = resp.json()
    text = _extract_text_from_response_json(data)
    if not text:
        raise RuntimeError("Gemini compare returned empty text response.")
    try:
        parsed = json.loads(_clean_json_text(text))
    except Exception:
        parsed = {"raw_text": text}
    return {
        "parsed": parsed,
        "raw_text": text,
        "attach_notes": attach_notes,
        "usage": data.get("usageMetadata", {}) if isinstance(data, dict) else {},
    }


def _build_timed_labels_prompt(episode_id: str = "", context_text: str = "") -> str:
    eid = str(episode_id or "").strip()
    eid_line = f"Episode ID: {eid}\n" if eid else ""
    context_block = ""
    context_clean = str(context_text or "").strip()
    if context_clean:
        context_block = f"""
6) Apply project-specific context below exactly:
[Project Context]
{context_clean}
""".strip()
    return f"""
You are generating Atlas timed action labels from the attached video.
{eid_line}Rules:
1) Output ONLY valid JSON (no markdown, no commentary).
2) JSON schema:
{{
  "segments": [
    {{"start_sec": 0.0, "end_sec": 1.2, "label": "action 1, action 2"}}
  ]
}}
3) Keep timestamps in seconds.
4) Ensure segments are chronological and non-overlapping.
5) Use "No Action" only when there is clearly no relevant action.
{context_block}
""".strip()


def _build_triplet_compare_prompt(
    *,
    tier2_text: str,
    api_text: str,
    chat_text: str,
    task_state_text: str,
    context_text: str = "",
    include_thought_process: bool = True,
) -> str:
    context_block = ""
    context_clean = str(context_text or "").strip()
    if context_clean:
        context_block = f"""
[Project Context]
{context_clean}
""".strip()
    thought_line = '  "thought_process": "short internal analysis before final verdict",' if include_thought_process else ""
    return f"""
You are a strict Atlas annotation QA judge.
Use attached videos as source of truth.
If OCR text has minor typo but refers to same physical object, prioritize physical consistency and note typo in major_issues.

Compare exactly 3 candidate solutions:
1) Tier2 (employee draft)
2) Gemini API (3.1 pro style)
3) Gemini Chat (3.1 pro style)

Decide which solution is best and safest (least hallucination).
If all are bad, choose "none".

Return ONLY valid JSON with this shape:
{{
{thought_line}
  "winner": "tier2|api|chat|none",
  "submit_safe_solution": "tier2|api|chat|none",
  "scores": {{"tier2": 0, "api": 0, "chat": 0}},
  "hallucination": {{"tier2": false, "api": false, "chat": false}},
  "major_issues": {{
    "tier2": [],
    "api": [],
    "chat": []
  }},
  "best_reason_short": "",
  "final_recommendation": ""
}}

{context_block}

[Tier2]
{tier2_text}

[Gemini API]
{api_text}

[Gemini Chat]
{chat_text}

[Task State Optional]
{task_state_text}
""".strip()


def generate_gemini_chat_timed_labels(
    *,
    config_path: str,
    video_path: str,
    video_path_limit: str = "",
    remote: str = "",
    cache_dir: str = "tmp/triplet_chat_labels_cache",
    model: str = "",
    out_txt: str = "",
    out_json: str = "",
    episode_id: str = "",
    auth_mode_override: str = "",
    prompt_scope: str = "timed_labels",
) -> Dict[str, Any]:
    cfg_path = Path(config_path)
    if not cfg_path.exists():
        raise RuntimeError(f"Config file not found: {cfg_path}")
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    if not isinstance(cfg, dict):
        raise RuntimeError("Config root must be a YAML object.")
    cfg_dir = cfg_path.parent.resolve()

    dotenv = _load_dotenv(Path(".env"))
    resolved_remote = str(remote or os.environ.get("RCLONE_REMOTE", "gdrive")).strip() or "gdrive"
    cache_root = Path(cache_dir).resolve()
    cache_root.mkdir(parents=True, exist_ok=True)

    video_main = _resolve_input_path(video_path, cache_root / "inputs", resolved_remote)
    video_limit: Optional[Path] = None
    if str(video_path_limit or "").strip():
        video_limit = _resolve_input_path(video_path_limit, cache_root / "inputs", resolved_remote)

    gem_cfg = cfg.get("gemini", {}) if isinstance(cfg.get("gemini"), dict) else {}
    scope = str(prompt_scope or "timed_labels").strip().lower() or "timed_labels"
    prompt_context = _build_prompt_context(gem_cfg, cfg_dir=cfg_dir, scope=scope)
    prompt = _build_timed_labels_prompt(episode_id=episode_id, context_text=prompt_context)

    selected_model = _first_non_empty(
        model,
        gem_cfg.get("chat_timed_model", ""),
        gem_cfg.get("timed_labels_model", ""),
        gem_cfg.get("model", "gemini-3.1-pro-preview"),
    )
    fallback_model = _first_non_empty(
        gem_cfg.get("chat_timed_fallback_model", ""),
        gem_cfg.get("timed_labels_fallback_model", ""),
        gem_cfg.get("triplet_fallback_model", "gemini-2.5-pro"),
    )

    cfg_for_call = dict(cfg)
    cfg_gem = dict(gem_cfg) if isinstance(gem_cfg, dict) else {}
    timed_temp = gem_cfg.get(f"{scope}_temperature", None)
    if timed_temp is None:
        timed_temp = gem_cfg.get("timed_labels_temperature", gem_cfg.get("chat_timed_temperature", None))
    if timed_temp is not None:
        cfg_gem["temperature"] = timed_temp
    scope_auth_mode = _first_non_empty(
        auth_mode_override,
        gem_cfg.get(f"{scope}_auth_mode", ""),
        gem_cfg.get("auth_mode", ""),
    )
    if scope_auth_mode:
        cfg_gem["auth_mode"] = scope_auth_mode
    timed_system_instruction = _first_non_empty(
        gem_cfg.get(f"{scope}_system_instruction_text", ""),
        gem_cfg.get("timed_labels_system_instruction_text", ""),
        gem_cfg.get("chat_timed_system_instruction_text", ""),
    )
    if timed_system_instruction:
        cfg_gem["system_instruction_text"] = timed_system_instruction
    # For chat_web mode: allow secondary attach so upload_opt can be used automatically.
    cfg_gem["chat_web_attach_secondary_video"] = True
    cfg_for_call["gemini"] = cfg_gem
    timed_schema_enabled = bool(
        gem_cfg.get(f"{scope}_response_schema_enabled", gem_cfg.get("timed_labels_response_schema_enabled", True))
    )
    timed_response_schema = _timed_labels_response_schema() if timed_schema_enabled else None

    model_candidates = [selected_model] if selected_model else []
    if fallback_model and fallback_model not in model_candidates:
        model_candidates.append(fallback_model)
    if not model_candidates:
        model_candidates = ["gemini-3.1-pro-preview"]

    result: Optional[Dict[str, Any]] = None
    used_model = model_candidates[0]
    attempt_errors: List[str] = []
    run_notes: List[str] = []

    for model_name in model_candidates:
        try:
            result = _call_gemini_compare(
                cfg=cfg_for_call,
                dotenv=dotenv,
                model=model_name,
                prompt=prompt,
                video_a=video_main,
                video_b=video_limit,
                cache_dir=cache_root / "video_inline",
                response_schema=timed_response_schema,
            )
            used_model = model_name
            notes = result.get("attach_notes", [])
            attached_any = False
            if isinstance(notes, list):
                attached_any = any("attached" in str(n or "").lower() for n in notes)
            # If no video was attached, retry once using upload_opt only (if available).
            if not attached_any and video_limit is not None:
                retry = _call_gemini_compare(
                    cfg=cfg_for_call,
                    dotenv=dotenv,
                    model=model_name,
                    prompt=prompt,
                    video_a=video_limit,
                    video_b=None,
                    cache_dir=cache_root / "video_inline",
                    response_schema=timed_response_schema,
                )
                retry_notes = retry.get("attach_notes", [])
                if isinstance(retry_notes, list):
                    retry_notes = [*retry_notes, "retry_with_upload_opt_video"]
                else:
                    retry_notes = ["retry_with_upload_opt_video"]
                retry["attach_notes"] = retry_notes
                result = retry
            break
        except Exception as exc:
            attempt_errors.append(f"{model_name}: {exc}")
            if video_limit is not None:
                try:
                    retry = _call_gemini_compare(
                        cfg=cfg_for_call,
                        dotenv=dotenv,
                        model=model_name,
                        prompt=prompt,
                        video_a=video_limit,
                        video_b=None,
                        cache_dir=cache_root / "video_inline",
                        response_schema=timed_response_schema,
                    )
                    retry_notes = retry.get("attach_notes", [])
                    if isinstance(retry_notes, list):
                        retry_notes = [*retry_notes, "retry_after_error_with_upload_opt_video"]
                    else:
                        retry_notes = ["retry_after_error_with_upload_opt_video"]
                    retry["attach_notes"] = retry_notes
                    result = retry
                    used_model = model_name
                    run_notes.append("fallback_to_upload_opt_after_error")
                    break
                except Exception as retry_exc:
                    attempt_errors.append(f"{model_name}/upload_opt: {retry_exc}")
                    continue
            continue

    if result is None:
        tail = " | ".join(attempt_errors[-4:]) if attempt_errors else "unknown timed labels failure"
        raise RuntimeError(tail)

    parsed = result.get("parsed", {})
    raw_text = str(result.get("raw_text") or "")
    segments = parse_timed_segments_payload(parsed)
    if not segments:
        segments = parse_timed_segments_text(raw_text)
    if not segments:
        raise RuntimeError("Gemini timed labels response did not contain parseable segments.")
    timed_text = segments_to_timed_text(segments)
    if not timed_text:
        raise RuntimeError("Gemini timed labels were parsed but empty after normalization.")

    out_txt_path = Path(out_txt).resolve() if str(out_txt or "").strip() else cache_root / "text_chat_generated.txt"
    out_txt_path.parent.mkdir(parents=True, exist_ok=True)
    out_txt_path.write_text(timed_text + "\n", encoding="utf-8")

    out_json_path: Optional[Path] = None
    if str(out_json or "").strip():
        out_json_path = Path(out_json).resolve()
        out_json_path.parent.mkdir(parents=True, exist_ok=True)
        out_json_path.write_text(
            json.dumps(
                {
                    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "episode_id": str(episode_id or "").strip().lower(),
                    "model": used_model,
                    "segments": segments,
                    "attach_notes": [*(result.get("attach_notes", []) if isinstance(result.get("attach_notes"), list) else []), *run_notes],
                    "raw_text": raw_text,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    return {
        "episode_id": str(episode_id or "").strip().lower(),
        "model": used_model,
        "segment_count": len(segments),
        "out_txt": str(out_txt_path),
        "out_json": str(out_json_path) if out_json_path else "",
        "attach_notes": [*(result.get("attach_notes", []) if isinstance(result.get("attach_notes"), list) else []), *run_notes],
    }


def run_triplet_compare(
    *,
    config_path: str,
    video_path: str,
    tier2_path: str,
    api_path: str,
    video_path_limit: str = "",
    chat_path: str = "",
    task_state_path: str = "",
    labels_path: str = "",
    remote: str = "",
    cache_dir: str = "tmp/triplet_compare_cache",
    model: str = "",
    out: str = "outputs/triplet_compare_result.json",
) -> Dict[str, Any]:
    cfg_path = Path(config_path)
    if not cfg_path.exists():
        raise RuntimeError(f"Config file not found: {cfg_path}")
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    if not isinstance(cfg, dict):
        raise RuntimeError("Config root must be a YAML object.")
    cfg_dir = cfg_path.parent.resolve()

    dotenv = _load_dotenv(Path(".env"))
    resolved_remote = str(remote or os.environ.get("RCLONE_REMOTE", "gdrive")).strip() or "gdrive"
    cache_root = Path(cache_dir).resolve()
    cache_root.mkdir(parents=True, exist_ok=True)

    video_main = _resolve_input_path(video_path, cache_root / "inputs", resolved_remote)
    video_limit = (
        _resolve_input_path(video_path_limit, cache_root / "inputs", resolved_remote)
        if str(video_path_limit or "").strip()
        else None
    )

    tier2_file = _resolve_input_path(tier2_path, cache_root / "inputs", resolved_remote)
    api_file = _resolve_input_path(api_path, cache_root / "inputs", resolved_remote)

    chat_file: Optional[Path] = None
    if str(chat_path or "").strip():
        chat_file = _resolve_input_path(chat_path, cache_root / "inputs", resolved_remote)
    elif str(labels_path or "").strip():
        chat_file = _resolve_input_path(labels_path, cache_root / "inputs", resolved_remote)

    task_state_file: Optional[Path] = None
    if str(task_state_path or "").strip():
        task_state_file = _resolve_input_path(task_state_path, cache_root / "inputs", resolved_remote)

    tier2_text = _load_text_or_json(tier2_file)
    api_text = _load_text_or_json(api_file)
    chat_text = _load_text_or_json(chat_file) if chat_file else ""
    task_state_text = _load_text_or_json(task_state_file) if task_state_file else ""

    gem_cfg = cfg.get("gemini", {}) if isinstance(cfg.get("gemini"), dict) else {}
    selected_model = _first_non_empty(
        model,
        gem_cfg.get("compare_model", ""),
        gem_cfg.get("triplet_compare_model", ""),
        gem_cfg.get("model", "gemini-3.1-pro-preview"),
    )
    fallback_model = _first_non_empty(
        gem_cfg.get("compare_fallback_model", ""),
        gem_cfg.get("triplet_compare_fallback_model", ""),
        gem_cfg.get("triplet_fallback_model", "gemini-2.5-pro"),
    )
    retry_attempts = max(1, int(gem_cfg.get("triplet_retry_attempts", 3) or 3))
    include_thought_process = bool(gem_cfg.get("compare_include_thought_process", True))
    compare_schema_enabled = bool(gem_cfg.get("compare_response_schema_enabled", True))
    compare_fail_on_none = bool(gem_cfg.get("compare_fail_on_none", False))
    prompt_context = _build_prompt_context(gem_cfg, cfg_dir=cfg_dir, scope="compare")
    prompt = _build_triplet_compare_prompt(
        tier2_text=tier2_text,
        api_text=api_text,
        chat_text=chat_text,
        task_state_text=task_state_text,
        context_text=prompt_context,
        include_thought_process=include_thought_process,
    )

    cfg_for_call = dict(cfg)
    cfg_gem = dict(gem_cfg) if isinstance(gem_cfg, dict) else {}
    compare_temp = gem_cfg.get("compare_temperature", gem_cfg.get("triplet_compare_temperature", None))
    if compare_temp is not None:
        cfg_gem["temperature"] = compare_temp
    compare_auth_mode = _first_non_empty(
        gem_cfg.get("compare_auth_mode", ""),
        gem_cfg.get("auth_mode", ""),
    )
    if compare_auth_mode:
        cfg_gem["auth_mode"] = compare_auth_mode
    compare_system_instruction = _first_non_empty(
        gem_cfg.get("compare_system_instruction_text", ""),
        gem_cfg.get("triplet_compare_system_instruction_text", ""),
    )
    if compare_system_instruction:
        cfg_gem["system_instruction_text"] = compare_system_instruction
    cfg_for_call["gemini"] = cfg_gem
    compare_response_schema = (
        _triplet_compare_response_schema(include_thought_process=include_thought_process)
        if compare_schema_enabled
        else None
    )

    # Robust execution strategy:
    # 1) retry transient HTTP failures
    # 2) retry without video when request is too large / malformed
    # 3) fallback to a stable model if requested model is unavailable
    model_candidates = [selected_model]
    if fallback_model and fallback_model not in model_candidates:
        model_candidates.append(fallback_model)

    run_notes: list[str] = []
    attempt_errors: list[str] = []
    result: Optional[Dict[str, Any]] = None
    used_model = selected_model

    for model_name in model_candidates:
        use_video = True
        for attempt in range(1, retry_attempts + 1):
            try:
                current_video_a = video_main if use_video else None
                current_video_b = video_limit if use_video else None
                result = _call_gemini_compare(
                    cfg=cfg_for_call,
                    dotenv=dotenv,
                    model=model_name,
                    prompt=prompt,
                    video_a=current_video_a,
                    video_b=current_video_b,
                    cache_dir=cache_root / "video_inline",
                    response_schema=compare_response_schema,
                )
                used_model = model_name
                if not use_video:
                    run_notes.append("retried_without_video")
                break
            except Exception as exc:
                msg = str(exc)
                low = msg.lower()
                attempt_errors.append(f"{model_name}#{attempt}: {msg}")
                is_transient = any(t in low for t in ("http 429", "http 500", "http 502", "http 503", "http 504", "timeout", "timed out"))
                is_size_or_payload = any(
                    t in low
                    for t in (
                        "http 400",
                        "http 413",
                        "payload",
                        "request too large",
                        "request entity too large",
                        "inline_data",
                        "inlinedata",
                        "content size",
                    )
                )
                is_model_issue = ("http 404" in low) or (
                    "model" in low and any(t in low for t in ("not found", "unsupported", "unavailable"))
                )

                if use_video and is_size_or_payload:
                    use_video = False
                    run_notes.append(f"retry_no_video_after_error: {msg[:160]}")
                    continue

                if is_transient and attempt < retry_attempts:
                    sleep_sec = min(20, 2 ** attempt)
                    run_notes.append(f"transient_retry_{attempt}_sleep_{sleep_sec}s")
                    time.sleep(sleep_sec)
                    continue

                if is_model_issue:
                    run_notes.append(f"model_issue_on_{model_name}")
                break
        if result is not None:
            break

    if result is None:
        tail = " | ".join(attempt_errors[-4:]) if attempt_errors else "unknown compare failure"
        raise RuntimeError(tail)

    if run_notes:
        existing = result.get("attach_notes", [])
        if not isinstance(existing, list):
            existing = []
        result["attach_notes"] = [*existing, *run_notes]
    judge_valid = _validate_triplet_judge_result(
        result.get("parsed", {}),
        require_thought_process=include_thought_process,
    )
    if compare_fail_on_none and str(judge_valid.get("winner") or "") == "none":
        raise RuntimeError("judge_result winner=none and compare_fail_on_none=true")

    out_path = Path(out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "model": used_model,
        "video_refs": {
            "video_path": video_path,
            "video_path_limit": video_path_limit,
            "resolved_video_path": str(video_main),
            "resolved_video_path_limit": str(video_limit) if video_limit else "",
        },
        "text_refs": {
            "tier2_path": tier2_path,
            "api_path": api_path,
            "chat_path": chat_path,
            "labels_path": labels_path,
            "task_state_path": task_state_path,
            "resolved_tier2_path": str(tier2_file),
            "resolved_api_path": str(api_file),
            "resolved_chat_path": str(chat_file) if chat_file else "",
            "resolved_task_state_path": str(task_state_file) if task_state_file else "",
        },
        "attach_notes": result.get("attach_notes", []),
        "judge_result": judge_valid,
        "judge_raw_text": result.get("raw_text", ""),
        "usage": result.get("usage", {}),
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    payload["output_path"] = str(out_path)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Triplet compare: Tier2 vs Gemini API vs Gemini Chat")
    parser.add_argument("--config", default="sample_web_auto_solver.yaml")
    parser.add_argument("--video-path", required=True, help="Local path or Drive folder-link+filename reference")
    parser.add_argument("--video-path-limit", default="", help="Second video path (optimized)")
    parser.add_argument("--tier2-path", required=True, help="Tier2 text/json path reference")
    parser.add_argument("--api-path", required=True, help="Gemini API output text/json path reference")
    parser.add_argument("--chat-path", default="", help="Gemini Chat output text/json path reference")
    parser.add_argument("--task-state-path", default="", help="Optional task_state JSON reference")
    parser.add_argument("--labels-path", default="", help="Optional labels JSON reference (used as chat fallback)")
    parser.add_argument("--remote", default=os.environ.get("RCLONE_REMOTE", "gdrive"))
    parser.add_argument("--cache-dir", default="tmp/triplet_compare_cache")
    parser.add_argument("--model", default="gemini-3.1-pro-preview")
    parser.add_argument("--out", default="outputs/triplet_compare_result.json")
    args = parser.parse_args()

    payload = run_triplet_compare(
        config_path=args.config,
        video_path=args.video_path,
        video_path_limit=args.video_path_limit,
        tier2_path=args.tier2_path,
        api_path=args.api_path,
        chat_path=args.chat_path,
        task_state_path=args.task_state_path,
        labels_path=args.labels_path,
        remote=args.remote,
        cache_dir=args.cache_dir,
        model=args.model,
        out=args.out,
    )

    judge = payload.get("judge_result", {})
    winner = ""
    if isinstance(judge, dict):
        winner = str(judge.get("winner", "") or "").strip()
    print(f"[triplet-compare] winner: {winner or 'unknown'}")
    print(f"[triplet-compare] output: {payload.get('output_path', '')}")


if __name__ == "__main__":
    main()
