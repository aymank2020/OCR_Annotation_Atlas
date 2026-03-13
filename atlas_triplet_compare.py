"""
Compare 3 candidate solutions (Tier2 / Gemini API / Gemini Chat) against up to 2 videos.

Supports:
- Local file paths
- Google Drive folder-link + filename references, e.g.
  https://drive.google.com/drive/folders/<FOLDER_ID>?usp=sharing\\video_x.mp4

Auth modes:
- gemini.auth_mode: api_key   (GEMINI_API_KEY / GOOGLE_API_KEY)
- gemini.auth_mode: vertex_ai (Service Account + Vertex endpoint)
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

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


def _video_part_for_inline(path: Path, max_mb: float, cache_dir: Path) -> Tuple[Optional[Dict[str, Any]], str]:
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
) -> Dict[str, Any]:
    gem = cfg.get("gemini", {}) if isinstance(cfg.get("gemini"), dict) else {}
    auth_mode = _normalize_auth_mode(gem.get("auth_mode", "api_key"))
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

    selected_model = str(model or "").strip() or str(
        ((cfg.get("gemini", {}) if isinstance(cfg.get("gemini"), dict) else {}).get("model", "gemini-3.1-pro-preview"))
    ).strip()

    prompt = f"""
You are a strict Atlas annotation QA judge.
Use attached videos as source of truth.

Compare exactly 3 candidate solutions:
1) Tier2 (employee draft)
2) Gemini API (3.1 pro style)
3) Gemini Chat (3.1 pro style)

Decide which solution is best and safest (least hallucination).
If all are bad, choose "none".

Return ONLY valid JSON with this shape:
{{
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

[Tier2]
{tier2_text}

[Gemini API]
{api_text}

[Gemini Chat]
{chat_text}

[Task State Optional]
{task_state_text}
""".strip()

    result = _call_gemini_compare(
        cfg=cfg,
        dotenv=dotenv,
        model=selected_model,
        prompt=prompt,
        video_a=video_main,
        video_b=video_limit,
        cache_dir=cache_root / "video_inline",
    )

    out_path = Path(out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "model": selected_model,
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
        "judge_result": result.get("parsed", {}),
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
