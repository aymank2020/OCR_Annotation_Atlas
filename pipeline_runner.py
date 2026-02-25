"""
Atlas multi-pass pipeline runner.

Stages:
1) Candidate generation (file / claude_vision / gemini_video)
2) Rule validation
3) Optional repair pass
4) Re-validation
5) Optional audit-judge pass
"""

from __future__ import annotations

import argparse
import base64
import html
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
import yaml

import atlas_claude_smart_ai2 as claude_video
import prompts
import validator


@dataclass
class PipelineConfig:
    candidate_json: str = ""
    episode_id: str = "episode"
    video_duration_sec: float = 0.0

    video_file: str = ""
    video_url: str = ""
    headers_json: str = ""

    candidate_provider: str = "file"
    candidate_fallback_provider: str = ""
    candidate_model: str = "gemini-2.5-pro"
    candidate_api_key: str = ""
    max_frames: int = 45
    skip_object_map: bool = False
    gemini_max_retries: int = 4
    gemini_retry_base_delay_sec: float = 2.0

    run_repair: bool = True
    repair_provider: str = "none"
    repair_model: str = "claude-opus-4-5"
    repair_api_key: str = ""
    fail_open_on_repair_error: bool = True
    repair_policy: str = "major_only"  # always | major_only | on_fail

    run_judge: bool = False
    judge_provider: str = "none"
    judge_model: str = "gpt-4o"
    judge_api_key: str = ""
    fail_open_on_judge_error: bool = True
    judge_policy: str = "on_major_or_repair"  # always | on_major_or_repair | on_fail | on_major_only

    output_dir: str = "outputs"
    output_prefix: str = "atlas_pipeline"
    save_debug_files: bool = False


def _clean_json_text(text: str) -> str:
    clean = re.sub(r"```json|```", "", text or "", flags=re.IGNORECASE).strip()
    start = clean.find("{")
    end = clean.rfind("}")
    if start >= 0 and end > start:
        return clean[start : end + 1]
    start = clean.find("[")
    end = clean.rfind("]")
    if start >= 0 and end > start:
        return clean[start : end + 1]
    return clean


def _parse_json_text(text: str) -> Dict[str, Any]:
    cleaned = _clean_json_text(text)
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError:
        # Gemini/OpenAI may return valid JSON followed by extra tokens.
        decoder = json.JSONDecoder()
        start = 0
        for i, ch in enumerate(cleaned):
            if ch in "{[":
                start = i
                break
        payload, _ = decoder.raw_decode(cleaned[start:])
    if isinstance(payload, list):
        return {"segments": payload}
    if isinstance(payload, dict):
        return payload
    raise ValueError("Model did not return JSON object/list")


def _parse_json_from_text_parts(parts: list[dict[str, Any]], full_data: Dict[str, Any]) -> Dict[str, Any]:
    candidate_texts = []
    for part in parts:
        if isinstance(part, dict) and isinstance(part.get("text"), str):
            candidate_texts.append(part["text"])

    # Try each part separately first (safer when model emits multiple JSON blobs across parts).
    last_err: Exception | None = None
    for txt in candidate_texts:
        try:
            return _parse_json_text(txt)
        except Exception as exc:  # pragma: no cover - defensive branch
            last_err = exc

    # Fallback: try concatenated content.
    joined = "".join(candidate_texts)
    if joined:
        try:
            return _parse_json_text(joined)
        except Exception as exc:
            last_err = exc

    raise RuntimeError(f"Could not parse JSON from model response: {full_data}") from last_err


def _load_headers(path: str) -> Dict[str, str]:
    if not path:
        return {}
    return {str(k): str(v) for k, v in json.loads(Path(path).read_text(encoding="utf-8")).items()}


def _read_json(path: str) -> Dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return {"segments": payload}
    if isinstance(payload, dict):
        return payload
    raise ValueError(f"Unsupported JSON at {path}")


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _save_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _now_tag() -> str:
    return time.strftime("%Y%m%d_%H%M%S")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _format_ts(seconds: float) -> str:
    seconds = max(0.0, _safe_float(seconds))
    mins = int(seconds // 60)
    remain = seconds - mins * 60
    return f"{mins}:{remain:04.1f}"


def _render_final_report_html(
    annotation: Dict[str, Any],
    report: Dict[str, Any],
    summary: Dict[str, Any],
) -> str:
    segments = annotation.get("segments", [])
    segments_sorted = sorted(
        segments,
        key=lambda s: (
            int(s.get("segment_index", 0)),
            _safe_float(s.get("start_sec"), 0.0),
            _safe_float(s.get("end_sec"), 0.0),
        ),
    )

    rows = []
    for idx, seg in enumerate(segments_sorted, start=1):
        start = _safe_float(seg.get("start_sec"), 0.0)
        end = _safe_float(seg.get("end_sec"), start)
        dur = max(0.0, end - start)
        label = str(seg.get("label", "")).strip() or "No label"
        granularity = str(seg.get("granularity", "coarse")).strip() or "coarse"
        confidence = _safe_float(seg.get("confidence"), 0.0)
        rows.append(
            f"""
            <div class="segment-row {'selected' if idx == 1 else ''}">
              <div class="segment-num">{idx}</div>
              <div class="segment-main">
                <div class="segment-time">
                  <span>{_format_ts(start)}</span>
                  <span class="sep">-</span>
                  <span>{_format_ts(end)}</span>
                  <span class="plus">+</span>
                  <span>({dur:.1f}s)</span>
                  <span class="meta">{html.escape(granularity)} | c={confidence:.2f}</span>
                </div>
                <div class="segment-label">{html.escape(label)}</div>
              </div>
              <div class="segment-actions">
                <button title="Edit">e</button>
                <button title="Split">s</button>
                <button title="Merge">m</button>
                <button title="Delete">d</button>
                <button title="Play">p</button>
              </div>
            </div>
            """
        )

    title = html.escape(str(annotation.get("episode_id", summary.get("episode_id", "episode"))))
    verdict = "PASS" if report.get("ok") else "REVIEW NEEDED"
    status_color = "var(--ok)" if verdict == "PASS" else "var(--warn)"
    triggers = report.get("major_fail_triggers", [])
    issues = ", ".join(triggers) if triggers else "none"
    provider_line = " / ".join(
        [
            f"candidate: {summary.get('candidate_provider', 'n/a')}",
            f"repair: {summary.get('repair_provider', 'none')}",
            f"judge: {summary.get('judge_provider', 'none')}",
        ]
    )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Atlas Final Segments - {title}</title>
  <style>
    :root {{
      --bg: #f5f7fb;
      --card: #ffffff;
      --line: #d9e1ef;
      --text: #1f2a44;
      --muted: #67748e;
      --blue: #306bff;
      --blue-soft: #eef3ff;
      --ok: #1f8b4c;
      --warn: #b45309;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: radial-gradient(1200px 500px at 20% -10%, #e9efff 0%, var(--bg) 45%);
      color: var(--text);
      font-family: "Segoe UI", "Tahoma", sans-serif;
    }}
    .wrap {{
      max-width: 1120px;
      margin: 28px auto;
      padding: 0 14px;
    }}
    .panel {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 16px;
      overflow: hidden;
      box-shadow: 0 14px 34px rgba(25, 33, 52, 0.08);
    }}
    .head {{
      padding: 14px 16px 10px;
      border-bottom: 1px solid var(--line);
      background: #fbfcff;
    }}
    .top {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 4px;
    }}
    .title {{
      font-size: 24px;
      font-weight: 700;
      letter-spacing: 0.2px;
    }}
    .status {{
      font-size: 12px;
      color: {status_color};
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 4px 10px;
      background: #fff;
      white-space: nowrap;
    }}
    .sub {{
      color: var(--muted);
      font-size: 13px;
      display: flex;
      gap: 16px;
      flex-wrap: wrap;
    }}
    .kbd {{
      color: #7f8aa1;
      font-size: 12px;
      padding-top: 4px;
    }}
    .list {{
      padding: 10px;
      display: grid;
      gap: 8px;
    }}
    .segment-row {{
      display: grid;
      grid-template-columns: 38px 1fr auto;
      align-items: start;
      gap: 12px;
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: #fff;
    }}
    .segment-row.selected {{
      border: 2px solid var(--blue);
      background: var(--blue-soft);
    }}
    .segment-num {{
      width: 30px;
      height: 30px;
      border-radius: 50%;
      background: #e9edf6;
      color: #44516e;
      font-weight: 700;
      display: grid;
      place-items: center;
      font-size: 13px;
      margin-top: 1px;
    }}
    .segment-row.selected .segment-num {{
      background: var(--blue);
      color: #fff;
    }}
    .segment-main {{
      min-width: 0;
    }}
    .segment-time {{
      font-family: "Consolas", "Courier New", monospace;
      font-size: 12px;
      color: #61708d;
      display: flex;
      flex-wrap: wrap;
      gap: 7px;
      align-items: center;
      margin-bottom: 4px;
    }}
    .sep, .plus {{
      color: #7f8aa1;
    }}
    .meta {{
      color: #8793ac;
      font-family: "Segoe UI", "Tahoma", sans-serif;
      font-size: 12px;
    }}
    .segment-label {{
      font-size: 22px;
      line-height: 1.22;
      color: #202f4d;
      word-break: break-word;
      font-weight: 500;
      letter-spacing: 0.1px;
    }}
    .segment-actions {{
      display: flex;
      gap: 6px;
      padding-top: 4px;
    }}
    .segment-actions button {{
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
      color: #5d6a83;
      font-size: 12px;
      width: 28px;
      height: 28px;
      cursor: default;
    }}
    .foot {{
      border-top: 1px solid var(--line);
      padding: 10px 14px;
      font-size: 12px;
      color: #6f7d96;
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
    }}
    @media (max-width: 760px) {{
      .segment-label {{ font-size: 16px; }}
      .segment-row {{ grid-template-columns: 34px 1fr; }}
      .segment-actions {{ grid-column: 2; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="panel">
      <div class="head">
        <div class="top">
          <div class="title">Segments ({len(segments_sorted)})</div>
          <div class="status">{verdict}</div>
        </div>
        <div class="sub">
          <span>Episode: {title}</span>
          <span>Major triggers: {html.escape(issues)}</span>
          <span>{html.escape(provider_line)}</span>
        </div>
        <div class="kbd">j / k Nav | e Edit | s Split | d Delete | m Merge | p Play</div>
      </div>
      <div class="list">
        {"".join(rows) if rows else '<div class="segment-row"><div class="segment-main">No segments.</div></div>'}
      </div>
      <div class="foot">
        <span>Generated: {html.escape(summary.get("generated_at", ""))}</span>
        <span>Final output: {html.escape(summary.get("output_prefix", ""))}</span>
      </div>
    </div>
  </div>
</body>
</html>
"""


def _resolve_key(explicit_key: str, env_name: str) -> str:
    value = explicit_key.strip()
    if value:
        return value

    value = os.environ.get(env_name, "").strip()
    if value:
        return value

    # Windows fallback: read user env from registry (covers setx before shell restart).
    if os.name == "nt" and env_name:
        try:
            import winreg  # type: ignore

            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Environment") as key:
                reg_value, _ = winreg.QueryValueEx(key, env_name)
                if isinstance(reg_value, str) and reg_value.strip():
                    return reg_value.strip()
        except Exception:
            pass

    return ""


def _has_segments(payload: Any) -> bool:
    return isinstance(payload, dict) and isinstance(payload.get("segments"), list)


def _call_anthropic_json(api_key: str, model: str, system_prompt: str, user_text: str) -> Dict[str, Any]:
    try:
        import anthropic
    except ImportError as exc:
        raise RuntimeError("Missing dependency anthropic. Install with: pip install anthropic") from exc

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=model,
        max_tokens=4000,
        system=system_prompt,
        messages=[{"role": "user", "content": [{"type": "text", "text": user_text}]}],
    )
    text_parts = []
    for block in getattr(response, "content", []):
        if getattr(block, "type", "") == "text":
            text_parts.append(getattr(block, "text", ""))
    return _parse_json_text("\n".join(text_parts))


def _call_openai_json(api_key: str, model: str, system_prompt: str, user_text: str) -> Dict[str, Any]:
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RuntimeError("Missing dependency openai. Install with: pip install openai") from exc

    client = OpenAI(api_key=api_key)
    messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_text}]

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0,
            response_format={"type": "json_object"},
        )
    except Exception:
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0,
        )
    content = resp.choices[0].message.content or ""
    return _parse_json_text(content)


def _call_gemini_json(api_key: str, model: str, system_prompt: str, user_text: str) -> Dict[str, Any]:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    headers = {
        "Content-Type": "application/json",
        "X-goog-api-key": api_key,
    }
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": system_prompt},
                    {"text": user_text},
                ],
            }
        ],
        "generationConfig": {
            "temperature": 0.0,
            "responseMimeType": "application/json",
        },
    }
    response = requests.post(url, headers=headers, json=payload, timeout=240)
    response.raise_for_status()
    data = response.json()
    parts = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])
    return _parse_json_from_text_parts(parts, data)


def call_model_json(provider: str, api_key: str, model: str, system_prompt: str, user_text: str) -> Dict[str, Any]:
    p = provider.strip().lower()
    if p in {"anthropic", "claude"}:
        return _call_anthropic_json(api_key, model, system_prompt, user_text)
    if p in {"openai", "codex", "openai_codex"}:
        return _call_openai_json(api_key, model, system_prompt, user_text)
    if p == "gemini":
        return _call_gemini_json(api_key, model, system_prompt, user_text)
    raise ValueError(f"Unsupported provider: {provider}")


def run_claude_video_candidate(cfg: PipelineConfig) -> Tuple[Dict[str, Any], float]:
    api_key = _resolve_key(cfg.candidate_api_key, "ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("Anthropic key is required for candidate_provider=claude_vision")

    args = argparse.Namespace(
        video_url=cfg.video_url,
        video_file=cfg.video_file,
        api_key=api_key,
        headers_json=cfg.headers_json,
        model=cfg.candidate_model or claude_video.DEFAULT_MODEL,
        max_frames=cfg.max_frames,
        output_prefix="candidate_temp",
        skip_object_map=cfg.skip_object_map,
    )
    result, duration = claude_video.run_pipeline(args)
    if not _has_segments(result):
        raise RuntimeError(f"Claude candidate did not return segments: {result}")
    return result, duration


def run_gemini_video_candidate(cfg: PipelineConfig) -> Tuple[Dict[str, Any], float]:
    api_key = _resolve_key(cfg.candidate_api_key, "GEMINI_API_KEY")
    if not api_key:
        api_key = _resolve_key(cfg.candidate_api_key, "GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("Gemini API key is required for candidate_provider=gemini_video")
    if not cfg.video_file:
        raise RuntimeError("gemini_video currently requires local --video-file")

    video_path = Path(cfg.video_file)
    if not video_path.exists():
        raise FileNotFoundError(f"Video file not found: {cfg.video_file}")

    video_bytes = video_path.read_bytes()
    b64_video = base64.b64encode(video_bytes).decode("ascii")
    model = cfg.candidate_model or "gemini-2.5-pro"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    headers = {
        "Content-Type": "application/json",
        "X-goog-api-key": api_key,
    }

    request_payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": prompts.VIDEO_ANNOTATION_PROMPT},
                    {"text": "Use strict JSON output only. Prefer coarse labels if uncertain."},
                    {"inline_data": {"mime_type": "video/mp4", "data": b64_video}},
                ],
            }
        ],
        "generationConfig": {
            "temperature": 0.0,
            "responseMimeType": "application/json",
        },
    }

    data: Dict[str, Any] = {}
    last_error_text = ""
    max_retries = max(0, int(cfg.gemini_max_retries))
    base_delay = max(0.5, float(cfg.gemini_retry_base_delay_sec))

    for attempt in range(max_retries + 1):
        response = requests.post(url, headers=headers, json=request_payload, timeout=480)
        if response.status_code == 200:
            data = response.json()
            break

        body = response.text[:1000]
        last_error_text = f"Gemini HTTP {response.status_code}: {body}"

        # Retry on temporary/rate limit failures only.
        if response.status_code in {429, 500, 502, 503, 504} and attempt < max_retries:
            sleep_sec = base_delay * (2 ** attempt)
            print(f"Gemini temporary error {response.status_code}. Retrying in {sleep_sec:.1f}s...")
            time.sleep(sleep_sec)
            continue

        raise RuntimeError(last_error_text)

    if not data:
        raise RuntimeError(last_error_text or "Gemini returned empty response")
    parts = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])
    parsed = _parse_json_from_text_parts(parts, data)
    if not _has_segments(parsed):
        raise RuntimeError(f"Gemini candidate did not return segments: {parsed}")
    duration = float(cfg.video_duration_sec or 0.0)
    return parsed, duration


def get_candidate_annotation(cfg: PipelineConfig) -> Tuple[Dict[str, Any], float]:
    provider = cfg.candidate_provider.strip().lower()
    if provider == "file":
        if not cfg.candidate_json:
            raise RuntimeError("candidate_provider=file requires input.candidate_json")
        return _read_json(cfg.candidate_json), float(cfg.video_duration_sec or 0.0)

    if provider in {"claude_vision", "claude_video"}:
        return run_claude_video_candidate(cfg)
    if provider == "gemini_video":
        try:
            return run_gemini_video_candidate(cfg)
        except Exception as gemini_exc:
            fallback = cfg.candidate_fallback_provider.strip().lower()
            print(f"Gemini candidate failed: {gemini_exc}")
            if fallback in {"claude_vision", "claude_video"}:
                print("Falling back to Claude video candidate...")
                try:
                    return run_claude_video_candidate(cfg)
                except Exception as claude_exc:
                    print(f"Claude fallback failed: {claude_exc}")
                    if cfg.candidate_json:
                        print(f"Falling back to candidate JSON file: {cfg.candidate_json}")
                        return _read_json(cfg.candidate_json), float(cfg.video_duration_sec or 0.0)
                    raise
            if fallback == "file" and cfg.candidate_json:
                print(f"Falling back to candidate JSON file: {cfg.candidate_json}")
                return _read_json(cfg.candidate_json), float(cfg.video_duration_sec or 0.0)
            raise
    raise RuntimeError("No candidate source provided. Set candidate_json or candidate_provider with video input.")


def _to_pipeline_config(raw: Dict[str, Any]) -> PipelineConfig:
    input_cfg = raw.get("input", {}) if isinstance(raw.get("input"), dict) else {}
    providers_cfg = raw.get("providers", {}) if isinstance(raw.get("providers"), dict) else {}
    stages_cfg = raw.get("stages", {}) if isinstance(raw.get("stages"), dict) else {}
    output_cfg = raw.get("output", {}) if isinstance(raw.get("output"), dict) else {}

    candidate_cfg = providers_cfg.get("candidate", {}) if isinstance(providers_cfg.get("candidate"), dict) else {}
    repair_cfg = providers_cfg.get("repair", {}) if isinstance(providers_cfg.get("repair"), dict) else {}
    judge_cfg = providers_cfg.get("judge", {}) if isinstance(providers_cfg.get("judge"), dict) else {}

    return PipelineConfig(
        candidate_json=str(input_cfg.get("candidate_json", "")),
        episode_id=str(input_cfg.get("episode_id", "episode")),
        video_duration_sec=float(input_cfg.get("video_duration_sec", 0.0) or 0.0),
        video_file=str(input_cfg.get("video_file", "")),
        video_url=str(input_cfg.get("video_url", "")),
        headers_json=str(input_cfg.get("headers_json", "")),
        candidate_provider=str(candidate_cfg.get("type", "file")),
        candidate_fallback_provider=str(candidate_cfg.get("fallback_type", "")),
        candidate_model=str(candidate_cfg.get("model", "gemini-2.5-pro")),
        candidate_api_key=str(candidate_cfg.get("api_key", "")),
        max_frames=int(candidate_cfg.get("max_frames", 45)),
        skip_object_map=bool(candidate_cfg.get("skip_object_map", False)),
        gemini_max_retries=int(candidate_cfg.get("gemini_max_retries", 4)),
        gemini_retry_base_delay_sec=float(candidate_cfg.get("gemini_retry_base_delay_sec", 2.0)),
        run_repair=bool(stages_cfg.get("run_repair", True)),
        repair_provider=str(repair_cfg.get("type", "none")),
        repair_model=str(repair_cfg.get("model", "claude-opus-4-5")),
        repair_api_key=str(repair_cfg.get("api_key", "")),
        fail_open_on_repair_error=bool(stages_cfg.get("fail_open_on_repair_error", True)),
        repair_policy=str(stages_cfg.get("repair_policy", "major_only")),
        run_judge=bool(stages_cfg.get("run_judge", False)),
        judge_provider=str(judge_cfg.get("type", "none")),
        judge_model=str(judge_cfg.get("model", "gpt-4o")),
        judge_api_key=str(judge_cfg.get("api_key", "")),
        fail_open_on_judge_error=bool(stages_cfg.get("fail_open_on_judge_error", True)),
        judge_policy=str(stages_cfg.get("judge_policy", "on_major_or_repair")),
        output_dir=str(output_cfg.get("dir", "outputs")),
        output_prefix=str(output_cfg.get("prefix", "atlas_pipeline")),
        save_debug_files=bool(output_cfg.get("save_debug_files", False)),
    )


def _preprocess_yaml_text(text: str) -> str:
    lines = text.splitlines()
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        # Ignore shell exports accidentally pasted into YAML.
        if stripped.startswith("setx "):
            continue
        if stripped.startswith("export "):
            continue
        if stripped.startswith("$env:"):
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines)


def load_config(path: str) -> PipelineConfig:
    raw_text = Path(path).read_text(encoding="utf-8")
    preprocessed = _preprocess_yaml_text(raw_text)
    raw = yaml.safe_load(preprocessed) or {}
    if not isinstance(raw, dict):
        raise ValueError("Config must be a YAML object")
    return _to_pipeline_config(raw)


def _should_run_repair(cfg: PipelineConfig, report_candidate: Dict[str, Any]) -> bool:
    if not cfg.run_repair or cfg.repair_provider.strip().lower() == "none":
        return False
    policy = cfg.repair_policy.strip().lower()
    if policy == "always":
        return bool(report_candidate.get("repair_recommended"))
    if policy == "on_fail":
        return not bool(report_candidate.get("ok"))
    # default: major_only
    return bool(report_candidate.get("major_fail_triggers"))


def _should_run_judge(
    cfg: PipelineConfig,
    report_repaired: Dict[str, Any],
    repair_result: Dict[str, Any],
) -> bool:
    if not cfg.run_judge or cfg.judge_provider.strip().lower() == "none":
        return False

    policy = cfg.judge_policy.strip().lower()
    major_exists = bool(report_repaired.get("major_fail_triggers"))
    repaired_ran = not bool(repair_result.get("skipped", True))
    is_fail = not bool(report_repaired.get("ok"))

    if policy == "always":
        return True
    if policy == "on_fail":
        return is_fail
    if policy == "on_major_only":
        return major_exists
    # default: on_major_or_repair
    return major_exists or repaired_ran


def run_pipeline(cfg: PipelineConfig) -> Dict[str, Any]:
    output_dir = Path(cfg.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    tag = _now_tag()

    candidate_raw, candidate_duration = get_candidate_annotation(cfg)
    normalized_candidate = validator.normalize_annotation(
        candidate_raw,
        episode_id=cfg.episode_id,
        video_duration_sec=cfg.video_duration_sec or candidate_duration,
    )
    report_candidate = validator.validate_episode(normalized_candidate)
    candidate_validated = report_candidate.get("normalized_annotation", normalized_candidate)

    repaired_annotation = candidate_validated
    repair_result: Dict[str, Any] = {"skipped": True, "reason": "repair policy skipped"}
    repair_payload: Optional[Dict[str, Any]] = None

    if _should_run_repair(cfg, report_candidate):
        repair_payload = validator.build_repair_payload(candidate_validated, report_candidate)
        repair_key_env = {
            "anthropic": "ANTHROPIC_API_KEY",
            "claude": "ANTHROPIC_API_KEY",
            "openai": "OPENAI_API_KEY",
            "codex": "OPENAI_API_KEY",
            "openai_codex": "OPENAI_API_KEY",
            "gemini": "GEMINI_API_KEY",
        }
        repair_env = repair_key_env.get(cfg.repair_provider.strip().lower(), "")
        repair_key = _resolve_key(cfg.repair_api_key, repair_env)
        if cfg.repair_provider.strip().lower() == "gemini" and not repair_key:
            repair_key = _resolve_key(cfg.repair_api_key, "GOOGLE_API_KEY")
        if not repair_key:
            msg = f"Missing API key for repair provider: {cfg.repair_provider}"
            if cfg.fail_open_on_repair_error:
                print(f"Repair skipped: {msg}")
                repair_result = {"skipped": True, "reason": msg}
            else:
                raise RuntimeError(msg)
        else:
            try:
                repaired_raw = call_model_json(
                    provider=cfg.repair_provider,
                    api_key=repair_key,
                    model=cfg.repair_model,
                    system_prompt=prompts.REPAIR_PROMPT,
                    user_text=json.dumps(repair_payload, ensure_ascii=False),
                )
                repaired_annotation = validator.normalize_annotation(
                    repaired_raw,
                    episode_id=normalized_candidate["episode_id"],
                    annotation_version=normalized_candidate["annotation_version"],
                    video_duration_sec=normalized_candidate["video_duration_sec"],
                )
                repair_result = {"skipped": False}
            except Exception as exc:
                if cfg.fail_open_on_repair_error:
                    print(f"Repair failed, continuing without repair: {exc}")
                    repair_result = {"skipped": True, "reason": str(exc)}
                else:
                    raise
    elif cfg.run_repair and cfg.repair_provider.strip().lower() != "none":
        repair_result = {
            "skipped": True,
            "reason": f"repair policy skipped ({cfg.repair_policy})",
        }
    else:
        repair_result = {"skipped": True, "reason": "repair disabled"}

    report_repaired = validator.validate_episode(repaired_annotation)
    repaired_validated = report_repaired.get("normalized_annotation", repaired_annotation)

    judge_result: Dict[str, Any] = {"skipped": True, "reason": "judge policy skipped"}
    if _should_run_judge(cfg, report_repaired, repair_result):
        judge_key_env = {
            "anthropic": "ANTHROPIC_API_KEY",
            "claude": "ANTHROPIC_API_KEY",
            "openai": "OPENAI_API_KEY",
            "codex": "OPENAI_API_KEY",
            "openai_codex": "OPENAI_API_KEY",
            "gemini": "GEMINI_API_KEY",
        }
        judge_env = judge_key_env.get(cfg.judge_provider.strip().lower(), "")
        judge_key = _resolve_key(cfg.judge_api_key, judge_env)
        if cfg.judge_provider.strip().lower() == "gemini" and not judge_key:
            judge_key = _resolve_key(cfg.judge_api_key, "GOOGLE_API_KEY")
        if not judge_key:
            msg = f"Missing API key for judge provider: {cfg.judge_provider}"
            if cfg.fail_open_on_judge_error:
                print(f"Judge skipped: {msg}")
                judge_result = {"skipped": True, "reason": msg}
            else:
                raise RuntimeError(msg)
        else:
            try:
                judge_result = call_model_json(
                    provider=cfg.judge_provider,
                    api_key=judge_key,
                    model=cfg.judge_model,
                    system_prompt=prompts.AUDIT_JUDGE_PROMPT,
                    user_text=json.dumps(repaired_validated, ensure_ascii=False),
                )
                if "skipped" not in judge_result:
                    judge_result["skipped"] = False
            except Exception as exc:
                if cfg.fail_open_on_judge_error:
                    print(f"Judge failed, continuing without judge: {exc}")
                    judge_result = {"skipped": True, "reason": str(exc)}
                else:
                    raise
    elif cfg.run_judge and cfg.judge_provider.strip().lower() != "none":
        judge_result = {"skipped": True, "reason": f"judge policy skipped ({cfg.judge_policy})"}
    else:
        judge_result = {"skipped": True, "reason": "judge disabled"}

    prefix = f"{cfg.output_prefix}_{tag}"
    files = {
        "final": output_dir / f"{prefix}_final.json",
        "final_report": output_dir / f"{prefix}_final_report.html",
        "summary": output_dir / f"{prefix}_summary.json",
    }
    debug_files = {
        "candidate_raw": output_dir / f"{prefix}_candidate_raw.json",
        "candidate_normalized": output_dir / f"{prefix}_candidate_normalized.json",
        "candidate_report": output_dir / f"{prefix}_candidate_report.json",
        "repaired": output_dir / f"{prefix}_repaired.json",
        "repaired_report": output_dir / f"{prefix}_repaired_report.json",
        "repair_payload": output_dir / f"{prefix}_repair_payload.json",
        "judge": output_dir / f"{prefix}_judge.json",
    }

    generated_at = time.strftime("%Y-%m-%d %H:%M:%S")
    final_payload = {
        "generated_at": generated_at,
        "episode_id": repaired_validated.get("episode_id", cfg.episode_id),
        "video_duration_sec": repaired_validated.get("video_duration_sec", cfg.video_duration_sec),
        "annotation_version": repaired_validated.get("annotation_version", "atlas_v1"),
        "segments": repaired_validated.get("segments", []),
        "validation": {
            "ok": bool(report_repaired.get("ok")),
            "major_fail_triggers": report_repaired.get("major_fail_triggers", []),
            "episode_errors": report_repaired.get("episode_errors", []),
            "episode_warnings": report_repaired.get("episode_warnings", []),
        },
        "repair_result": repair_result,
        "judge_result": judge_result if isinstance(judge_result, dict) else {"skipped": False},
        "pipeline": {
            "candidate_provider": cfg.candidate_provider,
            "repair_provider": cfg.repair_provider,
            "judge_provider": cfg.judge_provider,
        },
    }
    _save_json(files["final"], final_payload)

    summary_seed = {
        "generated_at": generated_at,
        "episode_id": final_payload["episode_id"],
        "output_prefix": prefix,
        "candidate_provider": cfg.candidate_provider,
        "repair_provider": cfg.repair_provider,
        "judge_provider": cfg.judge_provider,
    }
    report_html = _render_final_report_html(repaired_validated, report_repaired, summary_seed)
    _save_text(files["final_report"], report_html)

    if cfg.save_debug_files:
        _save_json(debug_files["candidate_raw"], candidate_raw)
        _save_json(debug_files["candidate_normalized"], candidate_validated)
        _save_json(debug_files["candidate_report"], report_candidate)
        _save_json(debug_files["repaired"], repaired_validated)
        _save_json(debug_files["repaired_report"], report_repaired)
        if repair_payload:
            _save_json(debug_files["repair_payload"], repair_payload)
        if isinstance(judge_result, dict):
            _save_json(debug_files["judge"], judge_result)

    files_out = {
        "summary": str(files["summary"]),
        "final": str(files["final"]),
        "final_report": str(files["final_report"]),
    }
    if cfg.save_debug_files:
        for name, path in debug_files.items():
            if path.exists():
                files_out[name] = str(path)

    summary = {
        "generated_at": generated_at,
        "episode_id": final_payload["episode_id"],
        "output_prefix": prefix,
        "candidate_provider": cfg.candidate_provider,
        "repair_provider": cfg.repair_provider,
        "judge_provider": cfg.judge_provider,
        "candidate_ok": bool(report_candidate.get("ok")),
        "repaired_ok": bool(report_repaired.get("ok")),
        "candidate_major_fail_triggers": report_candidate.get("major_fail_triggers", []),
        "repaired_major_fail_triggers": report_repaired.get("major_fail_triggers", []),
        "final_segments_count": len(final_payload.get("segments", [])),
        "save_debug_files": cfg.save_debug_files,
        "repair_result": repair_result,
        "judge_result": judge_result if isinstance(judge_result, dict) else {"skipped": False},
        "files": files_out,
    }
    _save_json(files["summary"], summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Atlas multi-pass annotation pipeline")
    parser.add_argument("--config", default="", help="YAML config path")

    parser.add_argument("--candidate-json", default="", help="Candidate annotation JSON")
    parser.add_argument("--episode-id", default="episode", help="Episode id")
    parser.add_argument("--duration", type=float, default=0.0, help="Video duration sec")
    parser.add_argument("--output-dir", default="outputs", help="Output directory")
    parser.add_argument("--output-prefix", default="atlas_pipeline", help="Output prefix")
    parser.add_argument(
        "--save-debug-files",
        action="store_true",
        help="Also write intermediate candidate/repair/judge artifacts",
    )
    return parser.parse_args()


def _config_from_args(args: argparse.Namespace) -> PipelineConfig:
    return PipelineConfig(
        candidate_json=args.candidate_json,
        episode_id=args.episode_id,
        video_duration_sec=args.duration,
        output_dir=args.output_dir,
        output_prefix=args.output_prefix,
        save_debug_files=bool(args.save_debug_files),
    )


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config) if args.config else _config_from_args(args)
    summary = run_pipeline(cfg)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()


