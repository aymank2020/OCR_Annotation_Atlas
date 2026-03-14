"""
Create Vertex AI cached content from local prompt/context files.

Usage:
  python vertex_create_cache.py --config sample_web_auto_solver_vps.yaml
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Tuple

import requests
import yaml


DEFAULT_SYSTEM_PROMPT = """You are a strict Atlas annotation QA judge.
Follow these core rules:
1) Timeline must be chronological, non-overlapping, and gapless.
2) Max 2 atomic actions per segment.
3) Use concise neutral imperative verbs.
4) Use "No Action" only for true inactivity.
5) Avoid temporal hallucination and invented actions/objects.
6) Output strict JSON when requested by the calling prompt.
""".strip()


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _try_read_file(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    try:
        return _read_text(path).strip()
    except Exception:
        return ""


def _post_cached_content(
    *,
    project: str,
    location: str,
    token: str,
    model: str,
    display_name: str,
    ttl_seconds: int,
    system_text: str,
    context_text: str,
) -> Tuple[int, str]:
    host = "aiplatform.googleapis.com" if location == "global" else f"{location}-aiplatform.googleapis.com"
    url = f"https://{host}/v1/projects/{project}/locations/{location}/cachedContents"
    payload: Dict[str, object] = {
        "model": f"projects/{project}/locations/{location}/publishers/google/models/{model}",
        "displayName": display_name,
        "ttl": f"{max(60, int(ttl_seconds))}s",
        "systemInstruction": {"parts": [{"text": system_text}]},
        "contents": [{"role": "user", "parts": [{"text": context_text}]}],
    }
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=180,
    )
    return resp.status_code, resp.text


def main() -> None:
    ap = argparse.ArgumentParser(description="Create Vertex AI context cache")
    ap.add_argument("--config", default="sample_web_auto_solver_vps.yaml")
    ap.add_argument("--system", default="prompts/system_prompt.txt")
    ap.add_argument("--context", default="prompts/atlas_vertex_context_pack.txt")
    ap.add_argument("--model", default="gemini-3.1-pro-preview")
    ap.add_argument("--display-name", default="atlas-qa-cache")
    ap.add_argument("--ttl-seconds", type=int, default=86400)
    args = ap.parse_args()

    try:
        from google.auth.transport.requests import Request
        from google.oauth2 import service_account
    except Exception as exc:
        raise RuntimeError(
            "Missing dependency google-auth. Install with: pip install google-auth requests pyyaml"
        ) from exc

    cfg_path = Path(args.config).resolve()
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    gem = cfg.get("gemini", {}) if isinstance(cfg, dict) else {}
    if not isinstance(gem, dict):
        raise RuntimeError("Invalid gemini section in config.")

    project = str(gem.get("vertex_project", "") or "").strip()
    location = str(gem.get("vertex_location", "us-central1") or "us-central1").strip()
    cred_raw = str(gem.get("vertex_credentials_path", "") or "").strip()

    if not project:
        raise RuntimeError("gemini.vertex_project is missing in config.")
    if not cred_raw:
        raise RuntimeError("gemini.vertex_credentials_path is missing in config.")

    cred_path = Path(cred_raw).resolve()
    if not cred_path.exists():
        raise FileNotFoundError(f"Credentials file not found: {cred_path}")

    cfg_dir = cfg_path.parent

    # Resolve system text (file -> config text -> default).
    system_text = _try_read_file(Path(args.system).resolve())
    if not system_text:
        system_text = str(gem.get("system_instruction_text", "") or "").strip()
    if not system_text:
        system_text = DEFAULT_SYSTEM_PROMPT
        print("[warn] system prompt file not found; using built-in default system prompt.")

    # Resolve context text (arg file -> gem.context_file -> gem.context_text).
    context_text = _try_read_file(Path(args.context).resolve())
    if not context_text:
        context_file_cfg = str(gem.get("context_file", "") or "").strip()
        if context_file_cfg:
            context_file_path = Path(context_file_cfg)
            if not context_file_path.is_absolute():
                context_file_path = (cfg_dir / context_file_path).resolve()
            context_text = _try_read_file(context_file_path)
    if not context_text:
        context_text = str(gem.get("context_text", "") or "").strip()
    if not context_text:
        raise FileNotFoundError(
            "No context content found. Provide --context file or set gemini.context_file/context_text in config."
        )

    creds = service_account.Credentials.from_service_account_file(
        str(cred_path),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    creds.refresh(Request())
    token = str(getattr(creds, "token", "") or "").strip()
    if not token:
        raise RuntimeError("Could not get access token.")

    status, text = _post_cached_content(
        project=project,
        location=location,
        token=token,
        model=args.model,
        display_name=args.display_name,
        ttl_seconds=args.ttl_seconds,
        system_text=system_text,
        context_text=context_text,
    )
    used_location = location
    if status != 200 and location != "global":
        print(f"[warn] create cache failed on location={location} (HTTP {status}), retrying on global...")
        status, text = _post_cached_content(
            project=project,
            location="global",
            token=token,
            model=args.model,
            display_name=args.display_name,
            ttl_seconds=args.ttl_seconds,
            system_text=system_text,
            context_text=context_text,
        )
        used_location = "global"

    print("HTTP", status)
    if status != 200:
        print(text[:4000])
        raise SystemExit(1)

    data = json.loads(text)
    print("CACHE_NAME=", data.get("name", ""))
    print("LOCATION_USED=", used_location)
    print(json.dumps(data, ensure_ascii=False, indent=2)[:4000])


if __name__ == "__main__":
    main()
