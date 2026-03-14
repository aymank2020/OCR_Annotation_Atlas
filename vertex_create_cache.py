"""
Create Vertex AI cached content from local prompt/context files.

Usage:
  python vertex_create_cache.py --config sample_web_auto_solver_vps.yaml
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import requests
import yaml
from google.auth.transport.requests import Request
from google.oauth2 import service_account


def _read_text(path: Path) -> str:
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Missing file: {path}")
    return path.read_text(encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Create Vertex AI context cache")
    ap.add_argument("--config", default="sample_web_auto_solver_vps.yaml")
    ap.add_argument("--system", default="prompts/system_prompt.txt")
    ap.add_argument("--context", default="prompts/atlas_vertex_context_pack.txt")
    ap.add_argument("--model", default="gemini-3.1-pro-preview")
    ap.add_argument("--display-name", default="atlas-qa-cache")
    ap.add_argument("--ttl-seconds", type=int, default=86400)
    args = ap.parse_args()

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

    system_text = _read_text(Path(args.system).resolve())
    context_text = _read_text(Path(args.context).resolve())

    creds = service_account.Credentials.from_service_account_file(
        str(cred_path),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    creds.refresh(Request())
    token = str(getattr(creds, "token", "") or "").strip()
    if not token:
        raise RuntimeError("Could not get access token.")

    url = (
        f"https://{location}-aiplatform.googleapis.com/v1/projects/{project}/"
        f"locations/{location}/cachedContents"
    )
    payload = {
        "model": f"publishers/google/models/{args.model}",
        "displayName": args.display_name,
        "ttl": f"{max(60, int(args.ttl_seconds))}s",
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

    print("HTTP", resp.status_code)
    if resp.status_code != 200:
        print(resp.text[:4000])
        raise SystemExit(1)

    data = resp.json()
    print("CACHE_NAME=", data.get("name", ""))
    print(json.dumps(data, ensure_ascii=False, indent=2)[:4000])


if __name__ == "__main__":
    main()
