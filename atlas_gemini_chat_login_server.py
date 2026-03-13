"""
Interactive Gemini Chat login helper for Linux servers.

This script opens Gemini in a persistent Playwright Chromium profile, so you can
complete Google login + 2FA once on the server and reuse the saved session later.

Typical flow:
1) Start Xvfb + VNC on the server (noVNC).
2) Run this script with DISPLAY set to that virtual screen.
3) Complete login in the remote browser UI.
4) Press Enter in terminal to save and close.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from playwright.sync_api import sync_playwright


def main() -> None:
    parser = argparse.ArgumentParser(description="Open Gemini chat on server and persist login profile")
    parser.add_argument(
        "--profile-dir",
        default="/root/OCR_annotation_Atlas/.state/gemini_chat_profile",
        help="Persistent Chromium user data dir",
    )
    parser.add_argument(
        "--url",
        default="https://gemini.google.com/app/b3006ba9f325b55c",
        help="Gemini chat URL",
    )
    parser.add_argument(
        "--channel",
        default="",
        help="Optional browser channel (chrome/chromium). Leave empty to use Playwright Chromium bundle.",
    )
    parser.add_argument("--timeout-ms", type=int, default=90000)
    args = parser.parse_args()

    display = str(os.environ.get("DISPLAY", "") or "").strip()
    if not display:
        raise SystemExit(
            "DISPLAY is empty. Start Xvfb/noVNC first, then run with DISPLAY set "
            "(example: DISPLAY=:99 python atlas_gemini_chat_login_server.py)."
        )

    profile_dir = Path(args.profile_dir).expanduser().resolve()
    profile_dir.mkdir(parents=True, exist_ok=True)
    storage_state = profile_dir / "storage_state.json"

    print(f"[gemini-login] DISPLAY={display}")
    print(f"[gemini-login] profile_dir={profile_dir}")
    print(f"[gemini-login] opening: {args.url}")

    with sync_playwright() as pw:
        launch_kwargs = {
            "user_data_dir": str(profile_dir),
            "headless": False,
        }
        if str(args.channel or "").strip():
            launch_kwargs["channel"] = str(args.channel).strip()
        context = pw.chromium.launch_persistent_context(**launch_kwargs)
        page = context.new_page()
        page.goto(args.url, wait_until="domcontentloaded", timeout=max(10000, int(args.timeout_ms)))

        print(
            "\n[gemini-login] Complete Google sign-in + 2FA in the opened browser window.\n"
            "[gemini-login] When Gemini chat is ready, return here and press Enter."
        )
        input()

        context.storage_state(path=str(storage_state))
        context.close()

    print(f"[gemini-login] saved profile: {profile_dir}")
    print(f"[gemini-login] saved storage state: {storage_state}")


if __name__ == "__main__":
    main()

