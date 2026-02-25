"""
Atlas browser auto-solver:
1) Login to audit.atlascapture.io
2) Auto-read OTP from Gmail (IMAP)
3) Open task room and extract segments
4) Send segments to Gemini API
5) Optionally write labels back into Atlas
"""

from __future__ import annotations

import argparse
import base64
import html
import imaplib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import time
from datetime import datetime, timedelta, timezone
from email import message_from_bytes
from email.header import decode_header
from email.message import Message
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
import yaml
from playwright.sync_api import Locator, Page, sync_playwright


DEFAULT_CONFIG: Dict[str, Any] = {
    "browser": {
        "headless": False,
        "slow_mo_ms": 120,
        "storage_state_path": ".state/atlas_auth.json",
        "force_login": False,
        "use_chrome_profile": False,
        "chrome_channel": "chrome",
        "chrome_user_data_dir": "",
        "chrome_profile_directory": "Default",
        "fallback_to_isolated_context_on_profile_error": True,
        "profile_launch_timeout_ms": 30000,
        "close_chrome_before_profile_launch": False,
        "profile_launch_retry_count": 1,
        "profile_launch_retry_delay_sec": 2.0,
        "clone_chrome_profile_to_temp": True,
        "cloned_user_data_dir": ".state/chrome_user_data_clone",
        "reuse_existing_cloned_profile": True,
        "prefer_profile_with_atlas_cookies": True,
    },
    "run": {
        "dry_run": True,
        "max_segments": 0,
        "max_episodes_per_run": 1,
        "reserve_cooldown_sec": 120,
        "reuse_cached_labels": True,
        "skip_unchanged_labels": True,
        "resume_from_artifacts": True,
        "resume_skip_video_steps_when_cached": True,
        "resume_skip_apply_steps_when_done": True,
        "use_task_scoped_artifacts": True,
        "enable_quality_review_submit": True,
        "adjust_timestamps": True,
        "play_full_video_before_labeling": False,
        "play_full_video_max_wait_sec": 900,
        "segment_resolve_attempts": 24,
        "segment_resolve_retry_ms": 800,
        "segment_resolve_sample_size": 8,
        "segment_resolve_row_text_timeout_ms": 350,
        "output_dir": "outputs",
        "segments_dump": "atlas_segments_dump.json",
        "labels_dump": "atlas_labels_from_gemini.json",
        "prompt_dump": "atlas_prompt.txt",
        "video_dump": "atlas_task_video.mp4",
    },
    "atlas": {
        "login_url": "https://audit.atlascapture.io/login?redirect=%2F",
        "dashboard_url": "https://audit.atlascapture.io/dashboard",
        "room_url": "https://audit.atlascapture.io/tasks/room/normal",
        "email": "",
        "auth_timeout_sec": 180,
        "wait_before_continue_sec": 5,
        "selectors": {
            "email_input": '#email || input#email || input[type="email"] || input[autocomplete="email"]',
            "start_button": 'button:has-text("Start Earning Today") || button[type="submit"]',
            "otp_input": '#code || input#code || input[inputmode="numeric"] || input[placeholder="000000"]',
            "verify_button": 'button:has-text("Verify") || button[type="submit"]',
            "tasks_nav": 'a[href*="/tasks"] || a:has-text("Tasks") || button:has-text("Tasks") || [data-testid*="tasks"]',
            "enter_workflow_button": 'button:has-text("Enter Standard Workflow") || text=/enter\\s+standard\\s+workflow/i',
            "continue_room_button": 'button:has-text("Continue to Room") || text=/continue\\s+to\\s+room/i',
            "label_button": 'button:has-text("Label") || text=/\\blabel\\b/i',
            "label_task_link": 'a[href*="/tasks/room/normal/label/"]',
            "reserve_episodes_button": 'button:has-text("Reserve 5 Episodes") || button:has-text("Reserve")',
            "confirm_reserve_button": 'button:has-text("I Understand") || button:has-text("Understand") || button:has-text("OK") || button:has-text("Okay") || button:has-text("Confirm")',
            "video_element": "video",
            "video_source": "video source",
            "loop_toggle_button": 'button:has-text("Loop OFF") || button:has-text("Loop ON") || button[title*="Toggle segment loop"]',
            "complete_button": 'button:has-text("Complete")',
            "quality_review_modal": 'div[role="dialog"]:has-text("Quality Review") || div:has-text("Quality Review")',
            "quality_review_checkbox": 'input[type="checkbox"] || [role="checkbox"] || label:has-text("I verify that I have reviewed")',
            "quality_review_submit_button": 'button:has-text("Submit")',
            "blocking_side_panel": 'div[class*="fixed"][class*="right-4"][class*="z-50"][class*="slide-in-from-right"] || div[class*="fixed"][class*="right-4"][class*="z-50"][class*="shadow-2xl"]',
            "blocking_side_panel_close": 'button:has-text("Close") || button:has-text("Dismiss") || button:has-text("Done") || button:has-text("Cancel") || [role="button"]:has-text("Close") || button[aria-label*="close" i] || button[title*="close" i]',
            "segment_rows": "div.space-y-1.p-2 > div.rounded-lg.border.p-3 || [data-testid*=\"segment\"] || [data-cy*=\"segment\"] || .segment-row || .seg-item || [class*=\"segment\"][class*=\"row\"]",
            "segment_label": 'p[title*="Double-click to edit"] || p.text-sm.font-medium.cursor-text || [data-testid*="label"] || [data-cy*="label"] || .segment-label || .seg-label || [class*="label"]',
            "segment_start": 'span.font-mono.text-xs || [data-testid*="start"] || [data-cy*="start"]',
            "segment_end": 'span.font-mono.text-xs.px-1\\.5.py-0\\.5 || [data-testid*="end"] || [data-cy*="end"]',
            "segment_time_plus_button": 'button:has(svg.lucide-plus)',
            "segment_time_minus_button": 'button:has(svg.lucide-minus)',
            "edit_button_in_row": 'button[title*="Edit"] || button:has-text("Edit") || [aria-label*="Edit"]',
            "label_input": 'textarea || [contenteditable="true"] || input[type="text"]',
            "save_button": 'button:has-text("Save") || button:has-text("Apply") || button:has-text("Done") || button:has-text("Submit")',
        },
        "timestamp_step_sec": 0.1,
        "timestamp_max_clicks_per_segment": 30,
    },
    "otp": {
        "provider": "gmail_imap",
        "gmail_email": "",
        "gmail_app_password": "",
        "imap_host": "imap.gmail.com",
        "imap_port": 993,
        "mailbox": "[Gmail]/All Mail",
        "sender_hint": "",
        "subject_hint": "",
        "code_regex": "\\b(\\d{6})\\b",
        "timeout_sec": 120,
        "poll_interval_sec": 4,
        "max_messages": 25,
        "unseen_only": False,
        "lookback_sec": 300,
    },
    "gemini": {
        "api_key": "",
        "model": "gemini-2.5-flash",
        "temperature": 0.0,
        "max_retries": 3,
        "retry_base_delay_sec": 2.0,
        "connect_timeout_sec": 30,
        "request_timeout_sec": 420,
        "attach_video": True,
        "require_video": False,
        "allow_text_only_fallback_on_network_error": True,
        "max_inline_video_mb": 20.0,
        "video_download_timeout_sec": 180,
        "min_video_bytes": 500000,
        "extra_instructions": "",
    },
}

_LAST_RESERVE_REQUEST_TS = 0.0


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


def _cfg_get(cfg: Dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = cfg
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def _resolve_secret(explicit: str, env_names: List[str]) -> str:
    if explicit and explicit.strip():
        return explicit.strip()
    for name in env_names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return ""


def _resolve_gemini_key(explicit: str) -> str:
    return _resolve_secret(explicit, ["GEMINI_API_KEY", "GOOGLE_API_KEY"])


def _default_chrome_user_data_dir() -> str:
    if os.name == "nt":
        local_app_data = os.environ.get("LOCALAPPDATA", "").strip()
        if local_app_data:
            return str(Path(local_app_data) / "Google" / "Chrome" / "User Data")
    return ""


def _looks_like_profile_dir_name(name: str) -> bool:
    n = (name or "").strip()
    return n == "Default" or n.startswith("Profile ")


def _is_direct_profile_path(path_value: str) -> bool:
    if not path_value:
        return False
    p = Path(path_value)
    if not p.exists() or not p.is_dir():
        return False
    if not _looks_like_profile_dir_name(p.name):
        return False
    return (p / "Preferences").exists()


def _resolve_atlas_email(cfg: Dict[str, Any]) -> str:
    return (
        str(_cfg_get(cfg, "atlas.email", "")).strip()
        or os.environ.get("ATLAS_LOGIN_EMAIL", "").strip()
        or os.environ.get("ATLAS_EMAIL", "").strip()
    )


def _detect_chrome_profile_for_email(user_data_dir: str, email: str) -> str:
    if not user_data_dir or not email:
        return ""
    root = Path(user_data_dir)
    if not root.exists():
        return ""
    target = email.strip().lower()
    for child in sorted(root.iterdir(), key=lambda p: p.name):
        if not child.is_dir():
            continue
        if not (child.name == "Default" or child.name.startswith("Profile ")):
            continue
        pref = child / "Preferences"
        if not pref.exists():
            continue
        try:
            raw = json.loads(pref.read_text(encoding="utf-8"))
        except Exception:
            continue
        account_info = raw.get("account_info", [])
        if isinstance(account_info, list):
            for acc in account_info:
                if not isinstance(acc, dict):
                    continue
                acc_email = str(acc.get("email", "")).strip().lower()
                if acc_email and acc_email == target:
                    return child.name
    return ""


def _count_site_cookies_in_profile(profile_dir: Path, domain_hint: str) -> int:
    if not profile_dir.exists():
        return 0
    db_candidates = [profile_dir / "Network" / "Cookies", profile_dir / "Cookies"]
    for db_path in db_candidates:
        if not db_path.exists():
            continue
        try:
            conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM cookies WHERE host_key LIKE ?",
                (f"%{domain_hint}%",),
            )
            row = cur.fetchone()
            conn.close()
            return int(row[0]) if row else 0
        except Exception:
            continue
    return 0


def _detect_chrome_profile_for_site_cookie(user_data_dir: str, domain_hint: str = "atlascapture.io") -> str:
    if not user_data_dir:
        return ""
    root = Path(user_data_dir)
    if not root.exists():
        return ""
    best_profile = ""
    best_count = 0
    for child in sorted(root.iterdir(), key=lambda p: p.name):
        if not child.is_dir():
            continue
        if not (child.name == "Default" or child.name.startswith("Profile ")):
            continue
        cookie_count = _count_site_cookies_in_profile(child, domain_hint=domain_hint)
        if cookie_count > best_count:
            best_count = cookie_count
            best_profile = child.name
    if best_profile:
        print(f"[browser] detected profile with atlas cookies: {best_profile} (cookies={best_count})")
    return best_profile


def _otp_provider(cfg: Dict[str, Any]) -> str:
    return str(_cfg_get(cfg, "otp.provider", "gmail_imap")).strip().lower()


def _otp_is_manual(cfg: Dict[str, Any]) -> bool:
    return _otp_provider(cfg) in {"manual", "manual_browser", "browser", "none"}


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _is_authenticated_page(page: Page) -> bool:
    url = (page.url or "").lower()
    if "/login" in url or "/verify" in url:
        return False
    return "/dashboard" in url or "/tasks" in url


def _restore_storage_state(context: Any, page: Page, state_path: Path) -> bool:
    if not state_path.exists():
        return False
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    restored_any = False
    cookies = data.get("cookies")
    if isinstance(cookies, list) and cookies:
        try:
            context.add_cookies(cookies)
            print(f"[auth] restored {len(cookies)} cookies from state: {state_path}")
            restored_any = True
        except Exception:
            pass
    origins = data.get("origins")
    if isinstance(origins, list) and origins:
        for item in origins:
            if not isinstance(item, dict):
                continue
            origin = str(item.get("origin", "")).strip()
            ls_items = item.get("localStorage")
            if not origin or not isinstance(ls_items, list):
                continue
            try:
                temp_page = context.new_page()
                temp_page.goto(origin, wait_until="domcontentloaded", timeout=30000)
                temp_page.evaluate(
                    """(items) => {
                        for (const it of items || []) {
                            if (it && typeof it.name === 'string') {
                                localStorage.setItem(it.name, String(it.value ?? ''));
                            }
                        }
                    }""",
                    ls_items,
                )
                temp_page.close()
                restored_any = True
            except Exception:
                continue
    try:
        if restored_any:
            page.goto("about:blank")
    except Exception:
        pass
    return restored_any


def _close_chrome_processes() -> None:
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/F", "/IM", "chrome.exe"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    else:
        subprocess.run(
            ["pkill", "-f", "chrome"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )


def _prepare_chrome_profile_clone(
    source_user_data_dir: str,
    profile_directory: str,
    target_user_data_dir: str,
    reuse_existing: bool = True,
) -> str:
    src_root = Path(source_user_data_dir)
    if not src_root.exists():
        raise FileNotFoundError(f"Chrome user data dir not found: {source_user_data_dir}")

    src_profile = src_root / profile_directory
    if not src_profile.exists():
        raise FileNotFoundError(f"Chrome profile directory not found: {src_profile}")

    dst_root = Path(target_user_data_dir).resolve()
    if reuse_existing and (dst_root / profile_directory).exists():
        print(f"[browser] reusing existing cloned profile: {dst_root}")
        return str(dst_root)
    if dst_root.exists():
        shutil.rmtree(dst_root, ignore_errors=True)
    dst_root.mkdir(parents=True, exist_ok=True)

    for root_name in ["Local State", "First Run"]:
        src_file = src_root / root_name
        if src_file.exists() and src_file.is_file():
            shutil.copy2(src_file, dst_root / root_name)

    # Keep stateful profile files; skip heavy caches and continue on locked files.
    skip_dir_names = {
        "Cache",
        "Code Cache",
        "GPUCache",
        "ShaderCache",
        "GrShaderCache",
        "DawnCache",
        "Service Worker",
        "Media Cache",
        "Crashpad",
    }
    copied = 0
    skipped = 0
    src_root_profile = src_profile.resolve()
    dst_root_profile = (dst_root / profile_directory).resolve()
    dst_root_profile.mkdir(parents=True, exist_ok=True)

    for root, dirs, files in os.walk(src_root_profile):
        dirs[:] = [d for d in dirs if d not in skip_dir_names]
        rel_root = Path(root).resolve().relative_to(src_root_profile)
        dst_dir = (dst_root_profile / rel_root).resolve()
        dst_dir.mkdir(parents=True, exist_ok=True)
        for file_name in files:
            src_file = Path(root) / file_name
            dst_file = dst_dir / file_name
            try:
                shutil.copy2(src_file, dst_file)
                copied += 1
            except Exception:
                skipped += 1

    print(f"[browser] profile clone done. copied_files={copied}, skipped_files={skipped}")
    return str(dst_root)


def _selector_variants(selector: str) -> List[str]:
    if not selector:
        return []
    if "||" in selector:
        return [part.strip() for part in selector.split("||") if part.strip()]
    return [selector.strip()]


def _any_locator_exists(page: Page, selector: str) -> bool:
    for candidate in _selector_variants(selector):
        try:
            if page.locator(candidate).count() > 0:
                return True
        except Exception:
            continue
    return False


def _first_visible_locator(page: Page, selector: str, timeout_ms: int = 4000) -> Locator | None:
    if not selector:
        return None
    deadline = time.time() + timeout_ms / 1000.0
    max_scan = 25
    while time.time() < deadline:
        for candidate in _selector_variants(selector):
            try:
                loc = page.locator(candidate)
                count = loc.count()
                if count <= 0:
                    continue
                scan = min(count, max_scan)
                for i in range(scan):
                    locator = loc.nth(i)
                    if locator.is_visible():
                        return locator
            except Exception:
                continue
        time.sleep(0.1)
    return None


def _safe_locator_click(page: Page, selector: str, timeout_ms: int = 4000) -> bool:
    locator = _first_visible_locator(page, selector, timeout_ms=timeout_ms)
    if locator is None:
        return False
    try:
        locator.click()
        return True
    except Exception:
        return False


def _safe_fill(page: Page, selector: str, value: str, timeout_ms: int = 4000) -> bool:
    locator = _first_visible_locator(page, selector, timeout_ms=timeout_ms)
    if locator is None:
        return False
    try:
        locator.fill(value)
        return True
    except Exception:
        try:
            locator.click()
            page.keyboard.press("Control+A")
            page.keyboard.type(value, delay=10)
            return True
        except Exception:
            return False


def _safe_locator_text(locator: Locator, timeout_ms: int = 1200) -> str:
    try:
        return (locator.inner_text(timeout=timeout_ms) or "").strip()
    except Exception:
        return ""


def _first_href_from_selector(page: Page, selector: str, max_scan: int = 40) -> str:
    for candidate in _selector_variants(selector):
        try:
            loc = page.locator(candidate)
            count = min(loc.count(), max_scan)
            for i in range(count):
                href = loc.nth(i).get_attribute("href")
                if href and href.strip():
                    return href.strip()
        except Exception:
            continue
    return ""


def _first_task_label_href_from_html(page: Page) -> str:
    try:
        html_doc = page.content()
    except Exception:
        return ""
    m = re.search(r'(/tasks/room/normal/label/[A-Za-z0-9]+)', html_doc or "")
    if not m:
        return ""
    return m.group(1).strip()


def _looks_like_video_url(url: str) -> bool:
    u = (url or "").lower()
    if not u:
        return False
    if u.startswith("blob:"):
        return False
    markers = [".mp4", ".webm", ".mov", ".m4v", ".m3u8", "video", "/media/"]
    return any(m in u for m in markers)


def _collect_video_url_candidates(page: Page, cfg: Dict[str, Any]) -> List[str]:
    selectors = _cfg_get(cfg, "atlas.selectors", {})
    video_sel = str(selectors.get("video_element", "video"))
    source_sel = str(selectors.get("video_source", "video source"))
    base_url = page.url

    seen: set[str] = set()
    out: List[str] = []

    def add(raw: str) -> None:
        raw = (raw or "").strip()
        if not raw:
            return
        if raw.startswith("blob:"):
            return
        if raw.startswith("//"):
            raw = f"https:{raw}"
        elif raw.startswith("/"):
            raw = urljoin(base_url, raw)
        parsed = urlparse(raw)
        if parsed.scheme not in {"http", "https"}:
            return
        norm = raw.strip()
        if norm in seen:
            return
        seen.add(norm)
        out.append(norm)

    # Video/source elements.
    for sel in _selector_variants(video_sel):
        try:
            loc = page.locator(sel)
            for i in range(min(loc.count(), 6)):
                item = loc.nth(i)
                add(item.get_attribute("src") or "")
                add(item.get_attribute("data-src") or "")
                try:
                    cur = item.evaluate("el => (el.currentSrc || '')")
                    if isinstance(cur, str):
                        add(cur)
                except Exception:
                    pass
        except Exception:
            continue

    for sel in _selector_variants(source_sel):
        try:
            loc = page.locator(sel)
            for i in range(min(loc.count(), 10)):
                item = loc.nth(i)
                add(item.get_attribute("src") or "")
        except Exception:
            continue

    # HTML links/resources.
    try:
        html_doc = page.content()
    except Exception:
        html_doc = ""
    for match in re.findall(r'["\']([^"\']+\.(?:mp4|webm|mov|m4v|m3u8)(?:\?[^"\']*)?)["\']', html_doc, flags=re.I):
        add(match)
    for match in re.findall(r'https?://[^\s"\'<>]+', html_doc):
        if _looks_like_video_url(match):
            add(match)

    # Performance resources.
    try:
        entries = page.evaluate("() => performance.getEntriesByType('resource').map(e => e.name)")
        if isinstance(entries, list):
            for item in entries:
                if isinstance(item, str) and _looks_like_video_url(item):
                    add(item)
    except Exception:
        pass

    # Prefer URLs with explicit video extensions first.
    out.sort(
        key=lambda u: (
            0 if re.search(r"\.(mp4|webm|mov|m4v|m3u8)(\?|$)", u, flags=re.I) else 1,
            0 if "atlascapture" in u.lower() else 1,
            len(u),
        )
    )
    return out


def _download_video_from_page_context(
    page: Page,
    context: Any,
    video_url: str,
    out_path: Path,
    timeout_sec: int,
) -> Path:
    sess = requests.Session()
    headers = {
        "Accept": "*/*",
        "Referer": page.url,
    }
    try:
        ua = page.evaluate("() => navigator.userAgent")
        if isinstance(ua, str) and ua.strip():
            headers["User-Agent"] = ua.strip()
    except Exception:
        pass

    try:
        cookies = context.cookies([video_url]) or context.cookies()
    except Exception:
        cookies = []
    for c in cookies:
        try:
            sess.cookies.set(
                c.get("name", ""),
                c.get("value", ""),
                domain=c.get("domain"),
                path=c.get("path", "/"),
            )
        except Exception:
            continue

    resp = sess.get(video_url, headers=headers, timeout=timeout_sec, stream=True, allow_redirects=True)
    resp.raise_for_status()
    _ensure_parent(out_path)
    written = 0
    with out_path.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            f.write(chunk)
            written += len(chunk)
    if written <= 0:
        raise RuntimeError("Downloaded video file is empty.")
    return out_path


def _is_probably_mp4(path: Path) -> bool:
    try:
        with path.open("rb") as f:
            head = f.read(64)
    except Exception:
        return False
    if len(head) < 12:
        return False
    # Common MP4 signature: box size then 'ftyp' within first 12 bytes.
    return b"ftyp" in head[:16]


def _prepare_video_for_gemini(
    page: Page,
    context: Any,
    cfg: Dict[str, Any],
    task_id: str = "",
) -> Optional[Path]:
    attach_video = bool(_cfg_get(cfg, "gemini.attach_video", True))
    if not attach_video:
        return None

    out_dir = Path(str(_cfg_get(cfg, "run.output_dir", "outputs")))
    out_dir.mkdir(parents=True, exist_ok=True)
    use_task_scoped = bool(_cfg_get(cfg, "run.use_task_scoped_artifacts", True))
    if use_task_scoped and task_id:
        video_name = f"video_{task_id}.mp4"
    else:
        video_name = str(_cfg_get(cfg, "run.video_dump", "atlas_task_video.mp4"))
    timeout_sec = int(_cfg_get(cfg, "gemini.video_download_timeout_sec", 180))
    require_video = bool(_cfg_get(cfg, "gemini.require_video", False))
    min_video_bytes = int(_cfg_get(cfg, "gemini.min_video_bytes", 500000))
    resume_from_artifacts = bool(_cfg_get(cfg, "run.resume_from_artifacts", True))

    primary_target = out_dir / video_name
    if resume_from_artifacts and primary_target.exists():
        try:
            size_bytes = primary_target.stat().st_size
            if size_bytes >= min_video_bytes and _is_probably_mp4(primary_target):
                size_mb = size_bytes / (1024 * 1024)
                print(f"[video] reusing existing file: {primary_target} ({size_mb:.1f} MB)")
                return primary_target
        except Exception:
            pass

    # Let the player/network settle a bit.
    page.wait_for_timeout(1500)
    _dismiss_blocking_modals(page)
    try:
        # Nudge playback to trigger actual media requests if lazy-loaded.
        page.evaluate(
            """() => {
                const v = document.querySelector('video');
                if (!v) return;
                try { v.muted = true; v.play(); } catch (e) {}
            }"""
        )
        page.wait_for_timeout(1200)
        page.evaluate(
            """() => {
                const v = document.querySelector('video');
                if (!v) return;
                try { v.pause(); } catch (e) {}
            }"""
        )
    except Exception:
        pass
    candidates = _collect_video_url_candidates(page, cfg)
    print(f"[video] candidate urls found: {len(candidates)}")
    for u in candidates[:5]:
        print(f"[video] candidate: {u}")

    last_err: Optional[Exception] = None
    for idx, url in enumerate(candidates[:20], start=1):
        target = primary_target
        if idx > 1:
            stem = Path(video_name).stem
            suffix = Path(video_name).suffix or ".mp4"
            target = out_dir / f"{stem}_{idx}{suffix}"
        try:
            _download_video_from_page_context(page=page, context=context, video_url=url, out_path=target, timeout_sec=timeout_sec)
            size_bytes = target.stat().st_size
            size_mb = size_bytes / (1024 * 1024)
            if size_bytes < min_video_bytes:
                print(f"[video] skip candidate (too small {size_bytes} bytes): {url}")
                continue
            if not _is_probably_mp4(target):
                print(f"[video] skip candidate (not mp4 signature): {url}")
                continue
            print(f"[video] downloaded: {target} ({size_mb:.1f} MB)")
            return target
        except Exception as exc:
            last_err = exc
            continue

    if require_video:
        raise RuntimeError(f"Could not download task video from page. Last error: {last_err}")
    print("[video] no downloadable video found; proceeding with text-only prompt.")
    return None


def _wait_for_any(page: Page, selector: str, timeout_ms: int = 8000) -> bool:
    deadline = time.time() + timeout_ms / 1000.0
    while time.time() < deadline:
        if _any_locator_exists(page, selector):
            return True
        time.sleep(0.2)
    return False


def _dismiss_blocking_modals(page: Page) -> None:
    modal_buttons = [
        'button:has-text("I Understand")',
        'button:has-text("Understand")',
        'button:has-text("OK")',
        'button:has-text("Okay")',
        'button:has-text("Got It")',
        'button:has-text("Accept")',
        'button:has-text("Dismiss")',
        'text=/I\\s*Understand/i',
        'text=/\\bunderstand\\b/i',
        'text=/\\bok\\b/i',
        'text=/\\bokay\\b/i',
        '[role="button"]:has-text("I Understand")',
        '[role="button"]:has-text("Understand")',
        '[role="button"]:has-text("OK")',
        '[role="button"]:has-text("Okay")',
        'button:has-text("Close")',
        'text=/\\bClose\\b/i',
        'button:has-text("Got it")',
        'text=/Got\\s*it/i',
        'button:has-text("Continue")',
        'text=/\\bContinue\\b/i',
    ]
    for _ in range(5):
        clicked_any = False
        for sel in modal_buttons:
            if _safe_locator_click(page, sel, timeout_ms=1200):
                clicked_any = True
        if not clicked_any:
            # Fallback JS click by visible text content.
            try:
                clicked_any = bool(
                    page.evaluate(
                        """() => {
                            const nodes = Array.from(document.querySelectorAll('button,[role="button"],a,div'));
                            for (const n of nodes) {
                                const t = (n.innerText || n.textContent || '').trim().toLowerCase();
                                if (!t) continue;
                                if (
                                    t.includes('understand') ||
                                    t === 'ok' ||
                                    t === 'okay' ||
                                    t === 'close' ||
                                    t.includes('got it') ||
                                    t === 'continue' ||
                                    t === 'accept' ||
                                    t === 'dismiss'
                                ) {
                                    n.click();
                                    return true;
                                }
                            }
                            return false;
                        }"""
                    )
                )
            except Exception:
                clicked_any = False
        if clicked_any:
            page.wait_for_timeout(600)
        else:
            break


def _dismiss_blocking_side_panel(page: Page, cfg: Dict[str, Any], aggressive: bool = False) -> bool:
    panel_sel = str(
        _cfg_get(
            cfg,
            "atlas.selectors.blocking_side_panel",
            'div[class*="fixed"][class*="right-4"][class*="z-50"][class*="slide-in-from-right"] || '
            'div[class*="fixed"][class*="right-4"][class*="z-50"][class*="shadow-2xl"]',
        )
    )
    close_sel = str(
        _cfg_get(
            cfg,
            "atlas.selectors.blocking_side_panel_close",
            'button:has-text("Close") || button:has-text("Dismiss") || button:has-text("Done") || '
            'button:has-text("Cancel") || [role="button"]:has-text("Close") || '
            'button[aria-label*="close" i] || button[title*="close" i]',
        )
    )
    changed = False
    panel_variants = _selector_variants(panel_sel)
    close_variants = _selector_variants(close_sel)

    for panel_variant in panel_variants:
        try:
            panel_loc = page.locator(panel_variant)
            count = min(panel_loc.count(), 4)
        except Exception:
            continue
        for i in range(count):
            panel = panel_loc.nth(i)
            try:
                if not panel.is_visible():
                    continue
            except Exception:
                continue
            for close_variant in close_variants:
                try:
                    btn = panel.locator(close_variant).first
                    if btn.count() <= 0 or not btn.is_visible():
                        continue
                    btn.click(timeout=700)
                    changed = True
                except Exception:
                    continue

    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(80)
    except Exception:
        pass

    if changed:
        page.wait_for_timeout(150)
        return True

    panel_present = False
    for panel_variant in panel_variants:
        try:
            loc = page.locator(panel_variant)
            if loc.count() <= 0:
                continue
            scan = min(loc.count(), 3)
            for i in range(scan):
                if loc.nth(i).is_visible():
                    panel_present = True
                    break
            if panel_present:
                break
        except Exception:
            continue

    if aggressive or panel_present:
        try:
            hidden = int(
                page.evaluate(
                    """() => {
                        let hidden = 0;
                        const nodes = Array.from(document.querySelectorAll('div,aside,section'));
                        for (const el of nodes) {
                            if (!el || typeof el.className !== 'string') continue;
                            const cls = el.className;
                            if (!cls.includes('fixed') || !cls.includes('right-4') || !cls.includes('z-50')) continue;
                            if (!(cls.includes('slide-in-from-right') || cls.includes('shadow-2xl'))) continue;
                            const style = window.getComputedStyle(el);
                            if ((style.position || '') !== 'fixed') continue;
                            if (style.display === 'none' || style.visibility === 'hidden') continue;
                            const rect = el.getBoundingClientRect();
                            if (rect.width < 260 || rect.width > 520 || rect.height < 120) continue;
                            if (rect.left < window.innerWidth * 0.45) continue;
                            el.setAttribute('data-codex-hidden-overlay', '1');
                            el.style.pointerEvents = 'none';
                            el.style.display = 'none';
                            hidden += 1;
                        }
                        return hidden;
                    }"""
                )
            )
            if hidden > 0:
                print(f"[atlas] neutralized blocking side panel(s): {hidden}")
                return True
        except Exception:
            pass
    return changed


def _click_segment_row_with_recovery(page: Page, rows: Locator, idx: int, cfg: Dict[str, Any]) -> None:
    last_exc: Exception | None = None
    for attempt in range(4):
        row = rows.nth(idx - 1)
        try:
            row.scroll_into_view_if_needed()
            row.click(timeout=2200)
            return
        except Exception as exc:
            last_exc = exc
            _dismiss_blocking_modals(page)
            _dismiss_blocking_side_panel(page, cfg, aggressive=(attempt >= 1))
            try:
                row = rows.nth(idx - 1)
                row.click(timeout=1200, force=True)
                return
            except Exception as force_exc:
                last_exc = force_exc
                page.wait_for_timeout(120 + attempt * 120)
    raise RuntimeError(str(last_exc) if last_exc else "failed to focus segment row")


def _respect_reserve_cooldown(cfg: Dict[str, Any]) -> None:
    global _LAST_RESERVE_REQUEST_TS
    cooldown_sec = max(0, int(_cfg_get(cfg, "run.reserve_cooldown_sec", 120)))
    if cooldown_sec <= 0:
        return
    if _LAST_RESERVE_REQUEST_TS <= 0:
        return
    elapsed = time.time() - _LAST_RESERVE_REQUEST_TS
    remaining = cooldown_sec - elapsed
    if remaining > 0:
        print(f"[atlas] waiting {int(remaining)}s before reserve request (cooldown).")
        time.sleep(remaining)


def _mark_reserve_request() -> None:
    global _LAST_RESERVE_REQUEST_TS
    _LAST_RESERVE_REQUEST_TS = time.time()


def _ensure_loop_off(page: Page, cfg: Dict[str, Any]) -> None:
    loop_sel = str(_cfg_get(cfg, "atlas.selectors.loop_toggle_button", "")).strip()
    if loop_sel:
        loop_loc = _first_visible_locator(page, loop_sel, timeout_ms=2200)
        if loop_loc is not None:
            try:
                txt = (_safe_locator_text(loop_loc, timeout_ms=700) or "").lower()
                title = (loop_loc.get_attribute("title") or "").lower()
                classes = (loop_loc.get_attribute("class") or "").lower()
                aria_pressed = (loop_loc.get_attribute("aria-pressed") or "").lower()
                should_toggle = False
                if "loop on" in txt:
                    should_toggle = True
                elif "toggle segment loop" in title and ("bg-primary" in classes or aria_pressed == "true"):
                    should_toggle = True
                if should_toggle:
                    loop_loc.click()
                    print("[video] loop toggled OFF.")
            except Exception:
                pass
    try:
        page.evaluate(
            """() => {
                const v = document.querySelector('video');
                if (v) v.loop = false;
            }"""
        )
    except Exception:
        pass


def _play_full_video_once(page: Page, cfg: Dict[str, Any]) -> None:
    if not bool(_cfg_get(cfg, "run.play_full_video_before_labeling", False)):
        return
    max_wait_sec = max(10, int(_cfg_get(cfg, "run.play_full_video_max_wait_sec", 900)))
    _ensure_loop_off(page, cfg)
    try:
        st = page.evaluate(
            """() => {
                const v = document.querySelector('video');
                if (!v) return null;
                try { v.loop = false; } catch (e) {}
                try { v.muted = true; } catch (e) {}
                try { v.playbackRate = 1; } catch (e) {}
                try { v.play(); } catch (e) {}
                return {
                    current: Number(v.currentTime || 0),
                    duration: Number(v.duration || 0)
                };
            }"""
        )
    except Exception:
        st = None
    if not st:
        print("[video] video element not found; skipping full-video playback step.")
        return

    current = float(st.get("current", 0) or 0)
    duration = float(st.get("duration", 0) or 0)
    if duration <= 0:
        print("[video] unknown duration; skipping full-video playback wait.")
        return

    wait_budget = min(max_wait_sec, max(5, int(duration - current + 3)))
    print(f"[video] playing video to end (duration={duration:.1f}s, wait_budget={wait_budget}s).")
    start = time.time()
    last_log = -999.0
    while time.time() - start < wait_budget:
        try:
            state = page.evaluate(
                """() => {
                    const v = document.querySelector('video');
                    if (!v) return null;
                    return {
                        ended: !!v.ended,
                        current: Number(v.currentTime || 0),
                        duration: Number(v.duration || 0)
                    };
                }"""
            )
        except Exception:
            state = None
        if not state:
            break
        cur = float(state.get("current", 0) or 0)
        dur = float(state.get("duration", 0) or 0)
        ended = bool(state.get("ended", False))
        if ended or (dur > 0 and cur >= dur - 0.2):
            print(f"[video] playback reached end at {cur:.1f}/{dur:.1f}s.")
            break
        elapsed = time.time() - start
        if elapsed - last_log >= 15:
            last_log = elapsed
            print(f"[video] playback progress: {cur:.1f}/{dur:.1f}s")
        page.wait_for_timeout(1000)
    try:
        page.evaluate(
            """() => {
                const v = document.querySelector('video');
                if (v) v.pause();
            }"""
        )
    except Exception:
        pass
    _ensure_loop_off(page, cfg)


def _task_id_from_url(url: str) -> str:
    m = re.search(r"/tasks/room/normal/label/([A-Za-z0-9]+)", url or "")
    return m.group(1) if m else ""


def _task_scoped_artifact_paths(cfg: Dict[str, Any], task_id: str) -> Dict[str, Path]:
    out_dir = Path(str(_cfg_get(cfg, "run.output_dir", "outputs")))
    out_dir.mkdir(parents=True, exist_ok=True)
    task = (task_id or "").strip() or "unknown_task"
    return {
        "video": out_dir / f"video_{task}.mp4",
        "text_current": out_dir / f"text_{task}_current.txt",
        "text_update": out_dir / f"text_{task}_update.txt",
        "segments_cache": out_dir / f"segments_{task}.json",
        "labels_dump": out_dir / f"labels_{task}.json",
        "prompt_dump": out_dir / f"prompt_{task}.txt",
        "state": out_dir / f"task_state_{task}.json",
    }


def _load_task_state(cfg: Dict[str, Any], task_id: str) -> Dict[str, Any]:
    if not task_id:
        return {}
    state_path = _task_scoped_artifact_paths(cfg, task_id)["state"]
    if not state_path.exists():
        return {}
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if isinstance(data, dict):
        return data
    return {}


def _save_task_state(cfg: Dict[str, Any], task_id: str, state: Dict[str, Any]) -> None:
    if not task_id:
        return
    state_path = _task_scoped_artifact_paths(cfg, task_id)["state"]
    try:
        _ensure_parent(state_path)
        state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _load_cached_segments(cfg: Dict[str, Any], task_id: str) -> Optional[List[Dict[str, Any]]]:
    if not task_id:
        return None
    seg_path = _task_scoped_artifact_paths(cfg, task_id)["segments_cache"]
    if not seg_path.exists():
        return None
    try:
        data = json.loads(seg_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if isinstance(data, dict) and isinstance(data.get("segments"), list):
        return data["segments"]
    if isinstance(data, list):
        return data
    return None


def _save_cached_segments(cfg: Dict[str, Any], task_id: str, segments: List[Dict[str, Any]]) -> None:
    if not task_id:
        return
    seg_path = _task_scoped_artifact_paths(cfg, task_id)["segments_cache"]
    try:
        _ensure_parent(seg_path)
        seg_path.write_text(json.dumps({"segments": segments}, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _save_task_text_files(
    cfg: Dict[str, Any],
    task_id: str,
    segments: List[Dict[str, Any]],
    segment_plan: Dict[int, Dict[str, Any]],
) -> None:
    if not task_id:
        return
    paths = _task_scoped_artifact_paths(cfg, task_id)
    current_lines: List[str] = []
    update_lines: List[str] = []
    by_idx_src: Dict[int, Dict[str, Any]] = {}
    for seg in segments:
        try:
            idx = int(seg.get("segment_index", 0))
        except Exception:
            continue
        by_idx_src[idx] = seg
    for idx in sorted(by_idx_src):
        src = by_idx_src[idx]
        cur_label = str(src.get("current_label", "")).strip()
        cur_start = src.get("start_sec", 0.0)
        cur_end = src.get("end_sec", 0.0)
        current_lines.append(f"{idx}\t{cur_start}\t{cur_end}\t{cur_label}")
        planned = segment_plan.get(idx) or {}
        upd_label = str(planned.get("label", cur_label)).strip()
        upd_start = planned.get("start_sec", cur_start)
        upd_end = planned.get("end_sec", cur_end)
        update_lines.append(f"{idx}\t{upd_start}\t{upd_end}\t{upd_label}")
    try:
        paths["text_current"].write_text("\n".join(current_lines) + ("\n" if current_lines else ""), encoding="utf-8")
        print(f"[out] text current: {paths['text_current']}")
    except Exception:
        pass
    try:
        paths["text_update"].write_text("\n".join(update_lines) + ("\n" if update_lines else ""), encoding="utf-8")
        print(f"[out] text update: {paths['text_update']}")
    except Exception:
        pass


def _labels_cache_path(cfg: Dict[str, Any], task_id: str) -> Optional[Path]:
    if not task_id:
        return None
    out_dir = Path(str(_cfg_get(cfg, "run.output_dir", "outputs")))
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"gemini_labels_cache_{task_id}.json"


def _load_cached_labels(cfg: Dict[str, Any], task_id: str) -> Optional[Dict[str, Any]]:
    if not bool(_cfg_get(cfg, "run.reuse_cached_labels", True)):
        return None
    cache_path = _labels_cache_path(cfg, task_id)
    if cache_path is None or not cache_path.exists():
        return None
    try:
        data = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if isinstance(data, dict) and isinstance(data.get("segments"), list):
        print(f"[gemini] using cached labels for task {task_id}: {cache_path}")
        return data
    return None


def _save_cached_labels(cfg: Dict[str, Any], task_id: str, payload: Dict[str, Any]) -> None:
    cache_path = _labels_cache_path(cfg, task_id)
    if cache_path is None:
        return
    try:
        cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[gemini] cached labels: {cache_path}")
    except Exception:
        pass


def _decode_mime_header(value: str) -> str:
    if not value:
        return ""
    out: List[str] = []
    for chunk, enc in decode_header(value):
        if isinstance(chunk, bytes):
            charset = enc or "utf-8"
            try:
                out.append(chunk.decode(charset, errors="ignore"))
            except Exception:
                out.append(chunk.decode("utf-8", errors="ignore"))
        else:
            out.append(str(chunk))
    return "".join(out).strip()


def _message_to_text(msg: Message) -> str:
    parts: List[str] = []
    if msg.is_multipart():
        walk = msg.walk()
    else:
        walk = [msg]

    for part in walk:
        ctype = (part.get_content_type() or "").lower()
        disp = (part.get("Content-Disposition") or "").lower()
        if "attachment" in disp:
            continue
        if ctype not in {"text/plain", "text/html"}:
            continue
        payload = part.get_payload(decode=True) or b""
        charset = part.get_content_charset() or "utf-8"
        try:
            text = payload.decode(charset, errors="ignore")
        except Exception:
            text = payload.decode("utf-8", errors="ignore")
        if ctype == "text/html":
            text = re.sub(r"(?is)<script.*?>.*?</script>|<style.*?>.*?</style>", " ", text)
            text = re.sub(r"(?is)<[^>]+>", " ", text)
            text = html.unescape(text)
        parts.append(text)
    return "\n".join(parts).strip()


def _extract_otp_from_messages(
    rows: List[Tuple[datetime, str, str, str]],
    code_regex: str,
    sender_hint: str,
    subject_hint: str,
    not_before: datetime,
) -> str:
    sender_hint = sender_hint.strip().lower()
    subject_hint = subject_hint.strip().lower()
    regex = re.compile(code_regex)

    for msg_dt, sender, subject, body in rows:
        if msg_dt < not_before:
            continue
        if sender_hint and sender_hint not in sender.lower():
            continue
        if subject_hint and subject_hint not in subject.lower():
            continue
        hay = "\n".join([subject, body])
        m = regex.search(hay)
        if m:
            return m.group(1) if m.groups() else m.group(0)
    return ""


def _imap_login_from_cfg(cfg: Dict[str, Any]) -> Tuple[str, int, str, str]:
    otp_cfg = _cfg_get(cfg, "otp", {})
    host = str(otp_cfg.get("imap_host", "imap.gmail.com"))
    port = int(otp_cfg.get("imap_port", 993))
    user = _resolve_secret(str(otp_cfg.get("gmail_email", "")), ["ATLAS_LOGIN_EMAIL", "GMAIL_EMAIL"])
    password = _resolve_secret(
        str(otp_cfg.get("gmail_app_password", "")),
        ["ATLAS_GMAIL_APP_PASSWORD", "GMAIL_APP_PASSWORD"],
    )
    # Gmail app passwords are often shown in grouped blocks (with spaces); normalize them.
    password = re.sub(r"\s+", "", password or "")
    if not user or not password:
        raise RuntimeError(
            "Missing Gmail IMAP credentials. Set otp.gmail_email + otp.gmail_app_password "
            "or env vars GMAIL_EMAIL + GMAIL_APP_PASSWORD."
        )
    return host, port, user, password


def _get_gmail_uid_watermark(cfg: Dict[str, Any]) -> Optional[int]:
    try:
        host, port, user, password = _imap_login_from_cfg(cfg)
    except Exception:
        return None

    imap = imaplib.IMAP4_SSL(host, port)
    try:
        imap.login(user, password)
        otp_cfg = _cfg_get(cfg, "otp", {})
        mailbox = str(otp_cfg.get("mailbox", "[Gmail]/All Mail")).strip() or "[Gmail]/All Mail"
        selected = _select_imap_mailbox(imap, mailbox)
        print(f"[otp] watermark mailbox: {selected}")
        status, data = imap.uid("search", None, "ALL")
        if status != "OK" or not data or not data[0]:
            return None
        parts = [p for p in data[0].split() if p]
        if not parts:
            return None
        try:
            return int(parts[-1].decode("utf-8", errors="ignore"))
        except Exception:
            return None
    except Exception:
        return None
    finally:
        try:
            imap.logout()
        except Exception:
            pass


def _extract_mailbox_name_from_list_line(line: str) -> str:
    line = line.strip()
    # Usually: (<flags>) "<delimiter>" "<mailbox>"
    m = re.search(r'"([^"]+)"\s*$', line)
    if m:
        return m.group(1).strip()
    parts = line.split(" ")
    if parts:
        return parts[-1].strip('"').strip()
    return ""


def _select_imap_mailbox(imap: imaplib.IMAP4_SSL, preferred: str) -> str:
    candidates: List[str] = []
    if preferred:
        candidates.append(preferred)
    candidates.extend(["INBOX", "[Gmail]/All Mail", "All Mail", "[Gmail]/Spam", "Spam"])

    status, data = imap.list()
    if status == "OK" and data:
        auto_all: List[str] = []
        for row in data:
            line = row.decode("utf-8", errors="ignore") if isinstance(row, bytes) else str(row)
            name = _extract_mailbox_name_from_list_line(line)
            if not name:
                continue
            if "\\All" in line or "all mail" in name.lower():
                auto_all.append(name)
            candidates.append(name)
        # Prefer folders flagged as \All.
        candidates = auto_all + candidates

    seen = set()
    ordered: List[str] = []
    for c in candidates:
        key = c.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(c.strip())

    for mailbox in ordered:
        for attempt in (mailbox, f'"{mailbox}"'):
            try:
                status, _ = imap.select(attempt)
                if status == "OK":
                    return attempt
            except Exception:
                continue
    raise RuntimeError("Could not select a readable mailbox for OTP search.")


def _fetch_otp_gmail_imap(cfg: Dict[str, Any], started_at_unix: float, min_uid: Optional[int] = None) -> str:
    host, port, user, password = _imap_login_from_cfg(cfg)
    otp_cfg = _cfg_get(cfg, "otp", {})
    mailbox = str(otp_cfg.get("mailbox", "[Gmail]/All Mail")).strip() or "[Gmail]/All Mail"

    timeout_sec = int(otp_cfg.get("timeout_sec", 120))
    poll_sec = max(1.0, float(otp_cfg.get("poll_interval_sec", 4)))
    max_messages = max(5, int(otp_cfg.get("max_messages", 25)))
    unseen_only = bool(otp_cfg.get("unseen_only", False))
    sender_hint = str(otp_cfg.get("sender_hint", ""))
    subject_hint = str(otp_cfg.get("subject_hint", ""))
    code_regex = str(otp_cfg.get("code_regex", r"\b(\d{6})\b"))
    lookback_sec = max(0, int(otp_cfg.get("lookback_sec", 300)))

    started_at = datetime.fromtimestamp(started_at_unix, tz=timezone.utc).replace(microsecond=0)
    not_before = started_at - timedelta(seconds=lookback_sec)
    deadline = time.time() + timeout_sec
    print(f"[otp] polling Gmail for OTP (timeout={timeout_sec}s)")

    while time.time() < deadline:
        rows: List[Tuple[datetime, str, str, str]] = []
        imap = imaplib.IMAP4_SSL(host, port)
        try:
            try:
                imap.login(user, password)
            except imaplib.IMAP4.error as exc:
                msg = str(exc)
                lowered = msg.lower()
                if "application-specific password required" in lowered or "app password" in lowered:
                    raise RuntimeError(
                        "Gmail IMAP login failed: App Password required. "
                        "Enable 2-Step Verification on this Google account, then create a 16-char App Password "
                        "and put it in otp.gmail_app_password (or env GMAIL_APP_PASSWORD)."
                    ) from exc
                raise RuntimeError(f"Gmail IMAP login failed: {msg}") from exc
            selected = _select_imap_mailbox(imap, mailbox)
            criteria = "UNSEEN" if unseen_only else "ALL"
            status, data = imap.uid("search", None, criteria)
            if status != "OK":
                raise RuntimeError(f"IMAP search failed: {status}")
            uid_items = [u for u in (data[0].split() if data and data[0] else []) if u]
            uids: List[str] = []
            for raw_uid in uid_items:
                try:
                    uid_int = int(raw_uid.decode("utf-8", errors="ignore"))
                except Exception:
                    continue
                if min_uid is not None and uid_int <= int(min_uid):
                    continue
                uids.append(str(uid_int))
            uids = uids[-max_messages:]

            for uid in reversed(uids):
                status, fetched = imap.uid("fetch", uid, "(RFC822 INTERNALDATE)")
                if status != "OK":
                    continue
                raw_bytes = b""
                msg_dt = datetime.now(timezone.utc)
                for entry in fetched:
                    if not isinstance(entry, tuple):
                        continue
                    if isinstance(entry[1], bytes):
                        raw_bytes = entry[1]
                    header_text = (entry[0] or b"").decode("utf-8", errors="ignore")
                    m = re.search(r'INTERNALDATE \"([^\"]+)\"', header_text)
                    if m:
                        try:
                            dt = parsedate_to_datetime(m.group(1))
                            msg_dt = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
                        except Exception:
                            pass
                if not raw_bytes:
                    continue

                msg = message_from_bytes(raw_bytes)
                subject = _decode_mime_header(msg.get("Subject", ""))
                sender = _decode_mime_header(msg.get("From", ""))
                body = _message_to_text(msg)
                rows.append((msg_dt, sender, subject, body))

            if not rows:
                time.sleep(poll_sec)
                continue

            code = _extract_otp_from_messages(
                rows=rows,
                code_regex=code_regex,
                sender_hint=sender_hint,
                subject_hint=subject_hint,
                not_before=not_before,
            )
            if not code:
                # Fallback: relax sender/subject hints to catch template changes.
                code = _extract_otp_from_messages(
                    rows=rows,
                    code_regex=code_regex,
                    sender_hint="",
                    subject_hint="",
                    not_before=not_before,
                )
            if code:
                print("[otp] OTP found.")
                return code
        finally:
            try:
                imap.logout()
            except Exception:
                pass
        time.sleep(poll_sec)

    raise TimeoutError("OTP not found in Gmail within timeout.")


def _resolve_otp_code(cfg: Dict[str, Any], started_at_unix: float, min_uid: Optional[int] = None) -> str:
    provider = _otp_provider(cfg)
    if provider in {"manual", "manual_browser", "browser", "none"}:
        return ""
    if provider in {"gmail_imap", "imap"}:
        return _fetch_otp_gmail_imap(cfg, started_at_unix, min_uid=min_uid)
    raise ValueError(f"Unsupported otp.provider: {provider}")


def _body_has_rate_limit(page: Page) -> bool:
    try:
        text = (page.inner_text("body") or "").lower()
    except Exception:
        return False
    return "too many request" in text or "rate limit" in text


def _wait_until_authenticated(page: Page, cfg: Dict[str, Any], timeout_sec: int) -> None:
    tasks_nav = str(_cfg_get(cfg, "atlas.selectors.tasks_nav", ""))
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        current = page.url.lower()
        is_loginish = "/login" in current or "/verify" in current
        if ("/dashboard" in current or "/tasks" in current) and not is_loginish:
            print(f"[auth] authenticated at {page.url}")
            return
        if tasks_nav and _any_locator_exists(page, tasks_nav) and not is_loginish:
            print(f"[auth] authenticated (tasks nav visible) at {page.url}")
            return
        if _body_has_rate_limit(page):
            raise RuntimeError("Atlas login is rate-limited: too many requests.")
        time.sleep(0.5)
    raise TimeoutError(f"Authentication timeout after {timeout_sec}s.")


def ensure_logged_in(page: Page, cfg: Dict[str, Any]) -> None:
    email = _resolve_atlas_email(cfg)
    login_url = str(_cfg_get(cfg, "atlas.login_url", DEFAULT_CONFIG["atlas"]["login_url"]))
    timeout_sec = int(_cfg_get(cfg, "atlas.auth_timeout_sec", 180))

    email_sel = str(_cfg_get(cfg, "atlas.selectors.email_input", ""))
    start_sel = str(_cfg_get(cfg, "atlas.selectors.start_button", ""))
    otp_sel = str(_cfg_get(cfg, "atlas.selectors.otp_input", ""))
    verify_sel = str(_cfg_get(cfg, "atlas.selectors.verify_button", ""))

    print(f"[auth] open login page: {login_url}")
    page.goto(login_url, wait_until="domcontentloaded")
    if "/dashboard" in page.url.lower() or "/tasks" in page.url.lower():
        print("[auth] already logged in via existing session.")
        return

    if _body_has_rate_limit(page):
        raise RuntimeError("Atlas login is rate-limited: 'Too many requests have been made'.")

    otp_uid_watermark: Optional[int] = None
    if email:
        if not _otp_is_manual(cfg):
            otp_uid_watermark = _get_gmail_uid_watermark(cfg)
            if otp_uid_watermark is not None:
                print(f"[otp] inbox uid watermark before request: {otp_uid_watermark}")
        if not _safe_fill(page, email_sel, email, timeout_ms=8000):
            raise RuntimeError("Could not fill Atlas email input.")
        if not _safe_locator_click(page, start_sel, timeout_ms=8000):
            raise RuntimeError("Could not click Atlas start button.")
    else:
        print("[auth] atlas.email not set; relying on existing logged-in session/profile only.")
        if "/login" in page.url.lower() or "/verify" in page.url.lower():
            raise RuntimeError(
                "No authenticated session found and atlas.email is empty. "
                "Set ATLAS_LOGIN_EMAIL (or atlas.email) only when you want to submit a fresh login."
            )
        _wait_until_authenticated(page, cfg, timeout_sec=timeout_sec)
        return

    page.wait_for_timeout(1200)
    body_text = page.inner_text("body").lower()
    if "too many request" in body_text:
        raise RuntimeError("Atlas login is rate-limited: 'Too many requests have been made'.")
    if "applications are not currently open" in body_text or "join waitlist" in body_text:
        raise RuntimeError("Login stopped at waitlist page. This account cannot continue automatically.")

    started_at = time.time()
    if _wait_for_any(page, otp_sel, timeout_ms=20000) or "/verify" in page.url.lower():
        if _otp_is_manual(cfg):
            print("[otp] manual mode: enter OTP in the opened browser window, then script will continue.")
        else:
            code = _resolve_otp_code(cfg, started_at, min_uid=otp_uid_watermark)
            if not _safe_fill(page, otp_sel, code, timeout_ms=8000):
                raise RuntimeError("Could not fill OTP code.")
            if not _safe_locator_click(page, verify_sel, timeout_ms=8000):
                raise RuntimeError("Could not click Verify button.")

    _wait_until_authenticated(page, cfg, timeout_sec=timeout_sec)


def goto_task_room(page: Page, cfg: Dict[str, Any]) -> bool:
    room_url = str(_cfg_get(cfg, "atlas.room_url", "")).strip()
    dashboard_url = str(_cfg_get(cfg, "atlas.dashboard_url", "")).strip()
    wait_sec = float(_cfg_get(cfg, "atlas.wait_before_continue_sec", 5))

    tasks_nav = str(_cfg_get(cfg, "atlas.selectors.tasks_nav", ""))
    enter_workflow = str(_cfg_get(cfg, "atlas.selectors.enter_workflow_button", ""))
    continue_room = str(_cfg_get(cfg, "atlas.selectors.continue_room_button", ""))
    label_button = str(_cfg_get(cfg, "atlas.selectors.label_button", ""))
    label_task_link = str(_cfg_get(cfg, "atlas.selectors.label_task_link", ""))
    reserve_btn = str(_cfg_get(cfg, "atlas.selectors.reserve_episodes_button", ""))
    confirm_reserve_btn = str(_cfg_get(cfg, "atlas.selectors.confirm_reserve_button", ""))

    if "/tasks/room/normal/label/" in page.url:
        return True

    if room_url:
        print(f"[atlas] goto room url: {room_url}")
        page.goto(room_url, wait_until="domcontentloaded")
    elif dashboard_url:
        print(f"[atlas] goto dashboard url: {dashboard_url}")
        page.goto(dashboard_url, wait_until="domcontentloaded")

    _safe_locator_click(page, tasks_nav, timeout_ms=3000)
    _safe_locator_click(page, enter_workflow, timeout_ms=4000)
    if wait_sec > 0:
        time.sleep(wait_sec)
    _safe_locator_click(page, continue_room, timeout_ms=4000)
    _safe_locator_click(page, label_button, timeout_ms=4000)

    # In room view, reserve episodes if needed, then open first concrete label task URL.
    if label_task_link:
        page.wait_for_timeout(1000)
        href_from_html = _first_task_label_href_from_html(page)
        if href_from_html:
            target = href_from_html if href_from_html.startswith("http") else f"https://audit.atlascapture.io{href_from_html}"
            page.goto(target, wait_until="domcontentloaded")
            print(f"[atlas] opened label task by html href: {target}")
            for _ in range(8):
                _dismiss_blocking_modals(page)
                body = (page.inner_text("body") or "").lower()
                if "loading..." not in body:
                    break
                page.wait_for_timeout(1200)
            return True

        link_loc = _first_visible_locator(page, label_task_link, timeout_ms=2500)
        if link_loc is None:
            reserved = False
            if reserve_btn:
                reserve_loc = _first_visible_locator(page, reserve_btn, timeout_ms=2500)
                if reserve_loc is not None:
                    _respect_reserve_cooldown(cfg)
                    try:
                        reserve_loc.click()
                        reserved = True
                        _mark_reserve_request()
                        print("[atlas] reserve requested.")
                    except Exception:
                        reserved = False
            if reserved:
                _safe_locator_click(page, confirm_reserve_btn, timeout_ms=3500)
                _wait_for_any(page, label_task_link, timeout_ms=12000)
                page.wait_for_timeout(800)
                href_from_html = _first_task_label_href_from_html(page)
                if href_from_html:
                    target = href_from_html if href_from_html.startswith("http") else f"https://audit.atlascapture.io{href_from_html}"
                    page.goto(target, wait_until="domcontentloaded")
                    print(f"[atlas] opened label task by html href: {target}")
                    for _ in range(8):
                        _dismiss_blocking_modals(page)
                        body = (page.inner_text("body") or "").lower()
                        if "loading..." not in body:
                            break
                        page.wait_for_timeout(1200)
                    return True
                _safe_locator_click(page, label_button, timeout_ms=4000)
                _wait_for_any(page, label_task_link, timeout_ms=12000)
            link_loc = _first_visible_locator(page, label_task_link, timeout_ms=5000)

        if link_loc is None:
            href = _first_href_from_selector(page, label_task_link)
            if href:
                target = href if href.startswith("http") else f"https://audit.atlascapture.io{href}"
                page.goto(target, wait_until="domcontentloaded")
                print(f"[atlas] opened label task by href: {target}")
                return True

        if link_loc is not None:
            try:
                href = link_loc.get_attribute("href")
                link_loc.click()
                if href:
                    print(f"[atlas] opened label task: {href}")
                for _ in range(8):
                    _dismiss_blocking_modals(page)
                    body = (page.inner_text("body") or "").lower()
                    if "loading..." not in body:
                        break
                    page.wait_for_timeout(1200)
            except Exception:
                pass
            if "/tasks/room/normal/label/" in page.url:
                return True
            href = _first_href_from_selector(page, label_task_link)
            if href:
                target = href if href.startswith("http") else f"https://audit.atlascapture.io{href}"
                page.goto(target, wait_until="domcontentloaded")
                print(f"[atlas] opened label task by href fallback: {target}")
                return True

    return "/tasks/room/normal/label/" in page.url


def _parse_mmss_to_seconds(token: str) -> float:
    token = token.strip()
    if not token:
        return 0.0
    if ":" not in token:
        try:
            return float(token)
        except ValueError:
            return 0.0
    left, right = token.split(":", 1)
    try:
        return int(left) * 60 + float(right)
    except ValueError:
        return 0.0


def _extract_start_end_from_text(text: str) -> Tuple[float, float]:
    matches = re.findall(r"\b\d+:\d{2}(?:\.\d+)?\b", text or "")
    if len(matches) >= 2:
        return _parse_mmss_to_seconds(matches[0]), _parse_mmss_to_seconds(matches[1])
    return 0.0, 0.0


def _resolve_rows_locator(
    page: Page,
    rows_selector: str,
    sample_size: int = 8,
    row_text_timeout_ms: int = 350,
) -> Tuple[str, Locator]:
    best_sel = ""
    best_score = -1
    best_count = 0

    for candidate in _selector_variants(rows_selector):
        try:
            loc = page.locator(candidate)
            count = loc.count()
            if count <= 0:
                continue
            sample = min(count, max(1, sample_size))
            ts_hits = 0
            for i in range(sample):
                text = _safe_locator_text(loc.nth(i), timeout_ms=max(120, row_text_timeout_ms))
                if re.search(r"\b\d+:\d{2}(?:\.\d+)?\b", text):
                    ts_hits += 1
            score = ts_hits * 10 + min(count, 50)
            if score > best_score:
                best_score = score
                best_count = count
                best_sel = candidate
        except Exception:
            continue

    if not best_sel:
        diagnostics: List[str] = []
        diagnostics.append("No segment rows found. Candidate selector counts:")
        for candidate in _selector_variants(rows_selector):
            try:
                c = page.locator(candidate).count()
            except Exception:
                c = -1
            diagnostics.append(f"  - {candidate} => {c}")
        try:
            body = page.inner_text("body")
            body_snippet = (body or "")[:1200].replace("\n", " | ")
            diagnostics.append(f"Body snippet: {body_snippet}")
        except Exception:
            pass
        raise RuntimeError("\n".join(diagnostics))
    print(f"[atlas] using segment rows selector: {best_sel} (count={best_count})")
    return best_sel, page.locator(best_sel)


def _first_text_from_row(row: Locator, selector: str) -> str:
    for candidate in _selector_variants(selector):
        try:
            text = _safe_locator_text(row.locator(candidate).first, timeout_ms=700)
            if text:
                return text
        except Exception:
            continue
    return ""


def extract_segments(page: Page, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    max_segments = int(_cfg_get(cfg, "run.max_segments", 0) or 0)
    resolve_attempts = max(1, int(_cfg_get(cfg, "run.segment_resolve_attempts", 24)))
    resolve_retry_ms = max(150, int(_cfg_get(cfg, "run.segment_resolve_retry_ms", 800)))
    resolve_sample_size = max(1, int(_cfg_get(cfg, "run.segment_resolve_sample_size", 8)))
    resolve_row_text_timeout_ms = max(100, int(_cfg_get(cfg, "run.segment_resolve_row_text_timeout_ms", 350)))
    rows_sel = str(_cfg_get(cfg, "atlas.selectors.segment_rows", ""))
    label_sel = str(_cfg_get(cfg, "atlas.selectors.segment_label", ""))
    start_sel = str(_cfg_get(cfg, "atlas.selectors.segment_start", ""))
    end_sel = str(_cfg_get(cfg, "atlas.selectors.segment_end", ""))

    last_error: Exception | None = None
    rows = None
    for attempt in range(1, resolve_attempts + 1):
        _dismiss_blocking_modals(page)
        try:
            _, rows = _resolve_rows_locator(
                page,
                rows_sel,
                sample_size=resolve_sample_size,
                row_text_timeout_ms=resolve_row_text_timeout_ms,
            )
            break
        except Exception as exc:
            last_error = exc
            if attempt == 1 or attempt % 3 == 0 or attempt == resolve_attempts:
                msg = str(exc).strip().replace("\n", " | ")
                if len(msg) > 220:
                    msg = msg[:220] + "..."
                print(f"[atlas] segment rows not ready (attempt {attempt}/{resolve_attempts}): {msg}")
            page.wait_for_timeout(resolve_retry_ms)
    if rows is None:
        if last_error:
            raise last_error
        raise RuntimeError("Could not resolve segment rows.")

    count = rows.count()
    limit = count if max_segments <= 0 else min(count, max_segments)

    items: List[Dict[str, Any]] = []
    for i in range(limit):
        row = rows.nth(i)
        row.scroll_into_view_if_needed()
        raw_text = _safe_locator_text(row, timeout_ms=2000)

        label = _first_text_from_row(row, label_sel)
        start_sec = _parse_mmss_to_seconds(_first_text_from_row(row, start_sel))
        end_sec = _parse_mmss_to_seconds(_first_text_from_row(row, end_sel))

        if not start_sec and not end_sec:
            start_sec, end_sec = _extract_start_end_from_text(raw_text)

        if not label and raw_text:
            lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
            if lines:
                label = lines[-1]

        items.append(
            {
                "segment_index": i + 1,
                "start_sec": round(start_sec, 3),
                "end_sec": round(end_sec, 3),
                "current_label": label,
                "raw_text": raw_text,
            }
        )
    return items


def _clean_json_text(text: str) -> str:
    clean = re.sub(r"```json|```", "", text or "", flags=re.IGNORECASE).strip()
    start = clean.find("{")
    end = clean.rfind("}")
    if start >= 0 and end > start:
        return clean[start : end + 1]
    return clean


def _parse_json_text(text: str) -> Dict[str, Any]:
    payload = json.loads(_clean_json_text(text))
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list):
        return {"segments": payload}
    raise ValueError("Gemini response is not JSON object/list")


def _parse_gemini_response(data: Dict[str, Any]) -> Dict[str, Any]:
    for candidate in data.get("candidates", []):
        for part in candidate.get("content", {}).get("parts", []):
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                try:
                    return _parse_json_text(text)
                except Exception:
                    continue
    raise RuntimeError(f"Could not parse JSON from Gemini response: {data}")


def build_prompt(segments: List[Dict[str, Any]], extra_instructions: str) -> str:
    header = (
        "You are an Atlas labeling assistant.\n"
        "You may receive the full task video as attached media plus employee segment text.\n"
        "Use the video as source of truth; employee labels may be wrong.\n"
        "For each segment index, output corrected label.\n"
        "If a segment timestamp is wrong, correct start_sec/end_sec.\n"
        "Label rules: imperative style, concise, no forbidden verbs (inspect/check/reach/examine/continue).\n"
        "Use \"No Action\" only as standalone label.\n"
        "Return strict JSON object only:\n"
        "{\"segments\":[{\"segment_index\":1,\"start_sec\":0.0,\"end_sec\":1.2,\"label\":\"...\"}]}\n"
        "Keep segment count and indices unchanged; timestamps must stay monotonic.\n"
    )
    lines = ["Segments input:"]
    for seg in segments:
        lines.append(
            f"- segment_index={seg['segment_index']} start_sec={seg['start_sec']} "
            f"end_sec={seg['end_sec']} current_label={json.dumps(seg.get('current_label', ''), ensure_ascii=False)} "
            f"raw_text={json.dumps(seg.get('raw_text', ''), ensure_ascii=False)}"
        )
    if extra_instructions.strip():
        lines.append("")
        lines.append("Extra instructions:")
        lines.append(extra_instructions.strip())
    return header + "\n".join(lines)


def call_gemini_labels(cfg: Dict[str, Any], prompt: str, video_file: Optional[Path] = None) -> Dict[str, Any]:
    model = str(_cfg_get(cfg, "gemini.model", "gemini-2.5-flash"))
    api_key = _resolve_gemini_key(str(_cfg_get(cfg, "gemini.api_key", "")))
    if not api_key:
        raise RuntimeError("Missing Gemini API key (gemini.api_key or GEMINI_API_KEY/GOOGLE_API_KEY).")

    max_retries = max(0, int(_cfg_get(cfg, "gemini.max_retries", 3)))
    base_delay = max(0.5, float(_cfg_get(cfg, "gemini.retry_base_delay_sec", 2.0)))
    temperature = float(_cfg_get(cfg, "gemini.temperature", 0.0))
    connect_timeout_sec = max(5, int(_cfg_get(cfg, "gemini.connect_timeout_sec", 30)))
    request_timeout_sec = max(30, int(_cfg_get(cfg, "gemini.request_timeout_sec", 420)))
    require_video = bool(_cfg_get(cfg, "gemini.require_video", False))
    allow_text_fallback = bool(
        _cfg_get(cfg, "gemini.allow_text_only_fallback_on_network_error", True)
    )
    max_inline_video_mb = float(_cfg_get(cfg, "gemini.max_inline_video_mb", 20.0))

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    headers = {"Content-Type": "application/json", "X-goog-api-key": api_key}
    parts: List[Dict[str, Any]] = [{"text": prompt}]
    video_part: Optional[Dict[str, Any]] = None

    if video_file is not None and video_file.exists():
        size_mb = video_file.stat().st_size / (1024 * 1024)
        if size_mb > max_inline_video_mb:
            msg = (
                f"Video is {size_mb:.1f} MB which exceeds max_inline_video_mb={max_inline_video_mb:.1f}. "
                "Increase gemini.max_inline_video_mb or provide smaller video."
            )
            if require_video:
                raise RuntimeError(msg)
            print(f"[video] {msg} Proceeding without attachment.")
        else:
            b64_video = base64.b64encode(video_file.read_bytes()).decode("ascii")
            video_part = {"inline_data": {"mime_type": "video/mp4", "data": b64_video}}
            print(f"[gemini] attached video to request ({size_mb:.1f} MB).")
    else:
        if require_video:
            raise RuntimeError("gemini.require_video=true but no downloadable video file was prepared.")
    include_video = video_part is not None
    if include_video:
        parts.append(video_part)

    last_error = ""
    for attempt in range(max_retries + 1):
        mode = "with-video" if include_video else "text-only"
        print(f"[gemini] request attempt {attempt + 1}/{max_retries + 1} (model={model}, mode={mode})")
        payload = {
            "contents": [{"role": "user", "parts": parts}],
            "generationConfig": {"temperature": temperature, "responseMimeType": "application/json"},
        }
        try:
            resp = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=(connect_timeout_sec, request_timeout_sec),
            )
        except requests.exceptions.RequestException as exc:
            last_error = f"Gemini network error: {exc}"
            if include_video and not require_video and allow_text_fallback:
                include_video = False
                parts = [{"text": prompt}]
                print("[gemini] network error while sending video; switching to text-only fallback.")
                continue
            if attempt < max_retries:
                delay = base_delay * (2**attempt)
                print(f"[gemini] network error, retrying in {delay:.1f}s")
                time.sleep(delay)
                continue
            break

        if resp.status_code == 200:
            print("[gemini] response received (HTTP 200).")
            return _parse_gemini_response(resp.json())

        last_error = f"Gemini HTTP {resp.status_code}: {resp.text[:800]}"
        if (
            include_video
            and not require_video
            and allow_text_fallback
            and resp.status_code in {400, 408, 413, 422}
        ):
            include_video = False
            parts = [{"text": prompt}]
            print(
                f"[gemini] HTTP {resp.status_code} while using video; switching to text-only fallback."
            )
            continue
        if resp.status_code in {429, 500, 502, 503, 504} and attempt < max_retries:
            delay = base_delay * (2**attempt)
            print(f"[gemini] temporary error {resp.status_code}, retrying in {delay:.1f}s")
            time.sleep(delay)
            continue
        break
    raise RuntimeError(last_error or "Gemini request failed")


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _normalize_segment_plan(
    payload: Dict[str, Any],
    source_segments: List[Dict[str, Any]],
) -> Dict[int, Dict[str, Any]]:
    items = payload.get("segments")
    if not isinstance(items, list):
        raise ValueError("Gemini payload must contain list at 'segments'")

    source_by_idx: Dict[int, Dict[str, Any]] = {int(seg["segment_index"]): seg for seg in source_segments}
    out: Dict[int, Dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        idx_raw = item.get("segment_index", item.get("index"))
        try:
            idx = int(idx_raw)
        except (TypeError, ValueError):
            continue
        source = source_by_idx.get(idx)
        if source is None:
            continue
        label = str(item.get("label", "")).strip() or str(source.get("current_label", "")).strip()
        start_src = _safe_float(source.get("start_sec", 0.0), 0.0)
        end_src = _safe_float(source.get("end_sec", 0.0), 0.0)
        start_sec = _safe_float(item.get("start_sec", start_src), start_src)
        end_sec = _safe_float(item.get("end_sec", end_src), end_src)
        if end_sec <= start_sec:
            start_sec = start_src
            end_sec = end_src
        out[idx] = {
            "segment_index": idx,
            "label": label,
            "start_sec": round(start_sec, 3),
            "end_sec": round(end_sec, 3),
        }
    for idx, source in source_by_idx.items():
        if idx in out:
            continue
        out[idx] = {
            "segment_index": idx,
            "label": str(source.get("current_label", "")).strip(),
            "start_sec": round(_safe_float(source.get("start_sec", 0.0), 0.0), 3),
            "end_sec": round(_safe_float(source.get("end_sec", 0.0), 0.0), 3),
        }
    if not out:
        raise ValueError("Gemini returned no usable segment plan")
    return out


def _normalize_label_map_from_plan(segment_plan: Dict[int, Dict[str, Any]]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for idx, item in segment_plan.items():
        label = str(item.get("label", "")).strip()
        if label:
            out[idx] = label
    if not out:
        raise ValueError("Segment plan has no usable labels")
    return out


def _first_visible_child_locator(row: Locator, selector: str, max_scan: int = 10) -> Optional[Locator]:
    for candidate in _selector_variants(selector):
        try:
            loc = row.locator(candidate)
            count = min(loc.count(), max_scan)
            for i in range(count):
                item = loc.nth(i)
                if item.is_visible() and item.is_enabled():
                    return item
        except Exception:
            continue
    return None


def apply_timestamp_adjustments(
    page: Page,
    cfg: Dict[str, Any],
    source_segments: List[Dict[str, Any]],
    segment_plan: Dict[int, Dict[str, Any]],
) -> Dict[str, Any]:
    if not bool(_cfg_get(cfg, "run.adjust_timestamps", True)):
        return {"adjusted": 0, "failed": []}

    _dismiss_blocking_modals(page)
    _dismiss_blocking_side_panel(page, cfg, aggressive=True)

    rows_sel = str(_cfg_get(cfg, "atlas.selectors.segment_rows", ""))
    plus_sel = str(_cfg_get(cfg, "atlas.selectors.segment_time_plus_button", 'button:has(svg.lucide-plus)'))
    minus_sel = str(_cfg_get(cfg, "atlas.selectors.segment_time_minus_button", 'button:has(svg.lucide-minus)'))
    step_sec = max(0.01, float(_cfg_get(cfg, "atlas.timestamp_step_sec", 0.1)))
    max_clicks = max(1, int(_cfg_get(cfg, "atlas.timestamp_max_clicks_per_segment", 30)))

    try:
        best_rows_sel, rows = _resolve_rows_locator(page, rows_sel)
    except Exception:
        return {"adjusted": 0, "failed": ["rows locator unavailable for timestamp adjustment"]}

    source_by_idx: Dict[int, Dict[str, Any]] = {int(seg["segment_index"]): seg for seg in source_segments}
    adjusted = 0
    failed: List[str] = []

    for idx in sorted(segment_plan):
        rows = page.locator(best_rows_sel)
        count = rows.count()
        if idx > count:
            continue
        src = source_by_idx.get(idx)
        if not src:
            continue
        target_end = _safe_float(segment_plan[idx].get("end_sec"), _safe_float(src.get("end_sec"), 0.0))
        current_end = _safe_float(src.get("end_sec"), 0.0)
        diff = target_end - current_end
        if abs(diff) < (step_sec / 2):
            continue

        row = rows.nth(idx - 1)
        clicks = min(max_clicks, int(round(abs(diff) / step_sec)))
        if clicks <= 0:
            continue
        use_plus = diff > 0
        btn_sel = plus_sel if use_plus else minus_sel
        btn = _first_visible_child_locator(row, btn_sel)
        if btn is None:
            failed.append(f"segment {idx}: timestamp {'plus' if use_plus else 'minus'} button not found")
            continue
        try:
            _click_segment_row_with_recovery(page, rows, idx, cfg)
            for _ in range(clicks):
                live_row = page.locator(best_rows_sel).nth(idx - 1)
                live_btn = _first_visible_child_locator(live_row, btn_sel)
                if live_btn is None:
                    raise RuntimeError(
                        f"timestamp {'plus' if use_plus else 'minus'} button disappeared during adjustment"
                    )
                try:
                    live_btn.click(timeout=900)
                except Exception:
                    _dismiss_blocking_side_panel(page, cfg, aggressive=True)
                    live_btn.click(timeout=900, force=True)
                time.sleep(0.03)
            adjusted += 1
        except Exception as exc:
            failed.append(f"segment {idx}: {exc}")

    return {"adjusted": adjusted, "failed": failed}


def _fill_input(locator: Locator, label: str, page: Page) -> None:
    locator.wait_for(state="visible", timeout=4000)
    locator.click()
    try:
        editable = bool(locator.evaluate("el => !!el.isContentEditable"))
    except Exception:
        editable = False

    if editable:
        page.keyboard.press("Control+A")
        page.keyboard.type(label, delay=8)
        return
    try:
        locator.fill(label)
    except Exception:
        page.keyboard.press("Control+A")
        page.keyboard.type(label, delay=8)


def _normalize_label_for_compare(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip().lower()


def _filter_unchanged_label_map(
    label_map: Dict[int, str],
    source_segments: List[Dict[str, Any]],
) -> Tuple[Dict[int, str], int]:
    source_by_idx: Dict[int, str] = {
        int(seg.get("segment_index", 0)): str(seg.get("current_label", "")).strip()
        for seg in source_segments
    }
    out: Dict[int, str] = {}
    skipped = 0
    for idx, target in label_map.items():
        current = source_by_idx.get(int(idx), "")
        if current and _normalize_label_for_compare(current) == _normalize_label_for_compare(target):
            skipped += 1
            continue
        out[int(idx)] = target
    return out, skipped


def _handle_quality_review_modal(page: Page, cfg: Dict[str, Any], timeout_ms: int = 8000) -> bool:
    if not bool(_cfg_get(cfg, "run.enable_quality_review_submit", True)):
        return True

    modal_sel = str(_cfg_get(cfg, "atlas.selectors.quality_review_modal", "")).strip()
    checkbox_sel = str(_cfg_get(cfg, "atlas.selectors.quality_review_checkbox", "")).strip()
    submit_sel = str(_cfg_get(cfg, "atlas.selectors.quality_review_submit_button", "")).strip()
    if not modal_sel or not checkbox_sel or not submit_sel:
        return True

    modal = _first_visible_locator(page, modal_sel, timeout_ms=timeout_ms)
    if modal is None:
        return True

    checked = False
    for candidate in _selector_variants(checkbox_sel):
        try:
            loc = modal.locator(candidate)
            scan = min(loc.count(), 4)
            for i in range(scan):
                cb = loc.nth(i)
                if not cb.is_visible():
                    continue
                try:
                    tag = str(cb.evaluate("el => (el.tagName || '').toLowerCase()"))
                    typ = str(cb.evaluate("el => (el.getAttribute('type') || '').toLowerCase()"))
                except Exception:
                    tag, typ = "", ""
                try:
                    if tag == "input" and typ == "checkbox":
                        cb.check(timeout=1200, force=True)
                    else:
                        cb.click(timeout=1200, force=True)
                    checked = True
                    break
                except Exception:
                    continue
            if checked:
                break
        except Exception:
            continue
    if checked:
        page.wait_for_timeout(250)

    submitted = False
    for _ in range(5):
        submit_btn: Optional[Locator] = None
        for candidate in _selector_variants(submit_sel):
            try:
                loc = modal.locator(candidate)
                scan = min(loc.count(), 4)
                for i in range(scan):
                    btn = loc.nth(i)
                    if btn.is_visible():
                        submit_btn = btn
                        break
                if submit_btn is not None:
                    break
            except Exception:
                continue
        if submit_btn is None:
            break
        try:
            disabled = bool(
                submit_btn.evaluate(
                    "el => !!el.disabled || String(el.getAttribute('aria-disabled') || '').toLowerCase() === 'true'"
                )
            )
        except Exception:
            disabled = False
        if disabled:
            page.wait_for_timeout(300)
            continue
        try:
            submit_btn.click(timeout=1500, force=True)
            submitted = True
            print("[atlas] quality review submitted.")
            page.wait_for_timeout(1300)
            break
        except Exception:
            page.wait_for_timeout(300)
            continue

    if not submitted:
        return False
    if _first_visible_locator(page, modal_sel, timeout_ms=1200) is not None:
        return False
    return True


def _submit_episode(page: Page, cfg: Dict[str, Any]) -> bool:
    complete_sel = str(_cfg_get(cfg, "atlas.selectors.complete_button", "")).strip()
    modal_sel = str(_cfg_get(cfg, "atlas.selectors.quality_review_modal", "")).strip()

    modal_open = bool(modal_sel and _first_visible_locator(page, modal_sel, timeout_ms=900) is not None)
    completed = modal_open

    if not modal_open and complete_sel:
        _dismiss_blocking_modals(page)
        _dismiss_blocking_side_panel(page, cfg, aggressive=True)
        completed = _safe_locator_click(page, complete_sel, timeout_ms=7000)
        if not completed:
            complete_loc = _first_visible_locator(page, complete_sel, timeout_ms=1800)
            if complete_loc is not None:
                try:
                    complete_loc.click(timeout=1200, force=True)
                    completed = True
                except Exception:
                    completed = False
        if not completed:
            try:
                completed = bool(
                    page.evaluate(
                        """() => {
                            const nodes = Array.from(document.querySelectorAll('button,[role="button"]'));
                            for (const n of nodes) {
                                const text = (n.innerText || n.textContent || '').trim().toLowerCase();
                                if (text !== 'complete') continue;
                                if (n.disabled) continue;
                                n.click();
                                return true;
                            }
                            return false;
                        }"""
                    )
                )
            except Exception:
                completed = False
        if completed:
            print("[atlas] clicked Complete button.")
            page.wait_for_timeout(900)

    if not completed and not modal_open:
        return False

    reviewed = _handle_quality_review_modal(page, cfg, timeout_ms=9000)
    return bool(completed and reviewed)


def apply_labels(page: Page, cfg: Dict[str, Any], label_map: Dict[int, str]) -> Dict[str, Any]:
    _dismiss_blocking_modals(page)
    _dismiss_blocking_side_panel(page, cfg, aggressive=True)

    rows_sel = str(_cfg_get(cfg, "atlas.selectors.segment_rows", ""))
    label_sel = str(_cfg_get(cfg, "atlas.selectors.segment_label", ""))
    edit_sel = str(_cfg_get(cfg, "atlas.selectors.edit_button_in_row", ""))
    input_sel = str(_cfg_get(cfg, "atlas.selectors.label_input", ""))
    save_sel = str(_cfg_get(cfg, "atlas.selectors.save_button", ""))
    complete_sel = str(_cfg_get(cfg, "atlas.selectors.complete_button", ""))
    skip_unchanged = bool(_cfg_get(cfg, "run.skip_unchanged_labels", True))

    best_rows_sel, rows = _resolve_rows_locator(page, rows_sel)
    failed: List[str] = []
    applied = 0
    skipped_unchanged = 0

    for idx in sorted(label_map):
        rows = page.locator(best_rows_sel)
        count = rows.count()
        if idx > count:
            failed.append(f"segment {idx}: row missing (count={count})")
            continue
        row = rows.nth(idx - 1)
        label = label_map[idx]
        try:
            _dismiss_blocking_modals(page)
            _dismiss_blocking_side_panel(page, cfg, aggressive=True)
            _click_segment_row_with_recovery(page, rows, idx, cfg)
            row = page.locator(best_rows_sel).nth(idx - 1)
            if skip_unchanged and label_sel:
                current_label = _first_text_from_row(row, label_sel)
                if current_label:
                    if _normalize_label_for_compare(current_label) == _normalize_label_for_compare(label):
                        skipped_unchanged += 1
                        continue

            clicked_edit = False
            for candidate in _selector_variants(edit_sel):
                edit_loc = row.locator(candidate).first
                if edit_loc.count() > 0 and edit_loc.is_visible():
                    try:
                        edit_loc.click(timeout=1500)
                    except Exception:
                        _dismiss_blocking_side_panel(page, cfg, aggressive=True)
                        edit_loc.click(timeout=1000, force=True)
                    clicked_edit = True
                    break
            if not clicked_edit:
                try:
                    row.dblclick(timeout=1200)
                    clicked_edit = True
                except Exception:
                    page.keyboard.press("e")

            input_loc = _first_visible_locator(page, input_sel, timeout_ms=5000)
            if input_loc is None:
                raise RuntimeError("label input not found")
            _fill_input(input_loc, label, page)

            saved = _safe_locator_click(page, save_sel, timeout_ms=2500) if save_sel else False
            if not saved:
                for candidate in _selector_variants(save_sel):
                    btn = _first_visible_locator(page, candidate, timeout_ms=900)
                    if btn is None:
                        continue
                    try:
                        btn.click(timeout=900, force=True)
                        saved = True
                        break
                    except Exception:
                        continue
            if not saved:
                page.keyboard.press("Control+Enter")

            applied += 1
            time.sleep(0.15)
        except Exception as exc:
            failed.append(f"segment {idx}: {exc}")

    completed = _submit_episode(page, cfg) if complete_sel else False

    return {
        "applied": applied,
        "skipped_unchanged": skipped_unchanged,
        "failed": failed,
        "completed": completed,
    }


def _save_outputs(
    cfg: Dict[str, Any],
    segments: List[Dict[str, Any]],
    prompt: str,
    labels_payload: Dict[str, Any],
    task_id: str = "",
) -> None:
    out_dir = Path(str(_cfg_get(cfg, "run.output_dir", "outputs")))
    out_dir.mkdir(parents=True, exist_ok=True)

    seg_path = out_dir / str(_cfg_get(cfg, "run.segments_dump", "atlas_segments_dump.json"))
    prompt_path = out_dir / str(_cfg_get(cfg, "run.prompt_dump", "atlas_prompt.txt"))
    labels_path = out_dir / str(_cfg_get(cfg, "run.labels_dump", "atlas_labels_from_gemini.json"))

    _ensure_parent(seg_path)
    _ensure_parent(prompt_path)
    _ensure_parent(labels_path)

    seg_path.write_text(json.dumps({"segments": segments}, indent=2, ensure_ascii=False), encoding="utf-8")
    prompt_path.write_text(prompt, encoding="utf-8")
    labels_path.write_text(json.dumps(labels_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"[out] segments: {seg_path}")
    print(f"[out] prompt:   {prompt_path}")
    print(f"[out] labels:   {labels_path}")

    if task_id and bool(_cfg_get(cfg, "run.use_task_scoped_artifacts", True)):
        scoped = _task_scoped_artifact_paths(cfg, task_id)
        try:
            scoped["segments_cache"].write_text(
                json.dumps({"segments": segments}, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            print(f"[out] segments task: {scoped['segments_cache']}")
        except Exception:
            pass
        try:
            scoped["prompt_dump"].write_text(prompt, encoding="utf-8")
            print(f"[out] prompt task:   {scoped['prompt_dump']}")
        except Exception:
            pass
        try:
            scoped["labels_dump"].write_text(
                json.dumps(labels_payload, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            print(f"[out] labels task:   {scoped['labels_dump']}")
        except Exception:
            pass


def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("Config root must be YAML object")
    return _deep_merge(DEFAULT_CONFIG, raw)


def run(cfg: Dict[str, Any], execute: bool) -> None:
    state_path = Path(str(_cfg_get(cfg, "browser.storage_state_path", ".state/atlas_auth.json")))
    force_login = bool(_cfg_get(cfg, "browser.force_login", False))
    headless = bool(_cfg_get(cfg, "browser.headless", False))
    slow_mo = int(_cfg_get(cfg, "browser.slow_mo_ms", 0))
    use_chrome_profile = bool(_cfg_get(cfg, "browser.use_chrome_profile", False))
    fallback_on_profile_error = bool(
        _cfg_get(cfg, "browser.fallback_to_isolated_context_on_profile_error", True)
    )
    profile_launch_timeout_ms = int(_cfg_get(cfg, "browser.profile_launch_timeout_ms", 30000))
    close_chrome_before_profile_launch = bool(
        _cfg_get(cfg, "browser.close_chrome_before_profile_launch", False)
    )
    profile_launch_retry_count = max(0, int(_cfg_get(cfg, "browser.profile_launch_retry_count", 1)))
    profile_launch_retry_delay_sec = max(0.2, float(_cfg_get(cfg, "browser.profile_launch_retry_delay_sec", 2.0)))
    clone_chrome_profile_to_temp = bool(_cfg_get(cfg, "browser.clone_chrome_profile_to_temp", True))
    reuse_existing_cloned_profile = bool(_cfg_get(cfg, "browser.reuse_existing_cloned_profile", True))
    prefer_profile_with_atlas_cookies = bool(_cfg_get(cfg, "browser.prefer_profile_with_atlas_cookies", True))
    cloned_user_data_dir = str(_cfg_get(cfg, "browser.cloned_user_data_dir", ".state/chrome_user_data_clone")).strip()
    chrome_channel = str(_cfg_get(cfg, "browser.chrome_channel", "chrome")).strip() or "chrome"
    chrome_user_data_dir = (
        str(_cfg_get(cfg, "browser.chrome_user_data_dir", "")).strip()
        or os.environ.get("CHROME_USER_DATA_DIR", "").strip()
        or _default_chrome_user_data_dir()
    )
    chrome_profile_directory_raw = (
        str(_cfg_get(cfg, "browser.chrome_profile_directory", "Default")).strip()
        or os.environ.get("CHROME_PROFILE_DIRECTORY", "").strip()
    )
    if chrome_profile_directory_raw.lower() in {"none", "direct", "no_profile_arg", "-"}:
        chrome_profile_directory = ""
    else:
        chrome_profile_directory = chrome_profile_directory_raw or "Default"
    atlas_email = _resolve_atlas_email(cfg)

    dry_run = bool(_cfg_get(cfg, "run.dry_run", True))
    if execute:
        dry_run = False

    with sync_playwright() as pw:
        browser = None
        profile_launch_args: List[str] = []
        using_profile_mode = use_chrome_profile
        if using_profile_mode:
            if not chrome_user_data_dir:
                raise RuntimeError(
                    "browser.use_chrome_profile=true but chrome_user_data_dir is empty. "
                    "Set browser.chrome_user_data_dir or CHROME_USER_DATA_DIR."
                )
            direct_profile_mode = _is_direct_profile_path(chrome_user_data_dir)
            if direct_profile_mode:
                if clone_chrome_profile_to_temp:
                    print("[browser] direct profile path detected; disabling clone mode.")
                clone_chrome_profile_to_temp = False
                prefer_profile_with_atlas_cookies = False
                if chrome_profile_directory:
                    print("[browser] direct profile path detected; ignoring chrome_profile_directory arg.")
                chrome_profile_directory = ""
            if prefer_profile_with_atlas_cookies and chrome_profile_directory.lower() in {
                "default",
                "auto",
                "detect",
                "by_email",
            }:
                detected_cookie_profile = _detect_chrome_profile_for_site_cookie(
                    chrome_user_data_dir,
                    domain_hint="atlascapture.io",
                )
                if detected_cookie_profile:
                    chrome_profile_directory = detected_cookie_profile
            if chrome_profile_directory.lower() in {"auto", "detect", "by_email"}:
                detected = _detect_chrome_profile_for_email(chrome_user_data_dir, atlas_email)
                if detected:
                    chrome_profile_directory = detected
                    print(f"[browser] auto-detected Chrome profile for {atlas_email}: {chrome_profile_directory}")
                else:
                    chrome_profile_directory = "Default"
                    print("[browser] profile auto-detect failed; fallback to Default.")
            if close_chrome_before_profile_launch:
                print("[browser] closing existing Chrome processes before profile clone/launch...")
                _close_chrome_processes()
                time.sleep(1.5)
            launch_user_data_dir = chrome_user_data_dir
            if clone_chrome_profile_to_temp:
                print(
                    f"[browser] cloning Chrome profile '{chrome_profile_directory}' "
                    f"to temp user-data-dir: {cloned_user_data_dir}"
                )
                launch_user_data_dir = _prepare_chrome_profile_clone(
                    source_user_data_dir=chrome_user_data_dir,
                    profile_directory=chrome_profile_directory,
                    target_user_data_dir=cloned_user_data_dir,
                    reuse_existing=reuse_existing_cloned_profile,
                )

            launch_args = [f"--profile-directory={chrome_profile_directory}"] if chrome_profile_directory else []
            profile_launch_args = list(launch_args)
            profile_log = chrome_profile_directory if chrome_profile_directory else "<direct-profile-path>"
            print(f"[browser] using Chrome profile: user_data_dir={launch_user_data_dir}, profile={profile_log}")
            last_profile_exc: Exception | None = None
            context = None
            for attempt in range(profile_launch_retry_count + 1):
                try:
                    context = pw.chromium.launch_persistent_context(
                        user_data_dir=launch_user_data_dir,
                        channel=chrome_channel,
                        headless=headless,
                        slow_mo=slow_mo,
                        args=launch_args,
                        timeout=profile_launch_timeout_ms,
                    )
                    last_profile_exc = None
                    break
                except Exception as exc:
                    last_profile_exc = exc
                    if attempt < profile_launch_retry_count:
                        print(
                            f"[browser] profile launch attempt {attempt + 1} failed; retrying in "
                            f"{profile_launch_retry_delay_sec:.1f}s..."
                        )
                        if close_chrome_before_profile_launch:
                            _close_chrome_processes()
                        time.sleep(profile_launch_retry_delay_sec)
                        continue

            if last_profile_exc is not None:
                if not fallback_on_profile_error:
                    raise RuntimeError(
                        "Failed to open Chrome persistent profile. "
                        "Close all Chrome windows for that profile and retry."
                    ) from last_profile_exc
                print(
                    "[browser] profile launch failed; falling back to isolated browser context "
                    "(close Chrome windows to use real profile directly)."
                )
                using_profile_mode = False
        if using_profile_mode:
            print("[browser] profile context launched.")
            if context.pages:
                page = context.pages[-1]
            else:
                page = context.new_page()
            if not force_login:
                _restore_storage_state(context, page, state_path)
            print(f"[browser] initial page url: {page.url}")
        else:
            browser = pw.chromium.launch(headless=headless, slow_mo=slow_mo)
            context_kwargs: Dict[str, Any] = {}
            if state_path.exists() and not force_login:
                context_kwargs["storage_state"] = str(state_path)
                print(f"[auth] using saved state: {state_path}")
            context = browser.new_context(**context_kwargs)
            page = context.new_page()

        try:
            room_url = str(_cfg_get(cfg, "atlas.room_url", "")).strip()
            if room_url:
                print(f"[run] opening room url: {room_url}")
                try:
                    page.goto(room_url, wait_until="commit", timeout=45000)
                    print(f"[run] page after room goto: {page.url}")
                    if (
                        using_profile_mode
                        and clone_chrome_profile_to_temp
                        and reuse_existing_cloned_profile
                        and "/login" in page.url.lower()
                    ):
                        print("[browser] cloned profile looks stale; refreshing clone from source profile and retrying once.")
                        context.close()
                        refreshed_user_data_dir = _prepare_chrome_profile_clone(
                            source_user_data_dir=chrome_user_data_dir,
                            profile_directory=chrome_profile_directory,
                            target_user_data_dir=cloned_user_data_dir,
                            reuse_existing=False,
                        )
                        context = pw.chromium.launch_persistent_context(
                            user_data_dir=refreshed_user_data_dir,
                            channel=chrome_channel,
                            headless=headless,
                            slow_mo=slow_mo,
                            args=profile_launch_args,
                            timeout=profile_launch_timeout_ms,
                        )
                        if context.pages:
                            page = context.pages[-1]
                        else:
                            page = context.new_page()
                        if not force_login:
                            _restore_storage_state(context, page, state_path)
                        page.goto(room_url, wait_until="commit", timeout=45000)
                        print(f"[run] page after refreshed-clone room goto: {page.url}")
                except Exception as exc:
                    print(f"[run] room goto failed: {exc}. Continuing with login flow.")

            if "/dashboard" not in page.url.lower() and "/tasks" not in page.url.lower():
                ensure_logged_in(page, cfg)
                if _is_authenticated_page(page):
                    _ensure_parent(state_path)
                    context.storage_state(path=str(state_path))
                    print(f"[auth] saved state: {state_path}")

            max_episodes_per_run = int(_cfg_get(cfg, "run.max_episodes_per_run", 1))
            resume_from_artifacts = bool(_cfg_get(cfg, "run.resume_from_artifacts", True))
            resume_skip_video_steps_when_cached = bool(_cfg_get(cfg, "run.resume_skip_video_steps_when_cached", True))
            resume_skip_apply_steps_when_done = bool(_cfg_get(cfg, "run.resume_skip_apply_steps_when_done", True))
            episode_no = 0
            while True:
                if max_episodes_per_run > 0 and episode_no >= max_episodes_per_run:
                    print(f"[run] reached max_episodes_per_run={max_episodes_per_run}.")
                    break

                opened = goto_task_room(page, cfg)
                if not opened:
                    print("[run] no label task available right now.")
                    break
                episode_no += 1
                print(f"[run] episode {episode_no} opened: {page.url}")
                task_id = _task_id_from_url(page.url)
                task_state = _load_task_state(cfg, task_id) if (resume_from_artifacts and task_id) else {}
                scoped_paths = _task_scoped_artifact_paths(cfg, task_id) if task_id else {}

                _dismiss_blocking_modals(page)
                labels_payload = _load_cached_labels(cfg, task_id) if task_id else None
                min_video_bytes = int(_cfg_get(cfg, "gemini.min_video_bytes", 500000))
                cached_video_file: Optional[Path] = None
                if task_id:
                    candidate = scoped_paths.get("video")
                    if candidate is not None and candidate.exists():
                        try:
                            if candidate.stat().st_size >= min_video_bytes and _is_probably_mp4(candidate):
                                cached_video_file = candidate
                        except Exception:
                            cached_video_file = None

                skip_video_steps = bool(
                    resume_from_artifacts
                    and resume_skip_video_steps_when_cached
                    and cached_video_file is not None
                    and labels_payload is not None
                )
                if skip_video_steps:
                    print(f"[run] resume mode: cached video+labels found for task {task_id}; skipping video playback.")
                else:
                    _ensure_loop_off(page, cfg)
                    _play_full_video_once(page, cfg)

                if cached_video_file is not None:
                    video_file = cached_video_file
                    print(f"[video] using cached task video: {video_file}")
                else:
                    print("[run] preparing task video for Gemini...")
                    video_file = _prepare_video_for_gemini(page, context, cfg, task_id=task_id)

                if task_id and video_file is not None and resume_from_artifacts:
                    task_state["video_path"] = str(video_file)
                    task_state["video_ready"] = True
                    _save_task_state(cfg, task_id, task_state)

                segments = None
                if resume_from_artifacts and task_id:
                    cached_segments = _load_cached_segments(cfg, task_id)
                    if cached_segments:
                        segments = cached_segments
                        print(f"[atlas] using cached segments for task {task_id}: {len(segments)}")
                if segments is None:
                    print("[run] extracting Atlas segments...")
                    segments = extract_segments(page, cfg)
                    print(f"[atlas] extracted {len(segments)} segments")
                    if task_id and resume_from_artifacts:
                        _save_cached_segments(cfg, task_id, segments)

                prompt = build_prompt(segments, str(_cfg_get(cfg, "gemini.extra_instructions", "")))
                if labels_payload is None:
                    print("[run] requesting labels from Gemini...")
                    labels_payload = call_gemini_labels(cfg, prompt, video_file=video_file)
                    if task_id:
                        _save_cached_labels(cfg, task_id, labels_payload)
                    if task_id and resume_from_artifacts:
                        task_state["labels_ready"] = True
                        _save_task_state(cfg, task_id, task_state)
                _save_outputs(cfg, segments, prompt, labels_payload, task_id=task_id)

                segment_plan = _normalize_segment_plan(labels_payload, segments)
                if task_id:
                    _save_task_text_files(cfg, task_id, segments, segment_plan)
                label_map = _normalize_label_map_from_plan(segment_plan)
                print(f"[gemini] usable labels: {len(label_map)}")
                pre_skipped_unchanged = 0
                if bool(_cfg_get(cfg, "run.skip_unchanged_labels", True)):
                    label_map, pre_skipped_unchanged = _filter_unchanged_label_map(label_map, segments)
                    if pre_skipped_unchanged:
                        print(f"[run] pre-skip unchanged labels: {pre_skipped_unchanged}")

                if dry_run:
                    print("[run] dry-run enabled; no labels were applied to Atlas")
                    break

                if resume_from_artifacts and resume_skip_apply_steps_when_done and bool(task_state.get("timestamps_done")):
                    ts_result = {"adjusted": 0, "failed": []}
                    print("[run] resume: skipping timestamp adjustments (already completed previously).")
                else:
                    ts_result = apply_timestamp_adjustments(page, cfg, segments, segment_plan)
                print(f"[run] timestamp adjustments: {ts_result['adjusted']}")
                if ts_result["failed"]:
                    print("[run] timestamp adjustment failures:")
                    for item in ts_result["failed"]:
                        print(f"  - {item}")
                elif task_id and resume_from_artifacts:
                    task_state["timestamps_done"] = True
                    _save_task_state(cfg, task_id, task_state)

                if resume_from_artifacts and resume_skip_apply_steps_when_done and bool(task_state.get("labels_applied")):
                    print("[run] resume: skipping label apply (already completed previously).")
                    result = {
                        "applied": 0,
                        "skipped_unchanged": 0,
                        "failed": [],
                        "completed": _submit_episode(page, cfg),
                    }
                else:
                    result = apply_labels(page, cfg, label_map)
                print(f"[run] applied labels: {result['applied']}")
                skipped_total = int(pre_skipped_unchanged) + int(result.get("skipped_unchanged", 0))
                if skipped_total:
                    print(f"[run] skipped unchanged labels: {skipped_total}")
                if result["failed"]:
                    print("[run] failures:")
                    for item in result["failed"]:
                        print(f"  - {item}")
                elif task_id and resume_from_artifacts:
                    task_state["labels_applied"] = True
                    _save_task_state(cfg, task_id, task_state)
                if not result.get("completed"):
                    print("[run] Complete button not clicked (not found or not visible).")
                elif task_id and resume_from_artifacts:
                    task_state["episode_submitted"] = True
                    _save_task_state(cfg, task_id, task_state)

                if room_url:
                    print("[run] returning to room page for next episode.")
                    page.goto(room_url, wait_until="domcontentloaded")
                    page.wait_for_timeout(1500)
        except Exception as exc:
            out_dir = Path(str(_cfg_get(cfg, "run.output_dir", "outputs")))
            out_dir.mkdir(parents=True, exist_ok=True)
            ts = time.strftime("%Y%m%d_%H%M%S")
            snap_path = out_dir / f"debug_failure_{ts}.png"
            html_path = out_dir / f"debug_failure_{ts}.html"
            try:
                page.screenshot(path=str(snap_path), full_page=True)
                print(f"[debug] screenshot saved: {snap_path}")
            except Exception:
                pass
            try:
                html_path.write_text(page.content(), encoding="utf-8")
                print(f"[debug] html saved: {html_path}")
            except Exception:
                pass
            raise
        finally:
            try:
                if _is_authenticated_page(page):
                    _ensure_parent(state_path)
                    context.storage_state(path=str(state_path))
                    print(f"[auth] saved state: {state_path}")
                else:
                    print("[auth] skip saving state (session not authenticated).")
            except Exception:
                pass
            context.close()
            if browser is not None:
                browser.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Atlas browser auto-solver (Atlas -> Gemini -> optional autofill)")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply labels to Atlas. Without this flag, script runs in dry-run mode.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(Path(args.config))
    run(cfg, execute=bool(args.execute))


if __name__ == "__main__":
    main()
