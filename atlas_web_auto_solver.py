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
import math
import random
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
        "restore_state_in_profile_mode": False,
        "use_chrome_profile": False,
        "proxy_server": "",
        "proxy_username": "",
        "proxy_password": "",
        "proxy_bypass": "",
        "clear_env_proxy_for_backend_requests": True,
        "chrome_channel": "chrome",
        "executable_path": "",
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
        "max_episodes_per_run": 5,
        "no_task_retry_count": 5,
        "no_task_retry_delay_sec": 5.0,
        "no_task_backoff_factor": 1.0,
        "no_task_max_delay_sec": 5.0,
        "clear_blocked_tasks_every_retry": True,
        "keep_alive_when_idle": True,
        "keep_alive_idle_cycle_pause_sec": 5.0,
        "skip_reserve_when_all_visible_blocked": False,
        "clear_blocked_tasks_after_all_visible_blocked_hits": 1,
        "reserve_cooldown_sec": 0,
        "reserve_min_interval_sec": 0,
        "reserve_wait_only_on_rate_limit": True,
        "reserve_attempts_per_visit": 3,
        "reserve_label_wait_timeout_ms": 12000,
        "reserve_label_wait_timeout_after_reserve_ms": 3500,
        "reserve_immediate_when_no_reserved": True,
        "reserve_skip_initial_label_scan_when_no_reserved": True,
        "reserve_no_reserved_probe_timeout_ms": 900,
        "reserve_refresh_after_click": True,
        "reserve_rate_limit_wait_sec": 5,
        "release_all_on_internal_error": True,
        "release_and_reserve_on_all_visible_blocked": True,
        "release_and_reserve_on_submit_unverified": True,
        "recycle_after_max_episodes": True,
        "release_all_after_batch": True,
        "release_all_wait_sec": 5,
        "goto_retry_count": 3,
        "goto_retry_delay_sec": 1.2,
        "skip_duplicate_task_in_run": True,
        "duplicate_task_retry_count": 3,
        "duplicate_task_retry_wait_sec": 2.0,
        "continue_on_episode_error": True,
        "max_episode_failures_per_run": 3,
        "episode_failure_retry_delay_sec": 4.0,
        "gemini_quota_retry_delay_sec": 15.0,
        "gemini_quota_global_pause_min_sec": 60.0,
        "gemini_quota_global_pause_step_sec": 60.0,
        "gemini_quota_task_block_max_wait_sec": 21600.0,
        "max_video_prepare_failures_per_task": 2,
        "max_gemini_failures_per_task": 1,
        "workflow_reentry_enter_clicks": 2,
        "workflow_reentry_second_click_delay_sec": 5.0,
        "min_delay_between_episodes_sec": 0.0,
        "max_delay_between_episodes_sec": 0.0,
        "reuse_cached_labels": True,
        "skip_unchanged_labels": True,
        "resume_from_artifacts": True,
        "resume_skip_video_steps_when_cached": True,
        "resume_skip_apply_steps_when_done": False,
        "allow_resume_auto_submit": False,
        "execute_force_fresh_gemini": True,
        "execute_force_live_segments": True,
        "execute_require_video_context": True,
        "segment_chunking_enabled": True,
        "segment_chunking_min_segments": 16,
        "segment_chunking_min_video_sec": 60.0,
        "segment_chunking_max_segments_per_request": 8,
        "segment_chunking_video_pad_sec": 1.0,
        "segment_chunking_keep_temp_files": False,
        "segment_chunking_include_previous_labels_context": True,
        "segment_chunking_max_previous_labels": 12,
        "segment_chunking_disable_operations": True,
        "segment_chunking_consistency_memory_enabled": True,
        "segment_chunking_consistency_memory_limit": 40,
        "segment_chunking_consistency_prompt_terms": 16,
        "segment_chunking_consistency_normalize_labels": True,
        "auto_continuity_merge_enabled": True,
        "auto_continuity_merge_min_run_segments": 3,
        "auto_continuity_merge_min_token_overlap": 1,
        "use_task_scoped_artifacts": True,
        "enable_quality_review_submit": True,
        "loop_off_on_episode_open": True,
        "enable_policy_gate": True,
        "block_apply_on_validation_fail": True,
        "skip_policy_lexical_checks_on_unchanged_labels": False,
        "ignore_timestamp_policy_errors_when_adjust_disabled": True,
        "ignore_no_action_standalone_policy_error": True,
        "no_action_pause_rewrite_enabled": True,
        "no_action_pause_rewrite_max_sec": 12.0,
        "no_action_pause_rewrite_min_overlap_tokens": 1,
        "no_action_pause_rewrite_prefer_next_adjust": True,
        "min_label_words": 2,
        "max_label_words": 20,
        "max_atomic_actions_per_label": 2,
        "forbidden_label_verbs": ["inspect", "check", "look", "examine", "reach", "rotate", "grab", "relocate"],
        "forbidden_narrative_words": ["another", "then", "next", "continue", "again"],
        "allowed_label_start_verbs": [
            "pick up",
            "place",
            "move",
            "adjust",
            "align",
            "hold",
            "cut",
            "open",
            "close",
            "peel",
            "secure",
            "wipe",
            "flip",
            "pull",
            "push",
            "insert",
            "remove",
            "attach",
            "detach",
            "connect",
            "disconnect",
            "tighten",
            "loosen",
            "screw",
            "unscrew",
            "press",
            "twist",
            "turn",
            "slide",
            "lift",
            "lower",
            "set",
            "position",
            "straighten",
            "comb",
            "detangle",
            "sand",
            "paint",
            "clean",
        ],
        "tier3_label_rewrite": True,
        "enable_structural_actions": True,
        "structural_allow_split": False,
        "structural_allow_merge": True,
        "structural_allow_delete": False,
        "requery_after_structural_actions": True,
        "max_structural_operations": 12,
        "structural_skip_if_segments_ge": 40,
        "structural_max_failures_per_episode": 4,
        "structural_wait_rows_delta_timeout_ms": 1800,
        "adjust_timestamps": False,
        "timestamp_adjust_mode": "off",
        "timestamp_skip_if_segments_ge": 24,
        "timestamp_click_timeout_ms": 350,
        "timestamp_click_pause_ms": 15,
        "timestamp_max_failures_per_episode": 10,
        "timestamp_max_total_clicks": 80,
        "timestamp_abort_on_first_failure": False,
        "timestamp_skip_disabled_buttons": True,
        "label_apply_progress_every": 5,
        "label_apply_max_total_sec": 600,
        "label_apply_max_failures": 18,
        "label_apply_input_timeout_ms": 3000,
        "label_apply_save_timeout_ms": 1800,
        "label_apply_edit_click_timeout_ms": 900,
        "submit_guard_enabled": True,
        "submit_guard_max_failure_ratio": 0.25,
        "submit_guard_min_applied_ratio": 0.9,
        "submit_guard_block_on_budget_exceeded": True,
        "play_full_video_before_labeling": False,
        "play_full_video_max_wait_sec": 900,
        "segment_resolve_attempts": 24,
        "segment_resolve_retry_ms": 800,
        "segment_resolve_sample_size": 8,
        "segment_resolve_row_text_timeout_ms": 350,
        "label_open_loading_max_checks": 5,
        "label_open_loading_wait_ms": 600,
        "modal_dismiss_passes": 2,
        "modal_dismiss_timeout_ms": 120,
        "modal_dismiss_post_click_wait_ms": 180,
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
            "reserve_episodes_button": 'button:has-text("Reserve 5 Episodes") || button:has-text("Reserve 4 Episodes") || button:has-text("Reserve 3 Episodes") || button:has-text("Reserve 2 Episodes") || button:has-text("Reserve 1 Episode") || button:has-text("Reserve 1 Episodes") || button:has-text("Reserve New Episode") || button:has-text("Reserve")',
            "confirm_reserve_button": 'button:has-text("I Understand") || button:has-text("Understand") || button:has-text("OK") || button:has-text("Okay") || button:has-text("Confirm")',
            "release_all_button": 'button:has-text("Release All") || button:has-text("Release all") || button:has-text("Release Episodes") || button:has-text("Release")',
            "confirm_release_button": 'div[role="dialog"] button:has-text("Release All") || div[role="dialog"] [role="button"]:has-text("Release All") || button:has-text("Release All") || button:has-text("Release all") || button:has-text("I Understand") || button:has-text("Understand") || button:has-text("Confirm") || button:has-text("Yes") || button:has-text("OK") || button:has-text("Okay") || text=/\\bRelease\\s*All\\b/i',
            "error_go_back_button": 'button:has-text("Go Back") || a:has-text("Go Back") || [role="button"]:has-text("Go Back")',
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
            "split_button_in_row": 'button[title*="Split"] || [aria-label*="Split"] || button:has(svg.lucide-scissors)',
            "delete_button_in_row": 'button[title*="Delete"] || [aria-label*="Delete"] || button:has(svg.lucide-trash) || button:has(svg.lucide-trash-2)',
            "merge_button_in_row": 'button[title*="Merge"] || [aria-label*="Merge"] || button:has(svg.lucide-git-merge)',
            "action_confirm_button": 'button:has-text("Confirm") || button:has-text("Yes") || button:has-text("Delete") || button:has-text("Merge") || button:has-text("Apply") || button:has-text("Continue")',
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
        "fallback_api_key": "",
        "prefer_fallback_key_as_primary": True,
        "quota_fallback_enabled": False,
        "quota_fallback_max_uses_per_run": 1,
        "model": "gemini-3.1-pro-preview",
        "retry_with_quota_fallback_model": True,
        "quota_fallback_model": "gemini-3-pro-preview",
        "quota_fallback_from_models": ["gemini-3.1-pro-preview"],
        "policy_retry_model": "gemini-2.5-pro",
        "retry_with_stronger_model_on_policy_fail": True,
        "policy_retry_only_if_flash": True,
        "system_instruction_file": "",
        "system_instruction_text": "",
        "temperature": 0.0,
        "top_p": 0.95,
        "top_k": 64,
        "max_output_tokens": 8192,
        "candidate_count": 1,
        "max_retries": 3,
        "retry_on_quota_429": False,
        "quota_retry_default_wait_sec": 12.0,
        "quota_cooldown_max_wait_sec": 120.0,
        "retry_base_delay_sec": 2.0,
        "retry_jitter_sec": 0.8,
        "max_backoff_sec": 30.0,
        "price_input_per_million": 0.30,
        "price_output_per_million": 2.50,
        "usage_log_file": "gemini_usage.jsonl",
        "rate_limit_enabled": True,
        "rate_limit_requests_per_minute": 6,
        "rate_limit_window_sec": 60.0,
        "rate_limit_min_interval_sec": 10.5,
        "connect_timeout_sec": 30,
        "request_timeout_sec": 420,
        "attach_video": True,
        "require_video": False,
        "allow_text_only_fallback_on_network_error": True,
        "skip_video_when_segments_le": 0,
        "video_transport": "auto",
        "files_api_fallback_to_inline": True,
        "file_ready_timeout_sec": 120,
        "file_ready_poll_sec": 2.0,
        "upload_request_timeout_sec": 180,
        "upload_chunk_bytes": 8388608,
        "upload_chunk_granularity_bytes": 8388608,
        "upload_chunk_max_retries": 5,
        "optimize_video_for_upload": True,
        "optimize_video_only_if_larger_mb": 8.0,
        "optimize_video_target_mb": 4.0,
        "optimize_video_target_fps": 10.0,
        "optimize_video_min_fps": 8.0,
        "optimize_video_min_width": 320,
        "optimize_video_min_short_side": 320,
        "optimize_video_scale_candidates": [0.75, 0.6, 0.5, 0.4, 0.33, 0.25, 0.2],
        "inline_retry_target_mb": [4.0, 2.5, 1.5, 1.0],
        "max_inline_video_mb": 20.0,
        "split_upload_enabled": True,
        "split_upload_only_if_larger_mb": 8.0,
        "split_upload_chunk_max_mb": 6.0,
        "split_upload_max_chunks": 4,
        "split_upload_reencode_on_copy_fail": True,
        "split_upload_inline_total_max_mb": 12.0,
        "reference_frames_enabled": True,
        "reference_frames_always": False,
        "reference_frame_attach_when_video_mb_le": 2.5,
        "reference_frame_count": 2,
        "reference_frame_positions": [0.2, 0.55, 0.85],
        "reference_frame_max_side": 960,
        "reference_frame_jpeg_quality": 82,
        "reference_frame_max_total_kb": 420,
        "video_download_timeout_sec": 180,
        "video_download_retries": 5,
        "video_download_chunk_bytes": 1048576,
        "video_download_retry_base_sec": 1.2,
        "video_download_use_playwright_fallback": True,
        "video_candidate_scan_attempts": 4,
        "video_candidate_scan_wait_ms": 1200,
        "validate_video_decode": True,
        "min_video_bytes": 500000,
        "extra_instructions": "",
    },
}

_LAST_RESERVE_REQUEST_TS = 0.0
_LAST_GEMINI_REQUEST_TS = 0.0
_GEMINI_REQUEST_TIMESTAMPS: List[float] = []
_GEMINI_QUOTA_COOLDOWN_UNTIL_TS = 0.0
_GEMINI_FALLBACK_USES = 0
_SCRIPT_BUILD = "2026-02-27.0905"


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


def _resolve_gemini_fallback_key(explicit: str) -> str:
    return _resolve_secret(
        explicit,
        [
            "GEMINI_API_KEY_FALLBACK",
            "GOOGLE_API_KEY_FALLBACK",
            "GEMINI_API_KEY_SECONDARY",
            "GOOGLE_API_KEY_SECONDARY",
        ],
    )


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


def _is_too_many_redirects_error(exc: Exception) -> bool:
    return "ERR_TOO_MANY_REDIRECTS" in str(exc or "")


def _clear_atlas_site_session(page: Page) -> None:
    try:
        ctx = page.context
    except Exception:
        ctx = None
    if ctx is not None:
        for domain in ("atlascapture.io", ".atlascapture.io", "audit.atlascapture.io"):
            try:
                ctx.clear_cookies(domain=domain)
            except Exception:
                continue
    for origin in ("https://audit.atlascapture.io", "https://atlascapture.io"):
        try:
            page.goto(origin, wait_until="domcontentloaded", timeout=20000)
            page.evaluate(
                """() => {
                    try { localStorage.clear(); } catch (e) {}
                    try { sessionStorage.clear(); } catch (e) {}
                }"""
            )
        except Exception:
            continue
    try:
        page.goto("about:blank", wait_until="commit", timeout=8000)
    except Exception:
        pass


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


def _goto_with_retry(
    page: Page,
    url: str,
    wait_until: str = "domcontentloaded",
    timeout_ms: int = 45000,
    cfg: Optional[Dict[str, Any]] = None,
    reason: str = "",
) -> bool:
    retry_count = max(0, int(_cfg_get(cfg, "run.goto_retry_count", 3) if cfg else 3))
    retry_delay_sec = max(0.2, float(_cfg_get(cfg, "run.goto_retry_delay_sec", 1.2) if cfg else 1.2))
    for attempt in range(retry_count + 1):
        try:
            page.goto(url, wait_until=wait_until, timeout=timeout_ms)
            return True
        except Exception as exc:
            msg = str(exc)
            transient = any(
                key in msg
                for key in (
                    "ERR_NETWORK_CHANGED",
                    "ERR_INTERNET_DISCONNECTED",
                    "ERR_CONNECTION_RESET",
                    "ERR_TIMED_OUT",
                    "ERR_ABORTED",
                )
            )
            if transient and attempt < retry_count:
                tag = f" ({reason})" if reason else ""
                short = msg.splitlines()[0][:200]
                print(
                    f"[nav] transient goto error{tag}; retry {attempt + 1}/{retry_count} "
                    f"in {retry_delay_sec:.1f}s: {short}"
                )
                time.sleep(retry_delay_sec)
                continue
            raise
    return False


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
        click_timeout_ms = max(300, min(timeout_ms, 2000))
        locator.click(timeout=click_timeout_ms, no_wait_after=True)
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


def _all_task_label_hrefs_from_page(page: Page) -> List[str]:
    base_url = page.url or "https://audit.atlascapture.io/tasks/room/normal"
    seen: set[str] = set()
    out: List[str] = []

    def _add(raw: str) -> None:
        raw = (raw or "").strip()
        if not raw:
            return
        if raw.startswith("/"):
            raw = urljoin(base_url, raw)
        if "/tasks/room/normal/label/" not in raw:
            return
        if raw in seen:
            return
        seen.add(raw)
        out.append(raw)

    # Fast-path: scan visible anchors first.
    for candidate in _selector_variants('a[href*="/tasks/room/normal/label/"]'):
        try:
            loc = page.locator(candidate)
            count = min(loc.count(), 80)
            for i in range(count):
                href = loc.nth(i).get_attribute("href")
                _add(str(href or ""))
        except Exception:
            continue

    try:
        hrefs_eval = page.evaluate(
            """() => {
                return Array.from(document.querySelectorAll('a[href*="/tasks/room/normal/label/"]'))
                    .map(a => (a.getAttribute('href') || a.href || '').trim())
                    .filter(Boolean);
            }"""
        )
        if isinstance(hrefs_eval, list):
            for item in hrefs_eval:
                if isinstance(item, str):
                    _add(item)
    except Exception:
        pass

    try:
        html_doc = page.content()
    except Exception:
        html_doc = ""
    for m in re.findall(r'(/tasks/room/normal/label/[A-Za-z0-9]+)', html_doc or ""):
        _add(m)
    return out


def _first_task_label_href_from_html(page: Page, skip_task_ids: Optional[set[str]] = None) -> str:
    blocked = skip_task_ids if skip_task_ids is not None else set()
    for href in _all_task_label_hrefs_from_page(page):
        tid = _task_id_from_url(href)
        if tid and tid in blocked:
            continue
        return href
    return ""


def _is_label_page_not_found(page: Page) -> bool:
    try:
        body = (page.inner_text("body") or "").lower()
    except Exception:
        body = ""
    if not body:
        return False
    markers = [
        "this page could not be found",
        "404: this page could not be found",
        "next-error-h1",
    ]
    return any(marker in body for marker in markers)


def _is_label_page_internal_error(page: Page) -> bool:
    try:
        body = (page.inner_text("body") or "").lower()
    except Exception:
        body = ""
    if not body:
        return False
    has_error = (
        "error loading episode" in body
        or "an internal error occurred" in body
        or "internal error occurred" in body
    )
    if not has_error:
        return False
    return "go back" in body or "/tasks/room/normal/label/" in (page.url or "").lower()


def _try_go_back_from_label_error(page: Page, cfg: Dict[str, Any], timeout_ms: int = 2500) -> bool:
    go_back_sel = str(_cfg_get(cfg, "atlas.selectors.error_go_back_button", "")).strip()
    if go_back_sel and _safe_locator_click(page, go_back_sel, timeout_ms=timeout_ms):
        page.wait_for_timeout(650)
        return True
    clicked = _safe_locator_click(page, 'button:has-text("Go Back") || a:has-text("Go Back")', timeout_ms=timeout_ms)
    if clicked:
        page.wait_for_timeout(650)
    return clicked


def _is_label_page_actionable(page: Page, cfg: Dict[str, Any], timeout_ms: int = 4500) -> bool:
    url_l = (page.url or "").lower()
    if "/tasks/room/normal/label/" not in url_l:
        return False

    selectors = _cfg_get(cfg, "atlas.selectors", {})
    rows_sel = str(selectors.get("segment_rows", "")).strip()
    video_sel = str(selectors.get("video_element", "video")).strip() or "video"

    deadline = time.time() + max(300, timeout_ms) / 1000.0
    while time.time() < deadline:
        _dismiss_blocking_modals(page, cfg)
        if _is_label_page_not_found(page):
            return False
        if _is_label_page_internal_error(page):
            _try_go_back_from_label_error(page, cfg, timeout_ms=800)
            return False

        if video_sel:
            if _first_visible_locator(page, video_sel, timeout_ms=250) is not None:
                return True

        if rows_sel:
            for candidate in _selector_variants(rows_sel):
                try:
                    if page.locator(candidate).count() > 0:
                        return True
                except Exception:
                    continue

        try:
            body = (page.inner_text("body") or "").lower()
        except Exception:
            body = ""
        if "label episode" in body and "segments" in body:
            return True

        page.wait_for_timeout(180)

    return False


def _looks_like_video_url(url: str) -> bool:
    raw = html.unescape((url or "").strip())
    if not raw:
        return False
    u = raw.lower()
    if u.startswith("blob:"):
        return False
    parsed = urlparse(u)
    path = parsed.path or ""
    if re.search(r"\.(mp4|webm|mov|m4v|m3u8)$", path, flags=re.I):
        return True
    if re.search(r"\.(woff2?|ttf|otf|css|js|map|png|jpe?g|gif|svg|ico)$", path, flags=re.I):
        return False
    return ("video" in path) or ("video" in parsed.query)


def _collect_video_url_candidates(page: Page, cfg: Dict[str, Any]) -> List[str]:
    selectors = _cfg_get(cfg, "atlas.selectors", {})
    video_sel = str(selectors.get("video_element", "video"))
    source_sel = str(selectors.get("video_source", "video source"))
    base_url = page.url

    seen: set[str] = set()
    out: List[str] = []

    def add(raw: str) -> None:
        raw = html.unescape((raw or "").strip())
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
        norm = parsed._replace(fragment="").geturl().strip()
        if not _looks_like_video_url(norm):
            return
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


def _download_video_via_playwright_request(
    page: Page,
    context: Any,
    video_url: str,
    out_path: Path,
    timeout_sec: int,
) -> Path:
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

    req_ctx = getattr(context, "request", None)
    if req_ctx is None:
        raise RuntimeError("playwright request context is unavailable")

    resp = req_ctx.get(
        video_url,
        headers=headers,
        timeout=max(15000, int(timeout_sec * 1000)),
        fail_on_status_code=False,
    )
    status = int(resp.status)
    if status not in {200, 206}:
        raise RuntimeError(f"playwright fallback status={status}")

    body = resp.body() or b""
    if not body:
        raise RuntimeError("playwright fallback returned empty body")

    if status == 206:
        cr = str((resp.headers or {}).get("content-range", "")).strip()
        m = re.search(r"/(\d+)$", cr)
        if m:
            try:
                total = int(m.group(1))
            except Exception:
                total = 0
            if total > 0 and len(body) < total:
                raise RuntimeError(
                    f"playwright fallback returned partial body ({len(body)}/{total})"
                )

    _ensure_parent(out_path)
    part_path = out_path.with_suffix(out_path.suffix + ".part")
    part_path.write_bytes(body)
    try:
        out_path.unlink(missing_ok=True)
    except Exception:
        pass
    part_path.replace(out_path)
    return out_path


def _download_video_from_page_context(
    page: Page,
    context: Any,
    video_url: str,
    out_path: Path,
    timeout_sec: int,
    cfg: Optional[Dict[str, Any]] = None,
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

    _ensure_parent(out_path)
    part_path = out_path.with_suffix(out_path.suffix + ".part")
    max_retries = max(0, int(_cfg_get(cfg or {}, "gemini.video_download_retries", 5)))
    chunk_bytes = max(64 * 1024, int(_cfg_get(cfg or {}, "gemini.video_download_chunk_bytes", 1024 * 1024)))
    retry_base = max(0.2, float(_cfg_get(cfg or {}, "gemini.video_download_retry_base_sec", 1.2)))
    use_playwright_fallback = bool(
        _cfg_get(cfg or {}, "gemini.video_download_use_playwright_fallback", True)
    )
    last_err: Optional[Exception] = None

    def _content_range_total(content_range: str) -> int:
        m = re.search(r"/(\d+)$", content_range or "")
        if not m:
            return 0
        try:
            return int(m.group(1))
        except Exception:
            return 0

    for attempt in range(max_retries + 1):
        resume_from = 0
        try:
            if part_path.exists():
                resume_from = int(part_path.stat().st_size)
        except Exception:
            resume_from = 0

        req_headers = dict(headers)
        if resume_from > 0:
            req_headers["Range"] = f"bytes={resume_from}-"

        try:
            with sess.get(
                video_url,
                headers=req_headers,
                timeout=(20, timeout_sec),
                stream=True,
                allow_redirects=True,
            ) as resp:
                status = int(resp.status_code)
                if status not in {200, 206}:
                    resp.raise_for_status()

                if resume_from > 0 and status == 200:
                    # Server ignored Range; restart clean download.
                    try:
                        part_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    resume_from = 0

                expected_total = 0
                cr = resp.headers.get("Content-Range", "")
                if cr:
                    expected_total = _content_range_total(cr)
                if expected_total <= 0:
                    cl = resp.headers.get("Content-Length", "")
                    try:
                        content_len = int(cl)
                    except Exception:
                        content_len = 0
                    if content_len > 0:
                        expected_total = resume_from + content_len

                mode = "ab" if (resume_from > 0 and status == 206 and part_path.exists()) else "wb"
                written_this_attempt = 0
                with part_path.open(mode) as f:
                    for chunk in resp.iter_content(chunk_size=chunk_bytes):
                        if not chunk:
                            continue
                        f.write(chunk)
                        written_this_attempt += len(chunk)

                current_size = int(part_path.stat().st_size) if part_path.exists() else 0
                if current_size <= 0:
                    raise RuntimeError("Downloaded video file is empty.")
                if expected_total > 0 and current_size < expected_total:
                    raise RuntimeError(
                        f"Incomplete download ({current_size}/{expected_total} bytes)"
                    )

                try:
                    out_path.unlink(missing_ok=True)
                except Exception:
                    pass
                part_path.replace(out_path)
                return out_path
        except Exception as exc:
            last_err = exc
            if attempt < max_retries:
                delay = retry_base * (2**attempt)
                try:
                    partial = int(part_path.stat().st_size) if part_path.exists() else 0
                except Exception:
                    partial = 0
                print(
                    f"[video] download retry {attempt + 1}/{max_retries} "
                    f"(partial={partial} bytes) in {delay:.1f}s"
                )
                time.sleep(delay)
                continue
            break

    if use_playwright_fallback:
        try:
            return _download_video_via_playwright_request(
                page=page,
                context=context,
                video_url=video_url,
                out_path=out_path,
                timeout_sec=timeout_sec,
            )
        except Exception as exc:
            if last_err is not None:
                raise RuntimeError(f"{last_err}; playwright fallback failed: {exc}") from exc
            raise RuntimeError(f"playwright fallback failed: {exc}") from exc

    raise RuntimeError(str(last_err) if last_err else "video download failed")


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


def _is_video_decodable(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        import cv2  # type: ignore
    except Exception:
        # If OpenCV is unavailable, do not block on decode probing.
        return True

    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        return False
    try:
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        probe_positions = [0]
        if frame_count > 2:
            probe_positions.append(max(0, frame_count // 2))
            probe_positions.append(max(0, frame_count - 2))
        for pos in probe_positions:
            try:
                cap.set(cv2.CAP_PROP_POS_FRAMES, float(pos))
            except Exception:
                pass
            ok, _ = cap.read()
            if not ok:
                return False
        return True
    finally:
        cap.release()


def _probe_video_stream_meta(path: Path) -> Tuple[int, int, float, int]:
    try:
        import cv2  # type: ignore
    except Exception:
        return 0, 0, 0.0, 0

    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        return 0, 0, 0.0, 0
    try:
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
        fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
        frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        return width, height, fps, frames
    finally:
        cap.release()


def _quality_preserving_scale_candidates(
    scales: List[float],
    src_w: int,
    src_h: int,
    min_width: int,
    min_short_side: int,
) -> List[float]:
    if src_w <= 0 or src_h <= 0:
        return scales

    min_width = max(2, int(min_width))
    min_short_side = max(2, int(min_short_side))
    short_side = min(src_w, src_h)

    width_floor = min_width / float(src_w)
    short_floor = min_short_side / float(short_side) if short_side > 0 else 0.0
    scale_floor = max(0.1, min(1.0, max(width_floor, short_floor)))

    filtered: List[float] = []
    for raw in scales:
        s = max(0.1, min(1.0, float(raw)))
        if s + 1e-6 >= scale_floor:
            filtered.append(s)

    if not filtered:
        filtered = [scale_floor]
    elif all(abs(s - scale_floor) > 1e-3 for s in filtered):
        filtered.append(scale_floor)

    # Keep largest scales first to preserve detail while meeting size target.
    uniq = sorted({round(s, 4) for s in filtered}, reverse=True)
    return [float(s) for s in uniq]


def _extract_reference_frame_inline_parts(
    video_file: Path,
    cfg: Dict[str, Any],
    trigger_video_mb: float,
) -> Tuple[List[Dict[str, Any]], int]:
    enabled = bool(_cfg_get(cfg, "gemini.reference_frames_enabled", True))
    if not enabled or video_file is None or not video_file.exists():
        return [], 0

    always = bool(_cfg_get(cfg, "gemini.reference_frames_always", False))
    trigger_mb = max(0.1, float(_cfg_get(cfg, "gemini.reference_frame_attach_when_video_mb_le", 2.5)))
    if not always and trigger_video_mb > trigger_mb:
        return [], 0

    try:
        import cv2  # type: ignore
    except Exception:
        return [], 0

    frame_count = max(1, int(_cfg_get(cfg, "gemini.reference_frame_count", 2)))
    max_side = max(240, int(_cfg_get(cfg, "gemini.reference_frame_max_side", 960)))
    jpeg_quality = max(50, min(95, int(_cfg_get(cfg, "gemini.reference_frame_jpeg_quality", 82))))
    max_total_bytes = max(64 * 1024, int(float(_cfg_get(cfg, "gemini.reference_frame_max_total_kb", 420)) * 1024))

    raw_positions = _cfg_get(cfg, "gemini.reference_frame_positions", [0.2, 0.55, 0.85])
    pos_list: List[float] = []
    if isinstance(raw_positions, list):
        for raw in raw_positions:
            try:
                v = float(raw)
            except Exception:
                continue
            if 0.0 <= v <= 1.0:
                pos_list.append(v)
    if not pos_list:
        step = 1.0 / float(frame_count + 1)
        pos_list = [step * (i + 1) for i in range(frame_count)]

    cap = cv2.VideoCapture(str(video_file))
    if not cap.isOpened():
        return [], 0

    try:
        frames_total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        if frames_total <= 0:
            return [], 0

        indices: List[int] = []
        for p in pos_list:
            idx = int(round((frames_total - 1) * max(0.0, min(1.0, p))))
            if idx not in indices:
                indices.append(idx)
            if len(indices) >= frame_count:
                break
        if not indices:
            return [], 0

        parts: List[Dict[str, Any]] = []
        total_bytes = 0
        for idx in indices:
            try:
                cap.set(cv2.CAP_PROP_POS_FRAMES, float(idx))
            except Exception:
                pass
            ok, frame = cap.read()
            if not ok or frame is None:
                continue
            h, w = frame.shape[:2]
            if h <= 0 or w <= 0:
                continue
            largest = max(h, w)
            if largest > max_side:
                scale = max_side / float(largest)
                nw = max(2, int(round(w * scale)))
                nh = max(2, int(round(h * scale)))
                frame = cv2.resize(frame, (nw, nh), interpolation=cv2.INTER_AREA)
            ok_enc, enc = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), jpeg_quality])
            if not ok_enc:
                continue
            data = bytes(enc.tobytes())
            if not data:
                continue
            if total_bytes + len(data) > max_total_bytes:
                break
            total_bytes += len(data)
            parts.append(
                {
                    "inline_data": {
                        "mime_type": "image/jpeg",
                        "data": base64.b64encode(data).decode("ascii"),
                    }
                }
            )
        return parts, total_bytes
    finally:
        cap.release()


def _ensure_even(value: int, minimum: int = 2) -> int:
    v = max(int(minimum), int(value))
    return v if v % 2 == 0 else v - 1


def _parse_float_list(value: Any, fallback: List[float]) -> List[float]:
    if isinstance(value, list):
        out: List[float] = []
        for item in value:
            try:
                n = float(item)
                if n > 0:
                    out.append(n)
            except Exception:
                continue
        if out:
            return out
        return list(fallback)
    if isinstance(value, str):
        out = []
        for raw in value.split(","):
            raw = raw.strip()
            if not raw:
                continue
            try:
                n = float(raw)
                if n > 0:
                    out.append(n)
            except Exception:
                continue
        if out:
            return out
    return list(fallback)


def _opencv_available() -> bool:
    try:
        import cv2  # type: ignore  # noqa: F401
        return True
    except Exception:
        return False


def _resolve_ffmpeg_binary() -> Optional[str]:
    candidates = [
        "ffmpeg",
        "ffmpeg.exe",
        "/usr/bin/ffmpeg",
        "/usr/local/bin/ffmpeg",
        "C:\\ffmpeg\\bin\\ffmpeg.exe",
    ]
    for candidate in candidates:
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
        try:
            p = Path(candidate)
            if p.exists() and p.is_file():
                return str(p)
        except Exception:
            continue
    return None


def _resolve_ffprobe_binary(ffmpeg_bin: Optional[str] = None) -> Optional[str]:
    if ffmpeg_bin:
        try:
            ffmpeg_path = Path(ffmpeg_bin)
            probe_name = "ffprobe.exe" if ffmpeg_path.suffix.lower() == ".exe" else "ffprobe"
            sibling = ffmpeg_path.with_name(probe_name)
            if sibling.exists() and sibling.is_file():
                return str(sibling)
        except Exception:
            pass
    candidates = [
        "ffprobe",
        "ffprobe.exe",
        "/usr/bin/ffprobe",
        "/usr/local/bin/ffprobe",
        "C:\\ffmpeg\\bin\\ffprobe.exe",
    ]
    for candidate in candidates:
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
        try:
            p = Path(candidate)
            if p.exists() and p.is_file():
                return str(p)
        except Exception:
            continue
    return None


def _probe_video_duration_seconds(video_file: Path, ffmpeg_bin: Optional[str] = None) -> float:
    if video_file is None or not video_file.exists():
        return 0.0

    try:
        _, _, fps, frames = _probe_video_stream_meta(video_file)
        if fps > 0 and frames > 0:
            duration = float(frames) / float(fps)
            if duration > 0.2:
                return duration
    except Exception:
        pass

    ffprobe_bin = _resolve_ffprobe_binary(ffmpeg_bin=ffmpeg_bin)
    if not ffprobe_bin:
        return 0.0
    cmd = [
        ffprobe_bin,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(video_file),
    ]
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
            timeout=60,
        )
        if proc.returncode != 0:
            return 0.0
        out = (proc.stdout or "").strip().splitlines()
        if not out:
            return 0.0
        duration = float(out[-1].strip())
        if duration > 0.2:
            return duration
    except Exception:
        return 0.0
    return 0.0


def _split_video_for_upload(video_file: Path, cfg: Dict[str, Any]) -> List[Path]:
    if video_file is None or not video_file.exists():
        return []
    if not bool(_cfg_get(cfg, "gemini.split_upload_enabled", True)):
        return []

    size_bytes = int(video_file.stat().st_size)
    size_mb = size_bytes / (1024 * 1024)
    trigger_mb = max(1.0, float(_cfg_get(cfg, "gemini.split_upload_only_if_larger_mb", 14.0)))
    if size_mb <= trigger_mb:
        return []

    chunk_max_mb = max(2.0, float(_cfg_get(cfg, "gemini.split_upload_chunk_max_mb", 6.0)))
    max_chunks = max(2, int(_cfg_get(cfg, "gemini.split_upload_max_chunks", 4)))
    split_count = int(math.ceil(size_mb / chunk_max_mb))
    split_count = max(2, min(max_chunks, split_count))
    if split_count <= 1:
        return []

    ffmpeg_bin = _resolve_ffmpeg_binary()
    if not ffmpeg_bin:
        print("[video] split upload skipped: ffmpeg not available.")
        return []

    duration_sec = _probe_video_duration_seconds(video_file, ffmpeg_bin=ffmpeg_bin)
    if duration_sec <= 0.2:
        print("[video] split upload skipped: could not determine video duration.")
        return []

    stem = video_file.stem
    parent = video_file.parent
    out_files = [parent / f"{stem}_upload_part{i + 1:02d}.mp4" for i in range(split_count)]
    if all(p.exists() and p.stat().st_size > 0 and _is_probably_mp4(p) for p in out_files):
        total_mb = sum(float(p.stat().st_size) for p in out_files) / (1024 * 1024)
        print(
            f"[video] using cached split upload parts: {len(out_files)} parts "
            f"({total_mb:.1f} MB total)."
        )
        return out_files

    # Remove stale parts before generating a fresh set.
    for stale in parent.glob(f"{stem}_upload_part*.mp4"):
        try:
            stale.unlink(missing_ok=True)
        except Exception:
            pass

    chunk_duration = duration_sec / float(split_count)
    use_reencode_on_copy_fail = bool(_cfg_get(cfg, "gemini.split_upload_reencode_on_copy_fail", True))
    print(
        f"[video] splitting upload video into {split_count} parts "
        f"(source={size_mb:.1f} MB, duration={duration_sec:.1f}s)."
    )
    produced: List[Path] = []
    for idx, out_path in enumerate(out_files):
        start_sec = idx * chunk_duration
        if idx == split_count - 1:
            dur_sec = max(0.2, duration_sec - start_sec)
        else:
            dur_sec = max(0.2, chunk_duration)

        cmd_copy = [
            ffmpeg_bin,
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            f"{start_sec:.3f}",
            "-t",
            f"{dur_sec:.3f}",
            "-i",
            str(video_file),
            "-map",
            "0:v:0",
            "-an",
            "-sn",
            "-dn",
            "-c:v",
            "copy",
            "-movflags",
            "+faststart",
            str(out_path),
        ]
        ok = False
        try:
            proc = subprocess.run(
                cmd_copy,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                text=True,
                timeout=240,
            )
            ok = proc.returncode == 0 and out_path.exists() and out_path.stat().st_size > 0 and _is_probably_mp4(out_path)
        except Exception:
            ok = False

        if not ok and use_reencode_on_copy_fail:
            cmd_enc = [
                ffmpeg_bin,
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-ss",
                f"{start_sec:.3f}",
                "-t",
                f"{dur_sec:.3f}",
                "-i",
                str(video_file),
                "-an",
                "-sn",
                "-dn",
                "-c:v",
                "libx264",
                "-preset",
                "faster",
                "-crf",
                "21",
                "-movflags",
                "+faststart",
                str(out_path),
            ]
            try:
                proc = subprocess.run(
                    cmd_enc,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                    text=True,
                    timeout=240,
                )
                ok = proc.returncode == 0 and out_path.exists() and out_path.stat().st_size > 0 and _is_probably_mp4(out_path)
            except Exception:
                ok = False

        if not ok:
            print(f"[video] split chunk failed at part {idx + 1}; falling back to single-file flow.")
            for p in out_files:
                try:
                    p.unlink(missing_ok=True)
                except Exception:
                    pass
            return []

        produced.append(out_path)
        try:
            part_mb = out_path.stat().st_size / (1024 * 1024)
            print(f"[video] split part {idx + 1}/{split_count}: {out_path.name} ({part_mb:.1f} MB)")
        except Exception:
            pass

    if len(produced) != split_count:
        return []
    return produced


def _segment_chunks(segments: List[Dict[str, Any]], max_per_chunk: int) -> List[List[Dict[str, Any]]]:
    if not segments:
        return []
    step = max(1, int(max_per_chunk))
    chunks: List[List[Dict[str, Any]]] = []
    for i in range(0, len(segments), step):
        chunks.append(segments[i : i + step])
    return chunks


def _extract_video_window(
    src_video: Path,
    out_video: Path,
    start_sec: float,
    end_sec: float,
    ffmpeg_bin: Optional[str] = None,
) -> bool:
    ffmpeg_path = ffmpeg_bin or _resolve_ffmpeg_binary()
    if not ffmpeg_path:
        return False
    if src_video is None or not src_video.exists():
        return False
    if end_sec <= start_sec:
        return False

    duration = max(0.2, float(end_sec - start_sec))
    try:
        _ensure_parent(out_video)
        if out_video.exists():
            out_video.unlink()
    except Exception:
        pass

    copy_cmd = [
        ffmpeg_path,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{max(0.0, start_sec):.3f}",
        "-t",
        f"{duration:.3f}",
        "-i",
        str(src_video),
        "-map",
        "0:v:0",
        "-an",
        "-sn",
        "-dn",
        "-c:v",
        "copy",
        "-movflags",
        "+faststart",
        str(out_video),
    ]
    try:
        proc = subprocess.run(
            copy_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
            timeout=240,
        )
        if proc.returncode == 0 and out_video.exists() and out_video.stat().st_size > 0 and _is_probably_mp4(out_video):
            return True
    except Exception:
        pass

    # Fallback re-encode for better cut accuracy when stream-copy fails.
    encode_cmd = [
        ffmpeg_path,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{max(0.0, start_sec):.3f}",
        "-t",
        f"{duration:.3f}",
        "-i",
        str(src_video),
        "-an",
        "-sn",
        "-dn",
        "-c:v",
        "libx264",
        "-preset",
        "fast",
        "-crf",
        "22",
        "-movflags",
        "+faststart",
        str(out_video),
    ]
    try:
        proc = subprocess.run(
            encode_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
            timeout=300,
        )
        return bool(
            proc.returncode == 0
            and out_video.exists()
            and out_video.stat().st_size > 0
            and _is_probably_mp4(out_video)
        )
    except Exception:
        return False


def _transcode_video_ffmpeg(
    src: Path,
    dst: Path,
    scale: float,
    target_fps: float,
    min_width: int,
    ffmpeg_bin: Optional[str] = None,
) -> Tuple[bool, str]:
    ffmpeg_path = ffmpeg_bin or _resolve_ffmpeg_binary()
    if not ffmpeg_path:
        return False, "ffmpeg binary not found"

    # Keep width even and avoid going below min_width to keep decoder compatibility.
    vf = (
        f"scale=max({min_width}\\,trunc(iw*{float(scale):.4f}/2)*2):-2,"
        f"fps={max(1.0, float(target_fps)):.2f}"
    )
    codec_attempts: List[List[str]] = [
        ["-c:v", "libx264", "-preset", "veryfast", "-crf", "30"],
        ["-c:v", "mpeg4", "-q:v", "10"],
    ]
    last_err = ""
    for codec_opts in codec_attempts:
        try:
            _ensure_parent(dst)
            if dst.exists():
                dst.unlink()
        except Exception:
            pass
        cmd = [
            ffmpeg_path,
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(src),
            "-vf",
            vf,
            "-an",
            "-sn",
            "-dn",
            *codec_opts,
            "-movflags",
            "+faststart",
            str(dst),
        ]
        try:
            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                text=True,
                timeout=420,
            )
        except Exception as exc:
            last_err = str(exc)
            continue
        if proc.returncode == 0 and dst.exists() and dst.stat().st_size > 0 and _is_probably_mp4(dst):
            return True, ""
        stderr_snippet = (proc.stderr or "").strip()
        if stderr_snippet:
            stderr_snippet = stderr_snippet.splitlines()[-1]
        last_err = stderr_snippet or f"ffmpeg exit code {proc.returncode}"
    return False, last_err


def _transcode_video_cv2(
    src: Path,
    dst: Path,
    scale: float,
    target_fps: float,
    min_width: int,
) -> bool:
    try:
        import cv2  # type: ignore
    except Exception:
        return False

    cap = cv2.VideoCapture(str(src))
    if not cap.isOpened():
        return False
    try:
        src_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
        src_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
        src_fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
        if src_w <= 0 or src_h <= 0:
            return False
        if src_fps <= 0.1 or src_fps > 240:
            src_fps = 24.0

        scaled_w = max(min_width, int(round(src_w * float(scale))))
        scaled_w = _ensure_even(scaled_w, minimum=min_width)
        scaled_h = int(round(src_h * (scaled_w / float(src_w))))
        scaled_h = _ensure_even(scaled_h, minimum=2)

        target_fps = max(1.0, min(float(target_fps), src_fps))
        frame_interval = max(1, int(round(src_fps / target_fps)))
        out_fps = max(1.0, src_fps / frame_interval)

        _ensure_parent(dst)
        if dst.exists():
            dst.unlink()

        writer = None
        for codec in ("mp4v", "avc1", "H264", "XVID"):
            try:
                fourcc = cv2.VideoWriter_fourcc(*codec)
                candidate = cv2.VideoWriter(str(dst), fourcc, out_fps, (scaled_w, scaled_h))
                if candidate.isOpened():
                    writer = candidate
                    break
                candidate.release()
            except Exception:
                continue
        if writer is None:
            return False

        frame_idx = 0
        written = 0
        while True:
            ok, frame = cap.read()
            if not ok:
                break
            if frame_interval > 1 and (frame_idx % frame_interval) != 0:
                frame_idx += 1
                continue
            if frame.shape[1] != scaled_w or frame.shape[0] != scaled_h:
                frame = cv2.resize(frame, (scaled_w, scaled_h), interpolation=cv2.INTER_AREA)
            writer.write(frame)
            written += 1
            frame_idx += 1
        writer.release()
        if written <= 0:
            return False
    finally:
        cap.release()

    return dst.exists() and dst.stat().st_size > 0 and _is_probably_mp4(dst)


def _maybe_optimize_video_for_upload(video_file: Path, cfg: Dict[str, Any]) -> Path:
    if video_file is None or not video_file.exists():
        return video_file

    enabled = bool(_cfg_get(cfg, "gemini.optimize_video_for_upload", True))
    if not enabled:
        return video_file

    size_bytes = int(video_file.stat().st_size)
    size_mb = size_bytes / (1024 * 1024)
    trigger_mb = max(1.0, float(_cfg_get(cfg, "gemini.optimize_video_only_if_larger_mb", 8.0)))
    if size_mb <= trigger_mb:
        return video_file

    target_mb = max(1.0, float(_cfg_get(cfg, "gemini.optimize_video_target_mb", 4.0)))
    target_bytes = int(target_mb * 1024 * 1024)
    target_fps = max(1.0, float(_cfg_get(cfg, "gemini.optimize_video_target_fps", 10.0)))
    min_fps = max(1.0, float(_cfg_get(cfg, "gemini.optimize_video_min_fps", 8.0)))
    target_fps = max(min_fps, target_fps)
    min_width = max(160, int(_cfg_get(cfg, "gemini.optimize_video_min_width", 320)))
    min_short_side = max(160, int(_cfg_get(cfg, "gemini.optimize_video_min_short_side", 320)))
    scales = _parse_float_list(
        _cfg_get(cfg, "gemini.optimize_video_scale_candidates", [0.75, 0.6, 0.5, 0.4, 0.33, 0.25, 0.2]),
        [0.75, 0.6, 0.5, 0.4, 0.33, 0.25, 0.2],
    )
    src_w, src_h, src_fps, _ = _probe_video_stream_meta(video_file)
    scales = _quality_preserving_scale_candidates(
        scales=scales,
        src_w=src_w,
        src_h=src_h,
        min_width=min_width,
        min_short_side=min_short_side,
    )

    out_file = video_file.with_name(f"{video_file.stem}_upload_opt.mp4")
    if out_file.exists():
        try:
            out_size = int(out_file.stat().st_size)
            if out_size > 0 and _is_probably_mp4(out_file) and out_size <= target_bytes:
                print(
                    f"[video] using cached optimized upload file: {out_file} "
                    f"({out_size / (1024 * 1024):.1f} MB)"
                )
                return out_file
        except Exception:
            pass

    src_meta_note = ""
    if src_w > 0 and src_h > 0:
        fps_note = f", {src_fps:.1f}fps" if src_fps > 0 else ""
        src_meta_note = f", source={src_w}x{src_h}{fps_note}"
    print(
        f"[video] optimizing video for upload: {video_file.name} "
        f"({size_mb:.1f} MB -> target <= {target_mb:.1f} MB{src_meta_note})"
    )
    cv2_available = _opencv_available()
    ffmpeg_bin = _resolve_ffmpeg_binary()
    if not cv2_available and ffmpeg_bin:
        print(f"[video] OpenCV unavailable; using ffmpeg optimizer backend: {ffmpeg_bin}")
    elif not cv2_available and not ffmpeg_bin:
        print("[video] OpenCV and ffmpeg are unavailable; cannot optimize upload video.")
    candidates: List[Path] = []
    best_path: Optional[Path] = None
    best_size = size_bytes
    backend_used: Optional[str] = None
    ffmpeg_last_error = ""

    for scale in scales:
        scale = max(0.1, min(1.0, float(scale)))
        suffix = int(round(scale * 100))
        cand = video_file.with_name(f"{video_file.stem}_upload_opt_s{suffix}.mp4")
        ok = False
        if cv2_available:
            try:
                ok = _transcode_video_cv2(
                    src=video_file,
                    dst=cand,
                    scale=scale,
                    target_fps=target_fps,
                    min_width=min_width,
                )
                if ok:
                    backend_used = "cv2"
            except Exception:
                ok = False
        if not ok and ffmpeg_bin:
            try:
                ok, ffmpeg_err = _transcode_video_ffmpeg(
                    src=video_file,
                    dst=cand,
                    scale=scale,
                    target_fps=target_fps,
                    min_width=min_width,
                    ffmpeg_bin=ffmpeg_bin,
                )
                if ok:
                    backend_used = "ffmpeg"
                elif ffmpeg_err:
                    ffmpeg_last_error = ffmpeg_err
            except Exception as exc:
                ffmpeg_last_error = str(exc)
                ok = False
        if not ok:
            continue
        candidates.append(cand)
        try:
            cand_size = int(cand.stat().st_size)
        except Exception:
            continue
        backend_note = f" ({backend_used})" if backend_used else ""
        print(f"[video] optimized candidate scale={scale:.2f}: {cand_size / (1024 * 1024):.1f} MB{backend_note}")
        if cand_size < best_size:
            best_size = cand_size
            best_path = cand
        if cand_size <= target_bytes:
            break

    if best_path is None:
        if ffmpeg_last_error:
            print(f"[video] ffmpeg optimizer failed: {ffmpeg_last_error}")
        print("[video] upload optimization not available; using original video.")
        return video_file

    try:
        if out_file.exists():
            out_file.unlink()
        best_path.replace(out_file)
    except Exception:
        out_file = best_path

    for cand in candidates:
        if cand == out_file:
            continue
        try:
            cand.unlink(missing_ok=True)
        except Exception:
            continue

    out_size = int(out_file.stat().st_size) if out_file.exists() else size_bytes
    if out_size >= size_bytes:
        print("[video] optimization did not reduce size enough; using original video.")
        return video_file
    print(
        f"[video] optimized upload video ready: {out_file} "
        f"({out_size / (1024 * 1024):.1f} MB)"
    )
    return out_file


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
    validate_video_decode = bool(_cfg_get(cfg, "gemini.validate_video_decode", True))
    scan_attempts = max(1, int(_cfg_get(cfg, "gemini.video_candidate_scan_attempts", 4)))
    scan_wait_ms = max(200, int(_cfg_get(cfg, "gemini.video_candidate_scan_wait_ms", 1200)))
    resume_from_artifacts = bool(_cfg_get(cfg, "run.resume_from_artifacts", True))

    primary_target = out_dir / video_name
    if resume_from_artifacts and primary_target.exists():
        try:
            size_bytes = primary_target.stat().st_size
            if size_bytes >= min_video_bytes and _is_probably_mp4(primary_target):
                if validate_video_decode and not _is_video_decodable(primary_target):
                    print(f"[video] cached file looks corrupted; re-downloading: {primary_target}")
                    try:
                        primary_target.unlink(missing_ok=True)
                    except Exception:
                        pass
                else:
                    size_mb = size_bytes / (1024 * 1024)
                    print(f"[video] reusing existing file: {primary_target} ({size_mb:.1f} MB)")
                    return primary_target
        except Exception:
            pass

    def _nudge_video_network() -> None:
        try:
            page.evaluate(
                """() => {
                    const v = document.querySelector('video');
                    if (!v) return;
                    try { v.muted = true; v.play(); } catch (e) {}
                }"""
            )
            page.wait_for_timeout(900)
            page.evaluate(
                """() => {
                    const v = document.querySelector('video');
                    if (!v) return;
                    try {
                        if (Number.isFinite(v.currentTime)) {
                            v.currentTime = Math.max(0, Number(v.currentTime || 0) + 0.05);
                        }
                        v.pause();
                    } catch (e) {}
                }"""
            )
        except Exception:
            pass

    network_seen: set[str] = set()
    network_candidates: List[str] = []

    def _remember_network_video_url(raw_url: str, content_type: str = "") -> None:
        try:
            raw = html.unescape((raw_url or "").strip())
            if not raw:
                return
            low_ct = (content_type or "").lower()
            if "video" not in low_ct and not _looks_like_video_url(raw):
                return
            if raw in network_seen:
                return
            network_seen.add(raw)
            network_candidates.append(raw)
        except Exception:
            return

    response_listener = None
    try:
        def _on_response(resp: Any) -> None:
            try:
                headers = resp.headers or {}
            except Exception:
                headers = {}
            try:
                content_type = str(headers.get("content-type", "") or "")
            except Exception:
                content_type = ""
            try:
                _remember_network_video_url(str(resp.url or ""), content_type=content_type)
            except Exception:
                return
        response_listener = _on_response
        page.on("response", response_listener)
    except Exception:
        response_listener = None

    def _rank_video_url(url: str) -> Tuple[int, int, int]:
        low = (url or "").lower()
        return (
            0 if re.search(r"\.(mp4|webm|mov|m4v|m3u8)(\?|$)", low, flags=re.I) else 1,
            0 if "atlascapture" in low or "cloudflarestorage.com" in low else 1,
            len(url or ""),
        )

    page.wait_for_timeout(1500)
    _dismiss_blocking_modals(page)
    candidates: List[str] = []
    for scan_idx in range(scan_attempts):
        if scan_idx > 0:
            page.wait_for_timeout(scan_wait_ms)
            _dismiss_blocking_modals(page)
        _nudge_video_network()
        candidates = _collect_video_url_candidates(page, cfg)
        for from_net in network_candidates:
            if from_net not in candidates:
                candidates.append(from_net)
        candidates.sort(key=_rank_video_url)
        if candidates:
            if scan_idx > 0:
                print(f"[video] candidate urls resolved after retry {scan_idx + 1}/{scan_attempts}.")
            break
        if scan_idx < scan_attempts - 1:
            print(f"[video] no candidate urls yet ({scan_idx + 1}/{scan_attempts}); retrying...")

    if response_listener is not None:
        try:
            page.remove_listener("response", response_listener)
        except Exception:
            pass

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
            _download_video_from_page_context(
                page=page,
                context=context,
                video_url=url,
                out_path=target,
                timeout_sec=timeout_sec,
                cfg=cfg,
            )
            size_bytes = target.stat().st_size
            size_mb = size_bytes / (1024 * 1024)
            if size_bytes < min_video_bytes:
                print(f"[video] skip candidate (too small {size_bytes} bytes): {url}")
                continue
            if not _is_probably_mp4(target):
                print(f"[video] skip candidate (not mp4 signature): {url}")
                continue
            if validate_video_decode and not _is_video_decodable(target):
                print(f"[video] skip candidate (decode check failed): {url}")
                continue
            print(f"[video] downloaded: {target} ({size_mb:.1f} MB)")
            return target
        except Exception as exc:
            last_err = exc
            continue

    if require_video:
        if last_err is None:
            last_err = RuntimeError("no candidate video URLs discovered")
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


def _dismiss_blocking_modals(page: Page, cfg: Optional[Dict[str, Any]] = None) -> None:
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
    passes = max(1, int(_cfg_get(cfg, "run.modal_dismiss_passes", 2) if cfg else 2))
    click_timeout_ms = max(
        50,
        int(_cfg_get(cfg, "run.modal_dismiss_timeout_ms", 120) if cfg else 120),
    )
    post_click_wait_ms = max(
        0,
        int(_cfg_get(cfg, "run.modal_dismiss_post_click_wait_ms", 180) if cfg else 180),
    )

    for _ in range(passes):
        clicked_any = False
        seen_any = False
        for sel in modal_buttons:
            loc = _first_visible_locator(page, sel, timeout_ms=click_timeout_ms)
            if loc is None:
                continue
            seen_any = True
            try:
                loc.click(timeout=click_timeout_ms)
                clicked_any = True
            except Exception:
                continue
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
            if post_click_wait_ms > 0:
                page.wait_for_timeout(post_click_wait_ms)
        else:
            if not seen_any:
                break
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
            _dismiss_blocking_side_panel(page, cfg, aggressive=True)
            row.scroll_into_view_if_needed()
            row.click(timeout=2200, no_wait_after=True)
            return
        except Exception as exc:
            last_exc = exc
            _dismiss_blocking_modals(page)
            _dismiss_blocking_side_panel(page, cfg, aggressive=(attempt >= 1))
            try:
                row = rows.nth(idx - 1)
                row.click(timeout=1200, force=True, no_wait_after=True)
                return
            except Exception as force_exc:
                last_exc = force_exc
                try:
                    row.evaluate(
                        """(el) => {
                            if (!el) return false;
                            const evt = { bubbles: true, cancelable: true, view: window };
                            el.dispatchEvent(new MouseEvent('mousedown', evt));
                            el.dispatchEvent(new MouseEvent('mouseup', evt));
                            el.dispatchEvent(new MouseEvent('click', evt));
                            if (typeof el.click === 'function') el.click();
                            return true;
                        }"""
                    )
                    return
                except Exception as js_exc:
                    last_exc = js_exc
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


def _respect_reserve_min_interval(cfg: Dict[str, Any]) -> None:
    global _LAST_RESERVE_REQUEST_TS
    min_interval_sec = max(0, int(_cfg_get(cfg, "run.reserve_min_interval_sec", 90)))
    if min_interval_sec <= 0:
        return
    if _LAST_RESERVE_REQUEST_TS <= 0:
        return
    elapsed = time.time() - _LAST_RESERVE_REQUEST_TS
    remaining = min_interval_sec - elapsed
    if remaining > 0:
        print(f"[atlas] waiting {int(remaining)}s before next reserve attempt (min-interval).")
        time.sleep(remaining)


def _mark_reserve_request() -> None:
    global _LAST_RESERVE_REQUEST_TS
    _LAST_RESERVE_REQUEST_TS = time.time()


def _click_reserve_button_dynamic(page: Page, cfg: Dict[str, Any], timeout_ms: int = 2500) -> Tuple[bool, str]:
    reserve_btn = str(_cfg_get(cfg, "atlas.selectors.reserve_episodes_button", "")).strip()
    reserve_loc = _first_visible_locator(page, reserve_btn, timeout_ms=timeout_ms) if reserve_btn else None
    if reserve_loc is not None:
        try:
            txt = (_safe_locator_text(reserve_loc, timeout_ms=600) or "").strip()
            reserve_loc.click(timeout=2000)
            return True, txt
        except Exception:
            pass

    try:
        result = page.evaluate(
            """() => {
                const items = Array.from(document.querySelectorAll('button, [role="button"], a'));
                const isVisible = (el) => {
                    const st = window.getComputedStyle(el);
                    const r = el.getBoundingClientRect();
                    return st && st.visibility !== 'hidden' && st.display !== 'none' && r.width > 0 && r.height > 0;
                };
                const pickScore = (text) => {
                    const t = (text || '').toLowerCase().replace(/\\s+/g, ' ').trim();
                    if (!t.includes('reserve')) return -1;
                    const m = t.match(/reserve\\s*(\\d+)\\s*episodes?/i);
                    if (m) return 100 + parseInt(m[1] || '0', 10);
                    if (t.includes('reserve new episode')) return 60;
                    if (t.includes('reserve') && t.includes('episode')) return 50;
                    return 10;
                };

                let best = null;
                for (const el of items) {
                    if (!isVisible(el)) continue;
                    const text = (el.innerText || el.textContent || '').replace(/\\s+/g, ' ').trim();
                    const score = pickScore(text);
                    if (score < 0) continue;
                    if (!best || score > best.score) best = { el, text, score };
                }
                if (!best) return { clicked: false, text: '' };
                best.el.click();
                return { clicked: true, text: best.text || '' };
            }"""
        )
        if isinstance(result, dict) and bool(result.get("clicked")):
            return True, str(result.get("text", "") or "")
    except Exception:
        pass
    return False, ""


def _extract_wait_seconds_from_page(page: Page, default_wait_sec: int = 5) -> int:
    try:
        body = (page.inner_text("body") or "").lower()
    except Exception:
        body = ""
    hard_cap = max(5, int(default_wait_sec))
    if not body:
        return hard_cap
    if not re.search(
        r"(too many requests|rate[\s-]?limit|try again in|please wait|temporarily unavailable)",
        body,
    ):
        return hard_cap
    m = re.search(r"(?:try again in|wait|after)\s*(\d+)\s*(seconds?|minutes?|mins?)", body)
    if m:
        try:
            amount = int(m.group(1))
            unit = (m.group(2) or "").lower()
            if amount <= 0:
                return hard_cap
            if unit.startswith("second"):
                return min(hard_cap, max(5, amount))
            return min(hard_cap, amount * 60)
        except Exception:
            pass
    return hard_cap


def _reserve_rate_limited(page: Page) -> bool:
    try:
        body = (page.inner_text("body") or "").lower()
    except Exception:
        body = ""
    if not body:
        return False
    if "reserve" not in body and "episode" not in body:
        return False
    explicit_patterns = [
        r"try again in\s*\d+\s*(?:second|seconds|minute|minutes|min)?",
        r"too many requests",
        r"rate[\s-]?limit",
        r"temporarily unavailable",
        r"please wait",
    ]
    for pat in explicit_patterns:
        try:
            if re.search(pat, body):
                return True
        except Exception:
            continue
    return False


def _room_has_no_reserved_episodes(page: Page, cfg: Dict[str, Any]) -> bool:
    probe_timeout_ms = max(200, int(_cfg_get(cfg, "run.reserve_no_reserved_probe_timeout_ms", 900)))
    reserve_btn_selector = str(_cfg_get(cfg, "atlas.selectors.reserve_episodes_button", "")).strip()
    body = ""
    try:
        body = (page.inner_text("body") or "").lower()
    except Exception:
        body = ""
    if not body:
        return False
    no_reserved_markers = (
        "no episodes reserved",
        "no episode reserved",
    )
    if not any(marker in body for marker in no_reserved_markers):
        return False
    if reserve_btn_selector:
        reserve_loc = _first_visible_locator(page, reserve_btn_selector, timeout_ms=probe_timeout_ms)
        if reserve_loc is not None:
            return True
    return "reserve" in body and "episode" in body


def _release_all_reserved_episodes(page: Page, cfg: Dict[str, Any]) -> bool:
    room_url = str(_cfg_get(cfg, "atlas.room_url", "")).strip()
    release_btn = str(_cfg_get(cfg, "atlas.selectors.release_all_button", "")).strip()
    confirm_release_btn = str(_cfg_get(cfg, "atlas.selectors.confirm_release_button", "")).strip()
    if not release_btn:
        return False

    if room_url:
        try:
            _goto_with_retry(
                page,
                room_url,
                wait_until="domcontentloaded",
                timeout_ms=45000,
                cfg=cfg,
                reason="room-before-release-all",
            )
        except Exception:
            pass
    _dismiss_blocking_modals(page, cfg)
    if not _safe_locator_click(page, release_btn, timeout_ms=3500):
        print("[atlas] release-all button not found; skipping release cycle.")
        return False
    page.wait_for_timeout(450)

    confirm_clicks = 0
    release_dialog_sel = (
        'div[role="dialog"]:has-text("Release all episodes") '
        '|| div[role="dialog"]:has-text("Release All") '
        '|| [role="dialog"]:has-text("Release all episodes")'
    )
    modal_release_btn = (
        'div[role="dialog"] button:has-text("Release All") '
        '|| div[role="dialog"] [role="button"]:has-text("Release All")'
    )
    try:
        if _wait_for_any(page, release_dialog_sel, timeout_ms=2200):
            if _safe_locator_click(page, modal_release_btn, timeout_ms=2600):
                confirm_clicks += 1
                page.wait_for_timeout(500)
    except Exception:
        pass

    if confirm_clicks == 0 and confirm_release_btn:
        for _ in range(2):
            if _safe_locator_click(page, confirm_release_btn, timeout_ms=2200):
                confirm_clicks += 1
                page.wait_for_timeout(450)
            else:
                break

    page.wait_for_timeout(850)
    total_clicks = 1 + confirm_clicks
    print(f"[atlas] release-all requested for current reserved episodes (clicks={total_clicks}).")
    return True


def _respect_episode_delay(cfg: Dict[str, Any]) -> None:
    min_delay = float(_cfg_get(cfg, "run.min_delay_between_episodes_sec", 0.0) or 0.0)
    max_delay = float(_cfg_get(cfg, "run.max_delay_between_episodes_sec", 0.0) or 0.0)
    if max_delay < min_delay:
        min_delay, max_delay = max_delay, min_delay
    min_delay = max(0.0, min_delay)
    max_delay = max(0.0, max_delay)
    if max_delay <= 0:
        return
    delay = min_delay if max_delay == min_delay else random.uniform(min_delay, max_delay)
    print(f"[run] waiting {delay:.1f}s before next episode.")
    time.sleep(delay)


def _compute_backoff_delay(cfg: Dict[str, Any], attempt: int) -> float:
    base_delay = max(0.2, float(_cfg_get(cfg, "gemini.retry_base_delay_sec", 2.0)))
    jitter_max = max(0.0, float(_cfg_get(cfg, "gemini.retry_jitter_sec", 0.8)))
    max_backoff = max(base_delay, float(_cfg_get(cfg, "gemini.max_backoff_sec", 30.0)))
    delay = min(max_backoff, base_delay * (2**attempt))
    if jitter_max > 0:
        delay += random.uniform(0.0, jitter_max)
    return delay


def _extract_retry_seconds_from_text(text: str, default_wait_sec: float = 0.0) -> float:
    body = (text or "").lower()
    if not body:
        return max(0.0, float(default_wait_sec))

    # Support compound waits like "Please retry in 3h52m42.1s".
    compound_match = re.search(r"(?:retry|try again|please retry)\s+in\s*([^\n\r,;]+)", body)
    if compound_match:
        fragment = compound_match.group(1)[:96]
        total_sec = 0.0
        token_count = 0
        for amount_str, unit in re.findall(
            r"([0-9]+(?:\.[0-9]+)?)\s*(h|hr|hrs|hour|hours|m|min|mins|minute|minutes|s|sec|secs|second|seconds)",
            fragment,
        ):
            try:
                amount = float(amount_str)
            except Exception:
                continue
            if amount <= 0:
                continue
            token_count += 1
            unit_char = unit[0]
            if unit_char == "h":
                total_sec += amount * 3600.0
            elif unit_char == "m":
                total_sec += amount * 60.0
            else:
                total_sec += amount
        if token_count > 0 and total_sec > 0:
            return total_sec

    patterns: List[Tuple[str, float]] = [
        (r"(?:retry|try again|please retry)\s+in\s*([0-9]+(?:\.[0-9]+)?)\s*(?:h|hr|hrs|hour|hours)\b", 3600.0),
        (r"(?:retry|try again|please retry)\s+in\s*([0-9]+(?:\.[0-9]+)?)\s*(?:s|sec|secs|second|seconds)\b", 1.0),
        (r"(?:retry|try again|please retry)\s+in\s*([0-9]+(?:\.[0-9]+)?)\s*(?:m|min|mins|minute|minutes)\b", 60.0),
        (r"wait\s*([0-9]+(?:\.[0-9]+)?)\s*(?:h|hr|hrs|hour|hours)\b", 3600.0),
        (r"wait\s*([0-9]+(?:\.[0-9]+)?)\s*(?:s|sec|secs|second|seconds)\b", 1.0),
        (r"wait\s*([0-9]+(?:\.[0-9]+)?)\s*(?:m|min|mins|minute|minutes)\b", 60.0),
    ]
    for pattern, multiplier in patterns:
        m = re.search(pattern, body)
        if not m:
            continue
        try:
            amount = float(m.group(1))
        except Exception:
            continue
        if amount > 0:
            return amount * multiplier
    return max(0.0, float(default_wait_sec))


def _extract_retry_seconds_from_response(resp: requests.Response, default_wait_sec: float = 0.0) -> float:
    retry_after = (resp.headers.get("Retry-After") or "").strip()
    if retry_after:
        try:
            value = float(retry_after)
            if value > 0:
                return value
        except Exception:
            pass
    return _extract_retry_seconds_from_text(resp.text or "", default_wait_sec=default_wait_sec)


def _set_gemini_quota_cooldown(wait_sec: float) -> float:
    global _GEMINI_QUOTA_COOLDOWN_UNTIL_TS
    wait_sec = max(0.0, float(wait_sec))
    if wait_sec <= 0:
        return 0.0
    until_ts = time.time() + wait_sec
    if until_ts > _GEMINI_QUOTA_COOLDOWN_UNTIL_TS:
        _GEMINI_QUOTA_COOLDOWN_UNTIL_TS = until_ts
    return wait_sec


def _respect_gemini_quota_cooldown(cfg: Dict[str, Any]) -> None:
    global _GEMINI_QUOTA_COOLDOWN_UNTIL_TS
    now = time.time()
    if _GEMINI_QUOTA_COOLDOWN_UNTIL_TS <= now:
        return
    remaining = _GEMINI_QUOTA_COOLDOWN_UNTIL_TS - now
    max_wait = max(0.0, float(_cfg_get(cfg, "gemini.quota_cooldown_max_wait_sec", 120.0)))
    wait_sec = remaining if max_wait <= 0 else min(remaining, max_wait)
    if wait_sec <= 0:
        return
    print(f"[gemini] quota cooldown active: sleeping {wait_sec:.1f}s.")
    time.sleep(wait_sec)
    if time.time() >= _GEMINI_QUOTA_COOLDOWN_UNTIL_TS - 0.05:
        _GEMINI_QUOTA_COOLDOWN_UNTIL_TS = 0.0


def _respect_gemini_rate_limit(cfg: Dict[str, Any]) -> None:
    global _LAST_GEMINI_REQUEST_TS, _GEMINI_REQUEST_TIMESTAMPS
    if not bool(_cfg_get(cfg, "gemini.rate_limit_enabled", True)):
        return
    rpm = max(1, int(_cfg_get(cfg, "gemini.rate_limit_requests_per_minute", 9)))
    window_sec = max(5.0, float(_cfg_get(cfg, "gemini.rate_limit_window_sec", 60.0)))
    min_interval_sec = max(0.0, float(_cfg_get(cfg, "gemini.rate_limit_min_interval_sec", 0.0)))

    now = time.time()
    cutoff = now - window_sec
    _GEMINI_REQUEST_TIMESTAMPS = [ts for ts in _GEMINI_REQUEST_TIMESTAMPS if ts >= cutoff]

    wait_sec = 0.0
    if len(_GEMINI_REQUEST_TIMESTAMPS) >= rpm:
        earliest = min(_GEMINI_REQUEST_TIMESTAMPS)
        wait_sec = max(wait_sec, (earliest + window_sec) - now + 0.01)
    if min_interval_sec > 0 and _LAST_GEMINI_REQUEST_TS > 0:
        wait_sec = max(wait_sec, (_LAST_GEMINI_REQUEST_TS + min_interval_sec) - now)

    if wait_sec > 0:
        print(
            f"[gemini] rate limiter: sleeping {wait_sec:.1f}s "
            f"(limit={rpm}/{int(window_sec)}s)."
        )
        time.sleep(wait_sec)
        now = time.time()
        cutoff = now - window_sec
        _GEMINI_REQUEST_TIMESTAMPS = [ts for ts in _GEMINI_REQUEST_TIMESTAMPS if ts >= cutoff]

    sent_at = time.time()
    _GEMINI_REQUEST_TIMESTAMPS.append(sent_at)
    _LAST_GEMINI_REQUEST_TS = sent_at


def _is_non_retriable_gemini_error(exc: Exception) -> bool:
    msg = str(exc or "").lower()
    if not msg:
        return False
    fatal_markers = [
        "missing gemini api key",
        "api key not valid",
        "permission denied",
        "unauthorized",
        "forbidden",
    ]
    return any(marker in msg for marker in fatal_markers)


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


def _invalidate_cached_labels(cfg: Dict[str, Any], task_id: str) -> None:
    cache_path = _labels_cache_path(cfg, task_id)
    if cache_path is None or not cache_path.exists():
        return
    try:
        cache_path.unlink()
        print(f"[gemini] invalidated cached labels for task {task_id}: {cache_path}")
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
    try:
        page.goto(login_url, wait_until="domcontentloaded")
    except Exception as exc:
        if _is_too_many_redirects_error(exc):
            print("[auth] login redirect loop detected; clearing Atlas session and retrying login page once.")
            _clear_atlas_site_session(page)
            page.goto(login_url, wait_until="domcontentloaded")
        else:
            raise
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


def goto_task_room(
    page: Page,
    cfg: Dict[str, Any],
    skip_task_ids: Optional[set[str]] = None,
    status_out: Optional[Dict[str, Any]] = None,
) -> bool:
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
    blocked_task_ids = skip_task_ids if skip_task_ids is not None else set()
    if isinstance(status_out, dict):
        status_out["all_visible_blocked"] = False
    release_all_on_internal_error = bool(_cfg_get(cfg, "run.release_all_on_internal_error", True))
    reserve_rate_limit_wait_sec = max(5, int(_cfg_get(cfg, "run.reserve_rate_limit_wait_sec", 5)))
    release_requested_by_internal_error = False
    current_url = (page.url or "").strip()
    current_l = current_url.lower()

    if "/tasks/room/normal/label/" in page.url:
        return True

    if room_url:
        print(f"[atlas] goto room url: {room_url}")
        room_norm = room_url.rstrip("/").lower()
        current_norm = current_url.rstrip("/").lower()
        if current_norm == room_norm or "/tasks/room/normal" in current_l:
            print("[atlas] already on room page; skipping duplicate room navigation.")
        else:
            _goto_with_retry(page, room_url, wait_until="domcontentloaded", timeout_ms=45000, cfg=cfg, reason="goto-room")
    elif dashboard_url:
        print(f"[atlas] goto dashboard url: {dashboard_url}")
        page.goto(dashboard_url, wait_until="domcontentloaded")

    def _recover_standard_workflow_entry() -> None:
        url_l = (page.url or "").lower()
        if "/tasks" not in url_l:
            return
        if "/tasks/room/normal" in url_l or "/tasks/room/normal/label/" in url_l:
            return
        enter_clicks = max(1, int(_cfg_get(cfg, "run.workflow_reentry_enter_clicks", 2)))
        second_click_delay_sec = max(0.0, float(_cfg_get(cfg, "run.workflow_reentry_second_click_delay_sec", 5.0)))
        clicked_any = False
        for i in range(enter_clicks):
            clicked = _safe_locator_click(page, enter_workflow, timeout_ms=5000)
            if clicked:
                clicked_any = True
                print(f"[atlas] workflow recovery: clicked Enter Standard Workflow ({i + 1}/{enter_clicks}).")
            if i == 0 and enter_clicks > 1 and second_click_delay_sec > 0:
                print(f"[atlas] workflow recovery: waiting {second_click_delay_sec:.1f}s before second Enter click.")
                time.sleep(second_click_delay_sec)
            page.wait_for_timeout(700)
        if not clicked_any:
            return
        _safe_locator_click(page, continue_room, timeout_ms=4500)
        _safe_locator_click(page, label_button, timeout_ms=4500)
        page.wait_for_timeout(700)

    def _wait_label_page_ready() -> None:
        checks = max(1, int(_cfg_get(cfg, "run.label_open_loading_max_checks", 5)))
        wait_ms = max(120, int(_cfg_get(cfg, "run.label_open_loading_wait_ms", 600)))
        for _ in range(checks):
            _dismiss_blocking_modals(page)
            try:
                body = (page.inner_text("body") or "").lower()
            except Exception:
                body = ""
            if "loading..." not in body:
                break
            page.wait_for_timeout(wait_ms)

    def _handle_internal_error_release_cycle() -> None:
        nonlocal release_requested_by_internal_error
        if not release_requested_by_internal_error:
            return
        _release_all_reserved_episodes(page, cfg)
        release_requested_by_internal_error = False
        if room_url:
            try:
                _goto_with_retry(
                    page,
                    room_url,
                    wait_until="domcontentloaded",
                    timeout_ms=45000,
                    cfg=cfg,
                    reason="room-after-release-internal-error",
                )
            except Exception:
                pass
        page.wait_for_timeout(900)

    def _open_label_target(target: str, reason: str, log_label: str) -> bool:
        nonlocal release_requested_by_internal_error
        _goto_with_retry(page, target, wait_until="commit", timeout_ms=45000, cfg=cfg, reason=reason)
        print(f"[atlas] opened label task by {log_label}: {target}")
        _wait_label_page_ready()
        if _is_label_page_actionable(page, cfg, timeout_ms=5000):
            return True
        bad_task_id = _task_id_from_url(page.url) or _task_id_from_url(target)
        if bad_task_id:
            blocked_task_ids.add(bad_task_id)
            print(f"[atlas] label page unavailable; task blocked for this run: {bad_task_id}")
        if _is_label_page_internal_error(page):
            _try_go_back_from_label_error(page, cfg)
            print("[atlas] label page failed with internal error; clicked Go Back.")
            if release_all_on_internal_error:
                release_requested_by_internal_error = True
                print("[atlas] internal error detected; release-all cycle requested.")
        elif _is_label_page_not_found(page):
            print("[atlas] label URL returned not-found page; trying another task.")
        else:
            print("[atlas] label page opened but video/segments are unavailable; trying another task.")
        if room_url:
            try:
                _goto_with_retry(
                    page,
                    room_url,
                    wait_until="domcontentloaded",
                    timeout_ms=45000,
                    cfg=cfg,
                    reason="room-after-invalid-label",
                )
            except Exception:
                pass
        return False

    _recover_standard_workflow_entry()

    # Fast-path: room page already has direct label links.
    if label_task_link:
        for href_from_html in _all_task_label_hrefs_from_page(page):
            tid = _task_id_from_url(href_from_html)
            if tid and tid in blocked_task_ids:
                continue
            target = href_from_html if href_from_html.startswith("http") else f"https://audit.atlascapture.io{href_from_html}"
            if _open_label_target(target, reason="open-label-fast", log_label="html href (fast-path)"):
                return True
            if release_requested_by_internal_error:
                _handle_internal_error_release_cycle()
                break

    current_l = (page.url or "").lower()
    if tasks_nav and "/tasks/room/normal" not in current_l and "/tasks/room/normal/label/" not in current_l:
        _safe_locator_click(page, tasks_nav, timeout_ms=3000)
    _safe_locator_click(page, enter_workflow, timeout_ms=4000)
    if wait_sec > 0:
        time.sleep(wait_sec)
    _safe_locator_click(page, continue_room, timeout_ms=4000)
    _safe_locator_click(page, label_button, timeout_ms=4000)

    # In room view, reserve episodes if needed, then open first concrete label task URL.
    if label_task_link:
        page.wait_for_timeout(1000)
        reserve_attempts = max(1, int(_cfg_get(cfg, "run.reserve_attempts_per_visit", 3)))
        label_wait_timeout_ms = max(1500, int(_cfg_get(cfg, "run.reserve_label_wait_timeout_ms", 12000)))
        label_wait_after_reserve_timeout_ms = max(
            1000, int(_cfg_get(cfg, "run.reserve_label_wait_timeout_after_reserve_ms", 3500))
        )
        reserve_refresh_after_click = bool(_cfg_get(cfg, "run.reserve_refresh_after_click", True))
        reserve_wait_only_on_rate_limit = bool(_cfg_get(cfg, "run.reserve_wait_only_on_rate_limit", True))
        reserve_immediate_when_no_reserved = bool(_cfg_get(cfg, "run.reserve_immediate_when_no_reserved", True))
        reserve_skip_initial_label_scan_when_no_reserved = bool(
            _cfg_get(cfg, "run.reserve_skip_initial_label_scan_when_no_reserved", True)
        )
        skip_reserve_when_all_visible_blocked = bool(
            _cfg_get(cfg, "run.skip_reserve_when_all_visible_blocked", False)
        )

        def _open_first_label_from_page(reason: str) -> bool:
            nonlocal release_requested_by_internal_error
            href_candidates = _all_task_label_hrefs_from_page(page)
            attempted_unblocked_candidate = False
            for href_from_html in href_candidates:
                tid = _task_id_from_url(href_from_html)
                if tid and tid in blocked_task_ids:
                    continue
                attempted_unblocked_candidate = True
                target = href_from_html if href_from_html.startswith("http") else f"https://audit.atlascapture.io{href_from_html}"
                if _open_label_target(target, reason=reason, log_label="html href"):
                    return True
                if release_requested_by_internal_error:
                    return False

            if href_candidates and blocked_task_ids and not attempted_unblocked_candidate:
                print(f"[atlas] all visible label tasks are blocked in this run ({len(blocked_task_ids)} blocked).")
                if isinstance(status_out, dict):
                    status_out["all_visible_blocked"] = True
                return False

            link_loc = _first_visible_locator(page, label_task_link, timeout_ms=2500)
            if link_loc is None:
                href = _first_href_from_selector(page, label_task_link)
                if href:
                    target = href if href.startswith("http") else f"https://audit.atlascapture.io{href}"
                    return _open_label_target(target, reason=f"{reason}-href", log_label="href")
                return False

            try:
                href = link_loc.get_attribute("href")
                link_loc.click()
                if href:
                    print(f"[atlas] opened label task: {href}")
                _wait_label_page_ready()
            except Exception:
                return False
            if "/tasks/room/normal/label/" in page.url:
                if _is_label_page_actionable(page, cfg, timeout_ms=5000):
                    return True
                bad_task_id = _task_id_from_url(page.url) or _task_id_from_url(href or "")
                if bad_task_id:
                    blocked_task_ids.add(bad_task_id)
                    print(f"[atlas] label page unavailable; task blocked for this run: {bad_task_id}")
                if _is_label_page_internal_error(page):
                    _try_go_back_from_label_error(page, cfg)
                    if release_all_on_internal_error:
                        release_requested_by_internal_error = True
                if room_url:
                    try:
                        _goto_with_retry(
                            page,
                            room_url,
                            wait_until="domcontentloaded",
                            timeout_ms=45000,
                            cfg=cfg,
                            reason="room-after-click-invalid-label",
                        )
                    except Exception:
                        pass
                return False
            href = _first_href_from_selector(page, label_task_link)
            if href:
                target = href if href.startswith("http") else f"https://audit.atlascapture.io{href}"
                return _open_label_target(target, reason=f"{reason}-href-fallback", log_label="href fallback")
            return False

        no_reserved_episodes = reserve_immediate_when_no_reserved and _room_has_no_reserved_episodes(page, cfg)
        if no_reserved_episodes:
            print("[atlas] no reserved episodes detected; reserving immediately.")
        try_initial_label_scan = not (no_reserved_episodes and reserve_skip_initial_label_scan_when_no_reserved)

        if try_initial_label_scan and _open_first_label_from_page("open-label-html"):
            return True
        if (
            try_initial_label_scan
            and
            isinstance(status_out, dict)
            and bool(status_out.get("all_visible_blocked"))
            and skip_reserve_when_all_visible_blocked
        ):
            print("[atlas] skipping reserve: all visible tasks are blocked in this run.")
            return False
        if release_requested_by_internal_error:
            _handle_internal_error_release_cycle()

        for reserve_attempt in range(1, reserve_attempts + 1):
            reserved = False
            reserve_label = ""
            if not no_reserved_episodes:
                _respect_reserve_min_interval(cfg)
                if not reserve_wait_only_on_rate_limit:
                    _respect_reserve_cooldown(cfg)
            clicked, reserve_label = _click_reserve_button_dynamic(page, cfg, timeout_ms=2500)
            if clicked:
                try:
                    reserved = True
                    _mark_reserve_request()
                    if reserve_label:
                        print(f"[atlas] reserve requested: '{reserve_label}' ({reserve_attempt}/{reserve_attempts}).")
                    else:
                        print(f"[atlas] reserve requested ({reserve_attempt}/{reserve_attempts}).")
                except Exception:
                    reserved = False

            if reserved:
                _safe_locator_click(page, confirm_reserve_btn, timeout_ms=4500)
                if _reserve_rate_limited(page):
                    wait_sec = _extract_wait_seconds_from_page(page, default_wait_sec=reserve_rate_limit_wait_sec)
                    print(f"[atlas] reserve is rate-limited; waiting {wait_sec}s then retrying reserve.")
                    time.sleep(wait_sec)
                    if room_url:
                        try:
                            _goto_with_retry(
                                page,
                                room_url,
                                wait_until="domcontentloaded",
                                timeout_ms=45000,
                                cfg=cfg,
                                reason="room-after-reserve-rate-limit",
                            )
                        except Exception:
                            pass
                    continue
                if reserve_refresh_after_click and room_url:
                    try:
                        _goto_with_retry(
                            page,
                            room_url,
                            wait_until="domcontentloaded",
                            timeout_ms=45000,
                            cfg=cfg,
                            reason="room-refresh-after-reserve",
                        )
                    except Exception:
                        pass

            _safe_locator_click(page, label_button, timeout_ms=3500)
            wait_timeout_ms = label_wait_after_reserve_timeout_ms if reserved else label_wait_timeout_ms
            if no_reserved_episodes and not reserved:
                wait_timeout_ms = min(wait_timeout_ms, 2500)
            _wait_for_any(page, label_task_link, timeout_ms=wait_timeout_ms)
            page.wait_for_timeout(700)
            if _open_first_label_from_page("open-label-after-reserve"):
                return True
            if release_requested_by_internal_error:
                _handle_internal_error_release_cycle()
                continue

            if reserve_attempt < reserve_attempts:
                page.wait_for_timeout(250 if no_reserved_episodes else 900)

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
    text = (text or "").replace("\u2192", "->").replace("\u2013", "-")
    matches = re.findall(r"\b\d+:\d{2}(?:\.\d+)?\b", text)
    if len(matches) >= 2:
        return _parse_mmss_to_seconds(matches[0]), _parse_mmss_to_seconds(matches[1])
    if len(matches) == 1:
        start = _parse_mmss_to_seconds(matches[0])
        # Atlas may show "(6.0s)" duration while end timestamp isn't directly extracted.
        dur_match = re.search(r"\((\d+(?:\.\d+)?)\s*s\)", text, flags=re.IGNORECASE)
        if dur_match:
            try:
                dur_sec = float(dur_match.group(1))
                if dur_sec > 0:
                    return start, start + dur_sec
            except ValueError:
                pass
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

        # Fallback extraction from row text when either timestamp is missing.
        fb_start, fb_end = _extract_start_end_from_text(raw_text)
        if start_sec <= 0 and fb_start > 0:
            start_sec = fb_start
        if end_sec <= 0 and fb_end > 0:
            end_sec = fb_end

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


def _enforce_gemini_output_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize Gemini output to the expected contract:
    {"operations": [...], "segments": [...]}
    """
    normalized = dict(payload or {})
    operations = normalized.get("operations", [])
    normalized["operations"] = operations if isinstance(operations, list) else []
    segments = normalized.get("segments")
    if not isinstance(segments, list):
        raise ValueError("Gemini payload must contain list at 'segments'")
    return normalized


def _parse_json_text(text: str) -> Dict[str, Any]:
    payload = json.loads(_clean_json_text(text))
    if isinstance(payload, dict):
        return _enforce_gemini_output_contract(payload)
    if isinstance(payload, list):
        return _enforce_gemini_output_contract({"operations": [], "segments": payload})
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


def _normalize_operation_action(action: str) -> str:
    a = str(action or "").strip().lower()
    aliases = {
        "e": "edit",
        "edit": "edit",
        "s": "split",
        "split": "split",
        "d": "delete",
        "delete": "delete",
        "remove": "delete",
        "m": "merge",
        "merge": "merge",
    }
    return aliases.get(a, "")


def _normalize_operations(payload: Dict[str, Any], cfg: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    raw_ops = payload.get("operations", [])
    if not isinstance(raw_ops, list):
        return []
    max_ops = max(0, int(_cfg_get(cfg or {}, "run.max_structural_operations", 12)))
    structural_allow_split = bool(_cfg_get(cfg or {}, "run.structural_allow_split", False))
    structural_allow_merge = bool(_cfg_get(cfg or {}, "run.structural_allow_merge", False))
    structural_allow_delete = bool(_cfg_get(cfg or {}, "run.structural_allow_delete", False))
    out: List[Dict[str, Any]] = []
    for item in raw_ops:
        action = ""
        idx = 0
        if isinstance(item, dict):
            action = _normalize_operation_action(
                str(item.get("action") or item.get("op") or item.get("type") or "")
            )
            idx_raw = item.get("segment_index", item.get("index", item.get("segment", 0)))
            try:
                idx = int(idx_raw)
            except Exception:
                idx = 0
        elif isinstance(item, str):
            token = item.strip().lower()
            # Examples: "split 3", "d 5"
            m = re.match(r"([a-z]+)\s+(\d+)$", token)
            if m:
                action = _normalize_operation_action(m.group(1))
                idx = int(m.group(2))
            else:
                action = _normalize_operation_action(token)
        if not action or idx <= 0:
            continue
        if action == "split" and not structural_allow_split:
            continue
        if action == "merge" and not structural_allow_merge:
            continue
        if action == "delete" and not structural_allow_delete:
            continue
        out.append({"action": action, "segment_index": idx})
        if max_ops and len(out) >= max_ops:
            break
    return out


def build_prompt(
    segments: List[Dict[str, Any]],
    extra_instructions: str,
    allow_operations: bool = True,
) -> str:
    header = (
        "You are an Atlas Standard Tier-3 labeling assistant.\n"
        "You may receive the full task video as attached media plus employee segment text.\n"
        "Use the video as source of truth; employee labels may be wrong.\n"
        "Destroy and rebuild: treat draft phrasing as potentially flawed and rewrite from scratch from video evidence.\n"
        "For each segment index, output corrected label and timestamps when needed.\n"
        "Apply one-mental-model policy: one continuous interaction toward one goal per segment.\n"
        "Gripper rule: treat gripper as an extension of hand.\n"
        "Usually do NOT mention the tool in labels; if unavoidable, use only 'gripper'.\n"
        "Never use tool terms like mechanical arm / robotic arm / robot arm / manipulator / claw arm.\n"
        "Split only when goal changes or hands disengage/restart; do not split only for No Action pauses.\n"
        "Continuity rule: if same coarse goal continues without disengaging from the object, keep one coarse segment.\n"
        "CRITICAL continuity: if draft has 3 or more consecutive segments of the same ongoing action "
        "and tool/object is never dropped, you MUST merge them.\n"
        "Coarse-goal verbs: avoid mechanical muscle-motion phrasing (e.g., 'move saw back and forth'). "
        "Use task-goal verbs (e.g., 'cut wood with saw', 'sand board with sandpaper').\n"
        "No token stuttering: never repeat words/phrases like 'detangle detangle' or 'pull loosened pull loosened'.\n"
        "No '-ing' verbs: use imperative commands only (e.g., 'turn mold', not 'turning the mold').\n"
        "Timestamp strictness: describe only what happens inside each exact segment start_sec/end_sec; "
        "do not shift actions into neighboring segments.\n"
        "Use coarse single-goal labels for repetitive actions; use dense labels only when needed.\n"
        "Dense labels may include multiple atomic actions separated by commas/and.\n"
        "Do not exceed 20 words or 2 atomic actions per label (typically one separator: a single comma or one 'and').\n"
        "Do not write narrative filler words like then/another/next/continue/again.\n"
        "For small corrective reorientation/reposition, prefer verb 'adjust'.\n"
        "Avoid forbidden verbs: rotate, inspect, check, look, examine, reach, grab, relocate.\n"
        "Use conservative object names that are directly visible.\n"
        "If object identity is unclear after careful inspection, use safe general nouns (tool/container/item).\n"
        "Do not guess hidden object identities and do not keep placeholder/default labels.\n"
        "If surface type/elevation is unclear (floor mat vs table/shelf), do not guess raised furniture.\n"
        "Use neutral location wording (on surface/on mat/on floor) unless elevation is clearly visible.\n"
        "Use 'place' only with explicit location (on/in/into/onto/at/to/inside/under/over).\n"
        "No-Action pause rule: if ego still holds the task object/tool during a pause, do not use 'No Action'. "
        "Keep/merge it with surrounding action.\n"
        "Attach verbs to objects: do not write 'pick up and place box' or 'pick up box and place under desk'; "
        "write 'pick up box, place box under desk'.\n"
        "If the segment clearly includes lift then placement, include both actions (pick up ..., place ...).\n"
        "Independent rewrite: treat input draft labels as potentially flawed; if a label violates Tier-3 phrasing, "
        "rewrite from scratch rather than patching shorthand.\n"
        "No shortcuts: do not merge distinct physical interactions into one invalid phrase to save words.\n"
        "Avoid body-part wording (hands/fingers/body parts) unless unavoidable.\n"
        "Examples:\n"
        "BAD: pick up and place stack of paper\n"
        "GOOD: pick up stack of paper from desk, place stack of paper into cardboard box\n"
        "BAD: place bag\n"
        "GOOD: place bag in cabinet\n"
        "BAD: seg1='move saw to cut wood board' + seg2='finish cutting wood board' while interaction is continuous\n"
        "GOOD: merge into one segment label 'cut wood board with saw'\n"
        "BAD: seg='No Action' while tool is still held between polish/adjust segments\n"
        "GOOD: merge/relabel pause into surrounding action; do not isolate No Action.\n"
        "BAD: paint chair -> dip paintbrush -> paint chair in separate short consecutive segments\n"
        "GOOD: merge micro-actions into one segment label 'paint chair with paintbrush' when tool is never dropped.\n"
        "BAD: move comb through wig to detangle\n"
        "GOOD: detangle wig with comb\n"
        "BAD: move hair straightener to press wig section\n"
        "GOOD: straighten wig section with hair straightener\n"
        "If a segment timestamp is wrong, correct start_sec/end_sec.\n"
        "Label rules: imperative style, concise, minimum 2 words, maximum 20 words.\n"
        "Use \"No Action\" only as standalone label.\n"
        "If boundaries are fundamentally wrong, you may request structural operations before final labels.\n"
        "Allowed operations: edit, split, delete, merge.\n"
        "Operation segment_index refers to the row index at execution time.\n"
        "Operations must be ordered exactly as they should be executed.\n"
        "Return strict JSON object only:\n"
        "Response must start with '{' and end with '}'.\n"
        "Do not wrap JSON in markdown code fences.\n"
        "{\"operations\":[{\"action\":\"split\",\"segment_index\":3}],"
        "\"segments\":[{\"segment_index\":1,\"start_sec\":0.0,\"end_sec\":1.2,\"label\":\"...\"}]}\n"
        "If no structural change is needed, return \"operations\":[]\n"
        "Keep segment count and indices unchanged; timestamps must stay monotonic.\n"
    )
    if not allow_operations:
        header += (
            "Structural operations are disabled for this pass.\n"
            "Return operations as an empty list.\n"
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


def _read_optional_text_file(path_text: str) -> str:
    path_raw = (path_text or "").strip()
    if not path_raw:
        return ""
    try:
        p = Path(path_raw)
        if not p.is_absolute():
            p = (Path.cwd() / p).resolve()
        if not p.exists():
            return ""
        return p.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _resolve_system_instruction(cfg: Dict[str, Any]) -> str:
    file_text = _read_optional_text_file(str(_cfg_get(cfg, "gemini.system_instruction_file", "")))
    inline_text = str(_cfg_get(cfg, "gemini.system_instruction_text", "")).strip()
    chunks = [txt for txt in [file_text, inline_text] if txt]
    return "\n\n".join(chunks).strip()


def _count_atomic_actions_in_label(label: str) -> int:
    text = (label or "").strip()
    if not text:
        return 0
    if text.lower() == "no action":
        return 1
    count = 0
    for part in re.split(r"\s*,\s*", text):
        chunk = part.strip()
        if not chunk:
            continue
        subparts = [p.strip() for p in re.split(r"\band\b", chunk, flags=re.IGNORECASE) if p.strip()]
        if subparts:
            count += len(subparts)
        else:
            count += 1
    return max(1, count)


_DISALLOWED_TOOL_TERMS = (
    "mechanical arm",
    "robotic arm",
    "robot arm",
    "manipulator",
    "robot gripper",
    "claw arm",
)


def _normalize_gripper_terms(text: str) -> str:
    out = text or ""
    for term in _DISALLOWED_TOOL_TERMS:
        out = re.sub(rf"\b{re.escape(term)}\b", "gripper", out, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", out).strip()


def _validate_segment_plan_against_policy(
    cfg: Dict[str, Any],
    source_segments: List[Dict[str, Any]],
    segment_plan: Dict[int, Dict[str, Any]],
) -> Dict[str, Any]:
    min_words = max(1, int(_cfg_get(cfg, "run.min_label_words", 2)))
    max_words = max(min_words, int(_cfg_get(cfg, "run.max_label_words", 20)))
    max_atomic_actions = max(1, int(_cfg_get(cfg, "run.max_atomic_actions_per_label", 2)))
    forbidden_verbs_raw = _cfg_get(cfg, "run.forbidden_label_verbs", [])
    forbidden_verbs = [str(v).strip().lower() for v in forbidden_verbs_raw if str(v).strip()]
    allowed_verb_token_patterns = _allowed_label_start_verb_token_patterns_from_cfg(cfg)
    forbidden_narrative_raw = _cfg_get(cfg, "run.forbidden_narrative_words", [])
    forbidden_narrative_words = [str(v).strip().lower() for v in forbidden_narrative_raw if str(v).strip()]
    skip_unchanged_lexical = bool(
        _cfg_get(cfg, "run.skip_policy_lexical_checks_on_unchanged_labels", False)
    )
    place_location_pattern = re.compile(r"\bplace\b.*\b(on|in|into|onto|at|to|inside|under|over)\b", re.IGNORECASE)
    chained_verb_without_object_pattern = re.compile(
        r"\b(pick up|place|move|adjust|hold|align|relocate)\s+and\s+(pick up|place|move|adjust|hold|align|relocate)\b",
        re.IGNORECASE,
    )
    orphan_second_place_pattern = re.compile(
        r"\band\s+place\s+(on|in|into|onto|at|to|inside|under|over)\b",
        re.IGNORECASE,
    )
    body_part_reference_pattern = re.compile(
        r"\b(hand|hands|finger|fingers|thumb|thumbs|palm|palms|wrist|wrists)\b",
        re.IGNORECASE,
    )
    token_stuttering_pattern = re.compile(
        r"\b([a-z]+(?:\s+[a-z]+){0,2})\s+\1\b",
        re.IGNORECASE,
    )
    mechanical_motion_pattern = re.compile(
        r"\bmove\s+(?:comb(?:\s+tail)?|hair\s+straightener)\b|"
        r"\bmove\s+\w+\s+back\s+and\s+forth\b",
        re.IGNORECASE,
    )

    source_by_idx: Dict[int, Dict[str, Any]] = {}
    for seg in source_segments:
        try:
            source_by_idx[int(seg.get("segment_index", 0))] = seg
        except Exception:
            continue

    errors: List[str] = []
    warnings: List[str] = []
    prev_start = -1.0
    prev_end = -1.0

    for idx in sorted(segment_plan):
        item = segment_plan[idx]
        label = str(item.get("label", "")).strip()
        label_l = label.lower()
        start = _safe_float(item.get("start_sec"), -1.0)
        end = _safe_float(item.get("end_sec"), -1.0)
        source = source_by_idx.get(idx)
        source_label = str(source.get("current_label", "")).strip() if source is not None else ""
        label_unchanged_from_source = (
            bool(source_label)
            and _normalize_label_for_compare(source_label) == _normalize_label_for_compare(label)
        )

        if not label:
            errors.append(f"segment {idx}: empty label")
        else:
            words = [w for w in re.split(r"\s+", label) if w]
            if label_unchanged_from_source and skip_unchanged_lexical:
                # Avoid blocking whole episodes on legacy/source labels that were not edited now.
                pass
            elif label != "No Action":
                if len(words) < min_words:
                    errors.append(f"segment {idx}: label has fewer than {min_words} words")
                if len(words) > max_words:
                    errors.append(f"segment {idx}: label has more than {max_words} words")
                if not _label_starts_with_allowed_action_verb(label, allowed_verb_token_patterns):
                    errors.append(f"segment {idx}: label must start with an allowed action verb")
                clause_starts_invalid = False
                for clause in _label_action_clauses(label):
                    if not _label_starts_with_allowed_action_verb(clause, allowed_verb_token_patterns):
                        clause_starts_invalid = True
                        break
                if clause_starts_invalid:
                    errors.append(f"segment {idx}: each action clause must start with an allowed action verb")
                for v in forbidden_verbs:
                    if re.search(rf"\b{re.escape(v)}\b", label_l):
                        errors.append(f"segment {idx}: forbidden verb '{v}' found")
                for token in forbidden_narrative_words:
                    if re.search(rf"\b{re.escape(token)}\b", label_l):
                        errors.append(f"segment {idx}: narrative token '{token}' found")
                for term in _DISALLOWED_TOOL_TERMS:
                    if re.search(rf"\b{re.escape(term)}\b", label_l):
                        errors.append(
                            f"segment {idx}: disallowed tool term '{term}' found (use 'gripper' only if unavoidable)"
                        )
                if re.search(r"\bgripper\b", label_l):
                    warnings.append(f"segment {idx}: label mentions 'gripper' (ensure tool mention is unavoidable)")
                if re.search(r"\d", label):
                    errors.append(f"segment {idx}: label contains numerals")
                if body_part_reference_pattern.search(label):
                    errors.append(f"segment {idx}: avoid body-part wording unless unavoidable")
                if token_stuttering_pattern.search(label):
                    errors.append(f"segment {idx}: repeated token/phrase detected (stuttering)")
                if mechanical_motion_pattern.search(label):
                    errors.append(f"segment {idx}: mechanical-motion phrasing detected (use coarse goal verb)")
                if "place" in label_l and not place_location_pattern.search(label):
                    errors.append(f"segment {idx}: 'place' missing explicit location")
                if chained_verb_without_object_pattern.search(label):
                    errors.append(
                        f"segment {idx}: verbs must attach to objects (avoid '<verb> and <verb>' chaining)"
                    )
                if orphan_second_place_pattern.search(label):
                    errors.append(f"segment {idx}: 'place' missing explicit object after conjunction")
                if re.search(r"\bno action\b", label_l) and label_l != "no action":
                    errors.append(f"segment {idx}: 'No Action' must be standalone")
                action_count = _count_atomic_actions_in_label(label)
                if action_count > max_atomic_actions:
                    errors.append(
                        f"segment {idx}: label has more than {max_atomic_actions} atomic actions"
                    )
            else:
                if "," in label or " and " in label_l:
                    errors.append(f"segment {idx}: 'No Action' must be standalone")

        if start < 0 or end < 0:
            errors.append(f"segment {idx}: invalid timestamp values")
        elif end <= start:
            errors.append(f"segment {idx}: end_sec must be greater than start_sec")

        if prev_start >= 0 and start < prev_start - 0.001:
            errors.append(f"segment {idx}: start_sec is not monotonic")
        if prev_end >= 0 and start < prev_end - 0.001:
            errors.append(f"segment {idx}: overlaps previous segment")
        prev_start = max(prev_start, start)
        prev_end = max(prev_end, end)

        if source is not None:
            src_start = _safe_float(source.get("start_sec"), start)
            src_end = _safe_float(source.get("end_sec"), end)
            if abs(start - src_start) > 12 or abs(end - src_end) > 12:
                warnings.append(f"segment {idx}: large timestamp drift from source")

    return {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "segment_count": len(segment_plan),
    }


def _save_validation_report(cfg: Dict[str, Any], task_id: str, report: Dict[str, Any]) -> Optional[Path]:
    out_dir = Path(str(_cfg_get(cfg, "run.output_dir", "outputs")))
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = f"validation_{task_id}.json" if task_id else "validation_report.json"
    path = out_dir / filename
    try:
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return path
    except Exception:
        return None


def _is_timestamp_policy_error(message: str) -> bool:
    m = str(message or "").strip().lower()
    if not m:
        return False
    markers = (
        "invalid timestamp values",
        "end_sec must be greater than start_sec",
        "start_sec is not monotonic",
        "overlaps previous segment",
    )
    return any(token in m for token in markers)


def _is_no_action_policy_error(message: str) -> bool:
    m = str(message or "").strip().lower()
    if not m:
        return False
    return "'no action' must be standalone" in m


def _gemini_file_state(payload: Dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""
    state_obj: Any = payload.get("state", "")
    if isinstance(state_obj, dict):
        for key in ("name", "state"):
            val = state_obj.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip().upper()
        return ""
    if isinstance(state_obj, str):
        return state_obj.strip().upper()
    return ""


def _wait_for_gemini_file_ready(
    api_key: str,
    file_name: str,
    cfg: Dict[str, Any],
    connect_timeout_sec: int,
    request_timeout_sec: int,
) -> None:
    file_name = (file_name or "").strip()
    if not file_name:
        return
    if not file_name.startswith("files/"):
        return

    timeout_sec = max(5, int(_cfg_get(cfg, "gemini.file_ready_timeout_sec", 120)))
    poll_sec = max(0.5, float(_cfg_get(cfg, "gemini.file_ready_poll_sec", 2.0)))
    deadline = time.time() + timeout_sec
    url = f"https://generativelanguage.googleapis.com/v1beta/{file_name}"

    while True:
        try:
            resp = requests.get(
                url,
                params={"key": api_key},
                timeout=(connect_timeout_sec, request_timeout_sec),
            )
            if resp.status_code == 200:
                payload = resp.json()
                state = _gemini_file_state(payload)
                if not state or state in {"ACTIVE", "READY", "SUCCEEDED"}:
                    return
                if state in {"FAILED", "ERROR", "CANCELLED"}:
                    raise RuntimeError(f"Gemini file processing failed: state={state}")
            elif resp.status_code in {404, 429, 500, 502, 503, 504}:
                pass
            else:
                snippet = (resp.text or "")[:200]
                raise RuntimeError(f"Gemini file state check failed HTTP {resp.status_code}: {snippet}")
        except requests.exceptions.RequestException:
            pass

        if time.time() >= deadline:
            raise TimeoutError(f"Gemini file was not ready within {timeout_sec}s: {file_name}")
        time.sleep(poll_sec)


def _normalize_upload_chunk_size(
    requested_chunk_bytes: int,
    size_bytes: int,
    chunk_granularity: int,
) -> int:
    requested = max(64 * 1024, int(requested_chunk_bytes))
    size = max(0, int(size_bytes))
    granularity = max(1, int(chunk_granularity))

    if size <= granularity:
        # Single finalize chunk can be smaller than granularity.
        return size

    # Gemini Files API requires non-final chunk sizes to be multiples of granularity.
    chunk = max(requested, granularity)
    if chunk % granularity != 0:
        chunk = (chunk // granularity) * granularity
        if chunk <= 0:
            chunk = granularity
    return chunk


def _upload_video_via_gemini_files_api(
    api_key: str,
    video_file: Path,
    cfg: Dict[str, Any],
    connect_timeout_sec: int,
    request_timeout_sec: int,
) -> Tuple[str, str]:
    if video_file is None or not video_file.exists():
        raise RuntimeError("Video file is missing for Gemini Files API upload.")

    size_bytes = int(video_file.stat().st_size)
    upload_timeout_sec = max(
        request_timeout_sec,
        int(_cfg_get(cfg, "gemini.upload_request_timeout_sec", 180)),
    )
    requested_chunk_bytes = max(
        64 * 1024,
        int(_cfg_get(cfg, "gemini.upload_chunk_bytes", 8 * 1024 * 1024)),
    )
    chunk_granularity = max(
        1,
        int(_cfg_get(cfg, "gemini.upload_chunk_granularity_bytes", 8 * 1024 * 1024)),
    )
    chunk_bytes = _normalize_upload_chunk_size(
        requested_chunk_bytes=requested_chunk_bytes,
        size_bytes=size_bytes,
        chunk_granularity=chunk_granularity,
    )
    if chunk_bytes != requested_chunk_bytes:
        print(
            "[gemini] adjusted upload_chunk_bytes "
            f"from {requested_chunk_bytes} to {chunk_bytes} "
            f"(granularity={chunk_granularity})."
        )
    chunk_retries = max(0, int(_cfg_get(cfg, "gemini.upload_chunk_max_retries", 5)))

    start_url = "https://generativelanguage.googleapis.com/upload/v1beta/files"
    start_headers = {
        "X-Goog-Upload-Protocol": "resumable",
        "X-Goog-Upload-Command": "start",
        "X-Goog-Upload-Header-Content-Length": str(size_bytes),
        "X-Goog-Upload-Header-Content-Type": "video/mp4",
        "Content-Type": "application/json",
    }
    start_payload = {"file": {"display_name": video_file.name}}
    start_resp: Optional[requests.Response] = None
    last_start_err = ""
    for attempt in range(chunk_retries + 1):
        try:
            start_resp = requests.post(
                start_url,
                params={"key": api_key},
                headers=start_headers,
                json=start_payload,
                timeout=(connect_timeout_sec, upload_timeout_sec),
            )
            if start_resp.status_code // 100 == 2:
                break
            snippet = (start_resp.text or "")[:300]
            last_start_err = f"HTTP {start_resp.status_code}: {snippet}"
        except requests.exceptions.RequestException as exc:
            last_start_err = str(exc)
        if attempt < chunk_retries:
            delay = _compute_backoff_delay(cfg, attempt)
            print(f"[gemini] files API start retry {attempt + 1}/{chunk_retries} in {delay:.1f}s")
            time.sleep(delay)
    if start_resp is None or start_resp.status_code // 100 != 2:
        raise RuntimeError(f"Gemini file upload start failed: {last_start_err}")

    upload_url = (
        start_resp.headers.get("X-Goog-Upload-URL")
        or start_resp.headers.get("x-goog-upload-url")
        or ""
    ).strip()
    if not upload_url:
        raise RuntimeError("Gemini file upload start succeeded but upload URL is missing.")

    def _query_uploaded_offset() -> Optional[int]:
        try:
            resp = requests.post(
                upload_url,
                headers={"X-Goog-Upload-Command": "query"},
                timeout=(connect_timeout_sec, upload_timeout_sec),
            )
        except requests.exceptions.RequestException:
            return None
        if resp.status_code // 100 != 2:
            return None
        raw = (
            resp.headers.get("X-Goog-Upload-Size-Received")
            or resp.headers.get("x-goog-upload-size-received")
            or ""
        ).strip()
        if not raw:
            return None
        try:
            return int(raw)
        except ValueError:
            return None

    data = video_file.read_bytes()
    offset = 0
    upload_resp: Optional[requests.Response] = None
    while offset < size_bytes:
        next_offset = min(size_bytes, offset + chunk_bytes)
        chunk = data[offset:next_offset]
        is_final = next_offset >= size_bytes
        command = "upload, finalize" if is_final else "upload"
        sent = False
        resynced = False
        last_chunk_err = ""

        for attempt in range(chunk_retries + 1):
            try:
                resp = requests.post(
                    upload_url,
                    headers={
                        "X-Goog-Upload-Offset": str(offset),
                        "X-Goog-Upload-Command": command,
                        "Content-Type": "video/mp4",
                    },
                    data=chunk,
                    timeout=(connect_timeout_sec, upload_timeout_sec),
                )
                if resp.status_code // 100 == 2:
                    upload_resp = resp if is_final else upload_resp
                    offset = next_offset
                    sent = True
                    break
                snippet = (resp.text or "")[:220]
                last_chunk_err = f"HTTP {resp.status_code}: {snippet}"
            except requests.exceptions.RequestException as exc:
                last_chunk_err = str(exc)

            if attempt < chunk_retries:
                remote_offset = _query_uploaded_offset()
                if remote_offset is not None and remote_offset > offset:
                    offset = min(remote_offset, size_bytes)
                    resynced = True
                    print(f"[gemini] files upload resumed at offset {offset}/{size_bytes}.")
                    break
                delay = _compute_backoff_delay(cfg, attempt)
                print(
                    f"[gemini] files chunk upload retry {attempt + 1}/{chunk_retries} "
                    f"at offset {offset} in {delay:.1f}s"
                )
                time.sleep(delay)

        if sent:
            continue
        if resynced:
            continue
        raise RuntimeError(f"Gemini file chunk upload failed at offset {offset}: {last_chunk_err}")

    if upload_resp is None:
        raise RuntimeError("Gemini file upload finalize response missing.")

    try:
        upload_payload = upload_resp.json()
    except Exception as exc:
        raise RuntimeError("Gemini file upload finalize returned non-JSON response.") from exc

    file_info: Dict[str, Any]
    if isinstance(upload_payload, dict) and isinstance(upload_payload.get("file"), dict):
        file_info = upload_payload["file"]
    elif isinstance(upload_payload, dict):
        file_info = upload_payload
    else:
        raise RuntimeError("Gemini file upload finalize returned unexpected payload shape.")

    file_uri = str(file_info.get("uri", "")).strip()
    file_name = str(file_info.get("name", "")).strip()
    if not file_uri:
        raise RuntimeError("Gemini file upload succeeded but file URI is missing.")

    _wait_for_gemini_file_ready(
        api_key=api_key,
        file_name=file_name,
        cfg=cfg,
        connect_timeout_sec=connect_timeout_sec,
        request_timeout_sec=request_timeout_sec,
    )
    return file_uri, file_name


def _is_gemini_quota_error_text(text: str) -> bool:
    body = (text or "").lower()
    quota_markers = (
        "quota exceeded",
        "exceeded your current quota",
        "free_tier",
        "resource_exhausted",
        "generate_content_free_tier_requests",
    )
    return any(marker in body for marker in quota_markers)


def _is_gemini_quota_exceeded_429(resp: requests.Response) -> bool:
    if resp.status_code != 429:
        return False
    return _is_gemini_quota_error_text(resp.text or "")


def _is_gemini_quota_error(exc: Exception) -> bool:
    return _is_gemini_quota_error_text(str(exc or ""))


def _build_gemini_generation_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build deterministic JSON-oriented generationConfig while keeping fields optional.
    """
    temperature = float(_cfg_get(cfg, "gemini.temperature", 0.0))
    candidate_count = max(1, int(_cfg_get(cfg, "gemini.candidate_count", 1)))
    top_p_raw = _cfg_get(cfg, "gemini.top_p", None)
    top_k_raw = _cfg_get(cfg, "gemini.top_k", None)
    max_output_tokens_raw = _cfg_get(cfg, "gemini.max_output_tokens", None)

    gen_cfg: Dict[str, Any] = {
        "temperature": temperature,
        "responseMimeType": "application/json",
        "candidateCount": candidate_count,
    }
    try:
        if top_p_raw is not None and str(top_p_raw).strip() != "":
            top_p = float(top_p_raw)
            if top_p > 0:
                gen_cfg["topP"] = top_p
    except Exception:
        pass
    try:
        if top_k_raw is not None and str(top_k_raw).strip() != "":
            top_k = int(top_k_raw)
            if top_k > 0:
                gen_cfg["topK"] = top_k
    except Exception:
        pass
    try:
        if max_output_tokens_raw is not None and str(max_output_tokens_raw).strip() != "":
            max_tokens = int(max_output_tokens_raw)
            if max_tokens > 0:
                gen_cfg["maxOutputTokens"] = max_tokens
    except Exception:
        pass
    return gen_cfg


def call_gemini_labels(
    cfg: Dict[str, Any],
    prompt: str,
    video_file: Optional[Path] = None,
    segment_count: int = 0,
    model_override: str = "",
) -> Dict[str, Any]:
    model = str(model_override or _cfg_get(cfg, "gemini.model", "gemini-3.1-pro-preview")).strip()
    configured_primary_key = _resolve_gemini_key(str(_cfg_get(cfg, "gemini.api_key", "")))
    configured_fallback_key = _resolve_gemini_fallback_key(str(_cfg_get(cfg, "gemini.fallback_api_key", "")))
    prefer_fallback_key_as_primary = bool(
        _cfg_get(cfg, "gemini.prefer_fallback_key_as_primary", True)
    )
    primary_api_key = configured_primary_key
    fallback_api_key = configured_fallback_key
    if prefer_fallback_key_as_primary and configured_fallback_key:
        primary_api_key = configured_fallback_key
        fallback_api_key = configured_primary_key
        print("[gemini] configured fallback key as primary key source.")
    elif not configured_primary_key and configured_fallback_key:
        primary_api_key = configured_fallback_key
        fallback_api_key = ""
        print("[gemini] primary key missing; using fallback key as primary.")
    if not primary_api_key:
        raise RuntimeError(
            "Missing Gemini API key. Set GEMINI_API_KEY_FALLBACK (preferred) "
            "or GEMINI_API_KEY/GOOGLE_API_KEY."
        )
    quota_fallback_enabled = bool(_cfg_get(cfg, "gemini.quota_fallback_enabled", False))
    quota_fallback_max_uses_per_run = max(
        0,
        int(_cfg_get(cfg, "gemini.quota_fallback_max_uses_per_run", 1)),
    )
    if fallback_api_key and fallback_api_key == primary_api_key:
        fallback_api_key = ""
    if not quota_fallback_enabled:
        fallback_api_key = ""

    system_instruction = _resolve_system_instruction(cfg)
    max_retries = max(0, int(_cfg_get(cfg, "gemini.max_retries", 3)))
    generation_config_template = _build_gemini_generation_config(cfg)
    connect_timeout_sec = max(5, int(_cfg_get(cfg, "gemini.connect_timeout_sec", 30)))
    request_timeout_sec = max(30, int(_cfg_get(cfg, "gemini.request_timeout_sec", 420)))
    require_video = bool(_cfg_get(cfg, "gemini.require_video", False))
    attach_video = bool(_cfg_get(cfg, "gemini.attach_video", True))
    video_attach_block_reason = ""
    skip_video_when_segments_le = max(0, int(_cfg_get(cfg, "gemini.skip_video_when_segments_le", 0)))
    allow_text_fallback = bool(
        _cfg_get(cfg, "gemini.allow_text_only_fallback_on_network_error", True)
    )
    video_transport = str(_cfg_get(cfg, "gemini.video_transport", "auto")).strip().lower() or "auto"
    files_api_fallback_to_inline = bool(_cfg_get(cfg, "gemini.files_api_fallback_to_inline", True))
    max_inline_video_mb = float(_cfg_get(cfg, "gemini.max_inline_video_mb", 20.0))
    inline_retry_targets_mb = _parse_float_list(
        _cfg_get(cfg, "gemini.inline_retry_target_mb", [4.0, 2.5, 1.5, 1.0]),
        [4.0, 2.5, 1.5, 1.0],
    )
    if require_video and not attach_video:
        raise RuntimeError("Invalid config: gemini.require_video=true but gemini.attach_video=false.")

    if (
        attach_video
        and not require_video
        and skip_video_when_segments_le > 0
        and segment_count > 0
        and segment_count <= skip_video_when_segments_le
    ):
        attach_video = False
        video_attach_block_reason = "short_episode_threshold"
        print(
            "[gemini] skipping video attachment for short episode: "
            f"segment_count={segment_count} <= {skip_video_when_segments_le}."
        )
    elif not attach_video:
        video_attach_block_reason = "disabled_by_config"

    active_api_key = primary_api_key
    active_key_name = "primary"
    can_use_secondary = bool(fallback_api_key) and quota_fallback_max_uses_per_run > 0

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    parts: List[Dict[str, Any]] = [{"text": prompt}]
    video_parts: List[Dict[str, Any]] = []
    frame_parts: List[Dict[str, Any]] = []
    reference_frame_bytes = 0
    reference_frame_count_used = 0
    video_transport_used = "none"
    if system_instruction:
        print(f"[gemini] using system instruction ({len(system_instruction)} chars).")

    def _rebuild_parts() -> None:
        nonlocal parts
        parts = [{"text": prompt}]
        if video_parts:
            if len(video_parts) > 1:
                parts.append(
                    {
                        "text": (
                            f"Video context is split into {len(video_parts)} sequential chunks. "
                            "Use all chunks together as one continuous episode timeline."
                        )
                    }
                )
            parts.extend(video_parts)
            if frame_parts:
                parts.extend(frame_parts)

    prepared_video_file = video_file
    prepared_video_files: List[Path] = []
    if attach_video and prepared_video_file is not None and prepared_video_file.exists():
        split_files = _split_video_for_upload(prepared_video_file, cfg)
        if split_files:
            prepared_video_files = split_files
            print(f"[video] split upload enabled: using {len(prepared_video_files)} video chunks.")
        else:
            prepared_video_file = _maybe_optimize_video_for_upload(prepared_video_file, cfg)
            if prepared_video_file is not None and prepared_video_file.exists():
                prepared_video_files = [prepared_video_file]
    source_video_for_retry = video_file if (video_file is not None and video_file.exists()) else prepared_video_file
    split_inline_total_max_mb = float(_cfg_get(cfg, "gemini.split_upload_inline_total_max_mb", 12.0))
    inline_retry_target_idx = 0

    def _build_files_api_video_parts(api_key: str) -> List[Dict[str, Any]]:
        built: List[Dict[str, Any]] = []
        total = len(prepared_video_files)
        for idx, vfile in enumerate(prepared_video_files, start=1):
            file_uri, file_name = _upload_video_via_gemini_files_api(
                api_key=api_key,
                video_file=vfile,
                cfg=cfg,
                connect_timeout_sec=connect_timeout_sec,
                request_timeout_sec=request_timeout_sec,
            )
            built.append({"file_data": {"mime_type": "video/mp4", "file_uri": file_uri}})
            if total > 1:
                print(f"[gemini] attached video chunk {idx}/{total} via Files API: {file_name or file_uri}")
            else:
                print(f"[gemini] attached video via Files API: {file_name or file_uri}")
        return built

    def _build_inline_video_parts(files: List[Path]) -> Optional[List[Dict[str, Any]]]:
        if not files:
            return None
        try:
            total_mb = sum(float(p.stat().st_size) for p in files) / (1024 * 1024)
        except Exception:
            total_mb = 0.0
        if len(files) > 1 and split_inline_total_max_mb > 0 and total_mb > split_inline_total_max_mb:
            return None
        built: List[Dict[str, Any]] = []
        for p in files:
            try:
                part_mb = p.stat().st_size / (1024 * 1024)
            except Exception:
                return None
            if part_mb > max_inline_video_mb:
                return None
            data = base64.b64encode(p.read_bytes()).decode("ascii")
            built.append({"inline_data": {"mime_type": "video/mp4", "data": data}})
        return built

    def _switch_to_smaller_inline_video() -> bool:
        nonlocal prepared_video_file, prepared_video_files, video_parts, include_video, fallback_used, inline_retry_target_idx
        nonlocal frame_parts, reference_frame_bytes, reference_frame_count_used
        if len(prepared_video_files) != 1:
            return False
        if source_video_for_retry is None or not source_video_for_retry.exists():
            return False
        if prepared_video_file is None or not prepared_video_file.exists():
            return False

        current_size = int(prepared_video_file.stat().st_size)
        while inline_retry_target_idx < len(inline_retry_targets_mb):
            target_mb = float(inline_retry_targets_mb[inline_retry_target_idx])
            inline_retry_target_idx += 1
            current_mb = current_size / (1024 * 1024)
            # If current file is already at or below this target, move to the next stricter target.
            if current_mb <= target_mb + 0.05:
                continue
            cfg_retry = _deep_merge(
                cfg,
                {
                    "gemini": {
                        "optimize_video_only_if_larger_mb": 0.0,
                        "optimize_video_target_mb": target_mb,
                    }
                },
            )
            candidate = _maybe_optimize_video_for_upload(source_video_for_retry, cfg_retry)
            if candidate is None or not candidate.exists():
                continue
            try:
                candidate_size = int(candidate.stat().st_size)
            except Exception:
                continue
            if candidate_size <= 0 or candidate_size >= current_size:
                continue
            prepared_video_file = candidate
            prepared_video_files = [prepared_video_file]
            built_inline = _build_inline_video_parts(prepared_video_files)
            if not built_inline:
                continue
            video_parts = built_inline
            include_video = True
            fallback_used = True
            frame_source = source_video_for_retry if source_video_for_retry.exists() else prepared_video_file
            frame_parts, reference_frame_bytes = _extract_reference_frame_inline_parts(
                frame_source,
                cfg,
                trigger_video_mb=(candidate_size / (1024 * 1024)),
            )
            reference_frame_count_used = len(frame_parts)
            _rebuild_parts()
            print(
                f"[gemini] retrying with smaller inline video "
                f"({candidate_size / (1024 * 1024):.1f} MB, target<={target_mb:.1f} MB)."
            )
            return True
        return False

    if attach_video and prepared_video_files:
        total_size_mb = 0.0
        try:
            total_size_mb = sum(float(p.stat().st_size) for p in prepared_video_files) / (1024 * 1024)
        except Exception:
            total_size_mb = 0.0
        wants_files_api = video_transport in {"auto", "files_api", "files"}
        inline_allowed = video_transport in {"auto", "inline"} or (
            wants_files_api and files_api_fallback_to_inline
        )

        if wants_files_api:
            try:
                video_parts = _build_files_api_video_parts(active_api_key)
                video_transport_used = "files_api-multi" if len(video_parts) > 1 else "files_api"
            except Exception as exc:
                print(f"[gemini] files API upload failed: {exc}")
                if not inline_allowed or not files_api_fallback_to_inline:
                    if require_video:
                        raise
                    print("[gemini] continuing without video after Files API failure.")
                else:
                    print("[gemini] falling back to inline video attachment after Files API failure.")

        if not video_parts and inline_allowed:
            if len(prepared_video_files) == 1 and total_size_mb > max_inline_video_mb:
                msg = (
                    f"Video is {total_size_mb:.1f} MB which exceeds max_inline_video_mb={max_inline_video_mb:.1f}. "
                    "Increase gemini.max_inline_video_mb or provide smaller video."
                )
                if require_video:
                    raise RuntimeError(msg)
                    print(f"[video] {msg} Proceeding without attachment.")
            else:
                built_inline = _build_inline_video_parts(prepared_video_files)
                if built_inline:
                    video_parts = built_inline
                    video_transport_used = "inline-multi" if len(video_parts) > 1 else "inline"
                    if len(video_parts) > 1:
                        print(
                            f"[gemini] attached split video inline ({len(video_parts)} parts, "
                            f"{total_size_mb:.1f} MB total)."
                        )
                    else:
                        print(f"[gemini] attached video inline ({total_size_mb:.1f} MB).")
                elif require_video:
                    raise RuntimeError(
                        "Split video inline payload exceeds limits; reduce split chunk size or use Files API."
                    )
                else:
                    print("[gemini] split inline video is too large; continuing without video attachment.")
    else:
        if not attach_video and video_file is not None:
            if video_attach_block_reason == "short_episode_threshold":
                print("[gemini] video attachment skipped for this request due to short-episode threshold.")
            else:
                print("[gemini] video attachment disabled by config (gemini.attach_video=false).")
        elif require_video:
            raise RuntimeError("gemini.require_video=true but no downloadable video file was prepared.")
    include_video = bool(video_parts)
    if include_video:
        try:
            if prepared_video_file is not None and prepared_video_file.exists():
                trigger_mb = prepared_video_file.stat().st_size / (1024 * 1024)
            elif prepared_video_files:
                trigger_mb = sum(float(p.stat().st_size) for p in prepared_video_files) / (1024 * 1024)
            else:
                trigger_mb = 0.0
        except Exception:
            trigger_mb = 0.0
        frame_source = (
            source_video_for_retry
            if (source_video_for_retry is not None and source_video_for_retry.exists())
            else prepared_video_file
        )
        if (frame_source is None or not frame_source.exists()) and prepared_video_files:
            frame_source = prepared_video_files[0]
        if frame_source is not None and frame_source.exists():
            frame_parts, reference_frame_bytes = _extract_reference_frame_inline_parts(
                frame_source,
                cfg,
                trigger_video_mb=trigger_mb,
            )
            reference_frame_count_used = len(frame_parts)
            if reference_frame_count_used > 0:
                print(
                    f"[gemini] attached {reference_frame_count_used} reference frame(s) "
                    f"({reference_frame_bytes / 1024:.0f} KB total)."
                )
    _rebuild_parts()

    last_error = ""
    used_video_in_success = False
    fallback_used = False

    def _switch_to_secondary_key_for_quota() -> bool:
        nonlocal active_api_key, active_key_name
        nonlocal include_video, video_parts, video_transport_used, fallback_used
        nonlocal prepared_video_files, prepared_video_file
        nonlocal frame_parts, reference_frame_bytes, reference_frame_count_used
        global _GEMINI_FALLBACK_USES
        if not quota_fallback_enabled or not can_use_secondary:
            return False
        if active_key_name != "primary":
            return False
        if _GEMINI_FALLBACK_USES >= quota_fallback_max_uses_per_run:
            return False
        active_api_key = fallback_api_key
        active_key_name = "secondary"
        _GEMINI_FALLBACK_USES += 1
        fallback_used = True
        print(
            "[gemini] primary key quota exhausted; switching to secondary key "
            f"({ _GEMINI_FALLBACK_USES }/{quota_fallback_max_uses_per_run}) for this request."
        )

        # Files API uploads are scoped to the key/project. Rebuild attachment for secondary key.
        if include_video and video_transport_used.startswith("files_api"):
            rebuilt = []
            try:
                rebuilt = _build_files_api_video_parts(active_api_key)
            except Exception as exc:
                print(f"[gemini] secondary-key Files API re-upload failed: {exc}")
                rebuilt = []
            if rebuilt:
                video_parts = rebuilt
                include_video = True
                video_transport_used = "files_api-multi" if len(video_parts) > 1 else "files_api"
                _rebuild_parts()
                print("[gemini] rebuilt Files API video attachment with secondary key.")
            else:
                inline_parts = _build_inline_video_parts(prepared_video_files)
                if inline_parts:
                    video_parts = inline_parts
                    include_video = True
                    video_transport_used = "inline-multi" if len(video_parts) > 1 else "inline"
                    _rebuild_parts()
                    print("[gemini] switched to inline video payload after secondary-key fallback.")
                elif not require_video and allow_text_fallback:
                    include_video = False
                    video_parts = []
                    frame_parts = []
                    reference_frame_bytes = 0
                    reference_frame_count_used = 0
                    _rebuild_parts()
                    print("[gemini] secondary-key fallback continuing in text-only mode (video too large for inline).")
        return True

    for attempt in range(max_retries + 1):
        mode = "with-video" if include_video else "text-only"
        print(
            f"[gemini] request attempt {attempt + 1}/{max_retries + 1} "
            f"(model={model}, mode={mode}, key={active_key_name})"
        )
        generation_config = dict(generation_config_template)
        payload = {
            "contents": [{"role": "user", "parts": parts}],
            "generationConfig": generation_config,
        }
        if system_instruction:
            payload["systemInstruction"] = {"parts": [{"text": system_instruction}]}
        try:
            _respect_gemini_quota_cooldown(cfg)
            _respect_gemini_rate_limit(cfg)
            headers = {"Content-Type": "application/json", "X-goog-api-key": active_api_key}
            resp = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=(connect_timeout_sec, request_timeout_sec),
            )
        except requests.exceptions.RequestException as exc:
            last_error = f"Gemini network error: {exc}"
            if include_video and video_transport_used.startswith("inline") and _switch_to_smaller_inline_video():
                continue
            if include_video and not require_video and allow_text_fallback:
                include_video = False
                video_parts = []
                frame_parts = []
                reference_frame_bytes = 0
                reference_frame_count_used = 0
                _rebuild_parts()
                fallback_used = True
                print("[gemini] network error while sending video; switching to text-only fallback.")
                continue
            if attempt < max_retries:
                delay = _compute_backoff_delay(cfg, attempt)
                print(f"[gemini] network error, retrying in {delay:.1f}s")
                time.sleep(delay)
                continue
            break

        if resp.status_code == 200:
            print("[gemini] response received (HTTP 200).")
            used_video_in_success = include_video
            raw_json = resp.json()
            parsed = _parse_gemini_response(raw_json)
            usage_meta = raw_json.get("usageMetadata", {}) if isinstance(raw_json, dict) else {}
            mode_name = "with-video" if include_video else "text-only"
            _log_gemini_usage(
                cfg,
                model=model,
                mode=mode_name,
                usage_meta=usage_meta,
                key_source=active_key_name,
            )
            if isinstance(parsed, dict):
                parsed["_meta"] = {
                    "video_attached": bool(used_video_in_success),
                    "mode": "with-video" if used_video_in_success else "text-only",
                    "fallback_used": bool(fallback_used),
                    "video_transport": video_transport_used,
                    "video_parts_count": int(len(video_parts)) if used_video_in_success else 0,
                    "reference_frames_attached": int(reference_frame_count_used),
                    "reference_frames_total_kb": round(reference_frame_bytes / 1024, 1),
                    "api_key_source": active_key_name,
                    "model": model,
                    "usage": usage_meta if isinstance(usage_meta, dict) else {},
                }
            return parsed

        last_error = f"Gemini HTTP {resp.status_code}: {resp.text[:800]}"
        if _is_gemini_quota_exceeded_429(resp):
            quota_default_wait = max(1.0, float(_cfg_get(cfg, "gemini.quota_retry_default_wait_sec", 12.0)))
            quota_wait_sec = _extract_retry_seconds_from_response(resp, default_wait_sec=quota_default_wait)
            if _switch_to_secondary_key_for_quota():
                continue
            cooldown_wait = _set_gemini_quota_cooldown(quota_wait_sec)
            retry_on_quota_429 = bool(_cfg_get(cfg, "gemini.retry_on_quota_429", False))
            if retry_on_quota_429 and attempt < max_retries:
                delay = max(cooldown_wait, _compute_backoff_delay(cfg, attempt))
                print(f"[gemini] quota error 429, retrying in {delay:.1f}s")
                time.sleep(delay)
                continue
            print(
                "[gemini] quota error 429 detected; skipping extra retries for this request "
                f"(cooldown={cooldown_wait:.1f}s)."
            )
            break
        if (
            include_video
            and not require_video
            and allow_text_fallback
            and resp.status_code in {400, 408, 413, 422}
        ):
            include_video = False
            video_parts = []
            frame_parts = []
            reference_frame_bytes = 0
            reference_frame_count_used = 0
            _rebuild_parts()
            fallback_used = True
            print(
                f"[gemini] HTTP {resp.status_code} while using video; switching to text-only fallback."
            )
            continue
        if include_video and video_transport_used.startswith("inline") and resp.status_code in {400, 408, 413, 422}:
            if _switch_to_smaller_inline_video():
                continue
        if resp.status_code in {429, 500, 502, 503, 504} and attempt < max_retries:
            delay = _compute_backoff_delay(cfg, attempt)
            print(f"[gemini] temporary error {resp.status_code}, retrying in {delay:.1f}s")
            time.sleep(delay)
            continue
        break
    raise RuntimeError(last_error or "Gemini request failed")


_CHUNK_CONSISTENCY_VERB_PREFIXES: Tuple[str, ...] = (
    "pick up",
    "place",
    "open",
    "close",
    "pull open",
    "push",
    "adjust",
    "move",
    "drag",
    "tighten",
    "loosen",
    "remove",
    "insert",
    "fold",
    "spread out",
    "sand",
    "twist",
    "pour",
    "scoop",
    "hold",
    "position",
    "align",
    "pry open",
    "drive",
    "set",
    "put",
)
_CHUNK_CONSISTENCY_EQUIVALENCE_GROUPS: Tuple[Tuple[str, ...], ...] = (
    ("table", "surface"),
)
_CHUNK_CONSISTENCY_PREPOSITION_RE = re.compile(
    r"\b(from|in|into|on|onto|under|inside|at|to|with|over|near|across|through)\b",
    flags=re.IGNORECASE,
)
_CHUNK_CONSISTENCY_TOKEN_RE = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)?", flags=re.IGNORECASE)
_CHUNK_CONSISTENCY_STOPWORDS: set[str] = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "to",
    "with",
    "from",
    "in",
    "into",
    "on",
    "onto",
    "under",
    "inside",
    "at",
    "over",
    "near",
    "across",
    "through",
}


def _consistency_norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip(" ,.;:")).lower()


def _consistency_tokens(text: str) -> List[str]:
    return [t.lower() for t in _CHUNK_CONSISTENCY_TOKEN_RE.findall(text or "")]


def _extract_consistency_terms_from_label(label: str, max_terms: int = 8) -> List[str]:
    if not label:
        return []
    terms: List[str] = []
    clauses = [c.strip().lower() for c in re.split(r",", label) if c and c.strip()]
    for clause in clauses:
        if clause == "no action":
            continue
        rest = clause
        for prefix in _CHUNK_CONSISTENCY_VERB_PREFIXES:
            token = prefix + " "
            if rest.startswith(token):
                rest = rest[len(token) :].strip()
                break
        if not rest:
            continue
        m = _CHUNK_CONSISTENCY_PREPOSITION_RE.search(rest)
        candidates = [rest]
        if m:
            candidates = [rest[: m.start()].strip(), rest[m.end() :].strip()]
        for cand in candidates:
            norm = _consistency_norm(re.sub(r"^(the|a|an)\s+", "", cand))
            if not norm:
                continue
            if norm in _CHUNK_CONSISTENCY_STOPWORDS:
                continue
            if len(_consistency_tokens(norm)) == 0:
                continue
            if norm not in terms:
                terms.append(norm)
                if len(terms) >= max_terms:
                    return terms
    return terms


def _find_equivalent_canonical_term(norm_term: str, canonical_terms: List[str]) -> str:
    if not norm_term:
        return ""
    for existing in canonical_terms:
        if _consistency_norm(existing) == norm_term:
            return existing

    for group in _CHUNK_CONSISTENCY_EQUIVALENCE_GROUPS:
        group_set = {_consistency_norm(x) for x in group}
        if norm_term in group_set:
            for existing in canonical_terms:
                if _consistency_norm(existing) in group_set:
                    return existing

    term_tokens = _consistency_tokens(norm_term)
    if not term_tokens:
        return ""
    term_head = term_tokens[-1]
    term_set = set(term_tokens)
    for existing in canonical_terms:
        existing_norm = _consistency_norm(existing)
        existing_tokens = _consistency_tokens(existing_norm)
        if not existing_tokens:
            continue
        if existing_tokens[-1] != term_head:
            continue
        existing_set = set(existing_tokens)
        overlap = term_set.intersection(existing_set)
        if term_set.issubset(existing_set) or existing_set.issubset(term_set):
            return existing
        if len(overlap) >= max(1, min(len(term_set), len(existing_set)) - 1):
            return existing
    return ""


def _apply_consistency_aliases_to_label(label: str, alias_to_canonical: Dict[str, str]) -> str:
    out = label or ""
    if not out or not alias_to_canonical:
        return out
    replacements = sorted(alias_to_canonical.items(), key=lambda item: len(item[0]), reverse=True)
    for alias_norm, canonical in replacements:
        src = _consistency_norm(alias_norm)
        dst = _consistency_norm(canonical)
        if not src or not dst or src == dst:
            continue
        pattern = r"(?<![a-z0-9])" + re.escape(src) + r"(?![a-z0-9])"
        out = re.sub(pattern, dst, out, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", out).strip()


def _update_chunk_consistency_memory(
    label: str,
    canonical_terms: List[str],
    alias_to_canonical: Dict[str, str],
    memory_limit: int,
) -> str:
    rewritten = _apply_consistency_aliases_to_label(label, alias_to_canonical)
    extracted_terms = _extract_consistency_terms_from_label(rewritten)
    for term in extracted_terms:
        term_norm = _consistency_norm(term)
        if not term_norm:
            continue
        if term_norm in alias_to_canonical:
            continue
        canonical = _find_equivalent_canonical_term(term_norm, canonical_terms)
        if canonical:
            alias_to_canonical[term_norm] = canonical
            rewritten = _apply_consistency_aliases_to_label(rewritten, alias_to_canonical)
            continue
        alias_to_canonical[term_norm] = term
        canonical_terms.append(term)

    if memory_limit > 0 and len(canonical_terms) > memory_limit:
        canonical_terms[:] = canonical_terms[-memory_limit:]
        allowed = {_consistency_norm(term) for term in canonical_terms}
        for alias_key in list(alias_to_canonical.keys()):
            alias_norm = _consistency_norm(alias_key)
            canonical_norm = _consistency_norm(alias_to_canonical.get(alias_key, ""))
            if alias_norm in allowed or canonical_norm in allowed:
                continue
            alias_to_canonical.pop(alias_key, None)
    return rewritten


def _build_chunk_consistency_prompt_hint(canonical_terms: List[str], max_terms: int) -> str:
    if not canonical_terms or max_terms <= 0:
        return ""
    selected = canonical_terms[-max_terms:]
    return (
        "PREFERRED OBJECT/LOCATION TERMS from previous chunks (must keep naming stable for same object): "
        + " | ".join(selected)
    )


def _request_labels_with_optional_segment_chunking(
    cfg: Dict[str, Any],
    segments: List[Dict[str, Any]],
    prompt: str,
    video_file: Optional[Path],
    allow_operations: bool,
    model_override: str = "",
    task_id: str = "",
) -> Dict[str, Any]:
    chunking_enabled = bool(_cfg_get(cfg, "run.segment_chunking_enabled", True))
    min_segments_for_chunking = max(2, int(_cfg_get(cfg, "run.segment_chunking_min_segments", 16)))
    min_video_sec_for_chunking = max(0.0, float(_cfg_get(cfg, "run.segment_chunking_min_video_sec", 60.0)))
    max_segments_per_chunk = max(2, int(_cfg_get(cfg, "run.segment_chunking_max_segments_per_request", 8)))
    chunking_disable_operations = bool(_cfg_get(cfg, "run.segment_chunking_disable_operations", True))
    chunking_video_pad_sec = max(0.0, float(_cfg_get(cfg, "run.segment_chunking_video_pad_sec", 1.0)))
    chunking_keep_temp_files = bool(_cfg_get(cfg, "run.segment_chunking_keep_temp_files", False))
    include_previous_labels_context = bool(
        _cfg_get(cfg, "run.segment_chunking_include_previous_labels_context", True)
    )
    max_previous_labels = max(0, int(_cfg_get(cfg, "run.segment_chunking_max_previous_labels", 12)))
    consistency_memory_enabled = bool(
        _cfg_get(cfg, "run.segment_chunking_consistency_memory_enabled", True)
    )
    consistency_memory_limit = max(8, int(_cfg_get(cfg, "run.segment_chunking_consistency_memory_limit", 40)))
    consistency_prompt_terms = max(0, int(_cfg_get(cfg, "run.segment_chunking_consistency_prompt_terms", 16)))
    consistency_normalize_labels = bool(
        _cfg_get(cfg, "run.segment_chunking_consistency_normalize_labels", True)
    )
    retry_with_quota_fallback_model = bool(
        _cfg_get(cfg, "gemini.retry_with_quota_fallback_model", True)
    )
    quota_fallback_model = str(_cfg_get(cfg, "gemini.quota_fallback_model", "gemini-3-pro-preview") or "").strip()
    quota_fallback_from_models_raw = _cfg_get(
        cfg, "gemini.quota_fallback_from_models", ["gemini-3.1-pro-preview"]
    )
    quota_fallback_from_models: set[str] = set()
    if isinstance(quota_fallback_from_models_raw, list):
        for item in quota_fallback_from_models_raw:
            value = str(item or "").strip().lower()
            if value:
                quota_fallback_from_models.add(value)
    else:
        raw_text = str(quota_fallback_from_models_raw or "").strip()
        if raw_text:
            for part in re.split(r"[,\|;]+", raw_text):
                value = str(part or "").strip().lower()
                if value:
                    quota_fallback_from_models.add(value)
    active_model_override = str(model_override or "").strip()

    def _call_labels(prompt_text: str, media_file: Optional[Path], seg_count: int) -> Dict[str, Any]:
        nonlocal active_model_override
        request_model = str(
            active_model_override or _cfg_get(cfg, "gemini.model", "gemini-3.1-pro-preview")
        ).strip()
        try:
            return call_gemini_labels(
                cfg,
                prompt_text,
                video_file=media_file,
                segment_count=seg_count,
                model_override=request_model,
            )
        except Exception as exc:
            if not _is_gemini_quota_error(exc):
                raise
            fallback_model = quota_fallback_model
            if not retry_with_quota_fallback_model:
                raise
            if not fallback_model or fallback_model.lower() == request_model.lower():
                raise
            if quota_fallback_from_models and request_model.lower() not in quota_fallback_from_models:
                raise
            print(
                "[gemini] quota model fallback engaged: "
                f"{request_model} -> {fallback_model}"
            )
            active_model_override = fallback_model
            return call_gemini_labels(
                cfg,
                prompt_text,
                video_file=media_file,
                segment_count=seg_count,
                model_override=active_model_override,
            )

    can_chunk_by_shape = bool(
        chunking_enabled
        and video_file is not None
        and video_file.exists()
        and len(segments) >= min_segments_for_chunking
        and max_segments_per_chunk < len(segments)
    )
    short_video_for_chunking = False
    video_duration_sec_for_chunking = 0.0
    if can_chunk_by_shape and min_video_sec_for_chunking > 0 and video_file is not None and video_file.exists():
        video_duration_sec_for_chunking = _probe_video_duration_seconds(video_file)
        short_video_for_chunking = (
            video_duration_sec_for_chunking > 0.2 and video_duration_sec_for_chunking < min_video_sec_for_chunking
        )
        if short_video_for_chunking:
            print(
                "[gemini] segment chunking skipped: short video "
                f"({video_duration_sec_for_chunking:.1f}s < {min_video_sec_for_chunking:.1f}s); "
                "using single-request flow."
            )

    should_chunk = bool(can_chunk_by_shape and not short_video_for_chunking)
    if not should_chunk:
        return _call_labels(prompt, video_file, len(segments))

    ffmpeg_bin = _resolve_ffmpeg_binary()
    if not ffmpeg_bin:
        print("[gemini] segment chunking skipped: ffmpeg not found; using single-request flow.")
        return _call_labels(prompt, video_file, len(segments))

    chunks = _segment_chunks(segments, max_segments_per_chunk)
    if len(chunks) <= 1:
        return _call_labels(prompt, video_file, len(segments))

    extra_base = str(_cfg_get(cfg, "gemini.extra_instructions", "") or "").strip()
    out_dir = Path(str(_cfg_get(cfg, "run.output_dir", "outputs")))
    out_dir.mkdir(parents=True, exist_ok=True)
    temp_chunk_files: List[Path] = []
    collected_labels: Dict[int, str] = {}
    prior_labels: List[str] = []
    consistency_terms: List[str] = []
    consistency_alias_to_canonical: Dict[str, str] = {}
    meta_key_sources: List[str] = []
    meta_models: List[str] = []
    chunk_count = len(chunks)
    print(
        f"[gemini] segment chunking enabled: total_segments={len(segments)} "
        f"chunks={chunk_count} max_per_chunk={max_segments_per_chunk}"
    )
    try:
        for chunk_idx, chunk_segments in enumerate(chunks, start=1):
            window_start = max(
                0.0,
                min(_safe_float(seg.get("start_sec"), 0.0) for seg in chunk_segments) - chunking_video_pad_sec,
            )
            window_end = max(_safe_float(seg.get("end_sec"), 0.0) for seg in chunk_segments) + chunking_video_pad_sec
            if window_end <= window_start:
                window_end = window_start + 1.0

            chunk_video_path = out_dir / f"video_{task_id or 'chunked'}_segchunk_{chunk_idx:02d}.mp4"
            clipped = _extract_video_window(
                src_video=video_file,
                out_video=chunk_video_path,
                start_sec=window_start,
                end_sec=window_end,
                ffmpeg_bin=ffmpeg_bin,
            )
            effective_video = chunk_video_path if clipped else video_file
            if clipped:
                temp_chunk_files.append(chunk_video_path)
            elif chunk_video_path.exists():
                try:
                    chunk_video_path.unlink(missing_ok=True)
                except Exception:
                    pass

            chunk_extra_parts: List[str] = []
            if extra_base:
                chunk_extra_parts.append(extra_base)
            chunk_extra_parts.append(
                f"This clip covers approximately {window_start:.1f}s to {window_end:.1f}s of the full episode timeline."
            )
            chunk_extra_parts.append(
                "Label only the listed segment_index rows in this chunk; do not invent extra rows."
            )
            if include_previous_labels_context and max_previous_labels > 0 and prior_labels:
                context_labels = prior_labels[-max_previous_labels:]
                chunk_extra_parts.append(
                    "Consistency context from previous chunks (keep object naming stable): "
                    + " | ".join(context_labels)
                )
            if consistency_memory_enabled and consistency_prompt_terms > 0 and consistency_terms:
                chunk_hint = _build_chunk_consistency_prompt_hint(
                    consistency_terms, max_terms=consistency_prompt_terms
                )
                if chunk_hint:
                    chunk_extra_parts.append(chunk_hint)

            chunk_allow_operations = allow_operations and (not chunking_disable_operations)
            chunk_prompt = build_prompt(
                chunk_segments,
                "\n".join(chunk_extra_parts),
                allow_operations=chunk_allow_operations,
            )
            print(
                f"[gemini] chunk request {chunk_idx}/{chunk_count}: "
                f"segments={len(chunk_segments)} window={window_start:.1f}-{window_end:.1f}s "
                f"video={effective_video.name if effective_video is not None else 'none'}"
            )
            chunk_payload = _call_labels(chunk_prompt, effective_video, len(chunk_segments))
            chunk_plan = _normalize_segment_plan(chunk_payload, chunk_segments, cfg=cfg)
            for seg in chunk_segments:
                idx = int(seg.get("segment_index", 0))
                item = chunk_plan.get(idx, {})
                label = str(item.get("label", "")).strip()
                if not label:
                    label = str(seg.get("current_label", "")).strip()
                if label:
                    if consistency_memory_enabled:
                        label = _update_chunk_consistency_memory(
                            label,
                            canonical_terms=consistency_terms,
                            alias_to_canonical=consistency_alias_to_canonical,
                            memory_limit=consistency_memory_limit,
                        )
                    collected_labels[idx] = label
                    prior_labels.append(label)
                    if len(prior_labels) > 128:
                        prior_labels = prior_labels[-128:]

            meta = chunk_payload.get("_meta", {}) if isinstance(chunk_payload, dict) else {}
            key_source = str(meta.get("api_key_source", "")).strip()
            if key_source:
                meta_key_sources.append(key_source)
            model_name = str(meta.get("model", "")).strip()
            if model_name:
                meta_models.append(model_name)

        combined_segments: List[Dict[str, Any]] = []
        for seg in segments:
            idx = int(seg.get("segment_index", 0))
            label = collected_labels.get(idx, str(seg.get("current_label", "")).strip())
            if consistency_memory_enabled and consistency_normalize_labels:
                label = _update_chunk_consistency_memory(
                    label,
                    canonical_terms=consistency_terms,
                    alias_to_canonical=consistency_alias_to_canonical,
                    memory_limit=consistency_memory_limit,
                )
            if bool(_cfg_get(cfg, "run.tier3_label_rewrite", True)):
                label = _rewrite_label_tier3(label)
            label = _normalize_label_min_safety(label)
            combined_segments.append(
                {
                    "segment_index": idx,
                    "start_sec": round(_safe_float(seg.get("start_sec"), 0.0), 3),
                    "end_sec": round(_safe_float(seg.get("end_sec"), 0.0), 3),
                    "label": label,
                }
            )

        key_source_meta = "primary"
        if "secondary" in meta_key_sources:
            key_source_meta = "secondary"
        model_meta = meta_models[-1] if meta_models else str(
            active_model_override or _cfg_get(cfg, "gemini.model", "gemini-3.1-pro-preview")
        )
        result: Dict[str, Any] = {
            "operations": [],
            "segments": combined_segments,
            "_meta": {
                "video_attached": True,
                "mode": "with-video",
                "fallback_used": False,
                "video_transport": "chunked-window",
                "video_parts_count": 1,
                "chunked": True,
                "chunk_count": chunk_count,
                "consistency_memory_terms": len(consistency_terms),
                "api_key_source": key_source_meta,
                "model": model_meta,
            },
        }
        print(
            f"[gemini] chunked labels merged: {len(combined_segments)} segments from {chunk_count} chunks; "
            f"consistency_terms={len(consistency_terms)}"
        )
        return result
    finally:
        if not chunking_keep_temp_files:
            for p in temp_chunk_files:
                try:
                    p.unlink(missing_ok=True)
                except Exception:
                    continue


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _short_error_text(exc: Exception, max_len: int = 180) -> str:
    raw = str(exc or "").strip()
    if not raw:
        return exc.__class__.__name__
    first = raw.splitlines()[0].strip()
    if len(first) > max_len:
        return first[:max_len] + "..."
    return first


def _log_gemini_usage(
    cfg: Dict[str, Any],
    model: str,
    mode: str,
    usage_meta: Dict[str, Any],
    key_source: str = "primary",
) -> None:
    if not isinstance(usage_meta, dict):
        return
    try:
        prompt_tokens = int(usage_meta.get("promptTokenCount", 0) or 0)
    except Exception:
        prompt_tokens = 0
    try:
        output_tokens = int(usage_meta.get("candidatesTokenCount", 0) or 0)
    except Exception:
        output_tokens = 0
    try:
        total_tokens = int(usage_meta.get("totalTokenCount", 0) or 0)
    except Exception:
        total_tokens = prompt_tokens + output_tokens

    if prompt_tokens <= 0 and output_tokens <= 0 and total_tokens <= 0:
        return

    in_price = float(_cfg_get(cfg, "gemini.price_input_per_million", 0.30))
    out_price = float(_cfg_get(cfg, "gemini.price_output_per_million", 2.50))
    est_cost = (prompt_tokens / 1_000_000.0) * in_price + (output_tokens / 1_000_000.0) * out_price
    print(
        "[gemini] usage: "
        f"prompt={prompt_tokens} output={output_tokens} total={total_tokens} "
        f"est_cost=${est_cost:.6f}"
    )

    out_dir = Path(str(_cfg_get(cfg, "run.output_dir", "outputs")))
    out_dir.mkdir(parents=True, exist_ok=True)
    usage_log_name = str(_cfg_get(cfg, "gemini.usage_log_file", "gemini_usage.jsonl")).strip() or "gemini_usage.jsonl"
    usage_log_path = out_dir / usage_log_name
    line = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "model": model,
        "mode": mode,
        "key_source": key_source,
        "prompt_tokens": prompt_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "estimated_cost_usd": round(est_cost, 8),
    }
    try:
        with usage_log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(line, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _label_main_verb(label: str) -> str:
    text = re.sub(r"\s+", " ", (label or "").strip()).lower()
    if not text:
        return ""
    m = re.match(r"([a-z]+)", text)
    return m.group(1) if m else ""


def _is_no_action_label(label: str) -> bool:
    normalized = re.sub(r"[\s_-]+", " ", (label or "").strip()).lower()
    return normalized in {"no action", "noaction"}


_LABEL_TOKEN_RE = re.compile(r"[a-z]+")
_LABEL_OVERLAP_STOPWORDS: set[str] = {
    "no",
    "action",
    "with",
    "on",
    "in",
    "into",
    "onto",
    "at",
    "to",
    "from",
    "under",
    "over",
    "inside",
}


def _label_content_tokens(label: str) -> set[str]:
    text = re.sub(r"\s+", " ", (label or "").strip()).lower()
    if not text:
        return set()
    tokens = set(_LABEL_TOKEN_RE.findall(text))
    return {tok for tok in tokens if tok and tok not in _LABEL_OVERLAP_STOPWORDS}


_AUTOFIX_ALLOWED_LABEL_START_VERB_TOKEN_PATTERNS: Tuple[Tuple[str, ...], ...] = (
    ("pick", "up"),
    ("place",),
    ("move",),
    ("adjust",),
    ("align",),
    ("hold",),
    ("cut",),
    ("open",),
    ("close",),
    ("peel",),
    ("secure",),
    ("wipe",),
    ("flip",),
    ("pull",),
    ("push",),
    ("insert",),
    ("remove",),
    ("attach",),
    ("detach",),
    ("connect",),
    ("disconnect",),
    ("tighten",),
    ("loosen",),
    ("screw",),
    ("unscrew",),
    ("press",),
    ("twist",),
    ("turn",),
    ("slide",),
    ("lift",),
    ("lower",),
    ("set",),
    ("position",),
    ("straighten",),
    ("comb",),
    ("detangle",),
    ("sand",),
    ("paint",),
    ("clean",),
)

_AUTOFIX_OBJECT_EXPECTING_VERBS: set[str] = {
    "pick up",
    "place",
    "move",
    "adjust",
    "align",
    "hold",
    "cut",
    "open",
    "close",
    "peel",
    "secure",
    "wipe",
    "flip",
    "pull",
    "push",
    "insert",
    "remove",
    "attach",
    "detach",
    "connect",
    "disconnect",
    "tighten",
    "loosen",
    "screw",
    "unscrew",
}

_AUTOFIX_VERB_HINT_MAP: Tuple[Tuple[str, str], ...] = (
    ("wire", "connect"),
    ("cable", "connect"),
    ("plug", "connect"),
    ("socket", "connect"),
    ("cloth", "wipe"),
    ("towel", "wipe"),
    ("rag", "wipe"),
    ("screw", "tighten"),
    ("bolt", "tighten"),
    ("nut", "tighten"),
    ("lid", "close"),
    ("door", "close"),
    ("cap", "close"),
    ("switch", "press"),
    ("button", "press"),
    ("paper", "place"),
    ("box", "place"),
)


def _allowed_label_start_verb_token_patterns_from_cfg(cfg: Dict[str, Any]) -> List[Tuple[str, ...]]:
    raw = _cfg_get(cfg, "run.allowed_label_start_verbs", [])
    patterns: List[Tuple[str, ...]] = []
    if isinstance(raw, list):
        for item in raw:
            tokens = tuple(re.findall(r"[a-z]+", str(item).lower()))
            if tokens:
                patterns.append(tokens)
    if not patterns:
        patterns = list(_AUTOFIX_ALLOWED_LABEL_START_VERB_TOKEN_PATTERNS)
    deduped: List[Tuple[str, ...]] = []
    seen: set[Tuple[str, ...]] = set()
    for pattern in patterns:
        if pattern in seen:
            continue
        seen.add(pattern)
        deduped.append(pattern)
    return deduped


def _label_starts_with_allowed_action_verb(
    action_phrase: str,
    allowed_verb_token_patterns: List[Tuple[str, ...]],
) -> bool:
    phrase = re.sub(r"\s+", " ", (action_phrase or "").strip()).lower()
    if not phrase or phrase == "no action":
        return False
    words = re.findall(r"[a-z]+", phrase)
    if not words:
        return False
    for pattern in allowed_verb_token_patterns:
        if not pattern:
            continue
        n = len(pattern)
        if len(words) >= n and tuple(words[:n]) == pattern:
            if any(word.endswith("ing") for word in words[:n]):
                return False
            return True
    return False


def _contains_forbidden_verb_in_label(label: str, forbidden_verbs: List[str]) -> bool:
    text = (label or "").strip().lower()
    if not text:
        return False
    for verb in forbidden_verbs:
        if re.search(rf"\b{re.escape(verb)}\b", text):
            return True
    return False


def _strip_forbidden_verbs_for_autofix(label: str, forbidden_verbs: List[str]) -> str:
    out = label or ""
    for verb in forbidden_verbs:
        if not verb:
            continue
        out = re.sub(rf"\b{re.escape(verb)}\b", "", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+", " ", out).strip(" ,.;:")
    return out


def _action_phrase_missing_object_for_autofix(action_phrase: str) -> bool:
    phrase = re.sub(r"\s+", " ", (action_phrase or "").strip()).lower()
    if not phrase:
        return True
    for verb in sorted(_AUTOFIX_OBJECT_EXPECTING_VERBS, key=len, reverse=True):
        if phrase == verb:
            return True
        if phrase.startswith(verb + " "):
            remaining = phrase[len(verb):].strip()
            if not remaining:
                return True
            if len(re.findall(r"[a-z]+", remaining)) == 0:
                return True
            return False
    return False


def _heuristic_autofix_verb_from_text(text: str) -> str:
    lowered = re.sub(r"\s+", " ", (text or "").strip()).lower()
    if lowered:
        for needle, verb in _AUTOFIX_VERB_HINT_MAP:
            if needle in lowered:
                return verb
    return "pick up"


def _autofix_label_candidate(
    cfg: Dict[str, Any],
    label: str,
    source_label: str,
    forbidden_verbs: List[str],
    allowed_verb_token_patterns: List[Tuple[str, ...]],
) -> str:
    min_words = max(1, int(_cfg_get(cfg, "run.min_label_words", 2)))
    max_words = max(min_words, int(_cfg_get(cfg, "run.max_label_words", 20)))

    def _normalize(x: str) -> str:
        out = _normalize_label_min_safety(x)
        out = _strip_forbidden_verbs_for_autofix(out, forbidden_verbs)
        out = re.sub(r"\b(?:then|another|continue|next)\b", "", out, flags=re.IGNORECASE)
        out = re.sub(r"\s+", " ", out).strip(" ,.;:")
        return out

    def _valid_candidate(x: str) -> bool:
        if not x or x.lower() == "no action":
            return False
        if not _label_starts_with_allowed_action_verb(x, allowed_verb_token_patterns):
            return False
        if _contains_forbidden_verb_in_label(x, forbidden_verbs):
            return False
        first_clause = x.split(",")[0].split(" and ")[0].strip()
        if _action_phrase_missing_object_for_autofix(first_clause):
            return False
        return True

    for base in (label, source_label):
        candidate = _normalize(base)
        if _valid_candidate(candidate):
            words = [w for w in candidate.split() if w]
            if len(words) < min_words:
                candidate = f"{candidate} item".strip()
            words = [w for w in candidate.split() if w]
            if len(words) > max_words:
                if "," in candidate:
                    candidate = candidate.split(",", 1)[0].strip()
                else:
                    candidate = " ".join(words[:max_words])
            return candidate

    base_text = _normalize(label or source_label or "")
    base_tokens = re.findall(r"[a-z]+", base_text.lower())
    object_tokens = list(base_tokens)
    for pattern in allowed_verb_token_patterns:
        n = len(pattern)
        if n > 0 and len(base_tokens) >= n and tuple(base_tokens[:n]) == pattern:
            object_tokens = base_tokens[n:]
            break

    object_tokens = [t for t in object_tokens if t not in {"and", "then"}]
    object_phrase = " ".join(object_tokens).strip() or "item"
    verb = _heuristic_autofix_verb_from_text(base_text)
    candidate = _normalize(f"{verb} {object_phrase}")

    if not _label_starts_with_allowed_action_verb(candidate, allowed_verb_token_patterns):
        candidate = _normalize(f"pick up {object_phrase}")
    if not candidate:
        candidate = "pick up item"

    words = [w for w in candidate.split() if w]
    if len(words) < min_words:
        candidate = f"{candidate} item".strip()
    words = [w for w in candidate.split() if w]
    if len(words) > max_words:
        candidate = " ".join(words[:max_words])
    return candidate


_MICRO_ACTION_VERBS: set[str] = {"dip", "reload", "wet"}


def _label_action_clauses(label: str) -> List[str]:
    text = re.sub(r"\s+", " ", (label or "").strip())
    if not text:
        return []
    parts: List[str] = []
    for chunk in text.split(","):
        subs = [s.strip() for s in re.split(r"\band\b", chunk, flags=re.IGNORECASE) if s.strip()]
        parts.extend(subs)
    return parts


def _label_goal_key(label: str) -> str:
    """
    Build a coarse goal key for continuity merge detection.
    Prefer the last non-micro verb; fallback to last verb.
    """
    if _is_no_action_label(label):
        return ""
    clauses = _label_action_clauses(label)
    if not clauses:
        return ""
    verbs: List[str] = []
    for clause in clauses:
        v = _label_main_verb(clause)
        if v:
            verbs.append(v)
    if not verbs:
        return ""
    non_micro = [v for v in verbs if v not in _MICRO_ACTION_VERBS]
    return (non_micro[-1] if non_micro else verbs[-1]).strip().lower()


def _build_auto_continuity_merge_operations(
    segment_plan: Dict[int, Dict[str, Any]],
    cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    if not bool(_cfg_get(cfg, "run.auto_continuity_merge_enabled", True)):
        return []
    if not bool(_cfg_get(cfg, "run.structural_allow_merge", True)):
        return []

    min_run = max(3, int(_cfg_get(cfg, "run.auto_continuity_merge_min_run_segments", 3)))
    min_overlap = max(0, int(_cfg_get(cfg, "run.auto_continuity_merge_min_token_overlap", 1)))

    ordered = sorted(int(k) for k in segment_plan.keys())
    if len(ordered) < min_run:
        return []

    def same_goal(i1: int, i2: int) -> bool:
        a = segment_plan.get(i1, {})
        b = segment_plan.get(i2, {})
        la = str(a.get("label", "")).strip()
        lb = str(b.get("label", "")).strip()
        ka = _label_goal_key(la)
        kb = _label_goal_key(lb)
        if not ka or not kb or ka != kb:
            return False
        overlap = len(_label_content_tokens(la).intersection(_label_content_tokens(lb)))
        return overlap >= min_overlap

    runs: List[Tuple[int, int]] = []
    run_start = ordered[0]
    run_end = ordered[0]
    for idx in ordered[1:]:
        if idx == run_end + 1 and same_goal(run_end, idx):
            run_end = idx
            continue
        if (run_end - run_start + 1) >= min_run:
            runs.append((run_start, run_end))
        run_start = idx
        run_end = idx
    if (run_end - run_start + 1) >= min_run:
        runs.append((run_start, run_end))

    if not runs:
        return []

    merge_indices: List[int] = []
    for start_idx, end_idx in runs:
        # Descending indices to keep operation row references stable.
        for idx in range(end_idx, start_idx, -1):
            merge_indices.append(idx)

    merge_indices = sorted(set(merge_indices), reverse=True)
    return [{"action": "merge", "segment_index": int(idx)} for idx in merge_indices]


def _rewrite_no_action_pauses_in_plan(segment_plan: Dict[int, Dict[str, Any]], cfg: Dict[str, Any]) -> int:
    if not bool(_cfg_get(cfg, "run.no_action_pause_rewrite_enabled", True)):
        return 0
    max_pause_sec = max(0.0, float(_cfg_get(cfg, "run.no_action_pause_rewrite_max_sec", 12.0)))
    min_overlap = max(1, int(_cfg_get(cfg, "run.no_action_pause_rewrite_min_overlap_tokens", 1)))
    prefer_next_adjust = bool(_cfg_get(cfg, "run.no_action_pause_rewrite_prefer_next_adjust", True))

    ordered_indices = sorted(segment_plan.keys())
    rewrites = 0
    for pos, idx in enumerate(ordered_indices):
        item = segment_plan.get(idx, {})
        label = str(item.get("label", "")).strip()
        if not _is_no_action_label(label):
            continue
        start_sec = _safe_float(item.get("start_sec", 0.0), 0.0)
        end_sec = _safe_float(item.get("end_sec", start_sec), start_sec)
        if (end_sec - start_sec) > max_pause_sec:
            continue
        if pos == 0 or pos >= len(ordered_indices) - 1:
            continue

        prev_item = segment_plan.get(ordered_indices[pos - 1], {})
        next_item = segment_plan.get(ordered_indices[pos + 1], {})
        prev_label = str(prev_item.get("label", "")).strip()
        next_label = str(next_item.get("label", "")).strip()
        if not prev_label or not next_label:
            continue
        if _is_no_action_label(prev_label) or _is_no_action_label(next_label):
            continue

        overlap = len(_label_content_tokens(prev_label).intersection(_label_content_tokens(next_label)))
        if overlap < min_overlap:
            continue

        replacement = prev_label
        if prefer_next_adjust and _label_main_verb(next_label) == "adjust":
            replacement = next_label
        elif _label_main_verb(prev_label) == _label_main_verb(next_label):
            replacement = prev_label

        if replacement and replacement != label:
            item["label"] = replacement
            segment_plan[idx] = item
            rewrites += 1
    return rewrites


_ING_TO_BASE_VERB_MAP: Dict[str, str] = {
    "positioning": "position",
    "scraping": "scrape",
    "lifting": "lift",
    "turning": "turn",
    "setting": "set",
    "placing": "place",
    "moving": "move",
    "polishing": "polish",
    "sanding": "sand",
    "leveling": "level",
    "dislodging": "dislodge",
    "adjusting": "adjust",
    "opening": "open",
    "closing": "close",
    "cutting": "cut",
    "pulling": "pull",
    "pushing": "push",
    "holding": "hold",
    "inserting": "insert",
    "removing": "remove",
    "twisting": "twist",
    "pouring": "pour",
    "scooping": "scoop",
    "filling": "fill",
    "compacting": "compact",
}


def _normalize_ing_verbs_to_imperative(text: str) -> str:
    out = text or ""
    if not out:
        return out
    for src, dst in _ING_TO_BASE_VERB_MAP.items():
        out = re.sub(rf"\b{re.escape(src)}\b", dst, out, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", out).strip()


_NUM_WORDS_0_TO_19 = [
    "zero",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
    "eleven",
    "twelve",
    "thirteen",
    "fourteen",
    "fifteen",
    "sixteen",
    "seventeen",
    "eighteen",
    "nineteen",
]
_NUM_TENS_WORDS = [
    "",
    "",
    "twenty",
    "thirty",
    "forty",
    "fifty",
    "sixty",
    "seventy",
    "eighty",
    "ninety",
]


def _int_to_words(n: int) -> str:
    if n < 0:
        return "minus " + _int_to_words(-n)
    if n < 20:
        return _NUM_WORDS_0_TO_19[n]
    if n < 100:
        tens, rem = divmod(n, 10)
        return _NUM_TENS_WORDS[tens] if rem == 0 else f"{_NUM_TENS_WORDS[tens]}-{_NUM_WORDS_0_TO_19[rem]}"
    if n < 1000:
        hundreds, rem = divmod(n, 100)
        return (
            f"{_NUM_WORDS_0_TO_19[hundreds]} hundred"
            if rem == 0
            else f"{_NUM_WORDS_0_TO_19[hundreds]} hundred {_int_to_words(rem)}"
        )
    if n < 10000:
        thousands, rem = divmod(n, 1000)
        return (
            f"{_NUM_WORDS_0_TO_19[thousands]} thousand"
            if rem == 0
            else f"{_NUM_WORDS_0_TO_19[thousands]} thousand {_int_to_words(rem)}"
        )
    return str(n)


def _replace_numerals_with_words(text: str) -> str:
    def repl(match: re.Match[str]) -> str:
        token = match.group(0)
        try:
            value = int(token)
        except (TypeError, ValueError):
            return token
        return _int_to_words(value)

    out = re.sub(r"\b\d+\b", repl, text or "")
    return re.sub(r"\s+", " ", out).strip()


def _expand_verb_object_attachment_patterns(text: str) -> str:
    """
    Normalize common chained-verb shorthand into explicit object-attached actions.
    Example: "pick up box and place under desk" -> "pick up box, place box under desk"
    """
    out = text or ""

    def _clean(token: str) -> str:
        return re.sub(r"\s+", " ", (token or "").strip(" ,"))

    def _repl(match: re.Match[str]) -> str:
        obj = _clean(match.group(1))
        prep = _clean(match.group(2)).lower()
        dest = _clean(match.group(3))
        if not obj or not prep or not dest:
            return match.group(0)
        return f"pick up {obj}, place {obj} {prep} {dest}"

    # Case A: object omitted after first verb.
    out = re.sub(
        r"\bpick up\s+and\s+place\s+([^,]+?)\s+(on|in|into|onto|at|to|inside|under|over)\s+([^,]+)",
        _repl,
        out,
        flags=re.IGNORECASE,
    )
    # Case B: object omitted after second verb.
    out = re.sub(
        r"\bpick up\s+([^,]+?)\s+and\s+place\s+(on|in|into|onto|at|to|inside|under|over)\s+([^,]+)",
        _repl,
        out,
        flags=re.IGNORECASE,
    )

    return re.sub(r"\s+", " ", out).strip(" ,")


def _normalize_mechanical_motion_to_goal(text: str) -> str:
    """
    Replace mechanical-motion wording with coarse goal verbs.
    Example: "move handsaw back and forth on wooden board" -> "cut wooden board with handsaw"
    """
    out = text or ""

    def _norm_obj(value: str) -> str:
        obj = re.sub(r"\s+", " ", (value or "").strip(" ,.;:"))
        obj = re.sub(r"^(?:on|onto|across|to|into|in)\s+", "", obj, flags=re.IGNORECASE)
        obj = re.sub(r"^(?:finish|fully)\s+cut(?:ting)?\s+", "", obj, flags=re.IGNORECASE)
        obj = re.sub(r"^cut(?:ting)?\s+", "", obj, flags=re.IGNORECASE)
        return re.sub(r"\s+", " ", obj).strip(" ,.;:")

    def _saw_repl(match: re.Match[str]) -> str:
        tool_raw = str(match.group("tool") or "").strip().lower()
        obj = _norm_obj(str(match.group("obj") or ""))
        if not obj:
            return match.group(0)
        tool = "handsaw" if "hand" in tool_raw else "saw"
        return f"cut {obj} with {tool}"

    # move saw/handsaw back and forth [on/across/to cut] <object>
    out = re.sub(
        r"\bmove\s+(?P<tool>hand\s*saw|handsaw|saw)\s+back\s+and\s+forth\s+"
        r"(?:(?:to\s+)?(?:(?:finish|fully)\s+)?cut(?:ting)?\s+)?"
        r"(?:(?:on|onto|across|to|into|in)\s+)?(?P<obj>[^,]+)",
        _saw_repl,
        out,
        flags=re.IGNORECASE,
    )

    def _sand_repl(match: re.Match[str]) -> str:
        obj = _norm_obj(str(match.group("obj") or ""))
        if not obj:
            return match.group(0)
        return f"sand {obj} with sandpaper"

    # move/rub sandpaper [back and forth] on <object>
    out = re.sub(
        r"\b(?:move|rub)\s+sandpaper(?:\s+back\s+and\s+forth)?\s+"
        r"(?:(?:on|onto|across|to|into|in)\s+)?(?P<obj>[^,]+)",
        _sand_repl,
        out,
        flags=re.IGNORECASE,
    )

    def _norm_hair_obj(value: str) -> str:
        obj = _norm_obj(value)
        obj = re.sub(r"\bsection\s+hair\b", "wig", obj, flags=re.IGNORECASE)
        obj = re.sub(r"\bwig\s+hair\b", "wig", obj, flags=re.IGNORECASE)
        obj = re.sub(r"\bhair\b", "wig", obj, flags=re.IGNORECASE)
        obj = re.sub(r"\s+", " ", obj).strip(" ,.;:")
        return obj or "wig"

    def _comb_section_repl(match: re.Match[str]) -> str:
        obj = _norm_hair_obj(str(match.group("obj") or "wig"))
        return f"section {obj} with comb"

    # move comb/tail through wig to section hair -> section wig with comb
    out = re.sub(
        r"\bmove\s+comb(?:\s+tail)?\s+through\s+(?P<obj>[^,]+?)\s+to\s+section(?:\s+hair)?\b",
        _comb_section_repl,
        out,
        flags=re.IGNORECASE,
    )

    def _comb_detangle_repl(match: re.Match[str]) -> str:
        obj = _norm_hair_obj(str(match.group("obj") or "wig"))
        return f"detangle {obj} with comb"

    out = re.sub(
        r"\bmove\s+comb\s+through\s+(?P<obj>[^,]+?)\s+to\s+detangle\b",
        _comb_detangle_repl,
        out,
        flags=re.IGNORECASE,
    )

    def _comb_style_repl(match: re.Match[str]) -> str:
        obj = _norm_hair_obj(str(match.group("obj") or "wig"))
        return f"comb {obj}"

    out = re.sub(
        r"\bmove\s+comb\s+through\s+(?P<obj>[^,]+?)\s+to\s+style\b",
        _comb_style_repl,
        out,
        flags=re.IGNORECASE,
    )

    def _comb_generic_repl(match: re.Match[str]) -> str:
        obj = _norm_hair_obj(str(match.group("obj") or "wig"))
        return f"comb {obj}"

    # move comb through wig -> comb wig
    out = re.sub(
        r"\bmove\s+comb\s+through\s+(?P<obj>[^,]+)\b",
        _comb_generic_repl,
        out,
        flags=re.IGNORECASE,
    )

    def _straightener_repl(match: re.Match[str]) -> str:
        obj = _norm_hair_obj(str(match.group("obj") or "wig"))
        return f"straighten {obj} with hair straightener"

    # move hair straightener to press wig section -> straighten wig with hair straightener
    out = re.sub(
        r"\bmove\s+hair\s+straightener\s+(?:to\s+)?(?:press|straighten)\s+(?P<obj>[^,]+)\b",
        _straightener_repl,
        out,
        flags=re.IGNORECASE,
    )

    return re.sub(r"\s+", " ", out).strip(" ,")


def _collapse_adjacent_duplicate_tokens(text: str) -> str:
    out = re.sub(r"\s+", " ", (text or "").strip())
    if not out:
        return out
    repeated_phrase = re.compile(r"\b([a-z]+(?:\s+[a-z]+){1,2})\s+\1\b", re.IGNORECASE)
    repeated_word = re.compile(r"\b([a-z]+)\s+\1\b", re.IGNORECASE)
    for _ in range(6):
        prev = out
        out = repeated_phrase.sub(r"\1", out)
        out = repeated_word.sub(r"\1", out)
        out = re.sub(r"\s+", " ", out).strip(" ,")
        if out == prev:
            break
    return out


def _rewrite_label_tier3(label: str) -> str:
    text = re.sub(r"\s+", " ", (label or "").strip())
    if not text:
        return text
    if text.lower() == "no action":
        return "No Action"

    # Keep labels imperative, remove narrative fillers, and normalize forbidden verbs.
    text = re.sub(r"\bthen\b", ",", text, flags=re.IGNORECASE)
    text = re.sub(r"\bnext\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bcontinue\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bagain\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\banother\b\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\brotate(?:d|s|ing)?\b", "adjust", text, flags=re.IGNORECASE)
    text = re.sub(r"\bturn(?:ed|s|ing)?\b", "adjust", text, flags=re.IGNORECASE)
    text = re.sub(r"\brelocate(?:d|s|ing)?\b", "move", text, flags=re.IGNORECASE)
    text = re.sub(r"\bgrab(?:bed|s|bing)?\b", "pick up", text, flags=re.IGNORECASE)
    text = _normalize_ing_verbs_to_imperative(text)
    text = _normalize_mechanical_motion_to_goal(text)
    text = _collapse_adjacent_duplicate_tokens(text)
    text = _normalize_gripper_terms(text)
    text = _replace_numerals_with_words(text)
    text = _expand_verb_object_attachment_patterns(text)
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r"\s+", " ", text).strip(" ,")

    clauses = [c.strip() for c in text.split(",") if c.strip()]
    if not clauses:
        return text

    # Remove exact duplicate clauses.
    deduped: List[str] = []
    seen: set[str] = set()
    for c in clauses:
        key = re.sub(r"\s+", " ", c).strip().lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(c)
    clauses = deduped

    return ", ".join(clauses).strip()


def _normalize_label_min_safety(label: str) -> str:
    text = re.sub(r"\s+", " ", (label or "").strip())
    if not text:
        return text
    if text.lower() == "no action":
        return "No Action"
    # Always enforce this minimal safety rewrite before policy gate.
    text = re.sub(r"\brotate(?:d|s|ing)?\b", "adjust", text, flags=re.IGNORECASE)
    text = re.sub(r"\bturn(?:ed|s|ing)?\b", "adjust", text, flags=re.IGNORECASE)
    text = re.sub(r"\brelocate(?:d|s|ing)?\b", "move", text, flags=re.IGNORECASE)
    text = _normalize_ing_verbs_to_imperative(text)
    text = _normalize_mechanical_motion_to_goal(text)
    text = _collapse_adjacent_duplicate_tokens(text)
    text = _normalize_gripper_terms(text)
    text = _replace_numerals_with_words(text)
    text = _expand_verb_object_attachment_patterns(text)
    text = re.sub(r"\s+", " ", text).strip(" ,")
    return text


def _normalize_segment_plan(
    payload: Dict[str, Any],
    source_segments: List[Dict[str, Any]],
    cfg: Optional[Dict[str, Any]] = None,
) -> Dict[int, Dict[str, Any]]:
    items = payload.get("segments")
    if not isinstance(items, list):
        raise ValueError("Gemini payload must contain list at 'segments'")

    effective_cfg = cfg or {}
    forbidden_verbs_raw = _cfg_get(effective_cfg, "run.forbidden_label_verbs", [])
    forbidden_verbs = [str(v).strip().lower() for v in forbidden_verbs_raw if str(v).strip()]
    allowed_verb_token_patterns = _allowed_label_start_verb_token_patterns_from_cfg(effective_cfg)

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
        source_label = str(source.get("current_label", "")).strip()
        label = str(item.get("label", "")).strip() or source_label
        if bool(_cfg_get(effective_cfg, "run.tier3_label_rewrite", True)):
            label = _rewrite_label_tier3(label)
        label = _autofix_label_candidate(
            effective_cfg,
            label,
            source_label,
            forbidden_verbs,
            allowed_verb_token_patterns,
        )
        label = _normalize_label_min_safety(label)
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
        source_label = str(source.get("current_label", "")).strip()
        if bool(_cfg_get(effective_cfg, "run.tier3_label_rewrite", True)):
            source_label = _rewrite_label_tier3(source_label)
        source_label = _autofix_label_candidate(
            effective_cfg,
            source_label,
            source_label,
            forbidden_verbs,
            allowed_verb_token_patterns,
        )
        source_label = _normalize_label_min_safety(source_label)
        out[idx] = {
            "segment_index": idx,
            "label": source_label,
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
    mode = str(_cfg_get(cfg, "run.timestamp_adjust_mode", "best_effort")).strip().lower() or "best_effort"
    if mode in {"off", "none", "disabled"}:
        return {"adjusted": 0, "failed": []}
    skip_if_segments_ge = max(0, int(_cfg_get(cfg, "run.timestamp_skip_if_segments_ge", 24)))
    if skip_if_segments_ge > 0 and len(segment_plan) >= skip_if_segments_ge and mode != "strict":
        print(
            f"[run] timestamp adjustments skipped: segment count {len(segment_plan)} >= "
            f"{skip_if_segments_ge} (mode={mode})."
        )
        return {"adjusted": 0, "failed": []}

    _dismiss_blocking_modals(page)
    _dismiss_blocking_side_panel(page, cfg, aggressive=True)

    rows_sel = str(_cfg_get(cfg, "atlas.selectors.segment_rows", ""))
    plus_sel = str(_cfg_get(cfg, "atlas.selectors.segment_time_plus_button", 'button:has(svg.lucide-plus)'))
    minus_sel = str(_cfg_get(cfg, "atlas.selectors.segment_time_minus_button", 'button:has(svg.lucide-minus)'))
    step_sec = max(0.01, float(_cfg_get(cfg, "atlas.timestamp_step_sec", 0.1)))
    max_clicks = max(1, int(_cfg_get(cfg, "atlas.timestamp_max_clicks_per_segment", 30)))
    click_timeout_ms = max(120, int(_cfg_get(cfg, "run.timestamp_click_timeout_ms", 350)))
    click_pause_ms = max(0, int(_cfg_get(cfg, "run.timestamp_click_pause_ms", 15)))
    max_failures = max(1, int(_cfg_get(cfg, "run.timestamp_max_failures_per_episode", 10)))
    max_total_clicks = max(1, int(_cfg_get(cfg, "run.timestamp_max_total_clicks", 80)))
    abort_on_first_failure = bool(_cfg_get(cfg, "run.timestamp_abort_on_first_failure", False))
    skip_disabled_buttons = bool(_cfg_get(cfg, "run.timestamp_skip_disabled_buttons", True))

    try:
        best_rows_sel, rows = _resolve_rows_locator(page, rows_sel)
    except Exception:
        return {"adjusted": 0, "failed": ["rows locator unavailable for timestamp adjustment"]}

    def _short_err(exc: Exception, max_len: int = 180) -> str:
        raw = str(exc or "").strip()
        if not raw:
            return exc.__class__.__name__
        first = raw.splitlines()[0].strip()
        if len(first) > max_len:
            return first[:max_len] + "..."
        return first

    source_by_idx: Dict[int, Dict[str, Any]] = {int(seg["segment_index"]): seg for seg in source_segments}
    adjusted = 0
    failed: List[str] = []
    total_clicks_done = 0

    for idx in sorted(segment_plan):
        if total_clicks_done >= max_total_clicks:
            print(
                f"[run] timestamp adjustment budget reached: "
                f"{total_clicks_done}/{max_total_clicks} clicks."
            )
            break
        if len(failed) >= max_failures:
            print(
                f"[run] timestamp adjustments stopped early after {len(failed)} failures "
                f"(limit={max_failures})."
            )
            break
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
        clicks = min(clicks, max_total_clicks - total_clicks_done)
        if clicks <= 0:
            break
        use_plus = diff > 0
        btn_sel = plus_sel if use_plus else minus_sel
        btn = _first_visible_child_locator(row, btn_sel)
        if btn is None:
            failed.append(f"segment {idx}: timestamp {'plus' if use_plus else 'minus'} button not found")
            if abort_on_first_failure:
                break
            continue
        if skip_disabled_buttons:
            try:
                if not btn.is_enabled():
                    failed.append(
                        f"segment {idx}: timestamp {'plus' if use_plus else 'minus'} button disabled"
                    )
                    if abort_on_first_failure:
                        break
                    continue
            except Exception:
                pass
        try:
            _click_segment_row_with_recovery(page, rows, idx, cfg)
            clicked_this_segment = 0
            for _ in range(clicks):
                live_row = page.locator(best_rows_sel).nth(idx - 1)
                live_btn = _first_visible_child_locator(live_row, btn_sel)
                if live_btn is None:
                    raise RuntimeError(
                        f"timestamp {'plus' if use_plus else 'minus'} button disappeared during adjustment"
                    )
                if skip_disabled_buttons:
                    try:
                        if not live_btn.is_enabled():
                            raise RuntimeError(
                                f"timestamp {'plus' if use_plus else 'minus'} button disabled during adjustment"
                            )
                    except RuntimeError:
                        raise
                    except Exception:
                        pass
                try:
                    live_btn.click(timeout=click_timeout_ms, no_wait_after=True)
                except Exception as click_exc:
                    _dismiss_blocking_side_panel(page, cfg, aggressive=True)
                    try:
                        live_btn.click(timeout=click_timeout_ms, force=True, no_wait_after=True)
                    except Exception as force_exc:
                        if mode == "strict":
                            raise force_exc
                        raise RuntimeError(_short_err(click_exc)) from force_exc
                clicked_this_segment += 1
                total_clicks_done += 1
                if click_pause_ms > 0:
                    time.sleep(click_pause_ms / 1000.0)
            if clicked_this_segment > 0:
                adjusted += 1
        except Exception as exc:
            failed.append(f"segment {idx}: {_short_err(exc)}")
            if abort_on_first_failure:
                break

    return {"adjusted": adjusted, "failed": failed}


def _action_selector_for_row(cfg: Dict[str, Any], action: str) -> str:
    if action == "edit":
        return str(_cfg_get(cfg, "atlas.selectors.edit_button_in_row", "")).strip()
    if action == "split":
        return str(_cfg_get(cfg, "atlas.selectors.split_button_in_row", "")).strip()
    if action == "delete":
        return str(_cfg_get(cfg, "atlas.selectors.delete_button_in_row", "")).strip()
    if action == "merge":
        return str(_cfg_get(cfg, "atlas.selectors.merge_button_in_row", "")).strip()
    return ""


def _action_hotkey(action: str) -> str:
    if action == "edit":
        return "e"
    if action == "split":
        return "s"
    if action == "delete":
        return "d"
    if action == "merge":
        return "m"
    return ""


def _confirm_action_dialog(page: Page, cfg: Dict[str, Any]) -> bool:
    confirm_sel = str(_cfg_get(cfg, "atlas.selectors.action_confirm_button", "")).strip()
    if not confirm_sel:
        return False
    clicked = _safe_locator_click(page, confirm_sel, timeout_ms=1200)
    if clicked:
        page.wait_for_timeout(250)
    return clicked


def _wait_rows_delta(page: Page, rows_selector: str, before_count: int, expected_delta: int, timeout_ms: int = 4000) -> bool:
    if expected_delta == 0:
        return True
    target = max(0, before_count + expected_delta)
    deadline = time.time() + (timeout_ms / 1000.0)
    while time.time() < deadline:
        try:
            current = page.locator(rows_selector).count()
            if current == target:
                return True
        except Exception:
            pass
        time.sleep(0.12)
    return False


def apply_segment_operations(page: Page, cfg: Dict[str, Any], operations: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not operations:
        return {"applied": 0, "structural_applied": 0, "failed": []}
    rows_sel = str(_cfg_get(cfg, "atlas.selectors.segment_rows", ""))
    sample_size = max(1, int(_cfg_get(cfg, "run.segment_resolve_sample_size", 8)))
    row_text_timeout_ms = max(100, int(_cfg_get(cfg, "run.segment_resolve_row_text_timeout_ms", 350)))
    structural_skip_if_segments_ge = max(0, int(_cfg_get(cfg, "run.structural_skip_if_segments_ge", 40)))
    structural_max_failures = max(1, int(_cfg_get(cfg, "run.structural_max_failures_per_episode", 4)))
    structural_wait_rows_delta_timeout_ms = max(
        600, int(_cfg_get(cfg, "run.structural_wait_rows_delta_timeout_ms", 1800))
    )
    failed: List[str] = []
    applied = 0
    structural_applied = 0

    if structural_skip_if_segments_ge > 0:
        try:
            _, probe_rows = _resolve_rows_locator(
                page,
                rows_sel,
                sample_size=sample_size,
                row_text_timeout_ms=row_text_timeout_ms,
            )
            seg_count = probe_rows.count()
            if seg_count >= structural_skip_if_segments_ge:
                print(
                    f"[run] structural operations skipped: segment count {seg_count} >= "
                    f"{structural_skip_if_segments_ge}."
                )
                return {"applied": 0, "structural_applied": 0, "failed": []}
        except Exception:
            pass

    for i, op in enumerate(operations, start=1):
        if len(failed) >= structural_max_failures:
            print(
                f"[run] structural operations stopped after {len(failed)} failures "
                f"(limit={structural_max_failures})."
            )
            break
        action = str(op.get("action", "")).strip().lower()
        idx = int(op.get("segment_index", 0) or 0)
        if action not in {"edit", "split", "delete", "merge"} or idx <= 0:
            failed.append(f"op#{i}: invalid operation payload {op}")
            continue

        _dismiss_blocking_modals(page, cfg)
        _dismiss_blocking_side_panel(page, cfg, aggressive=True)
        try:
            best_rows_sel, rows = _resolve_rows_locator(
                page,
                rows_sel,
                sample_size=sample_size,
                row_text_timeout_ms=row_text_timeout_ms,
            )
        except Exception as exc:
            failed.append(f"op#{i} {action} segment {idx}: rows unavailable ({exc})")
            continue

        count = rows.count()
        if idx > count:
            failed.append(f"op#{i} {action} segment {idx}: row missing (count={count})")
            continue

        row = rows.nth(idx - 1)
        try:
            _click_segment_row_with_recovery(page, rows, idx, cfg)
        except Exception as exc:
            failed.append(f"op#{i} {action} segment {idx}: cannot focus row ({exc})")
            continue

        before_count = count
        triggered = False
        btn_sel = _action_selector_for_row(cfg, action)
        if btn_sel:
            live_row = page.locator(best_rows_sel).nth(idx - 1)
            for candidate in _selector_variants(btn_sel):
                try:
                    btn = live_row.locator(candidate).first
                    if btn.count() > 0 and btn.is_visible():
                        btn.click(timeout=1200, no_wait_after=True)
                        triggered = True
                        break
                except Exception:
                    continue

        if not triggered:
            key = _action_hotkey(action)
            if key:
                try:
                    page.keyboard.press(key)
                    triggered = True
                except Exception:
                    triggered = False

        if not triggered:
            failed.append(f"op#{i} {action} segment {idx}: action trigger failed")
            continue

        if action in {"delete", "merge"}:
            _confirm_action_dialog(page, cfg)
            _dismiss_blocking_modals(page, cfg)

        expected_delta = 0
        if action == "split":
            expected_delta = 1
        elif action in {"delete", "merge"}:
            expected_delta = -1
        if not _wait_rows_delta(
            page,
            best_rows_sel,
            before_count,
            expected_delta,
            timeout_ms=structural_wait_rows_delta_timeout_ms,
        ):
            # Some actions may succeed without visible count change due to UI constraints.
            try:
                after_count = page.locator(best_rows_sel).count()
            except Exception:
                after_count = before_count
            if expected_delta != 0 and after_count == before_count:
                failed.append(
                    f"op#{i} {action} segment {idx}: no row-count change "
                    f"(expected {before_count + expected_delta}, got {after_count})"
                )
                continue

        applied += 1
        if action in {"split", "delete", "merge"}:
            structural_applied += 1
        print(f"[atlas] operation applied: {action} on segment {idx}")
        page.wait_for_timeout(220)

    return {"applied": applied, "structural_applied": structural_applied, "failed": failed}


def _fill_input(locator: Locator, label: str, page: Page) -> None:
    # Avoid long waits on stale textarea locators; keep label apply loop fast.
    try:
        locator.scroll_into_view_if_needed(timeout=700)
    except Exception:
        pass
    try:
        locator.click(timeout=900, force=True)
    except Exception:
        try:
            locator.click(timeout=700)
        except Exception:
            pass
    try:
        editable = bool(locator.evaluate("el => !!el.isContentEditable"))
    except Exception:
        editable = False

    if editable:
        page.keyboard.press("Control+A")
        page.keyboard.type(label, delay=8)
        return
    try:
        locator.fill(label, timeout=1600)
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
                        cb.click(timeout=1200, force=True, no_wait_after=True)
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

    def _find_submit_button(modal_loc: Locator) -> Optional[Locator]:
        for candidate in _selector_variants(submit_sel):
            try:
                loc = modal_loc.locator(candidate)
                scan = min(loc.count(), 4)
                for i in range(scan):
                    btn = loc.nth(i)
                    if btn.is_visible():
                        return btn
            except Exception:
                continue
        return None

    def _try_click_submit(modal_loc: Locator) -> bool:
        submit_btn = _find_submit_button(modal_loc)
        if submit_btn is None:
            return False
        try:
            disabled = bool(
                submit_btn.evaluate(
                    "el => !!el.disabled || String(el.getAttribute('aria-disabled') || '').toLowerCase() === 'true'"
                )
            )
        except Exception:
            disabled = False
        if disabled:
            return False
        try:
            submit_btn.click(timeout=1500, force=True, no_wait_after=True)
            return True
        except Exception:
            return False

    submitted = False
    for _ in range(5):
        submit_btn = _find_submit_button(modal)
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
            submit_btn.click(timeout=1500, force=True, no_wait_after=True)
            submitted = True
            print("[atlas] quality review submitted.")
            page.wait_for_timeout(1300)
            break
        except Exception:
            page.wait_for_timeout(300)
            continue

    if not submitted:
        return False

    # Atlas UI can keep the modal visible for a few seconds even after successful submit.
    # Keep polling and re-click submit (if still enabled) before deciding failure.
    for _ in range(18):
        current_modal = _first_visible_locator(page, modal_sel, timeout_ms=350)
        if current_modal is None:
            return True
        if _try_click_submit(current_modal):
            page.wait_for_timeout(450)
        else:
            page.wait_for_timeout(450)

    current_modal = _first_visible_locator(page, modal_sel, timeout_ms=350)
    if current_modal is None:
        return True
    submit_btn = _find_submit_button(current_modal)
    if submit_btn is None:
        # Modal still visible but submit button disappeared; likely accepted.
        return True
    try:
        disabled = bool(
            submit_btn.evaluate(
                "el => !!el.disabled || String(el.getAttribute('aria-disabled') || '').toLowerCase() === 'true'"
            )
        )
    except Exception:
        disabled = False
    if disabled:
        # Atlas sometimes keeps stale modal frame visible after successful submit.
        print("[atlas] quality review submit appears accepted (button disabled; modal still visible).")
        return True
    return False


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
                    complete_loc.click(timeout=1200, force=True, no_wait_after=True)
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
    progress_every = max(1, int(_cfg_get(cfg, "run.label_apply_progress_every", 5)))
    max_total_sec = max(30, int(_cfg_get(cfg, "run.label_apply_max_total_sec", 600)))
    max_failures = max(1, int(_cfg_get(cfg, "run.label_apply_max_failures", 18)))
    input_timeout_ms = max(800, int(_cfg_get(cfg, "run.label_apply_input_timeout_ms", 3000)))
    save_timeout_ms = max(300, int(_cfg_get(cfg, "run.label_apply_save_timeout_ms", 1800)))
    edit_click_timeout_ms = max(400, int(_cfg_get(cfg, "run.label_apply_edit_click_timeout_ms", 900)))
    submit_guard_enabled = bool(_cfg_get(cfg, "run.submit_guard_enabled", True))
    submit_guard_max_failure_ratio = min(
        1.0, max(0.0, float(_cfg_get(cfg, "run.submit_guard_max_failure_ratio", 0.25)))
    )
    submit_guard_min_applied_ratio = min(
        1.0, max(0.0, float(_cfg_get(cfg, "run.submit_guard_min_applied_ratio", 0.9)))
    )
    submit_guard_block_on_budget_exceeded = bool(
        _cfg_get(cfg, "run.submit_guard_block_on_budget_exceeded", True)
    )

    best_rows_sel, rows = _resolve_rows_locator(page, rows_sel)
    failed: List[str] = []
    applied = 0
    skipped_unchanged = 0
    total_targets = len(label_map)
    started_at = time.time()
    processed = 0
    if total_targets > 0:
        print(f"[run] apply labels started: targets={total_targets}")

    for idx in sorted(label_map):
        processed += 1
        elapsed = time.time() - started_at
        if elapsed > float(max_total_sec):
            failed.append(
                f"apply budget exceeded after {elapsed:.1f}s "
                f"(processed={processed - 1}/{total_targets})"
            )
            print(
                f"[run] apply labels stopped: exceeded {max_total_sec}s budget "
                f"after {processed - 1}/{total_targets} segments."
            )
            break
        if len(failed) >= max_failures:
            print(
                f"[run] apply labels stopped: failure limit {max_failures} reached "
                f"(processed={processed - 1}/{total_targets})."
            )
            break
        rows = page.locator(best_rows_sel)
        count = rows.count()
        if idx > count:
            failed.append(f"segment {idx}: row missing (count={count})")
            if processed % progress_every == 0:
                print(
                    f"[run] apply progress {processed}/{total_targets} "
                    f"(applied={applied}, skipped={skipped_unchanged}, failed={len(failed)})"
                )
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

            # Fastest path: row focus then keyboard edit hotkey.
            input_loc = None
            try:
                page.keyboard.press("e")
                input_loc = _first_visible_locator(page, input_sel, timeout_ms=min(1200, input_timeout_ms))
            except Exception:
                input_loc = None

            # Fallback: open editor by row double-click.
            if input_loc is None:
                try:
                    row.dblclick(timeout=max(500, edit_click_timeout_ms - 300))
                except Exception:
                    pass
                input_loc = _first_visible_locator(page, input_sel, timeout_ms=min(1200, input_timeout_ms))

            # Last fallback: explicit Edit button selectors.
            if input_loc is None:
                for candidate in _selector_variants(edit_sel):
                    edit_loc = row.locator(candidate).first
                    if edit_loc.count() > 0 and edit_loc.is_visible():
                        try:
                            edit_loc.click(timeout=edit_click_timeout_ms, no_wait_after=True)
                        except Exception:
                            _dismiss_blocking_side_panel(page, cfg, aggressive=True)
                            edit_loc.click(
                                timeout=max(400, edit_click_timeout_ms - 300),
                                force=True,
                                no_wait_after=True,
                            )
                        input_loc = _first_visible_locator(page, input_sel, timeout_ms=min(1800, input_timeout_ms))
                        if input_loc is not None:
                            break

            if input_loc is None:
                input_loc = _first_visible_locator(page, input_sel, timeout_ms=input_timeout_ms)
            if input_loc is None:
                raise RuntimeError("label input not found")
            _fill_input(input_loc, label, page)

            saved = _safe_locator_click(page, save_sel, timeout_ms=save_timeout_ms) if save_sel else False
            if not saved:
                for candidate in _selector_variants(save_sel):
                    btn = _first_visible_locator(page, candidate, timeout_ms=max(300, save_timeout_ms // 2))
                    if btn is None:
                        continue
                    try:
                        btn.click(timeout=max(300, save_timeout_ms // 2), force=True, no_wait_after=True)
                        saved = True
                        break
                    except Exception:
                        continue
            if not saved:
                page.keyboard.press("Control+Enter")

            applied += 1
            time.sleep(0.15)
        except Exception as exc:
            failed.append(f"segment {idx}: {_short_error_text(exc)}")
        if processed % progress_every == 0:
            print(
                f"[run] apply progress {processed}/{total_targets} "
                f"(applied={applied}, skipped={skipped_unchanged}, failed={len(failed)})"
            )

    submit_guard_reasons: List[str] = []
    budget_exceeded = any("apply budget exceeded" in str(msg).lower() for msg in failed)
    total_targets_safe = max(1, total_targets)
    failure_ratio = float(len(failed)) / float(total_targets_safe)
    applied_ratio = float(applied) / float(total_targets_safe)
    if submit_guard_enabled and total_targets > 0:
        if submit_guard_block_on_budget_exceeded and budget_exceeded:
            submit_guard_reasons.append("apply budget exceeded")
        if failure_ratio > submit_guard_max_failure_ratio:
            submit_guard_reasons.append(
                f"failure ratio {failure_ratio:.1%} > {submit_guard_max_failure_ratio:.1%}"
            )
        if applied_ratio < submit_guard_min_applied_ratio:
            submit_guard_reasons.append(
                f"applied ratio {applied_ratio:.1%} < {submit_guard_min_applied_ratio:.1%}"
            )

    if submit_guard_reasons:
        print("[run] submit guard blocked auto-submit for this episode:")
        for reason in submit_guard_reasons:
            print(f"  - {reason}")
        completed = False
    else:
        completed = _submit_episode(page, cfg) if complete_sel else False

    return {
        "applied": applied,
        "skipped_unchanged": skipped_unchanged,
        "failed": failed,
        "completed": completed,
        "submit_guard_blocked": bool(submit_guard_reasons),
        "submit_guard_reasons": submit_guard_reasons,
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


def _capture_debug_artifacts(page: Page, cfg: Dict[str, Any], prefix: str = "debug_failure") -> Tuple[Optional[Path], Optional[Path]]:
    out_dir = Path(str(_cfg_get(cfg, "run.output_dir", "outputs")))
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    snap_path = out_dir / f"{prefix}_{ts}.png"
    html_path = out_dir / f"{prefix}_{ts}.html"

    snap_saved: Optional[Path] = None
    html_saved: Optional[Path] = None
    try:
        page.screenshot(path=str(snap_path), full_page=True)
        snap_saved = snap_path
        print(f"[debug] screenshot saved: {snap_path}")
    except Exception:
        pass
    try:
        html_path.write_text(page.content(), encoding="utf-8")
        html_saved = html_path
        print(f"[debug] html saved: {html_path}")
    except Exception:
        pass
    return snap_saved, html_saved


def _apply_global_gemini_video_policy(cfg: Dict[str, Any]) -> None:
    """
    Enforce a low-cost + quality-preserving video policy for all accounts.
    This runs after YAML merge so old/new account files get consistent behavior.
    """
    gem = cfg.setdefault("gemini", {})
    if not isinstance(gem, dict):
        cfg["gemini"] = {}
        gem = cfg["gemini"]

    defaults = {
        "optimize_video_for_upload": True,
        "optimize_video_target_mb": 4.0,
        "optimize_video_target_fps": 10.0,
        "optimize_video_min_fps": 8.0,
        "optimize_video_min_width": 320,
        "optimize_video_min_short_side": 320,
        "split_upload_enabled": True,
        "split_upload_only_if_larger_mb": 8.0,
        "split_upload_chunk_max_mb": 6.0,
        "split_upload_max_chunks": 4,
        "split_upload_reencode_on_copy_fail": True,
        "split_upload_inline_total_max_mb": 12.0,
        "reference_frames_enabled": True,
        "reference_frames_always": False,
        "reference_frame_attach_when_video_mb_le": 2.5,
        "reference_frame_count": 2,
        "reference_frame_positions": [0.2, 0.55, 0.85],
        "reference_frame_max_side": 960,
        "reference_frame_jpeg_quality": 82,
        "reference_frame_max_total_kb": 420,
    }
    for key, value in defaults.items():
        gem.setdefault(key, value)

    preferred_model = "gemini-3.1-pro-preview"
    configured_model = str(gem.get("model", "") or "").strip()
    if configured_model in {"", "gemini-2.5-flash", "gemini-2.5-pro"}:
        if configured_model != preferred_model:
            gem["model"] = preferred_model
            print(f"[policy] gemini.model forced to {preferred_model}.")

    # Keep upload cost low while avoiding aggressive visual degradation.
    split_upload_enabled = bool(gem.get("split_upload_enabled", True))
    target_cap_mb = 8.0 if split_upload_enabled else 4.0
    try:
        target_mb = float(gem.get("optimize_video_target_mb", 4.0))
    except Exception:
        target_mb = 4.0
    gem["optimize_video_target_mb"] = min(target_cap_mb, max(1.0, target_mb))

    try:
        target_fps = float(gem.get("optimize_video_target_fps", 10.0))
    except Exception:
        target_fps = 10.0
    try:
        min_fps = float(gem.get("optimize_video_min_fps", 8.0))
    except Exception:
        min_fps = 8.0
    min_fps = max(8.0, min_fps)
    gem["optimize_video_min_fps"] = min_fps
    gem["optimize_video_target_fps"] = max(min_fps, target_fps)

    try:
        min_width = int(gem.get("optimize_video_min_width", 320))
    except Exception:
        min_width = 320
    try:
        min_short = int(gem.get("optimize_video_min_short_side", 320))
    except Exception:
        min_short = 320
    gem["optimize_video_min_width"] = max(320, min_width)
    gem["optimize_video_min_short_side"] = max(320, min_short)

    floor_surface_guard = (
        "If floor mat vs table is unclear, do not guess raised furniture; "
        "use neutral location wording."
    )
    extra = str(gem.get("extra_instructions", "") or "").strip()
    if floor_surface_guard.lower() not in extra.lower():
        gem["extra_instructions"] = (
            f"{extra}\n{floor_surface_guard}".strip() if extra else floor_surface_guard
        )


def _apply_global_run_policy(cfg: Dict[str, Any]) -> None:
    """
    Enforce safe run-level defaults that prevent known quality failures
    across older account YAML files.
    """
    run = cfg.setdefault("run", {})
    if not isinstance(run, dict):
        cfg["run"] = {}
        run = cfg["run"]

    run.setdefault("auto_continuity_merge_enabled", True)
    run.setdefault("auto_continuity_merge_min_run_segments", 3)
    run.setdefault("auto_continuity_merge_min_token_overlap", 1)
    run.setdefault("segment_chunking_min_video_sec", 60.0)
    run["skip_reserve_when_all_visible_blocked"] = False
    run["clear_blocked_tasks_after_all_visible_blocked_hits"] = 1
    run["clear_blocked_tasks_every_retry"] = True
    run["reserve_cooldown_sec"] = 0
    run["reserve_min_interval_sec"] = 0
    run["reserve_wait_only_on_rate_limit"] = True
    run["reserve_attempts_per_visit"] = max(3, int(run.get("reserve_attempts_per_visit", 3) or 3))
    run["reserve_rate_limit_wait_sec"] = 5
    run["release_and_reserve_on_all_visible_blocked"] = True
    run["release_and_reserve_on_submit_unverified"] = True
    run["no_task_retry_delay_sec"] = 5.0
    run["no_task_backoff_factor"] = 1.0
    run["no_task_max_delay_sec"] = 5.0
    run["keep_alive_idle_cycle_pause_sec"] = 5.0
    run["release_all_wait_sec"] = 5.0

    # Merge must stay enabled for continuity fixes to work.
    if not bool(run.get("structural_allow_merge", True)):
        run["structural_allow_merge"] = True
        print("[policy] run.structural_allow_merge forced ON for continuity safety.")


def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("Config root must be YAML object")
    cfg = _deep_merge(DEFAULT_CONFIG, raw)
    _apply_global_gemini_video_policy(cfg)
    _apply_global_run_policy(cfg)
    return cfg


def run(cfg: Dict[str, Any], execute: bool) -> None:
    global _GEMINI_FALLBACK_USES
    _GEMINI_FALLBACK_USES = 0
    state_path = Path(str(_cfg_get(cfg, "browser.storage_state_path", ".state/atlas_auth.json")))
    force_login = bool(_cfg_get(cfg, "browser.force_login", False))
    headless = bool(_cfg_get(cfg, "browser.headless", False))
    slow_mo = int(_cfg_get(cfg, "browser.slow_mo_ms", 0))
    use_chrome_profile = bool(_cfg_get(cfg, "browser.use_chrome_profile", False))
    restore_state_in_profile_mode = bool(_cfg_get(cfg, "browser.restore_state_in_profile_mode", False))
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
    browser_executable_path_raw = (
        str(_cfg_get(cfg, "browser.executable_path", "")).strip()
        or os.environ.get("BROWSER_EXECUTABLE_PATH", "").strip()
    )
    browser_executable_path = ""
    if browser_executable_path_raw:
        browser_executable_path = shutil.which(browser_executable_path_raw) or browser_executable_path_raw
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
    proxy_server_raw = (
        str(_cfg_get(cfg, "browser.proxy_server", "")).strip()
        or os.environ.get("ATLAS_PROXY_SERVER", "").strip()
    )
    proxy_username = (
        str(_cfg_get(cfg, "browser.proxy_username", "")).strip()
        or os.environ.get("ATLAS_PROXY_USERNAME", "").strip()
    )
    proxy_password = (
        str(_cfg_get(cfg, "browser.proxy_password", "")).strip()
        or os.environ.get("ATLAS_PROXY_PASSWORD", "").strip()
    )
    proxy_bypass = (
        str(_cfg_get(cfg, "browser.proxy_bypass", "")).strip()
        or os.environ.get("ATLAS_PROXY_BYPASS", "").strip()
    )
    clear_env_proxy_for_backend_requests = bool(
        _cfg_get(cfg, "browser.clear_env_proxy_for_backend_requests", True)
    )
    browser_proxy: Optional[Dict[str, str]] = None
    if proxy_server_raw:
        proxy_server = proxy_server_raw if "://" in proxy_server_raw else f"http://{proxy_server_raw}"
        browser_proxy = {"server": proxy_server}
        if proxy_username:
            browser_proxy["username"] = proxy_username
        if proxy_password:
            browser_proxy["password"] = proxy_password
        if proxy_bypass:
            browser_proxy["bypass"] = proxy_bypass
        print(f"[browser] proxy enabled: {proxy_server_raw} (auth={'yes' if proxy_username else 'no'})")
    if clear_env_proxy_for_backend_requests:
        cleared_proxy_env: List[str] = []
        for env_name in (
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ):
            if os.environ.pop(env_name, None):
                cleared_proxy_env.append(env_name)
        if cleared_proxy_env:
            print(
                "[net] cleared env proxy vars for backend requests: "
                + ", ".join(cleared_proxy_env)
            )
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
            if browser_executable_path:
                print(f"[browser] forcing executable path: {browser_executable_path}")
            last_profile_exc: Exception | None = None
            context = None
            for attempt in range(profile_launch_retry_count + 1):
                try:
                    launch_kwargs: Dict[str, Any] = {
                        "user_data_dir": launch_user_data_dir,
                        "headless": headless,
                        "slow_mo": slow_mo,
                        "proxy": browser_proxy,
                        "args": launch_args,
                        "timeout": profile_launch_timeout_ms,
                    }
                    if browser_executable_path:
                        launch_kwargs["executable_path"] = browser_executable_path
                    else:
                        launch_kwargs["channel"] = chrome_channel
                    context = pw.chromium.launch_persistent_context(
                        **launch_kwargs
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
            if not force_login and restore_state_in_profile_mode:
                _restore_storage_state(context, page, state_path)
            print(f"[browser] initial page url: {page.url}")
        else:
            launch_browser_kwargs: Dict[str, Any] = {
                "headless": headless,
                "slow_mo": slow_mo,
                "proxy": browser_proxy,
            }
            if browser_executable_path:
                print(f"[browser] forcing executable path: {browser_executable_path}")
                launch_browser_kwargs["executable_path"] = browser_executable_path
            browser = pw.chromium.launch(**launch_browser_kwargs)
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
                    _goto_with_retry(page, room_url, wait_until="commit", timeout_ms=45000, cfg=cfg, reason="initial-room")
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
                        refresh_kwargs: Dict[str, Any] = {
                            "user_data_dir": refreshed_user_data_dir,
                            "headless": headless,
                            "slow_mo": slow_mo,
                            "proxy": browser_proxy,
                            "args": profile_launch_args,
                            "timeout": profile_launch_timeout_ms,
                        }
                        if browser_executable_path:
                            refresh_kwargs["executable_path"] = browser_executable_path
                        else:
                            refresh_kwargs["channel"] = chrome_channel
                        context = pw.chromium.launch_persistent_context(
                            **refresh_kwargs
                        )
                        if context.pages:
                            page = context.pages[-1]
                        else:
                            page = context.new_page()
                        if not force_login and restore_state_in_profile_mode:
                            _restore_storage_state(context, page, state_path)
                        _goto_with_retry(page, room_url, wait_until="commit", timeout_ms=45000, cfg=cfg, reason="refreshed-clone-room")
                        print(f"[run] page after refreshed-clone room goto: {page.url}")
                except Exception as exc:
                    if _is_too_many_redirects_error(exc):
                        print("[run] room redirect loop detected; clearing Atlas session and retrying room once.")
                        _clear_atlas_site_session(page)
                        try:
                            _goto_with_retry(page, room_url, wait_until="commit", timeout_ms=45000, cfg=cfg, reason="initial-room-after-clear")
                            print(f"[run] page after room retry: {page.url}")
                        except Exception as retry_exc:
                            print(f"[run] room retry after clear failed: {retry_exc}. Continuing with login flow.")
                    else:
                        print(f"[run] room goto failed: {exc}. Continuing with login flow.")

            if "/dashboard" not in page.url.lower() and "/tasks" not in page.url.lower():
                ensure_logged_in(page, cfg)
                if _is_authenticated_page(page):
                    _ensure_parent(state_path)
                    context.storage_state(path=str(state_path))
                    print(f"[auth] saved state: {state_path}")

            max_episodes_per_run = int(_cfg_get(cfg, "run.max_episodes_per_run", 5))
            recycle_after_max_episodes = bool(_cfg_get(cfg, "run.recycle_after_max_episodes", True))
            release_all_after_batch = bool(_cfg_get(cfg, "run.release_all_after_batch", True))
            release_all_wait_sec = max(0.0, float(_cfg_get(cfg, "run.release_all_wait_sec", 5)))
            no_task_retry_count = max(0, int(_cfg_get(cfg, "run.no_task_retry_count", 5)))
            no_task_retry_delay_sec = max(0.0, float(_cfg_get(cfg, "run.no_task_retry_delay_sec", 5.0)))
            no_task_backoff_factor = max(1.0, float(_cfg_get(cfg, "run.no_task_backoff_factor", 1.0)))
            no_task_max_delay_sec = max(
                no_task_retry_delay_sec,
                float(_cfg_get(cfg, "run.no_task_max_delay_sec", max(5.0, no_task_retry_delay_sec))),
            )
            clear_blocked_tasks_every_retry = bool(
                _cfg_get(cfg, "run.clear_blocked_tasks_every_retry", True)
            )
            release_and_reserve_on_all_visible_blocked = bool(
                _cfg_get(cfg, "run.release_and_reserve_on_all_visible_blocked", True)
            )
            release_and_reserve_on_submit_unverified = bool(
                _cfg_get(cfg, "run.release_and_reserve_on_submit_unverified", True)
            )
            keep_alive_when_idle = bool(_cfg_get(cfg, "run.keep_alive_when_idle", True))
            keep_alive_idle_cycle_pause_sec = max(
                0.0, float(_cfg_get(cfg, "run.keep_alive_idle_cycle_pause_sec", 5.0))
            )
            clear_blocked_after_hits = max(
                1, int(_cfg_get(cfg, "run.clear_blocked_tasks_after_all_visible_blocked_hits", 1))
            )
            resume_from_artifacts = bool(_cfg_get(cfg, "run.resume_from_artifacts", True))
            resume_skip_video_steps_when_cached = bool(_cfg_get(cfg, "run.resume_skip_video_steps_when_cached", True))
            resume_skip_apply_steps_when_done = bool(_cfg_get(cfg, "run.resume_skip_apply_steps_when_done", True))
            allow_resume_auto_submit = bool(_cfg_get(cfg, "run.allow_resume_auto_submit", False))
            execute_force_fresh_gemini = bool(_cfg_get(cfg, "run.execute_force_fresh_gemini", True))
            execute_force_live_segments = bool(_cfg_get(cfg, "run.execute_force_live_segments", True))
            execute_require_video_context = bool(_cfg_get(cfg, "run.execute_require_video_context", True))
            skip_duplicate_task_in_run = bool(_cfg_get(cfg, "run.skip_duplicate_task_in_run", True))
            duplicate_task_retry_count = max(0, int(_cfg_get(cfg, "run.duplicate_task_retry_count", 3)))
            duplicate_task_retry_wait_sec = max(0.0, float(_cfg_get(cfg, "run.duplicate_task_retry_wait_sec", 2.0)))
            continue_on_episode_error = bool(_cfg_get(cfg, "run.continue_on_episode_error", True))
            max_episode_failures_per_run = max(0, int(_cfg_get(cfg, "run.max_episode_failures_per_run", 3)))
            episode_failure_retry_delay_sec = max(0.0, float(_cfg_get(cfg, "run.episode_failure_retry_delay_sec", 4.0)))
            gemini_quota_global_pause_min_sec = max(
                1.0, float(_cfg_get(cfg, "run.gemini_quota_global_pause_min_sec", 60.0))
            )
            gemini_quota_global_pause_step_sec = max(
                1.0, float(_cfg_get(cfg, "run.gemini_quota_global_pause_step_sec", 60.0))
            )
            gemini_quota_task_block_max_wait_sec = max(
                5.0, float(_cfg_get(cfg, "run.gemini_quota_task_block_max_wait_sec", 21600.0))
            )
            max_video_prepare_failures_per_task = max(1, int(_cfg_get(cfg, "run.max_video_prepare_failures_per_task", 2)))
            max_gemini_failures_per_task = max(1, int(_cfg_get(cfg, "run.max_gemini_failures_per_task", 1)))
            episode_no = 0
            seen_task_ids: set[str] = set()
            blocked_task_ids: set[str] = set()
            quota_blocked_task_until_ts: Dict[str, float] = {}
            gemini_quota_global_pause_until_ts = 0.0
            video_prepare_failures_by_task: Dict[str, int] = {}
            gemini_failures_by_task: Dict[str, int] = {}
            duplicate_hits = 0
            no_task_hits = 0
            all_visible_blocked_hits = 0
            consecutive_episode_failures = 0

            def _episode_failure_mode() -> str:
                if not continue_on_episode_error:
                    return "raise"
                if consecutive_episode_failures > max_episode_failures_per_run:
                    return "stop"
                return "continue"

            def _cleanup_expired_quota_blocks() -> None:
                now_ts = time.time()
                expired_ids = [
                    task for task, until_ts in quota_blocked_task_until_ts.items()
                    if until_ts <= now_ts
                ]
                for task in expired_ids:
                    quota_blocked_task_until_ts.pop(task, None)
                if expired_ids:
                    print(f"[run] cleared expired quota task cooldowns: {len(expired_ids)}")

            def _active_quota_blocked_task_ids() -> set[str]:
                _cleanup_expired_quota_blocks()
                now_ts = time.time()
                return {
                    task for task, until_ts in quota_blocked_task_until_ts.items()
                    if until_ts > now_ts
                }

            def _register_quota_failure(task_id: Optional[str], exc: Exception, phase_label: str) -> float:
                nonlocal gemini_quota_global_pause_until_ts
                base_delay_sec = max(1.0, float(_cfg_get(cfg, "run.gemini_quota_retry_delay_sec", 15.0)))
                quota_wait_sec = _extract_retry_seconds_from_text(str(exc or ""), default_wait_sec=base_delay_sec)
                quota_wait_sec = min(gemini_quota_task_block_max_wait_sec, max(base_delay_sec, quota_wait_sec))
                now_ts = time.time()

                if quota_wait_sec >= gemini_quota_global_pause_min_sec:
                    pause_until_ts = now_ts + quota_wait_sec
                    if pause_until_ts > gemini_quota_global_pause_until_ts:
                        gemini_quota_global_pause_until_ts = pause_until_ts
                    print(
                        "[run] Gemini quota cooldown activated globally "
                        f"for {quota_wait_sec:.1f}s ({phase_label})."
                    )

                if task_id:
                    blocked_until_ts = now_ts + quota_wait_sec
                    prev_until_ts = quota_blocked_task_until_ts.get(task_id, 0.0)
                    if blocked_until_ts > prev_until_ts:
                        quota_blocked_task_until_ts[task_id] = blocked_until_ts
                    blocked_task_ids.add(task_id)
                    print(
                        f"[run] gemini quota limit for task {task_id}; "
                        f"task blocked for {quota_wait_sec:.1f}s before retry."
                    )
                else:
                    print(
                        "[run] gemini quota limit encountered without task id; "
                        f"using retry wait {quota_wait_sec:.1f}s."
                    )

                return quota_wait_sec

            def _release_then_reserve_cycle(reason: str) -> bool:
                nonlocal no_task_hits, all_visible_blocked_hits, duplicate_hits
                print(f"[run] {reason}; triggering release-all + reserve-new cycle.")
                released = _release_all_reserved_episodes(page, cfg)
                if not released:
                    print("[run] release-all cycle skipped (button not found).")
                    return False
                blocked_task_ids.clear()
                seen_task_ids.clear()
                video_prepare_failures_by_task.clear()
                gemini_failures_by_task.clear()
                no_task_hits = 0
                all_visible_blocked_hits = 0
                duplicate_hits = 0
                if room_url:
                    try:
                        _goto_with_retry(
                            page,
                            room_url,
                            wait_until="domcontentloaded",
                            timeout_ms=45000,
                            cfg=cfg,
                            reason="room-after-release-reserve-cycle",
                        )
                    except Exception:
                        pass
                try:
                    immediate_status: Dict[str, Any] = {}
                    opened_now = goto_task_room(
                        page,
                        cfg,
                        skip_task_ids=set(),
                        status_out=immediate_status,
                    )
                    if opened_now:
                        print("[run] release+reserve cycle opened a new episode immediately.")
                except Exception:
                    pass
                return True

            while True:
                if max_episodes_per_run > 0 and episode_no >= max_episodes_per_run:
                    print(f"[run] reached max_episodes_per_run={max_episodes_per_run}.")
                    if recycle_after_max_episodes:
                        if release_all_after_batch:
                            _release_all_reserved_episodes(page, cfg)
                        if release_all_wait_sec > 0:
                            print(f"[run] waiting {release_all_wait_sec:.0f}s before reserving a new episode batch.")
                            time.sleep(release_all_wait_sec)
                        episode_no = 0
                        seen_task_ids.clear()
                        blocked_task_ids.clear()
                        video_prepare_failures_by_task.clear()
                        gemini_failures_by_task.clear()
                        duplicate_hits = 0
                        no_task_hits = 0
                        consecutive_episode_failures = 0
                        if room_url:
                            try:
                                _goto_with_retry(
                                    page,
                                    room_url,
                                    wait_until="domcontentloaded",
                                    timeout_ms=45000,
                                    cfg=cfg,
                                    reason="room-after-release-cycle",
                                )
                            except Exception:
                                pass
                        continue
                    break

                if gemini_quota_global_pause_until_ts > time.time():
                    remaining_sec = gemini_quota_global_pause_until_ts - time.time()
                    pause_sec = min(remaining_sec, gemini_quota_global_pause_step_sec)
                    pause_sec = max(1.0, pause_sec)
                    print(
                        "[run] Gemini daily quota pause is active; "
                        f"remaining {remaining_sec:.1f}s, sleeping {pause_sec:.1f}s."
                    )
                    time.sleep(pause_sec)
                    continue

                active_quota_blocked_task_ids = _active_quota_blocked_task_ids()
                skip_task_ids_for_open: set[str] = set(blocked_task_ids)
                skip_task_ids_for_open.update(active_quota_blocked_task_ids)
                if skip_duplicate_task_in_run:
                    skip_task_ids_for_open.update(seen_task_ids)
                open_status: Dict[str, Any] = {}
                blocked_before_open = set(blocked_task_ids)
                opened = goto_task_room(
                    page,
                    cfg,
                    skip_task_ids=skip_task_ids_for_open,
                    status_out=open_status,
                )
                # Important: do not pollute blocked_task_ids with seen_task_ids.
                newly_blocked = {
                    tid for tid in skip_task_ids_for_open
                    if tid not in blocked_before_open
                    and tid not in seen_task_ids
                    and tid not in active_quota_blocked_task_ids
                }
                if newly_blocked:
                    blocked_task_ids.update(newly_blocked)
                if not opened:
                    if blocked_task_ids and clear_blocked_tasks_every_retry:
                        print(
                            "[run] clearing blocked-task list before retry "
                            f"(size={len(blocked_task_ids)})."
                        )
                        blocked_task_ids.clear()
                        all_visible_blocked_hits = 0
                    if bool(open_status.get("all_visible_blocked")):
                        all_visible_blocked_hits += 1
                        if release_and_reserve_on_all_visible_blocked and all_visible_blocked_hits >= 1:
                            if _release_then_reserve_cycle("all visible tasks are blocked"):
                                continue
                        if blocked_task_ids and all_visible_blocked_hits >= clear_blocked_after_hits:
                            print(
                                "[run] all visible tasks stayed blocked across idle checks; "
                                f"clearing blocked-task list (size={len(blocked_task_ids)})."
                            )
                            blocked_task_ids.clear()
                            all_visible_blocked_hits = 0
                    else:
                        all_visible_blocked_hits = 0
                    no_task_hits += 1
                    keep_alive_pause_sec = 0.0
                    if no_task_hits > no_task_retry_count:
                        if keep_alive_when_idle and max_episodes_per_run <= 0:
                            print(
                                "[run] no label task available right now; "
                                "retry budget exhausted but keep-alive is enabled, continuing poll loop."
                            )
                            no_task_hits = max(1, no_task_retry_count)
                            keep_alive_pause_sec = keep_alive_idle_cycle_pause_sec
                        else:
                            print("[run] no label task available right now; retry budget exhausted.")
                            break
                    backoff_exp = max(0, no_task_hits - 1)
                    retry_delay_sec = min(
                        no_task_max_delay_sec,
                        no_task_retry_delay_sec * (no_task_backoff_factor**backoff_exp),
                    )
                    if active_quota_blocked_task_ids:
                        soonest_quota_release_sec = min(
                            max(0.0, quota_blocked_task_until_ts.get(tid, 0.0) - time.time())
                            for tid in active_quota_blocked_task_ids
                        )
                        quota_idle_wait_sec = min(
                            soonest_quota_release_sec,
                            gemini_quota_global_pause_step_sec,
                        )
                        if quota_idle_wait_sec > retry_delay_sec:
                            retry_delay_sec = quota_idle_wait_sec
                            print(
                                "[run] quota-task cooldown active while idle; "
                                f"next retry in {retry_delay_sec:.1f}s."
                            )
                    if keep_alive_pause_sec > retry_delay_sec:
                        retry_delay_sec = keep_alive_pause_sec
                    print(
                        f"[run] no label task available right now; retry "
                        f"{no_task_hits}/{no_task_retry_count} in {retry_delay_sec:.1f}s."
                    )
                    if room_url:
                        try:
                            _goto_with_retry(
                                page,
                                room_url,
                                wait_until="domcontentloaded",
                                timeout_ms=45000,
                                cfg=cfg,
                                reason="room-retry-no-task",
                            )
                        except Exception:
                            pass
                    if retry_delay_sec > 0:
                        time.sleep(retry_delay_sec)
                    continue
                no_task_hits = 0
                all_visible_blocked_hits = 0
                task_id = _task_id_from_url(page.url)
                if task_id and task_id in blocked_task_ids:
                    print(f"[run] opened blocked task again ({task_id}); retrying room selection.")
                    if room_url:
                        try:
                            _goto_with_retry(
                                page,
                                room_url,
                                wait_until="domcontentloaded",
                                timeout_ms=45000,
                                cfg=cfg,
                                reason="room-after-blocked-task",
                            )
                        except Exception:
                            pass
                    if no_task_retry_delay_sec > 0:
                        time.sleep(no_task_retry_delay_sec)
                    continue
                if skip_duplicate_task_in_run and task_id and task_id in seen_task_ids:
                    duplicate_hits += 1
                    print(
                        f"[run] duplicate task reopened in same run: {task_id} "
                        f"(retry {duplicate_hits}/{duplicate_task_retry_count})."
                    )
                    if duplicate_hits > duplicate_task_retry_count:
                        blocked_task_ids.add(task_id)
                        duplicate_hits = 0
                        print(
                            "[run] duplicate task retry budget exhausted; "
                            f"blocking task for this run and continuing: {task_id}"
                        )
                        if room_url:
                            try:
                                _goto_with_retry(
                                    page,
                                    room_url,
                                    wait_until="domcontentloaded",
                                    timeout_ms=45000,
                                    cfg=cfg,
                                    reason="room-after-duplicate-exhausted",
                                )
                            except Exception:
                                pass
                        if duplicate_task_retry_wait_sec > 0:
                            time.sleep(duplicate_task_retry_wait_sec)
                        continue
                    if duplicate_task_retry_wait_sec > 0:
                        time.sleep(duplicate_task_retry_wait_sec)
                    if room_url:
                        try:
                            _goto_with_retry(
                                page,
                                room_url,
                                wait_until="domcontentloaded",
                                timeout_ms=45000,
                                cfg=cfg,
                                reason="room-after-duplicate",
                            )
                        except Exception:
                            pass
                    continue
                duplicate_hits = 0
                episode_no += 1
                print(f"[run] episode {episode_no} opened: {page.url}")
                task_state = _load_task_state(cfg, task_id) if (resume_from_artifacts and task_id) else {}
                scoped_paths = _task_scoped_artifact_paths(cfg, task_id) if task_id else {}

                _dismiss_blocking_modals(page)
                if bool(_cfg_get(cfg, "run.loop_off_on_episode_open", True)):
                    # Fast-path: toggle loop off right after opening the episode.
                    _ensure_loop_off(page, cfg)
                labels_payload: Optional[Dict[str, Any]] = None
                if task_id:
                    if execute and execute_force_fresh_gemini:
                        print("[gemini] execute mode: forcing fresh Gemini evaluation (ignoring cached labels).")
                    else:
                        labels_payload = _load_cached_labels(cfg, task_id)
                min_video_bytes = int(_cfg_get(cfg, "gemini.min_video_bytes", 500000))
                validate_video_decode = bool(_cfg_get(cfg, "gemini.validate_video_decode", True))
                cached_video_file: Optional[Path] = None
                if task_id:
                    candidate = scoped_paths.get("video")
                    if candidate is not None and candidate.exists():
                        try:
                            if candidate.stat().st_size >= min_video_bytes and _is_probably_mp4(candidate):
                                if not validate_video_decode or _is_video_decodable(candidate):
                                    cached_video_file = candidate
                                else:
                                    print(f"[video] ignoring cached video with failed decode check: {candidate}")
                        except Exception:
                            cached_video_file = None

                skip_video_steps = bool(
                    resume_from_artifacts
                    and resume_skip_video_steps_when_cached
                    and (cached_video_file is not None or labels_payload is not None)
                )
                if skip_video_steps:
                    print(
                        f"[run] resume mode: skipping video playback "
                        f"(cached_video={cached_video_file is not None}, cached_labels={labels_payload is not None})."
                    )
                else:
                    _ensure_loop_off(page, cfg)
                    _play_full_video_once(page, cfg)

                if labels_payload is not None and skip_video_steps and cached_video_file is None:
                    video_file = None
                    print("[video] skipped video preparation (cached labels available).")
                elif cached_video_file is not None:
                    video_file = cached_video_file
                    print(f"[video] using cached task video: {video_file}")
                else:
                    print("[run] preparing task video for Gemini...")
                    try:
                        video_file = _prepare_video_for_gemini(page, context, cfg, task_id=task_id)
                    except Exception as exc:
                        consecutive_episode_failures += 1
                        print(f"[run] episode {episode_no} failed during video preparation: {exc}")
                        _capture_debug_artifacts(page, cfg, prefix="debug_episode_failure")
                        if task_id:
                            current_failures = int(video_prepare_failures_by_task.get(task_id, 0)) + 1
                            video_prepare_failures_by_task[task_id] = current_failures
                            print(
                                f"[run] video prepare failure for task {task_id}: "
                                f"{current_failures}/{max_video_prepare_failures_per_task}"
                            )
                            if current_failures >= max_video_prepare_failures_per_task:
                                blocked_task_ids.add(task_id)
                                print(f"[run] task blocked for this run due to repeated video failures: {task_id}")
                        if task_id and resume_from_artifacts:
                            task_state["last_error"] = str(exc)
                            _save_task_state(cfg, task_id, task_state)
                        failure_mode = _episode_failure_mode()
                        if failure_mode == "raise":
                            raise
                        if failure_mode == "stop":
                            print(
                                "[run] episode failure budget exhausted "
                                f"({consecutive_episode_failures}>{max_episode_failures_per_run}); stopping run."
                            )
                            break
                        if room_url:
                            print("[run] returning to room page after episode failure.")
                            try:
                                _goto_with_retry(
                                    page,
                                    room_url,
                                    wait_until="domcontentloaded",
                                    timeout_ms=45000,
                                    cfg=cfg,
                                    reason="room-after-episode-failure-video",
                                )
                            except Exception:
                                pass
                        if episode_failure_retry_delay_sec > 0:
                            print(f"[run] waiting {episode_failure_retry_delay_sec:.1f}s before retrying next episode.")
                            time.sleep(episode_failure_retry_delay_sec)
                        continue

                if task_id and video_file is not None and resume_from_artifacts:
                    task_state["video_path"] = str(video_file)
                    task_state["video_ready"] = True
                    _save_task_state(cfg, task_id, task_state)

                segments = None
                if not (execute and execute_force_live_segments) and resume_from_artifacts and task_id:
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

                enable_structural_actions = bool(_cfg_get(cfg, "run.enable_structural_actions", True))
                requery_after_structural_actions = bool(_cfg_get(cfg, "run.requery_after_structural_actions", True))
                prompt = build_prompt(
                    segments,
                    str(_cfg_get(cfg, "gemini.extra_instructions", "")),
                    allow_operations=True,
                )
                if labels_payload is None:
                    print("[run] requesting labels from Gemini...")
                    try:
                        labels_payload = _request_labels_with_optional_segment_chunking(
                            cfg,
                            segments,
                            prompt,
                            video_file,
                            allow_operations=True,
                            task_id=task_id,
                        )
                    except Exception as exc:
                        quota_error = _is_gemini_quota_error(exc)
                        quota_wait_sec = 0.0
                        if quota_error:
                            consecutive_episode_failures = 0
                        else:
                            consecutive_episode_failures += 1
                        print(f"[run] episode {episode_no} failed during Gemini request: {exc}")
                        _capture_debug_artifacts(page, cfg, prefix="debug_episode_failure")
                        if task_id:
                            if quota_error:
                                quota_wait_sec = _register_quota_failure(task_id, exc, "gemini-request")
                            else:
                                current_failures = int(gemini_failures_by_task.get(task_id, 0)) + 1
                                gemini_failures_by_task[task_id] = current_failures
                                print(
                                    f"[run] gemini failure for task {task_id}: "
                                    f"{current_failures}/{max_gemini_failures_per_task}"
                                )
                                if current_failures >= max_gemini_failures_per_task:
                                    blocked_task_ids.add(task_id)
                                    print(f"[run] task blocked for this run due to repeated Gemini failures: {task_id}")
                        if task_id and resume_from_artifacts:
                            task_state["last_error"] = str(exc)
                            _save_task_state(cfg, task_id, task_state)
                        if _is_non_retriable_gemini_error(exc):
                            print("[run] non-retriable Gemini error detected; stopping run.")
                            break
                        failure_mode = _episode_failure_mode()
                        if failure_mode == "raise":
                            raise
                        if failure_mode == "stop":
                            print(
                                "[run] episode failure budget exhausted "
                                f"({consecutive_episode_failures}>{max_episode_failures_per_run}); stopping run."
                            )
                            break
                        if room_url:
                            print("[run] returning to room page after episode failure.")
                            try:
                                _goto_with_retry(
                                    page,
                                    room_url,
                                    wait_until="domcontentloaded",
                                    timeout_ms=45000,
                                    cfg=cfg,
                                    reason="room-after-episode-failure-gemini",
                                )
                            except Exception:
                                pass
                        retry_delay_sec = episode_failure_retry_delay_sec
                        if quota_error:
                            quota_retry_delay_sec = max(
                                0.0,
                                float(_cfg_get(cfg, "run.gemini_quota_retry_delay_sec", 15.0)),
                            )
                            if quota_wait_sec <= 0:
                                quota_wait_sec = _register_quota_failure(task_id, exc, "gemini-request")
                            retry_delay_sec = max(retry_delay_sec, quota_retry_delay_sec, quota_wait_sec)
                        if retry_delay_sec > 0:
                            print(f"[run] waiting {retry_delay_sec:.1f}s before retrying next episode.")
                            time.sleep(retry_delay_sec)
                        continue
                    if execute and execute_require_video_context:
                        meta = labels_payload.get("_meta", {}) if isinstance(labels_payload, dict) else {}
                        video_attached = bool(meta.get("video_attached", False))
                        mode = str(meta.get("mode", "unknown"))
                        if not video_attached:
                            raise RuntimeError(
                                "Execute blocked: Gemini response is text-only (no video context). "
                                "Video review is required before apply/complete."
                            )
                        print(f"[gemini] execute guard: video context confirmed ({mode}).")
                    if task_id:
                        _save_cached_labels(cfg, task_id, labels_payload)
                    if task_id and resume_from_artifacts:
                        task_state["labels_ready"] = True
                        _save_task_state(cfg, task_id, task_state)

                operations = _normalize_operations(labels_payload, cfg=cfg)
                if not operations and execute and enable_structural_actions:
                    try:
                        merge_plan_preview = _normalize_segment_plan(labels_payload, segments, cfg=cfg)
                        auto_merge_ops = _build_auto_continuity_merge_operations(merge_plan_preview, cfg)
                        if auto_merge_ops:
                            operations = auto_merge_ops
                            print(
                                f"[policy] auto-generated merge operations for continuity: {len(auto_merge_ops)}"
                            )
                    except Exception as auto_merge_exc:
                        print(f"[policy] auto continuity-merge skipped: {auto_merge_exc}")
                if operations:
                    ops_text = ", ".join([f"{op['action']}#{op['segment_index']}" for op in operations[:20]])
                    print(f"[gemini] suggested operations ({len(operations)}): {ops_text}")

                if execute and enable_structural_actions and operations:
                    op_result = apply_segment_operations(page, cfg, operations)
                    print(
                        f"[run] operations applied: {op_result['applied']} "
                        f"(structural={op_result['structural_applied']})"
                    )
                    if op_result["failed"]:
                        print("[run] operation failures:")
                        for item in op_result["failed"]:
                            print(f"  - {item}")

                    if op_result["structural_applied"] > 0 and requery_after_structural_actions:
                        print("[run] structural changes detected; refreshing segments and requesting Gemini again...")
                        segments = extract_segments(page, cfg)
                        print(f"[atlas] extracted {len(segments)} segments (post-operations)")
                        if task_id and resume_from_artifacts:
                            _save_cached_segments(cfg, task_id, segments)
                        prompt = build_prompt(
                            segments,
                            str(_cfg_get(cfg, "gemini.extra_instructions", "")),
                            allow_operations=False,
                        )
                        try:
                            labels_payload = _request_labels_with_optional_segment_chunking(
                                cfg,
                                segments,
                                prompt,
                                video_file,
                                allow_operations=False,
                                task_id=task_id,
                            )
                        except Exception as exc:
                            quota_error = _is_gemini_quota_error(exc)
                            quota_wait_sec = 0.0
                            if quota_error:
                                consecutive_episode_failures = 0
                            else:
                                consecutive_episode_failures += 1
                            print(f"[run] episode {episode_no} failed during Gemini re-query: {exc}")
                            _capture_debug_artifacts(page, cfg, prefix="debug_episode_failure")
                            if task_id:
                                if quota_error:
                                    quota_wait_sec = _register_quota_failure(task_id, exc, "gemini-requery")
                                else:
                                    current_failures = int(gemini_failures_by_task.get(task_id, 0)) + 1
                                    gemini_failures_by_task[task_id] = current_failures
                                    print(
                                        f"[run] gemini re-query failure for task {task_id}: "
                                        f"{current_failures}/{max_gemini_failures_per_task}"
                                    )
                                    if current_failures >= max_gemini_failures_per_task:
                                        blocked_task_ids.add(task_id)
                                        print(f"[run] task blocked for this run due to repeated Gemini failures: {task_id}")
                            if task_id and resume_from_artifacts:
                                task_state["last_error"] = str(exc)
                                _save_task_state(cfg, task_id, task_state)
                            if _is_non_retriable_gemini_error(exc):
                                print("[run] non-retriable Gemini error detected; stopping run.")
                                break
                            failure_mode = _episode_failure_mode()
                            if failure_mode == "raise":
                                raise
                            if failure_mode == "stop":
                                print(
                                    "[run] episode failure budget exhausted "
                                    f"({consecutive_episode_failures}>{max_episode_failures_per_run}); stopping run."
                                )
                                break
                            if room_url:
                                print("[run] returning to room page after episode failure.")
                                try:
                                    _goto_with_retry(
                                        page,
                                        room_url,
                                        wait_until="domcontentloaded",
                                        timeout_ms=45000,
                                        cfg=cfg,
                                        reason="room-after-episode-failure-gemini-requery",
                                    )
                                except Exception:
                                    pass
                            retry_delay_sec = episode_failure_retry_delay_sec
                            if quota_error:
                                quota_retry_delay_sec = max(
                                    0.0,
                                    float(_cfg_get(cfg, "run.gemini_quota_retry_delay_sec", 15.0)),
                                )
                                if quota_wait_sec <= 0:
                                    quota_wait_sec = _register_quota_failure(task_id, exc, "gemini-requery")
                                retry_delay_sec = max(retry_delay_sec, quota_retry_delay_sec, quota_wait_sec)
                            if retry_delay_sec > 0:
                                print(f"[run] waiting {retry_delay_sec:.1f}s before retrying next episode.")
                                time.sleep(retry_delay_sec)
                            continue
                        if execute and execute_require_video_context:
                            meta = labels_payload.get("_meta", {}) if isinstance(labels_payload, dict) else {}
                            video_attached = bool(meta.get("video_attached", False))
                            mode = str(meta.get("mode", "unknown"))
                            if not video_attached:
                                raise RuntimeError(
                                    "Execute blocked: Gemini response is text-only (no video context). "
                                    "Video review is required before apply/complete."
                                )
                            print(f"[gemini] execute guard: video context confirmed ({mode}).")
                        post_ops = _normalize_operations(labels_payload, cfg=cfg)
                        if post_ops:
                            print("[run] ignoring operations in second pass (labels-only pass).")
                        if task_id:
                            _save_cached_labels(cfg, task_id, labels_payload)
                        if task_id and resume_from_artifacts:
                            task_state["labels_ready"] = True
                            _save_task_state(cfg, task_id, task_state)
                elif operations and not execute:
                    print("[run] operations skipped (dry-run mode).")

                _save_outputs(cfg, segments, prompt, labels_payload, task_id=task_id)

                segment_plan = _normalize_segment_plan(labels_payload, segments, cfg=cfg)
                no_action_rewrites = _rewrite_no_action_pauses_in_plan(segment_plan, cfg)
                if no_action_rewrites:
                    print(f"[policy] rewrote short no-action pauses: {no_action_rewrites}")
                if task_id:
                    _save_task_text_files(cfg, task_id, segments, segment_plan)

                if bool(_cfg_get(cfg, "run.enable_policy_gate", True)):
                    validation_report = _validate_segment_plan_against_policy(cfg, segments, segment_plan)
                    report_task_id = task_id or f"episode_{episode_no}"

                    raw_errors = [str(e).strip() for e in validation_report.get("errors", []) if str(e).strip()]
                    retry_with_stronger_model = bool(
                        _cfg_get(cfg, "gemini.retry_with_stronger_model_on_policy_fail", False)
                    )
                    policy_retry_model = str(_cfg_get(cfg, "gemini.policy_retry_model", "")).strip()
                    policy_retry_only_if_flash = bool(
                        _cfg_get(cfg, "gemini.policy_retry_only_if_flash", True)
                    )
                    current_model = str(
                        (labels_payload.get("_meta", {}) or {}).get(
                            "model",
                            _cfg_get(cfg, "gemini.model", "gemini-3.1-pro-preview"),
                        )
                    ).strip()

                    can_retry_with_stronger_model = (
                        retry_with_stronger_model
                        and bool(raw_errors)
                        and bool(policy_retry_model)
                        and policy_retry_model.lower() != current_model.lower()
                    )
                    if can_retry_with_stronger_model and policy_retry_only_if_flash:
                        can_retry_with_stronger_model = "flash" in current_model.lower()

                    if can_retry_with_stronger_model:
                        print(
                            "[policy] validation failed; retrying Gemini with stronger model "
                            f"({current_model} -> {policy_retry_model})..."
                        )
                        try:
                            retry_payload = _request_labels_with_optional_segment_chunking(
                                cfg,
                                segments,
                                prompt,
                                video_file,
                                allow_operations=False,
                                model_override=policy_retry_model,
                                task_id=task_id,
                            )
                            if execute and execute_require_video_context:
                                retry_meta = retry_payload.get("_meta", {}) if isinstance(retry_payload, dict) else {}
                                retry_video_attached = bool(retry_meta.get("video_attached", False))
                                retry_mode = str(retry_meta.get("mode", "unknown"))
                                if not retry_video_attached:
                                    raise RuntimeError(
                                        "Execute blocked: stronger-model retry returned text-only "
                                        "(no video context)."
                                    )
                                print(f"[gemini] stronger-model retry has video context ({retry_mode}).")

                            retry_plan = _normalize_segment_plan(retry_payload, segments, cfg=cfg)
                            retry_no_action_rewrites = _rewrite_no_action_pauses_in_plan(retry_plan, cfg)
                            if retry_no_action_rewrites:
                                print(
                                    "[policy] stronger-model pass rewrote short no-action pauses: "
                                    f"{retry_no_action_rewrites}"
                                )
                            retry_validation_report = _validate_segment_plan_against_policy(cfg, segments, retry_plan)
                            retry_raw_errors = [
                                str(e).strip()
                                for e in retry_validation_report.get("errors", [])
                                if str(e).strip()
                            ]

                            if len(retry_raw_errors) <= len(raw_errors):
                                print(
                                    "[policy] accepted stronger-model retry: "
                                    f"errors {len(raw_errors)} -> {len(retry_raw_errors)}"
                                )
                                labels_payload = retry_payload
                                segment_plan = retry_plan
                                validation_report = retry_validation_report
                                _save_outputs(cfg, segments, prompt, labels_payload, task_id=task_id)
                                if task_id:
                                    _save_task_text_files(cfg, task_id, segments, segment_plan)
                                    _save_cached_labels(cfg, task_id, labels_payload)
                                    if resume_from_artifacts:
                                        task_state["labels_ready"] = True
                                        _save_task_state(cfg, task_id, task_state)
                            else:
                                print(
                                    "[policy] kept primary-model output: stronger-model retry was not better "
                                    f"({len(raw_errors)} -> {len(retry_raw_errors)})."
                                )
                        except Exception as retry_exc:
                            print(f"[policy] stronger-model retry failed: {retry_exc}")

                    report_path = _save_validation_report(cfg, report_task_id, validation_report)
                    if report_path is not None:
                        print(f"[out] validation: {report_path}")

                    warnings = [str(w).strip() for w in validation_report.get("warnings", []) if str(w).strip()]
                    errors = [str(e).strip() for e in validation_report.get("errors", []) if str(e).strip()]
                    ignored_ts_errors: List[str] = []
                    if (
                        not bool(_cfg_get(cfg, "run.adjust_timestamps", True))
                        and bool(
                            _cfg_get(
                                cfg,
                                "run.ignore_timestamp_policy_errors_when_adjust_disabled",
                                True,
                            )
                        )
                    ):
                        ignored_ts_errors = [e for e in errors if _is_timestamp_policy_error(e)]
                        if ignored_ts_errors:
                            errors = [e for e in errors if not _is_timestamp_policy_error(e)]
                            print(
                                f"[policy] ignored timestamp errors: {len(ignored_ts_errors)} "
                                "(adjust_timestamps=false)"
                            )
                            for item in ignored_ts_errors[:10]:
                                print(f"  - {item}")
                    ignored_no_action_errors: List[str] = []
                    if bool(_cfg_get(cfg, "run.ignore_no_action_standalone_policy_error", True)):
                        ignored_no_action_errors = [e for e in errors if _is_no_action_policy_error(e)]
                        if ignored_no_action_errors:
                            errors = [e for e in errors if not _is_no_action_policy_error(e)]
                            print(
                                f"[policy] ignored no-action standalone errors: "
                                f"{len(ignored_no_action_errors)}"
                            )
                            for item in ignored_no_action_errors[:10]:
                                print(f"  - {item}")
                    if warnings:
                        print(f"[policy] warnings: {len(warnings)}")
                        for item in warnings[:10]:
                            print(f"  - {item}")
                    if task_id and resume_from_artifacts:
                        task_state["validation_ok"] = len(errors) == 0
                        _save_task_state(cfg, task_id, task_state)
                    if errors:
                        print(f"[policy] validation errors: {len(errors)}")
                        for item in errors[:20]:
                            print(f"  - {item}")
                        if bool(_cfg_get(cfg, "run.block_apply_on_validation_fail", True)):
                            print("[run] policy gate blocked apply for this episode.")
                            if task_id:
                                blocked_task_ids.add(task_id)
                                if skip_duplicate_task_in_run:
                                    seen_task_ids.add(task_id)
                                _invalidate_cached_labels(cfg, task_id)
                            if keep_alive_when_idle and max_episodes_per_run <= 0:
                                if room_url:
                                    print("[run] returning to room page after policy gate block.")
                                    try:
                                        _goto_with_retry(
                                            page,
                                            room_url,
                                            wait_until="domcontentloaded",
                                            timeout_ms=45000,
                                            cfg=cfg,
                                            reason="room-after-policy-block",
                                        )
                                    except Exception:
                                        pass
                                _respect_episode_delay(cfg)
                                continue
                            break

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
                    completed_from_resume = False
                    if allow_resume_auto_submit:
                        completed_from_resume = _submit_episode(page, cfg)
                    else:
                        print("[run] resume auto-submit disabled; not clicking Complete from stale state.")
                    result = {
                        "applied": 0,
                        "skipped_unchanged": 0,
                        "failed": [],
                        "completed": completed_from_resume,
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
                if bool(result.get("submit_guard_blocked")):
                    print("[run] episode submit skipped by submit guard.")
                    for item in result.get("submit_guard_reasons", [])[:10]:
                        print(f"  - {item}")
                elif not result.get("completed"):
                    print("[run] episode submit could not be fully verified (Complete/Quality confirmation not fully observed).")
                    if release_and_reserve_on_submit_unverified:
                        _release_then_reserve_cycle("submit could not be fully verified")
                    if task_id:
                        blocked_task_ids.add(task_id)
                elif task_id and resume_from_artifacts:
                    task_state["episode_submitted"] = True
                    _save_task_state(cfg, task_id, task_state)

                if task_id:
                    seen_task_ids.add(task_id)
                    if task_id in video_prepare_failures_by_task:
                        video_prepare_failures_by_task.pop(task_id, None)
                    if task_id in gemini_failures_by_task:
                        gemini_failures_by_task.pop(task_id, None)
                consecutive_episode_failures = 0

                if room_url:
                    print("[run] returning to room page for next episode.")
                    _goto_with_retry(
                        page,
                        room_url,
                        wait_until="domcontentloaded",
                        timeout_ms=45000,
                        cfg=cfg,
                        reason="room-after-episode",
                    )
                    page.wait_for_timeout(1500)
                _respect_episode_delay(cfg)
        except Exception as exc:
            _capture_debug_artifacts(page, cfg, prefix="debug_failure")
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
    parser.add_argument(
        "--max-episodes",
        type=int,
        default=None,
        help="Override run.max_episodes_per_run for this process only.",
    )
    parser.add_argument(
        "--gemini-model",
        type=str,
        default="",
        help="Override gemini.model for this process only (e.g., gemini-3.1-pro-preview).",
    )
    parser.add_argument(
        "--use-fallback-key",
        action="store_true",
        help="Use GEMINI_API_KEY_FALLBACK as primary key for this process only.",
    )
    return parser.parse_args()


def _apply_cli_overrides(cfg: Dict[str, Any], args: argparse.Namespace) -> None:
    run_cfg = cfg.setdefault("run", {})
    gem_cfg = cfg.setdefault("gemini", {})
    if not isinstance(run_cfg, dict):
        cfg["run"] = {}
        run_cfg = cfg["run"]
    if not isinstance(gem_cfg, dict):
        cfg["gemini"] = {}
        gem_cfg = cfg["gemini"]

    if args.max_episodes is not None:
        run_cfg["max_episodes_per_run"] = max(1, int(args.max_episodes))
        print(f"[cli] override: run.max_episodes_per_run={run_cfg['max_episodes_per_run']}")

    model_override = str(args.gemini_model or "").strip()
    if model_override:
        gem_cfg["model"] = model_override
        print(f"[cli] override: gemini.model={model_override}")

    if bool(args.use_fallback_key):
        fallback_key = _resolve_gemini_fallback_key(str(gem_cfg.get("fallback_api_key", "") or ""))
        if not fallback_key:
            raise RuntimeError(
                "--use-fallback-key requested but no fallback key found. "
                "Set GEMINI_API_KEY_FALLBACK (or gemini.fallback_api_key)."
            )
        gem_cfg["api_key"] = fallback_key
        gem_cfg["fallback_api_key"] = ""
        gem_cfg["quota_fallback_enabled"] = False
        print("[cli] override: using fallback Gemini API key as primary for this run.")


def main() -> None:
    print(f"[build] atlas_web_auto_solver {_SCRIPT_BUILD}")
    args = parse_args()
    cfg = load_config(Path(args.config))
    _apply_cli_overrides(cfg, args)
    run(cfg, execute=bool(args.execute))


if __name__ == "__main__":
    main()
