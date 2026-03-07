"""
Collect Atlas disputes/feedback data, map episodes to local outputs artifacts,
and request a Gemini progress review report.

Supports one-shot runs and continuous background collection.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
import yaml
from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright


DEFAULTS: Dict[str, Any] = {
    "browser": {
        "headless": False,
        "slow_mo_ms": 40,
        "storage_state_path": ".state/atlas_auth.json",
        "use_chrome_profile": True,
        "chrome_channel": "chrome",
        "chrome_user_data_dir": "",
        "chrome_profile_directory": "Default",
    },
    "gemini": {
        "model": "gemini-2.5-flash",
        "connect_timeout_sec": 30,
        "request_timeout_sec": 300,
        "temperature": 0.0,
        "candidate_count": 1,
        "top_p": 0.95,
        "top_k": 64,
        "max_output_tokens": 8192,
        "strict_json_only": True,
        "reject_markdown_code_fences": True,
    },
    "training_feedback": {
        "enabled": True,
        "continuous": False,
        "interval_sec": 3600,
        "max_cycles": 0,
        "max_episodes": 20,
        "root_dir": "outputs/training_feedback",
        "runs_subdir": "runs",
        "live_subdir": "live",
        "migrate_legacy_dirs": True,
        "max_consecutive_failures": 5,
        "track_t4_disputes": True,
        "t4_tracker_filename": "t4_tracker_state.json",
        "t4_transitions_filename": "t4_transitions_history.jsonl",
        "t4_lessons_filename": "t4_lessons_history.jsonl",
        "disputes_status_filters": ["Disputed", "Awaiting T2", "Both OK"],
        "enable_alignment_review": True,
        "alignment_lessons_filename": "alignment_lessons_history.jsonl",
        "alignment_max_episodes_in_prompt": 20,
        "alignment_max_chars_per_episode": 2500,
        # Keep false by default so this collector can run alongside solver.
        "use_chrome_profile": False,
    },
}


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def cfg_get(cfg: Dict[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = cfg
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def load_config(path: Path) -> Dict[str, Any]:
    raw = {}
    if path.exists():
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raw = {}
    return deep_merge(DEFAULTS, raw)


def first_visible(page: Page, selectors: List[str], timeout_ms: int = 1500):
    deadline = time.time() + (timeout_ms / 1000.0)
    while time.time() < deadline:
        for sel in selectors:
            try:
                loc = page.locator(sel)
                count = min(loc.count(), 8)
                for i in range(count):
                    item = loc.nth(i)
                    if item.is_visible():
                        return item
            except Exception:
                continue
        time.sleep(0.08)
    return None


def dismiss_understand_modals(page: Page) -> None:
    button_selectors = [
        'button:has-text("I Understand")',
        'button:has-text("Understand")',
        'button:has-text("OK")',
        'button:has-text("Okay")',
        'button:has-text("Got It")',
        'button:has-text("Continue")',
        'button:has-text("Close")',
        '[role="button"]:has-text("I Understand")',
        '[role="button"]:has-text("Understand")',
        '[role="button"]:has-text("OK")',
        '[role="button"]:has-text("Okay")',
    ]
    for _ in range(4):
        clicked = False
        for sel in button_selectors:
            loc = first_visible(page, [sel], timeout_ms=300)
            if loc is None:
                continue
            try:
                loc.click(timeout=700, force=True, no_wait_after=True)
                clicked = True
            except Exception:
                continue
        if not clicked:
            break
        page.wait_for_timeout(250)


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def page_text(page: Page, max_chars: int = 120000) -> str:
    try:
        txt = page.inner_text("body")
    except Exception:
        txt = ""
    txt = normalize_space(txt)
    return txt[:max_chars]


def extract_episode_id_from_text(text: str) -> str:
    raw = str(text or "")
    m = re.search(r"\bepisode\s+([a-f0-9]{6,})\b", raw, flags=re.I)
    if m:
        return m.group(1).lower()
    m = re.search(r"\b([a-f0-9]{16,})\b", raw, flags=re.I)
    return m.group(1).lower() if m else ""


def _safe_read_text(path: Path, max_chars: int = 1500) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")[:max_chars]
    except Exception:
        return ""


def summarize_matched_outputs(matches: List[Path], episode_id: str) -> Dict[str, Any]:
    eid = (episode_id or "").strip().lower()
    summary: Dict[str, Any] = {
        "episode_id": eid,
        "text_current": "",
        "text_update": "",
        "labels_json_excerpt": "",
        "validation_json_excerpt": "",
        "other_text_snippets": [],
    }
    if not matches:
        return summary

    used_other = 0
    for p in matches:
        name = p.name.lower()
        if p.suffix.lower() in {".mp4", ".png", ".jpg", ".jpeg", ".gif", ".webm", ".wav", ".mp3"}:
            continue
        try:
            if p.stat().st_size > 2_000_000:
                continue
        except Exception:
            continue
        txt = _safe_read_text(p, max_chars=1800)
        if not txt:
            continue

        if eid and f"text_{eid}_current.txt" in name and not summary["text_current"]:
            summary["text_current"] = txt
            continue
        if eid and f"text_{eid}_update.txt" in name and not summary["text_update"]:
            summary["text_update"] = txt
            continue
        if eid and f"labels_{eid}.json" in name and not summary["labels_json_excerpt"]:
            summary["labels_json_excerpt"] = txt
            continue
        if eid and f"validation_{eid}.json" in name and not summary["validation_json_excerpt"]:
            summary["validation_json_excerpt"] = txt
            continue

        if used_other < 3:
            summary["other_text_snippets"].append(
                {
                    "file": str(p),
                    "excerpt": txt[:900],
                }
            )
            used_other += 1
    return summary


def _extract_named_counter(text: str, label: str) -> Optional[int]:
    src = normalize_space(text)
    if not src:
        return None
    pattern = rf"\b{re.escape(label)}\b\s*[\(\[\:]?\s*(\d{{1,5}})\s*[\)\]]?"
    m = re.search(pattern, src, flags=re.I)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def derive_dispute_bucket(card_text: str, detail_text: str) -> str:
    combined = normalize_space(f"{card_text}\n{detail_text}").lower()
    if "both ok" in combined:
        return "both_ok"
    if "awaiting t2" in combined:
        return "awaiting_t2"
    if "disputed" in combined:
        return "disputed"
    return "unknown"


def save_page_artifacts(page: Page, out_dir: Path, stem: str) -> Dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    html_path = out_dir / f"{stem}.html"
    txt_path = out_dir / f"{stem}.txt"
    png_path = out_dir / f"{stem}.png"
    try:
        html_path.write_text(page.content(), encoding="utf-8")
    except Exception:
        pass
    try:
        txt_path.write_text(page_text(page), encoding="utf-8")
    except Exception:
        pass
    try:
        page.screenshot(path=str(png_path), full_page=True)
    except Exception:
        pass
    return {
        "html": str(html_path),
        "text": str(txt_path),
        "screenshot": str(png_path),
    }


def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False))
        f.write("\n")


def detect_episode_review_state(card_text: str, detail_text: str) -> Dict[str, Any]:
    combined = normalize_space(f"{card_text}\n{detail_text}")
    low = combined.lower()
    dispute_bucket = derive_dispute_bucket(card_text, detail_text)
    pending_signals = [
        "pending t4",
        "t4 auditor will review",
        "has disputed your changes",
        "disputed your changes",
        "awaiting t4",
        "disputed",
    ]
    resolved_signals = [
        "t3 final",
        "no feedback (t3 final)",
        "changes from t3",
        "audited",
        "quality audits",
        "both ok",
    ]
    pending = any(sig in low for sig in pending_signals)
    resolved = any(sig in low for sig in resolved_signals)
    score_match = re.search(r"\b(\d{1,3})\s*%\b", combined)
    tier_match = re.search(r"\b(T[1-4])\b", combined, flags=re.I)
    if pending:
        state = "pending_t4"
    elif resolved:
        state = "resolved"
    else:
        state = "unknown"
    return {
        "state": state,
        "dispute_bucket": dispute_bucket,
        "is_disputed": dispute_bucket == "disputed",
        "is_awaiting_t2": dispute_bucket == "awaiting_t2",
        "is_both_ok": dispute_bucket == "both_ok",
        "pending_t4": pending,
        "resolved": resolved,
        "score_percent": int(score_match.group(1)) if score_match else None,
        "tier": tier_match.group(1).upper() if tier_match else "",
        "signals_excerpt": combined[:1200],
    }


def extract_view_entries(page: Page) -> List[Dict[str, Any]]:
    js = r"""
() => {
  const out = [];
  const seen = new Set();
  const nodes = Array.from(document.querySelectorAll('a,button,[role="button"]'));
  const viewNodes = nodes.filter(n => /\bview\b/i.test((n.innerText || n.textContent || '').trim()));
  for (const n of viewNodes) {
    let box = n;
    for (let i = 0; i < 6 && box; i++) {
      const t = (box.innerText || '').trim();
      if (/Episode\s+[a-f0-9]{6,}/i.test(t)) break;
      box = box.parentElement;
    }
    const txt = (box && box.innerText) ? box.innerText : (n.innerText || n.textContent || '');
    const clean = txt.replace(/\s+/g, ' ').trim();
    const m = clean.match(/Episode\s+([a-f0-9]{6,})/i);
    const id = m ? m[1].toLowerCase() : '';
    const href = (n.getAttribute && n.getAttribute('href')) ? n.getAttribute('href') : (n.href || '');
    const key = id + '|' + href + '|' + clean.slice(0, 120);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      episode_id: id,
      href: href || '',
      text: clean,
      view_text: (n.innerText || n.textContent || '').replace(/\s+/g, ' ').trim(),
    });
  }
  return out;
}
"""
    try:
        rows = page.evaluate(js) or []
    except Exception:
        rows = []
    out: List[Dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "episode_id": str(item.get("episode_id", "")).strip().lower(),
                "href": str(item.get("href", "")).strip(),
                "text": str(item.get("text", "")).strip(),
                "view_text": str(item.get("view_text", "")).strip(),
            }
        )
    return out


def click_tab_by_text(page: Page, name: str) -> bool:
    candidates = [
        f'button:has-text("{name}")',
        f'[role="tab"]:has-text("{name}")',
        f'a:has-text("{name}")',
        f'text=/{re.escape(name)}/i',
    ]
    loc = first_visible(page, candidates, timeout_ms=2500)
    if loc is None:
        return False
    try:
        loc.click(timeout=1200, force=True, no_wait_after=True)
        page.wait_for_timeout(900)
        return True
    except Exception:
        return False


def find_output_matches(outputs_dir: Path, episode_id: str) -> List[Path]:
    eid = (episode_id or "").strip().lower()
    if not eid:
        return []
    matches: List[Path] = []
    for p in outputs_dir.glob("*"):
        try:
            name = p.name.lower()
            if eid in name and p.is_file():
                matches.append(p)
        except Exception:
            continue
    return sorted(matches)


def copy_matches(matches: List[Path], dest: Path) -> List[str]:
    dest.mkdir(parents=True, exist_ok=True)
    copied: List[str] = []
    for src in matches:
        try:
            tgt = dest / src.name
            if src.is_file():
                shutil.copy2(src, tgt)
                copied.append(str(tgt))
        except Exception:
            continue
    return copied


def resolve_gemini_key(cfg: Dict[str, Any]) -> str:
    explicit = str(cfg_get(cfg, "gemini.api_key", "")).strip()
    if explicit:
        return explicit
    for env_name in ["GEMINI_API_KEY", "GOOGLE_API_KEY"]:
        val = os.environ.get(env_name, "").strip()
        if val:
            return val
    return ""


def _extract_gemini_text(data: Dict[str, Any]) -> str:
    for candidate in data.get("candidates", []):
        content = candidate.get("content", {}) if isinstance(candidate, dict) else {}
        parts = content.get("parts", []) if isinstance(content, dict) else []
        if not isinstance(parts, list):
            continue
        text = "".join([str(p.get("text", "")) for p in parts if isinstance(p, dict)]).strip()
        if text:
            return text
    return ""


def _clean_json_text(text: str) -> str:
    clean = re.sub(r"```json|```", "", text or "", flags=re.IGNORECASE).strip()
    obj_start = clean.find("{")
    obj_end = clean.rfind("}")
    if obj_start >= 0 and obj_end > obj_start:
        return clean[obj_start : obj_end + 1]
    arr_start = clean.find("[")
    arr_end = clean.rfind("]")
    if arr_start >= 0 and arr_end > arr_start:
        return clean[arr_start : arr_end + 1]
    return clean


def _parse_gemini_json_text(
    text: str,
    strict_json_only: bool = True,
    reject_markdown_code_fences: bool = True,
) -> Dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        raise ValueError("Gemini returned empty text response.")
    if reject_markdown_code_fences and "```" in raw:
        raise ValueError("Gemini response included markdown code fences.")
    candidate = raw if strict_json_only else _clean_json_text(raw)
    if strict_json_only and not (candidate.startswith("{") and candidate.endswith("}")):
        raise ValueError("Gemini response must be raw JSON object only (no commentary).")
    payload = json.loads(candidate)
    if not isinstance(payload, dict):
        raise ValueError("Gemini JSON payload must be an object.")
    return payload


def _parse_gemini_response_json(cfg: Dict[str, Any], data: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
    text = _extract_gemini_text(data)
    parsed = _parse_gemini_json_text(
        text,
        strict_json_only=bool(cfg_get(cfg, "gemini.strict_json_only", True)),
        reject_markdown_code_fences=bool(cfg_get(cfg, "gemini.reject_markdown_code_fences", True)),
    )
    return parsed, text


def _build_gemini_generation_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    generation: Dict[str, Any] = {
        "temperature": float(cfg_get(cfg, "gemini.temperature", 0.0)),
        "responseMimeType": "application/json",
        "candidateCount": max(1, int(cfg_get(cfg, "gemini.candidate_count", 1))),
    }
    top_p_raw = cfg_get(cfg, "gemini.top_p", None)
    top_k_raw = cfg_get(cfg, "gemini.top_k", None)
    max_output_tokens_raw = cfg_get(cfg, "gemini.max_output_tokens", None)
    try:
        if top_p_raw is not None and str(top_p_raw).strip() != "":
            top_p = float(top_p_raw)
            if top_p > 0:
                generation["topP"] = top_p
    except Exception:
        pass
    try:
        if top_k_raw is not None and str(top_k_raw).strip() != "":
            top_k = int(top_k_raw)
            if top_k > 0:
                generation["topK"] = top_k
    except Exception:
        pass
    try:
        if max_output_tokens_raw is not None and str(max_output_tokens_raw).strip() != "":
            max_output_tokens = int(max_output_tokens_raw)
            if max_output_tokens > 0:
                generation["maxOutputTokens"] = max_output_tokens
    except Exception:
        pass
    return generation


def call_gemini_progress_review(cfg: Dict[str, Any], prompt_text: str) -> Dict[str, Any]:
    key = resolve_gemini_key(cfg)
    if not key:
        raise RuntimeError("Missing Gemini API key in env (GEMINI_API_KEY/GOOGLE_API_KEY).")
    model = str(cfg_get(cfg, "gemini.model", "gemini-2.5-flash"))
    connect_timeout = int(cfg_get(cfg, "gemini.connect_timeout_sec", 30))
    request_timeout = int(cfg_get(cfg, "gemini.request_timeout_sec", 300))
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    headers = {"Content-Type": "application/json", "X-goog-api-key": key}
    system_instruction = (
        "You are Atlas QA progress analyst. "
        "Given disputes/feedback/review evidence and episode-level artifacts, "
        "assess quality trend, root causes, and a concrete action plan to improve annotation quality. "
        "Return JSON object only with no markdown and no commentary. "
        "Response must start with '{' and end with '}'."
    )
    payload = {
        "systemInstruction": {"parts": [{"text": system_instruction}]},
        "contents": [{"role": "user", "parts": [{"text": prompt_text}]}],
        "generationConfig": _build_gemini_generation_config(cfg),
    }
    last_error = ""
    for attempt in range(4):
        try:
            resp = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=(connect_timeout, request_timeout),
            )
            if resp.status_code == 200:
                data = resp.json()
                parsed, _ = _parse_gemini_response_json(cfg, data)
                return {"http_status": 200, "raw": data, "parsed": parsed}
            last_error = f"HTTP {resp.status_code}: {(resp.text or '')[:500]}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep((2**attempt) * 1.0)
    raise RuntimeError(f"Gemini progress review failed: {last_error}")


def call_gemini_t4_lessons(cfg: Dict[str, Any], prompt_text: str) -> Dict[str, Any]:
    key = resolve_gemini_key(cfg)
    if not key:
        raise RuntimeError("Missing Gemini API key in env (GEMINI_API_KEY/GOOGLE_API_KEY).")
    model = str(cfg_get(cfg, "gemini.model", "gemini-2.5-flash"))
    connect_timeout = int(cfg_get(cfg, "gemini.connect_timeout_sec", 30))
    request_timeout = int(cfg_get(cfg, "gemini.request_timeout_sec", 300))
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    headers = {"Content-Type": "application/json", "X-goog-api-key": key}
    system_instruction = (
        "You are Atlas dispute-resolution trainer. "
        "Given episodes that moved from Pending T4 to final outcome, extract correction rules "
        "that reduce future disputes. Return JSON object only with no markdown and no commentary. "
        "Response must start with '{' and end with '}'."
    )
    payload = {
        "systemInstruction": {"parts": [{"text": system_instruction}]},
        "contents": [{"role": "user", "parts": [{"text": prompt_text}]}],
        "generationConfig": _build_gemini_generation_config(cfg),
    }
    last_error = ""
    for attempt in range(4):
        try:
            resp = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=(connect_timeout, request_timeout),
            )
            if resp.status_code == 200:
                data = resp.json()
                parsed, _ = _parse_gemini_response_json(cfg, data)
                return {"http_status": 200, "raw": data, "parsed": parsed}
            last_error = f"HTTP {resp.status_code}: {(resp.text or '')[:500]}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep((2**attempt) * 1.0)
    raise RuntimeError(f"Gemini T4 lessons failed: {last_error}")


def call_gemini_alignment_review(cfg: Dict[str, Any], prompt_text: str) -> Dict[str, Any]:
    key = resolve_gemini_key(cfg)
    if not key:
        raise RuntimeError("Missing Gemini API key in env (GEMINI_API_KEY/GOOGLE_API_KEY).")
    model = str(cfg_get(cfg, "gemini.model", "gemini-2.5-flash"))
    connect_timeout = int(cfg_get(cfg, "gemini.connect_timeout_sec", 30))
    request_timeout = int(cfg_get(cfg, "gemini.request_timeout_sec", 300))
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    headers = {"Content-Type": "application/json", "X-goog-api-key": key}
    system_instruction = (
        "You are Atlas dispute and feedback trainer. "
        "Compare employee outcomes from feedback/disputes with the local Gemini suggestion artifacts. "
        "Classify if both are correct, employee better, gemini better, or both need correction. "
        "Return JSON object only with no markdown and no commentary. "
        "Response must start with '{' and end with '}'."
    )
    payload = {
        "systemInstruction": {"parts": [{"text": system_instruction}]},
        "contents": [{"role": "user", "parts": [{"text": prompt_text}]}],
        "generationConfig": _build_gemini_generation_config(cfg),
    }
    last_error = ""
    for attempt in range(4):
        try:
            resp = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=(connect_timeout, request_timeout),
            )
            if resp.status_code == 200:
                data = resp.json()
                parsed, _ = _parse_gemini_response_json(cfg, data)
                return {"http_status": 200, "raw": data, "parsed": parsed}
            last_error = f"HTTP {resp.status_code}: {(resp.text or '')[:500]}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep((2**attempt) * 1.0)
    raise RuntimeError(f"Gemini alignment review failed: {last_error}")


def build_alignment_prompt(
    episodes: List[Dict[str, Any]],
    tabs: Dict[str, Any],
    status_filters: List[str],
    max_episodes: int,
    max_chars_per_episode: int,
) -> str:
    lines: List[str] = []
    lines.append("Atlas feedback/disputes alignment learning request.")
    lines.append(
        "Goal: compare employee-reviewed outcome against local Gemini artifacts and decide where both are correct."
    )
    lines.append("Prioritize dispute buckets: Disputed, Awaiting T2, Both OK.")
    lines.append("")
    lines.append("Return strict JSON with keys:")
    lines.append("Return JSON object only, no markdown code fences, no commentary.")
    lines.append(
        "episode_verdicts (array of {episode_id, dispute_bucket, verdict, confidence_0_1, reason, "
        "policy_update, prompt_patch, qa_guardrail}), "
        "bucket_summary, global_policy_updates, reviewer_checklist, top_failure_patterns."
    )
    lines.append("")

    lines.append("[STATUS VIEWS]")
    for tab_name in ("My Disputes", "My Reviews"):
        tab_info = tabs.get(tab_name, {}) if isinstance(tabs, dict) else {}
        status_views = tab_info.get("status_views", {}) if isinstance(tab_info, dict) else {}
        for status_name in status_filters:
            info = status_views.get(status_name, {}) if isinstance(status_views, dict) else {}
            if not isinstance(info, dict):
                continue
            count = info.get("counter_hint")
            excerpt = str(info.get("text_excerpt", ""))[:1000]
            lines.append(f"- tab={tab_name} status={status_name} count={count}")
            if excerpt:
                lines.append(f"  excerpt: {excerpt}")
    lines.append("")

    used = 0
    for ep in episodes:
        if used >= max(1, int(max_episodes)):
            break
        if not isinstance(ep, dict):
            continue
        eid = str(ep.get("episode_id", "")).strip()
        if not eid:
            continue
        bucket = "unknown"
        st = ep.get("status_snapshot", {})
        if isinstance(st, dict):
            bucket = str(st.get("dispute_bucket", "unknown"))
        lines.append(f"[EPISODE] {eid}")
        lines.append(f"bucket={bucket}")
        lines.append(f"card_text={str(ep.get('card_text',''))[:600]}")
        lines.append(f"detail_excerpt={str(ep.get('text_excerpt',''))[:max(200, int(max_chars_per_episode))]}")

        snippets = ep.get("matched_output_snippets", {})
        if isinstance(snippets, dict):
            lines.append(f"text_current={str(snippets.get('text_current',''))[:900]}")
            lines.append(f"text_update={str(snippets.get('text_update',''))[:900]}")
            lines.append(f"labels_json_excerpt={str(snippets.get('labels_json_excerpt',''))[:900]}")
            lines.append(f"validation_json_excerpt={str(snippets.get('validation_json_excerpt',''))[:900]}")
        lines.append("")
        used += 1
    return "\n".join(lines)


def build_t4_lessons_prompt(transitions: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    lines.append("Atlas Pending-T4 transition learning request.")
    lines.append("Analyze each episode transition and return strict JSON with keys:")
    lines.append("Return JSON object only, no markdown code fences, no commentary.")
    lines.append(
        "lessons (array of {episode_id, root_cause, corrected_policy, prompt_patch, qa_precheck}), "
        "global_rules, risky_patterns, operator_dos, operator_donts"
    )
    lines.append("")
    for item in transitions:
        lines.append(f"[EPISODE] {item.get('episode_id','')}")
        lines.append(f"pending_card: {item.get('pending_card_text','')[:500]}")
        lines.append(f"resolved_card: {item.get('resolved_card_text','')[:500]}")
        lines.append(f"pending_excerpt: {item.get('pending_excerpt','')[:1200]}")
        lines.append(f"resolved_excerpt: {item.get('resolved_excerpt','')[:1200]}")
        lines.append("")
    return "\n".join(lines)


def update_t4_tracker_state(
    tracker: Dict[str, Any],
    episodes: List[Dict[str, Any]],
    run_dir: Path,
    generated_at: str,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    episodes_state = tracker.setdefault("episodes", {})
    transitions: List[Dict[str, Any]] = []

    for ep in episodes:
        episode_id = str(ep.get("episode_id", "")).strip().lower()
        if not episode_id:
            continue
        status = ep.get("status_snapshot", {}) if isinstance(ep.get("status_snapshot"), dict) else {}
        state = str(status.get("state", "unknown")).strip().lower() or "unknown"
        card_text = str(ep.get("card_text", "")).strip()
        excerpt = str(ep.get("text_excerpt", "")).strip()[:2000]
        rec = episodes_state.get(episode_id, {})
        if not isinstance(rec, dict):
            rec = {}
        prev_state = str(rec.get("state", "unknown")).strip().lower() or "unknown"

        rec.setdefault("episode_id", episode_id)
        rec.setdefault("first_seen", generated_at)
        rec["last_seen"] = generated_at
        rec["state"] = state
        rec["last_run_dir"] = str(run_dir)
        rec["last_card_text"] = card_text[:1200]
        rec["last_excerpt"] = excerpt
        rec["last_score_percent"] = status.get("score_percent")
        rec["last_tier"] = status.get("tier", "")

        if state == "pending_t4":
            rec["pending_since"] = rec.get("pending_since") or generated_at
            rec["pending_card_text"] = card_text[:1200]
            rec["pending_excerpt"] = excerpt
        elif state == "resolved":
            rec["resolved_at"] = generated_at
            rec["resolved_card_text"] = card_text[:1200]
            rec["resolved_excerpt"] = excerpt
            if prev_state == "pending_t4":
                transitions.append(
                    {
                        "episode_id": episode_id,
                        "from_state": prev_state,
                        "to_state": state,
                        "pending_since": rec.get("pending_since"),
                        "resolved_at": generated_at,
                        "pending_card_text": rec.get("pending_card_text", ""),
                        "resolved_card_text": rec.get("resolved_card_text", ""),
                        "pending_excerpt": rec.get("pending_excerpt", ""),
                        "resolved_excerpt": rec.get("resolved_excerpt", ""),
                        "last_run_dir": str(run_dir),
                    }
                )
        episodes_state[episode_id] = rec

    tracker["last_updated"] = generated_at
    tracker["episodes"] = episodes_state
    return tracker, transitions


def resolve_training_paths(cfg: Dict[str, Any], root: Path) -> Tuple[Path, Path, Path, Path]:
    outputs_dir_cfg = str(cfg_get(cfg, "run.output_dir", "outputs")).strip() or "outputs"
    outputs_dir = Path(outputs_dir_cfg)
    if not outputs_dir.is_absolute():
        outputs_dir = root / outputs_dir

    root_dir_cfg = str(cfg_get(cfg, "training_feedback.root_dir", "outputs/training_feedback")).strip()
    training_root = Path(root_dir_cfg)
    if not training_root.is_absolute():
        training_root = root / training_root

    runs_subdir = str(cfg_get(cfg, "training_feedback.runs_subdir", "runs")).strip() or "runs"
    live_subdir = str(cfg_get(cfg, "training_feedback.live_subdir", "live")).strip() or "live"
    runs_root = training_root / runs_subdir
    live_root = training_root / live_subdir

    outputs_dir.mkdir(parents=True, exist_ok=True)
    training_root.mkdir(parents=True, exist_ok=True)
    runs_root.mkdir(parents=True, exist_ok=True)
    live_root.mkdir(parents=True, exist_ok=True)
    return outputs_dir, training_root, runs_root, live_root


def migrate_legacy_training_dirs(outputs_dir: Path, runs_root: Path) -> List[Dict[str, str]]:
    moved: List[Dict[str, str]] = []
    for old_dir in sorted(outputs_dir.glob("training_feedback_*")):
        if not old_dir.is_dir():
            continue
        target = runs_root / old_dir.name
        if target.exists():
            idx = 1
            while True:
                candidate = runs_root / f"{old_dir.name}_migrated_{idx}"
                if not candidate.exists():
                    target = candidate
                    break
                idx += 1
        try:
            shutil.move(str(old_dir), str(target))
            moved.append({"from": str(old_dir), "to": str(target)})
        except Exception:
            continue
    return moved


def unique_run_dir(runs_root: Path, prefix: str = "training_feedback") -> Path:
    base = f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    out = runs_root / base
    if not out.exists():
        out.mkdir(parents=True, exist_ok=False)
        return out
    idx = 1
    while True:
        candidate = runs_root / f"{base}_{idx}"
        if not candidate.exists():
            candidate.mkdir(parents=True, exist_ok=False)
            return candidate
        idx += 1


def write_live_indexes(
    training_root: Path,
    live_root: Path,
    run_index: Dict[str, Any],
) -> None:
    latest_json = training_root / "latest.json"
    latest_json.write_text(json.dumps(run_index, ensure_ascii=False, indent=2), encoding="utf-8")

    manifest_jsonl = training_root / "runs_manifest.jsonl"
    with manifest_jsonl.open("a", encoding="utf-8") as f:
        f.write(json.dumps(run_index, ensure_ascii=False))
        f.write("\n")

    live_summary = live_root / "last_run_summary.json"
    live_summary.write_text(json.dumps(run_index, ensure_ascii=False, indent=2), encoding="utf-8")


def launch_context(
    pw,
    cfg: Dict[str, Any],
    headless: bool,
    force_no_profile: bool,
) -> Tuple[BrowserContext, Optional[Browser]]:
    channel = str(cfg_get(cfg, "browser.chrome_channel", "chrome"))
    slow_mo = int(cfg_get(cfg, "browser.slow_mo_ms", 40))

    use_profile_cfg = bool(cfg_get(cfg, "training_feedback.use_chrome_profile", False))
    if force_no_profile:
        use_profile_cfg = False

    context: Optional[BrowserContext] = None
    browser: Optional[Browser] = None
    if use_profile_cfg:
        user_data_dir = str(cfg_get(cfg, "browser.chrome_user_data_dir", "")).strip()
        profile_dir = str(cfg_get(cfg, "browser.chrome_profile_directory", "Default")).strip()
        launch_args = [f"--profile-directory={profile_dir}"] if profile_dir else []
        if user_data_dir:
            try:
                context = pw.chromium.launch_persistent_context(
                    user_data_dir=user_data_dir,
                    channel=channel,
                    headless=headless,
                    slow_mo=slow_mo,
                    args=launch_args,
                    timeout=35000,
                )
                return context, None
            except Exception:
                context = None

    browser = pw.chromium.launch(channel=channel, headless=headless, slow_mo=slow_mo)
    ctx_kwargs: Dict[str, Any] = {}
    state_path = str(cfg_get(cfg, "browser.storage_state_path", ".state/atlas_auth.json")).strip()
    state_file = Path(state_path)
    if not state_file.is_absolute():
        state_file = Path.cwd() / state_file
    if state_path and state_file.exists():
        ctx_kwargs["storage_state"] = str(state_file)
    context = browser.new_context(**ctx_kwargs)
    return context, browser


def collect_training_snapshot(
    context: BrowserContext,
    cfg: Dict[str, Any],
    outputs_dir: Path,
    run_dir: Path,
    training_root: Path,
    live_root: Path,
    max_episodes: int,
    skip_gemini: bool,
) -> Dict[str, Any]:
    pages_dir = run_dir / "pages"
    episodes_dir = run_dir / "episodes"
    pages_dir.mkdir(parents=True, exist_ok=True)
    episodes_dir.mkdir(parents=True, exist_ok=True)

    disputes_url = "https://audit.atlascapture.io/disputes"
    feedback_url = "https://audit.atlascapture.io/feedback"
    tabs = ["Feedback", "My Disputes", "My Reviews", "Quality Audits"]
    status_filters = [
        str(s).strip()
        for s in (cfg_get(cfg, "training_feedback.disputes_status_filters", ["Disputed", "Awaiting T2", "Both OK"]) or [])
        if str(s).strip()
    ]

    data: Dict[str, Any] = {
        "generated_at": datetime.now().isoformat(),
        "disputes_url": disputes_url,
        "feedback_url": feedback_url,
        "tabs": {},
        "feedback_entries": [],
        "episodes": [],
    }

    disputes_page = context.new_page()
    feedback_page = context.new_page()
    details_page = context.new_page()
    for p in (disputes_page, feedback_page, details_page):
        p.set_default_timeout(12000)

    try:
        disputes_page.goto(disputes_url, wait_until="domcontentloaded")
        disputes_page.wait_for_timeout(1200)
        dismiss_understand_modals(disputes_page)
        data["disputes_root_artifacts"] = save_page_artifacts(disputes_page, pages_dir, "disputes_root")
        for tab in tabs:
            clicked = click_tab_by_text(disputes_page, tab)
            dismiss_understand_modals(disputes_page)
            stem = f"disputes_tab_{re.sub(r'[^a-zA-Z0-9]+', '_', tab.lower())}"
            artifacts = save_page_artifacts(disputes_page, pages_dir, stem)
            tab_obj: Dict[str, Any] = {
                "clicked": clicked,
                "url": disputes_page.url,
                "artifacts": artifacts,
                "text_excerpt": page_text(disputes_page, max_chars=8000),
            }
            if tab in {"My Disputes", "My Reviews"}:
                status_views: Dict[str, Any] = {}
                for status_name in status_filters:
                    status_clicked = click_tab_by_text(disputes_page, status_name)
                    dismiss_understand_modals(disputes_page)
                    status_stem = (
                        f"{stem}_status_{re.sub(r'[^a-zA-Z0-9]+', '_', status_name.lower())}"
                    )
                    status_artifacts = save_page_artifacts(disputes_page, pages_dir, status_stem)
                    status_text = page_text(disputes_page, max_chars=5000)
                    status_views[status_name] = {
                        "clicked": status_clicked,
                        "url": disputes_page.url,
                        "counter_hint": _extract_named_counter(status_text, status_name),
                        "artifacts": status_artifacts,
                        "text_excerpt": status_text,
                    }
                tab_obj["status_views"] = status_views
            data["tabs"][tab] = tab_obj

        feedback_page.goto(feedback_url, wait_until="domcontentloaded")
        feedback_page.wait_for_timeout(1200)
        dismiss_understand_modals(feedback_page)
        data["feedback_root_artifacts"] = save_page_artifacts(feedback_page, pages_dir, "feedback_root")
        entries = extract_view_entries(feedback_page)
        data["feedback_entries"] = entries

        used = 0
        for item in entries:
            if used >= max(1, int(max_episodes)):
                break
            episode_id = str(item.get("episode_id", "")).strip().lower()
            if not episode_id:
                episode_id = extract_episode_id_from_text(str(item.get("text", "")))
            href = str(item.get("href", "")).strip()
            if not episode_id and not href:
                continue
            if href:
                target = href if href.startswith("http") else urljoin(feedback_page.url, href)
            else:
                continue
            try:
                details_page.goto(target, wait_until="domcontentloaded")
                details_page.wait_for_timeout(1200)
                dismiss_understand_modals(details_page)
            except Exception:
                continue

            if not episode_id:
                m = re.search(
                    r"episode\s+([a-f0-9]{6,})",
                    page_text(details_page, max_chars=5000),
                    flags=re.I,
                )
                episode_id = m.group(1).lower() if m else ""
            if not episode_id:
                m = re.search(r"/([a-f0-9]{8,})", details_page.url, flags=re.I)
                episode_id = m.group(1).lower() if m else f"unknown_{used + 1}"

            ep_dir = episodes_dir / episode_id
            artifacts = save_page_artifacts(details_page, ep_dir, f"episode_{episode_id}_detail")
            text_excerpt = page_text(details_page, max_chars=20000)
            status_snapshot = detect_episode_review_state(str(item.get("text", "")), text_excerpt)
            matches = find_output_matches(outputs_dir, episode_id)
            copied = copy_matches(matches, ep_dir / "matched_outputs")
            matched_output_snippets = summarize_matched_outputs(matches, episode_id)

            data["episodes"].append(
                {
                    "episode_id": episode_id,
                    "source_href": target,
                    "current_url": details_page.url,
                    "card_text": item.get("text", ""),
                    "status_snapshot": status_snapshot,
                    "artifacts": artifacts,
                    "text_excerpt": text_excerpt,
                    "matched_output_files_count": len(matches),
                    "matched_output_files": [str(p) for p in matches],
                    "copied_output_files": copied,
                    "matched_output_snippets": matched_output_snippets,
                }
            )
            used += 1
    finally:
        for p in (details_page, feedback_page, disputes_page):
            try:
                p.close()
            except Exception:
                pass

    dataset_path = run_dir / "training_dataset.json"
    dataset_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_lines: List[str] = []
    summary_lines.append("Atlas training dataset review request.")
    summary_lines.append("Objective: evaluate annotation quality progress and produce corrective training plan.")
    summary_lines.append("")
    summary_lines.append("Disputes tabs snapshots:")
    for tab_name, tab_info in data.get("tabs", {}).items():
        excerpt = str(tab_info.get("text_excerpt", ""))[:2500]
        summary_lines.append(f"[TAB] {tab_name}")
        summary_lines.append(excerpt)
        summary_lines.append("")
    summary_lines.append("Feedback episode cards:")
    for row in data.get("feedback_entries", [])[:80]:
        summary_lines.append(
            f"- episode_id={row.get('episode_id', '')} href={row.get('href', '')} text={row.get('text', '')[:260]}"
        )
    summary_lines.append("")
    summary_lines.append("Episode detail evidence + matched local outputs:")
    for ep in data.get("episodes", [])[: max(1, int(max_episodes))]:
        st = ep.get("status_snapshot", {}) if isinstance(ep.get("status_snapshot"), dict) else {}
        summary_lines.append(
            f"[EP] {ep.get('episode_id')} matched_files={ep.get('matched_output_files_count', 0)} "
            f"state={st.get('state','unknown')} bucket={st.get('dispute_bucket','unknown')} "
            f"score={st.get('score_percent')}"
        )
        summary_lines.append(str(ep.get("text_excerpt", ""))[:3500])
        summary_lines.append("")
    summary_lines.append(
        "Return strict JSON with keys: "
        "progress_score_0_100, strengths, recurring_failures, root_causes, "
        "prompt_updates, qa_checklist, next_10_episode_plan."
    )
    summary_lines.append("Return JSON object only, no markdown code fences, no commentary.")
    prompt_text = "\n".join(summary_lines)
    prompt_path = run_dir / "gemini_progress_prompt.txt"
    prompt_path.write_text(prompt_text, encoding="utf-8")

    gemini_status = "skipped"
    if not skip_gemini:
        try:
            gemini_resp = call_gemini_progress_review(cfg, prompt_text)
            (run_dir / "gemini_progress_response.json").write_text(
                json.dumps(gemini_resp, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            parsed = gemini_resp.get("parsed", {})
            (run_dir / "gemini_progress_parsed.json").write_text(
                json.dumps(parsed, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            gemini_status = "ok"
        except Exception as exc:
            (run_dir / "gemini_progress_error.txt").write_text(str(exc), encoding="utf-8")
            gemini_status = "error"

    alignment_enabled = bool(cfg_get(cfg, "training_feedback.enable_alignment_review", True))
    alignment_status = "skipped"
    alignment_prompt_path = run_dir / "gemini_alignment_prompt.txt"
    alignment_max_episodes = int(cfg_get(cfg, "training_feedback.alignment_max_episodes_in_prompt", max_episodes))
    alignment_max_chars = int(cfg_get(cfg, "training_feedback.alignment_max_chars_per_episode", 2500))
    if alignment_enabled:
        try:
            alignment_prompt = build_alignment_prompt(
                episodes=data.get("episodes", []),
                tabs=data.get("tabs", {}),
                status_filters=status_filters,
                max_episodes=alignment_max_episodes,
                max_chars_per_episode=alignment_max_chars,
            )
            alignment_prompt_path.write_text(alignment_prompt, encoding="utf-8")
            if not skip_gemini:
                alignment_resp = call_gemini_alignment_review(cfg, alignment_prompt)
                (run_dir / "gemini_alignment_response.json").write_text(
                    json.dumps(alignment_resp, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                alignment_parsed = alignment_resp.get("parsed", {})
                (run_dir / "gemini_alignment_parsed.json").write_text(
                    json.dumps(alignment_parsed, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                # Optional convenience file for quick policy patching.
                if isinstance(alignment_parsed, dict):
                    policy_updates = alignment_parsed.get("global_policy_updates", [])
                    if isinstance(policy_updates, list) and policy_updates:
                        lines = []
                        for idx, item in enumerate(policy_updates, start=1):
                            lines.append(f"{idx}. {str(item)}")
                        (run_dir / "policy_update_suggestions.txt").write_text(
                            "\n".join(lines),
                            encoding="utf-8",
                        )
                    alignment_hist_name = str(
                        cfg_get(cfg, "training_feedback.alignment_lessons_filename", "alignment_lessons_history.jsonl")
                    ).strip() or "alignment_lessons_history.jsonl"
                    append_jsonl(
                        live_root / alignment_hist_name,
                        {
                            "generated_at": datetime.now().isoformat(),
                            "run_dir": str(run_dir),
                            "episodes_in_dataset": len(data.get("episodes", [])),
                            "parsed": alignment_parsed,
                        },
                    )
                alignment_status = "ok"
        except Exception as exc:
            (run_dir / "gemini_alignment_error.txt").write_text(str(exc), encoding="utf-8")
            alignment_status = "error"

    track_t4 = bool(cfg_get(cfg, "training_feedback.track_t4_disputes", True))
    t4_transitions: List[Dict[str, Any]] = []
    t4_lessons_status = "skipped"
    tracker_counts = {"total": 0, "pending_t4": 0, "resolved": 0}
    if track_t4:
        tracker_filename = str(cfg_get(cfg, "training_feedback.t4_tracker_filename", "t4_tracker_state.json")).strip()
        transitions_filename = str(
            cfg_get(cfg, "training_feedback.t4_transitions_filename", "t4_transitions_history.jsonl")
        ).strip()
        lessons_filename = str(cfg_get(cfg, "training_feedback.t4_lessons_filename", "t4_lessons_history.jsonl")).strip()
        tracker_path = live_root / (tracker_filename or "t4_tracker_state.json")
        transitions_path = live_root / (transitions_filename or "t4_transitions_history.jsonl")
        lessons_path = live_root / (lessons_filename or "t4_lessons_history.jsonl")

        tracker = load_json_file(tracker_path, {"episodes": {}, "last_updated": ""})
        if not isinstance(tracker, dict):
            tracker = {"episodes": {}, "last_updated": ""}
        tracker, t4_transitions = update_t4_tracker_state(
            tracker=tracker,
            episodes=data.get("episodes", []),
            run_dir=run_dir,
            generated_at=datetime.now().isoformat(),
        )
        tracker_path.write_text(json.dumps(tracker, ensure_ascii=False, indent=2), encoding="utf-8")
        (run_dir / "t4_tracker_snapshot.json").write_text(
            json.dumps(tracker, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (run_dir / "t4_transitions.json").write_text(
            json.dumps(t4_transitions, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        for tr in t4_transitions:
            append_jsonl(
                transitions_path,
                {
                    "generated_at": datetime.now().isoformat(),
                    "run_dir": str(run_dir),
                    **tr,
                },
            )

        eps = tracker.get("episodes", {})
        if isinstance(eps, dict):
            states = [str(v.get("state", "")) for v in eps.values() if isinstance(v, dict)]
            tracker_counts["total"] = len(states)
            tracker_counts["pending_t4"] = sum(1 for s in states if s == "pending_t4")
            tracker_counts["resolved"] = sum(1 for s in states if s == "resolved")

        if t4_transitions and not skip_gemini:
            try:
                t4_prompt = build_t4_lessons_prompt(t4_transitions)
                (run_dir / "gemini_t4_lessons_prompt.txt").write_text(t4_prompt, encoding="utf-8")
                t4_resp = call_gemini_t4_lessons(cfg, t4_prompt)
                (run_dir / "gemini_t4_lessons_response.json").write_text(
                    json.dumps(t4_resp, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                (run_dir / "gemini_t4_lessons_parsed.json").write_text(
                    json.dumps(t4_resp.get("parsed", {}), ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                append_jsonl(
                    lessons_path,
                    {
                        "generated_at": datetime.now().isoformat(),
                        "run_dir": str(run_dir),
                        "transitions_count": len(t4_transitions),
                        "parsed": t4_resp.get("parsed", {}),
                    },
                )
                t4_lessons_status = "ok"
            except Exception as exc:
                (run_dir / "gemini_t4_lessons_error.txt").write_text(str(exc), encoding="utf-8")
                t4_lessons_status = "error"

    bucket_counts = {"disputed": 0, "awaiting_t2": 0, "both_ok": 0, "unknown": 0}
    for ep in data.get("episodes", []):
        if not isinstance(ep, dict):
            continue
        st = ep.get("status_snapshot", {})
        bucket = "unknown"
        if isinstance(st, dict):
            bucket = str(st.get("dispute_bucket", "unknown")).strip().lower() or "unknown"
        if bucket not in bucket_counts:
            bucket_counts[bucket] = 0
        bucket_counts[bucket] += 1

    index = {
        "training_dir": str(run_dir),
        "dataset_json": str(dataset_path),
        "prompt": str(prompt_path),
        "alignment_prompt": str(alignment_prompt_path),
        "episodes_collected": len(data.get("episodes", [])),
        "feedback_entries_found": len(data.get("feedback_entries", [])),
        "tabs_captured": list(data.get("tabs", {}).keys()),
        "gemini_status": gemini_status,
        "alignment_enabled": alignment_enabled,
        "alignment_status": alignment_status,
        "bucket_disputed": bucket_counts.get("disputed", 0),
        "bucket_awaiting_t2": bucket_counts.get("awaiting_t2", 0),
        "bucket_both_ok": bucket_counts.get("both_ok", 0),
        "bucket_unknown": bucket_counts.get("unknown", 0),
        "t4_tracking_enabled": track_t4,
        "t4_transitions_detected": len(t4_transitions),
        "t4_lessons_status": t4_lessons_status,
        "t4_tracker_total_episodes": tracker_counts["total"],
        "t4_pending_episodes": tracker_counts["pending_t4"],
        "t4_resolved_episodes": tracker_counts["resolved"],
        "generated_at": datetime.now().isoformat(),
    }
    (run_dir / "INDEX.json").write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
    return index


def run_once_or_loop(args: argparse.Namespace) -> None:
    root = Path.cwd()
    cfg = load_config(root / args.config)

    outputs_dir, training_root, runs_root, live_root = resolve_training_paths(cfg, root)
    if bool(cfg_get(cfg, "training_feedback.migrate_legacy_dirs", True)):
        moved = migrate_legacy_training_dirs(outputs_dir, runs_root)
        if moved:
            (training_root / "legacy_migration.json").write_text(
                json.dumps({"moved": moved, "migrated_at": datetime.now().isoformat()}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    continuous = bool(args.continuous or cfg_get(cfg, "training_feedback.continuous", False))
    interval_sec = int(
        args.interval_sec if args.interval_sec is not None else cfg_get(cfg, "training_feedback.interval_sec", 3600)
    )
    max_cycles = int(
        args.max_cycles if args.max_cycles is not None else cfg_get(cfg, "training_feedback.max_cycles", 0)
    )
    max_episodes = int(
        args.max_episodes if args.max_episodes is not None else cfg_get(cfg, "training_feedback.max_episodes", 20)
    )
    max_failures = int(cfg_get(cfg, "training_feedback.max_consecutive_failures", 5))
    headless = bool(args.headless or cfg_get(cfg, "browser.headless", False))

    with sync_playwright() as pw:
        context, browser = launch_context(
            pw=pw,
            cfg=cfg,
            headless=headless,
            force_no_profile=bool(args.no_profile),
        )
        try:
            cycle = 0
            failures = 0
            while True:
                cycle += 1
                run_dir = unique_run_dir(runs_root)
                try:
                    index = collect_training_snapshot(
                        context=context,
                        cfg=cfg,
                        outputs_dir=outputs_dir,
                        run_dir=run_dir,
                        training_root=training_root,
                        live_root=live_root,
                        max_episodes=max_episodes,
                        skip_gemini=bool(args.skip_gemini),
                    )
                    index["cycle"] = cycle
                    index["continuous"] = continuous
                    index["interval_sec"] = interval_sec
                    (run_dir / "INDEX.json").write_text(
                        json.dumps(index, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                    write_live_indexes(training_root, live_root, index)
                    failures = 0
                    print(f"[training] saved: {run_dir}")
                    print(f"[training] episodes_collected: {index['episodes_collected']}")
                    print(f"[training] feedback_entries_found: {index['feedback_entries_found']}")
                    print(f"[training] gemini_status: {index['gemini_status']}")
                    if index.get("t4_tracking_enabled"):
                        print(
                            f"[training] t4 transitions={index.get('t4_transitions_detected', 0)} "
                            f"pending={index.get('t4_pending_episodes', 0)} "
                            f"resolved={index.get('t4_resolved_episodes', 0)} "
                            f"lessons={index.get('t4_lessons_status', 'skipped')}"
                        )
                except Exception as exc:
                    failures += 1
                    err = {
                        "generated_at": datetime.now().isoformat(),
                        "cycle": cycle,
                        "error": str(exc),
                    }
                    (run_dir / "RUN_ERROR.json").write_text(json.dumps(err, ensure_ascii=False, indent=2), encoding="utf-8")
                    print(f"[training] cycle failed: {exc}")
                    write_live_indexes(
                        training_root,
                        live_root,
                        {
                            "training_dir": str(run_dir),
                            "cycle": cycle,
                            "status": "error",
                            "error": str(exc),
                            "generated_at": datetime.now().isoformat(),
                        },
                    )

                if not continuous:
                    break
                if max_cycles > 0 and cycle >= max_cycles:
                    print(f"[training] reached max_cycles={max_cycles}.")
                    break
                if failures >= max_failures:
                    print(f"[training] stopped after {failures} consecutive failures.")
                    break
                sleep_for = max(3, interval_sec)
                print(f"[training] waiting {sleep_for}s before next cycle...")
                time.sleep(sleep_for)
        finally:
            context.close()
            if browser is not None:
                browser.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="sample_web_auto_solver.yaml")
    parser.add_argument("--max-episodes", type=int, default=None)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--interval-sec", type=int, default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--skip-gemini", action="store_true")
    parser.add_argument(
        "--no-profile",
        action="store_true",
        help="Use isolated context with storage_state (recommended when solver is running in parallel).",
    )
    args = parser.parse_args()
    run_once_or_loop(args)


if __name__ == "__main__":
    main()
