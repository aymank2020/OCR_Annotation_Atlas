"""
Build a single review index for all Atlas episodes from outputs/.

The index is intended for manual/AI audit loops (ChatGPT/Claude/Gemini)
to compare Tier2 vs Tier3 and inspect validation/disputes per episode.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


EPISODE_ID_PATTERN = re.compile(r"([0-9a-f]{24})", re.IGNORECASE)
NOT_FOUND_HINTS = (
    "not found",
    "404",
    "does not exist",
    "doesn't exist",
    "cannot find",
    "page you are looking",
)


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _extract_episode_id(text: str) -> Optional[str]:
    m = EPISODE_ID_PATTERN.search(text or "")
    return m.group(1).lower() if m else None


def _extract_ts_from_path(path: Path) -> str:
    m = re.search(r"(20\d{6}_\d{6})", str(path))
    if not m:
        return ""
    return m.group(1)


def _resolve_atlas_state_path(outputs_dir: Path, atlas_state_path: Optional[str]) -> Optional[Path]:
    candidates: List[Path] = []
    if atlas_state_path:
        p = Path(atlas_state_path).expanduser()
        candidates.append(p)
    candidates.extend(
        [
            Path(".state/atlas_auth.json"),
            outputs_dir.parent / ".state" / "atlas_auth.json",
            Path(__file__).resolve().parent / ".state" / "atlas_auth.json",
        ]
    )
    for c in candidates:
        try:
            p = c.resolve()
        except Exception:
            p = c
        if p.exists() and p.is_file():
            return p
    return None


def _load_cookie_header_from_storage_state(path: Optional[Path]) -> str:
    if not path:
        return ""
    payload = _load_json(path)
    if not isinstance(payload, dict):
        return ""
    cookies = payload.get("cookies")
    if not isinstance(cookies, list):
        return ""

    parts: List[str] = []
    now = datetime.now(timezone.utc).timestamp()
    for c in cookies:
        if not isinstance(c, dict):
            continue
        name = str(c.get("name") or "").strip()
        value = str(c.get("value") or "")
        if not name:
            continue
        expires = c.get("expires")
        if isinstance(expires, (int, float)) and expires > 0 and expires < now:
            continue
        domain = str(c.get("domain") or "").lower()
        if "atlascapture.io" not in domain:
            continue
        parts.append(f"{name}={value}")
    return "; ".join(parts)


def _probe_url(url: str, eid: str, cookie_header: str, timeout_sec: float) -> Dict[str, Any]:
    headers = {
        "User-Agent": "Mozilla/5.0 (AtlasReviewBuilder/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    if cookie_header:
        headers["Cookie"] = cookie_header

    status: Optional[int] = None
    final_url = url
    body_bytes = b""
    error = ""
    try:
        req = Request(url=url, headers=headers, method="GET")
        with urlopen(req, timeout=max(1.0, float(timeout_sec))) as resp:
            status = int(getattr(resp, "status", 200))
            final_url = str(resp.geturl() or url)
            body_bytes = resp.read(32768) or b""
    except HTTPError as exc:
        status = int(exc.code)
        final_url = str(exc.geturl() or url)
        try:
            body_bytes = exc.read(8192) or b""
        except Exception:
            body_bytes = b""
        error = f"http_{exc.code}"
    except URLError as exc:
        error = str(exc.reason)
    except Exception as exc:
        error = str(exc)

    body_l = body_bytes.decode("utf-8", errors="replace").lower()
    path_l = urlparse(final_url).path.lower()
    login_redirect = ("/login" in path_l) or ("signin" in path_l)
    has_not_found = any(token in body_l for token in NOT_FOUND_HINTS)
    has_eid = eid in path_l or eid in body_l
    body_hash = hashlib.sha1(body_bytes).hexdigest() if body_bytes else ""

    requested_path = urlparse(url).path.lower()
    path_match = bool(path_l and requested_path and (path_l == requested_path or requested_path in path_l))
    positive = bool(status and status < 400 and path_match and not login_redirect and not has_not_found)

    return {
        "url": url,
        "status": status,
        "final_url": final_url,
        "error": error,
        "positive": positive,
        "has_eid": has_eid,
        "login_redirect": login_redirect,
        "has_not_found": has_not_found,
        "body_hash": body_hash,
    }


def _probe_episode_status_from_atlas(
    eid: str,
    cookie_header: str,
    timeout_sec: float,
) -> Dict[str, Any]:
    urls = _build_atlas_urls(eid, status="unknown")
    disputes_probe = _probe_url(urls["disputes_url"], eid=eid, cookie_header=cookie_header, timeout_sec=timeout_sec)
    feedback_probe = _probe_url(urls["feedback_url"], eid=eid, cookie_header=cookie_header, timeout_sec=timeout_sec)

    disputes_ok = bool(disputes_probe.get("positive"))
    feedback_ok = bool(feedback_probe.get("positive"))

    resolved: Optional[str] = None
    reason = "no_positive_route"
    inconclusive = False

    if disputes_ok and not feedback_ok:
        resolved = "disputed"
        reason = "disputes_route_positive"
    elif feedback_ok and not disputes_ok:
        resolved = "submitted"
        reason = "feedback_route_positive"
    elif disputes_ok and feedback_ok:
        # If both routes look positive and same page shell was served, treat as inconclusive.
        if (
            disputes_probe.get("body_hash")
            and disputes_probe.get("body_hash") == feedback_probe.get("body_hash")
            and not disputes_probe.get("has_eid")
            and not feedback_probe.get("has_eid")
        ):
            inconclusive = True
            reason = "both_positive_same_shell"
        else:
            # Prefer disputes if both are positive because it is more specific.
            resolved = "disputed"
            reason = "both_positive_prefer_disputes"

    return {
        "status": resolved,
        "reason": reason,
        "inconclusive": inconclusive,
        "checked": [disputes_probe, feedback_probe],
    }


def _pick_first(paths: List[Path]) -> Optional[str]:
    if not paths:
        return None
    return str(sorted(paths)[0])


def _find_task_state_dirs(outputs_dir: Path) -> List[Path]:
    candidates = [
        outputs_dir / ".task_state",
        outputs_dir / "task_state",
        outputs_dir.parent / ".task_state",
        outputs_dir.parent / "task_state",
    ]
    return [p for p in candidates if p.exists() and p.is_dir()]


def _collect_episode_files(outputs_dir: Path) -> Dict[str, Dict[str, List[Path]]]:
    """
    Build a file map by episode id.
    file_map[eid][bucket] = [paths...]
    """
    file_map: Dict[str, Dict[str, List[Path]]] = defaultdict(lambda: defaultdict(list))

    # Targeted buckets from outputs root / nested folders.
    for path in outputs_dir.rglob("*"):
        if not path.is_file():
            continue
        eid = _extract_episode_id(path.name)
        if not eid:
            continue
        name_l = path.name.lower()
        rel = path.relative_to(outputs_dir)

        if name_l.startswith("video_") and path.suffix.lower() in {".mp4", ".webm", ".mov"}:
            file_map[eid]["videos"].append(path)
        if name_l.startswith("text_") and name_l.endswith("_current.txt"):
            file_map[eid]["tier2_text"].append(path)
        if name_l.startswith("text_") and name_l.endswith("_update.txt"):
            file_map[eid]["tier3_text"].append(path)
        if name_l.startswith("validation_") and path.suffix.lower() == ".json":
            file_map[eid]["validation"].append(path)
        if name_l.startswith("task_state_") and path.suffix.lower() == ".json":
            file_map[eid]["task_state_files"].append(path)
        if name_l.startswith("labels_") and path.suffix.lower() == ".json":
            file_map[eid]["labels"].append(path)
        if name_l.startswith("segments_") and path.suffix.lower() == ".json":
            file_map[eid]["segments"].append(path)
        if name_l.startswith("prompt_") and path.suffix.lower() == ".txt":
            file_map[eid]["prompts"].append(path)

        # Keep a bounded list of related files per episode.
        if len(file_map[eid]["related"]) < 200:
            file_map[eid]["related"].append(outputs_dir / rel)

    # Look into training_feedback runs/episodes/<episode_id>/ for tier/state/detail files.
    runs_root = outputs_dir / "training_feedback" / "runs"
    if runs_root.exists():
        for ep_dir in runs_root.glob("*/episodes/*"):
            if not ep_dir.is_dir():
                continue
            eid = _extract_episode_id(ep_dir.name)
            if not eid:
                # Fallback: resolve 24-char episode id from files inside this episode folder.
                for p in ep_dir.rglob("*"):
                    if not p.is_file():
                        continue
                    eid = _extract_episode_id(p.name)
                    if eid:
                        break
            if not eid:
                continue
            for p in ep_dir.rglob("*"):
                if not p.is_file():
                    continue
                name_l = p.name.lower()

                if name_l.startswith("task_state_") and p.suffix.lower() == ".json":
                    file_map[eid]["task_state_files"].append(p)
                elif "tier2" in name_l and p.suffix.lower() == ".json":
                    file_map[eid]["tier2_json"].append(p)
                elif ("tier3" in name_l or "final" in name_l or "repaired" in name_l) and p.suffix.lower() == ".json":
                    file_map[eid]["tier3_json"].append(p)
                elif "valid" in name_l and p.suffix.lower() == ".json":
                    file_map[eid]["validation"].append(p)
                elif "detail" in name_l and p.suffix.lower() in {".txt", ".html"}:
                    file_map[eid]["feedback_detail"].append(p)

                if len(file_map[eid]["related"]) < 200:
                    file_map[eid]["related"].append(p)

    return file_map


def _load_task_states(outputs_dir: Path) -> Dict[str, Dict[str, Any]]:
    states: Dict[str, Dict[str, Any]] = {}
    for state_dir in _find_task_state_dirs(outputs_dir):
        for f in state_dir.glob("*.json"):
            payload = _load_json(f)
            if not isinstance(payload, dict):
                continue
            eid = _extract_episode_id(f.stem) or f.stem.lower()
            states[eid] = payload
    return states


def _load_disputes(outputs_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    by_ep: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    live = outputs_dir / "training_feedback" / "live" / "t4_transitions_history.jsonl"
    for row in _load_jsonl(live):
        eid = str(row.get("episode_id") or "").lower().strip()
        if eid:
            by_ep[eid].append(row)
    return by_ep


def _merge_state_dicts(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if not items:
        return merged

    # Keep latest field values, but OR key boolean signals across all snapshots.
    for d in items:
        if isinstance(d, dict):
            merged.update(d)

    merged["episode_submitted"] = any(bool(d.get("episode_submitted") or d.get("submitted")) for d in items if isinstance(d, dict))
    merged["submitted"] = merged["episode_submitted"]
    merged["labels_applied"] = any(bool(d.get("labels_applied")) for d in items if isinstance(d, dict))
    merged["has_error"] = any(bool(d.get("last_error") or d.get("has_error")) for d in items if isinstance(d, dict))

    vals = [d.get("validation_ok") for d in items if isinstance(d, dict) and d.get("validation_ok") is not None]
    if vals:
        # Any successful validation snapshot counts as a pass signal.
        merged["validation_ok"] = any(bool(v) for v in vals)

    return merged


def _load_task_states_from_file_map(file_map: Dict[str, Dict[str, List[Path]]]) -> Dict[str, Dict[str, Any]]:
    states: Dict[str, Dict[str, Any]] = {}
    for eid, buckets in file_map.items():
        paths = buckets.get("task_state_files", [])
        if not paths:
            continue
        payloads: List[Dict[str, Any]] = []
        for p in sorted(paths):
            obj = _load_json(p)
            if isinstance(obj, dict):
                payloads.append(obj)
        if payloads:
            states[eid] = _merge_state_dicts(payloads)
    return states


def _load_usage(outputs_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    by_ep: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    usage_rows = _load_jsonl(outputs_dir / "gemini_usage.jsonl")
    for row in usage_rows:
        eid = str(row.get("episode_id") or row.get("task_id") or "").lower().strip()
        if eid:
            by_ep[eid].append(row)
    return by_ep


def _calc_status(
    state: Dict[str, Any],
    disputes_count: int,
    has_tier3: bool,
    has_feedback_evidence: bool,
) -> str:
    submitted = bool(state.get("episode_submitted") or state.get("submitted") or has_feedback_evidence)
    labels_applied = bool(state.get("labels_applied"))
    policy_ok = state.get("validation_ok")
    has_error = bool(state.get("last_error") or state.get("has_error"))

    if disputes_count > 0:
        return "disputed"
    if submitted:
        return "submitted"
    if policy_ok is False:
        return "policy_fail"
    if has_error:
        return "error"
    if labels_applied or has_tier3:
        return "labeled_not_submitted"
    return "unknown"


def _total_cost(rows: List[Dict[str, Any]]) -> float:
    return round(sum(float(r.get("estimated_cost_usd", 0) or 0.0) for r in rows), 6)


def _build_atlas_urls(eid: str, status: str) -> Dict[str, str]:
    task_url = f"https://audit.atlascapture.io/tasks/room/normal/label/{eid}"
    feedback_url = f"https://audit.atlascapture.io/feedback/{eid}"
    disputes_url = f"https://audit.atlascapture.io/disputes/{eid}"

    if status == "disputed":
        open_url = disputes_url
    elif status == "submitted":
        open_url = feedback_url
    else:
        open_url = task_url

    return {
        "task_url": task_url,
        "feedback_url": feedback_url,
        "disputes_url": disputes_url,
        "open_url": open_url,
    }


def build_index(
    outputs_dir: Path,
    probe_atlas_status: str = "auto",
    atlas_state_path: Optional[str] = None,
    probe_timeout_sec: float = 2.5,
) -> Dict[str, Any]:
    outputs_dir = outputs_dir.resolve()
    print(f"[review-builder] outputs_dir={outputs_dir}")

    file_map = _collect_episode_files(outputs_dir)
    states = _load_task_states(outputs_dir)
    states_from_runs = _load_task_states_from_file_map(file_map)
    for eid, st in states_from_runs.items():
        if eid in states:
            states[eid] = _merge_state_dicts([states[eid], st])
        else:
            states[eid] = st
    disputes = _load_disputes(outputs_dir)
    usage = _load_usage(outputs_dir)

    all_ids = set(file_map.keys()) | set(states.keys()) | set(disputes.keys()) | set(usage.keys())
    print(f"[review-builder] discovered episode ids: {len(all_ids)}")

    episodes: List[Dict[str, Any]] = []
    for eid in sorted(all_ids):
        files = file_map.get(eid, {})
        state = states.get(eid, {})
        ep_disputes = disputes.get(eid, [])
        ep_usage = usage.get(eid, [])

        tier2_text_path = _pick_first(files.get("tier2_text", []))
        tier3_text_path = _pick_first(files.get("tier3_text", []))
        validation_path = _pick_first(files.get("validation", []))

        tier2_text = Path(tier2_text_path).read_text(encoding="utf-8", errors="replace").strip() if tier2_text_path else ""
        tier3_text = Path(tier3_text_path).read_text(encoding="utf-8", errors="replace").strip() if tier3_text_path else ""

        tier2_json = _load_json(Path(_pick_first(files.get("tier2_json", [])))) if files.get("tier2_json") else None
        tier3_json = _load_json(Path(_pick_first(files.get("tier3_json", [])))) if files.get("tier3_json") else None
        validation = _load_json(Path(validation_path)) if validation_path else None

        has_feedback_evidence = bool(files.get("feedback_detail"))
        status = _calc_status(
            state,
            len(ep_disputes),
            bool(tier3_text or tier3_json),
            has_feedback_evidence=has_feedback_evidence,
        )
        urls = _build_atlas_urls(eid, status)
        episode_entry = {
            "episode_id": eid,
            "atlas_url": urls["open_url"],
            "open_url": urls["open_url"],
            "task_url": urls["task_url"],
            "feedback_url": urls["feedback_url"],
            "disputes_url": urls["disputes_url"],
            "review_status": status,
            "status_source": "local_evidence",
            "video_path": _pick_first(files.get("videos", [])),
            "tier2_text_path": tier2_text_path,
            "tier3_text_path": tier3_text_path,
            "tier2_text": tier2_text,
            "tier3_text": tier3_text,
            "tier2": tier2_json,
            "tier3": tier3_json,
            "validation_path": validation_path,
            "validation": validation,
            "task_state": state,
            "disputes_count": len(ep_disputes),
            "disputes": ep_disputes,
            "usage": ep_usage,
            "total_cost_usd": _total_cost(ep_usage),
            "related_files": [str(p) for p in files.get("related", [])],
        }
        episodes.append(episode_entry)

    probe_mode = (probe_atlas_status or "auto").strip().lower()
    atlas_state = _resolve_atlas_state_path(outputs_dir, atlas_state_path)
    cookie_header = _load_cookie_header_from_storage_state(atlas_state)
    should_probe = probe_mode in {"on", "auto"}
    if should_probe and probe_mode == "auto" and not cookie_header:
        should_probe = False

    if should_probe:
        probe_targets = [
            ep
            for ep in episodes
            if str(ep.get("review_status") or "").lower() in {"unknown", "labeled_not_submitted", "policy_fail", "error"}
        ]
        print(
            f"[review-builder] atlas probe: mode={probe_mode} targets={len(probe_targets)} "
            f"state={'yes' if atlas_state else 'no'} cookies={'yes' if bool(cookie_header) else 'no'}"
        )
        if probe_targets:
            max_workers = min(8, max(1, len(probe_targets)))
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                future_to_ep = {}
                for ep in probe_targets:
                    eid = str(ep.get("episode_id") or "").lower().strip()
                    if not eid:
                        continue
                    fut = pool.submit(
                        _probe_episode_status_from_atlas,
                        eid=eid,
                        cookie_header=cookie_header,
                        timeout_sec=probe_timeout_sec,
                    )
                    future_to_ep[fut] = ep

                resolved_count = 0
                for fut in as_completed(future_to_ep):
                    ep = future_to_ep[fut]
                    eid = str(ep.get("episode_id") or "").lower().strip()
                    try:
                        probe = fut.result()
                    except Exception as exc:
                        probe = {"status": None, "reason": f"probe_error:{exc}", "checked": []}
                    ep["atlas_probe"] = probe
                    resolved = str(probe.get("status") or "").strip().lower()
                    if resolved in {"submitted", "disputed"}:
                        resolved_count += 1
                        ep["review_status"] = resolved
                        ep["status_source"] = "atlas_url_probe"
                        urls = _build_atlas_urls(eid, resolved)
                        ep["atlas_url"] = urls["open_url"]
                        ep["open_url"] = urls["open_url"]
                        ep["task_url"] = urls["task_url"]
                        ep["feedback_url"] = urls["feedback_url"]
                        ep["disputes_url"] = urls["disputes_url"]
            print(f"[review-builder] atlas probe resolved statuses: {resolved_count}/{len(probe_targets)}")
    elif probe_mode == "on":
        print("[review-builder] atlas probe requested but no valid auth cookies were found; skipping probe.")

    priority = {
        "disputed": 0,
        "policy_fail": 1,
        "error": 2,
        "labeled_not_submitted": 3,
        "submitted": 4,
        "unknown": 5,
    }
    episodes.sort(key=lambda x: (priority.get(x["review_status"], 99), x["episode_id"]))

    counts = Counter(ep["review_status"] for ep in episodes)
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "outputs_dir": str(outputs_dir),
        "total": len(episodes),
        "probe_atlas_status": probe_mode,
        "probe_state_file": str(atlas_state) if atlas_state else "",
        "status_counts": dict(counts),
        "episodes": episodes,
    }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Build episodes review index for Atlas QA re-audit.")
    parser.add_argument("--outputs-dir", default="outputs", help="Path to outputs directory")
    parser.add_argument(
        "--out",
        default="episodes_review_index.json",
        help="Output JSON file path (recommended: outputs/episodes_review_index.json)",
    )
    parser.add_argument(
        "--probe-atlas-status",
        default="auto",
        choices=["off", "auto", "on"],
        help="Validate unresolved episode status via Atlas feedback/disputes URLs",
    )
    parser.add_argument(
        "--atlas-state",
        default="",
        help="Path to Playwright storage state JSON (atlas_auth.json) used for URL probe cookies",
    )
    parser.add_argument(
        "--probe-timeout-sec",
        type=float,
        default=2.5,
        help="Timeout per URL probe request in seconds",
    )
    args = parser.parse_args()

    payload = build_index(
        Path(args.outputs_dir),
        probe_atlas_status=args.probe_atlas_status,
        atlas_state_path=args.atlas_state or None,
        probe_timeout_sec=float(args.probe_timeout_sec),
    )
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[review-builder] wrote: {out_path}")
    print(f"[review-builder] total episodes: {payload['total']}")
    for status, count in sorted(payload["status_counts"].items()):
        print(f"  - {status}: {count}")


if __name__ == "__main__":
    main()
