"""
Simple watchdog for Atlas solver service.

Checks:
1) systemd service state (active/inactive)
2) latest solver log freshness (outputs/solver_live_*.log)

If unhealthy, restarts the service and logs an event to outputs/watchdog_events.jsonl.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_capture(cmd: List[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def _service_is_active(service: str) -> bool:
    p = _run_capture(["systemctl", "is-active", service])
    return p.returncode == 0 and p.stdout.strip() == "active"


def _restart_service(service: str) -> bool:
    p = _run_capture(["systemctl", "restart", service])
    return p.returncode == 0


def _latest_log(outputs_dir: Path, pattern: str) -> Optional[Path]:
    files = sorted(outputs_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _idle_minutes(log_file: Path) -> float:
    now = datetime.now(timezone.utc).timestamp()
    return max(0.0, (now - log_file.stat().st_mtime) / 60.0)


def _append_event(path: Path, row: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Watchdog for atlas-autopilot service.")
    parser.add_argument("--service", default="atlas-autopilot.service")
    parser.add_argument("--outputs-dir", default="outputs")
    parser.add_argument("--log-glob", default="solver_live_*.log")
    parser.add_argument("--max-log-idle-min", type=float, default=20.0)
    parser.add_argument("--events-file", default="outputs/watchdog_events.jsonl")
    args = parser.parse_args()

    outputs_dir = Path(args.outputs_dir)
    events_file = Path(args.events_file)
    reasons: List[str] = []

    service_ok = _service_is_active(args.service)
    if not service_ok:
        reasons.append("service_inactive")

    latest = _latest_log(outputs_dir, args.log_glob)
    idle_min = None
    if latest is None:
        reasons.append("log_not_found")
    else:
        idle_min = _idle_minutes(latest)
        if float(args.max_log_idle_min) > 0 and idle_min > float(args.max_log_idle_min):
            reasons.append(f"log_stale:{idle_min:.1f}m")

    action = "ok"
    restarted = False
    if reasons:
        restarted = _restart_service(args.service)
        action = "restart_ok" if restarted else "restart_failed"

    row: Dict[str, object] = {
        "ts_utc": _now_utc(),
        "service": args.service,
        "action": action,
        "reasons": reasons,
        "service_active_before": service_ok,
        "latest_log": str(latest) if latest else "",
        "idle_minutes": round(idle_min, 2) if idle_min is not None else None,
    }
    _append_event(events_file, row)
    print(json.dumps(row, ensure_ascii=False))

    if action == "restart_failed":
        raise SystemExit(2)


if __name__ == "__main__":
    main()

