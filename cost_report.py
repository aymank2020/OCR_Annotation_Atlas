"""
Cost report for Gemini/Vertex usage logs.

Reads JSONL from outputs/gemini_usage.jsonl and prints:
- total cost
- today's cost (based on a chosen timezone)
- last N hours cost
- grouped cost by mode and model
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _parse_ts_utc(raw: Any) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _load_usage_rows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        ts = _parse_ts_utc(row.get("ts_utc"))
        if ts is None:
            continue
        row["_dt_utc"] = ts
        row["_cost"] = _safe_float(row.get("estimated_cost_usd", 0.0), 0.0)
        row["_prompt_tokens"] = _safe_int(row.get("prompt_tokens", 0), 0)
        row["_output_tokens"] = _safe_int(row.get("output_tokens", 0), 0)
        row["_total_tokens"] = _safe_int(row.get("total_tokens", 0), 0)
        rows.append(row)
    return rows


def _sum_cost(rows: List[Dict[str, Any]]) -> float:
    return sum(_safe_float(r.get("_cost", 0.0), 0.0) for r in rows)


def _sum_tokens(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    prompt = sum(_safe_int(r.get("_prompt_tokens", 0), 0) for r in rows)
    output = sum(_safe_int(r.get("_output_tokens", 0), 0) for r in rows)
    total = sum(_safe_int(r.get("_total_tokens", 0), 0) for r in rows)
    return {"prompt_tokens": prompt, "output_tokens": output, "total_tokens": total}


def _group_cost(rows: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    grouped_cost: Dict[str, float] = defaultdict(float)
    grouped_count: Dict[str, int] = defaultdict(int)
    for row in rows:
        name = str(row.get(key, "unknown") or "unknown").strip() or "unknown"
        grouped_cost[name] += _safe_float(row.get("_cost", 0.0), 0.0)
        grouped_count[name] += 1
    out = [
        {"name": name, "rows": grouped_count[name], "usd": round(grouped_cost[name], 8)}
        for name in grouped_cost
    ]
    out.sort(key=lambda x: float(x["usd"]), reverse=True)
    return out


def build_report(usage_log: Path, tz_name: str, last_hours: float) -> Dict[str, Any]:
    rows = _load_usage_rows(usage_log)
    if not rows:
        return {"usage_log": str(usage_log), "rows": 0}

    now_utc = datetime.now(timezone.utc)
    window_cutoff = now_utc - timedelta(hours=max(0.1, last_hours))

    tz = timezone.utc
    tz_used = "UTC"
    if ZoneInfo is not None:
        try:
            tz = ZoneInfo(tz_name)  # type: ignore[misc]
            tz_used = tz_name
        except Exception:
            tz = timezone.utc
            tz_used = "UTC"

    now_local = now_utc.astimezone(tz)
    day_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    day_start_utc = day_start_local.astimezone(timezone.utc)

    rows_today = [r for r in rows if r["_dt_utc"] >= day_start_utc]
    rows_window = [r for r in rows if r["_dt_utc"] >= window_cutoff]

    latest = max(rows, key=lambda r: r["_dt_utc"])
    earliest = min(rows, key=lambda r: r["_dt_utc"])

    return {
        "usage_log": str(usage_log),
        "rows": len(rows),
        "timezone_used": tz_used,
        "last_hours": last_hours,
        "range": {
            "from_utc": earliest["_dt_utc"].isoformat(),
            "to_utc": latest["_dt_utc"].isoformat(),
        },
        "totals": {
            "all_usd": round(_sum_cost(rows), 8),
            "today_usd": round(_sum_cost(rows_today), 8),
            "last_window_usd": round(_sum_cost(rows_window), 8),
            **_sum_tokens(rows),
        },
        "group_by_mode": _group_cost(rows, "mode"),
        "group_by_model": _group_cost(rows, "model"),
        "last_row": {
            "ts_utc": latest.get("ts_utc", ""),
            "model": latest.get("model", ""),
            "mode": latest.get("mode", ""),
            "key_source": latest.get("key_source", ""),
            "estimated_cost_usd": round(_safe_float(latest.get("_cost", 0.0), 0.0), 8),
            "prompt_tokens": _safe_int(latest.get("_prompt_tokens", 0), 0),
            "output_tokens": _safe_int(latest.get("_output_tokens", 0), 0),
            "total_tokens": _safe_int(latest.get("_total_tokens", 0), 0),
        },
    }


def _print_human(report: Dict[str, Any], top: int) -> None:
    if report.get("rows", 0) <= 0:
        print(f"NO_USAGE_LOG_FOUND_OR_EMPTY: {report.get('usage_log', '')}")
        return

    totals = report["totals"]
    print(f"usage_log={report['usage_log']}")
    print(f"rows={report['rows']}")
    print(f"timezone_used={report['timezone_used']}")
    print(
        "range_utc="
        f"{report['range']['from_utc']} .. {report['range']['to_utc']}"
    )
    print(f"total_usd_all={totals['all_usd']:.6f}")
    print(f"total_usd_today={totals['today_usd']:.6f}")
    print(f"total_usd_last_{report['last_hours']}_hours={totals['last_window_usd']:.6f}")
    print(
        "tokens_all="
        f"prompt={totals['prompt_tokens']} "
        f"output={totals['output_tokens']} total={totals['total_tokens']}"
    )

    print("\nby_mode:")
    for item in report["group_by_mode"][: max(1, top)]:
        print(f"  {item['name']}: usd={float(item['usd']):.6f} rows={item['rows']}")

    print("\nby_model:")
    for item in report["group_by_model"][: max(1, top)]:
        print(f"  {item['name']}: usd={float(item['usd']):.6f} rows={item['rows']}")

    print("\nlast_row:")
    print(json.dumps(report["last_row"], ensure_ascii=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate cost report from gemini_usage.jsonl")
    parser.add_argument("--usage-log", default="outputs/gemini_usage.jsonl", help="Path to usage log JSONL")
    parser.add_argument("--timezone", default="Africa/Cairo", help="Timezone for 'today' aggregation")
    parser.add_argument("--last-hours", type=float, default=24.0, help="Sliding window in hours")
    parser.add_argument("--top", type=int, default=20, help="Max rows shown per group in text output")
    parser.add_argument("--json", action="store_true", help="Print as JSON")
    args = parser.parse_args()

    report = build_report(
        usage_log=Path(args.usage_log).resolve(),
        tz_name=str(args.timezone),
        last_hours=float(args.last_hours),
    )

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    _print_human(report, top=max(1, int(args.top)))


if __name__ == "__main__":
    main()

