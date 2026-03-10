"""
atlas_dashboard_gen.py
──────────────────────
Reads outputs/ directory and generates a standalone HTML operations dashboard.

Usage:
    python atlas_dashboard_gen.py                          # uses ./outputs
    python atlas_dashboard_gen.py --outputs-dir /path/to/outputs
    python atlas_dashboard_gen.py --open                   # auto-open in browser

Reads:
    outputs/gemini_usage.jsonl       → cost & token metrics per request
    outputs/.task_state/*.json       → per-episode state (submitted, errors, etc.)
    outputs/training_feedback/live/t4_transitions_history.jsonl  → disputes
    outputs/training_feedback/live/alignment_lessons_history.jsonl

Writes:
    outputs/atlas_dashboard.html
"""

from __future__ import annotations

import argparse
import json
import math
import os
import webbrowser
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


# ──────────────────────────────────────────────────────────────────────────────
# Data loaders
# ──────────────────────────────────────────────────────────────────────────────

def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows


def _load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def load_usage(outputs_dir: Path) -> List[Dict[str, Any]]:
    return _load_jsonl(outputs_dir / "gemini_usage.jsonl")


def load_task_states(outputs_dir: Path) -> List[Dict[str, Any]]:
    state_dir = outputs_dir / ".task_state"
    states: List[Dict[str, Any]] = []
    if not state_dir.exists():
        return states
    for f in sorted(state_dir.glob("*.json")):
        data = _load_json(f)
        if isinstance(data, dict):
            data.setdefault("_file", f.stem)
            states.append(data)
    return states


def load_transitions(outputs_dir: Path) -> List[Dict[str, Any]]:
    return _load_jsonl(
        outputs_dir / "training_feedback" / "live" / "t4_transitions_history.jsonl"
    )


def load_lessons(outputs_dir: Path) -> List[Dict[str, Any]]:
    return _load_jsonl(
        outputs_dir / "training_feedback" / "live" / "alignment_lessons_history.jsonl"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Metric computation
# ──────────────────────────────────────────────────────────────────────────────

def compute_cost_metrics(usage: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not usage:
        return {
            "total_cost_usd": 0.0, "total_requests": 0,
            "total_input_tokens": 0, "total_output_tokens": 0,
            "by_day": [], "by_model": {}, "avg_cost_per_request": 0.0,
        }

    total_cost = 0.0
    total_input = 0
    total_output = 0
    by_day: Dict[str, float] = defaultdict(float)
    by_model: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"requests": 0, "cost": 0.0, "tokens": 0})

    for row in usage:
        cost = float(row.get("estimated_cost_usd", 0) or 0)
        inp = int(row.get("prompt_tokens", 0) or 0)
        out = int(row.get("output_tokens", 0) or 0)
        model = str(row.get("model", "unknown") or "unknown").strip()
        ts = str(row.get("ts_utc", "") or "")
        day = ts[:10] if len(ts) >= 10 else "unknown"

        total_cost += cost
        total_input += inp
        total_output += out
        by_day[day] += cost
        by_model[model]["requests"] += 1
        by_model[model]["cost"] += cost
        by_model[model]["tokens"] += inp + out

    sorted_days = sorted(by_day.items())
    return {
        "total_cost_usd": round(total_cost, 6),
        "total_requests": len(usage),
        "total_input_tokens": total_input,
        "total_output_tokens": total_output,
        "avg_cost_per_request": round(total_cost / len(usage), 6) if usage else 0.0,
        "by_day": [{"date": d, "cost": round(c, 6)} for d, c in sorted_days],
        "by_model": {
            m: {"requests": v["requests"], "cost": round(v["cost"], 6), "tokens": v["tokens"]}
            for m, v in by_model.items()
        },
    }


def compute_episode_metrics(states: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not states:
        return {
            "total": 0, "submitted": 0, "labels_applied": 0,
            "labels_ready": 0, "has_error": 0,
            "policy_passed": 0, "policy_failed": 0,
            "submit_rate_pct": 0.0,
        }
    total = len(states)
    submitted = sum(1 for s in states if s.get("episode_submitted"))
    applied = sum(1 for s in states if s.get("labels_applied"))
    ready = sum(1 for s in states if s.get("labels_ready"))
    has_error = sum(1 for s in states if s.get("last_error"))
    policy_ok = sum(1 for s in states if s.get("validation_ok") is True)
    policy_fail = sum(1 for s in states if s.get("validation_ok") is False)
    return {
        "total": total,
        "submitted": submitted,
        "labels_applied": applied,
        "labels_ready": ready,
        "has_error": has_error,
        "policy_passed": policy_ok,
        "policy_failed": policy_fail,
        "submit_rate_pct": round(submitted / total * 100, 1) if total else 0.0,
        "policy_pass_rate_pct": round(policy_ok / (policy_ok + policy_fail) * 100, 1)
            if (policy_ok + policy_fail) > 0 else 0.0,
    }


def compute_dispute_metrics(transitions: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not transitions:
        return {"total_disputes": 0, "by_bucket": {}, "recent": []}
    by_bucket: Dict[str, int] = defaultdict(int)
    for t in transitions:
        bucket = str(t.get("dispute_bucket") or t.get("status") or "unknown")
        by_bucket[bucket] += 1

    recent = sorted(
        transitions,
        key=lambda x: str(x.get("ts_utc") or x.get("timestamp") or ""),
        reverse=True
    )[:10]

    return {
        "total_disputes": len(transitions),
        "by_bucket": dict(by_bucket),
        "recent": recent,
    }


def compute_lesson_metrics(lessons: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "total_lessons": len(lessons),
        "recent": sorted(
            lessons,
            key=lambda x: str(x.get("ts_utc") or x.get("timestamp") or ""),
            reverse=True,
        )[:5],
    }


# ──────────────────────────────────────────────────────────────────────────────
# HTML generation
# ──────────────────────────────────────────────────────────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Atlas Pipeline Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Cairo:wght@400;600;700&display=swap');

  :root {
    --bg: #0d0f14;
    --bg2: #141720;
    --bg3: #1c2030;
    --border: #2a2f42;
    --accent: #00e5c8;
    --accent2: #7c6fff;
    --accent3: #ff6b6b;
    --accent4: #ffd166;
    --text: #e2e8f0;
    --text2: #8892a4;
    --mono: 'IBM Plex Mono', monospace;
    --sans: 'Cairo', sans-serif;
  }

  * { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--sans);
    min-height: 100vh;
    padding: 0 0 60px;
  }

  .topbar {
    background: var(--bg2);
    border-bottom: 1px solid var(--border);
    padding: 18px 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: sticky;
    top: 0;
    z-index: 100;
  }

  .topbar-title {
    font-size: 1.1rem;
    font-weight: 700;
    color: var(--accent);
    letter-spacing: 0.04em;
    font-family: var(--mono);
  }

  .topbar-subtitle { font-size: 0.78rem; color: var(--text2); font-family: var(--mono); }

  .badge {
    background: var(--bg3);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 4px 12px;
    font-size: 0.72rem;
    font-family: var(--mono);
    color: var(--accent);
  }

  main { max-width: 1400px; margin: 0 auto; padding: 32px 24px 0; }

  .section-label {
    font-size: 0.7rem;
    font-family: var(--mono);
    color: var(--text2);
    text-transform: uppercase;
    letter-spacing: 0.12em;
    margin-bottom: 14px;
    padding-bottom: 6px;
    border-bottom: 1px solid var(--border);
  }

  /* KPI Grid */
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
    gap: 14px;
    margin-bottom: 36px;
  }

  .kpi-card {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px 18px 16px;
    position: relative;
    overflow: hidden;
    transition: border-color 0.2s;
  }

  .kpi-card:hover { border-color: var(--accent); }

  .kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: var(--accent-color, var(--accent));
  }

  .kpi-label {
    font-size: 0.7rem;
    color: var(--text2);
    font-family: var(--mono);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 10px;
  }

  .kpi-value {
    font-size: 2rem;
    font-weight: 700;
    font-family: var(--mono);
    color: var(--accent-color, var(--accent));
    line-height: 1;
  }

  .kpi-sub {
    font-size: 0.68rem;
    color: var(--text2);
    margin-top: 6px;
    font-family: var(--mono);
  }

  /* Charts row */
  .charts-row {
    display: grid;
    grid-template-columns: 2fr 1fr;
    gap: 16px;
    margin-bottom: 36px;
  }

  @media (max-width: 900px) {
    .charts-row { grid-template-columns: 1fr; }
  }

  .chart-card {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px;
  }

  .chart-title {
    font-size: 0.78rem;
    font-family: var(--mono);
    color: var(--text2);
    margin-bottom: 16px;
    text-transform: uppercase;
    letter-spacing: 0.08em;
  }

  .chart-wrap { position: relative; height: 200px; }

  /* Two column layout */
  .two-col {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    margin-bottom: 36px;
  }

  @media (max-width: 780px) {
    .two-col { grid-template-columns: 1fr; }
  }

  .panel {
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px;
  }

  .panel-title {
    font-size: 0.75rem;
    font-family: var(--mono);
    color: var(--text2);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 14px;
    padding-bottom: 10px;
    border-bottom: 1px solid var(--border);
  }

  /* Table */
  .data-table { width: 100%; border-collapse: collapse; font-size: 0.78rem; }

  .data-table th {
    text-align: right;
    padding: 8px 10px;
    color: var(--text2);
    font-family: var(--mono);
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    border-bottom: 1px solid var(--border);
  }

  .data-table td {
    padding: 9px 10px;
    border-bottom: 1px solid var(--border);
    color: var(--text);
    font-family: var(--mono);
    font-size: 0.75rem;
  }

  .data-table tr:last-child td { border-bottom: none; }
  .data-table tr:hover td { background: var(--bg3); }

  /* Status dots */
  .dot {
    display: inline-block;
    width: 7px; height: 7px;
    border-radius: 50%;
    margin-left: 6px;
    vertical-align: middle;
  }
  .dot-green { background: #22c55e; box-shadow: 0 0 6px #22c55e80; }
  .dot-red   { background: var(--accent3); box-shadow: 0 0 6px #ff6b6b80; }
  .dot-yellow{ background: var(--accent4); }
  .dot-blue  { background: var(--accent2); }

  /* Progress bar */
  .progress-wrap { background: var(--bg3); border-radius: 4px; height: 6px; overflow: hidden; margin-top: 4px; }
  .progress-fill { height: 100%; border-radius: 4px; background: var(--accent); transition: width 0.6s ease; }

  /* Model pills */
  .pill {
    display: inline-block;
    background: var(--bg3);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 2px 8px;
    font-family: var(--mono);
    font-size: 0.68rem;
    color: var(--accent2);
  }

  /* Lesson card */
  .lesson-card {
    background: var(--bg3);
    border: 1px solid var(--border);
    border-right: 3px solid var(--accent2);
    border-radius: 6px;
    padding: 12px 14px;
    margin-bottom: 10px;
    font-size: 0.76rem;
    line-height: 1.5;
  }

  .lesson-ts {
    font-family: var(--mono);
    font-size: 0.65rem;
    color: var(--text2);
    margin-top: 6px;
  }

  /* No data */
  .no-data {
    color: var(--text2);
    font-family: var(--mono);
    font-size: 0.76rem;
    text-align: center;
    padding: 30px;
    opacity: 0.6;
  }

  /* Footer */
  .footer {
    margin-top: 50px;
    padding: 18px 32px;
    border-top: 1px solid var(--border);
    font-family: var(--mono);
    font-size: 0.65rem;
    color: var(--text2);
    text-align: center;
  }
</style>
</head>
<body>

<div class="topbar">
  <div>
    <div class="topbar-title">▸ Atlas Pipeline Dashboard</div>
    <div class="topbar-subtitle">OCR_Annotation_Atlas · aymank2020</div>
  </div>
  <div class="badge">Generated: __GENERATED_AT__</div>
</div>

<main>

  <!-- KPIs -->
  <div class="section-label">مقاييس التشغيل — Key Metrics</div>
  <div class="kpi-grid" id="kpi-grid"></div>

  <!-- Cost chart + Model breakdown -->
  <div class="section-label">التكلفة والموارد — Cost & Resources</div>
  <div class="charts-row">
    <div class="chart-card">
      <div class="chart-title">التكلفة اليومية (USD)</div>
      <div class="chart-wrap"><canvas id="costChart"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-title">توزيع النماذج</div>
      <div class="chart-wrap"><canvas id="modelChart"></canvas></div>
    </div>
  </div>

  <!-- Episode status + Policy gate -->
  <div class="section-label">Episodes & Policy Gate</div>
  <div class="two-col">

    <div class="panel">
      <div class="panel-title">حالة الـ Episodes</div>
      <div id="episode-panel"></div>
    </div>

    <div class="panel">
      <div class="panel-title">Model Usage by Request</div>
      <div id="model-table-panel"></div>
    </div>

  </div>

  <!-- Disputes + Lessons -->
  <div class="section-label">التعلم المستمر — Continuous Learning</div>
  <div class="two-col">

    <div class="panel">
      <div class="panel-title">Disputes / T4 Transitions <span id="dispute-count" class="pill"></span></div>
      <div id="dispute-panel"></div>
    </div>

    <div class="panel">
      <div class="panel-title">آخر الـ Lessons</div>
      <div id="lessons-panel"></div>
    </div>

  </div>

</main>

<div class="footer">
  Atlas Pipeline Dashboard · Auto-generated · Data from outputs/ directory
</div>

<script>
const DATA = __JSON_DATA__;

// ── KPIs ──────────────────────────────────────────────────────────────────────
const kpis = [
  {
    label: "Total Cost (USD)",
    value: "$" + DATA.cost.total_cost_usd.toFixed(4),
    sub: DATA.cost.total_requests + " requests",
    color: "#00e5c8"
  },
  {
    label: "Cost / Request",
    value: "$" + DATA.cost.avg_cost_per_request.toFixed(6),
    sub: "avg per API call",
    color: "#00e5c8"
  },
  {
    label: "Input Tokens",
    value: (DATA.cost.total_input_tokens / 1000).toFixed(1) + "K",
    sub: "prompt tokens total",
    color: "#7c6fff"
  },
  {
    label: "Output Tokens",
    value: (DATA.cost.total_output_tokens / 1000).toFixed(1) + "K",
    sub: "candidate tokens total",
    color: "#7c6fff"
  },
  {
    label: "Episodes",
    value: DATA.episodes.total,
    sub: DATA.episodes.submitted + " submitted",
    color: "#ffd166"
  },
  {
    label: "Submit Rate",
    value: DATA.episodes.submit_rate_pct + "%",
    sub: DATA.episodes.submitted + "/" + DATA.episodes.total,
    color: "#22c55e"
  },
  {
    label: "Policy Pass Rate",
    value: DATA.episodes.policy_pass_rate_pct + "%",
    sub: DATA.episodes.policy_passed + " passed / " + DATA.episodes.policy_failed + " failed",
    color: "#22c55e"
  },
  {
    label: "Disputes",
    value: DATA.disputes.total_disputes,
    sub: "T4 transitions captured",
    color: "#ff6b6b"
  },
];

const grid = document.getElementById("kpi-grid");
kpis.forEach(k => {
  const card = document.createElement("div");
  card.className = "kpi-card";
  card.style.setProperty("--accent-color", k.color);
  card.innerHTML = `
    <div class="kpi-label">${k.label}</div>
    <div class="kpi-value">${k.value}</div>
    <div class="kpi-sub">${k.sub}</div>
  `;
  grid.appendChild(card);
});

// ── Cost chart ────────────────────────────────────────────────────────────────
const dayData = DATA.cost.by_day;
if (dayData.length > 0) {
  const ctx = document.getElementById("costChart").getContext("2d");
  new Chart(ctx, {
    type: "bar",
    data: {
      labels: dayData.map(d => d.date),
      datasets: [{
        label: "Cost USD",
        data: dayData.map(d => d.cost),
        backgroundColor: "#00e5c830",
        borderColor: "#00e5c8",
        borderWidth: 1.5,
        borderRadius: 4,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: "#8892a4", font: { family: "IBM Plex Mono", size: 10 } }, grid: { color: "#2a2f42" } },
        y: { ticks: { color: "#8892a4", font: { family: "IBM Plex Mono", size: 10 } }, grid: { color: "#2a2f42" } }
      }
    }
  });
} else {
  document.getElementById("costChart").parentElement.innerHTML = '<div class="no-data">لا توجد بيانات تكلفة بعد</div>';
}

// ── Model pie ─────────────────────────────────────────────────────────────────
const modelData = DATA.cost.by_model;
const modelKeys = Object.keys(modelData);
if (modelKeys.length > 0) {
  const ctx2 = document.getElementById("modelChart").getContext("2d");
  const palette = ["#00e5c8","#7c6fff","#ffd166","#ff6b6b","#22c55e","#38bdf8"];
  new Chart(ctx2, {
    type: "doughnut",
    data: {
      labels: modelKeys.map(k => k.replace("gemini-","g-")),
      datasets: [{
        data: modelKeys.map(k => modelData[k].requests),
        backgroundColor: palette.slice(0, modelKeys.length),
        borderColor: "#141720",
        borderWidth: 2,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: {
          position: "bottom",
          labels: { color: "#8892a4", font: { family: "IBM Plex Mono", size: 10 }, padding: 10 }
        }
      }
    }
  });
} else {
  document.getElementById("modelChart").parentElement.innerHTML = '<div class="no-data">لا توجد بيانات</div>';
}

// ── Episode panel ─────────────────────────────────────────────────────────────
const ep = DATA.episodes;
const epPanel = document.getElementById("episode-panel");
const epRows = [
  ["Submitted",      ep.submitted,      "dot-green"],
  ["Labels Applied", ep.labels_applied, "dot-green"],
  ["Labels Ready",   ep.labels_ready,   "dot-blue"],
  ["Policy Passed",  ep.policy_passed,  "dot-green"],
  ["Policy Failed",  ep.policy_failed,  "dot-red"],
  ["Has Error",      ep.has_error,      "dot-red"],
];
epPanel.innerHTML = epRows.map(([label, val, dot]) => `
  <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border)">
    <span style="font-family:var(--mono);font-size:0.75rem;color:var(--text2)">${label}</span>
    <span style="font-family:var(--mono);font-size:0.8rem">
      <span class="dot ${dot}"></span>${val}
    </span>
  </div>
`).join("") + `
  <div style="margin-top:16px">
    <div style="font-family:var(--mono);font-size:0.68rem;color:var(--text2);margin-bottom:6px">Submit Rate: ${ep.submit_rate_pct}%</div>
    <div class="progress-wrap"><div class="progress-fill" style="width:${ep.submit_rate_pct}%"></div></div>
    <div style="font-family:var(--mono);font-size:0.68rem;color:var(--text2);margin-top:10px;margin-bottom:6px">Policy Pass: ${ep.policy_pass_rate_pct}%</div>
    <div class="progress-wrap"><div class="progress-fill" style="width:${ep.policy_pass_rate_pct}%;background:var(--accent4)"></div></div>
  </div>
`;

// ── Model table ───────────────────────────────────────────────────────────────
const mtPanel = document.getElementById("model-table-panel");
if (modelKeys.length > 0) {
  mtPanel.innerHTML = `
    <table class="data-table">
      <thead><tr>
        <th>Model</th><th>Requests</th><th>Cost $</th><th>Tokens</th>
      </tr></thead>
      <tbody>
        ${modelKeys.map(k => `<tr>
          <td><span class="pill">${k.replace("gemini-","g-")}</span></td>
          <td>${modelData[k].requests}</td>
          <td>${modelData[k].cost.toFixed(6)}</td>
          <td>${(modelData[k].tokens/1000).toFixed(1)}K</td>
        </tr>`).join("")}
      </tbody>
    </table>
  `;
} else {
  mtPanel.innerHTML = '<div class="no-data">لا توجد بيانات</div>';
}

// ── Disputes ──────────────────────────────────────────────────────────────────
document.getElementById("dispute-count").textContent = DATA.disputes.total_disputes;
const dispPanel = document.getElementById("dispute-panel");
if (DATA.disputes.recent.length > 0) {
  const bucketColors = { disputed: "#ff6b6b", awaiting_t2: "#ffd166", both_ok: "#22c55e" };
  dispPanel.innerHTML = `
    <table class="data-table">
      <thead><tr><th>Episode</th><th>Status</th><th>Date</th></tr></thead>
      <tbody>
        ${DATA.disputes.recent.map(t => {
          const bucket = t.dispute_bucket || t.status || "?";
          const col = bucketColors[bucket] || "#8892a4";
          const ts = (t.ts_utc || t.timestamp || "").substring(0,10);
          const eid = t.episode_id || t._file || "—";
          return `<tr>
            <td style="font-size:0.68rem">${eid.substring(0,16)}</td>
            <td><span style="color:${col};font-family:var(--mono);font-size:0.68rem">${bucket}</span></td>
            <td style="font-size:0.68rem">${ts}</td>
          </tr>`;
        }).join("")}
      </tbody>
    </table>
  `;
} else {
  dispPanel.innerHTML = '<div class="no-data">لا توجد disputes بعد</div>';
}

// ── Lessons ───────────────────────────────────────────────────────────────────
const lessPanel = document.getElementById("lessons-panel");
if (DATA.lessons.recent.length > 0) {
  lessPanel.innerHTML = DATA.lessons.recent.map(l => {
    const text = l.lesson_text || l.lesson || l.summary || JSON.stringify(l).substring(0,200);
    const ts = (l.ts_utc || l.timestamp || "").substring(0,16);
    return `
      <div class="lesson-card">
        ${text.substring(0,300)}${text.length > 300 ? "…" : ""}
        <div class="lesson-ts">${ts}</div>
      </div>
    `;
  }).join("");
} else {
  lessPanel.innerHTML = '<div class="no-data">لا توجد lessons بعد</div>';
}
</script>
</body>
</html>
"""


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def generate_dashboard(outputs_dir: Path, open_browser: bool = False) -> Path:
    print(f"[dashboard] reading outputs from: {outputs_dir}")

    usage = load_usage(outputs_dir)
    states = load_task_states(outputs_dir)
    transitions = load_transitions(outputs_dir)
    lessons = load_lessons(outputs_dir)

    print(f"[dashboard] usage_rows={len(usage)} task_states={len(states)} "
          f"transitions={len(transitions)} lessons={len(lessons)}")

    data = {
        "cost": compute_cost_metrics(usage),
        "episodes": compute_episode_metrics(states),
        "disputes": compute_dispute_metrics(transitions),
        "lessons": compute_lesson_metrics(lessons),
    }

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    html = HTML_TEMPLATE.replace("__JSON_DATA__", json.dumps(data, ensure_ascii=False))
    html = html.replace("__GENERATED_AT__", generated_at)

    out_path = outputs_dir / "atlas_dashboard.html"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"[dashboard] ✓ saved: {out_path}")

    if open_browser:
        webbrowser.open(out_path.as_uri())

    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Atlas Pipeline Dashboard")
    parser.add_argument("--outputs-dir", default="outputs", help="Path to outputs directory")
    parser.add_argument("--open", action="store_true", help="Open dashboard in browser after generating")
    args = parser.parse_args()
    generate_dashboard(Path(args.outputs_dir), open_browser=args.open)


if __name__ == "__main__":
    main()
