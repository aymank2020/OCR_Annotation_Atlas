"""
Generate an interactive HTML viewer for episode re-audit results.

Input:
  - episodes_review_index.json (from atlas_review_builder.py)

Output:
  - atlas_review_viewer.html (single self-contained page with embedded data)
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _build_html(data: Dict[str, Any], title: str) -> str:
    data_json = json.dumps(data, ensure_ascii=False)
    title_esc = title.replace("<", "&lt;").replace(">", "&gt;")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title_esc}</title>
  <style>
    :root {{
      --bg: #0b1020;
      --panel: #131a2e;
      --panel2: #0f1528;
      --border: #26314f;
      --text: #e8edf9;
      --muted: #9cabcf;
      --accent: #67b3ff;
      --ok: #16c47f;
      --warn: #f8c146;
      --bad: #ff6b6b;
      --unknown: #8893b3;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Tahoma, Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}
    .top {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 14px 18px;
      border-bottom: 1px solid var(--border);
      background: linear-gradient(180deg, #101833, #0f1528);
      position: sticky;
      top: 0;
      z-index: 10;
    }}
    .title {{
      font-size: 16px;
      font-weight: 700;
    }}
    .title small {{
      color: var(--muted);
      font-weight: 500;
      margin-left: 8px;
    }}
    .layout {{
      display: grid;
      grid-template-columns: 340px 1fr;
      gap: 14px;
      padding: 14px;
      min-height: calc(100vh - 56px);
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 12px;
    }}
    .left {{
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }}
    .controls {{
      padding: 12px;
      border-bottom: 1px solid var(--border);
      display: grid;
      gap: 8px;
      background: var(--panel2);
    }}
    input, select {{
      width: 100%;
      padding: 8px 10px;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: #0a1022;
      color: var(--text);
    }}
    .list {{
      overflow: auto;
      padding: 8px;
      display: grid;
      gap: 8px;
    }}
    .ep {{
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px;
      background: #0f1730;
      cursor: pointer;
    }}
    .ep:hover {{ border-color: #36528a; }}
    .ep.active {{
      border-color: var(--accent);
      background: #152346;
    }}
    .eid {{
      font-family: Consolas, "Courier New", monospace;
      font-size: 12px;
      color: var(--text);
      margin-bottom: 6px;
    }}
    .meta {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 12px;
      color: var(--muted);
      gap: 8px;
    }}
    .badge {{
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 700;
      white-space: nowrap;
      border: 1px solid transparent;
    }}
    .main {{
      padding: 12px;
      display: grid;
      gap: 12px;
      align-content: start;
    }}
    .section {{
      padding: 12px;
    }}
    .section h3 {{
      margin: 0 0 10px 0;
      font-size: 14px;
      color: var(--accent);
    }}
    .grid2 {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }}
    .labelbox {{
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px;
      background: #0d1429;
    }}
    .labelbox .tt {{
      margin: 0 0 8px 0;
      font-weight: 700;
      font-size: 13px;
      color: #c8d7ff;
    }}
    pre {{
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: Consolas, "Courier New", monospace;
      font-size: 12px;
      line-height: 1.5;
      color: #dce6ff;
      max-height: 300px;
      overflow: auto;
    }}
    video {{
      width: 100%;
      max-height: 320px;
      border-radius: 10px;
      background: #000;
      border: 1px solid var(--border);
    }}
    a {{
      color: var(--accent);
      text-decoration: none;
    }}
    a:hover {{ text-decoration: underline; }}
    .stats {{
      font-size: 12px;
      color: var(--muted);
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
    }}
    .empty {{
      color: var(--muted);
      font-size: 13px;
      padding: 12px;
      border: 1px dashed var(--border);
      border-radius: 10px;
      background: #0d1429;
    }}
    .warn {{
      color: var(--warn);
      font-size: 12px;
      margin-top: 8px;
    }}
    @media (max-width: 1000px) {{
      .layout {{ grid-template-columns: 1fr; }}
      .grid2 {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="top">
    <div class="title">
      Atlas Episode Review Viewer | عارض مراجعة الحلقات
      <small id="metaInfo"></small>
    </div>
    <div class="stats" id="topStats"></div>
  </div>

  <div class="layout">
    <div class="card left">
      <div class="controls">
        <input id="search" placeholder="Search episode id | ابحث برقم الحلقة" />
        <select id="statusFilter"></select>
      </div>
      <div id="episodeList" class="list"></div>
    </div>

    <div class="card main">
      <div class="section">
        <h3>Episode | الحلقة</h3>
        <div id="episodeInfo" class="empty">Select an episode from the left list | اختر حلقة من القائمة</div>
      </div>

      <div class="section">
        <h3>Video | الفيديو</h3>
        <video id="videoPlayer" controls preload="metadata"></video>
        <div id="videoNote" class="warn"></div>
      </div>

      <div class="section grid2">
        <div class="labelbox">
          <div class="tt">Tier2 (Before) | قبل التعديل</div>
          <pre id="tier2Box">-</pre>
        </div>
        <div class="labelbox">
          <div class="tt">Tier3 (After) | بعد التعديل</div>
          <pre id="tier3Box">-</pre>
        </div>
      </div>

      <div class="section grid2">
        <div class="labelbox">
          <div class="tt">Validation | التحقق</div>
          <pre id="validationBox">-</pre>
        </div>
        <div class="labelbox">
          <div class="tt">Disputes (sample) | النزاعات (عينة)</div>
          <pre id="disputesBox">-</pre>
        </div>
      </div>
    </div>
  </div>

  <script>
    const DATA = {data_json};
    const episodes = Array.isArray(DATA.episodes) ? DATA.episodes : [];
    const statusCounts = episodes.reduce((acc, e) => {{
      const s = (e.review_status || "unknown");
      acc[s] = (acc[s] || 0) + 1;
      return acc;
    }}, {{}});

    const COLORS = {{
      submitted: ["#0f2a20", "#16c47f"],
      disputed: ["#341717", "#ff6b6b"],
      policy_fail: ["#38280e", "#f8c146"],
      error: ["#2f1e2b", "#e879f9"],
      labeled_not_submitted: ["#13253a", "#67b3ff"],
      unknown: ["#222a42", "#8893b3"]
    }};

    const metaInfo = document.getElementById("metaInfo");
    const topStats = document.getElementById("topStats");
    const listEl = document.getElementById("episodeList");
    const searchEl = document.getElementById("search");
    const statusEl = document.getElementById("statusFilter");
    const episodeInfo = document.getElementById("episodeInfo");
    const video = document.getElementById("videoPlayer");
    const videoNote = document.getElementById("videoNote");
    const tier2Box = document.getElementById("tier2Box");
    const tier3Box = document.getElementById("tier3Box");
    const validationBox = document.getElementById("validationBox");
    const disputesBox = document.getElementById("disputesBox");

    metaInfo.textContent = `generated: ${{DATA.generated_at_utc || "n/a"}}`;
    topStats.innerHTML = `
      <span>Total | الإجمالي: <b>${{episodes.length}}</b></span>
      <span>Statuses | الحالات: <b>${{Object.keys(statusCounts).length}}</b></span>
    `;

    function statusBadge(status) {{
      const s = status || "unknown";
      const c = COLORS[s] || COLORS.unknown;
      return `<span class="badge" style="background:${{c[0]}}; color:${{c[1]}}; border-color:${{c[1]}}33">${{s}}</span>`;
    }}

    function normalizePath(p) {{
      if (!p) return "";
      let s = String(p).replace(/\\\\/g, "/");
      if (/^https?:\\/\\//i.test(s)) return s;
      const low = s.toLowerCase();
      const i1 = low.lastIndexOf("/outputs/");
      if (i1 >= 0) s = s.slice(i1 + "/outputs/".length);
      const i2 = low.lastIndexOf("outputs/");
      if (i1 < 0 && i2 >= 0) s = s.slice(i2 + "outputs/".length);
      if (/^[a-zA-Z]:\\//.test(s)) return "";
      return encodeURI(s);
    }}

    function toText(v) {{
      if (!v) return "(missing)";
      if (typeof v === "string") return v;
      if (Array.isArray(v)) {{
        return v.map((s) => {{
          if (typeof s === "object" && s) {{
            const a = s.start_sec ?? s.start ?? "";
            const b = s.end_sec ?? s.end ?? "";
            return `${{a}} -> ${{b}} | ${{s.label || ""}}`;
          }}
          return String(s);
        }}).join("\\n");
      }}
      if (typeof v === "object") {{
        if (Array.isArray(v.segments)) {{
          return toText(v.segments);
        }}
        return JSON.stringify(v, null, 2);
      }}
      return String(v);
    }}

    function renderList() {{
      const q = (searchEl.value || "").trim().toLowerCase();
      const sf = statusEl.value;
      const filtered = episodes.filter((ep) => {{
        const okStatus = sf === "all" ? true : (ep.review_status || "unknown") === sf;
        const okSearch = !q ? true : String(ep.episode_id || "").toLowerCase().includes(q);
        return okStatus && okSearch;
      }});

      listEl.innerHTML = "";
      if (!filtered.length) {{
        listEl.innerHTML = `<div class="empty">No episodes match filter | لا توجد حلقات مطابقة</div>`;
        return;
      }}
      filtered.forEach((ep, idx) => {{
        const row = document.createElement("div");
        row.className = "ep";
        row.dataset.eid = ep.episode_id || "";
        row.innerHTML = `
          <div class="eid">${{ep.episode_id || "unknown"}}</div>
          <div class="meta">
            ${{statusBadge(ep.review_status || "unknown")}}
            <span>$${{Number(ep.total_cost_usd || 0).toFixed(4)}} | disputes: ${{ep.disputes_count || 0}}</span>
          </div>
        `;
        row.addEventListener("click", () => selectEpisode(ep, row));
        listEl.appendChild(row);
        if (idx === 0 && !document.querySelector(".ep.active")) {{
          selectEpisode(ep, row);
        }}
      }});
    }}

    function selectEpisode(ep, row) {{
      document.querySelectorAll(".ep.active").forEach((x) => x.classList.remove("active"));
      if (row) row.classList.add("active");

      const eid = ep.episode_id || "unknown";
      const status = ep.review_status || "unknown";
      const cost = Number(ep.total_cost_usd || 0).toFixed(6);
      const atlasUrl = ep.atlas_url || "";
      const hasVideo = Boolean(ep.video_path || ep.video_web_path);
      const src = normalizePath(ep.video_web_path || ep.video_path || "");

      episodeInfo.innerHTML = `
        <div class="stats">
          <span><b>ID:</b> ${{eid}}</span>
          <span><b>Status | الحالة:</b> ${{statusBadge(status)}}</span>
          <span><b>Cost | التكلفة:</b> $${{cost}}</span>
          <span><b>Disputes | النزاعات:</b> ${{ep.disputes_count || 0}}</span>
          <span><a href="${{atlasUrl}}" target="_blank">Open Atlas | فتح أطلس</a></span>
        </div>
      `;

      videoNote.textContent = "";
      if (src) {{
        video.src = src;
      }} else {{
        video.removeAttribute("src");
        video.load();
        videoNote.textContent = hasVideo
          ? "Video path exists but not web-accessible. Serve this page from outputs/ root and keep relative video paths."
          : "No video found for this episode | لا يوجد فيديو لهذه الحلقة.";
      }}

      tier2Box.textContent = toText(ep.tier2_text || ep.tier2);
      tier3Box.textContent = toText(ep.tier3_text || ep.tier3);
      validationBox.textContent = ep.validation ? JSON.stringify(ep.validation, null, 2) : "(missing)";
      const disputes = Array.isArray(ep.disputes) ? ep.disputes.slice(0, 5) : [];
      disputesBox.textContent = disputes.length ? JSON.stringify(disputes, null, 2) : "(none)";
    }}

    function initFilter() {{
      const keys = Object.keys(statusCounts).sort();
      statusEl.innerHTML = `<option value="all">All statuses | كل الحالات (${{episodes.length}})</option>` +
        keys.map((k) => `<option value="${{k}}">${{k}} (${{statusCounts[k]}})</option>`).join("");
    }}

    initFilter();
    searchEl.addEventListener("input", renderList);
    statusEl.addEventListener("change", renderList);
    renderList();
  </script>
</body>
</html>
"""


def generate_viewer(index_path: Path, out_path: Path, title: str) -> Path:
    payload = _load_json(index_path)
    html = _build_html(payload, title=title)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate episodes review HTML viewer.")
    parser.add_argument(
        "--index",
        default="outputs/episodes_review_index.json",
        help="Path to episodes_review_index.json",
    )
    parser.add_argument(
        "--out",
        default="outputs/atlas_review_viewer.html",
        help="Output HTML path",
    )
    parser.add_argument(
        "--title",
        default="Atlas Episode Review Viewer",
        help="Page title",
    )
    args = parser.parse_args()

    out = generate_viewer(Path(args.index).resolve(), Path(args.out).resolve(), title=args.title)
    print(f"[review-viewer] saved: {out}")


if __name__ == "__main__":
    main()
