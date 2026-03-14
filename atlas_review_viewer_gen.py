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
    .episode-select {{
      width: 100%;
      min-height: 44px;
      font-family: Consolas, "Courier New", monospace;
      font-size: 12px;
      background: #0a1022;
      color: var(--text);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 8px 10px;
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
    .grid3 {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }}
    .grid4 {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }}
    .labelbox {{
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px;
      background: #0d1429;
    }}
    .labels-row .labelbox {{
      display: flex;
      flex-direction: column;
      min-height: 460px;
    }}
    .labels-row pre {{
      flex: 1;
    }}
    .equal-row .labelbox {{
      display: flex;
      flex-direction: column;
      min-height: 280px;
    }}
    .equal-row pre {{
      flex: 1;
      max-height: none;
    }}
    .labelbox .tt {{
      margin: 0 0 8px 0;
      font-weight: 700;
      font-size: 13px;
      color: #c8d7ff;
    }}
    .timeline {{
      display: grid;
      gap: 6px;
      margin-bottom: 10px;
      max-height: 220px;
      overflow: auto;
      padding-right: 4px;
    }}
    .segrow {{
      border: 1px solid var(--border);
      border-radius: 8px;
      background: #101a33;
      padding: 6px;
      display: grid;
      gap: 4px;
    }}
    .segbtn {{
      text-align: left;
      border: 1px solid #35538a;
      border-radius: 6px;
      background: #132348;
      color: #bcd6ff;
      padding: 5px 8px;
      font-family: Consolas, "Courier New", monospace;
      font-size: 12px;
      cursor: pointer;
    }}
    .segbtn:hover {{ background: #1a315f; }}
    .seglabel {{
      font-size: 12px;
      color: #dce6ff;
      line-height: 1.45;
      word-break: break-word;
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
    .btn {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border: 1px solid #35538a;
      border-radius: 8px;
      background: #132348;
      color: #bcd6ff;
      font-size: 12px;
      cursor: pointer;
    }}
    .btn:hover {{
      background: #1a315f;
    }}
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
    textarea {{
      width: 100%;
      min-height: 140px;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: #0a1022;
      color: var(--text);
      padding: 10px;
      resize: vertical;
      font-family: Consolas, "Courier New", monospace;
      font-size: 12px;
      line-height: 1.5;
    }}
    .score-badge {{
      padding: 3px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      border: 1px solid transparent;
      display: inline-flex;
      align-items: center;
      min-width: 110px;
      justify-content: center;
    }}
    .score-green {{ background: #0f2a20; color: #16c47f; border-color: #16c47f55; }}
    .score-red {{ background: #341717; color: #ff6b6b; border-color: #ff6b6b55; }}
    .score-yellow {{ background: #38280e; color: #f8c146; border-color: #f8c14655; }}
    .score-gray {{ background: #222a42; color: #8893b3; border-color: #8893b355; }}
    .compare-summary {{
      gap: 10px;
      margin-bottom: 10px;
    }}
    .compare-summary b {{
      color: #dce6ff;
    }}
    @media (max-width: 1000px) {{
      .layout {{ grid-template-columns: 1fr; }}
      .grid2 {{ grid-template-columns: 1fr; }}
      .grid3 {{ grid-template-columns: 1fr; }}
      .grid4 {{ grid-template-columns: 1fr; }}
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
        <select id="episodeSelect" class="episode-select"></select>
      </div>
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

      <div class="section grid4 labels-row">
        <div class="labelbox">
          <div class="tt">Gemini Chat (Timed) | إجابة جيمناي شات بالتوقيت</div>
          <div id="chatTimeline" class="timeline"></div>
          <pre id="chatBox">-</pre>
        </div>
        <div class="labelbox">
          <div class="tt">Vertex Chat | إجابة شات Vertex</div>
          <div id="vertexChatTimeline" class="timeline"></div>
          <pre id="vertexChatBox">-</pre>
        </div>
        <div class="labelbox">
          <div class="tt">Tier2 (Before) | قبل التعديل</div>
          <div id="tier2Timeline" class="timeline"></div>
          <pre id="tier2Box">-</pre>
        </div>
        <div class="labelbox">
          <div class="tt">Tier3 (After) | بعد التعديل</div>
          <div id="tier3Timeline" class="timeline"></div>
          <pre id="tier3Box">-</pre>
        </div>
      </div>

      <div class="section grid2 equal-row">
        <div class="labelbox">
          <div class="tt">Validation | التحقق</div>
          <pre id="validationBox">-</pre>
        </div>
        <div class="labelbox">
          <div class="tt">Disputes (sample) | النزاعات (عينة)</div>
          <pre id="disputesBox">-</pre>
        </div>
      </div>

      <div class="section">
        <h3>Comparison Result | نتيجة المقارنة</h3>
        <div class="labelbox">
          <div id="tripletCompareSummary" class="stats compare-summary"></div>
          <div class="stats" style="margin-bottom:8px">
            <span><a id="openGeminiChatLink" href="https://gemini.google.com/app/b3006ba9f325b55c" target="_blank">Open Gemini Chat</a></span>
            <span><button id="geminiEvalSaveBtn" class="btn" type="button">Save Evaluate</button></span>
            <span><button id="geminiEvalClearBtn" class="btn" type="button">Clear</button></span>
            <span><button id="geminiEvalExportBtn" class="btn" type="button">Export JSON</button></span>
            <span><button id="geminiEvalImportBtn" class="btn" type="button">Import JSON</button></span>
            <input id="geminiEvalImportFile" type="file" accept=".json" style="display:none" />
          </div>
          <textarea id="geminiEvalText" placeholder="Paste Gemini chat evaluation here | الصق نتيجة Gemini هنا"></textarea>
          <div class="stats" style="margin-top:8px">
            <span>
              <input id="geminiEvalScoreInput" type="number" min="0" max="100" step="1" style="width:90px" placeholder="Score %" />
            </span>
            <span><button id="geminiEvalAutoScoreBtn" class="btn" type="button">Auto Score</button></span>
            <span id="geminiEvalScoreBadge" class="score-badge score-gray">N/A</span>
            <span id="geminiEvalSavedAt" style="color:var(--muted)"></span>
          </div>
          <pre id="tripletCompareBox">-</pre>
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
    const episodeSelectEl = document.getElementById("episodeSelect");
    const searchEl = document.getElementById("search");
    const statusEl = document.getElementById("statusFilter");
    const episodeInfo = document.getElementById("episodeInfo");
    const video = document.getElementById("videoPlayer");
    const videoNote = document.getElementById("videoNote");
    const tier2Box = document.getElementById("tier2Box");
    const tier3Box = document.getElementById("tier3Box");
    const tier2Timeline = document.getElementById("tier2Timeline");
    const tier3Timeline = document.getElementById("tier3Timeline");
    const chatBox = document.getElementById("chatBox");
    const chatTimeline = document.getElementById("chatTimeline");
    const vertexChatBox = document.getElementById("vertexChatBox");
    const vertexChatTimeline = document.getElementById("vertexChatTimeline");
    const validationBox = document.getElementById("validationBox");
    const disputesBox = document.getElementById("disputesBox");
    const tripletCompareSummary = document.getElementById("tripletCompareSummary");
    const tripletCompareBox = document.getElementById("tripletCompareBox");
    const geminiEvalText = document.getElementById("geminiEvalText");
    const geminiEvalScoreInput = document.getElementById("geminiEvalScoreInput");
    const geminiEvalScoreBadge = document.getElementById("geminiEvalScoreBadge");
    const geminiEvalSavedAt = document.getElementById("geminiEvalSavedAt");
    const geminiEvalSaveBtn = document.getElementById("geminiEvalSaveBtn");
    const geminiEvalClearBtn = document.getElementById("geminiEvalClearBtn");
    const geminiEvalAutoScoreBtn = document.getElementById("geminiEvalAutoScoreBtn");
    const geminiEvalExportBtn = document.getElementById("geminiEvalExportBtn");
    const geminiEvalImportBtn = document.getElementById("geminiEvalImportBtn");
    const geminiEvalImportFile = document.getElementById("geminiEvalImportFile");
    const openGeminiChatLink = document.getElementById("openGeminiChatLink");
    const GEMINI_CHAT_URL = "https://gemini.google.com/app/b3006ba9f325b55c";
    let currentEpisodeId = "";
    let evalStore = {{}};
    let tripletCompareStore = {{}};
    let tripletDetailStore = {{}};
    let textRefCache = {{}};
    let segStopAt = null;

    function loadLocalEvalStore() {{
      try {{
        const raw = localStorage.getItem("atlas_gemini_eval_store_v1");
        if (!raw) return {{}};
        const obj = JSON.parse(raw);
        return obj && typeof obj === "object" ? obj : {{}};
      }} catch (_) {{
        return {{}};
      }}
    }}

    function saveLocalEvalStore() {{
      try {{
        localStorage.setItem("atlas_gemini_eval_store_v1", JSON.stringify(evalStore));
      }} catch (_) {{}}
    }}

    function mergeEvalStores(base, incoming) {{
      const out = Object.assign({{}}, base || {{}});
      Object.entries(incoming || {{}}).forEach(([eid, rec]) => {{
        if (!rec || typeof rec !== "object") return;
        const oldRec = out[eid];
        if (!oldRec) {{
          out[eid] = rec;
          return;
        }}
        const oldTs = String(oldRec.updated_at_utc || "");
        const newTs = String(rec.updated_at_utc || "");
        out[eid] = newTs >= oldTs ? rec : oldRec;
      }});
      return out;
    }}

    async function loadExternalEvalStore() {{
      try {{
        const res = await fetch("gemini_chat_evaluations.json", {{ cache: "no-cache" }});
        if (!res.ok) return;
        const data = await res.json();
        let incoming = {{}};
        if (Array.isArray(data)) {{
          data.forEach((rec) => {{
            if (!rec || typeof rec !== "object") return;
            const eid = String(rec.episode_id || "");
            if (!eid) return;
            incoming[eid] = rec;
          }});
        }} else if (data && typeof data === "object") {{
          if (Array.isArray(data.evaluations)) {{
            data.evaluations.forEach((rec) => {{
              if (!rec || typeof rec !== "object") return;
              const eid = String(rec.episode_id || "");
              if (!eid) return;
              incoming[eid] = rec;
            }});
          }} else if (data.evaluations && typeof data.evaluations === "object") {{
            incoming = data.evaluations;
          }} else {{
            Object.entries(data).forEach(([eid, rec]) => {{
              if (!rec || typeof rec !== "object") return;
              if (!("text" in rec) && !("score_pct" in rec) && !("episode_id" in rec)) return;
              incoming[String(eid)] = rec;
            }});
          }}
        }}
        evalStore = mergeEvalStores(evalStore, incoming);
        saveLocalEvalStore();
        if (currentEpisodeId) renderEvalForEpisode(currentEpisodeId);
      }} catch (_) {{
        // optional file
      }}
    }}

    async function loadTripletCompareStore() {{
      try {{
        const res = await fetch("triplet_compare_results.jsonl", {{ cache: "no-cache" }});
        if (!res.ok) return;
        const raw = await res.text();
        const map = {{}};
        raw.split(/\\r?\\n/).forEach((line) => {{
          const txt = String(line || "").trim();
          if (!txt) return;
          try {{
            const rec = JSON.parse(txt);
            const eid = String(rec.episode_id || "");
            if (!eid) return;
            map[eid] = rec;
          }} catch (_) {{}}
        }});
        tripletCompareStore = map;
        tripletDetailStore = {{}};
        textRefCache = {{}};
        if (currentEpisodeId) {{
          renderTripletCompareForEpisode(currentEpisodeId);
          renderChatForEpisode(currentEpisodeId);
          renderVertexChatForEpisode(currentEpisodeId);
        }}
      }} catch (_) {{
        // optional file
      }}
    }}

    function scoreToBadge(score) {{
      const n = Number(score);
      if (!Number.isFinite(n)) {{
        return {{ cls: "score-gray", txt: "N/A" }};
      }}
      const v = Math.max(0, Math.min(100, Math.round(n)));
      if (v === 100) return {{ cls: "score-green", txt: "SUCCESS 100%" }};
      if (v === 0) return {{ cls: "score-red", txt: "FAIL 0%" }};
      return {{ cls: "score-yellow", txt: `${{v}}%` }};
    }}

    function applyScoreBadge(score) {{
      const b = scoreToBadge(score);
      geminiEvalScoreBadge.className = `score-badge ${{b.cls}}`;
      geminiEvalScoreBadge.textContent = b.txt;
    }}

    function parseScoreFromText(text) {{
      const t = String(text || "");
      const m = t.match(/(?:score|confidence|accuracy|quality|percent|نسبة|تقييم)\\s*[:=\\-]?\\s*(\\d{{1,3}})\\s*%?/i) || t.match(/(\\d{{1,3}})\\s*%/);
      if (!m) return null;
      const n = Number(m[1]);
      if (!Number.isFinite(n)) return null;
      return Math.max(0, Math.min(100, Math.round(n)));
    }}

    function renderEvalForEpisode(eid) {{
      const rec = evalStore[eid] || {{}};
      geminiEvalText.value = String(rec.text || "");
      if (rec.score_pct === null || rec.score_pct === undefined || rec.score_pct === "") {{
        geminiEvalScoreInput.value = "";
      }} else {{
        geminiEvalScoreInput.value = String(rec.score_pct);
      }}
      applyScoreBadge(rec.score_pct);
      geminiEvalSavedAt.textContent = rec.updated_at_utc ? `saved: ${{rec.updated_at_utc}}` : "";
      renderChatForEpisode(eid);
      renderVertexChatForEpisode(eid);
    }}

    async function renderChatForEpisode(eid) {{
      const safeEid = String(eid || "");
      if (!safeEid || safeEid === "__none__") {{
        chatBox.textContent = "-";
        renderTimeline(chatTimeline, []);
        return;
      }}
      chatBox.textContent = "Loading timed chat labels...";
      renderTimeline(chatTimeline, []);

      const chatRaw = await resolveChatTimedText(safeEid);
      if (safeEid !== currentEpisodeId) return;
      if (!chatRaw) {{
        chatBox.textContent = "(missing timed labels)";
        renderTimeline(chatTimeline, []);
        return;
      }}
      const segs = parseSegments(chatRaw);
      if (!segs.length) {{
        chatBox.textContent = "(missing timed labels)";
        renderTimeline(chatTimeline, []);
        return;
      }}
      chatBox.textContent = chatRaw;
      renderTimeline(chatTimeline, segs);
    }}

    async function renderVertexChatForEpisode(eid) {{
      const safeEid = String(eid || "");
      if (!safeEid || safeEid === "__none__") {{
        vertexChatBox.textContent = "-";
        renderTimeline(vertexChatTimeline, []);
        return;
      }}
      vertexChatBox.textContent = "Loading vertex chat labels...";
      renderTimeline(vertexChatTimeline, []);

      const vertexRaw = await resolveVertexChatText(safeEid);
      if (safeEid !== currentEpisodeId) return;
      if (!vertexRaw) {{
        vertexChatBox.textContent = "(missing vertex chat labels)";
        renderTimeline(vertexChatTimeline, []);
        return;
      }}
      const segs = parseSegments(vertexRaw);
      if (!segs.length) {{
        vertexChatBox.textContent = "(missing vertex chat labels)";
        renderTimeline(vertexChatTimeline, []);
        return;
      }}
      vertexChatBox.textContent = vertexRaw;
      renderTimeline(vertexChatTimeline, segs);
    }}

    async function renderTripletCompareForEpisode(eid) {{
      const safeEid = String(eid || "");
      const rec = tripletCompareStore[safeEid] || null;
      if (!rec) {{
        tripletCompareSummary.innerHTML = `<span>No triplet result loaded for this episode.</span>`;
        tripletCompareBox.textContent = "(missing)";
        return;
      }}
      const detail = await getTripletDetailForEpisode(safeEid);
      if (safeEid !== currentEpisodeId) return;
      const judge = detail && typeof detail === "object" && detail.judge_result && typeof detail.judge_result === "object"
        ? detail.judge_result
        : null;

      const winner = String((judge && judge.winner) || rec.winner || "none");
      const scoreRaw = (judge && judge.scores && typeof judge.scores === "object")
        ? judge.scores[winner]
        : rec.score_pct;
      const score = scoreRaw === null || scoreRaw === undefined || scoreRaw === "" ? "n/a" : String(scoreRaw);
      const reason = String(
        (judge && (judge.best_reason_short || judge.final_recommendation)) ||
        rec.reason ||
        ""
      );
      const submitSafe = String((judge && judge.submit_safe_solution) || rec.submit_safe_solution || "n/a");
      const statusTxt = rec.ok ? "ok" : (rec.skipped ? "skipped" : "error");
      tripletCompareSummary.innerHTML = `
        <span><b>Status:</b> ${{statusTxt}}</span>
        <span><b>Winner:</b> ${{winner}}</span>
        <span><b>Score:</b> ${{score}}</span>
        <span><b>Submit Safe:</b> ${{submitSafe}}</span>
      `;
      if (judge) {{
        tripletCompareBox.textContent = JSON.stringify(judge, null, 2);
        return;
      }}
      const fallbackDetails = {{
        episode_id: rec.episode_id || eid,
        status: statusTxt,
        winner: winner,
        score_pct: rec.score_pct,
        submit_safe_solution: rec.submit_safe_solution,
        reason: reason,
        issues: rec.issues || null,
        result_path: rec.result_path || "",
      }};
      tripletCompareBox.textContent = JSON.stringify(fallbackDetails, null, 2);
    }}

    function saveCurrentEval() {{
      if (!currentEpisodeId) return;
      const txt = String(geminiEvalText.value || "").trim();
      const manual = geminiEvalScoreInput.value === "" ? null : Number(geminiEvalScoreInput.value);
      const score = Number.isFinite(manual) ? Math.max(0, Math.min(100, Math.round(manual))) : parseScoreFromText(txt);
      const now = new Date().toISOString();
      evalStore[currentEpisodeId] = {{
        episode_id: currentEpisodeId,
        text: txt,
        score_pct: score,
        updated_at_utc: now,
        source: "viewer_manual",
      }};
      saveLocalEvalStore();
      renderEvalForEpisode(currentEpisodeId);
    }}

    function clearCurrentEval() {{
      if (!currentEpisodeId) return;
      delete evalStore[currentEpisodeId];
      saveLocalEvalStore();
      renderEvalForEpisode(currentEpisodeId);
    }}

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

    function parseTimeToSec(v) {{
      if (typeof v === "number") return v;
      const s = String(v || "").trim();
      if (!s) return null;
      if (/^\\d+(\\.\\d+)?$/.test(s)) return Number(s);
      const parts = s.split(":").map((x) => Number(x));
      if (parts.some((x) => Number.isNaN(x))) return null;
      if (parts.length === 2) {{
        return parts[0] * 60 + parts[1];
      }}
      if (parts.length === 3) {{
        return parts[0] * 3600 + parts[1] * 60 + parts[2];
      }}
      return null;
    }}

    function secToStamp(sec) {{
      const s = Number(sec || 0);
      const m = Math.floor(s / 60);
      const rem = (s - m * 60).toFixed(1).padStart(4, "0");
      return `${{m}}:${{rem}}`;
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

    function buildChatPrompt(ep) {{
      const tier2Raw = ep.tier2_text || ep.tier2;
      const tier3Raw = ep.tier3_text || ep.tier3;
      const validation = ep.validation || {{}};
      const disputes = Array.isArray(ep.disputes) ? ep.disputes : [];

      const validationSummary = {{
        ok: validation.ok,
        episode_errors: validation.episode_errors || [],
        episode_warnings: validation.episode_warnings || [],
        major_fail_triggers: validation.major_fail_triggers || [],
        device_class_conflicts: validation.device_class_conflicts || [],
      }};

      return [
        "I need a strict Atlas Tier-4 audit for this episode.",
        "",
        `Episode ID: ${{ep.episode_id || ""}}`,
        `Atlas URL: ${{ep.atlas_url || ep.open_url || ""}}`,
        `Current status: ${{ep.review_status || "unknown"}}`,
        "",
        "[Tier2 - before AI update]",
        toText(tier2Raw),
        "",
        "[Tier3 - after AI update]",
        toText(tier3Raw),
        "",
        "[Validator summary]",
        JSON.stringify(validationSummary, null, 2),
        "",
        "[Disputes summary]",
        `Count: ${{disputes.length}}`,
        "Sample:",
        JSON.stringify(disputes.slice(0, 3), null, 2),
        "",
        "Please evaluate:",
        "1) Which is better: Tier2 or Tier3?",
        "2) Is Tier3 submit-safe according to Atlas policy?",
        "3) List exact segment-level issues with rule names.",
        "4) Provide corrected labels for failing segments.",
        "5) Final verdict: PASS / FAIL with confidence.",
        "",
        "مهم: اكتب الإجابة النهائية بالكامل باللغة العربية بشكل واضح ومباشر.",
      ].join("\\n");
    }}

    function buildAiInputBundle(ep) {{
      const eid = String(ep.episode_id || "");
      const evalRec = evalStore[eid] || null;
      return {{
        schema_version: "atlas_ai_input_bundle_v1",
        generated_at_utc: new Date().toISOString(),
        source: "atlas_review_viewer",
        episode: {{
          episode_id: eid,
          review_status: ep.review_status || "unknown",
          atlas_url: ep.atlas_url || ep.open_url || "",
          task_url: ep.task_url || "",
          feedback_url: ep.feedback_url || "",
          disputes_url: ep.disputes_url || "",
          total_cost_usd: Number(ep.total_cost_usd || 0),
          tier2: ep.tier2_text || ep.tier2 || null,
          tier3: ep.tier3_text || ep.tier3 || null,
          validation: ep.validation || null,
          disputes: Array.isArray(ep.disputes) ? ep.disputes : [],
        }},
        ai_evaluate: evalRec,
      }};
    }}

    function buildAiInputPrompt(ep) {{
      const bundle = buildAiInputBundle(ep);
      return [
        "Use this AI input bundle for strict Atlas Tier-4 audit.",
        "اعتمد JSON التالي كمدخل مراجعة شامل للحلقة.",
        "",
        "[AI_INPUT_BUNDLE_JSON]",
        JSON.stringify(bundle, null, 2),
        "",
        "Required output:",
        "1) Compare Tier2 vs Tier3 vs Gemini Chat (Timed) vs Vertex Chat",
        "2) choose the safest winner",
        "3) exact segment-level issues with rule names",
        "4) corrected labels for failing segments",
        "5) final PASS/FAIL + confidence",
        "",
        "اكتب الإجابة النهائية بالعربية وبشكل مباشر.",
      ].join("\\n");
    }}

    function parseSegments(v) {{
      const out = [];
      if (!v) return out;

      if (Array.isArray(v)) {{
        v.forEach((s) => {{
          if (!s || typeof s !== "object") return;
          const a = s.start_sec ?? s.start;
          const b = s.end_sec ?? s.end;
          const aSec = parseTimeToSec(a);
          const bSec = parseTimeToSec(b);
          if (aSec == null || bSec == null) return;
          out.push({{
            start_sec: aSec,
            end_sec: bSec,
            start_text: typeof a === "number" ? secToStamp(aSec) : String(a),
            end_text: typeof b === "number" ? secToStamp(bSec) : String(b),
            label: String(s.label || "").trim(),
          }});
        }});
        return out;
      }}

      if (typeof v === "object" && Array.isArray(v.segments)) {{
        return parseSegments(v.segments);
      }}

      const lines = String(v).split(/\\r?\\n/).map((x) => x.trim()).filter(Boolean);
      lines.forEach((line) => {{
        const tabParts = line.split(/\\t+/).map((x) => x.trim()).filter(Boolean);
        if (tabParts.length >= 3) {{
          // Typical atlas text dump:
          // 1\t0.0\t3.3\tlabel
          // or 0.0\t3.3\tlabel
          const aRaw = tabParts.length >= 4 ? tabParts[1] : tabParts[0];
          const bRaw = tabParts.length >= 4 ? tabParts[2] : tabParts[1];
          const labelRaw = tabParts.length >= 4 ? tabParts.slice(3).join(" ") : tabParts.slice(2).join(" ");
          const aSec = parseTimeToSec(aRaw);
          const bSec = parseTimeToSec(bRaw);
          if (aSec != null && bSec != null) {{
            out.push({{
              start_sec: aSec,
              end_sec: bSec,
              start_text: secToStamp(aSec),
              end_text: secToStamp(bSec),
              label: String(labelRaw || "").trim(),
            }});
            return;
          }}
        }}

        const matches = line.match(/\\d+:\\d+(?:\\.\\d+)?/g);
        if (!matches || matches.length < 2) return;
        const startText = matches[0];
        const endText = matches[1];
        const startSec = parseTimeToSec(startText);
        const endSec = parseTimeToSec(endText);
        if (startSec == null || endSec == null) return;

        const endIdx = line.indexOf(endText);
        let label = endIdx >= 0 ? line.slice(endIdx + endText.length).trim() : "";
        label = label.replace(/^[-–—>|#\\d\\.\\s]+/, "").trim();
        out.push({{
          start_sec: startSec,
          end_sec: endSec,
          start_text: startText,
          end_text: endText,
          label,
        }});
      }});
      return out;
    }}

    async function loadTextFromRef(ref) {{
      const key = normalizePath(ref);
      if (!key) return "";
      if (Object.prototype.hasOwnProperty.call(textRefCache, key)) {{
        return String(textRefCache[key] || "");
      }}
      try {{
        const res = await fetch(key, {{ cache: "no-cache" }});
        if (!res.ok) {{
          textRefCache[key] = "";
          return "";
        }}
        const raw = await res.text();
        let out = raw;
        if (key.toLowerCase().endsWith(".json")) {{
          try {{
            out = toText(JSON.parse(raw));
          }} catch (_) {{
            out = raw;
          }}
        }}
        textRefCache[key] = out;
        return out;
      }} catch (_) {{
        textRefCache[key] = "";
        return "";
      }}
    }}

    async function getTripletDetailForEpisode(eid) {{
      const key = String(eid || "");
      if (!key) return null;
      if (Object.prototype.hasOwnProperty.call(tripletDetailStore, key)) {{
        return tripletDetailStore[key];
      }}
      const row = tripletCompareStore[key];
      if (!row || !row.result_path) {{
        tripletDetailStore[key] = null;
        return null;
      }}
      const path = normalizePath(row.result_path);
      if (!path) {{
        tripletDetailStore[key] = null;
        return null;
      }}
      try {{
        const res = await fetch(path, {{ cache: "no-cache" }});
        if (!res.ok) {{
          tripletDetailStore[key] = null;
          return null;
        }}
        const obj = await res.json();
        tripletDetailStore[key] = obj;
        return obj;
      }} catch (_) {{
        tripletDetailStore[key] = null;
        return null;
      }}
    }}

    async function resolveChatTimedText(eid) {{
      const key = String(eid || "");
      if (!key) return "";

      const detail = await getTripletDetailForEpisode(key);
      const refs = [];
      if (detail && typeof detail === "object" && detail.text_refs && typeof detail.text_refs === "object") {{
        const t = detail.text_refs;
        refs.push(t.resolved_chat_path, t.chat_path, t.labels_path);
      }}
      refs.push(`chat_reviews/${{key}}/text_${{key}}_chat.txt`);
      refs.push(`chat_reviews/${{key}}/labels_${{key}}.json`);

      for (const ref of refs) {{
        const txt = await loadTextFromRef(ref);
        if (!txt) continue;
        const segs = parseSegments(txt);
        if (segs.length > 0) return txt;
      }}
      return "";
    }}

    async function resolveVertexChatText(eid) {{
      const key = String(eid || "");
      if (!key) return "";

      const detail = await getTripletDetailForEpisode(key);
      const refs = [];
      if (detail && typeof detail === "object" && detail.text_refs && typeof detail.text_refs === "object") {{
        const t = detail.text_refs;
        refs.push(t.resolved_vertex_chat_path, t.vertex_chat_path);
      }}
      refs.push(`vertex_chat_reviews/${{key}}/text_${{key}}_vertex_chat.txt`);
      refs.push(`vertex_chat_reviews/${{key}}/labels_${{key}}_vertex_chat.json`);

      for (const ref of refs) {{
        const txt = await loadTextFromRef(ref);
        if (!txt) continue;
        const segs = parseSegments(txt);
        if (segs.length > 0) return txt;
      }}
      return "";
    }}

    function jumpToSegment(startSec, endSec) {{
      if (!Number.isFinite(startSec)) return;
      try {{
        video.currentTime = Math.max(0, startSec);
      }} catch (_) {{}}
      if (Number.isFinite(endSec) && endSec > startSec) {{
        segStopAt = endSec;
      }} else {{
        segStopAt = null;
      }}
      video.play().catch(() => {{}});
    }}

    function renderTimeline(container, segments) {{
      container.innerHTML = "";
      if (!segments.length) {{
        container.innerHTML = `<div class="empty">No parsed timestamps | لا توجد أزمنة قابلة للنقر</div>`;
        return;
      }}
      segments.forEach((s, i) => {{
        const row = document.createElement("div");
        row.className = "segrow";
        row.innerHTML = `
          <button class="segbtn" type="button">${{i + 1}}) ${{s.start_text}} → ${{s.end_text}}</button>
          <div class="seglabel">${{(s.label || "(no label)").replace(/</g, "&lt;").replace(/>/g, "&gt;")}}</div>
        `;
        const btn = row.querySelector(".segbtn");
        btn.addEventListener("click", () => jumpToSegment(s.start_sec, s.end_sec));
        container.appendChild(row);
      }});
    }}

    function clearEpisodeView(msg) {{
      const text = msg || "No episodes match filter | لا توجد حلقات مطابقة";
      episodeInfo.innerHTML = `<div class="empty">${{text}}</div>`;
      video.removeAttribute("src");
      video.load();
      videoNote.textContent = "";
      tier2Box.textContent = "-";
      tier3Box.textContent = "-";
      chatBox.textContent = "-";
      vertexChatBox.textContent = "-";
      validationBox.textContent = "-";
      disputesBox.textContent = "-";
      tripletCompareSummary.innerHTML = `<span>-</span>`;
      tripletCompareBox.textContent = "-";
      renderTimeline(tier2Timeline, []);
      renderTimeline(tier3Timeline, []);
      renderTimeline(chatTimeline, []);
      renderTimeline(vertexChatTimeline, []);
      renderEvalForEpisode("__none__");
    }}

    function getFilteredEpisodes() {{
      const q = (searchEl.value || "").trim().toLowerCase();
      const sf = statusEl.value;
      return episodes.filter((ep) => {{
        const okStatus = sf === "all" ? true : (ep.review_status || "unknown") === sf;
        const okSearch = !q ? true : String(ep.episode_id || "").toLowerCase().includes(q);
        return okStatus && okSearch;
      }});
    }}

    function renderEpisodeSelect() {{
      const filtered = getFilteredEpisodes();
      episodeSelectEl.innerHTML = "";
      if (!filtered.length) {{
        const opt = document.createElement("option");
        opt.value = "";
        opt.textContent = "No episodes match filter | لا توجد حلقات مطابقة";
        episodeSelectEl.appendChild(opt);
        currentEpisodeId = "";
        clearEpisodeView();
        return;
      }}

      filtered.forEach((ep) => {{
        const eid = String(ep.episode_id || "unknown");
        const status = String(ep.review_status || "unknown");
        const disputes = Number(ep.disputes_count || 0);
        const opt = document.createElement("option");
        opt.value = eid;
        opt.textContent = `${{eid}} | ${{status}} | disputes:${{disputes}}`;
        episodeSelectEl.appendChild(opt);
      }});

      const hasCurrent = filtered.some((ep) => String(ep.episode_id || "") === currentEpisodeId);
      const nextId = hasCurrent ? currentEpisodeId : String(filtered[0].episode_id || "");
      episodeSelectEl.value = nextId;
      const nextEpisode = filtered.find((ep) => String(ep.episode_id || "") === nextId);
      if (nextEpisode) selectEpisode(nextEpisode);
    }}

    function selectEpisode(ep) {{
      const eid = ep.episode_id || "unknown";
      currentEpisodeId = String(eid);
      const status = ep.review_status || "unknown";
      const cost = Number(ep.total_cost_usd || 0).toFixed(6);
      const atlasUrl = ep.atlas_url || ep.open_url || "";
      const taskUrl = ep.task_url || "";
      const feedbackUrl = ep.feedback_url || "";
      const disputesUrl = ep.disputes_url || "";
      const chatPromptRel = `chat_reviews/${{eid}}/chat_prompt.txt`;
      const chatMetaRel = `chat_reviews/${{eid}}/episode_meta.json`;
      const hasVideo = Boolean(ep.video_path || ep.video_web_path);
      const src = normalizePath(ep.video_web_path || ep.video_path || "");

      episodeInfo.innerHTML = `
        <div class="stats">
          <span><b>ID:</b> ${{eid}}</span>
          <span><b>Status | الحالة:</b> ${{statusBadge(status)}}</span>
          <span><b>Cost | التكلفة:</b> $${{cost}}</span>
          <span><b>Disputes | النزاعات:</b> ${{ep.disputes_count || 0}}</span>
          <span><a href="${{atlasUrl}}" target="_blank">Open Current | فتح الحالي</a></span>
          <span><a href="${{taskUrl}}" target="_blank">Task</a></span>
          <span><a href="${{feedbackUrl}}" target="_blank">Feedback</a></span>
          <span><a href="${{disputesUrl}}" target="_blank">Disputes</a></span>
          <span><a href="${{chatPromptRel}}" target="_blank">Chat Prompt</a></span>
          <span><a href="${{chatMetaRel}}" target="_blank">Episode Meta</a></span>
          <span><button id="copyPromptBtn" class="btn" type="button">Copy Chat Prompt</button></span>
          <span><button id="copyAiInputBtn" class="btn" type="button">Copy AI Input</button></span>
          <span><button id="exportAiBundleBtn" class="btn" type="button">Export AI Bundle</button></span>
        </div>
      `;
      if (openGeminiChatLink) {{
        openGeminiChatLink.href = GEMINI_CHAT_URL;
      }}
      const copyBtn = document.getElementById("copyPromptBtn");
      const copyAiInputBtn = document.getElementById("copyAiInputBtn");
      const exportAiBundleBtn = document.getElementById("exportAiBundleBtn");
      if (copyBtn) {{
        copyBtn.addEventListener("click", async () => {{
          const prompt = buildChatPrompt(ep);
          try {{
            await navigator.clipboard.writeText(prompt);
            copyBtn.textContent = "Copied";
            setTimeout(() => (copyBtn.textContent = "Copy Chat Prompt"), 1300);
          }} catch (_) {{
            copyBtn.textContent = "Copy failed";
            setTimeout(() => (copyBtn.textContent = "Copy Chat Prompt"), 1300);
          }}
        }});
      }}
      if (copyAiInputBtn) {{
        copyAiInputBtn.addEventListener("click", async () => {{
          const prompt = buildAiInputPrompt(ep);
          try {{
            await navigator.clipboard.writeText(prompt);
            copyAiInputBtn.textContent = "Copied";
            setTimeout(() => (copyAiInputBtn.textContent = "Copy AI Input"), 1300);
          }} catch (_) {{
            copyAiInputBtn.textContent = "Copy failed";
            setTimeout(() => (copyAiInputBtn.textContent = "Copy AI Input"), 1300);
          }}
        }});
      }}
      if (exportAiBundleBtn) {{
        exportAiBundleBtn.addEventListener("click", () => {{
          const bundle = buildAiInputBundle(ep);
          const blob = new Blob([JSON.stringify(bundle, null, 2)], {{ type: "application/json" }});
          const a = document.createElement("a");
          a.href = URL.createObjectURL(blob);
          a.download = `ai_input_bundle_${{eid}}.json`;
          a.click();
          setTimeout(() => URL.revokeObjectURL(a.href), 1000);
        }});
      }}

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

      const tier2Raw = ep.tier2_text || ep.tier2;
      const tier3Raw = ep.tier3_text || ep.tier3;
      tier2Box.textContent = toText(tier2Raw);
      tier3Box.textContent = toText(tier3Raw);
      renderTimeline(tier2Timeline, parseSegments(tier2Raw));
      renderTimeline(tier3Timeline, parseSegments(tier3Raw));
      validationBox.textContent = ep.validation ? JSON.stringify(ep.validation, null, 2) : "(missing)";
      const disputes = Array.isArray(ep.disputes) ? ep.disputes.slice(0, 5) : [];
      disputesBox.textContent = disputes.length ? JSON.stringify(disputes, null, 2) : "(none)";
      renderTripletCompareForEpisode(currentEpisodeId);
      renderEvalForEpisode(currentEpisodeId);
      renderVertexChatForEpisode(currentEpisodeId);
    }}

    function initFilter() {{
      const keys = Object.keys(statusCounts).sort();
      statusEl.innerHTML = `<option value="all">All statuses | كل الحالات (${{episodes.length}})</option>` +
        keys.map((k) => `<option value="${{k}}">${{k}} (${{statusCounts[k]}})</option>`).join("");
    }}

    initFilter();
    evalStore = loadLocalEvalStore();
    loadExternalEvalStore();
    loadTripletCompareStore();

    geminiEvalSaveBtn.addEventListener("click", saveCurrentEval);
    geminiEvalClearBtn.addEventListener("click", clearCurrentEval);
    geminiEvalAutoScoreBtn.addEventListener("click", () => {{
      const s = parseScoreFromText(geminiEvalText.value || "");
      if (s === null) {{
        applyScoreBadge(null);
        return;
      }}
      geminiEvalScoreInput.value = String(s);
      applyScoreBadge(s);
    }});
    geminiEvalExportBtn.addEventListener("click", () => {{
      const payload = {{
        generated_at_utc: new Date().toISOString(),
        source: "atlas_review_viewer",
        evaluations: evalStore,
      }};
      const blob = new Blob([JSON.stringify(payload, null, 2)], {{ type: "application/json" }});
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = "gemini_chat_evaluations.json";
      a.click();
      setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    }});
    geminiEvalImportBtn.addEventListener("click", () => geminiEvalImportFile.click());
    geminiEvalImportFile.addEventListener("change", (ev) => {{
      const f = ev.target.files && ev.target.files[0];
      if (!f) return;
      const r = new FileReader();
      r.onload = () => {{
        try {{
          const obj = JSON.parse(String(r.result || "{{}}"));
          let incoming = {{}};
          if (Array.isArray(obj)) {{
            obj.forEach((rec) => {{
              if (!rec || typeof rec !== "object") return;
              const eid = String(rec.episode_id || "");
              if (!eid) return;
              incoming[eid] = rec;
            }});
          }} else if (obj && typeof obj === "object") {{
            if (Array.isArray(obj.evaluations)) {{
              obj.evaluations.forEach((rec) => {{
                if (!rec || typeof rec !== "object") return;
                const eid = String(rec.episode_id || "");
                if (!eid) return;
                incoming[eid] = rec;
              }});
            }} else if (obj.evaluations && typeof obj.evaluations === "object") {{
              incoming = obj.evaluations;
            }} else {{
              incoming = obj;
            }}
          }}
          evalStore = mergeEvalStores(evalStore, incoming);
          saveLocalEvalStore();
          if (currentEpisodeId) renderEvalForEpisode(currentEpisodeId);
        }} catch (_) {{}}
      }};
      r.readAsText(f);
    }});
    video.addEventListener("timeupdate", () => {{
      if (segStopAt == null) return;
      if (video.currentTime >= segStopAt) {{
        video.pause();
        segStopAt = null;
      }}
    }});
    episodeSelectEl.addEventListener("change", () => {{
      const eid = String(episodeSelectEl.value || "");
      if (!eid) return;
      const ep = episodes.find((x) => String(x.episode_id || "") === eid);
      if (ep) selectEpisode(ep);
    }});
    searchEl.addEventListener("input", renderEpisodeSelect);
    statusEl.addEventListener("change", renderEpisodeSelect);
    renderEpisodeSelect();
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
