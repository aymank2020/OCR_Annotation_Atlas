/**
 * Atlas Capture – Annotation Tool
 * Full annotation workflow based on Atlas Capture Standard Text Annotation Rules
 */

'use strict';

// ============================================================
// STATE
// ============================================================
const state = {
  segments: [],          // { id, start, end, label, selected }
  history: [],           // saved episodes
  selectedId: null,
  undoStack: [],
  videoLoaded: false,
  looping: false,
  currentEpisode: { id: Date.now(), desc: '' },
  guidelines: {},        // current guidelines tab
};

let nextId = 1;
const video = document.getElementById('video-player');

// ============================================================
// VALIDATION ENGINE
// ============================================================
const FORBIDDEN_VERBS = ['inspect', 'check', 'reach'];
const FORBIDDEN_WORDS = ['hands', 'examine'];
const DISALLOWED_TOOL_TERMS = ['mechanical arm', 'robotic arm', 'robot arm', 'manipulator', 'claw arm'];
const ARTICLES = [' the ', ' a ', ' an '];
const ING_PATTERN = /\b\w+ing\b/g;
const NUMERAL_PATTERN = /\b\d+\b/;

function validateLabel(label) {
  const issues = [];
  const suggestions = [];
  const l = label.trim();
  const ALLOWED_START_VERBS = [
    'pick up', 'put down', 'set down', 'take out', 'take off', 'turn on', 'turn off', 'plug in', 'unplug',
    'pick', 'place', 'move', 'adjust', 'hold', 'align', 'relocate', 'tighten', 'loosen', 'wipe', 'clean',
    'paint', 'dip', 'remove', 'insert', 'pull', 'push', 'turn', 'open', 'close', 'unscrew', 'screw',
    'lift', 'set', 'attach', 'detach', 'apply', 'cut', 'drill', 'measure', 'fold', 'press', 'slide',
    'stack', 'pack', 'unpack', 'straighten', 'comb', 'spread', 'shake', 'pour', 'spray', 'peel', 'wrap',
    'lock', 'unlock', 'grasp', 'position', 'fit', 'mount', 'unmount', 'clip', 'unclip', 'twist', 'untwist',
    'raise', 'lower', 'connect', 'disconnect', 'bend'
  ];

  if (!l) { issues.push({ type: 'error', msg: 'Label cannot be empty' }); return { issues, suggestions, pass: false }; }
  if (l.split(' ').length < 2) { issues.push({ type: 'error', msg: 'Use at least 2 words per label' }); }
  if (l.toLowerCase() !== 'no action') {
    const lowerLabel = l.toLowerCase();
    const tokens = lowerLabel.match(/[a-z]+/g) || [];
    const separableTakeStart = tokens.length >= 3 && tokens[0] === 'take' && (tokens.slice(1, 6).includes('out') || tokens.slice(1, 6).includes('off'));
    const hasValidStart = ALLOWED_START_VERBS.some(v => lowerLabel.startsWith(v + ' ') || lowerLabel === v) || separableTakeStart;
    if (!hasValidStart) {
      const firstWord = l.split(' ')[0];
      issues.push({ type: 'error', msg: `Label must start with an approved physical action verb. Invalid start: "${firstWord}"` });
      suggestions.push('Start the sentence with a direct action like: pick up, place, adjust, wipe, etc.');
    }
  }

  // Forbidden verbs
  FORBIDDEN_VERBS.forEach(v => {
    if (l.toLowerCase().includes(v)) {
      issues.push({ type: 'error', msg: `Forbidden verb "${v}" — use "adjust" instead of inspect/check; fix timestamps if "reach" feels needed` });
    }
  });

  // Forbidden words
  FORBIDDEN_WORDS.forEach(w => {
    if (l.toLowerCase().includes(w)) {
      issues.push({ type: 'warning', msg: `Avoid "${w}" — don't reference body parts unless unavoidable` });
    }
  });

  // Gripper terminology policy
  DISALLOWED_TOOL_TERMS.forEach(t => {
    if (l.toLowerCase().includes(t)) {
      issues.push({ type: 'error', msg: `Use "gripper" only if unavoidable - avoid "${t}"` });
    }
  });
  if (/\bgripper\b/i.test(l)) {
    issues.push({ type: 'warning', msg: 'Only mention "gripper" when description is unclear without it' });
  }

  // Articles
  ARTICLES.forEach(a => {
    if ((' ' + l.toLowerCase() + ' ').includes(a)) {
      issues.push({ type: 'warning', msg: `Remove article "${a.trim()}" — labels should not use a/an/the` });
    }
  });

  // -ing verbs
  const ingMatches = l.match(ING_PATTERN);
  if (ingMatches) {
    issues.push({ type: 'warning', msg: `Avoid -ing verbs ("${ingMatches.join(', ')}") — use imperative form (e.g. "cut" not "cutting")` });
    suggestions.push('Use imperative voice: "pick up", "place", "cut", "move"');
  }

  // Numerals
  if (NUMERAL_PATTERN.test(l)) {
    issues.push({ type: 'error', msg: 'No numerals — use words ("three") or omit quantity' });
  }

  // No action combined with label
  if (/no action/i.test(l) && l.toLowerCase() !== 'no action') {
    issues.push({ type: 'error', msg: '"No Action" must not be combined with other actions in the same label' });
  }

  // Capitalize check
  if (l[0] !== l[0].toLowerCase()) {
    issues.push({ type: 'warning', msg: 'Labels should start with lowercase (imperative verb)' });
  }

  // Dense check — more than ~20 words
  const wordCount = l.split(/\s+/).length;
  if (wordCount > 20) {
    issues.push({ type: 'warning', msg: `Label is very long (${wordCount} words). Consider a coarse label to avoid hallucination risk` });
    suggestions.push('Long labels increase hallucination risk — prefer coarse goal label if possible');
  }

  // Multi-action separator check (if multiple verbs, needs comma or "and")
  const verbs = ['pick up', 'place', 'move', 'cut', 'adjust', 'put', 'grab', 'hold', 'close', 'open', 'peel', 'secure', 'smooth', 'insert'];
  const verbMatches = verbs.filter(v => l.toLowerCase().includes(v));
  if (verbMatches.length >= 2 && !l.includes(',') && !/ and /i.test(l)) {
    issues.push({ type: 'error', msg: 'Multiple actions need separator: use comma or "and" between actions' });
  }

  // Verb without object
  if (/^(pick up|place|move|cut|adjust|grab)\s*$/.test(l.toLowerCase())) {
    issues.push({ type: 'error', msg: 'Every verb must have an object — e.g. "pick up cup" not "pick up"' });
  }

  // Place without location
  if (/\bplace\b/.test(l.toLowerCase()) && !/place\s+\w+\s+(on|in|into|onto|at|to)\s+\w+/.test(l.toLowerCase())) {
    issues.push({ type: 'warning', msg: '"place" should include a location — e.g. "place cup on table"' });
    suggestions.push('"Place" always needs a location (general is fine): "place cup on table", "place cup in bin"');
  }

  // Intent-only language
  if (/\bprepare\b|\bget ready\b|\btry to\b/.test(l.toLowerCase())) {
    issues.push({ type: 'error', msg: 'No intent-only language — label the actual physical action observed' });
  }

  const pass = !issues.some(i => i.type === 'error');
  return { issues, suggestions, pass };
}

// ============================================================
// SEGMENT MANAGEMENT
// ============================================================
function createSegment(start = 0, end = 5, label = '') {
  return { id: nextId++, start, end, label, selected: false };
}

function pushUndo() {
  state.undoStack.push(JSON.parse(JSON.stringify(state.segments)));
  if (state.undoStack.length > 50) state.undoStack.shift();
}

function undo() {
  if (!state.undoStack.length) { showToast('Nothing to undo', 'info'); return; }
  state.segments = state.undoStack.pop();
  state.selectedId = null;
  renderSegments();
  showToast('Undone', 'info');
}

function addSegment() {
  pushUndo();
  const lastSeg = state.segments[state.segments.length - 1];
  const start = lastSeg ? lastSeg.end : 0;
  const end = start + 5;
  const seg = createSegment(start, end, '');
  state.segments.push(seg);
  selectSegment(seg.id);
  renderSegments();
  showToast('Segment added', 'info');
  openEditModal(seg.id);
}

function deleteSegment(id) {
  pushUndo();
  state.segments = state.segments.filter(s => s.id !== id);
  if (state.selectedId === id) state.selectedId = null;
  renderSegments();
  showToast('Segment deleted', 'info');
}

function splitSegment(id) {
  const idx = state.segments.findIndex(s => s.id === id);
  if (idx < 0) return;
  const seg = state.segments[idx];
  const mid = parseFloat(((seg.start + seg.end) / 2).toFixed(1));
  pushUndo();
  const newSeg = createSegment(mid, seg.end, '');
  seg.end = mid;
  state.segments.splice(idx + 1, 0, newSeg);
  renderSegments();
  showToast('Segment split', 'info');
}

function mergeWithNext(id) {
  const idx = state.segments.findIndex(s => s.id === id);
  if (idx < 0 || idx >= state.segments.length - 1) { showToast('No next segment to merge', 'error'); return; }
  pushUndo();
  const a = state.segments[idx];
  const b = state.segments[idx + 1];
  a.end = b.end;
  if (a.label && b.label) a.label = a.label + ', ' + b.label;
  else a.label = a.label || b.label;
  state.segments.splice(idx + 1, 1);
  renderSegments();
  showToast('Segments merged', 'info');
}

function adjustTime(id, which, delta) {
  const seg = state.segments.find(s => s.id === id);
  if (!seg) return;
  pushUndo();
  if (which === 'start') {
    seg.start = Math.max(0, parseFloat((seg.start + delta).toFixed(1)));
    if (seg.start >= seg.end) seg.start = seg.end - 0.1;
  } else {
    const maxEnd = video.duration || 9999;
    seg.end = Math.min(maxEnd, parseFloat((seg.end + delta).toFixed(1)));
    if (seg.end <= seg.start) seg.end = seg.start + 0.1;
  }
  renderSegments();
}

function selectSegment(id) {
  state.selectedId = id;
  state.segments.forEach(s => s.selected = s.id === id);
  renderSegments();
  if (video.duration && id) {
    const seg = state.segments.find(s => s.id === id);
    if (seg) video.currentTime = seg.start;
  }
}

function saveLabel(id, label) {
  const seg = state.segments.find(s => s.id === id);
  if (!seg) return;
  pushUndo();
  seg.label = label.trim();
  renderSegments();
  showToast('Label saved', 'success');
}

// ============================================================
// RENDER
// ============================================================
function renderSegments() {
  const list = document.getElementById('segments-list');
  const count = document.getElementById('seg-count');
  count.textContent = state.segments.length;

  if (!state.segments.length) {
    list.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text-hint)">No segments yet. Click "+ Add Segment" to begin.</div>';
    return;
  }

  list.innerHTML = state.segments.map((seg, i) => {
    const dur = (seg.end - seg.start).toFixed(1);
    const isSelected = seg.id === state.selectedId;
    const labelClass = !seg.label ? 'empty' : (seg.label === 'No Action' ? 'no-action' : '');
    const labelText = seg.label || '(click to edit label)';

    return `<div class="seg-item ${isSelected ? 'selected' : ''}" data-id="${seg.id}" onclick="selectSegment(${seg.id})">
      <div class="seg-num">${i + 1}</div>
      <div class="seg-body">
        <div class="seg-time">
          <button class="seg-time-btn" onclick="event.stopPropagation();adjustTime(${seg.id},'start',-0.2)" title="Decrease start">−</button>
          <span>${fmt(seg.start)}</span>
          <span style="color:var(--text-hint)">→</span>
          <span>${fmt(seg.end)}</span>
          <button class="seg-time-btn" onclick="event.stopPropagation();adjustTime(${seg.id},'end',0.2)" title="Increase end">+</button>
          <span class="seg-duration">(${dur}s)</span>
          ${(seg.end - seg.start) > 60 ? '<span style="color:#ef4444;font-size:11px;margin-left:6px">>60s</span>' : ''}
        </div>
        <div class="seg-label ${labelClass}">${labelText}</div>
      </div>
      <div class="seg-btns">
        <button class="seg-btn" onclick="event.stopPropagation();openEditModal(${seg.id})" title="Edit (E)">✎</button>
        <button class="seg-btn" onclick="event.stopPropagation();splitSegment(${seg.id})" title="Split (S)">✂</button>
        <button class="seg-btn" onclick="event.stopPropagation();mergeWithNext(${seg.id})" title="Merge with next (M)">⊕</button>
        <button class="seg-btn danger" onclick="event.stopPropagation();deleteSegment(${seg.id})" title="Delete (D)">🗑</button>
        <button class="seg-btn" onclick="event.stopPropagation();playSegment(${seg.id})" title="Play (P)">▶</button>
      </div>
    </div>`;
  }).join('');

  // Auto-scroll to selected
  if (state.selectedId && document.getElementById('auto-scroll').checked) {
    const el = list.querySelector(`[data-id="${state.selectedId}"]`);
    if (el) el.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }
}

function fmt(t) {
  const m = Math.floor(t / 60);
  const s = (t % 60).toFixed(1).padStart(4, '0');
  return `${m}:${s}`;
}

function playSegment(id) {
  const seg = state.segments.find(s => s.id === id);
  if (!seg || !state.videoLoaded) { showToast('Load a video first', 'info'); return; }
  video.currentTime = seg.start;
  video.play();
  const check = () => {
    if (video.currentTime >= seg.end) { video.pause(); video.removeEventListener('timeupdate', check); }
  };
  video.addEventListener('timeupdate', check);
}

// ============================================================
// EDIT MODAL
// ============================================================
let editingId = null;

function openEditModal(id) {
  const seg = state.segments.find(s => s.id === id);
  if (!seg) return;
  editingId = id;
  document.getElementById('modal-seg-id').textContent = state.segments.indexOf(seg) + 1;
  document.getElementById('modal-label-input').value = seg.label;
  document.getElementById('modal-realtime-feedback').textContent = '';
  document.getElementById('modal-realtime-feedback').className = 'modal-feedback';
  document.getElementById('edit-modal').classList.remove('hidden');
  setTimeout(() => document.getElementById('modal-label-input').focus(), 50);
}

function closeModal() {
  document.getElementById('edit-modal').classList.add('hidden');
  editingId = null;
}

document.getElementById('modal-label-input').addEventListener('input', e => {
  const label = e.target.value;
  if (!label.trim()) { document.getElementById('modal-realtime-feedback').textContent = ''; return; }
  const { issues } = validateLabel(label);
  const errors = issues.filter(i => i.type === 'error');
  const warns  = issues.filter(i => i.type === 'warning');
  const fb = document.getElementById('modal-realtime-feedback');
  if (errors.length) { fb.textContent = '✗ ' + errors[0].msg; fb.className = 'modal-feedback error'; }
  else if (warns.length) { fb.textContent = '⚠ ' + warns[0].msg; fb.className = 'modal-feedback warn'; }
  else { fb.textContent = '✓ Looks good'; fb.className = 'modal-feedback ok'; }
});

document.getElementById('modal-save').addEventListener('click', () => {
  if (editingId !== null) { saveLabel(editingId, document.getElementById('modal-label-input').value); }
  closeModal();
});
document.getElementById('modal-cancel').addEventListener('click', closeModal);
document.getElementById('modal-close').addEventListener('click', closeModal);

document.getElementById('modal-label-input').addEventListener('keydown', e => {
  if (e.key === 'Enter') { document.getElementById('modal-save').click(); }
  if (e.key === 'Escape') { closeModal(); }
});

// ============================================================
// VIDEO
// ============================================================
const dropZone = document.getElementById('drop-zone');

dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('drag-over'); });
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));
dropZone.addEventListener('drop', e => {
  e.preventDefault();
  dropZone.classList.remove('drag-over');
  const file = e.dataTransfer.files[0];
  if (file && file.type.startsWith('video/')) loadVideo(file);
});
dropZone.addEventListener('click', () => document.getElementById('video-input').click());
document.getElementById('video-input').addEventListener('change', e => {
  if (e.target.files[0]) loadVideo(e.target.files[0]);
});

function loadVideo(file) {
  const url = URL.createObjectURL(file);
  video.src = url;
  video.hidden = false;
  dropZone.style.display = 'none';
  document.getElementById('video-controls').style.display = 'flex';
  state.videoLoaded = true;
  document.getElementById('episode-desc').textContent = file.name;

  video.addEventListener('timeupdate', updateTimeDisplay);
  video.addEventListener('loadedmetadata', () => {
    updateTimeDisplay();
    // Auto-create segments based on video length if none exist
    if (!state.segments.length) {
      const dur = video.duration;
      const segDur = Math.min(10, Math.max(2, dur / 8));
      let t = 0;
      while (t < dur) {
        const end = Math.min(t + segDur, dur);
        state.segments.push(createSegment(parseFloat(t.toFixed(1)), parseFloat(end.toFixed(1)), ''));
        t = end;
      }
      renderSegments();
      showToast(`Auto-created ${state.segments.length} segments — review and label each`, 'info');
    }
  });
}

function updateTimeDisplay() {
  const cur = fmt(video.currentTime);
  const dur = fmt(video.duration || 0);
  document.getElementById('time-display').textContent = `${cur} / ${dur}`;
}

document.getElementById('btn-loop').addEventListener('click', () => {
  state.looping = !state.looping;
  video.loop = state.looping;
  const btn = document.getElementById('btn-loop');
  btn.textContent = state.looping ? '⟳ Loop ON' : '⟳ Loop OFF';
  btn.classList.toggle('active', state.looping);
});

document.getElementById('speed-select').addEventListener('change', e => {
  video.playbackRate = parseFloat(e.target.value);
});

// ============================================================
// KEYBOARD SHORTCUTS
// ============================================================
document.addEventListener('keydown', e => {
  const activeModal = !document.getElementById('edit-modal').classList.contains('hidden');
  if (activeModal) return;
  const tag = document.activeElement.tagName;
  if (tag === 'INPUT' || tag === 'TEXTAREA') return;

  switch (e.key.toLowerCase()) {
    case 'e':
      if (state.selectedId) openEditModal(state.selectedId);
      break;
    case 's':
      if (state.selectedId) splitSegment(state.selectedId);
      break;
    case 'd':
      if (state.selectedId) deleteSegment(state.selectedId);
      break;
    case 'm':
      if (state.selectedId) mergeWithNext(state.selectedId);
      break;
    case 'u':
      undo();
      break;
    case 'l':
      document.getElementById('btn-loop').click();
      break;
    case 'p':
      if (state.selectedId) playSegment(state.selectedId);
      break;
    case ' ':
      e.preventDefault();
      video.paused ? video.play() : video.pause();
      break;
    case 'j':
    case 'arrowup': {
      const idx = state.segments.findIndex(s => s.id === state.selectedId);
      if (idx > 0) selectSegment(state.segments[idx - 1].id);
      break;
    }
    case 'k':
    case 'arrowdown': {
      const idx2 = state.segments.findIndex(s => s.id === state.selectedId);
      if (idx2 >= 0 && idx2 < state.segments.length - 1) selectSegment(state.segments[idx2 + 1].id);
      break;
    }
  }
});

// ============================================================
// COMPLETE / REPORT
// ============================================================
document.getElementById('btn-complete').addEventListener('click', () => {
  const unlabeled = state.segments.filter(s => !s.label);
  if (unlabeled.length) {
    showToast(`${unlabeled.length} segment(s) still unlabeled!`, 'error');
    return;
  }
  const overDuration = state.segments.filter(s => (s.end - s.start) > 60);
  if (overDuration.length) {
    showToast(`${overDuration.length} segment(s) exceed 60 seconds - split them before completing`, 'error');
    return;
  }
  // Run validation on all labels
  let hasErrors = false;
  state.segments.forEach(seg => {
    const { pass } = validateLabel(seg.label);
    if (!pass) { hasErrors = true; }
  });
  if (hasErrors) {
    showToast('Some labels have errors — check the Validator tab', 'error');
    return;
  }
  saveToHistory();
  showToast('Episode completed and saved!', 'success');
});

document.getElementById('btn-report').addEventListener('click', () => {
  showToast('Report submitted. Thank you for flagging this issue.', 'info');
});

function saveToHistory() {
  const episode = {
    id: state.currentEpisode.id,
    desc: document.getElementById('episode-desc').textContent,
    timestamp: new Date().toLocaleString(),
    segments: JSON.parse(JSON.stringify(state.segments)),
  };
  state.history.unshift(episode);
  renderHistory();
  // Reset for new episode
  state.segments = [];
  state.selectedId = null;
  state.undoStack = [];
  nextId = 1;
  state.currentEpisode = { id: Date.now(), desc: '' };
  renderSegments();
}

// ============================================================
// HISTORY
// ============================================================
function renderHistory() {
  const list = document.getElementById('history-list');
  if (!state.history.length) {
    list.innerHTML = '<div class="empty-history">No completed episodes yet. Complete an annotation to see it here.</div>';
    return;
  }
  list.innerHTML = state.history.map(ep => `
    <div class="history-card">
      <h4>${ep.desc || 'Episode'} <small style="color:var(--text-muted);font-weight:400">${ep.timestamp}</small></h4>
      ${ep.segments.map((s, i) => `
        <div class="history-seg-row">
          <span class="history-seg-num">${i + 1}</span>
          <span class="history-seg-time">${fmt(s.start)} → ${fmt(s.end)}</span>
          <span class="history-seg-label">${s.label || '<em style="color:var(--text-hint)">unlabeled</em>'}</span>
        </div>`).join('')}
    </div>`).join('');
}

document.getElementById('btn-export-json').addEventListener('click', () => {
  if (!state.history.length) { showToast('No history to export', 'info'); return; }
  const blob = new Blob([JSON.stringify(state.history, null, 2)], { type: 'application/json' });
  download(blob, 'atlas_annotations.json');
});

document.getElementById('btn-export-csv').addEventListener('click', () => {
  if (!state.history.length) { showToast('No history to export', 'info'); return; }
  const rows = [['episode_id','episode_desc','timestamp','seg_num','start','end','duration','label']];
  state.history.forEach(ep => {
    ep.segments.forEach((s, i) => {
      rows.push([ep.id, ep.desc, ep.timestamp, i + 1, s.start, s.end, (s.end - s.start).toFixed(1), s.label]);
    });
  });
  const csv = rows.map(r => r.map(v => `"${String(v).replace(/"/g,'""')}"`).join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  download(blob, 'atlas_annotations.csv');
});

document.getElementById('btn-clear-history').addEventListener('click', () => {
  if (!confirm('Clear all annotation history?')) return;
  state.history = [];
  renderHistory();
  showToast('History cleared', 'info');
});

function download(blob, filename) {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
}

// ============================================================
// VALIDATOR PAGE
// ============================================================
document.getElementById('btn-validate').addEventListener('click', () => {
  const label = document.getElementById('validator-input').value;
  const { issues, suggestions, pass } = validateLabel(label);

  const results = document.getElementById('validator-results');
  results.classList.remove('hidden');

  const scoreEl = document.getElementById('val-score');
  const errCount = issues.filter(i => i.type === 'error').length;
  const warnCount = issues.filter(i => i.type === 'warning').length;

  if (pass && !warnCount) {
    scoreEl.className = 'val-score pass';
    scoreEl.innerHTML = '✓ PASS — Label looks correct';
  } else if (pass) {
    scoreEl.className = 'val-score warn';
    scoreEl.innerHTML = `⚠ PASS WITH WARNINGS — ${warnCount} warning(s)`;
  } else {
    scoreEl.className = 'val-score fail';
    scoreEl.innerHTML = `✗ FAIL — ${errCount} error(s), ${warnCount} warning(s)`;
  }

  document.getElementById('val-issues').innerHTML = issues.map(i =>
    `<div class="val-issue ${i.type}">
      <span class="issue-icon">${i.type === 'error' ? '✗' : i.type === 'warning' ? '⚠' : '✓'}</span>
      <span>${i.msg}</span>
    </div>`
  ).join('') || '<div class="val-issue ok"><span class="issue-icon">✓</span><span>No issues found!</span></div>';

  document.getElementById('val-suggestions').innerHTML = suggestions.length
    ? '<strong>Suggestions:</strong><br>' + suggestions.map(s => `• ${s}`).join('<br>')
    : '';
});

// Mistake grid
const COMMON_MISTAKES = [
  {
    title: 'Using "inspect" or "check"',
    desc: 'Forbidden verbs. Use "adjust" instead for small corrective actions.',
    bad: '✗ inspect box',
    good: '✓ adjust box',
  },
  {
    title: 'Using "reach"',
    desc: 'Usually means timestamps are wrong. Fix the segment start time instead.',
    bad: '✗ reach for cup',
    good: '✓ pick up cup (fix start timestamp)',
  },
  {
    title: 'Numerals in labels',
    desc: 'Never use numbers. Use words or omit quantity.',
    bad: '✗ pick up 3 knives',
    good: '✓ pick up three knives / pick up knives',
  },
  {
    title: 'Wrong gripper wording',
    desc: 'Treat the tool as hand extension. Do not use robotic/mechanical arm wording.',
    bad: '? use mechanical arm to grab block',
    good: '? pick up block',
  },
  {
    title: 'Segment too long',
    desc: 'Each segment must be 60 seconds or less. Split at disengagement points.',
    bad: '? one segment from 0.0s to 75.0s',
    good: '? split into shorter engagement-based segments',
  },
  {
    title: 'Missing separator',
    desc: 'Multiple actions need comma or "and" between them.',
    bad: '✗ pick up cup place cup on table',
    good: '✓ pick up cup, place cup on table',
  },
  {
    title: '"Place" without location',
    desc: '"Place" always requires at least a general location.',
    bad: '✗ place cup',
    good: '✓ place cup on table',
  },
  {
    title: 'Mixing Dense + Coarse',
    desc: 'A single segment must be either dense OR coarse — never both.',
    bad: '✗ pick up cup and wash dishes',
    good: '✓ wash dishes (coarse) OR pick up cup, place cup in sink (dense)',
  },
  {
    title: 'Combining "No Action"',
    desc: '"No Action" must never be combined with real actions.',
    bad: '✗ No Action, pick up cup',
    good: '✓ No Action (separate segment)',
  },
  {
    title: 'Intent-only language',
    desc: 'Label the physical action, not the mental intent.',
    bad: '✗ prepare to cut tape',
    good: '✓ pick up scissors, cut tape with scissors',
  },
  {
    title: 'Verb without object',
    desc: 'Every verb must have a clear object.',
    bad: '✗ pick up, place on table',
    good: '✓ pick up cup, place cup on table',
  },
  {
    title: 'Labeling movement/navigation',
    desc: 'Do NOT label walking or navigating through space.',
    bad: '✗ walk to table',
    good: '✓ No Action (or omit that segment)',
  },
  {
    title: 'Inconsistent naming',
    desc: 'Use the same name for the same object throughout the episode.',
    bad: '✗ pick up box → place cardboard box on table',
    good: '✓ pick up box → place box on table',
  },
  {
    title: 'Unnecessary adjectives',
    desc: 'Only use adjectives to disambiguate two similar items.',
    bad: '✗ pick up blue box (if only one box)',
    good: '✓ pick up box',
  },
];

document.getElementById('mistake-grid').innerHTML = COMMON_MISTAKES.map(m => `
  <div class="mistake-card">
    <h4>⚠ ${m.title}</h4>
    <p>${m.desc}</p>
    <div class="mc-ex mc-bad">${m.bad}</div>
    <div class="mc-ex mc-good">${m.good}</div>
  </div>`).join('');

// ============================================================
// GUIDELINES PAGE
// ============================================================
const GUIDELINES_DATA = {
  core: `
    <div class="guideline-section">
      <h2>Core Mental Model (Non-Negotiable)</h2>
      <div class="alert-box info">A segment represents <strong>one continuous interaction with a primary object toward a single goal.</strong></div>
      <div class="alert-box info">Gripper tasks: treat the gripper as an extension of hand. Usually do not mention the tool in labels.</div>
      <p>A segment typically begins when the hands engage the primary object and ends when that interaction is complete, <strong>when the hands disengage</strong>, or when the interaction focus or goal changes.</p>
      <h3>What Requires a Label</h3>
      <div class="do-dont-grid">
        <div class="do-box"><h4>✓ DO Label</h4><ul><li>Goal-oriented hand–object actions that matter to the task</li><li>Main actions involving hand dexterity</li><li>Main objects being interacted with</li></ul></div>
        <div class="dont-box"><h4>✗ DO NOT Label</h4><ul><li>Walking / navigating through space</li><li>Idle gestures</li><li>Looking / visually examining (no "inspect", "check")</li><li>Unrelated actions (adjusting camera, checking phone when not important)</li><li>"Reach" (see Action Verb Rules)</li></ul></div>
      </div>
      <div class="alert-box danger">No hand contact → <strong>No Action</strong> (default)</div>
      <div class="alert-box warning">Do NOT use numerical characters (e.g., 1, 2, 5th, 10th). Use words ("three") or omit quantities when not required.</div>
    </div>`,
  verbs: `
    <div class="guideline-section">
      <h2>Action Verb Rules</h2>
      <h3>4.1 Verbs you must NOT use</h3>
      <div class="rule-box bad">inspect / check</div>
      <div class="rule-box bad">reach (except only if the action is truncated/cut off at the end of the episode and no better verb is possible)</div>
      <p style="color:var(--warning)">If "reach" feels necessary, timestamps are usually wrong. Fix timestamps instead.</p>
      <h3>4.2 Verb Definitions</h3>
      <table class="verb-table">
        <thead><tr><th>Verb</th><th>Meaning</th><th>Audit Notes</th></tr></thead>
        <tbody>
          <tr><td>pick up</td><td>Object leaves a surface/container resting position</td><td>Required when using dense and a pickup occurred</td></tr>
          <tr><td>place</td><td>Object contacts a surface and is released/positioned</td><td>Required when using dense and a placement occurred</td></tr>
          <tr><td>move</td><td>Coarse relocation describing pick up + place as one goal, OR repositioning without detailing steps</td><td>✓ Allowed coarse substitute for "pick up and place" when relocation is the goal</td></tr>
          <tr><td>adjust</td><td>Small corrective change in position/orientation</td><td>Use instead of inspect/check</td></tr>
          <tr><td>hold</td><td>Maintain grip without relocating</td><td>Only if task-relevant</td></tr>
          <tr><td>grab</td><td>Grip itself is meaningful</td><td>Rare; use sparingly</td></tr>
        </tbody>
      </table>
      <h3>4.3 "Move" Clarification (Important)</h3>
      <p>Yes — "move __" can represent "pick up and place __" as a coarse label when the goal is relocation.</p>
      <div class="rule-box good">move mat to table (coarse)</div>
      <div class="rule-box good">move box onto shelf (coarse)</div>
      <p>If you choose dense, you must be explicit:</p>
      <div class="rule-box good">pick up mat, place mat on table (dense)</div>
      <h3>4.4 Attach verbs to objects</h3>
      <div class="rule-box bad">pick up, place on table</div>
      <div class="rule-box good">pick up cup, place cup on table</div>
    </div>`,
  format: `
    <div class="guideline-section">
      <h2>Label Format Rules</h2>
      <h3>2.1 Imperative Voice</h3>
      <p>Write labels as commands:</p>
      <div class="rule-box good">pick up spoon</div>
      <div class="rule-box good">place box on table</div>
      <h3>2.2 Consistency within an episode</h3>
      <p>Use consistent verbs and nouns where possible. If you choose "wash," don't alternate with "clean" without reason.</p>
      <h3>2.3 Action separators</h3>
      <p>When multiple actions are in one label, separate actions with <strong>comma</strong> or <strong>and</strong>:</p>
      <div class="rule-box good">pick up cup, place cup on table</div>
      <div class="rule-box good">pick up cup and place cup on table</div>
      <div class="rule-box bad">pick up cup place cup on table (no separator)</div>
      <h3>2.4 No numerals</h3>
      <div class="rule-box bad">pick up 3 knives</div>
      <div class="rule-box good">pick up three knives</div>
      <div class="rule-box good">pick up knives</div>
      <h3>2.5 No intent-only language</h3>
      <p>Don't add mental-state intent that isn't a physical action. Prefer the physical verb that occurred.</p>
      <div class="rule-box bad">prepare to cut tape</div>
      <div class="rule-box good">pick up scissors, cut tape with scissors</div>
      <h3>Dense vs Coarse</h3>
      <div class="alert-box danger">A segment is either <strong>Dense OR Coarse</strong> — do not mix within a single segment.</div>
      <div class="rule-box warn">Use Coarse when: A clear goal exists AND listing atomic steps risks errors/hallucination, OR the atomic steps are too many to list safely</div>
      <div class="rule-box info">Use Dense when: Multiple distinct hand actions are required to be accurate (no single goal verb fits)</div>
      <div class="alert-box warning">Dense is NOT "better" than coarse. Coarse is often preferred for accuracy.</div>
      <p>As a general guideline, a single segment label should usually contain no more than <strong>~20 words</strong> or <strong>~4 atomic actions</strong>.</p>
    </div>`,
  segments: `
    <div class="guideline-section">
      <h2>Segment Editing Rules</h2>
      <div class="alert-box warning">Hard limits: each segment <= 60 seconds and each label <= 20 words.</div>
      <h3>7.1 Timestamps</h3>
      <div class="rule-box info">Start: when the action begins (hands begin engaging toward contact to cover the full interaction)</div>
      <div class="rule-box info">End: when hands disengage and the interaction ends</div>
      <p>Minor idle time inside the segment is acceptable <strong>if the segment still represents one continuous interaction.</strong></p>
      <h3>7.2 Extend / Shorten</h3>
      <p>Use to align boundaries to the true action.</p>
      <div class="rule-box bad">Don't extend into a new action.</div>
      <div class="rule-box bad">Don't cut off completion of the action.</div>
      <h3>7.3 Merge (when allowed)</h3>
      <p>Merge adjacent segments only if:</p>
      <div class="rule-box good">Same action/goal, AND</div>
      <div class="rule-box good">Hands <strong>never disengage</strong> between them.</div>
      <h3>7.4 Do NOT merge when:</h3>
      <div class="rule-box bad">There are repeated pick up → place cycles with clear disengagement</div>
      <div class="rule-box bad">Different objects or different goals</div>
      <h3>7.5 Split (when required)</h3>
      <p>Split when:</p>
      <div class="rule-box good">Hands disengage and a new interaction begins, OR</div>
      <div class="rule-box good">A new goal/action begins that must be labeled separately</div>
      <h3>Decision Tree: Merge vs Split</h3>
      <div class="rule-box info">1. Hands disengage? → Yes: split | No: continue</div>
      <div class="rule-box info">2. Same goal? → Yes: merge/keep | No: split</div>
      <div class="rule-box info">3. Different object? → Yes: split</div>
      <div class="alert-box danger">Never merge: repeated pick up → place cycles with disengagement, or different goals "just to reduce count"</div>
    </div>`,
  edge: `
    <div class="guideline-section">
      <h2>Edge Cases & Reference</h2>
      <h3>No Action Rules</h3>
      <p>Use <code>No Action</code> only when:</p>
      <div class="rule-box warn">Hands touch nothing, OR</div>
      <div class="rule-box warn">Ego is idle / doing irrelevant behavior unrelated to the task</div>
      <div class="rule-box bad">Do not split solely to isolate "No Action" pauses.</div>
      <div class="rule-box bad">Do not combine "No Action" with real actions in a single label.</div>
      <div class="rule-box bad">Do not use "No Action" if the ego is holding an object and that hold is task-relevant.</div>
      <h3>Repeated Actions</h3>
      <div class="rule-box info">If the ego disengages and repeats → they are separate segments</div>
      <div class="rule-box info">If the ego never disengages → it is one segment (often coarse)</div>
      <h3>Simultaneous Actions</h3>
      <p>If multiple task-relevant actions happen in the same segment, include them either:</p>
      <div class="rule-box good">As a coarse goal label, OR</div>
      <div class="rule-box good">As dense enumerated actions</div>
      <div class="alert-box warning">…but do not invent steps.</div>
      <h3>Out-of-Frame Actions</h3>
      <div class="rule-box bad">Do not label actions that are fully out of frame.</div>
      <div class="rule-box good">If the action is partially visible but clearly occurring, the label is acceptable.</div>
      <div class="rule-box bad">Do not infer or guess actions that cannot be reasonably confirmed from on-screen evidence.</div>
      <h3>Object Rules</h3>
      <div class="rule-box good">Identify only what you can defend — if unsure, use general noun ("tool", "container", "cloth")</div>
      <div class="rule-box good">Stay consistent in object naming through the episode where possible</div>
      <div class="rule-box good">Use adjectives only to disambiguate (two similar items) — "blue cloth vs white cloth"</div>
      <div class="rule-box good">Avoid referencing body parts unless unavoidable — prefer "wash spoon" over "wash spoon with hand"</div>
      <h3>When to Escalate (via Discord)</h3>
      <div class="rule-box warn">Object cannot be identified after reasonable effort</div>
      <div class="rule-box warn">Action cannot be labeled without guessing</div>
      <div class="rule-box warn">Segment cannot be made accurate via coarse abstraction</div>
    </div>`,
  checklist: `
    <div class="guideline-section">
      <h2>Audit Fail Conditions</h2>
      <p>A segment fails audit if any of the following are true:</p>
      <div class="rule-box bad">Missed major task-relevant hand action</div>
      <div class="rule-box bad">Hallucinated (non-occurring) action/object</div>
      <div class="rule-box bad">Timestamps cut off the action or include a different action</div>
      <div class="rule-box bad">Forbidden verbs used ("inspect/check", "reach" except truncated-end exception)</div>
      <div class="rule-box bad">Dense/coarse mixed in one label</div>
      <div class="rule-box bad">"No Action" combined with action</div>
      <h2 style="margin-top:20px">Ideal Segment Checklist</h2>
      <div class="rule-box good">One goal</div>
      <div class="rule-box good">Full action coverage</div>
      <div class="rule-box good">Accurate verbs</div>
      <div class="rule-box good">No hallucinated steps</div>
      <div class="rule-box good">Dense OR coarse (not mixed)</div>
      <div class="alert-box info"><strong>Remember:</strong> Quality over quantity. A well-labeled segment accurately captures the main hand-object interaction from start to finish, using clear and consistent language.</div>
      <h2 style="margin-top:20px">Final Self-Check Before Submitting</h2>
      <div class="rule-box info">Did I miss a pick up or place?</div>
      <div class="rule-box info">Did I invent anything?</div>
      <div class="rule-box info">Did I split without disengagement?</div>
      <div class="rule-box info">Did I use a forbidden verb?</div>
      <div class="rule-box info">Is this dense OR coarse?</div>
      <div class="alert-box warning">If unsure → choose coarse.</div>
    </div>`,
};

function renderGuidelines(tab) {
  document.getElementById('guidelines-content').innerHTML = GUIDELINES_DATA[tab] || '';
  document.querySelectorAll('.gtab').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
}

document.querySelectorAll('.gtab').forEach(btn => {
  btn.addEventListener('click', () => renderGuidelines(btn.dataset.tab));
});
renderGuidelines('core');

// ============================================================
// NAVIGATION
// ============================================================
document.querySelectorAll('.nav-item').forEach(item => {
  item.addEventListener('click', e => {
    e.preventDefault();
    const page = item.dataset.page;
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    item.classList.add('active');
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.getElementById(`page-${page}`).classList.add('active');
    if (page === 'history') renderHistory();
  });
});

// ============================================================
// BUTTON HOOKS
// ============================================================
document.getElementById('btn-add-seg').addEventListener('click', addSegment);
document.getElementById('btn-undo').addEventListener('click', undo);

// ============================================================
// TOAST
// ============================================================
let toastTimer = null;
function showToast(msg, type = 'info') {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = `toast ${type}`;
  if (toastTimer) clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.add('hidden'), 3000);
}

// ============================================================
// INIT
// ============================================================
renderSegments();
renderHistory();
