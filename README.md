# Atlas Capture – OCR Annotation Tool

A comprehensive web-based annotation tool built for the **Atlas Capture Egocentric Video Annotation** workflow. Implements all rules from the *Atlas Capture Standard Text Annotation Rules* document.

## 🚀 Quick Start

No installation needed — open `index.html` directly in any modern browser.

```bash
git clone https://github.com/aymank2020/OCR_Annotation_Atlas.git
cd OCR_Annotation_Atlas
# Open index.html in browser (double-click or use Live Server)
```

## ✨ Features

### 📹 Annotation Interface
- **Video player** with drag-and-drop video loading
- **Auto-segment creation** when a video is loaded
- **Segment editing** — edit label, adjust timestamps (±0.2s), split, merge, delete
- **Loop mode** and playback speed controls (0.25x – 2x)
- **Play individual segments** to verify timing
- **Undo stack** (up to 50 operations)

### ⌨️ Keyboard Shortcuts
| Key | Action |
|-----|--------|
| `E` | Edit selected segment label |
| `S` | Split selected segment |
| `D` | Delete selected segment |
| `M` | Merge selected segment with next |
| `U` | Undo last action |
| `L` | Toggle loop |
| `P` | Play selected segment |
| `Space` | Play/Pause video |
| `J` / `↑` | Navigate to previous segment |
| `K` / `↓` | Navigate to next segment |

### ✅ Label Validator
Real-time validation checks against all annotation rules:
- Forbidden verbs (`inspect`, `check`, `reach`)
- Missing action separators (comma or "and")
- Numerals in labels
- Labels without objects
- `place` without location
- `-ing` verb forms
- Intent-only language
- Label length (>20 words warning)
- Mixing `No Action` with real actions

### 📚 Guidelines Reference
9-section built-in guidelines covering:
- Core Mental Model
- Action Verb Rules
- Label Format Rules
- Dense vs Coarse distinction
- Segment Editing (Timestamps, Merge, Split)
- No Action & Object Rules
- Edge Cases
- Audit Fail Conditions + Ideal Segment Checklist

### 📊 History & Export
- Save completed episodes to history
- Export annotations as **JSON** or **CSV**
- Review all past annotations in-app

## 📐 Annotation Rules Summary

| Rule | Guideline |
|------|-----------|
| Format | Imperative voice: `pick up spoon`, `place box on table` |
| Separators | Comma or "and" between actions |
| Numerals | No digits — use words or omit: `pick up three knives` |
| Forbidden verbs | Never use `inspect`, `check`, or `reach` |
| Dense vs Coarse | Never mix in one label |
| No Action | Only when hands touch nothing or ego is idle |
| Place | Always needs a location: `place cup on table` |
| Objects | Use only what you can defend; general nouns if unsure |

## 🏗 Project Structure

```
OCR_Annotation_Atlas/
├── index.html          # Main application
├── src/
│   ├── styles.css      # All styles
│   └── app.js          # Application logic & validation engine
├── docs/
│   └── guidelines.md   # Offline guidelines reference
└── README.md
```

## 🔗 Related Resources

- [Atlas Capture Standard Text Annotation Rules](https://docs.google.com/document/d/16kFg-gpJr6YN1rjDz54UL91AlA-g93AO4idifouIzyQ)
- [Tier 3 Standard Annotation Playbook](https://docs.google.com/document/d/1mbwwP45qgtBFewMD43-rdJ-JDaGqCuzw43xgBgylNh8)
- [Discord Community](https://discord.com) — for edge case escalation

## 📝 License

MIT
