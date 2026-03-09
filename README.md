# OCR Annotation Atlas - Tier3 Pipeline

Rule-first ecosystem for evaluating and repairing Tier2 annotation outputs against Atlas-style guidelines.

## Strategy docs

- `PROJECT_PLAN_EN.md`: architecture, workstreams, roadmap, and KPIs.
- `ENTERPRISE_PIPELINE_BRIEF_AR.md`: Arabic operational brief for autonomous end-to-end flow.

## Repository scope

This `main` branch contains the core production pipeline (validator, prompts, web solver, hybrid runner, and GUI).

Some operations modules discussed in architecture reviews (for example WhatsApp/Discord collectors or drive uploader timers) can live in extended deployment branches/snapshots and may not exist in this minimal core branch.

## Core components (implemented in this branch)

- `atlas_web_auto_solver.py`: Atlas browser automation (extract segments, call vision model, optionally apply labels).
- `validator.py`: deterministic policy gate (verbs, objects, numerals, No Action, overlap/duration checks).
- `pipeline_runner.py`: multi-pass orchestration (`candidate -> validate -> repair -> re-validate -> judge`).
- `repair_payload_builder.py`: creates repair payload from annotation + validator report.
- `prompts.py`: system prompts and JSON schema assets.
- `atlas_claude_smart_ai2.py`: Claude vision pipeline with normalization/autofix/QA hooks.
- `atlas_tier3_gui.py`: desktop GUI to run the pipeline.
- `app.js` + `atlas_annotation.html`: web annotation UI and local validation logic.
- `safe_update_preserve_local.sh`: safe update script that preserves runtime secrets/session state.

## Extended ecosystem modules (ops branches)

When present in your deployment branch, these modules extend the system into continuous learning + ops automation:

- `whatsapp_training_collector.py`: extracts operational feedback from WhatsApp chats.
- `discord_updates_collector.py`: harvests policy updates from Discord exports.
- `atlas_feedback_training_export.py`: exports disputes/feedback into reusable training datasets.
- `build_study_pack.py`: builds study/training bundles from outputs and errors.
- `upload_outputs_to_drive.sh`: backup/archive outputs to Google Drive via `rclone`.
- `sync_gemini_video_policy.py`: syncs video policy settings across YAML files.
- `atlas_training_supervisor.py`: multi-process supervisor with restart and incident logs.

## Hardening updates (March 2026)

The following strict policy updates are now in `main`:

- Strict verb-start gate in backend:
  - `validator.py::starts_with_allowed_action_verb`
  - rejects noun/adjective starts and disallows `-ing` verb starts.
- UI-side verb whitelist gate in frontend:
  - `app.js::validateLabel` blocks labels that do not start with approved action verbs.
- Autofix truncation fix:
  - `atlas_web_auto_solver.py::_autofix_label_candidate` now validates full combined labels first and avoids blind action splitting.
- Claude autofix safety improvements:
  - `atlas_claude_smart_ai2.py::autofix_label` uses clause-safe trimming before hard word-cap truncation.
- Claude default model corrected:
  - `atlas_claude_smart_ai2.py` defaults to `claude-3-5-sonnet-20241022`.
- Prompt-level narrative ban added:
  - `prompts.py` explicitly forbids narrative fillers (`then`, `another`, `continue`, `next`, `again`) in generation/repair/audit/normalization instructions.

Reference commits:

- `555eb66`: label autofix truncation + strict verb-start gate alignment.
- `e27d31a`: explicit forbidden narrative words in prompts.

## Output format (simplified)

By default each run writes:

- `<prefix>_final.json`
- `<prefix>_final_report.html`
- `<prefix>_summary.json`

Set `output.save_debug_files: true` to keep intermediate artifacts.

## Recommended workflow

1. Candidate generation (`file` / `gemini_video` / `claude_vision`).
2. Rule validation (`validator.py`).
3. Optional repair pass (`anthropic` / `openai` / `gemini`).
4. Re-validation.
5. Optional audit judge pass.

## Production policy recommendations

For safer execution quality, keep these settings in production configs:

- `run.execute_require_video_context: true`
- `gemini.require_video: true`
- `gemini.allow_text_only_fallback_on_network_error: false`

Rationale:

- Prevents text-only fallback when video upload fails.
- Avoids hallucinated action labels without visual evidence.

## Model-ID sanity checks (important)

Some sample YAMLs are templates and can contain placeholder model IDs.
Before enabling repair/judge in production, confirm every model ID against provider docs.

Safe examples:

- Anthropic: `claude-3-5-sonnet-20241022`
- OpenAI: `gpt-4o` or `gpt-4o-mini`
- Google: current Gemini model IDs from official docs

## GUI (desktop)

```bash
python atlas_tier3_gui.py
```

Or double-click:

```bash
launch_tier3_gui.bat
```

## Atlas web auto-solver

Setup once:

```bash
pip install playwright requests pyyaml
python -m playwright install chromium
```

Safe dry-run:

```bash
python atlas_web_auto_solver.py --config sample_web_auto_solver.yaml
```

Execute real writes:

```bash
python atlas_web_auto_solver.py --config sample_web_auto_solver.yaml --execute
```

Notes:

- OTP modes:
  - `otp.provider: gmail_imap`
  - `otp.provider: manual_browser`
- Session cache:
  - `.state/atlas_auth.json`
- Debug artifacts:
  - extracted segments
  - prompt payload
  - generated labels

## CLI

Run from YAML:

```bash
python pipeline_runner.py --config sample_config.yaml
```

Online hybrid (Gemini + OpenAI/Codex):

```bash
python pipeline_runner.py --config sample_config_online_hybrid.yaml
```

Triple hybrid (Gemini + Claude + OpenAI/Codex):

```bash
python pipeline_runner.py --config sample_config_gemini_claude_codex.yaml
```

Direct candidate JSON:

```bash
python pipeline_runner.py --candidate-json data/f01.json --episode-id f01 --duration 59 --output-dir outputs --output-prefix run_f01
```

Repair payload only:

```bash
python repair_payload_builder.py --annotation-json data/f01.json --output-json outputs/f01_repair_payload.json
```

## Safe VPS update (preserve keys/session)

```bash
cd /root/OCR_annotation_Atlas
chmod +x safe_update_preserve_local.sh
./safe_update_preserve_local.sh main /root/OCR_annotation_Atlas https://github.com/aymank2020/OCR_Annotation_Atlas.git
```

This preserves local runtime files (such as `.env`, local YAML overrides, and `.state/atlas_auth.json`) before pulling updates, then restores them after update.

## Provider notes

- Candidate providers: `file | claude_vision | gemini_video`.
- Repair/Judge providers: `none | anthropic | claude | openai | codex | openai_codex | gemini`.
- `codex` and `openai_codex` map to OpenAI API calls in `pipeline_runner.py`.
- `claude` maps to Anthropic API calls in `pipeline_runner.py`.

Environment keys:

- `ANTHROPIC_API_KEY`
- `OPENAI_API_KEY`
- `GEMINI_API_KEY` or `GOOGLE_API_KEY`

Security:

- Never commit API keys.
- Rotate immediately if exposed.

## Tests

```bash
python -m unittest discover -s tests -v
```
