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
- `atlas_dashboard_gen.py`: local HTML dashboard from `outputs/`.
- `atlas_finetune_exporter.py`: exports episodes/disputes for fine-tuning datasets.
- `atlas_review_builder.py`: builds `episodes_review_index.json` for full historical re-audit.
- `atlas_chat_exporter.py`: exports per-episode chat packages (`chat_prompt.txt` + metadata + optional video).
- `atlas_eval_store.py`: stores external Gemini Chat evaluations and writes per-episode chat text files to `outputs/chat_reviews/<episode_id>/text_<episode_id>_chat.txt`.
- `atlas_auto_sync_and_rebuild.py`: one-shot Python workflow to auto-sync from Drive (if needed) and rebuild review artifacts.
- `atlas_watchdog.py`: watchdog health check/restart for `atlas-autopilot.service`.
- `install_watchdog_cron.sh`: installs cron watchdog runner on Linux VPS.
- `atlas_triplet_compare.py`: evaluates 3 candidates (Tier2 / Gemini API / Gemini Chat) against up to 2 videos and returns one JSON verdict.
- `atlas_triplet_batch.py`: runs triplet compare in batch for many episodes and updates `gemini_chat_evaluations.json` so results appear in Dashboard/Viewer.

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
- Atlas rule alignment in backend:
  - `validator.py` enforces numerals as errors, `place` without location as error,
    and allows `reach` only in truncated-end edge cases.
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

## Full Historical Re-Audit (all episodes)

Build one index from all artifacts (video, Tier2, Tier3, validation, disputes, usage):

```bash
python atlas_review_builder.py --outputs-dir outputs --out outputs/episodes_review_index.json
```

Export ready-to-review chat folders:

```bash
python atlas_chat_exporter.py --index outputs/episodes_review_index.json --out-dir chat_reviews --only-status disputed,policy_fail,error,labeled_not_submitted --copy-video
```

Generate interactive viewer page (video + Tier2/Tier3 + validation + disputes):

```bash
python atlas_review_viewer_gen.py --index outputs/episodes_review_index.json --out outputs/atlas_review_viewer.html
```

Each folder in `chat_reviews/<episode_id>/` contains:

- `chat_prompt.txt`
- `episode_meta.json`
- `atlas_url.txt`
- episode video (when available)

## Full Historical Re-Audit (Google Drive source)

When server disk is limited and your historical artifacts live on Google Drive, run the Drive workflow script.

Prerequisites:

- `rclone` configured with a Drive remote (default remote name: `gdrive`)
- Python 3 available on server

Run by Drive folder link:

```bash
bash atlas_drive_review_workflow.sh \
  --drive-link "https://drive.google.com/drive/folders/<FOLDER_ID>?usp=sharing" \
  --upload-results 1
```

Run by remote path:

```bash
bash atlas_drive_review_workflow.sh \
  --drive-path "OCR_annotation_Atlas/outputs_archive" \
  --upload-results 1
```

Useful options:

- `--include-video 1`: also copy videos locally (default `0`)
- `--only-status disputed,policy_fail,error,labeled_not_submitted`
- `--limit 200`
- `--remote gdrive`

What it does:

1. Pulls metadata snapshot from Drive into `/tmp/atlas_drive_review/snapshot` (videos excluded by default).
2. Builds `episodes_review_index.json`.
3. Builds `atlas_dashboard.html` (operations metrics from Drive snapshot).
4. Builds `atlas_review_viewer.html` (interactive page for manual QA).
5. Exports chat packages (`chat_reviews/<episode_id>/...`).
6. Uploads generated dashboard/index/viewer/packages back to the same Drive folder when `--upload-results 1`.

## Triplet Compare (Tier2 vs API vs Chat)

Use this when there are no live tasks and you want to evaluate archived files/videos from Drive.

Inputs:

- Video source of truth: `--video-path` (required), `--video-path-limit` (optional second video)
- Candidate texts: `--tier2-path`, `--api-path`, `--chat-path` (or fallback `--labels-path`)
- Optional context: `--task-state-path`

Supported input refs:

- local file path
- Drive folder-link + filename suffix, e.g.:
  - `https://drive.google.com/drive/folders/<FOLDER_ID>?usp=sharing\video_x.mp4`

Example:

```bash
python atlas_triplet_compare.py \
  --config sample_web_auto_solver_vps.yaml \
  --video-path "https://drive.google.com/drive/folders/<FOLDER_ID>?usp=sharing\video_68d3c4ff6427d8caac511a05.mp4" \
  --video-path-limit "https://drive.google.com/drive/folders/<FOLDER_ID>?usp=sharing\video_68d3c4ff6427d8caac511a05_upload_opt.mp4" \
  --tier2-path "https://drive.google.com/drive/folders/<FOLDER_ID>?usp=sharing\text_68d3c4ff6427d8caac511a05_current.txt" \
  --api-path "https://drive.google.com/drive/folders/<FOLDER_ID>?usp=sharing\text_68d3c4ff6427d8caac511a05_update.txt" \
  --chat-path "https://drive.google.com/drive/folders/<FOLDER_ID>?usp=sharing\chat_68d3c4ff6427d8caac511a05.txt" \
  --task-state-path "https://drive.google.com/drive/folders/<FOLDER_ID>?usp=sharing\task_state_68d3c4ff6427d8caac511a05.json" \
  --labels-path "https://drive.google.com/drive/folders/<FOLDER_ID>?usp=sharing\labels_68d3c4ff6427d8caac511a05.json" \
  --remote gdrive \
  --model gemini-3.1-pro-preview \
  --out outputs/triplet_compare_result.json
```

Output:

- `outputs/triplet_compare_result.json` with:
  - winner (`tier2|api|chat|none`)
  - hallucination flags per candidate
  - short recommendation and issue list

## Triplet Batch (All Cases)

Run triplet compare across all episodes in `episodes_review_index.json`:

```bash
python atlas_triplet_batch.py \
  --config sample_web_auto_solver_vps.yaml \
  --outputs-dir outputs \
  --index outputs/episodes_review_index.json \
  --model gemini-3.1-pro-preview \
  --results-dir outputs/triplet_compare \
  --results-jsonl outputs/triplet_compare_results.jsonl \
  --source triplet_compare_batch
```

What it updates:

- per-episode compare files: `outputs/triplet_compare/triplet_compare_<episode_id>.json`
- run summary rows: `outputs/triplet_compare_results.jsonl`
- Gemini chat eval store: `outputs/gemini_chat_evaluations.json`
- per-episode chat text file: `outputs/chat_reviews/<episode_id>/text_<episode_id>_chat.txt`

## Auto Sync + Rebuild (Python one-shot)

If dashboard values are zero because local `outputs/` is empty while historical data is on Drive:

```bash
python atlas_auto_sync_and_rebuild.py \
  --outputs-dir outputs \
  --drive-link "https://drive.google.com/drive/folders/<FOLDER_ID>?usp=sharing" \
  --remote gdrive \
  --build-power-queue \
  --run-triplet-batch \
  --triplet-config sample_web_auto_solver_vps.yaml \
  --triplet-model gemini-3.1-pro-preview
```

This command can:

1. detect weak local outputs coverage,
2. sync metadata snapshot from Drive (videos optional),
3. rebuild index/dashboard/viewer/chat packages,
4. generate `outputs/power_automate_queue.csv`.
5. (optional) run batch triplet compare and publish results to dashboard/viewer data sources.

Open the generated pages on localhost:

```bash
python -m http.server 8080 --directory outputs
```

Then open:

- `http://localhost:8080/atlas_review_viewer.html`
- `http://localhost:8080/atlas_dashboard.html`

## Production policy recommendations

For safer execution quality, keep these settings in production configs:

- `run.execute_require_video_context: true`
- `run.resume_skip_video_steps_when_cached: false`
- `gemini.require_video: true`
- `gemini.allow_text_only_fallback_on_network_error: false`

Rationale:

- Prevents text-only fallback when video upload fails.
- Avoids hallucinated action labels without visual evidence.

Optional hard gate before final submit:

- `run.pre_submit_gemini_compare_enabled: true`
- `run.pre_submit_gemini_compare_model: gemini-3.1-pro-preview`
- `run.pre_submit_gemini_compare_block_on_reject: true`

This adds an extra Gemini review step right before `Submit` in Atlas Quality Review.
It compares Tier2 vs current Tier3 (and can attach video) and blocks auto-submit when verdict is reject.

Optional Gemini Web Chat gate (NOT API):

- `run.pre_submit_gemini_chat_compare_enabled: true`
- `run.pre_submit_gemini_chat_compare_url: https://gemini.google.com/app/<chat_id>`
- `run.pre_submit_gemini_chat_compare_video_source: auto` (`drive_link` / `local_file` / `none`)
- `run.pre_submit_gemini_chat_compare_block_on_fail: true`

Notes:

- This gate opens a new Playwright tab to Gemini Chat and asks for PASS/FAIL before final submit.
- For Drive-link mode, make sure Google Workspace extension is enabled in the same Google account session.
- For Linux VPS/headless, reuse an authenticated Chrome profile (`browser.use_chrome_profile: true`) so Gemini Chat opens already logged in.

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

## Watchdog (auto-restart on crash/stall)

Install watchdog cron (every 5 minutes):

```bash
cd /root/OCR_annotation_Atlas
chmod +x install_watchdog_cron.sh
./install_watchdog_cron.sh /root/OCR_annotation_Atlas atlas-autopilot.service 5
```

Manual watchdog run:

```bash
python3 atlas_watchdog.py --service atlas-autopilot.service --outputs-dir outputs
```

## Provider notes

- Candidate providers: `file | claude_vision | gemini_video`.
- Repair/Judge providers: `none | anthropic | claude | openai | codex | openai_codex | gemini`.
- `codex` and `openai_codex` map to OpenAI API calls in `pipeline_runner.py`.
- `claude` maps to Anthropic API calls in `pipeline_runner.py`.

Environment keys:

- `ANTHROPIC_API_KEY`
- `OPENAI_API_KEY`
- `GEMINI_API_KEY` or `GOOGLE_API_KEY`
- `GOOGLE_APPLICATION_CREDENTIALS` (when using `gemini.auth_mode: vertex_ai`)
- `GOOGLE_CLOUD_PROJECT` (optional if `gemini.vertex_project` is set)
- `GOOGLE_CLOUD_LOCATION` (optional if `gemini.vertex_location` is set)

Vertex AI mode for `atlas_web_auto_solver.py`:

- In YAML:
  - `gemini.auth_mode: vertex_ai`
  - `gemini.vertex_project: <gcp-project-id>`
  - `gemini.vertex_location: us-central1`
- In env:
  - `GOOGLE_APPLICATION_CREDENTIALS=/root/OCR_annotation_Atlas/secrets/vertex-sa.json`
- Dependency:
  - `pip install google-auth`

Security:

- Never commit API keys.
- Rotate immediately if exposed.

## Tests

```bash
python -m unittest discover -s tests -v
```
