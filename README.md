# OCR Annotation Atlas - Tier3 Pipeline

Rule-first pipeline for evaluating Tier2 annotation outputs against Atlas-style guidelines.

## What is implemented

- `validator.py`: deterministic rule engine (forbidden verbs, numerals, No Action rules, overlap, duration checks, etc.)
- `repair_payload_builder.py`: builds repair payload from annotation + validator report
- `pipeline_runner.py`: multi-pass pipeline runner
- `prompts.py`: production prompts + JSON schema asset
- `sample_config.yaml`: ready template for running the full flow
- `sample_config_online_hybrid.yaml`: ready Gemini + Codex/OpenAI online preset
- `sample_config_gemini_claude_codex.yaml`: ready Gemini + Claude + Codex preset (with JSON fallback)
- `atlas_tier3_gui.py`: desktop GUI to run pipeline without CLI
- `atlas_web_auto_solver.py`: browser automation (extract Atlas segments -> Gemini API -> optional auto-fill back to Atlas)

## Output format (simplified)

By default each run now writes only:

- `<prefix>_final.json` (final annotation)
- `<prefix>_final_report.html` (segment list UI similar to Atlas segment panel)
- `<prefix>_summary.json` (run metadata)

Set `output.save_debug_files: true` in config if you also want all intermediate files.

## Recommended workflow

1. Candidate generation (from file, Claude video, or Gemini video)
2. Rule validation (Python validator)
3. Optional repair pass (Anthropic/OpenAI/Gemini)
4. Re-validation
5. Optional audit judge pass

## GUI (easiest)

```bash
python atlas_tier3_gui.py
```

Or double-click:

```bash
launch_tier3_gui.bat
```

## Atlas Web Auto-Solver (no manual copy/paste)

This mode automates the manual loop of copying segments to Gemini and writing labels back.

Setup once:

```bash
pip install playwright requests pyyaml
python -m playwright install chromium
```

Run safe dry-run first:

```bash
python atlas_web_auto_solver.py --config sample_web_auto_solver.yaml
```

Apply labels to Atlas (real write):

```bash
python atlas_web_auto_solver.py --config sample_web_auto_solver.yaml --execute
```

Notes:
- Login + OTP can run in two modes:
  - `otp.provider: gmail_imap` (fully automatic from Gmail IMAP)
  - `otp.provider: manual_browser` (you type OTP manually in opened Chrome)
- Configure these fields in `sample_web_auto_solver.yaml` (or env vars):
  - `atlas.email`
  - `otp.gmail_email`
  - `otp.gmail_app_password` (Gmail App Password)
- If OTP detection is slow, increase:
  - `otp.timeout_sec`
  - `otp.lookback_sec`
- If OTP is not in Inbox, set mailbox explicitly:
  - `otp.mailbox: "[Gmail]/All Mail"`
- If you have multiple Chrome profiles, enable:
  - `browser.use_chrome_profile: true`
  - `browser.chrome_profile_directory: "auto"` (or set explicit value like `Profile 1`)
- If profile launch fails, close all Chrome windows first, then retry.
- Or keep `browser.fallback_to_isolated_context_on_profile_error: true` to continue automatically with isolated context.
- If profile startup hangs at `about:blank`, lower friction by keeping:
  - `browser.profile_launch_timeout_ms: 30000`
- On Windows, to avoid profile lock failures, enable:
  - `browser.close_chrome_before_profile_launch: true`
- For Chrome `DevTools remote debugging requires a non-default data directory`, enable:
  - `browser.clone_chrome_profile_to_temp: true`
- If Gmail IMAP returns `Application-specific password required`, create App Password here:
  - `https://myaccount.google.com/apppasswords`
- Session is cached in `.state/atlas_auth.json` after first successful run.
- Selectors are pre-tuned for current Atlas login/verify pages, with fallback chains for task-room UI.
- Script now attempts to download the current task video and attach it to Gemini API request together with segment text/timestamps.
- Script outputs debug artifacts to `outputs/`:
  - extracted segments JSON
  - prompt sent to Gemini
  - labels returned by Gemini

## CLI

Run from YAML config:

```bash
python pipeline_runner.py --config sample_config.yaml
```

Run online hybrid (Gemini + Codex/OpenAI):

```bash
python pipeline_runner.py --config sample_config_online_hybrid.yaml
```

Run triple hybrid (Gemini + Claude + Codex/OpenAI):

```bash
python pipeline_runner.py --config sample_config_gemini_claude_codex.yaml
```

Notes for this preset:
- Uses `gemini-2.5-flash` by default.
- If Gemini fails and Claude fallback fails, it automatically falls back to `input.candidate_json`.
- `fail_open_on_repair_error` / `fail_open_on_judge_error` keep pipeline running even if repair/judge providers fail.
- Cost-saving defaults:
  - `repair_policy: major_only`
  - `judge_policy: on_major_or_repair`
  - `save_debug_files: false` (fewer output files)

Run directly from candidate JSON:

```bash
python pipeline_runner.py --candidate-json data/f01.json --episode-id f01 --duration 59 --output-dir outputs --output-prefix run_f01
```

Build repair payload only:

```bash
python repair_payload_builder.py --annotation-json data/f01.json --output-json outputs/f01_repair_payload.json
```

## Provider notes

- `candidate_provider=file`: uses existing candidate JSON.
- `candidate_provider=claude_vision`: uses `atlas_claude_smart_ai2.py`.
- `candidate_provider=gemini_video`: supports local `video_file` (mp4).
- For Gemini candidate, you can set `fallback_type: claude_vision` in config to auto-fallback on 429/quota errors.
- Repair/Judge providers: `none | anthropic | claude | openai | codex | openai_codex | gemini`.
- `codex` and `openai_codex` are aliases to OpenAI API calls in `pipeline_runner.py`.
- `claude` is an alias to Anthropic API calls in `pipeline_runner.py`.

API keys can be set in config or env:

- `ANTHROPIC_API_KEY`
- `OPENAI_API_KEY`
- `GEMINI_API_KEY` or `GOOGLE_API_KEY`

Security:

- Never commit API keys in code/config files.
- If a key was shared in chat or committed, rotate/revoke it immediately and generate a new key.

## Tests

```bash
python -m unittest discover -s tests -v
```
