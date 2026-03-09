# OCR Annotation Atlas - Full Project Plan (English)

## 1) Project Objective
Build and operate a production-grade autonomous annotation pipeline that:
- Ingests Tier-2 draft segments from Atlas.
- Re-evaluates video evidence with multimodal LLMs.
- Enforces strict policy and quality rules.
- Applies validated labels back to Atlas automatically.
- Learns continuously from disputes, audits, and policy updates.

## 2) Current System Snapshot
Core modules already available:
- Browser + task automation: `atlas_web_auto_solver.py`
- Rule engine and quality gate: `validator.py`
- Claude vision fallback: `atlas_claude_smart_ai2.py`
- Feedback exporter and study-pack tooling: `atlas_feedback_training_export.py`, `build_study_pack.py`
- Pipeline orchestration: `pipeline_runner.py`, `atlas_training_supervisor.py`
- UI tooling: `app.js`, `atlas_annotation.html`, `atlas_tier3_gui.py`

## 3) Target Architecture
1. Orchestration Layer
- Supervisor process controls solver + collectors.
- Restart strategy, cooldowns, and health checks.

2. Inference Layer
- Primary model: Gemini 3.1 Pro.
- Fallback model: Claude 3.5 Sonnet.
- Optional repair/judge chain with OpenAI.

3. Policy Layer
- Strict action-verb gate.
- Forbidden vocabulary and style constraints.
- Timestamp and granularity integrity checks.

4. Learning Layer
- Export rejected/approved samples.
- Build reusable training and policy datasets.
- Sync policy changes from ops channels.

5. Delivery Layer
- Apply segment edits to Atlas.
- Save logs, validation reports, and run artifacts.

## 4) Workstreams and Execution Steps

### Workstream A - Reliability and Runtime Hardening
1. Standardize process start/stop scripts.
2. Add watchdog checks for stale solver logs.
3. Enforce retry budgets and backoff ceilings.
4. Add deterministic crash signatures in incident logs.

Deliverable: stable 24/7 operation with automated recovery.

### Workstream B - Policy Quality Enforcement
1. Keep strict verb-start checks in backend and frontend.
2. Keep sentence-safe autofix (no blind truncation of compound actions).
3. Keep forbidden-verb and no-narrative constraints aligned across prompts and code.
4. Add regression tests for common policy leaks.

Deliverable: low leakage rate for invalid labels.

### Workstream C - Model Orchestration and Cost Control
1. Keep model fallback chain with explicit trigger conditions.
2. Use dynamic video preprocessing/chunking to reduce payload cost.
3. Keep key rotation + quota handling robust.
4. Track per-episode model usage and estimated cost.

Deliverable: lower cost per episode with stable quality.

### Workstream D - Continuous Learning (Self-Improvement)
1. Export dispute/audit outcomes continuously.
2. Generate lessons and policy deltas from human feedback.
3. Maintain a managed policy block that can be auto-updated.
4. Build periodic study packs for review and retraining.

Deliverable: measurable quality lift across weekly runs.

### Workstream E - Productization (Web/App)
1. Build public-facing dashboard for progress, quality, and incidents.
2. Add API facade for controlled job submission.
3. Add user roles (operator/reviewer/admin).
4. Add mobile-friendly review UI for quick QA actions.

Deliverable: scalable operator-facing product.

## 5) 30/60/90-Day Delivery Plan

### Day 0-30
- Stabilize runtime and policy gate.
- Complete regression tests for critical label failures.
- Ensure safe update flow preserves all secrets/configs on VPS.

### Day 31-60
- Expand continuous learning outputs.
- Add dashboard metrics (throughput, fail reasons, cost).
- Improve auto-repair confidence routing.

### Day 61-90
- Release web admin panel + API.
- Add mobile-friendly reviewer workflow.
- Prepare dataset packs for selective fine-tuning/RAG indexing.

## 6) KPIs
- Policy pass rate.
- Rework rate after auto-fix/repair.
- Average handling time per episode.
- Cost per successfully submitted episode.
- Uptime and mean-time-to-recovery.

## 7) Risk Register
- Model quota/rate limits.
- UI selector drift in Atlas pages.
- Policy drift from ops channels.
- Secret leakage risk during deployment.
- Dataset quality drift due noisy feedback.

Mitigations:
- Multi-model fallback and key rotation.
- Selector fallback chains + smoke tests.
- Managed policy sync workflow.
- Safe-update with backup/restore of secret files.
- Human review checkpoints for high-risk samples.

## 8) Immediate Next Actions
1. Keep solver running with current stable config.
2. Review fresh logs and validation outputs daily.
3. Track dispute outcomes and feed lessons into weekly policy updates.
4. Start dashboard MVP for operations visibility.
