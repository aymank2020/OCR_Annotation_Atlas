"""
GUI for running the Atlas multi-pass pipeline.
"""

from __future__ import annotations

import os
import queue
import threading
import traceback
from pathlib import Path
from typing import Any, Dict, Optional

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

import pipeline_runner


class PipelineGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Atlas Pipeline Runner")
        self.root.geometry("1120x820")
        self.root.minsize(960, 680)

        self._queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self._worker: Optional[threading.Thread] = None

        self.config_path = tk.StringVar(value="")

        self.episode_id = tk.StringVar(value="episode")
        self.candidate_json = tk.StringVar(value="")
        self.video_duration = tk.StringVar(value="")
        self.video_file = tk.StringVar(value="")
        self.video_url = tk.StringVar(value="")
        self.headers_json = tk.StringVar(value="")

        self.candidate_provider = tk.StringVar(value="file")
        self.candidate_model = tk.StringVar(value="gemini-2.5-flash")
        self.candidate_api_key = tk.StringVar(value=os.environ.get("GEMINI_API_KEY", ""))
        self.max_frames = tk.StringVar(value="45")
        self.skip_object_map = tk.BooleanVar(value=False)

        self.run_repair = tk.BooleanVar(value=True)
        self.repair_provider = tk.StringVar(value="none")
        self.repair_model = tk.StringVar(value="gpt-4o")
        self.repair_api_key = tk.StringVar(value="")

        self.run_judge = tk.BooleanVar(value=False)
        self.judge_provider = tk.StringVar(value="none")
        self.judge_model = tk.StringVar(value="gpt-4o")
        self.judge_api_key = tk.StringVar(value="")

        self.output_dir = tk.StringVar(value="outputs")
        self.output_prefix = tk.StringVar(value="atlas_pipeline")
        self.save_debug_files = tk.BooleanVar(value=False)
        self.auto_open_report = tk.BooleanVar(value=True)

        self.status = tk.StringVar(value="Ready")
        self.last_summary: Optional[Dict[str, Any]] = None

        self._build()
        self.root.after(120, self._poll_queue)

    def _build(self) -> None:
        c = ttk.Frame(self.root, padding=12)
        c.pack(fill=tk.BOTH, expand=True)

        cfg = ttk.LabelFrame(c, text="Optional Config File", padding=10)
        cfg.pack(fill=tk.X)
        ttk.Entry(cfg, textvariable=self.config_path).grid(row=0, column=0, sticky="we", padx=(0, 8))
        ttk.Button(cfg, text="Browse", command=self._pick_config).grid(row=0, column=1, sticky="e")
        ttk.Label(cfg, text="If set, config file values are used instead of manual fields.").grid(
            row=1, column=0, columnspan=2, sticky="w", pady=(6, 0)
        )
        cfg.columnconfigure(0, weight=1)

        inp = ttk.LabelFrame(c, text="Input", padding=10)
        inp.pack(fill=tk.X, pady=(10, 0))

        ttk.Label(inp, text="Episode ID:").grid(row=0, column=0, sticky="w")
        ttk.Entry(inp, textvariable=self.episode_id, width=24).grid(row=1, column=0, sticky="w")

        ttk.Label(inp, text="Candidate JSON:").grid(row=0, column=1, sticky="w", padx=(10, 0))
        ttk.Entry(inp, textvariable=self.candidate_json).grid(row=1, column=1, sticky="we", padx=(10, 8))
        ttk.Button(inp, text="Browse", command=self._pick_candidate_json).grid(row=1, column=2, sticky="e")

        ttk.Label(inp, text="Video Duration (sec, optional):").grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(inp, textvariable=self.video_duration, width=24).grid(row=3, column=0, sticky="w")

        ttk.Label(inp, text="Video File (for video providers):").grid(row=2, column=1, sticky="w", padx=(10, 0), pady=(8, 0))
        ttk.Entry(inp, textvariable=self.video_file).grid(row=3, column=1, sticky="we", padx=(10, 8))
        ttk.Button(inp, text="Browse", command=self._pick_video_file).grid(row=3, column=2, sticky="e")

        ttk.Label(inp, text="Video URL (optional):").grid(row=4, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(inp, textvariable=self.video_url).grid(row=5, column=0, columnspan=3, sticky="we")

        ttk.Label(inp, text="Headers JSON (optional):").grid(row=6, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(inp, textvariable=self.headers_json).grid(row=7, column=0, columnspan=2, sticky="we", padx=(0, 8))
        ttk.Button(inp, text="Browse", command=self._pick_headers_json).grid(row=7, column=2, sticky="e")

        inp.columnconfigure(1, weight=1)

        prov = ttk.LabelFrame(c, text="Providers and Stages", padding=10)
        prov.pack(fill=tk.X, pady=(10, 0))

        ttk.Label(prov, text="Candidate Provider:").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            prov,
            textvariable=self.candidate_provider,
            values=["file", "claude_vision", "gemini_video"],
            state="readonly",
            width=16,
        ).grid(row=1, column=0, sticky="w")

        ttk.Label(prov, text="Candidate Model:").grid(row=0, column=1, sticky="w", padx=(10, 0))
        ttk.Entry(prov, textvariable=self.candidate_model, width=24).grid(row=1, column=1, sticky="w", padx=(10, 0))

        ttk.Label(prov, text="Candidate API Key:").grid(row=0, column=2, sticky="w", padx=(10, 0))
        ttk.Entry(prov, textvariable=self.candidate_api_key, width=30, show="*").grid(
            row=1, column=2, sticky="w", padx=(10, 0)
        )

        ttk.Label(prov, text="Max Frames:").grid(row=0, column=3, sticky="w", padx=(10, 0))
        ttk.Entry(prov, textvariable=self.max_frames, width=8).grid(row=1, column=3, sticky="w", padx=(10, 0))
        ttk.Checkbutton(prov, text="Skip object map", variable=self.skip_object_map).grid(
            row=1, column=4, sticky="w", padx=(10, 0)
        )

        ttk.Checkbutton(prov, text="Run Repair", variable=self.run_repair).grid(row=2, column=0, sticky="w", pady=(10, 0))
        ttk.Label(prov, text="Repair Provider:").grid(row=2, column=1, sticky="w", padx=(10, 0), pady=(10, 0))
        ttk.Combobox(
            prov,
            textvariable=self.repair_provider,
            values=["none", "anthropic", "claude", "openai", "codex", "openai_codex", "gemini"],
            state="readonly",
            width=16,
        ).grid(row=3, column=1, sticky="w", padx=(10, 0))
        ttk.Entry(prov, textvariable=self.repair_model, width=24).grid(row=3, column=2, sticky="w", padx=(10, 0))
        ttk.Entry(prov, textvariable=self.repair_api_key, width=30, show="*").grid(row=3, column=3, sticky="w", padx=(10, 0))

        ttk.Checkbutton(prov, text="Run Judge", variable=self.run_judge).grid(row=4, column=0, sticky="w", pady=(10, 0))
        ttk.Label(prov, text="Judge Provider:").grid(row=4, column=1, sticky="w", padx=(10, 0), pady=(10, 0))
        ttk.Combobox(
            prov,
            textvariable=self.judge_provider,
            values=["none", "anthropic", "claude", "openai", "codex", "openai_codex", "gemini"],
            state="readonly",
            width=16,
        ).grid(row=5, column=1, sticky="w", padx=(10, 0))
        ttk.Entry(prov, textvariable=self.judge_model, width=24).grid(row=5, column=2, sticky="w", padx=(10, 0))
        ttk.Entry(prov, textvariable=self.judge_api_key, width=30, show="*").grid(row=5, column=3, sticky="w", padx=(10, 0))

        out = ttk.LabelFrame(c, text="Output", padding=10)
        out.pack(fill=tk.X, pady=(10, 0))
        ttk.Label(out, text="Output Dir:").grid(row=0, column=0, sticky="w")
        ttk.Entry(out, textvariable=self.output_dir).grid(row=1, column=0, sticky="we", padx=(0, 8))
        ttk.Button(out, text="Browse", command=self._pick_output_dir).grid(row=1, column=1, sticky="e")

        ttk.Label(out, text="Output Prefix:").grid(row=0, column=2, sticky="w", padx=(10, 0))
        ttk.Entry(out, textvariable=self.output_prefix, width=28).grid(row=1, column=2, sticky="w", padx=(10, 0))
        ttk.Checkbutton(out, text="Save debug files", variable=self.save_debug_files).grid(
            row=2, column=0, sticky="w", pady=(10, 0)
        )
        ttk.Checkbutton(out, text="Open final report automatically", variable=self.auto_open_report).grid(
            row=2, column=2, sticky="w", padx=(10, 0), pady=(10, 0)
        )
        out.columnconfigure(0, weight=1)

        actions = ttk.Frame(c)
        actions.pack(fill=tk.X, pady=(10, 0))
        self.run_btn = ttk.Button(actions, text="Run Pipeline", command=self._run_clicked)
        self.run_btn.pack(side=tk.LEFT)
        ttk.Button(actions, text="Hybrid Preset (Gemini + Codex)", command=self._apply_hybrid_preset).pack(
            side=tk.LEFT, padx=(8, 0)
        )
        ttk.Button(actions, text="Preset (Gemini + Claude + Codex)", command=self._apply_triple_preset).pack(
            side=tk.LEFT, padx=(8, 0)
        )
        ttk.Button(actions, text="Open Final Report", command=self._open_last_report).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(actions, text="Open Output Folder", command=self._open_output_dir).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(actions, text="Clear Log", command=self._clear_log).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Label(actions, textvariable=self.status).pack(side=tk.RIGHT)

        logs = ttk.LabelFrame(c, text="Logs / Summary", padding=8)
        logs.pack(fill=tk.BOTH, expand=True, pady=(10, 0))

        self.log = tk.Text(logs, wrap="word", state="disabled")
        self.log.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ys = ttk.Scrollbar(logs, orient=tk.VERTICAL, command=self.log.yview)
        ys.pack(side=tk.RIGHT, fill=tk.Y)
        self.log["yscrollcommand"] = ys.set

    def _append(self, text: str) -> None:
        self.log.configure(state="normal")
        self.log.insert(tk.END, text + "\n")
        self.log.see(tk.END)
        self.log.configure(state="disabled")

    def _clear_log(self) -> None:
        self.log.configure(state="normal")
        self.log.delete("1.0", tk.END)
        self.log.configure(state="disabled")

    def _pick_config(self) -> None:
        path = filedialog.askopenfilename(title="Select YAML config", filetypes=(("YAML", "*.yaml *.yml"), ("All", "*.*")))
        if path:
            self.config_path.set(path)

    def _pick_candidate_json(self) -> None:
        path = filedialog.askopenfilename(title="Select candidate JSON", filetypes=(("JSON", "*.json"), ("All", "*.*")))
        if path:
            self.candidate_json.set(path)

    def _pick_video_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Select video file",
            filetypes=(("Video", "*.mp4 *.mov *.mkv *.webm"), ("All", "*.*")),
        )
        if path:
            self.video_file.set(path)

    def _pick_headers_json(self) -> None:
        path = filedialog.askopenfilename(title="Select headers JSON", filetypes=(("JSON", "*.json"), ("All", "*.*")))
        if path:
            self.headers_json.set(path)

    def _pick_output_dir(self) -> None:
        path = filedialog.askdirectory(title="Select output directory")
        if path:
            self.output_dir.set(path)

    def _open_output_dir(self) -> None:
        out = Path(self.output_dir.get().strip() or "outputs")
        out.mkdir(parents=True, exist_ok=True)
        try:
            os.startfile(str(out))  # type: ignore[attr-defined]
        except Exception as exc:
            messagebox.showerror("Open Folder Failed", str(exc))

    def _open_last_report(self) -> None:
        if not self.last_summary:
            messagebox.showinfo("No Report", "Run the pipeline first.")
            return
        report_path = str(self.last_summary.get("files", {}).get("final_report", "")).strip()
        if not report_path:
            messagebox.showinfo("No Report", "Final report path not found in summary.")
            return
        report_file = Path(report_path)
        if not report_file.exists():
            messagebox.showerror("Missing Report", f"File does not exist:\n{report_path}")
            return
        try:
            os.startfile(str(report_file))  # type: ignore[attr-defined]
        except Exception as exc:
            messagebox.showerror("Open Report Failed", str(exc))

    def _apply_hybrid_preset(self) -> None:
        self.candidate_provider.set("gemini_video")
        self.candidate_model.set("gemini-2.5-flash")
        self.run_repair.set(True)
        self.repair_provider.set("codex")
        self.repair_model.set("gpt-4o")
        self.run_judge.set(True)
        self.judge_provider.set("codex")
        self.judge_model.set("gpt-4o")
        self._append("Applied preset: Gemini(video) + Codex/OpenAI(repair+judge).")

    def _apply_triple_preset(self) -> None:
        self.candidate_provider.set("gemini_video")
        self.candidate_model.set("gemini-2.5-flash")
        self.run_repair.set(True)
        self.repair_provider.set("claude")
        self.repair_model.set("claude-opus-4-5")
        self.run_judge.set(True)
        self.judge_provider.set("codex")
        self.judge_model.set("gpt-4o")
        self._append("Applied preset: Gemini(video) + Claude(repair) + Codex/OpenAI(judge).")

    @staticmethod
    def _has_provider_key(provider: str, explicit_key: str) -> bool:
        p = provider.strip().lower()
        if p in {"none", ""}:
            return True
        if explicit_key.strip():
            return True
        if p in {"gemini"}:
            return bool(os.environ.get("GEMINI_API_KEY", "").strip() or os.environ.get("GOOGLE_API_KEY", "").strip())
        if p in {"openai", "codex", "openai_codex"}:
            return bool(os.environ.get("OPENAI_API_KEY", "").strip())
        if p in {"anthropic", "claude"}:
            return bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())
        return False

    def _validate(self) -> Optional[str]:
        if self.config_path.get().strip():
            path = Path(self.config_path.get().strip())
            if not path.exists():
                return "Config file does not exist."
            return None

        provider = self.candidate_provider.get().strip().lower()
        if not self.candidate_json.get().strip() and provider == "file":
            return "Candidate provider=file requires Candidate JSON path."
        if provider in {"claude_vision", "gemini_video"} and not (
            self.video_file.get().strip() or self.video_url.get().strip()
        ):
            return "Video provider requires Video File or Video URL."
        if provider == "gemini_video":
            cand_key = self.candidate_api_key.get().strip() or os.environ.get("GEMINI_API_KEY", "").strip()
            if not cand_key:
                cand_key = os.environ.get("GOOGLE_API_KEY", "").strip()
            if not cand_key:
                return "Gemini provider requires candidate API key (field or GEMINI_API_KEY env)."
        if provider in {"claude_vision", "claude_video"}:
            cand_key = self.candidate_api_key.get().strip() or os.environ.get("ANTHROPIC_API_KEY", "").strip()
            if not cand_key:
                return "Claude video provider requires candidate API key (field or ANTHROPIC_API_KEY env)."
        if self.video_duration.get().strip():
            try:
                if float(self.video_duration.get().strip()) <= 0:
                    return "Duration must be positive."
            except ValueError:
                return "Duration must be numeric."
        try:
            frames = int(self.max_frames.get().strip())
        except ValueError:
            return "Max Frames must be integer."
        if frames < 8 or frames > 120:
            return "Max Frames should be between 8 and 120."
        if self.run_repair.get():
            if not self._has_provider_key(self.repair_provider.get(), self.repair_api_key.get()):
                return "Repair provider key missing (set field or related env key)."
        if self.run_judge.get():
            if not self._has_provider_key(self.judge_provider.get(), self.judge_api_key.get()):
                return "Judge provider key missing (set field or related env key)."
        return None

    def _build_config(self) -> pipeline_runner.PipelineConfig:
        if self.config_path.get().strip():
            return pipeline_runner.load_config(self.config_path.get().strip())

        duration = float(self.video_duration.get().strip()) if self.video_duration.get().strip() else 0.0
        return pipeline_runner.PipelineConfig(
            candidate_json=self.candidate_json.get().strip(),
            episode_id=self.episode_id.get().strip() or "episode",
            video_duration_sec=duration,
            video_file=self.video_file.get().strip(),
            video_url=self.video_url.get().strip(),
            headers_json=self.headers_json.get().strip(),
            candidate_provider=self.candidate_provider.get().strip(),
            candidate_model=self.candidate_model.get().strip(),
            candidate_api_key=self.candidate_api_key.get().strip(),
            max_frames=int(self.max_frames.get().strip()),
            skip_object_map=self.skip_object_map.get(),
            run_repair=self.run_repair.get(),
            repair_provider=self.repair_provider.get().strip(),
            repair_model=self.repair_model.get().strip(),
            repair_api_key=self.repair_api_key.get().strip(),
            run_judge=self.run_judge.get(),
            judge_provider=self.judge_provider.get().strip(),
            judge_model=self.judge_model.get().strip(),
            judge_api_key=self.judge_api_key.get().strip(),
            output_dir=self.output_dir.get().strip() or "outputs",
            output_prefix=self.output_prefix.get().strip() or "atlas_pipeline",
            save_debug_files=self.save_debug_files.get(),
        )

    def _run_clicked(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        err = self._validate()
        if err:
            messagebox.showerror("Invalid Input", err)
            return
        self.run_btn.configure(state=tk.DISABLED)
        self.status.set("Running...")
        self._append("Starting pipeline...")
        self._worker = threading.Thread(target=self._run_worker, daemon=True)
        self._worker.start()

    def _run_worker(self) -> None:
        try:
            cfg = self._build_config()
            summary = pipeline_runner.run_pipeline(cfg)
            self._queue.put({"kind": "done", "summary": summary})
        except Exception:
            self._queue.put({"kind": "error", "trace": traceback.format_exc()})

    def _poll_queue(self) -> None:
        try:
            while True:
                payload = self._queue.get_nowait()
                kind = payload.get("kind")
                if kind == "done":
                    self.run_btn.configure(state=tk.NORMAL)
                    self.status.set("Completed")
                    summary = payload["summary"]
                    self.last_summary = summary
                    files = summary.get("files", {}) if isinstance(summary.get("files"), dict) else {}
                    final_json = str(files.get("final", ""))
                    final_report = str(files.get("final_report", ""))
                    self._append(f"Completed episode: {summary.get('episode_id', 'episode')}")
                    self._append(f"Final JSON: {final_json or '(missing)'}")
                    self._append(f"Final Report: {final_report or '(missing)'}")
                    self._append(f"Segments: {summary.get('final_segments_count', 0)}")

                    if self.auto_open_report.get() and final_report and Path(final_report).exists():
                        try:
                            os.startfile(final_report)  # type: ignore[attr-defined]
                        except Exception as exc:
                            self._append(f"Could not auto-open report: {exc}")

                    messagebox.showinfo(
                        "Pipeline Completed",
                        "Pipeline finished successfully.\n\n"
                        f"Final JSON:\n{final_json}\n\n"
                        f"Final Report:\n{final_report}",
                    )
                elif kind == "error":
                    self.run_btn.configure(state=tk.NORMAL)
                    self.status.set("Failed")
                    trace = str(payload.get("trace", "Unknown error"))
                    self._append("ERROR:\n" + trace)
                    messagebox.showerror("Pipeline Failed", "Pipeline failed. Check logs.")
        except queue.Empty:
            pass
        finally:
            self.root.after(120, self._poll_queue)


def json_dumps(payload: Dict[str, Any]) -> str:
    import json

    return json.dumps(payload, indent=2, ensure_ascii=False)


def main() -> None:
    root = tk.Tk()
    style = ttk.Style(root)
    if "vista" in style.theme_names():
        style.theme_use("vista")
    app = PipelineGUI(root)
    root.mainloop()
    del app


if __name__ == "__main__":
    main()
