import json
import tempfile
import unittest
from pathlib import Path

import pipeline_runner


class TestPipelineRunner(unittest.TestCase):
    def test_load_config(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "cfg.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        "input:",
                        "  episode_id: test_ep",
                        "  candidate_json: sample.json",
                        "  video_duration_sec: 10.0",
                        "providers:",
                        "  candidate:",
                        "    type: file",
                        "output:",
                        "  dir: out",
                        "  prefix: run",
                    ]
                ),
                encoding="utf-8",
            )
            cfg = pipeline_runner.load_config(str(cfg_path))
            self.assertEqual(cfg.episode_id, "test_ep")
            self.assertEqual(cfg.candidate_json, "sample.json")
            self.assertEqual(cfg.output_prefix, "run")

    def test_run_pipeline_with_candidate_file(self):
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            candidate_path = td_path / "candidate.json"
            candidate_payload = [
                {"step": 1, "start": "0:00.0", "end": "0:02.0", "description": "pick up tool"},
                {"step": 2, "start": "0:02.0", "end": "0:04.0", "description": "place tool on table"},
            ]
            candidate_path.write_text(json.dumps(candidate_payload), encoding="utf-8")

            cfg = pipeline_runner.PipelineConfig(
                candidate_json=str(candidate_path),
                episode_id="test_local",
                video_duration_sec=4.0,
                candidate_provider="file",
                run_repair=False,
                repair_provider="none",
                run_judge=False,
                judge_provider="none",
                output_dir=str(td_path / "out"),
                output_prefix="pipe",
            )
            summary = pipeline_runner.run_pipeline(cfg)

            self.assertIn("files", summary)
            self.assertTrue(summary["files"])
            summary_path = Path(summary["files"]["summary"])
            self.assertTrue(summary_path.exists())
            final_path = Path(summary["files"]["final"])
            self.assertTrue(final_path.exists())
            final_report_path = Path(summary["files"]["final_report"])
            self.assertTrue(final_report_path.exists())

    def test_load_config_ignores_setx_lines(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "cfg.yaml"
            cfg_path.write_text(
                "\n".join(
                    [
                        'setx GEMINI_API_KEY "abc"',
                        'setx OPENAI_API_KEY "def"',
                        "input:",
                        "  episode_id: test_ep2",
                        "  candidate_json: sample.json",
                        "providers:",
                        "  candidate:",
                        "    type: file",
                        "output:",
                        "  dir: out",
                        "  prefix: run2",
                    ]
                ),
                encoding="utf-8",
            )
            cfg = pipeline_runner.load_config(str(cfg_path))
            self.assertEqual(cfg.episode_id, "test_ep2")


if __name__ == "__main__":
    unittest.main()
