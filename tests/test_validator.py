import unittest

import validator


class TestValidator(unittest.TestCase):
    def test_normalize_annotation_from_step_format(self):
        raw = [
            {
                "step": 1,
                "start": "0:00.0",
                "end": "0:03.2",
                "description": "grab picture from pile",
            },
            {
                "step": 2,
                "start": "0:03.2",
                "end": "0:07.4",
                "description": "place picture on frame",
            },
        ]
        ann = validator.normalize_annotation(raw, episode_id="f01", video_duration_sec=10)
        self.assertEqual(ann["episode_id"], "f01")
        self.assertEqual(len(ann["segments"]), 2)
        self.assertAlmostEqual(ann["segments"][0]["start_sec"], 0.0, places=2)
        self.assertAlmostEqual(ann["segments"][1]["end_sec"], 7.4, places=2)

    def test_validate_episode_detects_label_violations(self):
        ann = validator.normalize_annotation(
            {
                "episode_id": "e1",
                "video_duration_sec": 4.0,
                "segments": [
                    {
                        "segment_index": 1,
                        "start_sec": 0.0,
                        "end_sec": 4.0,
                        "duration_sec": 4.0,
                        "label": "inspect 3 items",
                        "granularity": "coarse",
                        "primary_goal": "inspect items",
                        "primary_object": "items",
                        "confidence": 0.9,
                    }
                ],
            }
        )
        report = validator.validate_episode(ann)
        errors = report["segment_reports"][0]["errors"]
        self.assertIn("forbidden_verbs", errors)
        self.assertIn("numerals_present", errors)
        self.assertFalse(report["ok"])

    def test_validate_episode_detects_overlap(self):
        ann = validator.normalize_annotation(
            {
                "episode_id": "e2",
                "video_duration_sec": 8.0,
                "segments": [
                    {
                        "segment_index": 1,
                        "start_sec": 0.0,
                        "end_sec": 4.0,
                        "duration_sec": 4.0,
                        "label": "pick up tool",
                        "granularity": "coarse",
                        "primary_goal": "pick up tool",
                        "primary_object": "tool",
                        "confidence": 0.8,
                    },
                    {
                        "segment_index": 2,
                        "start_sec": 3.5,
                        "end_sec": 6.0,
                        "duration_sec": 2.5,
                        "label": "place tool on table",
                        "granularity": "coarse",
                        "primary_goal": "place tool",
                        "primary_object": "tool",
                        "confidence": 0.8,
                    },
                ],
            }
        )
        report = validator.validate_episode(ann)
        self.assertIn("timestamp_overlap", report["episode_errors"])

    def test_validate_episode_rejects_more_than_two_atomic_actions(self):
        ann = validator.normalize_annotation(
            {
                "episode_id": "e3",
                "video_duration_sec": 8.0,
                "segments": [
                    {
                        "segment_index": 1,
                        "start_sec": 0.0,
                        "end_sec": 4.0,
                        "duration_sec": 4.0,
                        "label": "pick up cup, place cup on table, move cup to sink",
                        "granularity": "dense",
                        "primary_goal": "relocate cup",
                        "primary_object": "cup",
                        "confidence": 0.9,
                    }
                ],
            }
        )
        report = validator.validate_episode(ann)
        errors = report["segment_reports"][0]["errors"]
        self.assertIn("too_many_atomic_actions", errors)
        self.assertFalse(report["ok"])

    def test_validate_episode_rejects_disallowed_tool_terms(self):
        ann = validator.normalize_annotation(
            {
                "episode_id": "e4",
                "video_duration_sec": 4.0,
                "segments": [
                    {
                        "segment_index": 1,
                        "start_sec": 0.0,
                        "end_sec": 4.0,
                        "duration_sec": 4.0,
                        "label": "use mechanical arm to pick up block",
                        "granularity": "coarse",
                        "primary_goal": "pick up block",
                        "primary_object": "block",
                        "confidence": 0.8,
                    }
                ],
            }
        )
        report = validator.validate_episode(ann)
        errors = report["segment_reports"][0]["errors"]
        self.assertIn("disallowed_tool_terms", errors)
        self.assertFalse(report["ok"])

    def test_starts_with_allowed_action_verb_accepts_multi_action_phrase(self):
        self.assertTrue(validator.starts_with_allowed_action_verb("pick up component, connect wires to component"))
        self.assertTrue(validator.starts_with_allowed_action_verb("connect wires to component"))

    def test_validate_episode_rejects_label_that_starts_with_noun_or_adjective(self):
        ann = validator.normalize_annotation(
            {
                "episode_id": "e5",
                "video_duration_sec": 4.0,
                "segments": [
                    {
                        "segment_index": 1,
                        "start_sec": 0.0,
                        "end_sec": 4.0,
                        "duration_sec": 4.0,
                        "label": "internal laptop component with cloth",
                        "granularity": "coarse",
                        "primary_goal": "clean component",
                        "primary_object": "component",
                        "confidence": 0.8,
                    }
                ],
            }
        )
        report = validator.validate_episode(ann)
        errors = report["segment_reports"][0]["errors"]
        self.assertIn("starts_with_non_action_verb", errors)
        self.assertIn("action_clause_starts_with_non_action_verb", errors)
        self.assertFalse(report["ok"])


if __name__ == "__main__":
    unittest.main()
