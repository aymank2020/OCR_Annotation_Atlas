import unittest

from atlas_web_auto_solver import (
    _apply_consistency_aliases_to_label,
    _autofix_label_candidate,
    _allowed_label_start_verb_token_patterns_from_cfg,
    _build_auto_continuity_merge_operations,
    _count_atomic_actions_in_label,
    _find_equivalent_canonical_term,
    _label_starts_with_allowed_action_verb,
    _normalize_operations,
    _normalize_upload_chunk_size,
    _rewrite_label_tier3,
    _update_chunk_consistency_memory,
)


class TestAtlasWebAutoSolver(unittest.TestCase):
    def test_chunk_size_is_raised_to_granularity(self) -> None:
        chunk = _normalize_upload_chunk_size(
            requested_chunk_bytes=262144,
            size_bytes=15 * 1024 * 1024,
            chunk_granularity=8 * 1024 * 1024,
        )
        self.assertEqual(chunk, 8 * 1024 * 1024)

    def test_chunk_size_is_snapped_to_multiple(self) -> None:
        chunk = _normalize_upload_chunk_size(
            requested_chunk_bytes=10 * 1024 * 1024,
            size_bytes=20 * 1024 * 1024,
            chunk_granularity=8 * 1024 * 1024,
        )
        self.assertEqual(chunk, 8 * 1024 * 1024)

    def test_small_file_uses_single_finalize_chunk(self) -> None:
        size = 5 * 1024 * 1024
        chunk = _normalize_upload_chunk_size(
            requested_chunk_bytes=1024 * 1024,
            size_bytes=size,
            chunk_granularity=8 * 1024 * 1024,
        )
        self.assertEqual(chunk, size)

    def test_normalize_operations_accepts_aliases(self) -> None:
        payload = {
            "operations": [
                {"action": "S", "segment_index": 2},
                {"action": "delete", "index": 4},
                {"op": "m", "segment": 5},
                {"action": "invalid", "segment_index": 1},
            ]
        }
        ops = _normalize_operations(payload, cfg={"run": {"max_structural_operations": 10}})
        self.assertEqual(
            ops,
            [
                {"action": "split", "segment_index": 2},
                {"action": "delete", "segment_index": 4},
                {"action": "merge", "segment_index": 5},
            ],
        )

    def test_rewrite_label_tier3_preserves_dense_style_with_cleanup(self) -> None:
        text = "cut grey fabric strip, rotate fabric, cut another grey fabric strip"
        out = _rewrite_label_tier3(text)
        self.assertIn(",", out)
        self.assertIn("adjust", out.lower())
        self.assertNotIn("rotate", out.lower())
        self.assertNotIn("another", out.lower())

    def test_rewrite_label_tier3_converts_numerals_to_words(self) -> None:
        text = "pick up 3 knives from table"
        out = _rewrite_label_tier3(text)
        self.assertEqual(out.lower(), "pick up three knives from table")
        self.assertNotRegex(out, r"\b\d+\b")

    def test_rewrite_label_tier3_normalizes_disallowed_tool_terms(self) -> None:
        text = "use mechanical arm to grab block"
        out = _rewrite_label_tier3(text)
        self.assertIn("gripper", out.lower())
        self.assertNotIn("mechanical arm", out.lower())

    def test_count_atomic_actions_counts_commas_and_and(self) -> None:
        label = "pick up cup, place cup on table and move cup to sink"
        self.assertEqual(_count_atomic_actions_in_label(label), 3)

    def test_chunk_consistency_memory_maps_case_naming(self) -> None:
        canonical_terms = ["watch case"]
        alias_map = {"watch case": "watch case"}
        out = _update_chunk_consistency_memory(
            "place digital watch case on table",
            canonical_terms=canonical_terms,
            alias_to_canonical=alias_map,
            memory_limit=40,
        )
        self.assertIn("watch case", out)
        self.assertNotIn("digital watch case", out)

    def test_chunk_consistency_memory_maps_surface_to_first_seen_table(self) -> None:
        canonical = _find_equivalent_canonical_term("surface", ["table"])
        self.assertEqual(canonical, "table")
        out = _apply_consistency_aliases_to_label(
            "place bag on surface",
            {"surface": "table"},
        )
        self.assertEqual(out, "place bag on table")

    def test_autofix_label_candidate_preserves_multi_action_clause(self) -> None:
        cfg = {"run": {"min_label_words": 2, "max_label_words": 20}}
        forbidden = ["inspect", "check", "look", "examine", "reach", "rotate", "grab", "relocate"]
        patterns = _allowed_label_start_verb_token_patterns_from_cfg(cfg)
        candidate = _autofix_label_candidate(
            cfg=cfg,
            label="pick up component, connect wires to component",
            source_label="pick up component, connect wires to component",
            forbidden_verbs=forbidden,
            allowed_verb_token_patterns=patterns,
        )
        self.assertEqual(candidate, "pick up component, connect wires to component")

    def test_autofix_label_candidate_repairs_noun_start_phrase(self) -> None:
        cfg = {"run": {"min_label_words": 2, "max_label_words": 20}}
        forbidden = ["inspect", "check", "look", "examine", "reach", "rotate", "grab", "relocate"]
        patterns = _allowed_label_start_verb_token_patterns_from_cfg(cfg)
        candidate = _autofix_label_candidate(
            cfg=cfg,
            label="internal laptop component with cloth",
            source_label="internal laptop component with cloth",
            forbidden_verbs=forbidden,
            allowed_verb_token_patterns=patterns,
        )
        self.assertTrue(_label_starts_with_allowed_action_verb(candidate, patterns))

    def test_autofix_label_candidate_adds_location_for_place(self) -> None:
        cfg = {"run": {"min_label_words": 2, "max_label_words": 20}}
        forbidden = ["inspect", "check", "look", "examine", "reach", "rotate", "grab", "relocate"]
        patterns = _allowed_label_start_verb_token_patterns_from_cfg(cfg)
        candidate = _autofix_label_candidate(
            cfg=cfg,
            label="place cup",
            source_label="place cup",
            forbidden_verbs=forbidden,
            allowed_verb_token_patterns=patterns,
        )
        self.assertEqual(candidate, "place cup on surface")

    def test_autofix_label_candidate_adds_location_inside_multi_action(self) -> None:
        cfg = {"run": {"min_label_words": 2, "max_label_words": 20}}
        forbidden = ["inspect", "check", "look", "examine", "reach", "rotate", "grab", "relocate"]
        patterns = _allowed_label_start_verb_token_patterns_from_cfg(cfg)
        candidate = _autofix_label_candidate(
            cfg=cfg,
            label="pick up cup, place cup",
            source_label="pick up cup, place cup",
            forbidden_verbs=forbidden,
            allowed_verb_token_patterns=patterns,
        )
        self.assertEqual(candidate, "pick up cup, place cup on surface")

    def test_autofix_label_candidate_preserves_separable_take_out(self) -> None:
        cfg = {"run": {"min_label_words": 2, "max_label_words": 20}}
        forbidden = ["inspect", "check", "look", "examine", "reach", "rotate", "grab", "relocate"]
        patterns = _allowed_label_start_verb_token_patterns_from_cfg(cfg)
        candidate = _autofix_label_candidate(
            cfg=cfg,
            label="take screwdriver out of bag",
            source_label="take screwdriver out of bag",
            forbidden_verbs=forbidden,
            allowed_verb_token_patterns=patterns,
        )
        self.assertTrue(_label_starts_with_allowed_action_verb(candidate, patterns))
        self.assertFalse(candidate.lower().startswith("tighten take"))

    def test_label_starts_with_allowed_action_verb_accepts_chisel(self) -> None:
        cfg = {"run": {"allowed_label_start_verbs": ["pick up", "place", "chisel"]}}
        patterns = _allowed_label_start_verb_token_patterns_from_cfg(cfg)
        self.assertTrue(_label_starts_with_allowed_action_verb("chisel metal edge", patterns))

    def test_auto_continuity_merge_rejects_alternation_pattern(self) -> None:
        cfg = {
            "run": {
                "auto_continuity_merge_enabled": True,
                "structural_allow_merge": True,
                "auto_continuity_merge_min_run_segments": 3,
                "auto_continuity_merge_min_token_overlap": 1,
            }
        }
        plan = {
            1: {"label": "sort electronic components on mat", "start_sec": 0.0, "end_sec": 5.2},
            2: {"label": "place electronic components into box", "start_sec": 5.2, "end_sec": 31.6},
            3: {"label": "place electronic components into box", "start_sec": 31.6, "end_sec": 33.5},
            4: {"label": "place electronic components into box", "start_sec": 33.5, "end_sec": 52.4},
            5: {"label": "sort electronic components on mat", "start_sec": 52.4, "end_sec": 56.2},
        }
        ops = _build_auto_continuity_merge_operations(plan, cfg)
        self.assertEqual(ops, [])

    def test_auto_continuity_merge_keeps_continuous_same_goal(self) -> None:
        cfg = {
            "run": {
                "auto_continuity_merge_enabled": True,
                "structural_allow_merge": True,
                "auto_continuity_merge_min_run_segments": 3,
                "auto_continuity_merge_min_token_overlap": 1,
            }
        }
        plan = {
            1: {"label": "place electronic component into box", "start_sec": 0.0, "end_sec": 4.0},
            2: {"label": "place electronic component into box", "start_sec": 4.0, "end_sec": 8.0},
            3: {"label": "place electronic component into box", "start_sec": 8.0, "end_sec": 12.0},
        }
        ops = _build_auto_continuity_merge_operations(plan, cfg)
        self.assertEqual(
            ops,
            [
                {"action": "merge", "segment_index": 3},
                {"action": "merge", "segment_index": 2},
            ],
        )


if __name__ == "__main__":
    unittest.main()
