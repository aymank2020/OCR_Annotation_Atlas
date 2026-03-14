"""
Build Few-shot examples + context bundle for Vertex cached content.

Inputs:
- outputs/episodes_review_index.json
- outputs/triplet_compare_results.jsonl (optional)
- outputs/triplet_compare/*.json

Outputs (default under outputs/vertex_fewshot):
- fewshot_examples.jsonl
- fewshot_examples.txt
- context_bundle.txt
- project_files_manifest.txt
- metadata.json
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from atlas_triplet_compare import parse_timed_segments_payload, segments_to_timed_text


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists() or not path.is_file():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not path.exists() or not path.is_file():
        return out
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = str(raw or "").strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


def _candidate_result_paths(results_jsonl: Path, results_dir: Path) -> List[Path]:
    seen: set[str] = set()
    out: List[Path] = []

    for row in _load_jsonl(results_jsonl):
        p_raw = str(row.get("result_path") or "").strip()
        if not p_raw:
            continue
        p = Path(p_raw)
        if not p.is_absolute():
            p = (results_jsonl.parent / p).resolve()
        if p.exists() and p.is_file():
            key = str(p.resolve())
            if key not in seen:
                seen.add(key)
                out.append(p.resolve())

    if results_dir.exists() and results_dir.is_dir():
        for p in sorted(results_dir.glob("triplet_compare_*.json")):
            key = str(p.resolve())
            if key not in seen:
                seen.add(key)
                out.append(p.resolve())
    return out


def _episode_id_from_result_path(path: Path) -> str:
    m = re.match(r"triplet_compare_(.+)\.json$", path.name)
    if m:
        return str(m.group(1) or "").strip().lower()
    return ""


def _try_resolve_file(path_value: Any, *, base_dir: Path) -> Optional[Path]:
    raw = str(path_value or "").strip()
    if not raw:
        return None
    p = Path(raw)
    if not p.is_absolute():
        p = (base_dir / p).resolve()
    if p.exists() and p.is_file():
        return p
    return None


def _to_text_from_file(path: Path) -> str:
    raw = path.read_text(encoding="utf-8", errors="replace")
    if path.suffix.lower() != ".json":
        return raw.strip()
    try:
        payload = json.loads(raw)
    except Exception:
        return raw.strip()
    segments = parse_timed_segments_payload(payload)
    if segments:
        return segments_to_timed_text(segments)
    if isinstance(payload, dict):
        for key in ("text", "labels", "content"):
            v = payload.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return raw.strip()


def _truncate(text: str, max_chars: int) -> str:
    txt = str(text or "").strip()
    if max_chars <= 0 or len(txt) <= max_chars:
        return txt
    tail = f"\n... [truncated at {max_chars} chars]"
    return txt[:max_chars].rstrip() + tail


def _first_text(paths: Iterable[Path], max_chars: int) -> Tuple[str, str]:
    for p in paths:
        if not p.exists() or not p.is_file():
            continue
        try:
            return _truncate(_to_text_from_file(p), max_chars), str(p)
        except Exception:
            continue
    return "", ""


def _build_candidates(detail: Dict[str, Any], episode_id: str, outputs_dir: Path, max_chars: int) -> Dict[str, Any]:
    refs = detail.get("text_refs", {}) if isinstance(detail.get("text_refs"), dict) else {}
    base_dir = detail.get("output_path", "")
    base_path = Path(str(base_dir or "")).resolve().parent if str(base_dir or "").strip() else outputs_dir

    tier2_paths: List[Path] = []
    api_paths: List[Path] = []
    chat_paths: List[Path] = []
    vertex_paths: List[Path] = []

    for key in ("resolved_tier2_path", "tier2_path"):
        p = _try_resolve_file(refs.get(key), base_dir=base_path)
        if p is not None:
            tier2_paths.append(p)

    for key in ("resolved_api_path", "api_path"):
        p = _try_resolve_file(refs.get(key), base_dir=base_path)
        if p is not None:
            api_paths.append(p)
    p = _try_resolve_file(outputs_dir / f"text_{episode_id}_update.txt", base_dir=base_path)
    if p is not None:
        api_paths.append(p)

    for key in ("resolved_chat_path", "chat_path", "labels_path"):
        p = _try_resolve_file(refs.get(key), base_dir=base_path)
        if p is not None:
            chat_paths.append(p)
    p = _try_resolve_file(outputs_dir / "chat_reviews" / episode_id / f"text_{episode_id}_chat.txt", base_dir=base_path)
    if p is not None:
        chat_paths.append(p)
    p = _try_resolve_file(outputs_dir / "chat_reviews" / episode_id / f"labels_{episode_id}.json", base_dir=base_path)
    if p is not None:
        chat_paths.append(p)

    for key in ("resolved_vertex_chat_path", "vertex_chat_path"):
        p = _try_resolve_file(refs.get(key), base_dir=base_path)
        if p is not None:
            vertex_paths.append(p)
    p = _try_resolve_file(outputs_dir / "vertex_chat_reviews" / episode_id / f"text_{episode_id}_vertex_chat.txt", base_dir=base_path)
    if p is not None:
        vertex_paths.append(p)
    p = _try_resolve_file(outputs_dir / "vertex_chat_reviews" / episode_id / f"labels_{episode_id}_vertex_chat.json", base_dir=base_path)
    if p is not None:
        vertex_paths.append(p)

    tier2_text, tier2_src = _first_text(tier2_paths, max_chars)
    api_text, api_src = _first_text(api_paths, max_chars)
    chat_text, chat_src = _first_text(chat_paths, max_chars)
    vertex_text, vertex_src = _first_text(vertex_paths, max_chars)

    return {
        "tier2": {"text": tier2_text, "source_path": tier2_src},
        "api": {"text": api_text, "source_path": api_src},
        "chat": {"text": chat_text, "source_path": chat_src},
        "vertex_chat": {"text": vertex_text, "source_path": vertex_src},
    }


def _judge_label(judge: Dict[str, Any], pass_score_threshold: int) -> str:
    winner = str(judge.get("winner") or "").strip().lower()
    submit_safe = str(judge.get("submit_safe_solution") or "").strip().lower()
    scores = judge.get("scores", {})
    winner_score = None
    if isinstance(scores, dict):
        try:
            winner_score = int(float(scores.get(winner))) if winner else None
        except Exception:
            winner_score = None
    if winner == "none":
        return "FAIL"
    if submit_safe and submit_safe != winner:
        return "FAIL"
    if winner_score is not None and winner_score < int(pass_score_threshold):
        return "FAIL"
    return "PASS"


def _project_manifest_lines(repo_root: Path) -> List[str]:
    candidates = [
        "atlas_triplet_compare.py",
        "atlas_triplet_batch.py",
        "atlas_review_viewer_gen.py",
        "atlas_auto_sync_and_rebuild.py",
        "atlas_web_auto_solver.py",
        "validator.py",
        "repair_payload_builder.py",
        "vertex_create_cache.py",
        "sample_web_auto_solver.yaml",
        "prompts/system_prompt.txt",
        "prompts/atlas_vertex_context_pack.txt",
    ]
    out: List[str] = []
    for rel in candidates:
        p = (repo_root / rel).resolve()
        if p.exists() and p.is_file():
            out.append(rel)
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Build Vertex few-shot/context bundle from compare results")
    ap.add_argument("--index", default="outputs/episodes_review_index.json")
    ap.add_argument("--results-jsonl", default="outputs/triplet_compare_results.jsonl")
    ap.add_argument("--results-dir", default="outputs/triplet_compare")
    ap.add_argument("--outputs-dir", default="outputs")
    ap.add_argument("--system-prompt", default="prompts/system_prompt.txt")
    ap.add_argument("--out-dir", default="outputs/vertex_fewshot")
    ap.add_argument("--max-examples", type=int, default=0, help="0 = all")
    ap.add_argument("--pass-score-threshold", type=int, default=70)
    ap.add_argument("--max-chars-per-candidate", type=int, default=3500)
    args = ap.parse_args()

    repo_root = Path(".").resolve()
    outputs_dir = Path(args.outputs_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    index_payload = _load_json(Path(args.index).resolve(), default={})
    episodes = index_payload.get("episodes", []) if isinstance(index_payload, dict) else []
    episode_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(episodes, list):
        for ep in episodes:
            if not isinstance(ep, dict):
                continue
            eid = str(ep.get("episode_id") or "").strip().lower()
            if eid:
                episode_map[eid] = ep

    result_paths = _candidate_result_paths(Path(args.results_jsonl).resolve(), Path(args.results_dir).resolve())
    examples: List[Dict[str, Any]] = []
    pass_count = 0
    fail_count = 0

    for result_path in result_paths:
        detail = _load_json(result_path, default={})
        if not isinstance(detail, dict):
            continue
        judge = detail.get("judge_result", {})
        if not isinstance(judge, dict):
            continue
        eid = _episode_id_from_result_path(result_path)
        if not eid:
            refs = detail.get("text_refs", {})
            if isinstance(refs, dict):
                raw = str(refs.get("tier2_path") or refs.get("api_path") or "").strip()
                m = re.search(r"text_([a-z0-9]+)_[a-z_]+\.txt", raw, flags=re.IGNORECASE)
                if m:
                    eid = str(m.group(1) or "").strip().lower()
        if not eid:
            continue

        candidates = _build_candidates(detail, eid, outputs_dir, max_chars=max(200, int(args.max_chars_per_candidate)))
        status = str((episode_map.get(eid) or {}).get("review_status") or "").strip().lower()
        label = _judge_label(judge, pass_score_threshold=max(0, int(args.pass_score_threshold)))
        if label == "PASS":
            pass_count += 1
        else:
            fail_count += 1

        record = {
            "episode_id": eid,
            "review_status": status,
            "result_path": str(result_path),
            "label": label,
            "input": {
                "tier2_before": candidates.get("tier2", {}),
                "tier3_api_after": candidates.get("api", {}),
                "gemini_chat_timed": candidates.get("chat", {}),
                "vertex_chat_timed": candidates.get("vertex_chat", {}),
            },
            "output": {
                "winner": str(judge.get("winner") or "").strip().lower(),
                "submit_safe_solution": str(judge.get("submit_safe_solution") or "").strip().lower(),
                "scores": judge.get("scores", {}),
                "hallucination": judge.get("hallucination", {}),
                "major_issues": judge.get("major_issues", {}),
                "best_reason_short": str(judge.get("best_reason_short") or "").strip(),
                "final_recommendation": str(judge.get("final_recommendation") or "").strip(),
            },
        }
        examples.append(record)

    if args.max_examples and int(args.max_examples) > 0:
        examples = examples[: int(args.max_examples)]

    fewshot_jsonl = out_dir / "fewshot_examples.jsonl"
    fewshot_txt = out_dir / "fewshot_examples.txt"
    manifest_txt = out_dir / "project_files_manifest.txt"
    context_bundle_txt = out_dir / "context_bundle.txt"
    metadata_json = out_dir / "metadata.json"

    with fewshot_jsonl.open("w", encoding="utf-8") as f:
        for rec in examples:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    blocks: List[str] = []
    for i, rec in enumerate(examples, start=1):
        inp = rec.get("input", {}) if isinstance(rec.get("input"), dict) else {}
        out = rec.get("output", {}) if isinstance(rec.get("output"), dict) else {}
        blocks.append(
            "\n".join(
                [
                    f"[Example {i}] label={rec.get('label')} episode_id={rec.get('episode_id')} status={rec.get('review_status')}",
                    "[Tier2 (Before)]",
                    str(((inp.get("tier2_before") or {}).get("text") or "")).strip() or "(missing)",
                    "[Tier3/API (After)]",
                    str(((inp.get("tier3_api_after") or {}).get("text") or "")).strip() or "(missing)",
                    "[Gemini Chat (Timed)]",
                    str(((inp.get("gemini_chat_timed") or {}).get("text") or "")).strip() or "(missing)",
                    "[Vertex Chat (Timed)]",
                    str(((inp.get("vertex_chat_timed") or {}).get("text") or "")).strip() or "(missing)",
                    "[Reference Verdict JSON]",
                    json.dumps(out, ensure_ascii=False, indent=2),
                ]
            ).strip()
        )
    fewshot_txt.write_text("\n\n" + ("\n\n".join(blocks) if blocks else "(no examples found)\n"), encoding="utf-8")

    system_prompt_path = Path(args.system_prompt).resolve()
    system_prompt_text = ""
    if system_prompt_path.exists() and system_prompt_path.is_file():
        system_prompt_text = system_prompt_path.read_text(encoding="utf-8", errors="replace").strip()
    if not system_prompt_text:
        system_prompt_text = (
            "You are a Strict Atlas Annotation QA Judge.\n"
            "Apply timeline continuity, max 2 atomic actions, and anti-hallucination rules."
        )

    manifest_lines = _project_manifest_lines(repo_root)
    manifest_txt.write_text("\n".join(manifest_lines) + ("\n" if manifest_lines else ""), encoding="utf-8")

    context_bundle = "\n\n".join(
        [
            "[SYSTEM INSTRUCTIONS]",
            system_prompt_text,
            "[PROJECT FILES MANIFEST]",
            manifest_txt.read_text(encoding="utf-8", errors="replace").strip(),
            "[FEW-SHOT EXAMPLES]",
            fewshot_txt.read_text(encoding="utf-8", errors="replace").strip(),
        ]
    ).strip()
    context_bundle_txt.write_text(context_bundle + "\n", encoding="utf-8")

    meta = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "examples_count": len(examples),
        "pass_count": pass_count,
        "fail_count": fail_count,
        "index_path": str(Path(args.index).resolve()),
        "results_jsonl": str(Path(args.results_jsonl).resolve()),
        "results_dir": str(Path(args.results_dir).resolve()),
        "system_prompt_path": str(system_prompt_path),
        "fewshot_jsonl": str(fewshot_jsonl),
        "fewshot_txt": str(fewshot_txt),
        "context_bundle_txt": str(context_bundle_txt),
        "project_manifest_txt": str(manifest_txt),
    }
    metadata_json.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"[fewshot-build] examples={len(examples)} pass={pass_count} fail={fail_count}")
    print(f"[fewshot-build] fewshot_jsonl={fewshot_jsonl}")
    print(f"[fewshot-build] context_bundle={context_bundle_txt}")
    print(f"[fewshot-build] metadata={metadata_json}")


if __name__ == "__main__":
    main()
