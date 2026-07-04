"""CLI for CAE saved-query presets, validation, and read-only execution."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_canonical_analytics_saved_queries import (
    DEFAULT_ENRICHMENTS_PATH,
    DEFAULT_OUTCOMES_PATH,
    DEFAULT_OUTPUT_DIR,
    load_saved_queries,
    preset_saved_queries,
    publish_saved_query_artifacts,
    run_saved_query,
    validate_saved_query,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage diagnostic Canonical Analytics Engine saved queries.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--outcomes-path", type=Path, default=DEFAULT_OUTCOMES_PATH)
    parser.add_argument("--enrichments-path", type=Path, default=DEFAULT_ENRICHMENTS_PATH)
    parser.add_argument("--action", choices=("export-summary", "list-presets", "validate", "run"), default="export-summary")
    parser.add_argument("--saved-query-id", help="Saved query id for --action run.")
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic preset/report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.action == "list-presets":
        presets = preset_saved_queries(now=args.now)
        print(json.dumps({"preset_count": len(presets), "preset_ids": [preset.saved_query_id for preset in presets]}, sort_keys=True))
        return 0
    published = publish_saved_query_artifacts(output_dir=args.output_dir, now=args.now)
    if args.action == "validate":
        rows = [validation.to_record() for validation in (validate_saved_query(query) for query in load_saved_queries(args.output_dir / "saved_analytics_queries.jsonl"))]
        print(json.dumps({"validation_count": len(rows), "validations": rows}, sort_keys=True))
        return 0
    if args.action == "run":
        queries = {query.saved_query_id: query for query in load_saved_queries(args.output_dir / "saved_analytics_queries.jsonl")}
        if not args.saved_query_id or args.saved_query_id not in queries:
            print(json.dumps({"error": "saved_query_id_not_found", "saved_query_id": args.saved_query_id}, sort_keys=True))
            return 2
        run_result = run_saved_query(
            queries[args.saved_query_id],
            outcomes_path=args.outcomes_path,
            enrichments_path=args.enrichments_path,
            output_dir=args.output_dir,
            write_execution_audit=True,
            generated_at=args.now,
        )
        print(json.dumps({
            "saved_query_id": run_result.saved_query.saved_query_id,
            "validation": run_result.validation.to_record(),
            "group_count": (run_result.result or {}).get("summary", {}).get("group_count"),
            "matched_count": (run_result.result or {}).get("summary", {}).get("matched_count"),
            "execution_id": (run_result.execution_record or {}).get("execution_id"),
            "query_fingerprint": (run_result.execution_record or {}).get("query_fingerprint"),
            "result_fingerprint": (run_result.execution_record or {}).get("result_fingerprint"),
            "diagnostic_only": (run_result.result or {}).get("diagnostic_only"),
            "production_recommendation": (run_result.result or {}).get("production_recommendation"),
            "trading_gate": (run_result.result or {}).get("trading_gate"),
        }, sort_keys=True))
        return 0
    print(json.dumps({
        "summary_path": str(published["summary_path"]),
        "saved_queries_path": str(published["saved_queries_path"]),
        "presets_path": str(published["presets_path"]),
        "summary": published["summary"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
