"""CLI for Research Discovery manual-review packs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_research_discovery_review_pack import (
    DEFAULT_CANDIDATES_PATH,
    DEFAULT_DISCOVERY_SUMMARY_PATH,
    DEFAULT_ENRICHMENTS_PATH,
    DEFAULT_OUTCOMES_PATH,
    DEFAULT_OUTPUT_DIR,
    run_research_discovery_review_pack,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a diagnostic manual-review pack for research discovery candidates.")
    parser.add_argument("--candidates-path", type=Path, default=DEFAULT_CANDIDATES_PATH)
    parser.add_argument("--discovery-summary-path", type=Path, default=DEFAULT_DISCOVERY_SUMMARY_PATH)
    parser.add_argument("--outcomes-path", type=Path, default=DEFAULT_OUTCOMES_PATH)
    parser.add_argument("--enrichments-path", type=Path, default=DEFAULT_ENRICHMENTS_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_research_discovery_review_pack(
        candidates_path=args.candidates_path,
        discovery_summary_path=args.discovery_summary_path,
        outcomes_path=args.outcomes_path,
        enrichments_path=args.enrichments_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    counts = result.review_pack.get("input_counts", {})
    print(
        json.dumps(
            {
                "schema_version": result.review_pack.get("schema_version"),
                "manual_review_candidates": counts.get("manual_review_candidates"),
                "research_grade_candidates": counts.get("research_grade_candidates"),
                "reviewed_candidates": len(result.review_pack.get("reviewed_candidates") or []),
                "json_path": str(result.json_path),
                "markdown_path": str(result.markdown_path),
                "diagnostic_only": result.review_pack.get("diagnostic_only"),
                "production_recommendation": result.review_pack.get("production_recommendation"),
                "trading_gate": result.review_pack.get("trading_gate"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
