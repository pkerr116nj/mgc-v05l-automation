"""CLI for the diagnostic Research Discovery Engine."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_research_discovery_engine import (
    DEFAULT_ENRICHMENT_SUMMARY_PATH,
    DEFAULT_ENRICHMENTS_PATH,
    DEFAULT_OUTCOMES_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_T3_SCORECARD_PATH,
    DEFAULT_T6_ANALYTICS_PATH,
    run_research_discovery_engine,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate diagnostic research hypotheses from canonical trade outcome enrichment."
    )
    parser.add_argument("--outcomes-path", type=Path, default=DEFAULT_OUTCOMES_PATH)
    parser.add_argument("--enrichments-path", type=Path, default=DEFAULT_ENRICHMENTS_PATH)
    parser.add_argument("--enrichment-summary-path", type=Path, default=DEFAULT_ENRICHMENT_SUMMARY_PATH)
    parser.add_argument("--t3-scorecard-path", type=Path, default=DEFAULT_T3_SCORECARD_PATH)
    parser.add_argument("--t6-analytics-path", type=Path, default=DEFAULT_T6_ANALYTICS_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_research_discovery_engine(
        outcomes_path=args.outcomes_path,
        enrichments_path=args.enrichments_path,
        enrichment_summary_path=args.enrichment_summary_path,
        t3_scorecard_path=args.t3_scorecard_path,
        t6_analytics_path=args.t6_analytics_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    print(
        json.dumps(
            {
                "schema_version": result.summary.get("schema_version"),
                "candidate_count": result.summary.get("input_counts", {}).get("candidates"),
                "sample_classes": result.summary.get("candidate_counts", {}).get("by_sample_class"),
                "recommendation_levels": result.summary.get("candidate_counts", {}).get("by_recommendation_level"),
                "candidates_path": str(result.candidates_path),
                "summary_path": str(result.summary_path),
                "diagnostic_only": result.summary.get("diagnostic_only"),
                "production_recommendation": result.summary.get("production_recommendation"),
                "trading_gate": result.summary.get("trading_gate"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
