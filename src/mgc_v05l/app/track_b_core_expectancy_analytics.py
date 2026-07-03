"""CLI for Track B analytics coverage and core expectancy reports."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_core_expectancy_analytics import (
    DEFAULT_ENRICHMENT_SUMMARY_PATH,
    DEFAULT_ENRICHMENTS_PATH,
    DEFAULT_OUTCOMES_PATH,
    DEFAULT_OUTPUT_DIR,
    run_core_expectancy_analytics,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build diagnostic analytics coverage and core expectancy reports.")
    parser.add_argument("--outcomes-path", type=Path, default=DEFAULT_OUTCOMES_PATH)
    parser.add_argument("--enrichments-path", type=Path, default=DEFAULT_ENRICHMENTS_PATH)
    parser.add_argument("--enrichment-summary-path", type=Path, default=DEFAULT_ENRICHMENT_SUMMARY_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_core_expectancy_analytics(
        outcomes_path=args.outcomes_path,
        enrichments_path=args.enrichments_path,
        enrichment_summary_path=args.enrichment_summary_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    print(
        json.dumps(
            {
                "schema_version": result.analytics.get("schema_version"),
                "classification_counts": _coverage_counts(result.coverage_matrix),
                "outcome_count": result.analytics.get("overall", {}).get("count"),
                "win_rate": result.analytics.get("overall", {}).get("win_rate"),
                "average_pnl_proxy": result.analytics.get("overall", {}).get("average_pnl_proxy"),
                "coverage_json_path": str(result.coverage_json_path),
                "analytics_json_path": str(result.analytics_json_path),
                "diagnostic_only": result.analytics.get("diagnostic_only"),
                "production_recommendation": result.analytics.get("production_recommendation"),
                "trading_gate": result.analytics.get("trading_gate"),
            },
            sort_keys=True,
        )
    )
    return 0


def _coverage_counts(matrix: dict) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in matrix.get("capabilities", []):
        status = str(row.get("status") or "UNKNOWN")
        counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items()))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
