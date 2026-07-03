"""CLI for context-aware Track B trade analytics."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_context_aware_trade_analytics import (
    DEFAULT_ENRICHMENT_SUMMARY_PATH,
    DEFAULT_ENRICHMENTS_PATH,
    DEFAULT_OUTCOMES_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_T3_SCORECARD_PATH,
    run_context_aware_trade_analytics,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build diagnostic context-aware analytics from canonical trade outcomes and enrichment."
    )
    parser.add_argument("--outcomes-path", type=Path, default=DEFAULT_OUTCOMES_PATH)
    parser.add_argument("--enrichments-path", type=Path, default=DEFAULT_ENRICHMENTS_PATH)
    parser.add_argument("--enrichment-summary-path", type=Path, default=DEFAULT_ENRICHMENT_SUMMARY_PATH)
    parser.add_argument("--t3-scorecard-path", type=Path, default=DEFAULT_T3_SCORECARD_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_context_aware_trade_analytics(
        outcomes_path=args.outcomes_path,
        enrichments_path=args.enrichments_path,
        enrichment_summary_path=args.enrichment_summary_path,
        t3_scorecard_path=args.t3_scorecard_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    overall = result.analytics.get("overall", {})
    print(
        json.dumps(
            {
                "schema_version": result.analytics.get("schema_version"),
                "joined_outcome_count": overall.get("joined_outcome_count"),
                "vix_context_count": overall.get("vix_context_count"),
                "vix_coverage": overall.get("vix_coverage"),
                "average_pnl_proxy": overall.get("average_pnl_proxy"),
                "win_rate": overall.get("win_rate"),
                "json_path": str(result.json_path),
                "markdown_path": str(result.markdown_path),
                "diagnostic_only": result.analytics.get("diagnostic_only"),
                "production_effect": result.analytics.get("production_effect"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

