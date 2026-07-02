"""CLI for Track B trade outcome scorecards."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_trade_outcome_scorecards import (
    DEFAULT_OUTCOMES_PATH,
    DEFAULT_OUTCOME_SUMMARY_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_T1_SCORECARD_PATH,
    run_trade_outcome_scorecards,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build diagnostic Track B trade outcome scorecards from canonical outcome records."
    )
    parser.add_argument("--outcomes-path", type=Path, default=DEFAULT_OUTCOMES_PATH)
    parser.add_argument("--outcome-summary-path", type=Path, default=DEFAULT_OUTCOME_SUMMARY_PATH)
    parser.add_argument("--t1-scorecard-path", type=Path, default=DEFAULT_T1_SCORECARD_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_trade_outcome_scorecards(
        outcomes_path=args.outcomes_path,
        outcome_summary_path=args.outcome_summary_path,
        t1_scorecard_path=args.t1_scorecard_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    overall = result.scorecards.get("overall", {})
    print(
        json.dumps(
            {
                "schema_version": result.scorecards.get("schema_version"),
                "outcome_count": overall.get("outcome_count"),
                "win_rate": overall.get("win_rate"),
                "average_realized_points": overall.get("average_realized_points"),
                "average_pnl_proxy": overall.get("average_pnl_proxy"),
                "data_quality_limited": overall.get("data_quality_limited"),
                "json_path": str(result.json_path),
                "markdown_path": str(result.markdown_path),
                "diagnostic_only": result.scorecards.get("diagnostic_only"),
                "production_effect": result.scorecards.get("production_effect"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
