"""CLI for RA9 winner vs. loser feature discovery."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_winner_loser_feature_discovery import (
    DEFAULT_ATTRIBUTIONS_PATH,
    DEFAULT_ENRICHMENTS_PATH,
    DEFAULT_OUTCOMES_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_TRADE_PATHS_PATH,
    run_winner_loser_feature_discovery,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare winner/loser entry features and emit diagnostic hypotheses.")
    parser.add_argument("--outcomes-path", type=Path, default=DEFAULT_OUTCOMES_PATH)
    parser.add_argument("--enrichments-path", type=Path, default=DEFAULT_ENRICHMENTS_PATH)
    parser.add_argument("--attributions-path", type=Path, default=DEFAULT_ATTRIBUTIONS_PATH)
    parser.add_argument("--trade-paths-path", type=Path, default=DEFAULT_TRADE_PATHS_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_winner_loser_feature_discovery(
        outcomes_path=args.outcomes_path,
        enrichments_path=args.enrichments_path,
        attributions_path=args.attributions_path,
        trade_paths_path=args.trade_paths_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    overall = result.summary.get("overall", {})
    print(
        json.dumps(
            {
                "schema_version": result.summary.get("schema_version"),
                "trade_count": overall.get("trade_count"),
                "lane_count": overall.get("lane_count"),
                "hypothesis_count": overall.get("hypothesis_count"),
                "lanes_with_hypotheses": overall.get("lanes_with_hypotheses"),
                "winner_count": overall.get("winner_count"),
                "loser_count": overall.get("loser_count"),
                "summary_path": str(result.summary_json_path),
                "hypotheses_path": str(result.hypotheses_path),
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
