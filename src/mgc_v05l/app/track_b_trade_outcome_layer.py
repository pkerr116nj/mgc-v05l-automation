"""CLI for the Track B canonical trade outcome layer."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_CANONICAL_TRADE_RECORDS,
    DEFAULT_CANONICAL_TRADE_PATHS,
    DEFAULT_CRFD_ROWS,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SIDE_SESSION_REPLAY,
    run_trade_outcome_layer,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build read-only Track B canonical trade outcome records. No broker/runtime/strategy authority is invoked."
    )
    parser.add_argument("--canonical-records-path", type=Path, default=DEFAULT_CANONICAL_TRADE_RECORDS)
    parser.add_argument("--side-session-replay-path", type=Path, default=DEFAULT_SIDE_SESSION_REPLAY)
    parser.add_argument("--crfd-rows-path", type=Path, default=DEFAULT_CRFD_ROWS)
    parser.add_argument("--canonical-trade-paths-path", type=Path, default=DEFAULT_CANONICAL_TRADE_PATHS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_trade_outcome_layer(
        canonical_records_path=args.canonical_records_path,
        side_session_replay_path=args.side_session_replay_path,
        crfd_rows_path=args.crfd_rows_path,
        canonical_trade_paths_path=args.canonical_trade_paths_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    overall = result.summary.get("overall", {})
    print(
        json.dumps(
            {
                "schema_version": result.summary.get("schema_version"),
                "outcome_count": overall.get("outcome_count"),
                "incomplete_or_unpaired_count": overall.get("incomplete_or_unpaired_count"),
                "average_realized_points": overall.get("average_realized_points"),
                "average_pnl_proxy": overall.get("average_pnl_proxy"),
                "mfe_available_count": overall.get("mfe_available_count"),
                "mae_available_count": overall.get("mae_available_count"),
                "outcomes_path": str(result.outcomes_path),
                "summary_path": str(result.summary_path),
                "diagnostic_only": result.summary.get("diagnostic_only"),
                "production_effect": result.summary.get("production_effect"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
