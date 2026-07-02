"""CLI for the Track B canonical trade analytics query layer."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_trade_analytics_query_layer import (
    DEFAULT_BLOCKED_INTENTS,
    DEFAULT_CANONICAL_TRADE_RECORDS,
    DEFAULT_CRFD_ROWS,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PAIRING_SUMMARY,
    DEFAULT_SIDE_SESSION_REPLAY,
    run_trade_analytics_query_layer,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build read-only Track B trade analytics scorecards. No broker/runtime/strategy authority is invoked."
    )
    parser.add_argument("--canonical-records-path", type=Path, default=DEFAULT_CANONICAL_TRADE_RECORDS)
    parser.add_argument("--pairing-summary-path", type=Path, default=DEFAULT_PAIRING_SUMMARY)
    parser.add_argument("--side-session-replay-path", type=Path, default=DEFAULT_SIDE_SESSION_REPLAY)
    parser.add_argument("--blocked-intents-path", type=Path, default=DEFAULT_BLOCKED_INTENTS)
    parser.add_argument("--crfd-rows-path", type=Path, default=DEFAULT_CRFD_ROWS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_trade_analytics_query_layer(
        canonical_records_path=args.canonical_records_path,
        pairing_summary_path=args.pairing_summary_path,
        side_session_replay_path=args.side_session_replay_path,
        blocked_intents_path=args.blocked_intents_path,
        crfd_rows_path=args.crfd_rows_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    overall = result.report.get("overall", {})
    print(
        json.dumps(
            {
                "schema_version": result.report.get("schema_version"),
                "canonical_record_count": overall.get("canonical_record_count"),
                "paired_trade_count": overall.get("paired_trade_count"),
                "pairing_rate": overall.get("pairing_rate"),
                "win_rate": overall.get("win_rate"),
                "expectancy_proxy": overall.get("expectancy_proxy"),
                "json_path": str(result.json_path),
                "markdown_path": str(result.markdown_path),
                "analytics_only": result.report.get("analytics_only"),
                "production_effect": result.report.get("production_effect"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
