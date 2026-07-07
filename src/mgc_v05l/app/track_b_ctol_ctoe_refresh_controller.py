"""CLI for AM1 CTOL/CTOE analytics refresh maintenance."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_ctol_ctoe_refresh_controller import (
    DEFAULT_CORE_EXPECTANCY_OUTPUT_DIR,
    DEFAULT_CTOE_OUTPUT_DIR,
    DEFAULT_CTOL_OUTPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    run_ctol_ctoe_refresh_controller,
)
from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_CANONICAL_TRADE_RECORDS,
    DEFAULT_CRFD_ROWS,
    DEFAULT_SIDE_SESSION_REPLAY,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Diagnose and optionally refresh stale CTOL/CTOE analytics artifacts.")
    parser.add_argument("--canonical-records-path", type=Path, default=DEFAULT_CANONICAL_TRADE_RECORDS)
    parser.add_argument("--side-session-replay-path", type=Path, default=DEFAULT_SIDE_SESSION_REPLAY)
    parser.add_argument("--crfd-rows-path", type=Path, default=DEFAULT_CRFD_ROWS)
    parser.add_argument("--ctol-output-dir", type=Path, default=DEFAULT_CTOL_OUTPUT_DIR)
    parser.add_argument("--ctoe-output-dir", type=Path, default=DEFAULT_CTOE_OUTPUT_DIR)
    parser.add_argument("--core-expectancy-output-dir", type=Path, default=DEFAULT_CORE_EXPECTANCY_OUTPUT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--refresh-if-stale", action="store_true")
    parser.add_argument("--include-core-expectancy", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_ctol_ctoe_refresh_controller(
        canonical_records_path=args.canonical_records_path,
        side_session_replay_path=args.side_session_replay_path,
        crfd_rows_path=args.crfd_rows_path,
        ctol_output_dir=args.ctol_output_dir,
        ctoe_output_dir=args.ctoe_output_dir,
        core_expectancy_output_dir=args.core_expectancy_output_dir,
        output_dir=args.output_dir,
        refresh_if_stale=args.refresh_if_stale,
        include_core_expectancy=args.include_core_expectancy,
        force=args.force,
        now=args.now,
    )
    after = result.status.get("after", {})
    staleness = after.get("staleness", {})
    print(
        json.dumps(
            {
                "schema_version": result.status.get("schema_version"),
                "classification": result.status.get("classification"),
                "ctol_stale": staleness.get("ctol_stale"),
                "ctoe_stale": staleness.get("ctoe_stale"),
                "ctol_refreshed": result.status.get("refreshed", {}).get("ctol"),
                "ctoe_refreshed": result.status.get("refreshed", {}).get("ctoe"),
                "core_expectancy_refreshed": result.status.get("refreshed", {}).get("core_expectancy"),
                "status_path": str(result.status_path),
                "diagnostic_only": result.status.get("diagnostic_only"),
                "production_recommendation": result.status.get("production_recommendation"),
                "trading_gate": result.status.get("trading_gate"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
