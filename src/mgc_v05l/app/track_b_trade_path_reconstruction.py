"""CLI for canonical trade path reconstruction."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_trade_path_reconstruction import (
    DEFAULT_FORWARD_CAPTURE_PATH,
    DEFAULT_OUTCOMES_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SIDE_SESSION_REPLAY_PATH,
    run_trade_path_reconstruction,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconstruct completed trade price paths from retained analytics market-data artifacts.")
    parser.add_argument("--outcomes-path", type=Path, default=DEFAULT_OUTCOMES_PATH)
    parser.add_argument("--side-session-replay-path", type=Path, default=DEFAULT_SIDE_SESSION_REPLAY_PATH)
    parser.add_argument("--forward-capture-path", type=Path, default=DEFAULT_FORWARD_CAPTURE_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_trade_path_reconstruction(
        outcomes_path=args.outcomes_path,
        side_session_replay_path=args.side_session_replay_path,
        forward_capture_path=args.forward_capture_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    overall = result.summary.get("overall", {})
    print(
        json.dumps(
            {
                "schema_version": result.summary.get("schema_version"),
                "completed_trades": overall.get("completed_trades"),
                "entry_to_exit_path_available_count": overall.get("entry_to_exit_path_available_count"),
                "entry_to_exit_path_coverage": overall.get("entry_to_exit_path_coverage"),
                "mfe_coverage": overall.get("mfe_coverage"),
                "mae_coverage": overall.get("mae_coverage"),
                "timebox_counterfactual_ready_count": overall.get("timebox_counterfactual_ready_count"),
                "trailing_exit_ready_count": overall.get("trailing_exit_ready_count"),
                "retained_path_capture_count": overall.get("retained_path_capture_count"),
                "retained_reused_path_count": overall.get("retained_reused_path_count"),
                "summary_path": str(result.summary_json_path),
                "path_jsonl_path": str(result.path_jsonl_path),
                "retained_path_capture_path": str(result.retained_path_capture_path),
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
