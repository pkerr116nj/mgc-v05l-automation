"""CLI for the diagnostic-only Canonical Research Feature Dataset builder."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT
from mgc_v05l.execution_core.track_b_research_feature_dataset import (
    DEFAULT_INSTRUMENTS,
    DEFAULT_TIMEFRAME,
    run_research_feature_dataset_builder,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build the diagnostic-only Canonical Research Feature Dataset from retained market data. "
            "No broker, runtime, strategy, or Managed Exit authority is invoked."
        )
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    parser.add_argument("--instruments", nargs="+", default=list(DEFAULT_INSTRUMENTS))
    parser.add_argument("--timeframe", default=DEFAULT_TIMEFRAME)
    parser.add_argument("--cadence-minutes", type=int, default=5)
    parser.add_argument("--max-rows", type=int, default=500)
    parser.add_argument("--live-observation", action="store_true", help="Mark rows backfill=false; still diagnostic-only.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_research_feature_dataset_builder(
        output_root=args.output_root,
        now=args.now,
        instruments=tuple(args.instruments),
        timeframe=args.timeframe,
        cadence_minutes=args.cadence_minutes,
        max_rows=args.max_rows,
        backfill=not args.live_observation,
    )
    print(
        json.dumps(
            {
                "schema_version": result.summary.get("schema_version"),
                "observation_count": result.summary.get("observation_count"),
                "time_coverage": result.summary.get("time_coverage"),
                "contracts": result.summary.get("contracts"),
                "readiness_for_gre": result.summary.get("readiness_for_gre"),
                "readiness_for_future_plugins": result.summary.get("readiness_for_future_plugins"),
                "rows_path": str(result.rows_path),
                "summary_path": str(result.summary_path),
                "diagnostic_only": result.summary.get("diagnostic_only"),
                "broker_authority": result.summary.get("broker_authority"),
                "runtime_authority": result.summary.get("runtime_authority"),
                "strategy_authority": result.summary.get("strategy_authority"),
                "managed_exit_authority": result.summary.get("managed_exit_authority"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
