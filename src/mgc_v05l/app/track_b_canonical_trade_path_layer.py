"""CLI for RA7 Canonical Trade Path layer."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_canonical_trade_path_layer import (
    DEFAULT_ATTRIBUTIONS_PATH,
    DEFAULT_ENRICHMENTS_PATH,
    DEFAULT_OUTCOMES_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RETAINED_PATHS,
    run_canonical_trade_path_layer,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Normalize retained trade paths into first-class canonical research objects.")
    parser.add_argument("--outcomes-path", type=Path, default=DEFAULT_OUTCOMES_PATH)
    parser.add_argument("--enrichments-path", type=Path, default=DEFAULT_ENRICHMENTS_PATH)
    parser.add_argument("--attributions-path", type=Path, default=DEFAULT_ATTRIBUTIONS_PATH)
    parser.add_argument("--retained-paths-path", type=Path, default=DEFAULT_RETAINED_PATHS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_canonical_trade_path_layer(
        outcomes_path=args.outcomes_path,
        enrichments_path=args.enrichments_path,
        attributions_path=args.attributions_path,
        retained_paths_path=args.retained_paths_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    overall = result.summary.get("overall", {})
    print(
        json.dumps(
            {
                "schema_version": result.summary.get("schema_version"),
                "canonical_trade_path_count": overall.get("canonical_trade_path_count"),
                "path_available_count": overall.get("path_available_count"),
                "path_available_rate": overall.get("path_available_rate"),
                "complete_path_count": overall.get("complete_path_count"),
                "partial_path_count": overall.get("partial_path_count"),
                "missing_source_count": overall.get("missing_source_count"),
                "timebox_ready_count": overall.get("timebox_ready_count"),
                "trailing_ready_count": overall.get("trailing_ready_count"),
                "vwap_avwap_ready_count": overall.get("vwap_avwap_ready_count"),
                "atr_ready_count": overall.get("atr_ready_count"),
                "canonical_paths_path": str(result.canonical_paths_path),
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
