"""CLI for diagnostic-only GRE validation logging."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT
from mgc_v05l.execution_core.track_b_gre_validation_logger import run_gre_validation_logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Append a diagnostic-only validation row for the latest Gold Regime Engine report. "
            "No broker, runtime, strategy, or Managed Exit authority is invoked."
        )
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--gre-path", type=Path, help="Optional explicit latest_gold_regime_engine.json path.")
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic validation logging.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_gre_validation_logger(output_root=args.output_root, now=args.now, gre_path=args.gre_path)
    print(
        json.dumps(
            {
                "schema_version": result.row.get("schema_version"),
                "regime_label": result.row.get("regime_label"),
                "confidence": result.row.get("confidence"),
                "validation_status": result.row.get("validation_status"),
                "available_horizons": result.row.get("available_horizons"),
                "missing_horizons": result.row.get("missing_horizons"),
                "direction_correctness": result.row.get("direction_correctness"),
                "diagnostic_only": result.row.get("diagnostic_only"),
                "rows_path": str(result.rows_path),
                "summary_path": str(result.summary_path),
                "broker_state_mutated": result.row.get("broker_state_mutated"),
                "order_intent_created": result.row.get("order_intent_created"),
                "submit_allowed": result.row.get("submit_allowed"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
