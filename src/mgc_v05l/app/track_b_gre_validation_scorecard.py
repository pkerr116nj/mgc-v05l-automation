"""CLI for the diagnostic-only GRE validation scorecard."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT
from mgc_v05l.execution_core.track_b_gre_validation_scorecard import run_gre_validation_scorecard


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a diagnostic-only scorecard from GRE validation rows. "
            "No broker, runtime, strategy, or Managed Exit authority is invoked."
        )
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--rows-path", type=Path, help="Optional explicit gre_validation_rows.jsonl path.")
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic scorecard generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_gre_validation_scorecard(output_root=args.output_root, rows_path=args.rows_path, now=args.now)
    scorecard = result.scorecard
    print(
        json.dumps(
            {
                "schema_version": scorecard.get("schema_version"),
                "readiness": scorecard.get("readiness_assessment", {}).get("classification"),
                "overall_metrics": scorecard.get("overall_metrics"),
                "json_path": str(result.json_path),
                "markdown_path": str(result.markdown_path),
                "diagnostic_only": scorecard.get("diagnostic_only"),
                "broker_authority": scorecard.get("broker_authority"),
                "runtime_authority": scorecard.get("runtime_authority"),
                "strategy_authority": scorecard.get("strategy_authority"),
                "managed_exit_authority": scorecard.get("managed_exit_authority"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
