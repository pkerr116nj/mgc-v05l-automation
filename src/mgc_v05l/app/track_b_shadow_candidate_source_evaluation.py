"""CLI for shadow observation candidate-source evaluation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT
from mgc_v05l.execution_core.track_b_shadow_candidate_source_evaluation import run_shadow_candidate_source_evaluation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate diagnostic candidate sources for GRE shadow observations. No runtime hook or trading gate is invoked."
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    parser.add_argument("--observation-rows-path", type=Path)
    parser.add_argument("--historical-rows-path", type=Path)
    parser.add_argument("--crfd-rows-path", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_shadow_candidate_source_evaluation(
        output_root=args.output_root,
        now=args.now,
        observation_rows_path=args.observation_rows_path,
        historical_rows_path=args.historical_rows_path,
        crfd_rows_path=args.crfd_rows_path,
    )
    print(
        json.dumps(
            {
                "schema_version": result.report.get("schema_version"),
                "canonical_v1": result.report.get("canonical_recommendation", {}).get(
                    "canonical_shadow_observation_candidate_source_v1"
                ),
                "population_count": len(result.report.get("candidate_populations") or []),
                "json_path": str(result.json_path),
                "markdown_path": str(result.markdown_path),
                "diagnostic_only": result.report.get("diagnostic_only"),
                "production_effect": result.report.get("production_effect"),
                "runtime_hook": result.report.get("safety_contract", {}).get("runtime_hook"),
                "trading_gate": result.report.get("safety_contract", {}).get("trading_gate"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
