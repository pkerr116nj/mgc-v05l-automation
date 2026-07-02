"""CLI for GRE shadow gate zero-allow diagnosis."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT
from mgc_v05l.execution_core.track_b_gre_shadow_zero_allow_diagnosis import run_zero_allow_diagnosis


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Diagnose zero-allow GRE shadow gate observations. Research/diagnostic only; no runtime hook or gate."
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    parser.add_argument("--observation-rows-path", type=Path)
    parser.add_argument("--historical-rows-path", type=Path)
    parser.add_argument("--crfd-rows-path", type=Path)
    parser.add_argument("--historical-simulation-path", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_zero_allow_diagnosis(
        output_root=args.output_root,
        now=args.now,
        observation_rows_path=args.observation_rows_path,
        historical_rows_path=args.historical_rows_path,
        crfd_rows_path=args.crfd_rows_path,
        historical_simulation_path=args.historical_simulation_path,
    )
    print(
        json.dumps(
            {
                "schema_version": result.report.get("schema_version"),
                "observation_count": result.report.get("offline_observation_diagnosis", {}).get("observation_count"),
                "result_counts": result.report.get("offline_observation_diagnosis", {}).get("result_counts"),
                "primary_driver": result.report.get("conclusions", {}).get("primary_driver"),
                "offline_comparable_to_r18": result.report.get("conclusions", {}).get("offline_population_comparable_to_r18"),
                "bug_found": result.report.get("conclusions", {}).get("bug_found_in_r21_generator"),
                "json_path": str(result.json_path),
                "markdown_path": str(result.markdown_path),
                "diagnostic_only": result.report.get("diagnostic_only"),
                "production_effect": result.report.get("production_effect"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
