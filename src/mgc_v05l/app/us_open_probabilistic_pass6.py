"""Runner for the U.S. open NDX risk-shaping feasibility pass."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ..research.us_open_follow_through.probabilistic_pass6 import run_probabilistic_pass6


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PASS1_ROOT = REPO_ROOT / "outputs" / "reports" / "us_open_probabilistic_pass1" / "index_universe_20200101_20260421_v2"
DEFAULT_WAREHOUSE_ROOT = REPO_ROOT / "outputs" / "research_platform" / "warehouse" / "historical_evaluator"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "us_open_probabilistic_pass6" / "index_universe_20200101_20260421_v1"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="us-open-probabilistic-pass6")
    parser.add_argument("--pass1-root", default=str(DEFAULT_PASS1_ROOT), help="Pass 1 artifact root.")
    parser.add_argument("--warehouse-root", default=str(DEFAULT_WAREHOUSE_ROOT), help="Warehouse root for raw 1m path data.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT), help="Pass 6 artifact output directory.")
    return parser


def run_us_open_probabilistic_pass6(*, pass1_root: Path, warehouse_root: Path, output_dir: Path) -> dict[str, Any]:
    result = run_probabilistic_pass6(
        pass1_root=pass1_root.resolve(),
        warehouse_root=warehouse_root.resolve(),
        output_dir=output_dir.resolve(),
    )
    return {
        "output_dir": str(output_dir.resolve()),
        "simulation_csv": result["artifacts"]["simulation_csv"],
        "stop_horizon_summary_table": result["artifacts"]["stop_horizon_summary_table"],
        "tail_risk_reduction_table": result["artifacts"]["tail_risk_reduction_table"],
        "stop_damage_table": result["artifacts"]["stop_damage_table"],
        "dev_holdout_stability_table": result["artifacts"]["dev_holdout_stability_table"],
        "summary_json": result["artifacts"]["summary_json"],
        "summary_markdown": result["artifacts"]["summary_markdown"],
        "storage_manifest": result["artifacts"]["storage_manifest"],
        "classification": result["summary"]["classification"],
        "row_counts": result["summary"]["row_counts"],
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_us_open_probabilistic_pass6(
        pass1_root=Path(args.pass1_root),
        warehouse_root=Path(args.warehouse_root),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
