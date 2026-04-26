"""Runner for the Asia Drift shadow-trading validation harness."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ..research.asia_drift.shadow_trading import run_asia_drift_shadow_trading


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_WAREHOUSE_ROOT = REPO_ROOT / "outputs" / "research_platform" / "warehouse" / "historical_evaluator"
DEFAULT_PASS6_CLASSIFICATION = REPO_ROOT / "outputs" / "reports" / "asia_drift_probabilistic_pass6" / "index_stack_20200101_20260421" / "reports" / "asia_drift_probabilistic_pass6_classification.csv"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "asia_drift_shadow_trading" / "index_stack_20200101_20260421"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-shadow-trading")
    parser.add_argument("--warehouse-root", default=str(DEFAULT_WAREHOUSE_ROOT), help="Warehouse root for historical and incremental data.")
    parser.add_argument("--pass6-classification-csv", default=str(DEFAULT_PASS6_CLASSIFICATION), help="Pass 6 classification CSV.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT), help="Output directory for shadow-trading artifacts.")
    return parser


def run_asia_drift_shadow_trading_app(
    *,
    warehouse_root: Path,
    pass6_classification_csv: Path,
    output_dir: Path,
) -> dict[str, Any]:
    result = run_asia_drift_shadow_trading(
        warehouse_root=warehouse_root.resolve(),
        pass6_classification_csv=pass6_classification_csv.resolve(),
        output_dir=output_dir.resolve(),
    )
    return {
        "output_dir": str(output_dir.resolve()),
        "artifacts": result["artifacts"],
        "row_counts": result["summary"]["row_counts"],
        "scope": result["summary"]["scope"],
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_asia_drift_shadow_trading_app(
        warehouse_root=Path(args.warehouse_root),
        pass6_classification_csv=Path(args.pass6_classification_csv),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
