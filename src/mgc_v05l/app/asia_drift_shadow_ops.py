from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from ..research.asia_drift.shadow_ops import run_asia_drift_shadow_ops


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_WAREHOUSE_ROOT = REPO_ROOT / "outputs" / "research_platform" / "warehouse" / "historical_evaluator"
DEFAULT_PASS6_CLASSIFICATION = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "asia_drift_probabilistic_pass6"
    / "index_stack_20200101_20260421"
    / "reports"
    / "asia_drift_probabilistic_pass6_classification.csv"
)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-shadow-ops")
    parser.add_argument("--start-date", type=_parse_date, default=None, help="Start trade date in YYYY-MM-DD.")
    parser.add_argument("--end-date", type=_parse_date, default=None, help="End trade date in YYYY-MM-DD.")
    parser.add_argument("--latest-session", action="store_true", help="Run for the latest available session date.")
    parser.add_argument("--warehouse-root", type=Path, default=DEFAULT_WAREHOUSE_ROOT)
    parser.add_argument("--pass6-classification-csv", type=Path, default=DEFAULT_PASS6_CLASSIFICATION)
    parser.add_argument("--shadow-output-dir", type=Path, required=True)
    parser.add_argument("--report-output-dir", type=Path, required=True)
    parser.add_argument("--health-output-dir", type=Path, required=True)
    parser.add_argument("--fail-on-warning", action="store_true")
    parser.add_argument("--skip-health", action="store_true")
    parser.add_argument("--skip-shadow", action="store_true")
    parser.add_argument("--skip-report", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_asia_drift_shadow_ops(
        start_date=args.start_date,
        end_date=args.end_date,
        latest_session=args.latest_session,
        warehouse_root=args.warehouse_root,
        pass6_classification_csv=args.pass6_classification_csv,
        shadow_output_dir=args.shadow_output_dir,
        report_output_dir=args.report_output_dir,
        health_output_dir=args.health_output_dir,
        fail_on_warning=args.fail_on_warning,
        skip_health=args.skip_health,
        skip_shadow=args.skip_shadow,
        skip_report=args.skip_report,
        overwrite=args.overwrite,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
