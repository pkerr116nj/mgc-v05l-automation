from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from mgc_v05l.research.asia_drift.shadow_reporting import run_asia_drift_shadow_report


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-shadow-report")
    parser.add_argument("--start-date", required=True, type=_parse_date, help="Start date in YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, type=_parse_date, help="End date in YYYY-MM-DD.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Output directory for reporting artifacts.",
    )
    parser.add_argument(
        "--shadow-output-dir",
        type=Path,
        default=None,
        help="Existing shadow harness output directory. Defaults to the matching date-range folder.",
    )
    parser.add_argument(
        "--warehouse-root",
        type=Path,
        default=Path("outputs/research_platform/warehouse/historical_evaluator"),
        help="Warehouse root for VIX/process checks.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    shadow_output_dir = args.shadow_output_dir
    if shadow_output_dir is None:
        start_token = args.start_date.strftime("%Y%m%d")
        end_token = args.end_date.strftime("%Y%m%d")
        shadow_output_dir = Path(f"outputs/reports/asia_drift_shadow_trading/index_stack_{start_token}_{end_token}")
    payload = run_asia_drift_shadow_report(
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        shadow_output_dir=shadow_output_dir,
        warehouse_root=args.warehouse_root,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
