"""Runner for the first controlled U.S. open observational probabilistic pass."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from ..research.us_open_follow_through.probabilistic_pass1 import DEFAULT_SYMBOLS, run_probabilistic_pass1


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_WAREHOUSE_ROOT = REPO_ROOT / "outputs" / "research_platform" / "warehouse" / "historical_evaluator"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "us_open_probabilistic_pass1" / "index_universe_20200101_20260421"
DEFAULT_START_TS = "2020-01-01T00:00:00-05:00"
DEFAULT_END_TS = "2026-04-21T23:59:00-04:00"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="us-open-probabilistic-pass1")
    parser.add_argument("--warehouse-root", default=str(DEFAULT_WAREHOUSE_ROOT), help="Parquet warehouse root path.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT), help="Research artifact output directory.")
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        default=None,
        help="Symbol to include. May be supplied multiple times. Defaults to ES/MES/NQ/MNQ.",
    )
    parser.add_argument("--start-ts", default=DEFAULT_START_TS, help="Inclusive ISO-8601 lower bound.")
    parser.add_argument("--end-ts", default=DEFAULT_END_TS, help="Inclusive ISO-8601 upper bound.")
    return parser


def run_us_open_probabilistic_pass1(
    *,
    warehouse_root: Path,
    output_dir: Path,
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> dict[str, Any]:
    result = run_probabilistic_pass1(
        warehouse_root=warehouse_root.resolve(),
        output_dir=output_dir.resolve(),
        symbols=symbols,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    return {
        "output_dir": str(output_dir.resolve()),
        "candidate_dataset_csv": result["artifacts"]["candidate_dataset_csv"],
        "candidate_dataset_parquet": result["artifacts"]["candidate_dataset_parquet"],
        "outcome_dataset_csv": result["artifacts"]["outcome_dataset_csv"],
        "outcome_dataset_parquet": result["artifacts"]["outcome_dataset_parquet"],
        "per_instrument_summary_csv": result["artifacts"]["per_instrument_summary_csv"],
        "pooled_summary_csv": result["artifacts"]["pooled_summary_csv"],
        "vix_conditioned_summary_csv": result["artifacts"]["vix_conditioned_summary_csv"],
        "dev_holdout_comparison_csv": result["artifacts"]["dev_holdout_comparison_csv"],
        "summary_json": result["artifacts"]["summary_json"],
        "summary_markdown": result["artifacts"]["summary_markdown"],
        "storage_manifest": result["artifacts"]["storage_manifest"],
        "row_counts": result["summary"]["row_counts"],
        "headline": result["summary"]["headline"],
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_us_open_probabilistic_pass1(
        warehouse_root=Path(args.warehouse_root),
        output_dir=Path(args.output_dir),
        symbols=tuple(args.symbols or DEFAULT_SYMBOLS),
        start_ts=datetime.fromisoformat(args.start_ts) if args.start_ts else None,
        end_ts=datetime.fromisoformat(args.end_ts) if args.end_ts else None,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
