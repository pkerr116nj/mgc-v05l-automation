"""CLI runner for VIX daily ingest."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from ..market_data.vix_daily_ingest import ingest_vix_daily


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "research_platform" / "warehouse" / "historical_evaluator"
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "research_regime_buckets.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="vix-daily-ingest")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Warehouse root directory.")
    parser.add_argument("--config-path", default=str(DEFAULT_CONFIG_PATH), help="VIX bucket/source config path.")
    parser.add_argument("--start-date", required=True, help="Inclusive start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", required=True, help="Inclusive end date (YYYY-MM-DD).")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = ingest_vix_daily(
        output_root=Path(args.output_root),
        config_path=Path(args.config_path),
        start_date=date.fromisoformat(args.start_date),
        end_date=date.fromisoformat(args.end_date),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

