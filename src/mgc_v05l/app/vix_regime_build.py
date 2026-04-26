"""CLI runner for VIX daily regime table materialization."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..research.regime.vix_regime_builder import build_vol_regime_daily


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_WAREHOUSE_ROOT = REPO_ROOT / "outputs" / "research_platform" / "warehouse" / "historical_evaluator"
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "research_regime_buckets.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="vix-regime-build")
    parser.add_argument("--warehouse-root", default=str(DEFAULT_WAREHOUSE_ROOT), help="Warehouse root directory.")
    parser.add_argument("--bucket-config", default=str(DEFAULT_CONFIG_PATH), help="Bucket config path.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = build_vol_regime_daily(
        warehouse_root=Path(args.warehouse_root),
        bucket_config_path=Path(args.bucket_config),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
