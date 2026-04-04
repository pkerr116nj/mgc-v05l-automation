"""Build a schema-preserving canonical verification subset."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..market_data.canonical_subset import build_schema_preserving_canonical_subset

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_DB = REPO_ROOT / "mgc_v05l.replay.sqlite3"
DEFAULT_TARGET_DB = REPO_ROOT / "mgc_v05l.canonical_subset.sqlite3"
DEFAULT_REPORT_PATH = REPO_ROOT / "outputs" / "reports" / "canonical_market_data" / "canonical_subset_build.json"
DEFAULT_SYMBOLS = ("MGC", "GC", "MES", "ES", "MNQ", "NQ", "CL", "6E", "ZN")
DEFAULT_TIMEFRAMES = ("1m", "5m", "10m")
DEFAULT_DATA_SOURCES = ("historical_1m_canonical", "historical_5m_canonical", "historical_10m_canonical")


def main() -> int:
    parser = argparse.ArgumentParser(prog="build-canonical-subset")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB))
    parser.add_argument("--target-db", default=str(DEFAULT_TARGET_DB))
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--symbol", action="append", default=None)
    parser.add_argument("--timeframe", action="append", default=None)
    parser.add_argument("--data-source", action="append", default=None)
    args = parser.parse_args()
    result = build_schema_preserving_canonical_subset(
        source_db_path=args.source_db,
        target_db_path=args.target_db,
        symbols=args.symbol or list(DEFAULT_SYMBOLS),
        timeframes=args.timeframe or list(DEFAULT_TIMEFRAMES),
        data_sources=args.data_source or list(DEFAULT_DATA_SOURCES),
        report_path=args.report_path,
    )
    print(json.dumps({**result.__dict__}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
