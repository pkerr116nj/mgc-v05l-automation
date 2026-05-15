"""CLI for Track B Databento Live runtime feed writer."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .track_b_databento_live_runtime_feed import (
    DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT,
    TrackBDatabentoLiveFeedConfig,
    run_track_b_databento_live_runtime_feed,
)
from .track_b_runtime_candle_capture import MGC_CONTINUOUS_SYMBOL, MGC_DATASET, MGC_LOCAL_SYMBOL


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Write bounded Track B Databento Live runtime candle artifacts. No broker or submit paths are invoked."
    )
    parser.add_argument("--expected-account-id", default="DUM882026")
    parser.add_argument("--account-id", default="DUM882026")
    parser.add_argument("--contract-key", default="MGC-202606")
    parser.add_argument("--instrument-family", default="MGC")
    parser.add_argument("--local-symbol", default=MGC_LOCAL_SYMBOL)
    parser.add_argument("--databento-continuous-symbol", default=MGC_CONTINUOUS_SYMBOL)
    parser.add_argument("--dataset", default=MGC_DATASET)
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--stype-in", default="continuous")
    parser.add_argument("--stype-out", default="instrument_id")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--max-bars", type=int, default=90)
    parser.add_argument("--min-bars", type=int, default=8)
    parser.add_argument("--max-records", type=int, default=90)
    parser.add_argument("--max-seconds", type=float, default=75.0)
    parser.add_argument("--max-latest-1m-age-seconds", type=int, default=180)
    parser.add_argument("--max-completed-5m-age-seconds", type=int, default=600)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--source-id", default="track_b_databento_live_runtime_feed")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBDatabentoLiveFeedConfig(
        expected_account_id=args.expected_account_id,
        account_id=args.account_id,
        contract_key=args.contract_key,
        instrument_family=args.instrument_family,
        local_symbol=args.local_symbol,
        databento_continuous_symbol=args.databento_continuous_symbol,
        dataset=args.dataset,
        schema=args.schema,
        stype_in=args.stype_in,
        stype_out=args.stype_out,
        timeframe=args.timeframe,
        max_bars=args.max_bars,
        min_bars=args.min_bars,
        max_records=args.max_records,
        max_seconds=args.max_seconds,
        max_latest_1m_age_seconds=args.max_latest_1m_age_seconds,
        max_completed_5m_age_seconds=args.max_completed_5m_age_seconds,
        env_file=args.env_file,
        output_root=args.output_root,
        source_id=args.source_id,
    )
    result = run_track_b_databento_live_runtime_feed(config=config)
    print(
        json.dumps(
            {
                "live_runtime_feed_verdict": result.report.get("live_runtime_feed_verdict"),
                "live_feed_connected": result.report.get("live_feed_connected"),
                "subscription_status": result.report.get("subscription_status"),
                "dataset": result.report.get("dataset"),
                "symbol": result.report.get("databento_continuous_symbol"),
                "schema": result.report.get("schema"),
                "stype_in": result.report.get("stype_in"),
                "stype_out": result.report.get("stype_out"),
                "latest_record_ts_event": result.report.get("latest_record_ts_event"),
                "latest_record_ts_recv": result.report.get("latest_record_ts_recv"),
                "latency_ms": result.report.get("latency_ms"),
                "latest_1m_timestamp": result.report.get("latest_1m_timestamp"),
                "latest_completed_5m_timestamp": result.report.get("latest_completed_5m_timestamp"),
                "fresh_for_execution": result.report.get("fresh_for_execution"),
                "primary_blocker": result.report.get("primary_blocker"),
                "report_json": str(result.report_json),
                "live_1m_candles_json": None if result.live_1m_candles_json is None else str(result.live_1m_candles_json),
                "submit_allowed": result.report.get("submit_allowed"),
                "submit_attempted": result.report.get("submit_attempted"),
                "live_money_readiness": result.report.get("live_money_readiness"),
            },
            sort_keys=True,
        )
    )
    return 0 if result.report.get("live_feed_connected") is True else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
