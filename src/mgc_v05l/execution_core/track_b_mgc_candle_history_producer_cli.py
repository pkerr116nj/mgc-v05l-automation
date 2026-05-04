"""CLI wrapper for Track B MGC candle-history producer."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Sequence

from .databento_quote_provider import NativeDatabentoQuoteTransport
from .track_b_mgc_candle_history_producer import (
    DEFAULT_TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_OUTPUT_ROOT,
    MGC_DATABENTO_CONTINUOUS_SYMBOL,
    MGC_DATASET,
    MGC_LOCAL_SYMBOL,
    TrackBMgcCandleHistoryProducerVerdict,
    fetch_databento_ohlcv_1m_records,
    produce_track_b_mgc_candle_history_input,
    provider_error_message,
    write_provider_error_report,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Produce bounded 1m MGC candle-history JSON for Track B market-history collection. "
            "Market-data evidence only; no broker, paper proof, listener, or submit calls."
        )
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--history-json", type=Path, help="Existing Databento-like candle/ohlcv history JSON.")
    source.add_argument("--fetch-databento-history", action="store_true", help="Fetch bounded Databento ohlcv-1m history.")
    parser.add_argument("--current-quote-report-json", required=True, type=Path)
    parser.add_argument("--expected-account-id", required=True)
    parser.add_argument("--strategy-id", required=True)
    parser.add_argument("--lane-id", required=True)
    parser.add_argument("--contract-key", default="MGC-202606")
    parser.add_argument("--databento-continuous-symbol", default=MGC_DATABENTO_CONTINUOUS_SYMBOL)
    parser.add_argument("--databento-symbol", help="Optional raw Databento symbol override, e.g. MGCM6.")
    parser.add_argument("--stype-in", default="continuous")
    parser.add_argument("--dataset", default=MGC_DATASET)
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--allowlisted-local-symbol", default=MGC_LOCAL_SYMBOL)
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--max-candles", type=int, default=50)
    parser.add_argument("--min-candles", type=int, default=3)
    parser.add_argument("--lookback-minutes", type=int, default=60)
    parser.add_argument("--history-start")
    parser.add_argument("--history-end")
    parser.add_argument("--base-url", default="https://hist.databento.com/v0")
    parser.add_argument("--source-id")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    current_quote = json.loads(args.current_quote_report_json.read_text(encoding="utf-8"))
    if not isinstance(current_quote, dict):
        raise ValueError("current quote report JSON must contain an object.")

    history_payload_path = args.history_json
    if args.fetch_databento_history:
        raw_api_key = str(os.environ.get("DATABENTO_API_KEY") or "").strip()
        if not raw_api_key:
            result = write_provider_error_report(
                primary_blocker="DATABENTO_API_KEY is required for bounded Databento candle-history fetch.",
                required_next_action="Set DATABENTO_API_KEY or provide --history-json with bounded candle history.",
                expected_account_id=args.expected_account_id,
                strategy_id=args.strategy_id,
                lane_id=args.lane_id,
                contract_key=args.contract_key,
                databento_continuous_symbol=args.databento_continuous_symbol,
                allowlisted_local_symbol=args.allowlisted_local_symbol,
                dataset=args.dataset,
                timeframe=args.timeframe,
                output_root=args.output_root,
                source_id=args.source_id,
            )
            _print_result(result)
            return 2
        try:
            end = _parse_time(args.history_end) if args.history_end else datetime.now(UTC)
            start = _parse_time(args.history_start) if args.history_start else end - timedelta(minutes=max(int(args.lookback_minutes), 1))
            symbol = args.databento_symbol or args.databento_continuous_symbol
            stype_in = "raw_symbol" if args.databento_symbol else args.stype_in
            records = fetch_databento_ohlcv_1m_records(
                transport=NativeDatabentoQuoteTransport(),
                api_key=raw_api_key,
                dataset=args.dataset,
                symbol=symbol,
                stype_in=stype_in,
                schema=args.schema,
                start=start,
                end=end,
                max_candles=args.max_candles,
                base_url=args.base_url,
            )
            history_payload: dict[str, object] = {
                "schema_version": "track_b_databento_ohlcv_history_raw_v1",
                "source_id": args.source_id or "databento_ohlcv_1m_history",
                "contract_key": args.contract_key,
                "account_id": args.expected_account_id,
                "instrument_family": "MGC",
                "strategy_id": args.strategy_id,
                "lane_id": args.lane_id,
                "dataset": args.dataset,
                "databento_continuous_symbol": args.databento_continuous_symbol,
                "symbol": args.allowlisted_local_symbol,
                "timeframe": args.timeframe,
                "history_provider_mode": "DATABENTO_HISTORICAL_BOUNDED_WITH_REALTIME_CURRENT",
                "candles": list(records),
            }
            history_payload_path = None
        except Exception as exc:  # noqa: BLE001 - provider failures become explicit artifacts.
            result = write_provider_error_report(
                primary_blocker=provider_error_message(exc),
                required_next_action="Retry with a bounded history window, valid Databento entitlement, or provide --history-json.",
                expected_account_id=args.expected_account_id,
                strategy_id=args.strategy_id,
                lane_id=args.lane_id,
                contract_key=args.contract_key,
                databento_continuous_symbol=args.databento_continuous_symbol,
                allowlisted_local_symbol=args.allowlisted_local_symbol,
                dataset=args.dataset,
                timeframe=args.timeframe,
                output_root=args.output_root,
                source_id=args.source_id,
            )
            _print_result(result)
            return 2
    else:
        history_payload = json.loads(args.history_json.read_text(encoding="utf-8"))

    result = produce_track_b_mgc_candle_history_input(
        history_payload=history_payload,
        current_quote_report_payload=current_quote,
        history_payload_path=history_payload_path,
        current_quote_report_path=args.current_quote_report_json,
        expected_account_id=args.expected_account_id,
        strategy_id=args.strategy_id,
        lane_id=args.lane_id,
        contract_key=args.contract_key,
        databento_continuous_symbol=args.databento_continuous_symbol,
        allowlisted_local_symbol=args.allowlisted_local_symbol,
        dataset=args.dataset,
        timeframe=args.timeframe,
        max_candles=args.max_candles,
        min_candles=args.min_candles,
        source_id=args.source_id,
        output_root=args.output_root,
    )
    _print_result(result)
    return 0 if result.verdict == TrackBMgcCandleHistoryProducerVerdict.WROTE_HISTORY_INPUT else 2


def _print_result(result) -> None:  # type: ignore[no-untyped-def]
    print(
        json.dumps(
            {
                "candle_history_producer_verdict": result.report["candle_history_producer_verdict"],
                "contract_key": result.report["contract_key"],
                "symbol": result.report["symbol"],
                "databento_continuous_symbol": result.report["databento_continuous_symbol"],
                "dataset": result.report["dataset"],
                "timeframe": result.report["timeframe"],
                "history_provider_mode": result.report["history_provider_mode"],
                "quote_provider_mode": result.report["quote_provider_mode"],
                "realtime_quote_received": result.report["realtime_quote_received"],
                "current_quote_available": result.report["current_quote_available"],
                "candles_produced": result.report["candles_produced"],
                "latest_candle_timestamp": result.report["latest_candle_timestamp"],
                "output_history_input_path": result.report["output_history_input_path"],
                "latest_history_input_path": result.report["latest_history_input_path"],
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "live_money_readiness": result.report["live_money_readiness"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
