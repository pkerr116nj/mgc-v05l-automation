"""CLI wrapper for Track B no-submit Databento candle observer."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Sequence

from .databento_candle_observer import (
    DEFAULT_DATABENTO_CANDLE_OBSERVER_OUTPUT_ROOT,
    DatabentoCandleObserverVerdict,
    observe_databento_candle_event,
    wait_for_current_databento_quote,
    watch_databento_candle_observer,
    write_databento_candle_observer_blocked_report,
)
from .databento_current_quote import (
    DEFAULT_CURRENT_QUOTE_OUTPUT_ROOT,
    DatabentoCurrentQuoteConfig,
    DatabentoCurrentQuoteProvider,
    DatabentoQuoteProviderCurrentQuoteTransport,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert a supplied Databento quote/candle artifact into Track B no-submit candle/event JSON.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--market-data-json", type=Path, help="Databento-like quote/candle JSON artifact.")
    group.add_argument("--quote-report-json", type=Path, help="Existing Track B Databento quote/current quote report JSON.")
    group.add_argument("--candle-json", type=Path, help="Databento-like candle JSON artifact.")
    group.add_argument("--live-current-quote", action="store_true", help="Pull one bounded Databento current quote report before producing a candle/event artifact.")
    parser.add_argument("--contract-key", required=True)
    parser.add_argument("--databento-continuous-symbol", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--databento-symbol", help="Optional raw Databento symbol override for market-data selection only.")
    parser.add_argument("--stype-in", default="continuous")
    parser.add_argument("--quote-schema", default="mbp-1")
    parser.add_argument("--allowlisted-local-symbol")
    parser.add_argument("--tick-size", default="0.1")
    parser.add_argument("--exchange", default="COMEX")
    parser.add_argument("--currency", default="USD")
    parser.add_argument("--max-age-seconds", type=int, default=15)
    parser.add_argument("--quote-lookback-seconds", type=int, default=300)
    parser.add_argument("--allow-available-end-fallback", action="store_true")
    parser.add_argument("--available-end-buffer-seconds", type=int, default=300)
    parser.add_argument("--current-quote-output-root", type=Path, default=DEFAULT_CURRENT_QUOTE_OUTPUT_ROOT)
    parser.add_argument("--expected-account-id", required=True)
    parser.add_argument("--strategy-id", required=True)
    parser.add_argument("--lane-id", required=True)
    parser.add_argument("--timeframe", default="quote_snapshot")
    parser.add_argument("--source-id")
    parser.add_argument("--signal-direction", help="Optional explicit LONG/SHORT/FLAT/NONE hint for downstream adapter review.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_DATABENTO_CANDLE_OBSERVER_OUTPUT_ROOT)
    parser.add_argument("--watch", action="store_true", help="Run bounded watch mode instead of one-shot conversion.")
    parser.add_argument("--max-cycles", type=int, default=1, help="Maximum watch cycles when --watch is enabled.")
    parser.add_argument("--poll-seconds", type=float, default=0.0, help="Seconds to sleep between watch cycles.")
    parser.add_argument("--wait-for-current-quote", action="store_true", help="With --live-current-quote, poll until a current quote is available or max wait cycles are exhausted.")
    parser.add_argument("--max-wait-cycles", type=int, default=1)
    parser.add_argument("--wait-poll-seconds", type=float, default=0.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    input_path = args.market_data_json or args.quote_report_json or args.candle_json
    if args.live_current_quote:
        return _run_live_current_quote(args)
    if args.watch:
        result = watch_databento_candle_observer(
            market_data_payload_reader=lambda: json.loads(input_path.read_text(encoding="utf-8")),
            contract_key=args.contract_key,
            databento_continuous_symbol=args.databento_continuous_symbol,
            dataset=args.dataset,
            expected_account_id=args.expected_account_id,
            strategy_id=args.strategy_id,
            lane_id=args.lane_id,
            timeframe=args.timeframe,
            source_id=args.source_id,
            signal_direction=args.signal_direction,
            output_root=args.output_root,
            max_cycles=args.max_cycles,
            poll_seconds=args.poll_seconds,
            sleep_func=time.sleep,
        )
        print(
            json.dumps(
                {
                    "observer_mode": result.heartbeat["observer_mode"],
                    "watch_id": result.heartbeat["watch_id"],
                    "current_cycle_number": result.heartbeat["current_cycle_number"],
                    "processed_cycles": result.heartbeat["processed_cycles"],
                    "no_data_cycles": result.heartbeat["no_data_cycles"],
                    "error_cycles": result.heartbeat["error_cycles"],
                    "last_observer_verdict": result.heartbeat["last_observer_verdict"],
                    "output_event_path": result.heartbeat["output_event_path"],
                    "watch_exited_normally": result.heartbeat["watch_exited_normally"],
                    "submit_allowed": result.heartbeat["submit_allowed"],
                    "submit_attempted": result.heartbeat["submit_attempted"],
                    "live_money_readiness": result.heartbeat["live_money_readiness"],
                    "listener_invoked": result.heartbeat["listener_invoked"],
                    "runner_invoked": result.heartbeat["runner_invoked"],
                    "heartbeat_json": str(result.heartbeat_json),
                },
                sort_keys=True,
            )
        )
        return 0

    payload = json.loads(input_path.read_text(encoding="utf-8"))
    result = observe_databento_candle_event(
        market_data_payload=payload,
        contract_key=args.contract_key,
        databento_continuous_symbol=args.databento_continuous_symbol,
        dataset=args.dataset,
        expected_account_id=args.expected_account_id,
        strategy_id=args.strategy_id,
        lane_id=args.lane_id,
        timeframe=args.timeframe,
        source_id=args.source_id,
        signal_direction=args.signal_direction,
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "observer_verdict": result.report["observer_verdict"],
                "source_id": result.report["source_id"],
                "contract_key": result.report["contract_key"],
                "databento_continuous_symbol": result.report["databento_continuous_symbol"],
                "dataset": result.report["dataset"],
                "event_timestamp": result.report["event_timestamp"],
                "output_candle_event_path": result.report["output_candle_event_path"],
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "live_money_readiness": result.report["live_money_readiness"],
                "listener_invoked": result.report["listener_invoked"],
                "runner_invoked": result.report["runner_invoked"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict == DatabentoCandleObserverVerdict.WROTE_EVENT else 2


def _run_live_current_quote(args: argparse.Namespace) -> int:
    raw_api_key = str(os.environ.get("DATABENTO_API_KEY") or "").strip()
    if not raw_api_key:
        result = write_databento_candle_observer_blocked_report(
            contract_key=args.contract_key,
            databento_continuous_symbol=args.databento_continuous_symbol,
            dataset=args.dataset,
            timeframe=args.timeframe,
            output_root=args.output_root,
            source_id=args.source_id or "databento_live_current_quote",
            primary_blocker="DATABENTO_API_KEY is required for bounded Databento current quote pull.",
            required_next_action="Set DATABENTO_API_KEY in the operator environment, then rerun the no-submit Databento observer command.",
        )
        _print_one_shot_result(result)
        return 2

    config = DatabentoCurrentQuoteConfig(
        contract_key=args.contract_key,
        dataset=args.dataset,
        databento_continuous_symbol=None if args.databento_symbol else args.databento_continuous_symbol,
        databento_symbol=args.databento_symbol,
        stype_in="raw_symbol" if args.databento_symbol else args.stype_in,
        schema=args.quote_schema,
        allowlisted_local_symbol=args.allowlisted_local_symbol,
        tick_size=args.tick_size,
        exchange=args.exchange,
        currency=args.currency,
        max_age_seconds=args.max_age_seconds,
        output_root=args.current_quote_output_root,
    )
    transport = DatabentoQuoteProviderCurrentQuoteTransport(
        api_key=raw_api_key,
        allowlisted_local_symbol=args.allowlisted_local_symbol,
        tick_size=args.tick_size,
        exchange=args.exchange,
        currency=args.currency,
        max_age_seconds=args.max_age_seconds,
        lookback_seconds=args.quote_lookback_seconds,
        allow_available_end_fallback=args.allow_available_end_fallback,
        available_end_buffer_seconds=args.available_end_buffer_seconds,
    )
    provider = DatabentoCurrentQuoteProvider(config=config, transport=transport)

    def read_current_quote_report() -> dict[str, object]:
        return provider.fetch_current_quote().report

    if args.wait_for_current_quote:
        result = wait_for_current_databento_quote(
            market_data_payload_reader=read_current_quote_report,
            contract_key=args.contract_key,
            databento_continuous_symbol=args.databento_continuous_symbol,
            dataset=args.dataset,
            expected_account_id=args.expected_account_id,
            strategy_id=args.strategy_id,
            lane_id=args.lane_id,
            timeframe=args.timeframe,
            source_id=args.source_id or "databento_wait_for_current_quote",
            signal_direction=args.signal_direction,
            market_data_connection_attempted=True,
            output_root=args.output_root,
            max_wait_cycles=args.max_wait_cycles,
            wait_poll_seconds=args.wait_poll_seconds,
            sleep_func=time.sleep,
        )
        print(
            json.dumps(
                {
                    "observer_mode": result.heartbeat["observer_mode"],
                    "wait_id": result.heartbeat["wait_id"],
                    "current_cycle_number": result.heartbeat["current_cycle_number"],
                    "max_wait_cycles": result.heartbeat["max_wait_cycles"],
                    "wait_poll_seconds": result.heartbeat["wait_poll_seconds"],
                    "successful_current_quote_cycles": result.heartbeat["successful_current_quote_cycles"],
                    "available_end_lag_cycles": result.heartbeat["available_end_lag_cycles"],
                    "no_data_cycles": result.heartbeat["no_data_cycles"],
                    "error_cycles": result.heartbeat["error_cycles"],
                    "last_requested_quote_end": result.heartbeat["last_requested_quote_end"],
                    "last_provider_available_end": result.heartbeat["last_provider_available_end"],
                    "last_observer_verdict": result.heartbeat["last_observer_verdict"],
                    "current_quote_available": result.heartbeat["current_quote_available"],
                    "wait_exited_normally": result.heartbeat["wait_exited_normally"],
                    "wait_succeeded": result.heartbeat["wait_succeeded"],
                    "required_next_action": result.heartbeat["required_next_action"],
                    "output_event_path": result.heartbeat["output_event_path"],
                    "submit_allowed": result.heartbeat["submit_allowed"],
                    "submit_attempted": result.heartbeat["submit_attempted"],
                    "live_money_readiness": result.heartbeat["live_money_readiness"],
                    "listener_invoked": result.heartbeat["listener_invoked"],
                    "runner_invoked": result.heartbeat["runner_invoked"],
                    "heartbeat_json": str(result.heartbeat_json),
                },
                sort_keys=True,
            )
        )
        return 0 if result.heartbeat["wait_succeeded"] else 2

    if args.watch:
        result = watch_databento_candle_observer(
            market_data_payload_reader=read_current_quote_report,
            contract_key=args.contract_key,
            databento_continuous_symbol=args.databento_continuous_symbol,
            dataset=args.dataset,
            expected_account_id=args.expected_account_id,
            strategy_id=args.strategy_id,
            lane_id=args.lane_id,
            timeframe=args.timeframe,
            source_id=args.source_id or "databento_live_current_quote",
            signal_direction=args.signal_direction,
            market_data_connection_attempted=True,
            output_root=args.output_root,
            max_cycles=args.max_cycles,
            poll_seconds=args.poll_seconds,
            sleep_func=time.sleep,
        )
        print(
            json.dumps(
                {
                    "observer_mode": result.heartbeat["observer_mode"],
                    "watch_id": result.heartbeat["watch_id"],
                    "current_cycle_number": result.heartbeat["current_cycle_number"],
                    "processed_cycles": result.heartbeat["processed_cycles"],
                    "no_data_cycles": result.heartbeat["no_data_cycles"],
                    "error_cycles": result.heartbeat["error_cycles"],
                    "last_observer_verdict": result.heartbeat["last_observer_verdict"],
                    "output_event_path": result.heartbeat["output_event_path"],
                    "watch_exited_normally": result.heartbeat["watch_exited_normally"],
                    "submit_allowed": result.heartbeat["submit_allowed"],
                    "submit_attempted": result.heartbeat["submit_attempted"],
                    "live_money_readiness": result.heartbeat["live_money_readiness"],
                    "listener_invoked": result.heartbeat["listener_invoked"],
                    "runner_invoked": result.heartbeat["runner_invoked"],
                    "heartbeat_json": str(result.heartbeat_json),
                },
                sort_keys=True,
            )
        )
        return 0

    result = observe_databento_candle_event(
        market_data_payload=read_current_quote_report(),
        contract_key=args.contract_key,
        databento_continuous_symbol=args.databento_continuous_symbol,
        dataset=args.dataset,
        expected_account_id=args.expected_account_id,
        strategy_id=args.strategy_id,
        lane_id=args.lane_id,
        timeframe=args.timeframe,
        source_id=args.source_id or "databento_live_current_quote",
        signal_direction=args.signal_direction,
        market_data_connection_attempted=True,
        output_root=args.output_root,
    )
    _print_one_shot_result(result)
    return 0 if result.verdict == DatabentoCandleObserverVerdict.WROTE_EVENT else 2


def _print_one_shot_result(result) -> None:  # type: ignore[no-untyped-def]
    print(
        json.dumps(
            {
                "observer_verdict": result.report["observer_verdict"],
                "source_id": result.report["source_id"],
                "contract_key": result.report["contract_key"],
                "databento_continuous_symbol": result.report["databento_continuous_symbol"],
                "dataset": result.report["dataset"],
                "event_timestamp": result.report["event_timestamp"],
                "output_candle_event_path": result.report["output_candle_event_path"],
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "live_money_readiness": result.report["live_money_readiness"],
                "listener_invoked": result.report["listener_invoked"],
                "runner_invoked": result.report["runner_invoked"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
