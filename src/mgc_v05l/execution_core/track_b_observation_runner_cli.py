"""CLI for Track B no-submit observation runner."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Sequence

from .databento_current_quote import (
    DEFAULT_CURRENT_QUOTE_OUTPUT_ROOT,
    DatabentoCurrentQuoteConfig,
    DatabentoCurrentQuoteProvider,
    DatabentoQuoteProviderCurrentQuoteTransport,
)
from .track_b_observation_runner import (
    DEFAULT_TRACK_B_OBSERVATION_RUNNER_OUTPUT_ROOT,
    run_track_b_observation_once,
    run_track_b_observation_watch,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Track B no-submit observation chain once or in bounded watch mode.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--market-data-json", type=Path)
    group.add_argument("--quote-report-json", type=Path)
    group.add_argument("--candle-json", type=Path)
    group.add_argument("--live-current-quote", action="store_true")
    parser.add_argument("--listener-config-json", type=Path, default=Path("examples/track_b_shadow_listener/listener_config.json"))
    parser.add_argument("--contract-key", required=True)
    parser.add_argument("--databento-continuous-symbol", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--databento-symbol")
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
    parser.add_argument("--signal-direction")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_OBSERVATION_RUNNER_OUTPUT_ROOT)
    parser.add_argument("--databento-observer-output-root", type=Path, default=Path("outputs/track_b_execution_core/databento_candle_observer"))
    parser.add_argument("--strategy-adapter-output-root", type=Path, default=Path("outputs/track_b_execution_core/strategy_signal_adapter"))
    parser.add_argument("--candle-producer-output-root", type=Path, default=Path("outputs/track_b_execution_core/candle_signal_producer"))
    parser.add_argument("--writer-output-root", type=Path, default=Path("outputs/track_b_execution_core/signal_batch_writer"))
    parser.add_argument("--listener-output-root", type=Path, default=Path("outputs/track_b_execution_core/shadow_listener"))
    parser.add_argument("--operator-status-output-root", type=Path, default=Path("outputs/track_b_execution_core/operator_status"))
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--max-cycles", type=int, default=1)
    parser.add_argument("--poll-seconds", type=float, default=0.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    listener_config = _read_json(args.listener_config_json)
    payload_reader, market_data_connection_attempted = _payload_reader(args)

    if args.watch:
        result = run_track_b_observation_watch(
            market_data_payload_reader=payload_reader,
            listener_config_payload=listener_config,
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
            databento_observer_output_root=args.databento_observer_output_root,
            strategy_adapter_output_root=args.strategy_adapter_output_root,
            listener_output_root=args.listener_output_root,
            operator_status_output_root=args.operator_status_output_root,
            max_cycles=args.max_cycles,
            poll_seconds=args.poll_seconds,
            sleep_func=time.sleep,
            market_data_connection_attempted=market_data_connection_attempted,
        )
    else:
        result = run_track_b_observation_once(
            market_data_payload=payload_reader(),
            listener_config_payload=listener_config,
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
            databento_observer_output_root=args.databento_observer_output_root,
            strategy_adapter_output_root=args.strategy_adapter_output_root,
            candle_producer_output_root=args.candle_producer_output_root,
            writer_output_root=args.writer_output_root,
            listener_output_root=args.listener_output_root,
            operator_status_output_root=args.operator_status_output_root,
            market_data_connection_attempted=market_data_connection_attempted,
        )

    print(
        json.dumps(
            {
                "runner_verdict": result.report["runner_verdict"],
                "mode": result.report["mode"],
                "current_cycle": result.report["current_cycle"],
                "databento_observer_verdict": result.report["databento_observer_verdict"],
                "strategy_adapter_verdict": result.report["strategy_adapter_verdict"],
                "candle_producer_verdict": result.report["candle_producer_verdict"],
                "signal_batch_writer_verdict": result.report["signal_batch_writer_verdict"],
                "listener_verdict": result.report["listener_verdict"],
                "listener_health_verdict": result.report["listener_health_verdict"],
                "operator_status_verdict": result.report["operator_status_verdict"],
                "latest_operator_status_path": result.report["latest_operator_status_path"],
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
    return 0


def _payload_reader(args: argparse.Namespace):  # type: ignore[no-untyped-def]
    input_path = args.market_data_json or args.quote_report_json or args.candle_json
    if input_path is not None:
        return lambda: _read_json(input_path), False

    api_key = str(os.environ.get("DATABENTO_API_KEY") or "").strip()
    if not api_key:
        return (
            lambda: {
                "quote_observed": False,
                "classification": "CURRENT_QUOTE_PROVIDER_ERROR",
                "provider_error": "DATABENTO_API_KEY is required for bounded Databento current quote pull.",
            },
            False,
        )
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
        api_key=api_key,
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
    return lambda: provider.fetch_current_quote().report, True


def _read_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return value


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
