"""CLI wrapper for Track B no-submit Databento candle observer."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Sequence

from .databento_candle_observer import (
    DEFAULT_DATABENTO_CANDLE_OBSERVER_OUTPUT_ROOT,
    DatabentoCandleObserverVerdict,
    observe_databento_candle_event,
    watch_databento_candle_observer,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert a supplied Databento quote/candle artifact into Track B no-submit candle/event JSON.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--market-data-json", type=Path, help="Databento-like quote/candle JSON artifact.")
    group.add_argument("--quote-report-json", type=Path, help="Existing Track B Databento quote/current quote report JSON.")
    group.add_argument("--candle-json", type=Path, help="Databento-like candle JSON artifact.")
    parser.add_argument("--contract-key", required=True)
    parser.add_argument("--databento-continuous-symbol", required=True)
    parser.add_argument("--dataset", required=True)
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    input_path = args.market_data_json or args.quote_report_json or args.candle_json
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


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
