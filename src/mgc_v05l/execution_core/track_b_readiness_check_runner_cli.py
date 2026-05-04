"""CLI for the Track B no-submit readiness check runner."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .track_b_readiness_check_runner import (
    DEFAULT_READINESS_CHECK_RUNNER_OUTPUT_ROOT,
    TrackBReadinessCheckRunnerConfig,
    run_track_b_readiness_check,
)
from .databento_current_quote import QuoteProviderMode


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run bounded Track B no-submit readiness checks before any separate paper proof decision."
    )
    parser.add_argument("--mode", default="PAPER")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7497)
    parser.add_argument("--client-id", type=int, default=17077)
    parser.add_argument("--account-id", default="DUM882026")
    parser.add_argument("--contract-key", default="MGC-202606")
    parser.add_argument("--broker-order-id", default="1")
    parser.add_argument("--perm-id", default="736787312")
    parser.add_argument("--market-data-mode", default="DELAYED")
    parser.add_argument("--databento-continuous-symbol", default="MGC.v.0")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--allowlisted-local-symbol", default="MGCM6")
    parser.add_argument("--tick-size", default="0.1")
    parser.add_argument("--exchange", default="COMEX")
    parser.add_argument("--currency", default="USD")
    parser.add_argument("--expected-account-id", default="DUM882026")
    parser.add_argument("--strategy-id", default="track_b_example_gold_shadow_v1")
    parser.add_argument("--lane-id", default="mgc_example_long_lmt_day")
    parser.add_argument("--timeframe", default="quote_snapshot")
    parser.add_argument("--source-id", default="track_b_readiness_check_runner")
    parser.add_argument("--proof-timing-status", default="ACTIVE_SESSION")
    parser.add_argument("--proof-timing-source", default="track_b_readiness_check_runner")
    parser.add_argument("--proof-timing-detail")
    parser.add_argument("--max-wait-cycles", type=int, default=1)
    parser.add_argument("--wait-poll-seconds", type=float, default=0.0)
    parser.add_argument(
        "--max-current-quote-age-seconds",
        type=int,
        help="Explicit tolerance for Databento provider available_end lag when classifying current-enough paper-readiness quote evidence.",
    )
    parser.add_argument("--quote-provider-mode", choices=[item.value for item in QuoteProviderMode], default=QuoteProviderMode.REALTIME.value)
    parser.add_argument("--use-databento-realtime-quote", action="store_true")
    parser.add_argument("--realtime-receive-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--quote-timeout-seconds", type=float, default=3.0)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_READINESS_CHECK_RUNNER_OUTPUT_ROOT)
    parser.add_argument("--recovery-output-root", type=Path, default=Path("outputs/track_b_execution_core/recovery_status"))
    parser.add_argument("--preflight-output-root", type=Path, default=Path("outputs/track_b_execution_core/preflight"))
    parser.add_argument("--databento-observer-output-root", type=Path, default=Path("outputs/track_b_execution_core/databento_candle_observer"))
    parser.add_argument("--current-quote-output-root", type=Path, default=Path("outputs/track_b_execution_core/current_quotes"))
    parser.add_argument("--readiness-summary-output-root", type=Path, default=Path("outputs/track_b_execution_core/readiness_summary"))
    parser.add_argument("--operator-status-output-root", type=Path, default=Path("outputs/track_b_execution_core/operator_status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBReadinessCheckRunnerConfig(
        mode=args.mode,
        host=args.host,
        port=args.port,
        client_id=args.client_id,
        account_id=args.account_id,
        contract_key=args.contract_key,
        broker_order_id=args.broker_order_id,
        perm_id=args.perm_id,
        market_data_mode=args.market_data_mode,
        databento_continuous_symbol=args.databento_continuous_symbol,
        dataset=args.dataset,
        allowlisted_local_symbol=args.allowlisted_local_symbol,
        tick_size=args.tick_size,
        exchange=args.exchange,
        currency=args.currency,
        expected_account_id=args.expected_account_id,
        strategy_id=args.strategy_id,
        lane_id=args.lane_id,
        timeframe=args.timeframe,
        source_id=args.source_id,
        proof_timing_status=args.proof_timing_status,
        proof_timing_source=args.proof_timing_source,
        proof_timing_detail=args.proof_timing_detail,
        max_wait_cycles=args.max_wait_cycles,
        wait_poll_seconds=args.wait_poll_seconds,
        max_current_quote_age_seconds=args.max_current_quote_age_seconds,
        quote_provider_mode=QuoteProviderMode.REALTIME.value if args.use_databento_realtime_quote else args.quote_provider_mode,
        realtime_receive_timeout_seconds=args.realtime_receive_timeout_seconds,
        request_timeout_seconds=args.request_timeout_seconds,
        quote_timeout_seconds=args.quote_timeout_seconds,
        output_root=args.output_root,
        recovery_output_root=args.recovery_output_root,
        preflight_output_root=args.preflight_output_root,
        databento_observer_output_root=args.databento_observer_output_root,
        current_quote_output_root=args.current_quote_output_root,
        readiness_summary_output_root=args.readiness_summary_output_root,
        operator_status_output_root=args.operator_status_output_root,
    )
    result = run_track_b_readiness_check(config=config)
    print(
        json.dumps(
            {
                "runner_verdict": result.report["runner_verdict"],
                "recovery_verdict": result.report["recovery_verdict"],
                "preflight_verdict": result.report["preflight_verdict"],
                "databento_observer_verdict": result.report["databento_observer_verdict"],
                "quote_provider_mode": result.report["quote_provider_mode"],
                "realtime_subscription_attempted": result.report["realtime_subscription_attempted"],
                "realtime_quote_received": result.report["realtime_quote_received"],
                "current_quote_available": result.report["current_quote_available"],
                "wait_succeeded": result.report["wait_succeeded"],
                "max_current_quote_age_seconds": result.report["max_current_quote_age_seconds"],
                "quote_age_seconds": result.report["quote_age_seconds"],
                "quote_freshness_verdict": result.report["quote_freshness_verdict"],
                "requested_quote_end": result.report["requested_quote_end"],
                "provider_available_end": result.report["provider_available_end"],
                "readiness_verdict": result.report["readiness_verdict"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "live_money_readiness": result.report["live_money_readiness"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.report["runner_verdict"] == "TRACK_B_READINESS_CHECK_READY_FOR_PAPER_PROOF_REVIEW" else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
