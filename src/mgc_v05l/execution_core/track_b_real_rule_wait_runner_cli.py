"""CLI for bounded Track B real-rule wait mode."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .track_b_real_rule_wait_runner import (
    DEFAULT_TRACK_B_REAL_RULE_WAIT_RUNNER_OUTPUT_ROOT,
    TrackBRealRuleWaitRunnerConfig,
    TrackBRealRuleWaitRunnerVerdict,
    run_track_b_real_rule_wait,
)
from .track_b_runtime_candle_capture import DEFAULT_TRACK_B_RUNTIME_CANDLE_CAPTURE_OUTPUT_ROOT
from .track_b_strategy_paper_runner import (
    DEFAULT_TRACK_B_STRATEGY_PAPER_RUNNER_OUTPUT_ROOT,
    TrackBStrategyPaperRunnerConfig,
)
from .track_b_strategy_rule_runner import DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run bounded Track B real MGC rule polling. Defaults to no broker mutation unless explicit PAPER submit gates pass."
    )
    parser.add_argument("--mode", default="PAPER")
    parser.add_argument("--runtime-candle-context-json", type=Path, help="Existing latest_runtime_mgc_1m_candles.json to evaluate each cycle.")
    parser.add_argument("--runtime-candle-source-json", type=Path, help="Supplied runtime candle JSON to capture into latest runtime context each cycle.")
    parser.add_argument("--max-cycles", type=int, default=1)
    parser.add_argument("--poll-seconds", type=float, default=0.0)
    parser.add_argument("--max-runtime-seconds", type=float)
    parser.add_argument("--runtime-candle-max-bars", type=int, default=250)
    parser.add_argument("--runtime-candle-min-bars", type=int, default=3)
    parser.add_argument("--runtime-candle-retention-runs", type=int, default=5)
    parser.add_argument("--runtime-candle-capture-output-root", type=Path, default=DEFAULT_TRACK_B_RUNTIME_CANDLE_CAPTURE_OUTPUT_ROOT)
    parser.add_argument("--inbox-dir", required=True, type=Path)
    parser.add_argument("--source-id", default="track_b_real_rule_wait_runner")
    parser.add_argument("--strategy-id", default="track_b_example_gold_shadow_v1")
    parser.add_argument("--lane-id", default="mgc_example_long_lmt_day")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7497)
    parser.add_argument("--client-id", type=int, default=17086)
    parser.add_argument("--account-id", default="DUM882026")
    parser.add_argument("--expected-account-id", default="DUM882026")
    parser.add_argument("--contract-key", default="MGC-202606")
    parser.add_argument("--side", default="BUY")
    parser.add_argument("--quantity", type=int)
    parser.add_argument("--order-type", default="LMT")
    parser.add_argument("--time-in-force", default="DAY")
    parser.add_argument("--submit-paper", action="store_true")
    parser.add_argument("--confirm-paper-submit", action="store_true")
    parser.add_argument("--manual-open-limit-price")
    parser.add_argument("--manual-close-limit-price")
    parser.add_argument("--broker-order-id")
    parser.add_argument("--perm-id")
    parser.add_argument("--market-data-mode", default="DELAYED")
    parser.add_argument("--databento-continuous-symbol", default="MGC.v.0")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--allowlisted-local-symbol", default="MGCM6")
    parser.add_argument("--con-id", type=int, default=712565978)
    parser.add_argument("--tick-size", default="0.1")
    parser.add_argument("--exchange", default="COMEX")
    parser.add_argument("--currency", default="USD")
    parser.add_argument("--timeframe", default="quote_snapshot")
    parser.add_argument("--proof-timing-status", default="ACTIVE_SESSION", choices=["ACTIVE_SESSION", "OUTSIDE_ACTIVE_SESSION", "UNKNOWN"])
    parser.add_argument("--proof-timing-source", default="track_b_real_rule_wait_runner")
    parser.add_argument("--proof-timing-detail")
    parser.add_argument("--max-wait-cycles", type=int, default=1)
    parser.add_argument("--wait-poll-seconds", type=float, default=0.0)
    parser.add_argument("--max-current-quote-age-seconds", type=int)
    parser.add_argument("--quote-provider-mode", default="REALTIME")
    parser.add_argument("--realtime-receive-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--quote-timeout-seconds", type=float, default=3.0)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_REAL_RULE_WAIT_RUNNER_OUTPUT_ROOT)
    parser.add_argument("--strategy-paper-output-root", type=Path, default=DEFAULT_TRACK_B_STRATEGY_PAPER_RUNNER_OUTPUT_ROOT)
    parser.add_argument("--feature-builder-output-root", type=Path, default=Path("outputs/track_b_execution_core/track_b_feature_builder"))
    parser.add_argument("--feature-builder-min-history-candles", type=int, default=3)
    parser.add_argument("--feature-builder-ema-span", type=int, default=3)
    parser.add_argument("--market-history-output-root", type=Path, default=Path("outputs/track_b_execution_core/track_b_market_history"))
    parser.add_argument("--market-history-max-candles", type=int, default=50)
    parser.add_argument("--market-history-min-candles", type=int, default=3)
    parser.add_argument("--strategy-rule-output-root", type=Path, default=Path("outputs/track_b_execution_core/track_b_strategy_rule_runner"))
    parser.add_argument("--strategy-adapter-output-root", type=Path, default=Path("outputs/track_b_execution_core/strategy_signal_adapter"))
    parser.add_argument("--candle-producer-output-root", type=Path, default=Path("outputs/track_b_execution_core/candle_signal_producer"))
    parser.add_argument("--writer-output-root", type=Path, default=Path("outputs/track_b_execution_core/signal_batch_writer"))
    parser.add_argument("--readiness-output-root", type=Path, default=Path("outputs/track_b_execution_core/track_b_readiness_check_runner"))
    parser.add_argument("--recovery-output-root", type=Path, default=Path("outputs/track_b_execution_core/recovery_status"))
    parser.add_argument("--preflight-output-root", type=Path, default=Path("outputs/track_b_execution_core/preflight"))
    parser.add_argument("--databento-observer-output-root", type=Path, default=Path("outputs/track_b_execution_core/databento_candle_observer"))
    parser.add_argument("--current-quote-output-root", type=Path, default=Path("outputs/track_b_execution_core/current_quotes"))
    parser.add_argument("--readiness-summary-output-root", type=Path, default=Path("outputs/track_b_execution_core/readiness_summary"))
    parser.add_argument("--paper-proof-output-root", type=Path, default=Path("outputs/track_b_execution_core/paper_proof"))
    parser.add_argument("--operator-status-output-root", type=Path, default=Path("outputs/track_b_execution_core/operator_status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    paper_config = TrackBStrategyPaperRunnerConfig(
        mode=args.mode,
        runtime_candle_context_json=args.runtime_candle_context_json,
        inbox_dir=args.inbox_dir,
        source_id=args.source_id,
        strategy_id=args.strategy_id,
        lane_id=args.lane_id,
        rule_id=DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID,
        rule_mode="MGC_EMA_MOMENTUM_RECLAIM_LONG",
        emit_signal=True,
        host=args.host,
        port=args.port,
        client_id=args.client_id,
        account_id=args.account_id,
        expected_account_id=args.expected_account_id,
        contract_key=args.contract_key,
        side=args.side,
        quantity=args.quantity,
        order_type=args.order_type,
        time_in_force=args.time_in_force,
        submit_paper=args.submit_paper,
        confirm_paper_submit=args.confirm_paper_submit,
        manual_open_limit_price=args.manual_open_limit_price,
        manual_close_limit_price=args.manual_close_limit_price,
        broker_order_id=args.broker_order_id,
        perm_id=args.perm_id,
        market_data_mode=args.market_data_mode,
        databento_continuous_symbol=args.databento_continuous_symbol,
        dataset=args.dataset,
        allowlisted_local_symbol=args.allowlisted_local_symbol,
        con_id=args.con_id,
        tick_size=args.tick_size,
        exchange=args.exchange,
        currency=args.currency,
        timeframe=args.timeframe,
        proof_timing_status=args.proof_timing_status,
        proof_timing_source=args.proof_timing_source,
        proof_timing_detail=args.proof_timing_detail,
        max_wait_cycles=args.max_wait_cycles,
        wait_poll_seconds=args.wait_poll_seconds,
        max_current_quote_age_seconds=args.max_current_quote_age_seconds,
        quote_provider_mode=args.quote_provider_mode,
        realtime_receive_timeout_seconds=args.realtime_receive_timeout_seconds,
        request_timeout_seconds=args.request_timeout_seconds,
        quote_timeout_seconds=args.quote_timeout_seconds,
        output_root=args.strategy_paper_output_root,
        runtime_candle_context_required=True,
        market_history_output_root=args.market_history_output_root,
        market_history_max_candles=args.market_history_max_candles,
        market_history_min_candles=args.market_history_min_candles,
        feature_builder_output_root=args.feature_builder_output_root,
        feature_builder_min_history_candles=args.feature_builder_min_history_candles,
        feature_builder_ema_span=args.feature_builder_ema_span,
        strategy_rule_output_root=args.strategy_rule_output_root,
        strategy_adapter_output_root=args.strategy_adapter_output_root,
        candle_producer_output_root=args.candle_producer_output_root,
        writer_output_root=args.writer_output_root,
        readiness_output_root=args.readiness_output_root,
        recovery_output_root=args.recovery_output_root,
        preflight_output_root=args.preflight_output_root,
        databento_observer_output_root=args.databento_observer_output_root,
        current_quote_output_root=args.current_quote_output_root,
        readiness_summary_output_root=args.readiness_summary_output_root,
        paper_proof_output_root=args.paper_proof_output_root,
        operator_status_output_root=args.operator_status_output_root,
    )
    result = run_track_b_real_rule_wait(
        config=TrackBRealRuleWaitRunnerConfig(
            strategy_paper_config=paper_config,
            runtime_candle_source_json=args.runtime_candle_source_json,
            runtime_candle_context_json=args.runtime_candle_context_json,
            max_cycles=args.max_cycles,
            poll_seconds=args.poll_seconds,
            max_runtime_seconds=args.max_runtime_seconds,
            runtime_candle_capture_output_root=args.runtime_candle_capture_output_root,
            runtime_candle_max_bars=args.runtime_candle_max_bars,
            runtime_candle_min_bars=args.runtime_candle_min_bars,
            runtime_candle_retention_runs=args.runtime_candle_retention_runs,
            output_root=args.output_root,
            source_id=args.source_id,
        )
    )
    print(
        json.dumps(
            {
                "real_rule_wait_runner_verdict": result.report["real_rule_wait_runner_verdict"],
                "cycles_attempted": result.report["cycles_attempted"],
                "final_rule_decision": result.report["final_rule_decision"],
                "ever_signal_emitted": result.report["ever_signal_emitted"],
                "mutation_attempted": result.report["mutation_attempted"],
                "paper_proof_invoked": result.report["paper_proof_invoked"],
                "submit_attempted": result.report["submit_attempted"],
                "broker_state_mutated": result.report["broker_state_mutated"],
                "live_money_readiness": result.report["live_money_readiness"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict in {
        TrackBRealRuleWaitRunnerVerdict.NO_SIGNAL_NO_MUTATION,
        TrackBRealRuleWaitRunnerVerdict.SIGNAL_READY_NO_SUBMIT,
        TrackBRealRuleWaitRunnerVerdict.SIGNAL_READY_FOR_EXPLICIT_PAPER_SUBMIT,
        TrackBRealRuleWaitRunnerVerdict.PAPER_PROOF_PASSED,
    } else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
