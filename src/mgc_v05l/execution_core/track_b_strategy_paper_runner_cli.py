"""CLI for the Track B controlled PAPER strategy runner."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .track_b_strategy_paper_runner import (
    DEFAULT_TRACK_B_STRATEGY_PAPER_RUNNER_OUTPUT_ROOT,
    TrackBStrategyPaperRunnerConfig,
    TrackBStrategyPaperRunnerVerdict,
    run_track_b_strategy_paper,
)
from .track_b_strategy_rule_runner import DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID, TrackBStrategyRuleMode


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Track B strategy rule -> readiness -> explicit PAPER proof handoff. Defaults to no-submit."
    )
    parser.add_argument("--mode", default="PAPER")
    parser.add_argument("--input-event-json", type=Path, help="Existing strategy-rule input event JSON. Use --feature-event-json for an existing built feature event.")
    parser.add_argument("--maintained-history-json", type=Path, help="Track B data-maintenance latest_good_mgc_1m_history.json to combine with a separate realtime current quote report.")
    parser.add_argument("--candle-history-json", type=Path, help="Bounded MGC OHLCV history JSON to run through the Track B candle-history producer, market-history collector, and feature builder.")
    parser.add_argument("--current-quote-report-json", type=Path, help="Current quote or Databento observer report JSON proving realtime quote availability for --candle-history-json.")
    parser.add_argument("--build-features-from", type=Path, help="Source MGC candle/quote history JSON to run through track_b_feature_builder before strategy evaluation.")
    parser.add_argument("--feature-event-json", type=Path, help="Existing track_b_feature_builder output event JSON to pass to the strategy rule.")
    parser.add_argument("--inbox-dir", required=True, type=Path)
    parser.add_argument("--source-id", default="track_b_strategy_paper_runner")
    parser.add_argument("--strategy-id", default="track_b_example_gold_shadow_v1")
    parser.add_argument("--lane-id", default="mgc_example_long_lmt_day")
    parser.add_argument("--rule-id", default=DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID)
    parser.add_argument("--rule-mode", choices=[item.value for item in TrackBStrategyRuleMode], default=TrackBStrategyRuleMode.MGC_EMA_MOMENTUM_RECLAIM_LONG.value)
    parser.add_argument("--emit-signal", action="store_true")
    parser.add_argument("--allow-fixture-input", action="store_true")
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
    parser.add_argument("--tick-size", default="0.1")
    parser.add_argument("--exchange", default="COMEX")
    parser.add_argument("--currency", default="USD")
    parser.add_argument("--timeframe", default="quote_snapshot")
    parser.add_argument("--proof-timing-status", default="ACTIVE_SESSION", choices=["ACTIVE_SESSION", "OUTSIDE_ACTIVE_SESSION", "UNKNOWN"])
    parser.add_argument("--proof-timing-source", default="track_b_strategy_paper_runner")
    parser.add_argument("--proof-timing-detail")
    parser.add_argument("--max-wait-cycles", type=int, default=1)
    parser.add_argument("--wait-poll-seconds", type=float, default=0.0)
    parser.add_argument("--max-current-quote-age-seconds", type=int)
    parser.add_argument("--quote-provider-mode", default="REALTIME")
    parser.add_argument("--realtime-receive-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--quote-timeout-seconds", type=float, default=3.0)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_STRATEGY_PAPER_RUNNER_OUTPUT_ROOT)
    parser.add_argument("--data-maintenance-output-root", type=Path, default=Path("outputs/track_b_execution_core/track_b_data_maintenance"))
    parser.add_argument("--max-maintained-history-age-seconds", type=int, default=900)
    parser.add_argument("--candle-history-producer-output-root", type=Path, default=Path("outputs/track_b_execution_core/track_b_mgc_candle_history_producer"))
    parser.add_argument("--candle-history-max-candles", type=int, default=50)
    parser.add_argument("--candle-history-min-candles", type=int, default=3)
    parser.add_argument("--market-history-output-root", type=Path, default=Path("outputs/track_b_execution_core/track_b_market_history"))
    parser.add_argument("--market-history-max-candles", type=int, default=50)
    parser.add_argument("--market-history-min-candles", type=int, default=3)
    parser.add_argument("--feature-builder-output-root", type=Path, default=Path("outputs/track_b_execution_core/track_b_feature_builder"))
    parser.add_argument("--feature-builder-min-history-candles", type=int, default=3)
    parser.add_argument("--feature-builder-ema-span", type=int, default=3)
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
    result = run_track_b_strategy_paper(
        config=TrackBStrategyPaperRunnerConfig(
            mode=args.mode,
            input_event_json=args.input_event_json,
            maintained_history_json=args.maintained_history_json,
            candle_history_json=args.candle_history_json,
            current_quote_report_json=args.current_quote_report_json,
            build_features_from_json=args.build_features_from,
            feature_event_json=args.feature_event_json,
            inbox_dir=args.inbox_dir,
            source_id=args.source_id,
            strategy_id=args.strategy_id,
            lane_id=args.lane_id,
            rule_id=args.rule_id,
            rule_mode=args.rule_mode,
            emit_signal=args.emit_signal,
            allow_fixture_input=args.allow_fixture_input,
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
            output_root=args.output_root,
            data_maintenance_output_root=args.data_maintenance_output_root,
            max_maintained_history_age_seconds=args.max_maintained_history_age_seconds,
            candle_history_producer_output_root=args.candle_history_producer_output_root,
            candle_history_max_candles=args.candle_history_max_candles,
            candle_history_min_candles=args.candle_history_min_candles,
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
    )
    print(
        json.dumps(
            {
                "strategy_paper_runner_verdict": result.report["strategy_paper_runner_verdict"],
                "mode": result.report["mode"],
                "maintained_history_path": result.report["maintained_history_path"],
                "data_maintenance_history_requested": result.report["data_maintenance_history_requested"],
                "maintained_history_age_seconds": result.report["maintained_history_age_seconds"],
                "max_maintained_history_age_seconds": result.report["max_maintained_history_age_seconds"],
                "maintained_history_ready": result.report["maintained_history_ready"],
                "candle_history_producer_invoked": result.report["candle_history_producer_invoked"],
                "candle_history_producer_verdict": result.report["candle_history_producer_verdict"],
                "candle_history_input_path": result.report["candle_history_input_path"],
                "market_history_collector_invoked": result.report["market_history_collector_invoked"],
                "market_history_collector_verdict": result.report["market_history_collector_verdict"],
                "market_history_event_path": result.report["market_history_event_path"],
                "feature_builder_invoked": result.report["feature_builder_invoked"],
                "feature_builder_verdict": result.report["feature_builder_verdict"],
                "feature_event_path": result.report["feature_event_path"],
                "strategy_rule_verdict": result.report["strategy_rule_verdict"],
                "rule_decision": result.report["rule_decision"],
                "signal_emitted": result.report["signal_emitted"],
                "signal_direction": result.report["signal_direction"],
                "readiness_invoked": result.report["readiness_invoked"],
                "readiness_runner_verdict": result.report["readiness_runner_verdict"],
                "readiness_verdict": result.report["readiness_verdict"],
                "paper_submit_requested": result.report["paper_submit_requested"],
                "paper_proof_invoked": result.report["paper_proof_invoked"],
                "paper_proof_classification": result.report["paper_proof_classification"],
                "paper_proof_report_path": result.report["paper_proof_report_path"],
                "final_flat": result.report["final_flat"],
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
    if result.verdict in {
        TrackBStrategyPaperRunnerVerdict.NO_SIGNAL,
        TrackBStrategyPaperRunnerVerdict.HUMAN_REVIEW_NO_SIGNAL,
        TrackBStrategyPaperRunnerVerdict.PAPER_READY_NO_SUBMIT_REQUESTED,
        TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_PASSED,
    }:
        return 0
    if result.verdict in {
        TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
        TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE,
    }:
        return 3
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
