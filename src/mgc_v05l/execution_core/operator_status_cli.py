"""CLI wrapper for Track B no-submit operator status summary."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .operator_status import DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT, OperatorStatusInputs, create_operator_status_summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create a Track B no-submit operator status summary from observer reports.")
    parser.add_argument("--backend-health-json", type=Path)
    parser.add_argument("--listener-heartbeat-json", type=Path)
    parser.add_argument("--listener-health-json", type=Path)
    parser.add_argument("--listener-cycle-json", type=Path)
    parser.add_argument("--shadow-runner-summary-json", type=Path)
    parser.add_argument("--attrition-report-json", type=Path)
    parser.add_argument("--track-b-observation-runner-report-json", type=Path)
    parser.add_argument("--track-b-readiness-check-runner-report-json", type=Path)
    parser.add_argument("--track-b-strategy-rule-runner-report-json", type=Path)
    parser.add_argument("--track-b-strategy-paper-runner-report-json", type=Path)
    parser.add_argument("--track-b-multi-strategy-runtime-cycle-report-json", type=Path)
    parser.add_argument("--track-b-shadow-monitor-report-json", type=Path)
    parser.add_argument("--track-b-shadow-monitor-heartbeat-json", type=Path)
    parser.add_argument("--databento-candle-observer-report-json", type=Path)
    parser.add_argument("--databento-candle-observer-heartbeat-json", type=Path)
    parser.add_argument("--strategy-signal-adapter-report-json", type=Path)
    parser.add_argument("--candle-signal-producer-report-json", type=Path)
    parser.add_argument("--signal-batch-writer-report-json", type=Path)
    parser.add_argument("--readiness-summary-json", type=Path)
    parser.add_argument("--recovery-report-json", type=Path)
    parser.add_argument("--preflight-report-json", type=Path)
    parser.add_argument("--quote-report-json", type=Path)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            backend_health_json=args.backend_health_json,
            listener_heartbeat_json=args.listener_heartbeat_json,
            listener_health_json=args.listener_health_json,
            listener_cycle_json=args.listener_cycle_json,
            shadow_runner_summary_json=args.shadow_runner_summary_json,
            attrition_report_json=args.attrition_report_json,
            track_b_observation_runner_report_json=args.track_b_observation_runner_report_json,
            track_b_readiness_check_runner_report_json=args.track_b_readiness_check_runner_report_json,
            track_b_strategy_rule_runner_report_json=args.track_b_strategy_rule_runner_report_json,
            track_b_strategy_paper_runner_report_json=args.track_b_strategy_paper_runner_report_json,
            track_b_multi_strategy_runtime_cycle_report_json=args.track_b_multi_strategy_runtime_cycle_report_json,
            track_b_shadow_monitor_report_json=args.track_b_shadow_monitor_report_json,
            track_b_shadow_monitor_heartbeat_json=args.track_b_shadow_monitor_heartbeat_json,
            databento_candle_observer_report_json=args.databento_candle_observer_report_json,
            databento_candle_observer_heartbeat_json=args.databento_candle_observer_heartbeat_json,
            strategy_signal_adapter_report_json=args.strategy_signal_adapter_report_json,
            candle_signal_producer_report_json=args.candle_signal_producer_report_json,
            signal_batch_writer_report_json=args.signal_batch_writer_report_json,
            readiness_summary_json=args.readiness_summary_json,
            recovery_report_json=args.recovery_report_json,
            preflight_report_json=args.preflight_report_json,
            quote_report_json=args.quote_report_json,
            output_root=args.output_root,
        )
    )
    print(
        json.dumps(
            {
                "status_verdict": result.report["status_verdict"],
                "backend_health_status": result.report["backend_health_status"],
                "backend_health_ready": result.report["backend_health_ready"],
                "backend_health_url": result.report["backend_health_url"],
                "listener_mode": result.report["listener_mode"],
                "listener_current_cycle_number": result.report["listener_current_cycle_number"],
                "listener_last_health_verdict": result.report["listener_last_health_verdict"],
                "shadow_listener_health_verdict": result.report["shadow_listener_health_verdict"],
                "observation_runner_verdict": result.report["observation_runner_verdict"],
                "observation_runner_mode": result.report["observation_runner_mode"],
                "observation_runner_current_cycle": result.report["observation_runner_current_cycle"],
                "observation_runner_watch_exited_normally": result.report["observation_runner_watch_exited_normally"],
                "readiness_check_runner_verdict": result.report["readiness_check_runner_verdict"],
                "readiness_check_runner_current_quote_available": result.report["readiness_check_runner_current_quote_available"],
                "readiness_check_runner_quote_provider_mode": result.report["readiness_check_runner_quote_provider_mode"],
                "readiness_check_runner_realtime_quote_received": result.report["readiness_check_runner_realtime_quote_received"],
                "readiness_check_runner_quote_freshness_verdict": result.report["readiness_check_runner_quote_freshness_verdict"],
                "readiness_check_runner_wait_succeeded": result.report["readiness_check_runner_wait_succeeded"],
                "readiness_check_runner_readiness_verdict": result.report["readiness_check_runner_readiness_verdict"],
                "readiness_check_runner_paper_proof_cli_called": result.report["readiness_check_runner_paper_proof_cli_called"],
                "strategy_rule_runner_verdict": result.report["strategy_rule_runner_verdict"],
                "strategy_rule_id": result.report["strategy_rule_id"],
                "strategy_rule_decision": result.report["strategy_rule_decision"],
                "strategy_rule_signal_emitted": result.report["strategy_rule_signal_emitted"],
                "strategy_rule_signal_direction": result.report["strategy_rule_signal_direction"],
                "strategy_rule_output_batch_path": result.report["strategy_rule_output_batch_path"],
                "strategy_paper_runner_verdict": result.report["strategy_paper_runner_verdict"],
                "strategy_paper_rule_decision": result.report["strategy_paper_rule_decision"],
                "strategy_paper_readiness_verdict": result.report["strategy_paper_readiness_verdict"],
                "strategy_paper_proof_invoked": result.report["strategy_paper_proof_invoked"],
                "strategy_paper_proof_classification": result.report["strategy_paper_proof_classification"],
                "strategy_paper_final_flat": result.report["strategy_paper_final_flat"],
                "latest_shadow_monitor_verdict": result.report["latest_shadow_monitor_verdict"],
                "latest_shadow_monitor_cycle_id": result.report["latest_shadow_monitor_cycle_id"],
                "shadow_monitor_running": result.report["shadow_monitor_running"],
                "shadow_monitor_instrument_families": result.report["shadow_monitor_instrument_families"],
                "shadow_monitor_evaluated_strategy_count": result.report["shadow_monitor_evaluated_strategy_count"],
                "shadow_monitor_submit_allowed": result.report["shadow_monitor_submit_allowed"],
                "shadow_monitor_submit_attempted": result.report["shadow_monitor_submit_attempted"],
                "shadow_monitor_broker_state_mutated": result.report["shadow_monitor_broker_state_mutated"],
                "shadow_monitor_live_money_readiness": result.report["shadow_monitor_live_money_readiness"],
                "multi_strategy_runtime_cycle_verdict": result.report["multi_strategy_runtime_cycle_verdict"],
                "multi_strategy_chosen_strategy_id": result.report["multi_strategy_chosen_strategy_id"],
                "multi_strategy_candidate_signals": result.report["multi_strategy_candidate_signals"],
                "multi_strategy_suppressed_signals": result.report["multi_strategy_suppressed_signals"],
                "multi_strategy_readiness_invoked": result.report["multi_strategy_readiness_invoked"],
                "multi_strategy_paper_proof_invoked": result.report["multi_strategy_paper_proof_invoked"],
                "multi_strategy_submit_attempted": result.report["multi_strategy_submit_attempted"],
                "multi_strategy_broker_state_mutated": result.report["multi_strategy_broker_state_mutated"],
                "databento_observer_verdict": result.report["databento_observer_verdict"],
                "databento_observer_mode": result.report["databento_observer_mode"],
                "databento_observer_current_cycle": result.report["databento_observer_current_cycle"],
                "databento_observer_last_verdict": result.report["databento_observer_last_verdict"],
                "strategy_adapter_verdict": result.report["strategy_adapter_verdict"],
                "candle_producer_verdict": result.report["candle_producer_verdict"],
                "signal_batch_writer_verdict": result.report["signal_batch_writer_verdict"],
                "signal_batch_writer_batch_json_path": result.report["signal_batch_writer_batch_json_path"],
                "readiness_verdict": result.report["readiness_verdict"],
                "recovery_verdict": result.report["recovery_verdict"],
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


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
