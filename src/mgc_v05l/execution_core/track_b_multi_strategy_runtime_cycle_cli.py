"""CLI for the bounded Track B multi-strategy runtime cycle."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .track_b_multi_strategy_runtime_cycle import (
    DEFAULT_TRACK_B_MULTI_STRATEGY_RUNTIME_CYCLE_OUTPUT_ROOT,
    TrackBMultiStrategyRuntimeCycleConfig,
    TrackBMultiStrategyRuntimeCycleVerdict,
    run_track_b_multi_strategy_runtime_cycle,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate registered Track B Asia strategy envelopes together and arbitrate at most one guarded PAPER candidate."
    )
    parser.add_argument("--asian-drift-event-json", type=Path)
    parser.add_argument("--pause-resume-short-event-json", type=Path)
    parser.add_argument("--breakout-retest-hold-long-event-json", type=Path)
    parser.add_argument("--first-bull-snap-turn-event-json", type=Path)
    parser.add_argument("--first-bear-snap-turn-event-json", type=Path)
    parser.add_argument("--london-late-pause-resume-short-event-json", type=Path)
    parser.add_argument("--asia-late-flat-pullback-pause-resume-long-event-json", type=Path)
    parser.add_argument("--inbox-dir", required=True, type=Path)
    parser.add_argument("--expected-account-id", default="DUM882026")
    parser.add_argument("--source-id", default="track_b_multi_strategy_runtime_cycle")
    parser.add_argument("--allow-fixture-input", action="store_true")
    parser.add_argument("--mode", default="PAPER")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7497)
    parser.add_argument("--client-id", type=int, default=17086)
    parser.add_argument("--account-id", default="DUM882026")
    parser.add_argument("--contract-key", default="MGC-202606")
    parser.add_argument("--side", default="BUY")
    parser.add_argument("--quantity", type=int)
    parser.add_argument("--submit-paper", action="store_true")
    parser.add_argument("--confirm-paper-submit", action="store_true")
    parser.add_argument("--manual-open-limit-price")
    parser.add_argument("--manual-close-limit-price")
    parser.add_argument("--allowlisted-local-symbol", default="MGCM6")
    parser.add_argument("--con-id", type=int, default=712565978)
    parser.add_argument("--proof-timing-status", default="ACTIVE_SESSION", choices=["ACTIVE_SESSION", "OUTSIDE_ACTIVE_SESSION", "UNKNOWN"])
    parser.add_argument("--proof-timing-source", default="track_b_multi_strategy_runtime_cycle")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_MULTI_STRATEGY_RUNTIME_CYCLE_OUTPUT_ROOT)
    parser.add_argument("--strategy-rule-output-root", type=Path, default=Path("outputs/track_b_execution_core/track_b_strategy_rule_runner"))
    parser.add_argument("--strategy-paper-runner-output-root", type=Path, default=Path("outputs/track_b_execution_core/track_b_strategy_paper_runner"))
    parser.add_argument("--update-operator-status", action="store_true")
    parser.add_argument("--operator-status-output-root", type=Path, default=Path("outputs/track_b_execution_core/operator_status"))
    parser.add_argument(
        "--backend-health-json",
        type=Path,
        default=Path("outputs/operator_dashboard/runtime/operator_dashboard_readiness.json"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_track_b_multi_strategy_runtime_cycle(
        config=TrackBMultiStrategyRuntimeCycleConfig(
            asian_drift_event_json=args.asian_drift_event_json,
            pause_resume_short_event_json=args.pause_resume_short_event_json,
            breakout_retest_hold_long_event_json=args.breakout_retest_hold_long_event_json,
            first_bull_snap_turn_event_json=args.first_bull_snap_turn_event_json,
            first_bear_snap_turn_event_json=args.first_bear_snap_turn_event_json,
            london_late_pause_resume_short_event_json=args.london_late_pause_resume_short_event_json,
            asia_late_flat_pullback_pause_resume_long_event_json=args.asia_late_flat_pullback_pause_resume_long_event_json,
            inbox_dir=args.inbox_dir,
            expected_account_id=args.expected_account_id,
            source_id=args.source_id,
            allow_fixture_input=args.allow_fixture_input,
            mode=args.mode,
            host=args.host,
            port=args.port,
            client_id=args.client_id,
            account_id=args.account_id,
            contract_key=args.contract_key,
            side=args.side,
            quantity=args.quantity,
            submit_paper=args.submit_paper,
            confirm_paper_submit=args.confirm_paper_submit,
            manual_open_limit_price=args.manual_open_limit_price,
            manual_close_limit_price=args.manual_close_limit_price,
            allowlisted_local_symbol=args.allowlisted_local_symbol,
            con_id=args.con_id,
            proof_timing_status=args.proof_timing_status,
            proof_timing_source=args.proof_timing_source,
            output_root=args.output_root,
            strategy_rule_output_root=args.strategy_rule_output_root,
            strategy_paper_runner_output_root=args.strategy_paper_runner_output_root,
            update_operator_status=args.update_operator_status,
            operator_status_output_root=args.operator_status_output_root,
            backend_health_json=args.backend_health_json,
        )
    )
    print(
        json.dumps(
            {
                "multi_strategy_runtime_cycle_verdict": result.report["multi_strategy_runtime_cycle_verdict"],
                "candidate_signals": result.report["candidate_signals"],
                "suppressed_signals": result.report["suppressed_signals"],
                "arbitration_result": result.report["arbitration_result"],
                "chosen_strategy_id": result.report["chosen_strategy_id"],
                "readiness_invoked": result.report["readiness_invoked"],
                "paper_proof_invoked": result.report["paper_proof_invoked"],
                "submit_attempted": result.report["submit_attempted"],
                "broker_state_mutated": result.report["broker_state_mutated"],
                "live_money_readiness": result.report["live_money_readiness"],
                "operator_status_invoked": result.report["operator_status_invoked"],
                "operator_status_verdict": result.report["operator_status_verdict"],
                "latest_operator_status_path": result.report["latest_operator_status_path"],
                "operator_status_error": result.report["operator_status_error"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict in {
        TrackBMultiStrategyRuntimeCycleVerdict.NO_SIGNAL_NO_MUTATION,
        TrackBMultiStrategyRuntimeCycleVerdict.SIGNAL_READY_NO_SUBMIT,
        TrackBMultiStrategyRuntimeCycleVerdict.PAPER_PROOF_PASSED,
    } else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
