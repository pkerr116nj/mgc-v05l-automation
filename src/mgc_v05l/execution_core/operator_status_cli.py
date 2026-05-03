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
    parser.add_argument("--listener-heartbeat-json", type=Path)
    parser.add_argument("--listener-health-json", type=Path)
    parser.add_argument("--listener-cycle-json", type=Path)
    parser.add_argument("--shadow-runner-summary-json", type=Path)
    parser.add_argument("--attrition-report-json", type=Path)
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
            listener_heartbeat_json=args.listener_heartbeat_json,
            listener_health_json=args.listener_health_json,
            listener_cycle_json=args.listener_cycle_json,
            shadow_runner_summary_json=args.shadow_runner_summary_json,
            attrition_report_json=args.attrition_report_json,
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
                "listener_mode": result.report["listener_mode"],
                "listener_current_cycle_number": result.report["listener_current_cycle_number"],
                "listener_last_health_verdict": result.report["listener_last_health_verdict"],
                "shadow_listener_health_verdict": result.report["shadow_listener_health_verdict"],
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
