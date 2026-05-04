"""CLI wrapper for Track B no-submit strategy rule runner."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .candle_signal_producer import DEFAULT_CANDLE_SIGNAL_PRODUCER_OUTPUT_ROOT
from .signal_batch_writer import DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT
from .strategy_signal_adapter import DEFAULT_STRATEGY_SIGNAL_ADAPTER_OUTPUT_ROOT
from .track_b_strategy_rule_runner import (
    DEFAULT_TRACK_B_STRATEGY_RULE_RUNNER_OUTPUT_ROOT,
    TrackBStrategyRuleMode,
    TrackBStrategyRuleRunnerVerdict,
    run_track_b_strategy_rule,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate one narrow Track B no-submit strategy rule against realtime Databento quote/candle evidence."
    )
    parser.add_argument("--input-event-json", required=True, type=Path)
    parser.add_argument("--inbox-dir", required=True, type=Path)
    parser.add_argument("--expected-account-id")
    parser.add_argument("--source-id")
    parser.add_argument("--strategy-id")
    parser.add_argument("--lane-id")
    parser.add_argument("--rule-id", default="mgc_realtime_quote_demo_long_v1")
    parser.add_argument("--rule-mode", choices=[item.value for item in TrackBStrategyRuleMode], default=TrackBStrategyRuleMode.HUMAN_REVIEW_ONLY.value)
    parser.add_argument("--emit-signal", action="store_true", help="Explicitly emit the no-submit rule signal when rule conditions pass.")
    parser.add_argument(
        "--allow-fixture-input",
        action="store_true",
        help="Allow fixture/demo evidence in tests or demos. Do not use this for realtime paper-readiness review.",
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_STRATEGY_RULE_RUNNER_OUTPUT_ROOT)
    parser.add_argument("--strategy-adapter-output-root", type=Path, default=DEFAULT_STRATEGY_SIGNAL_ADAPTER_OUTPUT_ROOT)
    parser.add_argument("--candle-producer-output-root", type=Path, default=DEFAULT_CANDLE_SIGNAL_PRODUCER_OUTPUT_ROOT)
    parser.add_argument("--writer-output-root", type=Path, default=DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = json.loads(args.input_event_json.read_text(encoding="utf-8"))
    result = run_track_b_strategy_rule(
        input_event_payload=payload,
        input_event_path=args.input_event_json,
        inbox_dir=args.inbox_dir,
        expected_account_id=args.expected_account_id,
        source_id=args.source_id,
        strategy_id=args.strategy_id,
        lane_id=args.lane_id,
        rule_id=args.rule_id,
        rule_mode=args.rule_mode,
        emit_signal=args.emit_signal,
        allow_fixture_input=args.allow_fixture_input,
        output_root=args.output_root,
        strategy_adapter_output_root=args.strategy_adapter_output_root,
        candle_producer_output_root=args.candle_producer_output_root,
        writer_output_root=args.writer_output_root,
    )
    print(
        json.dumps(
            {
                "strategy_rule_runner_verdict": result.report["strategy_rule_runner_verdict"],
                "strategy_rule_id": result.report["strategy_rule_id"],
                "rule_name": result.report["rule_name"],
                "rule_mode": result.report["rule_mode"],
                "decision": result.report["decision"],
                "decision_reason": result.report["decision_reason"],
                "signal_emitted": result.report["signal_emitted"],
                "signal_direction": result.report["signal_direction"],
                "input_quote_provider_mode": result.report["input_quote_provider_mode"],
                "realtime_quote_received": result.report["realtime_quote_received"],
                "current_quote_available": result.report["current_quote_available"],
                "downstream_strategy_adapter_report_path": result.report["downstream_strategy_adapter_report_path"],
                "downstream_candle_producer_report_path": result.report["downstream_candle_producer_report_path"],
                "downstream_signal_batch_writer_report_path": result.report["downstream_signal_batch_writer_report_path"],
                "output_batch_path": result.report["output_batch_path"],
                "paper_proof_cli_called": result.report["paper_proof_cli_called"],
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
    return 0 if result.verdict in {
        TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL,
        TrackBStrategyRuleRunnerVerdict.HUMAN_REVIEW_NO_SIGNAL,
        TrackBStrategyRuleRunnerVerdict.NO_SIGNAL,
    } else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
