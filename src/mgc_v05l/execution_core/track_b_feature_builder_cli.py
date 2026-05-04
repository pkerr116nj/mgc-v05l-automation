"""CLI wrapper for Track B no-submit MGC feature builder."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .track_b_feature_builder import (
    DEFAULT_TRACK_B_FEATURE_BUILDER_OUTPUT_ROOT,
    TrackBFeatureBuilderVerdict,
    build_track_b_mgc_feature_event,
)
from .track_b_strategy_rule_runner import DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build explicit EMA/VWAP momentum feature fields for the Track B MGC strategy rule. No-submit artifact path only."
    )
    parser.add_argument("--source-event-json", required=True, type=Path)
    parser.add_argument("--expected-account-id")
    parser.add_argument("--source-id")
    parser.add_argument("--rule-id", default=DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID)
    parser.add_argument("--min-history-candles", type=int, default=3)
    parser.add_argument("--ema-span", type=int, default=3)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_FEATURE_BUILDER_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = json.loads(args.source_event_json.read_text(encoding="utf-8"))
    result = build_track_b_mgc_feature_event(
        source_event_payload=payload,
        source_event_path=args.source_event_json,
        expected_account_id=args.expected_account_id,
        source_id=args.source_id,
        rule_id=args.rule_id,
        min_history_candles=args.min_history_candles,
        ema_span=args.ema_span,
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "feature_builder_verdict": result.report["feature_builder_verdict"],
                "rule_id": result.report["rule_id"],
                "source_quote_candle_event_path": result.report["source_quote_candle_event_path"],
                "feature_timestamp": result.report["feature_timestamp"],
                "quote_provider_mode": result.report["quote_provider_mode"],
                "realtime_quote_received": result.report["realtime_quote_received"],
                "current_quote_available": result.report["current_quote_available"],
                "signal_ready": result.report["signal_ready"],
                "output_feature_event_path": result.report["output_feature_event_path"],
                "latest_feature_event_path": result.report["latest_feature_event_path"],
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
    return 0 if result.verdict == TrackBFeatureBuilderVerdict.WROTE_FEATURE_EVENT else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
