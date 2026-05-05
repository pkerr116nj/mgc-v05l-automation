"""CLI for Track B session-strategy envelope production."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .track_b_session_strategy_envelope_producer import (
    DEFAULT_TRACK_B_SESSION_STRATEGY_ENVELOPE_OUTPUT_ROOT,
    TrackBSessionStrategyEnvelopeProducerVerdict,
    produce_track_b_session_strategy_envelopes,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Produce explicit Track B Asia Early, London-late, and Asia-late session strategy feature/state envelopes "
            "from bounded completed realtime MGC 5m candles."
        )
    )
    parser.add_argument("--runtime-5m-candles-json", required=True, type=Path)
    parser.add_argument("--expected-account-id", default="DUM882026")
    parser.add_argument("--source-id", default="track_b_session_strategy_envelope_producer")
    parser.add_argument("--min-completed-bars", type=int, default=8)
    parser.add_argument(
        "--max-completed-5m-age-seconds",
        type=int,
        default=900,
        help="Reject runtime context when the latest completed 5m candle is older than this threshold.",
    )
    parser.add_argument("--prior-bars-since-long-setup", type=int)
    parser.add_argument("--prior-bars-since-short-setup", type=int)
    parser.add_argument("--prior-bars-since-bull-snap", type=int)
    parser.add_argument("--prior-bars-since-bear-snap", type=int)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_SESSION_STRATEGY_ENVELOPE_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = json.loads(args.runtime_5m_candles_json.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit("runtime 5m candles JSON must contain an object")
    result = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=payload,
        runtime_5m_payload_path=args.runtime_5m_candles_json,
        expected_account_id=args.expected_account_id,
        source_id=args.source_id,
        output_root=args.output_root,
        min_completed_bars=args.min_completed_bars,
        max_completed_5m_age_seconds=args.max_completed_5m_age_seconds,
        prior_bars_since_long_setup=args.prior_bars_since_long_setup,
        prior_bars_since_short_setup=args.prior_bars_since_short_setup,
        prior_bars_since_bull_snap=args.prior_bars_since_bull_snap,
        prior_bars_since_bear_snap=args.prior_bars_since_bear_snap,
    )
    print(
        json.dumps(
            {
                "session_strategy_envelope_producer_verdict": result.verdict.value,
                "london_late_pause_resume_short_envelope_ready": result.report.get(
                    "london_late_pause_resume_short_envelope_ready"
                ),
                "asia_late_flat_pullback_pause_resume_long_envelope_ready": result.report.get(
                    "asia_late_flat_pullback_pause_resume_long_envelope_ready"
                ),
                "asia_early_pause_resume_short_envelope_ready": result.report.get(
                    "asia_early_pause_resume_short_envelope_ready"
                ),
                "asia_early_normal_breakout_retest_hold_long_envelope_ready": result.report.get(
                    "asia_early_normal_breakout_retest_hold_long_envelope_ready"
                ),
                "latest_completed_5m_candle_timestamp": result.report.get("latest_completed_5m_candle_timestamp"),
                "latest_completed_5m_candle_age_seconds": result.report.get("latest_completed_5m_candle_age_seconds"),
                "max_completed_5m_candle_age_seconds": result.report.get("max_completed_5m_candle_age_seconds"),
                "runtime_candle_context_stale": result.report.get("runtime_candle_context_stale"),
                "london_late_pause_resume_short_event_json": (
                    None
                    if result.london_late_pause_resume_short_event_json is None
                    else str(result.london_late_pause_resume_short_event_json)
                ),
                "asia_late_flat_pullback_pause_resume_long_event_json": (
                    None
                    if result.asia_late_flat_pullback_pause_resume_long_event_json is None
                    else str(result.asia_late_flat_pullback_pause_resume_long_event_json)
                ),
                "asia_early_pause_resume_short_event_json": (
                    None
                    if result.asia_early_pause_resume_short_event_json is None
                    else str(result.asia_early_pause_resume_short_event_json)
                ),
                "asia_early_normal_breakout_retest_hold_long_event_json": (
                    None
                    if result.asia_early_normal_breakout_retest_hold_long_event_json is None
                    else str(result.asia_early_normal_breakout_retest_hold_long_event_json)
                ),
                "primary_blocker": result.report.get("primary_blocker"),
                "required_next_action": result.report.get("required_next_action"),
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict == TrackBSessionStrategyEnvelopeProducerVerdict.WROTE_ENVELOPES else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
