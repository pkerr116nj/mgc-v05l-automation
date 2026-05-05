"""CLI for Track B snap-turn envelope production."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .track_b_snap_turn_envelope_producer import (
    DEFAULT_TRACK_B_SNAP_TURN_ENVELOPE_OUTPUT_ROOT,
    TrackBSnapTurnEnvelopeProducerVerdict,
    produce_track_b_snap_turn_envelopes,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Produce explicit Track B FIRST_BULL/FIRST_BEAR snap-turn feature/state envelopes "
            "from bounded completed realtime MGC 5m candles."
        )
    )
    parser.add_argument("--runtime-5m-candles-json", required=True, type=Path)
    parser.add_argument("--expected-account-id", default="DUM882026")
    parser.add_argument("--source-id", default="track_b_snap_turn_envelope_producer")
    parser.add_argument("--min-completed-bars", type=int, default=8)
    parser.add_argument("--prior-bars-since-bull-snap", type=int)
    parser.add_argument("--prior-bars-since-bear-snap", type=int)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_SNAP_TURN_ENVELOPE_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = json.loads(args.runtime_5m_candles_json.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit("runtime 5m candles JSON must contain an object")
    result = produce_track_b_snap_turn_envelopes(
        runtime_5m_payload=payload,
        runtime_5m_payload_path=args.runtime_5m_candles_json,
        expected_account_id=args.expected_account_id,
        source_id=args.source_id,
        output_root=args.output_root,
        min_completed_bars=args.min_completed_bars,
        prior_bars_since_bull_snap=args.prior_bars_since_bull_snap,
        prior_bars_since_bear_snap=args.prior_bars_since_bear_snap,
    )
    print(
        json.dumps(
            {
                "snap_turn_envelope_producer_verdict": result.verdict.value,
                "first_bull_snap_turn_envelope_ready": result.report.get("first_bull_snap_turn_envelope_ready"),
                "first_bear_snap_turn_envelope_ready": result.report.get("first_bear_snap_turn_envelope_ready"),
                "first_bull_snap_turn_event_json": (
                    None if result.first_bull_snap_turn_event_json is None else str(result.first_bull_snap_turn_event_json)
                ),
                "first_bear_snap_turn_event_json": (
                    None if result.first_bear_snap_turn_event_json is None else str(result.first_bear_snap_turn_event_json)
                ),
                "primary_blocker": result.report.get("primary_blocker"),
                "required_next_action": result.report.get("required_next_action"),
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict == TrackBSnapTurnEnvelopeProducerVerdict.WROTE_ENVELOPES else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
