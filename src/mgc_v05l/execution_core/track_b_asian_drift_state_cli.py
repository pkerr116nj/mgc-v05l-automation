"""CLI for writing explicit Track B Asian Drift state snapshots."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .track_b_asian_drift_state import (
    DEFAULT_TRACK_B_ASIAN_DRIFT_STATE_OUTPUT_ROOT,
    TrackBAsianDriftStateVerdict,
    write_track_b_asian_drift_state_snapshot,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate and write an explicit Asian Drift 5m state snapshot. Does not infer strategy state from raw candles."
    )
    parser.add_argument("--state-json", required=True, type=Path)
    parser.add_argument("--expected-account-id", default="DUM882026")
    parser.add_argument("--account-id", default="DUM882026")
    parser.add_argument("--contract-key", default="MGC-202606")
    parser.add_argument("--instrument-family", default="MGC")
    parser.add_argument("--source-id", default="track_b_asian_drift_state")
    parser.add_argument("--strategy-id", default="asian_drift_v1")
    parser.add_argument("--lane-id", default="mgc_example_long_lmt_day")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_ASIAN_DRIFT_STATE_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = json.loads(args.state_json.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit("--state-json must contain a JSON object.")
    result = write_track_b_asian_drift_state_snapshot(
        state_payload=payload,
        source_payload_path=args.state_json,
        expected_account_id=args.expected_account_id,
        account_id=args.account_id,
        contract_key=args.contract_key,
        instrument_family=args.instrument_family,
        source_id=args.source_id,
        strategy_id=args.strategy_id,
        lane_id=args.lane_id,
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "asian_drift_state_builder_verdict": result.report["asian_drift_state_builder_verdict"],
                "asian_drift_watch_verdict": result.report["asian_drift_watch_verdict"],
                "asian_drift_state_ready": result.report["asian_drift_state_ready"],
                "latest_asian_drift_state_snapshot_path": result.report["latest_asian_drift_state_snapshot_path"],
                "readiness_invoked": result.report["readiness_invoked"],
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
    return 0 if result.verdict == TrackBAsianDriftStateVerdict.WROTE_SNAPSHOT else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
