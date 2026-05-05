"""CLI for producing Track B Asian Drift 5m feature rows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .track_b_asian_drift_feature_rows import (
    DEFAULT_TRACK_B_ASIAN_DRIFT_FEATURE_ROWS_OUTPUT_ROOT,
    RECOVERY_CONFIRMED,
    TrackBAsianDriftFeatureRowsVerdict,
    produce_track_b_asian_drift_feature_rows,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Produce bounded Track B Asian Drift 5m feature rows from completed MGC 5m candles. "
            "No broker calls, no submit, and no inferred execution authority."
        )
    )
    parser.add_argument("--runtime-5m-candles-json", required=True, type=Path)
    parser.add_argument("--current-quote-report-json", type=Path)
    parser.add_argument("--expected-account-id", default="DUM882026")
    parser.add_argument("--account-id", default="DUM882026")
    parser.add_argument("--contract-key", default="MGC-202606")
    parser.add_argument("--instrument-family", default="MGC")
    parser.add_argument("--local-symbol", default="MGCM6")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--source-id", default="track_b_asian_drift_feature_rows")
    parser.add_argument("--strategy-id", default="asian_drift_v1")
    parser.add_argument("--lane-id", default="mgc_example_long_lmt_day")
    parser.add_argument("--calibration-profile", default=RECOVERY_CONFIRMED)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_ASIAN_DRIFT_FEATURE_ROWS_OUTPUT_ROOT)
    parser.add_argument(
        "--no-live-state",
        action="store_true",
        help="Only write feature rows; do not feed them into the Asian Drift live state producer.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    runtime_payload = json.loads(args.runtime_5m_candles_json.read_text(encoding="utf-8"))
    if not isinstance(runtime_payload, dict):
        raise SystemExit("--runtime-5m-candles-json must contain a JSON object.")
    quote_payload = None
    if args.current_quote_report_json is not None:
        quote_payload = json.loads(args.current_quote_report_json.read_text(encoding="utf-8"))
        if not isinstance(quote_payload, dict):
            raise SystemExit("--current-quote-report-json must contain a JSON object.")
    result = produce_track_b_asian_drift_feature_rows(
        runtime_5m_payload=runtime_payload,
        source_payload_path=args.runtime_5m_candles_json,
        current_quote_report_payload=quote_payload,
        current_quote_report_json=args.current_quote_report_json,
        expected_account_id=args.expected_account_id,
        account_id=args.account_id,
        contract_key=args.contract_key,
        instrument_family=args.instrument_family,
        local_symbol=args.local_symbol,
        dataset=args.dataset,
        source_id=args.source_id,
        strategy_id=args.strategy_id,
        lane_id=args.lane_id,
        calibration_profile=args.calibration_profile,
        output_root=args.output_root,
        invoke_live_state=not args.no_live_state,
    )
    print(
        json.dumps(
            {
                "asian_drift_feature_rows_verdict": result.report["asian_drift_feature_rows_verdict"],
                "asian_drift_watch_verdict": result.report["asian_drift_watch_verdict"],
                "feature_rows_ready": result.report["feature_rows_ready"],
                "feature_row_count": result.report["feature_row_count"],
                "live_state_invoked": result.report["live_state_invoked"],
                "live_state_verdict": result.report["live_state_verdict"],
                "asian_drift_state_ready": result.report["asian_drift_state_ready"],
                "latest_feature_rows_json_path": result.report["latest_feature_rows_json_path"],
                "latest_asian_drift_state_snapshot_path": result.report["latest_asian_drift_state_snapshot_path"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "paper_proof_invoked": result.report["paper_proof_invoked"],
                "submit_attempted": result.report["submit_attempted"],
                "broker_state_mutated": result.report["broker_state_mutated"],
                "live_money_readiness": result.report["live_money_readiness"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict == TrackBAsianDriftFeatureRowsVerdict.WROTE_ROWS else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
