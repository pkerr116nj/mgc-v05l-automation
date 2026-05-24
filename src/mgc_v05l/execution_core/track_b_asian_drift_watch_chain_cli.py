"""CLI for the Track B no-submit Asian Drift watch chain."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .track_b_asian_drift_watch_chain import (
    DEFAULT_TRACK_B_ASIAN_DRIFT_WATCH_CHAIN_OUTPUT_ROOT,
    TrackBAsianDriftWatchChainVerdict,
    run_track_b_asian_drift_watch_chain,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the bounded no-submit Track B Asian Drift watch chain from accumulated "
            "MGC 1m/5m candles through ASIAN_DRIFT_V1."
        )
    )
    parser.add_argument("--source-candles-json", required=True, type=Path)
    parser.add_argument("--current-quote-report-json", type=Path)
    parser.add_argument("--inbox-dir", type=Path, default=Path("examples/track_b_shadow_listener/inbox"))
    parser.add_argument("--expected-account-id", default="DUM882026")
    parser.add_argument("--account-id", default="DUM882026")
    parser.add_argument("--contract-key", default="MGC-202606")
    parser.add_argument("--instrument-family", default="MGC")
    parser.add_argument("--local-symbol", default="MGCM6")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--source-id", default="track_b_asian_drift_watch_chain")
    parser.add_argument("--strategy-id", default="asian_drift_v1")
    parser.add_argument("--lane-id", default="mgc_example_long_lmt_day")
    parser.add_argument("--max-source-bars", type=int, default=250)
    parser.add_argument(
        "--allow-legacy-runtime-candles",
        action="store_true",
        help="Allow diagnostic legacy candle paths; default P0 authority requires Phase-1 runtime market data.",
    )
    parser.add_argument(
        "--max-completed-5m-age-seconds",
        type=int,
        default=900,
        help="Reject runtime context when the latest completed 5m candle is older than this threshold.",
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_ASIAN_DRIFT_WATCH_CHAIN_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    candle_payload = json.loads(args.source_candles_json.read_text(encoding="utf-8"))
    if not isinstance(candle_payload, dict):
        raise SystemExit("--source-candles-json must contain a JSON object.")
    quote_payload = None
    if args.current_quote_report_json is not None:
        quote_payload = json.loads(args.current_quote_report_json.read_text(encoding="utf-8"))
        if not isinstance(quote_payload, dict):
            raise SystemExit("--current-quote-report-json must contain a JSON object.")
    result = run_track_b_asian_drift_watch_chain(
        candle_payload=candle_payload,
        source_payload_path=args.source_candles_json,
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
        max_source_bars=args.max_source_bars,
        allow_legacy_runtime_candles=bool(args.allow_legacy_runtime_candles),
        max_completed_5m_age_seconds=args.max_completed_5m_age_seconds,
        inbox_dir=args.inbox_dir,
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "asian_drift_watch_chain_verdict": result.report["asian_drift_watch_chain_verdict"],
                "asian_drift_watch_verdict": result.report["asian_drift_watch_verdict"],
                "completed_5m_bars_available": result.report["completed_5m_bars_available"],
                "latest_1m_candle_timestamp": result.report.get("latest_1m_candle_timestamp"),
                "latest_completed_5m_candle_timestamp": result.report.get("latest_completed_5m_candle_timestamp"),
                "latest_completed_5m_candle_age_seconds": result.report.get("latest_completed_5m_candle_age_seconds"),
                "max_completed_5m_candle_age_seconds": result.report.get("max_completed_5m_candle_age_seconds"),
                "runtime_candle_context_stale": result.report.get("runtime_candle_context_stale"),
                "feature_rows_available": result.report.get("feature_rows_available"),
                "asian_drift_state_ready": result.report.get("asian_drift_state_ready"),
                "rule_decision": result.report.get("rule_decision"),
                "signal_emitted": result.report.get("signal_emitted"),
                "signal_side": result.report["signal_side"],
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
    return 0 if result.verdict in {
        TrackBAsianDriftWatchChainVerdict.NO_SIGNAL_NO_MUTATION,
        TrackBAsianDriftWatchChainVerdict.SIGNAL_READY_NO_SUBMIT,
    } else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
