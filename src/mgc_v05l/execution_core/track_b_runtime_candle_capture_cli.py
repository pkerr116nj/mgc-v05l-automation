"""CLI for Track B bounded runtime MGC candle capture."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .track_b_runtime_candle_capture import (
    DEFAULT_TRACK_B_RUNTIME_CANDLE_CAPTURE_OUTPUT_ROOT,
    TrackBRuntimeCandleCaptureVerdict,
    capture_track_b_runtime_mgc_1m_candles,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Write a bounded Track B runtime MGC 1m candle context artifact. No broker or submit paths are invoked."
    )
    parser.add_argument("--runtime-candle-json", required=True, type=Path, help="Supplied runtime MGC candle JSON payload.")
    parser.add_argument("--expected-account-id", default="DUM882026")
    parser.add_argument("--account-id", default="DUM882026")
    parser.add_argument("--contract-key", default="MGC-202606")
    parser.add_argument("--local-symbol", default="MGCM6")
    parser.add_argument("--databento-continuous-symbol", default="MGC.v.0")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--max-bars", type=int, default=250)
    parser.add_argument("--min-bars", type=int, default=3)
    parser.add_argument("--candle-source-mode", default="SUPPLIED_RUNTIME_CANDLES")
    parser.add_argument("--source-id", default="track_b_runtime_candle_capture")
    parser.add_argument("--strategy-id", default="track_b_example_gold_shadow_v1")
    parser.add_argument("--lane-id", default="mgc_example_long_lmt_day")
    parser.add_argument("--retention-runs", type=int, default=5)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_RUNTIME_CANDLE_CAPTURE_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = json.loads(args.runtime_candle_json.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit("--runtime-candle-json must contain a JSON object.")
    result = capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=payload,
        source_payload_path=args.runtime_candle_json,
        expected_account_id=args.expected_account_id,
        account_id=args.account_id,
        contract_key=args.contract_key,
        local_symbol=args.local_symbol,
        databento_continuous_symbol=args.databento_continuous_symbol,
        dataset=args.dataset,
        timeframe=args.timeframe,
        max_bars=args.max_bars,
        min_bars=args.min_bars,
        candle_source_mode=args.candle_source_mode,
        source_id=args.source_id,
        strategy_id=args.strategy_id,
        lane_id=args.lane_id,
        retention_runs=args.retention_runs,
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "runtime_candle_capture_verdict": result.report["runtime_candle_capture_verdict"],
                "runtime_candle_context_ready": result.report["runtime_candle_context_ready"],
                "bars_available": result.report["bars_available"],
                "max_bars": result.report["max_bars"],
                "gap_count": result.report["gap_count"],
                "latest_runtime_candles_path": result.report["latest_runtime_candles_path"],
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
    return 0 if result.verdict == TrackBRuntimeCandleCaptureVerdict.WROTE_RUNTIME_CANDLES else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
