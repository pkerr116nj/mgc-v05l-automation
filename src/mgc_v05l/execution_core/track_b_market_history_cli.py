"""CLI wrapper for Track B bounded MGC market-history collector."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .track_b_market_history import (
    DEFAULT_TRACK_B_MARKET_HISTORY_OUTPUT_ROOT,
    TrackBMarketHistoryVerdict,
    collect_track_b_mgc_market_history,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize bounded MGC realtime/history candles into a Track B market-history event. "
            "Market-data evidence only; no broker, listener, strategy, proof, or submit calls."
        )
    )
    parser.add_argument("--market-history-json", required=True, type=Path)
    parser.add_argument("--expected-account-id")
    parser.add_argument("--contract-key", default="MGC-202606")
    parser.add_argument("--databento-continuous-symbol", default="MGC.v.0")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--allowlisted-local-symbol", default="MGCM6")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--max-candles", type=int, default=50)
    parser.add_argument("--min-candles", type=int, default=3)
    parser.add_argument("--source-id")
    parser.add_argument("--strategy-id")
    parser.add_argument("--lane-id")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_MARKET_HISTORY_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = json.loads(args.market_history_json.read_text(encoding="utf-8"))
    result = collect_track_b_mgc_market_history(
        market_history_payload=payload,
        source_payload_path=args.market_history_json,
        expected_account_id=args.expected_account_id,
        contract_key=args.contract_key,
        databento_continuous_symbol=args.databento_continuous_symbol,
        dataset=args.dataset,
        allowlisted_local_symbol=args.allowlisted_local_symbol,
        timeframe=args.timeframe,
        max_candles=args.max_candles,
        min_candles=args.min_candles,
        source_id=args.source_id,
        strategy_id=args.strategy_id,
        lane_id=args.lane_id,
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "market_history_verdict": result.report["market_history_verdict"],
                "contract_key": result.report["contract_key"],
                "databento_continuous_symbol": result.report["databento_continuous_symbol"],
                "dataset": result.report["dataset"],
                "timeframe": result.report["timeframe"],
                "candles_collected": result.report["candles_collected"],
                "latest_candle_timestamp": result.report["latest_candle_timestamp"],
                "quote_provider_mode": result.report["quote_provider_mode"],
                "realtime_quote_received": result.report["realtime_quote_received"],
                "current_quote_available": result.report["current_quote_available"],
                "output_history_event_path": result.report["output_history_event_path"],
                "latest_history_event_path": result.report["latest_history_event_path"],
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
    return 0 if result.verdict == TrackBMarketHistoryVerdict.WROTE_HISTORY_EVENT else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
