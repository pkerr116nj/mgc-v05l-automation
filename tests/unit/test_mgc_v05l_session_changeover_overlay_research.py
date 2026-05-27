from __future__ import annotations

from datetime import datetime, time
from pathlib import Path

import pandas as pd

from mgc_v05l.app.session_changeover_overlay_research import (
    CHANGEOVER_SPECS,
    NormalizedTrade,
    build_summary,
    classify_window,
    discover_replay_symbols,
    minutes_from_anchor,
    normalize_trade,
)


def test_changeover_window_classifies_inside_adjacent_and_outside() -> None:
    anchor = time(3, 0)

    assert classify_window(datetime.fromisoformat("2026-05-25T02:50:00-04:00"), anchor_time=anchor, window_minutes=15) == "inside_changeover"
    assert classify_window(datetime.fromisoformat("2026-05-25T03:20:00-04:00"), anchor_time=anchor, window_minutes=15) == "adjacent_non_changeover"
    assert classify_window(datetime.fromisoformat("2026-05-25T03:45:00-04:00"), anchor_time=anchor, window_minutes=15) == "outside"
    assert minutes_from_anchor(datetime.fromisoformat("2026-05-25T07:05:00-04:00"), time(7, 0)) == 5


def test_normalize_trade_accepts_general_trade_artifact() -> None:
    trade = normalize_trade(
        {
            "strategy_id": "breakout_retest_v1",
            "lane_id": "gc_london_open",
            "instrument": "GC",
            "direction": "LONG",
            "entry_ts": "2026-05-25T07:00:00-04:00",
            "exit_ts": "2026-05-25T07:20:00-04:00",
            "entry_price": 3300.0,
            "exit_price": 3302.5,
            "realized_pnl": 250.0,
            "mfe_points": 3.0,
            "mae_points": 0.6,
        },
        source_path=Path("outputs/reports/generic_strategy/trades.jsonl"),
    )

    assert trade is not None
    assert trade.source_family == "breakout_retest_v1"
    assert trade.direction == "LONG"
    assert trade.pnl_points == 2.5
    assert trade.pnl_cash == 250.0


def test_summary_is_research_only_and_does_not_grant_authority() -> None:
    bars = pd.DataFrame(
        [
            {
                "symbol": "MGC",
                "timeframe": "5m",
                "bar_ts": pd.Timestamp("2026-05-25T06:55:00Z"),
                "open": 4550.0,
                "high": 4552.0,
                "low": 4549.5,
                "close": 4551.5,
            },
            {
                "symbol": "MGC",
                "timeframe": "5m",
                "bar_ts": pd.Timestamp("2026-05-25T07:00:00Z"),
                "open": 4551.5,
                "high": 4554.0,
                "low": 4551.0,
                "close": 4553.0,
            },
            {
                "symbol": "MGC",
                "timeframe": "5m",
                "bar_ts": pd.Timestamp("2026-05-25T07:20:00Z"),
                "open": 4553.0,
                "high": 4553.5,
                "low": 4551.0,
                "close": 4551.5,
            },
        ]
    )
    trades = [
        NormalizedTrade(
            source_path="outputs/reports/generic/trades.jsonl",
            source_family="generic_breakout",
            strategy_id="generic_breakout",
            lane_id="generic_lane",
            instrument="MGC",
            direction="LONG",
            entry_ts=datetime.fromisoformat("2026-05-25T03:00:00-04:00"),
            exit_ts=datetime.fromisoformat("2026-05-25T03:15:00-04:00"),
            entry_price=4551.5,
            exit_price=4553.0,
            pnl_cash=15.0,
            pnl_points=1.5,
            mfe_points=2.5,
            mae_points=0.5,
        )
    ]

    summary = build_summary(bars=bars, trades=trades, repo_root=Path("/tmp/repo"), symbols=("MGC",))

    assert summary["research_only"] is True
    assert summary["live_trading_authority_changed"] is False
    assert summary["active_paper_config_changed"] is False
    assert summary["broker_mutation_allowed"] is False
    assert summary["submit_allowed"] is False
    assert summary["paper_proof_invoked"] is False
    assert {item["label"] for item in summary["session_label_recommendation"]["recommended_labels"]} == {
        spec["label"] for spec in CHANGEOVER_SPECS
    }


def test_replay_symbol_discovery_is_not_strategy_family_specific(tmp_path: Path) -> None:
    for symbol in ("MGC", "MNQ"):
        path = (
            tmp_path
            / "bars"
            / symbol
            / "2026Q2"
            / "datasets"
            / "derived_bars_5m"
            / f"symbol={symbol}"
            / "year=2026"
            / "shard_id=2026Q2"
        )
        path.mkdir(parents=True)
        (path / "bars.parquet").write_bytes(b"placeholder")

    assert discover_replay_symbols(repo_root=tmp_path, bar_root=Path("bars")) == ("MGC", "MNQ")
