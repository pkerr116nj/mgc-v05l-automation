from datetime import datetime

from mgc_v05l.app.mgc_impulse_burst_continuation_research import (
    Bar,
    _decision_bucket,
    _finalize_trade,
)


def _bar(ts: str, open_px: float, high_px: float, low_px: float, close_px: float, volume: float = 100.0) -> Bar:
    return Bar(
        timestamp=datetime.fromisoformat(ts),
        open=open_px,
        high=high_px,
        low=low_px,
        close=close_px,
        volume=volume,
    )


def test_finalize_trade_short_excursions_are_positive_and_directional() -> None:
    bars = [
        _bar("2026-03-01T10:00:00-05:00", 100.0, 100.5, 99.5, 99.8),
        _bar("2026-03-01T10:01:00-05:00", 99.8, 100.0, 98.8, 99.0),
        _bar("2026-03-01T10:02:00-05:00", 99.0, 99.2, 98.0, 98.2),
    ]
    trade = _finalize_trade(
        bars=bars,
        entry_index=1,
        exit_index=2,
        direction="SHORT",
        signal_phase="US_LATE",
        signal_bar_ts="2026-03-01T10:00:00-05:00",
    )
    assert trade is not None
    assert trade.captured_move > 0
    assert trade.max_favorable_move > 0
    assert trade.max_adverse_move >= 0


def test_decision_bucket_marks_promising_new_family() -> None:
    bucket = _decision_bucket(
        raw_events=20,
        filtered_events=14,
        trades=[object()] * 14,  # type: ignore[list-item]
        metrics={
            "realized_pnl": 120.0,
            "profit_factor": 1.8,
            "survives_without_top_1": True,
            "top_3_trade_contribution": 70.0,
            "false_start_rate": 0.2,
        },
    )
    assert bucket == "PROMISING_NEW_FAMILY"


def test_decision_bucket_marks_too_thin_before_clean_history_claims() -> None:
    bucket = _decision_bucket(
        raw_events=8,
        filtered_events=3,
        trades=[object()] * 3,  # type: ignore[list-item]
        metrics={
            "realized_pnl": 15.0,
            "profit_factor": 1.4,
            "survives_without_top_1": True,
            "top_3_trade_contribution": 60.0,
            "false_start_rate": 0.1,
        },
    )
    assert bucket == "TOO_THIN"
