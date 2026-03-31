from datetime import datetime, timedelta

from mgc_v05l.app.mgc_impulse_burst_continuation_research import Bar
from mgc_v05l.app.mgc_impulse_delayed_confirmation_revalidation import (
    _build_delayed_trade_outcome,
    _decision_bucket,
    _resolve_confirmation_resolution,
)
from mgc_v05l.app.mgc_impulse_burst_asymmetry_report import AcceptedEvent


def _bar(ts: datetime, open_: float, high: float, low: float, close: float) -> Bar:
    return Bar(timestamp=ts, open=open_, high=high, low=low, close=close, volume=100.0)


def test_confirmation_resolution_can_lock_after_second_bar() -> None:
    start = datetime(2026, 3, 1, 9, 30)
    bars = [
        _bar(start + timedelta(minutes=idx), 100 + idx, 101 + idx, 99 + idx, 100.8 + idx)
        for idx in range(8)
    ]
    bars.extend(
        [
            _bar(start + timedelta(minutes=8), 108.0, 109.5, 107.5, 109.2),
            _bar(start + timedelta(minutes=9), 109.2, 110.5, 109.0, 110.2),
            _bar(start + timedelta(minutes=10), 110.2, 111.2, 110.0, 111.0),
            _bar(start + timedelta(minutes=11), 111.0, 111.4, 110.8, 111.3),
        ]
    )
    resolution = _resolve_confirmation_resolution(
        bars=bars,
        signal_index=7,
        direction="LONG",
        entry_mode="NEXT_OPEN_AFTER_CONFIRM",
    )
    assert resolution is not None
    assert resolution.confirmation_index == 9
    assert resolution.resolution_timing == "after_bar_2_close"


def test_confirmation_resolution_can_wait_for_third_bar() -> None:
    start = datetime(2026, 3, 1, 9, 30)
    bars = [
        _bar(start + timedelta(minutes=idx), 100 + idx, 101 + idx, 99 + idx, 100.8 + idx)
        for idx in range(8)
    ]
    bars.extend(
        [
            _bar(start + timedelta(minutes=8), 108.0, 109.6, 107.8, 108.4),
            _bar(start + timedelta(minutes=9), 108.4, 110.2, 108.2, 108.1),
            _bar(start + timedelta(minutes=10), 108.1, 110.1, 108.0, 109.6),
            _bar(start + timedelta(minutes=11), 109.6, 110.3, 109.4, 110.0),
        ]
    )
    resolution = _resolve_confirmation_resolution(
        bars=bars,
        signal_index=7,
        direction="LONG",
        entry_mode="NEXT_OPEN_AFTER_CONFIRM",
    )
    assert resolution is not None
    assert resolution.confirmation_index == 10
    assert resolution.resolution_timing == "after_bar_3_close"


def test_confirmation_bar_close_trade_starts_after_entry_bar() -> None:
    start = datetime(2026, 3, 1, 9, 30)
    bars = [
        _bar(start + timedelta(minutes=idx), 100 + idx, 101 + idx, 99 + idx, 100.8 + idx)
        for idx in range(8)
    ]
    bars.extend(
        [
            _bar(start + timedelta(minutes=8), 108.0, 109.5, 107.5, 109.2),
            _bar(start + timedelta(minutes=9), 109.2, 110.5, 109.0, 110.2),
            _bar(start + timedelta(minutes=10), 110.2, 111.2, 110.0, 111.0),
            _bar(start + timedelta(minutes=11), 111.0, 111.4, 110.8, 110.9),
            _bar(start + timedelta(minutes=12), 110.9, 111.0, 109.0, 109.2),
        ]
    )
    event = AcceptedEvent(
        signal_index=7,
        impulse={"direction": "LONG", "burst_size_points": 3.5, "signal_phase": "US_MIDDAY"},
        base_exit_ts="",
    )
    resolution = _resolve_confirmation_resolution(
        bars=bars,
        signal_index=7,
        direction="LONG",
        entry_mode="CONFIRMATION_BAR_CLOSE",
    )
    trade = _build_delayed_trade_outcome(
        bars=bars,
        event=event,
        resolution=resolution,
        entry_mode="CONFIRMATION_BAR_CLOSE",
    )
    assert trade is not None
    assert trade.entry_ts == bars[9].timestamp.isoformat()
    assert trade.exit_ts == bars[12].timestamp.isoformat()


def test_decision_bucket_marks_recovered_variant_when_delayed_entry_keeps_quality() -> None:
    bucket = _decision_bucket(
        metrics={
            "trades": 72,
            "profit_factor": 2.4,
            "median_trade": 8.0,
            "top_3_contribution": 48.0,
            "survives_without_top_3": True,
            "realized_pnl": 2550.0,
        },
        raw_control_metrics={"realized_pnl": 1113.0},
        benchmark_metrics={"trades": 115, "realized_pnl": 4667.0},
    )
    assert bucket == "DELAYED_CONFIRMATION_RECOVERS_ENOUGH"
