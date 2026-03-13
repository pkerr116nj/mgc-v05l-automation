"""Tests for the research replay-report utility."""

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.config_models.settings import RuntimeMode, StrategySettings
from mgc_v05l.domain.enums import ReplayFillPolicy, VwapPolicy
from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.bar_models import build_bar_id
from mgc_v05l.research.replay_report import build_causal_momentum_report, write_causal_momentum_report_csv


def _build_settings(database_path: Path) -> StrategySettings:
    return StrategySettings(
        symbol="MGC",
        timeframe="5m",
        timezone="America/New_York",
        mode=RuntimeMode.REPLAY,
        database_url=f"sqlite:///{database_path}",
        replay_fill_policy=ReplayFillPolicy.NEXT_BAR_OPEN,
        vwap_policy=VwapPolicy.SESSION_RESET,
        trade_size=1,
        enable_bull_snap_longs=True,
        enable_bear_snap_shorts=True,
        enable_asia_vwap_longs=True,
        atr_len=2,
        stop_atr_mult=Decimal("0.10"),
        breakeven_at_r=Decimal("1.0"),
        max_bars_long=6,
        max_bars_short=4,
        allow_asia=True,
        allow_london=True,
        allow_us=True,
        asia_start=datetime.strptime("18:00", "%H:%M").time(),
        asia_end=datetime.strptime("23:00", "%H:%M").time(),
        london_start=datetime.strptime("03:00", "%H:%M").time(),
        london_end=datetime.strptime("08:30", "%H:%M").time(),
        us_start=datetime.strptime("08:30", "%H:%M").time(),
        us_end=datetime.strptime("17:00", "%H:%M").time(),
        anti_churn_bars=3,
        use_turn_family=True,
        turn_fast_len=3,
        turn_slow_len=6,
        turn_signal_len=2,
        turn_stretch_lookback=8,
        min_snap_down_stretch_atr=Decimal("1.20"),
        min_snap_bar_range_atr=Decimal("1.00"),
        min_snap_body_atr=Decimal("0.45"),
        min_snap_close_location=Decimal("0.72"),
        min_snap_velocity_delta_atr=Decimal("0.18"),
        snap_cooldown_bars=5,
        use_asia_bull_snap_thresholds=True,
        asia_min_snap_bar_range_atr=Decimal("0.80"),
        asia_min_snap_body_atr=Decimal("0.35"),
        asia_min_snap_velocity_delta_atr=Decimal("0.12"),
        use_bull_snap_location_filter=True,
        bull_snap_max_close_vs_slow_ema_atr=Decimal("0.15"),
        bull_snap_require_close_below_slow_ema=True,
        min_bear_snap_up_stretch_atr=Decimal("1.00"),
        min_bear_snap_bar_range_atr=Decimal("0.90"),
        min_bear_snap_body_atr=Decimal("0.40"),
        max_bear_snap_close_location=Decimal("0.28"),
        min_bear_snap_velocity_delta_atr=Decimal("0.16"),
        bear_snap_cooldown_bars=5,
        use_bear_snap_location_filter=True,
        bear_snap_min_close_vs_slow_ema_atr=Decimal("0.15"),
        bear_snap_require_close_above_slow_ema=True,
        below_vwap_lookback=5,
        require_green_reclaim_bar=True,
        reclaim_close_buffer_atr=Decimal("0.03"),
        min_vwap_bar_range_atr=Decimal("0.45"),
        use_vwap_volume_filter=False,
        min_vwap_vol_ratio=Decimal("1.00"),
        require_hold_close_above_vwap=True,
        require_hold_not_break_reclaim_low=True,
        require_acceptance_close_above_reclaim_high=True,
        require_acceptance_close_above_vwap=True,
        vwap_long_stop_atr_mult=Decimal("0.05"),
        vwap_long_breakeven_at_r=Decimal("0.50"),
        vwap_long_max_bars=4,
        use_vwap_hard_loss_exit=True,
        vwap_weak_close_lookback_bars=2,
        vol_len=20,
        show_debug_labels=False,
    )


def _build_bar(end_ts: datetime, open_price: str, high_price: str, low_price: str, close_price: str) -> Bar:
    start_ts = end_ts - timedelta(minutes=5)
    return Bar(
        bar_id=build_bar_id("MGC", "5m", end_ts),
        symbol="MGC",
        timeframe="5m",
        start_ts=start_ts,
        end_ts=end_ts,
        open=Decimal(open_price),
        high=Decimal(high_price),
        low=Decimal(low_price),
        close=Decimal(close_price),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=False,
        session_allowed=False,
    )


def test_research_report_builds_rows_with_derivative_fields(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path / "report.sqlite3")
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(datetime(2026, 3, 13, 18, 0, tzinfo=ny), "100", "101", "99", "100"),
        _build_bar(datetime(2026, 3, 13, 18, 5, tzinfo=ny), "100", "103", "99", "102"),
        _build_bar(datetime(2026, 3, 13, 18, 10, tzinfo=ny), "102", "104", "101", "103"),
    ]

    rows = build_causal_momentum_report(bars, settings, smoothing_length=2)

    assert len(rows) == 3
    assert rows[0].timestamp == bars[0].end_ts.isoformat()
    assert rows[1].atr > Decimal("0")
    assert rows[2].smoothed_price > Decimal("0")


def test_research_report_csv_writer_exports_expected_columns(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path / "report.sqlite3")
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(datetime(2026, 3, 13, 18, 0, tzinfo=ny), "100", "101", "99", "100"),
        _build_bar(datetime(2026, 3, 13, 18, 5, tzinfo=ny), "100", "103", "99", "102"),
    ]
    rows = build_causal_momentum_report(bars, settings, smoothing_length=2)
    output_path = tmp_path / "momentum_report.csv"

    written_path = write_causal_momentum_report_csv(rows, output_path)
    contents = written_path.read_text(encoding="utf-8")

    assert written_path == output_path
    assert "timestamp" in contents
    assert "first_derivative" in contents
    assert "momentum_turning_positive" in contents
