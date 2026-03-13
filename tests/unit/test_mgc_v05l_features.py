"""Feature-engine and swing-tracker tests."""

from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from mgc_v05l.config_models.settings import RuntimeMode, StrategySettings
from mgc_v05l.domain.enums import ReplayFillPolicy, VwapPolicy
from mgc_v05l.domain.models import Bar
from mgc_v05l.indicators.feature_engine import compute_features
from mgc_v05l.indicators.swing_tracker import update_swing_state
from mgc_v05l.market_data.bar_models import build_bar_id
from mgc_v05l.market_data.session_clock import classify_sessions
from mgc_v05l.strategy.trade_state import build_initial_state


def _build_settings() -> StrategySettings:
    return StrategySettings(
        symbol="MGC",
        timeframe="5m",
        timezone="America/New_York",
        mode=RuntimeMode.REPLAY,
        database_url="sqlite:///./mgc_v05l.sqlite3",
        replay_fill_policy=ReplayFillPolicy.NEXT_BAR_OPEN,
        vwap_policy=VwapPolicy.SESSION_RESET,
        trade_size=1,
        enable_bull_snap_longs=True,
        enable_bear_snap_shorts=True,
        enable_asia_vwap_longs=True,
        atr_len=14,
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
        show_debug_labels=True,
    )


def _build_bar(end_ts: datetime, open_price: str, high_price: str, low_price: str, close_price: str, volume: int) -> Bar:
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
        volume=volume,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=False,
        session_allowed=False,
    )


def test_swing_tracker_confirms_and_persists_swing_levels() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _build_bar(datetime(2026, 3, 13, 8, 30, tzinfo=ny), "100", "101", "99", "100", 100),
        _build_bar(datetime(2026, 3, 13, 8, 35, tzinfo=ny), "100", "102", "98", "101", 100),
        _build_bar(datetime(2026, 3, 13, 8, 40, tzinfo=ny), "101", "101.5", "99.5", "100.5", 100),
    ]

    swing_low_confirmed, swing_high_confirmed, last_swing_low, last_swing_high = update_swing_state(
        history,
        None,
        None,
    )

    assert swing_low_confirmed is True
    assert swing_high_confirmed is True
    assert last_swing_low == Decimal("98")
    assert last_swing_high == Decimal("102")


def test_feature_engine_computes_release_candidate_fields() -> None:
    settings = _build_settings()
    ny = ZoneInfo("America/New_York")
    raw_bars = [
        _build_bar(datetime(2026, 3, 13, 18, 0, tzinfo=ny), "100", "101", "99", "100.5", 100),
        _build_bar(datetime(2026, 3, 13, 18, 5, tzinfo=ny), "100.5", "102", "100", "101.5", 120),
        _build_bar(datetime(2026, 3, 13, 18, 10, tzinfo=ny), "101.5", "103", "101", "102.5", 140),
    ]
    history = [classify_sessions(bar, settings) for bar in raw_bars]
    state = build_initial_state(history[-1].end_ts)

    features = compute_features(history, state, settings)

    assert features.bar_id == history[-1].bar_id
    assert features.bar_range == Decimal("2")
    assert features.body_size == Decimal("1.0")
    assert features.avg_vol == Decimal("120")
    assert features.vol_ratio == Decimal("1.166666666666666666666666667")
    assert features.vwap > Decimal("0")
    assert features.vwap_buffer == settings.reclaim_close_buffer_atr * features.atr
