"""Risk and exit-engine tests."""

from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from mgc_v05l.config_models.settings import RuntimeMode, StrategySettings
from mgc_v05l.domain.enums import ExitReason, LongEntryFamily, PositionSide, ReplayFillPolicy, StrategyStatus, VwapPolicy
from mgc_v05l.domain.models import Bar, FeaturePacket
from mgc_v05l.market_data.bar_models import build_bar_id
from mgc_v05l.strategy.exit_engine import evaluate_exits
from mgc_v05l.strategy.risk_engine import compute_risk_context
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
        session_us=True,
        session_allowed=True,
    )


def _build_features(bar: Bar, **overrides) -> FeaturePacket:
    values = {
        "bar_id": bar.bar_id,
        "tr": Decimal("10"),
        "atr": Decimal("10"),
        "bar_range": bar.high - bar.low,
        "body_size": abs(bar.close - bar.open),
        "avg_vol": Decimal("100"),
        "vol_ratio": Decimal("1"),
        "turn_ema_fast": Decimal("100"),
        "turn_ema_slow": Decimal("99"),
        "velocity": Decimal("1"),
        "velocity_delta": Decimal("1"),
        "vwap": Decimal("99"),
        "vwap_buffer": Decimal("0.3"),
        "swing_low_confirmed": False,
        "swing_high_confirmed": False,
        "last_swing_low": None,
        "last_swing_high": None,
        "downside_stretch": Decimal("12"),
        "upside_stretch": Decimal("12"),
        "bull_close_strong": True,
        "bear_close_weak": True,
    }
    values.update(overrides)
    return FeaturePacket(**values)


def test_k_long_stop_logic_uses_last_three_lows() -> None:
    settings = _build_settings()
    ny = ZoneInfo("America/New_York")
    history = [
        _build_bar(datetime(2026, 3, 13, 8, 30, tzinfo=ny), "101", "103", "100", "102"),
        _build_bar(datetime(2026, 3, 13, 8, 35, tzinfo=ny), "102", "103", "99", "100"),
        _build_bar(datetime(2026, 3, 13, 8, 40, tzinfo=ny), "100", "102", "98", "101"),
    ]
    state = replace(
        build_initial_state(history[-1].end_ts),
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        entry_timestamp=history[-1].end_ts,
        entry_bar_id=history[-1].bar_id,
        long_entry_family=LongEntryFamily.K,
        bars_in_trade=2,
    )
    features = _build_features(history[-1])

    risk = compute_risk_context(history, features, state, settings)

    assert risk.k_long_stop_ref_base == Decimal("97.0")
    assert risk.active_long_stop_ref_base == Decimal("97.0")


def test_vwap_long_stop_logic_uses_reclaim_low() -> None:
    settings = _build_settings()
    ny = ZoneInfo("America/New_York")
    history = [
        _build_bar(datetime(2026, 3, 13, 18, 0, tzinfo=ny), "100", "101", "99", "100"),
        _build_bar(datetime(2026, 3, 13, 18, 5, tzinfo=ny), "100", "103", "99", "102"),
        _build_bar(datetime(2026, 3, 13, 18, 10, tzinfo=ny), "102", "104", "101", "103"),
    ]
    state = replace(
        build_initial_state(history[-1].end_ts),
        strategy_status=StrategyStatus.IN_LONG_VWAP,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        entry_timestamp=history[-1].end_ts,
        entry_bar_id=history[-1].bar_id,
        long_entry_family=LongEntryFamily.VWAP,
        asia_reclaim_bar_low=Decimal("95"),
        bars_in_trade=2,
    )
    features = _build_features(history[-1])

    risk = compute_risk_context(history, features, state, settings)

    assert risk.vwap_long_stop_ref_base == Decimal("94.50")
    assert risk.active_long_stop_ref_base == Decimal("94.50")


def test_break_even_arming_promotes_k_long_stop_to_entry() -> None:
    settings = _build_settings()
    ny = ZoneInfo("America/New_York")
    history = [
        _build_bar(datetime(2026, 3, 13, 8, 30, tzinfo=ny), "101", "101.5", "100", "101"),
        _build_bar(datetime(2026, 3, 13, 8, 35, tzinfo=ny), "101", "101.5", "99", "100"),
        _build_bar(datetime(2026, 3, 13, 8, 40, tzinfo=ny), "100", "102.5", "99.5", "102"),
    ]
    state = replace(
        build_initial_state(history[-1].end_ts),
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        entry_timestamp=history[-1].end_ts,
        entry_bar_id=history[-1].bar_id,
        long_entry_family=LongEntryFamily.K,
        bars_in_trade=2,
    )
    features = _build_features(history[-1], atr=Decimal("10"))

    risk = compute_risk_context(history, features, state, settings)

    assert risk.long_break_even_armed is True
    assert risk.active_long_stop_ref == Decimal("100")


def test_k_long_exit_priority_prefers_long_stop() -> None:
    settings = _build_settings()
    ny = ZoneInfo("America/New_York")
    history = [
        _build_bar(datetime(2026, 3, 13, 8, 30, tzinfo=ny), "103", "104", "101", "102"),
        _build_bar(datetime(2026, 3, 13, 8, 35, tzinfo=ny), "102", "103", "100", "101"),
        _build_bar(datetime(2026, 3, 13, 8, 40, tzinfo=ny), "101", "101.2", "98.5", "99"),
    ]
    state = replace(
        build_initial_state(history[-1].end_ts),
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        entry_timestamp=history[-1].end_ts,
        entry_bar_id=history[-1].bar_id,
        long_entry_family=LongEntryFamily.K,
        last_swing_low=Decimal("98.5"),
        long_be_armed=True,
        bars_in_trade=6,
    )
    features = _build_features(history[-1], bull_close_strong=False)
    risk = compute_risk_context(history, features, state, settings)

    decision = evaluate_exits(history, features, state, risk, settings)

    assert decision.long_exit is True
    assert decision.primary_reason is ExitReason.LONG_STOP
    assert ExitReason.LONG_TIME_EXIT in decision.all_true_reasons


def test_vwap_long_exit_uses_vwap_specific_reason() -> None:
    settings = _build_settings()
    ny = ZoneInfo("America/New_York")
    history = [
        _build_bar(datetime(2026, 3, 13, 18, 5, tzinfo=ny), "101", "102", "100", "101.5"),
        _build_bar(datetime(2026, 3, 13, 18, 10, tzinfo=ny), "101.5", "102", "100.8", "100.9"),
    ]
    state = replace(
        build_initial_state(history[-1].end_ts),
        strategy_status=StrategyStatus.IN_LONG_VWAP,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        entry_timestamp=history[-1].end_ts,
        entry_bar_id=history[-1].bar_id,
        long_entry_family=LongEntryFamily.VWAP,
        asia_reclaim_bar_low=Decimal("95"),
        bars_in_trade=2,
    )
    features = _build_features(history[-1], vwap=Decimal("101"), turn_ema_fast=Decimal("101.2"))
    risk = compute_risk_context(history, features, state, settings)

    decision = evaluate_exits(history, features, state, risk, settings)

    assert decision.long_exit is True
    assert decision.primary_reason is ExitReason.VWAP_LOSS


def test_short_exit_uses_short_specific_reason() -> None:
    settings = _build_settings()
    ny = ZoneInfo("America/New_York")
    history = [
        _build_bar(datetime(2026, 3, 13, 8, 30, tzinfo=ny), "99", "101", "98", "100"),
        _build_bar(datetime(2026, 3, 13, 8, 35, tzinfo=ny), "100", "102", "99", "100"),
        _build_bar(datetime(2026, 3, 13, 8, 40, tzinfo=ny), "100", "104", "99.5", "102"),
    ]
    state = replace(
        build_initial_state(history[-1].end_ts),
        strategy_status=StrategyStatus.IN_SHORT_K,
        position_side=PositionSide.SHORT,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        entry_timestamp=history[-1].end_ts,
        entry_bar_id=history[-1].bar_id,
        long_entry_family=LongEntryFamily.NONE,
        last_swing_high=Decimal("104.5"),
        short_be_armed=True,
        bars_in_trade=4,
    )
    features = _build_features(history[-1], bear_close_weak=False, turn_ema_fast=Decimal("99"))
    risk = compute_risk_context(history, features, state, settings)

    decision = evaluate_exits(history, features, state, risk, settings)

    assert decision.short_exit is True
    assert decision.primary_reason is ExitReason.SHORT_STOP
    assert ExitReason.SHORT_TIME_EXIT in decision.all_true_reasons
