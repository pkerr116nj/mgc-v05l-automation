"""Signal and session tests derived from the release candidate."""

from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from mgc_v05l.config_models.settings import RuntimeMode, StrategySettings
from mgc_v05l.domain.enums import LongEntryFamily, ReplayFillPolicy, VwapPolicy
from mgc_v05l.domain.models import Bar, FeaturePacket, SignalPacket
from mgc_v05l.market_data.bar_models import build_bar_id
from mgc_v05l.market_data.session_clock import classify_sessions
from mgc_v05l.signals.asia_vwap_reclaim import evaluate_asia_vwap_reclaim
from mgc_v05l.signals.bear_snap import evaluate_bear_snap
from mgc_v05l.signals.bull_snap import evaluate_bull_snap
from mgc_v05l.signals.entry_resolver import resolve_entries
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


def _build_bar(
    end_ts: datetime,
    open_price: str,
    high_price: str,
    low_price: str,
    close_price: str,
) -> Bar:
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


def _build_feature_packet(bar: Bar, **overrides: Any) -> FeaturePacket:
    values = {
        "bar_id": bar.bar_id,
        "tr": Decimal("5"),
        "atr": Decimal("5"),
        "bar_range": bar.high - bar.low,
        "body_size": abs(bar.close - bar.open),
        "avg_vol": Decimal("100"),
        "vol_ratio": Decimal("1"),
        "turn_ema_fast": Decimal("99.5"),
        "turn_ema_slow": Decimal("100"),
        "velocity": Decimal("-0.5"),
        "velocity_delta": Decimal("1.0"),
        "vwap": Decimal("99"),
        "vwap_buffer": Decimal("0.15"),
        "swing_low_confirmed": False,
        "swing_high_confirmed": False,
        "last_swing_low": None,
        "last_swing_high": None,
        "downside_stretch": Decimal("7"),
        "upside_stretch": Decimal("6"),
        "bull_close_strong": True,
        "bear_close_weak": True,
    }
    values.update(overrides)
    return FeaturePacket(**values)


def _blank_signal_packet(bar_id: str, first_bull_snap_turn: bool, asia_vwap_long_signal: bool, first_bear_snap_turn: bool) -> SignalPacket:
    return SignalPacket(
        bar_id=bar_id,
        bull_snap_downside_stretch_ok=False,
        bull_snap_range_ok=False,
        bull_snap_body_ok=False,
        bull_snap_close_strong=False,
        bull_snap_velocity_ok=False,
        bull_snap_reversal_bar=False,
        bull_snap_location_ok=False,
        bull_snap_raw=False,
        bull_snap_turn_candidate=False,
        first_bull_snap_turn=first_bull_snap_turn,
        below_vwap_recently=False,
        reclaim_range_ok=False,
        reclaim_vol_ok=False,
        reclaim_color_ok=False,
        reclaim_close_ok=False,
        asia_reclaim_bar_raw=False,
        asia_hold_bar=False,
        asia_hold_close_vwap_ok=False,
        asia_hold_low_ok=False,
        asia_hold_bar_ok=False,
        asia_acceptance_bar=False,
        asia_acceptance_close_high_ok=False,
        asia_acceptance_close_vwap_ok=False,
        asia_acceptance_bar_ok=False,
        asia_vwap_long_signal=asia_vwap_long_signal,
        bear_snap_up_stretch_ok=False,
        bear_snap_range_ok=False,
        bear_snap_body_ok=False,
        bear_snap_close_weak=False,
        bear_snap_velocity_ok=False,
        bear_snap_reversal_bar=False,
        bear_snap_location_ok=False,
        bear_snap_raw=False,
        bear_snap_turn_candidate=False,
        first_bear_snap_turn=first_bear_snap_turn,
        long_entry_raw=False,
        short_entry_raw=False,
        recent_long_setup=False,
        recent_short_setup=False,
        long_entry=False,
        short_entry=False,
        long_entry_source=None,
        short_entry_source=None,
    )


def test_session_classification_is_start_inclusive_end_exclusive() -> None:
    settings = _build_settings()
    ny = ZoneInfo("America/New_York")

    asia_open_bar = classify_sessions(
        _build_bar(datetime(2026, 3, 13, 18, 0, tzinfo=ny), "100", "101", "99", "100.5"),
        settings,
    )
    asia_close_bar = classify_sessions(
        _build_bar(datetime(2026, 3, 13, 23, 0, tzinfo=ny), "100", "101", "99", "100.5"),
        settings,
    )

    assert asia_open_bar.session_asia is True
    assert asia_close_bar.session_asia is False


def test_bull_snap_turn_candidate_passes_with_release_candidate_rules() -> None:
    settings = _build_settings()
    ny = ZoneInfo("America/New_York")
    previous_bar = classify_sessions(
        _build_bar(datetime(2026, 3, 13, 18, 0, tzinfo=ny), "98", "99", "97", "98"),
        settings,
    )
    current_bar = classify_sessions(
        _build_bar(datetime(2026, 3, 13, 18, 5, tzinfo=ny), "96", "101", "95", "100"),
        settings,
    )
    features = _build_feature_packet(current_bar)
    state = replace(build_initial_state(current_bar.end_ts), bars_since_bull_snap=6)

    result = evaluate_bull_snap([previous_bar, current_bar], features, state, settings)

    assert result["bull_snap_turn_candidate"] is True
    assert result["first_bull_snap_turn"] is True


def test_bear_snap_turn_candidate_passes_with_release_candidate_rules() -> None:
    settings = _build_settings()
    ny = ZoneInfo("America/New_York")
    previous_bar = classify_sessions(
        _build_bar(datetime(2026, 3, 13, 8, 30, tzinfo=ny), "101", "102", "100", "101"),
        settings,
    )
    current_bar = classify_sessions(
        _build_bar(datetime(2026, 3, 13, 8, 35, tzinfo=ny), "104", "105", "99", "100"),
        settings,
    )
    features = _build_feature_packet(
        current_bar,
        turn_ema_slow=Decimal("99"),
        velocity_delta=Decimal("-1.0"),
    )
    state = replace(build_initial_state(current_bar.end_ts), bars_since_bear_snap=6)

    result = evaluate_bear_snap([previous_bar, current_bar], features, state, settings)

    assert result["bear_snap_turn_candidate"] is True
    assert result["first_bear_snap_turn"] is True


def test_asia_reclaim_bar_and_acceptance_follow_release_candidate_sequence() -> None:
    settings = _build_settings()
    ny = ZoneInfo("America/New_York")
    reclaim_bar = classify_sessions(
        _build_bar(datetime(2026, 3, 13, 18, 5, tzinfo=ny), "99", "102", "98", "101"),
        settings,
    )
    prior_bar = classify_sessions(
        _build_bar(datetime(2026, 3, 13, 18, 0, tzinfo=ny), "100", "101", "98", "98.5"),
        settings,
    )
    reclaim_features = [
        _build_feature_packet(prior_bar, vwap=Decimal("99.5")),
        _build_feature_packet(reclaim_bar, vwap=Decimal("100.0"), vwap_buffer=Decimal("0.1")),
    ]
    reclaim_state = build_initial_state(reclaim_bar.end_ts)

    reclaim_result = evaluate_asia_vwap_reclaim([prior_bar, reclaim_bar], reclaim_features, reclaim_state, settings)

    assert reclaim_result["asia_reclaim_bar_raw"] is True

    hold_bar = classify_sessions(
        _build_bar(datetime(2026, 3, 13, 18, 10, tzinfo=ny), "101", "102", "100", "101.5"),
        settings,
    )
    acceptance_bar = classify_sessions(
        _build_bar(datetime(2026, 3, 13, 18, 15, tzinfo=ny), "101.5", "103", "101", "102.5"),
        settings,
    )
    acceptance_features = [
        _build_feature_packet(hold_bar, vwap=Decimal("100.0")),
        _build_feature_packet(acceptance_bar, vwap=Decimal("100.2")),
    ]
    acceptance_state = replace(
        build_initial_state(acceptance_bar.end_ts),
        bars_since_asia_reclaim=1,
        asia_reclaim_bar_low=Decimal("98"),
        asia_reclaim_bar_high=Decimal("102"),
        asia_reclaim_bar_vwap=Decimal("100.0"),
        bars_since_asia_vwap_signal=10,
    )

    acceptance_result = evaluate_asia_vwap_reclaim(
        [hold_bar, acceptance_bar],
        acceptance_features,
        acceptance_state,
        settings,
    )

    assert acceptance_result["asia_acceptance_bar_ok"] is True
    assert acceptance_result["asia_vwap_long_signal"] is True


def test_entry_resolution_gives_vwap_precedence_when_both_long_triggers_fire() -> None:
    settings = _build_settings()
    state = replace(build_initial_state(datetime.now(ZoneInfo("UTC"))), bars_since_long_setup=10, bars_since_short_setup=10)
    packet = _blank_signal_packet("bar-1", first_bull_snap_turn=True, asia_vwap_long_signal=True, first_bear_snap_turn=False)

    resolved = resolve_entries(packet, state, settings)

    assert resolved.long_entry is True
    assert resolved.long_entry_source == "asiaVWAPLongSignal"
