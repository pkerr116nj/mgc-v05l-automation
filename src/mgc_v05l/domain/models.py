"""Core domain models from the Phase 3A blueprint."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from .enums import HealthStatus, LongEntryFamily, PositionSide, ShortEntryFamily, StrategyStatus


@dataclass(frozen=True)
class StrategyState:
    strategy_status: StrategyStatus
    position_side: PositionSide
    broker_position_qty: int
    internal_position_qty: int
    entry_price: Optional[Decimal]
    entry_timestamp: Optional[datetime]
    entry_bar_id: Optional[str]
    long_entry_family: LongEntryFamily
    bars_in_trade: int
    long_be_armed: bool
    short_be_armed: bool
    last_swing_low: Optional[Decimal]
    last_swing_high: Optional[Decimal]
    asia_reclaim_bar_low: Optional[Decimal]
    asia_reclaim_bar_high: Optional[Decimal]
    asia_reclaim_bar_vwap: Optional[Decimal]
    bars_since_bull_snap: Optional[int]
    bars_since_bear_snap: Optional[int]
    bars_since_asia_reclaim: Optional[int]
    bars_since_asia_vwap_signal: Optional[int]
    bars_since_long_setup: Optional[int]
    bars_since_short_setup: Optional[int]
    last_signal_bar_id: Optional[str]
    last_order_intent_id: Optional[str]
    open_broker_order_id: Optional[str]
    entries_enabled: bool
    exits_enabled: bool
    operator_halt: bool
    same_underlying_entry_hold: bool
    same_underlying_hold_reason: Optional[str]
    reconcile_required: bool
    fault_code: Optional[str]
    updated_at: datetime
    short_entry_family: ShortEntryFamily = ShortEntryFamily.NONE
    short_entry_source: Optional[str] = None
    additive_short_max_favorable_excursion: Decimal = Decimal("0")
    additive_short_peak_threshold_reached: bool = False
    additive_short_giveback_from_peak: Decimal = Decimal("0")


@dataclass(frozen=True)
class Bar:
    bar_id: str
    symbol: str
    timeframe: str
    start_ts: datetime
    end_ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    is_final: bool
    session_asia: bool
    session_london: bool
    session_us: bool
    session_allowed: bool

    def __post_init__(self) -> None:
        if not self.bar_id:
            raise ValueError("Bar.bar_id is required.")
        if not self.symbol:
            raise ValueError("Bar.symbol is required.")
        if not self.timeframe:
            raise ValueError("Bar.timeframe is required.")
        if self.start_ts.tzinfo is None or self.start_ts.utcoffset() is None:
            raise ValueError("Bar.start_ts must be timezone-aware.")
        if self.end_ts.tzinfo is None or self.end_ts.utcoffset() is None:
            raise ValueError("Bar.end_ts must be timezone-aware.")
        if self.end_ts <= self.start_ts:
            raise ValueError("Bar.end_ts must be after Bar.start_ts.")
        if self.high < max(self.open, self.low, self.close):
            raise ValueError("Bar.high must be >= open, low, and close.")
        if self.low > min(self.open, self.high, self.close):
            raise ValueError("Bar.low must be <= open, high, and close.")
        if self.volume < 0:
            raise ValueError("Bar.volume must be >= 0.")


@dataclass(frozen=True)
class FeaturePacket:
    bar_id: str
    tr: Decimal
    atr: Decimal
    bar_range: Decimal
    body_size: Decimal
    avg_vol: Decimal
    vol_ratio: Decimal
    turn_ema_fast: Decimal
    turn_ema_slow: Decimal
    velocity: Decimal
    velocity_delta: Decimal
    vwap: Decimal
    vwap_buffer: Decimal
    swing_low_confirmed: bool
    swing_high_confirmed: bool
    last_swing_low: Optional[Decimal]
    last_swing_high: Optional[Decimal]
    downside_stretch: Decimal
    upside_stretch: Decimal
    bull_close_strong: bool
    bear_close_weak: bool


@dataclass(frozen=True)
class SignalPacket:
    bar_id: str
    bull_snap_downside_stretch_ok: bool
    bull_snap_range_ok: bool
    bull_snap_body_ok: bool
    bull_snap_close_strong: bool
    bull_snap_velocity_ok: bool
    bull_snap_reversal_bar: bool
    bull_snap_location_ok: bool
    bull_snap_raw: bool
    bull_snap_turn_candidate: bool
    first_bull_snap_turn: bool
    below_vwap_recently: bool
    reclaim_range_ok: bool
    reclaim_vol_ok: bool
    reclaim_color_ok: bool
    reclaim_close_ok: bool
    asia_reclaim_bar_raw: bool
    asia_hold_bar: bool
    asia_hold_close_vwap_ok: bool
    asia_hold_low_ok: bool
    asia_hold_bar_ok: bool
    asia_acceptance_bar: bool
    asia_acceptance_close_high_ok: bool
    asia_acceptance_close_vwap_ok: bool
    asia_acceptance_bar_ok: bool
    asia_vwap_long_signal: bool
    midday_pause_resume_long_turn_candidate: bool
    us_late_pause_resume_long_turn_candidate: bool
    us_late_failed_move_reversal_long_turn_candidate: bool
    us_late_breakout_retest_hold_long_turn_candidate: bool
    asia_early_breakout_retest_hold_long_turn_candidate: bool
    asia_early_normal_breakout_retest_hold_long_turn_candidate: bool
    asia_late_pause_resume_long_turn_candidate: bool
    asia_late_flat_pullback_pause_resume_long_turn_candidate: bool
    asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate: bool
    bear_snap_up_stretch_ok: bool
    bear_snap_range_ok: bool
    bear_snap_body_ok: bool
    bear_snap_close_weak: bool
    bear_snap_velocity_ok: bool
    bear_snap_reversal_bar: bool
    bear_snap_location_ok: bool
    bear_snap_raw: bool
    bear_snap_turn_candidate: bool
    first_bear_snap_turn: bool
    derivative_bear_slope_ok: bool
    derivative_bear_curvature_ok: bool
    derivative_bear_turn_candidate: bool
    derivative_bear_additive_turn_candidate: bool
    midday_compressed_failed_move_reversal_short_turn_candidate: bool
    midday_compressed_rebound_failed_move_reversal_short_turn_candidate: bool
    midday_expanded_pause_resume_short_turn_candidate: bool
    midday_compressed_pause_resume_short_turn_candidate: bool
    midday_pause_resume_short_turn_candidate: bool
    london_late_pause_resume_short_turn_candidate: bool
    asia_early_expanded_breakout_retest_hold_short_turn_candidate: bool
    asia_early_compressed_pause_resume_short_turn_candidate: bool
    asia_early_pause_resume_short_turn_candidate: bool
    long_entry_raw: bool
    short_entry_raw: bool
    recent_long_setup: bool
    recent_short_setup: bool
    long_entry: bool
    short_entry: bool
    long_entry_source: Optional[str]
    short_entry_source: Optional[str]

@dataclass(frozen=True)
class HealthSnapshot:
    market_data_ok: bool
    broker_ok: bool
    persistence_ok: bool
    reconciliation_clean: bool
    invariants_ok: bool
    health_status: HealthStatus
