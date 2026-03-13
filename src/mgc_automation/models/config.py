"""Typed strategy configuration models derived from the formal spec."""

from dataclasses import dataclass
from datetime import time
from decimal import Decimal


@dataclass(frozen=True)
class StrategyInputs:
    trade_size: int
    enable_bull_snap_longs: bool
    enable_bear_snap_shorts: bool
    enable_asia_vwap_longs: bool
    atr_len: int
    stop_atr_mult: Decimal
    breakeven_at_r: Decimal
    max_bars_long: int
    max_bars_short: int
    allow_asia: bool
    allow_london: bool
    allow_us: bool
    asia_start: time
    asia_end: time
    london_start: time
    london_end: time
    us_start: time
    us_end: time
    anti_churn_bars: int
    use_turn_family: bool
    turn_fast_len: int
    turn_slow_len: int
    turn_signal_len: int
    turn_stretch_lookback: int
    min_snap_down_stretch_atr: Decimal
    min_snap_bar_range_atr: Decimal
    min_snap_body_atr: Decimal
    min_snap_close_location: Decimal
    min_snap_velocity_delta_atr: Decimal
    snap_cooldown_bars: int
    use_asia_bull_snap_thresholds: bool
    asia_min_snap_bar_range_atr: Decimal
    asia_min_snap_body_atr: Decimal
    asia_min_snap_velocity_delta_atr: Decimal
    use_bull_snap_location_filter: bool
    bull_snap_max_close_vs_slow_ema_atr: Decimal
    bull_snap_require_close_below_slow_ema: bool
    min_bear_snap_up_stretch_atr: Decimal
    min_bear_snap_bar_range_atr: Decimal
    min_bear_snap_body_atr: Decimal
    max_bear_snap_close_location: Decimal
    min_bear_snap_velocity_delta_atr: Decimal
    bear_snap_cooldown_bars: int
    use_bear_snap_location_filter: bool
    bear_snap_min_close_vs_slow_ema_atr: Decimal
    bear_snap_require_close_above_slow_ema: bool
    below_vwap_lookback: int
    require_green_reclaim_bar: bool
    reclaim_close_buffer_atr: Decimal
    min_vwap_bar_range_atr: Decimal
    use_vwap_volume_filter: bool
    min_vwap_vol_ratio: Decimal
    require_hold_close_above_vwap: bool
    require_hold_not_break_reclaim_low: bool
    require_acceptance_close_above_reclaim_high: bool
    require_acceptance_close_above_vwap: bool
    vwap_long_stop_atr_mult: Decimal
    vwap_long_breakeven_at_r: Decimal
    vwap_long_max_bars: int
    use_vwap_hard_loss_exit: bool
    vwap_weak_close_lookback_bars: int
    vol_len: int
    show_debug_labels: bool
