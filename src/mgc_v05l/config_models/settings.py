"""Pydantic settings models for the MGC v0.5l runtime."""

from datetime import time
from decimal import Decimal
from enum import Enum
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..domain.enums import ReplayFillPolicy, VwapPolicy


class RuntimeMode(str, Enum):
    REPLAY = "replay"
    PAPER = "paper"
    LIVE = "live"


class StrategySettings(BaseModel):
    """Runtime settings with explicit fields and no hidden defaults."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: str
    timeframe: str
    timezone: str
    mode: RuntimeMode
    database_url: str
    replay_fill_policy: ReplayFillPolicy
    vwap_policy: VwapPolicy
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
    risk_floor: Decimal = Field(default=Decimal("0.01"))

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        if value != "MGC":
            raise ValueError("symbol must remain locked to MGC.")
        return value

    @field_validator("timeframe")
    @classmethod
    def validate_timeframe(cls, value: str) -> str:
        if value != "5m":
            raise ValueError("timeframe must remain locked to 5m.")
        return value

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        if value != "America/New_York":
            raise ValueError("timezone must remain locked to America/New_York.")
        return value

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        if not value.startswith("sqlite:///"):
            raise ValueError("database_url must use SQLite for this build.")
        return value

    @field_validator("replay_fill_policy")
    @classmethod
    def validate_replay_fill_policy(cls, value: ReplayFillPolicy) -> ReplayFillPolicy:
        if value != ReplayFillPolicy.NEXT_BAR_OPEN:
            raise ValueError("replay_fill_policy must remain locked to NEXT_BAR_OPEN.")
        return value

    @field_validator("vwap_policy")
    @classmethod
    def validate_vwap_policy(cls, value: VwapPolicy) -> VwapPolicy:
        if value != VwapPolicy.SESSION_RESET:
            raise ValueError("vwap_policy must remain locked to SESSION_RESET.")
        return value

    @field_validator(
        "trade_size",
        "atr_len",
        "max_bars_long",
        "max_bars_short",
        "anti_churn_bars",
        "turn_fast_len",
        "turn_slow_len",
        "turn_signal_len",
        "turn_stretch_lookback",
        "snap_cooldown_bars",
        "bear_snap_cooldown_bars",
        "below_vwap_lookback",
        "vwap_long_max_bars",
        "vwap_weak_close_lookback_bars",
        "vol_len",
    )
    @classmethod
    def validate_positive_ints(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("integer settings must be > 0.")
        return value

    @field_validator("risk_floor")
    @classmethod
    def validate_risk_floor(cls, value: Decimal) -> Decimal:
        if value != Decimal("0.01"):
            raise ValueError("risk_floor must remain the internal safety floor of 0.01.")
        return value

    @model_validator(mode="after")
    def validate_time_windows(self) -> "StrategySettings":
        return self

    def warmup_bars_required(self) -> int:
        """Return the documented minimum history requirement for entry eligibility."""
        return max(
            self.atr_len,
            self.turn_slow_len,
            self.turn_stretch_lookback + 2,
            self.below_vwap_lookback,
            self.vol_len,
            10,
        )

    @property
    def timezone_info(self) -> ZoneInfo:
        """Return the configured timezone object."""
        return ZoneInfo(self.timezone)
