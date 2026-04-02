"""Pydantic settings models for the MGC v0.5l runtime."""

import json
from pathlib import Path
from datetime import time
from decimal import Decimal
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..domain.enums import AddDirectionPolicy, ParticipationPolicy, ReplayFillPolicy, VwapPolicy
from ..market_data.timeframes import normalize_timeframe_label, timeframe_minutes


class RuntimeMode(str, Enum):
    REPLAY = "replay"
    PAPER = "paper"
    LIVE = "live"


class EnvironmentMode(str, Enum):
    # Retain stable serialized lane identifiers for config and artifact compatibility.
    BASELINE_PARITY = "baseline_parity_mode"
    RESEARCH_EXECUTION = "research_execution_mode"
    LIVE_EXECUTION = "live_execution_mode"


class ExecutionTimeframeRole(str, Enum):
    MATCHES_SIGNAL_EVALUATION = "matches_signal_evaluation"
    EXECUTION_DETAIL_ONLY = "execution_detail_only"


class StrategySettings(BaseModel):
    """Runtime settings with explicit fields and no hidden defaults."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    symbol: str
    timeframe: str
    environment_mode: EnvironmentMode = EnvironmentMode.BASELINE_PARITY
    structural_signal_timeframe: str | None = None
    execution_timeframe: str | None = None
    artifact_timeframe: str | None = None
    context_timeframes: tuple[str, ...] = ()
    execution_timeframe_role: ExecutionTimeframeRole = ExecutionTimeframeRole.MATCHES_SIGNAL_EVALUATION
    timezone: str
    mode: RuntimeMode
    database_url: str
    replay_fill_policy: ReplayFillPolicy
    vwap_policy: VwapPolicy
    trade_size: int
    enable_bull_snap_longs: bool
    enable_first_bull_snap_us_london: bool = True
    enable_us_midday_pause_resume_longs: bool = False
    enable_us_late_pause_resume_longs: bool = False
    enable_us_late_failed_move_reversal_longs: bool = False
    enable_us_late_breakout_retest_hold_longs: bool = False
    enable_asia_early_breakout_retest_hold_longs: bool = False
    enable_asia_early_normal_breakout_retest_hold_longs: bool = False
    enable_asia_late_pause_resume_longs: bool = False
    enable_asia_late_flat_pullback_pause_resume_longs: bool = False
    enable_asia_late_compressed_flat_pullback_pause_resume_longs: bool = False
    enable_bear_snap_shorts: bool
    enable_us_derivative_bear_shorts: bool = False
    enable_us_derivative_bear_additive_shorts: bool = False
    enable_us_midday_pause_resume_shorts: bool = False
    enable_us_midday_expanded_pause_resume_shorts: bool = False
    enable_us_midday_compressed_pause_resume_shorts: bool = False
    enable_us_midday_compressed_failed_move_reversal_shorts: bool = False
    enable_us_midday_compressed_rebound_failed_move_reversal_shorts: bool = False
    enable_london_late_pause_resume_shorts: bool = False
    enable_asia_early_pause_resume_shorts: bool = False
    enable_asia_early_compressed_pause_resume_shorts: bool = False
    enable_asia_early_expanded_breakout_retest_hold_shorts: bool = False
    enable_asia_vwap_longs: bool
    atr_len: int
    stop_atr_mult: Decimal
    breakeven_at_r: Decimal
    reconciliation_heartbeat_interval_seconds: int = 60
    order_lifecycle_watchdog_interval_seconds: int = 15
    order_ack_timeout_seconds: int = 30
    order_fill_timeout_seconds: int = 180
    order_timeout_reconcile_grace_seconds: int = 30
    order_timeout_retry_limit: int = 0
    runtime_supervisor_restart_window_seconds: int = 900
    runtime_supervisor_max_auto_restarts_per_window: int = 3
    runtime_supervisor_restart_backoff_seconds: int = 60
    runtime_supervisor_restart_suppression_seconds: int = 900
    runtime_supervisor_failure_cooldown_seconds: int = 180
    max_bars_long: int
    max_bars_short: int
    participation_policy: ParticipationPolicy = ParticipationPolicy.SINGLE_ENTRY_ONLY
    max_concurrent_entries: int = 1
    max_position_quantity: int | None = None
    max_adds_after_entry: int = 0
    add_direction_policy: AddDirectionPolicy = AddDirectionPolicy.SAME_DIRECTION_ONLY
    use_long_swing_exit: bool = True
    use_short_swing_exit: bool = True
    use_long_integrity_exit: bool = True
    use_short_integrity_exit: bool = True
    use_long_time_exit: bool = True
    use_short_time_exit: bool = True
    use_long_fast_ema_exit: bool = False
    use_short_fast_ema_exit: bool = False
    use_additive_short_stalled_exit: bool = False
    additive_short_stalled_exit_min_bars: int = 2
    use_additive_short_profit_protect_exit: bool = False
    additive_short_profit_protect_min_profit_r: Decimal = Decimal("1.00")
    additive_short_profit_protect_min_bars: int = 2
    use_additive_short_giveback_exit: bool = False
    additive_short_giveback_min_peak_profit_r: Decimal = Decimal("1.00")
    additive_short_giveback_fraction: Decimal = Decimal("0.50")
    take_profit_at_r: Decimal = Decimal("0.0")
    use_long_derivative_maturity_exit: bool = False
    use_short_derivative_maturity_exit: bool = False
    derivative_exit_min_profit_r: Decimal = Decimal("0.75")
    long_derivative_exit_min_normalized_velocity: Decimal = Decimal("0.10")
    long_derivative_exit_max_normalized_velocity_delta: Decimal = Decimal("-0.04")
    short_derivative_exit_max_normalized_velocity: Decimal = Decimal("-0.10")
    short_derivative_exit_min_normalized_velocity_delta: Decimal = Decimal("0.04")
    derivative_exit_require_counter_close: bool = True
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
    us_midday_pause_resume_long_min_normalized_slope: Decimal = Decimal("0.00")
    us_midday_pause_resume_long_max_normalized_slope: Decimal = Decimal("0.20")
    us_midday_pause_resume_long_min_normalized_curvature: Decimal = Decimal("0.10")
    us_midday_pause_resume_long_max_normalized_curvature: Decimal = Decimal("0.60")
    us_midday_pause_resume_long_max_range_expansion_ratio: Decimal = Decimal("1.25")
    us_midday_pause_resume_long_require_rebound_below_slow_ema: bool = True
    us_midday_pause_resume_long_require_one_bar_pullback: bool = True
    us_midday_pause_resume_long_require_break_above_prior_1_high: bool = True
    us_late_pause_resume_long_setup_curvature_min: Decimal = Decimal("0.15")
    us_late_pause_resume_long_min_resumption_curvature: Decimal = Decimal("0.15")
    us_late_pause_resume_long_max_range_expansion_ratio: Decimal = Decimal("1.25")
    us_late_pause_resume_long_exclude_1755_carryover: bool = False
    us_late_failed_move_reversal_long_failed_move_curvature_min: Decimal = Decimal("0.15")
    us_late_breakout_retest_hold_breakout_min_range_expansion_ratio: Decimal = Decimal("0.85")
    us_late_breakout_retest_hold_breakout_max_range_expansion_ratio: Decimal = Decimal("1.25")
    us_late_breakout_retest_hold_retest_min_range_expansion_ratio: Decimal = Decimal("1.25")
    asia_early_breakout_retest_hold_breakout_abs_slope_max: Decimal = Decimal("0.20")
    asia_early_breakout_retest_hold_breakout_min_range_expansion_ratio: Decimal = Decimal("0.85")
    asia_early_breakout_retest_hold_breakout_max_range_expansion_ratio: Decimal = Decimal("1.25")
    asia_late_pause_resume_long_pullback_max_range_expansion_ratio: Decimal = Decimal("0.85")
    asia_late_pause_resume_long_signal_min_range_expansion_ratio: Decimal = Decimal("0.85")
    asia_late_pause_resume_long_signal_max_range_expansion_ratio: Decimal = Decimal("1.25")
    asia_late_pause_resume_long_pullback_curvature_flat_threshold: Decimal = Decimal("0.15")
    asia_late_pause_resume_long_setup_max_range_expansion_ratio: Decimal = Decimal("0.85")
    min_bear_snap_up_stretch_atr: Decimal
    min_bear_snap_bar_range_atr: Decimal
    min_bear_snap_body_atr: Decimal
    max_bear_snap_close_location: Decimal
    min_bear_snap_velocity_delta_atr: Decimal
    bear_snap_cooldown_bars: int
    use_bear_snap_location_filter: bool
    bear_snap_min_close_vs_slow_ema_atr: Decimal
    bear_snap_require_close_above_slow_ema: bool
    us_derivative_bear_min_normalized_slope: Decimal = Decimal("-0.80")
    us_derivative_bear_max_normalized_slope: Decimal = Decimal("-0.15")
    us_derivative_bear_max_normalized_curvature: Decimal = Decimal("-0.25")
    us_derivative_bear_min_bar_range_atr: Decimal = Decimal("0.90")
    us_derivative_bear_min_body_atr: Decimal = Decimal("0.35")
    us_derivative_bear_max_close_location: Decimal = Decimal("0.35")
    us_derivative_bear_min_up_stretch_atr: Decimal = Decimal("0.75")
    us_derivative_bear_require_below_fast_ema: bool = True
    us_derivative_bear_require_below_vwap: bool = True
    us_derivative_bear_require_below_slow_ema: bool = False
    us_derivative_bear_min_close_below_slow_ema_atr: Decimal = Decimal("0.00")
    us_derivative_bear_window_start: time = time(8, 30)
    us_derivative_bear_window_end: time = time(17, 0)
    us_derivative_bear_max_distance_below_vwap_atr: Decimal = Decimal("999")
    us_derivative_bear_open_late_min_distance_below_vwap_atr: Decimal = Decimal("0")
    us_derivative_bear_open_late_min_body_atr: Decimal = Decimal("0")
    us_derivative_bear_open_late_max_close_location: Decimal = Decimal("1.00")
    us_derivative_bear_open_late_max_distance_below_fast_ema_atr: Decimal = Decimal("999")
    us_derivative_bear_allow_phase_preopen_opening: bool = True
    us_derivative_bear_allow_phase_cash_open_impulse: bool = True
    us_derivative_bear_allow_phase_open_late: bool = True
    us_derivative_bear_require_fast_below_slow: bool = False
    us_derivative_bear_cooldown_bars: int = 12
    us_derivative_bear_additive_min_normalized_slope: Decimal = Decimal("-0.10")
    us_derivative_bear_additive_max_normalized_slope: Decimal = Decimal("0.10")
    us_derivative_bear_additive_min_normalized_curvature: Decimal = Decimal("-0.50")
    us_derivative_bear_additive_max_normalized_curvature: Decimal = Decimal("-0.10")
    us_derivative_bear_additive_allow_phase_cash_open_impulse: bool = True
    us_derivative_bear_additive_allow_phase_open_late: bool = True
    us_derivative_bear_additive_open_late_min_distance_below_vwap_atr: Decimal = Decimal("0")
    us_derivative_bear_additive_open_late_max_prior_3_bar_avg_vwap_extension_atr: Decimal = Decimal("999")
    us_derivative_bear_additive_open_late_min_prior_3_bar_avg_curvature: Decimal = Decimal("-999")
    us_derivative_bear_additive_open_late_max_prior_1_bar_vwap_extension_atr: Decimal = Decimal("999")
    us_derivative_bear_additive_open_late_max_prior_5_bar_avg_slope: Decimal = Decimal("999")
    us_derivative_bear_additive_open_late_require_prior_3_any_below_vwap: bool = False
    us_derivative_bear_additive_open_late_require_not_two_bar_rebound: bool = False
    us_derivative_bear_additive_open_late_require_break_below_prior_1_low: bool = False
    us_derivative_bear_additive_open_late_require_break_below_prior_2_low: bool = False
    us_derivative_bear_additive_open_late_require_downside_expansion: bool = False
    us_midday_pause_resume_short_min_normalized_slope: Decimal = Decimal("-0.10")
    us_midday_pause_resume_short_max_normalized_slope: Decimal = Decimal("0.10")
    us_midday_pause_resume_short_min_normalized_curvature: Decimal = Decimal("-0.50")
    us_midday_pause_resume_short_max_normalized_curvature: Decimal = Decimal("-0.10")
    us_midday_pause_resume_short_max_range_expansion_ratio: Decimal = Decimal("1.25")
    us_midday_pause_resume_short_require_one_bar_rebound: bool = True
    us_midday_pause_resume_short_require_prior_3_any_positive_curvature: bool = True
    us_midday_pause_resume_short_require_break_below_prior_1_low: bool = True
    us_midday_expanded_pause_resume_short_setup_min_range_expansion_ratio: Decimal = Decimal("1.25")
    us_midday_compressed_pause_resume_short_setup_max_range_expansion_ratio: Decimal = Decimal("0.85")
    us_midday_compressed_pause_resume_short_signal_min_range_expansion_ratio: Decimal = Decimal("0.85")
    us_midday_compressed_pause_resume_short_signal_max_range_expansion_ratio: Decimal = Decimal("1.25")
    us_midday_failed_move_reversal_short_reversal_max_range_expansion_ratio: Decimal = Decimal("0.85")
    london_late_pause_resume_short_min_normalized_slope: Decimal = Decimal("-0.10")
    london_late_pause_resume_short_max_normalized_slope: Decimal = Decimal("0.10")
    london_late_pause_resume_short_min_normalized_curvature: Decimal = Decimal("-0.50")
    london_late_pause_resume_short_max_normalized_curvature: Decimal = Decimal("-0.10")
    london_late_pause_resume_short_max_range_expansion_ratio: Decimal = Decimal("1.25")
    london_late_pause_resume_short_min_distance_above_vwap_atr: Decimal = Decimal("0.25")
    london_late_pause_resume_short_require_above_slow_ema: bool = True
    london_late_pause_resume_short_require_one_bar_rebound: bool = True
    london_late_pause_resume_short_require_prior_3_any_positive_curvature: bool = True
    london_late_pause_resume_short_require_break_below_prior_1_low: bool = True
    asia_early_pause_resume_short_max_normalized_curvature: Decimal = Decimal("-0.15")
    asia_early_pause_resume_short_setup_curvature_flat_threshold: Decimal = Decimal("0.15")
    asia_early_pause_resume_short_max_range_expansion_ratio: Decimal = Decimal("1.25")
    asia_early_pause_resume_short_require_one_bar_rebound: bool = True
    asia_early_pause_resume_short_require_break_below_prior_1_low: bool = True
    asia_early_pause_resume_short_require_close_below_fast_ema: bool = True
    asia_early_compressed_pause_resume_short_rebound_max_range_expansion_ratio: Decimal = Decimal("0.85")
    asia_early_compressed_pause_resume_short_signal_min_range_expansion_ratio: Decimal = Decimal("0.85")
    asia_early_compressed_pause_resume_short_signal_max_range_expansion_ratio: Decimal = Decimal("1.25")
    asia_early_breakout_retest_hold_short_breakout_min_range_expansion_ratio: Decimal = Decimal("1.25")
    asia_early_breakout_retest_hold_short_retest_min_range_expansion_ratio: Decimal = Decimal("1.25")
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
    probationary_enforce_approved_branches: bool = False
    probationary_artifacts_dir: str = "./outputs/probationary"
    live_poll_interval_seconds: int = 30
    live_poll_lookback_minutes: int = 180
    live_strategy_pilot_enabled: bool = False
    live_strategy_pilot_submit_enabled: bool = False
    live_strategy_pilot_single_cycle_mode: bool = True
    live_strategy_pilot_regular_hours_only: bool = True
    live_strategy_pilot_max_quantity: int = 1
    probationary_paper_lanes_json: str = "[]"
    probationary_paper_execution_canary_enabled: bool = False
    probationary_paper_execution_canary_json: str = "{}"
    probationary_paper_execution_canary_force_fire_once_token: str = ""
    probationary_atpe_canary_enabled: bool = False
    probationary_atpe_canary_instruments_json: str = '["MES","MNQ"]'
    probationary_atpe_canary_live_poll_lookback_minutes: int = 1440
    probationary_gc_mgc_acceptance_enabled: bool = False
    probationary_gc_mgc_acceptance_live_poll_lookback_minutes: int = 1440
    probationary_paper_runtime_exclusive_config: bool = False
    probationary_operator_control_path: str = ""
    probationary_paper_lane_warning_open_loss_json: str = "{}"
    probationary_paper_desk_halt_new_entries_loss: Decimal = Decimal("-1500")
    probationary_paper_desk_flatten_and_halt_loss: Decimal = Decimal("-2500")
    probationary_paper_lane_realized_loser_limit_per_session: int = 2
    probationary_paper_lane_id: str = ""
    probationary_paper_lane_display_name: str = ""
    probationary_paper_lane_session_restriction: str = ""
    probationary_extra_approved_long_entry_sources_json: str = "[]"
    probationary_extra_approved_short_entry_sources_json: str = "[]"
    standalone_strategy_definitions_json: str = "[]"

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        normalized = str(value or "").strip().upper()
        if not normalized:
            raise ValueError("symbol is required.")
        return normalized

    @field_validator("timeframe")
    @classmethod
    def validate_timeframe(cls, value: str) -> str:
        return normalize_timeframe_label(value)

    @field_validator("structural_signal_timeframe", "execution_timeframe", "artifact_timeframe")
    @classmethod
    def validate_optional_timeframe_fields(cls, value: str | None) -> str | None:
        if value is None or str(value).strip() == "":
            return None
        return normalize_timeframe_label(str(value))

    @field_validator("context_timeframes", mode="before")
    @classmethod
    def validate_context_timeframes(cls, value: Any) -> tuple[str, ...]:
        if value in (None, "", (), []):
            return ()
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    parsed = None
                if isinstance(parsed, list):
                    raw_values = [str(item).strip() for item in parsed if str(item).strip()]
                else:
                    raw_values = [item.strip() for item in stripped.split(",") if item.strip()]
            else:
                raw_values = [item.strip() for item in stripped.split(",") if item.strip()]
        else:
            raw_values = [str(item).strip() for item in list(value) if str(item).strip()]
        ordered: list[str] = []
        seen: set[str] = set()
        for raw in raw_values:
            normalized = normalize_timeframe_label(raw)
            if normalized not in seen:
                ordered.append(normalized)
                seen.add(normalized)
        return tuple(ordered)

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
        return value

    @field_validator("vwap_policy")
    @classmethod
    def validate_vwap_policy(cls, value: VwapPolicy) -> VwapPolicy:
        if value != VwapPolicy.SESSION_RESET:
            raise ValueError("vwap_policy must remain locked to SESSION_RESET.")
        return value

    @field_validator(
        "trade_size",
        "reconciliation_heartbeat_interval_seconds",
        "order_lifecycle_watchdog_interval_seconds",
        "order_ack_timeout_seconds",
        "order_fill_timeout_seconds",
        "order_timeout_reconcile_grace_seconds",
        "runtime_supervisor_restart_window_seconds",
        "runtime_supervisor_max_auto_restarts_per_window",
        "runtime_supervisor_restart_backoff_seconds",
        "runtime_supervisor_restart_suppression_seconds",
        "runtime_supervisor_failure_cooldown_seconds",
        "atr_len",
        "max_bars_long",
        "max_bars_short",
        "max_concurrent_entries",
        "additive_short_stalled_exit_min_bars",
        "additive_short_profit_protect_min_bars",
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
        "live_strategy_pilot_max_quantity",
    )
    @classmethod
    def validate_positive_ints(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("integer settings must be > 0.")
        return value

    @field_validator("live_strategy_pilot_max_quantity")
    @classmethod
    def validate_live_strategy_pilot_max_quantity(cls, value: int) -> int:
        if value != 1:
            raise ValueError("live_strategy_pilot_max_quantity must remain locked to 1.")
        return value

    @field_validator("live_strategy_pilot_single_cycle_mode")
    @classmethod
    def validate_live_strategy_pilot_single_cycle_mode(cls, value: bool) -> bool:
        if value is not True:
            raise ValueError("live_strategy_pilot_single_cycle_mode must remain enabled for the first live strategy pilot.")
        return value

    @field_validator("order_timeout_retry_limit")
    @classmethod
    def validate_non_negative_ints(cls, value: int) -> int:
        if value < 0:
            raise ValueError("integer settings must be >= 0.")
        return value

    @field_validator("max_adds_after_entry")
    @classmethod
    def validate_max_adds_after_entry(cls, value: int) -> int:
        if value < 0:
            raise ValueError("max_adds_after_entry must be >= 0.")
        return value

    @field_validator("max_position_quantity")
    @classmethod
    def validate_max_position_quantity(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("max_position_quantity must be > 0 when configured.")
        return value

    @field_validator("probationary_paper_lane_realized_loser_limit_per_session")
    @classmethod
    def validate_probationary_paper_lane_realized_loser_limit_per_session(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("probationary_paper_lane_realized_loser_limit_per_session must be > 0.")
        return value

    @field_validator("risk_floor")
    @classmethod
    def validate_risk_floor(cls, value: Decimal) -> Decimal:
        if value != Decimal("0.01"):
            raise ValueError("risk_floor must remain the internal safety floor of 0.01.")
        return value

    @field_validator(
        "probationary_paper_desk_halt_new_entries_loss",
        "probationary_paper_desk_flatten_and_halt_loss",
    )
    @classmethod
    def validate_probationary_paper_loss_thresholds(cls, value: Decimal) -> Decimal:
        if value > 0:
            raise ValueError("probationary paper loss thresholds must be <= 0.")
        return value

    @field_validator("take_profit_at_r")
    @classmethod
    def validate_take_profit_at_r(cls, value: Decimal) -> Decimal:
        if value < 0:
            raise ValueError("take_profit_at_r must be >= 0.")
        return value

    @field_validator("derivative_exit_min_profit_r")
    @classmethod
    def validate_derivative_exit_min_profit_r(cls, value: Decimal) -> Decimal:
        if value < 0:
            raise ValueError("derivative_exit_min_profit_r must be >= 0.")
        return value

    @field_validator("additive_short_profit_protect_min_profit_r")
    @classmethod
    def validate_additive_short_profit_protect_min_profit_r(cls, value: Decimal) -> Decimal:
        if value < 0:
            raise ValueError("additive_short_profit_protect_min_profit_r must be >= 0.")
        return value

    @field_validator("additive_short_giveback_min_peak_profit_r")
    @classmethod
    def validate_additive_short_giveback_min_peak_profit_r(cls, value: Decimal) -> Decimal:
        if value < 0:
            raise ValueError("additive_short_giveback_min_peak_profit_r must be >= 0.")
        return value

    @field_validator("additive_short_giveback_fraction")
    @classmethod
    def validate_additive_short_giveback_fraction(cls, value: Decimal) -> Decimal:
        if value <= 0 or value > 1:
            raise ValueError("additive_short_giveback_fraction must be > 0 and <= 1.")
        return value

    @model_validator(mode="after")
    def validate_time_windows(self) -> "StrategySettings":
        if self.live_strategy_pilot_submit_enabled and not self.live_strategy_pilot_enabled:
            raise ValueError("live_strategy_pilot_submit_enabled requires live_strategy_pilot_enabled to be true.")
        if self.environment_mode is EnvironmentMode.BASELINE_PARITY:
            if self.timeframe != "5m":
                raise ValueError("baseline_parity_mode requires timeframe=5m.")
            if self.replay_fill_policy is not ReplayFillPolicy.NEXT_BAR_OPEN:
                raise ValueError("baseline_parity_mode requires replay_fill_policy=NEXT_BAR_OPEN.")
        structural_timeframe = self.structural_signal_timeframe or self.timeframe
        execution_timeframe = self.execution_timeframe or structural_timeframe
        artifact_timeframe = self.artifact_timeframe or structural_timeframe
        context_timeframes = tuple(self.context_timeframes or (structural_timeframe,))
        if self.environment_mode is EnvironmentMode.BASELINE_PARITY:
            if structural_timeframe != "5m":
                raise ValueError("baseline_parity_mode requires structural_signal_timeframe=5m.")
            if execution_timeframe != "5m":
                raise ValueError("baseline_parity_mode requires execution_timeframe=5m.")
            if context_timeframes != ("5m",):
                raise ValueError("baseline_parity_mode requires context_timeframes=(5m,).")
            if self.execution_timeframe_role is not ExecutionTimeframeRole.MATCHES_SIGNAL_EVALUATION:
                raise ValueError("baseline_parity_mode requires execution_timeframe_role=matches_signal_evaluation.")
        if self.environment_mode is EnvironmentMode.RESEARCH_EXECUTION:
            if self.execution_timeframe_role is ExecutionTimeframeRole.EXECUTION_DETAIL_ONLY:
                if normalize_timeframe_label(execution_timeframe) == normalize_timeframe_label(structural_timeframe):
                    raise ValueError(
                        "research_execution_mode with execution_detail_only requires execution_timeframe distinct from structural_signal_timeframe."
                    )
        if self.environment_mode is EnvironmentMode.LIVE_EXECUTION and self.mode not in {
            RuntimeMode.LIVE,
            RuntimeMode.PAPER,
        }:
            raise ValueError("live_execution_mode requires mode=live or paper.")
        if self.live_strategy_pilot_enabled:
            if self.mode is not RuntimeMode.LIVE:
                raise ValueError("live_strategy_pilot_enabled requires mode=live.")
            if self.symbol != "MGC":
                raise ValueError("live_strategy_pilot_enabled remains locked to symbol MGC.")
            if self.trade_size != 1:
                raise ValueError("live_strategy_pilot_enabled requires trade_size=1.")
        execution_minutes = timeframe_minutes(execution_timeframe)
        for context_timeframe in context_timeframes:
            context_minutes = timeframe_minutes(context_timeframe)
            if context_minutes < execution_minutes:
                raise ValueError("context_timeframes must not be lower than execution_timeframe.")
            if context_minutes % execution_minutes != 0:
                raise ValueError("context_timeframes must be whole-minute multiples of execution_timeframe.")
        if self.max_position_quantity is not None and self.max_position_quantity < self.trade_size:
            raise ValueError("max_position_quantity must be >= trade_size.")
        if self.participation_policy is ParticipationPolicy.SINGLE_ENTRY_ONLY:
            if self.max_concurrent_entries != 1:
                raise ValueError("SINGLE_ENTRY_ONLY requires max_concurrent_entries=1.")
            if self.max_adds_after_entry != 0:
                raise ValueError("SINGLE_ENTRY_ONLY requires max_adds_after_entry=0.")
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

    @property
    def resolved_structural_signal_timeframe(self) -> str:
        return self.structural_signal_timeframe or self.timeframe

    @property
    def resolved_execution_timeframe(self) -> str:
        return self.execution_timeframe or self.resolved_structural_signal_timeframe

    @property
    def resolved_artifact_timeframe(self) -> str:
        return self.artifact_timeframe or self.resolved_structural_signal_timeframe

    @property
    def resolved_context_timeframes(self) -> tuple[str, ...]:
        configured = tuple(self.context_timeframes or ())
        if configured:
            return configured
        return (self.resolved_structural_signal_timeframe,)

    @property
    def primary_context_timeframe(self) -> str:
        return self.resolved_context_timeframes[0]

    @property
    def uses_multi_timescale_execution(self) -> bool:
        return (
            self.resolved_execution_timeframe != self.primary_context_timeframe
            or len(self.resolved_context_timeframes) > 1
        )

    @property
    def probationary_artifacts_path(self) -> Path:
        """Return the root artifact path for the probationary runtime."""
        return Path(self.probationary_artifacts_dir)

    @property
    def resolved_probationary_operator_control_path(self) -> Path:
        configured = str(self.probationary_operator_control_path or "").strip()
        if configured:
            return Path(configured)
        return self.probationary_artifacts_path / "runtime" / "operator_control.json"

    @property
    def approved_long_entry_sources(self) -> frozenset[str]:
        """Return the runtime-allowed long entry sources for probationary mode."""
        allowed: set[str] = set()
        if self.enable_us_late_pause_resume_longs:
            allowed.add("usLatePauseResumeLongTurn")
        if self.enable_asia_early_normal_breakout_retest_hold_longs:
            allowed.add("asiaEarlyNormalBreakoutRetestHoldTurn")
        allowed.update(
            str(value)
            for value in self._parse_probationary_json_payload(
                self.probationary_extra_approved_long_entry_sources_json,
                list,
            )
            if value
        )
        return frozenset(allowed)

    @property
    def approved_short_entry_sources(self) -> frozenset[str]:
        """Return the runtime-allowed short entry sources for probationary mode."""
        allowed: set[str] = set()
        if self.enable_asia_early_pause_resume_shorts:
            allowed.add("asiaEarlyPauseResumeShortTurn")
        allowed.update(
            str(value)
            for value in self._parse_probationary_json_payload(
                self.probationary_extra_approved_short_entry_sources_json,
                list,
            )
            if value
        )
        return frozenset(allowed)

    @property
    def probationary_paper_lane_specs(self) -> tuple[dict[str, Any], ...]:
        """Return configured paper lane specs as raw dictionaries."""
        return tuple(self._parse_probationary_json_payload(self.probationary_paper_lanes_json, list))

    @property
    def probationary_paper_execution_canary_spec(self) -> dict[str, Any]:
        """Return the optional paper execution canary spec."""
        if not self.probationary_paper_execution_canary_enabled:
            return {}
        return dict(self._parse_probationary_json_payload(self.probationary_paper_execution_canary_json, dict))

    @property
    def probationary_atpe_canary_instruments(self) -> tuple[str, ...]:
        """Return the configured ATPE canary instruments."""
        values = self._parse_probationary_json_payload(self.probationary_atpe_canary_instruments_json, list)
        return tuple(str(value).strip().upper() for value in values if str(value).strip())

    @property
    def probationary_paper_lane_warning_open_loss(self) -> dict[str, Decimal]:
        """Return per-lane warning open-loss thresholds keyed by lane_id."""
        payload = self._parse_probationary_json_payload(self.probationary_paper_lane_warning_open_loss_json, dict)
        return {str(key): Decimal(str(value)) for key, value in payload.items()}

    @property
    def standalone_strategy_definitions(self) -> tuple[dict[str, Any], ...]:
        """Return configured standalone strategy definitions as raw dictionaries."""
        return tuple(self._parse_probationary_json_payload(self.standalone_strategy_definitions_json, list))

    @staticmethod
    def _parse_probationary_json_payload(raw_value: str, expected_type: type[list[Any]] | type[dict[str, Any]]) -> Any:
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid probationary JSON payload: {exc.msg}") from exc
        if not isinstance(payload, expected_type):
            raise ValueError(f"Expected probationary JSON payload of type {expected_type.__name__}.")
        return payload
