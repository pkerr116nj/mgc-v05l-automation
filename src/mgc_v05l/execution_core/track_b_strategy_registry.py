"""Track B strategy registry and migration guardrails.

The registry is the contract for plugging additional strategy adapters into the
single Track B runner path. It is intentionally metadata-only: it does not
submit, create order plans, invoke broker APIs, or evaluate strategy logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Sequence


class TrackBStrategyRegistryVerdict(str, Enum):
    READY = "TRACK_B_STRATEGY_REGISTRY_READY"
    NOT_READY = "TRACK_B_STRATEGY_REGISTRY_NOT_READY"
    REJECTED_UNREGISTERED = "TRACK_B_STRATEGY_REGISTRY_REJECTED_UNREGISTERED"
    REJECTED_METADATA = "TRACK_B_STRATEGY_REGISTRY_REJECTED_METADATA"
    BLOCKED_CONFLICTING_SIGNALS = "TRACK_B_STRATEGY_REGISTRY_BLOCKED_CONFLICTING_SIGNALS"
    BLOCKED_MULTIPLE_PAPER_CANDIDATES = "TRACK_B_STRATEGY_REGISTRY_BLOCKED_MULTIPLE_PAPER_CANDIDATES"
    NO_PAPER_CANDIDATE = "TRACK_B_STRATEGY_REGISTRY_NO_PAPER_CANDIDATE"


@dataclass(frozen=True)
class TrackBStrategyRegistryEntry:
    strategy_id: str
    rule_mode: str
    instrument_family: str
    timeframe: str
    required_feature_schema: tuple[str, ...]
    required_state_schema: tuple[str, ...]
    feature_version: str
    calibration_profile: str
    paper_eligible: bool
    live_money_eligible: bool = False
    evaluation_mode: str = "COMPLETED_BAR_ONLY"
    required_1m_context_bars: int = 40
    required_5m_context_bars: int = 8
    managed_exit_policy_id: str | None = None
    exit_not_available: bool = True
    rule_id: str | None = None
    accepted_strategy_ids: tuple[str, ...] = ()
    accepted_rule_ids: tuple[str, ...] = ()

    def report_metadata(self) -> dict[str, Any]:
        return {
            "strategy_registry_id": self.strategy_id,
            "strategy_registry_rule_id": self.rule_id,
            "strategy_registry_rule_mode": self.rule_mode,
            "strategy_registry_instrument_family": self.instrument_family,
            "strategy_registry_timeframe": self.timeframe,
            "strategy_registry_required_feature_schema": list(self.required_feature_schema),
            "strategy_registry_required_state_schema": list(self.required_state_schema),
            "strategy_registry_feature_version": self.feature_version,
            "strategy_registry_calibration_profile": self.calibration_profile,
            "strategy_registry_paper_eligible": self.paper_eligible,
            "strategy_registry_live_money_eligible": self.live_money_eligible,
            "strategy_registry_evaluation_mode": self.evaluation_mode,
            "strategy_registry_required_1m_context_bars": self.required_1m_context_bars,
            "strategy_registry_required_5m_context_bars": self.required_5m_context_bars,
            "strategy_registry_managed_exit_policy_id": self.managed_exit_policy_id,
            "strategy_registry_exit_not_available": self.exit_not_available,
        }


PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1 = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"


MGC_EMA_MOMENTUM_RECLAIM_LONG = TrackBStrategyRegistryEntry(
    strategy_id="mgc_ema_momentum_reclaim_long_v1",
    rule_id="mgc_ema_momentum_reclaim_long_v1",
    rule_mode="MGC_EMA_MOMENTUM_RECLAIM_LONG",
    instrument_family="MGC",
    timeframe="1m",
    required_feature_schema=("metadata.ema_momentum_features",),
    required_state_schema=(),
    feature_version="track_b_mgc_ema_momentum_features_v1",
    calibration_profile="track_b_phase2_initial",
    paper_eligible=True,
    live_money_eligible=False,
    accepted_strategy_ids=("mgc_ema_momentum_reclaim_long_v1", "track_b_example_gold_shadow_v1"),
    accepted_rule_ids=("mgc_ema_momentum_reclaim_long_v1",),
)

ASIAN_DRIFT_V1 = TrackBStrategyRegistryEntry(
    strategy_id="asian_drift_v1",
    rule_id="asian_drift_v1",
    rule_mode="ASIAN_DRIFT_V1",
    instrument_family="MGC",
    timeframe="5m",
    required_feature_schema=("feature_version", "calibration_profile"),
    required_state_schema=(
        "asia_drift_state",
        "asia_drift_regime",
        "hypothetical_entry_ready",
        "entry_window_open",
        "in_scope",
    ),
    feature_version="asia_drift_v1_phase1",
    calibration_profile="recovery_confirmed",
    paper_eligible=True,
    live_money_eligible=False,
    managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    exit_not_available=False,
    accepted_strategy_ids=("asian_drift_v1",),
    accepted_rule_ids=("asian_drift_v1",),
)

ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1 = TrackBStrategyRegistryEntry(
    strategy_id="ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1",
    rule_id="ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1",
    rule_mode="ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1",
    instrument_family="MGC",
    timeframe="5m",
    required_feature_schema=("feature_version", "calibration_profile"),
    required_state_schema=(
        "asia_drift_state",
        "asia_drift_regime",
        "entry_window_open",
        "in_scope",
        "late_join_classification",
        "asian_drift_diagnostic_classification",
    ),
    feature_version="asia_drift_v1_phase1",
    calibration_profile="recovery_confirmed",
    paper_eligible=True,
    live_money_eligible=False,
    managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    exit_not_available=False,
    accepted_strategy_ids=("ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1",),
    accepted_rule_ids=("ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1",),
)

ASIA_EARLY_PAUSE_RESUME_SHORT_V1 = TrackBStrategyRegistryEntry(
    strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
    rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
    rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
    instrument_family="MGC",
    timeframe="5m",
    required_feature_schema=(
        "metadata.asia_early_pause_resume_short_features.normalized_curvature",
        "metadata.asia_early_pause_resume_short_features.signal_range_expansion_ratio",
        "metadata.asia_early_pause_resume_short_features.setup_bar_curvature_is_flat",
        "metadata.asia_early_pause_resume_short_features.one_bar_rebound_before_signal",
        "metadata.asia_early_pause_resume_short_features.signal_breaks_prior_1_low",
        "metadata.asia_early_pause_resume_short_features.close_below_fast_ema",
        "metadata.asia_early_pause_resume_short_features.derivative_bear_close_weak",
        "metadata.asia_early_pause_resume_short_features.derivative_bear_range_ok",
        "metadata.asia_early_pause_resume_short_features.derivative_bear_body_ok",
        "metadata.asia_early_pause_resume_short_features.derivative_bear_stretch_ok",
        "metadata.asia_early_pause_resume_short_features.derivative_bear_cooldown_ok",
        "metadata.asia_early_pause_resume_short_features.no_competing_bear_short_candidate",
        "metadata.asia_early_pause_resume_short_features.feature_version",
        "metadata.asia_early_pause_resume_short_features.calibration_profile",
    ),
    required_state_schema=(
        "metadata.asia_early_pause_resume_short_state.derivative_phase",
        "metadata.asia_early_pause_resume_short_state.session_asia",
        "metadata.asia_early_pause_resume_short_state.allow_asia",
    ),
    feature_version="asia_early_pause_resume_short_v1_phase1",
    calibration_profile="probationary_baseline_v1",
    paper_eligible=True,
    live_money_eligible=False,
    managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    exit_not_available=False,
    accepted_strategy_ids=("ASIA_EARLY_PAUSE_RESUME_SHORT_V1",),
    accepted_rule_ids=("ASIA_EARLY_PAUSE_RESUME_SHORT_V1",),
)

ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1 = TrackBStrategyRegistryEntry(
    strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
    rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
    rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
    instrument_family="MGC",
    timeframe="5m",
    required_feature_schema=(
        "metadata.asia_early_normal_breakout_retest_hold_long_features.breakout_bar_slope_is_flat",
        "metadata.asia_early_normal_breakout_retest_hold_long_features.breakout_bar_expansion_is_normal",
        "metadata.asia_early_normal_breakout_retest_hold_long_features.breakout_breaks_prior_1_high",
        "metadata.asia_early_normal_breakout_retest_hold_long_features.signal_retests_and_holds_breakout_level",
        "metadata.asia_early_normal_breakout_retest_hold_long_features.feature_version",
        "metadata.asia_early_normal_breakout_retest_hold_long_features.calibration_profile",
    ),
    required_state_schema=(
        "metadata.asia_early_normal_breakout_retest_hold_long_state.asia_early_or_gc_mgc_london_open",
        "metadata.asia_early_normal_breakout_retest_hold_long_state.allow_asia",
        "metadata.asia_early_normal_breakout_retest_hold_long_state.no_first_bull_snap_turn",
        "metadata.asia_early_normal_breakout_retest_hold_long_state.prior_bars_since_long_setup_gt_anti_churn",
    ),
    feature_version="asia_early_normal_breakout_retest_hold_long_v1_phase1",
    calibration_profile="probationary_baseline_v1",
    paper_eligible=True,
    live_money_eligible=False,
    managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    exit_not_available=False,
    accepted_strategy_ids=("ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",),
    accepted_rule_ids=("ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",),
)

FIRST_BULL_SNAP_TURN_V1 = TrackBStrategyRegistryEntry(
    strategy_id="FIRST_BULL_SNAP_TURN_V1",
    rule_id="FIRST_BULL_SNAP_TURN_V1",
    rule_mode="FIRST_BULL_SNAP_TURN_V1",
    instrument_family="MGC",
    timeframe="5m",
    required_feature_schema=(
        "metadata.first_bull_snap_turn_features.bull_snap_downside_stretch_ok",
        "metadata.first_bull_snap_turn_features.bull_snap_range_ok",
        "metadata.first_bull_snap_turn_features.bull_snap_body_ok",
        "metadata.first_bull_snap_turn_features.bull_snap_close_strong",
        "metadata.first_bull_snap_turn_features.bull_snap_velocity_ok",
        "metadata.first_bull_snap_turn_features.bull_snap_reversal_bar",
        "metadata.first_bull_snap_turn_features.bull_snap_location_ok",
        "metadata.first_bull_snap_turn_features.bull_snap_raw",
        "metadata.first_bull_snap_turn_features.bull_snap_turn_candidate",
        "metadata.first_bull_snap_turn_features.first_bull_snap_turn",
        "metadata.first_bull_snap_turn_features.feature_version",
        "metadata.first_bull_snap_turn_features.calibration_profile",
    ),
    required_state_schema=(
        "metadata.first_bull_snap_turn_state.session_allowed",
        "metadata.first_bull_snap_turn_state.prior_bars_since_bull_snap_gt_cooldown",
        "metadata.first_bull_snap_turn_state.derivative_phase",
    ),
    feature_version="first_bull_snap_turn_v1_phase1",
    calibration_profile="probationary_baseline_v1",
    paper_eligible=True,
    live_money_eligible=False,
    managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    exit_not_available=False,
    accepted_strategy_ids=("FIRST_BULL_SNAP_TURN_V1",),
    accepted_rule_ids=("FIRST_BULL_SNAP_TURN_V1",),
)

FIRST_BEAR_SNAP_TURN_V1 = TrackBStrategyRegistryEntry(
    strategy_id="FIRST_BEAR_SNAP_TURN_V1",
    rule_id="FIRST_BEAR_SNAP_TURN_V1",
    rule_mode="FIRST_BEAR_SNAP_TURN_V1",
    instrument_family="MGC",
    timeframe="5m",
    required_feature_schema=(
        "metadata.first_bear_snap_turn_features.bear_snap_up_stretch_ok",
        "metadata.first_bear_snap_turn_features.bear_snap_range_ok",
        "metadata.first_bear_snap_turn_features.bear_snap_body_ok",
        "metadata.first_bear_snap_turn_features.bear_snap_close_weak",
        "metadata.first_bear_snap_turn_features.bear_snap_velocity_ok",
        "metadata.first_bear_snap_turn_features.bear_snap_reversal_bar",
        "metadata.first_bear_snap_turn_features.bear_snap_location_ok",
        "metadata.first_bear_snap_turn_features.bear_snap_raw",
        "metadata.first_bear_snap_turn_features.bear_snap_turn_candidate",
        "metadata.first_bear_snap_turn_features.first_bear_snap_turn",
        "metadata.first_bear_snap_turn_features.feature_version",
        "metadata.first_bear_snap_turn_features.calibration_profile",
    ),
    required_state_schema=(
        "metadata.first_bear_snap_turn_state.session_allowed",
        "metadata.first_bear_snap_turn_state.prior_bars_since_bear_snap_gt_cooldown",
        "metadata.first_bear_snap_turn_state.derivative_phase",
    ),
    feature_version="first_bear_snap_turn_v1_phase1",
    calibration_profile="probationary_baseline_v1",
    paper_eligible=True,
    live_money_eligible=False,
    managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    exit_not_available=False,
    accepted_strategy_ids=("FIRST_BEAR_SNAP_TURN_V1",),
    accepted_rule_ids=("FIRST_BEAR_SNAP_TURN_V1",),
)

LONDON_LATE_PAUSE_RESUME_SHORT_V1 = TrackBStrategyRegistryEntry(
    strategy_id="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
    rule_id="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
    rule_mode="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
    instrument_family="MGC",
    timeframe="5m",
    required_feature_schema=(
        "metadata.london_late_pause_resume_short_features.normalized_slope",
        "metadata.london_late_pause_resume_short_features.normalized_curvature",
        "metadata.london_late_pause_resume_short_features.signal_range_expansion_ratio",
        "metadata.london_late_pause_resume_short_features.derivative_bear_close_weak",
        "metadata.london_late_pause_resume_short_features.derivative_bear_range_ok",
        "metadata.london_late_pause_resume_short_features.derivative_bear_body_ok",
        "metadata.london_late_pause_resume_short_features.derivative_bear_stretch_ok",
        "metadata.london_late_pause_resume_short_features.slow_ema_ok",
        "metadata.london_late_pause_resume_short_features.one_bar_rebound_before_signal",
        "metadata.london_late_pause_resume_short_features.prior_3_any_positive_curvature",
        "metadata.london_late_pause_resume_short_features.signal_breaks_prior_1_low",
        "metadata.london_late_pause_resume_short_features.derivative_bear_cooldown_ok",
        "metadata.london_late_pause_resume_short_features.no_competing_bear_short_candidate",
        "metadata.london_late_pause_resume_short_features.feature_version",
        "metadata.london_late_pause_resume_short_features.calibration_profile",
    ),
    required_state_schema=(
        "metadata.london_late_pause_resume_short_state.derivative_phase",
        "metadata.london_late_pause_resume_short_state.session_london",
        "metadata.london_late_pause_resume_short_state.allow_london",
        "metadata.london_late_pause_resume_short_state.no_first_bear_snap_turn",
    ),
    feature_version="london_late_pause_resume_short_v1_phase1",
    calibration_profile="probationary_baseline_v1",
    paper_eligible=True,
    live_money_eligible=False,
    managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    exit_not_available=False,
    accepted_strategy_ids=("LONDON_LATE_PAUSE_RESUME_SHORT_V1",),
    accepted_rule_ids=("LONDON_LATE_PAUSE_RESUME_SHORT_V1",),
)

ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1 = TrackBStrategyRegistryEntry(
    strategy_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
    rule_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
    rule_mode="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
    instrument_family="MGC",
    timeframe="5m",
    required_feature_schema=(
        "metadata.asia_late_flat_pullback_pause_resume_long_features.bull_snap_close_strong",
        "metadata.asia_late_flat_pullback_pause_resume_long_features.one_bar_pullback_before_signal",
        "metadata.asia_late_flat_pullback_pause_resume_long_features.signal_breaks_prior_1_high",
        "metadata.asia_late_flat_pullback_pause_resume_long_features.pullback_range_expansion_ratio",
        "metadata.asia_late_flat_pullback_pause_resume_long_features.signal_range_expansion_ratio",
        "metadata.asia_late_flat_pullback_pause_resume_long_features.pullback_normalized_curvature",
        "metadata.asia_late_flat_pullback_pause_resume_long_features.prior_bars_since_long_setup_gt_anti_churn",
        "metadata.asia_late_flat_pullback_pause_resume_long_features.feature_version",
        "metadata.asia_late_flat_pullback_pause_resume_long_features.calibration_profile",
    ),
    required_state_schema=(
        "metadata.asia_late_flat_pullback_pause_resume_long_state.derivative_phase",
        "metadata.asia_late_flat_pullback_pause_resume_long_state.session_asia",
        "metadata.asia_late_flat_pullback_pause_resume_long_state.allow_asia",
        "metadata.asia_late_flat_pullback_pause_resume_long_state.no_first_bull_snap_turn",
    ),
    feature_version="asia_late_flat_pullback_pause_resume_long_v1_phase1",
    calibration_profile="probationary_baseline_v1",
    paper_eligible=True,
    live_money_eligible=False,
    managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    exit_not_available=False,
    accepted_strategy_ids=("ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",),
    accepted_rule_ids=("ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",),
)

US_DERIVATIVE_BEAR_TURN_V1 = TrackBStrategyRegistryEntry(
    strategy_id="US_DERIVATIVE_BEAR_TURN_V1",
    rule_id="US_DERIVATIVE_BEAR_TURN_V1",
    rule_mode="US_DERIVATIVE_BEAR_TURN_V1",
    instrument_family="MGC",
    timeframe="5m",
    required_feature_schema=(
        "metadata.us_derivative_bear_turn_features.normalized_slope",
        "metadata.us_derivative_bear_turn_features.normalized_curvature",
        "metadata.us_derivative_bear_turn_features.close_below_open",
        "metadata.us_derivative_bear_turn_features.close_below_previous_close",
        "metadata.us_derivative_bear_turn_features.derivative_bear_close_weak",
        "metadata.us_derivative_bear_turn_features.derivative_bear_range_ok",
        "metadata.us_derivative_bear_turn_features.derivative_bear_body_ok",
        "metadata.us_derivative_bear_turn_features.derivative_bear_stretch_ok",
        "metadata.us_derivative_bear_turn_features.derivative_bear_fast_ema_ok",
        "metadata.us_derivative_bear_turn_features.derivative_bear_vwap_ok",
        "metadata.us_derivative_bear_turn_features.derivative_bear_vwap_extension_ok",
        "metadata.us_derivative_bear_turn_features.derivative_bear_open_late_extension_floor_ok",
        "metadata.us_derivative_bear_turn_features.derivative_bear_open_late_body_ok",
        "metadata.us_derivative_bear_turn_features.derivative_bear_open_late_close_ok",
        "metadata.us_derivative_bear_turn_features.derivative_bear_open_late_fast_ema_extension_ok",
        "metadata.us_derivative_bear_turn_features.derivative_bear_slow_ema_ok",
        "metadata.us_derivative_bear_turn_features.derivative_bear_structure_ok",
        "metadata.us_derivative_bear_turn_features.derivative_bear_cooldown_ok",
        "metadata.us_derivative_bear_turn_features.feature_version",
        "metadata.us_derivative_bear_turn_features.calibration_profile",
    ),
    required_state_schema=(
        "metadata.us_derivative_bear_turn_state.derivative_phase",
        "metadata.us_derivative_bear_turn_state.session_us",
        "metadata.us_derivative_bear_turn_state.allow_us",
        "metadata.us_derivative_bear_turn_state.derivative_bear_window_ok",
        "metadata.us_derivative_bear_turn_state.derivative_bear_phase_ok",
    ),
    feature_version="us_derivative_bear_turn_v1_phase1",
    calibration_profile="probationary_baseline_v1",
    paper_eligible=True,
    live_money_eligible=False,
    evaluation_mode="COMPLETED_BAR_ONLY",
    managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    exit_not_available=False,
    accepted_strategy_ids=("US_DERIVATIVE_BEAR_TURN_V1",),
    accepted_rule_ids=("US_DERIVATIVE_BEAR_TURN_V1",),
)

MNQ_US_DERIVATIVE_BEAR_TURN_V1 = TrackBStrategyRegistryEntry(
    strategy_id="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
    rule_id="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
    rule_mode="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
    instrument_family="MNQ",
    timeframe="5m",
    required_feature_schema=(
        "metadata.mnq_us_derivative_bear_turn_features.normalized_slope",
        "metadata.mnq_us_derivative_bear_turn_features.normalized_curvature",
        "metadata.mnq_us_derivative_bear_turn_features.close_below_open",
        "metadata.mnq_us_derivative_bear_turn_features.close_below_previous_close",
        "metadata.mnq_us_derivative_bear_turn_features.derivative_bear_close_weak",
        "metadata.mnq_us_derivative_bear_turn_features.derivative_bear_range_ok",
        "metadata.mnq_us_derivative_bear_turn_features.derivative_bear_body_ok",
        "metadata.mnq_us_derivative_bear_turn_features.derivative_bear_stretch_ok",
        "metadata.mnq_us_derivative_bear_turn_features.derivative_bear_fast_ema_ok",
        "metadata.mnq_us_derivative_bear_turn_features.derivative_bear_vwap_ok",
        "metadata.mnq_us_derivative_bear_turn_features.derivative_bear_vwap_extension_ok",
        "metadata.mnq_us_derivative_bear_turn_features.derivative_bear_open_late_extension_floor_ok",
        "metadata.mnq_us_derivative_bear_turn_features.derivative_bear_open_late_body_ok",
        "metadata.mnq_us_derivative_bear_turn_features.derivative_bear_open_late_close_ok",
        "metadata.mnq_us_derivative_bear_turn_features.derivative_bear_open_late_fast_ema_extension_ok",
        "metadata.mnq_us_derivative_bear_turn_features.derivative_bear_slow_ema_ok",
        "metadata.mnq_us_derivative_bear_turn_features.derivative_bear_structure_ok",
        "metadata.mnq_us_derivative_bear_turn_features.derivative_bear_cooldown_ok",
        "metadata.mnq_us_derivative_bear_turn_features.feature_version",
        "metadata.mnq_us_derivative_bear_turn_features.calibration_profile",
    ),
    required_state_schema=(
        "metadata.mnq_us_derivative_bear_turn_state.derivative_phase",
        "metadata.mnq_us_derivative_bear_turn_state.session_us",
        "metadata.mnq_us_derivative_bear_turn_state.allow_us",
        "metadata.mnq_us_derivative_bear_turn_state.derivative_bear_window_ok",
        "metadata.mnq_us_derivative_bear_turn_state.derivative_bear_phase_ok",
    ),
    feature_version="mnq_us_derivative_bear_turn_v1_phase1",
    calibration_profile="probationary_baseline_v1",
    paper_eligible=True,
    live_money_eligible=False,
    evaluation_mode="COMPLETED_BAR_ONLY",
    managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    exit_not_available=False,
    accepted_strategy_ids=("MNQ_US_DERIVATIVE_BEAR_TURN_V1",),
    accepted_rule_ids=("MNQ_US_DERIVATIVE_BEAR_TURN_V1",),
)

MNQ_FIRST_BEAR_SNAP_TURN_V1 = TrackBStrategyRegistryEntry(
    strategy_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
    rule_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
    rule_mode="MNQ_FIRST_BEAR_SNAP_TURN_V1",
    instrument_family="MNQ",
    timeframe="5m",
    required_feature_schema=(
        "metadata.mnq_first_bear_snap_turn_features.bear_snap_up_stretch_ok",
        "metadata.mnq_first_bear_snap_turn_features.bear_snap_range_ok",
        "metadata.mnq_first_bear_snap_turn_features.bear_snap_body_ok",
        "metadata.mnq_first_bear_snap_turn_features.bear_snap_close_weak",
        "metadata.mnq_first_bear_snap_turn_features.bear_snap_velocity_ok",
        "metadata.mnq_first_bear_snap_turn_features.bear_snap_reversal_bar",
        "metadata.mnq_first_bear_snap_turn_features.bear_snap_location_ok",
        "metadata.mnq_first_bear_snap_turn_features.bear_snap_raw",
        "metadata.mnq_first_bear_snap_turn_features.bear_snap_turn_candidate",
        "metadata.mnq_first_bear_snap_turn_features.first_bear_snap_turn",
        "metadata.mnq_first_bear_snap_turn_features.feature_version",
        "metadata.mnq_first_bear_snap_turn_features.calibration_profile",
    ),
    required_state_schema=(
        "metadata.mnq_first_bear_snap_turn_state.session_allowed",
        "metadata.mnq_first_bear_snap_turn_state.prior_bars_since_bear_snap_gt_cooldown",
        "metadata.mnq_first_bear_snap_turn_state.derivative_phase",
    ),
    feature_version="mnq_first_bear_snap_turn_v1_phase1",
    calibration_profile="probationary_baseline_v1",
    paper_eligible=True,
    live_money_eligible=False,
    evaluation_mode="COMPLETED_BAR_ONLY",
    required_1m_context_bars=40,
    required_5m_context_bars=8,
    managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    exit_not_available=False,
    accepted_strategy_ids=("MNQ_FIRST_BEAR_SNAP_TURN_V1",),
    accepted_rule_ids=("MNQ_FIRST_BEAR_SNAP_TURN_V1",),
)

MNQ_FIRST_BULL_SNAP_TURN_V1 = TrackBStrategyRegistryEntry(
    strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
    rule_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
    rule_mode="MNQ_FIRST_BULL_SNAP_TURN_V1",
    instrument_family="MNQ",
    timeframe="5m",
    required_feature_schema=(
        "metadata.mnq_first_bull_snap_turn_features.bull_snap_downside_stretch_ok",
        "metadata.mnq_first_bull_snap_turn_features.bull_snap_range_ok",
        "metadata.mnq_first_bull_snap_turn_features.bull_snap_body_ok",
        "metadata.mnq_first_bull_snap_turn_features.bull_snap_close_strong",
        "metadata.mnq_first_bull_snap_turn_features.bull_snap_velocity_ok",
        "metadata.mnq_first_bull_snap_turn_features.bull_snap_reversal_bar",
        "metadata.mnq_first_bull_snap_turn_features.bull_snap_location_ok",
        "metadata.mnq_first_bull_snap_turn_features.bull_snap_raw",
        "metadata.mnq_first_bull_snap_turn_features.bull_snap_turn_candidate",
        "metadata.mnq_first_bull_snap_turn_features.first_bull_snap_turn",
        "metadata.mnq_first_bull_snap_turn_features.feature_version",
        "metadata.mnq_first_bull_snap_turn_features.calibration_profile",
    ),
    required_state_schema=(
        "metadata.mnq_first_bull_snap_turn_state.session_allowed",
        "metadata.mnq_first_bull_snap_turn_state.prior_bars_since_bull_snap_gt_cooldown",
        "metadata.mnq_first_bull_snap_turn_state.derivative_phase",
    ),
    feature_version="mnq_first_bull_snap_turn_v1_phase1",
    calibration_profile="probationary_baseline_v1",
    paper_eligible=True,
    live_money_eligible=False,
    evaluation_mode="COMPLETED_BAR_ONLY",
    required_1m_context_bars=40,
    required_5m_context_bars=8,
    managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    exit_not_available=False,
    accepted_strategy_ids=("MNQ_FIRST_BULL_SNAP_TURN_V1",),
    accepted_rule_ids=("MNQ_FIRST_BULL_SNAP_TURN_V1",),
)

PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_LONG_V1 = TrackBStrategyRegistryEntry(
    strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1",
    rule_id="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1",
    rule_mode="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1",
    instrument_family="MNQ",
    timeframe="1m",
    required_feature_schema=(),
    required_state_schema=(),
    feature_version="paper_active_evidence_london_open_participation_v1",
    calibration_profile="simple_london_open_reference_plus_recent_close_long",
    paper_eligible=True,
    live_money_eligible=False,
    required_1m_context_bars=2,
    required_5m_context_bars=0,
    managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
    exit_not_available=False,
    accepted_strategy_ids=("PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1",),
    accepted_rule_ids=("PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1",),
)

PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_SHORT_V1 = TrackBStrategyRegistryEntry(
    strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    rule_id="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    rule_mode="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    instrument_family="MNQ",
    timeframe="1m",
    required_feature_schema=(),
    required_state_schema=(),
    feature_version="paper_active_evidence_london_open_participation_v1",
    calibration_profile="simple_london_open_reference_plus_recent_close_short",
    paper_eligible=True,
    live_money_eligible=False,
    required_1m_context_bars=2,
    required_5m_context_bars=0,
    managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
    exit_not_available=False,
    accepted_strategy_ids=("PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1",),
    accepted_rule_ids=("PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1",),
)

PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_LONG_V1 = TrackBStrategyRegistryEntry(
    strategy_id="PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1",
    rule_id="PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1",
    rule_mode="PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1",
    instrument_family="MES",
    timeframe="1m",
    required_feature_schema=(),
    required_state_schema=(),
    feature_version="paper_active_evidence_london_open_participation_v1",
    calibration_profile="simple_london_open_reference_plus_recent_close_long",
    paper_eligible=True,
    live_money_eligible=False,
    required_1m_context_bars=2,
    required_5m_context_bars=0,
    managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
    exit_not_available=False,
    accepted_strategy_ids=("PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1",),
    accepted_rule_ids=("PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1",),
)

PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_SHORT_V1 = TrackBStrategyRegistryEntry(
    strategy_id="PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    rule_id="PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    rule_mode="PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    instrument_family="MES",
    timeframe="1m",
    required_feature_schema=(),
    required_state_schema=(),
    feature_version="paper_active_evidence_london_open_participation_v1",
    calibration_profile="simple_london_open_reference_plus_recent_close_short",
    paper_eligible=True,
    live_money_eligible=False,
    required_1m_context_bars=2,
    required_5m_context_bars=0,
    managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
    exit_not_available=False,
    accepted_strategy_ids=("PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1",),
    accepted_rule_ids=("PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1",),
)

PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_SHORT_V1 = TrackBStrategyRegistryEntry(
    strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1",
    rule_id="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1",
    rule_mode="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1",
    instrument_family="MNQ",
    timeframe="1m",
    required_feature_schema=(),
    required_state_schema=(),
    feature_version="paper_active_evidence_london_late_participation_v1",
    calibration_profile="simple_london_late_reference_plus_recent_close_short",
    paper_eligible=True,
    live_money_eligible=False,
    required_1m_context_bars=2,
    required_5m_context_bars=0,
    managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
    exit_not_available=False,
    accepted_strategy_ids=("PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1",),
    accepted_rule_ids=("PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1",),
)

PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_V1 = TrackBStrategyRegistryEntry(
    strategy_id="PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_PARTICIPATION_SHORT_V1",
    rule_id="PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_PARTICIPATION_SHORT_V1",
    rule_mode="PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_PARTICIPATION_SHORT_V1",
    instrument_family="MES",
    timeframe="1m",
    required_feature_schema=(),
    required_state_schema=(),
    feature_version="paper_active_evidence_london_late_participation_v1",
    calibration_profile="simple_london_late_reference_plus_recent_close_short",
    paper_eligible=True,
    live_money_eligible=False,
    required_1m_context_bars=2,
    required_5m_context_bars=0,
    managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
    exit_not_available=False,
    accepted_strategy_ids=("PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_PARTICIPATION_SHORT_V1",),
    accepted_rule_ids=("PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_PARTICIPATION_SHORT_V1",),
)

US_LATE_PAUSE_RESUME_LONG_V1 = TrackBStrategyRegistryEntry(
    strategy_id="US_LATE_PAUSE_RESUME_LONG_V1",
    rule_id="US_LATE_PAUSE_RESUME_LONG_V1",
    rule_mode="US_LATE_PAUSE_RESUME_LONG_V1",
    instrument_family="MGC",
    timeframe="5m",
    required_feature_schema=(
        "metadata.us_late_pause_resume_long_features.bull_snap_close_strong",
        "metadata.us_late_pause_resume_long_features.signal_range_expansion_ratio",
        "metadata.us_late_pause_resume_long_features.one_bar_pullback_before_signal",
        "metadata.us_late_pause_resume_long_features.signal_breaks_prior_1_high",
        "metadata.us_late_pause_resume_long_features.signal_ema_location_ok",
        "metadata.us_late_pause_resume_long_features.setup_bar_curvature_is_positive",
        "metadata.us_late_pause_resume_long_features.prior_bars_since_long_setup_gt_anti_churn",
        "metadata.us_late_pause_resume_long_features.feature_version",
        "metadata.us_late_pause_resume_long_features.calibration_profile",
    ),
    required_state_schema=(
        "metadata.us_late_pause_resume_long_state.derivative_phase",
        "metadata.us_late_pause_resume_long_state.session_us_late",
        "metadata.us_late_pause_resume_long_state.allow_us",
        "metadata.us_late_pause_resume_long_state.no_first_bull_snap_turn",
    ),
    feature_version="us_late_pause_resume_long_v1_phase1",
    calibration_profile="probationary_baseline_v1",
    paper_eligible=True,
    live_money_eligible=False,
    evaluation_mode="COMPLETED_BAR_ONLY",
    managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    exit_not_available=False,
    accepted_strategy_ids=("US_LATE_PAUSE_RESUME_LONG_V1",),
    accepted_rule_ids=("US_LATE_PAUSE_RESUME_LONG_V1",),
)

DEMO_WIRING_PROOF = TrackBStrategyRegistryEntry(
    strategy_id="track_b_demo_wiring_proof",
    rule_id="mgc_realtime_quote_demo_long_v1",
    rule_mode="DEMO_LONG_ONLY",
    instrument_family="MGC",
    timeframe="quote_snapshot",
    required_feature_schema=(),
    required_state_schema=(),
    feature_version="demo_wiring_proof_v1",
    calibration_profile="demo_wiring_proof",
    paper_eligible=True,
    live_money_eligible=False,
    accepted_strategy_ids=("track_b_demo_wiring_proof", "track_b_example_gold_shadow_v1"),
    accepted_rule_ids=("mgc_realtime_quote_demo_long_v1", "mgc_ema_momentum_reclaim_long_v1"),
)

HUMAN_REVIEW_ONLY = TrackBStrategyRegistryEntry(
    strategy_id="human_review_only",
    rule_id="human_review_only",
    rule_mode="HUMAN_REVIEW_ONLY",
    instrument_family="MGC",
    timeframe="quote_snapshot",
    required_feature_schema=(),
    required_state_schema=(),
    feature_version="human_review_only_v1",
    calibration_profile="manual_review",
    paper_eligible=False,
    live_money_eligible=False,
    accepted_strategy_ids=("human_review_only", "track_b_example_gold_shadow_v1"),
    accepted_rule_ids=("human_review_only", "mgc_ema_momentum_reclaim_long_v1"),
)

TRACK_B_STRATEGY_REGISTRY: tuple[TrackBStrategyRegistryEntry, ...] = (
    MGC_EMA_MOMENTUM_RECLAIM_LONG,
    ASIAN_DRIFT_V1,
    ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1,
    ASIA_EARLY_PAUSE_RESUME_SHORT_V1,
    ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1,
    FIRST_BULL_SNAP_TURN_V1,
    FIRST_BEAR_SNAP_TURN_V1,
    LONDON_LATE_PAUSE_RESUME_SHORT_V1,
    ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1,
    US_DERIVATIVE_BEAR_TURN_V1,
    MNQ_US_DERIVATIVE_BEAR_TURN_V1,
    MNQ_FIRST_BEAR_SNAP_TURN_V1,
    MNQ_FIRST_BULL_SNAP_TURN_V1,
    PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_LONG_V1,
    PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_SHORT_V1,
    PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_LONG_V1,
    PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_SHORT_V1,
    PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_SHORT_V1,
    PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_V1,
    US_LATE_PAUSE_RESUME_LONG_V1,
    DEMO_WIRING_PROOF,
    HUMAN_REVIEW_ONLY,
)


def get_track_b_strategy_registry() -> tuple[TrackBStrategyRegistryEntry, ...]:
    return TRACK_B_STRATEGY_REGISTRY


def validate_track_b_strategy_registry(
    entries: Sequence[TrackBStrategyRegistryEntry] = TRACK_B_STRATEGY_REGISTRY,
) -> list[str]:
    blockers: list[str] = []
    required_text_fields = (
        "strategy_id",
        "rule_mode",
        "instrument_family",
        "timeframe",
        "feature_version",
        "calibration_profile",
    )
    for index, entry in enumerate(entries):
        for field_name in required_text_fields:
            if not _text(getattr(entry, field_name)):
                blockers.append(f"registry entry {index} is missing required metadata field {field_name}.")
        if entry.live_money_eligible is not False:
            blockers.append(f"registry entry {entry.strategy_id or index} must keep live_money_eligible=false.")
        if entry.required_1m_context_bars < 0 or entry.required_5m_context_bars < 0:
            blockers.append(f"registry entry {entry.strategy_id or index} must use nonnegative context bar requirements.")
    return blockers


def resolve_track_b_strategy_registry_entry(
    *,
    rule_mode: str,
    rule_id: str | None,
    strategy_id: str | None,
) -> TrackBStrategyRegistryEntry | None:
    normalized_mode = _upper(rule_mode)
    normalized_rule_id = _text(rule_id)
    normalized_strategy_id = _text(strategy_id)
    mode_entries = [entry for entry in TRACK_B_STRATEGY_REGISTRY if entry.rule_mode == normalized_mode]
    for entry in mode_entries:
        rule_ids = set(entry.accepted_rule_ids or ())
        strategy_ids = set(entry.accepted_strategy_ids or ())
        rule_ok = normalized_rule_id is None or normalized_rule_id == entry.rule_id or normalized_rule_id in rule_ids
        strategy_ok = normalized_strategy_id is None or normalized_strategy_id == entry.strategy_id or normalized_strategy_id in strategy_ids
        if rule_ok and strategy_ok:
            return entry
    return None


def validate_strategy_event_against_registry(
    *,
    event: Mapping[str, Any],
    rule_mode: str,
    rule_id: str | None,
    strategy_id: str | None = None,
) -> tuple[TrackBStrategyRegistryEntry | None, str | None]:
    registry_blockers = validate_track_b_strategy_registry()
    if registry_blockers:
        return None, "; ".join(registry_blockers)
    actual_strategy_id = _text(strategy_id) or _text(event.get("strategy_id") or event.get("signal_family"))
    entry = resolve_track_b_strategy_registry_entry(
        rule_mode=rule_mode,
        rule_id=rule_id,
        strategy_id=actual_strategy_id,
    )
    if entry is None:
        return None, f"Track B strategy is not registered for rule_mode={rule_mode}, rule_id={rule_id}, strategy_id={actual_strategy_id}."
    if entry.live_money_eligible is not False:
        return entry, f"Track B strategy registry entry {entry.strategy_id} must keep live_money_eligible=false."
    missing = _missing_required_fields(event, entry.required_feature_schema + entry.required_state_schema)
    if missing:
        return entry, f"Track B strategy {entry.strategy_id} is NOT_READY; missing required feature/state fields: {', '.join(missing)}."
    return entry, None


def arbitrate_track_b_strategy_candidates(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    signal_candidates = [dict(candidate) for candidate in candidates if candidate.get("signal_emitted") is True]
    if not signal_candidates:
        return {
            "strategy_arbitration_verdict": TrackBStrategyRegistryVerdict.NO_PAPER_CANDIDATE.value,
            "chosen_candidate": None,
            "suppressed_candidates": [],
            "paper_candidate_count": 0,
            "primary_blocker": None,
            "required_next_action": "Continue bounded no-submit watch until exactly one registered strategy emits a paper-eligible signal.",
        }
    sides = {_upper(candidate.get("signal_side") or candidate.get("signal_direction") or candidate.get("decision")) for candidate in signal_candidates}
    sides.discard("")
    if len(sides) > 1:
        return {
            "strategy_arbitration_verdict": TrackBStrategyRegistryVerdict.BLOCKED_CONFLICTING_SIGNALS.value,
            "chosen_candidate": None,
            "suppressed_candidates": signal_candidates,
            "paper_candidate_count": len(signal_candidates),
            "primary_blocker": "Conflicting strategy signals require explicit arbitration; no Track B paper candidate is selected.",
            "required_next_action": "Resolve strategy arbitration before any PAPER handoff.",
        }
    paper_candidates = [candidate for candidate in signal_candidates if candidate.get("paper_eligible") is True]
    if len(paper_candidates) != 1:
        return {
            "strategy_arbitration_verdict": TrackBStrategyRegistryVerdict.BLOCKED_MULTIPLE_PAPER_CANDIDATES.value,
            "chosen_candidate": None,
            "suppressed_candidates": signal_candidates,
            "paper_candidate_count": len(paper_candidates),
            "primary_blocker": "Track B permits at most one paper-eligible strategy candidate per cycle.",
            "required_next_action": "Add explicit arbitration before allowing a PAPER handoff.",
        }
    chosen = paper_candidates[0]
    suppressed = [candidate for candidate in signal_candidates if candidate is not chosen]
    return {
        "strategy_arbitration_verdict": TrackBStrategyRegistryVerdict.READY.value,
        "chosen_candidate": chosen,
        "suppressed_candidates": suppressed,
        "paper_candidate_count": 1,
        "primary_blocker": None,
        "required_next_action": "Exactly one registered strategy paper candidate is available; PAPER submit still requires explicit Track B flags.",
    }


def _missing_required_fields(event: Mapping[str, Any], required_paths: Sequence[str]) -> list[str]:
    return [path for path in required_paths if _value_at_path(event, path) is None]


def _value_at_path(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, Mapping):
            return None
        if part not in current:
            return None
        current = current.get(part)
    return current


def _upper(value: object) -> str:
    return str(value or "").strip().upper()


def _text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None
