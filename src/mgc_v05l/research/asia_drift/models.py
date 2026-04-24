"""Research-only models for Asia Drift v1 Phase 1."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AsiaDriftSessionScopeTag:
    instrument: str
    timeframe: str
    bar_end_ts: datetime
    local_session_date: date
    asia_drift_session_id: str
    session_anchor_ts: datetime
    latest_entry_ts: datetime
    mandatory_exit_ts: datetime
    session_time_label: str
    subphase: str
    in_scope: bool
    entry_window_open: bool
    session_timeout: bool
    anchor_observed: bool
    scope_provenance: str


@dataclass(frozen=True)
class DriftAssessment:
    calibration_profile: str
    dominant_direction: str
    regime: str
    long_score: float
    short_score: float
    long_strength: str
    short_strength: str
    score_gap: float
    warmup_complete: bool
    chop_veto: bool
    post_spike_instability: bool
    extension_elevated: bool
    long_reasons: tuple[str, ...]
    short_reasons: tuple[str, ...]


@dataclass(frozen=True)
class PullbackAssessment:
    calibration_profile: str
    direction: str
    state: str
    reason: str | None
    veto_category: str | None
    depth_points: float
    depth_atr: float
    depth_fraction: float
    duration_bars: int
    speed: float
    severity: float
    expansion_ratio: float
    fast_pullback_class: str
    vwap_interaction: str
    structure_preserved: bool
    structure_break: bool
    warning_flag: bool
    warning_reason: str | None
    warning_category: str | None
    hard_invalidation_candidate: bool
    too_extended: bool
    drift_leg_start: float
    drift_leg_extreme: float
    protected_swing_price: float
    retracement_38: float
    retracement_50: float
    retracement_62: float
    envelope_low: float
    envelope_high: float


@dataclass(frozen=True)
class AsiaDriftFeatureRow:
    calibration_profile: str
    instrument: str
    timeframe: str
    decision_ts: datetime
    local_session_date: date
    asia_drift_session_id: str
    session_bar_index: int
    session_time_label: str
    subphase: str
    in_scope: bool
    entry_window_open: bool
    session_timeout: bool
    scope_provenance: str
    anchor_observed: bool
    open: float
    high: float
    low: float
    close: float
    volume: int
    range_points: float
    session_open: float
    session_vwap: float
    atr: float
    fast_ema: float
    slow_ema: float
    slow_ema_slope: float
    session_displacement_atr: float
    signed_session_displacement_long: float
    signed_session_displacement_short: float
    signed_vwap_displacement_long: float
    signed_vwap_displacement_short: float
    bars_since_last_drift_impulse: int
    regime_persistence_score: float
    regime_persistence_label: str
    recovery_score: float
    recovery_label: str
    renewed_signed_vwap_displacement: float
    slope_recovery_score: float
    close_location_recovery_score: float
    fresh_drift_impulse: bool
    countertrend_extension_failed: bool
    compression_followed_by_drift_expansion: bool
    slope_3_long: float
    slope_6_long: float
    slope_12_long: float
    slope_3_short: float
    slope_6_short: float
    slope_12_short: float
    slope_combo_long: float
    slope_combo_short: float
    efficiency_ratio_12: float
    close_location: float
    close_location_persistence_long: float
    close_location_persistence_short: float
    bar_overlap_ratio_8: float
    reversal_frequency_12: float
    directional_persistence_8: float
    local_realized_volatility: float
    realized_volatility_ratio: float
    upside_extension_atr: float
    downside_extension_atr: float
    long_drift_score: float
    short_drift_score: float
    long_drift_strength: str
    short_drift_strength: str
    dominant_direction: str
    regime: str
    score_gap: float
    chop_veto: bool
    post_spike_instability: bool
    extension_elevated: bool
    drift_long_reasons: tuple[str, ...]
    drift_short_reasons: tuple[str, ...]
    pullback_direction: str
    pullback_state: str
    pullback_reason: str | None
    pullback_veto_category: str | None
    pullback_depth_points: float
    pullback_depth_atr: float
    pullback_depth_fraction: float
    pullback_duration_bars: int
    pullback_speed: float
    pullback_severity: float
    pullback_expansion_ratio: float
    fast_pullback_class: str
    pullback_vwap_interaction: str
    pullback_structure_preserved: bool
    pullback_structure_break: bool
    pullback_warning_flag: bool
    pullback_warning_reason: str | None
    pullback_warning_category: str | None
    pullback_hard_invalidation_candidate: bool
    pullback_too_extended: bool
    drift_leg_start: float
    drift_leg_extreme: float
    protected_swing_price: float
    retracement_38: float
    retracement_50: float
    retracement_62: float
    envelope_low: float
    envelope_high: float
    hypothetical_entry_ready: bool
    thesis_invalidated_flag: bool
    feature_version: str = "asia_drift_v1_phase1"


@dataclass(frozen=True)
class AsiaDriftStateRow:
    calibration_profile: str
    instrument: str
    timeframe: str
    decision_ts: datetime
    asia_drift_session_id: str
    session_bar_index: int
    state: str
    previous_state: str | None
    transition_reason: str
    at_risk_reason: str | None
    bars_in_at_risk_state: int
    resolution_tag: str | None
    regime: str
    dominant_direction: str
    drift_strength: str
    regime_persistence_score: float
    pullback_state: str
    entry_window_open: bool
    in_scope: bool
    thesis_invalidated: bool
    hypothetical_entry_ready: bool


@dataclass(frozen=True)
class AsiaDriftSessionSummary:
    calibration_profile: str
    instrument: str
    timeframe: str
    asia_drift_session_id: str
    local_session_date: date
    bar_count: int
    in_scope_bar_count: int
    candidate_direction: str
    max_long_drift_score: float
    max_short_drift_score: float
    max_drift_strength: str
    entry_ready_bar_count: int
    at_risk_bar_count: int
    recovered_bar_count: int
    requalified_bar_count: int
    invalidated_bar_count: int
    state_counts: dict[str, int]
    regime_counts: dict[str, int]
    pullback_counts: dict[str, int]
    chop_veto_bar_count: int
    post_spike_bar_count: int
    promising_session: bool
    first_candidate_ts: datetime | None
    first_entry_ready_ts: datetime | None
    first_invalidated_ts: datetime | None


@dataclass(frozen=True)
class AsiaDriftPhase1Artifacts:
    root_dir: Path
    summary_json_path: Path
    summary_markdown_path: Path
    feature_rows_path: Path
    state_rows_path: Path
    session_summaries_path: Path
    candidate_manifest_path: Path
    storage_manifest_path: Path


@dataclass(frozen=True)
class AsiaDriftCalibrationProfile:
    name: str
    description: str
    pullback_speed_warning: float
    pullback_speed_disqualify: float
    pullback_depth_fraction_warning: float
    pullback_depth_fraction_disqualify: float
    pullback_depth_atr_warning: float
    pullback_depth_atr_disqualify: float
    pullback_expansion_warning: float
    pullback_expansion_disqualify: float
    normal_depth_fraction_max: float
    normal_depth_atr_max: float
    normal_speed_max: float
    disqualifying_confirmation_bars: int
    regime_loss_confirmation_bars: int
    at_risk_persistence_threshold: float
    invalidation_persistence_threshold: float
    drift_collapse_confirmation_bars: int
    vwap_reclaim_confirmation_bars: int
    ema_failure_confirmation_bars: int
    structure_break_confirmation_bars: int
    recovery_score_threshold: float
    requalification_score_threshold: float
    max_recovery_bars: int


@dataclass(frozen=True)
class AsiaDriftEntrySetup:
    setup_id: str
    calibration_profile: str
    instrument: str
    timeframe: str
    asia_drift_session_id: str
    local_session_date: date
    direction: str
    regime: str
    drift_strength: str
    armed_ts: datetime
    armed_subphase: str
    armed_session_bar_index: int
    armed_transition_reason: str
    limit_expiry_ts: datetime | None
    confirmation_expiry_ts: datetime | None
    session_timeout_ts: datetime
    latest_entry_ts: datetime
    entry_zone_low: float
    entry_zone_high: float
    entry_limit_price: float | None
    entry_zone_valid: bool
    pullback_pivot_price: float
    protected_swing_price: float
    drift_leg_extreme: float
    atr: float
    scope_provenance: str
    feature_version: str


@dataclass(frozen=True)
class AsiaDriftEntryEvaluation:
    evaluation_id: str
    setup_id: str
    calibration_profile: str
    instrument: str
    asia_drift_session_id: str
    entry_model: str
    direction: str
    status: str
    accepted: bool
    prerequisites_met: bool
    armed_ts: datetime
    evaluation_start_ts: datetime | None
    evaluation_end_ts: datetime | None
    entry_ts: datetime | None
    entry_price: float | None
    candidate_entry_price: float | None
    bars_waited: int
    cancellation_reason: str | None
    reason_tags: tuple[str, ...]
    continuation_reasserted_after_reject: bool
    missed_favorable_excursion_points: float
    missed_favorable_excursion_r: float
    scope_provenance: str
    feature_version: str


@dataclass(frozen=True)
class AsiaDriftExitProfile:
    name: str
    profit_target_r: float
    max_hold_bars: int
    giveback_activation_r: float
    giveback_fraction: float
    vwap_giveback_activation_r: float
    vwap_giveback_fraction: float


@dataclass(frozen=True)
class AsiaDriftTradeRecord:
    trade_id: str
    setup_id: str
    evaluation_id: str
    calibration_profile: str
    instrument: str
    asia_drift_session_id: str
    entry_model: str
    exit_profile: str
    direction: str
    entry_ts: datetime
    entry_price: float
    stop_price: float
    target_price: float
    initial_risk_points: float
    exit_ts: datetime
    exit_price: float
    exit_reason: str
    exit_hardness: str
    bars_held: int
    mfe_points: float
    mae_points: float
    mfe_r: float
    mae_r: float
    gross_r: float
    peak_open_profit_r: float
    continuation_achieved: bool
    continuation_threshold_r: float
    time_to_follow_through_bars: int | None
    time_to_failure_bars: int | None
    post_exit_followthrough_r: float
    post_exit_deterioration_r: float
    vwap_warning_count: int
    scope_provenance: str
    feature_version: str


@dataclass(frozen=True)
class AsiaDriftPhase2Artifacts:
    root_dir: Path
    phase1_root_dir: Path
    entry_setups_path: Path
    entry_evaluations_path: Path
    trade_records_path: Path
    diagnostics_json_path: Path
    summary_json_path: Path
    summary_markdown_path: Path
    storage_manifest_path: Path


def row_to_flat_dict(row: Any) -> dict[str, Any]:
    payload = dict(vars(row))
    for key, value in list(payload.items()):
        if isinstance(value, datetime):
            payload[key] = value.isoformat()
        elif isinstance(value, date):
            payload[key] = value.isoformat()
        elif isinstance(value, Path):
            payload[key] = str(value)
    return payload
