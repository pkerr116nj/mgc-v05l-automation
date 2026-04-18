"""Active Trend Participation Engine research models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any


class ConflictOutcome(str, Enum):
    NO_CONFLICT = "no_conflict"
    AGREEMENT = "agreement"
    SOFT_CONFLICT = "soft_conflict"
    HARD_CONFLICT_COOLDOWN = "hard_conflict_cooldown"


@dataclass(frozen=True)
class ResearchBar:
    instrument: str
    timeframe: str
    start_ts: datetime
    end_ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    session_label: str
    session_segment: str
    source: str = "sqlite"
    provenance: str = "historical_import"
    trading_calendar: str = "CME_INDEX_FUTURES"

    @property
    def range_points(self) -> float:
        return max(self.high - self.low, 1e-9)

    @property
    def body_points(self) -> float:
        return abs(self.close - self.open)


@dataclass(frozen=True)
class DataQualityIssue:
    instrument: str
    timeframe: str
    issue_type: str
    severity: str
    message: str
    bar_end_ts: datetime | None = None


@dataclass(frozen=True)
class FeatureState:
    instrument: str
    timeframe: str
    decision_ts: datetime
    session_date: date
    session_label: str
    session_segment: str
    open: float
    high: float
    low: float
    close: float
    range_points: float
    average_range: float
    slope_norm: float
    pullback_depth_norm: float
    expansion_ratio: float
    one_minute_slope_norm: float
    distance_from_recent_high_norm: float
    distance_from_recent_low_norm: float
    distance_from_session_open_norm: float
    trend_state: str
    pullback_state: str
    expansion_state: str
    bar_anatomy: str
    momentum_persistence: str
    reference_state: str
    volatility_range_state: str
    mtf_agreement_state: str
    regime_bucket: str
    volatility_bucket: str
    direction_bias: str
    atp_bias_state: str
    atp_bias_score: int
    atp_bias_reasons: tuple[str, ...]
    atp_long_bias_blockers: tuple[str, ...]
    atp_short_bias_blockers: tuple[str, ...]
    atp_fast_ema: float
    atp_slow_ema: float
    atp_slow_ema_slope_norm: float
    atp_session_vwap: float
    atp_directional_persistence_score: int
    atp_trend_extension_norm: float
    atp_pullback_state: str
    atp_pullback_envelope_state: str
    atp_pullback_reason: str | None
    atp_pullback_depth_points: float
    atp_pullback_depth_score: float
    atp_pullback_violence_score: float
    atp_pullback_min_reset_depth: float
    atp_pullback_standard_depth: float
    atp_pullback_stretched_depth: float
    atp_pullback_disqualify_depth: float
    atp_pullback_retracement_ratio: float
    atp_countertrend_velocity_norm: float
    atp_countertrend_range_expansion: float
    atp_structure_damage: bool
    atp_reference_displacement: float


@dataclass(frozen=True)
class PatternVariant:
    variant_id: str
    family: str
    side: str
    strictness: str
    description: str
    entry_window_bars_1m: int
    max_hold_bars_1m: int
    stop_atr_multiple: float
    target_r_multiple: float | None
    local_cooldown_bars_1m: int = 0
    reset_window_bars_5m: int = 1
    allow_reentry: bool = True
    reentry_policy: str = "all"
    trigger_reclaim_band_multiple: float = 0.0
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class HigherPrioritySignal:
    instrument: str
    side: str
    start_ts: datetime
    end_ts: datetime | None
    reason: str
    cooldown: bool = False


@dataclass(frozen=True)
class SignalDecision:
    decision_id: str
    instrument: str
    variant_id: str
    family: str
    side: str
    strictness: str
    decision_ts: datetime
    session_date: date
    session_segment: str
    regime_bucket: str
    volatility_bucket: str
    conflict_outcome: ConflictOutcome
    live_eligible: bool
    shadow_only: bool
    block_reason: str | None
    decision_bar_high: float
    decision_bar_low: float
    decision_bar_close: float
    decision_bar_open: float
    average_range: float
    setup_signature: str
    setup_state_signature: str
    setup_quality_score: float
    setup_quality_bucket: str
    feature_snapshot: dict[str, str]


@dataclass(frozen=True)
class AtpEntryState:
    instrument: str
    decision_ts: datetime
    session_date: date
    session_segment: str
    family_name: str
    bias_state: str
    pullback_state: str
    continuation_trigger_state: str
    entry_state: str
    blocker_codes: tuple[str, ...]
    primary_blocker: str | None
    raw_candidate: bool
    trigger_confirmed: bool
    entry_eligible: bool
    session_allowed: bool
    warmup_complete: bool
    runtime_ready: bool
    position_flat: bool
    one_position_rule_clear: bool
    setup_signature: str
    setup_state_signature: str
    setup_quality_score: float
    setup_quality_bucket: str
    feature_snapshot: dict[str, Any]
    side: str = "LONG"


@dataclass(frozen=True)
class AtpTimingState:
    instrument: str
    decision_ts: datetime
    session_date: date
    session_segment: str
    family_name: str
    context_entry_state: str
    timing_state: str
    vwap_price_quality_state: str
    blocker_codes: tuple[str, ...]
    primary_blocker: str | None
    setup_armed: bool
    timing_confirmed: bool
    executable_entry: bool
    invalidated_before_entry: bool
    setup_armed_but_not_executable: bool
    entry_executed: bool
    timing_bar_ts: datetime | None
    entry_ts: datetime | None
    entry_price: float | None
    feature_snapshot: dict[str, Any]
    side: str = "LONG"


@dataclass(frozen=True)
class TradeRecord:
    instrument: str
    variant_id: str
    family: str
    side: str
    live_eligible: bool
    shadow_only: bool
    conflict_outcome: ConflictOutcome
    decision_id: str
    decision_ts: datetime
    entry_ts: datetime
    exit_ts: datetime
    entry_price: float
    exit_price: float
    stop_price: float
    target_price: float | None
    pnl_points: float
    gross_pnl_cash: float
    pnl_cash: float
    fees_paid: float
    slippage_cost: float
    mfe_points: float
    mae_points: float
    bars_held_1m: int
    hold_minutes: float
    exit_reason: str
    is_reentry: bool
    reentry_type: str
    stopout: bool
    setup_signature: str
    setup_quality_bucket: str
    session_segment: str
    regime_bucket: str
    volatility_bucket: str


@dataclass(frozen=True)
class VariantExecutionAudit:
    instrument: str
    variant_id: str
    family: str
    side: str
    structural_candidates: int
    blocked_cooldown: int
    blocked_reset: int
    blocked_reentry_policy: int
    trigger_missed: int
    trigger_survived: int
    executed: int


@dataclass(frozen=True)
class PerformanceSummary:
    trade_count: int
    active_days: int
    trades_per_day: float
    expectancy: float
    expectancy_per_hour: float
    profit_factor: float
    max_drawdown: float
    win_rate: float
    avg_win: float
    avg_loss: float
    avg_hold_minutes: float
    stopout_rate: float
    reentry_trade_count: int
    reentry_expectancy: float
    net_pnl_cash: float
    gross_profit: float
    gross_loss: float
    gross_pnl_before_cost: float
    total_fees: float
    total_slippage_cost: float
    long_trade_count: int
    short_trade_count: int
    by_session: dict[str, dict[str, float]]
    by_regime: dict[str, dict[str, float]]
    by_volatility: dict[str, dict[str, float]]


@dataclass(frozen=True)
class WalkForwardFoldResult:
    fold_id: str
    train_dates: tuple[str, ...]
    test_dates: tuple[str, ...]
    selected_variants: tuple[str, ...]
    train_rankings: tuple[dict[str, Any], ...]
    oos_variant_metrics: dict[str, PerformanceSummary]


@dataclass(frozen=True)
class VariantEvaluation:
    variant: PatternVariant
    shadow_metrics: PerformanceSummary
    live_metrics: PerformanceSummary
    out_of_sample_metrics: PerformanceSummary
    fold_results: tuple[WalkForwardFoldResult, ...]
    conflict_breakdown: dict[str, int]
    parameter_stability: float
    robustness_notes: tuple[str, ...]


@dataclass(frozen=True)
class TrendParticipationArtifacts:
    root_dir: Path
    report_json_path: Path
    report_markdown_path: Path
    storage_manifest_path: Path
    report: dict[str, Any]
    storage_manifest: dict[str, Any] = field(default_factory=dict)
    phase1_diagnostics_json_path: Path | None = None
    phase1_diagnostics_markdown_path: Path | None = None
    phase2_diagnostics_json_path: Path | None = None
    phase2_diagnostics_markdown_path: Path | None = None
    phase3_diagnostics_json_path: Path | None = None
    phase3_diagnostics_markdown_path: Path | None = None
    performance_validation_json_path: Path | None = None
    performance_validation_markdown_path: Path | None = None
