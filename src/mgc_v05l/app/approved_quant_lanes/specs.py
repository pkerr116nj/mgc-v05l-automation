"""Canonical approved quant lane specifications."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SOURCE_OF_TRUTH_PATH = Path("outputs/reports/quant_futures_operator_baseline/operator_baseline_package.md")


@dataclass(frozen=True)
class ApprovedQuantLaneSpec:
    lane_id: str
    lane_name: str
    variant_id: str
    family: str
    direction: str
    symbols: tuple[str, ...]
    allowed_sessions: tuple[str, ...]
    excluded_sessions: tuple[str, ...]
    permanent_exclusions: tuple[str, ...]
    behavioral_thesis: str
    indispensable_conditions: tuple[str, ...]
    optional_refinements: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]
    fragility_notes: tuple[str, ...]
    monitoring_metrics: tuple[str, ...]
    hold_bars: int
    stop_r: float
    target_r: float | None
    exit_style: str
    params: dict[str, float]
    gating_mode: str
    approval_source: str
    structural_invalidation_r: float | None = None
    review_owner: str = "quant+operator"


BREAKOUT_METALS_US_UNKNOWN_CONTINUATION = ApprovedQuantLaneSpec(
    lane_id="phase2c.breakout.metals_only.us_unknown.baseline",
    lane_name="breakout_metals_us_unknown_continuation",
    variant_id="phase2c.breakout.metals_only.us_unknown.baseline",
    family="breakout_continuation",
    direction="LONG",
    symbols=("GC", "MGC", "HG", "PL"),
    allowed_sessions=("US", "UNKNOWN"),
    excluded_sessions=("ASIA", "LONDON"),
    permanent_exclusions=("6J", "LONDON", "broad_fx_metals_breakout", "cross_universe_breakout"),
    behavioral_thesis=(
        "Metals continuation after compression resolves upward and the breakout is accepted while the broader regime is already aligned up."
    ),
    indispensable_conditions=(
        "metals_only_membership",
        "us_unknown_session_restriction",
        "regime_up",
        "compression_60",
        "compression_5",
        "breakout_up",
        "close_pos_strength",
        "positive_slope_60",
    ),
    optional_refinements=(
        "no_soft_continuation_score_required",
        "no_extra_symbol_overlay",
    ),
    invalidation_conditions=(
        "two_consecutive_rolling_30_trade_windows_negative_net_020",
        "persistent_negative_net_025",
        "one_symbol_pnl_story",
        "unexplained_unknown_session_dominance",
    ),
    fragility_notes=(
        "breakout_up_threshold_near_030",
        "close_pos_threshold_near_068",
        "compression_cutoffs_may_shift_with_volatility",
        "unknown_session_should_be_monitored_for_label_drift",
    ),
    monitoring_metrics=(
        "signal_count_by_symbol",
        "signal_count_by_session",
        "realized_expectancy_net_020",
        "realized_expectancy_net_025",
        "rolling_30_trade_expectancy",
        "rolling_hit_rate",
        "rolling_avg_win",
        "rolling_avg_loss",
        "symbol_breadth_cost_020",
        "symbol_concentration",
        "slippage_drift",
    ),
    hold_bars=24,
    stop_r=1.0,
    target_r=None,
    exit_style="time_stop_only",
    params={
        "compression_60_max": 0.90,
        "compression_5_max": 0.82,
        "breakout_min": 0.30,
        "close_pos_min": 0.68,
        "slope_60_min": 0.20,
    },
    gating_mode="hard",
    approval_source=str(SOURCE_OF_TRUTH_PATH),
)


FAILED_MOVE_NO_US_REVERSAL_SHORT = ApprovedQuantLaneSpec(
    lane_id="phase2c.failed.core4_plus_qc.no_us.baseline",
    lane_name="failed_move_no_us_reversal_short",
    variant_id="phase2c.failed.core4_plus_qc.no_us.baseline",
    family="failed_move_reversal",
    direction="SHORT",
    symbols=("CL", "ES", "6E", "6J", "QC"),
    allowed_sessions=("ASIA", "LONDON", "UNKNOWN"),
    excluded_sessions=("US",),
    permanent_exclusions=("US", "ZT", "soft_reversal_score_required", "broad_failed_move_family"),
    behavioral_thesis=(
        "Failed upside excursions reverse after extension when a breakout cannot hold, and the unwind continues better outside the US session."
    ),
    indispensable_conditions=(
        "no_us_session_restriction",
        "failed_breakout_short",
        "dist_240_extension",
        "close_pos_low",
        "body_r_conviction",
    ),
    optional_refinements=(
        "no_soft_reversal_score_required",
    ),
    invalidation_conditions=(
        "two_consecutive_rolling_windows_negative_net_020",
        "persistent_negative_net_025",
        "core4_depends_on_qc",
        "one_symbol_or_one_session_cluster_only",
    ),
    fragility_notes=(
        "dist_240_threshold_near_105",
        "body_r_threshold_near_028",
        "close_pos_near_lower_third_cutoff",
        "reversal_bar_fill_quality_can_degrade",
    ),
    monitoring_metrics=(
        "signal_count_by_symbol",
        "signal_count_by_session",
        "realized_expectancy_net_020",
        "realized_expectancy_net_025",
        "rolling_25_trade_expectancy",
        "rolling_40_trade_expectancy",
        "rolling_hit_rate",
        "rolling_avg_win",
        "rolling_avg_loss",
        "core_vs_qc_attribution",
        "session_attribution",
        "slippage_drift",
    ),
    hold_bars=12,
    stop_r=1.0,
    target_r=1.5,
    exit_style="target_stop_time_plus_structure",
    structural_invalidation_r=0.45,
    params={
        "dist_240_extreme": 1.05,
        "close_pos_max": 0.34,
        "body_r_min": 0.28,
    },
    gating_mode="ungated",
    approval_source=str(SOURCE_OF_TRUTH_PATH),
)


def approved_quant_lane_specs() -> tuple[ApprovedQuantLaneSpec, ...]:
    return (
        BREAKOUT_METALS_US_UNKNOWN_CONTINUATION,
        FAILED_MOVE_NO_US_REVERSAL_SHORT,
    )


def get_approved_quant_lane_spec(lane_id: str) -> ApprovedQuantLaneSpec:
    for spec in approved_quant_lane_specs():
        if spec.lane_id == lane_id:
            return spec
    raise KeyError(f"Unknown approved quant lane: {lane_id}")


def approved_quant_lane_scope_payload(spec: ApprovedQuantLaneSpec) -> dict[str, Any]:
    return {
        "lane_id": spec.lane_id,
        "lane_name": spec.lane_name,
        "variant_id": spec.variant_id,
        "family": spec.family,
        "direction": spec.direction,
        "symbols": list(spec.symbols),
        "allowed_sessions": list(spec.allowed_sessions),
        "excluded_sessions": list(spec.excluded_sessions),
        "permanent_exclusions": list(spec.permanent_exclusions),
        "indispensable_conditions": list(spec.indispensable_conditions),
        "optional_refinements": list(spec.optional_refinements),
        "hold_bars": spec.hold_bars,
        "stop_r": spec.stop_r,
        "target_r": spec.target_r,
        "exit_style": spec.exit_style,
        "structural_invalidation_r": spec.structural_invalidation_r,
        "params": spec.params,
        "gating_mode": spec.gating_mode,
    }


def approved_quant_lane_scope_fingerprint(spec: ApprovedQuantLaneSpec) -> str:
    payload = approved_quant_lane_scope_payload(spec)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
