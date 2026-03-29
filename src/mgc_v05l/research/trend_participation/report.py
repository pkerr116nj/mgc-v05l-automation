"""Promotion-style reporting for Active Trend Participation Engine research."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from .models import PerformanceSummary, VariantEvaluation


def build_report_payload(
    *,
    evaluations: Iterable[VariantEvaluation],
    data_summary: dict[str, Any],
    storage_manifest: dict[str, Any],
) -> dict[str, Any]:
    evaluation_rows = list(evaluations)
    sorted_rows = sorted(
        evaluation_rows,
        key=lambda row: (
            row.out_of_sample_metrics.expectancy_per_hour,
            row.out_of_sample_metrics.expectancy,
            row.out_of_sample_metrics.trades_per_day,
            row.out_of_sample_metrics.profit_factor,
        ),
        reverse=True,
    )
    shortlist = [_promotion_row(item) for item in sorted_rows if _survives_oos(item)]
    watchlist = [_promotion_row(item) for item in sorted_rows if not _survives_oos(item)]
    return {
        "module": "Active Trend Participation Engine",
        "objective": (
            "Interpretable, active intraday long/short trend participation using 5m structural decisions, 1m timing, "
            "controlled re-entry, and explicit lower-priority conflict handling."
        ),
        "project_structure": {
            "package_root": "src/mgc_v05l/research/trend_participation",
            "components": [
                "storage.py",
                "features.py",
                "patterns.py",
                "backtest.py",
                "conflict.py",
                "report.py",
                "engine.py",
                "setup_reset_tracking",
                "reentry_and_cooldown_state",
            ],
        },
        "data_schema": {
            "raw_bars": [
                "instrument",
                "timeframe",
                "start_ts",
                "end_ts",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "session_label",
                "session_segment",
                "source",
                "provenance",
                "trading_calendar",
            ],
            "features": [
                "decision_ts",
                "trend_state",
                "pullback_state",
                "expansion_state",
                "bar_anatomy",
                "momentum_persistence",
                "reference_state",
                "volatility_range_state",
                "mtf_agreement_state",
                "regime_bucket",
                "volatility_bucket",
                "direction_bias",
                "atp_bias_state",
                "atp_bias_score",
                "atp_bias_reasons",
                "atp_long_bias_blockers",
                "atp_short_bias_blockers",
                "atp_fast_ema",
                "atp_slow_ema",
                "atp_slow_ema_slope_norm",
                "atp_session_vwap",
                "atp_directional_persistence_score",
                "atp_trend_extension_norm",
                "atp_pullback_state",
                "atp_pullback_envelope_state",
                "atp_pullback_reason",
                "atp_pullback_depth_points",
                "atp_pullback_depth_score",
                "atp_pullback_violence_score",
                "atp_pullback_min_reset_depth",
                "atp_pullback_standard_depth",
                "atp_pullback_stretched_depth",
                "atp_pullback_disqualify_depth",
                "atp_pullback_retracement_ratio",
                "atp_countertrend_velocity_norm",
                "atp_countertrend_range_expansion",
                "atp_structure_damage",
                "atp_reference_displacement",
            ],
            "signals": [
                "decision_id",
                "variant_id",
                "family",
                "side",
                "decision_ts",
                "conflict_outcome",
                "live_eligible",
                "shadow_only",
                "block_reason",
                "setup_signature",
                "setup_state_signature",
                "setup_quality_score",
                "setup_quality_bucket",
            ],
            "phase2_entry_states": [
                "decision_ts",
                "family_name",
                "bias_state",
                "pullback_state",
                "continuation_trigger_state",
                "entry_state",
                "blocker_codes",
                "primary_blocker",
                "raw_candidate",
                "trigger_confirmed",
                "entry_eligible",
                "session_allowed",
                "warmup_complete",
                "runtime_ready",
                "position_flat",
                "one_position_rule_clear",
                "setup_signature",
                "setup_state_signature",
                "setup_quality_score",
                "setup_quality_bucket",
            ],
            "phase3_timing_states": [
                "decision_ts",
                "family_name",
                "context_entry_state",
                "timing_state",
                "vwap_price_quality_state",
                "blocker_codes",
                "primary_blocker",
                "setup_armed",
                "timing_confirmed",
                "executable_entry",
                "invalidated_before_entry",
                "setup_armed_but_not_executable",
                "entry_executed",
                "timing_bar_ts",
                "entry_ts",
                "entry_price",
            ],
            "trades": [
                "variant_id",
                "instrument",
                "decision_id",
                "entry_ts",
                "exit_ts",
                "entry_price",
                "exit_price",
                "pnl_points",
                "gross_pnl_cash",
                "pnl_cash",
                "fees_paid",
                "slippage_cost",
                "mfe_points",
                "mae_points",
                "exit_reason",
                "is_reentry",
                "reentry_type",
                "stopout",
                "hold_minutes",
                "setup_quality_bucket",
            ],
        },
        "data_summary": data_summary,
        "storage_manifest": storage_manifest,
        "candidate_shortlist": shortlist,
        "watchlist": watchlist,
        "conflict_framework": {
            "priority_rule": "Active Trend Participation Engine yields to higher-priority live strategies whenever overlap exists.",
            "outcomes": [
                "no_conflict",
                "agreement",
                "soft_conflict",
                "hard_conflict_cooldown",
            ],
            "shadow_logging": "Blocked signals remain in the signal store as shadow observations for post-trade evaluation.",
        },
        "promotion_report_format": {
            "required_metrics": [
                "total_trades",
                "trades_per_day",
                "long_vs_short_trade_counts",
                "expectancy_per_trade",
                "expectancy_per_unit_time",
                "profit_factor",
                "max_drawdown",
                "average_hold_time",
                "stopout_rate",
                "reentry_performance",
                "session_breakdown",
                "regime_breakdown",
                "parameter_stability",
                "net_performance_after_costs",
            ],
            "admission_rule": (
                "Healthy activity is a feature, not a defect. Variants survive when that activity stays net positive after "
                "realistic costs, remains positive out of sample, and nearby settings remain stable."
            ),
        },
    }


def write_report_files(
    *,
    json_path: Path,
    markdown_path: Path,
    payload: dict[str, Any],
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def _promotion_row(evaluation: VariantEvaluation) -> dict[str, Any]:
    return {
        "variant_id": evaluation.variant.variant_id,
        "family": evaluation.variant.family,
        "side": evaluation.variant.side,
        "strictness": evaluation.variant.strictness,
        "description": evaluation.variant.description,
        "shadow_metrics": _metrics_payload(evaluation.shadow_metrics),
        "live_metrics": _metrics_payload(evaluation.live_metrics),
        "out_of_sample_metrics": _metrics_payload(evaluation.out_of_sample_metrics),
        "conflict_breakdown": evaluation.conflict_breakdown,
        "parameter_stability": evaluation.parameter_stability,
        "robustness_notes": list(evaluation.robustness_notes),
        "recommended_for_shadow": _survives_oos(evaluation),
    }


def _metrics_payload(metrics: PerformanceSummary) -> dict[str, Any]:
    return {
        "total_trades": metrics.trade_count,
        "active_days": metrics.active_days,
        "trades_per_day": round(metrics.trades_per_day, 4),
        "expectancy_per_trade": round(metrics.expectancy, 4),
        "expectancy_per_hour": round(metrics.expectancy_per_hour, 4),
        "profit_factor": round(metrics.profit_factor, 4),
        "max_drawdown": round(metrics.max_drawdown, 4),
        "win_rate": round(metrics.win_rate, 4),
        "avg_hold_minutes": round(metrics.avg_hold_minutes, 4),
        "stopout_rate": round(metrics.stopout_rate, 4),
        "reentry_trade_count": metrics.reentry_trade_count,
        "reentry_expectancy": round(metrics.reentry_expectancy, 4),
        "net_pnl_cash": round(metrics.net_pnl_cash, 4),
        "gross_pnl_before_cost": round(metrics.gross_pnl_before_cost, 4),
        "total_fees": round(metrics.total_fees, 4),
        "total_slippage_cost": round(metrics.total_slippage_cost, 4),
        "long_trade_count": metrics.long_trade_count,
        "short_trade_count": metrics.short_trade_count,
        "by_session": metrics.by_session,
        "by_regime": metrics.by_regime,
        "by_volatility": metrics.by_volatility,
    }


def _survives_oos(evaluation: VariantEvaluation) -> bool:
    metrics = evaluation.out_of_sample_metrics
    return (
        metrics.trade_count >= 3
        and metrics.expectancy > 0.0
        and metrics.expectancy_per_hour > 0.0
        and metrics.profit_factor > 1.0
        and evaluation.parameter_stability >= 0.5
        and metrics.trades_per_day >= 0.5
    )


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Active Trend Participation Engine",
        "",
        payload["objective"],
        "",
        "## Shortlist",
    ]
    shortlist = payload.get("candidate_shortlist") or []
    if not shortlist:
        lines.append("- No candidates survived the current out-of-sample gate.")
    else:
        for row in shortlist:
            metrics = row["out_of_sample_metrics"]
            lines.append(
                f"- `{row['variant_id']}`: expectancy/trade={metrics['expectancy_per_trade']}, "
                f"expectancy/hour={metrics['expectancy_per_hour']}, pf={metrics['profit_factor']}, "
                f"trades={metrics['total_trades']}, trades/day={metrics['trades_per_day']}, "
                f"reentry_exp={metrics['reentry_expectancy']}, stability={row['parameter_stability']}"
            )
    lines.extend(
        [
            "",
            "## Storage",
            "- Historical bars are designed for Parquet storage.",
            "- Research views are designed for DuckDB registration.",
            "- Raw bars, features, signals, and trades each have independent durable datasets.",
            "",
            "## Conflict Policy",
            "- Higher-priority strategy wins.",
            "- Agreement still logs a shadow signal but is not live-eligible.",
            "- Soft conflicts and hard cooldowns remain shadow-only.",
        ]
    )
    return "\n".join(lines) + "\n"
