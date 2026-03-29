"""Phase 5 diagnostic and salvage review for the Active Trend Participation Engine."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from .backtest import backtest_decisions_with_audit
from .models import ConflictOutcome, HigherPrioritySignal, PatternVariant, SignalDecision
from .phase4 import (
    PHASE3_BEST_LONG_VARIANT_ID,
    RollingWindow,
    _bucket_breakdown,
    _metrics_payload,
    _run_window,
    _shared_1m_coverage,
    _window_payload,
    build_rolling_windows,
)
@dataclass(frozen=True)
class CandidateSpec:
    candidate_id: str
    description: str
    source_kind: str
    side: str
    variant_id: str | None = None
    quality_buckets: frozenset[str] | None = None
    baseline_name: str | None = None


def run_phase5_review(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    instruments: tuple[str, ...] = ("MES", "MNQ"),
    higher_priority_signals: Iterable[HigherPrioritySignal] = (),
    window_days: int = 7,
) -> dict[str, Path]:
    output_root = output_dir.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    shared_start, shared_end = _shared_1m_coverage(sqlite_path=source_sqlite_path, instruments=instruments)
    windows = build_rolling_windows(shared_start=shared_start, shared_end=shared_end, window_days=window_days)
    window_runs = [
        _run_window(
            source_sqlite_path=source_sqlite_path,
            instruments=instruments,
            start_ts=window.start_ts,
            end_ts=window.end_ts,
            higher_priority_signals=higher_priority_signals,
        )
        for window in windows
    ]

    candidate_specs = _candidate_specs()
    candidate_reports: dict[str, dict[str, Any]] = {}
    candidate_trade_map: dict[str, list[Any]] = {}
    for spec in candidate_specs:
        report, trades = _evaluate_candidate(spec=spec, windows=windows, window_runs=window_runs)
        candidate_reports[spec.candidate_id] = report
        candidate_trade_map[spec.candidate_id] = trades

    long_quality_ids = [
        "long_unrestricted",
        "long_medium_only",
        "long_medium_high",
    ]
    short_quality_ids = [
        "short_active_unrestricted",
        "short_active_high_only",
        "short_aggressive_unrestricted",
        "short_aggressive_high_only",
    ]
    best_long_id = _best_candidate_id(candidate_reports, candidate_ids=long_quality_ids)
    best_short_id = _best_candidate_id(candidate_reports, candidate_ids=short_quality_ids)
    best_long = candidate_reports[best_long_id]
    best_short = candidate_reports[best_short_id]

    simple_baseline_ids = [
        "baseline_simple_pullback_long",
        "baseline_simple_pullback_short",
        "baseline_vwap_continuation_long",
        "baseline_vwap_continuation_short",
    ]
    simple_baselines = {candidate_id: candidate_reports[candidate_id] for candidate_id in simple_baseline_ids}

    combined_trades = _combine_candidate_trades(
        long_trades=candidate_trade_map[best_long_id],
        short_trades=candidate_trade_map[best_short_id],
    )
    recommendation = _recommendation(
        best_long=best_long,
        best_short=best_short,
        simple_baselines=simple_baselines,
    )

    payload = {
        "module": "Active Trend Participation Engine",
        "phase": "phase5",
        "objective": (
            "Determine whether a durable high-quality sub-engine exists inside the current framework, or whether the "
            "current strategy family should be treated as research-only and deprioritized."
        ),
        "review_windows": [_window_payload(window) for window in windows],
        "quality_gated_robustness_report": {
            "long_candidates": {candidate_id: candidate_reports[candidate_id] for candidate_id in long_quality_ids},
            "short_candidates": {candidate_id: candidate_reports[candidate_id] for candidate_id in short_quality_ids},
            "best_long_candidate_id": best_long_id,
            "best_short_candidate_id": best_short_id,
        },
        "regime_session_decomposition": {
            "best_long": _decomposition_payload(best_long),
            "best_short": _decomposition_payload(best_short),
        },
        "cost_fragility_summary": {
            candidate_id: candidate_reports[candidate_id]["cost_fragility"]
            for candidate_id in long_quality_ids + short_quality_ids
        },
        "simple_baseline_comparison": {
            "baselines": simple_baselines,
            "best_long_vs_baselines": _baseline_comparison(target=best_long, baselines=simple_baselines, side="LONG"),
            "best_short_vs_baselines": _baseline_comparison(target=best_short, baselines=simple_baselines, side="SHORT"),
        },
        "side_by_side_candidate_comparison": {
            "best_long": best_long,
            "best_short": best_short,
        },
        "combined_portfolio_view": {
            "aggregate_metrics": _metrics_payload(combined_trades),
            "note": "Combined as separate research lanes only; no unified live promotion implied.",
        },
        "recommendation": recommendation,
    }

    json_path = output_root / "phase5_review.json"
    markdown_path = output_root / "phase5_review.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {"json_path": json_path, "markdown_path": markdown_path}


def _candidate_specs() -> tuple[CandidateSpec, ...]:
    return (
        CandidateSpec(
            candidate_id="long_unrestricted",
            description="Phase 3 best long lane without quality gating.",
            source_kind="variant",
            side="LONG",
            variant_id=PHASE3_BEST_LONG_VARIANT_ID,
        ),
        CandidateSpec(
            candidate_id="long_medium_only",
            description="Best long lane restricted to MEDIUM-quality setups only.",
            source_kind="variant",
            side="LONG",
            variant_id=PHASE3_BEST_LONG_VARIANT_ID,
            quality_buckets=frozenset({"MEDIUM"}),
        ),
        CandidateSpec(
            candidate_id="long_medium_high",
            description="Best long lane restricted to MEDIUM/HIGH-quality setups only.",
            source_kind="variant",
            side="LONG",
            variant_id=PHASE3_BEST_LONG_VARIANT_ID,
            quality_buckets=frozenset({"MEDIUM", "HIGH"}),
        ),
        CandidateSpec(
            candidate_id="short_active_unrestricted",
            description="Failed-countertrend short active lane without quality gating.",
            source_kind="variant",
            side="SHORT",
            variant_id="trend_participation.failed_countertrend_resumption.short.active",
        ),
        CandidateSpec(
            candidate_id="short_active_high_only",
            description="Failed-countertrend short active lane restricted to HIGH-quality setups only.",
            source_kind="variant",
            side="SHORT",
            variant_id="trend_participation.failed_countertrend_resumption.short.active",
            quality_buckets=frozenset({"HIGH"}),
        ),
        CandidateSpec(
            candidate_id="short_aggressive_unrestricted",
            description="Failed-countertrend short aggressive lane without quality gating.",
            source_kind="variant",
            side="SHORT",
            variant_id="trend_participation.failed_countertrend_resumption.short.aggressive",
        ),
        CandidateSpec(
            candidate_id="short_aggressive_high_only",
            description="Failed-countertrend short aggressive lane restricted to HIGH-quality setups only.",
            source_kind="variant",
            side="SHORT",
            variant_id="trend_participation.failed_countertrend_resumption.short.aggressive",
            quality_buckets=frozenset({"HIGH"}),
        ),
        CandidateSpec(
            candidate_id="baseline_simple_pullback_long",
            description="Simple 5m trend-follow plus 1m pullback entry baseline.",
            source_kind="baseline",
            side="LONG",
            baseline_name="simple_pullback",
        ),
        CandidateSpec(
            candidate_id="baseline_simple_pullback_short",
            description="Simple 5m trend-follow plus 1m pullback entry baseline.",
            source_kind="baseline",
            side="SHORT",
            baseline_name="simple_pullback",
        ),
        CandidateSpec(
            candidate_id="baseline_vwap_continuation_long",
            description="Simple session-VWAP continuation baseline.",
            source_kind="baseline",
            side="LONG",
            baseline_name="vwap_continuation",
        ),
        CandidateSpec(
            candidate_id="baseline_vwap_continuation_short",
            description="Simple session-VWAP continuation baseline.",
            source_kind="baseline",
            side="SHORT",
            baseline_name="vwap_continuation",
        ),
    )


def _evaluate_candidate(
    *,
    spec: CandidateSpec,
    windows: list[RollingWindow],
    window_runs: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[Any]]:
    aggregate_trades = []
    rolling_rows = []
    for window, run in zip(windows, window_runs, strict=True):
        window_trades = _candidate_window_trades(spec=spec, run=run, window=window)
        aggregate_trades.extend(window_trades)
        rolling_rows.append(
            {
                "window": _window_payload(window),
                "metrics": _metrics_payload(window_trades),
            }
        )
    aggregate_trades = sorted(aggregate_trades, key=lambda item: item.decision_ts)
    aggregate_metrics = _metrics_payload(aggregate_trades)
    return {
        "candidate_id": spec.candidate_id,
        "description": spec.description,
        "side": spec.side,
        "aggregate_metrics": aggregate_metrics,
        "rolling_window_metrics": rolling_rows,
        "quality_bucket_breakdown": _bucket_breakdown(aggregate_trades, key_name="setup_quality_bucket"),
        "session_breakdown": _bucket_breakdown(aggregate_trades, key_name="session_segment"),
        "volatility_breakdown": _bucket_breakdown(aggregate_trades, key_name="volatility_bucket"),
        "regime_breakdown": _bucket_breakdown(aggregate_trades, key_name="regime_bucket"),
        "cost_fragility": _cost_fragility_summary(aggregate_trades),
    }, aggregate_trades


def _candidate_window_trades(*, spec: CandidateSpec, run: dict[str, Any], window: RollingWindow) -> list[Any]:
    if spec.source_kind == "variant":
        variant = run["variants_by_id"][spec.variant_id]
        decisions = list(run["decisions_by_variant"].get(spec.variant_id, []))
        if spec.quality_buckets is not None:
            decisions = [decision for decision in decisions if decision.setup_quality_bucket in spec.quality_buckets]
        trades, _ = backtest_decisions_with_audit(
            decisions=decisions,
            bars_1m=run["bars_1m"],
            variants_by_id={spec.variant_id: variant},
            point_values=run["point_values"],
            include_shadow_only=False,
        )
        return sorted(trades, key=lambda item: item.decision_ts)

    decisions, variant = _baseline_decisions(spec=spec, run=run, window=window)
    trades, _ = backtest_decisions_with_audit(
        decisions=decisions,
        bars_1m=run["bars_1m"],
        variants_by_id={variant.variant_id: variant},
        point_values=run["point_values"],
        include_shadow_only=False,
    )
    return sorted(trades, key=lambda item: item.decision_ts)


def _baseline_decisions(*, spec: CandidateSpec, run: dict[str, Any], window: RollingWindow) -> tuple[list[SignalDecision], PatternVariant]:
    variant = PatternVariant(
        variant_id=f"baseline.{spec.baseline_name}.{spec.side.lower()}",
        family=spec.baseline_name or "baseline",
        side=spec.side,
        strictness="baseline",
        description=spec.description,
        entry_window_bars_1m=5,
        max_hold_bars_1m=18,
        stop_atr_multiple=0.85,
        target_r_multiple=1.3,
        local_cooldown_bars_1m=2,
        reset_window_bars_5m=1,
        allow_reentry=False,
        reentry_policy="structural_only",
        trigger_reclaim_band_multiple=0.08,
        notes=("simple_baseline",),
    )
    vwap_map = _session_vwap_map(bars_1m=run["bars_1m"])
    decisions = []
    for feature in run["features"]:
        if spec.baseline_name == "simple_pullback":
            if not _simple_pullback_match(feature=feature, side=spec.side):
                continue
        elif not _vwap_continuation_match(feature=feature, side=spec.side, vwap_map=vwap_map):
            continue
        quality_bucket = "MEDIUM" if feature.pullback_state in {"SHALLOW", "MODERATE"} else "LOW"
        decisions.append(
            SignalDecision(
                decision_id=f"{feature.instrument}|{variant.variant_id}|{feature.decision_ts.isoformat()}",
                instrument=feature.instrument,
                variant_id=variant.variant_id,
                family=variant.family,
                side=variant.side,
                strictness=variant.strictness,
                decision_ts=feature.decision_ts,
                session_date=feature.session_date,
                session_segment=feature.session_segment,
                regime_bucket=feature.regime_bucket,
                volatility_bucket=feature.volatility_bucket,
                conflict_outcome=ConflictOutcome.NO_CONFLICT,
                live_eligible=True,
                shadow_only=False,
                block_reason=None,
                decision_bar_high=feature.high,
                decision_bar_low=feature.low,
                decision_bar_close=feature.close,
                decision_bar_open=feature.open,
                average_range=feature.average_range,
                setup_signature=f"{variant.family}|{variant.side}|{feature.session_segment}|{feature.trend_state}|{feature.pullback_state}",
                setup_state_signature=(
                    f"{feature.trend_state}|{feature.pullback_state}|{feature.expansion_state}|"
                    f"{feature.bar_anatomy}|{feature.reference_state}|{feature.mtf_agreement_state}"
                ),
                setup_quality_score=1.5 if quality_bucket == "MEDIUM" else 1.0,
                setup_quality_bucket=quality_bucket,
                feature_snapshot={
                    "trend_state": feature.trend_state,
                    "pullback_state": feature.pullback_state,
                    "expansion_state": feature.expansion_state,
                    "bar_anatomy": feature.bar_anatomy,
                    "reference_state": feature.reference_state,
                    "mtf_agreement_state": feature.mtf_agreement_state,
                },
            )
        )
    return decisions, variant


def _simple_pullback_match(*, feature: Any, side: str) -> bool:
    if side == "LONG":
        return (
            feature.trend_state in {"UP", "STRONG_UP"}
            and feature.mtf_agreement_state == "ALIGNED_UP"
            and feature.pullback_state in {"SHALLOW", "MODERATE"}
            and feature.bar_anatomy in {"LOWER_REJECTION", "BULL_IMPULSE", "BALANCED"}
        )
    return (
        feature.trend_state in {"DOWN", "STRONG_DOWN"}
        and feature.mtf_agreement_state == "ALIGNED_DOWN"
        and feature.pullback_state in {"SHALLOW", "MODERATE"}
        and feature.bar_anatomy in {"UPPER_REJECTION", "BEAR_IMPULSE", "BALANCED"}
    )


def _vwap_continuation_match(*, feature: Any, side: str, vwap_map: dict[tuple[str, str, str], float]) -> bool:
    session_key = (feature.instrument, feature.session_date.isoformat(), feature.session_segment)
    session_vwap = vwap_map.get(session_key)
    if session_vwap is None:
        return False
    if side == "LONG":
        return (
            feature.trend_state in {"UP", "STRONG_UP"}
            and feature.mtf_agreement_state == "ALIGNED_UP"
            and feature.close >= session_vwap
            and feature.pullback_state in {"SHALLOW", "MODERATE"}
        )
    return (
        feature.trend_state in {"DOWN", "STRONG_DOWN"}
        and feature.mtf_agreement_state == "ALIGNED_DOWN"
        and feature.close <= session_vwap
        and feature.pullback_state in {"SHALLOW", "MODERATE"}
    )


def _session_vwap_map(*, bars_1m: list[Any]) -> dict[tuple[str, str, str], float]:
    cumulative: dict[tuple[str, str, str], tuple[float, float]] = {}
    vwap_by_key: dict[tuple[str, str, str], float] = {}
    for bar in sorted(bars_1m, key=lambda item: (item.instrument, item.end_ts)):
        local_date = bar.end_ts.astimezone().date().isoformat()
        key = (bar.instrument, local_date, bar.session_segment)
        typical_price = (bar.high + bar.low + bar.close) / 3.0
        pv_sum, volume_sum = cumulative.get(key, (0.0, 0.0))
        pv_sum += typical_price * max(bar.volume, 1)
        volume_sum += max(bar.volume, 1)
        cumulative[key] = (pv_sum, volume_sum)
        vwap_by_key[key] = pv_sum / max(volume_sum, 1.0)
    return vwap_by_key


def _cost_fragility_summary(trades: list[Any]) -> dict[str, Any]:
    aggregate = _pre_post_cost_payload(trades)
    by_bucket = {}
    for bucket, breakdown in _group_trades(trades, key_func=lambda trade: trade.setup_quality_bucket).items():
        by_bucket[bucket] = _pre_post_cost_payload(breakdown)
    return {
        "aggregate": aggregate,
        "by_quality_bucket": by_bucket,
        "diagnosis": _fragility_diagnosis(aggregate),
    }


def _pre_post_cost_payload(trades: list[Any]) -> dict[str, Any]:
    if not trades:
        return {
            "trade_count": 0,
            "pre_cost_expectancy": 0.0,
            "post_cost_expectancy": 0.0,
            "cost_drag_per_trade": 0.0,
        }
    gross_total = sum(trade.gross_pnl_cash for trade in trades)
    net_total = sum(trade.pnl_cash for trade in trades)
    trade_count = len(trades)
    pre_cost_expectancy = gross_total / trade_count
    post_cost_expectancy = net_total / trade_count
    return {
        "trade_count": trade_count,
        "pre_cost_expectancy": round(pre_cost_expectancy, 4),
        "post_cost_expectancy": round(post_cost_expectancy, 4),
        "cost_drag_per_trade": round(pre_cost_expectancy - post_cost_expectancy, 4),
    }


def _fragility_diagnosis(payload: dict[str, Any]) -> str:
    if payload["trade_count"] == 0:
        return "no_trades"
    if payload["pre_cost_expectancy"] > 0.0 and payload["post_cost_expectancy"] <= 0.0:
        return "execution_fragile"
    if payload["pre_cost_expectancy"] <= 0.0 and payload["post_cost_expectancy"] <= 0.0:
        return "raw_signal_weakness"
    return "cost_resilient"


def _group_trades(trades: list[Any], *, key_func: Callable[[Any], str]) -> dict[str, list[Any]]:
    grouped: dict[str, list[Any]] = {}
    for trade in trades:
        grouped.setdefault(str(key_func(trade)), []).append(trade)
    return grouped


def _best_candidate_id(candidate_reports: dict[str, dict[str, Any]], *, candidate_ids: list[str]) -> str:
    rows = [candidate_reports[candidate_id] for candidate_id in candidate_ids]
    rows.sort(
        key=lambda row: (
            row["aggregate_metrics"]["net_pnl_cash"],
            row["aggregate_metrics"]["profit_factor"],
            row["aggregate_metrics"]["expectancy_per_trade"],
        ),
        reverse=True,
    )
    return rows[0]["candidate_id"]


def _decomposition_payload(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": report["candidate_id"],
        "session_breakdown": report["session_breakdown"],
        "volatility_breakdown": report["volatility_breakdown"],
        "regime_breakdown": report["regime_breakdown"],
    }


def _baseline_comparison(*, target: dict[str, Any], baselines: dict[str, dict[str, Any]], side: str) -> dict[str, Any]:
    relevant = {
        candidate_id: report
        for candidate_id, report in baselines.items()
        if report["side"] == side
    }
    comparisons = {}
    target_metrics = target["aggregate_metrics"]
    for candidate_id, report in relevant.items():
        baseline_metrics = report["aggregate_metrics"]
        comparisons[candidate_id] = {
            "baseline_metrics": baseline_metrics,
            "delta_vs_target": {
                "expectancy_per_trade": round(target_metrics["expectancy_per_trade"] - baseline_metrics["expectancy_per_trade"], 4),
                "profit_factor": round(target_metrics["profit_factor"] - baseline_metrics["profit_factor"], 4),
                "net_pnl_cash": round(target_metrics["net_pnl_cash"] - baseline_metrics["net_pnl_cash"], 4),
                "max_drawdown": round(target_metrics["max_drawdown"] - baseline_metrics["max_drawdown"], 4),
            },
        }
    return comparisons


def _combine_candidate_trades(*, long_trades: list[Any], short_trades: list[Any]) -> list[Any]:
    return sorted(long_trades + short_trades, key=lambda item: item.decision_ts)


def _recommendation(
    *,
    best_long: dict[str, Any],
    best_short: dict[str, Any],
    simple_baselines: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    long_metrics = best_long["aggregate_metrics"]
    short_metrics = best_short["aggregate_metrics"]
    if long_metrics["profit_factor"] > 1.0 and long_metrics["net_pnl_cash"] > 0.0 and short_metrics["profit_factor"] <= 1.0:
        return {
            "decision": "a) continue long lane only",
            "reason": "A durable long-only salvage candidate exists while the short side remains below promotion quality.",
        }
    if short_metrics["profit_factor"] > 1.0 and short_metrics["net_pnl_cash"] > 0.0 and long_metrics["profit_factor"] <= 1.0:
        return {
            "decision": "b) continue short lane only",
            "reason": "A durable short-only salvage candidate exists while the long side remains below promotion quality.",
        }
    if long_metrics["profit_factor"] > 1.0 and short_metrics["profit_factor"] > 1.0:
        return {
            "decision": "c) continue both as separate research tracks",
            "reason": "Both sides show enough standalone viability to justify continued separate-track development.",
        }
    return {
        "decision": "d) freeze/deprioritize this strategy family pending new feature work",
        "reason": (
            "The quality-gated salvage variants and simple baselines do not produce a durable full-span candidate. "
            "Treat the current framework as research-only until new features or execution assumptions materially change the edge."
        ),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    long_best = payload["quality_gated_robustness_report"]["best_long_candidate_id"]
    short_best = payload["quality_gated_robustness_report"]["best_short_candidate_id"]
    long_report = payload["quality_gated_robustness_report"]["long_candidates"][long_best]
    short_report = payload["quality_gated_robustness_report"]["short_candidates"][short_best]
    lines = [
        "# Active Trend Participation Engine Phase 5",
        "",
        "## Quality-Gated Salvage",
        f"- Best long salvage candidate: `{long_best}`",
        f"- Long net after costs: {long_report['aggregate_metrics']['net_pnl_cash']}",
        f"- Long profit factor: {long_report['aggregate_metrics']['profit_factor']}",
        f"- Best short salvage candidate: `{short_best}`",
        f"- Short net after costs: {short_report['aggregate_metrics']['net_pnl_cash']}",
        f"- Short profit factor: {short_report['aggregate_metrics']['profit_factor']}",
        "",
        "## Recommendation",
        f"- {payload['recommendation']['decision']}: {payload['recommendation']['reason']}",
    ]
    return "\n".join(lines) + "\n"
