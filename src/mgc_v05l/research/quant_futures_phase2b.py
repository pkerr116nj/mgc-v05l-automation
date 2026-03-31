"""Phase 2B specialization and post-cost ranking for futures quant research."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .quant_futures import (
    CandidateSpec,
    StrategyResearchArtifacts,
    _FrameSeries,
    _avg,
    _build_candidate_specs,
    _clip,
    _expectancy,
    _max_drawdown,
    _median,
    _profit_factor,
    _resolve_exit,
    _sharpe_proxy,
    _top_positive_share,
    _walk_forward_positive_ratio,
    _win_rate,
)
from .quant_futures_phase2a import _build_symbol_store

BREAKOUT_TARGET = "breakout_acceptance.long.gated.tight"
FAILED_TARGET = "failed_move_reversal.short.ungated.tight"

BREAKOUT_CLUSTER = ("6J", "GC", "MGC", "HG", "PL")
FAILED_CLUSTER = ("CL", "6E", "ES", "QC", "ZT", "6J")

ALL_SESSIONS = ("ASIA", "LONDON", "US", "UNKNOWN")
BREAKOUT_SESSION_FILTERS = {
    "all": ALL_SESSIONS,
    "no_london": ("ASIA", "US", "UNKNOWN"),
    "asia_unknown": ("ASIA", "UNKNOWN"),
    "us_unknown": ("US", "UNKNOWN"),
}
FAILED_SESSION_FILTERS = {
    "all": ALL_SESSIONS,
    "no_us": ("ASIA", "LONDON", "UNKNOWN"),
    "london_unknown": ("LONDON", "UNKNOWN"),
    "london_only": ("LONDON",),
}
MEAN_REVERSION_SESSION_FILTERS = {
    "no_us": ("ASIA", "LONDON", "UNKNOWN"),
    "us_only": ("US",),
    "london_unknown": ("LONDON", "UNKNOWN"),
}


@dataclass(frozen=True)
class Phase2BVariantSpec:
    variant_id: str
    lane: str
    family: str
    direction: str
    cluster_name: str
    symbols: tuple[str, ...]
    session_name: str
    allowed_sessions: tuple[str, ...]
    params: dict[str, float]
    hold_bars: int
    stop_r: float
    target_r: float
    gating_mode: str
    score_field: str | None
    score_threshold: float | None
    recommendation_hint: str


@dataclass(frozen=True)
class Phase2BTrade:
    variant_id: str
    lane: str
    family: str
    direction: str
    symbol: str
    entry_ts: str
    exit_ts: str
    entry_session: str
    r_multiple: float
    holding_bars: int
    feature_snapshot: dict[str, float | str | bool]


def run_quant_futures_phase2b(
    *,
    database_path: str | Path,
    execution_timeframe: str = "5m",
    symbols: tuple[str, ...] | None = None,
    output_dir: str | Path | None = None,
) -> StrategyResearchArtifacts:
    resolved_database_path = Path(database_path).resolve()
    resolved_output_dir = Path(output_dir or Path.cwd() / "outputs" / "reports" / "quant_futures_phase2b").resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    selected_symbols = tuple(symbols) if symbols else tuple(sorted(set(BREAKOUT_CLUSTER + FAILED_CLUSTER)))
    symbol_store = _build_symbol_store(
        database_path=resolved_database_path,
        execution_timeframe=execution_timeframe,
        symbols=selected_symbols,
    )
    variants = _build_phase2b_variants()
    evaluated = [_evaluate_variant(variant=variant, symbol_store=symbol_store) for variant in variants]
    _attach_threshold_sensitivity(evaluated)
    for row in evaluated:
        row["summary"]["realism_score"] = _phase2b_rank_score(row["summary"])

    evaluated.sort(key=lambda row: (-row["summary"]["realism_score"], -row["summary"]["cost_expectancy_r_010"], row["variant"]["variant_id"]))

    breakout_rows = [row for row in evaluated if row["variant"]["lane"] == "specialized_breakout"]
    failed_rows = [row for row in evaluated if row["variant"]["lane"] == "specialized_failed_reversal"]
    mean_reversion_rows = [row for row in evaluated if row["variant"]["lane"] == "short_horizon_mean_reversion"]

    breakout_best = breakout_rows[0]
    failed_best = failed_rows[0]
    mean_reversion_best = mean_reversion_rows[0]

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "database_path": str(resolved_database_path),
        "execution_timeframe": execution_timeframe,
        "symbols_tested": list(selected_symbols),
        "phase": "2B",
        "ranking_method": {
            "primary": "post-cost expectancy at 0.10R and post-cost positive symbol share",
            "secondary": "walk-forward stability, trade count, and session/instrument breadth",
            "penalties": "threshold sensitivity, narrow session concentration, concentration risk, drawdown, and small gross edge",
        },
        "specialized_rankings": {
            "overall_top": [_render_rank_row(row) for row in evaluated[:12]],
            "breakout_cluster": [_render_rank_row(row) for row in breakout_rows[:8]],
            "failed_reversal_cluster": [_render_rank_row(row) for row in failed_rows[:8]],
            "mean_reversion_lane": [_render_rank_row(row) for row in mean_reversion_rows[:8]],
        },
        "family_assessments": {
            "breakout_acceptance_long": _family_assessment(
                label="breakout_acceptance.long.gated.tight",
                rows=breakout_rows,
                best_row=breakout_best,
            ),
            "failed_move_reversal_short": _family_assessment(
                label="failed_move_reversal.short.ungated.tight",
                rows=failed_rows,
                best_row=failed_best,
            ),
            "short_horizon_mean_reversion": _family_assessment(
                label="short_horizon_mean_reversion",
                rows=mean_reversion_rows,
                best_row=mean_reversion_best,
            ),
        },
        "post_cost_results": {
            "breakout_best": _post_cost_snapshot(breakout_best),
            "failed_best": _post_cost_snapshot(failed_best),
            "mean_reversion_best": _post_cost_snapshot(mean_reversion_best),
        },
        "recommendations": _build_phase2b_recommendations(
            breakout_best=breakout_best,
            failed_best=failed_best,
            mean_reversion_best=mean_reversion_best,
        ),
    }

    json_path = resolved_output_dir / "quant_futures_phase2b_report.json"
    markdown_path = resolved_output_dir / "quant_futures_phase2b_report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_phase2b_markdown(report).strip() + "\n", encoding="utf-8")
    return StrategyResearchArtifacts(json_path=json_path, markdown_path=markdown_path, report=report)


def _build_phase2b_variants() -> list[Phase2BVariantSpec]:
    specs = {spec.candidate_id: spec for spec in _build_candidate_specs()}
    breakout_spec = specs[BREAKOUT_TARGET]
    failed_spec = specs[FAILED_TARGET]
    variants: list[Phase2BVariantSpec] = []

    for session_name, sessions in BREAKOUT_SESSION_FILTERS.items():
        variants.append(
            _variant_from_spec(
                spec=breakout_spec,
                variant_id=f"phase2b.breakout.long.{session_name}.baseline",
                lane="specialized_breakout",
                cluster_name="fx_metals_cluster",
                symbols=BREAKOUT_CLUSTER,
                session_name=session_name,
                allowed_sessions=sessions,
                gating_mode="hard",
                score_field=None,
                score_threshold=None,
                recommendation_hint="continue_cluster_session_split",
            )
        )
        for threshold in (0.56, 0.64):
            variants.append(
                _variant_from_spec(
                    spec=breakout_spec,
                    variant_id=f"phase2b.breakout.long.{session_name}.soft_{str(threshold).replace('.', '_')}",
                    lane="specialized_breakout",
                    cluster_name="fx_metals_cluster",
                    symbols=BREAKOUT_CLUSTER,
                    session_name=session_name,
                    allowed_sessions=sessions,
                    gating_mode="soft",
                    score_field="continuation_score_long",
                    score_threshold=threshold,
                    recommendation_hint="continue_cluster_session_split",
                )
            )

    for session_name, sessions in FAILED_SESSION_FILTERS.items():
        variants.append(
            _variant_from_spec(
                spec=failed_spec,
                variant_id=f"phase2b.failed.short.{session_name}.baseline",
                lane="specialized_failed_reversal",
                cluster_name="macro_reversal_cluster",
                symbols=FAILED_CLUSTER,
                session_name=session_name,
                allowed_sessions=sessions,
                gating_mode="ungated",
                score_field=None,
                score_threshold=None,
                recommendation_hint="simplify_then_split",
            )
        )
        for threshold in (0.30, 0.40):
            variants.append(
                _variant_from_spec(
                    spec=failed_spec,
                    variant_id=f"phase2b.failed.short.{session_name}.soft_{str(threshold).replace('.', '_')}",
                    lane="specialized_failed_reversal",
                    cluster_name="macro_reversal_cluster",
                    symbols=FAILED_CLUSTER,
                    session_name=session_name,
                    allowed_sessions=sessions,
                    gating_mode="soft",
                    score_field="reversal_score_short",
                    score_threshold=threshold,
                    recommendation_hint="simplify_then_split",
                )
            )
        variants.append(
            _variant_from_spec(
                spec=failed_spec,
                variant_id=f"phase2b.failed.short.{session_name}.core_simple",
                lane="specialized_failed_reversal",
                cluster_name="macro_reversal_cluster",
                symbols=FAILED_CLUSTER,
                session_name=session_name,
                allowed_sessions=sessions,
                gating_mode="ungated",
                score_field=None,
                score_threshold=None,
                params_override={"dist_240_extreme": 0.0, "body_r_min": 0.18, "close_pos_min": 0.58},
                recommendation_hint="simplify_then_split",
            )
        )

    mean_reversion_symbols = tuple(sorted(set(BREAKOUT_CLUSTER + FAILED_CLUSTER)))
    for session_name, sessions in MEAN_REVERSION_SESSION_FILTERS.items():
        variants.extend(
            [
                Phase2BVariantSpec(
                    variant_id=f"phase2b.meanrev.stretch.long.{session_name}",
                    lane="short_horizon_mean_reversion",
                    family="stretch_reversion",
                    direction="LONG",
                    cluster_name="rotational_cross_asset",
                    symbols=mean_reversion_symbols,
                    session_name=session_name,
                    allowed_sessions=sessions,
                    params={
                        "stretch_min": 1.15,
                        "abs_slope_240_max": 0.65,
                        "abs_slope_720_max": 0.55,
                        "eff_240_max": 0.58,
                        "close_pos_min": 0.55,
                        "trigger_delta_min": 0.02,
                    },
                    hold_bars=5,
                    stop_r=0.85,
                    target_r=0.95,
                    gating_mode="ungated",
                    score_field=None,
                    score_threshold=None,
                    recommendation_hint="keep_only_if_post_cost_positive",
                ),
                Phase2BVariantSpec(
                    variant_id=f"phase2b.meanrev.stretch.short.{session_name}",
                    lane="short_horizon_mean_reversion",
                    family="stretch_reversion",
                    direction="SHORT",
                    cluster_name="rotational_cross_asset",
                    symbols=mean_reversion_symbols,
                    session_name=session_name,
                    allowed_sessions=sessions,
                    params={
                        "stretch_min": 1.15,
                        "abs_slope_240_max": 0.65,
                        "abs_slope_720_max": 0.55,
                        "eff_240_max": 0.58,
                        "close_pos_min": 0.55,
                        "trigger_delta_min": 0.02,
                    },
                    hold_bars=5,
                    stop_r=0.85,
                    target_r=0.95,
                    gating_mode="ungated",
                    score_field=None,
                    score_threshold=None,
                    recommendation_hint="keep_only_if_post_cost_positive",
                ),
                Phase2BVariantSpec(
                    variant_id=f"phase2b.meanrev.failed_breakout.short.{session_name}",
                    lane="short_horizon_mean_reversion",
                    family="failed_breakout_reversion",
                    direction="SHORT",
                    cluster_name="rotational_cross_asset",
                    symbols=mean_reversion_symbols,
                    session_name=session_name,
                    allowed_sessions=sessions,
                    params={
                        "abs_slope_240_max": 0.75,
                        "abs_slope_720_max": 0.65,
                        "eff_240_max": 0.65,
                        "close_pos_min": 0.58,
                        "body_r_min": 0.12,
                    },
                    hold_bars=4,
                    stop_r=0.80,
                    target_r=0.85,
                    gating_mode="ungated",
                    score_field=None,
                    score_threshold=None,
                    recommendation_hint="keep_only_if_post_cost_positive",
                ),
                Phase2BVariantSpec(
                    variant_id=f"phase2b.meanrev.failed_breakout.long.{session_name}",
                    lane="short_horizon_mean_reversion",
                    family="failed_breakout_reversion",
                    direction="LONG",
                    cluster_name="rotational_cross_asset",
                    symbols=mean_reversion_symbols,
                    session_name=session_name,
                    allowed_sessions=sessions,
                    params={
                        "abs_slope_240_max": 0.75,
                        "abs_slope_720_max": 0.65,
                        "eff_240_max": 0.65,
                        "close_pos_min": 0.58,
                        "body_r_min": 0.12,
                    },
                    hold_bars=4,
                    stop_r=0.80,
                    target_r=0.85,
                    gating_mode="ungated",
                    score_field=None,
                    score_threshold=None,
                    recommendation_hint="keep_only_if_post_cost_positive",
                ),
            ]
        )
    return variants


def _variant_from_spec(
    *,
    spec: CandidateSpec,
    variant_id: str,
    lane: str,
    cluster_name: str,
    symbols: tuple[str, ...],
    session_name: str,
    allowed_sessions: tuple[str, ...],
    gating_mode: str,
    score_field: str | None,
    score_threshold: float | None,
    recommendation_hint: str,
    params_override: dict[str, float] | None = None,
) -> Phase2BVariantSpec:
    params = dict(spec.params)
    if params_override:
        params.update(params_override)
    return Phase2BVariantSpec(
        variant_id=variant_id,
        lane=lane,
        family=spec.family,
        direction=spec.direction,
        cluster_name=cluster_name,
        symbols=symbols,
        session_name=session_name,
        allowed_sessions=allowed_sessions,
        params=params,
        hold_bars=spec.hold_bars,
        stop_r=spec.stop_r,
        target_r=spec.target_r,
        gating_mode=gating_mode,
        score_field=score_field,
        score_threshold=score_threshold,
        recommendation_hint=recommendation_hint,
    )


def _evaluate_variant(
    *,
    variant: Phase2BVariantSpec,
    symbol_store: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    trades: list[Phase2BTrade] = []
    for symbol in variant.symbols:
        payload = symbol_store.get(symbol)
        if payload is None:
            continue
        trades.extend(
            _simulate_variant(
                variant=variant,
                symbol=symbol,
                execution=payload["execution"],
                features=payload["features"],
            )
        )
    summary = _summarize_variant(variant=variant, trades=trades)
    return {"variant": asdict(variant), "trades": trades, "summary": summary}


def _simulate_variant(
    *,
    variant: Phase2BVariantSpec,
    symbol: str,
    execution: _FrameSeries,
    features: list[dict[str, Any]],
) -> list[Phase2BTrade]:
    trades: list[Phase2BTrade] = []
    next_available_index = 0
    for index, feature in enumerate(features):
        if index < next_available_index or index + 1 >= len(execution.bars):
            continue
        if not feature.get("ready"):
            continue
        if str(feature["session_label"]) not in variant.allowed_sessions:
            continue
        if not _variant_signal_matches(variant, feature):
            continue

        entry_index = index + 1
        entry_price = execution.opens[entry_index]
        risk = max(float(feature["risk_unit"]), 1e-6)
        stop_price = entry_price - variant.stop_r * risk if variant.direction == "LONG" else entry_price + variant.stop_r * risk
        target_price = entry_price + variant.target_r * risk if variant.direction == "LONG" else entry_price - variant.target_r * risk
        exit_index, exit_price, _ = _resolve_exit(
            direction=variant.direction,
            execution=execution,
            entry_index=entry_index,
            hold_bars=variant.hold_bars,
            stop_price=stop_price,
            target_price=target_price,
        )
        signed_move = exit_price - entry_price if variant.direction == "LONG" else entry_price - exit_price
        trades.append(
            Phase2BTrade(
                variant_id=variant.variant_id,
                lane=variant.lane,
                family=variant.family,
                direction=variant.direction,
                symbol=symbol,
                entry_ts=execution.timestamps[entry_index].isoformat(),
                exit_ts=execution.timestamps[exit_index].isoformat(),
                entry_session=str(feature["session_label"]),
                r_multiple=signed_move / risk,
                holding_bars=max(exit_index - entry_index + 1, 1),
                feature_snapshot=_phase2b_feature_snapshot(feature),
            )
        )
        next_available_index = exit_index + 1
    return trades


def _variant_signal_matches(variant: Phase2BVariantSpec, feature: dict[str, Any]) -> bool:
    if not _context_pass(variant, feature):
        return False
    params = variant.params

    if variant.family == "breakout_acceptance" and variant.direction == "LONG":
        return (
            feature["compression_60"] <= params["compression_60_max"]
            and feature["compression_5"] <= params["compression_5_max"]
            and feature["breakout_up"] >= params["breakout_min"]
            and feature["close_pos"] >= params["close_pos_min"]
            and feature["slope_60"] >= params["slope_60_min"]
        )

    if variant.family == "failed_move_reversal" and variant.direction == "SHORT":
        dist_requirement = params["dist_240_extreme"]
        dist_ok = True if dist_requirement <= 0.0 else feature["dist_240"] >= dist_requirement
        return (
            feature["failed_breakout_short"]
            and feature["close_pos"] <= 1.0 - params["close_pos_min"]
            and feature["body_r"] >= params["body_r_min"]
            and dist_ok
        )

    if variant.family == "stretch_reversion":
        weak_trend = (
            abs(feature["slope_240"]) <= params["abs_slope_240_max"]
            and abs(feature["slope_720"]) <= params["abs_slope_720_max"]
            and feature["eff_240"] <= params["eff_240_max"]
        )
        if not weak_trend:
            return False
        if variant.direction == "LONG":
            return (
                feature["dist_60"] <= -params["stretch_min"]
                and feature["prev_close_delta_r"] >= params["trigger_delta_min"]
                and feature["close_pos"] >= params["close_pos_min"]
            )
        return (
            feature["dist_60"] >= params["stretch_min"]
            and feature["prev_close_delta_r"] <= -params["trigger_delta_min"]
            and feature["close_pos"] <= 1.0 - params["close_pos_min"]
        )

    if variant.family == "failed_breakout_reversion":
        weak_trend = (
            abs(feature["slope_240"]) <= params["abs_slope_240_max"]
            and abs(feature["slope_720"]) <= params["abs_slope_720_max"]
            and feature["eff_240"] <= params["eff_240_max"]
        )
        if not weak_trend:
            return False
        if variant.direction == "SHORT":
            return (
                feature["failed_breakout_short"]
                and feature["close_pos"] <= 1.0 - params["close_pos_min"]
                and feature["body_r"] >= params["body_r_min"]
            )
        return (
            feature["failed_breakout_long"]
            and feature["close_pos"] >= params["close_pos_min"]
            and feature["body_r"] >= params["body_r_min"]
        )

    return False


def _context_pass(variant: Phase2BVariantSpec, feature: dict[str, Any]) -> bool:
    if variant.gating_mode == "ungated":
        return True
    if variant.gating_mode == "hard":
        if variant.family == "breakout_acceptance":
            return bool(feature["regime_up"])
        if variant.family == "failed_move_reversal":
            return bool(feature["extended_up"] or feature["regime_neutral"] or feature["regime_up"])
    if variant.gating_mode == "soft":
        if variant.score_field is None or variant.score_threshold is None:
            return True
        return float(feature[variant.score_field]) >= float(variant.score_threshold)
    return True


def _phase2b_feature_snapshot(feature: dict[str, Any]) -> dict[str, float | str | bool]:
    keys = (
        "session_label",
        "compression_60",
        "compression_5",
        "breakout_up",
        "close_pos",
        "slope_60",
        "slope_240",
        "slope_720",
        "dist_60",
        "dist_240",
        "body_r",
        "continuation_score_long",
        "reversal_score_short",
        "failed_breakout_short",
        "failed_breakout_long",
    )
    return {key: feature[key] for key in keys if key in feature}


def _summarize_variant(
    *,
    variant: Phase2BVariantSpec,
    trades: list[Phase2BTrade],
) -> dict[str, Any]:
    r_values = [trade.r_multiple for trade in trades]
    per_symbol_rows = []
    for symbol in variant.symbols:
        symbol_trades = [trade for trade in trades if trade.symbol == symbol]
        if not symbol_trades:
            continue
        values = [trade.r_multiple for trade in symbol_trades]
        adjusted = [value - 0.10 for value in values]
        per_symbol_rows.append(
            {
                "symbol": symbol,
                "trade_count": len(values),
                "expectancy_r": _expectancy(values),
                "cost_expectancy_r_010": _expectancy(adjusted),
                "profit_factor": _profit_factor(values),
                "walk_forward_positive_ratio": _walk_forward_positive_ratio(symbol_trades),  # type: ignore[arg-type]
            }
        )
    per_symbol_rows.sort(key=lambda row: (-row["cost_expectancy_r_010"], -row["expectancy_r"], row["symbol"]))

    per_session_rows = []
    for session in variant.allowed_sessions:
        session_trades = [trade for trade in trades if trade.entry_session == session]
        if not session_trades:
            continue
        values = [trade.r_multiple for trade in session_trades]
        per_session_rows.append(
            {
                "session": session,
                "trade_count": len(values),
                "share_of_trades": round(len(values) / float(max(len(trades), 1)), 6),
                "expectancy_r": _expectancy(values),
                "cost_expectancy_r_010": _expectancy([value - 0.10 for value in values]),
                "profit_factor": _profit_factor(values),
            }
        )
    per_session_rows.sort(key=lambda row: row["session"])

    positive_symbol_share = round(sum(1 for row in per_symbol_rows if row["expectancy_r"] > 0.0) / float(max(len(variant.symbols), 1)), 6)
    positive_symbol_share_cost = round(sum(1 for row in per_symbol_rows if row["cost_expectancy_r_010"] > 0.0) / float(max(len(variant.symbols), 1)), 6)
    concentration = _phase2b_concentration(r_values)
    cost_005 = _expectancy([value - 0.05 for value in r_values]) if r_values else 0.0
    cost_010 = _expectancy([value - 0.10 for value in r_values]) if r_values else 0.0
    dominant_session_share = max((row["share_of_trades"] for row in per_session_rows), default=0.0)
    return {
        "trade_count": len(r_values),
        "expectancy_r": _expectancy(r_values),
        "cost_expectancy_r_005": cost_005,
        "cost_expectancy_r_010": cost_010,
        "win_rate": _win_rate(r_values),
        "profit_factor": _profit_factor(r_values),
        "max_drawdown_r": _max_drawdown(r_values),
        "sharpe_proxy": _sharpe_proxy(r_values),
        "positive_symbol_share": positive_symbol_share,
        "positive_symbol_share_cost_010": positive_symbol_share_cost,
        "walk_forward_positive_ratio": _walk_forward_positive_ratio(trades),  # type: ignore[arg-type]
        "avg_holding_bars": _avg([trade.holding_bars for trade in trades]),
        "per_symbol": per_symbol_rows,
        "session_breakdown": per_session_rows,
        "concentration": concentration,
        "dominant_session_share": dominant_session_share,
        "threshold_sensitivity_penalty": 0.0,
        "gross_edge_penalty": round(_clip((0.05 - _expectancy(r_values)) / 0.05, 0.0, 1.0), 6) if r_values else 1.0,
        "viable_post_cost": bool(cost_010 > 0.0 and positive_symbol_share_cost >= 0.4 and _walk_forward_positive_ratio(trades) >= 0.6),  # type: ignore[arg-type]
    }


def _attach_threshold_sensitivity(evaluated: list[dict[str, Any]]) -> None:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in evaluated:
        variant = row["variant"]
        grouped[(variant["lane"], variant["cluster_name"], variant["session_name"])].append(row)
    for rows in grouped.values():
        cost_values = [row["summary"]["cost_expectancy_r_010"] for row in rows]
        span = max(cost_values) - min(cost_values) if cost_values else 0.0
        sign_flip = min(cost_values) < 0.0 < max(cost_values) if cost_values else False
        penalty = _clip(span / 0.08, 0.0, 1.0)
        if sign_flip:
            penalty = min(1.0, penalty + 0.20)
        for row in rows:
            row["summary"]["threshold_sensitivity_penalty"] = round(penalty, 6)


def _phase2b_rank_score(summary: dict[str, Any]) -> float:
    cost010_score = _clip((summary["cost_expectancy_r_010"] + 0.05) / 0.12, 0.0, 1.0)
    cost005_score = _clip((summary["cost_expectancy_r_005"] + 0.05) / 0.18, 0.0, 1.0)
    breadth_score = summary["positive_symbol_share_cost_010"]
    walk_score = summary["walk_forward_positive_ratio"]
    trade_count_score = _clip(summary["trade_count"] / 80.0, 0.0, 1.0)
    drawdown_penalty = _clip(summary["max_drawdown_r"] / 25.0, 0.0, 1.0)
    concentration_penalty = _clip(summary["concentration"]["top_3_share_of_total_r"] / 0.80, 0.0, 1.0)
    session_penalty = _clip((summary["dominant_session_share"] - 0.55) / 0.35, 0.0, 1.0)
    score = 100.0 * (
        0.34 * cost010_score
        + 0.18 * breadth_score
        + 0.16 * walk_score
        + 0.10 * cost005_score
        + 0.08 * trade_count_score
        - 0.06 * drawdown_penalty
        - 0.04 * concentration_penalty
        - 0.08 * summary["threshold_sensitivity_penalty"]
        - 0.04 * session_penalty
        - 0.08 * summary["gross_edge_penalty"]
    )
    return round(score, 4)


def _phase2b_concentration(r_values: list[float]) -> dict[str, Any]:
    ordered = sorted(r_values, reverse=True)
    total = sum(r_values)
    return {
        "top_1_share_of_total_r": round(sum(ordered[:1]) / total, 6) if total != 0.0 else 0.0,
        "top_3_share_of_total_r": round(sum(ordered[:3]) / total, 6) if total != 0.0 else 0.0,
        "top_5_share_of_total_r": round(sum(ordered[:5]) / total, 6) if total != 0.0 else 0.0,
        "returns_without_top_1_r": round(sum(ordered[1:]), 6) if len(ordered) > 1 else round(sum(ordered), 6),
        "top_positive_share": _top_positive_share(r_values, 3),
    }


def _render_rank_row(row: dict[str, Any]) -> dict[str, Any]:
    summary = row["summary"]
    variant = row["variant"]
    return {
        "variant_id": variant["variant_id"],
        "lane": variant["lane"],
        "cluster_name": variant["cluster_name"],
        "session_name": variant["session_name"],
        "family": variant["family"],
        "direction": variant["direction"],
        "realism_score": summary["realism_score"],
        "expectancy_r": summary["expectancy_r"],
        "cost_expectancy_r_010": summary["cost_expectancy_r_010"],
        "positive_symbol_share_cost_010": summary["positive_symbol_share_cost_010"],
        "walk_forward_positive_ratio": summary["walk_forward_positive_ratio"],
        "trade_count": summary["trade_count"],
        "viable_post_cost": summary["viable_post_cost"],
    }


def _family_assessment(*, label: str, rows: list[dict[str, Any]], best_row: dict[str, Any]) -> dict[str, Any]:
    summary = best_row["summary"]
    return {
        "label": label,
        "best_variant": {
            "variant": best_row["variant"],
            "summary": summary,
        },
        "top_variants": [_render_rank_row(row) for row in rows[:6]],
        "best_symbols": [row["symbol"] for row in summary["per_symbol"][:6]],
        "weak_symbols": [row["symbol"] for row in summary["per_symbol"][-4:]],
        "session_profile": summary["session_breakdown"],
        "post_cost_viable_after_narrowing": summary["viable_post_cost"],
        "recommendation": _family_recommendation(best_row),
    }


def _family_recommendation(row: dict[str, Any]) -> dict[str, Any]:
    variant = row["variant"]
    summary = row["summary"]
    action = "discard"
    reason = "The narrowed family still does not survive realistic execution assumptions."
    if summary["viable_post_cost"]:
        action = "keep"
        reason = "The narrowed family survives post-cost checks and merits continued refinement."
    elif summary["cost_expectancy_r_010"] <= 0.0 and summary["cost_expectancy_r_005"] > 0.0:
        action = "split"
        reason = "The family shows a small gross edge but still fails stricter cost assumptions, so it should be narrowed further by instrument and session."
    elif variant["lane"] == "short_horizon_mean_reversion" and summary["expectancy_r"] > 0.0:
        action = "keep_narrow_lane"
        reason = "The lane is not post-cost viable yet, but the gross edge is positive enough to justify narrow follow-up work."
    return {"action": action, "reason": reason}


def _post_cost_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    summary = row["summary"]
    return {
        "variant_id": row["variant"]["variant_id"],
        "expectancy_r": summary["expectancy_r"],
        "cost_expectancy_r_005": summary["cost_expectancy_r_005"],
        "cost_expectancy_r_010": summary["cost_expectancy_r_010"],
        "positive_symbol_share_cost_010": summary["positive_symbol_share_cost_010"],
        "walk_forward_positive_ratio": summary["walk_forward_positive_ratio"],
        "viable_post_cost": summary["viable_post_cost"],
    }


def _build_phase2b_recommendations(
    *,
    breakout_best: dict[str, Any],
    failed_best: dict[str, Any],
    mean_reversion_best: dict[str, Any],
) -> dict[str, Any]:
    return {
        "what_to_keep": [
            _family_recommendation(failed_best) if failed_best["summary"]["expectancy_r"] > 0.0 else None,
            _family_recommendation(breakout_best) if breakout_best["summary"]["expectancy_r"] > 0.0 else None,
            _family_recommendation(mean_reversion_best) if mean_reversion_best["summary"]["expectancy_r"] > 0.0 else None,
        ],
        "breakout": _family_recommendation(breakout_best),
        "failed_reversal": _family_recommendation(failed_best),
        "mean_reversion": _family_recommendation(mean_reversion_best),
        "clear_recommendation": (
            "Split the breakout and failed-reversal families further by cluster/session, and keep mean reversion only if you want a separate narrow lane rather than a promotion path."
            if mean_reversion_best["summary"]["expectancy_r"] > 0.0
            else "Keep splitting the two current families by cluster/session and discard the re-opened mean-reversion lane for now."
        ),
    }


def _render_phase2b_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Quant Futures Phase 2B",
        "",
        f"Execution timeframe: {report['execution_timeframe']}",
        f"Symbols tested: {', '.join(report['symbols_tested'])}",
        "",
        "## Specialized Rankings",
    ]
    for row in report["specialized_rankings"]["overall_top"][:10]:
        lines.append(
            f"- {row['variant_id']}: realism={round(row['realism_score'], 2)}, cost010={round(row['cost_expectancy_r_010'], 4)}R, "
            f"gross={round(row['expectancy_r'], 4)}R, breadth_post_cost={round(row['positive_symbol_share_cost_010'], 4)}, "
            f"walk={round(row['walk_forward_positive_ratio'], 4)}, viable_post_cost={row['viable_post_cost']}"
        )
    lines.extend(["", "## Family Results"])
    for key, value in report["family_assessments"].items():
        best = value["best_variant"]["summary"]
        lines.append(
            f"- {key}: best={value['best_variant']['variant']['variant_id']}, gross={round(best['expectancy_r'], 4)}R, "
            f"cost010={round(best['cost_expectancy_r_010'], 4)}R, breadth_post_cost={round(best['positive_symbol_share_cost_010'], 4)}, "
            f"recommendation={value['recommendation']['action']}"
        )
    lines.extend(["", "## Recommendations", f"- {report['recommendations']['clear_recommendation']}"])
    return "\n".join(lines)
