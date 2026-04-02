"""Phase 2C narrowed-lane specialization and robustness testing for futures quant research."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass, replace
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

BREAKOUT_FULL_CLUSTER = ("6J", "GC", "MGC", "HG", "PL")
BREAKOUT_METALS_CLUSTER = ("GC", "MGC", "HG", "PL")
FAILED_CORE_CLUSTER = ("CL", "ES", "6E", "6J")
FAILED_PLUS_QC_CLUSTER = ("CL", "ES", "6E", "6J", "QC")
FAILED_PLUS_ZT_CLUSTER = ("CL", "ES", "6E", "6J", "ZT")
FAILED_FULL_CLUSTER = ("CL", "ES", "6E", "6J", "QC", "ZT")
MEAN_REVERSION_SANDBOX = ("GC", "MGC", "HG", "PL", "CL", "ES")

ALL_SESSIONS = ("ASIA", "LONDON", "US", "UNKNOWN")
BREAKOUT_SESSION_FILTERS = {
    "all": ALL_SESSIONS,
    "us_unknown": ("US", "UNKNOWN"),
    "no_london": ("ASIA", "US", "UNKNOWN"),
}
FAILED_SESSION_FILTERS = {
    "all": ALL_SESSIONS,
    "no_us": ("ASIA", "LONDON", "UNKNOWN"),
    "london_unknown": ("LONDON", "UNKNOWN"),
}


@dataclass(frozen=True)
class Phase2CVariantSpec:
    variant_id: str
    role: str
    family: str
    direction: str
    lane_name: str
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
    require_compression: bool = True
    require_extension: bool = True
    require_failed_breakout: bool = True
    complexity_points: int = 4


@dataclass(frozen=True)
class Phase2CTrade:
    variant_id: str
    family: str
    direction: str
    symbol: str
    entry_ts: str
    exit_ts: str
    entry_session: str
    r_multiple: float
    holding_bars: int
    feature_snapshot: dict[str, float | str | bool]


def run_quant_futures_phase2c(
    *,
    database_path: str | Path,
    execution_timeframe: str = "5m",
    symbols: tuple[str, ...] | None = None,
    output_dir: str | Path | None = None,
) -> StrategyResearchArtifacts:
    resolved_database_path = Path(database_path).resolve()
    resolved_output_dir = Path(output_dir or Path.cwd() / "outputs" / "reports" / "quant_futures_phase2c").resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    selected_symbols = tuple(symbols) if symbols else tuple(
        sorted(
            set(
                BREAKOUT_FULL_CLUSTER
                + FAILED_FULL_CLUSTER
                + MEAN_REVERSION_SANDBOX
            )
        )
    )
    symbol_store = _build_symbol_store(
        database_path=resolved_database_path,
        execution_timeframe=execution_timeframe,
        symbols=selected_symbols,
    )
    variants = _build_phase2c_variants()
    evaluated = [_evaluate_variant(variant=variant, symbol_store=symbol_store) for variant in variants]
    rows_by_id = {row["variant"]["variant_id"]: row for row in evaluated}

    breakout_lane_rows = [row for row in evaluated if row["variant"]["family"] == "breakout_acceptance" and row["variant"]["role"] == "lane"]
    failed_lane_rows = [row for row in evaluated if row["variant"]["family"] == "failed_move_reversal" and row["variant"]["role"] == "lane"]
    mean_reversion_rows = [row for row in evaluated if row["variant"]["family"] == "mean_reversion_sandbox"]

    for row in breakout_lane_rows + failed_lane_rows:
        row["robustness"] = _build_robustness_suite(row=row, symbol_store=symbol_store)
        row["summary"]["phase2c_score"] = _phase2c_rank_score(
            summary=row["summary"],
            robustness=row["robustness"],
            variant=row["variant"],
        )
        row["promotion_shortlist"] = _promotion_shortlist_status(
            summary=row["summary"],
            robustness=row["robustness"],
            variant=row["variant"],
        )

    for row in mean_reversion_rows:
        row["summary"]["phase2c_score"] = _phase2c_sandbox_score(row["summary"])
        row["promotion_shortlist"] = False

    breakout_lane_rows.sort(key=lambda row: (-row["summary"]["phase2c_score"], -row["summary"]["cost_expectancy_r_015"], row["variant"]["variant_id"]))
    failed_lane_rows.sort(key=lambda row: (-row["summary"]["phase2c_score"], -row["summary"]["cost_expectancy_r_015"], row["variant"]["variant_id"]))
    mean_reversion_rows.sort(key=lambda row: (-row["summary"]["phase2c_score"], -row["summary"]["cost_expectancy_r_010"], row["variant"]["variant_id"]))

    breakout_best = breakout_lane_rows[0]
    failed_best = failed_lane_rows[0]
    mean_reversion_best = mean_reversion_rows[0] if mean_reversion_rows else None

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "database_path": str(resolved_database_path),
        "execution_timeframe": execution_timeframe,
        "symbols_tested": list(selected_symbols),
        "phase": "2C",
        "specialization_bias": "narrow local lanes only; no broadening",
        "narrowed_lane_results": {
            "breakout_acceptance_long": [_render_rank_row(row) for row in breakout_lane_rows],
            "failed_move_reversal_short": [_render_rank_row(row) for row in failed_lane_rows],
            "mean_reversion_sandbox": [_render_rank_row(row) for row in mean_reversion_rows],
        },
        "best_candidate_per_family": {
            "breakout_acceptance_long": _family_result(
                label="breakout_acceptance.long",
                best_row=breakout_best,
                rows=breakout_lane_rows,
                rows_by_id=rows_by_id,
            ),
            "failed_move_reversal_short": _family_result(
                label="failed_move_reversal.short",
                best_row=failed_best,
                rows=failed_lane_rows,
                rows_by_id=rows_by_id,
            ),
            "mean_reversion_sandbox": _mean_reversion_result(mean_reversion_best),
        },
        "promotion_shortlist_status": {
            "breakout_acceptance_long": breakout_best["promotion_shortlist"],
            "failed_move_reversal_short": failed_best["promotion_shortlist"],
            "mean_reversion_sandbox": False,
        },
        "permanent_discards": _build_permanent_discards(
            breakout_best=breakout_best,
            failed_best=failed_best,
            mean_reversion_best=mean_reversion_best,
        ),
        "overall_recommendation": _build_phase2c_recommendation(
            breakout_best=breakout_best,
            failed_best=failed_best,
            mean_reversion_best=mean_reversion_best,
        ),
    }

    json_path = resolved_output_dir / "quant_futures_phase2c_report.json"
    markdown_path = resolved_output_dir / "quant_futures_phase2c_report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_phase2c_markdown(report).strip() + "\n", encoding="utf-8")
    return StrategyResearchArtifacts(json_path=json_path, markdown_path=markdown_path, report=report)


def _build_phase2c_variants() -> list[Phase2CVariantSpec]:
    specs = {spec.candidate_id: spec for spec in _build_candidate_specs()}
    breakout_spec = specs[BREAKOUT_TARGET]
    failed_spec = specs[FAILED_TARGET]
    variants: list[Phase2CVariantSpec] = []

    breakout_lane_definitions = [
        ("fx_metals_full", BREAKOUT_FULL_CLUSTER),
        ("metals_only", BREAKOUT_METALS_CLUSTER),
    ]
    for cluster_name, cluster_symbols in breakout_lane_definitions:
        for session_name, sessions in BREAKOUT_SESSION_FILTERS.items():
            variants.append(
                _variant_from_spec(
                    spec=breakout_spec,
                    variant_id=f"phase2c.breakout.{cluster_name}.{session_name}.baseline",
                    role="lane",
                    lane_name="fx_metals_continuation",
                    cluster_name=cluster_name,
                    symbols=cluster_symbols,
                    session_name=session_name,
                    allowed_sessions=sessions,
                    gating_mode="hard",
                    score_field=None,
                    score_threshold=None,
                    complexity_points=4,
                )
            )
            variants.append(
                _variant_from_spec(
                    spec=breakout_spec,
                    variant_id=f"phase2c.breakout.{cluster_name}.{session_name}.soft_0_56",
                    role="lane",
                    lane_name="fx_metals_continuation",
                    cluster_name=cluster_name,
                    symbols=cluster_symbols,
                    session_name=session_name,
                    allowed_sessions=sessions,
                    gating_mode="soft",
                    score_field="continuation_score_long",
                    score_threshold=0.56,
                    complexity_points=5,
                )
            )

    for symbol in BREAKOUT_FULL_CLUSTER:
        variants.append(
            _variant_from_spec(
                spec=breakout_spec,
                variant_id=f"phase2c.breakout.instrument.{symbol.lower()}.us_unknown.soft_0_56",
                role="instrument",
                lane_name="fx_metals_continuation",
                cluster_name=symbol,
                symbols=(symbol,),
                session_name="us_unknown",
                allowed_sessions=BREAKOUT_SESSION_FILTERS["us_unknown"],
                gating_mode="soft",
                score_field="continuation_score_long",
                score_threshold=0.56,
                complexity_points=5,
            )
        )

    variants.append(
        _variant_from_spec(
            spec=breakout_spec,
            variant_id="phase2c.breakout.fx_metals_full.us_unknown.soft_0_56.no_compression",
            role="driver",
            lane_name="fx_metals_continuation",
            cluster_name="fx_metals_full",
            symbols=BREAKOUT_FULL_CLUSTER,
            session_name="us_unknown",
            allowed_sessions=BREAKOUT_SESSION_FILTERS["us_unknown"],
            gating_mode="soft",
            score_field="continuation_score_long",
            score_threshold=0.56,
            params_override={"compression_60_max": 9.0, "compression_5_max": 9.0},
            require_compression=False,
            complexity_points=4,
        )
    )

    failed_lane_definitions = [
        ("core4", FAILED_CORE_CLUSTER),
        ("core4_plus_qc", FAILED_PLUS_QC_CLUSTER),
        ("core4_plus_zt", FAILED_PLUS_ZT_CLUSTER),
        ("core4_plus_qc_zt", FAILED_FULL_CLUSTER),
    ]
    for cluster_name, cluster_symbols in failed_lane_definitions:
        for session_name, sessions in FAILED_SESSION_FILTERS.items():
            variants.append(
                _variant_from_spec(
                    spec=failed_spec,
                    variant_id=f"phase2c.failed.{cluster_name}.{session_name}.baseline",
                    role="lane",
                    lane_name="reversal_context_short",
                    cluster_name=cluster_name,
                    symbols=cluster_symbols,
                    session_name=session_name,
                    allowed_sessions=sessions,
                    gating_mode="ungated",
                    score_field=None,
                    score_threshold=None,
                    complexity_points=4,
                )
            )
            variants.append(
                _variant_from_spec(
                    spec=failed_spec,
                    variant_id=f"phase2c.failed.{cluster_name}.{session_name}.soft_0_30",
                    role="lane",
                    lane_name="reversal_context_short",
                    cluster_name=cluster_name,
                    symbols=cluster_symbols,
                    session_name=session_name,
                    allowed_sessions=sessions,
                    gating_mode="soft",
                    score_field="reversal_score_short",
                    score_threshold=0.30,
                    complexity_points=5,
                )
            )

    variants.extend(
        [
            _variant_from_spec(
                spec=failed_spec,
                variant_id="phase2c.failed.core4.no_us.no_extension",
                role="driver",
                lane_name="reversal_context_short",
                cluster_name="core4",
                symbols=FAILED_CORE_CLUSTER,
                session_name="no_us",
                allowed_sessions=FAILED_SESSION_FILTERS["no_us"],
                gating_mode="ungated",
                score_field=None,
                score_threshold=None,
                params_override={"dist_240_extreme": 0.0},
                require_extension=False,
                complexity_points=3,
            ),
            _variant_from_spec(
                spec=failed_spec,
                variant_id="phase2c.failed.core4.no_us.no_failed_breakout",
                role="driver",
                lane_name="reversal_context_short",
                cluster_name="core4",
                symbols=FAILED_CORE_CLUSTER,
                session_name="no_us",
                allowed_sessions=FAILED_SESSION_FILTERS["no_us"],
                gating_mode="ungated",
                score_field=None,
                score_threshold=None,
                require_failed_breakout=False,
                complexity_points=3,
            ),
        ]
    )

    mean_reversion_spec = CandidateSpec(
        candidate_id="phase2c.mean_reversion.failed_breakout.long",
        family="mean_reversion_sandbox",
        direction="LONG",
        gated=False,
        variant="sandbox",
        interpretability_level="Level 1: directly interpretable",
        description="Small sandbox for failed-breakout reversion in US session only.",
        feature_classes=("failed_breakout", "weak_trend_filter", "short_hold"),
        entry_logic="Enter long after a failed downside breakout in a weak-trend regime during US session.",
        exit_logic="Short holding period with modest target and tighter stop.",
        translation_feasibility="Medium",
        hold_bars=4,
        stop_r=0.80,
        target_r=0.85,
        params={
            "abs_slope_240_max": 0.75,
            "abs_slope_720_max": 0.65,
            "eff_240_max": 0.65,
            "close_pos_min": 0.58,
            "body_r_min": 0.12,
        },
    )
    variants.extend(
        [
            _variant_from_spec(
                spec=mean_reversion_spec,
                variant_id="phase2c.meanrev.cross_asset.us_only",
                role="sandbox",
                lane_name="failed_breakout_reversion_sandbox",
                cluster_name="cross_asset",
                symbols=MEAN_REVERSION_SANDBOX,
                session_name="us_only",
                allowed_sessions=("US",),
                gating_mode="ungated",
                score_field=None,
                score_threshold=None,
                complexity_points=4,
            ),
            _variant_from_spec(
                spec=mean_reversion_spec,
                variant_id="phase2c.meanrev.metals.us_only",
                role="sandbox",
                lane_name="failed_breakout_reversion_sandbox",
                cluster_name="metals_only",
                symbols=BREAKOUT_METALS_CLUSTER,
                session_name="us_only",
                allowed_sessions=("US",),
                gating_mode="ungated",
                score_field=None,
                score_threshold=None,
                complexity_points=4,
            ),
        ]
    )
    return variants


def _variant_from_spec(
    *,
    spec: CandidateSpec,
    variant_id: str,
    role: str,
    lane_name: str,
    cluster_name: str,
    symbols: tuple[str, ...],
    session_name: str,
    allowed_sessions: tuple[str, ...],
    gating_mode: str,
    score_field: str | None,
    score_threshold: float | None,
    params_override: dict[str, float] | None = None,
    require_compression: bool = True,
    require_extension: bool = True,
    require_failed_breakout: bool = True,
    complexity_points: int = 4,
) -> Phase2CVariantSpec:
    params = dict(spec.params)
    if params_override:
        params.update(params_override)
    return Phase2CVariantSpec(
        variant_id=variant_id,
        role=role,
        family=spec.family,
        direction=spec.direction,
        lane_name=lane_name,
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
        require_compression=require_compression,
        require_extension=require_extension,
        require_failed_breakout=require_failed_breakout,
        complexity_points=complexity_points,
    )


def _evaluate_variant(
    *,
    variant: Phase2CVariantSpec,
    symbol_store: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    trades: list[Phase2CTrade] = []
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
    return {
        "variant": asdict(variant),
        "trades": trades,
        "summary": _summarize_variant(variant=variant, trades=trades),
    }


def _simulate_variant(
    *,
    variant: Phase2CVariantSpec,
    symbol: str,
    execution: _FrameSeries,
    features: list[dict[str, Any]],
) -> list[Phase2CTrade]:
    trades: list[Phase2CTrade] = []
    next_available_index = 0
    for index, feature in enumerate(features):
        if index < next_available_index or index + 1 >= len(execution.bars):
            continue
        if not feature.get("ready"):
            continue
        if str(feature["session_label"]) not in variant.allowed_sessions:
            continue
        if not _variant_signal_matches(variant=variant, feature=feature):
            continue

        entry_index = index + 1
        entry_price = execution.opens[entry_index]
        risk = max(float(feature["risk_unit"]), 1e-6)
        if variant.direction == "LONG":
            stop_price = entry_price - variant.stop_r * risk
            target_price = entry_price + variant.target_r * risk
        else:
            stop_price = entry_price + variant.stop_r * risk
            target_price = entry_price - variant.target_r * risk
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
            Phase2CTrade(
                variant_id=variant.variant_id,
                family=variant.family,
                direction=variant.direction,
                symbol=symbol,
                entry_ts=execution.timestamps[entry_index].isoformat(),
                exit_ts=execution.timestamps[exit_index].isoformat(),
                entry_session=str(feature["session_label"]),
                r_multiple=signed_move / risk,
                holding_bars=max(exit_index - entry_index + 1, 1),
                feature_snapshot=_feature_snapshot(feature),
            )
        )
        next_available_index = exit_index + 1
    return trades


def _variant_signal_matches(*, variant: Phase2CVariantSpec, feature: dict[str, Any]) -> bool:
    if not _context_pass(variant=variant, feature=feature):
        return False
    params = variant.params

    if variant.family == "breakout_acceptance" and variant.direction == "LONG":
        compression_ok = True
        if variant.require_compression:
            compression_ok = (
                feature["compression_60"] <= params["compression_60_max"]
                and feature["compression_5"] <= params["compression_5_max"]
            )
        return (
            compression_ok
            and feature["breakout_up"] >= params["breakout_min"]
            and feature["close_pos"] >= params["close_pos_min"]
            and feature["slope_60"] >= params["slope_60_min"]
        )

    if variant.family == "failed_move_reversal" and variant.direction == "SHORT":
        extension_ok = True
        if variant.require_extension:
            extension_ok = feature["dist_240"] >= params["dist_240_extreme"]
        failed_breakout_ok = True
        if variant.require_failed_breakout:
            failed_breakout_ok = bool(feature["failed_breakout_short"])
        return (
            extension_ok
            and failed_breakout_ok
            and feature["close_pos"] <= 1.0 - params["close_pos_min"]
            and feature["body_r"] >= params["body_r_min"]
        )

    if variant.family == "mean_reversion_sandbox" and variant.direction == "LONG":
        weak_trend = (
            abs(feature["slope_240"]) <= params["abs_slope_240_max"]
            and abs(feature["slope_720"]) <= params["abs_slope_720_max"]
            and feature["eff_240"] <= params["eff_240_max"]
        )
        return (
            weak_trend
            and bool(feature["failed_breakout_long"])
            and feature["close_pos"] >= params["close_pos_min"]
            and feature["body_r"] >= params["body_r_min"]
        )

    return False


def _context_pass(*, variant: Phase2CVariantSpec, feature: dict[str, Any]) -> bool:
    if variant.gating_mode == "ungated":
        return True
    if variant.gating_mode == "hard":
        if variant.family == "breakout_acceptance":
            return bool(feature["regime_up"])
        return True
    if variant.gating_mode == "soft":
        if variant.score_field is None or variant.score_threshold is None:
            return True
        return float(feature[variant.score_field]) >= float(variant.score_threshold)
    return True


def _feature_snapshot(feature: dict[str, Any]) -> dict[str, float | str | bool]:
    keys = (
        "session_label",
        "compression_60",
        "compression_5",
        "breakout_up",
        "close_pos",
        "slope_60",
        "slope_240",
        "slope_720",
        "dist_240",
        "body_r",
        "continuation_score_long",
        "reversal_score_short",
        "failed_breakout_short",
        "failed_breakout_long",
    )
    return {key: feature[key] for key in keys if key in feature}


def _summarize_variant(*, variant: Phase2CVariantSpec, trades: list[Phase2CTrade]) -> dict[str, Any]:
    return _summary_from_trades(trades=trades, symbols=variant.symbols, sessions=variant.allowed_sessions)


def _summary_from_trades(
    *,
    trades: list[Phase2CTrade],
    symbols: tuple[str, ...],
    sessions: tuple[str, ...],
) -> dict[str, Any]:
    r_values = [trade.r_multiple for trade in trades]
    per_symbol = []
    for symbol in symbols:
        symbol_trades = [trade for trade in trades if trade.symbol == symbol]
        values = [trade.r_multiple for trade in symbol_trades]
        total_r = round(sum(values), 6) if values else 0.0
        per_symbol.append(
            {
                "symbol": symbol,
                "trade_count": len(values),
                "total_r": total_r,
                "expectancy_r": _expectancy(values),
                "cost_expectancy_r_010": _expectancy([value - 0.10 for value in values]),
                "cost_expectancy_r_015": _expectancy([value - 0.15 for value in values]),
                "profit_factor": _profit_factor(values),
                "walk_forward_positive_ratio": _walk_forward_positive_ratio(symbol_trades),  # type: ignore[arg-type]
            }
        )
    per_symbol.sort(key=lambda row: (-row["cost_expectancy_r_015"], -row["trade_count"], row["symbol"]))

    session_breakdown = []
    for session in sessions:
        session_trades = [trade for trade in trades if trade.entry_session == session]
        if not session_trades:
            continue
        values = [trade.r_multiple for trade in session_trades]
        session_breakdown.append(
            {
                "session": session,
                "trade_count": len(values),
                "share_of_trades": round(len(values) / float(max(len(trades), 1)), 6),
                "expectancy_r": _expectancy(values),
                "cost_expectancy_r_010": _expectancy([value - 0.10 for value in values]),
                "cost_expectancy_r_015": _expectancy([value - 0.15 for value in values]),
                "profit_factor": _profit_factor(values),
            }
        )
    session_breakdown.sort(key=lambda row: row["session"])

    positive_symbol_share_cost_010 = round(
        sum(1 for row in per_symbol if row["cost_expectancy_r_010"] > 0.0) / float(max(len(symbols), 1)),
        6,
    )
    positive_symbol_share_cost_015 = round(
        sum(1 for row in per_symbol if row["cost_expectancy_r_015"] > 0.0) / float(max(len(symbols), 1)),
        6,
    )
    cost_profile = {
        "cost_005": _expectancy([value - 0.05 for value in r_values]) if r_values else 0.0,
        "cost_010": _expectancy([value - 0.10 for value in r_values]) if r_values else 0.0,
        "cost_0125": _expectancy([value - 0.125 for value in r_values]) if r_values else 0.0,
        "cost_015": _expectancy([value - 0.15 for value in r_values]) if r_values else 0.0,
        "cost_020": _expectancy([value - 0.20 for value in r_values]) if r_values else 0.0,
    }
    concentration = _lane_concentration(trades=trades, r_values=r_values)

    return {
        "trade_count": len(r_values),
        "expectancy_r": _expectancy(r_values),
        "cost_expectancy_r_010": cost_profile["cost_010"],
        "cost_expectancy_r_015": cost_profile["cost_015"],
        "cost_expectancy_r_020": cost_profile["cost_020"],
        "win_rate": _win_rate(r_values),
        "profit_factor": _profit_factor(r_values),
        "max_drawdown_r": _max_drawdown(r_values),
        "sharpe_proxy": _sharpe_proxy(r_values),
        "avg_holding_bars": _avg([trade.holding_bars for trade in trades]),
        "walk_forward_positive_ratio": _walk_forward_positive_ratio(trades),  # type: ignore[arg-type]
        "positive_symbol_share_cost_010": positive_symbol_share_cost_010,
        "positive_symbol_share_cost_015": positive_symbol_share_cost_015,
        "per_symbol": per_symbol,
        "session_breakdown": session_breakdown,
        "cost_profile": cost_profile,
        "concentration": concentration,
        "dominant_session_share": max((row["share_of_trades"] for row in session_breakdown), default=0.0),
    }


def _lane_concentration(*, trades: list[Phase2CTrade], r_values: list[float]) -> dict[str, Any]:
    ordered = sorted(r_values, reverse=True)
    total = sum(r_values)
    symbol_totals: dict[str, float] = defaultdict(float)
    for trade in trades:
        symbol_totals[trade.symbol] += trade.r_multiple
    dominant_symbol_total = max(symbol_totals.values(), default=0.0)
    dominant_symbol = max(symbol_totals, key=symbol_totals.get) if symbol_totals else None
    return {
        "top_1_share_of_total_r": round(sum(ordered[:1]) / total, 6) if total != 0.0 else 0.0,
        "top_3_share_of_total_r": round(sum(ordered[:3]) / total, 6) if total != 0.0 else 0.0,
        "top_5_share_of_total_r": round(sum(ordered[:5]) / total, 6) if total != 0.0 else 0.0,
        "returns_without_top_1_r": round(sum(ordered[1:]), 6) if len(ordered) > 1 else round(sum(ordered), 6),
        "top_positive_share": _top_positive_share(r_values, 3),
        "dominant_symbol": dominant_symbol,
        "dominant_symbol_share_of_total_r": round(dominant_symbol_total / total, 6) if total != 0.0 else 0.0,
    }


def _build_robustness_suite(
    *,
    row: dict[str, Any],
    symbol_store: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    variant = Phase2CVariantSpec(**row["variant"])
    trades = row["trades"]
    leave_one_symbol_out = []
    active_symbols = [symbol for symbol in variant.symbols if any(trade.symbol == symbol for trade in trades)]
    for symbol in active_symbols:
        subset = [trade for trade in trades if trade.symbol != symbol]
        summary = _summary_from_trades(
            trades=subset,
            symbols=tuple(sym for sym in variant.symbols if sym != symbol),
            sessions=variant.allowed_sessions,
        )
        leave_one_symbol_out.append(
            {
                "excluded_symbol": symbol,
                "trade_count": summary["trade_count"],
                "cost_expectancy_r_010": summary["cost_expectancy_r_010"],
                "cost_expectancy_r_015": summary["cost_expectancy_r_015"],
            }
        )

    leave_one_session_out = []
    present_sessions = sorted({trade.entry_session for trade in trades})
    for session in present_sessions:
        subset = [trade for trade in trades if trade.entry_session != session]
        summary = _summary_from_trades(
            trades=subset,
            symbols=variant.symbols,
            sessions=tuple(sess for sess in variant.allowed_sessions if sess != session),
        )
        leave_one_session_out.append(
            {
                "excluded_session": session,
                "trade_count": summary["trade_count"],
                "cost_expectancy_r_010": summary["cost_expectancy_r_010"],
                "cost_expectancy_r_015": summary["cost_expectancy_r_015"],
            }
        )

    subperiod = _subperiod_stability(trades)
    perturbation = _run_perturbation_tests(variant=variant, symbol_store=symbol_store)
    return {
        "leave_one_symbol_out": leave_one_symbol_out,
        "leave_one_symbol_out_positive_ratio": round(
            sum(1 for item in leave_one_symbol_out if item["cost_expectancy_r_015"] > 0.0)
            / float(max(len(leave_one_symbol_out), 1)),
            6,
        ),
        "leave_one_session_out": leave_one_session_out,
        "leave_one_session_out_positive_ratio": round(
            sum(1 for item in leave_one_session_out if item["cost_expectancy_r_015"] > 0.0)
            / float(max(len(leave_one_session_out), 1)),
            6,
        ),
        "subperiod_stability": subperiod,
        "perturbation": perturbation,
    }


def _subperiod_stability(trades: list[Phase2CTrade]) -> dict[str, Any]:
    if not trades:
        return {
            "windows": [],
            "positive_ratio_cost_010": 0.0,
            "positive_ratio_cost_015": 0.0,
            "worst_cost_expectancy_r_015": 0.0,
        }
    ordered = sorted(trades, key=lambda trade: trade.entry_ts)
    bucket_size = max(len(ordered) // 6, 1)
    windows = []
    for start in range(0, len(ordered), bucket_size):
        bucket = ordered[start : start + bucket_size]
        values = [trade.r_multiple for trade in bucket]
        windows.append(
            {
                "start_ts": bucket[0].entry_ts,
                "end_ts": bucket[-1].entry_ts,
                "trade_count": len(bucket),
                "cost_expectancy_r_010": _expectancy([value - 0.10 for value in values]),
                "cost_expectancy_r_015": _expectancy([value - 0.15 for value in values]),
            }
        )
    positive_010 = sum(1 for window in windows if window["cost_expectancy_r_010"] > 0.0)
    positive_015 = sum(1 for window in windows if window["cost_expectancy_r_015"] > 0.0)
    return {
        "windows": windows,
        "positive_ratio_cost_010": round(positive_010 / float(len(windows)), 6) if windows else 0.0,
        "positive_ratio_cost_015": round(positive_015 / float(len(windows)), 6) if windows else 0.0,
        "worst_cost_expectancy_r_015": min((window["cost_expectancy_r_015"] for window in windows), default=0.0),
    }


def _run_perturbation_tests(
    *,
    variant: Phase2CVariantSpec,
    symbol_store: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    neighbors = _build_perturbation_neighbors(variant)
    rows = [_evaluate_variant(variant=neighbor, symbol_store=symbol_store) for neighbor in neighbors]
    summaries = [neighbor_row["summary"] for neighbor_row in rows]
    positive_010 = sum(1 for summary in summaries if summary["cost_expectancy_r_010"] > 0.0)
    positive_015 = sum(1 for summary in summaries if summary["cost_expectancy_r_015"] > 0.0)
    return {
        "tested_neighbors": len(summaries),
        "positive_ratio_cost_010": round(positive_010 / float(max(len(summaries), 1)), 6),
        "positive_ratio_cost_015": round(positive_015 / float(max(len(summaries), 1)), 6),
        "median_cost_expectancy_r_015": _avg([summary["cost_expectancy_r_015"] for summary in summaries]),
        "worst_cost_expectancy_r_015": min((summary["cost_expectancy_r_015"] for summary in summaries), default=0.0),
        "best_cost_expectancy_r_015": max((summary["cost_expectancy_r_015"] for summary in summaries), default=0.0),
    }


def _build_perturbation_neighbors(variant: Phase2CVariantSpec) -> list[Phase2CVariantSpec]:
    neighbors = [variant]
    if variant.family == "breakout_acceptance":
        for compression_60_max, breakout_min, close_pos_min in (
            (variant.params["compression_60_max"] - 0.08, variant.params["breakout_min"], variant.params["close_pos_min"]),
            (variant.params["compression_60_max"] + 0.08, variant.params["breakout_min"], variant.params["close_pos_min"]),
            (variant.params["compression_60_max"], variant.params["breakout_min"] - 0.06, variant.params["close_pos_min"]),
            (variant.params["compression_60_max"], variant.params["breakout_min"] + 0.06, variant.params["close_pos_min"]),
            (variant.params["compression_60_max"], variant.params["breakout_min"], variant.params["close_pos_min"] - 0.04),
            (variant.params["compression_60_max"], variant.params["breakout_min"], variant.params["close_pos_min"] + 0.04),
        ):
            params = dict(variant.params)
            params["compression_60_max"] = max(compression_60_max, 0.40)
            params["breakout_min"] = max(breakout_min, 0.10)
            params["close_pos_min"] = _clip(close_pos_min, 0.52, 0.86)
            neighbor = replace(variant, params=params)
            neighbors.append(neighbor)
        if variant.gating_mode == "soft" and variant.score_threshold is not None:
            for delta in (-0.08, 0.08):
                neighbors.append(
                    replace(
                        variant,
                        score_threshold=_clip(variant.score_threshold + delta, 0.40, 0.72),
                    )
                )
    elif variant.family == "failed_move_reversal":
        for close_pos_min, body_r_min, dist_240_extreme in (
            (variant.params["close_pos_min"] - 0.04, variant.params["body_r_min"], variant.params["dist_240_extreme"]),
            (variant.params["close_pos_min"] + 0.04, variant.params["body_r_min"], variant.params["dist_240_extreme"]),
            (variant.params["close_pos_min"], variant.params["body_r_min"] - 0.06, variant.params["dist_240_extreme"]),
            (variant.params["close_pos_min"], variant.params["body_r_min"] + 0.06, variant.params["dist_240_extreme"]),
            (variant.params["close_pos_min"], variant.params["body_r_min"], variant.params["dist_240_extreme"] - 0.20),
            (variant.params["close_pos_min"], variant.params["body_r_min"], variant.params["dist_240_extreme"] + 0.20),
        ):
            params = dict(variant.params)
            params["close_pos_min"] = _clip(close_pos_min, 0.52, 0.82)
            params["body_r_min"] = _clip(body_r_min, 0.10, 0.42)
            params["dist_240_extreme"] = max(dist_240_extreme, 0.0)
            neighbors.append(replace(variant, params=params))
        if variant.gating_mode == "soft" and variant.score_threshold is not None:
            for delta in (-0.10, 0.10):
                neighbors.append(
                    replace(
                        variant,
                        score_threshold=_clip(variant.score_threshold + delta, 0.20, 0.60),
                    )
                )
    return neighbors


def _phase2c_rank_score(
    *,
    summary: dict[str, Any],
    robustness: dict[str, Any],
    variant: dict[str, Any],
) -> float:
    cost010_score = _clip((summary["cost_expectancy_r_010"] + 0.05) / 0.18, 0.0, 1.0)
    cost015_score = _clip((summary["cost_expectancy_r_015"] + 0.05) / 0.16, 0.0, 1.0)
    cost020_score = _clip((summary["cost_expectancy_r_020"] + 0.05) / 0.18, 0.0, 1.0)
    breadth_score = summary["positive_symbol_share_cost_015"]
    walk_score = summary["walk_forward_positive_ratio"]
    leave_symbol_score = robustness["leave_one_symbol_out_positive_ratio"]
    leave_session_score = robustness["leave_one_session_out_positive_ratio"]
    subperiod_score = robustness["subperiod_stability"]["positive_ratio_cost_015"]
    perturb_score = robustness["perturbation"]["positive_ratio_cost_015"]
    trade_count_score = _clip(summary["trade_count"] / 180.0, 0.0, 1.0)
    simplicity_bonus = _clip((6.0 - float(variant["complexity_points"])) / 4.0, 0.0, 1.0)
    concentration_penalty = _clip(summary["concentration"]["dominant_symbol_share_of_total_r"] / 0.55, 0.0, 1.0)
    session_penalty = _clip((summary["dominant_session_share"] - 0.60) / 0.30, 0.0, 1.0)
    drawdown_penalty = _clip(summary["max_drawdown_r"] / 30.0, 0.0, 1.0)
    worst_bucket_penalty = _clip(-robustness["subperiod_stability"]["worst_cost_expectancy_r_015"] / 0.20, 0.0, 1.0)
    score = 100.0 * (
        0.24 * cost015_score
        + 0.14 * cost010_score
        + 0.08 * cost020_score
        + 0.10 * breadth_score
        + 0.08 * walk_score
        + 0.10 * leave_symbol_score
        + 0.08 * leave_session_score
        + 0.08 * subperiod_score
        + 0.06 * perturb_score
        + 0.04 * trade_count_score
        + 0.04 * simplicity_bonus
        - 0.08 * concentration_penalty
        - 0.04 * session_penalty
        - 0.04 * drawdown_penalty
        - 0.04 * worst_bucket_penalty
    )
    return round(score, 4)


def _phase2c_sandbox_score(summary: dict[str, Any]) -> float:
    score = 100.0 * (
        0.45 * _clip((summary["cost_expectancy_r_010"] + 0.10) / 0.20, 0.0, 1.0)
        + 0.20 * summary["walk_forward_positive_ratio"]
        + 0.20 * summary["positive_symbol_share_cost_010"]
        + 0.15 * _clip(summary["trade_count"] / 150.0, 0.0, 1.0)
    )
    return round(score - 15.0, 4)


def _promotion_shortlist_status(
    *,
    summary: dict[str, Any],
    robustness: dict[str, Any],
    variant: dict[str, Any],
) -> bool:
    return bool(
        summary["cost_expectancy_r_015"] > 0.0
        and summary["cost_expectancy_r_020"] >= -0.02
        and summary["positive_symbol_share_cost_015"] >= 0.50
        and robustness["leave_one_symbol_out_positive_ratio"] >= 0.75
        and robustness["leave_one_session_out_positive_ratio"] >= 0.50
        and robustness["subperiod_stability"]["positive_ratio_cost_015"] >= 0.50
        and robustness["perturbation"]["positive_ratio_cost_015"] >= 0.40
        and summary["concentration"]["dominant_symbol_share_of_total_r"] <= 0.60
        and summary["trade_count"] >= 100
        and variant["role"] == "lane"
    )


def _family_result(
    *,
    label: str,
    best_row: dict[str, Any],
    rows: list[dict[str, Any]],
    rows_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    variant = best_row["variant"]
    family_key = "breakout" if best_row["variant"]["family"] == "breakout_acceptance" else "failed"
    return {
        "label": label,
        "best_variant": {
            "variant": variant,
            "summary": best_row["summary"],
            "robustness": best_row["robustness"],
        },
        "top_lane_variants": [_render_rank_row(row) for row in rows[:6]],
        "driver_tests": _driver_tests(best_row=best_row, rows_by_id=rows_by_id),
        "instrument_specific_diagnostics": _instrument_diagnostics(
            rows_by_id=rows_by_id,
            family_key=family_key,
        ),
        "robustness_verdict": _robustness_verdict(best_row),
        "economic_explanation": _economic_explanation(best_row),
        "promotion_shortlist_status": best_row["promotion_shortlist"],
    }


def _mean_reversion_result(best_row: dict[str, Any] | None) -> dict[str, Any]:
    if best_row is None:
        return {
            "best_variant": None,
            "promotion_shortlist_status": False,
            "recommendation": "discard",
        }
    recommendation = "discard"
    if best_row["summary"]["cost_expectancy_r_010"] > 0.0:
        recommendation = "keep_small_sandbox_only"
    elif best_row["summary"]["expectancy_r"] > 0.0:
        recommendation = "keep_small_sandbox_only"
    return {
        "best_variant": {
            "variant": best_row["variant"],
            "summary": best_row["summary"],
        },
        "promotion_shortlist_status": False,
        "recommendation": recommendation,
    }


def _driver_tests(*, best_row: dict[str, Any], rows_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    variant = best_row["variant"]
    if variant["family"] == "breakout_acceptance":
        cluster = variant["cluster_name"]
        baseline_id = f"phase2c.breakout.{cluster}.{variant['session_name']}.baseline"
        soft_id = f"phase2c.breakout.{cluster}.{variant['session_name']}.soft_0_56"
        no_compression_id = "phase2c.breakout.fx_metals_full.us_unknown.soft_0_56.no_compression"
        all_id = f"phase2c.breakout.{cluster}.all.soft_0_56"
        no_london_id = f"phase2c.breakout.{cluster}.no_london.soft_0_56"
        us_unknown_id = f"phase2c.breakout.{cluster}.us_unknown.soft_0_56"
        return {
            "soft_continuation_score": _driver_compare(
                left=rows_by_id.get(soft_id),
                right=rows_by_id.get(baseline_id),
                label_left="soft_score",
                label_right="baseline",
            ),
            "compression_to_continuation": _driver_compare(
                left=best_row,
                right=rows_by_id.get(no_compression_id),
                label_left="with_compression",
                label_right="no_compression",
            ),
            "session_filtering": {
                "all": _snapshot(rows_by_id.get(all_id)),
                "us_unknown": _snapshot(rows_by_id.get(us_unknown_id)),
                "no_london": _snapshot(rows_by_id.get(no_london_id)),
            },
            "symbol_membership": {
                "current_cluster": variant["cluster_name"],
                "full_cluster": _snapshot(rows_by_id.get(f"phase2c.breakout.fx_metals_full.{variant['session_name']}.soft_0_56")),
                "metals_only": _snapshot(rows_by_id.get(f"phase2c.breakout.metals_only.{variant['session_name']}.soft_0_56")),
            },
        }
    baseline_id = f"phase2c.failed.{variant['cluster_name']}.{variant['session_name']}.baseline"
    soft_id = f"phase2c.failed.{variant['cluster_name']}.{variant['session_name']}.soft_0_30"
    no_extension_id = "phase2c.failed.core4.no_us.no_extension"
    no_failed_breakout_id = "phase2c.failed.core4.no_us.no_failed_breakout"
    return {
        "reversal_score": _driver_compare(
            left=rows_by_id.get(soft_id),
            right=rows_by_id.get(baseline_id),
            label_left="soft_score",
            label_right="baseline",
        ),
        "extension_requirement": _driver_compare(
            left=best_row,
            right=rows_by_id.get(no_extension_id),
            label_left="with_extension",
            label_right="no_extension",
        ),
        "failed_breakout_requirement": _driver_compare(
            left=best_row,
            right=rows_by_id.get(no_failed_breakout_id),
            label_left="with_failed_breakout",
            label_right="no_failed_breakout",
        ),
        "session_filtering": {
            "all": _snapshot(rows_by_id.get(f"phase2c.failed.{variant['cluster_name']}.all.baseline")),
            "no_us": _snapshot(rows_by_id.get(f"phase2c.failed.{variant['cluster_name']}.no_us.baseline")),
            "london_unknown": _snapshot(rows_by_id.get(f"phase2c.failed.{variant['cluster_name']}.london_unknown.baseline")),
        },
        "symbol_additivity": {
            "core4": _snapshot(rows_by_id.get("phase2c.failed.core4.no_us.baseline")),
            "core4_plus_qc": _snapshot(rows_by_id.get("phase2c.failed.core4_plus_qc.no_us.baseline")),
            "core4_plus_zt": _snapshot(rows_by_id.get("phase2c.failed.core4_plus_zt.no_us.baseline")),
            "core4_plus_qc_zt": _snapshot(rows_by_id.get("phase2c.failed.core4_plus_qc_zt.no_us.baseline")),
        },
    }


def _snapshot(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "variant_id": row["variant"]["variant_id"],
        "cost_expectancy_r_010": row["summary"]["cost_expectancy_r_010"],
        "cost_expectancy_r_015": row["summary"]["cost_expectancy_r_015"],
        "trade_count": row["summary"]["trade_count"],
    }


def _driver_compare(
    *,
    left: dict[str, Any] | None,
    right: dict[str, Any] | None,
    label_left: str,
    label_right: str,
) -> dict[str, Any] | None:
    if left is None or right is None:
        return None
    return {
        label_left: _snapshot(left),
        label_right: _snapshot(right),
        "delta_cost_expectancy_r_015": round(
            left["summary"]["cost_expectancy_r_015"] - right["summary"]["cost_expectancy_r_015"],
            6,
        ),
    }


def _instrument_diagnostics(*, rows_by_id: dict[str, dict[str, Any]], family_key: str) -> list[dict[str, Any]]:
    if family_key != "breakout":
        return []
    rows = []
    for symbol in BREAKOUT_FULL_CLUSTER:
        row = rows_by_id.get(f"phase2c.breakout.instrument.{symbol.lower()}.us_unknown.soft_0_56")
        if row is None:
            continue
        rows.append(
            {
                "symbol": symbol,
                "cost_expectancy_r_010": row["summary"]["cost_expectancy_r_010"],
                "cost_expectancy_r_015": row["summary"]["cost_expectancy_r_015"],
                "trade_count": row["summary"]["trade_count"],
            }
        )
    rows.sort(key=lambda item: (-item["cost_expectancy_r_015"], -item["trade_count"], item["symbol"]))
    return rows


def _robustness_verdict(row: dict[str, Any]) -> dict[str, Any]:
    summary = row["summary"]
    robustness = row["robustness"]
    verdict = "continue"
    if row["promotion_shortlist"]:
        verdict = "promotion_shortlist_ready"
    elif summary["cost_expectancy_r_015"] <= 0.0 or robustness["perturbation"]["positive_ratio_cost_015"] < 0.30:
        verdict = "not_ready"
    return {
        "verdict": verdict,
        "cost_expectancy_r_015": summary["cost_expectancy_r_015"],
        "leave_one_symbol_out_positive_ratio": robustness["leave_one_symbol_out_positive_ratio"],
        "leave_one_session_out_positive_ratio": robustness["leave_one_session_out_positive_ratio"],
        "subperiod_positive_ratio_cost_015": robustness["subperiod_stability"]["positive_ratio_cost_015"],
        "perturbation_positive_ratio_cost_015": robustness["perturbation"]["positive_ratio_cost_015"],
    }


def _economic_explanation(row: dict[str, Any]) -> str:
    variant = row["variant"]
    if variant["family"] == "breakout_acceptance":
        return (
            "The lane behaves like a compression-to-expansion continuation setup that works when 5m breakout acceptance is aligned with a softer multi-horizon trend score, especially outside London and within FX/metals symbols."
        )
    return (
        "The lane behaves like an extension-and-rejection short setup where local failed upside excursions reverse better outside the US session, especially in the CL/ES/6E/6J core."
    )


def _build_permanent_discards(
    *,
    breakout_best: dict[str, Any],
    failed_best: dict[str, Any],
    mean_reversion_best: dict[str, Any] | None,
) -> list[str]:
    discards = [
        "Broad cross-universe versions of both breakout long and failed-move short should stay discarded.",
        "Broad stretch mean reversion remains discarded.",
    ]
    breakout_sessions = breakout_best["summary"]["session_breakdown"]
    if any(row["session"] == "LONDON" and row["cost_expectancy_r_015"] < 0.0 for row in breakout_sessions):
        discards.append("Standalone London breakout-long exposure should be treated as discarded inside the FX/metals lane.")
    if mean_reversion_best is not None and mean_reversion_best["summary"]["cost_expectancy_r_010"] <= 0.0:
        discards.append("Broad mean-reversion promotion work should stay discarded; only a tiny failed-breakout sandbox remains optional.")
    if failed_best["variant"]["cluster_name"] != "core4_plus_qc_zt":
        discards.append("Thin additive slices that do not improve the CL/ES/6E/6J core should be discarded from the main failed-move lane.")
    return discards


def _build_phase2c_recommendation(
    *,
    breakout_best: dict[str, Any],
    failed_best: dict[str, Any],
    mean_reversion_best: dict[str, Any] | None,
) -> dict[str, Any]:
    keep = []
    if breakout_best["summary"]["cost_expectancy_r_015"] > 0.0:
        keep.append(breakout_best["variant"]["variant_id"])
    if failed_best["summary"]["cost_expectancy_r_015"] > 0.0:
        keep.append(failed_best["variant"]["variant_id"])
    return {
        "keep": keep,
        "promotion_shortlist": [
            breakout_best["variant"]["variant_id"] if breakout_best["promotion_shortlist"] else None,
            failed_best["variant"]["variant_id"] if failed_best["promotion_shortlist"] else None,
        ],
        "mean_reversion": (
            "keep_small_sandbox_only"
            if mean_reversion_best is not None and mean_reversion_best["summary"]["expectancy_r"] > 0.0
            else "discard"
        ),
        "summary": (
            "Keep the best narrowed breakout lane and the best narrowed failed-move lane under continued specialization; do not broaden either family, and keep mean reversion only as a tiny failed-breakout sandbox."
        ),
    }


def _render_rank_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "variant_id": row["variant"]["variant_id"],
        "cluster_name": row["variant"]["cluster_name"],
        "session_name": row["variant"]["session_name"],
        "role": row["variant"]["role"],
        "trade_count": row["summary"]["trade_count"],
        "expectancy_r": row["summary"]["expectancy_r"],
        "cost_expectancy_r_010": row["summary"]["cost_expectancy_r_010"],
        "cost_expectancy_r_015": row["summary"]["cost_expectancy_r_015"],
        "cost_expectancy_r_020": row["summary"]["cost_expectancy_r_020"],
        "positive_symbol_share_cost_015": row["summary"]["positive_symbol_share_cost_015"],
        "walk_forward_positive_ratio": row["summary"]["walk_forward_positive_ratio"],
        "phase2c_score": row["summary"]["phase2c_score"],
        "promotion_shortlist": row.get("promotion_shortlist", False),
    }


def _render_phase2c_markdown(report: dict[str, Any]) -> str:
    breakout = report["best_candidate_per_family"]["breakout_acceptance_long"]
    failed = report["best_candidate_per_family"]["failed_move_reversal_short"]
    mean_reversion = report["best_candidate_per_family"]["mean_reversion_sandbox"]
    lines = [
        "# Quant Futures Phase 2C",
        "",
        f"Execution timeframe: {report['execution_timeframe']}",
        f"Symbols tested: {', '.join(report['symbols_tested'])}",
        "",
        "## Best Candidates",
        (
            f"- Breakout long: {breakout['best_variant']['variant']['variant_id']} "
            f"(cost015={round(breakout['best_variant']['summary']['cost_expectancy_r_015'], 4)}R, "
            f"shortlist={breakout['promotion_shortlist_status']})"
        ),
        (
            f"- Failed-move short: {failed['best_variant']['variant']['variant_id']} "
            f"(cost015={round(failed['best_variant']['summary']['cost_expectancy_r_015'], 4)}R, "
            f"shortlist={failed['promotion_shortlist_status']})"
        ),
        (
            f"- Mean reversion sandbox: {mean_reversion['best_variant']['variant']['variant_id']} "
            f"(cost010={round(mean_reversion['best_variant']['summary']['cost_expectancy_r_010'], 4)}R)"
            if mean_reversion["best_variant"]
            else "- Mean reversion sandbox: no viable row"
        ),
        "",
        "## Narrowed-Lane Rankings",
    ]
    for family_key in ("breakout_acceptance_long", "failed_move_reversal_short"):
        lines.append(f"- {family_key}:")
        for row in report["narrowed_lane_results"][family_key][:5]:
            lines.append(
                f"  {row['variant_id']} | cost015={round(row['cost_expectancy_r_015'], 4)}R | "
                f"cost020={round(row['cost_expectancy_r_020'], 4)}R | trades={row['trade_count']} | "
                f"shortlist={row['promotion_shortlist']}"
            )
    lines.extend(["", "## Recommendation", f"- {report['overall_recommendation']['summary']}"])
    return "\n".join(lines)
