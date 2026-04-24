"""Shared helpers for Phase 4 robustness diagnostics."""

from __future__ import annotations

import math
import random
from dataclasses import replace
from datetime import timedelta

from ..config.schemas import ValidationConfig
from ..data.contracts import EquityPoint, PositionPoint, StrategyBacktest, TradeRecord
from ..data.normalization import normalize_strategy_backtest
from ..performance.metrics import CanonicalPerformanceSnapshot, calculate_canonical_metrics
from ..reporting.models import ValidationModuleResult
from ..utils.stats import clamp01, percentile, safe_divide, safe_mean


def robustness_insufficient_evidence_result(
    *,
    module_name: str,
    summary: str,
    score_fields: tuple[str, ...],
    diagnostics: dict[str, object] | None = None,
    artifacts: dict[str, object] | None = None,
    recommendations: list[str] | None = None,
) -> ValidationModuleResult:
    metrics: dict[str, float | int | str | bool | None] = {"insufficient_evidence": True}
    for field_name in score_fields:
        metrics[field_name] = None
    return ValidationModuleResult(
        module_name=module_name,
        status="warn",
        summary=summary,
        metrics=metrics,
        diagnostics=diagnostics or {},
        artifacts=artifacts or {},
        recommendations=recommendations or [],
    )


def ensure_robustness_prerequisites(
    strategy: StrategyBacktest,
    config: ValidationConfig,
    *,
    module_name: str,
    score_fields: tuple[str, ...],
) -> ValidationModuleResult | None:
    normalized = normalize_strategy_backtest(strategy)
    if len(normalized.trades) < config.thresholds.minimum_trade_count:
        return robustness_insufficient_evidence_result(
            module_name=module_name,
            summary=(
                "Insufficient evidence: robustness testing needs a broader trade sample before "
                "bootstrap, perturbation, or permutation results are credible."
            ),
            score_fields=score_fields,
            diagnostics={
                "trade_count": len(normalized.trades),
                "minimum_trade_count": config.thresholds.minimum_trade_count,
            },
            recommendations=[
                "Collect more realized trades before treating robustness diagnostics as decision-grade evidence."
            ],
        )
    return None


def make_rng(config: ValidationConfig, *, salt: int = 0) -> random.Random:
    return random.Random(config.robustness.random_seed + salt)


def sequence_trade_variant(
    strategy: StrategyBacktest,
    trades: tuple[TradeRecord, ...],
    *,
    suffix: str,
) -> StrategyBacktest:
    base = normalize_strategy_backtest(strategy)
    start = base.equity_curve[0].timestamp
    current = start
    remapped: list[TradeRecord] = []
    for trade in trades:
        hold_minutes = max(1.0, float(trade.holding_minutes or 0.0))
        hold_seconds = max(60, int(round(hold_minutes * 60.0)))
        entry_time = current
        exit_time = entry_time + timedelta(seconds=hold_seconds)
        remapped.append(
            replace(
                trade,
                entry_time=entry_time,
                exit_time=exit_time,
                holding_minutes=hold_seconds / 60.0,
                holding_bars=max(1, trade.holding_bars),
            )
        )
        current = exit_time + timedelta(minutes=1)

    equity_curve = _equity_curve_from_trades(remapped, starting_equity=base.equity_curve[0].equity, start=start)
    position_series = _position_series_from_trades(remapped, start=start)
    metadata = dict(base.metadata)
    metadata["robustness_variant"] = suffix
    return StrategyBacktest(
        strategy_name=f"{base.strategy_name}:{suffix}",
        symbol=base.symbol,
        timeframe=base.timeframe,
        parameters=base.parameters,
        bar_data=(),
        trades=tuple(remapped),
        equity_curve=equity_curve,
        position_series=position_series,
        provenance=base.provenance,
        metadata=metadata,
    )


def evaluate_trade_variant(
    strategy: StrategyBacktest,
    trades: tuple[TradeRecord, ...],
    *,
    suffix: str,
    config: ValidationConfig,
) -> CanonicalPerformanceSnapshot:
    variant = sequence_trade_variant(strategy, trades, suffix=suffix)
    return calculate_canonical_metrics(variant, config)


def build_pnl_override_trade(
    trade: TradeRecord,
    *,
    net_pnl: float,
    gross_pnl: float | None = None,
) -> TradeRecord:
    gross = net_pnl if gross_pnl is None else gross_pnl
    return replace(trade, net_pnl=net_pnl, gross_pnl=gross)


def metric_interval(values: list[float], *, lower_q: float = 0.05, upper_q: float = 0.95) -> dict[str, float]:
    ordered = [float(value) for value in values]
    return {
        "lower": percentile(ordered, lower_q),
        "median": percentile(ordered, 0.50),
        "upper": percentile(ordered, upper_q),
        "width": percentile(ordered, upper_q) - percentile(ordered, lower_q),
    }


def bounded_width_score(interval_width: float, observed_scale: float) -> float:
    return clamp01(1.0 - safe_divide(interval_width, abs(observed_scale) + 1.0, default=1.0))


def finite_ratio(value: float, *, cap: float = 10.0) -> float:
    if math.isnan(value):
        return 0.0
    if math.isinf(value):
        return cap if value > 0 else 0.0
    return value


def positive_fraction(values: list[float]) -> float:
    return safe_mean([1.0 if value > 0 else 0.0 for value in values])


def percentile_rank(values: list[float], observed: float) -> float:
    if not values:
        return 0.0
    less_equal = sum(1 for value in values if value <= observed)
    return clamp01(less_equal / len(values))


def retention_score(observed: float, stressed_values: list[float]) -> float:
    if not stressed_values:
        return 0.0
    stressed_median = percentile(stressed_values, 0.50)
    return clamp01(safe_divide(stressed_median, abs(observed) + 1.0, default=0.0))


def worst_case_retention(observed: float, stressed_values: list[float]) -> float:
    if not stressed_values:
        return 0.0
    stressed_worst = min(stressed_values)
    if observed <= 0:
        return 0.0 if stressed_worst < observed else 1.0
    return clamp01(safe_divide(stressed_worst, observed, default=0.0))


def _equity_curve_from_trades(
    trades: list[TradeRecord],
    *,
    starting_equity: float,
    start,
) -> tuple[EquityPoint, ...]:
    equity = starting_equity
    points = [EquityPoint(timestamp=start, equity=equity)]
    for trade in trades:
        equity += trade.net_pnl
        points.append(EquityPoint(timestamp=trade.exit_time, equity=equity, session_label=trade.session_label))
    return tuple(points)


def _position_series_from_trades(
    trades: list[TradeRecord],
    *,
    start,
) -> tuple[PositionPoint, ...]:
    deltas: dict[object, float] = {start: 0.0}
    for trade in trades:
        signed_qty = trade.qty if trade.direction == "long" else -trade.qty
        deltas[trade.entry_time] = deltas.get(trade.entry_time, 0.0) + signed_qty
        deltas[trade.exit_time] = deltas.get(trade.exit_time, 0.0) - signed_qty

    running = 0.0
    points: list[PositionPoint] = []
    for timestamp in sorted(deltas):
        running += deltas[timestamp]
        points.append(PositionPoint(timestamp=timestamp, position=running))
    return tuple(points)
