"""Normalization helpers for validation inputs."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from .contracts import EquityPoint, FeatureSeries, PriceBar, StrategyBacktest, TradeRecord


def sort_price_bars(bars: tuple[PriceBar, ...]) -> tuple[PriceBar, ...]:
    return tuple(sorted(bars, key=lambda item: item.timestamp))


def sort_trades(trades: tuple[TradeRecord, ...]) -> tuple[TradeRecord, ...]:
    return tuple(sorted(trades, key=lambda item: (item.entry_time, item.exit_time)))


def sort_equity_curve(points: tuple[EquityPoint, ...]) -> tuple[EquityPoint, ...]:
    return tuple(sorted(points, key=lambda item: item.timestamp))


def normalize_feature_series(feature: FeatureSeries) -> FeatureSeries:
    paired = sorted(zip(feature.timestamps, feature.values), key=lambda item: item[0])
    timestamps = tuple(timestamp for timestamp, _ in paired)
    values = tuple(value for _, value in paired)
    return replace(
        feature,
        timestamps=timestamps,
        values=values,
        price_context=sort_price_bars(feature.price_context),
    )


def normalize_strategy_backtest(strategy: StrategyBacktest) -> StrategyBacktest:
    return replace(
        strategy,
        bar_data=sort_price_bars(strategy.bar_data),
        trades=sort_trades(strategy.trades),
        equity_curve=sort_equity_curve(strategy.equity_curve),
        position_series=tuple(sorted(strategy.position_series, key=lambda item: item.timestamp)),
    )


def slice_strategy_backtest(
    strategy: StrategyBacktest,
    start: datetime,
    end: datetime,
    suffix: str,
) -> StrategyBacktest:
    bar_data = tuple(bar for bar in strategy.bar_data if start <= bar.timestamp <= end)
    trades = tuple(trade for trade in strategy.trades if start <= trade.exit_time <= end)
    equity_curve = tuple(point for point in strategy.equity_curve if start <= point.timestamp <= end)
    position_series = tuple(point for point in strategy.position_series if start <= point.timestamp <= end)
    if not equity_curve:
        raise ValueError("Cannot slice StrategyBacktest into an empty equity curve window.")
    metadata = dict(strategy.metadata)
    metadata["slice_start"] = start.isoformat()
    metadata["slice_end"] = end.isoformat()
    return StrategyBacktest(
        strategy_name=f"{strategy.strategy_name}:{suffix}",
        symbol=strategy.symbol,
        timeframe=strategy.timeframe,
        parameters=strategy.parameters,
        bar_data=bar_data,
        trades=trades,
        equity_curve=equity_curve,
        position_series=position_series,
        metadata=metadata,
    )
