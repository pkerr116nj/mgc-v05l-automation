from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from validation_layer.config.defaults import build_debug_config
from validation_layer.data.contracts import EquityPoint, PositionPoint, StrategyBacktest, TradeRecord
from validation_layer.performance.metrics import calculate_canonical_metrics
from validation_layer.performance.trade_normalization import run_trade_normalization_module


def _ts(index: int) -> datetime:
    base = datetime(2026, 2, 2, 9, 30, tzinfo=ZoneInfo("America/New_York"))
    return base + timedelta(minutes=5 * index)


def _build_strategy(trade_pnls: list[float]) -> StrategyBacktest:
    equity = 100_000.0
    trades = []
    curve = [EquityPoint(timestamp=_ts(0), equity=equity)]
    positions = []
    for index, pnl in enumerate(trade_pnls, start=1):
        entry_time = _ts(index * 2 - 1)
        exit_time = _ts(index * 2)
        trades.append(
            TradeRecord(
                entry_time=entry_time,
                exit_time=exit_time,
                direction="long" if index % 2 else "short",
                qty=1.0,
                gross_pnl=pnl,
                net_pnl=pnl,
                mae=min(0.0, pnl) - 0.5,
                mfe=max(0.0, pnl) + 0.5,
                holding_bars=2,
                holding_minutes=10.0,
                session_label="us",
            )
        )
        positions.append(PositionPoint(timestamp=entry_time, position=1.0))
        equity += pnl
        curve.append(EquityPoint(timestamp=exit_time, equity=equity))
        positions.append(PositionPoint(timestamp=exit_time, position=0.0))

    return StrategyBacktest(
        strategy_name="stable_edge",
        symbol="MGC",
        timeframe="5m",
        parameters={"hold": 2},
        bar_data=(),
        trades=tuple(trades),
        equity_curve=tuple(curve),
        position_series=tuple(positions),
        metadata={"margin_per_contract": 5000.0},
    )


def test_canonical_metrics_are_correct_for_basic_case() -> None:
    metrics = calculate_canonical_metrics(_build_strategy([10.0, -5.0, 20.0]), build_debug_config())

    assert metrics.net_pnl == 25.0
    assert metrics.profit_factor == 6.0
    assert round(metrics.expectancy, 4) == round(25.0 / 3.0, 4)
    assert round(metrics.hit_rate, 4) == round(2.0 / 3.0, 4)
    assert metrics.average_win == 15.0
    assert metrics.average_loss == -5.0


def test_trade_normalization_penalizes_high_turnover_low_value_case() -> None:
    strategy = _build_strategy([0.1] * 20)
    result = run_trade_normalization_module(strategy, build_debug_config())

    assert result.metrics["capital_efficiency_score"] < 0.35
    assert result.status == "fail"


def test_metrics_are_stable_under_trade_ordering() -> None:
    config = build_debug_config()
    strategy = _build_strategy([8.0, -3.0, 5.0, 6.0])
    reversed_strategy = StrategyBacktest(
        strategy_name=strategy.strategy_name,
        symbol=strategy.symbol,
        timeframe=strategy.timeframe,
        parameters=strategy.parameters,
        bar_data=strategy.bar_data,
        trades=tuple(reversed(strategy.trades)),
        equity_curve=tuple(reversed(strategy.equity_curve)),
        position_series=tuple(reversed(strategy.position_series)),
        metadata=strategy.metadata,
    )

    left = calculate_canonical_metrics(strategy, config)
    right = calculate_canonical_metrics(reversed_strategy, config)

    assert left.net_pnl == right.net_pnl
    assert left.expectancy == right.expectancy
    assert left.profit_factor == right.profit_factor
