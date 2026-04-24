from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from validation_layer.config.defaults import build_debug_config
from validation_layer.data.contracts import EquityPoint, FeatureSeries, PositionPoint, StrategyBacktest, TradeRecord
from validation_layer.orchestration.pipeline import run_validation_pipeline


def _ts(index: int) -> datetime:
    base = datetime(2026, 3, 2, 9, 30, tzinfo=ZoneInfo("America/New_York"))
    return base + timedelta(minutes=5 * index)


def _build_feature(values: list[float], name: str) -> FeatureSeries:
    return FeatureSeries(
        name=name,
        symbol="MGC",
        timeframe="5m",
        session_filter=None,
        timestamps=tuple(_ts(index) for index in range(len(values))),
        values=tuple(values),
    )


def _build_strategy(name: str, trade_pnls: list[float]) -> StrategyBacktest:
    equity = 100_000.0
    trades = []
    curve = [EquityPoint(timestamp=_ts(0), equity=equity)]
    positions = [PositionPoint(timestamp=_ts(0), position=0.0)]
    for index, pnl in enumerate(trade_pnls, start=1):
        entry = _ts(index * 2 - 1)
        exit_time = _ts(index * 2)
        trades.append(
            TradeRecord(
                entry_time=entry,
                exit_time=exit_time,
                direction="long",
                qty=1.0,
                gross_pnl=pnl,
                net_pnl=pnl,
                mae=min(-0.5, pnl),
                mfe=max(0.5, pnl),
                holding_bars=2,
                holding_minutes=10.0,
                session_label="us",
            )
        )
        positions.append(PositionPoint(timestamp=entry, position=1.0))
        equity += pnl
        curve.append(EquityPoint(timestamp=exit_time, equity=equity))
        positions.append(PositionPoint(timestamp=exit_time, position=0.0))

    return StrategyBacktest(
        strategy_name=name,
        symbol="MGC",
        timeframe="5m",
        parameters={"lookback": 5},
        bar_data=(),
        trades=tuple(trades),
        equity_curve=tuple(curve),
        position_series=tuple(positions),
        metadata={"margin_per_contract": 5000.0},
    )


def test_feature_pipeline_scores_stable_series_above_churny_series() -> None:
    stable_values = [0.05 * ((index % 4) - 1.5) for index in range(60)]
    noisy_values = [(-1.0) ** index * (index % 7) for index in range(60)]

    stable_report = run_validation_pipeline(_build_feature(stable_values, "stable_feature"), build_debug_config())
    noisy_report = run_validation_pipeline(_build_feature(noisy_values, "noisy_feature"), build_debug_config())

    stable_stationarity = next(result for result in stable_report.module_results if result.module_name == "stationarity")
    noisy_stationarity = next(result for result in noisy_report.module_results if result.module_name == "stationarity")

    assert stable_stationarity.metrics["stationarity_score"] > noisy_stationarity.metrics["stationarity_score"]


def test_strategy_pipeline_penalizes_split_dependent_case() -> None:
    stable_strategy = _build_strategy("stable_strategy", [8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 9.0, 8.0, 10.0, 11.0, 12.0, 9.0])
    split_dependent = _build_strategy("split_dependent", [20.0, 18.0, 15.0, 14.0, -25.0, -20.0, -18.0, -15.0, 18.0, 16.0, 14.0, 12.0])

    stable_report = run_validation_pipeline(stable_strategy, build_debug_config())
    split_report = run_validation_pipeline(split_dependent, build_debug_config())

    stable_score = next(result for result in stable_report.module_results if result.module_name == "split_comparison")
    split_score = next(result for result in split_report.module_results if result.module_name == "split_comparison")

    assert stable_score.metrics["split_dependence_score"] >= split_score.metrics["split_dependence_score"]
