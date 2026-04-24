from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from validation_layer.config.defaults import build_debug_config
from validation_layer.data.contracts import EquityPoint, PositionPoint, StrategyBacktest
from validation_layer.robustness.drawdown import calculate_drawdown_profile, run_drawdown_module


def _ts(index: int) -> datetime:
    base = datetime(2026, 2, 10, 9, 30, tzinfo=ZoneInfo("America/New_York"))
    return base + timedelta(minutes=5 * index)


def _build_strategy(equity_values: list[float]) -> StrategyBacktest:
    curve = tuple(EquityPoint(timestamp=_ts(index), equity=value) for index, value in enumerate(equity_values))
    positions = tuple(PositionPoint(timestamp=_ts(index), position=0.0) for index in range(len(equity_values)))
    return StrategyBacktest(
        strategy_name="drawdown_case",
        symbol="MGC",
        timeframe="5m",
        parameters={},
        bar_data=(),
        trades=(),
        equity_curve=curve,
        position_series=positions,
    )


def test_drawdown_profile_captures_depth_and_duration() -> None:
    strategy = _build_strategy([100.0, 110.0, 105.0, 90.0, 92.0, 111.0])
    profile = calculate_drawdown_profile(strategy, build_debug_config())

    assert profile.max_drawdown == 20.0
    assert profile.drawdown_duration == 3
    assert profile.recovery_duration == 3
    assert profile.average_drawdown > 0.0


def test_drawdown_module_warns_or_fails_for_painful_profile() -> None:
    strategy = _build_strategy([100.0, 130.0, 80.0, 82.0, 84.0, 86.0])
    result = run_drawdown_module(strategy, build_debug_config())

    assert result.status in {"warn", "fail"}
    assert result.metrics["max_drawdown"] == 50.0
