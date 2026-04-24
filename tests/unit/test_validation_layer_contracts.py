from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from validation_layer.data.contracts import EquityPoint, FeatureSeries, StrategyBacktest, TradeRecord


def _ts(index: int) -> datetime:
    base = datetime(2026, 1, 5, 9, 30, tzinfo=ZoneInfo("America/New_York"))
    return base + timedelta(minutes=5 * index)


def test_feature_series_rejects_mismatched_lengths() -> None:
    with pytest.raises(ValueError, match="same length"):
        FeatureSeries(
            name="feature",
            symbol="MGC",
            timeframe="5m",
            session_filter=None,
            timestamps=(_ts(0), _ts(1)),
            values=(1.0,),
        )


def test_trade_record_requires_forward_time() -> None:
    with pytest.raises(ValueError, match="greater than or equal"):
        TradeRecord(
            entry_time=_ts(1),
            exit_time=_ts(0),
            direction="long",
            qty=1.0,
            gross_pnl=1.0,
            net_pnl=1.0,
            mae=0.2,
            mfe=0.5,
            holding_bars=1,
            holding_minutes=5.0,
        )


def test_strategy_backtest_requires_equity_curve() -> None:
    with pytest.raises(ValueError, match="equity_curve"):
        StrategyBacktest(
            strategy_name="demo",
            symbol="MGC",
            timeframe="5m",
            parameters={},
            bar_data=(),
            trades=(),
            equity_curve=(),
            position_series=(),
        )


def test_contracts_do_not_silently_mutate_inputs() -> None:
    timestamps = [_ts(1), _ts(0)]
    values = [2.0, 1.0]
    feature = FeatureSeries(
        name="feature",
        symbol="MGC",
        timeframe="5m",
        session_filter=None,
        timestamps=tuple(timestamps),
        values=tuple(values),
    )

    timestamps.append(_ts(2))
    values.append(3.0)

    assert len(feature.timestamps) == 2
    assert len(feature.values) == 2


def test_equity_point_metadata_is_copied() -> None:
    metadata = {"tag": "alpha"}
    point = EquityPoint(timestamp=_ts(0), equity=100_000.0, metadata=metadata)
    metadata["tag"] = "beta"

    assert point.metadata["tag"] == "alpha"
