"""Optional replay-analysis utilities for experimental causal momentum features."""

from __future__ import annotations

import csv
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from ..config_models import StrategySettings
from ..domain.models import Bar, StrategyState
from ..indicators.feature_engine import compute_features
from ..strategy.trade_state import build_initial_state
from .causal_momentum import CausalMomentumFeature, compute_causal_momentum_features


@dataclass(frozen=True)
class CausalMomentumReportRow:
    bar_id: str
    timestamp: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    atr: Decimal
    smoothed_price: Decimal
    first_derivative: Decimal
    second_derivative: Decimal
    normalized_slope: Decimal
    normalized_curvature: Decimal
    momentum_compressing_up: bool
    momentum_compressing_down: bool
    momentum_turning_positive: bool
    momentum_turning_negative: bool


def build_causal_momentum_report(
    bars: Sequence[Bar],
    settings: StrategySettings,
    smoothing_length: int = 3,
    normalization_floor: Decimal = Decimal("0.01"),
) -> list[CausalMomentumReportRow]:
    """Build a replay-aligned report of experimental causal momentum features."""
    if not bars:
        return []

    state = build_initial_state(bars[0].end_ts)
    atr_values: list[Decimal] = []
    for index in range(len(bars)):
        features = compute_features(bars[: index + 1], state, settings)
        atr_values.append(features.atr)
        state = _advance_research_state(state, features)

    momentum_features = compute_causal_momentum_features(
        prices=[bar.close for bar in bars],
        volatility_scale=atr_values,
        smoothing_length=smoothing_length,
        normalization_floor=normalization_floor,
    )

    return [
        _build_report_row(bar, atr, momentum_feature)
        for bar, atr, momentum_feature in zip(bars, atr_values, momentum_features)
    ]


def write_causal_momentum_report_csv(rows: Sequence[CausalMomentumReportRow], output_path: str | Path) -> Path:
    """Write a causal momentum report CSV for inspection."""
    path = Path(output_path)
    fieldnames = [
        "bar_id",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "atr",
        "smoothed_price",
        "first_derivative",
        "second_derivative",
        "normalized_slope",
        "normalized_curvature",
        "momentum_compressing_up",
        "momentum_compressing_down",
        "momentum_turning_positive",
        "momentum_turning_negative",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "bar_id": row.bar_id,
                    "timestamp": row.timestamp,
                    "open": str(row.open),
                    "high": str(row.high),
                    "low": str(row.low),
                    "close": str(row.close),
                    "volume": row.volume,
                    "atr": str(row.atr),
                    "smoothed_price": str(row.smoothed_price),
                    "first_derivative": str(row.first_derivative),
                    "second_derivative": str(row.second_derivative),
                    "normalized_slope": str(row.normalized_slope),
                    "normalized_curvature": str(row.normalized_curvature),
                    "momentum_compressing_up": str(row.momentum_compressing_up),
                    "momentum_compressing_down": str(row.momentum_compressing_down),
                    "momentum_turning_positive": str(row.momentum_turning_positive),
                    "momentum_turning_negative": str(row.momentum_turning_negative),
                }
            )
    return path


def _advance_research_state(state: StrategyState, features) -> StrategyState:
    return replace(
        state,
        last_swing_low=features.last_swing_low,
        last_swing_high=features.last_swing_high,
        updated_at=state.updated_at,
    )


def _build_report_row(bar: Bar, atr: Decimal, feature: CausalMomentumFeature) -> CausalMomentumReportRow:
    return CausalMomentumReportRow(
        bar_id=bar.bar_id,
        timestamp=bar.end_ts.isoformat(),
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        atr=atr,
        smoothed_price=feature.smoothed_price,
        first_derivative=feature.first_derivative,
        second_derivative=feature.second_derivative,
        normalized_slope=feature.normalized_slope,
        normalized_curvature=feature.normalized_curvature,
        momentum_compressing_up=feature.momentum_compressing_up,
        momentum_compressing_down=feature.momentum_compressing_down,
        momentum_turning_positive=feature.momentum_turning_positive,
        momentum_turning_negative=feature.momentum_turning_negative,
    )
