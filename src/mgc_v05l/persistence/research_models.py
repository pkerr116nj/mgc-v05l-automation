"""Typed records for the additive research schema extension."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional


@dataclass(frozen=True)
class InstrumentRecord:
    ticker: str
    asset_class: str
    instrument_id: Optional[int] = None
    cusip: Optional[str] = None
    description: Optional[str] = None
    exchange: Optional[str] = None
    multiplier: Optional[Decimal] = None
    is_active: bool = True


@dataclass(frozen=True)
class ExperimentRunRecord:
    name: str
    started_at: datetime
    experiment_run_id: Optional[int] = None
    description: Optional[str] = None
    market_universe: Optional[str] = None
    timeframe: Optional[str] = None
    feature_version: Optional[str] = None
    signal_version: Optional[str] = None
    sizing_version: Optional[str] = None
    config_json: Optional[str] = None
    completed_at: Optional[datetime] = None


@dataclass(frozen=True)
class DerivedFeatureRecord:
    bar_id: str
    created_at: datetime
    experiment_run_id: Optional[int] = None
    feature_id: Optional[int] = None
    atr: Optional[Decimal] = None
    vwap: Optional[Decimal] = None
    ema_fast: Optional[Decimal] = None
    ema_slow: Optional[Decimal] = None
    velocity: Optional[Decimal] = None
    velocity_delta: Optional[Decimal] = None
    stretch_down: Optional[Decimal] = None
    stretch_up: Optional[Decimal] = None
    smoothed_close: Optional[Decimal] = None
    momentum_raw: Optional[Decimal] = None
    momentum_norm: Optional[Decimal] = None
    momentum_delta: Optional[Decimal] = None
    momentum_acceleration: Optional[Decimal] = None
    volume_ratio: Optional[Decimal] = None
    signed_impulse: Optional[Decimal] = None
    smoothed_signed_impulse: Optional[Decimal] = None
    impulse_delta: Optional[Decimal] = None


@dataclass(frozen=True)
class SignalEvaluationRecord:
    bar_id: str
    experiment_run_id: int
    created_at: datetime
    bull_snap_raw: bool
    bear_snap_raw: bool
    asia_vwap_reclaim_raw: bool
    momentum_compressing_up: bool
    momentum_turning_positive: bool
    momentum_compressing_down: bool
    momentum_turning_negative: bool
    filter_pass_long: bool
    filter_pass_short: bool
    trigger_long_math: bool
    trigger_short_math: bool
    warmup_complete: bool = False
    compression_long: bool = False
    reclaim_long: bool = False
    separation_long: bool = False
    structure_long_candidate: bool = False
    compression_short: bool = False
    failure_short: bool = False
    separation_short: bool = False
    structure_short_candidate: bool = False
    signal_eval_id: Optional[int] = None
    quality_score_long: Optional[Decimal] = None
    quality_score_short: Optional[Decimal] = None
    size_recommendation_long: Optional[Decimal] = None
    size_recommendation_short: Optional[Decimal] = None


@dataclass(frozen=True)
class TradeOutcomeRecord:
    experiment_run_id: int
    entry_bar_id: str
    ticker: str
    timeframe: str
    side: str
    entry_reason: str
    entry_price: Decimal
    size: Decimal
    created_at: datetime
    trade_id: Optional[int] = None
    exit_bar_id: Optional[str] = None
    entry_family: Optional[str] = None
    exit_price: Optional[Decimal] = None
    bars_held: Optional[int] = None
    pnl: Optional[Decimal] = None
    mae: Optional[Decimal] = None
    mfe: Optional[Decimal] = None
    exit_reason: Optional[str] = None
    quality_score_at_entry: Optional[Decimal] = None
    size_recommendation_at_entry: Optional[Decimal] = None
