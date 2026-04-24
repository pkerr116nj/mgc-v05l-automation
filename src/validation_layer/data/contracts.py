"""Canonical contracts for features, backtests, and optimization runs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Sequence


def _coerce_mapping(mapping: Mapping[str, Any] | None) -> dict[str, Any]:
    return dict(mapping or {})


def _coerce_tuple(items: Sequence[Any] | None) -> tuple[Any, ...]:
    return tuple(items or ())


@dataclass(frozen=True)
class PriceBar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    session_label: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _coerce_mapping(self.metadata))


@dataclass(frozen=True)
class TradeRecord:
    entry_time: datetime
    exit_time: datetime
    direction: str
    qty: float
    gross_pnl: float
    net_pnl: float
    mae: float
    mfe: float
    holding_bars: int
    holding_minutes: float
    session_label: str | None = None
    validation_metadata: TradeValidationMetadata | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.exit_time < self.entry_time:
            raise ValueError("TradeRecord exit_time must be greater than or equal to entry_time.")
        if self.direction not in {"long", "short"}:
            raise ValueError("TradeRecord direction must be 'long' or 'short'.")
        object.__setattr__(self, "metadata", _coerce_mapping(self.metadata))


@dataclass(frozen=True)
class EquityPoint:
    timestamp: datetime
    equity: float
    session_label: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _coerce_mapping(self.metadata))


@dataclass(frozen=True)
class PositionPoint:
    timestamp: datetime
    position: float
    exposure: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", _coerce_mapping(self.metadata))


@dataclass(frozen=True)
class ModelProvenance:
    model_name: str
    model_version: str
    parameters: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "parameters", _coerce_mapping(self.parameters))


@dataclass(frozen=True)
class ExecutionAssumptions:
    fill_policy: str
    entry_fill_basis: str
    exit_fill_basis: str
    slippage_model: ModelProvenance
    fee_model: ModelProvenance
    execution_mode: str
    conservative_bias: str | None = None
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "notes", _coerce_tuple(self.notes))


@dataclass(frozen=True)
class SplitWindow:
    split_id: str
    fold_index: int | None
    role: str
    train_start: datetime | None = None
    train_end: datetime | None = None
    test_start: datetime | None = None
    test_end: datetime | None = None
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "notes", _coerce_tuple(self.notes))


@dataclass(frozen=True)
class DataProvenance:
    source_type: str
    data_source: str
    data_version: str
    provenance_id: str
    feature_version: str | None = None
    code_version: str | None = None
    coverage_start: datetime | None = None
    coverage_end: datetime | None = None
    artifact_ids: dict[str, str] = field(default_factory=dict)
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "artifact_ids", {str(key): str(value) for key, value in self.artifact_ids.items()})
        object.__setattr__(self, "tags", _coerce_tuple(self.tags))


@dataclass(frozen=True)
class OptimizationLinkage:
    optimization_id: str
    objective_name: str
    chosen_parameter_hash: str
    source_artifact_id: str | None = None
    split_family: str | None = None


@dataclass(frozen=True)
class TradeValidationMetadata:
    trade_id: str
    signal_id: str | None
    setup_family: str
    setup_variant: str | None
    decision_time: datetime | None
    exit_reason: str | None
    execution_model: str | None
    fill_policy: str
    slippage_cost: float | None = None
    fee_cost: float | None = None
    tags: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "tags", _coerce_mapping(self.tags))


@dataclass(frozen=True)
class StrategyBacktestProvenance:
    producer_contract_version: str
    producer_id: str
    producer_family: str
    generated_at: datetime
    study_mode: str
    split_method: str
    split_windows: tuple[SplitWindow, ...]
    execution_assumptions: ExecutionAssumptions
    data_provenance: DataProvenance
    optimization_linkage: OptimizationLinkage | None = None
    artifact_references: dict[str, str] = field(default_factory=dict)
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "split_windows", _coerce_tuple(self.split_windows))
        object.__setattr__(
            self,
            "artifact_references",
            {str(key): str(value) for key, value in self.artifact_references.items()},
        )
        object.__setattr__(self, "tags", _coerce_tuple(self.tags))


@dataclass(frozen=True)
class FeatureSeries:
    name: str
    symbol: str
    timeframe: str
    session_filter: str | None
    timestamps: tuple[datetime, ...]
    values: tuple[float, ...]
    price_context: tuple[PriceBar, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        timestamps = _coerce_tuple(self.timestamps)
        values = _coerce_tuple(self.values)
        price_context = _coerce_tuple(self.price_context)
        if len(timestamps) != len(values):
            raise ValueError("FeatureSeries timestamps and values must have the same length.")
        if not timestamps:
            raise ValueError("FeatureSeries must contain at least one observation.")
        object.__setattr__(self, "timestamps", timestamps)
        object.__setattr__(self, "values", tuple(float(value) for value in values))
        object.__setattr__(self, "price_context", price_context)
        object.__setattr__(self, "metadata", _coerce_mapping(self.metadata))


@dataclass(frozen=True)
class StrategyBacktest:
    strategy_name: str
    symbol: str
    timeframe: str
    parameters: dict[str, Any]
    bar_data: tuple[PriceBar, ...]
    trades: tuple[TradeRecord, ...]
    equity_curve: tuple[EquityPoint, ...]
    position_series: tuple[PositionPoint, ...]
    provenance: StrategyBacktestProvenance | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "parameters", _coerce_mapping(self.parameters))
        object.__setattr__(self, "bar_data", _coerce_tuple(self.bar_data))
        object.__setattr__(self, "trades", _coerce_tuple(self.trades))
        object.__setattr__(self, "equity_curve", _coerce_tuple(self.equity_curve))
        object.__setattr__(self, "position_series", _coerce_tuple(self.position_series))
        object.__setattr__(self, "metadata", _coerce_mapping(self.metadata))
        if not self.equity_curve:
            raise ValueError("StrategyBacktest equity_curve must contain at least one point.")


@dataclass(frozen=True)
class OptimizationTrial:
    parameter_values: dict[str, Any]
    in_sample_metrics: dict[str, float]
    out_of_sample_metrics: dict[str, float]
    split_id: str | None
    rank: int | None
    objective_value: float
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "parameter_values", _coerce_mapping(self.parameter_values))
        object.__setattr__(self, "in_sample_metrics", dict(self.in_sample_metrics))
        object.__setattr__(self, "out_of_sample_metrics", dict(self.out_of_sample_metrics))
        object.__setattr__(self, "metadata", _coerce_mapping(self.metadata))


@dataclass(frozen=True)
class OptimizationRun:
    strategy_name: str
    search_space: dict[str, Any]
    trials: tuple[OptimizationTrial, ...]
    chosen_parameters: dict[str, Any]
    objective_name: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        trials = _coerce_tuple(self.trials)
        if not trials:
            raise ValueError("OptimizationRun trials must contain the optimization path.")
        object.__setattr__(self, "search_space", _coerce_mapping(self.search_space))
        object.__setattr__(self, "trials", trials)
        object.__setattr__(self, "chosen_parameters", _coerce_mapping(self.chosen_parameters))
        object.__setattr__(self, "metadata", _coerce_mapping(self.metadata))


@dataclass(frozen=True)
class ValidationSubject:
    feature_series: FeatureSeries | None = None
    strategy_backtest: StrategyBacktest | None = None
    optimization_run: OptimizationRun | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.feature_series is None and self.strategy_backtest is None and self.optimization_run is None:
            raise ValueError("ValidationSubject requires a feature_series, strategy_backtest, or optimization_run.")
        if self.strategy_backtest is not None and self.optimization_run is not None:
            if self.strategy_backtest.strategy_name != self.optimization_run.strategy_name:
                raise ValueError("ValidationSubject strategy_backtest and optimization_run must reference the same strategy_name.")
        object.__setattr__(self, "metadata", _coerce_mapping(self.metadata))

    @property
    def subject_type(self) -> str:
        return "strategy" if self.strategy_backtest is not None or self.optimization_run is not None else "feature"

    @property
    def subject_name(self) -> str:
        if self.strategy_backtest is not None:
            return self.strategy_backtest.strategy_name
        if self.optimization_run is not None:
            return self.optimization_run.strategy_name
        if self.feature_series is not None:
            return self.feature_series.name
        raise RuntimeError("ValidationSubject is missing a subject.")
