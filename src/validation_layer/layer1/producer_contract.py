"""Formal Layer 1 producer/provenance contract for strategy backtests."""

from __future__ import annotations

from datetime import UTC, datetime

from ..data.contracts import (
    DataProvenance,
    ExecutionAssumptions,
    ModelProvenance,
    OptimizationLinkage,
    SplitWindow,
    StrategyBacktestProvenance,
    TradeValidationMetadata,
)

LAYER1_PRODUCER_CONTRACT_VERSION = "layer1_strategy_backtest_producer_v1"

REQUIRED_LAYER1_PROVENANCE_FIELDS = (
    "producer_contract_version",
    "producer_id",
    "producer_family",
    "generated_at",
    "study_mode",
    "split_method",
    "split_windows",
    "execution_assumptions",
    "data_provenance",
)


def full_sample_split_window(*, start: datetime | None, end: datetime | None, split_id: str = "full_sample") -> SplitWindow:
    return SplitWindow(
        split_id=split_id,
        fold_index=0,
        role="full",
        train_start=start,
        train_end=end,
        test_start=start,
        test_end=end,
    )


def build_execution_assumptions(
    *,
    fill_policy: str,
    entry_fill_basis: str,
    exit_fill_basis: str,
    execution_mode: str,
    slippage_model_name: str,
    slippage_model_version: str,
    slippage_parameters: dict[str, object],
    fee_model_name: str,
    fee_model_version: str,
    fee_parameters: dict[str, object],
    conservative_bias: str | None = None,
    notes: tuple[str, ...] = (),
) -> ExecutionAssumptions:
    return ExecutionAssumptions(
        fill_policy=fill_policy,
        entry_fill_basis=entry_fill_basis,
        exit_fill_basis=exit_fill_basis,
        execution_mode=execution_mode,
        slippage_model=ModelProvenance(slippage_model_name, slippage_model_version, dict(slippage_parameters)),
        fee_model=ModelProvenance(fee_model_name, fee_model_version, dict(fee_parameters)),
        conservative_bias=conservative_bias,
        notes=notes,
    )


def build_data_provenance(
    *,
    source_type: str,
    data_source: str,
    data_version: str,
    provenance_id: str,
    feature_version: str | None = None,
    code_version: str | None = None,
    coverage_start: datetime | None = None,
    coverage_end: datetime | None = None,
    artifact_ids: dict[str, str] | None = None,
    tags: tuple[str, ...] = (),
) -> DataProvenance:
    return DataProvenance(
        source_type=source_type,
        data_source=data_source,
        data_version=data_version,
        provenance_id=provenance_id,
        feature_version=feature_version,
        code_version=code_version,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        artifact_ids=artifact_ids or {},
        tags=tags,
    )


def build_strategy_backtest_provenance(
    *,
    producer_id: str,
    producer_family: str,
    study_mode: str,
    split_method: str,
    split_windows: tuple[SplitWindow, ...],
    execution_assumptions: ExecutionAssumptions,
    data_provenance: DataProvenance,
    optimization_linkage: OptimizationLinkage | None = None,
    artifact_references: dict[str, str] | None = None,
    tags: tuple[str, ...] = (),
    generated_at: datetime | None = None,
) -> StrategyBacktestProvenance:
    return StrategyBacktestProvenance(
        producer_contract_version=LAYER1_PRODUCER_CONTRACT_VERSION,
        producer_id=producer_id,
        producer_family=producer_family,
        generated_at=generated_at or datetime.now(UTC),
        study_mode=study_mode,
        split_method=split_method,
        split_windows=split_windows,
        execution_assumptions=execution_assumptions,
        data_provenance=data_provenance,
        optimization_linkage=optimization_linkage,
        artifact_references=artifact_references or {},
        tags=tags,
    )


def build_trade_validation_metadata(
    *,
    trade_id: str,
    setup_family: str,
    fill_policy: str,
    signal_id: str | None = None,
    setup_variant: str | None = None,
    decision_time: datetime | None = None,
    exit_reason: str | None = None,
    execution_model: str | None = None,
    slippage_cost: float | None = None,
    fee_cost: float | None = None,
    tags: dict[str, object] | None = None,
) -> TradeValidationMetadata:
    return TradeValidationMetadata(
        trade_id=trade_id,
        signal_id=signal_id,
        setup_family=setup_family,
        setup_variant=setup_variant,
        decision_time=decision_time,
        exit_reason=exit_reason,
        execution_model=execution_model,
        fill_policy=fill_policy,
        slippage_cost=slippage_cost,
        fee_cost=fee_cost,
        tags=tags or {},
    )
