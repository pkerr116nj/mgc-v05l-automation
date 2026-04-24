"""Layer 1 provenance prerequisite checks for Layer 2 validation."""

from __future__ import annotations

from ..config.schemas import ValidationConfig
from ..data.contracts import StrategyBacktest, StrategyBacktestProvenance
from ..reporting.models import ValidationModuleResult
from ..utils.stats import safe_divide
from .producer_contract import REQUIRED_LAYER1_PROVENANCE_FIELDS


def _missing_provenance_fields(provenance: StrategyBacktestProvenance | None) -> list[str]:
    if provenance is None:
        return list(REQUIRED_LAYER1_PROVENANCE_FIELDS)
    missing: list[str] = []
    for field_name in REQUIRED_LAYER1_PROVENANCE_FIELDS:
        if getattr(provenance, field_name, None) in (None, "", ()):
            missing.append(field_name)
    if not provenance.execution_assumptions.slippage_model.model_name:
        missing.append("execution_assumptions.slippage_model.model_name")
    if not provenance.execution_assumptions.fee_model.model_name:
        missing.append("execution_assumptions.fee_model.model_name")
    if not provenance.data_provenance.data_version:
        missing.append("data_provenance.data_version")
    if not provenance.data_provenance.provenance_id:
        missing.append("data_provenance.provenance_id")
    return missing


def _missing_trade_metadata_fields(strategy: StrategyBacktest) -> dict[str, list[str]]:
    missing: dict[str, list[str]] = {}
    fill_policy = (
        strategy.provenance.execution_assumptions.fill_policy if strategy.provenance is not None else None
    )
    for trade in strategy.trades:
        fields: list[str] = []
        if trade.validation_metadata is None:
            fields.append("validation_metadata")
        else:
            if not trade.validation_metadata.trade_id:
                fields.append("trade_id")
            if not trade.validation_metadata.setup_family:
                fields.append("setup_family")
            if trade.validation_metadata.decision_time is None:
                fields.append("decision_time")
            if not trade.validation_metadata.fill_policy:
                fields.append("fill_policy")
            if fill_policy is not None and trade.validation_metadata.fill_policy != fill_policy:
                fields.append("fill_policy_mismatch")
        if fields:
            missing[trade.entry_time.isoformat()] = fields
    return missing


def run_layer1_prerequisites_module(strategy: StrategyBacktest, config: ValidationConfig) -> ValidationModuleResult:
    missing_provenance = _missing_provenance_fields(strategy.provenance)
    missing_trade_metadata = _missing_trade_metadata_fields(strategy)
    total_required_checks = 2 + len(strategy.trades)
    missing_check_count = int(bool(missing_provenance)) + len(missing_trade_metadata)
    coverage = 1.0 - safe_divide(missing_check_count, max(1, total_required_checks), default=1.0)
    insufficient = bool(missing_provenance or missing_trade_metadata)
    status = "pass"
    summary = "Layer 1 provenance contract is complete and audit-ready."
    recommendations: list[str] = []
    if insufficient:
        status = "warn"
        summary = "Insufficient evidence: Layer 1 provenance is missing required audit metadata."
        recommendations.append("Do not treat this backtest as comparable or promotion-ready until the Layer 1 contract is complete.")
    return ValidationModuleResult(
        module_name="layer1_prerequisites",
        status=status,
        summary=summary,
        metrics={
            "layer1_provenance_score": round(coverage, config.report_precision),
            "insufficient_evidence": insufficient,
            "missing_provenance_field_count": len(missing_provenance),
            "missing_trade_metadata_count": len(missing_trade_metadata),
        },
        diagnostics={
            "missing_provenance_fields": missing_provenance,
            "missing_trade_metadata": missing_trade_metadata,
            "required_contract_fields": list(REQUIRED_LAYER1_PROVENANCE_FIELDS),
        },
        artifacts={},
        recommendations=recommendations,
    )
