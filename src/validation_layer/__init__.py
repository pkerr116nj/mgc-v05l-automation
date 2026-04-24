"""Unified validation layer for futures research governance."""

from .config.defaults import build_debug_config, build_default_config, build_strict_config
from .config.schemas import ValidationConfig
from .data.contracts import (
    DataProvenance,
    ExecutionAssumptions,
    FeatureSeries,
    ModelProvenance,
    OptimizationRun,
    OptimizationLinkage,
    SplitWindow,
    StrategyBacktest,
    StrategyBacktestProvenance,
    TradeValidationMetadata,
    ValidationSubject,
)
from .layer1.atp_optimization import (
    AtpPromotionOptimizationBundle,
    build_optimization_run_from_atp_window_history,
    build_strategy_backtest_from_atp_candidate_history,
    rerun_atp_promotion_add_validation_bundle,
)
from .layer1.asia_london_family import (
    AsiaLondonCandidateBundle,
    AsiaLondonFamilySource,
    build_asia_london_candidate_bundle,
    build_asia_london_optimization_run,
    load_asia_london_family_source,
)
from .layer1.pilot import (
    run_approved_quant_layer1_pilot_rerun,
    run_asia_london_participation_family_pilot_rerun,
    run_atp_layer1_pilot_rerun,
    run_atp_promotion_add_optimization_pilot_rerun,
    run_layer1_pilot_rerun,
)
from .orchestration.evidence_building import run_targeted_evidence_building_assessment
from .orchestration.asia_london_simplification import run_asia_london_simplification_pass
from .orchestration.live_universe_audit import build_live_candidate_universe_inventory, run_live_candidate_universe_audit
from .orchestration.pipeline import run_validation_pipeline
from .reporting.json_report import render_json_report
from .reporting.markdown_report import render_markdown_report
from .reporting.models import ValidationModuleResult, ValidationReport

__all__ = [
    "FeatureSeries",
    "DataProvenance",
    "ExecutionAssumptions",
    "ModelProvenance",
    "OptimizationRun",
    "OptimizationLinkage",
    "SplitWindow",
    "StrategyBacktest",
    "StrategyBacktestProvenance",
    "AtpPromotionOptimizationBundle",
    "AsiaLondonCandidateBundle",
    "AsiaLondonFamilySource",
    "TradeValidationMetadata",
    "ValidationConfig",
    "ValidationModuleResult",
    "ValidationReport",
    "ValidationSubject",
    "build_asia_london_candidate_bundle",
    "build_asia_london_optimization_run",
    "build_optimization_run_from_atp_window_history",
    "build_strategy_backtest_from_atp_candidate_history",
    "build_debug_config",
    "build_default_config",
    "build_strict_config",
    "load_asia_london_family_source",
    "render_json_report",
    "render_markdown_report",
    "build_live_candidate_universe_inventory",
    "run_asia_london_simplification_pass",
    "run_approved_quant_layer1_pilot_rerun",
    "run_asia_london_participation_family_pilot_rerun",
    "run_atp_layer1_pilot_rerun",
    "run_atp_promotion_add_optimization_pilot_rerun",
    "run_live_candidate_universe_audit",
    "run_layer1_pilot_rerun",
    "run_targeted_evidence_building_assessment",
    "run_validation_pipeline",
    "rerun_atp_promotion_add_validation_bundle",
]
