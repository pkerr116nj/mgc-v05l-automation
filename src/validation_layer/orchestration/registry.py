"""Registry of validation modules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from ..config.schemas import ValidationConfig
from ..data.contracts import FeatureSeries, OptimizationRun, StrategyBacktest
from ..features.entropy import run_entropy_module
from ..features.stationarity import run_stationarity_module
from ..layer1.prerequisites import run_layer1_prerequisites_module
from ..optimization.parameter_surface import run_parameter_surface_module
from ..optimization.cscv_pbo import run_cscv_pbo_module
from ..optimization.selection_bias import run_selection_bias_module
from ..optimization.train_bias import run_training_bias_module
from ..performance.metrics import run_canonical_performance_module
from ..performance.trade_normalization import run_trade_normalization_module
from ..reporting.models import ValidationModuleResult
from ..robustness.bootstrap import run_bootstrap_module
from ..robustness.drawdown import run_drawdown_module
from ..robustness.market_permutation import run_market_permutation_module
from ..robustness.monte_carlo import run_monte_carlo_module
from ..validation.cross_validation import run_cross_validation_module
from ..validation.walkforward import run_walkforward_module


@dataclass(frozen=True)
class ModuleRunner:
    module_name: str
    runner: Callable[[object, ValidationConfig], ValidationModuleResult]


def feature_runners(config: ValidationConfig) -> tuple[ModuleRunner, ...]:
    runners: list[ModuleRunner] = []
    if config.module_flags.stationarity:
        runners.append(ModuleRunner("stationarity", run_stationarity_module))
    if config.module_flags.entropy:
        runners.append(ModuleRunner("entropy", run_entropy_module))
    return tuple(runners)


def strategy_runners(config: ValidationConfig) -> tuple[ModuleRunner, ...]:
    runners: list[ModuleRunner] = []
    if config.module_flags.layer1_prerequisites:
        runners.append(ModuleRunner("layer1_prerequisites", run_layer1_prerequisites_module))
    if config.module_flags.canonical_performance:
        runners.append(ModuleRunner("canonical_performance", run_canonical_performance_module))
    if config.module_flags.drawdown:
        runners.append(ModuleRunner("drawdown", run_drawdown_module))
    if config.module_flags.trade_normalization:
        runners.append(ModuleRunner("trade_normalization", run_trade_normalization_module))
    if config.module_flags.walkforward:
        runners.append(ModuleRunner("walkforward", run_walkforward_module))
    if config.module_flags.cross_validation:
        runners.append(ModuleRunner("cross_validation", run_cross_validation_module))
    if config.module_flags.bootstrap:
        runners.append(ModuleRunner("bootstrap", run_bootstrap_module))
    if config.module_flags.monte_carlo:
        runners.append(ModuleRunner("monte_carlo", run_monte_carlo_module))
    if config.module_flags.market_permutation or config.module_flags.confidence_tests:
        runners.append(ModuleRunner("market_permutation", run_market_permutation_module))
    return tuple(runners)


def optimization_runners(config: ValidationConfig) -> tuple[ModuleRunner, ...]:
    runners: list[ModuleRunner] = []
    if config.module_flags.parameter_surface:
        runners.append(ModuleRunner("parameter_surface", run_parameter_surface_module))
    if config.module_flags.training_bias:
        runners.append(ModuleRunner("train_bias", run_training_bias_module))
    if config.module_flags.selection_bias:
        runners.append(ModuleRunner("selection_bias", run_selection_bias_module))
    if config.module_flags.cscv_pbo:
        runners.append(ModuleRunner("cscv_pbo", run_cscv_pbo_module))
    return tuple(runners)


FeatureRunner = Callable[[FeatureSeries, ValidationConfig], ValidationModuleResult]
StrategyRunner = Callable[[StrategyBacktest, ValidationConfig], ValidationModuleResult]
OptimizationRunner = Callable[[OptimizationRun, ValidationConfig], ValidationModuleResult]
