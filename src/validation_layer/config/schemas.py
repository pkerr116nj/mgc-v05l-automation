"""Typed configuration contracts for the validation pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SessionFilter:
    name: str
    start_hour: int
    start_minute: int
    end_hour: int
    end_minute: int


@dataclass(frozen=True)
class ModuleFlags:
    layer1_prerequisites: bool = True
    stationarity: bool = True
    entropy: bool = True
    canonical_performance: bool = True
    drawdown: bool = True
    walkforward: bool = True
    cross_validation: bool = True
    split_comparison: bool = True
    trade_normalization: bool = True
    training_bias: bool = True
    selection_bias: bool = True
    parameter_surface: bool = True
    cscv_pbo: bool = True
    bootstrap: bool = True
    monte_carlo: bool = True
    confidence_tests: bool = False
    market_permutation: bool = True
    overlap: bool = False


@dataclass(frozen=True)
class ThresholdConfig:
    feature_stability_min: float = 0.45
    entropy_min: float = 0.40
    walkforward_min: float = 0.45
    cross_validation_min: float = 0.45
    split_dependence_min: float = 0.40
    capital_efficiency_min: float = 0.35
    time_efficiency_min: float = 0.35
    drawdown_min: float = 0.35
    pain_profile_min: float = 0.35
    max_fold_failures: int = 1
    minimum_trade_count: int = 12
    minimum_feature_length: int = 40
    probation_composite_min: float = 0.58
    promote_composite_min: float = 0.78


@dataclass(frozen=True)
class SplitConfig:
    walkforward_folds: int = 4
    cross_validation_folds: int = 4
    min_fold_size: int = 20
    min_train_fraction: float = 0.50
    min_test_fraction: float = 0.15


@dataclass(frozen=True)
class RegimeConfig:
    bucket_count: int = 3
    labels: tuple[str, ...] = ("early", "mid", "late")


@dataclass(frozen=True)
class RobustnessSamplingConfig:
    bootstrap_samples: int = 250
    monte_carlo_iterations: int = 250
    random_seed: int = 17


@dataclass(frozen=True)
class ValidationConfig:
    module_flags: ModuleFlags = field(default_factory=ModuleFlags)
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    split_config: SplitConfig = field(default_factory=SplitConfig)
    regime_config: RegimeConfig = field(default_factory=RegimeConfig)
    robustness: RobustnessSamplingConfig = field(default_factory=RobustnessSamplingConfig)
    stationarity_windows: tuple[int, ...] = (10, 25)
    entropy_bins: int = 5
    session_filters: tuple[SessionFilter, ...] = (
        SessionFilter("asia", 18, 0, 23, 59),
        SessionFilter("london", 0, 0, 8, 29),
        SessionFilter("us", 8, 30, 16, 59),
    )
    default_margin_per_contract: float = 5000.0
    report_precision: int = 4
    enable_optional_stationarity_tests: bool = False
