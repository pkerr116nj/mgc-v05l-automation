"""Default config profiles for the validation pipeline."""

from __future__ import annotations

from dataclasses import replace

from .schemas import ModuleFlags, SplitConfig, ThresholdConfig, ValidationConfig


def build_default_config() -> ValidationConfig:
    return ValidationConfig()


def build_strict_config() -> ValidationConfig:
    return ValidationConfig(
        thresholds=ThresholdConfig(
            feature_stability_min=0.55,
            entropy_min=0.48,
            walkforward_min=0.55,
            cross_validation_min=0.55,
            split_dependence_min=0.52,
            capital_efficiency_min=0.45,
            time_efficiency_min=0.45,
            drawdown_min=0.45,
            pain_profile_min=0.45,
            max_fold_failures=0,
            minimum_trade_count=20,
            minimum_feature_length=60,
            probation_composite_min=0.65,
            promote_composite_min=0.84,
        ),
        split_config=SplitConfig(
            walkforward_folds=5,
            cross_validation_folds=5,
            min_fold_size=24,
            min_train_fraction=0.55,
            min_test_fraction=0.18,
        ),
    )


def build_debug_config() -> ValidationConfig:
    return ValidationConfig(
        thresholds=replace(
            ThresholdConfig(),
            minimum_trade_count=6,
            minimum_feature_length=20,
        ),
        split_config=SplitConfig(
            walkforward_folds=3,
            cross_validation_folds=3,
            min_fold_size=8,
            min_train_fraction=0.45,
            min_test_fraction=0.20,
        ),
        module_flags=replace(
            ModuleFlags(),
            split_comparison=True,
        ),
        stationarity_windows=(6, 12),
        entropy_bins=4,
    )
