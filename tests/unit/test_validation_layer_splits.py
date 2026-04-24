from __future__ import annotations

from validation_layer.config.defaults import build_debug_config
from validation_layer.validation.cross_validation import generate_cross_validation_splits
from validation_layer.validation.walkforward import generate_walkforward_splits


def test_walkforward_split_generation_covers_series_without_overlap() -> None:
    config = build_debug_config()
    splits = generate_walkforward_splits(30, config)

    assert splits
    assert splits[0].start_index == 0
    assert splits[-1].end_index == 29
    assert all(left.end_index < right.start_index for left, right in zip(splits, splits[1:]))


def test_cross_validation_split_generation_produces_contiguous_folds() -> None:
    config = build_debug_config()
    splits = generate_cross_validation_splits(30, config)

    assert splits
    assert splits[0].start_index == 0
    assert splits[-1].end_index == 29
    assert all(left.end_index < right.start_index for left, right in zip(splits, splits[1:]))
