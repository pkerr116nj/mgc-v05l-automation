from __future__ import annotations

from datetime import datetime, timezone

from mgc_v05l.app.mgc_range_compression_break_validation import (
    ValidationHit,
    _classify_variant,
    _control_too_similar,
    _summarize_returns,
)


def _hit(value: float, *, event_id: str = "e1") -> ValidationHit:
    return ValidationHit(
        event_id=event_id,
        variant="base",
        panel="combined_full_dataset",
        decision_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        year=2024,
        session="US",
        direction="LONG",
        forward_return_15m=value / 2,
        forward_return_30m=value / 1.5,
        forward_return_60m=value,
        mfe_15m=max(value, 0.0),
        mfe_30m=max(value, 0.0),
        mfe_60m=max(value, 0.0),
        mae_15m=max(-value, 0.0),
        mae_30m=max(-value, 0.0),
        mae_60m=max(-value, 0.0),
    )


def test_cost_sensitivity_haircuts_returns_by_tick_size() -> None:
    rows = [_hit(0.5, event_id="a"), _hit(-0.2, event_id="b")]

    zero = _summarize_returns(rows, horizon=60, cost_ticks=0)
    three = _summarize_returns(rows, horizon=60, cost_ticks=3)

    assert zero["average_return"] == 0.15
    assert three["average_return"] == -0.15
    assert zero["profit_factor"] == 2.5


def test_control_too_similar_flags_close_control_frequency() -> None:
    assert _control_too_similar(favorable=0.005, control=0.004)
    assert not _control_too_similar(favorable=0.005, control=0.003)


def test_classifier_fails_after_costs_before_validation_candidate() -> None:
    zero = {"average_return": 0.05, "profit_factor": 1.2, "top3_positive_return_share": 0.01}
    three = {"average_return": -0.01, "profit_factor": 0.9, "top3_positive_return_share": 0.01}

    assert (
        _classify_variant(
            occurrence_count=500,
            zero_cost=zero,
            three_cost=three,
            favorable_frequency=0.005,
            adverse_frequency=0.001,
            control_frequency=0.002,
        )
        == "FAILS_AFTER_COSTS"
    )


def test_classifier_accepts_durable_positive_candidate() -> None:
    zero = {"average_return": 0.2, "profit_factor": 1.4, "top3_positive_return_share": 0.01}
    three = {"average_return": 0.05, "profit_factor": 1.1, "top3_positive_return_share": 0.01}

    assert (
        _classify_variant(
            occurrence_count=500,
            zero_cost=zero,
            three_cost=three,
            favorable_frequency=0.006,
            adverse_frequency=0.001,
            control_frequency=0.003,
        )
        == "VALIDATION_CANDIDATE"
    )
