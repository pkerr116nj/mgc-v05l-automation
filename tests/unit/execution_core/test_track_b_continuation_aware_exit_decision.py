from __future__ import annotations

from datetime import datetime, timezone

from mgc_v05l.execution_core.track_b_continuation_aware_exit_decision import (
    EXIT_DECAY_DETECTED,
    EXIT_HARD_MAX_DURATION,
    EXIT_LIFECYCLE_UNSAFE,
    EXIT_REVERSAL_DETECTED,
    EXIT_SAFE_STATE_OVERRIDE,
    HOLD_CONTINUATION_CONFIRMED,
    HOLD_MINIMUM_WINDOW,
    INSUFFICIENT_DATA_HOLD_OR_FALLBACK,
    TIME_PLUS_CONTINUATION_EXIT_V1,
    build_time_plus_continuation_exit_decision,
)


def _now() -> datetime:
    return datetime(2026, 5, 24, 14, 0, tzinfo=timezone.utc)


def _long_aligned_candles() -> list[dict[str, str]]:
    return [
        {"open": "100.0", "high": "101.4", "low": "99.8", "close": "101.1"},
        {"open": "101.1", "high": "102.2", "low": "100.9", "close": "102.0"},
        {"open": "102.0", "high": "103.0", "low": "101.8", "close": "102.8"},
    ]


def _short_reversal_candles() -> list[dict[str, str]]:
    return [
        {"open": "100.0", "high": "101.2", "low": "99.8", "close": "101.0"},
        {"open": "101.0", "high": "102.1", "low": "100.7", "close": "101.9"},
    ]


def _base(**overrides: object) -> dict:
    payload: dict[str, object] = {
        "strategy_id": "asian_drift_v1",
        "symbol": "MGC",
        "side": "LONG",
        "entry_time": "2026-05-24T13:30:00+00:00",
        "current_time": _now(),
        "completed_5m_candles": _long_aligned_candles(),
        "position_age_minutes": 15,
        "mfe": "2.0",
        "mae": "-0.3",
        "unrealized_pnl": "1.4",
        "microtrend_state": {"trend": "ALIGNED_CONTINUATION"},
        "participation_state": {"participation": "STRONG_PARTICIPATING"},
        "safe_state_classification": "SAFE_STATE_NORMAL",
        "lifecycle_reconciliation_classification": "CLEAN",
    }
    payload.update(overrides)
    return payload


def test_minimum_hold_returns_hold_minimum_window() -> None:
    decision = build_time_plus_continuation_exit_decision(**_base(position_age_minutes=5))

    assert decision["exit_policy_id"] == TIME_PLUS_CONTINUATION_EXIT_V1
    assert decision["exit_state"] == HOLD_MINIMUM_WINDOW
    assert decision["should_request_close"] is False
    assert decision["close_intent_preview"]["would_submit"] is False


def test_strong_aligned_pressure_returns_hold_continuation_confirmed() -> None:
    decision = build_time_plus_continuation_exit_decision(**_base())

    assert decision["exit_state"] == HOLD_CONTINUATION_CONFIRMED
    assert decision["continuation_quality_state"] == "STRONG_ALIGNED_CONTINUATION"
    assert decision["should_request_close"] is False


def test_decaying_pressure_returns_exit_decay_preview() -> None:
    decision = build_time_plus_continuation_exit_decision(
        **_base(
            completed_5m_candles=[
                {"open": "100.0", "high": "100.5", "low": "99.8", "close": "100.1"},
                {"open": "100.1", "high": "100.3", "low": "99.9", "close": "100.0"},
            ],
            microtrend_state={"trend": "DECAYING"},
            participation_state={"participation": "FADING"},
            mfe="2.0",
            unrealized_pnl="0.2",
            position_age_minutes=15,
        )
    )

    assert decision["exit_state"] == EXIT_DECAY_DETECTED
    assert decision["close_intent_preview"]["would_close_reason"] == EXIT_DECAY_DETECTED
    assert decision["should_request_close"] is False


def test_reversal_pressure_returns_exit_reversal_preview() -> None:
    decision = build_time_plus_continuation_exit_decision(
        **_base(
            strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            side="SHORT",
            completed_5m_candles=_short_reversal_candles(),
            microtrend_state={"trend": "REVERSAL"},
            participation_state={"participation": "ADVERSE"},
            unrealized_pnl="-1.0",
            position_age_minutes=15,
        )
    )

    assert decision["exit_state"] == EXIT_REVERSAL_DETECTED
    assert decision["close_intent_preview"]["would_order_action"] == "BUY"
    assert decision["should_request_close"] is False


def test_hard_max_age_returns_exit_hard_max_preview() -> None:
    decision = build_time_plus_continuation_exit_decision(**_base(position_age_minutes=35))

    assert decision["exit_state"] == EXIT_HARD_MAX_DURATION
    assert decision["hard_max_remaining_minutes"] == "0"
    assert decision["should_request_close"] is False


def test_unsafe_safe_state_returns_safe_state_override_preview() -> None:
    decision = build_time_plus_continuation_exit_decision(
        **_base(safe_state_classification="SAFE_STATE_HARD_HOLD")
    )

    assert decision["exit_state"] == EXIT_SAFE_STATE_OVERRIDE
    assert decision["broker_mutation_allowed"] is False
    assert decision["should_request_close"] is False


def test_lifecycle_unsafe_returns_lifecycle_unsafe_preview() -> None:
    decision = build_time_plus_continuation_exit_decision(
        **_base(lifecycle_reconciliation_classification="REVIEW_REQUIRED")
    )

    assert decision["exit_state"] == EXIT_LIFECYCLE_UNSAFE
    assert decision["not_lifecycle_authority"] is True
    assert decision["should_request_close"] is False


def test_insufficient_candles_returns_hold_or_fallback() -> None:
    decision = build_time_plus_continuation_exit_decision(**_base(completed_5m_candles=[]))

    assert decision["exit_state"] == INSUFFICIENT_DATA_HOLD_OR_FALLBACK
    assert "At least two completed 5m candles" in decision["reason"]
    assert decision["should_request_close"] is False


def test_dashboard_projections_are_not_consumed() -> None:
    decision = build_time_plus_continuation_exit_decision(**_base())

    assert decision["dashboard_projection_consumed"] is False
    assert decision["dry_run_only"] is True
    assert decision["not_order_authority"] is True
