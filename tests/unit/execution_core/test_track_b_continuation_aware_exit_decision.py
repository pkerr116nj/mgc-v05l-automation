from __future__ import annotations

import json
from datetime import datetime, timezone

from mgc_v05l.execution_core.track_b_continuation_aware_exit_decision import (
    ASIAN_DRIFT_CONTINUATION_LONG_LEASH_V1,
    ASIAN_DRIFT_TRUE_DRIFT_HOLD_V1,
    ASIA_EARLY_PAUSE_RESUME_SHORT_MEDIUM_LEASH_V1,
    BREAKOUT_RETEST_CONTINUATION_HOLD_V1,
    EXIT_DECAY_DETECTED,
    EXIT_HARD_MAX_DURATION,
    EXIT_LIFECYCLE_UNSAFE,
    EXIT_REVERSAL_DETECTED,
    EXIT_SAFE_STATE_OVERRIDE,
    HOLD_CONTINUATION_CONFIRMED,
    HOLD_MINIMUM_WINDOW,
    INSUFFICIENT_DATA_HOLD_OR_FALLBACK,
    SNAP_TURN_FAST_DECAY_V1,
    TIME_PLUS_CONTINUATION_EXIT_V1,
    build_time_plus_continuation_exit_decision,
    write_continuation_aware_exit_preview_artifacts,
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
    assert decision["exit_profile_id"] == ASIAN_DRIFT_CONTINUATION_LONG_LEASH_V1
    assert decision["hard_max_hold_minutes"] == 120
    assert decision["decay_sensitivity"] == "LOW_MODERATE"
    assert decision["continuation_quality_state"] == "STRONG_ALIGNED_CONTINUATION"
    assert decision["should_request_close"] is False


def test_asian_drift_long_leash_quiet_aligned_drift_holds_not_decay() -> None:
    decision = build_time_plus_continuation_exit_decision(
        **_base(
            position_age_minutes=80,
            completed_5m_candles=[
                {"open": "100.00", "high": "100.25", "low": "99.95", "close": "100.12"},
                {"open": "100.12", "high": "100.32", "low": "100.08", "close": "100.22"},
                {"open": "100.22", "high": "100.44", "low": "100.18", "close": "100.34"},
            ],
            microtrend_state={"trend": "ALIGNED_LOW_VOL_DRIFT"},
            participation_state={"participation": "QUIET_ALIGNED_DRIFT"},
            mfe="1.2",
            mae="-0.2",
            unrealized_pnl="0.8",
        )
    )

    assert decision["exit_profile_id"] == ASIAN_DRIFT_CONTINUATION_LONG_LEASH_V1
    assert decision["exit_state"] == HOLD_CONTINUATION_CONFIRMED
    assert decision["should_request_close"] is False


def test_asian_drift_true_drift_profile_allows_360_minute_hard_max_in_dry_run() -> None:
    decision = build_time_plus_continuation_exit_decision(
        **_base(
            family_profile_id=ASIAN_DRIFT_TRUE_DRIFT_HOLD_V1,
            position_age_minutes=240,
            microtrend_state={"trend": "ALIGNED_LOW_VOL_DRIFT"},
            participation_state={"participation": "QUIET_ALIGNED_DRIFT"},
            mfe="3.0",
            mae="-0.4",
            unrealized_pnl="2.2",
        )
    )

    assert decision["exit_profile_id"] == ASIAN_DRIFT_TRUE_DRIFT_HOLD_V1
    assert decision["paper_experimental_profile"] is True
    assert decision["hard_max_hold_minutes"] == 360
    assert decision["hard_max_remaining_minutes"] == "120"
    assert decision["exit_state"] == HOLD_CONTINUATION_CONFIRMED


def test_remaining_p0_strategies_have_preview_profile_mapping_without_runtime_exit_enablement() -> None:
    cases = {
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1": BREAKOUT_RETEST_CONTINUATION_HOLD_V1,
        "MNQ_FIRST_BEAR_SNAP_TURN_V1": SNAP_TURN_FAST_DECAY_V1,
        "MNQ_FIRST_BULL_SNAP_TURN_V1": SNAP_TURN_FAST_DECAY_V1,
    }

    for strategy_id, expected_profile in cases.items():
        decision = build_time_plus_continuation_exit_decision(
            strategy_id=strategy_id,
            symbol="MNQ" if strategy_id.startswith("MNQ") else "MGC",
            side="SHORT" if "BEAR" in strategy_id else "LONG",
            current_time=_now(),
            completed_5m_candles=(),
            position_age_minutes=12,
        )

        assert decision["exit_profile_id"] == expected_profile
        assert decision["exit_state"] == INSUFFICIENT_DATA_HOLD_OR_FALLBACK
        assert decision["dry_run_only"] is True
        assert decision["should_request_close"] is False
        assert decision["close_intent_preview"]["would_submit"] is False


def test_asian_drift_true_drift_exits_on_firm_reversal_despite_long_leash() -> None:
    decision = build_time_plus_continuation_exit_decision(
        **_base(
            family_profile_id=ASIAN_DRIFT_TRUE_DRIFT_HOLD_V1,
            position_age_minutes=120,
            completed_5m_candles=[
                {"open": "102.0", "high": "102.1", "low": "100.7", "close": "100.9"},
                {"open": "100.9", "high": "101.0", "low": "99.8", "close": "100.0"},
            ],
            microtrend_state={"trend": "FIRM_REVERSAL"},
            participation_state={"participation": "ADVERSE"},
            unrealized_pnl="-1.5",
        )
    )

    assert decision["exit_profile_id"] == ASIAN_DRIFT_TRUE_DRIFT_HOLD_V1
    assert decision["exit_state"] == EXIT_REVERSAL_DETECTED
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


def test_pause_resume_short_flags_failed_continuation_sooner() -> None:
    decision = build_time_plus_continuation_exit_decision(
        **_base(
            strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            side="SHORT",
            family_profile_id=ASIA_EARLY_PAUSE_RESUME_SHORT_MEDIUM_LEASH_V1,
            completed_5m_candles=[
                {"open": "100.0", "high": "100.4", "low": "99.8", "close": "100.1"},
                {"open": "100.1", "high": "100.5", "low": "99.9", "close": "100.3"},
            ],
            microtrend_state={"trend": "FAILED_CONTINUATION"},
            participation_state={"participation": "FADING"},
            mfe="0.8",
            unrealized_pnl="0.1",
            position_age_minutes=14,
        )
    )

    assert decision["exit_profile_id"] == ASIA_EARLY_PAUSE_RESUME_SHORT_MEDIUM_LEASH_V1
    assert decision["exit_state"] in {EXIT_DECAY_DETECTED, EXIT_REVERSAL_DETECTED}
    assert decision["should_request_close"] is False


def test_hard_max_age_returns_exit_hard_max_preview() -> None:
    decision = build_time_plus_continuation_exit_decision(
        **_base(
            strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            family_profile_id=ASIA_EARLY_PAUSE_RESUME_SHORT_MEDIUM_LEASH_V1,
            position_age_minutes=30,
        )
    )

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
    assert "completed_5m_candles" in decision["missing_inputs"]
    assert decision["should_request_close"] is False


def test_dashboard_projections_are_not_consumed() -> None:
    decision = build_time_plus_continuation_exit_decision(**_base())

    assert decision["dashboard_projection_consumed"] is False
    assert decision["dry_run_only"] is True
    assert decision["not_order_authority"] is True


def test_diagnostic_artifact_writer_writes_latest_and_jsonl(tmp_path) -> None:  # type: ignore[no-untyped-def]
    decision = build_time_plus_continuation_exit_decision(**_base())
    latest = tmp_path / "latest_continuation_aware_exit_preview.json"
    event_log = tmp_path / "continuation_aware_exit_previews.jsonl"

    result = write_continuation_aware_exit_preview_artifacts(
        preview=decision,
        output_path=latest,
        event_log_path=event_log,
    )

    payload = json.loads(latest.read_text(encoding="utf-8"))
    rows = [json.loads(line) for line in event_log.read_text(encoding="utf-8").splitlines()]
    assert result["event_appended"] is True
    assert payload["dry_run_only"] is True
    assert payload["not_order_authority"] is True
    assert payload["not_lifecycle_authority"] is True
    assert payload["should_request_close"] is False
    assert payload["close_intent_preview"]["would_submit"] is False
    assert rows[-1]["exit_profile_id"] == ASIAN_DRIFT_CONTINUATION_LONG_LEASH_V1
