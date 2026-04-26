from __future__ import annotations

from datetime import UTC, datetime

from mgc_v05l.research.asia_drift.shadow_trading import (
    _build_daily_trade_rows,
    _build_execution_policy,
    _execution_eligible,
    _simulate_candidate,
)


def test_execution_eligible_requires_favorable_late_asia() -> None:
    assert _execution_eligible(state="TRADE_FAVORABLE", timing_within_asia="LATE_ASIA") is True
    assert _execution_eligible(state="TRADE_NEUTRAL", timing_within_asia="LATE_ASIA") is False
    assert _execution_eligible(state="TRADE_FAVORABLE", timing_within_asia="PRE_LONDON") is False


def test_simulate_candidate_logs_non_eligible_without_trade() -> None:
    row = {
        "candidate_id": "x",
        "decision_ts": "2026-04-01T20:30:00-04:00",
        "instrument": "ES",
        "direction": "LONG",
        "timing_within_asia": "PRE_LONDON",
        "decision_close": 100.0,
    }
    raw_series = {"timestamps": [], "closes": [], "highs": [], "lows": []}
    payload = _simulate_candidate(
        row=row,
        state="TRADE_NEUTRAL",
        raw_series=raw_series,
        execution_eligible=False,
        skip_reason="state_or_timing_not_eligible",
    )
    assert payload["simulated_trade"] is False
    assert payload["exit_reason"] == "NOT_SIMULATED"
    assert payload["skip_reason"] == "state_or_timing_not_eligible"


def test_execution_policy_uses_first_qualifying_signal_per_instrument_session() -> None:
    candidate_rows = [
        {
            "candidate_id": "a",
            "environment_key": "fav",
            "instrument": "ES",
            "decision_ts": datetime(2026, 4, 1, 20, 30, tzinfo=UTC).isoformat(),
            "timing_within_asia": "LATE_ASIA",
        },
        {
            "candidate_id": "b",
            "environment_key": "fav",
            "instrument": "ES",
            "decision_ts": datetime(2026, 4, 1, 20, 35, tzinfo=UTC).isoformat(),
            "timing_within_asia": "LATE_ASIA",
        },
    ]
    state_lookup = {"fav": "TRADE_FAVORABLE"}
    policy = _build_execution_policy(candidate_rows, state_lookup=state_lookup)
    assert policy["a"]["execution_eligible"] is True
    assert policy["b"]["execution_eligible"] is False
    assert policy["b"]["skip_reason"] == "first_signal_only_per_instrument_session"


def test_execution_policy_blocks_overlapping_trade_before_later_session() -> None:
    candidate_rows = [
        {
            "candidate_id": "a",
            "environment_key": "fav",
            "instrument": "ES",
            "decision_ts": "2026-04-01T17:30:00-04:00",
            "timing_within_asia": "LATE_ASIA",
        },
        {
            "candidate_id": "b",
            "environment_key": "fav",
            "instrument": "ES",
            "decision_ts": "2026-04-01T18:15:00-04:00",
            "timing_within_asia": "LATE_ASIA",
        },
    ]
    state_lookup = {"fav": "TRADE_FAVORABLE"}
    policy = _build_execution_policy(candidate_rows, state_lookup=state_lookup)
    assert policy["a"]["execution_eligible"] is True
    assert policy["b"]["execution_eligible"] is False
    assert policy["b"]["skip_reason"] == "overlapping_trade_not_allowed"


def test_build_daily_trade_rows_accumulates_return_and_win_rate() -> None:
    rows = [
        {
            "trade_date": "2026-04-01",
            "timestamp": datetime.fromisoformat("2026-04-01T20:30:00-04:00").isoformat(),
            "return_points": 5.0,
        },
        {
            "trade_date": "2026-04-01",
            "timestamp": datetime.fromisoformat("2026-04-01T21:30:00-04:00").isoformat(),
            "return_points": -2.0,
        },
        {
            "trade_date": "2026-04-02",
            "timestamp": datetime.fromisoformat("2026-04-02T20:30:00-04:00").isoformat(),
            "return_points": 1.0,
        },
    ]
    daily = _build_daily_trade_rows(rows)
    assert daily[0]["trade_count"] == 2
    assert daily[0]["total_return_points"] == 3.0
    assert daily[1]["cumulative_return_points"] == 4.0
