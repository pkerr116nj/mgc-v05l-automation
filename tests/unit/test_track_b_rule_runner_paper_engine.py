from __future__ import annotations

from datetime import UTC, datetime, timedelta

from mgc_v05l.app.track_b_rule_runner_paper_engine import _state_freshness_blocker


def test_rule_runner_paper_engine_requires_timestamp_coherent_input() -> None:
    current = datetime(2026, 5, 27, 8, 0, tzinfo=UTC)

    blocker = _state_freshness_blocker(
        {"candle_timestamp": (current - timedelta(minutes=20)).isoformat()},
        current_bar_end=current,
        config={"timestamp_tolerance_seconds": 360},
    )

    assert blocker == "track_b_rule_runner_input_event_not_timestamp_coherent"


def test_rule_runner_paper_engine_accepts_fresh_timestamp_coherent_input() -> None:
    current = datetime(2026, 5, 27, 8, 0, tzinfo=UTC)

    blocker = _state_freshness_blocker(
        {"candle_timestamp": (current - timedelta(minutes=1)).isoformat()},
        current_bar_end=current,
        config={"timestamp_tolerance_seconds": 360},
    )

    assert blocker is None
