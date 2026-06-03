from __future__ import annotations

from datetime import UTC, datetime, timedelta

from mgc_v05l.execution_core.track_b_monitoring_honesty import (
    one_shot_monitoring_contract,
    timed_sampling_monitoring_contract,
    validate_monitoring_claim,
)


def test_short_sampling_monitor_does_not_allow_observed_for_15_minutes_claim() -> None:
    started = datetime(2026, 6, 1, 20, 0, tzinfo=UTC)
    contract = timed_sampling_monitoring_contract(
        requested_duration_seconds=900,
        actual_start_timestamp=started,
        actual_end_timestamp=started + timedelta(seconds=5),
        sample_cadence_seconds=1,
        sample_count=2,
        command_or_process="test monitor",
        artifact_path="/tmp/monitor.json",
    ).to_dict()

    assert contract["monitoring_mode"] == "TIMED_SAMPLING_MONITOR"
    assert contract["actual_elapsed_seconds"] == 5
    assert contract["duration_fulfilled"] is False
    assert contract["truthful_summary_label"] == "sampled briefly"
    assert validate_monitoring_claim(claim_text="observed for 15 minutes", contract=contract) == [
        "OBSERVED_FOR_15_MINUTES_REQUIRES_ELAPSED_SECONDS_AT_LEAST_900",
        "UNDER_DURATION_MONITOR_MUST_REPORT_SHORTFALL_PLAINLY",
    ]


def test_full_sampling_monitor_allows_observed_for_15_minutes_claim() -> None:
    started = datetime(2026, 6, 1, 20, 0, tzinfo=UTC)
    contract = timed_sampling_monitoring_contract(
        requested_duration_seconds=900,
        actual_start_timestamp=started,
        actual_end_timestamp=started + timedelta(seconds=901),
        sample_cadence_seconds=60,
        sample_count=16,
        command_or_process="test monitor",
        artifact_path="/tmp/monitor.json",
    ).to_dict()

    assert contract["duration_fulfilled"] is True
    assert contract["claim_guardrails"]["observed_for_15_minutes_allowed"] is True
    assert validate_monitoring_claim(claim_text="observed for 15 minutes", contract=contract) == []


def test_one_shot_status_check_cannot_be_called_live_watch() -> None:
    started = datetime(2026, 6, 1, 20, 0, tzinfo=UTC)
    contract = one_shot_monitoring_contract(
        started_at=started,
        ended_at=started,
        command_or_process="test monitor",
        artifact_path="/tmp/monitor.json",
    ).to_dict()

    assert validate_monitoring_claim(claim_text="I ran a live watch", contract=contract) == [
        "ONE_SHOT_STATUS_CHECK_CANNOT_BE_DESCRIBED_AS_LIVE_WATCH"
    ]
