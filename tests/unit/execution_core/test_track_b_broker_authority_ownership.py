from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution_core.track_b_broker_authority_ownership import (
    build_broker_authority_ownership_status,
    record_non_owner_hot_write_attempt,
)


def _lease(generation_id: str = "gen-1", writer: str = "ibkr_broker_truth_refresher") -> dict[str, object]:
    return {
        "authority_generation_id": generation_id,
        "authority_writer": writer,
        "generated_at": "2026-06-06T08:00:00+00:00",
        "lease_state": "ACTIVE",
    }


def _bsa(generation_id: str = "gen-1", writer: str = "ibkr_broker_truth_refresher") -> dict[str, object]:
    return {
        "authority_generation_id": generation_id,
        "authority_writer": writer,
        "generated_at": "2026-06-06T08:00:00+00:00",
        "classification": "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE",
    }


def test_aligned_broker_publisher_is_accepted(tmp_path: Path) -> None:
    status = build_broker_authority_ownership_status(
        repo_root=tmp_path,
        lease=_lease(),
        broker_session_authority=_bsa(),
        generated_at="2026-06-06T08:00:01+00:00",
        writer_pid=1234,
        service_label="track_b_ibkr_broker_truth_refresh",
        expected_min_commit="c56ad805",
        source_commit="c56ad805c63606b3e0bd8ae545698c7810819d4e",
    )

    assert status["classification"] == "BROKER_AUTHORITY_PUBLISHER_HEALTHY"
    assert status["lease_bsa_generation_aligned"] is True
    assert status["authority_writer"] == "ibkr_broker_truth_refresher"
    assert status["writer_pid"] == 1234
    assert status["service_label"] == "track_b_ibkr_broker_truth_refresh"
    assert status["next_safe_action"] == "NO_ACTION"
    assert status["broker_mutation_allowed"] is False


def test_stale_running_writer_gets_reload_needed(tmp_path: Path) -> None:
    status = build_broker_authority_ownership_status(
        repo_root=tmp_path,
        lease=_lease(),
        broker_session_authority=_bsa(),
        expected_min_commit="c56ad805",
        source_commit="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )

    assert status["classification"] == "BROKER_AUTHORITY_WRITER_RELOAD_REQUIRED"
    assert status["running_writer_needs_reload"] is True
    assert status["next_safe_action"] == "RELOAD_AUTHORITY_REFRESHER_SERVICE"


def test_duplicate_hot_writer_detected_when_writer_mismatches(tmp_path: Path) -> None:
    status = build_broker_authority_ownership_status(
        repo_root=tmp_path,
        lease=_lease(writer="track_b_broker_truth_lease_classifier"),
        broker_session_authority=_bsa(),
    )

    assert status["classification"] == "BROKER_AUTHORITY_DUPLICATE_HOT_WRITER_DETECTED"
    assert status["duplicate_hot_writer_detected"] is True
    assert status["next_safe_action"] == "RELOAD_AUTHORITY_REFRESHER_SERVICE"


def test_non_owner_attempt_is_reported_not_silent(tmp_path: Path) -> None:
    lease_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"
    lease_path.parent.mkdir(parents=True)

    status = record_non_owner_hot_write_attempt(
        path=lease_path,
        incoming_lease=_lease(generation_id="derived", writer="track_b_broker_truth_lease_classifier"),
        existing_lease=_lease(generation_id="broker-owned"),
        generated_at="2026-06-06T08:01:00+00:00",
    )

    persisted = json.loads(
        (tmp_path / "outputs/operator_dashboard/runtime/latest_broker_authority_ownership.json").read_text(
            encoding="utf-8"
        )
    )
    assert status["classification"] == "BROKER_AUTHORITY_DUPLICATE_HOT_WRITER_DETECTED"
    assert persisted["non_owner_hot_write_attempt_count"] == 1
    assert persisted["non_owner_hot_write_attempts"][0]["write_skipped"] is True
    assert persisted["next_safe_action"] == "RELOAD_AUTHORITY_REFRESHER_SERVICE"
