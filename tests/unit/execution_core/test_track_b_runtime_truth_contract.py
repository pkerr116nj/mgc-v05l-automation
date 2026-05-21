from __future__ import annotations

from datetime import datetime, timezone

import pytest

from mgc_v05l.execution_core.track_b_runtime_truth_contract import (
    FRESHNESS_FRESH,
    FRESHNESS_MISSING_ARTIFACT,
    FRESHNESS_STALE,
    HEARTBEAT_ARTIFACT_MISSING,
    HEARTBEAT_ARTIFACT_STALE,
    HEARTBEAT_HEALTHY,
    HEARTBEAT_PROCESS_DOWN,
    HEARTBEAT_WRONG_ROOT,
    SCHEMA_VERSION,
    WRITER_DUPLICATE,
    WRITER_MISSING,
    WRITER_SINGLE,
    build_runtime_truth_contract,
    classify_freshness,
    classify_heartbeat,
    classify_writer_authority,
    validate_runtime_truth_contract,
)


NOW = datetime(2026, 5, 21, 12, 0, tzinfo=timezone.utc)


def test_classify_freshness_uses_one_ttl_clock() -> None:
    fresh = classify_freshness(generated_at="2026-05-21T11:58:31+00:00", freshness_ttl_seconds=90, now=NOW)
    stale = classify_freshness(generated_at="2026-05-21T11:58:29+00:00", freshness_ttl_seconds=90, now=NOW)

    assert fresh.freshness_state == FRESHNESS_FRESH
    assert fresh.stale_reason is None
    assert stale.freshness_state == FRESHNESS_STALE
    assert stale.stale_reason == "age_seconds>90"


def test_missing_artifact_and_missing_timestamp_are_distinct() -> None:
    missing_artifact = classify_freshness(generated_at=None, freshness_ttl_seconds=90, now=NOW, artifact_present=False)
    missing_timestamp = classify_freshness(generated_at=None, freshness_ttl_seconds=90, now=NOW, artifact_present=True)

    assert missing_artifact.freshness_state == FRESHNESS_MISSING_ARTIFACT
    assert missing_artifact.stale_reason == "artifact_missing"
    assert missing_timestamp.freshness_state == "MISSING_TIMESTAMP"
    assert missing_timestamp.stale_reason == "timestamp_missing"


def test_heartbeat_prioritizes_root_and_process_before_artifact_freshness() -> None:
    assert (
        classify_heartbeat(
            process_running=True,
            root_ok=False,
            command_ok=True,
            freshness_state=FRESHNESS_FRESH,
        )
        == HEARTBEAT_WRONG_ROOT
    )
    assert (
        classify_heartbeat(
            process_running=False,
            root_ok=True,
            command_ok=True,
            freshness_state=FRESHNESS_FRESH,
        )
        == HEARTBEAT_PROCESS_DOWN
    )
    assert (
        classify_heartbeat(
            process_running=True,
            root_ok=True,
            command_ok=True,
            freshness_state=FRESHNESS_STALE,
        )
        == HEARTBEAT_ARTIFACT_STALE
    )
    assert (
        classify_heartbeat(
            process_running=True,
            root_ok=True,
            command_ok=True,
            freshness_state=FRESHNESS_MISSING_ARTIFACT,
        )
        == HEARTBEAT_ARTIFACT_MISSING
    )
    assert (
        classify_heartbeat(
            process_running=True,
            root_ok=True,
            command_ok=True,
            freshness_state=FRESHNESS_FRESH,
        )
        == HEARTBEAT_HEALTHY
    )


def test_writer_authority_detects_missing_single_and_duplicate_writers() -> None:
    assert classify_writer_authority([]) == WRITER_MISSING
    assert (
        classify_writer_authority(
            [
                {"process_running": True, "heartbeat_state": HEARTBEAT_HEALTHY},
                {"process_running": False, "heartbeat_state": HEARTBEAT_PROCESS_DOWN},
            ]
        )
        == WRITER_SINGLE
    )
    assert (
        classify_writer_authority(
            [
                {"process_running": True, "heartbeat_state": HEARTBEAT_HEALTHY},
                {"process_running": True, "heartbeat_state": HEARTBEAT_HEALTHY},
            ]
        )
        == WRITER_DUPLICATE
    )


def test_build_runtime_truth_contract_is_paper_only_and_schema_valid() -> None:
    payload = build_runtime_truth_contract(
        runtime_instance_id="track-b-paper-20260521-120000-123",
        service_name="paper_runtime",
        producer_pid=123,
        producer_root="/Users/patrick/Dev/MGC-v05l-automation",
        generated_at=NOW,
        last_success_at=NOW,
        freshness_ttl_seconds=180,
        heartbeat_state=HEARTBEAT_HEALTHY,
        writer_authority=WRITER_SINGLE,
        source_commit="abc123",
        config_fingerprint="sha256:deadbeef",
        restart_generation=2,
    )

    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["runtime_mode"] == "PAPER"
    assert payload["freshness_state"] == FRESHNESS_FRESH
    assert payload["heartbeat_state"] == HEARTBEAT_HEALTHY
    assert validate_runtime_truth_contract(payload) == ()


def test_validate_runtime_truth_contract_rejects_live_mode() -> None:
    payload = build_runtime_truth_contract(
        runtime_instance_id="track-b-paper-20260521-120000-123",
        service_name="paper_runtime",
        producer_pid=123,
        producer_root="/Users/patrick/Dev/MGC-v05l-automation",
        generated_at=NOW,
        freshness_ttl_seconds=180,
        heartbeat_state=HEARTBEAT_HEALTHY,
        writer_authority=WRITER_SINGLE,
    )
    payload["runtime_mode"] = "LIVE"

    with pytest.raises(ValueError, match="runtime_mode_not_paper"):
        validate_runtime_truth_contract(payload)
