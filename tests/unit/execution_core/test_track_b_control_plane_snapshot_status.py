from __future__ import annotations

from datetime import UTC, datetime, timedelta

from mgc_v05l.execution_core.track_b_control_plane_snapshot_status import (
    CONTROL_PLANE_INCOHERENT_BLOCKED,
    CONTROL_PLANE_MISSING_DIAGNOSTIC_ONLY,
    CONTROL_PLANE_READY,
    CONTROL_PLANE_REQUIRED,
    CONTROL_PLANE_STALE_DIAGNOSTIC_ONLY,
    classify_control_plane_snapshot_status,
)


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_missing_snapshot_blocks_launch_as_required() -> None:
    status = classify_control_plane_snapshot_status({}, now=NOW, required_for_launch=True)

    assert status["classification"] == CONTROL_PLANE_REQUIRED
    assert status["diagnostic_only"] is True
    assert status["control_plane_snapshot_missing"] is True
    assert status["safe_to_start_runtime"] is False


def test_missing_status_snapshot_is_diagnostic_only() -> None:
    status = classify_control_plane_snapshot_status({}, now=NOW)

    assert status["classification"] == CONTROL_PLANE_MISSING_DIAGNOSTIC_ONLY
    assert status["diagnostic_only"] is True
    assert status["not_routing_authority"] is True
    assert status["safe_to_start_runtime"] is False


def test_stale_snapshot_is_diagnostic_only_and_cannot_start() -> None:
    status = classify_control_plane_snapshot_status(_snapshot(generated_at=NOW - timedelta(minutes=10)), now=NOW)

    assert status["classification"] == CONTROL_PLANE_STALE_DIAGNOSTIC_ONLY
    assert status["control_plane_snapshot_stale"] is True
    assert status["diagnostic_only"] is True
    assert status["safe_to_start_runtime"] is False


def test_incoherent_snapshot_blocks() -> None:
    status = classify_control_plane_snapshot_status(_snapshot(coherence="STALE_OR_MIXED"), now=NOW)

    assert status["classification"] == CONTROL_PLANE_INCOHERENT_BLOCKED
    assert status["control_plane_snapshot_incoherent"] is True
    assert status["safe_to_start_runtime"] is False


def test_ready_snapshot_can_surface_safe_to_start() -> None:
    status = classify_control_plane_snapshot_status(_snapshot(), now=NOW)

    assert status["classification"] == CONTROL_PLANE_READY
    assert status["diagnostic_only"] is False
    assert status["safe_to_start_runtime"] is True


def _snapshot(*, generated_at: datetime = NOW, coherence: str = "COHERENT") -> dict[str, object]:
    return {
        "generated_at": generated_at.isoformat(),
        "classification": "CONTROL_PLANE_SNAPSHOT_READY",
        "shared_truth_coherence_status": coherence,
        "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
        "supervisor_mode": "READY_FOR_OPERATOR_START",
        "safe_to_start_runtime": True,
        "blockers": [],
    }
