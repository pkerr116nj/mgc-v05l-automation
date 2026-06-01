from __future__ import annotations

import json
import os
import errno
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from mgc_v05l.execution_core import track_b_authority_refresh_heartbeat as heartbeat
from mgc_v05l.execution_core.track_b_authority_refresh_heartbeat import (
    AUTHORITY_REFRESH_FAILED,
    AUTHORITY_REFRESH_INTERVAL_SECONDS,
    AUTHORITY_REFRESHED,
    BRIDGE_PRE_ACTION_MAX_AGE_SECONDS,
    DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT,
    TrackBAuthorityRefreshHeartbeatConfig,
    refresh_track_b_paper_authority_if_due,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _active_runtime_truth(repo_root: Path, now: datetime) -> None:
    _write_json(
        repo_root / heartbeat.DEFAULT_PAPER_RUNTIME_TRUTH_ARTIFACT,
        {
            "generated_at": now.isoformat(),
            "heartbeat_state": "HEALTHY",
            "producer_pid": os.getpid(),
            "runtime_instance_id": "runtime-test",
        },
    )


def _operator_status(repo_root: Path, *, current_session: str = "US", active_lanes: list[dict] | None = None) -> None:
    lanes = active_lanes or [
        {"lane_id": "lane_us", "allowed_sessions": ["US"]},
    ]
    _write_json(
        repo_root / heartbeat.DEFAULT_PAPER_OPERATOR_STATUS_ARTIFACT,
        {
            "generated_at": datetime(2026, 6, 1, 12, 0, tzinfo=UTC).isoformat(),
            "current_detected_session": current_session,
            "active_lane_ids": [str(row["lane_id"]) for row in lanes],
            "lanes": lanes,
        },
    )


def _patch_successful_refresh(monkeypatch: pytest.MonkeyPatch, calls: list[str]) -> None:
    def _shared(*, now: datetime, **_: object) -> dict:
        calls.append("shared")
        return {"generated_at": now.isoformat(), "classification": "SHARED_TRUTH_READY"}

    def _planner(*, now: datetime, **_: object) -> dict:
        calls.append("planner")
        return {"generated_at": now.isoformat(), "classification": "NO_ACTION_NEEDED"}

    def _control_plane(*, now: datetime, **_: object) -> dict:
        calls.append("control_plane")
        return {
            "generated_at": now.isoformat(),
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "control_plane_snapshot_id": f"cp-{len(calls)}",
            "runtime_safe_state_classification": "SAFE_STATE_NORMAL",
        }

    def _supervisor(*, now: datetime, **_: object) -> dict:
        calls.append("supervisor")
        return {
            "generated_at": now.isoformat(),
            "classification": "SUPERVISOR_RUNTIME_ALREADY_HEALTHY",
        }

    monkeypatch.setattr(heartbeat, "refresh_track_b_shared_truth", _shared)
    monkeypatch.setattr(heartbeat, "build_track_b_order_adjustment_plan", _planner)
    monkeypatch.setattr(
        heartbeat,
        "write_track_b_order_adjustment_plan",
        lambda *, config, payload: config.resolve(config.output_path),
    )
    monkeypatch.setattr(heartbeat, "build_track_b_control_plane_snapshot", _control_plane)
    monkeypatch.setattr(
        heartbeat,
        "write_track_b_control_plane_snapshot",
        lambda *, config, payload: _write_json(config.resolve(config.output_path), dict(payload))
        or config.resolve(config.output_path),
    )
    monkeypatch.setattr(heartbeat, "build_track_b_runtime_supervisor_authority", _supervisor)
    monkeypatch.setattr(
        heartbeat,
        "write_track_b_runtime_supervisor_authority",
        lambda *, config, payload: config.resolve(config.output_path),
    )
    monkeypatch.setattr(
        heartbeat,
        "write_canonical_readiness_artifact",
        lambda **_: calls.append("readiness")
        or {"canonical_readiness": "READY_SUBMIT_CAPABLE", "submit_allowed": True},
    )


def test_runtime_active_stale_control_plane_triggers_refresh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    stale = now - timedelta(minutes=10)
    _active_runtime_truth(tmp_path, now)
    _write_json(tmp_path / DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT, {"generated_at": stale.isoformat()})
    calls: list[str] = []

    monkeypatch.setattr(heartbeat, "refresh_track_b_shared_truth", lambda **_: calls.append("shared") or {"classification": "OK"})
    monkeypatch.setattr(
        heartbeat,
        "build_track_b_order_adjustment_plan",
        lambda **_: calls.append("planner") or {"classification": "NO_ACTION_NEEDED"},
    )
    monkeypatch.setattr(
        heartbeat,
        "write_track_b_order_adjustment_plan",
        lambda *, config, payload: config.resolve(config.output_path),
    )
    monkeypatch.setattr(
        heartbeat,
        "build_track_b_control_plane_snapshot",
        lambda **_: calls.append("control_plane")
        or {
            "generated_at": now.isoformat(),
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "control_plane_snapshot_id": "cp-1",
            "runtime_safe_state_classification": "SAFE_STATE_SUBMIT_ALLOWED",
        },
    )
    monkeypatch.setattr(
        heartbeat,
        "write_track_b_control_plane_snapshot",
        lambda *, config, payload: config.resolve(config.output_path),
    )
    monkeypatch.setattr(
        heartbeat,
        "build_track_b_runtime_supervisor_authority",
        lambda **_: calls.append("supervisor") or {"classification": "SUPERVISOR_RUNTIME_ALREADY_HEALTHY"},
    )
    monkeypatch.setattr(
        heartbeat,
        "write_track_b_runtime_supervisor_authority",
        lambda *, config, payload: config.resolve(config.output_path),
    )
    monkeypatch.setattr(
        heartbeat,
        "write_canonical_readiness_artifact",
        lambda **_: calls.append("readiness") or {"canonical_readiness": "READY_SUBMIT_CAPABLE", "submit_allowed": True},
    )

    payload = refresh_track_b_paper_authority_if_due(
        config=TrackBAuthorityRefreshHeartbeatConfig(repo_root=tmp_path),
        now=now,
    )

    assert payload["classification"] == AUTHORITY_REFRESHED
    assert calls == ["shared", "planner", "control_plane", "supervisor", "readiness"]
    assert payload["latest_successful_refresh_at"] == now.isoformat()
    assert payload["control_plane_snapshot_id"] == "cp-1"
    assert payload["runtime_supervisor_classification"] == "SUPERVISOR_RUNTIME_ALREADY_HEALTHY"
    assert payload["artifact_paths"]["runtime_supervisor_authority"].endswith(
        "latest_runtime_supervisor_authority.json"
    )
    assert payload["artifact_paths"]["safe_state_envelope"].endswith("latest_runtime_safe_state_envelope.json")
    assert (tmp_path / heartbeat.DEFAULT_AUTHORITY_REFRESH_HEARTBEAT_ARTIFACT).exists()


def test_refresh_failure_preserves_previous_success_and_does_not_fake_freshness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    prior_success = (now - timedelta(minutes=3)).isoformat()
    stale = now - timedelta(minutes=10)
    _active_runtime_truth(tmp_path, now)
    _write_json(tmp_path / DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT, {"generated_at": stale.isoformat()})
    _write_json(
        tmp_path / heartbeat.DEFAULT_AUTHORITY_REFRESH_HEARTBEAT_ARTIFACT,
        {"latest_successful_refresh_at": prior_success},
    )
    monkeypatch.setattr(heartbeat, "refresh_track_b_shared_truth", lambda **_: {"classification": "OK"})
    monkeypatch.setattr(heartbeat, "build_track_b_order_adjustment_plan", lambda **_: (_ for _ in ()).throw(RuntimeError("boom")))

    payload = refresh_track_b_paper_authority_if_due(
        config=TrackBAuthorityRefreshHeartbeatConfig(repo_root=tmp_path),
        now=now,
        force=True,
    )

    assert payload["classification"] == AUTHORITY_REFRESH_FAILED
    assert payload["latest_successful_refresh_at"] == prior_success
    assert "AUTHORITY_REFRESH_FAILED" in payload["reason_codes"]
    assert "AUTHORITY_REFRESH_FAILED_ORDER_ADJUSTMENT_PLANNER" in payload["reason_codes"]
    assert payload["authority_fresh"] is False
    assert payload["activity_classification"] == "AUTHORITY_REFRESH_FAILED"
    assert payload["last_failure_at"] == now.isoformat()
    stale_payload = json.loads((tmp_path / DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT).read_text(encoding="utf-8"))
    assert stale_payload["generated_at"] == stale.isoformat()


def test_heartbeat_repeats_across_multiple_runtime_cycles(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    start = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    _active_runtime_truth(tmp_path, start)
    _operator_status(tmp_path, current_session="US")
    calls: list[str] = []
    _patch_successful_refresh(monkeypatch, calls)

    config = TrackBAuthorityRefreshHeartbeatConfig(repo_root=tmp_path)
    first = refresh_track_b_paper_authority_if_due(config=config, now=start)
    second = refresh_track_b_paper_authority_if_due(config=config, now=start + timedelta(seconds=91))
    third = refresh_track_b_paper_authority_if_due(config=config, now=start + timedelta(seconds=182))

    assert [first["classification"], second["classification"], third["classification"]] == [
        AUTHORITY_REFRESHED,
        AUTHORITY_REFRESHED,
        AUTHORITY_REFRESHED,
    ]
    assert third["latest_successful_refresh_at"] == (start + timedelta(seconds=182)).isoformat()
    assert len([call for call in calls if call == "control_plane"]) == 3
    events = (tmp_path / heartbeat.DEFAULT_AUTHORITY_REFRESH_EVENTS_ARTIFACT).read_text(encoding="utf-8").splitlines()
    assert len(events) == 3


def test_out_of_window_runtime_still_refreshes_authority(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    _active_runtime_truth(tmp_path, now)
    _operator_status(
        tmp_path,
        current_session="LONDON_LATE",
        active_lanes=[
            {"lane_id": "lane_us", "allowed_sessions": ["US"]},
            {"lane_id": "lane_globex", "allowed_sessions": ["GLOBEX", "ASIA_EARLY", "ASIA_LATE"]},
        ],
    )
    calls: list[str] = []
    _patch_successful_refresh(monkeypatch, calls)

    payload = refresh_track_b_paper_authority_if_due(
        config=TrackBAuthorityRefreshHeartbeatConfig(repo_root=tmp_path),
        now=now,
    )

    assert payload["classification"] == AUTHORITY_REFRESHED
    assert payload["all_active_lanes_out_of_window"] is True
    assert payload["activity_classification"] == "OUT_OF_WINDOW_BUT_AUTHORITY_FRESH"
    assert "control_plane" in calls


def test_active_runtime_heartbeat_artifact_stays_fresh_when_not_due(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    _active_runtime_truth(tmp_path, now)
    _operator_status(tmp_path, current_session="US")
    calls: list[str] = []
    _patch_successful_refresh(monkeypatch, calls)
    config = TrackBAuthorityRefreshHeartbeatConfig(repo_root=tmp_path)

    refresh_track_b_paper_authority_if_due(config=config, now=now)
    payload = refresh_track_b_paper_authority_if_due(config=config, now=now + timedelta(seconds=60))

    assert payload["classification"] == heartbeat.AUTHORITY_REFRESH_SKIPPED_NOT_DUE
    assert payload["authority_fresh"] is True
    assert payload["latest_successful_refresh_age_seconds"] == 60
    assert payload["latest_successful_refresh_age_seconds"] < payload["bridge_max_age_seconds"]


def test_runtime_call_failure_recorder_writes_visible_failure(tmp_path: Path) -> None:
    now = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    prior_success = (now - timedelta(minutes=10)).isoformat()
    _active_runtime_truth(tmp_path, now)
    _write_json(
        tmp_path / heartbeat.DEFAULT_AUTHORITY_REFRESH_HEARTBEAT_ARTIFACT,
        {"latest_successful_refresh_at": prior_success},
    )

    payload = heartbeat.record_track_b_authority_refresh_runtime_failure(
        config=TrackBAuthorityRefreshHeartbeatConfig(repo_root=tmp_path),
        exception=RuntimeError("loop failed"),
        now=now,
    )

    assert payload["classification"] == AUTHORITY_REFRESH_FAILED
    assert payload["latest_successful_refresh_at"] == prior_success
    assert payload["authority_fresh"] is False
    assert "AUTHORITY_REFRESH_RUNTIME_CALL_FAILED" in payload["reason_codes"]
    assert payload["exception_message"] == "loop failed"
    artifact = json.loads(
        (tmp_path / heartbeat.DEFAULT_AUTHORITY_REFRESH_HEARTBEAT_ARTIFACT).read_text(encoding="utf-8")
    )
    assert artifact["classification"] == AUTHORITY_REFRESH_FAILED


def test_stale_readiness_state_triggers_refresh_even_when_interval_not_due(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    _active_runtime_truth(tmp_path, now)
    _write_json(tmp_path / DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT, {"generated_at": now.isoformat()})
    _write_json(
        tmp_path / heartbeat.DEFAULT_AUTHORITY_REFRESH_HEARTBEAT_ARTIFACT,
        {"latest_successful_refresh_at": (now - timedelta(seconds=10)).isoformat()},
    )
    _write_json(
        tmp_path / heartbeat.DEFAULT_CANONICAL_READINESS_ARTIFACT,
        {"canonical_readiness": "BLOCKED_STALE_TRUTH", "generated_at": now.isoformat()},
    )
    calls: list[str] = []
    monkeypatch.setattr(heartbeat, "refresh_track_b_shared_truth", lambda **_: calls.append("shared") or {})
    monkeypatch.setattr(heartbeat, "build_track_b_order_adjustment_plan", lambda **_: {"classification": "NO_ACTION_NEEDED"})
    monkeypatch.setattr(heartbeat, "write_track_b_order_adjustment_plan", lambda *, config, payload: config.resolve(config.output_path))
    monkeypatch.setattr(
        heartbeat,
        "build_track_b_control_plane_snapshot",
        lambda **_: {
            "generated_at": now.isoformat(),
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "control_plane_snapshot_id": "cp-fresh",
        },
    )
    monkeypatch.setattr(heartbeat, "write_track_b_control_plane_snapshot", lambda *, config, payload: config.resolve(config.output_path))
    monkeypatch.setattr(heartbeat, "write_canonical_readiness_artifact", lambda **_: {"canonical_readiness": "READY_SUBMIT_CAPABLE", "submit_allowed": True})

    payload = refresh_track_b_paper_authority_if_due(
        config=TrackBAuthorityRefreshHeartbeatConfig(repo_root=tmp_path),
        now=now,
    )

    assert payload["classification"] == AUTHORITY_REFRESHED
    assert calls == ["shared"]


def test_runtime_inactive_skips_refresh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(heartbeat, "refresh_track_b_shared_truth", lambda **_: calls.append("shared") or {})

    payload = refresh_track_b_paper_authority_if_due(
        config=TrackBAuthorityRefreshHeartbeatConfig(repo_root=tmp_path),
        now=datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
    )

    assert payload["classification"] == heartbeat.AUTHORITY_REFRESH_SKIPPED_RUNTIME_INACTIVE
    assert payload["reason_codes"] == ["RUNTIME_INACTIVE"]
    assert calls == []


def test_permission_limited_process_probe_treats_eperm_as_running(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_eperm(pid: int, signal: int) -> None:
        raise PermissionError(errno.EPERM, "operation not permitted")

    monkeypatch.setattr(heartbeat.os, "kill", _raise_eperm)

    assert heartbeat._process_running(12345) is True


def test_refresh_interval_is_inside_bridge_window_and_uses_bridge_source() -> None:
    assert AUTHORITY_REFRESH_INTERVAL_SECONDS < BRIDGE_PRE_ACTION_MAX_AGE_SECONDS
    config = TrackBAuthorityRefreshHeartbeatConfig(repo_root=Path("/tmp/repo"))
    assert config.control_plane_snapshot_path == DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT


def test_status_script_refreshes_authority_before_operability() -> None:
    source = Path("scripts/track_b_status_paper_stack.sh").read_text(encoding="utf-8")

    assert "track_b_authority_refresh_heartbeat" in source
    assert "AUTHORITY_REFRESH_RC" in source
    assert "authority_refresh_fresh" in source
    assert "all_lanes_out_of_window" in source
    assert "OUT_OF_WINDOW_BUT_AUTHORITY_FRESH" in source
    assert "BLOCKED_STALE_TRUTH" in source


def test_probationary_runtime_invokes_read_only_authority_refresh_heartbeat() -> None:
    source = Path("src/mgc_v05l/app/probationary_runtime.py").read_text(encoding="utf-8")

    assert "TrackBAuthorityRefreshHeartbeatConfig" in source
    assert "refresh_track_b_paper_authority_if_due" in source
    assert "record_track_b_authority_refresh_runtime_failure" in source
    assert "_refresh_track_b_authority_for_active_paper_runtime(self._settings)" in source
    assert "runtime_active=True" in source
