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
    assert payload["last_failure_at"] == now.isoformat()
    stale_payload = json.loads((tmp_path / DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT).read_text(encoding="utf-8"))
    assert stale_payload["generated_at"] == stale.isoformat()


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
    assert "_refresh_track_b_authority_for_active_paper_runtime(self._settings)" in source
    assert "runtime_active=True" in source
