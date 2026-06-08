from __future__ import annotations

import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Sequence

import pytest

from mgc_v05l.app.track_b_operator_readiness_refresher import (
    RefreshConfig,
    _refresh_commands,
    _write_json_atomic,
    read_status,
    refresh_once,
    run_supervisor,
)


def test_refresh_commands_are_read_only_and_cover_dashboard_artifacts(tmp_path: Path) -> None:
    commands = _refresh_commands(repo_root=tmp_path, preflight_mode="monday-live")
    names = [name for name, _command in commands]
    flattened = " ".join(part for _name, command in commands for part in command)

    assert names == [
        "phase1_runtime_data_readiness",
        "phase1_ticker_readiness_matrix",
        "broker_truth_broker_truth_lease_bsa",
        "track_b_paper_broker_reconciliation",
        "open_order_truth",
        "managed_position_registry",
        "managed_order_registry",
        "shared_truth",
        "canonical_readiness",
        "agent_health",
        "control_plane_snapshot",
        "operator_decision_surface",
    ]
    assert "phase1_runtime_data_readiness" in flattened
    assert "phase1_ticker_readiness_matrix" in flattened
    assert "ibkr_broker_truth_refresher" in flattened
    assert "--read-only" in flattened
    assert "track_b_paper_broker_reconciliation" in flattened
    assert "track_b_open_order_truth" in flattened
    assert "track_b_managed_position_registry" in flattened
    assert "track_b_managed_order_registry" in flattened
    assert "track_b_shared_truth_refresh_cli" in flattened
    assert "track_b_canonical_readiness" in flattened
    assert "track_b_agent_health" in flattened
    assert "track_b_control_plane_snapshot" in flattened
    assert "track_b_operator_decision_surface" in flattened
    assert names.index("broker_truth_broker_truth_lease_bsa") < names.index("track_b_paper_broker_reconciliation")
    assert names.index("track_b_paper_broker_reconciliation") < names.index("open_order_truth")
    assert names.index("open_order_truth") < names.index("managed_order_registry")
    assert names.index("managed_position_registry") < names.index("managed_order_registry")
    assert names.index("managed_order_registry") < names.index("shared_truth")
    assert names.index("shared_truth") < names.index("canonical_readiness")
    assert names.index("agent_health") < names.index("control_plane_snapshot")
    assert names.index("control_plane_snapshot") < names.index("operator_decision_surface")
    assert "placeOrder" not in flattened
    assert "--summary-output-path" in flattened
    assert "cancelOrder" not in flattened
    assert "reqGlobalCancel" not in flattened
    assert "paper_proof" not in flattened


def test_refresh_once_writes_status_and_keeps_submit_authority_false(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def fake_runner(command: Sequence[str], _repo_root: Path, _timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        calls.append(list(command))
        return subprocess.CompletedProcess(list(command), 0, stdout="ok", stderr="")

    status_path = tmp_path / "latest_status.json"
    ownership_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_authority_ownership.json"
    ownership_path.parent.mkdir(parents=True)
    ownership_path.write_text(
        json.dumps(
            {
                "classification": "BROKER_AUTHORITY_PUBLISHER_HEALTHY",
                "authority_writer": "ibkr_broker_truth_refresher",
                "authority_generation_id": "gen-1",
                "writer_pid": 123,
                "service_label": "track_b_ibkr_broker_truth_refresh",
                "source_commit": "c56ad805",
                "expected_min_commit": "c56ad805",
                "broker_authority_publisher_healthy": True,
                "lease_bsa_generation_aligned": True,
                "duplicate_hot_writer_detected": False,
                "non_owner_hot_write_attempt_count": 0,
                "running_writer_needs_reload": False,
                "next_safe_action": "NO_ACTION",
            }
        ),
        encoding="utf-8",
    )
    payload = refresh_once(
        config=RefreshConfig(repo_root=tmp_path, status_path=status_path),
        runner=fake_runner,
    )

    assert len(calls) == 12
    assert payload["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_READY"
    assert payload["authority_generation_id"].startswith("track-b-operator-authority-refresh-")
    assert payload["last_success"] is True
    assert payload["authority_refresh_orchestration"] == "TRACK_B_ACTIVE_RUNTIME_DEPENDENCY_CHAIN_V1"
    assert payload["authority_refresh_cadence_seconds"] == payload["refresh_seconds"]
    assert payload["submit_authority"] is False
    assert payload["broker_authority_ownership"]["classification"] == "BROKER_AUTHORITY_PUBLISHER_HEALTHY"
    assert payload["broker_authority_ownership"]["lease_bsa_generation_aligned"] is True
    assert payload["broker_authority_ownership"]["next_safe_action"] == "NO_ACTION"
    assert payload["paper_proof_invoked"] is False
    assert payload["live_money_eligible"] is False
    assert payload["dependency_refresh_failures"] == []
    assert [row["step"] for row in payload["dependency_refresh_steps"]] == [
        "phase1_runtime_data_readiness",
        "phase1_ticker_readiness_matrix",
        "broker_truth_broker_truth_lease_bsa",
        "track_b_paper_broker_reconciliation",
        "open_order_truth",
        "managed_position_registry",
        "managed_order_registry",
        "shared_truth",
        "canonical_readiness",
        "agent_health",
        "control_plane_snapshot",
        "operator_decision_surface",
    ]
    assert all(
        row["authority_generation_id"] == payload["authority_generation_id"]
        for row in payload["dependency_refresh_steps"]
    )
    assert "latest_open_order_truth.json" in payload["refreshed_artifacts"]["open_order_truth"]
    assert "latest_managed_orders.json" in payload["refreshed_artifacts"]["managed_order_registry"]
    assert "latest_managed_positions.json" in payload["refreshed_artifacts"]["managed_position_registry"]
    assert "latest_track_b_shared_truth_refresh.json" in payload["refreshed_artifacts"]["shared_truth"]
    assert "latest_canonical_readiness.json" in payload["refreshed_artifacts"]["canonical_readiness"]
    assert "latest_canonical_readiness_summary.json" in payload["refreshed_artifacts"]["canonical_readiness_summary"]
    assert "latest_control_plane_snapshot.json" in payload["refreshed_artifacts"]["control_plane_snapshot"]
    assert (
        "latest_operator_decision_surface.json"
        in payload["refreshed_artifacts"]["operator_decision_surface"]
    )
    assert json.loads(status_path.read_text(encoding="utf-8"))["classification"] == payload["classification"]



def test_refresh_once_writes_heartbeat_when_configured(tmp_path: Path) -> None:
    def fake_runner(command: Sequence[str], _repo_root: Path, _timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(list(command), 0, stdout="ok", stderr="")

    heartbeat_path = tmp_path / "heartbeat.json"
    payload = refresh_once(
        config=RefreshConfig(
            repo_root=tmp_path,
            status_path=tmp_path / "status.json",
            heartbeat_path=heartbeat_path,
        ),
        runner=fake_runner,
    )

    heartbeat = json.loads(heartbeat_path.read_text(encoding="utf-8"))
    assert payload["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_READY"
    assert heartbeat["schema_version"] == "track_b_operator_readiness_refresher_heartbeat_v1"
    assert heartbeat["authority_generation_id"] == payload["authority_generation_id"]
    assert heartbeat["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_READY"
    assert heartbeat["authority_refresh_cadence_seconds"] == payload["refresh_seconds"]
    assert heartbeat["dependency_refresh_steps"] == payload["dependency_refresh_steps"]
    assert heartbeat["dependency_refresh_failures"] == []
    assert heartbeat["refreshed_artifacts"] == payload["refreshed_artifacts"]
    assert heartbeat["fresh"] is True
    assert heartbeat["refresh_running"] is False
    assert heartbeat["submit_authority"] is False
    assert heartbeat["paper_proof_invoked"] is False
    assert heartbeat["live_money_eligible"] is False


def test_atomic_json_writer_handles_repeated_rapid_writes(tmp_path: Path) -> None:
    target = tmp_path / "status.json"

    for index in range(50):
        _write_json_atomic(target, {"sequence": index, "submit_authority": False})

    assert json.loads(target.read_text(encoding="utf-8")) == {
        "sequence": 49,
        "submit_authority": False,
    }
    assert not list(tmp_path.glob(".status.json.*.tmp"))


def test_atomic_json_writer_handles_overlapping_writes_without_temp_collision(tmp_path: Path) -> None:
    target = tmp_path / "heartbeat.json"

    def write_one(index: int) -> None:
        _write_json_atomic(target, {"sequence": index, "submit_authority": False})

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(write_one, range(40)))

    written = json.loads(target.read_text(encoding="utf-8"))
    assert 0 <= written["sequence"] < 40
    assert written["submit_authority"] is False
    assert not list(tmp_path.glob(".heartbeat.json.*.tmp"))


def test_atomic_json_writer_reports_failure_without_corrupting_existing_json(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "status.json"
    target.write_text(json.dumps({"sequence": "old"}) + "\n", encoding="utf-8")

    def fail_replace(self: Path, target_path: Path) -> None:
        raise FileNotFoundError(f"forced replace failure for {target_path}")

    monkeypatch.setattr(Path, "replace", fail_replace)

    with pytest.raises(RuntimeError, match="atomic json write failed"):
        _write_json_atomic(target, {"sequence": "new"})

    assert json.loads(target.read_text(encoding="utf-8")) == {"sequence": "old"}
    assert not list(tmp_path.glob(".status.json.*.tmp"))


def test_refresh_once_treats_non_ready_canonical_readiness_as_refreshed_state(tmp_path: Path) -> None:
    def fake_runner(command: Sequence[str], _repo_root: Path, _timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        name = " ".join(command)
        if "track_b_canonical_readiness" in name:
            return subprocess.CompletedProcess(list(command), 2, stdout='{"classification": "NOT_READY_DEPENDENCY"}', stderr="")
        return subprocess.CompletedProcess(list(command), 0, stdout="ok", stderr="")

    payload = refresh_once(
        config=RefreshConfig(
            repo_root=tmp_path,
            status_path=tmp_path
            / "outputs/reports/track_b_operator_readiness_refresher/latest_track_b_operator_readiness_refresher_status.json",
        ),
        runner=fake_runner,
    )

    assert payload["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_READY"
    assert payload["last_success"] is True
    canonical = next(row for row in payload["commands"] if row["name"] == "canonical_readiness")
    assert canonical["returncode"] == 2
    assert canonical["succeeded"] is True


def test_refresh_once_treats_classified_control_plane_block_as_refreshed_state(tmp_path: Path) -> None:
    def fake_runner(command: Sequence[str], _repo_root: Path, _timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        name = " ".join(command)
        if "track_b_control_plane_snapshot" in name:
            return subprocess.CompletedProcess(list(command), 2, stdout='{"classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED"}', stderr="")
        return subprocess.CompletedProcess(list(command), 0, stdout="ok", stderr="")

    payload = refresh_once(
        config=RefreshConfig(
            repo_root=tmp_path,
            status_path=tmp_path
            / "outputs/reports/track_b_operator_readiness_refresher/latest_track_b_operator_readiness_refresher_status.json",
        ),
        runner=fake_runner,
    )

    control_plane = next(row for row in payload["commands"] if row["name"] == "control_plane_snapshot")
    assert payload["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_READY"
    assert control_plane["returncode"] == 2
    assert control_plane["succeeded"] is True
    assert payload["dependency_refresh_failures"] == []


def test_refresh_once_fails_closed_when_a_refresh_command_fails(tmp_path: Path) -> None:
    def fake_runner(command: Sequence[str], _repo_root: Path, _timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        name = " ".join(command)
        return subprocess.CompletedProcess(list(command), 1 if "track_b_managed_order_registry" in name else 0, stdout="", stderr="blocked")

    payload = refresh_once(
        config=RefreshConfig(repo_root=tmp_path, status_path=tmp_path / "status.json"),
        runner=fake_runner,
    )

    assert payload["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_FAILED"
    assert payload["last_success"] is False
    assert payload["last_success_at"] is None
    assert payload["submit_authority"] is False
    assert payload["last_failure"] is True
    failure = payload["dependency_refresh_failures"][0]
    assert failure["step"] == "managed_order_registry"
    assert failure["code"] == "managed_order_registry_refresh_failed"
    managed_order = next(row for row in payload["commands"] if row["name"] == "managed_order_registry")
    assert managed_order["returncode"] == 1


def test_refresh_once_fails_with_exact_dependency_when_reconciliation_refresh_fails(tmp_path: Path) -> None:
    def fake_runner(command: Sequence[str], _repo_root: Path, _timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        name = " ".join(command)
        if "track_b_paper_broker_reconciliation" in name:
            return subprocess.CompletedProcess(list(command), 1, stdout="", stderr="reconciliation stale")
        return subprocess.CompletedProcess(list(command), 0, stdout="ok", stderr="")

    payload = refresh_once(
        config=RefreshConfig(repo_root=tmp_path, status_path=tmp_path / "status.json"),
        runner=fake_runner,
    )

    assert payload["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_FAILED"
    assert payload["last_success"] is False
    assert payload["dependency_refresh_failures"] == [
        {
            "step": "track_b_paper_broker_reconciliation",
            "code": "track_b_paper_broker_reconciliation_refresh_failed",
            "authority_generation_id": payload["authority_generation_id"],
            "returncode": 1,
            "stderr_tail": "reconciliation stale",
        }
    ]


def test_refresh_once_publishes_degraded_ods_when_shared_truth_fails(tmp_path: Path) -> None:
    def fake_runner(command: Sequence[str], _repo_root: Path, _timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        name = " ".join(command)
        if "track_b_shared_truth_refresh_cli" in name:
            return subprocess.CompletedProcess(list(command), 2, stdout="", stderr="shared truth blocked")
        return subprocess.CompletedProcess(list(command), 0, stdout="ok", stderr="")

    payload = refresh_once(
        config=RefreshConfig(
            repo_root=tmp_path,
            status_path=tmp_path
            / "outputs/reports/track_b_operator_readiness_refresher/latest_track_b_operator_readiness_refresher_status.json",
        ),
        runner=fake_runner,
    )

    ods_path = tmp_path / "outputs/track_b_execution_core/operator_decision_surface/latest_operator_decision_surface.json"
    ods = json.loads(ods_path.read_text(encoding="utf-8"))

    assert payload["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_FAILED"
    assert payload["dependency_refresh_failures"][0]["code"] == "shared_truth_refresh_failed"
    assert payload["post_status_ods_publication"]["succeeded"] is True
    assert payload["post_status_ods_publication"]["broker_state"] == "UNKNOWN"
    assert ods["first_blocker"] == {
        "code": "shared_truth_refresh_failed",
        "detail": "shared_truth refresh failed with returncode=2.",
        "source": "operator_readiness_refresher",
    }
    assert ods["next_safe_action"] == "REFRESH_AUTHORITY"
    assert ods["submit_allowed"]["submit_allowed"] is False


def test_missing_status_is_safe_and_non_authoritative(tmp_path: Path) -> None:
    payload = read_status(status_path=tmp_path / "missing.json")

    assert payload["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_STATUS_MISSING"
    assert payload["refresh_running"] is False
    assert payload["submit_authority"] is False
    assert payload["paper_proof_invoked"] is False
    assert payload["live_money_eligible"] is False


def test_stale_status_is_reclassified_loudly(tmp_path: Path) -> None:
    status_path = tmp_path / "status.json"
    status_path.write_text(
        json.dumps(
            {
                "schema_version": "track_b_operator_readiness_refresher_status_v1",
                "generated_at": "2000-01-01T00:00:00+00:00",
                "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_READY",
                "last_success": True,
                "refresh_seconds": 60.0,
                "submit_authority": False,
                "paper_proof_invoked": False,
                "live_money_eligible": False,
            }
        ),
        encoding="utf-8",
    )

    payload = read_status(status_path=status_path)

    assert payload["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_STALE"
    assert payload["source_classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_READY"
    assert payload["fresh"] is False
    assert payload["freshness_threshold_seconds"] == 180.0


def test_refresh_once_reports_runner_exceptions_without_dying(tmp_path: Path) -> None:
    def fake_runner(command: Sequence[str], _repo_root: Path, _timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        if "track_b_managed_order_registry" in " ".join(command):
            raise RuntimeError("boom")
        return subprocess.CompletedProcess(list(command), 0, stdout="ok", stderr="")

    payload = refresh_once(
        config=RefreshConfig(repo_root=tmp_path, status_path=tmp_path / "status.json"),
        runner=fake_runner,
    )

    assert payload["classification"] == "TRACK_B_OPERATOR_READINESS_REFRESH_FAILED"
    assert payload["last_failure"] is True
    managed_order = next(row for row in payload["commands"] if row["name"] == "managed_order_registry")
    assert managed_order["returncode"] == 1
    assert "refresh command exception" in managed_order["stderr_tail"]


def test_run_supervisor_writes_supervisor_status_and_pid_files(monkeypatch, tmp_path: Path) -> None:
    started: list[list[str]] = []

    class FakeChild:
        def __init__(self, command: Sequence[str], **_kwargs: object) -> None:
            self.command = list(command)
            self.pid = 12345
            self._polls = 0
            started.append(self.command)

        def poll(self) -> int | None:
            self._polls += 1
            return None if self._polls == 1 else 0

        def terminate(self) -> None:
            return None

        def wait(self, timeout: float | None = None) -> int:
            return 0

        def kill(self) -> None:
            return None

    monkeypatch.setattr(subprocess, "Popen", FakeChild)
    monkeypatch.setattr("mgc_v05l.app.track_b_operator_readiness_refresher.time.sleep", lambda _seconds: (_ for _ in ()).throw(KeyboardInterrupt()))

    try:
        run_supervisor(
            config=RefreshConfig(
                repo_root=tmp_path,
                status_path=tmp_path / "status.json",
                heartbeat_path=tmp_path / "heartbeat.json",
                canonical_readiness_path=tmp_path / "canonical.json",
            ),
            service_pid_path=tmp_path / "service.pid",
            child_pid_path=tmp_path / "child.pid",
            supervisor_status_path=tmp_path / "supervisor.json",
        )
    except KeyboardInterrupt:
        pass

    status = json.loads((tmp_path / "supervisor.json").read_text(encoding="utf-8"))
    assert started
    assert "--service" in started[0]
    assert "--canonical-readiness-path" in started[0]
    assert "--canonical-readiness-summary-path" in started[0]
    assert status["schema_version"] == "track_b_operator_readiness_refresh_supervisor_v1"
    assert status["submit_authority"] is False
    assert status["paper_proof_invoked"] is False
    assert status["live_money_eligible"] is False
