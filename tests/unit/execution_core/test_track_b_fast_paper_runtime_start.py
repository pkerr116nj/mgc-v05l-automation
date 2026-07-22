from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pytest

from mgc_v05l.execution_core import track_b_fast_paper_runtime_start as fast


NOW = "2026-07-22T08:05:00+00:00"


class DummySocket:
    def __enter__(self) -> "DummySocket":
        return self

    def __exit__(self, *_: object) -> None:
        return None


def write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def manifest(repo: Path) -> Path:
    path = repo / "config/track_b_paper_runtime_manifest.json"
    payload = {
        "schema_version": "track_b_paper_runtime_manifest_v1",
        "manifest_id": "test_manifest",
        "account_id": "DUM882026",
        "mode": "PAPER",
        "paper_only": True,
        "host": "127.0.0.1",
        "port": 7497,
        "broker_client_id": 9077,
        "profile": "test_profile",
        "expected_lane_count": 71,
        "authoritative_lane_config_path": "config/lanes.yaml",
        "config_paths": ["config/base.yaml", "config/lanes.yaml"],
        "schwab_config_path": "config/schwab.local.json",
        "required_services": {
            "phase1": {
                "supervisor_status_path": "outputs/phase1_supervisor.json",
                "listener_status_path": "outputs/phase1_listener.json",
                "max_age_seconds": 180,
            },
            "managed_exit": {
                "status_path": "outputs/managed_exit.json",
                "max_age_seconds": 180,
                "required_apply_mode": "GUARDED_CLOSE_ONLY_APPLY",
            },
        },
        "runtime_paths": {
            "pid_path": "outputs/runtime/paper.pid",
            "pid_metadata_path": "outputs/runtime/paper.pid.json",
            "log_path": "outputs/runtime/paper.log",
            "runtime_truth_path": "outputs/runtime/paper_runtime_truth.json",
            "config_in_force_path": "outputs/runtime/paper_config_in_force.json",
            "startup_progress_path": "outputs/runtime/paper_post_truth_startup_progress.json",
        },
        "timeouts": {
            "startup_lock_stale_seconds": 60,
            "broker_snapshot_timeout_seconds": 10,
            "runtime_verify_timeout_seconds": 1,
            "runtime_heartbeat_fresh_seconds": 20,
            "service_fresh_seconds": 180,
            "recovery_retry_limit": 3,
            "recovery_retry_sleep_seconds": 2,
        },
    }
    write_json(path, payload)
    (repo / "config").mkdir(exist_ok=True)
    (repo / "config/base.yaml").write_text("paper: true\n", encoding="utf-8")
    lanes = [{"lane_id": f"lane_{index}"} for index in range(71)]
    (repo / "config/lanes.yaml").write_text(
        "probationary_paper_lanes_json: " + json.dumps(lanes, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (repo / "config/schwab.local.json").write_text("{}\n", encoding="utf-8")
    return path


def write_services(repo: Path, *, generated_at: str = NOW) -> None:
    write_json(
        repo / "outputs/phase1_supervisor.json",
        {
            "generated_at": generated_at,
            "classification": "PHASE1_DATABENTO_LIVE_SUPERVISOR_RUNNING",
            "child_pid": os.getpid(),
        },
    )
    write_json(
        repo / "outputs/phase1_listener.json",
        {
            "generated_at": generated_at,
            "latest_record_at": generated_at,
            "listener_alive": True,
            "provider_status": "RUNNING",
        },
    )
    write_json(
        repo / "outputs/managed_exit.json",
        {
            "generated_at": generated_at,
            "pid": os.getpid(),
            "classification": "NO_ELIGIBLE_EXITS",
            "apply_mode": "GUARDED_CLOSE_ONLY_APPLY",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )


def write_broker(repo: Path, *, positions: list[dict[str, Any]] | None = None, orders: list[dict[str, Any]] | None = None) -> None:
    positions = positions or []
    orders = orders or []
    write_json(
        repo / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json",
        {
            "generated_at": NOW,
            "account": "DUM882026",
            "fresh": True,
            "positions_complete": True,
            "open_orders_complete": True,
        },
    )
    write_json(
        repo / "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json",
        {
            "generated_at": NOW,
            "ok": True,
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "positions_complete": True,
            "positions": positions,
        },
    )
    write_json(
        repo / "outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json",
        {
            "generated_at": NOW,
            "ok": True,
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "open_orders_complete": True,
            "open_orders": orders,
        },
    )


@pytest.fixture(autouse=True)
def reachable_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fast.socket, "create_connection", lambda *_args, **_kwargs: DummySocket())


def ok_refresh(_manifest: Mapping[str, Any], _repo: Path) -> Mapping[str, Any]:
    return {"ok": True, "elapsed_seconds": 0.1}


def test_broker_flat_fast_start_ignores_stale_derived_artifacts(tmp_path: Path) -> None:
    manifest_path = manifest(tmp_path)
    write_services(tmp_path)
    write_broker(tmp_path)
    write_json(tmp_path / "outputs/stale_certification.json", {"classification": "PLATFORM_NOT_CERTIFIED"})

    result = fast.run_fast_paper_runtime_start(
        command="preflight",
        repo_root=tmp_path,
        manifest_path=manifest_path,
        broker_refresher=ok_refresh,
        now=NOW,
    )

    assert result["ok"] is True
    assert result["classification"] == "FAST_START_READY"
    assert result["details"]["stale_derived_artifact_policy"] == "diagnostic_only_when_broker_flat"


def test_unmanaged_current_futures_exposure_blocks_startup(tmp_path: Path) -> None:
    manifest_path = manifest(tmp_path)
    write_services(tmp_path)
    write_broker(tmp_path, positions=[{"symbol": "MNQ", "local_symbol": "MNQU6", "security_type": "FUT", "quantity": 1}])

    result = fast.run_fast_paper_runtime_start(
        command="preflight",
        repo_root=tmp_path,
        manifest_path=manifest_path,
        broker_refresher=ok_refresh,
        now=NOW,
    )

    assert result["ok"] is False
    assert result["first_blocker"] == "unexplained_current_futures_exposure"


def test_conflicting_current_futures_order_blocks_startup(tmp_path: Path) -> None:
    manifest_path = manifest(tmp_path)
    write_services(tmp_path)
    write_broker(tmp_path, orders=[{"symbol": "MNQ", "local_symbol": "MNQU6", "security_type": "FUT", "order_id": 1}])

    result = fast.run_fast_paper_runtime_start(
        command="preflight",
        repo_root=tmp_path,
        manifest_path=manifest_path,
        broker_refresher=ok_refresh,
        now=NOW,
    )

    assert result["ok"] is False
    assert result["first_blocker"] == "conflicting_current_futures_orders"


def test_required_service_freshness_blocks_before_broker_refresh(tmp_path: Path) -> None:
    manifest_path = manifest(tmp_path)
    write_services(tmp_path, generated_at="2026-07-22T07:00:00+00:00")
    write_broker(tmp_path)

    result = fast.run_fast_paper_runtime_start(
        command="preflight",
        repo_root=tmp_path,
        manifest_path=manifest_path,
        broker_refresher=ok_refresh,
        now=NOW,
    )

    assert result["ok"] is False
    assert result["first_blocker"] == "phase1_not_healthy"


def test_runtime_status_requires_manifest_lane_count_and_fresh_heartbeat(tmp_path: Path) -> None:
    manifest_path = manifest(tmp_path)
    write_json(tmp_path / "outputs/runtime/paper_runtime_truth.json", {"generated_at": NOW, "lane_count": 71, "heartbeat_state": "HEALTHY"})
    write_json(tmp_path / "outputs/runtime/paper_post_truth_startup_progress.json", {"stage": "trading_loop", "state": "RUNNING"})
    (tmp_path / "outputs/runtime").mkdir(parents=True, exist_ok=True)
    (tmp_path / "outputs/runtime/paper.pid").write_text(f"{os.getpid()}\n", encoding="utf-8")

    result = fast.run_fast_paper_runtime_start(command="status", repo_root=tmp_path, manifest_path=manifest_path, now=NOW)

    assert result["ok"] is True
    assert result["details"]["lane_count"] == 71


def test_simulated_validation_demonstrates_three_starts_and_three_recoveries(tmp_path: Path) -> None:
    manifest_path = manifest(tmp_path)

    result = fast.run_fast_paper_runtime_start(
        command="simulate-validation",
        repo_root=tmp_path,
        manifest_path=manifest_path,
        now=NOW,
    )

    assert result["ok"] is True
    assert result["all_under_15_seconds"] is True
    assert [row["kind"] for row in result["runs"]].count("clean_start") == 3
    assert [row["kind"] for row in result["runs"]].count("runtime_crash_auto_recovery") == 3
    assert all(row["lane_count"] == 71 for row in result["runs"])


def test_manifest_requires_authoritative_lane_config_last(tmp_path: Path) -> None:
    manifest_path = manifest(tmp_path)
    payload = json.loads(manifest_path.read_text())
    payload["config_paths"] = ["config/lanes.yaml", "config/base.yaml"]
    manifest_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

    result = fast.run_fast_paper_runtime_start(
        command="validate-manifest",
        repo_root=tmp_path,
        manifest_path=manifest_path,
        now=NOW,
    )

    assert result["ok"] is False
    assert result["first_blocker"] == "authoritative_lane_config_not_last"


def test_start_uses_manifest_command_and_single_broker_refresh(tmp_path: Path) -> None:
    manifest_path = manifest(tmp_path)
    write_services(tmp_path)
    write_broker(tmp_path)
    calls = {"broker": 0, "launcher": 0}

    def broker_refresh(_manifest: Mapping[str, Any], _repo: Path) -> Mapping[str, Any]:
        calls["broker"] += 1
        return {"ok": True, "elapsed_seconds": 0.1}

    def launcher(command: Sequence[str], _env: Mapping[str, str], _cwd: Path, _log_path: Path) -> int:
        calls["launcher"] += 1
        assert "probationary-paper-soak" in command
        assert str(tmp_path / "config/base.yaml") in command
        write_json(tmp_path / "outputs/runtime/paper_runtime_truth.json", {"generated_at": datetime.now(timezone.utc).isoformat(), "lane_count": 71, "heartbeat_state": "HEALTHY"})
        write_json(tmp_path / "outputs/runtime/paper_post_truth_startup_progress.json", {"stage": "trading_loop", "state": "RUNNING"})
        return os.getpid()

    result = fast.run_fast_paper_runtime_start(
        command="start",
        repo_root=tmp_path,
        manifest_path=manifest_path,
        broker_refresher=broker_refresh,
        runtime_launcher=launcher,
        sleep_fn=lambda _seconds: None,
        now=NOW,
    )

    assert result["ok"] is True
    assert result["classification"] == "RUNTIME_STARTED"
    assert calls == {"broker": 1, "launcher": 1}
