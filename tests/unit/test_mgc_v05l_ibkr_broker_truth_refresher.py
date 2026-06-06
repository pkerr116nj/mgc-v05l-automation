from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.app.ibkr_broker_truth_refresher import (
    BrokerTruthRefreshConfig,
    build_broker_truth_refresh_status,
    load_broker_truth_refresh_status,
    refresh_seconds_from_env,
    run_broker_truth_refresh_once,
)
from mgc_v05l.execution.ibkr_read_only_verifier import IbkrReadOnlyVerificationArtifacts
from mgc_v05l.execution_core.track_b_readiness_state import write_canonical_readiness_artifact


def _artifacts(
    *,
    classification: str = "IBKR_READ_ONLY_CONNECTED",
    positions_complete: bool = True,
    open_orders_complete: bool = True,
    errors: list[dict[str, object]] | None = None,
    account_ok: bool | None = None,
    generated_at: str = "2026-05-11T12:00:00+00:00",
) -> IbkrReadOnlyVerificationArtifacts:
    account_truth_ok = account_ok if account_ok is not None else classification != "IBKR_READ_ONLY_BLOCKED"
    return IbkrReadOnlyVerificationArtifacts(
        classification=classification,
        connection_report={
            "classification": classification,
            "connection_check": {"host": "127.0.0.1", "port": 7497, "client_id": 9077, "connected": True},
            "account_truth_check": {"ok": account_truth_ok},
            "position_truth_check": {"ok": positions_complete},
            "open_order_truth_check": {"ok": open_orders_complete},
            "errors": errors or [],
        },
        account_truth_snapshot={"selected_account_id": "DUM882026"},
        positions_snapshot={
            "generated_at": generated_at,
            "selected_account_id": "DUM882026",
            "ok": positions_complete,
            "positions_complete": positions_complete,
            "position_count": 2,
            "positions": [{"symbol": "AAPL"}, {"symbol": "GC"}],
        },
        open_orders_snapshot={
            "generated_at": generated_at,
            "selected_account_id": "DUM882026",
            "ok": open_orders_complete,
            "open_orders_complete": open_orders_complete,
            "open_order_count": 0,
            "open_orders": [],
        },
        contract_qualification_report={"ok": True},
        market_data_probe_report=None,
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_clean_reconciliation_inputs(repo_root: Path, generated_at: str) -> None:
    _write_json(
        repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "generated_at": generated_at,
            "broker_reconciled": True,
            "review_required_count": 0,
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "lifecycle_open_position_count": 0,
            "lifecycle_open_order_count": 0,
            "live_money_eligible": False,
            "submit_authority": False,
        },
    )
    _write_json(
        repo_root / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json",
        {
            "generated_at": generated_at,
            "open_position_count": 0,
            "open_positions": [],
            "manual_broker_action_detected": False,
            "live_money_eligible": False,
        },
    )
    _write_json(
        repo_root / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_paper_trade_summary.json",
        {
            "generated_at": generated_at,
            "unknown_open_order_count": 0,
            "lifecycle_open_order_count": 0,
            "unresolved_intent_count": 0,
            "order_status_callbacks_complete": True,
            "last_order_status_at": generated_at,
            "live_money_eligible": False,
        },
    )
    _write_json(
        repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json",
        {"generated_at": generated_at, "canonical_readiness": "READY_OBSERVATION_ONLY", "live_money_eligible": False},
    )
    _write_json(
        repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_maintenance_supervisor_decision.json",
        {"generated_at": generated_at, "supervisor_state": "OBSERVING", "live_money_eligible": False},
    )
    _write_json(
        repo_root / "outputs" / "track_b_execution_core" / "control_plane" / "latest_control_plane_snapshot.json",
        {
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "generated_at": generated_at,
            "control_plane_snapshot_id": "track-b-control-plane-test",
            "shared_truth_refresh_generation_id": "track-b-shared-truth-test",
            "runtime_supervisor_decision_id": "track-b-paper-supervisor-test",
            "shared_truth_coherence_status": "COHERENT",
            "live_money_eligible": False,
        },
    )


def _lease_path(repo_root: Path) -> Path:
    return repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"


def _broker_session_authority_path(repo_root: Path) -> Path:
    return repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_session_authority.json"


def test_refresh_seconds_from_env_defaults_and_accepts_positive_values() -> None:
    assert refresh_seconds_from_env({}) == 60.0
    assert refresh_seconds_from_env({"TRACK_B_BROKER_TRUTH_REFRESH_SECONDS": "15"}) == 15.0
    assert refresh_seconds_from_env({"TRACK_B_BROKER_TRUTH_REFRESH_SECONDS": "nope"}) == 60.0


def test_broker_truth_refresh_once_uses_read_only_verifier_and_writes_status(tmp_path: Path) -> None:
    calls: list[object] = []
    _write_clean_reconciliation_inputs(tmp_path, "2026-05-11T12:00:00+00:00")
    fixed_time = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc)

    def verifier(*, config):
        calls.append(config)
        return _artifacts()

    def writer(*, output_dir: Path, artifacts: IbkrReadOnlyVerificationArtifacts) -> None:
        calls.append(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "ibkr_positions_snapshot.json").write_text(json.dumps(artifacts.positions_snapshot), encoding="utf-8")
        (output_dir / "ibkr_open_orders_snapshot.json").write_text(json.dumps(artifacts.open_orders_snapshot), encoding="utf-8")

    config = BrokerTruthRefreshConfig(
        repo_root=tmp_path,
        output_dir=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification",
        status_path=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json",
        var_status_path=tmp_path / "var" / "ibkr_broker_truth_refresh_status.json",
    )

    status = run_broker_truth_refresh_once(config=config, verifier=verifier, artifact_writer=writer, now_fn=lambda: fixed_time)

    assert status["classification"] == "BROKER_TRUTH_REFRESH_READY"
    assert status["account"] == "DUM882026"
    assert status["host"] == "127.0.0.1"
    assert status["port"] == 7497
    assert status["client_id"] == 9077
    assert status["positions_complete"] is True
    assert status["open_orders_complete"] is True
    assert status["position_count"] == 2
    assert status["open_order_count"] == 0
    assert status["fresh"] is True
    assert status["submit_authority"] is False
    assert status["live_money_eligible"] is False
    assert status["paper_proof_invoked"] is False
    assert status["broker_truth_lease_refresh"]["ok"] is True
    assert status["broker_truth_lease_refresh"]["lease_state"] == "ACTIVE"
    assert (
        status["broker_truth_lease_refresh"]["broker_session_authority_classification"]
        == "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE"
    )
    assert _lease_path(tmp_path).exists()
    lease = json.loads(_lease_path(tmp_path).read_text(encoding="utf-8"))
    authority = json.loads(_broker_session_authority_path(tmp_path).read_text(encoding="utf-8"))
    assert lease["authority_writer"] == "ibkr_broker_truth_refresher"
    assert authority["authority_writer"] == "ibkr_broker_truth_refresher"
    assert lease["authority_generation_id"] == authority["authority_generation_id"]
    assert lease["authority_source_timestamp"] == authority["authority_source_timestamp"]
    assert lease["broker_session_owner"] == authority["broker_session_owner"]
    assert lease["position_snapshot_timestamp"] == authority["position_snapshot_timestamp"]
    assert lease["open_order_snapshot_timestamp"] == authority["open_order_snapshot_timestamp"]
    assert lease["callback_timestamps"] == authority["callback_timestamps"]
    assert authority["schema_version"] == "track_b_broker_session_authority_v1"
    assert authority["broker_session_owner"]["client_id"] == 9077
    assert authority["broker_mutation_allowed"] is False
    assert authority["live_money_eligible"] is False
    loaded = load_broker_truth_refresh_status(status_path=config.status_path)
    assert loaded["source_classification"] == "BROKER_TRUTH_REFRESH_READY"
    assert loaded["classification"] in {"BROKER_TRUTH_REFRESH_FRESH", "BROKER_TRUTH_REFRESH_STALE"}
    assert loaded["fresh"] is (loaded["classification"] == "BROKER_TRUTH_REFRESH_FRESH")
    assert (tmp_path / "var" / "ibkr_broker_truth_refresh_status.json").exists()
    assert calls


def test_broker_truth_refresh_writes_heartbeat_when_configured(tmp_path: Path) -> None:
    fixed_time = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc)
    heartbeat_path = tmp_path / "var" / "track_b_broker_truth_refresh_heartbeat.json"
    config = BrokerTruthRefreshConfig(
        repo_root=tmp_path,
        output_dir=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification",
        status_path=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json",
        var_status_path=tmp_path / "var" / "ibkr_broker_truth_refresh_status.json",
        heartbeat_path=heartbeat_path,
    )

    status = run_broker_truth_refresh_once(
        config=config,
        verifier=lambda *, config: _artifacts(generated_at=fixed_time.isoformat()),
        artifact_writer=lambda *, output_dir, artifacts: output_dir.mkdir(parents=True, exist_ok=True),
        now_fn=lambda: fixed_time,
    )

    heartbeat = json.loads(heartbeat_path.read_text(encoding="utf-8"))
    assert status["classification"] == "BROKER_TRUTH_REFRESH_READY"
    assert heartbeat["service"] == "track_b_ibkr_broker_truth_refresh"
    assert heartbeat["repo_root"] == str(tmp_path)
    assert heartbeat["classification"] == "BROKER_TRUTH_REFRESH_READY"
    assert heartbeat["last_success"] is True
    assert heartbeat["submit_authority"] is False
    assert heartbeat["live_money_eligible"] is False


def test_broker_truth_refresh_skips_reconnect_probe_during_active_exposure(tmp_path: Path) -> None:
    fixed_time = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc)
    _write_clean_reconciliation_inputs(tmp_path, fixed_time.isoformat())
    _write_json(
        _lease_path(tmp_path),
        {
            "generated_at": fixed_time.isoformat(),
            "track_b_broker_position_count": 1,
            "positions": [{"symbol": "MES", "local_symbol": "MESM6", "quantity": "1.0"}],
            "live_money_eligible": False,
        },
    )
    observed_probe_reconnect: list[bool] = []

    def verifier(*, config):
        observed_probe_reconnect.append(bool(config.probe_reconnect))
        artifacts = _artifacts()
        artifacts.connection_report["reconnect_check"] = {
            "status": "DIAGNOSTIC_REFRESH_SKIPPED_ACTIVE_EXPOSURE",
            "ok": True,
            "detail": "Reconnect cycling was skipped because active Track B exposure may require callback continuity.",
        }
        return artifacts

    config = BrokerTruthRefreshConfig(
        repo_root=tmp_path,
        output_dir=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification",
        status_path=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json",
        var_status_path=tmp_path / "var" / "ibkr_broker_truth_refresh_status.json",
    )

    status = run_broker_truth_refresh_once(
        config=config,
        verifier=verifier,
        artifact_writer=lambda *, output_dir, artifacts: output_dir.mkdir(parents=True, exist_ok=True),
        now_fn=lambda: fixed_time,
    )

    assert status["classification"] == "BROKER_TRUTH_REFRESH_READY"
    assert observed_probe_reconnect == [False]


def test_successful_broker_truth_refresh_clears_expired_lease(tmp_path: Path) -> None:
    fixed_time = datetime(2026, 5, 20, 9, 20, tzinfo=timezone.utc)
    _write_clean_reconciliation_inputs(tmp_path, fixed_time.isoformat())
    _write_json(
        _lease_path(tmp_path),
        {
            "lease_state": "ACTIVE",
            "generated_at": "2026-05-20T08:20:00+00:00",
            "entry_valid_until": "2026-05-20T08:25:00+00:00",
            "exit_valid_until": "2026-05-20T08:35:00+00:00",
            "submit_entry_allowed": True,
            "submit_exit_allowed": True,
            "live_money_eligible": False,
        },
    )
    config = BrokerTruthRefreshConfig(
        repo_root=tmp_path,
        output_dir=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification",
        status_path=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json",
        var_status_path=tmp_path / "var" / "ibkr_broker_truth_refresh_status.json",
    )

    status = run_broker_truth_refresh_once(
        config=config,
        verifier=lambda *, config: _artifacts(generated_at=fixed_time.isoformat()),
        artifact_writer=lambda *, output_dir, artifacts: (
            output_dir.mkdir(parents=True, exist_ok=True),
            (output_dir / "ibkr_positions_snapshot.json").write_text(
                json.dumps(artifacts.positions_snapshot), encoding="utf-8"
            ),
            (output_dir / "ibkr_open_orders_snapshot.json").write_text(
                json.dumps(artifacts.open_orders_snapshot), encoding="utf-8"
            ),
        ),
        now_fn=lambda: fixed_time,
    )
    lease = json.loads(_lease_path(tmp_path).read_text(encoding="utf-8"))

    assert status["classification"] == "BROKER_TRUTH_REFRESH_READY"
    assert status["broker_truth_lease_refresh"]["lease_state"] == "ACTIVE"
    assert lease["lease_state"] == "ACTIVE"
    assert lease["generated_at"] == fixed_time.isoformat()
    assert lease["entry_valid_until"] == "2026-05-20T09:25:00+00:00"
    assert lease["submit_entry_allowed"] is True
    assert lease["live_money_eligible"] is False


def test_canonical_readiness_releases_after_fresh_lease_and_clean_reconciliation(tmp_path: Path, monkeypatch) -> None:
    fixed_time = datetime(2026, 5, 20, 9, 20, tzinfo=timezone.utc)
    _write_clean_reconciliation_inputs(tmp_path, fixed_time.isoformat())
    paper_root = tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session"
    runtime_dir = paper_root / "runtime"
    _write_json(
        paper_root / "operator_status.json",
        {
            "source_runtime_pid": 100,
            "source_runtime_repo_root": str(tmp_path),
            "active_lane_ids": ["mgc"],
            "usable_lane_count": 1,
            "entries_enabled": True,
            "operator_halt": False,
            "last_processed_bar_end_ts": "2026-05-20T09:19:00+00:00",
            "health": {"market_data_ok": True},
        },
    )
    _write_json(runtime_dir / "paper_config_in_force.json", {"lanes": [{"lane_id": "mgc"}]})
    _write_json(
        runtime_dir / "paper_lane_quarantine_status.json",
        {"classification": "PAPER_LANE_QUARANTINE_CLEAR", "quarantine_count": 0, "live_money_eligible": False},
    )
    _write_json(runtime_dir / "market_data_transport_probe.json", {"status": "ok", "runtime_ready": True})
    _write_json(
        paper_root / "live_timing_summary_latest.json",
        {
            "lane_id": "mgc",
            "broker_truth": {
                "account_health": {
                    "status": "HEALTHY",
                    "route_destination": "ibkr_paper_bridge_submit_capable",
                }
            },
        },
    )
    config = BrokerTruthRefreshConfig(
        repo_root=tmp_path,
        output_dir=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification",
        status_path=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json",
        var_status_path=tmp_path / "var" / "ibkr_broker_truth_refresh_status.json",
    )
    run_broker_truth_refresh_once(
        config=config,
        verifier=lambda *, config: _artifacts(),
        artifact_writer=lambda *, output_dir, artifacts: output_dir.mkdir(parents=True, exist_ok=True),
        now_fn=lambda: fixed_time,
    )
    monkeypatch.setattr("mgc_v05l.execution_core.track_b_readiness_state._pid_running", lambda pid: pid == 100)

    readiness = write_canonical_readiness_artifact(
        repo_root=tmp_path,
        expected_root=tmp_path,
        output_path=tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json",
        now=fixed_time,
    )

    assert readiness["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert readiness["broker_truth_lease"]["lease_state"] == "ACTIVE"
    assert readiness["readiness_blockers"] == []


def test_failed_refresh_preserves_last_successful_canonical_truth(tmp_path: Path) -> None:
    fixed_time = datetime(2999, 5, 18, 10, 0, tzinfo=timezone.utc)
    config = BrokerTruthRefreshConfig(
        repo_root=tmp_path,
        output_dir=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification",
        status_path=tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json",
        var_status_path=tmp_path / "var" / "ibkr_broker_truth_refresh_status.json",
        refresh_lease_artifact=False,
    )

    good_artifacts = _artifacts()
    good_artifacts.positions_snapshot["marker"] = "last-good"
    failed_artifacts = _artifacts(
        classification="IBKR_READ_ONLY_BLOCKED",
        positions_complete=False,
        open_orders_complete=False,
    )
    failed_artifacts.positions_snapshot["marker"] = "failed-attempt"

    def writer(*, output_dir: Path, artifacts: IbkrReadOnlyVerificationArtifacts) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "ibkr_positions_snapshot.json").write_text(json.dumps(artifacts.positions_snapshot), encoding="utf-8")
        (output_dir / "ibkr_open_orders_snapshot.json").write_text(json.dumps(artifacts.open_orders_snapshot), encoding="utf-8")

    first = run_broker_truth_refresh_once(
        config=config,
        verifier=lambda *, config: good_artifacts,
        artifact_writer=writer,
        now_fn=lambda: fixed_time,
    )
    second = run_broker_truth_refresh_once(
        config=config,
        verifier=lambda *, config: failed_artifacts,
        artifact_writer=writer,
        now_fn=lambda: fixed_time,
    )

    canonical_positions = json.loads((config.output_dir / "ibkr_positions_snapshot.json").read_text(encoding="utf-8"))
    attempt_positions = json.loads((config.output_dir / "latest_attempt" / "ibkr_positions_snapshot.json").read_text(encoding="utf-8"))
    latest_attempt_status = json.loads((config.output_dir / "ibkr_broker_truth_latest_attempt_status.json").read_text(encoding="utf-8"))
    loaded = load_broker_truth_refresh_status(status_path=config.status_path)

    assert first["classification"] == "BROKER_TRUTH_REFRESH_READY"
    assert second["classification"] == "BROKER_TRUTH_REFRESH_LAST_SUCCESS_PRESERVED"
    assert second["last_success"] is True
    assert second["positions_complete"] is True
    assert second["open_orders_complete"] is True
    assert second["latest_attempt_status"]["classification"] == "BROKER_TRUTH_REFRESH_FAILED"
    assert second["last_successful_broker_truth"]["positions_snapshot_path"] == str(config.output_dir / "ibkr_positions_snapshot.json")
    assert latest_attempt_status["classification"] == "BROKER_TRUTH_REFRESH_FAILED"
    assert canonical_positions["marker"] == "last-good"
    assert attempt_positions["marker"] == "failed-attempt"
    assert loaded["source_classification"] == "BROKER_TRUTH_REFRESH_LAST_SUCCESS_PRESERVED"
    assert loaded["last_successful_broker_truth"]["fresh"] is True
    assert loaded["latest_attempt_status"]["classification"] == "BROKER_TRUTH_REFRESH_FAILED"


def test_broker_truth_refresh_failure_is_not_treated_as_clear_truth(tmp_path: Path) -> None:
    fixed_time = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc)
    status = build_broker_truth_refresh_status(
        config=BrokerTruthRefreshConfig(repo_root=tmp_path, output_dir=tmp_path),
        started_at=fixed_time,
        completed_at=fixed_time,
        artifacts=_artifacts(classification="IBKR_READ_ONLY_BLOCKED", positions_complete=False, open_orders_complete=False),
        error=None,
    )

    assert status["classification"] == "BROKER_TRUTH_REFRESH_FAILED"
    assert status["last_success"] is False
    assert status["positions_complete"] is False
    assert status["open_orders_complete"] is False
    assert status["submit_authority"] is False
    assert status["live_money_eligible"] is False


def test_load_broker_truth_refresh_status_recomputes_stale_age_and_blocks_fresh_flag(tmp_path: Path) -> None:
    status_path = tmp_path / "ibkr_broker_truth_refresh_status.json"
    status_path.write_text(
        json.dumps(
            {
                "classification": "BROKER_TRUTH_REFRESH_READY",
                "generated_at": "2026-05-11T12:00:00+00:00",
                "last_success": True,
                "positions_complete": True,
                "open_orders_complete": True,
                "refresh_seconds": 60,
                "submit_authority": False,
                "live_money_eligible": False,
            }
        ),
        encoding="utf-8",
    )

    loaded = load_broker_truth_refresh_status(status_path=status_path)

    assert loaded["source_classification"] == "BROKER_TRUTH_REFRESH_READY"
    assert loaded["classification"] == "BROKER_TRUTH_REFRESH_STALE"
    assert loaded["fresh"] is False
    assert loaded["age_seconds"] > loaded["freshness_threshold_seconds"]
    assert loaded["submit_authority"] is False
    assert loaded["live_money_eligible"] is False


def test_broker_truth_refresh_ignores_benign_2100_after_complete_snapshots(tmp_path: Path) -> None:
    fixed_time = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc)
    status = build_broker_truth_refresh_status(
        config=BrokerTruthRefreshConfig(repo_root=tmp_path, output_dir=tmp_path),
        started_at=fixed_time,
        completed_at=fixed_time,
        artifacts=_artifacts(
            classification="IBKR_READ_ONLY_BLOCKED",
            positions_complete=True,
            open_orders_complete=True,
            account_ok=True,
            errors=[
                {"code": 2104, "message": "Market data farm connection is OK:usfuture"},
                {"code": 2100, "message": "API client has been unsubscribed from account data."},
            ],
        ),
        error=None,
    )

    assert status["classification"] == "BROKER_TRUTH_REFRESH_READY"
    assert status["last_success"] is True
    assert status["benign_account_unsubscribe_ignored"] is True
    assert status["positions_complete"] is True
    assert status["open_orders_complete"] is True
    assert status["submit_authority"] is False


def test_broker_truth_refresh_blocks_2100_before_complete_snapshots(tmp_path: Path) -> None:
    fixed_time = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc)
    status = build_broker_truth_refresh_status(
        config=BrokerTruthRefreshConfig(repo_root=tmp_path, output_dir=tmp_path),
        started_at=fixed_time,
        completed_at=fixed_time,
        artifacts=_artifacts(
            classification="IBKR_READ_ONLY_BLOCKED",
            positions_complete=False,
            open_orders_complete=True,
            account_ok=True,
            errors=[{"code": 2100, "message": "API client has been unsubscribed from account data."}],
        ),
        error=None,
    )

    assert status["classification"] == "BROKER_TRUTH_REFRESH_FAILED"
    assert status["last_success"] is False
    assert status["benign_account_unsubscribe_ignored"] is False
    assert status["positions_complete"] is False
    assert status["submit_authority"] is False


def test_broker_truth_refresher_source_has_no_broker_mutation_or_paper_proof() -> None:
    source = Path("src/mgc_v05l/app/ibkr_broker_truth_refresher.py").read_text(encoding="utf-8")

    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "reqGlobalCancel" not in source
    assert "reqAutoOpenOrders" not in source
    assert "run_paper_proof" not in source
    assert "live_money_eligible\": True" not in source
