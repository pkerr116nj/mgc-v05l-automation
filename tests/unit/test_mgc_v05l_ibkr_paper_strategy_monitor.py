from __future__ import annotations

import json
import os
from pathlib import Path

from mgc_v05l.execution.ibkr_paper_strategy_monitor import (
    DEFAULT_PAPER_STRATEGY_MONITOR_STARTUP_GRACE_SECONDS,
    IbkrPaperStrategyMonitorError,
    IbkrPaperStrategyMonitorDaemonConfig,
    IbkrPaperStrategyMonitorConfig,
    build_paper_strategy_monitor_service_status,
    load_paper_strategy_monitor_status,
    mark_paper_strategy_monitor_service_stopped,
    paper_strategy_monitor_startup_validation_permanent_failure,
    paper_strategy_monitor_startup_validation_ready,
    run_ibkr_paper_strategy_monitor_daemon,
    run_ibkr_paper_strategy_monitor,
    write_paper_strategy_monitor_service_status_artifacts,
    write_ibkr_paper_strategy_monitor_daemon_artifacts,
    write_ibkr_paper_strategy_monitor_artifacts,
)
import mgc_v05l.execution.ibkr_paper_strategy_monitor as monitor_module


def _config(tmp_path: Path, **overrides: object) -> IbkrPaperStrategyMonitorConfig:
    payload = {
        "repo_root": tmp_path,
        "mode": "PAPER",
        "host": "127.0.0.1",
        "port": 7497,
        "client_id": 9245,
        "account_id": "DUM882026",
        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
        "symbol": "MGC",
        "contract_month": "202606",
        "exact_expiry": "20260626",
        "con_id": 712565978,
        "local_symbol": "MGCM6",
        "dashboard_url": "http://127.0.0.1:8790/api/dashboard",
        "ledger_path": Path("var") / "paper_strategy_position_ledger.json",
        "output_dir": Path("outputs") / "reports" / "paper_strategy_monitor",
        "recent_fill_lookback_minutes": 240,
        "bridge_report_path": Path("outputs") / "reports" / "ibkr_paper_strategy_bridge" / "ibkr_paper_strategy_bridge_report.json",
        "prepared_bundle_path": Path("outputs") / "reports" / "ibkr_paper_strategy_bridge" / "prepared_manual_harness" / "ibkr_manual_paper_fill_test_frozen_preview.json",
        "strategy_tracking_snapshot_path": Path("outputs") / "reports" / "ibkr_paper_strategy_tracking_snapshot" / "strategy_position_snapshot.json",
    }
    payload.update(overrides)
    return IbkrPaperStrategyMonitorConfig(**payload)


def _dashboard_payload(*, stale: bool = True, attached: bool = True) -> dict[str, object]:
    return {
        "dashboard_meta": {"degraded": False},
        "supervised_paper_operability": {
            "dashboard_attached": attached,
            "launch_allowed": attached,
            "state": "USABLE" if attached else "UNAVAILABLE",
            "summary_line": "ready" if attached else "fallback",
        },
        "startup_control_plane": {"overall_state": "READY" if attached else "DOWN"},
        "paper": {
            "status": {"stale": stale, "market_data_semantics": "STALE" if stale else "LIVE_DELAYED"},
            "temporary_paper_runtime_integrity": {"temp_paper_blocked": False, "mismatch_status": "CLEAR"},
            "readiness": {"current_detected_session": "US_MIDDAY"},
        },
    }


def _reconciliation_report(*, quantity: float = 1.0, perm_id: int = 490708968, client_id: int = 10221) -> dict[str, object]:
    exec_row = {
        "broker_order_id": 1,
        "client_id": client_id,
        "con_id": 712565978,
        "executed_at": "2026-04-28T15:34:38.556912+00:00",
        "execution_id": "0000e1a7.69f1fa35.01.01",
        "expiry": "20260626",
        "local_symbol": "MGCM6",
        "perm_id": perm_id,
        "price": 4586.7,
        "quantity": 1.0,
        "side": "BOT",
        "symbol": "MGC",
    }
    return {
        "classification": "IBKR_POSITION_RECONCILED_LONG_MGC" if quantity == 1.0 else "IBKR_POSITION_RECONCILED_FLAT",
        "diagnosis": {
            "latest_exact_position_quantity": quantity,
            "latest_matching_perm_id": perm_id,
        },
        "execution_truth": {
            "recent_matching_execution_rows": [exec_row],
            "matching_execution_rows": [exec_row],
        },
        "portfolio_update_summary": {
            "rows": [
                {
                    "average_cost": 45867.97,
                    "market_price": 4587.3,
                    "market_value": 45873.0,
                    "quantity": quantity,
                    "realized_pnl": 14.18,
                    "unrealized_pnl": 5.03,
                    "updated_at": "2026-04-28T15:34:29.936252+00:00",
                }
            ]
        },
        "provider_snapshot": {"open_orders": []},
        "account_truth": {"net_liquidation": 1100299.05, "buying_power": 3747308.30, "currency": "USD"},
    }


class _Artifacts:
    def __init__(self, report: dict[str, object]) -> None:
        self.classification = str(report.get("classification"))
        self.report = report


def _write_ownership_evidence(tmp_path: Path, *, perm_id: int = 490708968, client_id: int = 10221) -> None:
    bridge_path = tmp_path / "outputs" / "reports" / "ibkr_paper_strategy_bridge"
    prepared_path = bridge_path / "prepared_manual_harness"
    tracking_path = tmp_path / "outputs" / "reports" / "ibkr_paper_strategy_tracking_snapshot"
    prepared_path.mkdir(parents=True, exist_ok=True)
    tracking_path.mkdir(parents=True, exist_ok=True)
    (bridge_path / "ibkr_paper_strategy_bridge_report.json").write_text(
        json.dumps(
            {
                "intent": {
                    "intent_id": "intent-1",
                    "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                    "action": "BUY",
                    "quantity": 1.0,
                }
            }
        ),
        encoding="utf-8",
    )
    (prepared_path / "ibkr_manual_paper_fill_test_frozen_preview.json").write_text(
        json.dumps({"preview_payload": {"environment": {"client_id": client_id}}}),
        encoding="utf-8",
    )
    (tracking_path / "strategy_position_snapshot.json").write_text(
        json.dumps(
            {
                "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                "latest_matching_perm_id": perm_id,
                "current_reconciled_quantity": 1.0,
            }
        ),
        encoding="utf-8",
    )


def _write_prior_adopted_position_evidence(tmp_path: Path, *, perm_id: int = 490708968, client_id: int = 10221) -> None:
    output_dir = tmp_path / "outputs" / "reports" / "paper_strategy_monitor"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "paper_strategy_monitor_audit.jsonl").write_text(
        json.dumps(
            {
                "event_type": "strategy_ownership_resolved",
                "ownership": {
                    "classification": "adopted",
                    "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                    "perm_id": perm_id,
                    "execution_id": "0000e1a7.69f1fa35.01.01",
                    "client_id": client_id,
                    "source_intent_id": "intent-1",
                    "detail": "Bridge intent, prepared frozen preview clientId, prior strategy snapshot, and current broker execution permId all align to ATP_COMPANION_V1_ASIA_US.",
                    "sources": [],
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    executor_dir = tmp_path / "outputs" / "reports" / "ibkr_paper_strategy_executor"
    executor_dir.mkdir(parents=True, exist_ok=True)
    executor_payload = {
        "strategy_position": {
            "strategy_id": "ATP_COMPANION_V1_ASIA_US",
            "adopted_from_broker_truth": True,
            "average_entry_price": 4586.7,
            "con_id": 712565978,
            "entry_timestamp": "2026-04-28T19:17:07.088381+00:00",
            "execution_id": "0000e1a7.69f1fa35.01.01",
            "local_symbol": "MGCM6",
            "order_id": 1,
            "perm_id": perm_id,
            "quantity": 1.0,
            "side": "LONG",
            "source_intent_id": "intent-1",
        }
    }
    (executor_dir / "ibkr_paper_strategy_executor_report.json").write_text(
        json.dumps(executor_payload),
        encoding="utf-8",
    )
    var_dir = tmp_path / "var"
    var_dir.mkdir(parents=True, exist_ok=True)
    (var_dir / "paper_strategy_executor_loop_status.json").write_text(
        json.dumps(executor_payload),
        encoding="utf-8",
    )


def _write_local_operator_artifacts(
    tmp_path: Path,
    *,
    readiness_generated_at: str = "2999-01-01T00:00:00+00:00",
    startup_generated_at: str = "2999-01-01T00:00:00+00:00",
    supervised_generated_at: str = "2999-01-01T00:00:00+00:00",
    integrity_generated_at: str = "2999-01-01T00:00:00+00:00",
    paper_runtime_ready: bool = True,
    paper_trade_allowed: bool = True,
    market_data_stale_count: int = 0,
    bar_authority_unavailable_count: int = 0,
    blocking_fault_count: int = 0,
    temp_paper_blocked: bool = False,
) -> None:
    output_dir = tmp_path / "outputs" / "operator_dashboard"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "paper_readiness_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": readiness_generated_at,
                "paper_runtime_ready": paper_runtime_ready,
                "paper_trade_allowed": paper_trade_allowed,
                "market_data_stale_count": market_data_stale_count,
                "bar_authority_unavailable_count": bar_authority_unavailable_count,
                "blocking_fault_count": blocking_fault_count,
                "runtime_running": True,
                "current_detected_session": "US_MIDDAY",
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "startup_control_plane_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": startup_generated_at,
                "overall_state": "READY",
                "launch_allowed": True,
                "summary_line": "Startup dependencies are aligned.",
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "supervised_paper_operability_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": supervised_generated_at,
                "app_usable_for_supervised_paper": True,
                "launch_allowed": True,
                "state": "USABLE",
                "summary_line": "Application is usable for supervised paper operation.",
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "paper_temporary_paper_runtime_integrity_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": integrity_generated_at,
                "temp_paper_blocked": temp_paper_blocked,
                "mismatch_status": "CLEAR",
            }
        ),
        encoding="utf-8",
    )


def test_adopts_current_broker_position_and_blocks_submit_when_runtime_is_stale(tmp_path: Path) -> None:
    _write_ownership_evidence(tmp_path)

    artifacts = run_ibkr_paper_strategy_monitor(
        config=_config(tmp_path),
        reconciliation_runner=lambda **_: _Artifacts(_reconciliation_report()),
        dashboard_fetcher=lambda _: _dashboard_payload(stale=True),
    )

    assert artifacts.classification == "PAPER_STRATEGY_POSITION_ADOPTED"
    assert artifacts.status["submit_allowed"] is False
    assert "paper_runtime_stale" in artifacts.status["block_reasons"]
    assert artifacts.ledger["positions"][0]["strategy_id"] == "ATP_COMPANION_V1_ASIA_US"
    assert artifacts.ledger["positions"][0]["perm_id"] == 490708968


def test_orphan_position_is_classified_when_ownership_cannot_be_proven(tmp_path: Path) -> None:
    artifacts = run_ibkr_paper_strategy_monitor(
        config=_config(tmp_path),
        reconciliation_runner=lambda **_: _Artifacts(_reconciliation_report()),
        dashboard_fetcher=lambda _: _dashboard_payload(stale=False),
    )

    assert artifacts.classification == "PAPER_STRATEGY_ORPHAN_POSITION"
    assert artifacts.status["submit_allowed"] is False
    assert "orphan_broker_position" in artifacts.status["block_reasons"]
    assert artifacts.ledger["positions"] == []
    assert len(artifacts.ledger["orphan_positions"]) == 1


def test_restores_lost_strategy_attribution_from_prior_adopted_evidence(tmp_path: Path) -> None:
    _write_ownership_evidence(tmp_path)
    _write_prior_adopted_position_evidence(tmp_path)
    bridge_report_path = tmp_path / "outputs" / "reports" / "ibkr_paper_strategy_bridge" / "ibkr_paper_strategy_bridge_report.json"
    bridge_report_path.write_text(
        json.dumps({"intent": {"intent_id": "newer-intent", "strategy_id": "gc_1x_all_lanes__asia_early_long", "action": "NO_ACTION", "quantity": 0.0}}),
        encoding="utf-8",
    )
    prepared_bundle_path = tmp_path / "outputs" / "reports" / "ibkr_paper_strategy_bridge" / "prepared_manual_harness" / "ibkr_manual_paper_fill_test_frozen_preview.json"
    prepared_bundle_path.write_text(
        json.dumps({"preview_payload": {"environment": {"client_id": 19999}}}),
        encoding="utf-8",
    )

    report = _reconciliation_report(perm_id=None)
    report["diagnosis"]["latest_matching_perm_id"] = None
    report["execution_truth"] = {"recent_matching_execution_rows": [], "matching_execution_rows": []}

    artifacts = run_ibkr_paper_strategy_monitor(
        config=_config(tmp_path),
        reconciliation_runner=lambda **_: _Artifacts(report),
        dashboard_fetcher=lambda _: _dashboard_payload(stale=False),
    )

    assert artifacts.classification == "PAPER_STRATEGY_POSITION_ADOPTED"
    assert artifacts.ledger["positions"][0]["strategy_id"] == "ATP_COMPANION_V1_ASIA_US"
    assert artifacts.ledger["positions"][0]["perm_id"] == 490708968
    assert artifacts.ledger["positions"][0]["average_entry_price"] == 4586.7
    assert artifacts.ledger["positions"][0]["previously_adopted"] is True
    assert any(event["event_type"] == "paper_orphan_reconciliation_adoption" for event in artifacts.audit_events)


def test_prior_adopted_position_evidence_uses_bounded_audit_tail(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(monitor_module, "_PRIOR_ADOPTED_EVIDENCE_TAIL_BYTES", 8192)
    _write_ownership_evidence(tmp_path)
    output_dir = tmp_path / "outputs" / "reports" / "paper_strategy_monitor"
    output_dir.mkdir(parents=True, exist_ok=True)
    prior_row = {
        "event_type": "strategy_ownership_resolved",
        "ownership": {
            "classification": "adopted",
            "strategy_id": "ATP_COMPANION_V1_ASIA_US",
            "perm_id": 490708968,
            "execution_id": "0000e1a7.69f1fa35.01.01",
            "client_id": 10221,
            "source_intent_id": "intent-1",
            "quantity": 1.0,
            "side": "LONG",
            "average_entry_price": 4586.7,
            "sources": [],
        },
    }
    audit_path = output_dir / "paper_strategy_monitor_audit.jsonl"
    audit_path.write_text(
        json.dumps({"event_type": "old_noise"}) + "\n"
        + ("x" * 16_384)
        + "\n"
        + json.dumps(prior_row)
        + "\n",
        encoding="utf-8",
    )

    report = _reconciliation_report(perm_id=None)
    report["diagnosis"]["latest_matching_perm_id"] = None
    report["execution_truth"] = {"recent_matching_execution_rows": [], "matching_execution_rows": []}

    artifacts = run_ibkr_paper_strategy_monitor(
        config=_config(tmp_path),
        reconciliation_runner=lambda **_: _Artifacts(report),
        dashboard_fetcher=lambda _: _dashboard_payload(stale=False),
    )

    assert artifacts.classification == "PAPER_STRATEGY_POSITION_ADOPTED"
    assert artifacts.ledger["positions"][0]["perm_id"] == 490708968
    assert artifacts.ledger["positions"][0]["previously_adopted"] is True


def test_ledger_broker_mismatch_is_classified(tmp_path: Path) -> None:
    ledger_path = tmp_path / "var" / "paper_strategy_position_ledger.json"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(
        json.dumps(
            {
                "positions": [
                    {
                        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                        "quantity": 0.0,
                        "adopted_from_broker_truth": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    _write_ownership_evidence(tmp_path)

    artifacts = run_ibkr_paper_strategy_monitor(
        config=_config(tmp_path),
        reconciliation_runner=lambda **_: _Artifacts(_reconciliation_report()),
        dashboard_fetcher=lambda _: _dashboard_payload(stale=False),
    )

    assert artifacts.classification == "PAPER_STRATEGY_LEDGER_BROKER_MISMATCH"
    assert "ledger_broker_mismatch" in artifacts.status["block_reasons"]


def test_flat_after_adopted_position_preserves_strategy_ownership(tmp_path: Path) -> None:
    ledger_path = tmp_path / "var" / "paper_strategy_position_ledger.json"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(
        json.dumps(
            {
                "positions": [
                    {
                        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                        "quantity": 1.0,
                        "side": "LONG",
                        "perm_id": 490708968,
                        "execution_id": "0000e1a7.69f1fa35.01.01",
                        "source_intent_id": "intent-1",
                        "adopted_from_broker_truth": True,
                        "average_entry_price": 4586.7,
                        "order_id": 1,
                        "entry_timestamp": "2026-04-28T19:31:29.243706+00:00",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    _write_ownership_evidence(tmp_path)

    artifacts = run_ibkr_paper_strategy_monitor(
        config=_config(tmp_path),
        reconciliation_runner=lambda **_: _Artifacts(_reconciliation_report(quantity=0.0)),
        dashboard_fetcher=lambda _: _dashboard_payload(stale=False),
    )

    assert artifacts.classification == "PAPER_STRATEGY_MONITOR_ACTIVE"
    assert artifacts.status["broker_position_quantity"] == 0.0
    assert artifacts.status["ledger_position_quantity"] == 0.0
    assert "ledger_broker_mismatch" not in artifacts.status["block_reasons"]
    assert artifacts.ledger["positions"][0]["strategy_id"] == "ATP_COMPANION_V1_ASIA_US"
    assert artifacts.ledger["positions"][0]["state"] == "FLAT"


def test_snapshot_fallback_display_state_does_not_block_clean_flat_submit_authority(tmp_path: Path) -> None:
    _write_prior_adopted_position_evidence(tmp_path)

    artifacts = run_ibkr_paper_strategy_monitor(
        config=_config(tmp_path),
        reconciliation_runner=lambda **_: _Artifacts(_reconciliation_report(quantity=0.0)),
        dashboard_fetcher=lambda _: _dashboard_payload(stale=False, attached=False),
    )

    assert artifacts.status["submit_allowed"] is True
    assert "source_snapshot_fallback" not in artifacts.status["block_reasons"]
    assert "backend_down" not in artifacts.status["block_reasons"]
    assert artifacts.status["detail"] == "Preserved ATP ownership on the reconciled flat paper position using prior adopted evidence from the opened broker lot."


def test_dashboard_timeout_uses_fresh_local_operator_artifacts_for_clean_flat_submit_authority(tmp_path: Path) -> None:
    _write_prior_adopted_position_evidence(tmp_path)
    _write_local_operator_artifacts(tmp_path)

    artifacts = run_ibkr_paper_strategy_monitor(
        config=_config(tmp_path),
        reconciliation_runner=lambda **_: _Artifacts(_reconciliation_report(quantity=0.0)),
        dashboard_fetcher=lambda _: (_ for _ in ()).throw(
            IbkrPaperStrategyMonitorError("Live dashboard payload could not be loaded from http://127.0.0.1:8790/api/dashboard: timed out")
        ),
    )

    assert artifacts.status["submit_allowed"] is True
    assert "backend_down" not in artifacts.status["block_reasons"]
    assert "paper_runtime_stale" not in artifacts.status["block_reasons"]
    assert artifacts.status["backend_gate"]["authoritative_source"] == "LOCAL_OPERATOR_ARTIFACTS"
    assert artifacts.status["backend_gate"]["dashboard_timeout_degraded"] is True
    assert artifacts.status["backend_gate"]["paper_trade_allowed"] is True
    assert "Dashboard telemetry degraded:" in artifacts.status["detail"]


def test_dashboard_timeout_still_fails_closed_when_local_readiness_is_stale(tmp_path: Path) -> None:
    _write_prior_adopted_position_evidence(tmp_path)
    _write_local_operator_artifacts(
        tmp_path,
        readiness_generated_at="2000-01-01T00:00:00+00:00",
        startup_generated_at="2000-01-01T00:00:00+00:00",
        supervised_generated_at="2000-01-01T00:00:00+00:00",
        integrity_generated_at="2000-01-01T00:00:00+00:00",
        paper_trade_allowed=False,
        market_data_stale_count=1,
    )

    artifacts = run_ibkr_paper_strategy_monitor(
        config=_config(tmp_path),
        reconciliation_runner=lambda **_: _Artifacts(_reconciliation_report(quantity=0.0)),
        dashboard_fetcher=lambda _: (_ for _ in ()).throw(
            IbkrPaperStrategyMonitorError("Live dashboard payload could not be loaded from http://127.0.0.1:8790/api/dashboard: timed out")
        ),
    )

    assert artifacts.status["submit_allowed"] is False
    assert "backend_down" in artifacts.status["block_reasons"]
    assert "paper_runtime_stale" in artifacts.status["block_reasons"]
    assert artifacts.status["backend_gate"]["authoritative_source"] == "LOCAL_OPERATOR_ARTIFACTS"
    assert artifacts.status["backend_gate"]["backend_healthy"] is False


def test_write_artifacts_and_load_status(tmp_path: Path) -> None:
    _write_ownership_evidence(tmp_path)
    config = _config(tmp_path)
    artifacts = run_ibkr_paper_strategy_monitor(
        config=config,
        reconciliation_runner=lambda **_: _Artifacts(_reconciliation_report()),
        dashboard_fetcher=lambda _: _dashboard_payload(stale=False),
    )

    write_ibkr_paper_strategy_monitor_artifacts(config=config, artifacts=artifacts)

    status = load_paper_strategy_monitor_status(repo_root=tmp_path)
    assert status["classification"] == "PAPER_STRATEGY_POSITION_ADOPTED"
    assert status["submit_allowed"] is False
    assert "paper_strategy_monitor_snapshot_only" in status["block_reasons"]
    assert (tmp_path / "outputs" / "reports" / "paper_strategy_monitor" / "paper_strategy_position_ledger.json").exists()
    assert (tmp_path / "var" / "paper_strategy_position_ledger.json").exists()


def test_daemon_writes_runtime_status_and_loader_prefers_fresh_runtime_file(tmp_path: Path) -> None:
    _write_ownership_evidence(tmp_path)
    monitor_config = _config(tmp_path)
    daemon_config = IbkrPaperStrategyMonitorDaemonConfig(
        monitor_config=monitor_config,
        poll_interval_seconds=0.0,
        max_cycles=2,
        freshness_window_seconds=60.0,
    )

    artifacts = run_ibkr_paper_strategy_monitor_daemon(
        config=daemon_config,
        sleep_fn=lambda _: None,
        cycle_runner=lambda **_: run_ibkr_paper_strategy_monitor(
            config=monitor_config,
            reconciliation_runner=lambda **__: _Artifacts(_reconciliation_report()),
            dashboard_fetcher=lambda ___: _dashboard_payload(stale=False),
        ),
    )
    write_ibkr_paper_strategy_monitor_daemon_artifacts(config=daemon_config, artifacts=artifacts)

    status = load_paper_strategy_monitor_status(repo_root=tmp_path)
    assert artifacts.classification == "PAPER_STRATEGY_MONITOR_ACTIVE"
    assert status["submit_allowed"] is False
    assert "paper_strategy_monitor_not_running" in status["block_reasons"]
    assert status["health_classification"] == "STOPPED"
    assert status["freshness_window_seconds"] == 60.0
    assert (tmp_path / "outputs" / "reports" / "paper_strategy_monitor" / "paper_strategy_monitor_runtime_status.json").exists()
    assert (tmp_path / "var" / "paper_strategy_monitor_runtime_status.json").exists()
    assert (tmp_path / "var" / "paper_strategy_monitor_heartbeat.json").exists()
    assert (tmp_path / "var" / "paper_strategy_monitor_audit.jsonl").exists()


def test_load_status_allows_submit_when_runtime_is_healthy_and_running(tmp_path: Path) -> None:
    runtime_path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text(
        json.dumps(
            {
                "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
                "monitor_running": True,
                "submit_allowed": True,
                "block_reasons": [],
                "health_classification": "HEALTHY",
                "account_id": "DUM882026",
                "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
                "last_successful_broker_refresh": "2999-01-01T00:00:00+00:00",
                "freshness_window_seconds": 60.0,
            }
        ),
        encoding="utf-8",
    )

    status = load_paper_strategy_monitor_status(repo_root=tmp_path)

    assert status["submit_allowed"] is True
    assert status["stale"] is False


def test_load_status_blocks_when_runtime_is_stale(tmp_path: Path) -> None:
    runtime_path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text(
        json.dumps(
            {
                "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
                "monitor_running": True,
                "submit_allowed": True,
                "block_reasons": [],
                "health_classification": "HEALTHY",
                "last_successful_broker_refresh": "2000-01-01T00:00:00+00:00",
                "freshness_window_seconds": 1.0,
            }
        ),
        encoding="utf-8",
    )

    status = load_paper_strategy_monitor_status(repo_root=tmp_path)

    assert status["submit_allowed"] is False
    assert status["stale"] is True
    assert "paper_strategy_monitor_runtime_stale" in status["block_reasons"]


def test_load_status_prefers_fresher_runtime_over_older_service_report(tmp_path: Path) -> None:
    runtime_path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
                "monitor_running": True,
                "submit_allowed": True,
                "block_reasons": [],
                "detail": "Preserved ATP ownership on the reconciled flat paper position using prior adopted evidence from the opened broker lot.",
                "health_classification": "HEALTHY",
                "account_id": "DUM882026",
                "exact_contract": {"symbol": "MGC", "expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
                "last_successful_broker_refresh": "2999-01-01T00:00:00+00:00",
                "freshness_window_seconds": 60.0,
            }
        ),
        encoding="utf-8",
    )
    service_status_path = tmp_path / "outputs" / "reports" / "paper_strategy_monitor" / "paper_monitor_service_status_report.json"
    service_status_path.parent.mkdir(parents=True, exist_ok=True)
    service_status_path.write_text(
        json.dumps(
            {
                "generated_at": "2000-01-01T00:00:00+00:00",
                "classification": "PAPER_MONITOR_SERVICE_READY",
                "service_process_running": True,
                "monitor_running": True,
                "bridge_allowed": False,
                "block_reasons": [],
                "health_classification": "HEALTHY",
                "stale": False,
                "runtime_classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
                "last_successful_broker_refresh": "2000-01-01T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "var" / "paper_strategy_monitor_service.pid").write_text(f"{os.getpid()}\n", encoding="utf-8")

    status = load_paper_strategy_monitor_status(repo_root=tmp_path)

    assert status["submit_allowed"] is True
    assert status["stale"] is False
    assert status["detail"] == "Preserved ATP ownership on the reconciled flat paper position using prior adopted evidence from the opened broker lot."
    assert status["last_successful_broker_refresh"] == "2999-01-01T00:00:00+00:00"


def test_load_status_marks_runtime_stopped_when_runtime_pid_is_dead(tmp_path: Path) -> None:
    runtime_path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text(
        json.dumps(
            {
                "classification": "PAPER_STRATEGY_MONITOR_PARTIAL",
                "monitor_running": True,
                "submit_allowed": False,
                "block_reasons": ["orphan_broker_position"],
                "health_classification": "ORPHAN_BROKER_POSITION",
                "last_successful_broker_refresh": "2999-01-01T00:00:00+00:00",
                "freshness_window_seconds": 60.0,
                "broker_position_quantity": 1.0,
                "ledger_position_quantity": 0.0,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "var" / "paper_strategy_monitor_service.pid").write_text("999999\n", encoding="utf-8")

    status = load_paper_strategy_monitor_status(repo_root=tmp_path)

    assert status["classification"] == "PAPER_STRATEGY_MONITOR_BLOCKED"
    assert status["monitor_running"] is False
    assert "paper_strategy_monitor_not_running" in status["block_reasons"]
    assert status["health_classification"] == "STOPPED"


def test_service_status_reports_ready_when_runtime_is_live(tmp_path: Path) -> None:
    runtime_path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text(
        json.dumps(
            {
                "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
                "monitor_running": True,
                "submit_allowed": True,
                "block_reasons": [],
                "health_classification": "HEALTHY",
                "ibkr_connection_state": "CONNECTED",
                "broker_position_quantity": 0.0,
                "ledger_position_quantity": 0.0,
                "open_order_count": 0,
                "last_successful_broker_refresh": "2999-01-01T00:00:00+00:00",
                "freshness_window_seconds": 60.0,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "var" / "paper_strategy_monitor_heartbeat.json").write_text(
        json.dumps({"generated_at": "2999-01-01T00:00:00+00:00"}),
        encoding="utf-8",
    )
    (tmp_path / "var" / "paper_strategy_monitor_service.pid").write_text(f"{os.getpid()}\n", encoding="utf-8")

    report = build_paper_strategy_monitor_service_status(repo_root=tmp_path)

    assert report["classification"] == "PAPER_MONITOR_SERVICE_READY"
    assert report["bridge_allowed"] is True
    assert report["bridge_blocked"] is False
    assert report["current_broker_mgc_position"] == 0.0
    assert report["strategy_ledger_mgc_position"] == 0.0


def test_startup_validation_accepts_running_connected_monitor() -> None:
    report = {
        "service_process_running": True,
        "monitor_running": True,
        "ibkr_connection_state": "CONNECTED",
        "last_successful_broker_refresh": "2026-05-01T15:59:29.592604+00:00",
    }

    assert paper_strategy_monitor_startup_validation_ready(report) is True
    assert paper_strategy_monitor_startup_validation_permanent_failure(report) is False


def test_startup_validation_flags_permanent_disconnect_without_refresh() -> None:
    report = {
        "service_process_running": True,
        "monitor_running": True,
        "ibkr_connection_state": "DISCONNECTED",
        "last_successful_broker_refresh": None,
    }

    assert paper_strategy_monitor_startup_validation_ready(report) is False
    assert paper_strategy_monitor_startup_validation_permanent_failure(report) is True


def test_startup_grace_default_is_long_enough_for_first_broker_refresh() -> None:
    assert DEFAULT_PAPER_STRATEGY_MONITOR_STARTUP_GRACE_SECONDS == 30.0


def test_mark_service_stopped_blocks_and_writes_report(tmp_path: Path) -> None:
    runtime_path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_path.write_text(
        json.dumps(
            {
                "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
                "monitor_running": True,
                "submit_allowed": True,
                "block_reasons": [],
                "health_classification": "HEALTHY",
                "monitor_health": "HEALTHY",
                "broker_position_quantity": 0.0,
                "ledger_position_quantity": 0.0,
                "last_successful_broker_refresh": "2999-01-01T00:00:00+00:00",
                "freshness_window_seconds": 60.0,
            }
        ),
        encoding="utf-8",
    )

    mark_paper_strategy_monitor_service_stopped(repo_root=tmp_path, reason="paper_strategy_monitor_stop_requested")
    report = build_paper_strategy_monitor_service_status(repo_root=tmp_path)
    write_paper_strategy_monitor_service_status_artifacts(repo_root=tmp_path, report=report)

    assert report["classification"] == "PAPER_MONITOR_SERVICE_BLOCKED"
    assert report["bridge_allowed"] is False
    assert report["monitor_running"] is False
    assert report["stale"] is True
    assert report["exact_block_reason"] in {"paper_strategy_monitor_not_running", "paper_strategy_monitor_stop_requested"}
    assert (tmp_path / "outputs" / "reports" / "paper_strategy_monitor" / "paper_monitor_service_status_report.json").exists()


def test_daemon_reports_disconnected_cycle_without_crashing(tmp_path: Path) -> None:
    _write_ownership_evidence(tmp_path)
    monitor_config = _config(tmp_path)
    daemon_config = IbkrPaperStrategyMonitorDaemonConfig(
        monitor_config=monitor_config,
        poll_interval_seconds=0.0,
        max_cycles=1,
        freshness_window_seconds=60.0,
    )

    artifacts = run_ibkr_paper_strategy_monitor_daemon(
        config=daemon_config,
        sleep_fn=lambda _: None,
        cycle_runner=lambda **_: (_ for _ in ()).throw(RuntimeError("tws disconnected")),
    )

    assert artifacts.classification == "PAPER_STRATEGY_MONITOR_DISCONNECTED"
    assert artifacts.runtime_status["health_classification"] == "DISCONNECTED"
    assert artifacts.runtime_status["submit_allowed"] is False
