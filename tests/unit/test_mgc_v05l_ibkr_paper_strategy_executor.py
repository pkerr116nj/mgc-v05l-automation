from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution.ibkr_paper_strategy_executor import (
    IbkrPaperStrategyExecutorConfig,
    IbkrPaperStrategyExecutorLoopConfig,
    run_ibkr_paper_strategy_executor,
    run_ibkr_paper_strategy_executor_loop,
    write_ibkr_paper_strategy_executor_artifacts,
    write_ibkr_paper_strategy_executor_loop_artifacts,
)


def _config(tmp_path: Path, **overrides: object) -> IbkrPaperStrategyExecutorConfig:
    payload = {
        "repo_root": tmp_path,
        "mode": "PAPER",
        "host": "127.0.0.1",
        "port": 7497,
        "client_id": 9301,
        "account_id": "DUM882026",
        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
        "symbol": "MGC",
        "contract_month": "202606",
        "exact_expiry": "20260626",
        "con_id": 712565978,
        "local_symbol": "MGCM6",
        "quantity": 1.0,
        "output_dir": Path("outputs") / "reports" / "ibkr_paper_strategy_executor",
        "ledger_path": Path("var") / "paper_strategy_position_ledger.json",
        "monitor_status_path": Path("var") / "paper_strategy_monitor_runtime_status.json",
        "dashboard_snapshot_path": Path("outputs") / "operator_dashboard" / "dashboard_api_snapshot.json",
        "dashboard_freshness_seconds": 120.0,
        "monitor_freshness_seconds": 45.0,
        "supervised_submit_enabled": True,
        "force_exit_long": False,
        "allow_direct_reconciliation_close": False,
    }
    payload.update(overrides)
    return IbkrPaperStrategyExecutorConfig(**payload)


def _write_monitor(tmp_path: Path, **overrides: object) -> None:
    payload = {
        "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
        "monitor_running": True,
        "health_classification": "HEALTHY",
        "stale": False,
        "submit_allowed": True,
        "strategy_id": "ATP_COMPANION_V1_ASIA_US",
        "account_id": "DUM882026",
        "exact_contract": {
            "symbol": "MGC",
            "expiry": "20260626",
            "con_id": 712565978,
            "local_symbol": "MGCM6",
        },
        "broker_position_quantity": 1.0,
        "ledger_position_quantity": 1.0,
        "average_entry_price": 4586.7,
        "unrealized_pnl": 241.03,
        "realized_pnl": 14.18,
        "open_order_count": 0,
        "age_seconds": 5.0,
        "block_reasons": [],
    }
    payload.update(overrides)
    path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_ledger(tmp_path: Path, *, quantity: float = 1.0, side: str = "LONG") -> None:
    payload = {
        "positions": [
            {
                "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                "account_id": "DUM882026",
                "symbol": "MGC",
                "expiry": "20260626",
                "con_id": 712565978,
                "local_symbol": "MGCM6",
                "quantity": quantity,
                "side": side,
                "average_entry_price": 4586.7 if quantity else None,
            }
        ]
    }
    path = tmp_path / "var" / "paper_strategy_position_ledger.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_dashboard_snapshot(
    tmp_path: Path,
    *,
    launch_allowed: bool = True,
    attached: bool = True,
    strategy_hint: str | None = None,
) -> None:
    payload: dict[str, object] = {
        "dashboard_meta": {"degraded": False},
        "supervised_paper_operability": {
            "dashboard_attached": attached,
            "launch_allowed": launch_allowed,
            "state": "USABLE" if attached else "UNAVAILABLE",
            "summary_line": "ready" if attached else "down",
        },
        "startup_control_plane": {"overall_state": "READY" if attached else "DOWN"},
        "paper": {
            "status": {"stale": False, "market_data_semantics": "LIVE"},
            "readiness": {"current_detected_session": "US_LATE"},
            "tracked_strategies": {"details_by_strategy_id": {}},
        },
    }
    if strategy_hint is not None:
        paper = dict(payload["paper"])
        tracked = dict(paper["tracked_strategies"])
        tracked["details_by_strategy_id"] = {
            "atp_companion_v1_asia_us": {
                "runtime_state": {
                    "executor_decision": strategy_hint,
                }
            }
        }
        paper["tracked_strategies"] = tracked
        payload["paper"] = paper
    path = tmp_path / "outputs" / "operator_dashboard" / "dashboard_api_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_holding_long_when_monitor_is_healthy_and_no_exit_signal_exists(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path, quantity=1.0, side="LONG")
    _write_dashboard_snapshot(tmp_path)

    artifacts = run_ibkr_paper_strategy_executor(config=_config(tmp_path))

    assert artifacts.classification == "PAPER_STRATEGY_EXECUTOR_HOLDING_LONG"
    assert artifacts.report["decision"] == "HOLD_LONG"
    assert artifacts.report["strategy_position"]["quantity"] == 1.0


def test_blocks_when_monitor_is_stale(tmp_path: Path) -> None:
    _write_monitor(tmp_path, stale=True, age_seconds=120.0)
    _write_ledger(tmp_path, quantity=1.0, side="LONG")
    _write_dashboard_snapshot(tmp_path)

    artifacts = run_ibkr_paper_strategy_executor(config=_config(tmp_path))

    assert artifacts.classification == "PAPER_STRATEGY_EXECUTOR_BLOCKED"
    assert artifacts.report["decision"] == "BLOCKED_NEEDS_REVIEW"
    failed = [row["name"] for row in artifacts.report["preflight_checks"] if not row["passed"]]
    assert "monitor_fresh" in failed


def test_exit_long_delegates_to_unattended_close_path(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path, quantity=1.0, side="LONG")
    _write_dashboard_snapshot(tmp_path, strategy_hint="EXIT_LONG")

    class _CloseArtifacts:
        classification = "PAPER_CLOSE_FILLED_FLAT"
        report = {"summary": "filled flat"}

    artifacts = run_ibkr_paper_strategy_executor(
        config=_config(tmp_path),
        close_runner=lambda **_: _CloseArtifacts(),
    )

    assert artifacts.classification == "PAPER_STRATEGY_EXECUTOR_EXIT_FILLED_FLAT"
    assert artifacts.report["decision"] == "EXIT_LONG"
    assert artifacts.report["delegated_result"]["classification"] == "PAPER_CLOSE_FILLED_FLAT"


def test_force_exit_long_uses_direct_reconciliation_when_runtime_wrapper_is_stale(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        classification="PAPER_STRATEGY_MONITOR_DISCONNECTED",
        monitor_running=False,
        health_classification="DISCONNECTED",
        stale=False,
        block_reasons=["monitor_disconnected", "paper_strategy_monitor_not_running"],
    )
    _write_ledger(tmp_path, quantity=1.0, side="LONG")
    _write_dashboard_snapshot(tmp_path)

    class _CloseArtifacts:
        classification = "PAPER_CLOSE_FILLED_FLAT"
        report = {"summary": "filled flat"}

    observed: dict[str, object] = {}

    class _MonitorArtifacts:
        classification = "PAPER_STRATEGY_POSITION_ADOPTED"
        ledger = {
            "positions": [
                {
                    "strategy_id": "ATP_COMPANION_V1_ASIA_US",
                    "account_id": "DUM882026",
                    "symbol": "MGC",
                    "expiry": "20260626",
                    "con_id": 712565978,
                    "local_symbol": "MGCM6",
                    "quantity": 1.0,
                    "side": "LONG",
                    "average_entry_price": 4586.7,
                }
            ]
        }
        pnl_snapshot = {}
        status = {
            "classification": "PAPER_STRATEGY_POSITION_ADOPTED",
            "ownership_proven": True,
            "strategy_id": "ATP_COMPANION_V1_ASIA_US",
            "account_id": "DUM882026",
            "exact_contract": {
                "symbol": "MGC",
                "expiry": "20260626",
                "con_id": 712565978,
                "local_symbol": "MGCM6",
            },
            "broker_position_quantity": 1.0,
            "ledger_position_quantity": 1.0,
            "open_order_count": 0,
            "block_reasons": ["paper_strategy_monitor_not_running"],
            "backend_gate": {
                "backend_healthy": True,
                "live_source_ready": True,
                "launch_allowed": True,
                "paper_runtime_stale": False,
                "temp_paper_blocked": False,
                "session_classification": "US_LATE",
            },
            "generated_at": "2026-04-29T10:00:00+00:00",
        }
        audit_events = []
        broker_report = {}

    def _close_runner(*, config):
        observed["caller_path"] = config.caller_path
        return _CloseArtifacts()

    refresh_calls = {"count": 0}

    def _monitor_refresh_runner(*, config):
        refresh_calls["count"] += 1
        if refresh_calls["count"] == 1:
            return _MonitorArtifacts()
        flat = _MonitorArtifacts()
        flat.ledger = {"positions": [{"strategy_id": "ATP_COMPANION_V1_ASIA_US", "quantity": 0.0, "side": "FLAT"}]}
        flat.status = {
            **flat.status,
            "broker_position_quantity": 0.0,
            "ledger_position_quantity": 0.0,
        }
        return flat

    artifacts = run_ibkr_paper_strategy_executor(
        config=_config(
            tmp_path,
            force_exit_long=True,
            allow_direct_reconciliation_close=True,
        ),
        close_runner=_close_runner,
        monitor_refresh_runner=_monitor_refresh_runner,
    )

    assert artifacts.classification == "PAPER_STRATEGY_EXECUTOR_EXIT_FILLED_FLAT"
    assert artifacts.report["decision"] == "EXIT_LONG"
    assert "direct broker/ledger reconciliation snapshot" in artifacts.report["preflight_note"]
    assert observed["caller_path"] == "ibkr_paper_strategy_executor"


def test_reconciliation_failure_when_exit_delegate_does_not_flatten_cleanly(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path, quantity=1.0, side="LONG")
    _write_dashboard_snapshot(tmp_path, strategy_hint="EXIT_LONG")

    class _CloseArtifacts:
        classification = "IBKR_UNATTENDED_CLOSE_UNKNOWN"
        report = {"detail": "order disappeared without fill evidence"}

    artifacts = run_ibkr_paper_strategy_executor(
        config=_config(tmp_path),
        close_runner=lambda **_: _CloseArtifacts(),
    )

    assert artifacts.classification == "PAPER_STRATEGY_EXECUTOR_RECONCILIATION_FAILED"
    assert artifacts.report["decision"] == "EXIT_LONG"


def test_exit_long_uses_reconciled_ledger_quantity_not_hard_coded_order_size(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path, quantity=0.5, side="LONG")
    _write_dashboard_snapshot(tmp_path, strategy_hint="EXIT_LONG")
    observed: dict[str, object] = {}

    class _CloseArtifacts:
        classification = "PAPER_CLOSE_FILLED_FLAT"
        report = {"summary": "filled flat"}

    def _close_runner(*, config):
        observed["quantity"] = config.quantity
        return _CloseArtifacts()

    artifacts = run_ibkr_paper_strategy_executor(
        config=_config(tmp_path, quantity=1.0),
        close_runner=_close_runner,
    )

    assert artifacts.classification == "PAPER_STRATEGY_EXECUTOR_EXIT_FILLED_FLAT"
    assert observed["quantity"] == 0.5


def test_write_artifacts_serializes_report_and_summary_csv(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path, quantity=1.0, side="LONG")
    _write_dashboard_snapshot(tmp_path)
    config = _config(tmp_path)

    artifacts = run_ibkr_paper_strategy_executor(config=config)
    write_ibkr_paper_strategy_executor_artifacts(config=config, artifacts=artifacts)

    output_dir = tmp_path / "outputs" / "reports" / "ibkr_paper_strategy_executor"
    assert (output_dir / "ibkr_paper_strategy_executor_report.json").exists()
    assert (output_dir / "ibkr_paper_strategy_executor_report.md").exists()
    assert (output_dir / "ibkr_paper_strategy_executor_audit.jsonl").exists()
    assert (output_dir / "per_strategy_paper_status_summary.csv").exists()


def test_loop_holding_classification_and_runtime_files(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path, quantity=1.0, side="LONG")
    _write_dashboard_snapshot(tmp_path)
    config = _config(tmp_path)

    artifacts = run_ibkr_paper_strategy_executor_loop(
        config=IbkrPaperStrategyExecutorLoopConfig(
            executor_config=config,
            poll_interval_seconds=0.0,
            max_cycles=2,
        ),
        sleep_fn=lambda _seconds: None,
    )

    assert artifacts.classification == "PAPER_STRATEGY_EXECUTOR_LOOP_HOLDING"
    assert artifacts.runtime_status["cycles_completed"] == 2
    assert artifacts.runtime_status["last_executor_decision"] == "HOLD_LONG"
    assert artifacts.runtime_status["loop_running"] is False

    write_ibkr_paper_strategy_executor_loop_artifacts(
        config=IbkrPaperStrategyExecutorLoopConfig(
            executor_config=config,
            poll_interval_seconds=0.0,
            max_cycles=2,
        ),
        artifacts=artifacts,
    )
    output_dir = tmp_path / "outputs" / "reports" / "ibkr_paper_strategy_executor"
    assert (output_dir / "paper_strategy_executor_loop_status.json").exists()
    assert (output_dir / "paper_strategy_executor_loop_report.md").exists()
    assert (output_dir / "paper_strategy_executor_loop_audit.jsonl").exists()
    assert (tmp_path / "var" / "paper_strategy_executor_loop_status.json").exists()
    assert (tmp_path / "var" / "paper_strategy_executor_loop_audit.jsonl").exists()


def test_loop_blocks_when_monitor_is_stale(tmp_path: Path) -> None:
    _write_monitor(tmp_path, stale=True, age_seconds=120.0)
    _write_ledger(tmp_path, quantity=1.0, side="LONG")
    _write_dashboard_snapshot(tmp_path)

    artifacts = run_ibkr_paper_strategy_executor_loop(
        config=IbkrPaperStrategyExecutorLoopConfig(
            executor_config=_config(tmp_path),
            poll_interval_seconds=0.0,
            max_cycles=1,
            stop_on_blocked=True,
        ),
        sleep_fn=lambda _seconds: None,
    )

    assert artifacts.classification == "PAPER_STRATEGY_EXECUTOR_LOOP_BLOCKED"
    assert artifacts.runtime_status["last_executor_decision"] == "BLOCKED_NEEDS_REVIEW"


def test_loop_stops_cleanly_when_stop_requested(tmp_path: Path) -> None:
    _write_monitor(tmp_path)
    _write_ledger(tmp_path, quantity=1.0, side="LONG")
    _write_dashboard_snapshot(tmp_path)

    stop_state = {"count": 0}

    def _should_stop() -> bool:
        stop_state["count"] += 1
        return stop_state["count"] > 1

    artifacts = run_ibkr_paper_strategy_executor_loop(
        config=IbkrPaperStrategyExecutorLoopConfig(
            executor_config=_config(tmp_path),
            poll_interval_seconds=0.0,
            max_cycles=0,
        ),
        sleep_fn=lambda _seconds: None,
        should_stop=_should_stop,
    )

    assert artifacts.classification == "PAPER_STRATEGY_EXECUTOR_LOOP_STOPPED"
    assert artifacts.runtime_status["cycles_completed"] == 1
    assert artifacts.runtime_status["loop_running"] is False
