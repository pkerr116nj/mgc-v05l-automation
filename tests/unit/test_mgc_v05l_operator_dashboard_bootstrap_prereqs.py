from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mgc_v05l.app.operator_dashboard import (
    DASHBOARD_PAYLOAD_SCHEMA_VERSION,
    DashboardServerInfo,
    OperatorDashboardService,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_dashboard_bootstrap_prerequisites_report_missing_replay_db_and_auth_env(
    tmp_path: Path,
    monkeypatch,
) -> None:
    missing_db = tmp_path / "mgc_v05l.replay.sqlite3"
    monkeypatch.setenv("MGC_BOOTSTRAP_REPLAY_DB_STATUS", "missing")
    monkeypatch.setenv("MGC_BOOTSTRAP_REPLAY_DB_PATH", str(missing_db))
    monkeypatch.setenv(
        "MGC_BOOTSTRAP_REPLAY_DB_NEXT_ACTION",
        f"Run `bash scripts/backfill_schwab_1m_history.sh` to create and populate {missing_db}.",
    )
    monkeypatch.setenv(
        "MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_MISSING_NAMES",
        "SCHWAB_APP_KEY SCHWAB_APP_SECRET SCHWAB_CALLBACK_URL",
    )
    monkeypatch.setenv("MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_STATUS", "missing")
    monkeypatch.setenv(
        "MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_NEXT_ACTION",
        "Export SCHWAB_APP_KEY, SCHWAB_APP_SECRET, and SCHWAB_CALLBACK_URL or source .local/schwab_env.sh before running Schwab-backed bootstrap actions.",
    )
    monkeypatch.delenv("SCHWAB_APP_KEY", raising=False)
    monkeypatch.delenv("SCHWAB_APP_SECRET", raising=False)
    monkeypatch.delenv("SCHWAB_CALLBACK_URL", raising=False)

    payload = OperatorDashboardService(tmp_path)._dashboard_bootstrap_prerequisites_payload()  # noqa: SLF001

    assert payload["status"] == "reduced_mode"
    assert payload["reduced_mode"] is True
    assert payload["issue_count"] == 2
    replay_item = next(item for item in payload["items"] if item["key"] == "replay_database")
    auth_item = next(item for item in payload["items"] if item["key"] == "schwab_auth_env")
    assert replay_item["status"] == "missing"
    assert "backfill_schwab_1m_history.sh" in replay_item["next_action"]
    assert auth_item["status"] == "missing"
    assert auth_item["missing_names"] == ["SCHWAB_APP_KEY", "SCHWAB_APP_SECRET", "SCHWAB_CALLBACK_URL"]


def test_dashboard_snapshot_surfaces_bootstrap_prerequisites_without_replay_db(
    tmp_path: Path,
    monkeypatch,
) -> None:
    missing_db = tmp_path / "mgc_v05l.replay.sqlite3"
    monkeypatch.setenv("MGC_BOOTSTRAP_REPLAY_DB_STATUS", "missing")
    monkeypatch.setenv("MGC_BOOTSTRAP_REPLAY_DB_PATH", str(missing_db))
    monkeypatch.setenv(
        "MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_MISSING_NAMES",
        "SCHWAB_APP_KEY SCHWAB_APP_SECRET SCHWAB_CALLBACK_URL",
    )
    monkeypatch.setenv("MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_STATUS", "missing")
    monkeypatch.delenv("SCHWAB_APP_KEY", raising=False)
    monkeypatch.delenv("SCHWAB_APP_SECRET", raising=False)
    monkeypatch.delenv("SCHWAB_CALLBACK_URL", raising=False)

    snapshot = OperatorDashboardService(REPO_ROOT).snapshot()

    assert snapshot["bootstrap_prerequisites"]["reduced_mode"] is True
    assert snapshot["bootstrap_prerequisites"]["issue_count"] == 2
    assert snapshot["operator_surface"]["runtime_readiness"]["bootstrap_prerequisites"]["reduced_mode"] is True
    assert snapshot["bootstrap_prerequisites"]["items"][0]["status"] in {"missing", "ready"}
    assert isinstance(snapshot["research_capture"]["status_line"], str)


def test_dashboard_bootstrap_prerequisites_report_ready_when_inputs_exist(
    tmp_path: Path,
    monkeypatch,
) -> None:
    replay_db = tmp_path / "mgc_v05l.replay.sqlite3"
    replay_db.write_text("", encoding="utf-8")
    monkeypatch.setenv("MGC_BOOTSTRAP_REPLAY_DB_STATUS", "ready")
    monkeypatch.setenv("MGC_BOOTSTRAP_REPLAY_DB_PATH", str(replay_db))
    monkeypatch.setenv("MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_MISSING_NAMES", "")
    monkeypatch.setenv("MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_STATUS", "ready")
    monkeypatch.setenv("SCHWAB_APP_KEY", "key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182/callback")

    payload = OperatorDashboardService(tmp_path)._dashboard_bootstrap_prerequisites_payload()  # noqa: SLF001

    assert payload["status"] == "ready"
    assert payload["reduced_mode"] is False
    assert payload["issue_count"] == 0


def test_startup_control_plane_blocks_launch_when_dependencies_are_missing(tmp_path: Path) -> None:
    service = OperatorDashboardService(
        tmp_path,
        server_info=DashboardServerInfo(
            host="127.0.0.1",
            port=8790,
            url="http://127.0.0.1:8790/",
            pid=1234,
            started_at="2026-04-10T19:09:00Z",
            build_stamp="test-build",
            instance_id="test-instance",
            info_file=None,
        ),
    )
    service._server_info = DashboardServerInfo(  # noqa: SLF001
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=123,
        started_at="2026-04-08T11:59:50Z",
        build_stamp="build-test",
        instance_id="instance-test",
        info_file=str(tmp_path / "operator_dashboard.json"),
    )

    payload = service._startup_control_plane_payload(  # noqa: SLF001
        generated_at="2026-04-08T12:00:00Z",
        auth_status={
            "runtime_ready": False,
            "detail": "Schwab auth is missing or expired.",
            "next_action": "Auth Gate Check",
            "source": "test_fixture",
        },
        market_context={
            "feed_state": "UNAVAILABLE",
            "note": "Market-data feed is not connected.",
            "diagnostic_artifact": "/api/operator-artifact/market-index-strip-diagnostics",
        },
        paper={
            "running": False,
            "status": {"reconciliation_semantics": "DIRTY"},
            "runtime_recovery": {
                "status": "STOPPED_MANUAL_REQUIRED",
                "operator_message": "Paper runtime is not yet active.",
                "next_action": "Start Runtime",
            },
            "readiness": {
                "heartbeat_reconciliation_summary": {"active_issue_count": 1, "reason": "Heartbeat mismatch detected."},
                "order_timeout_watchdog_summary": {"active_issue_count": 0},
                "restore_validation_summary": {"unresolved_issue_count": 1},
            },
            "entry_eligibility": {
                "state_note": "Paper runtime is stopped.",
                "clear_action": "Start Runtime",
            },
        },
    )

    assert payload["overall_state"] == "BLOCKED"
    assert payload["launch_allowed"] is False
    assert payload["counts"]["blocked"] >= 2
    assert payload["counts"]["reconciliation_required"] == 1
    dependency_rows = {item["key"]: item for item in payload["dependencies"]}
    assert dependency_rows["market_data_connectivity"]["state"] == "BLOCKED"
    assert dependency_rows["schwab_connectivity"]["state"] == "BLOCKED"
    assert dependency_rows["paper_runtime"]["state"] == "BLOCKED"
    assert dependency_rows["reconciliation"]["state"] == "RECONCILIATION_REQUIRED"
    assert payload["primary_dependency_key"] in {"schwab_connectivity", "market_data_connectivity", "paper_runtime"}


def test_startup_control_plane_allows_launch_when_paper_only_dependencies_are_clean(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    service._server_info = DashboardServerInfo(  # noqa: SLF001
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=123,
        started_at="2026-04-08T12:04:50Z",
        build_stamp="build-test",
        instance_id="instance-test",
        info_file=str(tmp_path / "operator_dashboard.json"),
    )
    service._dashboard_probe.update(  # noqa: SLF001
        {
            "state": "ready",
            "stable_ready": True,
            "stable_ready_since": "2026-04-08T12:04:57Z",
            "consecutive_ready_samples": 3,
            "phase": "stable_attached",
            "phase_detail": "Dashboard/API and tracked paper runtime remained attached across the manager stability window.",
            "api_dashboard_responding": True,
            "operator_surface_loadable": True,
            "dashboard_attached": True,
            "paper_runtime_ready": True,
            "generated_at": "2026-04-08T12:05:00Z",
            "checked_at": "2026-04-08T12:05:00Z",
        }
    )

    payload = service._startup_control_plane_payload(  # noqa: SLF001
        generated_at="2026-04-08T12:05:00Z",
        auth_status={
            "runtime_ready": True,
            "detail": "Schwab connectivity/auth is ready.",
            "next_action": "Refresh",
            "source": "test_fixture",
        },
        market_context={
            "feed_state": "LIVE",
            "note": "Market-data connectivity is live.",
            "diagnostic_artifact": "/api/operator-artifact/market-index-strip-diagnostics",
        },
        paper={
            "running": True,
            "status": {"reconciliation_semantics": "CLEAN", "entries_enabled": True, "operator_halt": False},
            "runtime_recovery": {
                "status": "RUNNING",
                "operator_message": "Paper runtime is active.",
                "next_action": "Refresh",
            },
            "readiness": {
                "heartbeat_reconciliation_summary": {"active_issue_count": 0},
                "order_timeout_watchdog_summary": {"active_issue_count": 0},
                "restore_validation_summary": {"unresolved_issue_count": 0},
            },
            "entry_eligibility": {
                "state_note": "Paper runtime is active.",
                "clear_action": "Refresh",
            },
        },
    )

    assert payload["overall_state"] == "READY"
    assert payload["launch_allowed"] is True
    assert payload["counts"]["ready"] == 5
    assert payload["counts"]["needs_attention_now"] == 0
    assert payload["convergence"]["stable_ready"] is True
    assert payload["convergence"]["phase"] == "stable_attached"
    dependency_rows = {item["key"]: item for item in payload["dependencies"]}
    assert all(item["state"] == "READY" for item in dependency_rows.values())


def test_startup_control_plane_stays_warming_until_dashboard_probe_is_stable(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    service._server_info = DashboardServerInfo(  # noqa: SLF001
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=123,
        started_at="2026-04-08T12:05:50Z",
        build_stamp="build-test",
        instance_id="instance-test",
        info_file=str(tmp_path / "operator_dashboard.json"),
    )
    service._dashboard_probe.update(  # noqa: SLF001
        {
            "state": "warming",
            "stable_ready": False,
            "stable_ready_since": None,
            "consecutive_ready_samples": 1,
            "phase": "paper_runtime_verified",
            "phase_detail": "Dashboard/API and tracked paper runtime are healthy, but the manager stability window is still accumulating.",
            "api_dashboard_responding": True,
            "operator_surface_loadable": True,
            "dashboard_attached": True,
            "paper_runtime_ready": True,
            "generated_at": "2026-04-08T12:06:00Z",
            "checked_at": "2026-04-08T12:06:00Z",
        }
    )

    payload = service._startup_control_plane_payload(  # noqa: SLF001
        generated_at="2026-04-08T12:06:00Z",
        auth_status={
            "runtime_ready": True,
            "detail": "Schwab connectivity/auth is ready.",
            "next_action": "Refresh",
            "source": "test_fixture",
        },
        market_context={
            "feed_state": "LIVE",
            "note": "Market-data connectivity is live.",
            "diagnostic_artifact": "/api/operator-artifact/market-index-strip-diagnostics",
        },
        paper={
            "running": True,
            "status": {"reconciliation_semantics": "CLEAN", "entries_enabled": True, "operator_halt": False},
            "runtime_recovery": {
                "status": "RUNNING",
                "operator_message": "Paper runtime is active.",
                "next_action": "Refresh",
            },
            "temporary_paper_runtime_integrity": {"mismatch_status": "MATCHED"},
            "readiness": {
                "heartbeat_reconciliation_summary": {"active_issue_count": 0},
                "order_timeout_watchdog_summary": {"active_issue_count": 0},
                "restore_validation_summary": {"unresolved_issue_count": 0},
            },
            "entry_eligibility": {
                "state_note": "Paper runtime is active.",
                "clear_action": "Refresh",
            },
        },
    )

    assert payload["overall_state"] == "READY"
    assert payload["launch_allowed"] is True
    assert payload["launch_candidate"] is True
    assert payload["dependencies_aligned"] is True
    assert payload["convergence"]["stable_ready"] is False
    dependency_rows = {item["key"]: item for item in payload["dependencies"]}
    assert dependency_rows["dashboard_backend"]["state"] == "READY"
    assert dependency_rows["dashboard_backend"]["launch_blocking"] is False


def test_health_payload_only_turns_ready_after_stable_attached_probe(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    service._dashboard_probe.update(  # noqa: SLF001
        {
            "state": "warming",
            "stable_ready": False,
            "stable_ready_since": None,
            "consecutive_ready_samples": 2,
            "phase": "paper_runtime_verified",
            "phase_detail": "Manager stability window is still accumulating.",
            "api_dashboard_responding": True,
            "operator_surface_loadable": True,
            "dashboard_attached": True,
            "paper_runtime_ready": True,
        }
    )

    warming = service.health_payload()
    assert warming["ready"] is False
    assert warming["status"] == "starting"
    assert warming["checks"]["startup_convergence_stable"]["ok"] is False

    service._dashboard_probe.update(  # noqa: SLF001
        {
            "state": "ready",
            "stable_ready": True,
            "stable_ready_since": "2026-04-08T12:07:00Z",
            "consecutive_ready_samples": 3,
            "phase": "stable_attached",
            "phase_detail": "Dashboard/API and tracked paper runtime remained attached across the manager stability window.",
            "api_dashboard_responding": True,
            "operator_surface_loadable": True,
            "dashboard_attached": True,
            "paper_runtime_ready": True,
        }
    )

    ready = service.health_payload()
    assert ready["ready"] is True
    assert ready["status"] == "ok"
    assert ready["checks"]["startup_convergence_stable"]["ok"] is True


def test_supervised_paper_operability_requires_real_runtime_usability(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)

    startup_ready = {
        "overall_state": "READY",
        "launch_allowed": True,
        "convergence": {"dashboard_attached": True},
        "primary_next_action_label": "Refresh",
    }
    usable = service._supervised_paper_operability_payload(  # noqa: SLF001
        generated_at="2026-04-08T12:08:00Z",
        startup_control_plane=startup_ready,
        paper={
            "running": True,
            "operator_state": {"operator_halt": False},
            "temporary_paper_runtime_integrity": {"mismatch_status": "MATCHED"},
            "runtime_recovery": {"operator_message": "Paper runtime is active."},
            "entry_eligibility": {"clear_action": "Refresh"},
            "readiness": {
                "runtime_running": True,
                "runtime_phase": "RUNNING",
                "entries_enabled": True,
                "operator_halt": False,
                "usable_lane_count": 3,
                "halted_lane_count": 1,
                "lane_status_summary": {"eligible_to_trade_count": 3},
            },
        },
    )
    assert usable["app_usable_for_supervised_paper"] is True
    assert usable["state"] == "USABLE"

    idle_but_attached = service._supervised_paper_operability_payload(  # noqa: SLF001
        generated_at="2026-04-08T12:08:30Z",
        startup_control_plane=startup_ready,
        paper={
            "running": True,
            "operator_state": {"operator_halt": False},
            "temporary_paper_runtime_integrity": {"mismatch_status": "MATCHED"},
            "runtime_recovery": {"operator_message": "Paper runtime is active."},
            "entry_eligibility": {
                "clear_action": "Refresh",
                "state_note": "No lane is currently triggering, but the runtime is armed.",
            },
            "readiness": {
                "runtime_running": True,
                "runtime_phase": "RUNNING",
                "entries_enabled": True,
                "operator_halt": False,
                "usable_lane_count": 0,
                "halted_lane_count": 0,
                "lane_status_summary": {"eligible_to_trade_count": 0},
            },
        },
    )
    assert idle_but_attached["app_usable_for_supervised_paper"] is True
    assert idle_but_attached["unusable_reason_code"] is None

    halted = service._supervised_paper_operability_payload(  # noqa: SLF001
        generated_at="2026-04-08T12:08:00Z",
        startup_control_plane=startup_ready,
        paper={
            "running": True,
            "operator_state": {"operator_halt": True},
            "temporary_paper_runtime_integrity": {"mismatch_status": "MATCHED"},
            "runtime_recovery": {"operator_message": "Paper runtime is active."},
            "entry_eligibility": {
                "clear_action": "Resume Entries",
                "state_note": "Paper runtime is attached, but entries remain halted.",
            },
            "readiness": {
                "runtime_running": True,
                "runtime_phase": "HALTED",
                "entries_enabled": False,
                "operator_halt": True,
                "usable_lane_count": 0,
                "halted_lane_count": 3,
                "lane_status_summary": {"eligible_to_trade_count": 0},
            },
        },
    )
    assert halted["app_usable_for_supervised_paper"] is False
    assert halted["unusable_reason_code"] == "paper_entries_halted"
    assert halted["primary_next_action"] == "Resume Entries"


def test_supervised_paper_operability_uses_startup_action_for_attach_incomplete_warmup(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)

    startup_warming = {
        "overall_state": "WARMING",
        "launch_allowed": False,
        "primary_reason_code": "stability_window_incomplete",
        "primary_reason": "Tracked paper runtime is verified, but the manager stability window has not completed yet.",
        "primary_next_action_label": "Wait for recovery",
        "primary_next_action_kind": "wait",
        "convergence": {
            "dashboard_attached": True,
            "phase": "paper_runtime_verified",
            "reason_code": "stability_window_incomplete",
        },
    }
    payload = service._supervised_paper_operability_payload(  # noqa: SLF001
        generated_at="2026-04-10T18:58:00Z",
        startup_control_plane=startup_warming,
        paper={
            "running": True,
            "operator_state": {"operator_halt": False},
            "temporary_paper_runtime_integrity": {"mismatch_status": "MATCHED"},
            "runtime_recovery": {"operator_message": "Paper runtime is active."},
            "entry_eligibility": {
                "clear_action": "No action needed; already eligible",
                "state_note": "The runtime is armed.",
            },
            "readiness": {
                "runtime_running": True,
                "runtime_phase": "RUNNING",
                "entries_enabled": True,
                "operator_halt": False,
                "usable_lane_count": 0,
                "halted_lane_count": 0,
                "lane_status_summary": {"eligible_to_trade_count": 0},
            },
        },
    )

    assert payload["app_usable_for_supervised_paper"] is True
    assert payload["state"] == "USABLE"
    assert payload["unusable_reason_code"] is None
    assert payload["primary_next_action"] == "Wait for recovery"
    assert payload["operator_action_required"] is False


def test_startup_convergence_does_not_fail_core_attach_when_temp_paper_overlay_mapping_is_unresolved(tmp_path: Path) -> None:
    service = OperatorDashboardService(
        tmp_path,
        server_info=DashboardServerInfo(
            host="127.0.0.1",
            port=8790,
            url="http://127.0.0.1:8790/",
            pid=1234,
            started_at="2026-04-10T19:09:00Z",
            build_stamp="test-build",
            instance_id="test-instance",
            info_file=None,
        ),
    )
    service._dashboard_probe.update(  # noqa: SLF001
        {
            "api_dashboard_responding": True,
            "operator_surface_loadable": True,
            "dashboard_attached": True,
            "paper_runtime_ready": True,
            "state": "warming",
        }
    )

    payload = service._startup_control_plane_payload(  # noqa: SLF001
        generated_at="2026-04-10T19:10:00Z",
        auth_status={
            "runtime_ready": True,
            "detail": "Schwab connectivity/auth is ready.",
            "next_action": "Refresh",
            "source": "test_fixture",
        },
        market_context={
            "feed_state": "LIVE",
            "note": "Market-data connectivity is live.",
            "diagnostic_artifact": "/api/operator-artifact/market-index-strip-diagnostics",
        },
        paper={
            "running": True,
            "status": {"reconciliation_semantics": "CLEAN", "entries_enabled": True, "operator_halt": False},
            "runtime_recovery": {
                "status": "RUNNING",
                "operator_message": "Paper runtime is active.",
                "next_action": "Refresh",
            },
            "temporary_paper_runtime_integrity": {
                "mismatch_status": "MISMATCH",
                "block_reason_code": "unresolved_temp_paper_overlay_mapping",
                "block_reason": "Enabled temporary paper lanes are already loaded in the running runtime, but the restart overlay mapping is still incomplete.",
            },
            "readiness": {
                "heartbeat_reconciliation_summary": {"active_issue_count": 0},
                "order_timeout_watchdog_summary": {"active_issue_count": 0},
                "restore_validation_summary": {"unresolved_issue_count": 0},
            },
            "entry_eligibility": {
                "state_note": "Paper runtime is active.",
                "clear_action": "Refresh",
            },
        },
    )

    assert payload["convergence"]["paper_runtime_ready"] is True
    assert payload["convergence"]["reason_code"] == "stability_window_incomplete"


def test_startup_control_plane_does_not_call_halted_running_runtime_ready(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    service._server_info = DashboardServerInfo(  # noqa: SLF001
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=123,
        started_at="2026-04-08T12:04:50Z",
        build_stamp="build-test",
        instance_id="instance-test",
        info_file=str(tmp_path / "operator_dashboard.json"),
    )

    payload = service._startup_control_plane_payload(  # noqa: SLF001
        generated_at="2026-04-08T12:05:00Z",
        auth_status={
            "runtime_ready": True,
            "detail": "Schwab connectivity/auth is ready.",
            "next_action": "Refresh",
            "source": "test_fixture",
        },
        market_context={
            "feed_state": "LIVE",
            "note": "Market-data connectivity is live.",
            "diagnostic_artifact": "/api/operator-artifact/market-index-strip-diagnostics",
        },
        paper={
            "running": True,
            "status": {
                "reconciliation_semantics": "CLEAN",
                "entries_enabled": False,
                "operator_halt": True,
            },
            "runtime_recovery": {
                "status": "RUNNING",
                "operator_message": "Paper runtime is active.",
                "next_action": "Resume Entries",
            },
            "readiness": {
                "runtime_status_detail": "Paper runtime is attached, but entries remain halted.",
                "heartbeat_reconciliation_summary": {"active_issue_count": 0},
                "order_timeout_watchdog_summary": {"active_issue_count": 0},
                "restore_validation_summary": {"unresolved_issue_count": 0},
            },
            "entry_eligibility": {
                "state_note": "Paper runtime is attached, but entries remain halted.",
                "fireability_summary": "Not eligible now; primary blocker is ENTRIES HALTED BY OPERATOR.",
                "clear_action": "Resume Entries",
            },
        },
    )

    dependency_rows = {item["key"]: item for item in payload["dependencies"]}
    assert dependency_rows["paper_runtime"]["state"] == "BLOCKED"
    assert dependency_rows["paper_runtime"]["next_action_label"] == "Resume Entries"
    assert payload["launch_allowed"] is False


def test_cached_dashboard_snapshot_requires_current_instance(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    generated_at = datetime.now(timezone.utc).isoformat()
    service._server_info = DashboardServerInfo(  # noqa: SLF001
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=123,
        started_at="2026-04-08T12:07:50Z",
        build_stamp="build-test",
        instance_id="instance-current",
        info_file=str(tmp_path / "operator_dashboard.json"),
    )

    service._dashboard_snapshot_path.write_text(  # noqa: SLF001
        json.dumps(
                {
                    "payload_version": DASHBOARD_PAYLOAD_SCHEMA_VERSION,
                    "generated_at": generated_at,
                    "dashboard_meta": {"server_instance_id": "instance-stale"},
                    "operator_surface": {"ok": True},
                }
        ),
        encoding="utf-8",
    )
    assert service.cached_dashboard_snapshot() is None
    service._record_dashboard_probe(snapshot=None, error=RuntimeError("dashboard probe degraded"))  # noqa: SLF001
    stale_payload = service.cached_dashboard_snapshot(allow_stale_instance=True)
    assert stale_payload is not None
    assert stale_payload["dashboard_meta"]["snapshot_fallback_active"] is True
    assert stale_payload["dashboard_meta"]["snapshot_instance_stale"] is True
    assert stale_payload["dashboard_meta"]["current_server_instance_id"] == "instance-current"
    assert stale_payload["startup_control_plane"]["overall_state"] == "DEGRADED"
    assert stale_payload["supervised_paper_operability"]["app_usable_for_supervised_paper"] is False
    assert "dashboard probe degraded" in stale_payload["supervised_paper_operability"]["summary_line"]

    service._dashboard_snapshot_path.write_text(  # noqa: SLF001
        json.dumps(
                {
                    "payload_version": DASHBOARD_PAYLOAD_SCHEMA_VERSION,
                    "generated_at": generated_at,
                    "dashboard_meta": {"server_instance_id": "instance-current"},
                    "operator_surface": {"ok": True},
                }
        ),
        encoding="utf-8",
    )
    assert service.cached_dashboard_snapshot() == {
        "payload_version": DASHBOARD_PAYLOAD_SCHEMA_VERSION,
        "generated_at": generated_at,
        "dashboard_meta": {"server_instance_id": "instance-current"},
        "operator_surface": {"ok": True},
    }


def test_cached_dashboard_snapshot_invalidates_when_runtime_sources_are_newer(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    generated_at = datetime.now(timezone.utc)
    service._server_info = DashboardServerInfo(  # noqa: SLF001
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=123,
        started_at="2026-04-08T12:07:50Z",
        build_stamp="build-test",
        instance_id="instance-current",
        info_file=str(tmp_path / "operator_dashboard.json"),
    )
    snapshot_payload = {
        "payload_version": DASHBOARD_PAYLOAD_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "dashboard_meta": {"server_instance_id": "instance-current"},
        "operator_surface": {"ok": True},
    }
    service._dashboard_snapshot_path.write_text(json.dumps(snapshot_payload), encoding="utf-8")  # noqa: SLF001

    source_path = tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_status.json"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text('{"updated_at":"2026-04-17T12:47:37+00:00"}', encoding="utf-8")
    future_source_time = (generated_at + timedelta(seconds=45)).timestamp()
    os.utime(source_path, (future_source_time, future_source_time))

    assert service.cached_dashboard_snapshot() is None

    degraded = service.cached_dashboard_snapshot(allow_stale_instance=True)
    assert degraded is not None
    assert degraded["dashboard_meta"]["snapshot_fallback_active"] is True
    assert degraded["dashboard_meta"]["snapshot_instance_stale"] is False
    assert "older than the live runtime artifacts" in degraded["supervised_paper_operability"]["summary_line"]


def test_cached_dashboard_snapshot_cache_age_only_keeps_existing_usability(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    generated_at = datetime.now(timezone.utc) - timedelta(seconds=90)
    service._server_info = DashboardServerInfo(  # noqa: SLF001
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=123,
        started_at="2026-04-08T12:07:50Z",
        build_stamp="build-test",
        instance_id="instance-current",
        info_file=str(tmp_path / "operator_dashboard.json"),
    )
    snapshot_payload = {
        "payload_version": DASHBOARD_PAYLOAD_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "dashboard_meta": {"server_instance_id": "instance-current"},
        "operator_surface": {"ok": True},
        "startup_control_plane": {"launch_allowed": True, "overall_state": "READY"},
        "supervised_paper_operability": {
            "state": "USABLE",
            "app_usable_for_supervised_paper": True,
            "summary_line": "Application is usable for supervised paper operation.",
        },
    }
    service._dashboard_snapshot_path.write_text(json.dumps(snapshot_payload), encoding="utf-8")  # noqa: SLF001

    assert service.cached_dashboard_snapshot() is None

    degraded = service.cached_dashboard_snapshot(allow_stale_instance=True)
    assert degraded is not None
    assert degraded["dashboard_meta"]["source"] == "live_api_stale_cache"
    assert degraded["dashboard_meta"]["snapshot_fallback_active"] is False
    assert degraded["startup_control_plane"]["launch_allowed"] is True
    assert degraded["supervised_paper_operability"]["app_usable_for_supervised_paper"] is True


def test_cached_dashboard_snapshot_tolerates_short_runtime_source_lead(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    generated_at = datetime.now(timezone.utc)
    service._server_info = DashboardServerInfo(  # noqa: SLF001
        host="127.0.0.1",
        port=8790,
        url="http://127.0.0.1:8790/",
        pid=123,
        started_at="2026-04-08T12:07:50Z",
        build_stamp="build-test",
        instance_id="instance-current",
        info_file=str(tmp_path / "operator_dashboard.json"),
    )
    snapshot_payload = {
        "payload_version": DASHBOARD_PAYLOAD_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "dashboard_meta": {"server_instance_id": "instance-current"},
        "operator_surface": {"ok": True},
    }
    service._dashboard_snapshot_path.write_text(json.dumps(snapshot_payload), encoding="utf-8")  # noqa: SLF001

    source_path = tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_status.json"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text('{"updated_at":"2026-04-17T12:47:30+00:00"}', encoding="utf-8")
    future_source_time = (generated_at + timedelta(seconds=20)).timestamp()
    os.utime(source_path, (future_source_time, future_source_time))

    assert service.cached_dashboard_snapshot() is not None


def test_runtime_derived_payload_cache_serves_recent_snapshot_while_runtime_advances(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    generated_at = datetime.now(timezone.utc)
    payload_path = tmp_path / "paper_signal_intent_fill_audit_snapshot.json"
    payload_path.write_text(
        json.dumps(
            {
                "payload_version": DASHBOARD_PAYLOAD_SCHEMA_VERSION,
                "generated_at": generated_at.isoformat(),
                "session_date": "2026-04-18",
                "rows": [],
            }
        ),
        encoding="utf-8",
    )

    payload, fresh = service._load_cached_runtime_derived_payload(
        payload_path,
        runtime_updated_at=(generated_at + timedelta(seconds=10)).isoformat(),
        session_date="2026-04-18",
        freshness_seconds=5.0,
        max_stale_seconds=300.0,
        runtime_updated_at_grace_seconds=1.0,
    )

    assert payload is not None
    assert payload["session_date"] == "2026-04-18"
    assert fresh is False
