from __future__ import annotations

from mgc_v05l.app.headless_supervised_paper import build_headless_supervised_paper_contract


def test_headless_supervised_paper_contract_reports_usable_when_backend_and_runtime_are_ready() -> None:
    contract = build_headless_supervised_paper_contract(
        health_payload={
            "ready": True,
            "status": "ok",
            "phase": "stable_attached",
            "phase_detail": "Dashboard/API and tracked paper runtime remained attached across the manager stability window.",
        },
        startup_control_plane={
            "overall_state": "READY",
            "launch_allowed": True,
            "dependencies": [
                {"key": "dashboard_backend", "label": "Dashboard / Backend", "state": "READY", "reason": "Dashboard attached."},
                {"key": "schwab_connectivity", "label": "Schwab Connectivity / Auth", "state": "READY", "reason": "Token is runtime-ready."},
                {"key": "paper_runtime", "label": "Paper Runtime", "state": "READY", "reason": "Paper runtime is active."},
                {"key": "reconciliation", "label": "Reconciliation Needed", "state": "READY", "reason": "Reconciliation is clean."},
            ],
        },
        supervised_paper_operability={
            "app_usable_for_supervised_paper": True,
            "state": "USABLE",
            "dashboard_attached": True,
            "startup_ready": True,
            "launch_allowed": True,
            "runtime_running": True,
            "paper_runtime_phase": "RUNNING",
            "paper_runtime_ready": True,
            "entries_enabled": True,
            "operator_halt": False,
            "usable_lane_count": 0,
            "eligible_to_trade_count": 0,
            "halted_lane_count": 0,
            "operator_action_required": False,
            "primary_next_action": "Refresh",
        },
        dashboard_info={
            "dashboard_api_url": "http://127.0.0.1:8790/api/dashboard",
            "health_url": "http://127.0.0.1:8790/health",
            "pid": 123,
            "instance_id": "instance-ready",
        },
    )

    assert contract["app_usable_for_supervised_paper"] is True
    assert contract["overall_state"] == "USABLE"
    assert contract["backend"]["attached"] is True
    assert contract["paper_runtime"]["usable"] is True
    assert contract["auth"]["usable"] is True
    assert contract["reconciliation"]["needed"] is False
    assert contract["operator_ui"]["packaged_desktop_required_for_uptime"] is False


def test_headless_supervised_paper_contract_fails_closed_when_backend_is_unavailable() -> None:
    contract = build_headless_supervised_paper_contract(
        health_payload={},
        startup_control_plane={},
        supervised_paper_operability={},
        dashboard_info={},
    )

    assert contract["app_usable_for_supervised_paper"] is False
    assert contract["operator_action_required"] is True
    assert contract["backend"]["health_ready"] is False
    assert contract["backend"]["attached"] is False
    assert contract["overall_state"] == "BACKEND_UNAVAILABLE"
    assert contract["unusable_reason_code"] == "backend_health_unreachable"


def test_headless_supervised_paper_contract_keeps_auth_and_reconciliation_visible_when_not_usable() -> None:
    contract = build_headless_supervised_paper_contract(
        health_payload={"ready": True, "status": "ok"},
        startup_control_plane={
            "overall_state": "BLOCKED",
            "launch_allowed": False,
            "primary_reason": "Schwab auth is missing or expired.",
            "dependencies": [
                {"key": "dashboard_backend", "label": "Dashboard / Backend", "state": "READY", "reason": "Dashboard attached."},
                {"key": "schwab_connectivity", "label": "Schwab Connectivity / Auth", "state": "BLOCKED", "reason": "Schwab auth is missing or expired.", "next_action_label": "Auth Gate Check"},
                {"key": "paper_runtime", "label": "Paper Runtime", "state": "READY", "reason": "Paper runtime is active."},
                {
                    "key": "reconciliation",
                    "label": "Reconciliation Needed",
                    "state": "RECONCILIATION_REQUIRED",
                    "reason": "Outstanding reconciliation review is required.",
                    "next_action_label": "Force Reconcile",
                },
            ],
        },
        supervised_paper_operability={
            "app_usable_for_supervised_paper": False,
            "state": "ATTACH_INCOMPLETE",
            "unusable_reason": "Schwab auth is missing or expired.",
            "unusable_reason_code": "schwab_auth_not_ready",
            "dashboard_attached": True,
            "startup_ready": False,
            "launch_allowed": False,
            "runtime_running": True,
            "paper_runtime_phase": "RUNNING",
            "paper_runtime_ready": True,
            "entries_enabled": True,
            "operator_halt": False,
            "operator_action_required": True,
            "primary_next_action": "Auth Gate Check",
        },
        dashboard_info={"dashboard_api_url": "http://127.0.0.1:8790/api/dashboard"},
    )

    assert contract["app_usable_for_supervised_paper"] is False
    assert contract["auth"]["usable"] is False
    assert contract["auth"]["dependency"]["next_action"] == "Auth Gate Check"
    assert contract["reconciliation"]["needed"] is True
    assert contract["reconciliation"]["dependency"]["next_action"] == "Force Reconcile"
    assert contract["backend"]["attached"] is True
    assert contract["overall_state"] == "ATTACH_INCOMPLETE"


def test_headless_supervised_paper_contract_prefers_live_service_truth_over_stale_startup_snapshot() -> None:
    contract = build_headless_supervised_paper_contract(
        health_payload={
            "ready": True,
            "status": "ok",
            "phase": "stable_attached",
            "phase_detail": "Dashboard/API and tracked paper runtime remained attached across the manager stability window.",
        },
        startup_control_plane={
            "overall_state": "WARMING",
            "launch_allowed": False,
            "primary_reason": "Tracked paper runtime is verified, but the manager stability window has not completed yet.",
            "dependencies": [
                {"key": "dashboard_backend", "label": "Dashboard / Backend", "state": "READY", "reason": "Dashboard attached."},
                {"key": "schwab_connectivity", "label": "Schwab Connectivity / Auth", "state": "READY", "reason": "Token is runtime-ready."},
                {"key": "paper_runtime", "label": "Paper Runtime", "state": "READY", "reason": "Paper runtime is active."},
                {"key": "reconciliation", "label": "Reconciliation Needed", "state": "READY", "reason": "Reconciliation is clean."},
            ],
        },
        supervised_paper_operability={
            "app_usable_for_supervised_paper": True,
            "state": "USABLE",
            "dashboard_attached": True,
            "startup_ready": True,
            "launch_allowed": True,
            "runtime_running": True,
            "paper_runtime_phase": "RUNNING",
            "paper_runtime_ready": True,
            "entries_enabled": True,
            "operator_halt": False,
            "operator_action_required": False,
            "primary_next_action": "No action needed; already eligible",
        },
        dashboard_info={
            "dashboard_api_url": "http://127.0.0.1:8790/api/dashboard",
            "health_url": "http://127.0.0.1:8790/health",
            "pid": 123,
            "instance_id": "instance-ready",
        },
    )

    assert contract["app_usable_for_supervised_paper"] is True
    assert contract["backend"]["attached"] is True
    assert contract["backend"]["startup_ready"] is False
    assert contract["backend"]["launch_allowed"] is False
    assert contract["overall_state"] == "USABLE"
