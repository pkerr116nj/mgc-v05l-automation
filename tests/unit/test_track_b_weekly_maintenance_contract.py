from __future__ import annotations

import plistlib
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DOC_PATH = REPO_ROOT / "docs" / "track_b_weekly_maintenance_contract.md"
PLIST_PATH = (
    REPO_ROOT
    / "deploy"
    / "launchd"
    / "templates"
    / "com.mgc_v05l.weekly_data_maintenance.dry_run.plist"
)
ORCHESTRATOR_PLIST_PATH = (
    REPO_ROOT
    / "deploy"
    / "launchd"
    / "templates"
    / "com.mgc_v05l.track_b_weekly_maintenance_orchestrator.plist"
)


def _plist() -> dict[str, object]:
    return plistlib.loads(PLIST_PATH.read_bytes())


def _orchestrator_plist() -> dict[str, object]:
    return plistlib.loads(ORCHESTRATOR_PLIST_PATH.read_bytes())


def test_weekly_maintenance_launchd_template_is_dry_run_only() -> None:
    payload = _plist()
    args = [str(item) for item in payload["ProgramArguments"]]
    text = PLIST_PATH.read_text(encoding="utf-8")

    assert payload["Label"] == "com.mgc_v05l.weekly_data_maintenance.dry_run"
    assert "mgc_v05l.app.weekly_data_maintenance" in args
    assert "--mode" in args
    assert args[args.index("--mode") + 1] == "dry-run"
    assert "apply" not in args
    assert "--confirm-disposable-build-cleanup" not in args
    assert "Template only" in text
    assert "Not installed" in text


def test_weekly_maintenance_launchd_template_has_safe_schedule_and_paths() -> None:
    payload = _plist()
    schedule = payload["StartCalendarInterval"]
    stdout = str(payload["StandardOutPath"])
    stderr = str(payload["StandardErrorPath"])

    assert schedule == {"Weekday": 0, "Hour": 9, "Minute": 0}
    assert payload["WorkingDirectory"] == "/Users/patrick/Dev/MGC-v05l-automation"
    assert "/outputs/reports/weekly_data_maintenance/" in stdout
    assert "/outputs/reports/weekly_data_maintenance/" in stderr
    assert stdout.endswith("weekly_data_maintenance.stdout.log")
    assert stderr.endswith("weekly_data_maintenance.stderr.log")


def test_weekly_maintenance_launchd_template_has_no_mutating_terms() -> None:
    forbidden = {
        "paper_proof",
        "placeOrder",
        "cancelOrder",
        "reqGlobalCancel",
        "globalCancel",
        "submit_order",
        "close_position",
        "flatten",
        "live_money",
    }
    text = PLIST_PATH.read_text(encoding="utf-8")

    assert not any(term in text for term in forbidden)


def test_weekly_orchestrator_launchd_template_runs_hourly_sat_sun() -> None:
    payload = _orchestrator_plist()
    args = [str(item) for item in payload["ProgramArguments"]]
    schedule = payload["StartCalendarInterval"]
    stdout = str(payload["StandardOutPath"])
    stderr = str(payload["StandardErrorPath"])
    text = ORCHESTRATOR_PLIST_PATH.read_text(encoding="utf-8")

    assert payload["Label"] == "com.mgc_v05l.track_b_weekly_maintenance_orchestrator"
    assert "mgc_v05l.execution_core.track_b_weekly_maintenance_orchestrator" in args
    assert "--output-path" in args
    assert (
        args[args.index("--output-path") + 1]
        == "outputs/track_b_execution_core/weekly_maintenance/latest_weekly_maintenance_orchestrator.json"
    )
    assert "--markdown-report-root" in args
    assert args[args.index("--markdown-report-root") + 1] == "outputs/reports/weekly_maintenance"
    assert isinstance(schedule, list)
    assert {"Weekday": 6, "Minute": 0} in schedule
    assert {"Weekday": 0, "Minute": 0} in schedule
    assert "/outputs/reports/weekly_maintenance/" in stdout
    assert "/outputs/reports/weekly_maintenance/" in stderr
    assert "Template only" in text
    assert "Not installed" in text


def test_weekly_orchestrator_launchd_template_has_no_mutating_terms() -> None:
    forbidden = {
        "apply",
        "paper_proof",
        "placeOrder",
        "cancelOrder",
        "reqGlobalCancel",
        "globalCancel",
        "submit_order",
        "close_position",
        "flatten",
        "live_money",
    }
    text = ORCHESTRATOR_PLIST_PATH.read_text(encoding="utf-8")
    args = [str(item) for item in _orchestrator_plist()["ProgramArguments"]]

    assert "paper_proof" not in text
    assert not any(term in args for term in forbidden)


def test_weekly_maintenance_contract_separates_hygiene_from_data_maintenance() -> None:
    text = DOC_PATH.read_text(encoding="utf-8")

    assert "weekly_data_maintenance" in text
    assert "diagnostic-only and non-proof-blocking" in text
    assert "track_b_data_maintenance_cli" in text
    assert "historical MGC 1m maintenance" in text
    assert "Neither lane may promote itself to proof authority" in text
    assert "Saturday 00:00 America/New_York" in text
    assert "Sunday 01:00 America/New_York" in text
    assert "Sunday 16:00 America/New_York" in text
    assert "WEEKLY_MAINTENANCE_ALREADY_COMPLETE" in text
    assert "WEEKLY_MAINTENANCE_ALERT_REQUIRED" in text
    assert "alert_required=true" in text
    assert "fresh Control Plane Snapshot evidence" in text
    assert "Phase-1 readiness and runtime market-data provenance" in text
    assert "broker truth lease and clean broker reconciliation" in text
