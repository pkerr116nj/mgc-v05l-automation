from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from mgc_v05l.app.weekly_data_maintenance import (
    PHASE1_RUNTIME_TICKER_ORDER,
    WeeklyMaintenanceConfig,
    build_weekly_data_maintenance_report,
    main,
    write_weekly_data_maintenance_report,
)


def _write(path: Path, text: str = "{}") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_apply_mode_rejected_before_outputs(tmp_path: Path) -> None:
    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, mode="apply")
    )

    assert report["final_verdict"] == "APPLY_MODE_NOT_IMPLEMENTED"
    assert report["review_required"] is True


def test_default_symbols_are_phase1_ten(tmp_path: Path) -> None:
    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, week_ending=date(2026, 5, 8))
    )

    assert tuple(report["symbols"]) == PHASE1_RUNTIME_TICKER_ORDER
    assert report["phase1_symbol_count"] == 10
    assert report["week_start"] == "2026-05-02"
    assert report["week_end"] == "2026-05-08"


def test_active_runtime_files_are_preserved_not_delete_candidates(tmp_path: Path) -> None:
    _write(
        tmp_path / "var" / "runtime_market_data" / "MNQ" / "1m" / "latest_runtime_candles.json",
        '{"symbol":"MNQ"}',
    )
    _write(
        tmp_path / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data" / "GC" / "1m" / "latest_runtime_candles.json",
        '{"symbol":"GC"}',
    )

    report = build_weekly_data_maintenance_report(config=WeeklyMaintenanceConfig(repo_root=tmp_path))
    rows = {row["path"]: row for row in report["classifications"]}

    assert rows["var/runtime_market_data/MNQ/1m/latest_runtime_candles.json"]["classification"] == "HOT_DECISION_RUNTIME_DATA"
    assert rows["outputs/track_b_execution_core/phase1_runtime_market_data/GC/1m/latest_runtime_candles.json"]["classification"] == "HOT_DECISION_RUNTIME_DATA"
    assert report["delete_candidates_count"] == 0


def test_broker_review_evidence_is_archive_or_preserve_not_delete(tmp_path: Path) -> None:
    _write(tmp_path / "outputs" / "reports" / "manual_reconciliation_close" / "latest_review_required.json")
    _write(tmp_path / "outputs" / "reports" / "ibkr_bridge" / "broker_order_evidence.json")

    report = build_weekly_data_maintenance_report(config=WeeklyMaintenanceConfig(repo_root=tmp_path))
    rows = {row["path"]: row for row in report["classifications"]}

    assert rows["outputs/reports/manual_reconciliation_close/latest_review_required.json"]["classification"] == "COLD_ARCHIVE_CANDIDATE"
    assert rows["outputs/reports/ibkr_bridge/broker_order_evidence.json"]["classification"] == "COLD_ARCHIVE_CANDIDATE"
    assert report["delete_candidates_count"] == 0


def test_disposable_build_metadata_is_delete_candidate_dry_run_only(tmp_path: Path) -> None:
    _write(tmp_path / "src" / "mgc_v05l_automation.egg-info" / "PKG-INFO", "metadata")
    _write(tmp_path / ".pytest_cache" / "v" / "cache" / "nodeids", "[]")
    _write(tmp_path / "src" / "mgc_v05l" / "app" / "__pycache__" / "x.pyc", "bytecode")

    report = build_weekly_data_maintenance_report(config=WeeklyMaintenanceConfig(repo_root=tmp_path))

    disposable = [
        row for row in report["classifications"] if row["classification"] == "DISPOSABLE_BUILD"
    ]
    assert len(disposable) == 3
    assert report["delete_candidates_count"] == 3
    assert all(row["recommended_action"] == "delete_candidate_dry_run_only" for row in disposable)


def test_research_offline_paths_are_deferred(tmp_path: Path) -> None:
    _write(tmp_path / "docs" / "atp_companion_note.md", "note")
    _write(tmp_path / "docs" / "us_open_ndx_note.md", "note")
    _write(tmp_path / "src" / "mgc_v05l" / "app" / "atp_probe.py", "")
    _write(tmp_path / "src" / "mgc_v05l" / "app" / "us_open_probe.py", "")
    _write(tmp_path / "src" / "mgc_v05l" / "research" / "offline.py", "")
    _write(tmp_path / "examples" / "track_b_shadow_listener" / "inbox" / "signal.json", "{}")

    report = build_weekly_data_maintenance_report(config=WeeklyMaintenanceConfig(repo_root=tmp_path))

    assert report["deferred_research_count"] == 6
    assert {
        row["classification"] for row in report["classifications"]
    } == {"DEFERRED_RESEARCH_OFFLINE"}


def test_old_root_detection_triggers_review_required(tmp_path: Path) -> None:
    _write(
        tmp_path / "scripts" / "bad.sh",
        "/Users/patrick/Documents/MGC-v05l-automation/scripts/run_probationary_paper_soak.sh",
    )

    report = build_weekly_data_maintenance_report(config=WeeklyMaintenanceConfig(repo_root=tmp_path))

    assert report["review_required"] is True
    assert report["final_verdict"] == "DRY_RUN_ONLY_REVIEW_REQUIRED"
    assert report["old_root_hits"] == ["scripts/bad.sh:1"]


def test_report_schema_and_written_artifacts(tmp_path: Path) -> None:
    report = build_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, week_ending=date(2026, 5, 8))
    )
    written = write_weekly_data_maintenance_report(
        config=WeeklyMaintenanceConfig(repo_root=tmp_path, week_ending=date(2026, 5, 8)),
        report=report,
    )

    payload = json.loads(written["json"].read_text(encoding="utf-8"))
    assert payload["schema_version"] == "weekly_data_maintenance_v1"
    assert payload["policy"]["delete_files"] is False
    assert payload["policy"]["move_files"] is False
    assert written["markdown"].name == "weekly_data_maintenance_2026-05-08.md"


def test_cli_dry_run_writes_report_and_apply_returns_two(tmp_path: Path) -> None:
    output_root = tmp_path / "reports"
    assert (
        main(
            [
                "--mode",
                "dry-run",
                "--repo-root",
                str(tmp_path),
                "--week-ending",
                "2026-05-08",
                "--output-root",
                str(output_root),
            ]
        )
        == 0
    )
    assert (output_root / "latest_weekly_data_maintenance_report.json").exists()
    assert main(["--mode", "apply", "--repo-root", str(tmp_path), "--output-root", str(output_root)]) == 2
