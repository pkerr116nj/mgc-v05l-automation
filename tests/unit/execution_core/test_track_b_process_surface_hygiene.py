from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_process_surface_hygiene import (
    PROCESS_SURFACE_ATTENTION,
    PROCESS_SURFACE_CLEAR,
    TrackBProcessSurfaceHygieneConfig,
    build_track_b_process_surface_hygiene,
    write_track_b_process_surface_hygiene,
)


NOW = datetime(2026, 5, 24, 1, 0, tzinfo=UTC)


def test_process_hygiene_classifies_expected_support_and_diagnostics(tmp_path: Path) -> None:
    rows = [
        {"pid": 1001, "command": "python -m mgc_v05l.execution_core.phase1_databento_live_runtime_candles --mode service"},
        {"pid": 1002, "command": "python -m mgc_v05l.app.ibkr_broker_truth_refresher --service --read-only"},
        {"pid": 1003, "command": "python -m mgc_v05l.app.operator_dashboard_cli --host 127.0.0.1"},
        {"pid": 1004, "command": "python -m mgc_v05l.app.track_b_operator_readiness_refresher --service"},
        {"pid": 1005, "command": "python -m mgc_v05l.app.ibkr_paper_strategy_monitor --mode PAPER"},
        {"pid": 1006, "command": "bash /repo/scripts/track_b_paper_preflight.sh --mode monday-live"},
        {"pid": 1007, "command": "python -m pytest tests/unit/test_example.py"},
    ]

    payload = build_track_b_process_surface_hygiene(
        config=TrackBProcessSurfaceHygieneConfig(repo_root=tmp_path),
        now=NOW,
        process_rows=rows,
        pid_running=lambda pid: True,
    )

    assert payload["classification"] == PROCESS_SURFACE_CLEAR
    assert payload["proof_blocking_processes"] == []
    assert payload["summary"]["expected_support_process_count"] == 4
    assert payload["summary"]["diagnostic_process_count"] == 3
    assert payload["summary"]["paper_strategy_monitor_count"] == 1
    assert payload["summary"]["paper_preflight_script_count"] == 1
    assert payload["summary"]["pytest_process_count"] == 1


def test_process_hygiene_blocks_active_runtime_writer(tmp_path: Path) -> None:
    rows = [
        {"pid": 2001, "command": "python -m mgc_v05l.app.main probationary-paper-soak --config paper.yaml"},
    ]

    payload = build_track_b_process_surface_hygiene(
        config=TrackBProcessSurfaceHygieneConfig(repo_root=tmp_path),
        now=NOW,
        process_rows=rows,
        pid_running=lambda pid: True,
    )

    assert payload["classification"] == PROCESS_SURFACE_ATTENTION
    assert payload["summary"]["active_runtime_writer_count"] == 1
    assert payload["proof_blocking_processes"][0]["kind"] == "track_b_paper_runtime_writer"


def test_process_hygiene_does_not_treat_audit_commands_as_runtime_writer(tmp_path: Path) -> None:
    rows = [
        {"pid": 2001, "command": "git diff --check -- scripts/run_probationary_paper_soak.sh"},
        {"pid": 2002, "command": "rg -n run_probationary_paper_soak.sh scripts"},
    ]

    payload = build_track_b_process_surface_hygiene(
        config=TrackBProcessSurfaceHygieneConfig(repo_root=tmp_path),
        now=NOW,
        process_rows=rows,
        pid_running=lambda pid: True,
    )

    assert payload["classification"] == PROCESS_SURFACE_CLEAR
    assert payload["proof_blocking_processes"] == []
    assert payload["summary"]["active_runtime_writer_count"] == 0


def test_process_hygiene_reports_stale_pid_files(tmp_path: Path) -> None:
    pid_path = tmp_path / "outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid"
    pid_path.parent.mkdir(parents=True)
    pid_path.write_text("424242\n", encoding="utf-8")

    payload = build_track_b_process_surface_hygiene(
        config=TrackBProcessSurfaceHygieneConfig(repo_root=tmp_path),
        now=NOW,
        process_rows=[],
        pid_running=lambda pid: False,
    )

    assert payload["classification"] == PROCESS_SURFACE_CLEAR
    assert payload["stale_pid_files"] == [
        {
            "label": "track_b_paper_runtime",
            "path": str(pid_path),
            "pid": 424242,
            "reason": "pid_not_running",
        }
    ]


def test_process_hygiene_write_uses_authority_artifact(tmp_path: Path) -> None:
    config = TrackBProcessSurfaceHygieneConfig(repo_root=tmp_path)
    payload = build_track_b_process_surface_hygiene(
        config=config,
        now=NOW,
        process_rows=[],
        pid_running=lambda pid: True,
    )

    path = write_track_b_process_surface_hygiene(config=config, payload=payload)

    assert path == tmp_path / "outputs/track_b_execution_core/process_hygiene/latest_track_b_process_surface_hygiene.json"
    assert path.exists()
