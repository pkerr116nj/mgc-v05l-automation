from __future__ import annotations

import plistlib
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_service_ownership import (
    CANONICAL_PAPER_CONFIG_STACK,
    REVIEW_OVERLAY_CONFIG,
    TrackBServiceOwnershipConfig,
    build_launchd_plist,
    build_service_ownership_artifact,
    service_specs,
    validate_single_broker_mutating_runtime,
    write_launchd_plist_templates,
)


def test_service_specs_have_exactly_one_broker_mutating_runtime(tmp_path: Path) -> None:
    specs = service_specs(tmp_path)

    broker_mutating = [spec for spec in specs if spec.broker_mutating]

    assert [spec.service_id for spec in broker_mutating] == ["track_b_paper_runtime"]
    assert all("live" not in " ".join(spec.command).lower() for spec in broker_mutating)
    assert any(spec.service_id == "track_b_hourly_paper_runtime_recovery" for spec in specs)


def test_paper_runtime_plist_uses_canonical_stack_without_review_overlay(tmp_path: Path) -> None:
    paper_runtime = next(spec for spec in service_specs(tmp_path) if spec.service_id == "track_b_paper_runtime")

    plist = build_launchd_plist(paper_runtime, repo_root=tmp_path)
    config_stack = plist["EnvironmentVariables"]["MGC_HEADLESS_SUPERVISED_PAPER_CONFIG_PATHS"]

    for item in CANONICAL_PAPER_CONFIG_STACK:
        assert str(tmp_path / item) in config_stack
    assert REVIEW_OVERLAY_CONFIG not in config_stack
    assert plist["EnvironmentVariables"]["MGC_HEADLESS_REQUIRED_PAPER_CONFIGS"] == config_stack


def test_hourly_recovery_plist_is_launchd_owned_enabled_and_canonical_only(tmp_path: Path) -> None:
    recovery = next(spec for spec in service_specs(tmp_path) if spec.service_id == "track_b_hourly_paper_runtime_recovery")

    plist = build_launchd_plist(recovery, repo_root=tmp_path)

    assert recovery.owner == "launchd_user_agent"
    assert recovery.disabled_by_default is False
    assert recovery.start_interval_seconds == 120
    assert int(recovery.start_interval_seconds or 0) < 300
    assert "Disabled" not in plist
    assert plist["RunAtLoad"] is True
    assert plist["StartInterval"] == 120
    assert "canonical paper-stack status/start only" in recovery.restart_policy
    assert plist["ProgramArguments"] == ["/bin/bash", "scripts/track_b_hourly_paper_runtime_recovery.sh", "tick"]
    assert recovery.status_command == ("bash", "scripts/track_b_hourly_paper_runtime_recovery.sh", "status")
    assert recovery.start_command == ("bash", "scripts/track_b_hourly_paper_runtime_recovery.sh", "enable")
    assert recovery.stop_command == ("bash", "scripts/track_b_hourly_paper_runtime_recovery.sh", "disable")


def test_duplicate_writer_guard_blocks_manual_and_launchd_runtime() -> None:
    specs = service_specs(Path("/repo"))
    process_rows = [
        {
            "pid": 101,
            "command": "python -m mgc_v05l.app.main probationary-paper-soak --config config/base.yaml",
        },
        {"pid": 102, "command": "launchctl com.mgc.trackb.paper-runtime"},
    ]

    guard = validate_single_broker_mutating_runtime(specs=specs, process_rows=process_rows)

    assert guard["classification"] == "DUPLICATE_BROKER_MUTATING_RUNTIME_BLOCKED"
    assert guard["cutover_allowed_without_stop"] is False


def test_manual_runtime_requires_cutover_before_launchd_load() -> None:
    specs = service_specs(Path("/repo"))
    process_rows = [
        {
            "pid": 72320,
            "command": "python -m mgc_v05l.app.main probationary-paper-soak --config config/base.yaml",
        }
    ]

    guard = validate_single_broker_mutating_runtime(specs=specs, process_rows=process_rows)

    assert guard["classification"] == "MANUAL_RUNTIME_ACTIVE_CUTOVER_REQUIRED"
    assert guard["active_broker_mutating_runtime_count"] == 1
    assert guard["cutover_allowed_without_stop"] is False


def test_artifact_is_phase1_read_only_and_paper_only(tmp_path: Path) -> None:
    config = TrackBServiceOwnershipConfig(repo_root=tmp_path)

    artifact = build_service_ownership_artifact(
        config=config,
        now=datetime(2026, 5, 28, tzinfo=UTC),
        process_rows=[],
    )

    assert artifact["phase"] == "PHASE_1_TEMPLATE_ONLY_NO_CUTOVER"
    assert artifact["broker_mutation_allowed"] is False
    assert artifact["submit_authority"] is False
    assert artifact["live_money_eligible"] is False
    assert artifact["paper_proof_invoked"] is False
    assert artifact["summary"]["broker_mutating_service_count"] == 1


def test_write_launchd_plist_templates_is_repo_local(tmp_path: Path) -> None:
    config = TrackBServiceOwnershipConfig(
        repo_root=tmp_path,
        plist_output_dir=Path("var/launchd/track_b"),
    )

    paths = write_launchd_plist_templates(config=config)

    assert paths
    assert all(path.is_relative_to(tmp_path) for path in paths)
    labels = {plistlib.loads(path.read_bytes())["Label"] for path in paths}
    assert "com.mgc.trackb.paper-runtime" in labels
    assert "com.mgc.trackb.paper-runtime-recovery" in labels
