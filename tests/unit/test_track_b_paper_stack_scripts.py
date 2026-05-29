from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
START_SCRIPT = REPO_ROOT / "scripts" / "track_b_start_paper_stack.sh"
STATUS_SCRIPT = REPO_ROOT / "scripts" / "track_b_status_paper_stack.sh"
RECOVERY_SCRIPT = REPO_ROOT / "scripts" / "track_b_hourly_paper_runtime_recovery.sh"
PAPER_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper.yaml"


def test_paper_stack_start_uses_canonical_config_without_review_overlay() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "config/base.yaml" in source
    assert "config/live.yaml" in source
    assert "config/probationary_pattern_engine.yaml" in source
    assert "config/headless_supervised_paper_runtime.yaml" in source
    assert "config/probationary_pattern_engine_paper.yaml" in source
    assert "probationary_pattern_engine_paper_mnq_mgc_plus_mnq_us_intraday_review.yaml" in source
    assert "BLOCKED_FORBIDDEN_REVIEW_OVERLAY" in source


def test_paper_stack_start_launches_foreground_runtime_inside_screen() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "screen -dmS" in source
    assert "exec bash" in source
    assert "--background" not in source
    assert "run_probationary_paper_soak.sh" in source


def test_paper_stack_start_uses_launchctl_not_nohup_as_fallback() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "launchctl submit" in source
    assert "BLOCKED_UNSUPPORTED_CARRIER" in source
    assert "nohup /bin/bash" not in source


def test_paper_stack_start_requires_sustained_readiness() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "TRACK_B_PAPER_STACK_STABLE_SECONDS" in source
    assert "RUNTIME_RUNNING_WAITING_FOR_SUSTAINED_READINESS" in source
    assert "BLOCKED_RUNTIME_EXITED_DURING_STARTUP" in source
    assert "remained READY_SUBMIT_CAPABLE" in source


def test_paper_stack_start_enables_recovery_service_unless_operator_opts_out() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "ensure_recovery_service_enabled" in source
    assert "track_b_hourly_paper_runtime_recovery.sh\" enable" in source
    assert "TRACK_B_PAPER_STACK_DISABLE_RECOVERY_SERVICE" in source
    assert "MGC_TRACK_B_DISABLE_STANDALONE_RECOVERY" in source
    assert "WARNING_RECOVERY_SERVICE_ENABLE_FAILED" in source


def test_recovery_operator_controls_and_status_are_launchd_based() -> None:
    source = RECOVERY_SCRIPT.read_text(encoding="utf-8")

    assert "RECOVERY_ACTIVE" in source
    assert "RECOVERY_DISABLED_BY_OPERATOR" in source
    assert "SUPERVISOR_PAUSED" in source
    assert "launchctl print" in source
    assert "launchctl list" in source
    assert "last_action" in source
    assert "last_blocker" in source
    assert "recovery_disabled_by_operator.json" in source


def test_recovery_enable_disable_manage_launchd_and_operator_marker() -> None:
    source = RECOVERY_SCRIPT.read_text(encoding="utf-8")

    assert "launchctl bootstrap" in source
    assert "launchctl enable" in source
    assert "rm -f \"${DISABLED_MARKER}\"" in source
    assert "launchctl bootout" in source
    assert "launchctl disable" in source
    assert "write_disabled_marker" in source


def test_recovery_tick_actions_are_safe_and_canonical() -> None:
    source = RECOVERY_SCRIPT.read_text(encoding="utf-8")

    assert "NO_ACTION_RUNTIME_RUNNING" in source
    assert "NO_ACTION_BLOCKED_GATES" in source
    assert "NO_ACTION_DUPLICATE_WRITER" in source
    assert "START_REQUESTED_CANONICAL_PAPER_STACK" in source
    assert "duplicate_writer.duplicate_writer_detected" in source
    assert "restart_allowed_if_runtime_down" in source
    assert "bash \"${START_SCRIPT}\"" in source
    assert "placeorder" not in source.lower()
    assert "cancelorder" not in source.lower()
    assert "flatten" not in source.lower()


def test_status_reports_dashboard_as_non_authority_and_duplicate_writer_state() -> None:
    source = STATUS_SCRIPT.read_text(encoding="utf-8")

    assert "dashboard" not in source.lower() or "operator_dashboard_readiness" in source
    assert "duplicate_writer_detected" in source
    assert "review_overlay_active" in source
    assert "live_money_eligible" in source
    assert "paper_proof_invoked" in source
    assert "running_process_owner_unknown" in source
    assert "runtime_owner_metadata_unavailable" in source
    assert "live_scheduler_classification" in source
    assert "hourly_recovery_paused" in source
    assert "stale_hourly_recovery_artifact_live_scheduler_paused" in source
    assert "recovery_authoritative" in source
    assert "standalone_recovery_classification" in source
    assert "RECOVERY_ACTIVE" in source
    assert "RECOVERY_DISABLED_BY_OPERATOR" in source
    assert "standalone_recovery_launchd_loaded" in source
    assert "launchctl\", \"print\"" in source
    assert "standalone_recovery_last_action" in source
    assert "standalone_recovery_last_blocker" in source


def test_canonical_paper_config_uses_phase1_artifact_market_data() -> None:
    source = PAPER_CONFIG.read_text(encoding="utf-8")

    assert 'probationary_paper_market_data_source: "phase1_runtime_artifact"' in source
