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
    assert "READY_TO_START_DIAGNOSTIC_ONLY" in source
    assert "submit remains disabled" in source


def test_paper_stack_restart_uses_owned_exposure_authority() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")
    soak_source = (REPO_ROOT / "scripts" / "run_probationary_paper_soak.sh").read_text(encoding="utf-8")

    assert "track_b_paper_stack_restart_precheck" in source
    assert "restart_authority_allowed" in source
    assert "track_b_paper_stack_restart_precheck" in soak_source
    assert "_safe_owned_exposure_restart_override" in soak_source
    assert "RUNTIME_DOWN_WITH_BROKER_EXPOSURE" in soak_source
    assert "RESTART_ALLOWED_FLAT_RECONCILED" not in source
    assert "restart_precheck_classification" in source
    assert "BLOCKED_UNMANAGED_EXPOSURE" not in source
    assert "Broker/lifecycle/safety state is not clean enough for a controlled restart." in source


def test_paper_stack_start_enables_recovery_service_unless_operator_opts_out() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "ensure_recovery_service_enabled" in source
    assert "track_b_hourly_paper_runtime_recovery.sh\" enable" in source
    assert "TRACK_B_PAPER_STACK_DISABLE_RECOVERY_SERVICE" in source
    assert "MGC_TRACK_B_DISABLE_STANDALONE_RECOVERY" in source
    assert "WARNING_RECOVERY_SERVICE_ENABLE_FAILED" in source


def test_paper_stack_start_has_session_coverage_active_evidence_profile() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "mnq_mes_session_coverage_active_evidence" in source
    assert '"PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_SHORT_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_LONG_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_SHORT_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_LONG_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_SHORT_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_LONG_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_SHORT_V1"' in source
    assert 'PROOF_REQUIRED_SYMBOLS="MNQ,MES"' in source


def test_paper_stack_start_has_london_open_active_evidence_extension_profile() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "mnq_mes_london_open_active_evidence" in source
    assert '"extends_profile": "mnq_mes_session_coverage_active_evidence"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1"' in source
    assert '"PAPER_WATCH_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_LONG_SHADOW_V1"' in source
    assert "LONDON_LATE_CANONICAL_ANCHOR_NOT_YET_DEFINED_FOR_BROKER_AUTHORITY" in source


def test_paper_stack_start_has_london_late_mnq_short_active_evidence_profile() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "mnq_mes_london_late_mnq_short_active_evidence" in source
    assert '"extends_profile": "mnq_mes_session_coverage_active_evidence"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1"' in source
    assert '"PAPER_WATCH_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_LONG_SHADOW_V1"' in source
    assert '"PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_LONG_SHADOW_V1"' in source
    assert '"PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_SHADOW_V1"' in source
    assert "LONDON_LATE_MNQ_SHORT_ONLY_INITIAL_PAPER_ELEVATION" in source
    assert "COMBINED_MNQ_MES_LONDON_CONFLICT_GROUP_LIMITS_SESSION_TO_ONE_TRADE" in source


def test_recovery_operator_controls_and_status_are_launchd_based() -> None:
    source = RECOVERY_SCRIPT.read_text(encoding="utf-8")

    assert "RECOVERY_ACTIVE" in source
    assert "RECOVERY_DISABLED_BY_OPERATOR" in source
    assert "SUPERVISOR_PAUSED" in source
    assert "RECOVERY_TICK_INTERVAL_SECONDS=120" in source
    assert "ACTIVE_SESSION_WATCHDOG_120S" in source
    assert "canonical_paper_stack_restart_precheck" in source
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
    assert "RECOVERY_TICK_INTERVAL_SECONDS = 120.0" in source
    assert "RECOVERY_TICK_STALE_SECONDS" in source
    assert "stale_recovery_tick_live_scheduler_paused" in source
    assert "recovery_tick_fresh" in source
    assert "runtime_start_allowed" in source
    assert "submit_allowed" in source
    assert "recovery_authoritative" in source
    assert "standalone_recovery_classification" in source
    assert "RECOVERY_ACTIVE" in source
    assert "RECOVERY_DISABLED_BY_OPERATOR" in source
    assert "standalone_recovery_launchd_loaded" in source
    assert "launchctl\", \"print\"" in source
    assert "standalone_recovery_last_action" in source
    assert "standalone_recovery_last_blocker" in source


def test_status_surfaces_current_hot_path_registry_truth_diagnostics_read_only() -> None:
    source = STATUS_SCRIPT.read_text(encoding="utf-8")

    assert "TrackBRegistryTruthDiagnosticsConfig" in source
    assert "TrackBDiagnosticsMode.CURRENT_HOT_PATH" in source
    assert "registry_truth_diagnostics" in source
    assert '"diagnostic_only": True' in source
    assert "track_b_managed_futures_position_count" in source
    assert "broker_open_order_count" in source
    assert "lifecycle_open_position_count" in source
    assert "current_scope_review_required_count" in source
    assert "historical_quarantined_count" in source
    assert "latest_lifecycle_stress_preflight_hard_failure_count" in source
    assert "stale_authority_reason_codes" in source
    assert "full_artifact_audit_mode" in source
    assert "separate_diagnostic_only" in source
    assert "submit_gate" not in source


def test_status_refreshes_runtime_authority_and_reports_idle_window_classification() -> None:
    source = STATUS_SCRIPT.read_text(encoding="utf-8")

    assert "track_b_authority_refresh_heartbeat" in source
    assert "track_b_live_runtime_environment_watchdog" in source
    assert "AUTHORITY_REFRESH_RC" in source
    assert "LIVE_RUNTIME_ENVIRONMENT_RC" in source
    assert "authority_refresh" in source
    assert "live_runtime_environment" in source
    assert "liveness_contract" in source
    assert "latest_successful_refresh_at" in source
    assert "authority_refresh_fresh" in source
    assert "all_lanes_out_of_window" in source
    assert "OUT_OF_WINDOW_BUT_AUTHORITY_FRESH" in source
    assert "BLOCKED_STALE_TRUTH" in source
    assert "activity_classification" in source
    assert "active_window_lane_count" in source
    assert "out_of_window_lane_count" in source
    assert "broker_mutation_allowed" in source


def test_canonical_paper_config_uses_phase1_artifact_market_data() -> None:
    source = PAPER_CONFIG.read_text(encoding="utf-8")

    assert 'probationary_paper_market_data_source: "phase1_runtime_artifact"' in source
