import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
START_SCRIPT = REPO_ROOT / "scripts" / "track_b_start_paper_stack.sh"
STATUS_SCRIPT = REPO_ROOT / "scripts" / "track_b_status_paper_stack.sh"
RECOVERY_SCRIPT = REPO_ROOT / "scripts" / "track_b_hourly_paper_runtime_recovery.sh"
PAPER_CONFIG = REPO_ROOT / "config" / "probationary_pattern_engine_paper.yaml"
GUARDED_ROSTER_CONFIG = REPO_ROOT / "config" / "track_b_guarded_paper_roster.json"


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


def test_paper_stack_start_refreshes_authority_evidence_before_carrier_launch() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "run_startup_preflight_evidence_refresh" in source
    assert "track_b_readiness_state" in source
    assert "track_b_control_plane_snapshot" in source
    assert "--no-broker-lease-history" in source
    assert "startup_preflight_refresh_attempted" in source
    assert "startup_preflight_refresh_classification" in source
    assert "refreshed_artifact_paths" in source
    assert "remaining_start_blockers" in source

    launch_block = source[source.index('if ! run_startup_preflight_evidence_refresh; then') :]
    assert launch_block.index("run_startup_preflight_evidence_refresh") < launch_block.index(
        "write_approved_profile_artifact"
    )
    assert launch_block.index("run_startup_preflight_evidence_refresh") < launch_block.index(
        'cat > "${WRAPPER_PATH}"'
    )
    assert launch_block.index("run_startup_preflight_evidence_refresh") < launch_block.index(
        "launchctl submit"
    )
    assert launch_block.index("run_startup_preflight_evidence_refresh") < launch_block.index(
        "screen -dmS"
    )


def test_paper_stack_start_blocks_before_carrier_when_preflight_refresh_fails() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "BLOCKED_START_PREFLIGHT_REFRESH" in source
    assert "STARTUP_PREFLIGHT_REFRESH_FAILED" in source
    assert "STARTUP_PREFLIGHT_REFRESH_BLOCKED" in source
    assert "canonical_readiness_refresh_failed" in source
    assert "control_plane_refresh_failed" in source
    assert "control_plane_primary_blocker" in source

    launch_block = source[source.index('if ! run_startup_preflight_evidence_refresh; then') :]
    blocked = launch_block.index('exit 2')
    assert blocked < launch_block.index('cat > "${WRAPPER_PATH}"')
    assert blocked < launch_block.index("launchctl submit")


def _startup_preflight_decision_python() -> str:
    source = START_SCRIPT.read_text(encoding="utf-8")
    return source.split('> "${result_json}" <<\'PY\'\n', 1)[1].split("\nPY\n", 1)[0]


def _run_startup_preflight_decision(
    tmp_path: Path,
    *,
    control: dict,
    reconciliation: dict | None = None,
    status: dict | None = None,
    readiness: dict | None = None,
    readiness_rc: int = 0,
    control_rc: int = 0,
    status_rc: int = 0,
) -> dict:
    repo_root = tmp_path / "repo"
    reconciliation_path = (
        repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation_path.parent.mkdir(parents=True, exist_ok=True)
    reconciliation_path.write_text(
        json.dumps(
            reconciliation
            or {
                "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
                "broker_reconciled": True,
                "track_b_broker_position_count": 0,
                "track_b_broker_open_order_count": 0,
                "current_scope_lifecycle_open_position_count": 0,
                "lifecycle_open_order_count": 0,
            }
        ),
        encoding="utf-8",
    )
    status_path = tmp_path / "status.json"
    readiness_path = tmp_path / "readiness.json"
    control_path = tmp_path / "control.json"
    status_path.write_text(
        json.dumps(
            status
            or {
                "safety": {
                    "paper_only": True,
                    "live_money_eligible": False,
                    "paper_proof_invoked": False,
                    "broker_mutation_allowed": False,
                }
            }
        ),
        encoding="utf-8",
    )
    readiness_path.write_text(json.dumps(readiness or {}), encoding="utf-8")
    control_path.write_text(json.dumps(control), encoding="utf-8")
    completed = subprocess.run(
        [
            sys.executable,
            "-",
            str(repo_root),
            str(status_path),
            str(readiness_path),
            str(control_path),
            str(readiness_rc),
            str(control_rc),
            str(status_rc),
            str(readiness_path),
            str(control_path),
        ],
        input=_startup_preflight_decision_python(),
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(completed.stdout)


def test_paper_stack_start_allows_ready_control_plane_with_informational_primary_reason(tmp_path: Path) -> None:
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
            "top_line_classification": "READY_FOR_OPERATOR_START",
            "supervisor_mode": "READY_FOR_OPERATOR_START",
            "blockers": [],
            "primary_blocking_agent_id": "",
            "primary_blocking_reason": "Shared truth is clean and PAPER policy allows bounded autonomous runtime retry.",
        },
    )

    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_CLEAN"
    assert result["remaining_start_blockers"] == []


def test_paper_stack_start_blocks_control_plane_not_start_safe_with_primary_reason(tmp_path: Path) -> None:
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
            "safe_to_start_runtime": False,
            "top_line_classification": "BLOCKED",
            "blockers": [],
            "primary_blocking_agent_id": "",
            "primary_blocking_reason": "canonical_readiness_refresher: artifact_stale",
        },
    )

    codes = {row["code"] for row in result["remaining_start_blockers"]}
    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_BLOCKED"
    assert "control_plane_start_not_allowed" in codes
    assert "control_plane_primary_blocker" in codes


def test_paper_stack_start_blocks_real_control_plane_blocker_sources(tmp_path: Path) -> None:
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
            "top_line_classification": "READY_FOR_OPERATOR_START",
            "blockers": [{"code": "open_order_truth", "detail": "ORDER_TRUTH_STALE"}],
            "primary_blocking_agent_id": "managed_order_registry",
            "primary_blocking_reason": "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED",
        },
    )

    codes = {row["code"] for row in result["remaining_start_blockers"]}
    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_BLOCKED"
    assert "control_plane_reported_blockers" in codes
    assert "control_plane_primary_blocker" in codes


def test_paper_stack_start_blocks_explicit_unsafe_or_hard_hold_status(tmp_path: Path) -> None:
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
            "top_line_classification": "READY_FOR_OPERATOR_START",
            "safe_state_classification": "SAFE_STATE_HARD_HOLD",
            "blockers": [],
            "primary_blocking_agent_id": "",
            "primary_blocking_reason": "diagnostic text",
        },
    )

    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_BLOCKED"
    assert {row["code"] for row in result["remaining_start_blockers"]} == {
        "control_plane_explicit_unsafe_status"
    }


def test_paper_stack_start_preflight_refresh_still_blocks_non_flat_broker_state(tmp_path: Path) -> None:
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
            "top_line_classification": "READY_FOR_OPERATOR_START",
            "blockers": [],
        },
        reconciliation={
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_position_count": 1,
            "track_b_broker_open_order_count": 0,
            "current_scope_lifecycle_open_position_count": 0,
            "lifecycle_open_order_count": 0,
        },
    )

    codes = {row["code"] for row in result["remaining_start_blockers"]}
    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_BLOCKED"
    assert "broker_positions_or_orders_not_flat" in codes


def test_paper_stack_start_preflight_refresh_preserves_broker_safety_gates() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")
    refresh_block = source.split("run_startup_preflight_evidence_refresh() {", 1)[1].split(
        "\nscreen_available()",
        1,
    )[0]

    assert "broker_positions_or_orders_not_flat" in refresh_block
    assert "lifecycle_positions_or_orders_not_flat" in refresh_block
    assert "broker_lifecycle_not_reconciled" in refresh_block
    assert "live_money_eligible_not_false" in refresh_block
    assert "paper_proof_invoked_not_false" in refresh_block
    assert "broker_mutation_allowed_not_false" in refresh_block
    assert "control_plane_start_not_allowed" in refresh_block
    assert "STARTUP_PREFLIGHT_REFRESH_CLEAN" in refresh_block

    lowered = refresh_block.lower()
    assert "placeorder" not in lowered
    assert "cancelorder" not in lowered
    assert "reqglobalcancel" not in lowered
    assert "global_cancel" not in lowered
    assert "broad_flatten" not in lowered


def test_paper_stack_start_timeout_reports_startup_phase_without_exit_change() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "startup_phase_timeout_detail" in source
    assert "startup_phase_current_phase" in source
    assert "startup_phase_current_blocker" in source
    assert "startup_blocker_phase" in source
    assert "startup_blocker=" in source
    timeout_block = source[source.index('write_startup_artifact "BLOCKED_START_TIMEOUT"') :]
    assert "timeout_startup_phase_detail" in timeout_block
    assert timeout_block.index('write_startup_artifact "BLOCKED_START_TIMEOUT"') < timeout_block.index("exit 1")
    assert "exit 0" not in timeout_block[: timeout_block.index("exit 1")]


def test_paper_stack_status_surfaces_startup_phase_diagnostic_only() -> None:
    source = STATUS_SCRIPT.read_text(encoding="utf-8")

    assert "classify_track_b_startup_phase" in source
    assert '"startup_phase": startup_phase' in source
    assert '"startup_phase_diagnostic_only": True' in source
    assert '"startup_phase_classification": startup_phase.get("classification")' in source
    assert '"startup_phase_current_phase": startup_phase.get("phase")' in source
    assert '"startup_phase_current_blocker": (startup_phase.get("current_blockers") or [None])[0]' in source
    assert '"startup_phase_submit_authority": startup_phase.get("submit_authority")' in source
    assert '"startup_phase_broker_mutation_allowed": startup_phase.get("broker_mutation_allowed")' in source
    assert '"startup_phase_paper_proof_invoked": startup_phase.get("paper_proof_invoked")' in source
    assert '"startup_phase_live_money_eligible": startup_phase.get("live_money_eligible")' in source
    assert "startup_phase=" in source
    assert "ready_submit_capable = startup_phase" not in source
    assert "normal_submit_allowed = startup_phase" not in source


def test_paper_stack_runtime_pid_metadata_temp_path_is_per_process() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert 'tmp = path.with_name(f".{path.name}.{sys.argv[2]}.tmp")' in source
    metadata_block = source.split('"${PYTHON_BIN}" - <<\'PY\' "${PID_METADATA_FILE}"', 1)[1].split("PY", 1)[0]
    assert 'tmp = path.with_name(f".{path.name}.{sys.argv[2]}.tmp")' in metadata_block
    assert 'tmp = path.with_name(f".{path.name}.tmp")' not in metadata_block


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


def test_recovery_tick_does_not_treat_absent_runtime_with_broker_exposure_as_running() -> None:
    source = RECOVERY_SCRIPT.read_text(encoding="utf-8")

    assert "live_runtime_environment.classification" in source
    assert "live_runtime_environment.runtime.pid_alive" in source
    assert '"${live_runtime_classification}" == "RUNTIME_DOWN_WITH_BROKER_EXPOSURE"' in source
    assert '"${live_runtime_pid_alive}" == "false"' in source
    runtime_running_block = source.split('if [[ "${runtime_running}" == "true" ]]; then', 1)[0]
    assert "NO_ACTION_RUNTIME_RUNNING" not in runtime_running_block


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


def test_paper_stack_start_has_full_session_active_evidence_profile() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "mnq_mes_full_session_active_evidence" in source
    assert '"profile": "mnq_mes_full_session_active_evidence"' in source
    assert '"extends_profile": "mnq_mes_session_coverage_active_evidence"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_SHORT_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_LONG_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_SHORT_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_LONG_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_SHORT_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_LONG_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_SHORT_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1"' in source
    assert '"PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1"' in source
    assert '"PAPER_WATCH_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_LONG_SHADOW_V1"' in source
    assert '"PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_LONG_SHADOW_V1"' in source
    assert '"PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_SHADOW_V1"' in source
    assert "FULL_SESSION_PROFILE_INITIAL_LONDON_LATE_MNQ_SHORT_ONLY_ELEVATION" in source
    assert 'PROOF_REQUIRED_SYMBOLS="MNQ,MES"' in source

    block = source.split('elif [[ "${STACK_PROFILE}" == "mnq_mes_full_session_active_evidence" ]]; then', 1)[1]
    roster_json = block.split("cat > \"${SCOPED_ROSTER_PATH}\" <<'JSON'", 1)[1].split("\nJSON", 1)[0]
    roster = json.loads(roster_json)
    assert roster["enabled_strategy_ids"] == [
        "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_SHORT_V1",
        "PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_LONG_V1",
        "PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_SHORT_V1",
        "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_LONG_V1",
        "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_SHORT_V1",
        "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_LONG_V1",
        "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_SHORT_V1",
        "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1",
        "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1",
        "PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1",
        "PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1",
        "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1",
    ]
    assert len(roster["enabled_strategy_ids"]) == 13
    assert roster["shadow_only_strategy_ids"] == [
        "PAPER_WATCH_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_LONG_SHADOW_V1",
        "PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_LONG_SHADOW_V1",
        "PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_SHADOW_V1",
    ]


def test_paper_stack_generated_profile_rosters_carry_authority_contract() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")
    blocks = source.split("cat > \"${SCOPED_ROSTER_PATH}\" <<'JSON'")[1:]

    assert blocks
    for block in blocks:
        roster_json = block.split("\nJSON", 1)[0]
        roster = json.loads(roster_json)
        assert roster["schema_version"] == "track_b_guarded_paper_roster_v1"
        assert roster["paper_account_id"] == "DUM882026"
        assert roster["live_money_eligible"] is False
        assert roster["paper_proof_invoked"] is False
        assert isinstance(roster["enabled_strategy_ids"], list)
        assert roster["disabled_strategy_ids"] == []
        assert roster["max_quantity_per_strategy"] == 1


def test_source_controlled_guarded_roster_carries_authority_contract() -> None:
    roster = json.loads(GUARDED_ROSTER_CONFIG.read_text(encoding="utf-8"))

    assert roster["schema_version"] == "track_b_guarded_paper_roster_v1"
    assert roster["authority_scope"] == "DEFAULT_FALLBACK_GUARDED_PAPER_ROSTER"
    assert "TRACK_B_GUARDED_PAPER_ROSTER_PATH" in roster["update_policy"]
    assert roster["paper_account_id"] == "DUM882026"
    assert roster["live_money_eligible"] is False
    assert roster["paper_proof_invoked"] is False
    assert isinstance(roster["enabled_strategy_ids"], list)
    assert roster["disabled_strategy_ids"] == []
    assert roster["max_quantity_per_strategy"] == 1


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


def test_recovery_tick_actions_are_safe_and_profile_preserving() -> None:
    source = RECOVERY_SCRIPT.read_text(encoding="utf-8")

    assert "NO_ACTION_RUNTIME_RUNNING" in source
    assert "NO_ACTION_BLOCKED_GATES" in source
    assert "NO_ACTION_DUPLICATE_WRITER" in source
    assert "START_REQUESTED_APPROVED_PAPER_STACK" in source
    assert "RECOVERY_BLOCKED_PROFILE_NOT_APPROVED" in source
    assert "duplicate_writer.duplicate_writer_detected" in source
    assert "restart_allowed_if_runtime_down" in source
    assert "track_b_paper_stack_restart_precheck" in source
    assert "restart_authority_allowed" in source
    assert "restart_authority_classification" in source
    assert "resolve_recovery_profile" in source
    assert "approved PAPER stack profile missing or unsafe" in source
    assert "TRACK_B_PAPER_STACK_PROFILE=\"${recovery_requested_profile}\" bash \"${START_SCRIPT}\"" in source
    assert "\n    bash \"${START_SCRIPT}\"" not in source
    assert "START_REQUESTED_CANONICAL_PAPER_STACK" not in source
    assert "placeorder" not in source.lower()
    assert "cancelorder" not in source.lower()
    assert "flatten" not in source.lower()


def test_recovery_profile_status_fields_are_reported() -> None:
    source = RECOVERY_SCRIPT.read_text(encoding="utf-8")

    assert "recovery_requested_profile" in source
    assert "recovery_profile_source" in source
    assert "recovery_profile_approved" in source
    assert "recovery_profile_blocker" in source
    assert "approved_paper_stack_profile.json" in source


def test_recovery_approved_profile_sources_exclude_implicit_canonical() -> None:
    source = RECOVERY_SCRIPT.read_text(encoding="utf-8")

    assert '"mnq_mes_full_session_active_evidence"' in source
    assert '"mnq_mes_globex_active_evidence"' in source
    assert '"mnq_mes_session_coverage_active_evidence"' in source
    assert "TRACK_B_PAPER_RECOVERY_ALLOW_CANONICAL" in source
    assert "RECOVERY_BLOCKED_CANONICAL_PROFILE_NOT_EXPLICITLY_APPROVED" in source
    assert '"canonical"' not in source[source.index("approved_profiles = {") : source.index("def load_json")]


def test_start_script_persists_approved_noncanonical_profile_for_recovery() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "APPROVED_PROFILE_ARTIFACT" in source
    assert "write_approved_profile_artifact" in source
    assert '"track_b_approved_paper_stack_profile_v1"' in source
    assert '"track_b_start_paper_stack_operator_profile"' in source
    assert '"recovery_requested_profile": stack_profile' in source
    assert 'if [[ "${STACK_PROFILE}" == "canonical" ]]' in source


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
    assert "CURRENT_HOT_PATH_CURRENT_SCOPE" in source
    assert '"diagnostic_only": True' in source
    assert "current_scope_diagnostic_only" in source
    assert "current_broker_truth" in source
    assert "current_lifecycle_truth" in source
    assert "track_b_managed_futures_position_count" in source
    assert "broker_open_order_count" in source
    assert "lifecycle_open_position_count" in source
    assert "current_scope_review_required_count" in source
    assert "current_scope_trade_states" in source
    assert "current_scope_trade_state_counts" in source
    assert "registry_trade_state_counts" in source
    assert "historical_review_required_count" in source
    assert "historical_review_required_trade_ids" in source
    assert "historical_registry_debris" in source
    assert "historical_quarantined_count" in source
    assert '"current_hot_path_blocking": False' in source
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
    assert "normal_submit_authority" in source
    assert "risk_reducing_close_authority" in source
    assert "risk_reducing_close_authority_visible_normal_entries_not_implied" in source
    assert "RISK_REDUCING_CLOSE_AUTHORITY_VISIBLE" in source
    assert "active_window_lane_count" in source
    assert "out_of_window_lane_count" in source
    assert "broker_mutation_allowed" in source


def test_canonical_paper_config_uses_phase1_artifact_market_data() -> None:
    source = PAPER_CONFIG.read_text(encoding="utf-8")

    assert 'probationary_paper_market_data_source: "phase1_runtime_artifact"' in source
