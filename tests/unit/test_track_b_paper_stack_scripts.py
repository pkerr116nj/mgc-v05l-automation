import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
START_SCRIPT = REPO_ROOT / "scripts" / "track_b_start_paper_stack.sh"
STATUS_SCRIPT = REPO_ROOT / "scripts" / "track_b_status_paper_stack.sh"
RECOVERY_SCRIPT = REPO_ROOT / "scripts" / "track_b_hourly_paper_runtime_recovery.sh"
THIN_RECOVERY_SCRIPT = REPO_ROOT / "scripts" / "track_b_thin_paper_runtime_recovery.sh"
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


def test_paper_stack_start_launches_runtime_under_detached_parent_monitor() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert 'runtime_pid="\\$!"' in source
    assert 'wait "\\${runtime_pid}"' in source
    assert "RUNTIME_EXITED_AFTER_INITIAL_TRUTH" in source
    assert "track_b_paper_stack_wrapper_child_exit" in source
    assert "RUNTIME_EXIT_STATUS_CAPTURE_FAILED" in source
    assert "probationary_paper_detached_child_status.json" in source
    assert "write_detached_child_status \"started\"" in source
    assert "write_detached_child_status \"exited\"" in source
    assert "write_detached_child_final_status_on_wrapper_exit" in source
    assert "track_b_paper_stack_wrapper_final_status" in source
    assert "--parent-pid \"\\$\\$\"" in source
    assert "--child-command \"\\${runtime_child_command}\"" in source
    assert "runtime_child_command=" in source
    assert "--background" not in source
    assert "run_probationary_paper_soak.sh" in source


def test_paper_minimal_start_uses_launchctl_for_durable_wrapper_parent() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    minimal_carrier_block = source[
        source.index("if paper_minimal_startup_enabled; then", source.index('carrier="screen"'))
        : source.index(
            'elif [[ "${PREFERRED_CARRIER}" == "screen" ]]; then',
            source.index("BLOCKED_NO_DIRECT_CARRIER"),
        )
    ]

    assert "BLOCKED_LAUNCHCTL_DISABLED_FOR_PAPER_MINIMAL_STARTUP" not in minimal_carrier_block
    assert "BLOCKED_LAUNCHCTL_UNAVAILABLE" in minimal_carrier_block
    assert "BLOCKED_SCREEN_DISABLED_FOR_PAPER_MINIMAL_STARTUP" in minimal_carrier_block
    assert "screen_available" not in minimal_carrier_block
    assert "launchctl_available" in minimal_carrier_block
    assert "nohup_available" in minimal_carrier_block
    assert "carrier=\"launchctl\"" in minimal_carrier_block
    assert "carrier=\"nohup\"" in minimal_carrier_block
    assert "launchctl submit" not in minimal_carrier_block

    assert "nohup /bin/bash" in source
    assert "launchctl submit" in source


def test_paper_stack_start_enforces_single_authoritative_runtime_scope_before_launch() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert source.index('STACK_PROFILE="${TRACK_B_PAPER_STACK_PROFILE:-canonical}"') < source.index(
        'START_LOCK_DIR="${RUNTIME_DIR}/paper_stack_${STACK_PROFILE}.runtime_scope.lock"'
    )
    assert "acquire_authoritative_runtime_scope_lock()" in source
    assert "BLOCKED_DUPLICATE_RUNTIME_CARRIER" in source
    assert "BLOCKED_OVERLAPPING_RUNTIME_START" in source
    assert "BLOCKED_RUNTIME_SCOPE_LOCK_HELD" in source
    assert 'lock_dir.mkdir(parents=True, exist_ok=False)' in source
    assert 'wrapper_path in command and "track_b_paper_stack_runtime_wrapper.sh" in command' in source
    assert '"mgc_v05l.app.main" in command' in source
    assert '"probationary-paper-soak" in command' in source
    assert "scoped_config_path in command" in source
    assert "rm -rf \"${START_LOCK_DIR}\"" in source

    launch_start = source.index('lock_result="${STACK_DIR}/.runtime_scope_lock.$$.json"')
    assert launch_start < source.index('screen -dmS "${session_name}"')
    assert launch_start < source.index('nohup /bin/bash "${WRAPPER_PATH}"')
    assert launch_start < source.index("launchctl submit")


def test_detached_wrapper_owns_runtime_scope_lock_until_exit() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")
    wrapper_start = source.index('cat > "${wrapper_tmp}" <<WRAPPER')
    wrapper_block = source[wrapper_start : source.index("\nWRAPPER", wrapper_start + 1)]

    assert 'export MGC_TRACK_B_PAPER_STACK_RUNTIME_SCOPE_LOCK_DIR="' in wrapper_block
    assert "release_runtime_scope_lock_on_wrapper_exit()" in wrapper_block
    assert 'rm -rf "\\${lock_dir}"' in wrapper_block
    assert "verify_single_runtime_carrier_on_wrapper_start()" in wrapper_block
    assert "track_b_paper_stack_wrapper_duplicate_carrier_blocked" in wrapper_block
    assert "BLOCKED_DUPLICATE_RUNTIME_CARRIER" in wrapper_block
    assert "write_detached_child_final_status_on_wrapper_exit" in wrapper_block
    assert "release_runtime_scope_lock_on_wrapper_exit" in wrapper_block
    assert source.index("write_detached_child_final_status_on_wrapper_exit") < source.index(
        "release_runtime_scope_lock_on_wrapper_exit()"
    )
    assert wrapper_block.index("verify_single_runtime_carrier_on_wrapper_start") < wrapper_block.index(
        'if "${PYTHON_BIN}" - <<\'PY\' "${RUNTIME_DIR}/paper_runtime_truth.json"'
    )


def test_paper_stack_start_requires_sustained_readiness() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "TRACK_B_PAPER_STACK_STABLE_SECONDS" in source
    assert "RUNTIME_RUNNING_WAITING_FOR_SUSTAINED_READINESS" in source
    assert "BLOCKED_RUNTIME_EXITED_DURING_STARTUP" in source
    assert "remained READY_SUBMIT_CAPABLE" in source
    assert "READY_TO_START_DIAGNOSTIC_ONLY" in source
    assert "submit remains disabled" in source


def test_paper_minimal_start_requires_durable_liveness_and_truth_advancement() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")
    minimal_wait_block = source[
        source.index("if paper_minimal_startup_enabled; then", source.index('if [[ "${carrier}" == "screen" ]]'))
        : source.index("\ndeadline=$((SECONDS + WAIT_SECONDS))", source.index("if paper_minimal_startup_enabled; then", source.index('if [[ "${carrier}" == "screen" ]]')))
    ]

    assert "verify_direct_paper_runtime_shape" in minimal_wait_block
    assert "READY_SUBMIT_CAPABLE" in minimal_wait_block
    assert "direct PAPER_MINIMAL_STARTUP_V1 process path" in minimal_wait_block
    assert "RUNTIME_RUNNING_WAITING_FOR_MINIMAL_STARTUP_STABILITY" in minimal_wait_block
    assert "RUNTIME_RUNNING_WAITING_FOR_MINIMAL_RUNTIME_SHAPE" in minimal_wait_block
    assert "RUNTIME_EXITED_BEFORE_DURABLE_READY" in minimal_wait_block
    assert "STABLE_SECONDS" in minimal_wait_block
    assert "candidate_pid" in minimal_wait_block
    assert "first_truth_generated_at" in minimal_wait_block
    assert "last_truth_generated_at" in minimal_wait_block
    assert "truth_advanced" in minimal_wait_block
    assert '[[ "${truth_advanced}" == "true" ]]' in minimal_wait_block
    assert "detached_child_ready_authority" in minimal_wait_block
    assert 'payload.get("child_final_status") != "RUNNING"' in source
    assert 'payload.get("process_alive") is not True' in source
    assert 'payload.get("classification") != "RUNTIME_CHILD_RUNNING_CYCLE_OBSERVED"' in source
    assert "RUNTIME_RUNNING_WAITING_FOR_DETACHED_CHILD_AUTHORITY" in minimal_wait_block
    assert "refreshed detached-child monitor authority did not prove" in minimal_wait_block
    assert "same-PID liveness" in minimal_wait_block
    assert 'elif [[ -n "${pid}" ]]; then' in minimal_wait_block
    assert "Runtime wrote PID" in minimal_wait_block


def test_paper_minimal_start_accepts_post_truth_progress_heartbeat_without_first_truth_ready() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")
    verifier_block = source[
        source.index("\nverify_direct_paper_runtime_shape() {")
        : source.index("\npost_truth_startup_progress_heartbeat() {")
    ]
    progress_block = source[
        source.index("\npost_truth_startup_progress_heartbeat() {")
        : source.index("\nwrite_runtime_config_paths_file() {")
    ]
    minimal_wait_block = source[
        source.index("if paper_minimal_startup_enabled; then", source.index('if [[ "${carrier}" == "screen" ]]'))
        : source.index("\ndeadline=$((SECONDS + WAIT_SECONDS))", source.index("if paper_minimal_startup_enabled; then", source.index('if [[ "${carrier}" == "screen" ]]')))
    ]

    assert "POST_TRUTH_PROGRESS_FILE" in source
    assert "paper_post_truth_startup_progress.json" in source
    assert "progress_fresh_for_post_truth_startup" in verifier_block
    assert "and not progress_fresh_for_post_truth_startup" in verifier_block
    assert 'progress_state not in {"STARTED", "IN_PROGRESS", "COMPLETED"}' in progress_block
    assert (
        'progress_stage not in {"authority_refresh", "watchdog_liveness_refresh", "lane_restore", "runtime_cycle"}'
        in progress_block
    )
    assert 'progress_stage in {"authority_refresh", "watchdog_liveness_refresh", "lane_restore", "runtime_cycle"}' in verifier_block
    assert "progress_pid != pid" in progress_block
    assert "RUNTIME_RUNNING_POST_TRUTH_AUTHORITY_REFRESH" in minimal_wait_block
    assert "deadline=$((SECONDS + WAIT_SECONDS))" in minimal_wait_block
    assert "waiting for runtime truth to advance before readiness" in minimal_wait_block
    assert "truth_advanced=\"false\"" in minimal_wait_block
    assert '[[ "${truth_advanced}" == "true" ]]' in minimal_wait_block
    assert "advanced runtime truth after post-truth startup progress" in minimal_wait_block


def test_paper_minimal_shape_verifier_checks_commit_profile_lane_count_and_truth_freshness() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")
    verifier_block = source[
        source.index("\nverify_direct_paper_runtime_shape() {")
        : source.index("\nwrite_runtime_config_paths_file() {")
    ]

    assert 'truth.get("source_commit") != expected_commit' in verifier_block
    assert 'config.get("profile") != expected_profile' in verifier_block
    assert 'expected_lane_count = 71' in verifier_block
    assert 'truth.get("lane_count")' in verifier_block
    assert 'execution_modes != {"IBKR_PAPER_BRIDGE"}' in verifier_block
    assert 'truth.get("freshness_state")' in verifier_block
    assert 'truth.get("heartbeat_state")' in verifier_block
    assert "datetime.now(timezone.utc)" in verifier_block
    assert "print(generated_at)" in verifier_block


def test_paper_stack_startup_artifact_carries_launch_exit_status_when_available() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")
    writer_block = source[
        source.index("\nwrite_startup_artifact() {")
        : source.index("\nrun_startup_preflight_evidence_refresh() {")
    ]

    assert "launch_status_file" in writer_block
    assert '"runtime_launch_status": launch_status or None' in writer_block
    assert '"runtime_detached_child_status": detached_child_status or None' in writer_block
    assert '"runtime_exit_status"' in writer_block
    assert "exit_source = detached_child_status" in writer_block
    assert "exit_source = detached_child_status or launch_status" not in writer_block
    assert 'exit_source.get("child_exit_code")' in writer_block
    assert 'exit_source.get("child_exit_signal")' in writer_block
    assert 'exit_source.get("termination_reason")' in writer_block
    assert source.index('rm -f "${LAUNCH_STATUS_FILE}"') < source.index('if [[ "${carrier}" == "screen" ]]; then')
    assert source.index('rm -f "${PID_FILE}" "${PID_METADATA_FILE}"') < source.index(
        'if [[ "${carrier}" == "screen" ]]; then'
    )
    assert '"child_exit_code": exit_code' in source
    assert '"termination_reason": "runtime_exited_after_initial_truth" if first_truth else "runtime_exited_before_runtime_truth"' in source


def test_paper_stack_start_refreshes_authority_evidence_before_carrier_launch() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "run_startup_preflight_evidence_refresh" in source
    assert "track_b_readiness_state" in source
    assert "track_b_control_plane_snapshot" in source
    assert "ibkr_broker_truth_refresher" in source
    assert "track_b_paper_broker_reconciliation" in source
    assert "track_b_open_order_truth" in source
    assert "track_b_managed_position_registry" in source
    assert "track_b_managed_order_registry" in source
    assert "track_b_shared_truth_refresh_cli" in source
    assert "--no-broker-lease-history" in source
    assert "startup_preflight_refresh_attempted" in source
    assert "startup_preflight_dependency_refresh_attempted" in source
    assert "dependency_refresh_steps" in source
    assert "dependency_refresh_failures" in source
    assert "startup_preflight_refresh_classification" in source
    assert "refreshed_artifact_paths" in source
    assert "remaining_start_blockers" in source

    launch_block = source[source.index('if ! run_startup_preflight_evidence_refresh; then') :]
    assert launch_block.index("run_startup_preflight_evidence_refresh") < launch_block.index(
        "write_approved_profile_artifact"
    )
    assert launch_block.index("run_startup_preflight_evidence_refresh") < launch_block.index(
        'cat > "${wrapper_tmp}"'
    )
    assert launch_block.index("run_startup_preflight_evidence_refresh") < launch_block.index(
        "launchctl submit"
    )
    assert launch_block.index("run_startup_preflight_evidence_refresh") < launch_block.index(
        "screen -dmS"
    )


def test_paper_stack_start_has_minimal_startup_v1_path_for_explicit_paper_starts() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "PAPER_MINIMAL_STARTUP_V1" in source
    assert "track_b_paper_minimal_startup" in source
    assert "BLOCKED_PAPER_MINIMAL_STARTUP_V1" in source
    assert "run_paper_minimal_startup_preflight" in source
    assert "TRACK_B_PAPER_MINIMAL_STARTUP_V1" in source
    assert "MGC_TRACK_B_PAPER_MINIMAL_STARTUP_V1" in source
    assert "MGC_TRACK_B_PAPER_MINIMAL_STARTUP_CLASSIFICATION" in source


def test_paper_stack_minimal_startup_path_does_not_invoke_full_rebuild_before_launch() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")
    minimal_launch_block = source[
        source.index("if paper_minimal_startup_enabled; then", source.index("\nwrite_runtime_config_paths_file\n"))
        : source.index("if ! paper_minimal_startup_enabled; then", source.index("\nwrite_runtime_config_paths_file\n"))
    ]

    assert "run_paper_minimal_startup_preflight" in minimal_launch_block
    assert "run_startup_preflight_evidence_refresh" not in minimal_launch_block
    assert "track_b_control_plane_snapshot" not in minimal_launch_block
    assert "track_b_shared_truth_refresh_cli" not in minimal_launch_block
    assert "track_b_managed_position_registry" not in minimal_launch_block


def test_paper_stack_writes_selected_config_paths_before_minimal_startup() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")
    start_block = source[
        source.index("write_runtime_config_paths_file()") : source.index(
            'if [[ "${already_running}" == "true" ]]; then'
        )
    ]

    assert 'CANONICAL_CONFIGS+=("${SCOPED_CONFIG_PATH}")' in source
    assert 'printf \'%s\\n\' "${config_path}" >> "${CONFIG_PATHS_FILE}"' in start_block
    assert start_block.index("\nwrite_runtime_config_paths_file\n") < start_block.index(
        "run_paper_minimal_startup_preflight"
    )


def test_full_session_profile_materialization_prefers_existing_scoped_overlay() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")
    full_session_block = source[
        source.index('elif [[ "${STACK_PROFILE}" == "mnq_mes_full_session_active_evidence" ]]')
        : source.index('elif [[ "${STACK_PROFILE}" != "canonical" ]]')
    ]

    assert "scoped_profile_lane_source_config" in source
    assert "_load_source_lanes" in source
    assert "$(scoped_profile_lane_source_config" in full_session_block
    assert '"${SCOPED_CONFIG_PATH}" "${RUNTIME_DIR}/paper_config_in_force.json"' in full_session_block
    assert 'export MGC_TRACK_B_PAPER_STACK_PROFILE="${STACK_PROFILE}"' in source


def test_paper_stack_minimal_startup_owns_restart_authority() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    already_running_block = source[
        source.index('if [[ "${already_running}" == "true" ]]; then') : source.index(
            'write_startup_artifact "${restart_precheck_classification}"'
        )
    ]
    assert "RESTART_ALLOWED_PAPER_MINIMAL_STARTUP_V1" in already_running_block
    assert "legacy restart precheck is diagnostic only" in already_running_block
    assert "track_b_paper_stack_restart_precheck" in already_running_block

    minimal_start = already_running_block.index(
        'restart_precheck_classification="RESTART_ALLOWED_PAPER_MINIMAL_STARTUP_V1"'
    )
    legacy_start = already_running_block.index('else\n    restart_precheck=')
    minimal_branch = already_running_block[
        minimal_start:legacy_start
    ]
    legacy_branch = already_running_block[legacy_start:]
    assert "track_b_paper_stack_restart_precheck" not in minimal_branch
    assert "track_b_paper_stack_restart_precheck" in legacy_branch


def test_paper_minimal_restart_stops_only_exact_pid_without_legacy_wrapper() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "stop_exact_runtime_pid_for_minimal_restart" in source
    stop_function = source[
        source.index("stop_exact_runtime_pid_for_minimal_restart() {")
        : source.index("\nverify_direct_paper_runtime_shape() {")
    ]
    assert 'kill -TERM "${pid}"' in stop_function
    assert 'kill -KILL "${pid}"' in stop_function
    assert "BLOCKED_EXACT_PID_STOP_FAILED" in stop_function
    assert "pkill" not in stop_function
    assert "killall" not in stop_function
    assert "stop_probationary_paper_soak.sh" not in stop_function

    restart_block = source[
        source.index('write_startup_artifact "${restart_precheck_classification}"')
        : source.index("\nfi\n\nif paper_minimal_startup_enabled; then", source.index('write_startup_artifact "${restart_precheck_classification}"'))
    ]
    minimal_branch = restart_block[
        restart_block.index("if paper_minimal_startup_enabled; then")
        : restart_block.index("else")
    ]
    legacy_branch = restart_block[restart_block.index("else") :]
    assert 'stop_exact_runtime_pid_for_minimal_restart "${pid}"' in minimal_branch
    assert "stop_probationary_paper_soak.sh" not in minimal_branch
    assert "stop_probationary_paper_soak.sh" in legacy_branch


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
    assert blocked < launch_block.index('cat > "${wrapper_tmp}"')
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
    broker_truth: dict | None = None,
    open_order_truth: dict | None = None,
    managed_positions: dict | None = None,
    managed_positions_artifact: dict | None = None,
    managed_orders: dict | None = None,
    managed_orders_artifact: dict | None = None,
    shared_truth: dict | None = None,
    broker_truth_rc: int = 0,
    reconciliation_rc: int = 0,
    open_order_rc: int = 0,
    managed_position_rc: int = 0,
    managed_order_rc: int = 0,
    shared_truth_rc: int = 0,
    readiness_rc: int = 0,
    control_rc: int = 0,
    status_rc: int = 0,
    safe_state: dict | None = None,
    guardian: dict | None = None,
    stack_profile: str = "mnq_mes_full_session_active_evidence",
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
    safe_state_path = repo_root / "outputs" / "track_b_execution_core" / "safe_state" / "latest_runtime_safe_state_envelope.json"
    guardian_path = (
        repo_root
        / "outputs"
        / "track_b_execution_core"
        / "broker_position_guardian"
        / "latest_broker_position_guardian.json"
    )
    managed_positions_artifact_path = (
        repo_root
        / "outputs"
        / "track_b_execution_core"
        / "managed_positions"
        / "latest_managed_positions.json"
    )
    managed_orders_artifact_path = (
        repo_root
        / "outputs"
        / "track_b_execution_core"
        / "managed_orders"
        / "latest_managed_orders.json"
    )
    safe_state_path.parent.mkdir(parents=True, exist_ok=True)
    guardian_path.parent.mkdir(parents=True, exist_ok=True)
    managed_positions_artifact_path.parent.mkdir(parents=True, exist_ok=True)
    managed_orders_artifact_path.parent.mkdir(parents=True, exist_ok=True)
    safe_state_path.write_text(
        json.dumps(
            safe_state
            or {
                "classification": "SAFE_STATE_NORMAL",
                "close_authority": {
                    "allowed": True,
                    "broad_flatten_allowed": False,
                    "global_flatten_allowed": False,
                },
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            }
        ),
        encoding="utf-8",
    )
    guardian_path.write_text(
        json.dumps(
            guardian
            or {
                "classification": "BROKER_POSITION_GUARDIAN_READY",
                "managed_close_authority": {
                    "allowed": True,
                    "broad_flatten_allowed": False,
                    "global_flatten_allowed": False,
                    "candidates": [],
                },
            }
        ),
        encoding="utf-8",
    )
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
    broker_truth_path = tmp_path / "broker_truth.json"
    reconciliation_stdout_path = tmp_path / "reconciliation_stdout.json"
    open_order_truth_path = tmp_path / "open_order_truth.json"
    managed_positions_path = tmp_path / "managed_positions.json"
    managed_orders_path = tmp_path / "managed_orders.json"
    shared_truth_path = tmp_path / "shared_truth.json"
    status_path = tmp_path / "status.json"
    readiness_path = tmp_path / "readiness.json"
    control_path = tmp_path / "control.json"
    default_positions_snapshot_path = (
        repo_root / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json"
    )
    default_open_orders_snapshot_path = (
        repo_root / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json"
    )
    default_positions_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    default_positions_snapshot_path.write_text(
        json.dumps(
            {
                "account": "DUM882026",
                "selected_account_id": "DUM882026",
                "ok": True,
                "positions_complete": True,
                "positions": [
                    {
                        "account_id": "DUM882026",
                        "security_type": "FUT",
                        "symbol": "MNQ",
                        "local_symbol": "MNQU6",
                        "quantity": "0",
                    },
                    {
                        "account_id": "DUM882026",
                        "security_type": "FUT",
                        "symbol": "MES",
                        "local_symbol": "MESU6",
                        "quantity": "0",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    default_open_orders_snapshot_path.write_text(
        json.dumps(
            {
                "account": "DUM882026",
                "selected_account_id": "DUM882026",
                "ok": True,
                "open_orders_complete": True,
                "open_order_count": 0,
                "open_orders": [],
            }
        ),
        encoding="utf-8",
    )
    clean_reconciliation = reconciliation or {
        "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
        "broker_reconciled": True,
        "track_b_broker_position_count": 0,
        "track_b_broker_open_order_count": 0,
        "current_scope_lifecycle_open_position_count": 0,
        "lifecycle_open_order_count": 0,
    }
    reconciliation_stdout_path.write_text(json.dumps(clean_reconciliation), encoding="utf-8")
    broker_truth_path.write_text(
        json.dumps(
            broker_truth
            or {
                "classification": "BROKER_TRUTH_REFRESH_READY",
                "account": "DUM882026",
                "fresh": True,
                "positions_complete": True,
                "open_orders_complete": True,
                "open_order_count": 0,
                "unknown_open_order_count": 0,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
                "positions_snapshot_path": str(default_positions_snapshot_path),
                "open_orders_snapshot_path": str(default_open_orders_snapshot_path),
                "output_path": str(tmp_path / "latest_broker_truth.json"),
            }
        ),
        encoding="utf-8",
    )
    open_order_truth_path.write_text(
        json.dumps(
            open_order_truth
            or {
                "classification": "NO_OPEN_ORDERS",
                "output_path": str(tmp_path / "latest_open_order_truth.json"),
            }
        ),
        encoding="utf-8",
    )
    managed_positions_path.write_text(
        json.dumps(
            managed_positions
            or {
                "classification": "NO_MANAGED_POSITIONS",
                "output_path": str(tmp_path / "latest_managed_positions.json"),
            }
        ),
        encoding="utf-8",
    )
    managed_positions_artifact_path.write_text(
        json.dumps(
            managed_positions_artifact
            if managed_positions_artifact is not None
            else managed_positions
            or {
                "classification": "NO_MANAGED_POSITIONS",
                "output_path": str(tmp_path / "latest_managed_positions.json"),
            }
        ),
        encoding="utf-8",
    )
    managed_orders_path.write_text(
        json.dumps(
            managed_orders
            or {
                "classification": "NO_MANAGED_ORDERS",
                "output_path": str(tmp_path / "latest_managed_orders.json"),
            }
        ),
        encoding="utf-8",
    )
    managed_orders_artifact_path.write_text(
        json.dumps(
            managed_orders_artifact
            if managed_orders_artifact is not None
            else managed_orders
            or {
                "classification": "NO_MANAGED_ORDERS",
                "output_path": str(tmp_path / "latest_managed_orders.json"),
            }
        ),
        encoding="utf-8",
    )
    shared_truth_path.write_text(
        json.dumps(
            shared_truth
            or {
                "runtime_start_preflight": {
                    "classification": "SHARED_TRUTH_RUNTIME_START_CLEAN",
                    "clean_for_runtime_start": True,
                    "blockers": [],
                },
                "output_path": str(tmp_path / "latest_shared_truth.json"),
            }
        ),
        encoding="utf-8",
    )
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
            str(broker_truth_path),
            str(reconciliation_stdout_path),
            str(open_order_truth_path),
            str(managed_positions_path),
            str(managed_orders_path),
            str(shared_truth_path),
            str(status_path),
            str(readiness_path),
            str(control_path),
            str(broker_truth_rc),
            str(reconciliation_rc),
            str(open_order_rc),
            str(managed_position_rc),
            str(managed_order_rc),
            str(shared_truth_rc),
            str(readiness_rc),
            str(control_rc),
            str(status_rc),
            str(readiness_path),
            str(control_path),
            stack_profile,
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
    assert result["startup_preflight_dependency_refresh_attempted"] is True
    assert [row["step"] for row in result["dependency_refresh_steps"]] == [
        "broker_truth_broker_truth_lease_bsa",
        "broker_lifecycle_reconciliation",
        "open_order_truth",
        "managed_position_registry",
        "managed_order_registry",
        "shared_truth",
        "canonical_readiness",
        "control_plane_snapshot",
        "paper_stack_status",
    ]


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


def test_paper_stack_start_treats_control_plane_blocked_exit_as_published_authority(tmp_path: Path) -> None:
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
            "safe_to_start_runtime": False,
            "top_line_classification": "CONTROL_PLANE_BLOCKED",
            "blockers": [{"code": "agent_health_blocks_runtime_submit", "detail": "runtime down"}],
            "primary_blocking_agent_id": "agent_health",
            "primary_blocking_reason": "runtime down",
        },
        control_rc=2,
    )

    codes = {row["code"] for row in result["remaining_start_blockers"]}
    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_BLOCKED"
    assert result["dependency_refresh_failures"] == []
    assert "control_plane_snapshot_refresh_failed" not in codes
    assert "control_plane_refresh_failed" not in codes
    assert "control_plane_start_not_allowed" in codes
    assert "control_plane_reported_blockers" in codes
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


def test_paper_stack_start_demotes_stale_lifecycle_when_fresh_broker_truth_clean(tmp_path: Path) -> None:
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
            "top_line_classification": "READY_FOR_OPERATOR_START",
            "blockers": [],
        },
        reconciliation={
            "classification": "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            "broker_reconciled": False,
            "track_b_broker_position_count": 1,
            "track_b_broker_open_order_count": 0,
            "current_scope_lifecycle_open_position_count": 1,
            "lifecycle_open_order_count": 0,
        },
        open_order_truth={
            "classification": "ORDER_TRUTH_STALE",
            "unknown_open_order_count": 0,
            "summary": {"open_order_count": 0, "unknown_open_order_count": 0},
        },
        managed_positions={"classification": "TRACK_B_MANAGED_POSITIONS_REVIEW_REQUIRED"},
        managed_orders={"classification": "TRACK_B_MANAGED_ORDERS_REVIEW_REQUIRED"},
        shared_truth={
            "runtime_start_preflight": {
                "classification": "SHARED_TRUTH_RUNTIME_START_STALE",
                "clean_for_runtime_start": False,
                "blockers": [{"code": "broker_truth_lease_invalidated_contradiction"}],
            },
            "output_path": str(tmp_path / "latest_shared_truth.json"),
        },
    )

    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_CLEAN"
    assert result["remaining_start_blockers"] == []
    authority = result["broker_startup_authority"]
    assert authority["broker_truth_clean"] is True
    assert authority["classification"] == "FRESH_COMPLETE_CLEAN_BROKER_TRUTH"


def test_paper_stack_start_preflight_refresh_still_blocks_non_flat_broker_state(tmp_path: Path) -> None:
    positions_path = tmp_path / "actual_positions.json"
    orders_path = tmp_path / "actual_orders.json"
    positions_path.write_text(
        json.dumps(
            {
                "account": "DUM882026",
                "selected_account_id": "DUM882026",
                "positions": [
                    {
                        "account_id": "DUM882026",
                        "security_type": "FUT",
                        "symbol": "MNQ",
                        "local_symbol": "MNQU6",
                        "quantity": "1",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    orders_path.write_text(
        json.dumps(
            {
                "account": "DUM882026",
                "selected_account_id": "DUM882026",
                "open_order_count": 0,
                "open_orders": [],
            }
        ),
        encoding="utf-8",
    )
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
            "top_line_classification": "READY_FOR_OPERATOR_START",
            "blockers": [],
        },
        broker_truth={
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "account": "DUM882026",
            "fresh": True,
            "positions_complete": True,
            "open_orders_complete": True,
            "open_order_count": 0,
            "unknown_open_order_count": 0,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "positions_snapshot_path": str(positions_path),
            "open_orders_snapshot_path": str(orders_path),
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
    assert "broker_startup_authority_track_b_futures_positions_unmanaged_or_ambiguous" in codes


def test_paper_stack_start_preflight_blocks_actual_open_order_from_fresh_broker_truth(tmp_path: Path) -> None:
    positions_path = tmp_path / "flat_positions.json"
    orders_path = tmp_path / "open_orders.json"
    positions_path.write_text(
        json.dumps(
            {
                "account": "DUM882026",
                "selected_account_id": "DUM882026",
                "positions": [
                    {
                        "account_id": "DUM882026",
                        "security_type": "FUT",
                        "symbol": "MNQ",
                        "local_symbol": "MNQU6",
                        "quantity": "0",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    orders_path.write_text(
        json.dumps(
            {
                "account": "DUM882026",
                "selected_account_id": "DUM882026",
                "open_order_count": 1,
                "open_orders": [{"account_id": "DUM882026", "symbol": "MNQ", "local_symbol": "MNQU6"}],
            }
        ),
        encoding="utf-8",
    )

    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
            "top_line_classification": "READY_FOR_OPERATOR_START",
            "blockers": [],
        },
        broker_truth={
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "account": "DUM882026",
            "fresh": True,
            "positions_complete": True,
            "open_orders_complete": True,
            "open_order_count": 0,
            "unknown_open_order_count": 0,
            "positions_snapshot_path": str(positions_path),
            "open_orders_snapshot_path": str(orders_path),
        },
    )

    codes = {row["code"] for row in result["remaining_start_blockers"]}
    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_BLOCKED"
    assert "broker_startup_authority_broker_open_orders_present" in codes


def test_paper_stack_start_allows_owned_managed_exposure_maintenance_restore(tmp_path: Path) -> None:
    lifecycle_id = "reserved_submit_mnq_us_active_participation_short_20260605T153313319800Z_c9ec4e3f156c"
    trade_id = "trade_e372351a-26f7-464f-ad9f-f5b4b2c04893"
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
            "safe_to_start_runtime": False,
            "top_line_classification": "BLOCKED",
            "blockers": [
                {"code": "agent_health_blocks_runtime_submit", "detail": "runtime down"},
                {
                    "agent_id": "canonical_readiness_refresher",
                    "reason": "artifact_stale",
                    "status": "STALE",
                },
                {
                    "agent_id": "track_b_paper_runtime",
                    "reason": "RUNTIME_DOWN_WITH_BROKER_EXPOSURE",
                    "status": "STOPPED_UNEXPECTED",
                },
            ],
            "primary_blocking_agent_id": "canonical_readiness_refresher",
            "primary_blocking_reason": "artifact_stale",
        },
        control_rc=2,
        reconciliation={
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_position_count": 1,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "current_scope_lifecycle_open_position_count": 1,
            "lifecycle_open_order_count": 0,
            "current_exposure_owner_resolution": {
                "classification": "OWNED_MANAGED_EXPOSURE",
                "owned_exposure_count": 1,
                "owned_exposures": [{"lifecycle_id": lifecycle_id, "trade_id": trade_id}],
                "ambiguous_exposures": [],
            },
        },
        status={
            "runtime": {"running": False},
            "safety": {
                "paper_only": True,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
                "broker_mutation_allowed": False,
            },
            "registry_truth_diagnostics": {
                "current_scope_trade_states": [
                    {
                        "current_derived_state": "OPEN_MANAGED",
                        "registry_agrees_with_reconciliation": True,
                        "lifecycle_id": lifecycle_id,
                        "trade_id": trade_id,
                    }
                ]
            },
        },
        managed_positions={
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "exit_due": True,
                    "lifecycle_id": lifecycle_id,
                    "trade_id": trade_id,
                    "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
                }
            ],
        },
        managed_orders={
            "classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "managed_orders": [
                {
                    "classification": "POSITION_WITHOUT_CLOSE_ORDER",
                    "lifecycle_id": lifecycle_id,
                    "trade_id": trade_id,
                    "working": False,
                }
            ],
        },
    )

    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_CLEAN"
    assert result["startup_mode"] == "OWNED_MANAGED_EXPOSURE_MAINTENANCE_RESTORE"
    assert result["remaining_start_blockers"] == []
    assert result["dependency_refresh_failures"] == []
    restore = result["owned_managed_exposure_maintenance_restore"]
    assert restore["allowed"] is True
    assert restore["lifecycle_id"] == lifecycle_id
    assert restore["trade_id"] == trade_id


def test_paper_stack_start_allows_owned_restore_from_durable_current_scope_when_refresh_is_stale(
    tmp_path: Path,
) -> None:
    lifecycle_id = "reserved_submit_mes_globex_active_participation_long_20260605T025014090792Z_f15a0bc2cab9"
    trade_id = "trade_c572b141-df6d-4c2b-8fae-f582cb8f4d2e"
    current_managed_positions = {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "managed_positions": [
            {
                "classification": "OPEN_MANAGED_EXIT_DUE",
                "exit_due": True,
                "lifecycle_id": lifecycle_id,
                "trade_id": trade_id,
                "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
            }
        ],
    }
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
            "safe_to_start_runtime": False,
            "top_line_classification": "CONTROL_PLANE_BLOCKED",
            "paper_action_policy": "SCOPED_RECOVERY_ELIGIBLE",
            "safe_state_classification": "SAFE_STATE_NORMAL",
            "blockers": [{"code": "agent_health_blocks_runtime_submit", "detail": "runtime down"}],
            "prioritized_blockers": [
                {
                    "agent_id": "canonical_readiness_refresher",
                    "reason": "artifact_stale",
                    "status": "STALE",
                },
                {
                    "agent_id": "track_b_paper_runtime",
                    "reason": "RUNTIME_DOWN_WITH_BROKER_EXPOSURE",
                    "status": "STOPPED_UNEXPECTED",
                },
            ],
            "primary_blocking_agent_id": "canonical_readiness_refresher",
            "primary_blocking_reason": "artifact_stale",
        },
        control_rc=2,
        reconciliation={
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_position_count": 1,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "current_scope_lifecycle_open_position_count": 1,
            "lifecycle_open_order_count": 0,
            "current_exposure_owner_resolution": {
                "classification": "OWNED_MANAGED_EXPOSURE",
                "owned_exposure_count": 1,
                "owned_exposures": [
                    {
                        "classification": "OWNED_MANAGED_EXPOSURE",
                        "lifecycle_id": lifecycle_id,
                        "trade_id": trade_id,
                        "lifecycle_position": {
                            "classification": "OPEN_MANAGED",
                            "lifecycle_id": lifecycle_id,
                            "trade_id": trade_id,
                            "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
                        },
                    }
                ],
                "ambiguous_exposures": [],
            },
        },
        status={
            "runtime": {"running": False},
            "safety": {
                "paper_only": True,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
                "broker_mutation_allowed": False,
            },
            "registry_truth_diagnostics": {
                "current_scope_trade_states": [
                    {
                        "current_derived_state": "OPEN_MANAGED",
                        "registry_agrees_with_reconciliation": True,
                        "lifecycle_id": lifecycle_id,
                        "trade_id": trade_id,
                    }
                ]
            },
        },
        managed_positions={"classification": "STALE_MANAGED_POSITION_EVIDENCE", "managed_positions": []},
        managed_positions_artifact=current_managed_positions,
        managed_orders={"classification": "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED", "managed_orders": []},
        managed_orders_artifact={
            "classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "managed_orders": [
                {
                    "classification": "POSITION_WITHOUT_CLOSE_ORDER",
                    "canonical_managed_position": current_managed_positions["managed_positions"][0],
                }
            ],
        },
    )

    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_CLEAN"
    assert result["startup_mode"] == "OWNED_MANAGED_EXPOSURE_MAINTENANCE_RESTORE"
    assert result["remaining_start_blockers"] == []
    restore = result["owned_managed_exposure_maintenance_restore"]
    assert restore["allowed"] is True
    assert restore["managed_exposure_count"] == 1
    assert restore["lifecycle_ids"] == [lifecycle_id]


def test_paper_stack_start_allows_multiple_exact_owned_managed_exposures(tmp_path: Path) -> None:
    exposures = [
        (
            "reserved_submit_mnq_globex_active_participation_long_20260605T024927888743Z_98114344bc68",
            "trade_eb811018-c06f-468c-bb66-0daa6cd3d886",
        ),
        (
            "reserved_submit_mes_globex_active_participation_long_20260605T025014090792Z_f15a0bc2cab9",
            "trade_c572b141-df6d-4c2b-8fae-f582cb8f4d2e",
        ),
    ]
    managed_rows = [
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "exit_due": True,
            "lifecycle_id": lifecycle_id,
            "trade_id": trade_id,
            "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        }
        for lifecycle_id, trade_id in exposures
    ]
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
            "safe_to_start_runtime": False,
            "top_line_classification": "CONTROL_PLANE_BLOCKED",
            "safe_state_classification": "SAFE_STATE_NORMAL",
            "blockers": [{"code": "agent_health_blocks_runtime_submit", "detail": "runtime down"}],
            "prioritized_blockers": [
                {"agent_id": "track_b_paper_runtime", "reason": "RUNTIME_DOWN_WITH_BROKER_EXPOSURE"}
            ],
        },
        control_rc=2,
        reconciliation={
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_position_count": 2,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "current_scope_lifecycle_open_position_count": 2,
            "lifecycle_open_order_count": 0,
            "current_exposure_owner_resolution": {
                "classification": "OWNED_MANAGED_EXPOSURE",
                "owned_exposure_count": 2,
                "owned_exposures": [
                    {"lifecycle_id": lifecycle_id, "trade_id": trade_id, "lifecycle_position": row}
                    for row, (lifecycle_id, trade_id) in zip(managed_rows, exposures, strict=True)
                ],
                "ambiguous_exposures": [],
            },
        },
        status={
            "runtime": {"running": False},
            "safety": {
                "paper_only": True,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
                "broker_mutation_allowed": False,
            },
            "registry_truth_diagnostics": {
                "current_scope_trade_states": [
                    {
                        "current_derived_state": "OPEN_MANAGED",
                        "registry_agrees_with_reconciliation": True,
                        "lifecycle_id": lifecycle_id,
                        "trade_id": trade_id,
                    }
                    for lifecycle_id, trade_id in exposures
                ]
            },
        },
        managed_positions={"classification": "OPEN_MANAGED_EXIT_DUE", "managed_positions": managed_rows},
        managed_orders={"classification": "POSITION_WITHOUT_CLOSE_ORDER", "managed_orders": []},
    )

    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_CLEAN"
    assert result["startup_mode"] == "OWNED_MANAGED_EXPOSURE_MAINTENANCE_RESTORE"
    assert result["owned_managed_exposure_maintenance_restore"]["managed_exposure_count"] == 2


def test_paper_stack_start_blocks_owned_restore_wrong_profile(tmp_path: Path) -> None:
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
            "safe_to_start_runtime": False,
            "top_line_classification": "BLOCKED",
            "blockers": [],
        },
        reconciliation={
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_position_count": 1,
            "track_b_broker_open_order_count": 0,
            "current_scope_lifecycle_open_position_count": 1,
            "lifecycle_open_order_count": 0,
            "current_exposure_owner_resolution": {
                "classification": "OWNED_MANAGED_EXPOSURE",
                "owned_exposure_count": 1,
            },
        },
        managed_positions={
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [{"classification": "OPEN_MANAGED_EXIT_DUE", "exit_due": True}],
        },
        managed_orders={"classification": "POSITION_WITHOUT_CLOSE_ORDER"},
        status={
            "runtime": {"running": False},
            "safety": {
                "paper_only": True,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
                "broker_mutation_allowed": False,
            },
        },
        stack_profile="canonical",
    )

    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_BLOCKED"
    assert result["startup_mode"] == "STANDARD_START"
    assert "profile_not_approved_for_maintenance_restore" in result["owned_managed_exposure_maintenance_restore"]["blockers"]


def test_paper_stack_start_preflight_refresh_demotes_stale_reconciliation_refresh_failure(tmp_path: Path) -> None:
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
            "top_line_classification": "READY_FOR_OPERATOR_START",
            "blockers": [],
        },
        reconciliation_rc=1,
    )

    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_CLEAN"
    assert result["remaining_start_blockers"] == []
    assert result["dependency_refresh_failures"] == []


def test_paper_stack_start_preflight_refresh_demotes_stale_open_order_truth(tmp_path: Path) -> None:
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
            "top_line_classification": "READY_FOR_OPERATOR_START",
            "blockers": [],
        },
        open_order_truth={"classification": "ORDER_TRUTH_STALE"},
    )

    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_CLEAN"
    assert result["remaining_start_blockers"] == []


def test_paper_stack_start_preflight_refresh_demotes_stale_managed_orders(tmp_path: Path) -> None:
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
            "top_line_classification": "READY_FOR_OPERATOR_START",
            "blockers": [],
        },
        managed_orders={"classification": "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED"},
    )

    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_CLEAN"
    assert result["remaining_start_blockers"] == []


def test_paper_stack_start_preflight_refresh_demotes_dirty_reconciliation_when_broker_truth_clean(tmp_path: Path) -> None:
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
            "top_line_classification": "READY_FOR_OPERATOR_START",
            "blockers": [],
        },
        reconciliation={
            "classification": "TRACK_B_PAPER_BROKER_LIFECYCLE_MISMATCH",
            "broker_reconciled": False,
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "current_scope_lifecycle_open_position_count": 0,
            "lifecycle_open_order_count": 0,
        },
    )

    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_CLEAN"
    assert result["remaining_start_blockers"] == []


def test_paper_stack_start_preflight_refresh_blocks_live_money_and_paper_proof(tmp_path: Path) -> None:
    result = _run_startup_preflight_decision(
        tmp_path,
        control={
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
            "top_line_classification": "READY_FOR_OPERATOR_START",
            "blockers": [],
        },
        status={
            "safety": {
                "paper_only": True,
                "live_money_eligible": True,
                "paper_proof_invoked": True,
                "broker_mutation_allowed": False,
            }
        },
    )

    codes = {row["code"] for row in result["remaining_start_blockers"]}
    assert result["classification"] == "STARTUP_PREFLIGHT_REFRESH_BLOCKED"
    assert "live_money_eligible_not_false" in codes
    assert "paper_proof_invoked_not_false" in codes


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
    assert "broad_flatten_allowed" in lowered


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


def test_paper_stack_status_derives_startup_profile_from_config_stack() -> None:
    source = STATUS_SCRIPT.read_text(encoding="utf-8")

    assert "infer_profile_from_config_stack" in source
    assert 'name.startswith("paper_stack_")' in source
    assert 'startup_config_in_force["profile_id"] = profile_from_stack' in source
    assert 'startup_config_in_force["config_fingerprint"]' in source
    assert 'startup_config_in_force["lane_count"]' in source
    assert '"config_in_force": startup_config_in_force' in source


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
    assert "OWNED_MANAGED_EXPOSURE_MAINTENANCE_RESTORE" in source
    assert "OWNED_MANAGED_EXPOSURE_MAINTENANCE_RESTORE" in soak_source
    assert "OWNED_MANAGED_EXPOSURE_MAINTENANCE_RESTORE_ACTIVE" in source
    assert "RUNTIME_RUNNING_WAITING_FOR_MAINTENANCE_RESTORE_STABILITY" in source
    assert 'readiness.get("submit_allowed") is not True' in source
    assert 'readiness.get("ready_submit_capable") is not True' in source
    assert "RESTART_ALLOWED_OWNED_MANAGED_EXPOSURE" in soak_source
    assert "MGC_TRACK_B_PAPER_STACK_OWNED_MANAGED_EXPOSURE_RESTORE_JSON" in source
    assert "MGC_TRACK_B_PAPER_STACK_OWNED_MANAGED_EXPOSURE_RESTORE_JSON" in soak_source
    assert "OWNED_MANAGED_EXPOSURE_MAINTENANCE_RESTORE handoff accepted by restart precheck" in soak_source
    control_gate_python = soak_source.split(
        '"${PYTHON_BIN}" - <<\'PY\' "${CONTROL_PLANE_SNAPSHOT_FILE}" "${PAPER_STACK_STATUS_FILE}"',
        1,
    )[1].split("\nPY\n", 1)[0]
    assert "import os" in control_gate_python
    control_handoff_python = soak_source.split(
        'if "${PYTHON_BIN}" - <<\'PY\' "${PAPER_STACK_STATUS_FILE}" >/dev/null;',
        1,
    )[1].split("\nPY\n", 1)[0]
    assert "RESTART_ALLOWED_OWNED_MANAGED_EXPOSURE" in control_handoff_python
    assert "classify_paper_stack_restart_precheck" in control_handoff_python
    assert "track_b_control_plane_snapshot" in soak_source
    assert '("canonical_readiness_refresher", "artifact_stale")' in soak_source
    assert 'reason in {"Position Truth", "Broker Truth Lease"}' in source
    assert 'detail in {"Position Truth", "Broker Truth Lease"}' in soak_source
    assert "RUNTIME_DOWN_WITH_BROKER_EXPOSURE" in soak_source
    assert "RESTART_ALLOWED_FLAT_RECONCILED" not in source
    assert "restart_precheck_classification" in source
    assert "BLOCKED_UNMANAGED_EXPOSURE" not in source
    assert "Broker/lifecycle/safety state is not clean enough for a controlled restart." in source


def test_paper_stack_start_enables_recovery_service_unless_operator_opts_out() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert "ensure_recovery_service_enabled" in source
    ensure_block = source[
        source.index("ensure_recovery_service_enabled() {")
        : source.index("\nensure_recovery_service_enabled", source.index("ensure_recovery_service_enabled() {"))
    ]
    assert "paper_minimal_startup_enabled" in ensure_block
    assert ensure_block.index("paper_minimal_startup_enabled") < ensure_block.index(
        "track_b_hourly_paper_runtime_recovery.sh"
    )
    assert "track_b_hourly_paper_runtime_recovery.sh\" enable" in source
    assert "TRACK_B_PAPER_STACK_DISABLE_RECOVERY_SERVICE" in source
    assert "MGC_TRACK_B_DISABLE_STANDALONE_RECOVERY" in source
    assert "WARNING_RECOVERY_SERVICE_ENABLE_FAILED" in source


def test_recovery_tick_uses_thin_runtime_recovery_without_legacy_status_gates() -> None:
    source = RECOVERY_SCRIPT.read_text(encoding="utf-8")

    assert 'THIN_RECOVERY_SCRIPT="${REPO_ROOT}/scripts/track_b_thin_paper_runtime_recovery.sh"' in source
    tick_block = source[source.index("  tick)\n") : source.index("  enable)\n")]
    assert 'bash "${THIN_RECOVERY_SCRIPT}" check' in tick_block
    assert 'bash "${THIN_RECOVERY_SCRIPT}" start' in tick_block
    assert 'bash "${THIN_RECOVERY_SCRIPT}" restart' in tick_block
    assert "THIN_RECOVERY_RESTART_REQUIRED" in tick_block
    assert "BROKER_TRUTH_NOT_CLEAN_RECOVERY_BLOCKED" in tick_block
    assert "NO_ACTION_BROKER_TRUTH_NOT_CLEAN" in tick_block
    assert "START_REQUESTED_THIN_PAPER_RECOVERY" in tick_block
    assert "track_b_status_paper_stack.sh" not in tick_block
    assert "track_b_paper_stack_restart_precheck" not in tick_block
    assert "run_managed_exit_actuator" not in tick_block
    assert "next_action" not in tick_block
    assert "broker_lifecycle" not in tick_block


def test_thin_recovery_script_uses_broker_truth_and_direct_minimal_start_only() -> None:
    source = THIN_RECOVERY_SCRIPT.read_text(encoding="utf-8")

    assert "ibkr_broker_truth_refresher" in source
    assert "BROKER_TRUTH_NOT_CLEAN_RECOVERY_BLOCKED" in source
    assert "WORKTREE_NOT_CLEAN_RECOVERY_BLOCKED" in source
    assert "TRACK_B_PAPER_STACK_RESTART=1" in source
    assert "TRACK_B_PAPER_MINIMAL_STARTUP_V1=1" in source
    assert "TRACK_B_PAPER_STACK_DISABLE_RECOVERY_SERVICE=1" in source
    assert "verify_runtime_shape" in source
    assert "mnq_mes_full_session_active_evidence" in source
    assert "IBKR_PAPER_BRIDGE" in source
    assert "expected_lanes = int" in source
    assert 'EXPECTED_LANES="${TRACK_B_PAPER_EXPECTED_LANE_COUNT:-71}"' in source
    assert "classify_fresh_complete_clean_broker_truth" in source
    assert "track_b_status_paper_stack.sh" not in source
    assert "track_b_paper_stack_restart_precheck" not in source
    assert "track_b_control_plane_snapshot" not in source
    assert "latest_track_b_paper_broker_reconciliation" not in source
    assert "launchctl" not in source
    assert "stop_probationary_paper_soak.sh" not in source
    assert "pkill" not in source


def test_paper_stack_startup_uses_unique_atomic_artifact_writes() -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")

    assert 'tmp = path.with_name(f".{path.name}.tmp")' not in source
    assert 'tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")' in source
    assert 'tmp = path.with_name(f".{path.name}.{sys.argv[2]}.tmp")' in source
    assert 'wrapper_tmp="${WRAPPER_PATH}.$$.$RANDOM.tmp"' in source
    assert 'cat > "${wrapper_tmp}" <<WRAPPER' in source
    assert 'mv "${wrapper_tmp}" "${WRAPPER_PATH}"' in source


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
    assert 'PROOF_REQUIRED_SYMBOLS="GC,MGC,NQ,ES,ZT,ZF,ZN,ZB,MNQ,MES"' in source


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
    assert '"PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_PARTICIPATION_SHORT_V1"' in source
    assert '"MNQ_US_DERIVATIVE_BEAR_TURN_V1"' in source
    assert '"PAPER_WATCH_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_LONG_SHADOW_V1"' in source
    assert '"PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_LONG_SHADOW_V1"' in source
    assert "FULL_SESSION_PROFILE_INITIAL_LONDON_LATE_SHORT_ONLY_ELEVATION" in source
    assert 'PROOF_REQUIRED_SYMBOLS="GC,MGC,NQ,ES,ZT,ZF,ZN,ZB,MNQ,MES"' in source

    block = source.split('elif [[ "${STACK_PROFILE}" == "mnq_mes_full_session_active_evidence" ]]; then', 1)[1]
    roster_json = block.split("cat > \"${SCOPED_ROSTER_PATH}\" <<'JSON'", 1)[1].split("\nJSON", 1)[0]
    assert '"PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_SHADOW_V1"' not in roster_json
    roster = json.loads(roster_json)
    current_ids = [
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
        "PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_PARTICIPATION_SHORT_V1",
    ]
    batch_ids = [
        f"PAPER_ACTIVE_EVIDENCE_{symbol}_{session}_PARTICIPATION_{side}_V1"
        for symbol in ("MGC", "GC", "NQ", "ES", "ZT", "ZF", "ZN", "ZB")
        for session, side in (
            ("US", "LONG"),
            ("US", "SHORT"),
            ("GLOBEX", "LONG"),
            ("GLOBEX", "SHORT"),
            ("LONDON_OPEN", "LONG"),
            ("LONDON_OPEN", "SHORT"),
            ("LONDON_LATE", "SHORT"),
        )
    ]
    expected_ids = [*current_ids, *batch_ids, "MNQ_US_DERIVATIVE_BEAR_TURN_V1"]
    assert roster["enabled_strategy_ids"] == expected_ids
    assert len(roster["enabled_strategy_ids"]) == 71
    assert any("_ZT_" in strategy_id for strategy_id in expected_ids)
    assert any("_ZF_" in strategy_id for strategy_id in expected_ids)
    assert any("_ZN_" in strategy_id for strategy_id in expected_ids)
    assert any("_ZB_" in strategy_id for strategy_id in expected_ids)
    assert not any("_PL_" in strategy_id for strategy_id in expected_ids)
    assert roster["shadow_only_strategy_ids"] == [
        "PAPER_WATCH_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_LONG_SHADOW_V1",
        "PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_LONG_SHADOW_V1",
    ]


def test_paper_stack_full_session_materializes_seventy_one_lane_specs(tmp_path: Path) -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")
    assert "materialize_scoped_lane_config_from_roster" in source

    block = source.split('elif [[ "${STACK_PROFILE}" == "mnq_mes_full_session_active_evidence" ]]; then', 1)[1]
    assert "probationary_paper_lanes_json: '[]'" not in block.split("elif [[", 1)[0]
    roster_json = block.split("cat > \"${SCOPED_ROSTER_PATH}\" <<'JSON'", 1)[1].split("\nJSON", 1)[0]
    roster = json.loads(roster_json)

    helper_block = source.split("materialize_scoped_lane_config_from_roster() {", 1)[1]
    helper_python = helper_block.split("<<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
    roster_path = tmp_path / "roster.json"
    source_config_path = tmp_path / "paper_config_in_force.json"
    output_config_path = tmp_path / "paper_stack_mnq_mes_full_session_active_evidence.yaml"
    roster_path.write_text(json.dumps(roster), encoding="utf-8")
    source_config_path.write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": f"lane_{index}",
                        "long_sources": [strategy_id],
                        "short_sources": [],
                        "symbol": "MNQ" if "MNQ" in strategy_id else "MES",
                    }
                    for index, strategy_id in enumerate(roster["enabled_strategy_ids"])
                ]
            }
        ),
        encoding="utf-8",
    )

    subprocess.run(
        [sys.executable, "-", str(roster_path), str(source_config_path), str(output_config_path), str(REPO_ROOT)],
        input=helper_python,
        text=True,
        check=True,
    )

    generated = output_config_path.read_text(encoding="utf-8")
    assert "probationary_paper_runtime_exclusive_config: true" in generated
    raw_lanes = generated.split("probationary_paper_lanes_json: ", 1)[1].strip()
    lanes = json.loads(raw_lanes)
    assert len(lanes) == 71
    assert [lane["long_sources"][0] for lane in lanes] == roster["enabled_strategy_ids"]
    assert {lane["execution_mode"] for lane in lanes} == {"IBKR_PAPER_BRIDGE"}
    assert {lane["current_order_destination"] for lane in lanes} == {"ibkr_paper_bridge_submit_capable"}
    assert {lane["runtime_overlay_params"]["execution_mode"] for lane in lanes} == {"IBKR_PAPER_BRIDGE"}


def test_paper_stack_full_session_materializer_fills_missing_lane_specs_from_contract(tmp_path: Path) -> None:
    source = START_SCRIPT.read_text(encoding="utf-8")
    block = source.split('elif [[ "${STACK_PROFILE}" == "mnq_mes_full_session_active_evidence" ]]; then', 1)[1]
    roster_json = block.split("cat > \"${SCOPED_ROSTER_PATH}\" <<'JSON'", 1)[1].split("\nJSON", 1)[0]
    roster = json.loads(roster_json)
    helper_block = source.split("materialize_scoped_lane_config_from_roster() {", 1)[1]
    helper_python = helper_block.split("<<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
    existing_ids = [
        strategy_id
        for strategy_id in roster["enabled_strategy_ids"]
        if strategy_id
        not in {
            "PAPER_ACTIVE_EVIDENCE_MGC_US_PARTICIPATION_LONG_V1",
            "PAPER_ACTIVE_EVIDENCE_ZT_US_PARTICIPATION_LONG_V1",
            "PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_PARTICIPATION_SHORT_V1",
            "MNQ_US_DERIVATIVE_BEAR_TURN_V1",
        }
    ]
    roster_path = tmp_path / "roster.json"
    source_config_path = tmp_path / "paper_config_in_force.json"
    output_config_path = tmp_path / "paper_stack_mnq_mes_full_session_active_evidence.yaml"
    roster_path.write_text(json.dumps(roster), encoding="utf-8")
    source_config_path.write_text(
        json.dumps(
            {
                "lanes": [
                    {
                        "lane_id": f"lane_{index}",
                        "long_sources": [strategy_id],
                        "short_sources": [],
                        "symbol": "MNQ" if "MNQ" in strategy_id else "MES",
                    }
                    for index, strategy_id in enumerate(existing_ids)
                ]
            }
        ),
        encoding="utf-8",
    )

    subprocess.run(
        [sys.executable, "-", str(roster_path), str(source_config_path), str(output_config_path), str(REPO_ROOT)],
        input=helper_python,
        text=True,
        check=True,
    )

    raw_lanes = output_config_path.read_text(encoding="utf-8").split("probationary_paper_lanes_json: ", 1)[1].strip()
    lanes = json.loads(raw_lanes)
    by_source = {
        next(iter([*lane.get("long_sources", []), *lane.get("short_sources", [])]), None): lane
        for lane in lanes
    }
    mes_late = by_source["PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_PARTICIPATION_SHORT_V1"]
    mgc_us = by_source["PAPER_ACTIVE_EVIDENCE_MGC_US_PARTICIPATION_LONG_V1"]
    zt_us = by_source["PAPER_ACTIVE_EVIDENCE_ZT_US_PARTICIPATION_LONG_V1"]
    assert len(lanes) == 71
    assert mes_late["lane_id"] == "mes_london_late_active_participation_short"
    assert mes_late["symbol"] == "MES"
    assert mes_late["session_restriction"] == "LONDON_LATE"
    assert mes_late["execution_mode"] == "IBKR_PAPER_BRIDGE"
    assert mes_late["runtime_overlay_params"]["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
    assert mgc_us["lane_id"] == "mgc_us_active_participation_long"
    assert mgc_us["symbol"] == "MGC"
    assert mgc_us["local_symbol"] == "MGCQ6"
    assert mgc_us["con_id"] == 732156883
    assert mgc_us["point_value"] == "10"
    assert mgc_us["session_restriction"] == "US"
    assert mgc_us["max_position_quantity"] == 1
    assert mgc_us["runtime_overlay_params"]["input_event_path"].endswith(
        "latest_mgc_us_active_participation_long_event_envelope.json"
    )
    assert zt_us["lane_id"] == "zt_us_active_participation_long"
    assert zt_us["symbol"] == "ZT"
    assert zt_us["local_symbol"] == "ZTU6"
    assert zt_us["con_id"] == 842590391
    assert zt_us["point_value"] == "2000"
    assert zt_us["session_restriction"] == "US"
    assert zt_us["runtime_overlay_params"]["input_event_path"].endswith(
        "latest_zt_us_active_participation_long_event_envelope.json"
    )
    mnq_derivative = by_source["MNQ_US_DERIVATIVE_BEAR_TURN_V1"]
    assert mnq_derivative["lane_id"] == "mnq_us_derivative_bear_turn"
    assert mnq_derivative["symbol"] == "MNQ"
    assert mnq_derivative["runtime_kind"] == "track_b_rule_runner_paper_strategy_engine"
    assert mnq_derivative["execution_mode"] == "IBKR_PAPER_BRIDGE"
    assert mnq_derivative["runtime_overlay_params"]["input_event_path"].endswith(
        "latest_mnq_us_derivative_bear_turn_event_envelope.json"
    )


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
    assert "THIN_RECOVERY_RESTART_REQUIRED" in source
    assert "NO_ACTION_BROKER_TRUTH_NOT_CLEAN" in source
    assert "BROKER_TRUTH_NOT_CLEAN_RECOVERY_BLOCKED" in source
    assert "START_REQUESTED_THIN_PAPER_RECOVERY" in source
    assert 'TRACK_B_PAPER_STACK_PROFILE="mnq_mes_full_session_active_evidence" bash "${THIN_RECOVERY_SCRIPT}" check' in source
    assert 'TRACK_B_PAPER_STACK_PROFILE="mnq_mes_full_session_active_evidence" bash "${THIN_RECOVERY_SCRIPT}" restart' in source
    assert 'TRACK_B_PAPER_STACK_PROFILE="mnq_mes_full_session_active_evidence" bash "${THIN_RECOVERY_SCRIPT}" start' in source
    assert "START_REQUESTED_CANONICAL_PAPER_STACK" not in source
    assert "START_REQUESTED_APPROVED_PAPER_STACK" not in source
    assert "NO_ACTION_BLOCKED_GATES" not in source
    assert "NO_ACTION_DUPLICATE_WRITER" not in source
    assert "restart_allowed_if_runtime_down" not in source
    assert "duplicate_writer.duplicate_writer_detected" not in source
    assert "track_b_paper_stack_restart_precheck" not in source
    assert "resolve_recovery_profile" in source
    assert "resolve_recovery_profile" not in source[source.index("  tick)\n") : source.index("  enable)\n")]
    assert "placeorder" not in source.lower()
    assert "cancelorder" not in source.lower()
    assert "flatten" not in source.lower()


def test_recovery_tick_does_not_mutate_broker_before_thin_restart() -> None:
    source = RECOVERY_SCRIPT.read_text(encoding="utf-8")
    tick_block = source[source.index("  tick)\n") : source.index("  enable)\n")]

    assert "run_managed_exit_actuator" not in tick_block
    assert "mgc_v05l.execution_core.track_b_managed_exit_actuator" not in tick_block
    assert "--apply" not in tick_block
    assert "--operator-authorized-managed-exit" not in tick_block
    assert 'bash "${THIN_RECOVERY_SCRIPT}" check' in tick_block
    assert 'bash "${THIN_RECOVERY_SCRIPT}" restart' in tick_block
    assert 'bash "${THIN_RECOVERY_SCRIPT}" start' in tick_block


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
