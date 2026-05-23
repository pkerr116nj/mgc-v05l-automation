from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_SCRIPT = REPO_ROOT / "scripts" / "run_headless_supervised_paper_service.sh"
STATUS_SCRIPT = REPO_ROOT / "scripts" / "show_headless_supervised_paper_status.sh"
PROBATIONARY_PAPER_SOAK_SCRIPT = REPO_ROOT / "scripts" / "run_probationary_paper_soak.sh"
STOP_PAPER_SCRIPT = REPO_ROOT / "scripts" / "stop_probationary_paper_soak.sh"
OPERATOR_READINESS_STATUS_SCRIPT = REPO_ROOT / "scripts" / "status-track-b-operator-readiness-refresh"
OPERATOR_READINESS_START_SCRIPT = REPO_ROOT / "scripts" / "start-track-b-operator-readiness-refresh"
OPERATOR_READINESS_STOP_SCRIPT = REPO_ROOT / "scripts" / "stop-track-b-operator-readiness-refresh"



def test_operator_readiness_status_distinguishes_service_liveness_from_fresh_artifact() -> None:
    script = OPERATOR_READINESS_STATUS_SCRIPT.read_text(encoding="utf-8")

    assert "service_running=true" in script
    assert "service_running=false" in script
    assert "status_fresh=true" in script
    assert "status_fresh=false" in script
    assert "heartbeat_fresh=true" in script
    assert "last_success=" in script
    assert "last_failure=" in script
    assert "refresh_age_seconds=" in script
    assert 'pid_is_running "${service_pid}" || status_is_fresh' not in script


def test_operator_readiness_start_script_supervises_and_blocks_duplicates() -> None:
    script = OPERATOR_READINESS_START_SCRIPT.read_text(encoding="utf-8")

    assert "--supervisor" in script
    assert "--child-pid-path" in script
    assert "--supervisor-status-path" in script
    assert "TRACK_B_OPERATOR_READINESS_ALLOW_DUPLICATE" in script
    assert "--canonical-readiness-path" in script
    assert "mgc_v05l.app.track_b_operator_readiness_refresher" in script
    assert "track_b_operator_readiness_refresh_service.pid" in script
    assert "track_b_operator_readiness_refresh_child.pid" in script


def test_operator_readiness_stop_script_stops_supervisor_and_child() -> None:
    script = OPERATOR_READINESS_STOP_SCRIPT.read_text(encoding="utf-8")

    assert "track_b_operator_readiness_refresh_service.pid" in script
    assert "track_b_operator_readiness_refresh_child.pid" in script
    assert 'stop_pid "supervisor"' in script
    assert 'stop_pid "child"' in script

def test_status_script_produces_canonical_readiness_without_dashboard_ownership() -> None:
    script = STATUS_SCRIPT.read_text(encoding="utf-8")

    assert "mgc_v05l.app.track_b_canonical_readiness" in script
    assert "mgc_v05l.app.track_b_operator_readiness_refresher" in script
    assert "mgc_v05l.app.track_b_broker_truth_lease" in script
    assert script.index("refresh_operator_readiness_artifacts") < script.index("refresh_broker_truth_lease")
    assert script.index("refresh_broker_truth_lease") < script.index("refresh_canonical_readiness")
    assert "--repo-root \"${REPO_ROOT}\"" in script
    assert "--expected-root \"${REPO_ROOT}\"" in script
    assert "--output-path \"${CANONICAL_READINESS_FILE}\"" in script
    assert "/api/dashboard" in script
    assert "print_canonical_readiness_summary" in script
    assert "merge_canonical_readiness_status" in script
    assert "canonical_readiness_exit_for_classification" in script
    assert "status[\"canonical_readiness\"]" in script
    assert "status[\"shared_truth\"]" in script
    assert "Market closed/no fresh bars expected" in script
    assert "\"source_authority\": \"execution_core_authority\"" in script
    assert "\"projection_only\": True" in script
    assert "shared_truth_open_order_truth" in script
    assert "shared_truth_order_adjustment_planner" in script
    assert "latest_track_b_position_truth.json" not in script
    assert "latest_track_b_open_order_truth.json" not in script
    assert "latest_track_b_managed_orders.json" not in script
    assert "status[\"lane_quarantine\"]" in script
    assert script.index("refresh_canonical_readiness") < script.index("fetch_health_snapshot")


def test_status_script_surfaces_advisory_maintenance_supervisor_decision() -> None:
    script = STATUS_SCRIPT.read_text(encoding="utf-8")

    assert "mgc_v05l.app.track_b_readiness_maintenance_supervisor" in script
    assert "--repo-root \"${REPO_ROOT}\"" in script
    assert "--output-path \"${MAINTENANCE_SUPERVISOR_FILE}\"" in script
    assert "DEFAULT_MAINTENANCE_SUPERVISOR_FILE" in script
    assert "DEFAULT_MAINTENANCE_SUPERVISOR_SUMMARY_FILE" in script
    assert "refresh_maintenance_supervisor" in script
    assert "print_maintenance_supervisor_summary" in script
    assert "merge_maintenance_supervisor_status" in script
    assert "status[\"maintenance_supervisor\"]" in script
    assert "status[\"maintenance_supervisor_state\"]" in script
    assert "status[\"maintenance_supervisor_recommended_actions\"]" in script
    assert "operator_action_required" in script
    assert "submit_block_required" in script
    assert script.index("refresh_canonical_readiness") < script.index("refresh_maintenance_supervisor")
    assert script.index("merge_canonical_readiness_status") < script.index("merge_maintenance_supervisor_status")


def test_status_script_reads_paper_runtime_truth_as_evidence_only() -> None:
    script = STATUS_SCRIPT.read_text(encoding="utf-8")

    assert "DEFAULT_PAPER_RUNTIME_TRUTH_FILE" in script
    assert "paper_runtime_truth.json" in script
    assert "merge_paper_runtime_truth_status" in script
    assert "paper_runtime_truth_evidence_only" in script
    assert "paper_runtime_truth_writer_authority" in script
    assert "paper_runtime_truth_b_plus_threshold" in script
    assert script.index("merge_canonical_readiness_status") < script.index("merge_paper_runtime_truth_status")
    assert "ready_submit_capable\" = truth" not in script
    assert "paper_trade_allowed\" = truth" not in script


def test_status_script_surfaces_runtime_generation_evidence_only() -> None:
    script = STATUS_SCRIPT.read_text(encoding="utf-8")

    assert "DEFAULT_PAPER_PID_METADATA_FILE" in script
    assert "probationary_paper.pid.json" in script
    assert "merge_paper_runtime_generation_status" in script
    assert "paper_runtime_generation_evidence_only" in script
    assert "paper_runtime_pid_metadata_state" in script
    assert "paper_runtime_generation_mismatches" in script
    assert "paper_runtime_generation_duplicate_writer_state" in script
    assert "classify_pid_metadata" in script
    assert "classify_runtime_launch_guard" in script
    assert "runtime_generation_mismatches" in script
    assert "paper_runtime_launch_guard_classification" in script
    assert "paper_runtime_generation_config_in_force_freshness" in script
    assert "paper_runtime_generation_operator_status_freshness" in script
    assert "paper_runtime_generation_runtime_truth_freshness" in script


def test_status_script_surfaces_control_plane_services_as_projection_only() -> None:
    script = STATUS_SCRIPT.read_text(encoding="utf-8")

    assert "DEFAULT_AGENT_REGISTRY_FILE" in script
    assert "DEFAULT_AGENT_HEALTH_FILE" in script
    assert "DEFAULT_SELF_RECOVER_RULES_FILE" in script
    assert "DEFAULT_CRASH_LOOP_PROTECTION_FILE" in script
    assert "DEFAULT_RUNTIME_RESUME_SEMANTICS_FILE" in script
    assert "DEFAULT_PAPER_RECOVERY_POLICY_FILE" in script
    assert "merge_control_plane_services_status" in script
    assert "status[\"track_b_control_plane\"]" in script
    assert "\"source_authority\": \"execution_core_authority\"" in script
    assert "\"projection_only\": True" in script
    assert "\"not_routing_authority\": True" in script
    assert "WAIT_MARKET_CLOSED" in script
    assert "RESUME_BLOCKED_MARKET_CLOSED" in script
    assert "MARKET_CLOSED_NO_FRESH_BARS" in script
    assert "runtime_resume_safe_to_start_runtime" in script
    assert "paper_recovery_policy" in script
    assert "paper_recovery_diagnostic" in script
    assert "requires_operator_ack_for_paper" in script
    assert "operator_ack_advisory_only_for_paper" in script
    assert script.index("merge_paper_runtime_generation_status") < script.index("merge_control_plane_services_status")
    assert "latest_track_b_runtime_resume_semantics.json" not in script
    assert "latest_track_b_crash_loop_protection.json" not in script
    assert script.index("merge_paper_runtime_truth_status") < script.index("merge_paper_runtime_generation_status")
    assert "ready_submit_capable\" = pid_metadata" not in script
    assert "paper_trade_allowed\" = pid_metadata" not in script


def test_launch_script_uses_generation_guard_for_stale_pid_and_duplicate_writers() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert "classify_paper_runtime_launch_guard" in script
    assert "track_b_paper_runtime_launch_guard_v1" in script
    assert "classify_runtime_launch_guard" in script
    assert "LAUNCH_PID_ACCEPTED" in script
    assert "LAUNCH_STALE_PID_CLEANUP_ALLOWED" in script
    assert "LAUNCH_CONFLICTING_WRITER_BLOCKED" in script
    assert "LAUNCH_WRONG_ROOT_BLOCKED" in script
    assert "LAUNCH_ZOMBIE_PID_REJECTED" in script
    assert "PAPER_RUNTIME_LAUNCH_GUARD_BLOCKED" in script
    assert 'cleanup_stale_paper_pid_metadata_if_allowed' in script
    assert 'rm -f "${PAPER_PID_FILE}" "${PAPER_PID_METADATA_FILE}"' in script
    assert 'broker_clean = str(reconciliation.get("classification") or "") == "TRACK_B_PAPER_BROKER_RECONCILED"' in script
    assert script.index("classify_paper_runtime_launch_guard()") < script.index("start_paper_runtime()")
    start_flow = script[script.index("start_paper_runtime()") :]
    assert start_flow.index("guard_classification=\"$(classify_paper_runtime_launch_guard)\"") < start_flow.index(
        "launch_background_paper_runtime"
    )


def test_status_script_keeps_readiness_exit_code_primary_over_supervisor() -> None:
    script = STATUS_SCRIPT.read_text(encoding="utf-8")

    assert "maintenance_supervisor_exit_code=$?" in script
    assert "canonical_state=\"$(canonical_readiness_classification)\"" in script
    assert script.rstrip().endswith('canonical_readiness_exit_for_classification "${canonical_state}"')
    assert "exit_code_for_state" not in script


def test_launch_script_passes_canonical_readiness_paths_to_status_script() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert "DEFAULT_CANONICAL_READINESS_FILE" in script
    assert "DEFAULT_CANONICAL_READINESS_SUMMARY_FILE" in script
    assert "refresh_canonical_readiness_for_launch \"pre-launch\"" in script
    assert "refresh_canonical_readiness_for_launch \"post-launch\"" in script
    assert "fail_fast_if_hard_canonical_blocker" in script
    assert "NOT_READY_WRONG_ROOT|NOT_READY_CONFIG|NOT_READY_RECONCILIATION" in script
    assert "--canonical-readiness-output \"${CANONICAL_READINESS_FILE}\"" in script
    assert "--canonical-readiness-summary-output \"${CANONICAL_READINESS_SUMMARY_FILE}\"" in script
    assert "mgc_v05l.app.track_b_canonical_readiness" in script
    assert "mgc_v05l.app.track_b_operator_readiness_refresher" in script
    assert "mgc_v05l.app.track_b_broker_truth_lease" in script
    assert "refresh_operator_readiness_for_launch" in script
    assert "refresh_broker_truth_lease_for_launch" in script
    assert script.index("refresh_operator_readiness_for_launch || true") < script.index("refresh_broker_truth_lease_for_launch || true")
    assert "broker_truth_lease_state" in script
    assert "shared_truth_open_order_truth" in script
    assert "shared_truth_order_adjustment_planner" in script
    assert "Headless supervised paper host is READY_SUBMIT_CAPABLE." in script


def test_runtime_start_consults_supervisor_authority_v2_before_spawn() -> None:
    script = PROBATIONARY_PAPER_SOAK_SCRIPT.read_text(encoding="utf-8")

    assert "DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_FILE" in script
    assert "run_runtime_supervisor_start_preflight" in script
    assert "mgc_v05l.execution_core.track_b_runtime_supervisor_authority" in script
    assert "--no-dashboard-projection" in script
    assert "SUPERVISOR_RUNTIME_START_ALLOWED" in script
    assert "READY_FOR_OPERATOR_START" in script
    assert "safe_to_start_runtime" in script
    assert "RUNTIME_SUPERVISOR_START_BLOCKED" in script
    assert "operator_ack_required" in script
    assert "runtime_supervisor_authority" in script
    assert "latest_track_b_runtime_supervisor_authority.json" not in script
    assert script.index("run_shared_truth_runtime_start_preflight") < script.index("run_runtime_supervisor_start_preflight")
    assert script.index("run_runtime_supervisor_start_preflight") < script.index("nohup \"${LAUNCH_PYTHON_BIN}\"")


def test_headless_launch_uses_supervisor_authority_v2_as_final_pre_spawn_gate() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert "DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_FILE" in script
    assert "refresh_runtime_supervisor_for_launch" in script
    assert "runtime_supervisor_start_gate" in script
    assert "runtime_supervisor_blocked_reason" in script
    assert "RUNTIME_SUPERVISOR_START_BLOCKED" in script
    assert "SUPERVISOR_RUNTIME_START_ALLOWED" in script
    assert "READY_FOR_OPERATOR_START" in script
    assert "safe_to_start_runtime" in script
    assert "operator_ack_required" in script
    assert "runtime_supervisor_authority_path" in script
    assert "latest_track_b_runtime_supervisor_authority.json" not in script
    start_flow = script[script.index("persist_requested_config_paths\nif ! assert_required_config_paths_present") :]
    assert start_flow.index("refresh_canonical_readiness_for_launch \"pre-launch\"") < start_flow.index(
        "refresh_runtime_supervisor_for_launch"
    )
    assert start_flow.index("runtime_supervisor_start_gate") < start_flow.index("if ! start_paper_runtime")


def test_launch_script_uses_profile_aware_broker_truth_sidecar_policy() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert "BROKER_TRUTH_REFRESH_PROFILE" in script
    assert "DEFAULT_START_BROKER_TRUTH_REFRESH=1" in script
    assert "dev|development|test|local)" in script
    assert "DEFAULT_START_BROKER_TRUTH_REFRESH=0" in script
    assert "--start-broker-truth-refresh)" in script
    assert "--no-start-broker-truth-refresh)" in script
    assert "--strict-broker-truth-refresh)" in script
    assert "truthy_flag \"${START_BROKER_TRUTH_REFRESH}\"" in script


def test_launch_script_makes_explicit_config_stack_authoritative() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert "DEFAULT_REQUESTED_CONFIG_PATHS_FILE" in script
    assert "DEFAULT_PAPER_CONFIG_PATHS_FILE" in script
    assert "MGC_HEADLESS_REQUIRED_PAPER_CONFIGS" in script
    assert "MGC_HEADLESS_REQUIRED_PAPER_CONFIG_PATHS" in script
    assert "persist_requested_config_paths" in script
    assert "assert_required_config_paths_present" in script
    assert "Requested paper runtime config stack is missing required config paths." in script
    assert "assert_runtime_config_paths_match_request" in script
    assert "cp \"${REQUESTED_CONFIG_PATHS_FILE}\" \"${PAPER_CONFIG_PATHS_FILE}\"" in script
    assert "export MGC_PROBATIONARY_PAPER_CONFIG_PATHS={q(requested_stack)}" in script
    assert "pre-existing-runtime" in script
    assert "post-start" in script
    assert "pre-success" in script
    assert "Active paper runtime config paths did not match requested launch config stack" in script


def test_launch_script_non_screen_wrapper_uses_resolved_paths_and_env() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert "write_paper_runtime_wrapper" in script
    assert "local wrapper_path=\"${PAPER_PID_FILE}.runtime_wrapper.sh\"" in script
    assert "requested_stack=\"$(requested_config_paths_arg)\"" in script
    assert "required_stack=\"$(resolved_config_paths_arg \"${REQUIRED_PAPER_CONFIG_PATHS}\")\"" in script
    assert "export REPO_ROOT={q(repo_root)}" in script
    assert "export PYTHON_BIN={q(python_bin)}" in script
    assert "export PYTHONPATH={q(str(Path(repo_root) / \"src\"))}" in script
    assert "export MGC_HEADLESS_SUPERVISED_PAPER_CONFIG_PATHS={q(requested_stack)}" in script
    assert "export MGC_HEADLESS_REQUIRED_PAPER_CONFIGS={q(required_stack)}" in script
    assert "export MGC_HEADLESS_REQUIRED_PAPER_CONFIG_PATHS={q(required_stack)}" in script
    assert "export MGC_TRACK_B_RUNTIME_INSTANCE_ID={q(runtime_instance_id)}" in script
    assert "export MGC_TRACK_B_PAPER_RUNTIME_RESTART_GENERATION={q(restart_generation)}" in script
    assert "export MGC_TRACK_B_PAPER_PID_METADATA_FILE={q(pid_metadata_file)}" in script
    assert "export MGC_TRACK_B_PAPER_CONFIG_FINGERPRINT={q(config_fingerprint)}" in script
    assert "headless_runtime_wrapper_start" in script
    assert "headless_runtime_wrapper_exec" in script


def test_launch_script_creates_generation_metadata_before_paper_start() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert "DEFAULT_PAPER_PID_METADATA_FILE" in script
    assert "prepare_paper_runtime_generation" in script
    assert "track_b_paper_runtime_pid_metadata_v1" in script
    assert "PAPER_RUNTIME_INSTANCE_ID=\"track-b-paper-runtime-$(date -u +%Y%m%dT%H%M%SZ)-$$\"" in script
    assert "PAPER_RUNTIME_RESTART_GENERATION" in script
    assert "PAPER_RUNTIME_CONFIG_FINGERPRINT" in script
    assert "\"pid\": None" in script
    assert "\"launcher_pid\": int(launcher_pid)" in script
    assert "\"expected_project_root\": str(root)" in script
    assert "MGC_TRACK_B_PAPER_LAUNCH_STARTED_AT" in script
    assert script.index("prepare_paper_runtime_generation") < script.index("wrapper_path=\"$(write_paper_runtime_wrapper)\"")


def test_launch_script_uses_launchctl_paper_runtime_contract_without_screen_fallback() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert "DEFAULT_PAPER_WRAPPER_PID_FILE" in script
    assert "PAPER_WRAPPER_PID_FILE" in script
    assert "launch_background_paper_runtime" in script
    launch_function = script[script.index("launch_background_paper_runtime()") : script.index("launch_screen_dashboard_manager()")]
    assert "rm -f \"${PAPER_WRAPPER_PID_FILE}\"" in launch_function
    assert "rm -f \"${PAPER_PID_FILE}.screen_session\"" in launch_function
    assert "launchctl_submit_available" in launch_function
    assert "launch_detached_paper_runtime" in launch_function
    assert "CANONICAL_PAPER_LAUNCHER_UNAVAILABLE" in launch_function
    assert "screen_available" not in launch_function
    assert "screen -dmS \"${session_name}\" /bin/bash \"${wrapper_path}\"" not in launch_function
    assert "nohup /bin/bash \"${wrapper_path}\" >> \"${PAPER_LOG_FILE}\" 2>&1 &" not in launch_function
    assert "echo \"$!\" > \"${PAPER_WRAPPER_PID_FILE}\"" not in launch_function
    assert "launch_background_paper_runtime\n    return 0" not in launch_function
    assert "launch_rc=$?" in launch_function
    assert "return \"${launch_rc}\"" in launch_function
    assert "-o \"${PAPER_LAUNCHCTL_STDOUT_FILE}\"" in script
    assert "-e \"${PAPER_LAUNCHCTL_STDERR_FILE}\"" in script
    assert "-- /bin/bash \"${wrapper_path}\"" in script
    assert ">\"${PAPER_LAUNCHCTL_STDOUT_FILE}\" 2>\"${PAPER_LAUNCHCTL_STDERR_FILE}\"" not in script
    assert '"launchctl_command": [' in script
    assert "write_launchctl_runtime_status \"LAUNCHCTL_SUBMIT_ACCEPTED\"" in script
    assert "write_launchctl_runtime_status \"LAUNCHCTL_SUBMIT_FAILED\"" in script
    assert launch_function.index("rm -f \"${PAPER_WRAPPER_PID_FILE}\"") < launch_function.index(
        "launchctl_submit_available"
    )
    assert launch_function.index("launchctl_submit_available") < launch_function.index(
        "launch_detached_paper_runtime"
    )
    assert launch_function.index("launch_detached_paper_runtime") < launch_function.index("return \"${launch_rc}\"")


def test_launch_script_propagates_launchctl_submit_failure_before_pid_polling() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    paper_launch = script[script.index("launch_background_paper_runtime()") : script.index("launch_screen_dashboard_manager()")]
    detached_launch = script[script.index("launch_detached_paper_runtime()") : script.index("launch_detached_dashboard_manager()")]
    start_flow = script[script.index("if ! start_paper_runtime") :]

    assert "return 0" not in paper_launch
    assert "return \"${launch_rc}\"" in paper_launch
    assert "write_launchctl_runtime_status \"LAUNCHCTL_SUBMIT_FAILED\"" in detached_launch
    assert "launchctl remove \"${label}\"" in detached_launch
    assert "rm -f \"${PAPER_PID_FILE}.launchctl_label\"" in detached_launch
    assert start_flow.index("if ! start_paper_runtime") < start_flow.index(
        'wait_for_runtime_config_paths_match_request "post-start"'
    )


def test_launch_script_retains_screen_only_for_dashboard_manager_not_paper_runtime() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    paper_launch = script[script.index("launch_background_paper_runtime()") : script.index("launch_screen_dashboard_manager()")]
    dashboard_launch = script[script.index("launch_screen_dashboard_manager()") : script.index("launch_detached_paper_runtime()")]

    assert "screen_available" not in paper_launch
    assert "screen -dmS" not in paper_launch
    assert "screen -dmS" in dashboard_launch


def test_launch_script_uses_direct_supervisor_as_canonical_paper_launcher() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert 'PAPER_RUNTIME_LAUNCH_METHOD="${MGC_HEADLESS_PAPER_LAUNCH_METHOD:-direct}"' in script
    paper_launch = script[script.index("launch_background_paper_runtime()") : script.index("launch_screen_dashboard_manager()")]
    assert "launch_direct_paper_runtime" in paper_launch
    assert "diagnostic-launchctl" in paper_launch
    assert "MGC_PROBATIONARY_PAPER_RUNTIME_TRUTH_FILE" in script
    assert "run_probationary_paper_soak.sh" in script


def test_launch_script_propagates_paper_runtime_start_failure() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    start_fn = script[script.index("start_paper_runtime()") : script.index("refresh_phase1_reconciliation_for_launch()")]
    assert "launch_background_paper_runtime" in start_fn
    assert "return $?" in start_fn
    assert "launch_background_paper_runtime\n  return 0" not in start_fn


def test_launch_script_polls_for_late_post_start_runtime_pid() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert "MGC_HEADLESS_POST_START_PID_WAIT_TIMEOUT_SECONDS" in script
    assert "--post-start-pid-wait-timeout-seconds" in script
    assert "wait_for_runtime_config_paths_match_request()" in script
    assert "local deadline=$((SECONDS + timeout_seconds))" in script
    assert "assert_runtime_config_paths_match_request \"${phase}\"" in script
    assert 'case "${rc}" in' in script
    assert "RUNTIME_PID_UNAVAILABLE: Paper runtime PID did not become available during ${phase} within ${timeout_seconds}s." in script
    assert 'wait_for_runtime_config_paths_match_request "post-start" "${POST_START_PID_WAIT_TIMEOUT_SECONDS}"' in script
    assert "write_launchctl_runtime_status \"RUNTIME_PID_UNAVAILABLE_AFTER_LAUNCHCTL_SUBMIT\"" in script
    assert "write_launchctl_runtime_status \"RUNTIME_PID_AVAILABLE\"" in script
    launch_flow = script[script.index("if ! start_paper_runtime") :]
    assert launch_flow.index("wait_for_runtime_config_paths_match_request \"post-start\"") < launch_flow.index(
        "start_dashboard_manager"
    )


def test_launch_script_pid_polling_fails_closed_for_wrong_root_or_config() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert 'subprocess.check_output(["ps", "-p", pid, "-o", "command="]' in script
    assert 'subprocess.check_output(["lsof", "-a", "-p", pid, "-d", "cwd", "-Fn"]' in script
    assert 'runtime_markers = ("mgc_v05l.app.main", "probationary-paper-soak")' in script
    assert "RUNTIME_PID_PENDING" in script
    assert "RUNTIME_PID_UNAVAILABLE: Paper runtime PID is unavailable during ${phase}." in script
    assert "RUNTIME_EXITED_BEFORE_PID" in script
    assert "Paper runtime root mismatch during {phase}" in script
    assert "Paper runtime config path mismatch during {phase}" in script
    assert "raise SystemExit(2)" in script
    assert "2)\n        return 2" in script
    assert "3)\n        return 3" in script
    assert "stop_paper_runtime_best_effort" in script
    assert script.index("Paper runtime root mismatch during {phase}") < script.index("missing = [path for path in requested")


def test_launch_script_wrapper_writes_pre_exec_status_before_pid() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    wrapper = script[script.index("write_paper_runtime_wrapper()") : script.index("launch_background_paper_runtime()")]
    assert "MGC_TRACK_B_PAPER_WRAPPER_STATUS_FILE" in wrapper
    assert "write_wrapper_status \"WRAPPER_STARTED\"" in wrapper
    assert "write_wrapper_status \"WRAPPER_PRE_EXEC_FAILURE\" \"source_commit_mismatch\"" in wrapper
    assert "write_wrapper_status \"WRAPPER_EXECING_RUNTIME\"" in wrapper
    assert wrapper.index("write_wrapper_status \"WRAPPER_STARTED\"") < wrapper.index("echo \"$$\" > \"$MGC_HEADLESS_PAPER_PID_FILE\"")


def test_stop_script_clears_stale_launchctl_label_without_pid_file() -> None:
    script = STOP_PAPER_SCRIPT.read_text(encoding="utf-8")

    no_pid_branch = script[script.index("if [[ ! -f \"${PID_FILE}\" ]]") :]
    assert "rm -f \"${LAUNCHCTL_LABEL_FILE}\"" in no_pid_branch
    assert no_pid_branch.index("rm -f \"${LAUNCHCTL_LABEL_FILE}\"") < no_pid_branch.index(
        "No probationary paper PID file found"
    )


def test_launch_script_post_start_guard_still_enforces_required_overlay_stack() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert "assert_required_config_paths_present" in script
    assert "Missing required paper config path(s): " in script
    assert "Requested paper runtime config stack is missing required config paths." in script
    assert "persist_requested_config_paths" in script
    assert script.index("persist_requested_config_paths") < script.index("assert_required_config_paths_present")
    assert script.index("assert_required_config_paths_present") < script.index("start_paper_runtime")


def test_launch_script_reports_missing_runtime_pid_separately_from_config_mismatch() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert "RUNTIME_PID_UNAVAILABLE" in script
    assert "Active paper runtime config paths did not match requested launch config stack." in script
    post_start = script[script.index('wait_for_runtime_config_paths_match_request "post-start"') :]
    assert "wait_rc=$?" in post_start
    assert "if [[ \"${wait_rc}\" -eq 1 ]]" in post_start
    assert "Paper runtime PID unavailable during post-start." in post_start
    assert "Paper runtime exited before a valid Python runtime PID became available during post-start." in post_start
    assert "Active paper runtime config paths did not match requested launch config stack." in post_start


def test_status_script_reports_late_runtime_convergence_without_authority_inversion() -> None:
    script = STATUS_SCRIPT.read_text(encoding="utf-8")

    assert "DEFAULT_PAPER_RUNTIME_LAUNCH_STATUS_FILE" in script
    assert "classify_launch_status_convergence" in script
    assert "paper_runtime_launch_status_original_classification" in script
    assert "paper_runtime_launch_status_effective_classification" in script
    assert "paper_runtime_launch_status_runtime_converged" in script
    assert "paper_runtime_launch_status_stale_failure_superseded" in script
    assert script.index("merge_paper_runtime_truth_status") < script.index("merge_paper_runtime_generation_status")


def test_launch_script_refreshes_reconciliation_before_success_and_stops_on_hard_blocks() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert "refresh_phase1_reconciliation_for_launch" in script
    assert "mgc_v05l.execution_core.track_b_paper_broker_reconciliation" in script
    assert "refresh_phase1_reconciliation_for_launch" in script
    assert "refresh_canonical_readiness_for_launch \"pre-success\"" in script
    assert "Failed to refresh Phase-1 reconciliation before claiming launch success." in script
    assert "stop_paper_runtime_best_effort" in script
    assert script.index("refresh_phase1_reconciliation_for_launch") < script.index(
        "Headless supervised paper host is READY_SUBMIT_CAPABLE."
    )


def test_launch_script_refresher_failure_is_sidecar_warning_unless_strict() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    assert "Failed to start the read-only broker-truth refresh sidecar in strict mode." in script
    assert "canonical readiness remains authoritative and will fail closed" in script
    assert "write_startup_summary \"BLOCKED\" \"Failed to start the read-only broker-truth refresh service.\"" not in script
    assert script.index("start_broker_truth_refresher") < script.index("refresh_canonical_readiness_for_launch \"post-launch\"")


def test_scripts_do_not_add_broker_order_api_calls() -> None:
    combined = "\n".join(
        [
            RUN_SCRIPT.read_text(encoding="utf-8"),
            STATUS_SCRIPT.read_text(encoding="utf-8"),
        ]
    )
    forbidden = (
        "place" + "Order",
        "cancel" + "Order",
        "req" + "GlobalCancel",
        "place" + "_order",
        "submit" + "_order",
        "broker" + ".submit",
        "broker" + ".cancel",
        "broker" + ".close",
        "close" + "Position",
    )
    assert not any(token in combined for token in forbidden)
