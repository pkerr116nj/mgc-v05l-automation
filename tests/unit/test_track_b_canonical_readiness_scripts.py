from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_SCRIPT = REPO_ROOT / "scripts" / "run_headless_supervised_paper_service.sh"
STATUS_SCRIPT = REPO_ROOT / "scripts" / "show_headless_supervised_paper_status.sh"
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
    assert "Headless supervised paper host is READY_SUBMIT_CAPABLE." in script


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
    assert "headless_runtime_wrapper_start" in script
    assert "headless_runtime_wrapper_exec" in script


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
    assert "launch_background_paper_runtime\n  return 0" in script
    assert "launchctl submit -l \"${label}\" -- /bin/bash \"${wrapper_path}\"" in script
    assert launch_function.index("rm -f \"${PAPER_WRAPPER_PID_FILE}\"") < launch_function.index(
        "launchctl_submit_available"
    )
    assert launch_function.index("launchctl_submit_available") < launch_function.index(
        "launch_detached_paper_runtime"
    )


def test_launch_script_retains_screen_only_for_dashboard_manager_not_paper_runtime() -> None:
    script = RUN_SCRIPT.read_text(encoding="utf-8")

    paper_launch = script[script.index("launch_background_paper_runtime()") : script.index("launch_screen_dashboard_manager()")]
    dashboard_launch = script[script.index("launch_screen_dashboard_manager()") : script.index("launch_detached_paper_runtime()")]

    assert "screen_available" not in paper_launch
    assert "screen -dmS" not in paper_launch
    assert "screen -dmS" in dashboard_launch


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
