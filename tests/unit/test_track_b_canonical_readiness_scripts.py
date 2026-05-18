from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_SCRIPT = REPO_ROOT / "scripts" / "run_headless_supervised_paper_service.sh"
STATUS_SCRIPT = REPO_ROOT / "scripts" / "show_headless_supervised_paper_status.sh"


def test_status_script_produces_canonical_readiness_without_dashboard_ownership() -> None:
    script = STATUS_SCRIPT.read_text(encoding="utf-8")

    assert "mgc_v05l.app.track_b_canonical_readiness" in script
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
        "placeOrder",
        "cancelOrder",
        "reqGlobalCancel",
        "place_order",
        "submit_order",
        "broker.submit",
        "broker.cancel",
        "broker.close",
        "closePosition",
    )
    assert not any(token in combined for token in forbidden)
