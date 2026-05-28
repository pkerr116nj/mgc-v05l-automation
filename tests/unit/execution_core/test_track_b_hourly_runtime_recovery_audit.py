from __future__ import annotations

from datetime import UTC, datetime

from mgc_v05l.execution_core.track_b_hourly_runtime_recovery_audit import (
    CONTROL_PLANE_REAL_BLOCK,
    RECOVERY_POLICY_TOO_PASSIVE,
    RUNTIME_HEALTHY_NO_ACTION,
    SUPERVISOR_NOT_INSTALLED,
    build_hourly_runtime_recovery_audit,
)


NOW = datetime(2026, 5, 27, 7, 15, tzinfo=UTC)


def test_runtime_missing_clean_authority_recommends_restart_but_uninstalled_hourly_supervisor_is_root_cause() -> None:
    payload = build_hourly_runtime_recovery_audit(
        self_healing_health=_health(runtime_running=False, restart_allowed=True),
        scheduler_evidence={"launchd_matching_labels": [], "crontab_matching_entries": []},
        now=NOW,
    )

    assert payload["classification"] == SUPERVISOR_NOT_INSTALLED
    assert payload["hourly_supervisor"]["installed"] is False
    assert payload["restart_decision"]["classification"] == "RUNTIME_RESTART_RECOMMENDED_BY_EXISTING_SELF_HEALING_PLAN"
    assert payload["broker_mutation_allowed"] is False
    assert payload["paper_proof_invoked"] is False


def test_installed_supervisor_with_restart_action_ready_is_too_passive() -> None:
    payload = build_hourly_runtime_recovery_audit(
        self_healing_health=_health(runtime_running=False, restart_allowed=True),
        scheduler_evidence={"launchd_matching_labels": ["123\t0\tcom.mgc_v05l.track_b_hourly_runtime_recovery"], "crontab_matching_entries": []},
        now=NOW,
    )

    assert payload["classification"] == RECOVERY_POLICY_TOO_PASSIVE
    assert payload["hourly_supervisor"]["running"] is True


def test_dirty_broker_or_control_plane_blocks_restart() -> None:
    payload = build_hourly_runtime_recovery_audit(
        self_healing_health=_health(runtime_running=False, restart_allowed=False, blockers=["unknown_open_orders"]),
        scheduler_evidence={"launchd_matching_labels": ["123\t0\tcom.mgc_v05l.track_b_hourly_runtime_recovery"], "crontab_matching_entries": []},
        now=NOW,
    )

    assert payload["classification"] == CONTROL_PLANE_REAL_BLOCK
    assert payload["restart_decision"]["classification"] == "RUNTIME_RESTART_NOT_ALLOWED"
    assert "unknown_open_orders" in payload["restart_decision"]["blockers"]


def test_canonical_operability_stale_truth_blocks_hourly_recovery_restart() -> None:
    payload = build_hourly_runtime_recovery_audit(
        self_healing_health=_health(runtime_running=False, restart_allowed=True),
        scheduler_evidence={
            "launchd_matching_labels": ["123\t0\tcom.mgc_v05l.track_b_hourly_runtime_recovery"],
            "crontab_matching_entries": [],
        },
        operability_contract={
            "canonical_state": "BLOCKED_STALE_TRUTH",
            "restart_allowed_if_runtime_down": False,
            "blocker_family": "BLOCKED_STALE_TRUTH",
            "blockers": [{"code": "authority_artifact_stale"}],
        },
        now=NOW,
    )

    assert payload["classification"] == "STALE_RUNTIME_AUTHORITY"
    assert payload["restart_decision"]["classification"] == "RUNTIME_RESTART_NOT_ALLOWED_BY_CANONICAL_OPERABILITY"
    assert payload["canonical_operability"]["restart_allowed_if_runtime_down"] is False


def test_healthy_runtime_is_no_action_when_supervisor_running() -> None:
    payload = build_hourly_runtime_recovery_audit(
        self_healing_health=_health(runtime_running=True, restart_allowed=False),
        scheduler_evidence={"launchd_matching_labels": ["123\t0\tcom.mgc_v05l.track_b_hourly_runtime_recovery"], "crontab_matching_entries": []},
        now=NOW,
    )

    assert payload["classification"] == RUNTIME_HEALTHY_NO_ACTION
    assert payload["restart_decision"]["classification"] == "RUNTIME_HEALTHY_NO_RESTART_NEEDED"


def test_active_codex_automation_counts_as_hourly_supervisor() -> None:
    payload = build_hourly_runtime_recovery_audit(
        self_healing_health=_health(runtime_running=True, restart_allowed=False),
        scheduler_evidence={
            "launchd_matching_labels": [],
            "crontab_matching_entries": [],
            "codex_automation_matching_entries": [
                {
                    "id": "track-b-hourly-paper-runtime-recovery",
                    "name": "Track B hourly PAPER runtime recovery",
                    "status": "ACTIVE",
                    "rrule": "FREQ=HOURLY;INTERVAL=1;BYMINUTE=0;BYSECOND=0",
                }
            ],
        },
        now=NOW,
    )

    assert payload["classification"] == RUNTIME_HEALTHY_NO_ACTION
    assert payload["hourly_supervisor"]["installed"] is True
    assert payload["hourly_supervisor"]["running"] is True


def test_unrelated_launchd_labels_do_not_count_as_hourly_recovery() -> None:
    payload = build_hourly_runtime_recovery_audit(
        self_healing_health=_health(runtime_running=True, restart_allowed=False),
        scheduler_evidence={
            "launchd_matching_labels": [],
            "crontab_matching_entries": [],
            "raw_examples": [
                "711\t0\tcom.apple.wallpaper.agent",
                "91838\t-9\tcom.apple.mlruntimed",
                "-\t1\tcom.mgc_v05l.track_b_sunday_preflight",
            ],
        },
        now=NOW,
    )

    assert payload["classification"] == SUPERVISOR_NOT_INSTALLED


def _health(*, runtime_running: bool, restart_allowed: bool, blockers: list[str] | None = None) -> dict:
    blockers = blockers or []
    return {
        "classification": "SELF_HEALING_READY" if runtime_running else "AUTO_RESTART_ELIGIBLE",
        "runtime_restart_eligible": restart_allowed,
        "runtime_restart_blockers": blockers,
        "restart_candidates": [] if runtime_running else ["paper_runtime"],
        "agents": {
            "paper_runtime": {
                "health_state": "HEALTHY" if runtime_running else "UNHEALTHY",
                "process_running": runtime_running,
                "restart_candidate": not runtime_running,
                "blockers": [] if runtime_running else ["paper_runtime_not_running"],
            }
        },
        "last_restart_plan": {
            "classification": "RESTART_PLAN_READY" if restart_allowed else "RESTART_PLAN_BLOCKED",
            "restart_allowed": restart_allowed,
            "paper_runtime_auto_restart_allowed": restart_allowed,
            "global_blockers": blockers,
            "blocked": [{"agent_id": "paper_runtime", "blockers": blockers}] if blockers else [],
        },
    }
