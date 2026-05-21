from __future__ import annotations

import subprocess
from pathlib import Path

import mgc_v05l.app.track_b_self_healing_supervisor as supervisor
from mgc_v05l.app.track_b_self_healing_supervisor import (
    evaluate_track_b_self_healing_restart_action,
    plan_track_b_self_healing_restarts,
    render_track_b_self_healing_status,
    run_track_b_self_healing_status,
)
from tests.unit.execution_core.test_track_b_self_healing_supervisor import NOW, _process_probe, _write_runtime_artifacts


def test_status_writer_writes_and_renders_summary(tmp_path: Path) -> None:
    _write_runtime_artifacts(tmp_path)
    output = tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_self_healing_health.json"

    health = run_track_b_self_healing_status(
        repo_root=tmp_path,
        expected_root=tmp_path,
        output_path=output,
        write=True,
        process_probe=_process_probe(tmp_path),
        now=NOW,
    )
    rendered = render_track_b_self_healing_status(health)

    assert output.exists()
    assert "classification=SELF_HEALING_READY" in rendered
    assert "broker_truth_refresher: health=HEALTHY" in rendered
    assert "auto_restart_allowed=false" in rendered
    assert health["generated_at"]


def test_dry_run_reports_restart_without_mutation(tmp_path: Path) -> None:
    health = _eligible_health("broker_truth_refresher")
    audit = tmp_path / "audit.jsonl"
    calls: list[tuple[str, ...]] = []

    action = evaluate_track_b_self_healing_restart_action(
        health=health,
        repo_root=tmp_path,
        mode="dry-run",
        audit_path=audit,
        now=NOW,
        command_runner=lambda command, root: calls.append(tuple(command)) or {"returncode": 0},
    )

    assert action["plan"]["classification"] == "RESTART_PLAN_READY"
    assert action["plan"]["actions"][0]["agent_id"] == "broker_truth_refresher"
    assert calls == []
    assert not audit.exists()


def test_apply_restarts_eligible_sidecar_and_writes_audit(tmp_path: Path) -> None:
    health = _eligible_health("broker_truth_refresher")
    audit = tmp_path / "audit.jsonl"
    calls: list[tuple[str, ...]] = []

    action = evaluate_track_b_self_healing_restart_action(
        health=health,
        repo_root=tmp_path,
        mode="apply",
        audit_path=audit,
        now=NOW,
        command_runner=lambda command, root: calls.append(tuple(command)) or {"command": list(command), "returncode": 0},
    )

    assert action["attempt"]["classification"] == "RESTART_APPLIED"
    assert calls == [
        ("bash", "scripts/stop-track-b-broker-truth-refresh"),
        ("bash", "scripts/start-track-b-broker-truth-refresh"),
    ]
    assert '"classification": "RESTART_APPLIED"' in audit.read_text(encoding="utf-8")


def test_open_orders_block_restart(tmp_path: Path) -> None:
    health = _eligible_health("paper_runtime")
    health["broker_safety"]["unknown_open_order_count"] = 1

    plan = plan_track_b_self_healing_restarts(health=health, now=NOW)

    assert plan["classification"] == "RESTART_PLAN_BLOCKED"
    assert "unknown_open_orders" in plan["global_blockers"]
    assert plan["actions"] == ()


def test_review_required_blocks_restart() -> None:
    health = _eligible_health("paper_runtime")
    health["broker_safety"]["review_required_count"] = 1

    plan = plan_track_b_self_healing_restarts(health=health, now=NOW)

    assert "lifecycle_review_required" in plan["global_blockers"]
    assert plan["actions"] == ()


def test_live_money_blocks_restart() -> None:
    health = _eligible_health("paper_runtime")
    health["live_money_eligible"] = True

    plan = plan_track_b_self_healing_restarts(health=health, now=NOW)

    assert "live_money_eligible_true" in plan["global_blockers"]
    assert plan["actions"] == ()


def test_duplicate_conflicting_process_blocks_restart() -> None:
    health = _eligible_health("paper_runtime")
    health["broker_safety"]["duplicate_conflicting_runtime_count"] = 1

    plan = plan_track_b_self_healing_restarts(health=health, now=NOW)

    assert "duplicate_conflicting_runtimes" in plan["global_blockers"]
    assert plan["actions"] == ()


def test_dead_paper_runtime_with_clean_broker_state_is_restart_eligible() -> None:
    health = _eligible_health("paper_runtime")

    plan = plan_track_b_self_healing_restarts(health=health, now=NOW)

    assert plan["classification"] == "RESTART_PLAN_READY"
    assert plan["paper_runtime_auto_restart_allowed"] is True
    assert plan["actions"][0]["agent_id"] == "paper_runtime"
    start_command = plan["actions"][0]["commands"][1]
    assert start_command[0] == "env"
    assert start_command[1].startswith("MGC_HEADLESS_SUPERVISED_PAPER_CONFIG_PATHS=")
    assert start_command[2].startswith("MGC_HEADLESS_REQUIRED_PAPER_CONFIGS=")
    assert start_command[3] == "bash"
    assert "probationary_pattern_engine_paper_mnq_mgc_plus_mnq_us_intraday_review.yaml" in start_command[1]
    assert "probationary_pattern_engine_paper_mnq_mgc_plus_mnq_us_intraday_review.yaml" in start_command[2]
    assert start_command[4:] == (
        "scripts/run_headless_supervised_paper_service.sh",
        "--no-start-dashboard",
        "--wait-timeout-seconds",
        "120",
        "--post-start-pid-wait-timeout-seconds",
        "45",
    )


def test_candidate_health_blocker_does_not_block_its_own_restart() -> None:
    health = _eligible_health("paper_runtime")
    health["blockers"] = ["paper_runtime_not_running"]

    plan = plan_track_b_self_healing_restarts(health=health, now=NOW)

    assert plan["classification"] == "RESTART_PLAN_READY"
    assert plan["global_blockers"] == ()
    assert plan["actions"][0]["agent_id"] == "paper_runtime"


def test_apply_restarts_paper_runtime_through_supervised_launcher(tmp_path: Path) -> None:
    health = _eligible_health("paper_runtime")
    audit = tmp_path / "audit.jsonl"
    calls: list[tuple[str, ...]] = []

    action = evaluate_track_b_self_healing_restart_action(
        health=health,
        repo_root=tmp_path,
        mode="apply",
        audit_path=audit,
        now=NOW,
        command_runner=lambda command, root: calls.append(tuple(command)) or {"command": list(command), "returncode": 0},
    )

    assert action["attempt"]["classification"] == "RESTART_APPLIED"
    assert calls[0] == ("bash", "scripts/stop_probationary_paper_soak.sh")
    assert calls[1][0] == "env"
    assert calls[1][3:6] == ("bash", "scripts/run_headless_supervised_paper_service.sh", "--no-start-dashboard")
    assert "submit" not in " ".join(" ".join(call) for call in calls).lower()
    assert "cancel" not in " ".join(" ".join(call) for call in calls).lower()
    assert "close" not in " ".join(" ".join(call) for call in calls).lower()
    assert "flatten" not in " ".join(" ".join(call) for call in calls).lower()


def test_apply_continues_when_paper_stop_finds_no_pid_file(tmp_path: Path) -> None:
    health = _eligible_health("paper_runtime")
    audit = tmp_path / "audit.jsonl"
    calls: list[tuple[str, ...]] = []

    def runner(command, root):
        calls.append(tuple(command))
        if any(str(part).endswith("stop_probationary_paper_soak.sh") for part in command):
            return {
                "command": list(command),
                "returncode": 1,
                "stdout_tail": "No probationary paper PID file found at /tmp/probationary_paper.pid.",
                "stderr_tail": "",
            }
        return {"command": list(command), "returncode": 0}

    action = evaluate_track_b_self_healing_restart_action(
        health=health,
        repo_root=tmp_path,
        mode="apply",
        audit_path=audit,
        now=NOW,
        command_runner=runner,
    )

    assert action["attempt"]["classification"] == "RESTART_APPLIED"
    assert calls[0] == ("bash", "scripts/stop_probationary_paper_soak.sh")
    assert calls[1][0] == "env"
    assert calls[1][3:6] == ("bash", "scripts/run_headless_supervised_paper_service.sh", "--no-start-dashboard")


def test_stop_only_no_pid_audit_row_does_not_poison_runtime_retry_policy() -> None:
    health = _eligible_health("paper_runtime")
    audit = [
        {
            "generated_at": NOW.isoformat(),
            "results": [
                {
                    "agent_id": "paper_runtime",
                    "classification": "RESTART_FAILED",
                    "failure_class": "COMMAND_FAILED",
                    "commands": [
                        {
                            "command": ["bash", "scripts/stop_probationary_paper_soak.sh"],
                            "returncode": 1,
                            "stdout_tail": "No probationary paper PID file found at /tmp/probationary_paper.pid.",
                            "stderr_tail": "",
                        }
                    ],
                }
            ],
        }
    ]

    plan = plan_track_b_self_healing_restarts(health=health, audit_entries=audit, now=NOW)

    assert plan["classification"] == "RESTART_PLAN_READY"
    assert plan["retry_policy"]["paper_runtime"]["classification"] == "RESTART_ALLOWED"
    assert plan["retry_policy"]["paper_runtime"]["retry_attempt_count"] == 0


def test_restart_command_timeout_is_recorded_not_raised(tmp_path: Path, monkeypatch) -> None:
    launch_status = tmp_path / "outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper_launch_status.json"
    launch_status.parent.mkdir(parents=True)
    launch_status.write_text('{"classification": "RUNTIME_PID_AVAILABLE", "launchctl_exit_code": 0}\n', encoding="utf-8")

    def raise_timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=kwargs.get("timeout"), output="warming", stderr="")

    monkeypatch.setattr(supervisor.subprocess, "run", raise_timeout)

    result = supervisor._run_restart_command(
        (
            "bash",
            "scripts/run_headless_supervised_paper_service.sh",
            "--no-start-dashboard",
        ),
        tmp_path,
    )

    assert result["classification"] == "RESTART_COMMAND_TIMEOUT"
    assert result["returncode"] == 124
    assert result["launch_status_classification"] == "RUNTIME_PID_AVAILABLE"


def test_unresolved_ownership_blocks_runtime_restart() -> None:
    health = _eligible_health("paper_runtime")
    health["broker_safety"]["unresolved_submit_intent_ownership_count"] = 1

    plan = plan_track_b_self_healing_restarts(health=health, now=NOW)

    assert plan["actions"] == ()
    assert "unresolved_submit_ownership" in plan["global_blockers"]


def test_broker_reconciliation_mismatch_blocks_runtime_restart() -> None:
    health = _eligible_health("paper_runtime")
    health["broker_safety"]["classification"] = "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    health["broker_safety"]["broker_reconciled"] = False

    plan = plan_track_b_self_healing_restarts(health=health, now=NOW)

    assert plan["actions"] == ()
    assert "broker_reconciliation_mismatch" in plan["global_blockers"]


def test_invalid_broker_lease_blocks_runtime_restart() -> None:
    health = _eligible_health("paper_runtime")
    health["broker_safety"]["broker_truth_lease_state"] = "EXPIRED_BLOCK_NEW_ENTRIES"

    plan = plan_track_b_self_healing_restarts(health=health, now=NOW)

    assert plan["actions"] == ()
    assert "broker_truth_lease_not_active" in plan["blocked"][0]["blockers"]


def test_restart_cooldown_blocks_repeat_attempt(tmp_path: Path) -> None:
    health = _eligible_health("broker_truth_refresher")
    audit = [
        {
            "generated_at": NOW.isoformat(),
            "results": [{"agent_id": "broker_truth_refresher", "classification": "RESTART_SUCCEEDED"}],
        }
    ]

    plan = plan_track_b_self_healing_restarts(health=health, audit_entries=audit, now=NOW)

    assert plan["actions"] == ()
    assert "restart_cooldown_active" in plan["blocked"][0]["blockers"]


def test_launchctl_submit_failure_is_retryable_once_when_broker_clean() -> None:
    health = _eligible_health("paper_runtime")
    audit = [
        {
            "generated_at": NOW.isoformat(),
            "results": [
                {
                    "agent_id": "paper_runtime",
                    "classification": "RESTART_FAILED",
                    "failure_class": "LAUNCHCTL_SUBMIT_FAILED",
                }
            ],
        }
    ]

    plan = plan_track_b_self_healing_restarts(health=health, audit_entries=audit, now=NOW)

    assert plan["classification"] == "RESTART_PLAN_READY"
    assert plan["actions"][0]["agent_id"] == "paper_runtime"
    assert plan["retry_policy_classification"] == "RESTART_RETRY_IMMEDIATE"
    assert plan["retry_policy"]["paper_runtime"]["failure_class"] == "LAUNCHCTL_SUBMIT_FAILED"


def test_repeated_launchctl_submit_failure_exhausts_budget() -> None:
    health = _eligible_health("paper_runtime")
    audit = [
        {
            "generated_at": NOW.isoformat(),
            "results": [
                {
                    "agent_id": "paper_runtime",
                    "classification": "RESTART_FAILED",
                    "failure_class": "LAUNCHCTL_SUBMIT_FAILED",
                }
            ],
        }
        for _ in range(3)
    ]

    plan = plan_track_b_self_healing_restarts(health=health, audit_entries=audit, now=NOW)

    assert plan["actions"] == ()
    assert plan["retry_policy_classification"] == "RESTART_BUDGET_EXHAUSTED"
    assert "restart_max_attempts_exceeded" in plan["blocked"][0]["blockers"]


def test_runtime_launch_retry_cooldown_blocks_hammering_after_second_failure() -> None:
    health = _eligible_health("paper_runtime")
    audit = [
        {
            "generated_at": NOW.isoformat(),
            "results": [
                {
                    "agent_id": "paper_runtime",
                    "classification": "RESTART_FAILED",
                    "failure_class": "RUNTIME_PID_UNAVAILABLE_AFTER_LAUNCHCTL_SUBMIT",
                }
            ],
        }
    ]

    plan = plan_track_b_self_healing_restarts(health=health, audit_entries=audit, now=NOW)

    assert plan["actions"] == ()
    assert plan["retry_policy_classification"] == "RESTART_COOLDOWN_ACTIVE"
    assert "restart_cooldown_active" in plan["blocked"][0]["blockers"]


def test_successful_runtime_start_resets_launch_failure_budget() -> None:
    health = _eligible_health("paper_runtime")
    audit = [
        {
            "generated_at": NOW.isoformat(),
            "results": [
                {
                    "agent_id": "paper_runtime",
                    "classification": "RESTART_SUCCEEDED",
                    "failure_class": None,
                }
            ],
        }
    ]

    plan = plan_track_b_self_healing_restarts(health=health, audit_entries=audit, now=NOW)

    assert plan["classification"] == "RESTART_PLAN_READY"
    assert plan["retry_policy"]["paper_runtime"]["classification"] == "RESTART_ALLOWED"
    assert plan["retry_policy"]["paper_runtime"]["failure_class"] is None


def test_broker_unsafe_state_prevents_launch_retry_even_after_retryable_failure() -> None:
    health = _eligible_health("paper_runtime")
    health["broker_safety"]["unknown_open_order_count"] = 1
    audit = [
        {
            "generated_at": NOW.isoformat(),
            "results": [
                {
                    "agent_id": "paper_runtime",
                    "classification": "RESTART_FAILED",
                    "failure_class": "LAUNCHCTL_SUBMIT_FAILED",
                }
            ],
        }
    ]

    plan = plan_track_b_self_healing_restarts(health=health, audit_entries=audit, now=NOW)

    assert plan["actions"] == ()
    assert "unknown_open_orders" in plan["global_blockers"]


def test_duplicate_writer_prevents_launch_retry_even_after_retryable_failure() -> None:
    health = _eligible_health("paper_runtime")
    health["broker_safety"]["duplicate_conflicting_runtime_count"] = 1
    audit = [
        {
            "generated_at": NOW.isoformat(),
            "results": [
                {
                    "agent_id": "paper_runtime",
                    "classification": "RESTART_FAILED",
                    "failure_class": "LAUNCHCTL_SUBMIT_FAILED",
                }
            ],
        }
    ]

    plan = plan_track_b_self_healing_restarts(health=health, audit_entries=audit, now=NOW)

    assert plan["actions"] == ()
    assert "duplicate_conflicting_runtimes" in plan["global_blockers"]


def _eligible_health(agent_id: str) -> dict:
    health = {
        "classification": "AUTO_RESTART_ELIGIBLE",
        "restart_candidates": [agent_id],
        "blockers": [],
        "warnings": [],
        "live_money_eligible": False,
        "broker_safety": {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "unknown_open_order_count": 0,
            "track_b_broker_open_order_count": 0,
            "track_b_broker_position_count": 0,
            "review_required_count": 0,
            "lifecycle_open_position_count": 0,
            "unresolved_submit_intent_ownership_count": 0,
            "broker_truth_lease_state": "ACTIVE",
            "live_money_eligible": False,
            "duplicate_conflicting_runtime_count": 0,
        },
        "agents": {
            "phase1_candle_supervisor": {
                "display_name": "phase1_candle_supervisor",
                "health_state": "HEALTHY",
                "process_running": True,
                "root_ok": True,
                "restart_eligible": True,
                "restart_candidate": False,
                "operator_required": False,
                "blockers": [],
            },
            agent_id: {
                "display_name": agent_id,
                "health_state": "UNHEALTHY",
                "process_running": False,
                "root_ok": True,
                "restart_eligible": True,
                "restart_candidate": True,
                "operator_required": False,
                "blockers": [f"{agent_id}_not_running"],
            }
        },
    }
    return health
