from __future__ import annotations

from pathlib import Path

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
    health = _eligible_health("broker_truth_refresher")
    health["broker_safety"]["unknown_open_order_count"] = 1

    plan = plan_track_b_self_healing_restarts(health=health, now=NOW)

    assert plan["classification"] == "RESTART_PLAN_BLOCKED"
    assert "unknown_open_orders" in plan["global_blockers"]
    assert plan["actions"] == ()


def test_review_required_blocks_restart() -> None:
    health = _eligible_health("operator_readiness_refresher")
    health["broker_safety"]["review_required_count"] = 1

    plan = plan_track_b_self_healing_restarts(health=health, now=NOW)

    assert "lifecycle_review_required" in plan["global_blockers"]
    assert plan["actions"] == ()


def test_live_money_blocks_restart() -> None:
    health = _eligible_health("operator_readiness_refresher")
    health["live_money_eligible"] = True

    plan = plan_track_b_self_healing_restarts(health=health, now=NOW)

    assert "live_money_eligible_true" in plan["global_blockers"]
    assert plan["actions"] == ()


def test_duplicate_conflicting_process_blocks_restart() -> None:
    health = _eligible_health("phase1_candle_supervisor")
    health["broker_safety"]["duplicate_conflicting_runtime_count"] = 1

    plan = plan_track_b_self_healing_restarts(health=health, now=NOW)

    assert "duplicate_conflicting_runtimes" in plan["global_blockers"]
    assert plan["actions"] == ()


def test_paper_runtime_is_never_auto_restarted() -> None:
    health = _eligible_health("paper_runtime")
    health["agents"]["paper_runtime"]["restart_eligible"] = True
    health["agents"]["paper_runtime"]["restart_candidate"] = True

    plan = plan_track_b_self_healing_restarts(health=health, now=NOW)

    assert plan["actions"] == ()
    assert plan["blocked"][0]["agent_id"] == "paper_runtime"
    assert "paper_runtime_auto_restart_forbidden" in plan["blocked"][0]["blockers"]


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
            "review_required_count": 0,
            "lifecycle_open_position_count": 0,
            "live_money_eligible": False,
            "duplicate_conflicting_runtime_count": 0,
        },
        "agents": {
            agent_id: {
                "display_name": agent_id,
                "health_state": "UNHEALTHY",
                "process_running": False,
                "root_ok": True,
                "restart_eligible": agent_id != "paper_runtime",
                "restart_candidate": True,
                "operator_required": False,
                "blockers": [f"{agent_id}_not_running"],
            }
        },
    }
    if agent_id == "paper_runtime":
        health["agents"][agent_id]["restart_eligible"] = False
    return health
