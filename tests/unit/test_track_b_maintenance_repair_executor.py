from __future__ import annotations

import inspect
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.app import track_b_maintenance_repair_executor as executor
from mgc_v05l.app.track_b_maintenance_repair_executor import RepairExecutorConfig, run_repair_executor


def now() -> datetime:
    return datetime(2026, 5, 18, 14, 0, tzinfo=timezone.utc)


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def seed_artifacts(repo_root: Path, *, actions: list[str] | None = None, state: str = "DEGRADED") -> None:
    write_json(
        repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_maintenance_supervisor_decision.json",
        {
            "generated_at": "2026-05-18T13:59:00+00:00",
            "supervisor_state": state,
            "recommended_actions": actions if actions is not None else ["RESTART_SIDECAR"],
            "canonical_readiness": "NOT_READY_DEPENDENCY",
            "blockers": [],
            "live_money_eligible": False,
        },
    )
    write_json(
        repo_root / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json",
        {
            "classification": "BROKER_TRUTH_REFRESH_STALE",
            "fresh": False,
            "last_failure": True,
            "positions_complete": True,
            "open_orders_complete": True,
            "live_money_eligible": False,
        },
    )
    write_json(
        repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json",
        {
            "canonical_readiness": "NOT_READY_DEPENDENCY",
            "live_money_eligible": False,
        },
    )


def config(repo_root: Path, *, apply: bool = False, cooldown_seconds: int = 300, max_retries: int = 2) -> RepairExecutorConfig:
    return RepairExecutorConfig(
        repo_root=repo_root,
        apply=apply,
        cooldown_seconds=cooldown_seconds,
        max_retries=max_retries,
        python_bin="python-test",
    )


def test_dry_run_dead_sidecar_recommends_restart(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["RESTART_SIDECAR"])

    result = run_repair_executor(config=config(tmp_path), pid_checker=lambda pid: False, now_fn=now)

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_DRY_RUN_READY"
    assert result["repair_action"] == "START_BROKER_TRUTH_SIDECAR"
    assert result["would_execute"] is True
    assert result["executed"] is False
    assert result["commands"][0][0] == "bash"


def test_apply_starts_sidecar_using_safe_command_mock(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["RESTART_SIDECAR"])
    commands: list[list[str]] = []

    def runner(command, **kwargs):  # type: ignore[no-untyped-def]
        commands.append(list(command))
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    result = run_repair_executor(
        config=config(tmp_path, apply=True),
        command_runner=runner,
        pid_checker=lambda pid: False,
        now_fn=now,
    )

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_APPLIED"
    assert result["repair_action"] == "START_BROKER_TRUTH_SIDECAR"
    assert commands[0] == ["bash", str(tmp_path / "scripts" / "start-track-b-broker-truth-refresh")]
    assert commands[1][:3] == ["python-test", "-m", "mgc_v05l.app.track_b_canonical_readiness"]
    assert result["canonical_readiness_rerun"]["returncode"] == 0


def test_live_sidecar_with_no_relevant_recommendation_has_no_action(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["NO_ACTION"], state="RECOVERED")
    (tmp_path / "var").mkdir()
    (tmp_path / "var" / "track_b_broker_truth_refresh_service.pid").write_text("123\n", encoding="utf-8")

    result = run_repair_executor(config=config(tmp_path), pid_checker=lambda pid: True, now_fn=now)

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_DRY_RUN_NO_ACTION"
    assert result["repair_action"] == "NO_ACTION"
    assert result["would_execute"] is False


def test_alive_stale_sidecar_refreshes_once_instead_of_restart(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["REFRESH_BROKER_TRUTH", "RETRY"])
    (tmp_path / "var").mkdir()
    (tmp_path / "var" / "track_b_broker_truth_refresh_service.pid").write_text("123\n", encoding="utf-8")

    result = run_repair_executor(config=config(tmp_path), pid_checker=lambda pid: True, now_fn=now)

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_DRY_RUN_READY"
    assert result["repair_action"] == "REFRESH_BROKER_TRUTH_ONCE"
    command = result["commands"][0]
    assert command[:3] == ["python-test", "-m", "mgc_v05l.app.ibkr_broker_truth_refresher"]
    assert "--once" in command
    assert "--read-only" in command


def test_wrong_root_blocks_apply(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["RESTART_SIDECAR"])
    decision_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_maintenance_supervisor_decision.json"
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    decision["canonical_readiness"] = "NOT_READY_WRONG_ROOT"
    decision["blockers"] = [{"code": "wrong_root_process"}]
    write_json(decision_path, decision)

    result = run_repair_executor(config=config(tmp_path, apply=True), pid_checker=lambda pid: False, now_fn=now)

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_APPLY_BLOCKED"
    assert "Wrong-root" in result["blocked_reason"]
    assert result["executed"] is False


def test_operator_required_blocks_apply(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["RESTART_SIDECAR"], state="OPERATOR_REQUIRED")

    result = run_repair_executor(config=config(tmp_path, apply=True), pid_checker=lambda pid: False, now_fn=now)

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_APPLY_BLOCKED"
    assert "OPERATOR_REQUIRED" in result["blocked_reason"]
    assert result["executed"] is False


def test_cooldown_prevents_rapid_loop(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["RESTART_SIDECAR"])
    write_json(
        tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_maintenance_repair_result.json",
        {
            "classification": "TRACK_B_MAINTENANCE_REPAIR_APPLIED",
            "cooldown_until": (now() + timedelta(seconds=60)).isoformat(),
            "retry_count": 1,
        },
    )

    result = run_repair_executor(config=config(tmp_path, apply=True), pid_checker=lambda pid: False, now_fn=now)

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_APPLY_BLOCKED"
    assert "cooldown" in result["blocked_reason"]
    assert result["executed"] is False


def test_history_artifact_appended(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["RESTART_SIDECAR"])

    run_repair_executor(config=config(tmp_path), pid_checker=lambda pid: False, now_fn=now)
    run_repair_executor(config=config(tmp_path), pid_checker=lambda pid: False, now_fn=now)

    history = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "maintenance_repair_history.jsonl"
    rows = [json.loads(line) for line in history.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 2
    assert rows[0]["classification"] == "TRACK_B_MAINTENANCE_REPAIR_DRY_RUN_READY"


def test_cli_defaults_to_dry_run(tmp_path: Path, capsys) -> None:
    seed_artifacts(tmp_path, actions=["RESTART_SIDECAR"])

    exit_code = executor.main(["--repo-root", str(tmp_path), "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["mode"] == "dry_run"
    assert payload["executed"] is False


def test_executor_source_has_no_broker_order_apis() -> None:
    source = inspect.getsource(executor)
    forbidden = (
        "placeOrder",
        "cancelOrder",
        "globalCancel",
        "reqGlobalCancel",
        "place_order",
        "submit_order",
        "broker.submit",
        "broker.cancel",
        "broker.close",
        "closePosition",
    )
    assert not any(token in source for token in forbidden)
    assert '"order_api_allowed": False' in source
    assert '"broker_mutation_allowed": False' in source
