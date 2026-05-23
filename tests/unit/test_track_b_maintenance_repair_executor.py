from __future__ import annotations

import inspect
import json
import sys
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


def seed_artifacts(
    repo_root: Path,
    *,
    actions: list[str] | None = None,
    state: str = "DEGRADED",
    shared_truth_generated_at: str = "2026-05-18T13:59:00+00:00",
    open_order_truth_classification: str = "NO_OPEN_ORDERS",
    managed_order_registry_classification: str = "NO_MANAGED_ORDERS",
    position_truth_classification: str = "CLEAN_FLAT_READY",
    managed_position_registry_classification: str = "NO_MANAGED_POSITIONS",
    runtime_supervisor_classification: str = "SUPERVISOR_NO_ACTION_NEEDED",
) -> None:
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
            "broker_truth_lease": {
                "lease_state": "ACTIVE_DEGRADED_REFRESH_FAILING",
                "live_money_eligible": False,
                "blockers": [],
            },
        },
    )
    seed_shared_truth(
        repo_root,
        generated_at=shared_truth_generated_at,
        open_order_truth_classification=open_order_truth_classification,
        managed_order_registry_classification=managed_order_registry_classification,
        position_truth_classification=position_truth_classification,
        managed_position_registry_classification=managed_position_registry_classification,
        runtime_supervisor_classification=runtime_supervisor_classification,
    )


def seed_shared_truth(
    repo_root: Path,
    *,
    generated_at: str = "2026-05-18T13:59:00+00:00",
    open_order_truth_classification: str = "NO_OPEN_ORDERS",
    managed_order_registry_classification: str = "NO_MANAGED_ORDERS",
    position_truth_classification: str = "CLEAN_FLAT_READY",
    managed_position_registry_classification: str = "NO_MANAGED_POSITIONS",
    runtime_supervisor_classification: str = "SUPERVISOR_NO_ACTION_NEEDED",
    self_recover_recommendation: str = "NO_ACTION_NEEDED",
    crash_loop_classification: str = "NO_CRASH_LOOP",
    runtime_resume_classification: str = "RESUME_ALLOWED_CLEAN",
) -> None:
    write_json(
        repo_root / "outputs" / "track_b_execution_core" / "runtime_supervisor" / "latest_runtime_supervisor_authority.json",
        {"classification": runtime_supervisor_classification, "generated_at": generated_at},
    )
    write_json(
        repo_root / "outputs" / "track_b_execution_core" / "self_recover" / "latest_self_recover_rules.json",
        {"classification": self_recover_recommendation, "recommendation": self_recover_recommendation, "generated_at": generated_at},
    )
    write_json(
        repo_root / "outputs" / "track_b_execution_core" / "crash_loop_protection" / "latest_crash_loop_protection.json",
        {"classification": crash_loop_classification, "generated_at": generated_at},
    )
    write_json(
        repo_root / "outputs" / "track_b_execution_core" / "runtime_resume" / "latest_runtime_resume_semantics.json",
        {"classification": runtime_resume_classification, "generated_at": generated_at},
    )
    write_json(
        repo_root / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json",
        {"classification": open_order_truth_classification, "generated_at": generated_at, "order_states": []},
    )
    write_json(
        repo_root / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json",
        {"classification": managed_order_registry_classification, "generated_at": generated_at, "managed_orders": []},
    )
    write_json(
        repo_root / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {
            "classification": position_truth_classification,
            "generated_at": generated_at,
            "position_states": [],
            "summary": {"overall_classification": position_truth_classification},
        },
    )
    write_json(
        repo_root / "outputs" / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json",
        {"classification": managed_position_registry_classification, "generated_at": generated_at, "managed_positions": []},
    )
    write_json(
        repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        {"classification": "TRACK_B_PAPER_BROKER_RECONCILED", "generated_at": generated_at},
    )
    write_json(
        repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json",
        {"classification": "ACTIVE", "lease_state": "ACTIVE", "generated_at": generated_at},
    )


def config(repo_root: Path, *, apply: bool = False, cooldown_seconds: int = 300, max_retries: int = 2) -> RepairExecutorConfig:
    return RepairExecutorConfig(
        repo_root=repo_root,
        apply=apply,
        cooldown_seconds=cooldown_seconds,
        max_retries=max_retries,
        python_bin="python-test",
    )


def phase1_config(repo_root: Path, *, apply: bool = False, cooldown_seconds: int = 300, max_retries: int = 2) -> RepairExecutorConfig:
    return RepairExecutorConfig(
        repo_root=repo_root,
        action="phase1-reconciliation",
        apply=apply,
        cooldown_seconds=cooldown_seconds,
        max_retries=max_retries,
        python_bin="python-test",
    )


def default_python_config(repo_root: Path, *, apply: bool = False) -> RepairExecutorConfig:
    return RepairExecutorConfig(repo_root=repo_root, apply=apply, max_retries=2)


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


def test_apply_canonical_rerun_uses_current_interpreter_not_bare_python(tmp_path: Path, monkeypatch) -> None:
    seed_artifacts(tmp_path, actions=["RESTART_SIDECAR"])
    monkeypatch.delenv("PYTHON_BIN", raising=False)
    commands: list[list[str]] = []

    def runner(command, **kwargs):  # type: ignore[no-untyped-def]
        commands.append(list(command))
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    result = run_repair_executor(
        config=default_python_config(tmp_path, apply=True),
        command_runner=runner,
        pid_checker=lambda pid: False,
        now_fn=now,
    )

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_APPLIED"
    assert commands[1][:3] == [sys.executable, "-m", "mgc_v05l.app.track_b_canonical_readiness"]
    assert commands[1][0] != "python"


def test_default_python_bin_prefers_repo_venv(tmp_path: Path, monkeypatch) -> None:
    seed_artifacts(tmp_path, actions=["REFRESH_BROKER_TRUTH"])
    monkeypatch.delenv("PYTHON_BIN", raising=False)
    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True)
    venv_python.write_text("# placeholder\n", encoding="utf-8")
    (tmp_path / "var").mkdir()
    (tmp_path / "var" / "track_b_broker_truth_refresh_service.pid").write_text("123\n", encoding="utf-8")

    result = run_repair_executor(
        config=default_python_config(tmp_path),
        pid_checker=lambda pid: True,
        now_fn=now,
    )

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_DRY_RUN_READY"
    assert result["commands"][0][:3] == [str(venv_python), "-m", "mgc_v05l.app.ibkr_broker_truth_refresher"]
    assert result["commands"][0][0] != "python"


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


def test_phase1_reconciliation_dry_run_recommends_refresh(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["REFRESH_RECONCILIATION"])

    result = run_repair_executor(config=phase1_config(tmp_path), pid_checker=lambda pid: True, now_fn=now)

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_DRY_RUN_READY"
    assert result["repair_plan_classification"] == "REPAIR_PLAN_READY"
    assert result["action"] == "phase1-reconciliation"
    assert result["repair_action"] == "REFRESH_PHASE1_RECONCILIATION"
    assert result["would_execute"] is True
    assert result["proposed_actions"][0]["repair_action"] == "REFRESH_PHASE1_RECONCILIATION"
    assert result["commands"][0][:3] == ["python-test", "-m", "mgc_v05l.execution_core.track_b_paper_broker_reconciliation"]
    assert "--repo-root" in result["commands"][0]
    assert result["authority"]["runtime_restart_allowed"] is False


def test_executor_blocks_when_shared_truth_stale(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["REFRESH_RECONCILIATION"], shared_truth_generated_at="2026-05-18T13:00:00+00:00")

    result = run_repair_executor(config=phase1_config(tmp_path), pid_checker=lambda pid: True, now_fn=now)

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_DRY_RUN_BLOCKED"
    assert result["repair_plan_classification"] == "REPAIR_PLAN_BLOCKED_SHARED_TRUTH"
    assert "stale/missing" in result["blocked_reason"]
    assert result["would_execute"] is False


def test_executor_blocks_suspicious_order_state(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["REFRESH_RECONCILIATION"], open_order_truth_classification="SUSPICIOUS_ORDER_STATE")

    result = run_repair_executor(config=phase1_config(tmp_path), pid_checker=lambda pid: True, now_fn=now)

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_DRY_RUN_BLOCKED"
    assert "SUSPICIOUS_ORDER_STATE" in result["blocked_reason"]
    assert result["blocked_actions"][0]["repair_action"] == "REFRESH_PHASE1_RECONCILIATION"


def test_executor_blocks_runtime_supervisor_manual_review(tmp_path: Path) -> None:
    seed_artifacts(
        tmp_path,
        actions=["REFRESH_RECONCILIATION"],
        runtime_supervisor_classification="SUPERVISOR_MANUAL_REVIEW_REQUIRED",
    )

    result = run_repair_executor(config=phase1_config(tmp_path), pid_checker=lambda pid: True, now_fn=now)

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_DRY_RUN_BLOCKED"
    assert "SUPERVISOR_MANUAL_REVIEW_REQUIRED" in result["blocked_reason"]


def test_executor_requires_explicit_operator_authorization_for_apply(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["REFRESH_RECONCILIATION"])
    commands: list[list[str]] = []

    def runner(command, **kwargs):  # type: ignore[no-untyped-def]
        commands.append(list(command))
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    result = run_repair_executor(
        config=phase1_config(tmp_path, apply=True),
        command_runner=runner,
        pid_checker=lambda pid: True,
        now_fn=now,
    )

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_APPLIED"
    assert result["requires_operator_authorization"] is True
    assert commands


def test_executor_does_not_consume_dashboard_projections_as_authority() -> None:
    source = inspect.getsource(executor)

    assert "latest_track_b_open_order_truth.json" not in source
    assert "latest_track_b_managed_orders.json" not in source
    assert "latest_track_b_position_truth.json" not in source
    assert "latest_track_b_managed_positions.json" not in source
    assert "latest_track_b_runtime_supervisor_authority.json" not in source


def test_phase1_reconciliation_apply_uses_safe_python_and_reruns_readiness(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["REFRESH_RECONCILIATION"])
    commands: list[list[str]] = []

    def runner(command, **kwargs):  # type: ignore[no-untyped-def]
        commands.append(list(command))
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    result = run_repair_executor(
        config=phase1_config(tmp_path, apply=True),
        command_runner=runner,
        pid_checker=lambda pid: True,
        now_fn=now,
    )

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_APPLIED"
    assert commands[0][:3] == ["python-test", "-m", "mgc_v05l.execution_core.track_b_paper_broker_reconciliation"]
    assert commands[1][:3] == ["python-test", "-m", "mgc_v05l.app.track_b_canonical_readiness"]
    assert result["canonical_readiness_rerun"]["returncode"] == 0
    latest = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_maintenance_repair_result.json"
    history = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "maintenance_repair_history.jsonl"
    assert json.loads(latest.read_text(encoding="utf-8"))["action"] == "phase1-reconciliation"
    assert json.loads(history.read_text(encoding="utf-8").splitlines()[-1])["repair_action"] == "REFRESH_PHASE1_RECONCILIATION"


def test_phase1_reconciliation_apply_failure_blocks_success(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["REFRESH_RECONCILIATION"])

    def runner(command, **kwargs):  # type: ignore[no-untyped-def]
        return SimpleNamespace(returncode=1, stdout="", stderr="reconciliation dirty")

    result = run_repair_executor(
        config=phase1_config(tmp_path, apply=True),
        command_runner=runner,
        pid_checker=lambda pid: True,
        now_fn=now,
    )

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_APPLY_FAILED"
    assert result["executed"] is True
    assert result["canonical_readiness_rerun"] is None


def test_phase1_reconciliation_operator_required_blocks_apply(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["REFRESH_RECONCILIATION"], state="OPERATOR_REQUIRED")

    result = run_repair_executor(config=phase1_config(tmp_path, apply=True), pid_checker=lambda pid: True, now_fn=now)

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_APPLY_BLOCKED"
    assert "OPERATOR_REQUIRED" in result["blocked_reason"]
    assert result["executed"] is False


def test_phase1_reconciliation_live_money_blocks_apply(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["REFRESH_RECONCILIATION"])
    canonical_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
    canonical = json.loads(canonical_path.read_text(encoding="utf-8"))
    canonical["live_money_eligible"] = True
    write_json(canonical_path, canonical)

    result = run_repair_executor(config=phase1_config(tmp_path, apply=True), pid_checker=lambda pid: True, now_fn=now)

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_APPLY_BLOCKED"
    assert "Live-money" in result["blocked_reason"]
    assert result["executed"] is False


def test_phase1_reconciliation_invalid_lease_blocks_apply(tmp_path: Path) -> None:
    seed_artifacts(tmp_path, actions=["REFRESH_RECONCILIATION"])
    canonical_path = tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
    canonical = json.loads(canonical_path.read_text(encoding="utf-8"))
    canonical["broker_truth_lease"]["lease_state"] = "INVALIDATED_UNKNOWN_OPEN_ORDERS"
    write_json(canonical_path, canonical)

    result = run_repair_executor(config=phase1_config(tmp_path, apply=True), pid_checker=lambda pid: True, now_fn=now)

    assert result["classification"] == "TRACK_B_MAINTENANCE_REPAIR_APPLY_BLOCKED"
    assert "INVALIDATED_UNKNOWN_OPEN_ORDERS" in result["blocked_reason"]
    assert result["executed"] is False


def test_cli_defaults_to_dry_run(tmp_path: Path, capsys) -> None:
    seed_artifacts(tmp_path, actions=["RESTART_SIDECAR"], shared_truth_generated_at=datetime.now(timezone.utc).isoformat())

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
