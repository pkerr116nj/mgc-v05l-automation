from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_paper_autonomous_recovery_executor import (
    BUDGET_GATE_BLOCKED_COOLDOWN,
    BUDGET_GATE_BLOCKED_EXHAUSTED,
    BUDGET_GATE_BLOCKED_MISSING,
    BUDGET_GATE_BLOCKED_QUARANTINE,
    BUDGET_GATE_PASS,
    EXECUTOR_BLOCKED_BUDGET_EXHAUSTED,
    EXECUTOR_BLOCKED_PRE_ACTION_VALIDATION,
    EXECUTOR_DRY_RUN_READY,
    TrackBPaperAutonomousRecoveryExecutorConfig,
    build_track_b_paper_autonomous_recovery_executor_attempt,
    run_track_b_paper_autonomous_recovery_executor_dry_run,
)
from mgc_v05l.execution_core.track_b_recovery_budget_ledger import target_identity_hash
from mgc_v05l.execution_core.track_b_pre_action_snapshot_validator import (
    PRE_ACTION_BLOCKED_HARD_INVARIANT,
    PRE_ACTION_BLOCKED_SNAPSHOT_STALE,
    PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH,
    PRE_ACTION_SNAPSHOT_VALID,
)


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)
TARGET = {"symbol": "MGC", "contract": "MGCM6", "quantity": 1, "action": "SELL"}


def test_valid_coherent_snapshot_and_matching_plan_creates_dry_run_attempt(tmp_path: Path) -> None:
    _seed_valid(tmp_path, action_type="RUNTIME_RETRY", plan_classification="PLAN_RUNTIME_RETRY")

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_DRY_RUN_READY
    assert payload["execution_enabled"] is False
    assert payload["control_plane_snapshot_id"] == "snapshot-1"
    assert payload["shared_truth_generation_id"] == "generation-1"
    assert payload["pre_action_validation"]["classification"] == PRE_ACTION_SNAPSHOT_VALID
    assert payload["would_mutate_runtime"] is True
    assert payload["would_mutate_broker"] is False
    assert payload["action_adapter"]["adapter_name"] == "RUNTIME_RETRY_DISABLED_V1"
    assert payload["action_adapter"]["adapter_enabled"] is False
    assert payload["action_adapter"]["execution_enabled"] is False
    assert payload["action_adapter"]["blocked_reason"] == "ADAPTER_DISABLED"
    assert payload["action_adapter"]["budget_gate_classification"] == BUDGET_GATE_PASS
    assert payload["action_adapter"]["attempts_remaining"] == 1
    assert payload["action_adapter"]["budget_exhausted"] is False
    assert payload["action_adapter"]["would_execute_command"][0] == "bash"
    assert payload["action_adapter"]["launch_command_exists"] is True


def test_runtime_retry_adapter_stays_disabled_when_placeholder_flag_is_set(tmp_path: Path) -> None:
    _seed_valid(tmp_path, action_type="RUNTIME_RETRY", plan_classification="PLAN_RUNTIME_RETRY")

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path, enable_runtime_retry_adapter=True),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_DRY_RUN_READY
    assert payload["action_adapter"]["enable_runtime_retry_adapter_requested"] is True
    assert payload["action_adapter"]["adapter_enabled"] is False
    assert payload["action_adapter"]["execution_enabled"] is False
    assert payload["action_adapter"]["blocked_reason"] == "ADAPTER_DISABLED_PENDING_SECOND_ENABLE_FLAG"


def test_runtime_retry_budget_exhausted_blocks_before_adapter_disabled(tmp_path: Path) -> None:
    _seed_valid(
        tmp_path,
        action_type="RUNTIME_RETRY",
        plan_classification="PLAN_RUNTIME_RETRY",
        budget_attempts_remaining=0,
        budget_exhausted=True,
    )

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_BLOCKED_BUDGET_EXHAUSTED
    assert payload["action_adapter"]["budget_gate_classification"] == BUDGET_GATE_BLOCKED_EXHAUSTED
    assert payload["action_adapter"]["blocked_reason"] == BUDGET_GATE_BLOCKED_EXHAUSTED
    assert payload["action_adapter"]["adapter_enabled"] is False
    assert payload["action_adapter"]["execution_enabled"] is False


def test_runtime_retry_budget_cooldown_blocks_before_adapter_disabled(tmp_path: Path) -> None:
    _seed_valid(
        tmp_path,
        action_type="RUNTIME_RETRY",
        plan_classification="PLAN_RUNTIME_RETRY",
        budget_cooldown_until=datetime(2026, 5, 23, 12, 5, tzinfo=UTC),
    )

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_BLOCKED_BUDGET_EXHAUSTED
    assert payload["action_adapter"]["budget_gate_classification"] == BUDGET_GATE_BLOCKED_COOLDOWN
    assert payload["action_adapter"]["blocked_reason"] == BUDGET_GATE_BLOCKED_COOLDOWN


def test_runtime_retry_budget_quarantine_blocks_before_adapter_disabled(tmp_path: Path) -> None:
    _seed_valid(
        tmp_path,
        action_type="RUNTIME_RETRY",
        plan_classification="PLAN_RUNTIME_RETRY",
        budget_quarantine_required=True,
    )

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_BLOCKED_BUDGET_EXHAUSTED
    assert payload["action_adapter"]["budget_gate_classification"] == BUDGET_GATE_BLOCKED_QUARANTINE
    assert payload["action_adapter"]["blocked_reason"] == BUDGET_GATE_BLOCKED_QUARANTINE


def test_runtime_retry_missing_budget_ledger_blocks_before_adapter_disabled(tmp_path: Path) -> None:
    _seed_valid(
        tmp_path,
        action_type="RUNTIME_RETRY",
        plan_classification="PLAN_RUNTIME_RETRY",
        include_budget_ledger=False,
    )

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_BLOCKED_BUDGET_EXHAUSTED
    assert payload["action_adapter"]["budget_gate_classification"] == BUDGET_GATE_BLOCKED_MISSING
    assert payload["action_adapter"]["blocked_reason"] == BUDGET_GATE_BLOCKED_MISSING


def test_runtime_retry_adapter_blocks_when_snapshot_start_not_allowed(tmp_path: Path) -> None:
    _seed_valid(
        tmp_path,
        action_type="RUNTIME_RETRY",
        plan_classification="PLAN_RUNTIME_RETRY",
        snapshot_safe_to_start_runtime=False,
    )

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_DRY_RUN_READY
    assert payload["action_adapter"]["blocked_reason"] == "SNAPSHOT_START_NOT_ALLOWED"
    assert payload["action_adapter"]["execution_enabled"] is False


def test_market_data_restart_dry_run_can_plan_but_not_execute(tmp_path: Path) -> None:
    _seed_valid(
        tmp_path,
        action_type="MARKET_DATA_RESTART",
        plan_classification="PLAN_MARKET_DATA_RESTART",
        target_identity={"agent_id": "phase1_databento_live_candles"},
    )

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_MARKET_DATA_RESTART",
        action_type="MARKET_DATA_RESTART",
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_DRY_RUN_READY
    assert payload["would_mutate_runtime"] is True
    assert payload["execution_enabled"] is False
    assert payload["action_adapter"]["adapter_name"] == "NO_ADAPTER_BOUNDARY_V1"
    assert payload["action_adapter"]["execution_enabled"] is False


def test_plan_action_mismatch_blocks_before_adapter_enablement(tmp_path: Path) -> None:
    _seed_valid(tmp_path, action_type="QUARANTINE_OBSERVE_ONLY", plan_classification="PLAN_QUARANTINE_OBSERVE_ONLY")

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_BLOCKED_PRE_ACTION_VALIDATION
    assert payload["action_adapter"]["blocked_reason"] == "PRE_ACTION_VALIDATION_BLOCKED"


def test_validator_blocked_state_blocks_executor_attempt(tmp_path: Path) -> None:
    _seed_valid(tmp_path, include_snapshot=False)

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_BLOCKED_PRE_ACTION_VALIDATION
    assert payload["pre_action_validation"]["valid"] is False
    assert payload["action_adapter"]["blocked_reason"] == "PRE_ACTION_VALIDATION_BLOCKED"
    assert payload["action_adapter"]["budget_gate_classification"] == BUDGET_GATE_PASS


def test_stale_snapshot_blocks_executor_attempt(tmp_path: Path) -> None:
    _seed_valid(tmp_path, snapshot_generated_at=datetime(2026, 5, 23, 11, 0, tzinfo=UTC))

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        max_snapshot_age_seconds=60,
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_BLOCKED_PRE_ACTION_VALIDATION
    assert payload["pre_action_validation"]["classification"] == PRE_ACTION_BLOCKED_SNAPSHOT_STALE


def test_target_identity_mismatch_blocks_executor_attempt(tmp_path: Path) -> None:
    _seed_valid(
        tmp_path,
        action_type="SCOPED_POSITION_CLEANUP",
        plan_classification="PLAN_SCOPED_POSITION_CLEANUP",
        target_identity=TARGET,
    )

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_SCOPED_POSITION_CLEANUP",
        action_type="SCOPED_POSITION_CLEANUP",
        target_identity={**TARGET, "contract": "MNQM6"},
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_BLOCKED_PRE_ACTION_VALIDATION
    assert payload["pre_action_validation"]["classification"] == PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH


def test_budget_exhausted_blocks_executor_attempt(tmp_path: Path) -> None:
    _seed_valid(tmp_path, action_type="RUNTIME_RETRY", plan_classification="PLAN_RUNTIME_RETRY")
    config = TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path, max_attempts_per_target=1)
    first = run_track_b_paper_autonomous_recovery_executor_dry_run(
        config=config,
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    second = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=config,
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert first["classification"] == EXECUTOR_DRY_RUN_READY
    assert second["classification"] == EXECUTOR_BLOCKED_BUDGET_EXHAUSTED
    assert second["budget"]["budget_exhausted"] is True


def test_live_money_hard_block_flows_from_validator(tmp_path: Path) -> None:
    _seed_valid(tmp_path, live_money_eligible=True)

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_BLOCKED_PRE_ACTION_VALIDATION
    assert payload["pre_action_validation"]["classification"] == PRE_ACTION_BLOCKED_HARD_INVARIANT
    assert payload["action_adapter"]["blocked_reason"] == "PRE_ACTION_VALIDATION_BLOCKED"


def test_duplicate_writer_hard_block_flows_from_validator(tmp_path: Path) -> None:
    _seed_valid(tmp_path, duplicate_writer_count=1)

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_BLOCKED_PRE_ACTION_VALIDATION
    assert payload["pre_action_validation"]["classification"] == PRE_ACTION_BLOCKED_HARD_INVARIANT
    assert payload["action_adapter"]["blocked_reason"] == "PRE_ACTION_VALIDATION_BLOCKED"


def test_audit_artifacts_are_written(tmp_path: Path) -> None:
    _seed_valid(tmp_path, action_type="RUNTIME_RETRY", plan_classification="PLAN_RUNTIME_RETRY")

    result = run_track_b_paper_autonomous_recovery_executor_dry_run(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    attempt_path = Path(result["written_artifacts"]["attempt"])
    latest_path = Path(result["written_artifacts"]["latest"])
    event_log_path = Path(result["written_artifacts"]["event_log"])
    assert attempt_path.exists()
    assert latest_path.exists()
    assert event_log_path.exists()
    assert json.loads(latest_path.read_text(encoding="utf-8"))["recovery_attempt_id"] == result["recovery_attempt_id"]
    assert json.loads(latest_path.read_text(encoding="utf-8"))["action_adapter"]["would_execute_command"][0] == "bash"
    rows = [json.loads(line) for line in event_log_path.read_text(encoding="utf-8").splitlines()]
    assert rows[-1]["recovery_attempt_id"] == result["recovery_attempt_id"]
    assert rows[-1]["execution_enabled"] is False


def test_dashboard_projection_is_not_consumed(tmp_path: Path) -> None:
    _seed_valid(tmp_path, include_snapshot=False)
    _write_json(
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_control_plane_snapshot.json",
        {"control_plane_snapshot_id": "projection-only", "shared_truth_coherence_status": "COHERENT"},
    )

    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=TrackBPaperAutonomousRecoveryExecutorConfig(repo_root=tmp_path),
        expected_plan_classification="PLAN_RUNTIME_RETRY",
        action_type="RUNTIME_RETRY",
        now=NOW,
    )

    assert payload["classification"] == EXECUTOR_BLOCKED_PRE_ACTION_VALIDATION
    assert "operator_dashboard" not in json.dumps(payload["pre_action_validation"]["source_artifact_paths"])


def test_runtime_retry_adapter_has_no_process_execution_path() -> None:
    source = Path("src/mgc_v05l/execution_core/track_b_paper_autonomous_recovery_executor.py").read_text(
        encoding="utf-8"
    )

    assert "subprocess" not in source
    assert "Popen" not in source
    assert "os.system" not in source


def _seed_valid(
    root: Path,
    *,
    include_snapshot: bool = True,
    snapshot_generated_at: datetime = NOW,
    action_type: str = "RUNTIME_RETRY",
    plan_classification: str = "PLAN_RUNTIME_RETRY",
    target_identity: dict | None = None,
    live_money_eligible: bool = False,
    duplicate_writer_count: int = 0,
    snapshot_safe_to_start_runtime: bool = True,
    include_budget_ledger: bool = True,
    budget_attempts_remaining: int = 1,
    budget_attempts_used: int = 0,
    budget_exhausted: bool = False,
    budget_quarantine_required: bool = False,
    budget_cooldown_until: datetime | None = None,
) -> None:
    launcher_path = root / "scripts" / "run_headless_supervised_paper_service.sh"
    launcher_path.parent.mkdir(parents=True, exist_ok=True)
    launcher_path.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    if include_snapshot:
        _write_json(
            root / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
            {
                "generated_at": snapshot_generated_at.isoformat(),
                "control_plane_snapshot_id": "snapshot-1",
                "shared_truth_refresh_generation_id": "generation-1",
                "shared_truth_coherence_status": "COHERENT",
                "runtime_supervisor_decision_id": "supervisor-1",
                "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
                "safe_to_start_runtime": snapshot_safe_to_start_runtime,
                "duplicate_writer_count": duplicate_writer_count,
                "live_money_eligible": live_money_eligible,
            },
        )
    _write_json(
        root / "outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json",
        {
            "generated_at": NOW.isoformat(),
            "supervisor_decision_id": "supervisor-1",
            "classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "duplicate_writer_count": duplicate_writer_count,
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": plan_classification,
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
            "execution_enabled": False,
            "live_money_eligible": live_money_eligible,
            "proposed_actions": [
                {
                    "action_id": action_type.lower(),
                    "action_type": action_type,
                    "target_identity": target_identity or {},
                    "execution_enabled": False,
                }
            ],
        },
    )
    if include_budget_ledger:
        normalized_target = {
            str(key): str(value)
            for key, value in (target_identity or {}).items()
            if value not in (None, "")
        }
        _write_json(
            root / "outputs/track_b_execution_core/recovery_budget/latest_recovery_budget_ledger.json",
            {
                "generated_at": NOW.isoformat(),
                "classification": "RECOVERY_BUDGET_EXHAUSTED" if budget_exhausted else "RECOVERY_BUDGET_AVAILABLE",
                "budget_exhausted": budget_exhausted,
                "quarantine_required": budget_quarantine_required,
                "entries": [
                    {
                        "budget_key": "track_b_paper_runtime|RUNTIME_RETRY|test|*|runtime_retry",
                        "agent_id": "track_b_paper_runtime",
                        "action_type": "RUNTIME_RETRY",
                        "target_identity": normalized_target,
                        "target_identity_hash": target_identity_hash(normalized_target),
                        "attempts_used": budget_attempts_used,
                        "attempts_remaining": budget_attempts_remaining,
                        "budget_exhausted": budget_exhausted,
                        "cooldown_until": None if budget_cooldown_until is None else budget_cooldown_until.isoformat(),
                        "quarantine_required": budget_quarantine_required,
                    }
                ],
                "summary": {
                    "budget_exhausted": budget_exhausted,
                    "quarantine_required": budget_quarantine_required,
                    "minimum_attempts_remaining": budget_attempts_remaining,
                },
                "artifact_paths": {
                    "authority": str(
                        root
                        / "outputs"
                        / "track_b_execution_core"
                        / "recovery_budget"
                        / "latest_recovery_budget_ledger.json"
                    )
                },
            },
        )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
