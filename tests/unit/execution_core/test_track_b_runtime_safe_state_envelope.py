from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_runtime_safe_state_envelope import (
    SAFE_STATE_BROKER_MUTATION_LIMIT_HIT,
    SAFE_STATE_HARD_HOLD,
    SAFE_STATE_LIFECYCLE_DISAGREEMENT_LIMIT_HIT,
    SAFE_STATE_NORMAL,
    SAFE_STATE_POSITION_LIMIT_HIT,
    TrackBRuntimeSafeStateEnvelopeConfig,
    build_track_b_runtime_safe_state_envelope,
    write_track_b_runtime_safe_state_envelope,
)


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_clean_state_is_normal(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_NORMAL
    assert payload["broker_mutation_allowed"] is True
    assert payload["runtime_start_allowed"] is True
    assert payload["submit_allowed"] is True
    assert payload["observe_only"] is False
    assert payload["recovery_only"] is False
    assert payload["tripped_limits"] == []


def test_live_money_true_hard_holds(tmp_path: Path) -> None:
    _seed_base(tmp_path, live_money_eligible=True)

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_HARD_HOLD
    assert payload["runtime_start_allowed"] is False
    assert payload["submit_allowed"] is False
    assert payload["broker_mutation_allowed"] is False
    assert any(row["limit_id"] == "live_money_eligible" for row in payload["tripped_limits"])


def test_duplicate_writer_hard_holds(tmp_path: Path) -> None:
    _seed_base(tmp_path, duplicate_writer=True)

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_HARD_HOLD
    assert payload["observe_only"] is True
    assert any(row["limit_id"] == "duplicate_runtime_writer" for row in payload["tripped_limits"])


def test_broker_position_guardian_hard_hold_blocks_submit(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/broker_position_guardian/latest_broker_position_guardian.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "BROKER_POSITION_GUARDIAN_HARD_HOLD",
            "hard_classifications": ["UNAUTHORIZED_REVERSE_EXPOSURE"],
            "operator_explanation": "Unauthorized reverse exposure detected.",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_HARD_HOLD
    assert payload["submit_allowed"] is False
    assert payload["broker_mutation_allowed"] is False
    assert any(row["limit_id"] == "broker_position_guardian_hard_hold" for row in payload["tripped_limits"])


def test_too_many_submits_hits_broker_mutation_limit(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/strategy_bridge/latest_strategy_bridge_submit_report.json",
        {
            "generated_at": NOW.isoformat(),
            "submits_per_symbol_window": {"MGC": 4},
            "broker_mutation_attempts_per_window": 4,
            "failed_broker_mutations_per_window": 0,
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_BROKER_MUTATION_LIMIT_HIT
    assert payload["recovery_only"] is True
    assert payload["submit_allowed"] is False
    assert any(row["limit_id"] == "submits_per_symbol_per_window" for row in payload["tripped_limits"])


def test_lifecycle_disagreements_shift_to_observe_only(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/lifecycle_state/latest_lifecycle_state_summary.json",
        {
            "generated_at": NOW.isoformat(),
            "consecutive_reconciliation_disagreement_count": 3,
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_LIFECYCLE_DISAGREEMENT_LIMIT_HIT
    assert payload["observe_only"] is True
    assert payload["broker_mutation_allowed"] is False


def test_position_limit_hit_hard_holds(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "OPEN_MANAGED_POSITIONS",
            "managed_positions": [
                {"strategy_lane_id": "lane-1", "classification": "OPEN_MANAGED"},
                {"strategy_lane_id": "lane-1", "classification": "OPEN_MANAGED"},
            ],
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_POSITION_LIMIT_HIT
    assert payload["runtime_start_allowed"] is False
    assert any(row["limit_id"] == "managed_open_positions_per_strategy_lane" for row in payload["tripped_limits"])


def test_dashboard_projection_is_not_consumed(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/operator_dashboard/runtime/latest_runtime_safe_state_envelope.json",
        {"safe_state_classification": SAFE_STATE_HARD_HOLD, "projection_only": True},
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_NORMAL
    assert "operator_dashboard" not in json.dumps(payload["source_artifact_paths"])


def test_write_authority_artifact(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    config = TrackBRuntimeSafeStateEnvelopeConfig(repo_root=tmp_path)
    payload = build_track_b_runtime_safe_state_envelope(config=config, now=NOW)

    path = write_track_b_runtime_safe_state_envelope(config=config, payload=payload)

    assert path == tmp_path / "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json"
    written = json.loads(path.read_text(encoding="utf-8"))
    assert written["safe_state_classification"] == SAFE_STATE_NORMAL


def _build(root: Path) -> dict:
    return build_track_b_runtime_safe_state_envelope(
        config=TrackBRuntimeSafeStateEnvelopeConfig(repo_root=root),
        now=NOW,
    )


def _seed_base(root: Path, *, live_money_eligible: bool = False, duplicate_writer: bool = False) -> None:
    _write(
        root / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "generated_at": NOW.isoformat(),
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
            "safe_to_start_runtime": True,
            "runtime_resume_proposed_next_runtime_generation_id": "runtime-generation-next",
            "agent_health_has_duplicate_writer": duplicate_writer,
            "duplicate_process_count": 1 if duplicate_writer else 0,
            "live_money_eligible": live_money_eligible,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/runtime_resume/latest_runtime_resume_semantics.json",
        {
            "generated_at": NOW.isoformat(),
            "proposed_next_runtime_generation_id": "runtime-generation-next",
            "previous_runtime_generation_id": "runtime-generation-previous",
            "live_money_eligible": live_money_eligible,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/recovery_budget/latest_recovery_budget_ledger.json",
        {"generated_at": NOW.isoformat(), "classification": "RECOVERY_BUDGET_AVAILABLE"},
    )
    _write(
        root / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_recovery_attempt_history.json",
        {"generated_at": NOW.isoformat(), "recent_attempts": []},
    )
    _write(
        root / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "NO_MANAGED_ORDERS",
            "managed_orders": [],
            "summary": {"managed_order_count": 0},
            "live_money_eligible": live_money_eligible,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "NO_OPEN_ORDERS",
            "order_states": [],
            "live_money_eligible": live_money_eligible,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/position_truth/latest_position_truth.json",
        {"generated_at": NOW.isoformat(), "classification": "CLEAN_FLAT_READY", "live_money_eligible": live_money_eligible},
    )
    _write(
        root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "NO_MANAGED_POSITIONS",
            "managed_positions": [],
            "live_money_eligible": live_money_eligible,
        },
    )
    _write(
        root / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {"generated_at": NOW.isoformat(), "classification": "TRACK_B_PAPER_BROKER_RECONCILED", "live_money_eligible": live_money_eligible},
    )


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
