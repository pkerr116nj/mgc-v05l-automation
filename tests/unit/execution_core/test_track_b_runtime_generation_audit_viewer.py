from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_runtime_generation_audit_viewer import (
    RUNTIME_GENERATION_AUDIT_INCONSISTENT,
    RUNTIME_GENERATION_AUDIT_MISSING_GENERATION,
    RUNTIME_GENERATION_AUDIT_NO_EVENTS,
    RUNTIME_GENERATION_AUDIT_READY,
    TrackBRuntimeGenerationAuditViewerConfig,
    build_track_b_runtime_generation_audit,
    write_track_b_runtime_generation_audit,
)


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_missing_generation_id(tmp_path: Path) -> None:
    payload = build_track_b_runtime_generation_audit(
        config=TrackBRuntimeGenerationAuditViewerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == RUNTIME_GENERATION_AUDIT_MISSING_GENERATION
    assert payload["runtime_generation_id"] == ""
    assert payload["read_only"] is True


def test_no_events_for_requested_generation(tmp_path: Path) -> None:
    payload = build_track_b_runtime_generation_audit(
        config=TrackBRuntimeGenerationAuditViewerConfig(repo_root=tmp_path, runtime_generation_id="runtime-gen-1"),
        now=NOW,
    )

    assert payload["classification"] == RUNTIME_GENERATION_AUDIT_NO_EVENTS
    assert payload["runtime_generation_id"] == "runtime-gen-1"
    assert payload["control_plane_decision_chain"] == []
    assert "No matching events" in payload["operator_summary"]


def test_clean_generation_with_one_control_plane_snapshot(tmp_path: Path) -> None:
    _seed_control_plane(tmp_path, generation_id="runtime-gen-1")
    _seed_clean_truth(tmp_path)

    payload = build_track_b_runtime_generation_audit(
        config=TrackBRuntimeGenerationAuditViewerConfig(repo_root=tmp_path, runtime_generation_id="runtime-gen-1"),
        now=NOW,
    )

    assert payload["classification"] == RUNTIME_GENERATION_AUDIT_READY
    assert payload["generation_start"] == NOW.isoformat()
    assert payload["generation_end"] == NOW.isoformat()
    assert payload["control_plane_decision_chain"][0]["control_plane_snapshot_id"] == "snapshot-1"
    assert payload["final_state"]["position_truth_classification"] == "CLEAN_FLAT_READY"
    assert payload["unresolved_questions"] == []


def test_generation_with_recovery_attempt(tmp_path: Path) -> None:
    _seed_control_plane(tmp_path, generation_id="runtime-gen-1")
    _seed_clean_truth(tmp_path)
    event = {
        "generated_at": "2026-05-23T12:01:00+00:00",
        "recovery_attempt_id": "attempt-1",
        "runtime_generation_id": "runtime-gen-1",
        "classification": "EXECUTOR_DRY_RUN_READY",
        "action_type": "RUNTIME_RETRY",
        "recovery_budget_key": "track_b_paper_runtime|RUNTIME_RETRY|test",
        "execution_enabled": False,
    }
    _write_jsonl(
        tmp_path
        / "outputs/track_b_execution_core/paper_autonomous_recovery/executor_attempts/paper_autonomous_recovery_executor_events.jsonl",
        [event],
    )

    payload = build_track_b_runtime_generation_audit(
        config=TrackBRuntimeGenerationAuditViewerConfig(repo_root=tmp_path, runtime_generation_id="runtime-gen-1"),
        now=NOW,
    )

    assert payload["classification"] == RUNTIME_GENERATION_AUDIT_READY
    assert payload["recovery_events"][0]["event_id"] == "attempt-1"
    assert payload["recovery_events"][0]["action_type"] == "RUNTIME_RETRY"
    assert payload["generation_end"] == "2026-05-23T12:01:00+00:00"


def test_inconsistent_broker_lifecycle_state(tmp_path: Path) -> None:
    _seed_control_plane(tmp_path, generation_id="runtime-gen-1")
    _seed_clean_truth(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "OPEN_MANAGED_POSITIONS",
            "managed_positions": [{"runtime_generation_id": "runtime-gen-1", "classification": "OPEN_MANAGED"}],
            "live_money_eligible": False,
        },
    )

    payload = build_track_b_runtime_generation_audit(
        config=TrackBRuntimeGenerationAuditViewerConfig(repo_root=tmp_path, runtime_generation_id="runtime-gen-1"),
        now=NOW,
    )

    assert payload["classification"] == RUNTIME_GENERATION_AUDIT_INCONSISTENT
    assert payload["unresolved_questions"][0]["code"] == "position_truth_managed_position_mismatch"
    assert "inconsistent" in payload["operator_summary"]


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    _seed_control_plane(tmp_path, generation_id="runtime-gen-1")
    _seed_clean_truth(tmp_path)
    _write(
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_control_plane_snapshot.json",
        {
            "projection_only": True,
            "not_routing_authority": True,
            "runtime_resume_proposed_next_runtime_generation_id": "projection-gen",
            "control_plane_snapshot_id": "projection-snapshot",
        },
    )

    payload = build_track_b_runtime_generation_audit(
        config=TrackBRuntimeGenerationAuditViewerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["runtime_generation_id"] == "runtime-gen-1"
    assert payload["dashboard_projection_consumed"] is False
    assert "operator_dashboard" not in json.dumps(payload["source_artifact_paths"])


def test_write_json_and_markdown_report(tmp_path: Path) -> None:
    _seed_control_plane(tmp_path, generation_id="runtime-gen-1")
    _seed_clean_truth(tmp_path)
    config = TrackBRuntimeGenerationAuditViewerConfig(repo_root=tmp_path, runtime_generation_id="runtime-gen-1")
    payload = build_track_b_runtime_generation_audit(config=config, now=NOW)

    json_path, markdown_path = write_track_b_runtime_generation_audit(config=config, payload=payload)

    assert json_path == tmp_path / "outputs/track_b_execution_core/runtime_generation_audit/latest_runtime_generation_audit.json"
    assert markdown_path == tmp_path / "outputs/reports/runtime_generation_audit/latest_runtime_generation_audit.md"
    assert json.loads(json_path.read_text(encoding="utf-8"))["classification"] == RUNTIME_GENERATION_AUDIT_READY
    assert "# Track B Runtime Generation Audit" in markdown_path.read_text(encoding="utf-8")  # type: ignore[union-attr]


def _seed_control_plane(root: Path, *, generation_id: str) -> None:
    _write(
        root / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "generated_at": NOW.isoformat(),
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "shared-truth-1",
            "runtime_resume_proposed_next_runtime_generation_id": generation_id,
            "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "supervisor_mode": "READY_FOR_OPERATOR_START",
            "runtime_resume_action_policy": "NEW_RUNTIME_GENERATION_ALLOWED",
            "safe_state_classification": "SAFE_STATE_NORMAL",
            "safe_to_start_runtime": True,
            "top_line_classification": "READY_FOR_OPERATOR_START",
            "operator_explanation": "Ready for supervised runtime start.",
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/runtime_resume/latest_runtime_resume_semantics.json",
        {
            "generated_at": NOW.isoformat(),
            "proposed_next_runtime_generation_id": generation_id,
            "previous_source_commit": "test-head",
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json",
        {
            "generated_at": NOW.isoformat(),
            "runtime_generation_id": generation_id,
            "safe_state_classification": "SAFE_STATE_NORMAL",
            "classification": "SAFE_STATE_NORMAL",
            "observe_only": False,
            "recovery_only": False,
            "tripped_limits": [],
            "live_money_eligible": False,
        },
    )


def _seed_clean_truth(root: Path) -> None:
    _write(
        root / "outputs/track_b_execution_core/position_truth/latest_position_truth.json",
        {"generated_at": NOW.isoformat(), "classification": "CLEAN_FLAT_READY", "live_money_eligible": False},
    )
    _write(
        root / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {"generated_at": NOW.isoformat(), "classification": "NO_OPEN_ORDERS", "order_states": [], "live_money_eligible": False},
    )
    _write(
        root / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        {"generated_at": NOW.isoformat(), "classification": "NO_MANAGED_ORDERS", "managed_orders": [], "live_money_eligible": False},
    )
    _write(
        root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {"generated_at": NOW.isoformat(), "classification": "NO_MANAGED_POSITIONS", "managed_positions": [], "live_money_eligible": False},
    )
    _write(
        root / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "live_money_eligible": False,
        },
    )


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
