from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_truth_snapshot import (
    AGGREGATE_PLACEHOLDER_DIAGNOSTIC_ONLY,
    BROKER_TRUTH_CONFLICT_REVIEW_REQUIRED,
    BROKER_TRUTH_STALE,
    CONTRACT_ENTRY_BLOCKED,
    CONTRACT_ENTRY_CLOSE_ONLY,
    CONTROL_PLANE_STALE,
    EXACT_LIFECYCLE_OWNER_RESOLVED,
    FILL_NOT_BROKER_BACKED,
    LOCAL_ARTIFACT_NOT_AUTHORITY,
    PLANNER_SNAPSHOT_MISMATCH,
    RECOVERY_ACTIVE,
    RECOVERY_PAUSED,
    SAFE_STATE_RUNTIME_AUTHORITY_CONFLICT,
    SAFE_STATE_SUBMIT_BLOCKED,
    TRUTH_CONFLICT_REVIEW_REQUIRED,
    TRUTH_SNAPSHOT_OK,
    TrackBTruthSnapshotConfig,
    build_track_b_truth_snapshot,
)


NOW = datetime(2026, 5, 31, 12, 0, tzinfo=UTC)


def test_broker_truth_beats_local_artifact_for_position_facts(tmp_path: Path) -> None:
    config = _seed_clean(tmp_path)
    _write_json(
        tmp_path / config.local_paper_artifact_path,
        {
            "generated_at": NOW.isoformat(),
            "paper_position_rows": [{"symbol": "MNQ", "position": 1}],
        },
    )

    snapshot = build_track_b_truth_snapshot(config=config, now=NOW)

    assert snapshot.broker_truth.broker_position_count == 0
    assert snapshot.lifecycle.open_position_count == 0
    assert snapshot.classification == TRUTH_SNAPSHOT_OK
    assert snapshot.diagnostic_sources[0].diagnostic_only is True


def test_stale_artifact_cannot_authorize_submit(tmp_path: Path) -> None:
    config = _seed_clean(tmp_path)
    _write_json(
        tmp_path / config.broker_status_path,
        {
            "generated_at": (NOW - timedelta(minutes=10)).isoformat(),
            "positions_snapshot_path": str(tmp_path / config.broker_positions_path),
            "open_orders_snapshot_path": str(tmp_path / config.broker_open_orders_path),
        },
    )

    snapshot = build_track_b_truth_snapshot(config=config, now=NOW)

    assert snapshot.runtime.submit_capable is True
    assert snapshot.broker_truth.fresh is False
    assert BROKER_TRUTH_STALE in snapshot.reason_codes
    assert snapshot.classification == TRUTH_CONFLICT_REVIEW_REQUIRED


def test_local_paper_row_without_perm_or_exec_is_not_broker_backed(tmp_path: Path) -> None:
    config = _seed_clean(tmp_path)
    _write_json(tmp_path / config.broker_backed_evidence_path, {"generated_at": NOW.isoformat(), "fills": []})
    _write_json(
        tmp_path / config.local_paper_artifact_path,
        {
            "generated_at": NOW.isoformat(),
            "fills": [{"order_id": "paper-1", "client_id": "paper-client"}],
        },
    )

    snapshot = build_track_b_truth_snapshot(config=config, now=NOW)

    assert snapshot.broker_backed_evidence.broker_backed is False
    assert FILL_NOT_BROKER_BACKED in snapshot.reason_codes
    assert LOCAL_ARTIFACT_NOT_AUTHORITY in snapshot.reason_codes


def test_broker_lifecycle_open_mismatch_creates_truth_conflict(tmp_path: Path) -> None:
    config = _seed_clean(tmp_path)
    _write_json(
        tmp_path / config.broker_positions_path,
        {"generated_at": NOW.isoformat(), "positions": [{"symbol": "MNQ", "position": 1}]},
    )
    _write_json(
        tmp_path / config.reconciliation_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "DIRTY",
            "broker_reconciled": False,
            "review_required_count": 1,
        },
    )

    snapshot = build_track_b_truth_snapshot(config=config, now=NOW)

    assert snapshot.broker_truth.broker_position_count == 1
    assert snapshot.lifecycle.open_position_count == 0
    assert any(conflict.classification == BROKER_TRUTH_CONFLICT_REVIEW_REQUIRED for conflict in snapshot.conflicts)


def test_lifecycle_exact_identity_overrides_aggregate_multiple(tmp_path: Path) -> None:
    config = _seed_clean(tmp_path)
    _write_json(
        tmp_path / config.managed_position_registry_path,
        {
            "generated_at": NOW.isoformat(),
            "managed_positions": [
                {
                    "lifecycle_id": "life-1",
                    "account_id": "MULTIPLE",
                    "exact_lifecycle_account_id": "DUM882026",
                    "con_id": 770561201,
                    "localSymbol": "MNQM6",
                    "qty": 1,
                    "state": "OPEN_MANAGED",
                }
            ],
        },
    )
    _write_json(
        tmp_path / config.broker_positions_path,
        {"generated_at": NOW.isoformat(), "positions": [{"symbol": "MNQ", "position": 1}]},
    )
    _write_json(
        tmp_path / config.reconciliation_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "BROKER_LIFECYCLE_RECONCILED",
            "broker_reconciled": True,
            "review_required_count": 0,
        },
    )

    snapshot = build_track_b_truth_snapshot(config=config, now=NOW)

    assert snapshot.lifecycle.exact_owner_resolved is True
    assert EXACT_LIFECYCLE_OWNER_RESOLVED in snapshot.reason_codes
    assert AGGREGATE_PLACEHOLDER_DIAGNOSTIC_ONLY in snapshot.reason_codes
    assert not any(conflict.classification == BROKER_TRUTH_CONFLICT_REVIEW_REQUIRED for conflict in snapshot.conflicts)


def test_safe_state_submit_authority_is_separate_from_runtime_start_authority(tmp_path: Path) -> None:
    config = _seed_clean(tmp_path)
    _write_json(
        tmp_path / config.safe_state_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "SAFE_STATE_RECOVERY_ONLY",
            "submit_allowed": False,
            "runtime_start_allowed": True,
        },
    )

    snapshot = build_track_b_truth_snapshot(config=config, now=NOW)

    assert snapshot.safe_state.runtime_start_allowed is True
    assert snapshot.safe_state.submit_allowed is False
    assert SAFE_STATE_SUBMIT_BLOCKED in snapshot.reason_codes
    assert SAFE_STATE_RUNTIME_AUTHORITY_CONFLICT in snapshot.reason_codes


def test_contract_resolver_entry_blocked_but_exit_allowed(tmp_path: Path) -> None:
    config = _seed_clean(tmp_path)
    _write_json(
        tmp_path / config.contract_status_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "CONTRACT_EXIT_OR_MANAGEMENT_ALLOWED",
            "symbol": "MGC",
            "entry_status": CONTRACT_ENTRY_BLOCKED,
            "exit_status": "EXIT_ORIGINAL_CONTRACT_ALLOWED",
            "selected_contract": {"localSymbol": "MGCM6", "conId": 123, "expiry": "202606"},
        },
    )

    snapshot = build_track_b_truth_snapshot(config=config, now=NOW)

    assert snapshot.contract_status.entry_status == CONTRACT_ENTRY_BLOCKED
    assert snapshot.contract_status.exit_status == "EXIT_ORIGINAL_CONTRACT_ALLOWED"


def test_close_only_contract_status_is_preserved(tmp_path: Path) -> None:
    config = _seed_clean(tmp_path)
    _write_json(
        tmp_path / config.contract_status_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "CONTRACT_EXIT_OR_MANAGEMENT_ALLOWED",
            "symbol": "MGC",
            "selected_contract": {"localSymbol": "MGCM6", "conId": 123, "expiry": "202606"},
        },
    )

    snapshot = build_track_b_truth_snapshot(config=config, now=NOW)

    assert snapshot.contract_status.entry_status == CONTRACT_ENTRY_CLOSE_ONLY
    assert snapshot.contract_status.exit_status == "EXIT_ORIGINAL_CONTRACT_ALLOWED"


def test_planner_snapshot_mismatch_is_classified(tmp_path: Path) -> None:
    config = _seed_clean(tmp_path)
    _write_json(
        tmp_path / config.planner_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "PLAN_SCOPED_POSITION_CLEANUP",
            "control_plane_snapshot_id": "stale-snapshot",
        },
    )

    snapshot = build_track_b_truth_snapshot(config=config, now=NOW)

    assert PLANNER_SNAPSHOT_MISMATCH in snapshot.reason_codes
    assert any(conflict.classification == PLANNER_SNAPSHOT_MISMATCH for conflict in snapshot.conflicts)


def test_recovery_active_and_paused_are_classified(tmp_path: Path) -> None:
    config = _seed_clean(tmp_path)
    _write_json(
        tmp_path / config.recovery_status_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "RECOVERY_ACTIVE",
            "launchd_loaded": True,
            "launchd_enabled": True,
            "last_tick": NOW.isoformat(),
        },
    )

    active = build_track_b_truth_snapshot(config=config, now=NOW)
    assert active.recovery.active is True
    assert RECOVERY_ACTIVE in active.reason_codes

    _write_json(
        tmp_path / config.recovery_status_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "SUPERVISOR_PAUSED",
            "launchd_loaded": False,
            "launchd_enabled": False,
        },
    )

    paused = build_track_b_truth_snapshot(config=config, now=NOW)
    assert paused.recovery.paused is True
    assert RECOVERY_PAUSED in paused.reason_codes


def test_all_source_paths_and_freshness_are_included(tmp_path: Path) -> None:
    config = _seed_clean(tmp_path)

    snapshot = build_track_b_truth_snapshot(config=config, now=NOW)
    payload = snapshot.to_dict()

    assert payload["source_paths"]["runtime_truth"].endswith("latest_runtime_environment_truth.json")
    assert payload["runtime"]["source"]["fresh"] is True
    assert payload["broker_truth"]["source"]["freshness_seconds"] == 0.0
    assert payload["diagnostic_sources"][0]["diagnostic_only"] is True


def test_stale_control_plane_is_not_submit_authority(tmp_path: Path) -> None:
    config = _seed_clean(tmp_path)
    _write_json(
        tmp_path / config.control_plane_path,
        {
            "generated_at": (NOW - timedelta(minutes=10)).isoformat(),
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
            "shared_truth_coherence_status": "COHERENT",
        },
    )

    snapshot = build_track_b_truth_snapshot(config=config, now=NOW)

    assert snapshot.control_plane.fresh is False
    assert CONTROL_PLANE_STALE in snapshot.reason_codes
    assert snapshot.classification == TRUTH_CONFLICT_REVIEW_REQUIRED


def _seed_clean(tmp_path: Path) -> TrackBTruthSnapshotConfig:
    config = TrackBTruthSnapshotConfig(repo_root=tmp_path)
    _write_json(
        tmp_path / config.runtime_truth_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "RUNTIME_ACTIVE_TRADE_CAPABLE",
            "runtime": {
                "pid": 1234,
                "pid_alive": True,
                "runtime_instance_id": "generation-1",
                "lane_count": 8,
            },
            "canonical_readiness": {
                "classification": "READY_SUBMIT_CAPABLE",
                "ready_submit_capable": True,
            },
        },
    )
    _write_json(
        tmp_path / config.recovery_status_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "RECOVERY_ACTIVE",
            "launchd_loaded": True,
            "launchd_enabled": True,
            "last_tick": NOW.isoformat(),
        },
    )
    _write_json(
        tmp_path / config.recovery_audit_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "RUNTIME_HEALTHY_NO_ACTION",
            "hourly_supervisor": {"classification": "SUPERVISOR_RUNNING", "active": True},
        },
    )
    _write_json(
        tmp_path / config.broker_status_path,
        {
            "generated_at": NOW.isoformat(),
            "positions_snapshot_path": str(tmp_path / config.broker_positions_path),
            "open_orders_snapshot_path": str(tmp_path / config.broker_open_orders_path),
        },
    )
    _write_json(tmp_path / config.broker_positions_path, {"generated_at": NOW.isoformat(), "positions": []})
    _write_json(tmp_path / config.broker_open_orders_path, {"generated_at": NOW.isoformat(), "open_orders": []})
    _write_json(tmp_path / config.lifecycle_live_position_path, {"generated_at": NOW.isoformat(), "open_positions": []})
    _write_json(tmp_path / config.managed_position_registry_path, {"generated_at": NOW.isoformat(), "managed_positions": []})
    _write_json(tmp_path / config.managed_order_registry_path, {"generated_at": NOW.isoformat(), "managed_orders": []})
    _write_json(
        tmp_path / config.reconciliation_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "BROKER_LIFECYCLE_RECONCILED",
            "broker_reconciled": True,
            "review_required_count": 0,
        },
    )
    _write_json(
        tmp_path / config.safe_state_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "SAFE_STATE_NORMAL",
            "submit_allowed": True,
            "runtime_start_allowed": True,
        },
    )
    _write_json(
        tmp_path / config.control_plane_path,
        {
            "generated_at": NOW.isoformat(),
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
            "shared_truth_coherence_status": "COHERENT",
        },
    )
    _write_json(
        tmp_path / config.planner_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "PLAN_SCOPED_POSITION_CLEANUP",
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
        },
    )
    _write_json(
        tmp_path / config.supervisor_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "supervisor_decision_id": "supervisor-1",
        },
    )
    _write_json(
        tmp_path / config.contract_status_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "CONTRACT_ALLOWED",
            "submit_allowed": True,
            "symbol": "MNQ",
            "selected_contract": {"localSymbol": "MNQM6", "conId": 770561201, "expiry": "202606"},
        },
    )
    _write_json(
        tmp_path / config.broker_backed_evidence_path,
        {
            "generated_at": NOW.isoformat(),
            "fills": [
                {
                    "order_id": "39",
                    "client_id": "7",
                    "perm_id": "2047276405",
                    "exec_id": "0000e1a7.6a29f525.01.01",
                }
            ],
        },
    )
    _write_json(tmp_path / config.local_paper_artifact_path, {"generated_at": NOW.isoformat(), "local_rows": []})
    if config.dashboard_runtime_path is not None:
        _write_json(tmp_path / config.dashboard_runtime_path, {"generated_at": NOW.isoformat(), "diagnostic": True})
    return config


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
