from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_current_scope_state import (
    build_current_scope_state,
    write_current_scope_state,
)


NOW = datetime(2026, 6, 10, 11, 30, tzinfo=UTC)


def test_before_trade_flat_state_is_canonical_flat(tmp_path: Path) -> None:
    _seed_flat(tmp_path)

    payload = build_current_scope_state(repo_root=tmp_path, now=NOW)

    assert payload["classification"] == "FLAT"
    assert payload["broker_state_for_ods"] == "FLAT"
    assert payload["first_blocker_for_ods"] is None
    assert payload["next_safe_action_for_ods"] == "NO_ACTION"
    assert payload["broker_position_count"] == 0
    assert payload["broker_open_order_count"] == 0


def test_entry_filled_managed_match_is_open_managed(tmp_path: Path) -> None:
    _seed_open_managed(tmp_path, managed_classification="OPEN_MANAGED_MATCHED")

    payload = build_current_scope_state(repo_root=tmp_path, now=NOW)

    assert payload["classification"] == "OPEN_MANAGED"
    assert payload["broker_state_for_ods"] == "EXPOSED_MANAGED"
    assert payload["managed_positions"]["classification"] == "OPEN_MANAGED_MATCHED"


def test_exit_due_position_is_open_managed_exit_due(tmp_path: Path) -> None:
    _seed_open_managed(tmp_path, managed_classification="OPEN_MANAGED_EXIT_DUE")

    payload = build_current_scope_state(repo_root=tmp_path, now=NOW)

    assert payload["classification"] == "OPEN_MANAGED_EXIT_DUE"
    assert payload["broker_state_for_ods"] == "EXPOSED_MANAGED"


def test_working_close_order_classifies_open_orders(tmp_path: Path) -> None:
    _seed_open_managed(tmp_path, managed_classification="OPEN_MANAGED_CLOSE_WORKING", open_orders=True)

    payload = build_current_scope_state(repo_root=tmp_path, now=NOW)

    assert payload["classification"] == "OPEN_ORDERS"
    assert payload["broker_open_order_count"] == 1
    assert payload["broker_state_for_ods"] == "OPEN_ORDERS"


def test_close_filled_flat_with_stale_lifecycle_debris_is_flat(tmp_path: Path) -> None:
    _seed_flat(
        tmp_path,
        managed_positions={
            "classification": "NO_MANAGED_POSITIONS",
            "managed_positions": [
                {
                    "local_symbol": "MNQM6",
                    "quantity": "-1",
                    "historical_only": True,
                    "diagnostic_only": True,
                }
            ],
        },
    )

    payload = build_current_scope_state(repo_root=tmp_path, now=NOW)

    assert payload["classification"] == "FLAT"
    assert payload["managed_positions"]["count"] == 1


def test_post_close_broker_flat_beats_stale_reconciliation_and_managed_projection(tmp_path: Path) -> None:
    _seed_flat(tmp_path)
    lease = _broker_lease(
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "security_type": "FUT",
                "quantity": "0.0",
                "updated_at": NOW.isoformat(),
            }
        ],
        open_orders=[],
    )
    lease["track_b_broker_position_count"] = 1
    lease["latest_relevant_executions"] = [
        {
            "account_id": "DUM882026",
            "action": "BUY",
            "local_symbol": "MNQM6",
            "order_id": "94",
            "perm_id": "2075599403",
            "exec_id": "0000e1a7.6a3cb895.01.01",
            "price": "28687.5",
            "qty": "1",
            "fill_timestamp": "2026-06-10T12:13:01.351599+00:00",
        }
    ]
    _write(tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json", lease)
    stale_at = (NOW - timedelta(seconds=420)).isoformat()
    _write(
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        _reconciliation(positions=1) | {"generated_at": stale_at},
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        _base("track_b_managed_position_registry_v1")
        | {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [
                {
                    "local_symbol": "MNQM6",
                    "quantity": "-1",
                    "lifecycle_id": "reserved_submit_mnq_london_late_active_participation_short",
                    "source": "stale_pre_close_projection",
                }
            ],
        },
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        _base("track_b_managed_order_registry_v1")
        | {
            "classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "managed_orders": [{"local_symbol": "MNQM6", "action": "BUY", "quantity": "1"}],
        },
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/post_broker_mutation_refresh/latest_post_broker_mutation_refresh.json",
        _base("track_b_post_broker_mutation_refresh_v1")
        | {
            "classification": "POST_BROKER_MUTATION_REFRESH_DEGRADED",
            "first_failing_step": {"name": "shared_truth", "returncode": 124},
        },
    )

    payload = build_current_scope_state(repo_root=tmp_path, now=NOW)

    assert payload["classification"] == "FLAT"
    assert payload["broker_state_for_ods"] == "FLAT"
    assert payload["first_blocker_for_ods"] is None
    assert payload["next_safe_action_for_ods"] == "NO_ACTION"
    assert payload["broker_position_count"] == 0
    assert payload["broker_open_order_count"] == 0
    assert payload["latest_relevant_executions"][0]["order_id"] == "94"
    assert any(row["code"] == "reconciliation_stale" and row["diagnostic_only"] for row in payload["diagnostics"])
    assert any(row["code"] == "post_mutation_refresh_degraded" for row in payload["diagnostics"])
    assert any(
        row["code"] == "managed_positions_contradicted_by_current_broker_flat"
        and row["diagnostic_only"]
        for row in payload["diagnostics"]
    )


def test_shared_truth_timeout_after_flat_close_is_diagnostic_only(tmp_path: Path) -> None:
    _seed_flat(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/post_broker_mutation_refresh/latest_post_broker_mutation_refresh.json",
        {
            "schema_version": "track_b_post_broker_mutation_refresh_v1",
            "generated_at": NOW.isoformat(),
            "classification": "POST_BROKER_MUTATION_REFRESH_DEGRADED",
            "first_failing_step": {"name": "shared_truth", "returncode": 124},
        },
    )

    payload = build_current_scope_state(repo_root=tmp_path, now=NOW)

    assert payload["classification"] == "FLAT"
    assert any(row["code"] == "post_mutation_refresh_degraded" for row in payload["diagnostics"])


def test_control_plane_stale_after_flat_close_is_diagnostic_only(tmp_path: Path) -> None:
    _seed_flat(tmp_path)
    stale = (NOW - timedelta(seconds=800)).isoformat()
    _write(
        tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "schema_version": "track_b_control_plane_snapshot_v1",
            "generated_at": stale,
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
        },
    )

    payload = build_current_scope_state(repo_root=tmp_path, now=NOW)

    assert payload["classification"] == "FLAT"
    assert any(row["code"] == "control_plane_stale" for row in payload["diagnostics"])


def test_broker_truth_unavailable_requires_refresh(tmp_path: Path) -> None:
    _seed_flat(tmp_path)
    (tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json").unlink()

    payload = build_current_scope_state(repo_root=tmp_path, now=NOW)

    assert payload["classification"] == "UNKNOWN_REFRESH_REQUIRED"
    assert payload["first_blocker_for_ods"]["code"] == "broker_truth_refresh_required"


def test_unknown_same_contract_order_is_unknown_order_risk(tmp_path: Path) -> None:
    _seed_flat(tmp_path)
    _write(
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        _reconciliation(unknown_orders=1),
    )

    payload = build_current_scope_state(repo_root=tmp_path, now=NOW)

    assert payload["classification"] == "UNKNOWN_ORDER_RISK"
    assert payload["first_blocker_for_ods"]["code"] == "unknown_order_risk"


def test_live_proof_or_wrong_account_blocks_safety(tmp_path: Path) -> None:
    _seed_flat(tmp_path)
    lease = _broker_lease()
    lease["account_id"] = "LIVE123"
    _write(tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json", lease)

    payload = build_current_scope_state(repo_root=tmp_path, now=NOW)

    assert payload["classification"] == "BLOCKED_SAFETY"
    assert payload["first_blocker_for_ods"]["code"] == "wrong_account"


def test_write_current_scope_state_persists_artifact(tmp_path: Path) -> None:
    _seed_flat(tmp_path)

    payload = write_current_scope_state(repo_root=tmp_path, now=NOW)

    path = tmp_path / "outputs/track_b_execution_core/current_scope_state/latest_current_scope_state.json"
    assert path.exists()
    assert json.loads(path.read_text())["classification"] == payload["classification"]


def _seed_flat(tmp_path: Path, *, managed_positions: dict | None = None) -> None:
    _write(tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json", _broker_lease())
    _write(
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        _reconciliation(),
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        _base("track_b_open_order_truth_v1") | {"classification": "NO_OPEN_ORDERS"},
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        _base("track_b_managed_position_registry_v1")
        | (managed_positions or {"classification": "NO_MANAGED_POSITIONS", "managed_positions": []}),
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        _base("track_b_managed_order_registry_v1") | {"classification": "NO_MANAGED_ORDERS", "managed_orders": []},
    )
    _write(
        tmp_path / "outputs/operator_dashboard/runtime/latest_broker_session_authority.json",
        _base("track_b_broker_session_authority_v1")
        | {
            "classification": "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS",
            "allowed_uses": {"new_entry": True},
        },
    )
    _write(
        tmp_path / "outputs/operator_dashboard/runtime/latest_canonical_readiness.json",
        _base("track_b_canonical_readiness_v1")
        | {"canonical_readiness": "READY_SUBMIT_CAPABLE", "submit_allowed": True},
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        _base("track_b_control_plane_snapshot_v1") | {"classification": "CONTROL_PLANE_SNAPSHOT_READY"},
    )


def _seed_open_managed(tmp_path: Path, *, managed_classification: str, open_orders: bool = False) -> None:
    _seed_flat(tmp_path)
    _write(
        tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json",
        _broker_lease(
            positions=[{"symbol": "MES", "local_symbol": "MESM6", "security_type": "FUT", "quantity": "-1"}],
            open_orders=[
                {"symbol": "MES", "local_symbol": "MESM6", "security_type": "FUT", "action": "BUY", "quantity": "1"}
            ]
            if open_orders
            else [],
        ),
    )
    _write(
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        _reconciliation(positions=1, open_orders=1 if open_orders else 0),
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        _base("track_b_open_order_truth_v1")
        | {"classification": "OPEN_CLOSE_ORDER_WORKING" if open_orders else "NO_OPEN_ORDERS"},
    )
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        _base("track_b_managed_position_registry_v1")
        | {
            "classification": managed_classification,
            "managed_positions": [{"local_symbol": "MESM6", "quantity": "-1", "lifecycle_id": "life-1"}],
        },
    )


def _broker_lease(*, positions: list[dict] | None = None, open_orders: list[dict] | None = None) -> dict:
    positions = positions or []
    open_orders = open_orders or []
    return _base("track_b_broker_truth_lease_v1") | {
        "account_id": "DUM882026",
        "broker_reconciled": True,
        "broker_position_lease": {"complete": True, "fresh": True},
        "broker_open_order_lease": {"complete": True, "fresh": True},
        "positions": positions,
        "open_orders": open_orders,
        "track_b_broker_position_count": len(positions),
        "track_b_broker_open_order_count": len(open_orders),
        "unknown_broker_open_order_count": 0,
    }


def _reconciliation(*, positions: int = 0, open_orders: int = 0, unknown_orders: int = 0) -> dict:
    return _base("track_b_paper_broker_reconciliation_v1") | {
        "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
        "broker_reconciled": True,
        "track_b_broker_position_count": positions,
        "track_b_broker_open_order_count": open_orders,
        "unknown_broker_open_order_count": unknown_orders,
        "current_scope_review_required_count": 0,
        "current_scope_lifecycle_open_position_count": positions,
        "current_exposure_owner_resolution": {
            "classification": "OWNED_MANAGED_EXPOSURE" if positions else "NO_OPEN_EXPOSURE"
        },
        "track_b_broker_positions": [
            {"symbol": "MES", "local_symbol": "MESM6", "security_type": "FUT", "quantity": "-1"}
        ]
        if positions
        else [],
        "track_b_broker_open_orders": [
            {"symbol": "MES", "local_symbol": "MESM6", "security_type": "FUT", "action": "BUY", "quantity": "1"}
        ]
        if open_orders
        else [],
    }


def _base(schema_version: str) -> dict:
    return {
        "schema_version": schema_version,
        "generated_at": NOW.isoformat(),
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "account_id": "DUM882026",
    }


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
