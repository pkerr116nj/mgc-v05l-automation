from __future__ import annotations

from datetime import UTC, datetime

from mgc_v05l.execution_core.track_b_broker_position_guardian import (
    BROKER_LIFECYCLE_POSITION_MISMATCH,
    BROKER_POSITION_GUARDIAN_HARD_HOLD,
    BROKER_POSITION_GUARDIAN_READY,
    DUPLICATE_CLOSE_ORDER_BLOCKED,
    UNAUTHORIZED_REVERSE_EXPOSURE,
    TrackBBrokerPositionGuardianConfig,
    build_track_b_broker_position_guardian,
)


NOW = datetime(2026, 5, 25, 12, 15, tzinfo=UTC)


def test_clean_flat_guardian_ready() -> None:
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(),
    )

    assert payload["classification"] == BROKER_POSITION_GUARDIAN_READY
    assert payload["hard_classifications"] == []
    assert payload["new_entries_allowed"] is True


def test_duplicate_close_order_blocks() -> None:
    inputs = _inputs(
        open_order_truth={
            "classification": "DUPLICATE_CLOSE_ORDER",
            "duplicate_close_order_groups": [{"duplicate_key": "DUM882026|MNQM6|SELL|1", "count": 2}],
            "open_orders": [
                {"local_symbol": "MNQM6", "action": "SELL", "quantity": "1", "broker_order_id": "36"},
                {"local_symbol": "MNQM6", "action": "SELL", "quantity": "1", "broker_order_id": "37"},
            ],
        },
        managed_order_registry={
            "classification": "DUPLICATE_CLOSE_ORDER_BLOCKED",
            "managed_orders": [
                {"local_symbol": "MNQM6", "action": "SELL", "quantity": "1", "broker_order_id": "36"},
                {"local_symbol": "MNQM6", "action": "SELL", "quantity": "1", "broker_order_id": "37"},
            ],
        },
    )

    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=inputs,
    )

    assert payload["classification"] == BROKER_POSITION_GUARDIAN_HARD_HOLD
    assert DUPLICATE_CLOSE_ORDER_BLOCKED in payload["hard_classifications"]
    assert payload["guarded_roster_submit_allowed"] is False


def test_exit_fill_expected_flat_with_short_broker_position_is_unauthorized_reverse() -> None:
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            position_truth={"broker_positions": [_mnq_position("-1")]},
            reconciliation={
                "classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT",
                "blockers": [
                    {
                        "broker_truth_settlement": {
                            "event": {
                                "event_type": "EXIT_FILL_EXPECTING_BROKER_FLAT",
                                "broker_order_id": "36",
                                "local_symbol": "MNQM6",
                                "con_id": 770561201,
                            }
                        }
                    }
                ],
            },
        ),
    )

    assert payload["classification"] == BROKER_POSITION_GUARDIAN_HARD_HOLD
    assert UNAUTHORIZED_REVERSE_EXPOSURE in payload["hard_classifications"]
    assert BROKER_LIFECYCLE_POSITION_MISMATCH in payload["hard_classifications"]
    assert payload["scoped_remediation_plan"]["classification"] == "SCOPED_REVERSE_EXPOSURE_FLATTEN_PLAN_READY"
    assert payload["scoped_remediation_plan"]["action"] == "BUY"
    assert payload["scoped_remediation_plan"]["quantity"] == "1"
    assert payload["scoped_remediation_plan"]["local_symbol"] == "MNQM6"
    assert payload["scoped_remediation_plan"]["con_id"] == 770561201
    assert payload["scoped_remediation_plan"]["broad_flatten_allowed"] is False


def test_lifecycle_owner_without_broker_position_blocks() -> None:
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            managed_position_registry={
                "classification": "OPEN_MANAGED_MATCHED",
                "lifecycle_open_positions": [
                    {
                        "account_id": "DUM882026",
                        "symbol": "MNQ",
                        "local_symbol": "MNQM6",
                        "con_id": 770561201,
                        "quantity": "1",
                        "side": "LONG",
                        "lifecycle_id": "lifecycle-1",
                    }
                ],
            }
        ),
    )

    assert payload["classification"] == BROKER_POSITION_GUARDIAN_HARD_HOLD
    assert BROKER_LIFECYCLE_POSITION_MISMATCH in payload["hard_classifications"]


def test_aggregate_same_lane_units_match_broker_quantity_without_guardian_hold() -> None:
    lifecycle_position = {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "3",
        "aggregate_qty": "-3",
        "side": "SHORT",
        "lifecycle_id": "lifecycle-48",
        "lifecycle_unit_count": 3,
        "lifecycle_units": [
            {"lifecycle_id": "lifecycle-46", "signed_qty": "-1"},
            {"lifecycle_id": "lifecycle-47", "signed_qty": "-1"},
            {"lifecycle_id": "lifecycle-48", "signed_qty": "-1"},
        ],
        "duplicate_same_lane_exposure": True,
        "pyramiding_allowed": False,
    }
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            position_truth={"broker_positions": [_mnq_position("-3")]},
            managed_position_registry={
                "classification": "OPEN_MANAGED_EXIT_DUE",
                "managed_positions": [lifecycle_position],
                "lifecycle_open_positions": [lifecycle_position],
            },
            reconciliation={
                "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
                "track_b_lifecycle_positions": [lifecycle_position],
                "blockers": [],
            },
        ),
    )

    assert payload["classification"] == BROKER_POSITION_GUARDIAN_READY
    assert BROKER_LIFECYCLE_POSITION_MISMATCH not in payload["hard_classifications"]


def test_collapsed_registry_quantity_against_broker_hard_holds() -> None:
    collapsed = {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
        "side": "SHORT",
        "lifecycle_id": "lifecycle-48",
        "lifecycle_unit_count": 3,
    }
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            position_truth={"broker_positions": [_mnq_position("-3")]},
            managed_position_registry={
                "classification": "OPEN_MANAGED_EXIT_DUE",
                "managed_positions": [collapsed],
                "lifecycle_open_positions": [collapsed],
            },
            reconciliation={"classification": "TRACK_B_PAPER_BROKER_RECONCILED", "blockers": []},
        ),
    )

    assert payload["classification"] == BROKER_POSITION_GUARDIAN_HARD_HOLD
    assert BROKER_LIFECYCLE_POSITION_MISMATCH in payload["hard_classifications"]


def _inputs(**overrides: dict) -> dict:
    payload = {
        "position_truth": {"classification": "CLEAN_FLAT_READY", "broker_positions": []},
        "open_order_truth": {"classification": "NO_OPEN_ORDERS", "open_orders": []},
        "managed_order_registry": {"classification": "NO_MANAGED_ORDERS", "managed_orders": []},
        "managed_position_registry": {
            "classification": "NO_MANAGED_POSITIONS",
            "managed_positions": [],
            "lifecycle_open_positions": [],
        },
        "reconciliation": {"classification": "TRACK_B_PAPER_BROKER_RECONCILED", "blockers": []},
    }
    payload.update(overrides)
    return payload


def _mnq_position(quantity: str) -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "expiry": "20260618",
        "quantity": quantity,
    }
