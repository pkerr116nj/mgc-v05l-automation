from __future__ import annotations

from datetime import UTC, datetime

from mgc_v05l.execution_core.track_b_broker_position_guardian import (
    BROKER_LIFECYCLE_POSITION_MISMATCH,
    BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING,
    BROKER_POSITION_GUARDIAN_HARD_HOLD,
    BROKER_POSITION_GUARDIAN_READY,
    DUPLICATE_CLOSE_ORDER_BLOCKED,
    OPEN_ORDER_MANAGED_REGISTRY_MISMATCH,
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


def test_reverse_exposure_plan_canonicalizes_missing_broker_con_id_from_lifecycle_owner() -> None:
    broker_position = _mnq_position("-1")
    broker_position.pop("con_id")
    lifecycle_position = {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
        "side": "LONG",
        "lifecycle_id": "lifecycle-long",
    }
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            position_truth={"broker_positions": [broker_position]},
            managed_position_registry={
                "classification": "OPEN_MANAGED_EXIT_DUE",
                "managed_positions": [lifecycle_position],
                "lifecycle_open_positions": [lifecycle_position],
            },
        ),
    )

    assert payload["classification"] == BROKER_POSITION_GUARDIAN_HARD_HOLD
    assert UNAUTHORIZED_REVERSE_EXPOSURE in payload["hard_classifications"]
    assert payload["scoped_remediation_plan"]["classification"] == "SCOPED_REVERSE_EXPOSURE_FLATTEN_PLAN_READY"
    assert payload["scoped_remediation_plan"]["action"] == "BUY"
    assert payload["scoped_remediation_plan"]["con_id"] == "770561201"
    assert payload["scoped_remediation_plan"]["broad_flatten_allowed"] is False


def test_hard_hold_allows_exact_registry_backed_risk_reducing_close() -> None:
    inputs = _inputs(
        position_truth={"broker_positions": [_mnq_position("1")]},
        reconciliation=_registry_matched_reconciliation(quantity="1"),
    )

    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=inputs,
    )

    assert payload["classification"] == BROKER_POSITION_GUARDIAN_HARD_HOLD
    assert payload["new_entries_allowed"] is False
    assert payload["global_flatten_allowed"] is False
    assert payload["close_submit_allowed"] is True
    assert payload["managed_close_mutation_allowed"] is True
    assert payload["managed_close_authority"]["classification"] == BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING
    assert payload["managed_close_authority"]["candidates"][0]["action"] == "SELL"


def test_registry_backed_close_canonicalizes_missing_broker_con_id() -> None:
    broker_position = _mnq_position("1")
    broker_position.pop("con_id")
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            position_truth={"broker_positions": [broker_position]},
            reconciliation=_registry_matched_reconciliation(quantity="1"),
        ),
    )

    assert payload["managed_close_mutation_allowed"] is True
    assert payload["managed_close_authority"]["classification"] == BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING
    assert payload["managed_close_authority"]["candidates"][0]["con_id"] == 770561201
    assert payload["managed_close_authority"]["broad_flatten_allowed"] is False


def test_close_authority_prefers_reconciliation_match_owner_over_stale_managed_position() -> None:
    stale_position = {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
        "side": "SHORT",
        "trade_id": "trade_stale_owner",
        "lifecycle_id": "stale_lifecycle",
        "broker_position": _mnq_position("-1"),
        "lifecycle_position": {
            "trade_id": "trade_stale_owner",
            "lifecycle_id": "stale_lifecycle",
            "entry_exec_ids": ["exec_stale"],
            "entry_perm_ids": ["perm_stale"],
        },
    }
    current_lifecycle = {
        "trade_id": "trade_current_owner",
        "lifecycle_id": "current_lifecycle",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
        "side": "SHORT",
        "entry_exec_ids": ["exec_current"],
        "entry_perm_ids": ["perm_current"],
    }

    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            position_truth={"broker_positions": [_mnq_position("-1")]},
            managed_position_registry={
                "classification": "OPEN_MANAGED_EXIT_DUE",
                "managed_positions": [stale_position],
                "lifecycle_open_positions": [stale_position],
            },
            reconciliation={
                "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
                "registry_reconciliation": {
                    "classification": "REGISTRY_RECONCILIATION_MATCHED",
                    "blocking": False,
                    "mapped_records": [],
                },
                "position_match_report": {
                    "state": "BROKER_AND_LIFECYCLE_OPEN_MATCHED",
                    "matched": True,
                    "matches": [
                        {
                            "broker_position": _mnq_position("-1"),
                            "lifecycle_position": current_lifecycle,
                        }
                    ],
                },
            },
        ),
    )

    assert payload["managed_close_mutation_allowed"] is True
    candidate = payload["managed_close_authority"]["candidates"][0]
    assert candidate["trade_id"] == "trade_current_owner"
    assert candidate["lifecycle_id"] == "current_lifecycle"
    assert candidate["action"] == "BUY"


def test_registry_backed_close_blocks_wrong_quantity() -> None:
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            position_truth={"broker_positions": [_mnq_position("1")]},
            reconciliation=_registry_matched_reconciliation(quantity="2"),
        ),
    )

    assert payload["managed_close_mutation_allowed"] is False
    assert "CLOSE_QUANTITY_EXCEEDS_BROKER_POSITION" in payload["managed_close_authority"]["reason_codes"]


def test_registry_backed_close_blocks_contract_mismatch() -> None:
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            position_truth={"broker_positions": [_mnq_position("1")]},
            reconciliation=_registry_matched_reconciliation(local_symbol="MESM6", con_id=770561194),
        ),
    )

    assert payload["managed_close_mutation_allowed"] is False
    assert "REGISTRY_MAPPED_RECORD_NOT_FOUND" in payload["managed_close_authority"]["reason_codes"]


def test_registry_backed_close_blocks_missing_trade_identity() -> None:
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            position_truth={"broker_positions": [_mnq_position("1")]},
            reconciliation=_registry_matched_reconciliation(trade_id="", lifecycle_id=""),
        ),
    )

    assert payload["managed_close_mutation_allowed"] is False
    assert "TRADE_ID_MISSING" in payload["managed_close_authority"]["reason_codes"]
    assert "LIFECYCLE_ID_MISSING" in payload["managed_close_authority"]["reason_codes"]


def test_registry_backed_close_blocks_registry_conflict() -> None:
    reconciliation = _registry_matched_reconciliation(quantity="1")
    reconciliation["registry_reconciliation"]["blocking"] = True

    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(position_truth={"broker_positions": [_mnq_position("1")]}, reconciliation=reconciliation),
    )

    assert payload["managed_close_mutation_allowed"] is False
    assert "REGISTRY_RECONCILIATION_NOT_MATCHED" in payload["managed_close_authority"]["reason_codes"]


def test_registry_backed_close_blocks_conflicting_close_order() -> None:
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            position_truth={"broker_positions": [_mnq_position("1")]},
            open_order_truth={
                "classification": "OPEN_CLOSE_ORDER_WORKING",
                "open_orders": [{"local_symbol": "MNQM6", "action": "SELL", "quantity": "1", "broker_order_id": "59"}],
            },
            reconciliation=_registry_matched_reconciliation(quantity="1"),
        ),
    )

    assert payload["managed_close_mutation_allowed"] is False
    assert "CONFLICTING_CLOSE_ORDER" in payload["managed_close_authority"]["reason_codes"]


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

    assert payload["classification"] == BROKER_POSITION_GUARDIAN_READY
    assert BROKER_LIFECYCLE_POSITION_MISMATCH in payload["hard_classifications"]
    assert payload["global_hard_classifications"] == []
    assert payload["blocked_entry_symbols"] == ["MNQ"]
    assert payload["new_entries_allowed"] is True


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

    assert payload["classification"] == BROKER_POSITION_GUARDIAN_READY
    assert BROKER_LIFECYCLE_POSITION_MISMATCH in payload["hard_classifications"]
    assert payload["global_hard_classifications"] == []
    assert payload["blocked_entry_symbols"] == ["MNQ"]
    assert payload["new_entries_allowed"] is True


def test_fresh_broker_flat_invalidates_stale_lifecycle_owner() -> None:
    stale_lifecycle = {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
        "side": "LONG",
        "lifecycle_id": "stale-lifecycle",
    }
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            position_truth=_fresh_position_truth([]),
            managed_position_registry={
                "classification": "OPEN_MANAGED_MATCHED",
                "managed_positions": [],
                "lifecycle_open_positions": [stale_lifecycle],
            },
        ),
    )

    assert payload["classification"] == BROKER_POSITION_GUARDIAN_READY
    assert payload["hard_classifications"] == []
    invalidation = payload["current_truth_invalidation"]
    assert invalidation["invalidated_finding_count"] == 1
    invalidated = invalidation["invalidated_findings"][0]
    assert invalidated["classification"] == BROKER_LIFECYCLE_POSITION_MISMATCH
    assert invalidated["historical_only"] is True
    assert invalidated["diagnostic_only"] is True
    assert invalidated["current_scope_active"] is False
    assert invalidated["invalidated_by_current_truth"] is True


def test_fresh_no_open_orders_invalidates_stale_managed_order_mismatch() -> None:
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            position_truth=_fresh_position_truth([]),
            open_order_truth=_global_no_open_orders(),
            managed_order_registry={
                "classification": "CLOSE_ORDER_MODIFIABLE",
                "managed_orders": [
                    {
                        "local_symbol": "MNQM6",
                        "action": "SELL",
                        "quantity": "1",
                        "broker_order_id": "stale-37",
                        "working": True,
                    }
                ],
            },
        ),
    )

    assert payload["classification"] == BROKER_POSITION_GUARDIAN_READY
    assert OPEN_ORDER_MANAGED_REGISTRY_MISMATCH not in payload["hard_classifications"]
    invalidated = payload["current_truth_invalidation"]["invalidated_findings"]
    assert [row["classification"] for row in invalidated] == [OPEN_ORDER_MANAGED_REGISTRY_MISMATCH]
    assert invalidated[0]["diagnostic_only"] is True


def test_fresh_current_side_contradiction_still_hard_holds() -> None:
    lifecycle_position = {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
        "side": "LONG",
        "lifecycle_id": "current-lifecycle",
    }
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            position_truth=_fresh_position_truth([_mnq_position("-1")]),
            managed_position_registry={
                "classification": "OPEN_MANAGED_MATCHED",
                "managed_positions": [lifecycle_position],
                "lifecycle_open_positions": [lifecycle_position],
            },
        ),
    )

    assert payload["classification"] == BROKER_POSITION_GUARDIAN_HARD_HOLD
    assert UNAUTHORIZED_REVERSE_EXPOSURE in payload["hard_classifications"]
    assert payload["current_truth_invalidation"]["invalidated_finding_count"] == 0


def test_current_open_order_ambiguity_still_hard_holds() -> None:
    payload = build_track_b_broker_position_guardian(
        config=TrackBBrokerPositionGuardianConfig(),
        now=NOW,
        input_overrides=_inputs(
            open_order_truth={
                "classification": "UNKNOWN_OPEN_ORDERS_PRESENT",
                "canonical_refresh_scope": "GLOBAL_COMPLETE",
                "open_order_count": 1,
                "open_orders": [
                    {"local_symbol": "MNQM6", "action": "BUY", "quantity": "1", "broker_order_id": "88"}
                ],
            },
            managed_order_registry={"classification": "NO_MANAGED_ORDERS", "managed_orders": []},
        ),
    )

    assert payload["classification"] == BROKER_POSITION_GUARDIAN_HARD_HOLD
    assert OPEN_ORDER_MANAGED_REGISTRY_MISMATCH in payload["hard_classifications"]
    assert payload["current_truth_invalidation"]["invalidated_finding_count"] == 0

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


def _fresh_position_truth(positions: list[dict]) -> dict:
    return {
        "classification": "CLEAN_FLAT_READY" if not positions else "ATTENTION_REQUIRED",
        "generated_at": NOW.isoformat(),
        "broker_positions": positions,
        "dmc_metadata": {
            "refresh_scope": {
                "scope_type": "GLOBAL_COMPLETE",
                "partial": False,
            }
        },
    }


def _global_no_open_orders() -> dict:
    return {
        "classification": "NO_OPEN_ORDERS",
        "canonical_refresh_scope": "GLOBAL_COMPLETE",
        "generated_at": NOW.isoformat(),
        "open_order_count": 0,
        "open_orders": [],
        "duplicate_close_order_groups": [],
    }


def _mnq_position(quantity: str) -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "expiry": "20260618",
        "quantity": quantity,
    }


def _registry_matched_reconciliation(
    *,
    quantity: str = "1",
    trade_id: str = "trade-1",
    lifecycle_id: str = "lifecycle-1",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
) -> dict:
    return {
        "classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        "blockers": [
            {
                "broker_truth_settlement": {
                    "event": {
                        "event_type": "EXIT_FILL_EXPECTING_BROKER_FLAT",
                        "broker_order_id": "59",
                        "local_symbol": "MNQM6",
                        "con_id": 770561201,
                    }
                }
            }
        ],
        "registry_reconciliation": {
            "classification": "REGISTRY_RECONCILIATION_MATCHED",
            "blocking": False,
            "mapped_records": [
                {
                    "trade_id": trade_id,
                    "lifecycle_id": lifecycle_id,
                    "account_id": "DUM882026",
                    "symbol": "MNQ",
                    "instrument_family": "MNQ",
                    "local_symbol": local_symbol,
                    "con_id": con_id,
                    "quantity": quantity,
                    "side": "LONG",
                    "current_state": "OPEN_MANAGED",
                    "entry_perm_id": "1955790757",
                    "entry_exec_id": "0000e1a7.6a2c281d.01.01",
                    "lane_id": "mnq_us_active_participation_long",
                    "strategy_id": "mnq_us_active_participation_long",
                }
            ],
        },
    }
