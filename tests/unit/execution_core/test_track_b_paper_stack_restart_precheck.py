from __future__ import annotations

from copy import deepcopy

from mgc_v05l.execution_core.track_b_paper_stack_restart_precheck import (
    BLOCKED_DUPLICATE_WRITER,
    BLOCKED_LIVE_MONEY_OR_PAPER_PROOF,
    BLOCKED_OPEN_ORDERS,
    BLOCKED_RECOVERY_INACTIVE,
    BLOCKED_STALE_BROKER_TRUTH,
    BLOCKED_UNMANAGED_EXPOSURE,
    RESTART_ALLOWED_FLAT_RECONCILED,
    RESTART_ALLOWED_OWNED_MANAGED_EXPOSURE,
    classify_paper_stack_restart_precheck,
)


def test_flat_reconciled_restart_allowed() -> None:
    payload = _status()

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is True
    assert result.classification == RESTART_ALLOWED_FLAT_RECONCILED


def test_owned_managed_exposure_restart_allowed() -> None:
    payload = _status(
        reconciliation_classification="BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        track_b_positions=1,
        lifecycle_positions=1,
    )
    payload["live_runtime_environment"]["restart_policy"] = {
        "owned_exposure_restart_allowed": True,
        "pre_restart_exposure_resolution_classification": "PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED",
        "reason_codes": [],
    }

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is True
    assert result.classification == RESTART_ALLOWED_OWNED_MANAGED_EXPOSURE


def test_reconciled_owned_managed_exposure_restart_uses_maintenance_restore_classification() -> None:
    payload = _status(
        reconciliation_classification="TRACK_B_PAPER_BROKER_RECONCILED",
        track_b_positions=1,
        lifecycle_positions=1,
    )
    payload["live_runtime_environment"]["restart_policy"] = {
        "owned_exposure_restart_allowed": True,
        "pre_restart_exposure_resolution_classification": "PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED",
        "reason_codes": [],
    }

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is True
    assert result.classification == RESTART_ALLOWED_OWNED_MANAGED_EXPOSURE
    assert "PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED" in result.reason_codes


def test_runtime_down_managed_exposure_restart_allowed_for_maintenance_restoration() -> None:
    payload = _status(
        reconciliation_classification="BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        track_b_positions=1,
        lifecycle_positions=1,
    )
    payload["live_runtime_environment"]["classification"] = "RUNTIME_DOWN_WITH_BROKER_EXPOSURE"
    payload["live_runtime_environment"]["restart_policy"] = {
        "owned_exposure_restart_allowed": True,
        "pre_restart_exposure_resolution_classification": "MANAGED_EXPOSURE_RESOLVED",
        "reason_codes": [],
    }

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is True
    assert result.classification == RESTART_ALLOWED_OWNED_MANAGED_EXPOSURE
    assert "MANAGED_EXPOSURE_RESOLVED" in result.reason_codes


def test_unmanaged_exposure_blocks_restart() -> None:
    payload = _status(
        reconciliation_classification="BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        track_b_positions=1,
        lifecycle_positions=0,
    )
    payload["live_runtime_environment"]["restart_policy"] = {
        "owned_exposure_restart_allowed": False,
        "reason_codes": ["NO_AUTOMATIC_RESTART_OPEN_EXPOSURE_WITHOUT_PROVEN_IDENTITY"],
    }

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is False
    assert result.classification == BLOCKED_UNMANAGED_EXPOSURE
    assert "NO_AUTOMATIC_RESTART_OPEN_EXPOSURE_WITHOUT_PROVEN_IDENTITY" in result.reason_codes


def test_stale_raw_lifecycle_count_does_not_block_when_current_scope_flat() -> None:
    payload = _status(
        reconciliation_classification="BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        track_b_positions=0,
        lifecycle_positions=1,
        current_scope_lifecycle_positions=0,
        stale_superseded_lifecycle_positions=1,
    )
    payload["live_runtime_environment"]["restart_policy"] = {
        "owned_exposure_restart_allowed": False,
        "reason_codes": [],
    }

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is True
    assert result.classification == RESTART_ALLOWED_FLAT_RECONCILED
    assert "FRESH_COMPLETE_CLEAN_BROKER_TRUTH" in result.reason_codes


def test_clean_fresh_broker_truth_allows_restart_when_lifecycle_mismatch_is_stale() -> None:
    payload = _status(
        reconciliation_classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
        track_b_positions=0,
        lifecycle_positions=1,
        current_scope_lifecycle_positions=1,
    )
    payload["broker_truth_status"] = {
        "account": "DUM882026",
        "fresh": True,
        "positions_complete": True,
        "open_orders_complete": True,
        "open_order_count": 0,
        "unknown_open_order_count": 0,
        "positions": [
            {"account_id": "DUM882026", "security_type": "FUT", "symbol": "MNQ", "local_symbol": "MNQU6", "quantity": "0"},
            {"account_id": "DUM882026", "security_type": "FUT", "symbol": "MES", "local_symbol": "MESU6", "quantity": "0"},
        ],
    }
    payload["open_order_truth"] = {"classification": "ORDER_TRUTH_STALE"}

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is True
    assert result.classification == RESTART_ALLOWED_FLAT_RECONCILED
    assert "FRESH_COMPLETE_CLEAN_BROKER_TRUTH" in result.reason_codes


def test_actual_broker_position_blocks_restart_even_when_lifecycle_projection_is_flat() -> None:
    payload = _status(
        reconciliation_classification="TRACK_B_PAPER_BROKER_RECONCILED",
        track_b_positions=0,
        lifecycle_positions=0,
        current_scope_lifecycle_positions=0,
    )
    payload["broker_truth_status"] = {
        "account": "DUM882026",
        "fresh": True,
        "positions_complete": True,
        "open_orders_complete": True,
        "open_order_count": 0,
        "unknown_open_order_count": 0,
        "positions": [
            {"account_id": "DUM882026", "security_type": "FUT", "symbol": "MGC", "local_symbol": "MGCQ6", "quantity": "1"},
        ],
    }

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is False
    assert result.classification == BLOCKED_UNMANAGED_EXPOSURE
    assert "track_b_futures_positions_present" in result.reason_codes


def test_incomplete_fresh_broker_truth_blocks_restart_authority() -> None:
    payload = _status()
    payload["broker_truth_status"] = {
        "account": "DUM882026",
        "fresh": True,
        "positions_complete": False,
        "open_orders_complete": True,
        "open_order_count": 0,
        "unknown_open_order_count": 0,
        "positions": [],
    }

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is False
    assert result.classification == BLOCKED_STALE_BROKER_TRUTH
    assert "broker_positions_incomplete" in result.reason_codes


def test_stale_raw_lifecycle_count_does_not_block_flat_reconciled_restart() -> None:
    payload = _status(
        reconciliation_classification="TRACK_B_PAPER_BROKER_RECONCILED",
        track_b_positions=0,
        lifecycle_positions=1,
        current_scope_lifecycle_positions=0,
        stale_superseded_lifecycle_positions=1,
    )

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is True
    assert result.classification == RESTART_ALLOWED_FLAT_RECONCILED


def test_current_scope_lifecycle_position_blocks_restart_when_not_reconciled() -> None:
    payload = _status(
        reconciliation_classification="BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        track_b_positions=0,
        lifecycle_positions=1,
        current_scope_lifecycle_positions=1,
        stale_superseded_lifecycle_positions=0,
    )
    payload["live_runtime_environment"]["restart_policy"] = {
        "owned_exposure_restart_allowed": False,
        "reason_codes": ["CURRENT_SCOPE_LIFECYCLE_POSITION_PRESENT"],
    }

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is True
    assert result.classification == RESTART_ALLOWED_FLAT_RECONCILED
    assert "FRESH_COMPLETE_CLEAN_BROKER_TRUTH" in result.reason_codes


def test_runtime_down_ambiguous_broker_state_blocks_restart() -> None:
    payload = _status(
        reconciliation_classification="BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        track_b_positions=1,
        lifecycle_positions=1,
    )
    payload["live_runtime_environment"]["classification"] = "RUNTIME_DOWN_WITH_BROKER_EXPOSURE"
    payload["live_runtime_environment"]["restart_policy"] = {
        "owned_exposure_restart_allowed": False,
        "pre_restart_exposure_resolution_classification": "AMBIGUOUS_BROKER_STATE",
        "reason_codes": ["AMBIGUOUS_BROKER_STATE"],
    }

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is False
    assert result.classification == BLOCKED_UNMANAGED_EXPOSURE
    assert "AMBIGUOUS_BROKER_STATE" in result.reason_codes


def test_open_orders_block_restart_even_with_owned_exposure() -> None:
    payload = _status(
        reconciliation_classification="BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        track_b_positions=1,
        lifecycle_positions=1,
        broker_open_orders=1,
    )
    payload["live_runtime_environment"]["restart_policy"] = {
        "owned_exposure_restart_allowed": True,
        "pre_restart_exposure_resolution_classification": "PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED",
    }

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is False
    assert result.classification == BLOCKED_OPEN_ORDERS


def test_duplicate_writer_blocks_restart() -> None:
    payload = _status()
    payload["duplicate_writer"]["duplicate_writer_detected"] = True

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is False
    assert result.classification == BLOCKED_DUPLICATE_WRITER


def test_recovery_inactive_blocks_restart() -> None:
    payload = _status()
    payload["recovery"]["classification"] = "RECOVERY_DISABLED_BY_OPERATOR"

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is False
    assert result.classification == BLOCKED_RECOVERY_INACTIVE


def test_live_money_blocks_restart() -> None:
    payload = _status()
    payload["safety"]["live_money_eligible"] = True

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is False
    assert result.classification == BLOCKED_LIVE_MONEY_OR_PAPER_PROOF


def test_paper_proof_blocks_restart() -> None:
    payload = _status()
    payload["safety"]["paper_proof_invoked"] = True

    result = classify_paper_stack_restart_precheck(payload)

    assert result.restart_allowed is False
    assert result.classification == BLOCKED_LIVE_MONEY_OR_PAPER_PROOF


def _status(
    *,
    reconciliation_classification: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_open_orders: int = 0,
    track_b_positions: int = 0,
    lifecycle_positions: int = 0,
    current_scope_lifecycle_positions: int | None = None,
    stale_superseded_lifecycle_positions: int = 0,
) -> dict:
    payload = {
        "safety": {
            "paper_only": True,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "broker_mutation_allowed": False,
        },
        "duplicate_writer": {"duplicate_writer_detected": False},
        "config": {"review_overlay_active": False},
        "recovery": {
            "classification": "RECOVERY_ACTIVE",
            "recovery_authoritative": True,
            "standalone_recovery_classification": "RECOVERY_ACTIVE",
        },
        "broker_lifecycle": {
            "reconciliation_classification": reconciliation_classification,
            "broker_truth_fresh": True,
        },
        "registry_truth_diagnostics": {
            "broker_open_order_count": broker_open_orders,
            "track_b_managed_futures_position_count": track_b_positions,
            "raw_lifecycle_open_position_count": lifecycle_positions,
            "current_scope_lifecycle_open_position_count": (
                lifecycle_positions if current_scope_lifecycle_positions is None else current_scope_lifecycle_positions
            ),
            "stale_superseded_lifecycle_projection_count": stale_superseded_lifecycle_positions,
            "lifecycle_open_position_count": lifecycle_positions,
        },
        "live_runtime_environment": {
            "restart_policy": {
                "owned_exposure_restart_allowed": False,
                "reason_codes": [],
            }
        },
    }
    return deepcopy(payload)
