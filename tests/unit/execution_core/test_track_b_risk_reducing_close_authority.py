from __future__ import annotations

from mgc_v05l.execution_core.track_b_risk_reducing_close_authority import (
    RISK_REDUCING_CLOSE_ALLOWED_RUNTIME_STALE_WITH_BROKER_EXPOSURE,
    RISK_REDUCING_CLOSE_BLOCKED_CONFLICTING_OPEN_ORDER,
    RISK_REDUCING_CLOSE_BLOCKED_GUARDIAN_NOT_READY,
    RISK_REDUCING_CLOSE_BLOCKED_NOT_BROKER_BACKED,
    RISK_REDUCING_CLOSE_BLOCKED_NOT_EXIT_DUE,
    RISK_REDUCING_CLOSE_BLOCKED_OWNER_AMBIGUOUS,
    RISK_REDUCING_CLOSE_BLOCKED_WOULD_INCREASE_OR_FLIP_EXPOSURE,
    classify_runtime_stale_risk_reducing_close,
)


def test_stale_runtime_blocks_entry_but_allows_exact_guardian_approved_exit_due_close() -> None:
    result = _classify()

    assert result["classification"] == RISK_REDUCING_CLOSE_ALLOWED_RUNTIME_STALE_WITH_BROKER_EXPOSURE
    assert result["allowed"] is True
    assert result["entry_submit_allowed"] is False
    assert result["close_candidate"] == {
        "trade_id": "trade-1",
        "lifecycle_id": "lifecycle-1",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "action": "BUY",
        "quantity": "1",
        "classification": RISK_REDUCING_CLOSE_ALLOWED_RUNTIME_STALE_WITH_BROKER_EXPOSURE,
    }
    assert result["broad_flatten_allowed"] is False
    assert result["global_flatten_allowed"] is False
    assert result["live_money_eligible"] is False
    assert result["paper_proof_invoked"] is False


def test_stale_runtime_does_not_allow_close_if_not_exit_due() -> None:
    position = _position()
    position["classification"] = "OPEN_MANAGED_MATCHED"

    result = _classify(position=position)

    assert result["classification"] == RISK_REDUCING_CLOSE_BLOCKED_NOT_EXIT_DUE
    assert result["allowed"] is False


def test_stale_runtime_does_not_allow_close_if_owner_ambiguous() -> None:
    position = _position()
    position["owner_resolution_classification"] = "AMBIGUOUS_EXPOSURE_OWNERSHIP"

    result = _classify(position=position)

    assert result["classification"] == RISK_REDUCING_CLOSE_BLOCKED_OWNER_AMBIGUOUS
    assert result["allowed"] is False


def test_stale_runtime_does_not_allow_close_if_guardian_not_ready() -> None:
    guardian = _guardian()
    guardian["managed_close_authority"] = {
        "classification": "BROKER_POSITION_GUARDIAN_CLOSE_BLOCKED",
        "allowed": False,
        "reason_codes": ["REGISTRY_RECONCILIATION_NOT_MATCHED"],
        "candidates": [],
    }

    result = _classify(guardian=guardian)

    assert result["classification"] == RISK_REDUCING_CLOSE_BLOCKED_GUARDIAN_NOT_READY
    assert result["reason_codes"] == ["REGISTRY_RECONCILIATION_NOT_MATCHED"]


def test_stale_runtime_does_not_allow_close_if_open_order_conflicts() -> None:
    result = _classify(open_order_truth={"classification": "OPEN_ORDERS_PRESENT", "open_orders": [{"order_id": 9}]})

    assert result["classification"] == RISK_REDUCING_CLOSE_BLOCKED_CONFLICTING_OPEN_ORDER
    assert result["allowed"] is False


def test_close_is_rejected_if_qty_would_over_close_or_flip() -> None:
    guardian = _guardian()
    guardian["managed_close_authority"]["candidates"][0]["quantity"] = "2"

    result = _classify(guardian=guardian)

    assert result["classification"] == RISK_REDUCING_CLOSE_BLOCKED_WOULD_INCREASE_OR_FLIP_EXPOSURE
    assert "CLOSE_QUANTITY_NOT_EXACT_BROKER_QTY" in result["reason_codes"]


def test_close_is_rejected_if_con_id_account_or_local_symbol_mismatch() -> None:
    guardian = _guardian()
    guardian["managed_close_authority"]["candidates"][0]["con_id"] = 123

    result = _classify(guardian=guardian)

    assert result["classification"] == RISK_REDUCING_CLOSE_BLOCKED_OWNER_AMBIGUOUS
    assert result["reason_codes"] == ["GUARDIAN_CANDIDATE_OWNER_NOT_FOUND"]


def test_stale_owner_candidates_do_not_block_fresh_exact_close() -> None:
    registry = _managed_position_registry()
    registry["full_audit_positions"] = [
        {
            "classification": "AMBIGUOUS_EXPOSURE_OWNERSHIP",
            "diagnostic_only": True,
            "trade_id": "stale-trade",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
        }
    ]

    result = _classify(managed_position_registry=registry)

    assert result["classification"] == RISK_REDUCING_CLOSE_ALLOWED_RUNTIME_STALE_WITH_BROKER_EXPOSURE


def test_close_is_rejected_if_not_broker_backed() -> None:
    position = _position()
    position["lifecycle_position"]["entry_exec_id"] = ""
    position["lifecycle_position"]["entry_perm_id"] = ""
    position["lifecycle_position"]["entry_broker_identity"] = {}

    result = _classify(position=position)

    assert result["classification"] == RISK_REDUCING_CLOSE_BLOCKED_NOT_BROKER_BACKED


def _classify(
    *,
    control_plane_snapshot=None,
    guardian=None,
    managed_position_registry=None,
    position=None,
    managed_order_registry=None,
    open_order_truth=None,
) -> dict:
    registry = managed_position_registry or _managed_position_registry(position=position)
    return classify_runtime_stale_risk_reducing_close(
        control_plane_snapshot=control_plane_snapshot or _control_plane(),
        guardian=guardian or _guardian(),
        managed_position_registry=registry,
        managed_order_registry=managed_order_registry or _managed_order_registry(),
        open_order_truth=open_order_truth or {"classification": "NO_OPEN_ORDERS", "open_orders": []},
    )


def _control_plane() -> dict:
    return {
        "runtime_authority_exposure_classification": "RUNTIME_AUTHORITY_STALE_WITH_BROKER_EXPOSURE",
        "runtime_authority_stale_with_broker_exposure": True,
        "submit_authority": False,
    }


def _managed_position_registry(*, position=None) -> dict:
    return {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "managed_positions": [position or _position()],
    }


def _position() -> dict:
    return {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "trade_id": "trade-1",
        "lifecycle_id": "lifecycle-1",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
        "side": "SHORT",
        "broker_position": {
            "account_id": "DUM882026",
            "symbol": "MNQ",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "quantity": "-1",
        },
        "lifecycle_position": {
            "entry_exec_id": "0000e1a7.test.01.01",
            "entry_perm_id": 1421894440,
            "entry_broker_identity": {
                "exec_id": "0000e1a7.test.01.01",
                "perm_id": 1421894440,
            },
        },
    }


def _managed_order_registry() -> dict:
    return {
        "classification": "POSITION_WITHOUT_CLOSE_ORDER",
        "managed_orders": [
            {
                "classification": "POSITION_WITHOUT_CLOSE_ORDER",
                "trade_id": "trade-1",
                "lifecycle_id": "lifecycle-1",
                "action": "BUY",
                "quantity": "1",
                "working": False,
            }
        ],
    }


def _guardian() -> dict:
    return {
        "classification": "BROKER_POSITION_GUARDIAN_READY",
        "managed_close_authority": {
            "classification": "BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING",
            "allowed": True,
            "reason_codes": [],
            "candidates": [
                {
                    "classification": "BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING",
                    "trade_id": "trade-1",
                    "lifecycle_id": "lifecycle-1",
                    "account_id": "DUM882026",
                    "symbol": "MNQ",
                    "local_symbol": "MNQM6",
                    "con_id": 770561201,
                    "action": "BUY",
                    "quantity": "1",
                }
            ],
        },
    }
