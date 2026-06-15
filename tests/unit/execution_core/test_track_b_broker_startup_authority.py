from __future__ import annotations

from mgc_v05l.execution_core.track_b_broker_startup_authority import (
    BROKER_TRUTH_NOT_STARTUP_CLEAN,
    FRESH_COMPLETE_CLEAN_BROKER_TRUTH,
    classify_fresh_complete_clean_broker_truth,
)


def test_clean_fresh_broker_truth_downgrades_stale_derived_artifacts() -> None:
    authority = classify_fresh_complete_clean_broker_truth(
        broker_truth_status=_status(),
        positions_snapshot=_positions(),
        open_orders_snapshot=_orders(),
        reconciliation={"classification": "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED", "broker_reconciled": False},
        open_order_truth={"classification": "ORDER_TRUTH_STALE"},
        expected_account_id="DUM882026",
    )

    assert authority.broker_truth_clean is True
    assert authority.classification == FRESH_COMPLETE_CLEAN_BROKER_TRUTH
    assert authority.blockers == ()
    assert "reconciliation_classification:TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED" in authority.diagnostics
    assert "open_order_truth_classification:ORDER_TRUTH_STALE" in authority.diagnostics


def test_actual_broker_position_blocks_startup_authority() -> None:
    positions = _positions()
    positions["positions"][0]["quantity"] = "1"

    authority = classify_fresh_complete_clean_broker_truth(
        broker_truth_status=_status(),
        positions_snapshot=positions,
        open_orders_snapshot=_orders(),
        expected_account_id="DUM882026",
    )

    assert authority.classification == BROKER_TRUTH_NOT_STARTUP_CLEAN
    assert "track_b_futures_positions_present" in authority.blockers


def test_actual_open_order_blocks_startup_authority() -> None:
    orders = _orders(open_orders=[{"account_id": "DUM882026", "symbol": "MNQ"}])

    authority = classify_fresh_complete_clean_broker_truth(
        broker_truth_status=_status(open_order_count=1),
        positions_snapshot=_positions(),
        open_orders_snapshot=orders,
        expected_account_id="DUM882026",
    )

    assert "broker_open_orders_present" in authority.blockers


def test_unknown_order_blocks_startup_authority() -> None:
    authority = classify_fresh_complete_clean_broker_truth(
        broker_truth_status=_status(unknown_open_order_count=1),
        positions_snapshot=_positions(),
        open_orders_snapshot=_orders(),
        expected_account_id="DUM882026",
    )

    assert "unknown_open_orders_present" in authority.blockers


def test_incomplete_broker_truth_blocks_startup_authority() -> None:
    authority = classify_fresh_complete_clean_broker_truth(
        broker_truth_status=_status(fresh=False, positions_complete=False),
        positions_snapshot=_positions(),
        open_orders_snapshot=_orders(),
        expected_account_id="DUM882026",
    )

    assert "broker_truth_not_fresh" in authority.blockers
    assert "broker_positions_incomplete" in authority.blockers


def test_live_money_or_paper_proof_blocks_startup_authority() -> None:
    live = classify_fresh_complete_clean_broker_truth(
        broker_truth_status=_status(live_money_eligible=True),
        positions_snapshot=_positions(),
        open_orders_snapshot=_orders(),
        expected_account_id="DUM882026",
    )
    proof = classify_fresh_complete_clean_broker_truth(
        broker_truth_status=_status(paper_proof_invoked=True),
        positions_snapshot=_positions(),
        open_orders_snapshot=_orders(),
        expected_account_id="DUM882026",
    )

    assert "live_money_eligible" in live.blockers
    assert "paper_proof_invoked" in proof.blockers


def test_account_identity_mismatch_blocks_startup_authority() -> None:
    authority = classify_fresh_complete_clean_broker_truth(
        broker_truth_status=_status(account="OTHER"),
        positions_snapshot=_positions(account="OTHER"),
        open_orders_snapshot=_orders(account="OTHER"),
        expected_account_id="DUM882026",
    )

    assert "broker_account_not_confirmed" in authority.blockers


def _status(
    *,
    account: str = "DUM882026",
    fresh: bool = True,
    positions_complete: bool = True,
    open_orders_complete: bool = True,
    open_order_count: int = 0,
    unknown_open_order_count: int = 0,
    live_money_eligible: bool = False,
    paper_proof_invoked: bool = False,
) -> dict:
    return {
        "account": account,
        "fresh": fresh,
        "positions_complete": positions_complete,
        "open_orders_complete": open_orders_complete,
        "open_order_count": open_order_count,
        "unknown_open_order_count": unknown_open_order_count,
        "live_money_eligible": live_money_eligible,
        "paper_proof_invoked": paper_proof_invoked,
    }


def _positions(*, account: str = "DUM882026") -> dict:
    return {
        "selected_account_id": account,
        "positions": [
            {"account_id": account, "security_type": "FUT", "symbol": "MNQ", "local_symbol": "MNQU6", "quantity": "0"},
            {"account_id": account, "security_type": "FUT", "symbol": "MES", "local_symbol": "MESU6", "quantity": "0"},
            {"account_id": account, "security_type": "FUT", "symbol": "MGC", "local_symbol": "MGCQ6", "quantity": "0"},
            {"account_id": account, "security_type": "STK", "symbol": "AAPL", "local_symbol": "AAPL", "quantity": "900"},
        ],
    }


def _orders(*, account: str = "DUM882026", open_orders: list[dict] | None = None) -> dict:
    return {
        "selected_account_id": account,
        "open_orders": open_orders or [],
        "open_order_count": len(open_orders or []),
    }
