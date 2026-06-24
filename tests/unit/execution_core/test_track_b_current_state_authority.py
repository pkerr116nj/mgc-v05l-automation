from __future__ import annotations

from datetime import datetime, timezone

from mgc_v05l.execution_core.track_b_current_state_authority import (
    CURRENT_STATE_AUTHORITY_ALLOWED,
    CURRENT_STATE_AUTHORITY_BLOCKED,
    CurrentStateAuthorityInput,
    evaluate_current_state_authority,
    open_order_truth_duplicate_close_group_count,
    open_order_truth_is_global_no_open_orders,
    open_order_truth_open_order_count,
)


NOW = datetime(2026, 6, 18, 14, 0, tzinfo=timezone.utc)


def _positions(*, rows: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {"ok": True, "account": "DUM882026", "positions": list(rows or [])}


def _orders(*, rows: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {"ok": True, "account": "DUM882026", "open_orders_complete": True, "open_orders": list(rows or [])}


def _input(**overrides: object) -> CurrentStateAuthorityInput:
    payload = {
        "account_id": "DUM882026",
        "instrument": "MGC",
        "action": "BUY_TO_OPEN",
        "quantity": 1.0,
        "broker_positions_snapshot": _positions(),
        "broker_open_orders_snapshot": _orders(),
        "open_order_truth": {"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
        "runtime_price": {"price": 3400.5, "timestamp": "2026-06-18T13:59:00+00:00"},
        "contract": {
            "symbol": "MGC",
            "contract_month": "202608",
            "expiry": "20260827",
            "local_symbol": "MGCQ6",
            "con_id": 123456789,
        },
        "now": NOW,
        "diagnostics": {
            "stale_reconciliation": {"classification": "BROKER_LIFECYCLE_RECONCILIATION_NOT_CLEAN"},
            "stale_safe_state": {"classification": "SAFE_STATE_HARD_HOLD"},
        },
    }
    payload.update(overrides)
    return CurrentStateAuthorityInput(**payload)


def _classification(**overrides: object) -> tuple[str, list[str]]:
    result = evaluate_current_state_authority(_input(**overrides))
    return str(result["classification"]), list(result["block_reasons"])


def test_clean_current_truth_allows_even_when_deprecated_artifacts_disagree() -> None:
    result = evaluate_current_state_authority(_input())

    assert result["classification"] == CURRENT_STATE_AUTHORITY_ALLOWED
    assert result["block_reasons"] == []
    assert result["diagnostics"]["stale_safe_state"]["classification"] == "SAFE_STATE_HARD_HOLD"


def test_clean_current_truth_allows_rates_and_non_rates_symbols() -> None:
    contracts = {
        "GC": ("GCQ6", 111111101, 3400.5),
        "MGC": ("MGCQ6", 111111102, 3400.5),
        "NQ": ("NQU6", 111111103, 22000.25),
        "ES": ("ESU6", 111111104, 5900.25),
        "MES": ("MESU6", 111111105, 5900.25),
        "ZT": ("ZTU6", 111111106, 103.1875),
        "ZF": ("ZFU6", 111111107, 106.75),
        "ZN": ("ZNU6", 111111108, 110.5),
        "ZB": ("ZBU6", 111111109, 117.25),
    }

    for symbol, (local_symbol, con_id, price) in contracts.items():
        result = evaluate_current_state_authority(
            _input(
                instrument=symbol,
                runtime_price={"price": price, "timestamp": "2026-06-18T13:59:00+00:00"},
                contract={
                    "symbol": symbol,
                    "contract_month": "202609",
                    "expiry": "20260930",
                    "local_symbol": local_symbol,
                    "con_id": con_id,
                },
            )
        )

        assert result["classification"] == CURRENT_STATE_AUTHORITY_ALLOWED
        assert result["block_reasons"] == []


def test_same_instrument_broker_exposure_blocks_flat_start() -> None:
    classification, reasons = _classification(
        broker_positions_snapshot=_positions(
            rows=[
                {
                    "account_id": "DUM882026",
                    "security_type": "FUT",
                    "symbol": "MGC",
                    "local_symbol": "MGCQ6",
                    "quantity": "1",
                }
            ]
        )
    )

    assert classification == CURRENT_STATE_AUTHORITY_BLOCKED
    assert "broker_nonflat_flat_start_violation" in reasons


def test_other_instrument_broker_exposure_does_not_block_instrument_scoped_flat_start() -> None:
    classification, reasons = _classification(
        broker_positions_snapshot=_positions(
            rows=[
                {
                    "account_id": "DUM882026",
                    "security_type": "FUT",
                    "symbol": "GC",
                    "local_symbol": "GCQ6",
                    "quantity": "1",
                }
            ]
        )
    )

    assert classification == CURRENT_STATE_AUTHORITY_ALLOWED
    assert "broker_nonflat_flat_start_violation" not in reasons


def test_real_track_b_open_order_blocks() -> None:
    classification, reasons = _classification(
        broker_open_orders_snapshot=_orders(
            rows=[
                {
                    "account_id": "DUM882026",
                    "security_type": "FUT",
                    "symbol": "MGC",
                    "local_symbol": "MGCQ6",
                    "order_id": "77",
                }
            ]
        )
    )

    assert classification == CURRENT_STATE_AUTHORITY_BLOCKED
    assert "duplicate_or_conflicting_working_order" in reasons


def test_rates_open_order_with_only_local_symbol_is_track_b_scoped() -> None:
    classification, reasons = _classification(
        instrument="ZT",
        runtime_price={"price": 103.1875, "timestamp": "2026-06-18T13:59:00+00:00"},
        contract={
            "symbol": "ZT",
            "contract_month": "202609",
            "expiry": "20260930",
            "local_symbol": "ZTU6",
            "con_id": 842590372,
        },
        broker_open_orders_snapshot=_orders(
            rows=[
                {
                    "account_id": "DUM882026",
                    "security_type": "FUT",
                    "local_symbol": "ZTU6",
                    "order_id": "88",
                }
            ]
        ),
    )

    assert classification == CURRENT_STATE_AUTHORITY_BLOCKED
    assert "duplicate_or_conflicting_working_order" in reasons


def test_unknown_orders_block() -> None:
    classification, reasons = _classification(open_order_truth={"unknown_open_order_count": 1})

    assert classification == CURRENT_STATE_AUTHORITY_BLOCKED
    assert "unknown_open_orders" in reasons


def test_global_no_open_order_truth_helper_requires_clean_complete_scope() -> None:
    clean = {
        "classification": "NO_OPEN_ORDERS",
        "canonical_refresh_scope": "GLOBAL_COMPLETE",
        "broker_open_orders": [],
        "unknown_open_orders": [],
        "duplicate_close_order_groups": [],
    }

    assert open_order_truth_is_global_no_open_orders(clean) is True
    assert open_order_truth_open_order_count(clean) == 0
    assert open_order_truth_duplicate_close_group_count(clean) == 0
    assert open_order_truth_is_global_no_open_orders({**clean, "canonical_refresh_scope": "PARTIAL_DIAGNOSTIC"}) is False
    assert open_order_truth_is_global_no_open_orders(
        {**clean, "broker_open_orders": [{"local_symbol": "METU6", "order_id": 328}]}
    ) is False
    assert open_order_truth_is_global_no_open_orders(
        {**clean, "duplicate_close_order_groups": [{"local_symbol": "METU6", "order_ids": [1, 2]}]}
    ) is False


def test_unresolved_identity_and_stale_price_block() -> None:
    classification, reasons = _classification(
        runtime_price={"price": 3400.5, "timestamp": "2026-06-18T13:40:00+00:00"},
        contract={"symbol": "MGC", "contract_month": "202608"},
    )

    assert classification == CURRENT_STATE_AUTHORITY_BLOCKED
    assert "runtime_price_stale" in reasons
    assert "unresolved_con_id" in reasons


def test_live_money_or_paper_proof_blocks() -> None:
    classification, reasons = _classification(live_money_eligible=True, paper_proof=True)

    assert classification == CURRENT_STATE_AUTHORITY_BLOCKED
    assert "live_money_eligible" in reasons
    assert "paper_proof_true" in reasons
