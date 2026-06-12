from __future__ import annotations

from datetime import datetime, timezone

from mgc_v05l.execution_core.track_b_broker_market_truth_entry_authority import (
    BROKER_MARKET_TRUTH_ENTRY_ALLOWED,
    BROKER_MARKET_TRUTH_ENTRY_BLOCKED,
    BrokerMarketTruthEntryAuthorityInput,
    evaluate_broker_market_truth_entry_authority,
)


ACTIVE_LANES = (
    "mnq_us_active_participation_long",
    "mnq_us_active_participation_short",
    "mes_us_active_participation_long",
    "mes_us_active_participation_short",
    "mnq_globex_active_participation_long",
    "mnq_globex_active_participation_short",
    "mes_globex_active_participation_long",
    "mes_globex_active_participation_short",
    "mnq_london_open_active_participation_long",
    "mnq_london_open_active_participation_short",
    "mes_london_open_active_participation_long",
    "mes_london_open_active_participation_short",
    "mnq_london_late_active_participation_short",
)


def _positions(*, rows: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "ok": True,
        "account": "DUM882026",
        "positions": list(rows or []),
    }


def _orders(*, rows: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "ok": True,
        "account": "DUM882026",
        "open_orders_complete": True,
        "open_orders": list(rows or []),
    }


def _input(**overrides: object) -> BrokerMarketTruthEntryAuthorityInput:
    payload = {
        "account_id": "DUM882026",
        "mode": "PAPER",
        "route_destination": "ibkr_paper_bridge_submit_capable",
        "execution_mode": "IBKR_PAPER_BRIDGE",
        "lane_id": "mnq_us_active_participation_long",
        "instrument": "MNQ",
        "action": "BUY",
        "quantity": 1.0,
        "active_profile_lane_ids": ACTIVE_LANES,
        "broker_positions_snapshot": _positions(),
        "broker_open_orders_snapshot": _orders(),
        "open_order_truth": {"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
        "runtime_price": {
            "price": 29500.25,
            "timestamp": "2026-06-11T14:00:00+00:00",
        },
        "contract": {
            "symbol": "MNQ",
            "contract_month": "202609",
            "expiry": "20260918",
            "local_symbol": "MNQU6",
            "con_id": 793356225,
        },
        "now": datetime(2026, 6, 11, 14, 1, tzinfo=timezone.utc),
    }
    payload.update(overrides)
    return BrokerMarketTruthEntryAuthorityInput(**payload)


def _classification(**overrides: object) -> tuple[str, list[str]]:
    result = evaluate_broker_market_truth_entry_authority(_input(**overrides))
    return str(result["classification"]), list(result["block_reasons"])


def test_all_active_profile_lanes_are_allowed_with_clean_broker_and_market_truth() -> None:
    for lane_id in ACTIVE_LANES:
        instrument = "MES" if lane_id.startswith("mes_") else "MNQ"
        local_symbol = "MESU6" if instrument == "MES" else "MNQU6"
        con_id = 793356194 if instrument == "MES" else 793356225
        result = evaluate_broker_market_truth_entry_authority(
            _input(
                lane_id=lane_id,
                instrument=instrument,
                contract={
                    "symbol": instrument,
                    "contract_month": "202609",
                    "expiry": "20260918",
                    "local_symbol": local_symbol,
                    "con_id": con_id,
                },
            )
        )

        assert result["classification"] == BROKER_MARKET_TRUTH_ENTRY_ALLOWED
        assert result["block_reasons"] == []


def test_diagnostic_subsystem_disagreements_do_not_block_clean_broker_market_truth() -> None:
    result = evaluate_broker_market_truth_entry_authority(
        _input(
            diagnostics={
                "cached_governance": {"submit_allowed": False, "block_reasons": ["lane_not_yet_submit_ported"]},
                "reconciliation": {"classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT"},
                "registry": {"classification": "CURRENT_HOT_PATH_EXPOSURE_BLOCKED"},
            }
        )
    )

    assert result["classification"] == BROKER_MARKET_TRUTH_ENTRY_ALLOWED
    assert result["diagnostics"]["cached_governance"]["submit_allowed"] is False


def test_broker_nonflat_flat_start_blocks() -> None:
    classification, reasons = _classification(
        broker_positions_snapshot=_positions(
            rows=[
                {
                    "account_id": "DUM882026",
                    "security_type": "FUT",
                    "symbol": "MNQ",
                    "local_symbol": "MNQU6",
                    "quantity": "-1",
                }
            ]
        )
    )

    assert classification == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert "broker_nonflat_flat_start_violation" in reasons


def test_unknown_order_blocks() -> None:
    classification, reasons = _classification(open_order_truth={"unknown_open_order_count": 1})

    assert classification == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert "unknown_open_orders" in reasons


def test_missing_price_blocks() -> None:
    classification, reasons = _classification(runtime_price={})

    assert classification == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert "runtime_price_unavailable" in reasons


def test_unresolved_contract_blocks() -> None:
    classification, reasons = _classification(contract={"symbol": "MNQ", "contract_month": "202609"})

    assert classification == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert "unresolved_con_id" in reasons


def test_wrong_account_live_proof_and_non_paper_block() -> None:
    classification, reasons = _classification(
        account_id="DU_BAD",
        mode="LIVE",
        paper_only=False,
        live_money_eligible=True,
        paper_proof=True,
    )

    assert classification == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert {
        "wrong_account",
        "non_paper_mode",
        "paper_only_false",
        "live_money_eligible",
        "paper_proof_true",
    }.issubset(set(reasons))
