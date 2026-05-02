from __future__ import annotations

from datetime import datetime, timezone

import pytest

from mgc_v05l.execution_core.ibkr_paper_adapter import (
    IbkrPaperAdapter,
    IbkrPaperConfigError,
    IbkrPaperCorrelationError,
    IbkrPaperReadinessError,
    IbkrPaperSubmitDisabledError,
)
from mgc_v05l.execution_core.models import IntentKind, OrderIntent, SubmitAttempt, SubmitAttemptState


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def allowlist() -> dict[str, dict[str, str]]:
    return {
        "MGC-202606": {
            "symbol": "MGC",
            "security_type": "FUT",
            "exchange": "COMEX",
            "currency": "USD",
            "local_symbol": "MGCM6",
            "con_id": "12345",
            "tick_size": "0.1",
        }
    }


def adapter(**overrides: object) -> IbkrPaperAdapter:
    kwargs = {
        "mode": "PAPER",
        "host": "127.0.0.1",
        "port": 7497,
        "client_id": 77,
        "account_id": "DU1234567",
        "contract_allowlist": allowlist(),
    }
    kwargs.update(overrides)
    return IbkrPaperAdapter(**kwargs)


def order_intent(**overrides: object) -> OrderIntent:
    kwargs = {
        "order_intent_id": "intent-1",
        "signal_event_id": "signal-1",
        "run_id": "run-1",
        "intent_kind": IntentKind.OPEN,
        "account_id": "DU1234567",
        "symbol": "MGC",
        "contract_key": "MGC-202606",
        "action": "BUY",
        "quantity": 1,
        "order_type": "LMT",
        "limit_price": "2345.2",
        "time_in_force": "DAY",
        "paper_only": True,
        "created_at": aware_now(),
        "reason": "test",
    }
    kwargs.update(overrides)
    return OrderIntent(**kwargs)


def submit_attempt(**overrides: object) -> SubmitAttempt:
    kwargs = {
        "submit_attempt_id": "submit-1",
        "order_intent_id": "intent-1",
        "run_id": "run-1",
        "account_id": "DU1234567",
        "broker": "IBKR",
        "environment": {"mode": "PAPER", "host": "127.0.0.1", "port": 7497, "client_id": 77},
        "pre_submit_reconciliation_id": "recon-1",
        "open_order_baseline_event_id": "open-orders-1",
        "request_digest": "digest-1",
        "state": SubmitAttemptState.CREATED,
        "submitted_at": aware_now(),
        "broker_order_id": "1001",
        "perm_id": "9001",
    }
    kwargs.update(overrides)
    return SubmitAttempt(**kwargs)


@pytest.mark.parametrize("mode", ["LIVE", "", "PRODUCTION"])
def test_adapter_rejects_non_paper_mode(mode: str) -> None:
    with pytest.raises(IbkrPaperConfigError, match="mode must be PAPER"):
        adapter(mode=mode)


def test_adapter_rejects_non_localhost_host() -> None:
    with pytest.raises(IbkrPaperConfigError, match="host must be 127.0.0.1"):
        adapter(host="localhost")


@pytest.mark.parametrize("port", [7496, 4001, 4002])
def test_adapter_rejects_non_paper_ports(port: int) -> None:
    with pytest.raises(IbkrPaperConfigError, match="port must be 7497"):
        adapter(port=port)


def test_adapter_rejects_missing_account_id() -> None:
    with pytest.raises(IbkrPaperConfigError, match="account_id is required"):
        adapter(account_id="")


def test_adapter_rejects_implicit_first_account_selection() -> None:
    paper = adapter()
    paper.record_managed_accounts(("DU1234567", "DU7654321"))

    with pytest.raises(IbkrPaperReadinessError, match="implicit first-account selection"):
        paper.require_account(None)


def test_managed_account_exact_match_passes() -> None:
    paper = adapter()
    paper.record_managed_accounts(("DU1234567", "DU7654321"))

    assert paper.require_configured_account() == "DU1234567"


def test_managed_account_mismatch_fails() -> None:
    paper = adapter()
    paper.record_managed_accounts(("DU7654321",))

    with pytest.raises(IbkrPaperReadinessError, match="configured paper account"):
        paper.require_configured_account()


def test_missing_next_valid_id_fails_readiness() -> None:
    paper = adapter()
    paper.record_managed_accounts(("DU1234567",))

    with pytest.raises(IbkrPaperReadinessError, match="missing nextValidId"):
        paper.require_ready()
    assert paper.readiness_report()["next_valid_id_ready"] is False


def test_callback_mapping_creates_broker_order_without_synthetic_fill() -> None:
    paper = adapter()
    paper.register_submit_context(submit_attempt=submit_attempt(), order_intent=order_intent(), created_at=aware_now())

    order = paper.map_order_callback(
        submit_attempt_id="submit-1",
        account_id="DU1234567",
        broker_order_id="1001",
        perm_id="9001",
        client_id=77,
        contract_key="MGC-202606",
        action="BUY",
        quantity=1,
        order_type="LMT",
        limit_price="2345.2",
        status="Submitted",
        filled_quantity=0,
        remaining_quantity=1,
        average_fill_price=None,
        observed_at=aware_now(),
        raw={"callback": "openOrder/orderStatus"},
    )

    assert order.broker_order_id == "1001"
    assert order.status == "Submitted"
    assert paper.snapshot_open_orders(contract_key="MGC-202606") == (order,)


def test_exec_details_mapping_creates_fill_only_with_valid_correlation() -> None:
    paper = adapter()
    paper.register_submit_context(submit_attempt=submit_attempt(), order_intent=order_intent(), created_at=aware_now())

    fill = paper.map_exec_details(
        submit_attempt_id="submit-1",
        account_id="DU1234567",
        broker_order_id="1001",
        perm_id="9001",
        execution_id="exec-1",
        contract_key="MGC-202606",
        action="BUY",
        quantity=1,
        price="2345.2",
        filled_at=aware_now(),
        raw={"callback": "execDetails"},
    )

    assert fill.fill_event_id == "ibkr_fill_submit-1_exec-1"
    assert fill.order_intent_id == "intent-1"


def test_account_mismatch_marks_submit_context_ambiguous() -> None:
    paper = adapter()
    paper.register_submit_context(submit_attempt=submit_attempt(), order_intent=order_intent(), created_at=aware_now())

    with pytest.raises(IbkrPaperCorrelationError, match="account mismatch"):
        paper.map_exec_details(
            submit_attempt_id="submit-1",
            account_id="OTHER",
            broker_order_id="1001",
            perm_id="9001",
            execution_id="exec-1",
            contract_key="MGC-202606",
            action="BUY",
            quantity=1,
            price="2345.2",
            filled_at=aware_now(),
        )
    assert paper.ambiguous_contexts["submit-1"] == "account mismatch"


def test_contract_mismatch_fails_correlation() -> None:
    paper = adapter(contract_allowlist={**allowlist(), "MNQ-202606": {"symbol": "MNQ", "tick_size": "0.25"}})
    paper.register_submit_context(submit_attempt=submit_attempt(), order_intent=order_intent(), created_at=aware_now())

    with pytest.raises(IbkrPaperCorrelationError, match="contract mismatch"):
        paper.map_exec_details(
            submit_attempt_id="submit-1",
            account_id="DU1234567",
            broker_order_id="1001",
            perm_id="9001",
            execution_id="exec-1",
            contract_key="MNQ-202606",
            action="BUY",
            quantity=1,
            price="2345.2",
            filled_at=aware_now(),
        )


def test_position_callback_maps_to_track_b_position_state() -> None:
    paper = adapter()

    position = paper.record_position_callback(
        run_id="run-1",
        account_id="DU1234567",
        contract_key="MGC-202606",
        signed_quantity=1,
        average_price="2345.2",
        observed_at=aware_now(),
        raw={"callback": "position"},
    )

    assert position.signed_quantity == 1
    assert paper.snapshot_position(contract_key="MGC-202606") == position


def test_quote_observation_requires_exact_allowlisted_contract() -> None:
    paper = adapter()

    quote = paper.observe_quote(
        run_id="run-1",
        contract_key="MGC-202606",
        bid="2345.0",
        ask="2345.1",
        last="2345.05",
        observed_at=aware_now(),
    )

    assert quote.source == "ibkr_paper_adapter"
    with pytest.raises(IbkrPaperConfigError, match="allowlisted"):
        paper.observe_quote(
            run_id="run-1",
            contract_key="GC-202606",
            bid="2345.0",
            ask="2345.1",
            last="2345.05",
            observed_at=aware_now(),
        )


def test_submit_limit_order_fails_closed_when_disabled() -> None:
    paper = adapter(submit_enabled=False)

    with pytest.raises(IbkrPaperSubmitDisabledError, match="submit_enabled"):
        paper.submit_limit_order(submit_attempt=submit_attempt(), order_intent=order_intent())
