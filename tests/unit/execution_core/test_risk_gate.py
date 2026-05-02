from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mgc_v05l.execution_core.models import (
    IntentKind,
    OrderIntent,
    PositionSource,
    PositionState,
    ReconciliationResult,
    ReconciliationStage,
    ReconciliationStatus,
)
from mgc_v05l.execution_core.risk_gate import evaluate_order_gate


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def order_intent(**overrides: object) -> OrderIntent:
    kwargs = {
        "order_intent_id": "intent-1",
        "signal_event_id": "sig-1",
        "run_id": "run-1",
        "intent_kind": IntentKind.OPEN,
        "account_id": "DUM123",
        "symbol": "MGC",
        "contract_key": "MGC-202606",
        "action": "BUY",
        "quantity": 1,
        "order_type": "LMT",
        "limit_price": Decimal("2345.1"),
        "time_in_force": "DAY",
        "paper_only": True,
        "created_at": aware_now(),
        "reason": "proof",
    }
    kwargs.update(overrides)
    return OrderIntent(**kwargs)


def position(quantity: int, *, account_id: str = "DUM123", open_order_ids: tuple[str, ...] = ()) -> PositionState:
    return PositionState(
        position_state_id=f"broker-pos-{quantity}",
        run_id="run-1",
        source=PositionSource.BROKER,
        account_id=account_id,
        contract_key="MGC-202606",
        signed_quantity=quantity,
        average_price=None if quantity == 0 else Decimal("2345.1"),
        open_order_ids=open_order_ids,
        observed_at=aware_now(),
    )


def reconciliation(stage: ReconciliationStage, status: ReconciliationStatus = ReconciliationStatus.CLEAN) -> ReconciliationResult:
    return ReconciliationResult(
        reconciliation_id=f"recon-{stage.value}",
        run_id="run-1",
        stage=stage,
        status=status,
        account_id="DUM123",
        contract_key="MGC-202606",
        expected_signed_quantity=0 if stage == ReconciliationStage.PRE_OPEN else 1,
        broker_position_state_id="broker-pos",
        ledger_position_state_id="ledger-pos",
        broker_open_order_ids=(),
        ledger_open_order_ids=(),
        fill_event_ids=(),
        issues=(),
        required_action="No action required.",
        created_at=aware_now(),
    )


def base_gate_kwargs() -> dict[str, object]:
    return {
        "order_intent": order_intent(),
        "environment": {"mode": "PAPER", "host": "127.0.0.1", "port": 7497},
        "configured_account_id": "DUM123",
        "contract_allowlist": {"MGC-202606": {"symbol": "MGC"}},
        "broker_position": position(0),
        "broker_open_orders": (),
        "reconciliation": reconciliation(ReconciliationStage.PRE_OPEN),
        "created_at": aware_now(),
    }


def test_open_gate_passes_when_all_preconditions_are_clean() -> None:
    decision = evaluate_order_gate(**base_gate_kwargs())

    assert decision.passed is True
    assert decision.blocking_reason is None


@pytest.mark.parametrize(
    ("override", "expected_reason"),
    [
        ({"environment": {"mode": "LIVE", "host": "127.0.0.1", "port": 7497}}, "mode must be PAPER"),
        ({"environment": {"mode": "PAPER", "host": "localhost", "port": 7497}}, "host must be 127.0.0.1"),
        ({"environment": {"mode": "PAPER", "host": "127.0.0.1", "port": 7496}}, "port must be 7497"),
        ({"configured_account_id": ""}, "configured account_id is required"),
        ({"contract_allowlist": {}}, "must be explicitly allowlisted"),
        ({"broker_position": position(0, open_order_ids=("1001",))}, "broker open orders must be empty"),
        ({"broker_position": position(1)}, "broker must be flat before an open intent"),
        ({"reconciliation": reconciliation(ReconciliationStage.PRE_OPEN, ReconciliationStatus.BLOCKED)}, "reconciliation must be CLEAN"),
        ({"order_intent": order_intent(extra_fields={"parent_id": "1"})}, "bracket/OCO/parent/child/algo"),
    ],
)
def test_open_gate_fails_closed_for_blocking_conditions(override: dict[str, object], expected_reason: str) -> None:
    kwargs = base_gate_kwargs()
    kwargs.update(override)

    decision = evaluate_order_gate(**kwargs)

    assert decision.passed is False
    assert expected_reason in str(decision.blocking_reason)


def test_close_gate_requires_expected_one_lot_position() -> None:
    kwargs = base_gate_kwargs()
    kwargs.update(
        {
            "order_intent": order_intent(intent_kind=IntentKind.CLOSE, action="SELL"),
            "broker_position": position(0),
            "reconciliation": reconciliation(ReconciliationStage.PRE_CLOSE),
        }
    )

    decision = evaluate_order_gate(**kwargs)

    assert decision.passed is False
    assert "exact 1-lot position" in str(decision.blocking_reason)

