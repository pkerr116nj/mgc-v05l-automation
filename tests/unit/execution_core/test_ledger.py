from __future__ import annotations

from datetime import datetime, timezone

import pytest

from mgc_v05l.execution_core.ledger import JsonlLedger, LedgerError
from mgc_v05l.execution_core.models import CancelAttempt, FillEvent


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def fill(fill_id: str, action: str, execution_id: str, price: str) -> FillEvent:
    return FillEvent(
        fill_event_id=fill_id,
        run_id="run-1",
        submit_attempt_id=f"submit-{fill_id}",
        order_intent_id=f"intent-{fill_id}",
        account_id="DUM123",
        broker_order_id=f"order-{fill_id}",
        perm_id="perm-1",
        execution_id=execution_id,
        contract_key="MGC-202606",
        action=action,
        quantity=1,
        price=price,
        filled_at=aware_now(),
    )


def test_ledger_appends_envelopes_and_replays_flat_position(tmp_path) -> None:  # type: ignore[no-untyped-def]
    ledger = JsonlLedger(tmp_path / "events.jsonl")

    first = ledger.append_model_event(event_type="fill_event_created", model=fill("open", "BUY", "exec-open", "2345.1"))
    second = ledger.append_model_event(event_type="fill_event_created", model=fill("close", "SELL", "exec-close", "2345.5"))

    events = ledger.read_events(run_id="run-1")
    position = ledger.replay_position(run_id="run-1", account_id="DUM123", contract_key="MGC-202606")

    assert [event.sequence for event in events] == [1, 2]
    assert first.payload_sha256
    assert second.payload_sha256
    assert position.signed_quantity == 0
    assert position.average_price is None
    assert position.source_event_ids == (first.event_id, second.event_id)


def test_ledger_records_durable_cancel_attempt(tmp_path) -> None:  # type: ignore[no-untyped-def]
    ledger = JsonlLedger(tmp_path / "events.jsonl")
    cancel = CancelAttempt(
        cancel_attempt_id="cancel-1",
        run_id="run-1",
        submit_attempt_id="submit-1",
        broker_order_id="1001",
        perm_id="perm-1",
        account_id="DUM123",
        contract_key="MGC-202606",
        requested_at=aware_now(),
        observed_cancel_status="Cancelled",
        confirmed_at=aware_now(),
        raw={"status": "Cancelled"},
    )

    event = ledger.append_cancel_attempt(cancel)

    assert event.event_type == "cancel_attempt_created"
    assert event.correlation_id == "submit-1"
    assert ledger.read_events()[0].payload["cancel_attempt_id"] == "cancel-1"


def test_ledger_identity_validation_rejects_duplicate_order_intent_id(tmp_path) -> None:  # type: ignore[no-untyped-def]
    ledger = JsonlLedger(tmp_path / "events.jsonl")
    payload = {"run_id": "run-1", "order_intent_id": "intent-1"}
    ledger.append_event(run_id="run-1", event_type="order_intent_created", payload=payload)
    ledger.append_event(run_id="run-1", event_type="order_intent_created", payload=payload)

    with pytest.raises(LedgerError, match="Duplicate order_intent_id"):
        ledger.validate_identity_uniqueness(run_id="run-1")


def test_ledger_identity_validation_rejects_duplicate_submit_attempt_id(tmp_path) -> None:  # type: ignore[no-untyped-def]
    ledger = JsonlLedger(tmp_path / "events.jsonl")
    payload = {"run_id": "run-1", "submit_attempt_id": "submit-1"}
    ledger.append_event(run_id="run-1", event_type="submit_attempt_created", payload=payload)
    ledger.append_event(run_id="run-1", event_type="submit_attempt_created", payload=payload)

    with pytest.raises(LedgerError, match="Duplicate submit_attempt_id"):
        ledger.validate_identity_uniqueness(run_id="run-1")


def test_ledger_identity_validation_rejects_two_submits_for_one_intent(tmp_path) -> None:  # type: ignore[no-untyped-def]
    ledger = JsonlLedger(tmp_path / "events.jsonl")
    ledger.append_event(
        run_id="run-1",
        event_type="submit_attempt_created",
        payload={"run_id": "run-1", "order_intent_id": "intent-1", "submit_attempt_id": "submit-1"},
    )
    ledger.append_event(
        run_id="run-1",
        event_type="submit_attempt_created",
        payload={"run_id": "run-1", "order_intent_id": "intent-1", "submit_attempt_id": "submit-2"},
    )

    with pytest.raises(LedgerError, match="active submit_attempt for order_intent_id"):
        ledger.validate_identity_uniqueness(run_id="run-1")


def test_ledger_identity_validation_rejects_duplicate_execution_id(tmp_path) -> None:  # type: ignore[no-untyped-def]
    ledger = JsonlLedger(tmp_path / "events.jsonl")
    ledger.append_model_event(event_type="fill_event_created", model=fill("open", "BUY", "exec-duplicate", "2345.1"))
    ledger.append_model_event(event_type="fill_event_created", model=fill("close", "SELL", "exec-duplicate", "2345.5"))

    with pytest.raises(LedgerError, match="Duplicate execution_id"):
        ledger.validate_identity_uniqueness(run_id="run-1")
