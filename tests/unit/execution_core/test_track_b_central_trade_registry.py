from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from mgc_v05l.execution_core.track_b_central_trade_registry import (
    TradeCurrentState,
    TradeEvent,
    TradeEventType,
    events_from_json,
    events_to_json,
    generate_trade_id,
    records_from_json,
    records_to_json,
    reduce_trade_events,
)


NOW = datetime(2026, 5, 31, 12, 0, tzinfo=UTC)


def test_clean_full_lifecycle_reduces_to_closed_flat() -> None:
    events = _clean_full_lifecycle_events()

    record = reduce_trade_events(events)

    assert record.current_state == TradeCurrentState.CLOSED_FLAT
    assert record.broker_backed_entry is True
    assert record.broker_backed_exit is True
    assert record.open_qty == Decimal("0")
    assert record.entry_price == Decimal("30395.00")
    assert record.exit_price == Decimal("30405.00")
    assert record.ownership_identity is not None
    assert record.ownership_identity.lifecycle_id == "life-1"


def test_passive_entry_cancel_reduces_to_cancelled() -> None:
    base = _base_event(TradeEventType.ENTRY_INTENT_CREATED)
    submitted = replace(
        _base_event(TradeEventType.ENTRY_ORDER_SUBMITTED, offset=1),
        order_id="entry-order-1",
        client_id="client-7",
    )
    cancelled = replace(
        _base_event(TradeEventType.ENTRY_ORDER_CANCELLED, offset=2),
        order_id="entry-order-1",
        client_id="client-7",
        reason_codes=("PASSIVE_ENTRY_CANCELLED",),
    )

    record = reduce_trade_events([base, submitted, cancelled])

    assert record.current_state == TradeCurrentState.CANCELLED
    assert record.broker_backed_entry is False
    assert record.open_qty == Decimal("0")
    assert "PASSIVE_ENTRY_CANCELLED" in record.latest_reason_codes


def test_fill_without_broker_ids_becomes_review_required() -> None:
    entry = _base_event(TradeEventType.ENTRY_INTENT_CREATED)
    fill = replace(
        _base_event(TradeEventType.ENTRY_FILL_BROKER_BACKED, offset=1),
        order_id="entry-order-1",
        client_id="client-7",
        perm_id=None,
        exec_id=None,
        price=Decimal("30395.00"),
    )

    record = reduce_trade_events([entry, fill])

    assert record.current_state == TradeCurrentState.REVIEW_REQUIRED
    assert record.broker_backed_entry is False
    assert "BROKER_BACKED_EVENT_MISSING_BROKER_IDS" in record.latest_reason_codes


def test_entry_fill_opens_managed_state() -> None:
    entry = _base_event(TradeEventType.ENTRY_INTENT_CREATED)
    fill = _entry_fill(offset=1)
    managed = _managed(offset=2)

    record = reduce_trade_events([entry, fill, managed])

    assert record.current_state == TradeCurrentState.OPEN_MANAGED
    assert record.broker_backed_entry is True
    assert record.open_qty == Decimal("1")


def test_exit_fill_closes_flat() -> None:
    events = _clean_full_lifecycle_events()[:-1]

    record = reduce_trade_events(events)

    assert record.current_state == TradeCurrentState.CLOSED_FLAT
    assert record.broker_backed_exit is True
    assert record.open_qty == Decimal("0")


def test_manual_operator_close_recorded_is_distinct_from_autonomous_close() -> None:
    entry = _base_event(TradeEventType.ENTRY_INTENT_CREATED)
    fill = _entry_fill(offset=1)
    managed = _managed(offset=2)
    manual_close = replace(
        _base_event(TradeEventType.MANUAL_OPERATOR_CLOSE_RECORDED, offset=3),
        lifecycle_id="life-1",
        action="SELL_TO_CLOSE",
        price=Decimal("30401.00"),
    )

    record = reduce_trade_events([entry, fill, managed, manual_close])

    assert record.current_state == TradeCurrentState.CLOSED_FLAT
    assert record.manual_operator_close_recorded is True
    assert record.broker_backed_exit is False
    assert "MANUAL_OPERATOR_CLOSE_RECORDED" in record.latest_reason_codes


def test_recovery_adoption_event_attaches_existing_broker_backed_position() -> None:
    adoption = replace(
        _base_event(TradeEventType.RECOVERY_ADOPTION_RECORDED),
        lifecycle_id="life-1",
        order_id="entry-order-1",
        client_id="client-7",
        perm_id="2047276405",
        exec_id="0000e1a7.6a29f525.01.01",
        price=Decimal("30395.00"),
        reason_codes=("RECOVERY_ADOPTION_RECORDED",),
    )

    record = reduce_trade_events([adoption])

    assert record.current_state == TradeCurrentState.OPEN_MANAGED
    assert record.recovery_adoption_recorded is True
    assert record.broker_backed_entry is True
    assert record.ownership_identity is not None
    assert record.ownership_identity.lifecycle_id == "life-1"


def test_recovery_adoption_repairs_prior_missing_broker_fill_review() -> None:
    intent = _base_event(TradeEventType.ENTRY_INTENT_CREATED)
    missing_fill = replace(
        _base_event(TradeEventType.ENTRY_FILL_BROKER_BACKED, offset=1),
        order_id="entry-order-1",
        client_id="client-7",
        perm_id="2047276405",
        exec_id=None,
        price=Decimal("30395.00"),
        reason_codes=("BROKER_BACKED_FILL_MISSING_PERM_OR_EXEC",),
    )
    review = replace(
        _base_event(TradeEventType.REVIEW_REQUIRED, offset=2),
        reason_codes=("RECOVERY_ADOPTION_REVIEW_REQUIRED",),
    )
    adoption = replace(
        _base_event(TradeEventType.RECOVERY_ADOPTION_RECORDED, offset=3),
        lifecycle_id="life-1",
        order_id="entry-order-1",
        client_id="client-7",
        perm_id="2047276405",
        exec_id="0000e1a7.6a29f525.01.01",
        price=Decimal("30395.00"),
        reason_codes=("RECOVERY_ADOPTION_RECORDED",),
    )

    record = reduce_trade_events([intent, missing_fill, review, adoption])

    assert record.current_state == TradeCurrentState.OPEN_MANAGED
    assert record.broker_backed_entry is True
    assert record.ownership_identity is not None
    assert record.ownership_identity.lifecycle_id == "life-1"
    assert "BROKER_BACKED_FILL_MISSING_PERM_OR_EXEC" not in record.latest_reason_codes


def test_aggregate_review_event_does_not_override_exact_broker_backed_open_identity() -> None:
    adoption = replace(
        _base_event(TradeEventType.RECOVERY_ADOPTION_RECORDED),
        lifecycle_id="life-1",
        order_id="entry-order-1",
        client_id="client-7",
        perm_id="2047276405",
        exec_id="0000e1a7.6a29f525.01.01",
        price=Decimal("30395.00"),
        reason_codes=("RECOVERY_ADOPTION_RECORDED",),
    )
    aggregate_review = replace(
        _base_event(TradeEventType.REVIEW_REQUIRED, offset=1),
        lifecycle_id="life-1",
        account_id="MULTIPLE",
        reason_codes=("REGISTRY_RECONCILIATION_REVIEW_REQUIRED",),
    )

    record = reduce_trade_events([adoption, aggregate_review])

    assert record.current_state == TradeCurrentState.OPEN_MANAGED
    assert record.broker_backed_entry is True
    assert "REGISTRY_RECONCILIATION_REVIEW_REQUIRED" not in record.latest_reason_codes
    assert "ACCOUNT_ID_MISMATCH" not in record.latest_reason_codes


def test_late_exact_adoption_supersedes_transient_settlement_review_rows() -> None:
    intent = _base_event(TradeEventType.ENTRY_INTENT_CREATED)
    missing_fill_review = replace(
        _base_event(TradeEventType.REVIEW_REQUIRED, offset=1),
        lifecycle_id="life-1",
        reason_codes=("BROKER_BACKED_FILL_MISSING_PERM_OR_EXEC",),
    )
    settlement_review = replace(
        _base_event(TradeEventType.REVIEW_REQUIRED, offset=2),
        lifecycle_id="life-1",
        reason_codes=("REGISTRY_RECONCILIATION_REVIEW_REQUIRED",),
    )
    adoption_review = replace(
        _base_event(TradeEventType.REVIEW_REQUIRED, offset=3),
        lifecycle_id="life-1",
        reason_codes=("RECOVERY_ADOPTION_REVIEW_REQUIRED",),
    )
    adoption = replace(
        _base_event(TradeEventType.RECOVERY_ADOPTION_RECORDED, offset=4),
        lifecycle_id="life-1",
        order_id="entry-order-1",
        client_id="client-7",
        perm_id="2047276405",
        exec_id="0000e1a7.6a29f525.01.01",
        price=Decimal("30395.00"),
        reason_codes=("RECOVERY_ADOPTION_RECORDED",),
    )
    managed = _managed(offset=5)
    aggregate_review = replace(
        _base_event(TradeEventType.REVIEW_REQUIRED, offset=6),
        lifecycle_id="life-1",
        account_id="MULTIPLE",
        reason_codes=("REGISTRY_RECONCILIATION_REVIEW_REQUIRED",),
    )

    record = reduce_trade_events([
        intent,
        missing_fill_review,
        settlement_review,
        adoption_review,
        adoption,
        managed,
        aggregate_review,
    ])

    assert record.current_state == TradeCurrentState.OPEN_MANAGED
    assert record.broker_backed_entry is True
    assert "REVIEW_REQUIRED_EVENT" not in record.latest_reason_codes
    assert "BROKER_BACKED_FILL_MISSING_PERM_OR_EXEC" not in record.latest_reason_codes
    assert "REGISTRY_RECONCILIATION_REVIEW_REQUIRED" not in record.latest_reason_codes
    assert "RECOVERY_ADOPTION_REVIEW_REQUIRED" not in record.latest_reason_codes


def test_exact_review_event_still_blocks_broker_backed_open_identity() -> None:
    adoption = replace(
        _base_event(TradeEventType.RECOVERY_ADOPTION_RECORDED),
        lifecycle_id="life-1",
        order_id="entry-order-1",
        client_id="client-7",
        perm_id="2047276405",
        exec_id="0000e1a7.6a29f525.01.01",
        price=Decimal("30395.00"),
        reason_codes=("RECOVERY_ADOPTION_RECORDED",),
    )
    current_review = replace(
        _base_event(TradeEventType.REVIEW_REQUIRED, offset=1),
        lifecycle_id="life-1",
        account_id="DUM882026",
        reason_codes=("REGISTRY_RECONCILIATION_REVIEW_REQUIRED",),
    )

    record = reduce_trade_events([adoption, current_review])

    assert record.current_state == TradeCurrentState.REVIEW_REQUIRED
    assert "REGISTRY_RECONCILIATION_REVIEW_REQUIRED" in record.latest_reason_codes


def test_conflicting_lifecycle_conid_or_account_creates_review_required() -> None:
    events = [_base_event(TradeEventType.ENTRY_INTENT_CREATED), _entry_fill(offset=1), _managed(offset=2)]
    conflicting_exit = replace(
        _base_event(TradeEventType.EXIT_INTENT_CREATED, offset=3),
        lifecycle_id="life-1",
        action="SELL_TO_CLOSE",
        account_id="OTHER_ACCOUNT",
        con_id=770561202,
        local_symbol="MNQU6",
    )

    record = reduce_trade_events([*events, conflicting_exit])

    assert record.current_state == TradeCurrentState.REVIEW_REQUIRED
    assert "ACCOUNT_ID_MISMATCH" in record.latest_reason_codes
    assert "CON_ID_MISMATCH" in record.latest_reason_codes
    assert "LOCAL_SYMBOL_MISMATCH" in record.latest_reason_codes


def test_json_roundtrip_preserves_event_chain_and_derived_state() -> None:
    events = _clean_full_lifecycle_events()
    record = reduce_trade_events(events)

    restored_events = events_from_json(events_to_json(events))
    restored_records = records_from_json(records_to_json([record]))

    assert restored_events == tuple(events)
    assert restored_records[0] == record
    assert reduce_trade_events(restored_events).current_state == TradeCurrentState.CLOSED_FLAT


def test_trade_id_generation_is_stable_and_uses_broker_identity() -> None:
    trade_id = generate_trade_id(
        account_id="DUM882026",
        con_id=770561201,
        lane_id="mnq_us_active_participation_long",
        entry_action="BUY_TO_OPEN",
        entry_perm_id="2047276405",
        generated_at=NOW,
    )

    assert trade_id.startswith("trade_DUM882026_770561201_mnq_us_active_participation_long_BUY_TO_OPEN_2047276405")


def _clean_full_lifecycle_events() -> list[TradeEvent]:
    return [
        _base_event(TradeEventType.ENTRY_INTENT_CREATED),
        replace(_base_event(TradeEventType.ENTRY_ORDER_SUBMITTED, offset=1), order_id="entry-order-1", client_id="client-7"),
        _entry_fill(offset=2),
        _managed(offset=3),
        replace(_base_event(TradeEventType.RECONCILED_OPEN, offset=4), lifecycle_id="life-1"),
        replace(_base_event(TradeEventType.EXIT_INTENT_CREATED, offset=5), lifecycle_id="life-1", action="SELL_TO_CLOSE"),
        replace(
            _base_event(TradeEventType.EXIT_ORDER_SUBMITTED, offset=6),
            lifecycle_id="life-1",
            action="SELL_TO_CLOSE",
            order_id="exit-order-1",
            client_id="client-7",
        ),
        replace(
            _base_event(TradeEventType.EXIT_FILL_BROKER_BACKED, offset=7),
            lifecycle_id="life-1",
            action="SELL_TO_CLOSE",
            order_id="exit-order-1",
            client_id="client-7",
            perm_id="2047276410",
            exec_id="0000e1a7.6a29f525.01.02",
            price=Decimal("30405.00"),
        ),
        replace(_base_event(TradeEventType.RECONCILED_FLAT, offset=8), lifecycle_id="life-1", action="SELL_TO_CLOSE"),
    ]


def _entry_fill(*, offset: int) -> TradeEvent:
    return replace(
        _base_event(TradeEventType.ENTRY_FILL_BROKER_BACKED, offset=offset),
        order_id="entry-order-1",
        client_id="client-7",
        perm_id="2047276405",
        exec_id="0000e1a7.6a29f525.01.01",
        price=Decimal("30395.00"),
    )


def _managed(*, offset: int) -> TradeEvent:
    return replace(
        _base_event(TradeEventType.LIFECYCLE_OPEN_MANAGED, offset=offset),
        lifecycle_id="life-1",
        perm_id="2047276405",
        exec_id="0000e1a7.6a29f525.01.01",
        reason_codes=("MANAGED_POSITION_OPENED",),
    )


def _base_event(event_type: TradeEventType, *, offset: int = 0) -> TradeEvent:
    return TradeEvent(
        event_id=f"event-{offset}-{event_type.value}",
        event_type=event_type,
        generated_at=NOW + timedelta(seconds=offset),
        trade_id="trade-DUM882026-770561201-entry",
        lifecycle_id=None,
        lane_id="mnq_us_active_participation_long",
        thesis_strategy_id="mnq_us_active_participation_long",
        account_id="DUM882026",
        symbol="MNQ",
        con_id=770561201,
        local_symbol="MNQM6",
        expiry="202606",
        side="LONG",
        action="BUY_TO_OPEN",
        qty=Decimal("1"),
        source_artifact_path=f"synthetic://trade_registry/{event_type.value}",
        metadata={"simulated": True},
    )
