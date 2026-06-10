from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEvent, TradeEventType, reduce_trade_events
from mgc_v05l.execution_core.track_b_terminal_registry_truth import (
    BROKER_FLAT_EVIDENCE_GATED_CLEANUP_TERMINAL,
    TERMINAL_CLOSED_FLAT,
    TERMINAL_NOT_SUPERSEDED,
    resolve_terminal_registry_truth,
)


NOW = datetime(2026, 6, 3, 0, 44, tzinfo=UTC)


def test_evidence_gated_broker_flat_cleanup_is_terminal_without_broker_backed_exit() -> None:
    record = _cleanup_record()

    terminal = resolve_terminal_registry_truth(
        records=[record],
        identity=_identity(),
        broker_positions=[],
        broker_open_orders=[],
    )

    assert terminal.classification == BROKER_FLAT_EVIDENCE_GATED_CLEANUP_TERMINAL
    assert terminal.terminal_closed_flat is True
    assert terminal.record is not None
    assert terminal.record.broker_backed_exit is False
    assert terminal.to_dict()["broker_backed_exit"] is False
    assert "BROKER_BACKED_EXIT_EVIDENCE_NOT_CLAIMED" in terminal.reason_codes


def test_broker_backed_exit_terminal_still_works() -> None:
    record = reduce_trade_events(
        [
            _event(TradeEventType.ENTRY_FILL_BROKER_BACKED),
            _event(
                TradeEventType.EXIT_FILL_BROKER_BACKED,
                generated_at=NOW + timedelta(seconds=1),
                action="BUY",
                order_id="66",
                perm_id="665807057",
                exec_id="0000e1a7.exit.01.01",
            ),
        ]
    )

    terminal = resolve_terminal_registry_truth(
        records=[record],
        identity=_identity(),
        broker_positions=[],
        broker_open_orders=[],
    )

    assert terminal.classification == TERMINAL_CLOSED_FLAT
    assert terminal.record is not None
    assert terminal.record.broker_backed_exit is True


def test_missing_cleanup_proof_fails_closed() -> None:
    record = _cleanup_record(metadata={"historical_only": True})

    terminal = resolve_terminal_registry_truth(
        records=[record],
        identity=_identity(),
        broker_positions=[],
        broker_open_orders=[],
    )

    assert terminal.classification == TERMINAL_NOT_SUPERSEDED
    assert terminal.terminal_closed_flat is False


def test_cleanup_terminal_blocks_when_broker_position_still_linked() -> None:
    record = _cleanup_record()

    terminal = resolve_terminal_registry_truth(
        records=[record],
        identity=_identity(),
        broker_positions=[{"account_id": "DUM882026", "local_symbol": "MESM6", "con_id": 770561194, "quantity": "-1"}],
        broker_open_orders=[],
    )

    assert terminal.classification == TERMINAL_NOT_SUPERSEDED
    assert terminal.reason_codes == ("CURRENT_BROKER_EXPOSURE_OR_ORDER_LINKED",)


def test_flat_broker_position_row_does_not_block_terminal_cleanup() -> None:
    record = _cleanup_record()

    terminal = resolve_terminal_registry_truth(
        records=[record],
        identity=_identity(),
        broker_positions=[{"account_id": "DUM882026", "local_symbol": "MESM6", "con_id": 770561194, "quantity": "0.0"}],
        broker_open_orders=[],
    )

    assert terminal.classification == BROKER_FLAT_EVIDENCE_GATED_CLEANUP_TERMINAL
    assert terminal.terminal_closed_flat is True


def test_cleanup_terminal_blocks_when_open_order_still_linked() -> None:
    record = _cleanup_record()

    terminal = resolve_terminal_registry_truth(
        records=[record],
        identity=_identity(),
        broker_positions=[],
        broker_open_orders=[{"account_id": "DUM882026", "local_symbol": "MESM6", "con_id": 770561194, "order_id": "6"}],
    )

    assert terminal.classification == TERMINAL_NOT_SUPERSEDED
    assert terminal.reason_codes == ("CURRENT_BROKER_EXPOSURE_OR_ORDER_LINKED",)


def test_cleanup_terminal_fails_closed_on_wrong_identity() -> None:
    record = _cleanup_record()

    wrong_account = resolve_terminal_registry_truth(
        records=[record],
        identity={**_identity(), "account_id": "OTHER"},
        broker_positions=[],
        broker_open_orders=[],
    )
    wrong_contract = resolve_terminal_registry_truth(
        records=[record],
        identity={**_identity(), "con_id": 770561999},
        broker_positions=[],
        broker_open_orders=[],
    )

    assert wrong_account.classification == TERMINAL_NOT_SUPERSEDED
    assert wrong_contract.classification == TERMINAL_NOT_SUPERSEDED


def _cleanup_record(*, metadata: dict[str, object] | None = None):
    cleanup_metadata = {
        "historical_only": True,
        "not_current_exposure": True,
        "not_current_open_order": True,
        "broker_flat_proof_path": "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json",
        "open_orders_proof_path": "outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json",
    }
    if metadata is not None:
        cleanup_metadata = metadata
    return reduce_trade_events(
        [
            _event(TradeEventType.ENTRY_FILL_BROKER_BACKED),
            _event(
                TradeEventType.RECONCILED_FLAT_HISTORICAL_CLEANUP,
                generated_at=NOW + timedelta(seconds=1),
                action="HISTORICAL_FLAT_CLEANUP",
                reason_codes=(
                    "HISTORICAL_SUBMIT_INTENT_RESOLVED_FLAT",
                    "BROKER_FLAT_PROOF_CONFIRMED",
                    "NO_OPEN_ORDER_PROOF_CONFIRMED",
                    "NOT_CURRENT_EXPOSURE",
                    "NOT_CURRENT_OPEN_ORDER",
                ),
                metadata=cleanup_metadata,
            ),
        ]
    )


def _event(
    event_type: TradeEventType,
    *,
    generated_at: datetime = NOW,
    action: str = "SELL",
    order_id: str = "2",
    client_id: str = "10973",
    perm_id: str = "665735662",
    exec_id: str = "0000e1a7.6a2e7d30.01.01",
    reason_codes: tuple[str, ...] = (),
    metadata: dict[str, object] | None = None,
) -> TradeEvent:
    return TradeEvent(
        event_id=f"{event_type.value}_{generated_at.timestamp()}",
        event_type=event_type,
        generated_at=generated_at,
        trade_id="trade_696f40f3-5a3a-4166-b11e-fb59401c50ed",
        lifecycle_id="reserved_submit_mes_us_active_participation_short_20260602T185428677911Z_0c5caf5f40f7",
        lane_id="mes_us_active_participation_short",
        thesis_strategy_id="mes_us_active_participation_short",
        account_id="DUM882026",
        symbol="MES",
        con_id=770561194,
        local_symbol="MESM6",
        expiry="20260618",
        side="SHORT",
        action=action,
        qty=Decimal("1"),
        source_artifact_path="outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        order_id=order_id,
        client_id=client_id,
        perm_id=perm_id,
        exec_id=exec_id,
        reason_codes=reason_codes,
        metadata=metadata or {},
    )


def _identity() -> dict[str, object]:
    return {
        "trade_id": "trade_696f40f3-5a3a-4166-b11e-fb59401c50ed",
        "lifecycle_id": "reserved_submit_mes_us_active_participation_short_20260602T185428677911Z_0c5caf5f40f7",
        "account_id": "DUM882026",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "quantity": "1",
    }
