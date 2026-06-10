"""Canonical terminal registry truth overlay for Track B current projections.

This module is read-only. It resolves whether an append-only registry trade
chain has terminal broker-backed flat evidence that must outrank weaker
lifecycle or diagnostic projections.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeCurrentState, TradeEventType, TradeRegistryRecord


TERMINAL_CLOSED_FLAT = "CLOSED_FLAT"
BROKER_FLAT_EVIDENCE_GATED_CLEANUP_TERMINAL = "BROKER_FLAT_EVIDENCE_GATED_CLEANUP_TERMINAL"
TERMINAL_NOT_SUPERSEDED = "NOT_SUPERSEDED"
TERMINAL_AMBIGUOUS = "AMBIGUOUS"


@dataclass(frozen=True)
class TerminalRegistryTruth:
    classification: str
    record: TradeRegistryRecord | None
    reason_codes: tuple[str, ...]

    @property
    def terminal_closed_flat(self) -> bool:
        return self.classification in {
            TERMINAL_CLOSED_FLAT,
            BROKER_FLAT_EVIDENCE_GATED_CLEANUP_TERMINAL,
        } and self.record is not None

    def to_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "trade_id": self.record.trade_id if self.record is not None else None,
            "lifecycle_id": self.record.ownership_identity.lifecycle_id
            if self.record is not None and self.record.ownership_identity is not None
            else None,
            "current_state": self.record.current_state.value if self.record is not None else None,
            "broker_backed_exit": self.record.broker_backed_exit if self.record is not None else None,
            "open_qty": str(self.record.open_qty) if self.record is not None else None,
            "reason_codes": list(self.reason_codes),
        }


def resolve_terminal_registry_truth(
    *,
    records: Sequence[TradeRegistryRecord],
    identity: Mapping[str, Any],
    broker_positions: Sequence[Mapping[str, Any]],
    broker_open_orders: Sequence[Mapping[str, Any]],
) -> TerminalRegistryTruth:
    """Resolve terminal CLOSED_FLAT truth for a current-scope identity.

    Broker-backed exit evidence is the strongest terminal proof. Evidence-gated
    historical cleanup can also suppress stale lifecycle projections when the
    registry is CLOSED_FLAT and the cleanup event proves broker flat/no-order
    state without pretending a broker-backed exit fill occurred. The resolver
    fails closed on ambiguity or any current broker position/order linkage.
    """

    candidates = _terminal_candidates(records=records, identity=identity)
    if not candidates:
        return TerminalRegistryTruth(TERMINAL_NOT_SUPERSEDED, None, ("NO_TERMINAL_REGISTRY_PROOF",))
    if len(candidates) > 1:
        return TerminalRegistryTruth(
            TERMINAL_AMBIGUOUS,
            None,
            ("AMBIGUOUS_TERMINAL_REGISTRY_PROOF",),
        )
    record = candidates[0]
    linked_identity = _identity_with_record_direction(identity=identity, record=record)
    if _has_linked_broker_position(linked_identity, broker_positions) or _has_linked_open_order(linked_identity, broker_open_orders):
        return TerminalRegistryTruth(
            TERMINAL_NOT_SUPERSEDED,
            record,
            ("CURRENT_BROKER_EXPOSURE_OR_ORDER_LINKED",),
        )
    if _record_has_broker_backed_flat_exit(record):
        return TerminalRegistryTruth(
            TERMINAL_CLOSED_FLAT,
            record,
            (
                "BROKER_BACKED_CLOSED_FLAT_REGISTRY_SUPERSEDES_OPEN_LIFECYCLE_PROJECTION",
                "BROKER_BACKED_EXIT_EVIDENCE_CONFIRMED",
                "BROKER_FLAT_PROOF_CONFIRMED",
                "NO_OPEN_ORDER_PROOF_CONFIRMED",
            ),
        )
    if _record_has_evidence_gated_broker_flat_cleanup(record):
        return TerminalRegistryTruth(
            BROKER_FLAT_EVIDENCE_GATED_CLEANUP_TERMINAL,
            record,
            (
                "EVIDENCE_GATED_BROKER_FLAT_CLEANUP_SUPERSEDES_OPEN_LIFECYCLE_PROJECTION",
                "BROKER_BACKED_ENTRY_EVIDENCE_CONFIRMED",
                "BROKER_BACKED_EXIT_EVIDENCE_NOT_CLAIMED",
                "BROKER_FLAT_PROOF_CONFIRMED",
                "NO_OPEN_ORDER_PROOF_CONFIRMED",
            ),
        )
    return TerminalRegistryTruth(
        TERMINAL_NOT_SUPERSEDED,
        record,
        ("NO_BROKER_BACKED_EXIT_OR_EVIDENCE_GATED_CLEANUP_TERMINAL_PROOF",),
    )


def filter_terminal_superseded_current_rows(
    *,
    rows: Sequence[Any],
    records: Sequence[TradeRegistryRecord],
    broker_positions: Sequence[Mapping[str, Any]],
    broker_open_orders: Sequence[Mapping[str, Any]],
) -> tuple[tuple[Any, ...], tuple[dict[str, Any], ...]]:
    current: list[Any] = []
    superseded: list[dict[str, Any]] = []
    for row in rows:
        identity = _identity_from_row(row)
        terminal = resolve_terminal_registry_truth(
            records=records,
            identity=identity,
            broker_positions=broker_positions,
            broker_open_orders=broker_open_orders,
        )
        if terminal.terminal_closed_flat:
            superseded.append(
                {
                    "classification": "STALE_SUPERSEDED_LIFECYCLE_PROJECTION",
                    "terminal_registry_truth": terminal.to_dict(),
                    "row": _row_to_dict(row),
                }
            )
            continue
        current.append(row)
    return tuple(current), tuple(superseded)


def _record_has_broker_backed_flat_exit(record: TradeRegistryRecord) -> bool:
    return record.broker_backed_exit is True and record.open_qty == Decimal("0")


def _record_has_evidence_gated_broker_flat_cleanup(record: TradeRegistryRecord) -> bool:
    if record.current_state != TradeCurrentState.CLOSED_FLAT:
        return False
    if record.open_qty != Decimal("0"):
        return False
    if record.broker_backed_entry is not True or record.broker_backed_exit is True:
        return False
    if "RECONCILED_FLAT_HISTORICAL_CLEANUP" not in record.latest_reason_codes:
        return False
    cleanup_events = [
        event for event in record.event_chain if event.event_type == TradeEventType.RECONCILED_FLAT_HISTORICAL_CLEANUP
    ]
    return any(_cleanup_event_has_flat_no_order_proof(event) for event in cleanup_events)


def _identity_with_record_direction(
    *,
    identity: Mapping[str, Any],
    record: TradeRegistryRecord,
) -> dict[str, Any]:
    payload = dict(identity)
    owner = record.ownership_identity
    if owner is not None:
        payload.setdefault("side", owner.side)
        payload.setdefault("qty", owner.qty)
    return payload


def _cleanup_event_has_flat_no_order_proof(event: Any) -> bool:
    reason_codes = {str(item).strip().upper() for item in getattr(event, "reason_codes", ()) or ()}
    metadata = getattr(event, "metadata", None)
    metadata = metadata if isinstance(metadata, Mapping) else {}
    has_flat_reason = bool(
        reason_codes
        & {
            "BROKER_FLAT_PROOF_CONFIRMED",
            "BROKER_LIFECYCLE_FLAT_CONFIRMED",
        }
    )
    has_no_order_reason = "NO_OPEN_ORDER_PROOF_CONFIRMED" in reason_codes
    not_current_exposure = metadata.get("not_current_exposure") is True or "NOT_CURRENT_EXPOSURE" in reason_codes
    not_current_open_order = metadata.get("not_current_open_order") is True or "NOT_CURRENT_OPEN_ORDER" in reason_codes
    broker_flat_proof_path = _text(metadata.get("broker_flat_proof_path") or metadata.get("broker_positions_snapshot_path"))
    open_orders_proof_path = _text(metadata.get("open_orders_proof_path") or metadata.get("broker_open_orders_snapshot_path"))
    return all(
        (
            has_flat_reason,
            has_no_order_reason,
            not_current_exposure,
            not_current_open_order,
            broker_flat_proof_path,
            open_orders_proof_path,
        )
    )


def _terminal_candidates(
    *,
    records: Sequence[TradeRegistryRecord],
    identity: Mapping[str, Any],
) -> list[TradeRegistryRecord]:
    terminal_records = [
        record
        for record in records
        if _record_has_broker_backed_flat_exit(record) or _record_has_evidence_gated_broker_flat_cleanup(record)
    ]
    trade_id = _text(identity.get("trade_id"))
    if trade_id:
        return [
            record
            for record in terminal_records
            if record.trade_id == trade_id and _record_identity_fields_compatible(record, identity)
        ]
    trade_ids = {str(item).strip() for item in identity.get("trade_ids") or [] if str(item or "").strip()}
    if trade_ids:
        return [
            record
            for record in terminal_records
            if record.trade_id in trade_ids and _record_identity_fields_compatible(record, identity)
        ]
    lifecycle_id = _text(identity.get("lifecycle_id"))
    if lifecycle_id:
        return [
            record
            for record in terminal_records
            if record.ownership_identity is not None
            and record.ownership_identity.lifecycle_id == lifecycle_id
            and _record_identity_fields_compatible(record, identity)
        ]
    return [record for record in terminal_records if _record_matches_identity(record, identity)]


def _record_identity_fields_compatible(record: TradeRegistryRecord, identity: Mapping[str, Any]) -> bool:
    owner = record.ownership_identity
    if owner is None:
        return False
    ident_con = _int_or_none(identity.get("con_id") or identity.get("conId"))
    if ident_con is not None and ident_con != owner.con_id:
        return False
    ident_local = _text(identity.get("local_symbol") or identity.get("localSymbol")).upper()
    if ident_local and ident_local != owner.local_symbol.upper():
        return False
    if not _account_matches(owner.account_id, identity.get("account_id") or identity.get("account")):
        return False
    return _quantity_matches(owner.qty, identity)


def _record_matches_identity(record: TradeRegistryRecord, identity: Mapping[str, Any]) -> bool:
    owner = record.ownership_identity
    if owner is None:
        return False
    trade_id = _text(identity.get("trade_id"))
    lifecycle_id = _text(identity.get("lifecycle_id"))
    if trade_id and record.trade_id == trade_id:
        return _record_identity_fields_compatible(record, identity)
    if lifecycle_id and owner.lifecycle_id == lifecycle_id:
        return _record_identity_fields_compatible(record, identity)
    return (
        _int_or_none(identity.get("con_id") or identity.get("conId")) == owner.con_id
        and _text(identity.get("local_symbol") or identity.get("localSymbol")).upper() == owner.local_symbol.upper()
        and _account_matches(owner.account_id, identity.get("account_id") or identity.get("account"))
        and _quantity_matches(owner.qty, identity)
    )


def _has_linked_broker_position(identity: Mapping[str, Any], broker_positions: Sequence[Mapping[str, Any]]) -> bool:
    return any(_is_nonzero_broker_position(row) and _row_matches_identity(row, identity) for row in broker_positions)


def _has_linked_open_order(identity: Mapping[str, Any], broker_open_orders: Sequence[Mapping[str, Any]]) -> bool:
    return any(_row_matches_identity(row, identity) for row in broker_open_orders)


def _row_matches_identity(row: Mapping[str, Any], identity: Mapping[str, Any]) -> bool:
    row_con = _int_or_none(row.get("con_id") or row.get("conId"))
    ident_con = _int_or_none(identity.get("con_id") or identity.get("conId"))
    if row_con is not None and ident_con is not None and row_con != ident_con:
        return False
    row_local = _text(row.get("local_symbol") or row.get("localSymbol")).upper()
    ident_local = _text(identity.get("local_symbol") or identity.get("localSymbol")).upper()
    if row_local and ident_local and row_local != ident_local:
        return False
    row_symbol = _symbol(row)
    ident_symbol = _symbol(identity)
    if row_symbol and ident_symbol and row_symbol != ident_symbol:
        return False
    if not _direction_compatible(row, identity):
        return False
    return bool(row_con is not None or ident_con is not None or row_local or ident_local or row_symbol or ident_symbol)


def _is_nonzero_broker_position(row: Mapping[str, Any]) -> bool:
    quantity = _decimal_or_none(row.get("quantity") or row.get("position") or row.get("qty"))
    return quantity is not None and quantity != 0


def _identity_from_row(row: Any) -> Mapping[str, Any]:
    if isinstance(row, Mapping):
        return row
    payload = _row_to_dict(row)
    return payload


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    to_dict = getattr(row, "to_dict", None)
    if callable(to_dict):
        payload = to_dict()
        return dict(payload) if isinstance(payload, Mapping) else {}
    return {
        "trade_id": getattr(row, "trade_id", None),
        "lifecycle_id": getattr(row, "lifecycle_id", None),
        "con_id": getattr(row, "con_id", None),
        "local_symbol": getattr(row, "local_symbol", None),
        "symbol": getattr(row, "symbol", None),
    }


def _account_matches(owner_account: str, value: Any) -> bool:
    account = _text(value)
    return not account or account.upper() in {"MULTIPLE", "UNKNOWN", "MISSING"} or account == owner_account


def _quantity_matches(owner_qty: Decimal, identity: Mapping[str, Any]) -> bool:
    qty = _decimal_or_none(identity.get("quantity") or identity.get("qty") or identity.get("aggregate_qty"))
    return qty is None or abs(qty) == abs(owner_qty)


def _direction_compatible(row: Mapping[str, Any], identity: Mapping[str, Any]) -> bool:
    row_sign = _signed_direction(row)
    identity_sign = _signed_direction(identity)
    return row_sign is None or identity_sign is None or row_sign == identity_sign


def _signed_direction(row: Mapping[str, Any]) -> int | None:
    signed = _decimal_or_none(row.get("signed_qty") or row.get("aggregate_qty"))
    if signed is not None and signed != 0:
        return 1 if signed > 0 else -1
    qty = _decimal_or_none(row.get("quantity") or row.get("qty"))
    side = _text(row.get("side") or row.get("action")).upper()
    if qty is None:
        return None
    if qty < 0:
        return -1
    if qty > 0:
        if side in {"SHORT", "SELL", "SELL_TO_OPEN"}:
            return -1
        if side in {"LONG", "BUY", "BUY_TO_OPEN"}:
            return 1
        return 1
    return None


def _symbol(row: Mapping[str, Any]) -> str:
    return _text(row.get("track_b_root") or row.get("instrument_family") or row.get("symbol")).upper()


def _text(value: Any) -> str:
    return str(value or "").strip()


def _int_or_none(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _decimal_or_none(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except Exception:
        return None
