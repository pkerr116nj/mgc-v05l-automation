"""Canonical terminal registry truth overlay for Track B current projections.

This module is read-only. It resolves whether an append-only registry trade
chain has terminal broker-backed flat evidence that must outrank weaker
lifecycle or diagnostic projections.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeRegistryRecord


TERMINAL_CLOSED_FLAT = "CLOSED_FLAT"
TERMINAL_NOT_SUPERSEDED = "NOT_SUPERSEDED"
TERMINAL_AMBIGUOUS = "AMBIGUOUS"


@dataclass(frozen=True)
class TerminalRegistryTruth:
    classification: str
    record: TradeRegistryRecord | None
    reason_codes: tuple[str, ...]

    @property
    def terminal_closed_flat(self) -> bool:
        return self.classification == TERMINAL_CLOSED_FLAT and self.record is not None

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

    The overlay only returns CLOSED_FLAT when a matching registry chain has
    broker-backed exit evidence, open quantity is zero, and current broker truth
    is flat with no linked open order. It fails closed on ambiguity.
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
    if _has_linked_broker_position(identity, broker_positions) or _has_linked_open_order(identity, broker_open_orders):
        return TerminalRegistryTruth(
            TERMINAL_NOT_SUPERSEDED,
            record,
            ("CURRENT_BROKER_EXPOSURE_OR_ORDER_LINKED",),
        )
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


def _terminal_candidates(
    *,
    records: Sequence[TradeRegistryRecord],
    identity: Mapping[str, Any],
) -> list[TradeRegistryRecord]:
    terminal_records = [record for record in records if _record_has_broker_backed_flat_exit(record)]
    trade_id = _text(identity.get("trade_id"))
    if trade_id:
        return [record for record in terminal_records if record.trade_id == trade_id]
    trade_ids = {str(item).strip() for item in identity.get("trade_ids") or [] if str(item or "").strip()}
    if trade_ids:
        return [record for record in terminal_records if record.trade_id in trade_ids]
    lifecycle_id = _text(identity.get("lifecycle_id"))
    if lifecycle_id:
        return [
            record
            for record in terminal_records
            if record.ownership_identity is not None and record.ownership_identity.lifecycle_id == lifecycle_id
        ]
    return [record for record in terminal_records if _record_matches_identity(record, identity)]


def _record_matches_identity(record: TradeRegistryRecord, identity: Mapping[str, Any]) -> bool:
    owner = record.ownership_identity
    if owner is None:
        return False
    trade_id = _text(identity.get("trade_id"))
    lifecycle_id = _text(identity.get("lifecycle_id"))
    if trade_id and record.trade_id == trade_id:
        return True
    if lifecycle_id and owner.lifecycle_id == lifecycle_id:
        return True
    return (
        _int_or_none(identity.get("con_id") or identity.get("conId")) == owner.con_id
        and _text(identity.get("local_symbol") or identity.get("localSymbol")).upper() == owner.local_symbol.upper()
        and _account_matches(owner.account_id, identity.get("account_id") or identity.get("account"))
        and _quantity_matches(owner.qty, identity)
    )


def _has_linked_broker_position(identity: Mapping[str, Any], broker_positions: Sequence[Mapping[str, Any]]) -> bool:
    return any(_row_matches_identity(row, identity) for row in broker_positions)


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
    return bool(row_con is not None or ident_con is not None or row_local or ident_local or row_symbol or ident_symbol)


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
