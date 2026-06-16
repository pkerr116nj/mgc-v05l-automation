"""Canonical broker-position identity helpers for Track B PAPER.

Broker position projections are not all shaped the same way. Some include
``con_id`` and some only carry account/localSymbol/symbol. This module resolves
the missing contract id only from exact localSymbol/account-compatible
authorities and fails closed when more than one contract id is possible.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeRegistryRecord
from mgc_v05l.execution_core.track_b_contract_identity import normalize_track_b_contract_row


IDENTITY_READY = "BROKER_POSITION_IDENTITY_READY"
IDENTITY_NOT_READY = "BROKER_POSITION_IDENTITY_NOT_READY"
IDENTITY_AMBIGUOUS = "BROKER_POSITION_IDENTITY_AMBIGUOUS"


@dataclass(frozen=True)
class BrokerPositionIdentityResolution:
    classification: str
    canonical_position: dict[str, Any]
    reason_codes: tuple[str, ...]
    source: str | None = None
    candidates: tuple[Mapping[str, Any], ...] = ()

    @property
    def ready(self) -> bool:
        return self.classification == IDENTITY_READY

    def to_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "canonical_position": dict(self.canonical_position),
            "reason_codes": list(self.reason_codes),
            "source": self.source,
            "candidates": [dict(item) for item in self.candidates],
        }


def canonicalize_broker_position_identity(
    *,
    broker_position: Mapping[str, Any],
    registry_records: Sequence[TradeRegistryRecord] = (),
    registry_rows: Sequence[Mapping[str, Any]] = (),
    lifecycle_positions: Sequence[Mapping[str, Any]] = (),
    lifecycle_reports: Sequence[Mapping[str, Any]] = (),
    contract_resolver_status: Mapping[str, Any] | None = None,
) -> BrokerPositionIdentityResolution:
    """Return broker position with canonical ``con_id`` when exact evidence exists."""

    canonical = normalize_track_b_contract_row(broker_position)
    current_con_id = _int_or_none(canonical.get("con_id") or canonical.get("conId"))
    if current_con_id is not None:
        canonical["con_id"] = current_con_id
        resolved_from_registry = _contract_identity_ready(canonical)
        reason_code = "VALIDATED_CONTRACT_IDENTITY_RESOLVED" if resolved_from_registry else "BROKER_POSITION_CON_ID_PRESENT"
        return BrokerPositionIdentityResolution(
            classification=IDENTITY_READY,
            canonical_position=canonical,
            reason_codes=(reason_code,),
            source="VALIDATED_TRACK_B_FUTURES_CONTRACT_REGISTRY" if resolved_from_registry else "BROKER_POSITION",
        )

    candidates = _dedupe_candidates(
        [
            *_registry_record_candidates(broker_position=broker_position, registry_records=registry_records),
            *_row_candidates(source="REGISTRY_RECONCILIATION", broker_position=broker_position, rows=registry_rows),
            *_row_candidates(source="LIFECYCLE_POSITION", broker_position=broker_position, rows=lifecycle_positions),
            *_row_candidates(source="LIFECYCLE_REPORT", broker_position=broker_position, rows=lifecycle_reports),
            *_contract_resolver_candidates(
                broker_position=broker_position,
                contract_resolver_status=contract_resolver_status or {},
            ),
        ]
    )
    con_ids = {int(item["con_id"]) for item in candidates if _int_or_none(item.get("con_id")) is not None}
    if len(con_ids) == 1:
        con_id = next(iter(con_ids))
        selected = [item for item in candidates if _int_or_none(item.get("con_id")) == con_id]
        canonical["con_id"] = con_id
        canonical.setdefault("local_symbol", selected[0].get("local_symbol"))
        canonical.setdefault("symbol", selected[0].get("symbol"))
        canonical["canonical_identity_source"] = selected[0].get("source")
        return BrokerPositionIdentityResolution(
            classification=IDENTITY_READY,
            canonical_position=canonical,
            reason_codes=("CANONICAL_CON_ID_RESOLVED_FROM_EXACT_LOCAL_SYMBOL",),
            source=str(selected[0].get("source") or ""),
            candidates=tuple(selected),
        )
    if len(con_ids) > 1:
        return BrokerPositionIdentityResolution(
            classification=IDENTITY_AMBIGUOUS,
            canonical_position=canonical,
            reason_codes=("AMBIGUOUS_CON_ID_FOR_LOCAL_SYMBOL_ACCOUNT",),
            candidates=tuple(candidates),
        )
    return BrokerPositionIdentityResolution(
        classification=IDENTITY_NOT_READY,
        canonical_position=canonical,
        reason_codes=("CON_ID_MISSING_AND_NO_EXACT_LOCAL_SYMBOL_AUTHORITY",),
        candidates=tuple(candidates),
    )


def _registry_record_candidates(
    *,
    broker_position: Mapping[str, Any],
    registry_records: Sequence[TradeRegistryRecord],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in registry_records:
        owner = record.ownership_identity
        if owner is None:
            continue
        row = {
            "source": "CENTRAL_TRADE_REGISTRY",
            "trade_id": record.trade_id,
            "lifecycle_id": owner.lifecycle_id,
            "account_id": owner.account_id,
            "symbol": owner.symbol,
            "local_symbol": owner.local_symbol,
            "con_id": owner.con_id,
            "quantity": str(owner.qty),
            "side": owner.side,
        }
        if _candidate_matches_broker_position(candidate=row, broker_position=broker_position):
            rows.append(row)
    return rows


def _contract_identity_ready(row: Mapping[str, Any]) -> bool:
    identity = row.get("contract_identity")
    return isinstance(identity, Mapping) and identity.get("resolved") is True


def _row_candidates(
    *,
    source: str,
    broker_position: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for item in rows:
        row = {
            "source": source,
            "trade_id": item.get("trade_id"),
            "lifecycle_id": item.get("lifecycle_id"),
            "account_id": item.get("account_id") or item.get("account"),
            "symbol": item.get("symbol") or item.get("track_b_root") or item.get("instrument_family"),
            "local_symbol": item.get("local_symbol") or item.get("localSymbol"),
            "con_id": item.get("con_id") or item.get("conId"),
            "quantity": item.get("quantity"),
            "side": item.get("side"),
        }
        if _candidate_matches_broker_position(candidate=row, broker_position=broker_position):
            candidates.append(row)
    return candidates


def _contract_resolver_candidates(
    *,
    broker_position: Mapping[str, Any],
    contract_resolver_status: Mapping[str, Any],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    selected = contract_resolver_status.get("selected_contracts")
    rows: list[Mapping[str, Any]] = []
    if isinstance(selected, Mapping):
        rows.extend(dict(item) for item in selected.values() if isinstance(item, Mapping))
    selected_contract = contract_resolver_status.get("selected_contract")
    if isinstance(selected_contract, Mapping):
        rows.append(selected_contract)
    contract_results = contract_resolver_status.get("contract_results")
    if isinstance(contract_results, Mapping):
        for result in contract_results.values():
            if isinstance(result, Mapping) and isinstance(result.get("selected_contract"), Mapping):
                rows.append(result["selected_contract"])
    details = contract_resolver_status.get("contract_details_reports")
    if isinstance(details, Mapping):
        for report in details.values():
            if isinstance(report, Mapping):
                api_details = report.get("api_contract_details")
                if isinstance(api_details, list):
                    rows.extend(item for item in api_details if isinstance(item, Mapping))
    for item in rows:
        row = {
            "source": "CONTRACT_RESOLVER_STATUS",
            "account_id": None,
            "symbol": item.get("symbol") or item.get("trading_class"),
            "local_symbol": item.get("local_symbol") or item.get("localSymbol"),
            "con_id": item.get("con_id") or item.get("conId"),
            "quantity": broker_position.get("quantity"),
        }
        if _candidate_matches_broker_position(candidate=row, broker_position=broker_position, allow_missing_account=True):
            candidates.append(row)
    return candidates


def _candidate_matches_broker_position(
    *,
    candidate: Mapping[str, Any],
    broker_position: Mapping[str, Any],
    allow_missing_account: bool = False,
) -> bool:
    con_id = _int_or_none(candidate.get("con_id") or candidate.get("conId"))
    if con_id is None:
        return False
    candidate_local = _text(candidate.get("local_symbol") or candidate.get("localSymbol")).upper()
    broker_local = _text(broker_position.get("local_symbol") or broker_position.get("localSymbol")).upper()
    if not candidate_local or not broker_local or candidate_local != broker_local:
        return False
    candidate_account = _text(candidate.get("account_id") or candidate.get("account"))
    broker_account = _text(broker_position.get("account_id") or broker_position.get("account"))
    if not allow_missing_account or candidate_account:
        if not candidate_account or not broker_account or candidate_account != broker_account:
            return False
    candidate_symbol = _text(candidate.get("symbol") or candidate.get("track_b_root") or candidate.get("instrument_family")).upper()
    broker_symbol = _text(broker_position.get("symbol") or broker_position.get("track_b_root") or broker_position.get("instrument_family")).upper()
    if candidate_symbol and broker_symbol and candidate_symbol != broker_symbol:
        return False
    return True


def _dedupe_candidates(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for item in candidates:
        key = (
            str(item.get("source") or ""),
            str(item.get("account_id") or ""),
            str(item.get("local_symbol") or "").upper(),
            str(item.get("con_id") or ""),
            str(item.get("trade_id") or item.get("lifecycle_id") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dict(item))
    return deduped


def _int_or_none(value: object) -> int | None:
    try:
        if value in {None, ""}:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _text(value: object) -> str:
    return str(value or "").strip()


def decimal_or_none(value: object) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
