"""Shared current broker exposure ownership resolver for Track B PAPER.

This module is read-only. It centralizes the current broker-position owner
decision so restart, reconciliation, managed-position, and authority consumers
do not independently arbitrate stale registry/lifecycle rows.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_broker_position_identity import (
    IDENTITY_AMBIGUOUS,
    IDENTITY_NOT_READY,
    canonicalize_broker_position_identity,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import TradeCurrentState, TradeRegistryRecord
from mgc_v05l.execution_core.track_b_live_trade_registry import load_live_trade_registry_records
from mgc_v05l.execution_core.track_b_terminal_registry_truth import (
    filter_terminal_superseded_current_rows,
)


NO_OPEN_EXPOSURE = "NO_OPEN_EXPOSURE"
OWNED_MANAGED_EXPOSURE = "OWNED_MANAGED_EXPOSURE"
OWNED_MANAGED_EXIT_DUE = "OWNED_MANAGED_EXIT_DUE"
UNMANAGED_BROKER_EXPOSURE = "UNMANAGED_BROKER_EXPOSURE"
AMBIGUOUS_EXPOSURE_OWNERSHIP = "AMBIGUOUS_EXPOSURE_OWNERSHIP"
STALE_SUPERSEDED_FULL_AUDIT_ONLY = "STALE_SUPERSEDED_FULL_AUDIT_ONLY"

OPEN_REGISTRY_STATES = {
    TradeCurrentState.OPEN_MANAGED,
    TradeCurrentState.EXIT_DUE,
    TradeCurrentState.WORKING_EXIT,
}


@dataclass(frozen=True)
class CurrentExposureOwnerResolverConfig:
    repo_root: Path
    contract_resolver_status_path: Path = (
        Path("outputs") / "track_b_execution_core" / "contract_resolver" / "latest_contract_resolver_status.json"
    )
    managed_position_registry_path: Path = (
        Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
    )

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def resolve_current_exposure_ownership(
    *,
    config: CurrentExposureOwnerResolverConfig,
    broker_positions: Sequence[Mapping[str, Any]],
    broker_open_orders: Sequence[Mapping[str, Any]] = (),
    registry_records: Sequence[TradeRegistryRecord] | None = None,
    lifecycle_positions: Sequence[Mapping[str, Any]] = (),
    lifecycle_reports: Sequence[Mapping[str, Any]] = (),
    managed_position_registry: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve current broker exposure to exactly one canonical owner when provable."""

    broker_rows = [dict(row) for row in broker_positions if _position_key(row) and _decimal(row.get("quantity"))]
    open_order_rows = [dict(row) for row in broker_open_orders]
    records = tuple(registry_records) if registry_records is not None else load_live_trade_registry_records(
        repo_root=config.repo_root
    )
    managed_positions_payload = (
        dict(managed_position_registry)
        if isinstance(managed_position_registry, Mapping)
        else _read_json(config.resolve(config.managed_position_registry_path))
    )
    lifecycle_rows = [dict(row) for row in lifecycle_positions if _position_key(row)]
    contract_resolver_status = _read_json(config.resolve(config.contract_resolver_status_path))
    terminal_filtered, terminal_superseded_rows = filter_terminal_superseded_current_rows(
        rows=tuple(lifecycle_rows),
        records=records,
        broker_positions=broker_rows,
        broker_open_orders=open_order_rows,
    )
    lifecycle_rows = [dict(row) for row in terminal_filtered if isinstance(row, Mapping)]

    if not broker_rows:
        return {
            "classification": NO_OPEN_EXPOSURE,
            "broker_position_count": 0,
            "broker_open_order_count": len(open_order_rows),
            "owned_exposure_count": 0,
            "review_required_exposure_count": 0,
            "owned_exposures": [],
            "review_required_exposures": [],
            "resolved_lifecycle_positions": [],
            "stale_superseded_full_audit_only": list(terminal_superseded_rows),
            "read_only": True,
            "no_broad_flatten_generated": True,
        }

    owned: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = list(terminal_superseded_rows)
    resolved_lifecycle_positions: list[dict[str, Any]] = []
    active_records = [record for record in records if record.current_state in OPEN_REGISTRY_STATES]

    for raw_broker_position in broker_rows:
        identity = canonicalize_broker_position_identity(
            broker_position=raw_broker_position,
            registry_records=active_records,
            lifecycle_positions=lifecycle_rows,
            lifecycle_reports=lifecycle_reports,
            contract_resolver_status=contract_resolver_status,
        )
        broker_position = identity.canonical_position
        key = _position_key(broker_position)
        if identity.classification in {IDENTITY_NOT_READY, IDENTITY_AMBIGUOUS}:
            review.append(
                {
                    "classification": AMBIGUOUS_EXPOSURE_OWNERSHIP
                    if identity.classification == IDENTITY_AMBIGUOUS
                    else UNMANAGED_BROKER_EXPOSURE,
                    "reason_codes": list(identity.reason_codes),
                    "broker_position": raw_broker_position,
                    "canonical_broker_position": broker_position,
                    "canonical_identity_resolution": identity.to_dict(),
                    "position_key": key,
                }
            )
            continue

        lifecycle_match = _matching_lifecycle_position(broker_position, lifecycle_rows)
        registry_matches = _registry_matches_for_broker_position(active_records, broker_position)
        owner_record, superseded_records, owner_reason_codes = _select_current_registry_owner(
            registry_matches=registry_matches,
            lifecycle_match=lifecycle_match,
        )
        if owner_record is None and registry_matches:
            review.append(
                {
                    "classification": AMBIGUOUS_EXPOSURE_OWNERSHIP,
                    "reason_codes": owner_reason_codes or ["MULTIPLE_REGISTRY_TRADE_IDS_MATCH_BROKER_POSITION"],
                    "broker_position": raw_broker_position,
                    "canonical_broker_position": broker_position,
                    "canonical_identity_resolution": identity.to_dict(),
                    "matching_trade_ids": [record.trade_id for record in registry_matches],
                    "position_key": key,
                }
            )
            continue
        stale.extend(_stale_record_rows(superseded_records))

        if owner_record is not None:
            lifecycle_row = _lifecycle_position_from_registry_record(
                record=owner_record,
                broker_position=broker_position,
                lifecycle_reports=lifecycle_reports,
            )
            exit_due = _owner_exit_due(
                record=owner_record,
                lifecycle_row=lifecycle_row,
                lifecycle_match=lifecycle_match,
                managed_position_registry=managed_positions_payload,
            )
            classification = OWNED_MANAGED_EXIT_DUE if exit_due else OWNED_MANAGED_EXPOSURE
            exposure = {
                "classification": classification,
                "reason_codes": owner_reason_codes,
                "broker_position": raw_broker_position,
                "canonical_broker_position": broker_position,
                "canonical_identity_resolution": identity.to_dict(),
                "lifecycle_position": lifecycle_row,
                "trade_id": owner_record.trade_id,
                "lifecycle_id": lifecycle_row.get("lifecycle_id"),
                "position_key": key,
                "current_state": owner_record.current_state.value,
                "exit_due": exit_due,
            }
            owned.append(exposure)
            resolved_lifecycle_positions.append(lifecycle_row)
            continue

        if lifecycle_match:
            exit_due = _lifecycle_exit_due(lifecycle_match)
            exposure = {
                "classification": OWNED_MANAGED_EXIT_DUE if exit_due else OWNED_MANAGED_EXPOSURE,
                "reason_codes": ["EXACT_LIFECYCLE_PROJECTION_MATCHED_BROKER_POSITION"],
                "broker_position": raw_broker_position,
                "canonical_broker_position": broker_position,
                "canonical_identity_resolution": identity.to_dict(),
                "lifecycle_position": lifecycle_match,
                "trade_id": lifecycle_match.get("trade_id"),
                "lifecycle_id": lifecycle_match.get("lifecycle_id"),
                "position_key": key,
                "exit_due": exit_due,
            }
            owned.append(exposure)
            resolved_lifecycle_positions.append(lifecycle_match)
            continue

        adoptable = _adoptable_lifecycle_report_for_broker_position(
            broker_position=broker_position,
            lifecycle_reports=lifecycle_reports,
        )
        if adoptable:
            lifecycle_row = _lifecycle_position_from_lifecycle_report(adoptable, broker_position=broker_position)
            exposure = {
                "classification": OWNED_MANAGED_EXIT_DUE
                if _lifecycle_exit_due(lifecycle_row)
                or _lifecycle_exit_due(adoptable)
                or _managed_registry_exit_due_for_owner(managed_positions_payload, lifecycle_row)
                else OWNED_MANAGED_EXPOSURE,
                "reason_codes": ["EXACT_BROKER_BACKED_LIFECYCLE_REPORT_CAN_REPAIR_PROJECTION"],
                "broker_position": raw_broker_position,
                "canonical_broker_position": broker_position,
                "canonical_identity_resolution": identity.to_dict(),
                "lifecycle_position": lifecycle_row,
                "trade_id": lifecycle_row.get("trade_id"),
                "lifecycle_id": lifecycle_row.get("lifecycle_id"),
                "position_key": key,
                "exit_due": _lifecycle_exit_due(lifecycle_row)
                or _lifecycle_exit_due(adoptable)
                or _managed_registry_exit_due_for_owner(managed_positions_payload, lifecycle_row),
            }
            owned.append(exposure)
            resolved_lifecycle_positions.append(lifecycle_row)
            continue

        review.append(
            {
                "classification": UNMANAGED_BROKER_EXPOSURE,
                "reason_codes": ["NO_EXACT_REGISTRY_OR_BROKER_BACKED_LIFECYCLE_IDENTITY"],
                "broker_position": raw_broker_position,
                "canonical_broker_position": broker_position,
                "canonical_identity_resolution": identity.to_dict(),
                "position_key": key,
            }
        )

    all_owned = len(owned) == len(broker_rows) and not review
    if all_owned:
        classification = (
            OWNED_MANAGED_EXIT_DUE
            if any(item.get("classification") == OWNED_MANAGED_EXIT_DUE for item in owned)
            else OWNED_MANAGED_EXPOSURE
        )
    elif any(item.get("classification") == AMBIGUOUS_EXPOSURE_OWNERSHIP for item in review):
        classification = AMBIGUOUS_EXPOSURE_OWNERSHIP
    else:
        classification = UNMANAGED_BROKER_EXPOSURE

    return {
        "classification": classification,
        "broker_position_count": len(broker_rows),
        "broker_open_order_count": len(open_order_rows),
        "owned_exposure_count": len(owned),
        "review_required_exposure_count": len(review),
        "owned_exposures": owned,
        "review_required_exposures": review,
        "resolved_lifecycle_positions": resolved_lifecycle_positions,
        "stale_superseded_full_audit_only": stale,
        "read_only": True,
        "no_broad_flatten_generated": True,
    }


def _select_current_registry_owner(
    *,
    registry_matches: Sequence[TradeRegistryRecord],
    lifecycle_match: Mapping[str, Any],
) -> tuple[TradeRegistryRecord | None, tuple[TradeRegistryRecord, ...], list[str]]:
    required = [record for record in registry_matches if _registry_record_has_required_identity(record)]
    if not required:
        return None, (), []
    lifecycle_id = str(lifecycle_match.get("lifecycle_id") or "")
    trade_id = str(lifecycle_match.get("trade_id") or "")
    if lifecycle_id or trade_id:
        lifecycle_records = [
            record
            for record in required
            if (trade_id and record.trade_id == trade_id)
            or (
                lifecycle_id
                and record.ownership_identity is not None
                and record.ownership_identity.lifecycle_id == lifecycle_id
            )
        ]
        if len(lifecycle_records) == 1:
            owner = lifecycle_records[0]
            return (
                owner,
                tuple(record for record in required if record.trade_id != owner.trade_id),
                ["EXACT_LIFECYCLE_PROJECTION_MATCHED_BROKER_POSITION"],
            )
        if len(lifecycle_records) > 1:
            return None, (), ["MULTIPLE_LIFECYCLE_MATCHED_REGISTRY_OWNERS"]
    if len(required) == 1:
        return required[0], (), ["REGISTRY_OPEN_MANAGED_MATCHED_BROKER_POSITION"]

    ranked = sorted(required, key=_latest_broker_backed_entry_time, reverse=True)
    newest = ranked[0]
    newest_time = _latest_broker_backed_entry_time(newest)
    if newest_time is not None and sum(1 for item in ranked if _latest_broker_backed_entry_time(item) == newest_time) == 1:
        return (
            newest,
            tuple(record for record in required if record.trade_id != newest.trade_id),
            [
                "NEWEST_EXACT_BROKER_BACKED_ENTRY_SELECTED",
                "OLDER_MATCHING_OPEN_CHAINS_SCOPED_FULL_AUDIT_ONLY",
            ],
        )
    return None, (), ["MULTIPLE_PLAUSIBLE_CURRENT_REGISTRY_OWNERS"]


def _registry_matches_for_broker_position(
    records: Sequence[TradeRegistryRecord],
    broker_position: Mapping[str, Any],
) -> list[TradeRegistryRecord]:
    return [
        record
        for record in records
        if record.ownership_identity is not None
        and record.broker_backed_entry
        and _same_account(record.ownership_identity.account_id, broker_position.get("account_id"))
        and str(record.ownership_identity.local_symbol or "").upper()
        == str(broker_position.get("local_symbol") or "").upper()
        and int(record.ownership_identity.con_id) == int(broker_position.get("con_id") or 0)
        and _signed_owner_qty(record) == _decimal(broker_position.get("quantity"))
    ]


def _registry_record_has_required_identity(record: TradeRegistryRecord) -> bool:
    owner = record.ownership_identity
    return bool(
        owner
        and record.trade_id
        and owner.lifecycle_id
        and owner.account_id
        and owner.local_symbol
        and owner.con_id
        and record.broker_backed_entry
    )


def _lifecycle_position_from_registry_record(
    *,
    record: TradeRegistryRecord,
    broker_position: Mapping[str, Any],
    lifecycle_reports: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    owner = record.ownership_identity
    assert owner is not None
    report = _lifecycle_report_for_id(owner.lifecycle_id or "", lifecycle_reports)
    return {
        "source": "CURRENT_EXPOSURE_OWNER_RESOLVER",
        "projection_repair": "LIFECYCLE_PROJECTION_STALE",
        "trade_id": record.trade_id,
        "lifecycle_id": owner.lifecycle_id,
        "lane_id": owner.lane_id,
        "strategy_id": owner.thesis_strategy_id,
        "account_id": owner.account_id,
        "instrument_family": owner.symbol,
        "track_b_root": owner.symbol,
        "symbol": owner.symbol,
        "contract_key": _contract_key(owner.symbol, owner.expiry),
        "local_symbol": owner.local_symbol,
        "con_id": owner.con_id,
        "expiry": owner.expiry or str(broker_position.get("expiry") or ""),
        "quantity": str(owner.qty),
        "aggregate_qty": _decimal_display(_signed_owner_qty(record)),
        "side": owner.side,
        "avg_entry_price": _first_nonempty(record.entry_price, _entry_fill(report).get("price")),
        "entry_timestamp": _first_nonempty(report.get("entry_timestamp"), _entry_fill(report).get("filled_at")),
        "managed_exit_policy_id": _first_nonempty(
            report.get("managed_exit_policy_id"),
            _metadata_value(record, "managed_exit_policy_id"),
        ),
        "exit_due": _owner_exit_due(
            record=record,
            lifecycle_row=report,
            lifecycle_match={},
            managed_position_registry={},
        ),
        "entry_perm_ids": [_latest_event_value(record, "perm_id")],
        "entry_order_ids": [_latest_event_value(record, "order_id")],
        "entry_exec_ids": [_latest_event_value(record, "exec_id")],
        "paper_lifecycle_report_path": report.get("report_json_path"),
    }


def _adoptable_lifecycle_report_for_broker_position(
    *,
    broker_position: Mapping[str, Any],
    lifecycle_reports: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    matches = [
        dict(report)
        for report in lifecycle_reports
        if _lifecycle_report_broker_backed(report)
        and _position_key(report) == _position_key(broker_position)
        and _same_account(report.get("account_id"), broker_position.get("account_id"))
        and int(report.get("con_id") or 0) == int(broker_position.get("con_id") or 0)
        and _signed_report_qty(report) == _decimal(broker_position.get("quantity"))
    ]
    return matches[-1] if len(matches) == 1 else {}


def _lifecycle_position_from_lifecycle_report(
    report: Mapping[str, Any],
    *,
    broker_position: Mapping[str, Any],
) -> dict[str, Any]:
    fill = _entry_fill(report)
    side = str(report.get("side") or _entry_side_from_action(fill.get("action") or report.get("action")) or "").upper()
    qty = _decimal(report.get("quantity") or fill.get("qty") or fill.get("quantity") or "1") or Decimal("1")
    return {
        "source": "CURRENT_EXPOSURE_OWNER_RESOLVER",
        "projection_repair": "BROKER_BACKED_LIFECYCLE_REPORT_ADOPTABLE",
        "trade_id": report.get("trade_id"),
        "lifecycle_id": report.get("lifecycle_id"),
        "lane_id": report.get("lane_id") or report.get("strategy_id"),
        "strategy_id": report.get("strategy_id"),
        "account_id": report.get("account_id") or broker_position.get("account_id"),
        "instrument_family": report.get("instrument_family") or report.get("symbol") or broker_position.get("symbol"),
        "track_b_root": report.get("instrument_family") or report.get("symbol") or broker_position.get("symbol"),
        "symbol": report.get("symbol") or report.get("instrument_family") or broker_position.get("symbol"),
        "contract_key": report.get("contract_key") or _contract_key(
            str(report.get("instrument_family") or broker_position.get("symbol") or ""),
            str(report.get("expiry") or broker_position.get("expiry") or ""),
        ),
        "local_symbol": report.get("local_symbol") or broker_position.get("local_symbol"),
        "con_id": report.get("con_id") or broker_position.get("con_id"),
        "expiry": report.get("expiry") or broker_position.get("expiry"),
        "quantity": str(qty),
        "aggregate_qty": _decimal_display(_decimal(broker_position.get("quantity"))),
        "side": side,
        "avg_entry_price": fill.get("price") or report.get("entry_price"),
        "entry_timestamp": report.get("entry_timestamp") or fill.get("filled_at"),
        "managed_exit_policy_id": report.get("managed_exit_policy_id"),
        "exit_due": _lifecycle_exit_due(report),
        "entry_perm_ids": [fill.get("perm_id")],
        "entry_order_ids": [fill.get("order_id")],
        "entry_exec_ids": [fill.get("exec_id")],
        "paper_lifecycle_report_path": report.get("report_json_path"),
    }


def _matching_lifecycle_position(
    broker_position: Mapping[str, Any],
    lifecycle_positions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    matches = [
        dict(row)
        for row in lifecycle_positions
        if _position_key(row) == _position_key(broker_position)
        and _lifecycle_identity_proves_broker_position(row, broker_position)
    ]
    return matches[-1] if len(matches) == 1 else {}


def _lifecycle_identity_proves_broker_position(
    lifecycle_position: Mapping[str, Any],
    broker_position: Mapping[str, Any],
) -> bool:
    return (
        _same_account(lifecycle_position.get("account_id"), broker_position.get("account_id"))
        and int(lifecycle_position.get("con_id") or 0) == int(broker_position.get("con_id") or 0)
        and str(lifecycle_position.get("local_symbol") or "").upper()
        == str(broker_position.get("local_symbol") or "").upper()
        and _signed_lifecycle_qty(lifecycle_position) == _decimal(broker_position.get("quantity"))
        and bool(lifecycle_position.get("lifecycle_id"))
    )


def _owner_exit_due(
    *,
    record: TradeRegistryRecord,
    lifecycle_row: Mapping[str, Any],
    lifecycle_match: Mapping[str, Any],
    managed_position_registry: Mapping[str, Any],
) -> bool:
    return (
        record.current_state == TradeCurrentState.EXIT_DUE
        or _lifecycle_exit_due(lifecycle_row)
        or _lifecycle_exit_due(lifecycle_match)
        or _managed_registry_exit_due_for_owner(managed_position_registry, lifecycle_row)
    )


def _lifecycle_exit_due(row: Mapping[str, Any]) -> bool:
    values = {
        str(row.get("classification") or "").upper(),
        str(row.get("current_state") or "").upper(),
        str(row.get("state") or "").upper(),
        str(row.get("exit_status") or "").upper(),
    }
    return row.get("exit_due") is True or "OPEN_MANAGED_EXIT_DUE" in values or "EXIT_DUE" in values


def _managed_registry_exit_due_for_owner(
    managed_position_registry: Mapping[str, Any],
    owner: Mapping[str, Any],
) -> bool:
    trade_id = str(owner.get("trade_id") or "")
    lifecycle_id = str(owner.get("lifecycle_id") or "")
    if not trade_id and not lifecycle_id:
        return False
    for position in managed_position_registry.get("managed_positions") or []:
        if not isinstance(position, Mapping):
            continue
        if trade_id and str(position.get("trade_id") or "") != trade_id:
            continue
        if lifecycle_id and str(position.get("lifecycle_id") or "") != lifecycle_id:
            continue
        return _lifecycle_exit_due(position)
    return False


def _stale_record_rows(records: Sequence[TradeRegistryRecord]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in records:
        owner = record.ownership_identity
        rows.append(
            {
                "classification": STALE_SUPERSEDED_FULL_AUDIT_ONLY,
                "reason_codes": ["OLDER_MATCHING_OPEN_CHAIN_SUPERSEDED_BY_CURRENT_BROKER_BACKED_OWNER"],
                "trade_id": record.trade_id,
                "lifecycle_id": owner.lifecycle_id if owner is not None else None,
                "current_state": record.current_state.value,
                "broker_backed_entry": record.broker_backed_entry,
                "broker_backed_exit": record.broker_backed_exit,
                "open_qty": str(record.open_qty),
            }
        )
    return rows


def _latest_broker_backed_entry_time(record: TradeRegistryRecord) -> datetime | None:
    times = [
        event.generated_at
        for event in record.event_chain
        if str(event.event_type.value) == "ENTRY_FILL_BROKER_BACKED" and event.broker_backed
    ]
    return max(times) if times else None


def _lifecycle_report_for_id(lifecycle_id: str, lifecycle_reports: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    matches = [dict(report) for report in lifecycle_reports if str(report.get("lifecycle_id") or "") == lifecycle_id]
    return matches[-1] if matches else {}


def _lifecycle_report_broker_backed(report: Mapping[str, Any]) -> bool:
    fill = _entry_fill(report)
    return bool(fill.get("exec_id") and fill.get("perm_id"))


def _signed_owner_qty(record: TradeRegistryRecord) -> Decimal | None:
    owner = record.ownership_identity
    if owner is None:
        return None
    return -owner.qty if str(owner.side).upper() == "SHORT" else owner.qty


def _signed_lifecycle_qty(row: Mapping[str, Any]) -> Decimal | None:
    aggregate = _decimal(row.get("aggregate_qty"))
    if aggregate is not None:
        return aggregate
    qty = _decimal(row.get("quantity"))
    if qty is None:
        return None
    return -qty if str(row.get("side") or "").upper() == "SHORT" and qty > 0 else qty


def _signed_report_qty(report: Mapping[str, Any]) -> Decimal | None:
    broker_qty = _decimal(report.get("aggregate_qty"))
    if broker_qty is not None:
        return broker_qty
    fill = _entry_fill(report)
    qty = _decimal(report.get("quantity") or fill.get("qty") or fill.get("quantity") or "1")
    if qty is None:
        return None
    side = str(report.get("side") or _entry_side_from_action(fill.get("action") or report.get("action")) or "").upper()
    return -qty if side == "SHORT" else qty


def _position_key(row: Mapping[str, Any]) -> str:
    return str(row.get("local_symbol") or row.get("contract_key") or row.get("position_key") or "").upper()


def _same_account(left: object, right: object) -> bool:
    return str(left or "").strip() == str(right or "").strip() and bool(str(left or "").strip())


def _contract_key(symbol: str, expiry: str) -> str:
    return f"{str(symbol).upper()}-{str(expiry)[:6]}" if symbol and expiry else ""


def _entry_fill(report: Mapping[str, Any]) -> dict[str, Any]:
    fill = report.get("entry_fill")
    return dict(fill) if isinstance(fill, Mapping) else {}


def _entry_side_from_action(action: object) -> str:
    value = str(action or "").upper()
    if value in {"SELL", "SELL_TO_OPEN"}:
        return "SHORT"
    if value in {"BUY", "BUY_TO_OPEN"}:
        return "LONG"
    return ""


def _latest_event_value(record: TradeRegistryRecord, field_name: str) -> str | None:
    for event in reversed(record.event_chain):
        value = getattr(event, field_name, None)
        if value:
            return str(value)
    return None


def _metadata_value(record: TradeRegistryRecord, key: str) -> str:
    for event in reversed(record.event_chain):
        metadata = event.metadata if isinstance(event.metadata, Mapping) else {}
        value = metadata.get(key)
        if value:
            return str(value)
    return ""


def _first_nonempty(*values: object) -> str | None:
    for value in values:
        if value not in {None, ""}:
            return str(value)
    return None


def _decimal(value: object) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _decimal_display(value: Decimal | None) -> str | None:
    if value is None:
        return None
    if value == value.to_integral_value():
        return str(value.quantize(Decimal("1")))
    return str(value.normalize())


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}
