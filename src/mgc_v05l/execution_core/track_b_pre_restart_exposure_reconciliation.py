"""Read-only exposure resolver for restart and managed-close decisions.

The resolver answers one narrow question: when broker truth shows exposure,
can we prove it is an exact registry-backed managed position before treating
it as unmanaged?  It never submits, cancels, flattens, or rewrites broker
state.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeCurrentState, TradeRegistryRecord
from mgc_v05l.execution_core.track_b_live_trade_registry import load_live_trade_registry_records


NO_OPEN_EXPOSURE = "NO_OPEN_EXPOSURE"
MANAGED_EXPOSURE_RESOLVED = "MANAGED_EXPOSURE_RESOLVED"
PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED = "PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED"
ADOPTABLE_BROKER_BACKED_EXPOSURE = "ADOPTABLE_BROKER_BACKED_EXPOSURE"
REVIEW_REQUIRED_UNMANAGED_BROKER_EXPOSURE = "REVIEW_REQUIRED_UNMANAGED_BROKER_EXPOSURE"
REVIEW_REQUIRED_AMBIGUOUS_MANAGED_EXPOSURE = "REVIEW_REQUIRED_AMBIGUOUS_MANAGED_EXPOSURE"

OPEN_REGISTRY_STATES = {
    TradeCurrentState.OPEN_MANAGED,
    TradeCurrentState.EXIT_DUE,
    TradeCurrentState.WORKING_EXIT,
}


@dataclass(frozen=True)
class PreRestartExposureResolverConfig:
    repo_root: Path


def resolve_pre_restart_exposure_reconciliation(
    *,
    config: PreRestartExposureResolverConfig,
    broker_positions: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
    broker_open_orders: Sequence[Mapping[str, Any]] = (),
    lifecycle_reports: Sequence[Mapping[str, Any]] = (),
    registry_records: Sequence[TradeRegistryRecord] | None = None,
) -> dict[str, Any]:
    """Resolve current broker exposure into managed/adoptable/unmanaged buckets."""

    broker_rows = [dict(row) for row in broker_positions if _position_key(row)]
    lifecycle_rows = [dict(row) for row in lifecycle_positions if _position_key(row)]
    if not broker_rows:
        return {
            "classification": NO_OPEN_EXPOSURE,
            "broker_position_count": 0,
            "broker_open_order_count": len(list(broker_open_orders)),
            "resolved_managed_exposure_count": 0,
            "review_required_exposure_count": 0,
            "resolved_lifecycle_positions": [],
            "managed_exposures": [],
            "review_required_exposures": [],
            "restart_with_owned_exposure_allowed": False,
            "no_broad_flatten_generated": True,
            "read_only": True,
        }

    records = tuple(registry_records) if registry_records is not None else load_live_trade_registry_records(
        repo_root=config.repo_root
    )
    active_records = [record for record in records if record.current_state in OPEN_REGISTRY_STATES]
    managed: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []
    resolved_lifecycle_positions: list[dict[str, Any]] = []

    for broker_position in broker_rows:
        key = _position_key(broker_position)
        lifecycle_match = _matching_lifecycle_position(broker_position, lifecycle_rows)
        registry_matches = _registry_matches_for_broker_position(active_records, broker_position)
        if lifecycle_match and _lifecycle_identity_proves_broker_position(lifecycle_match, broker_position):
            exposure = {
                "classification": MANAGED_EXPOSURE_RESOLVED,
                "reason_codes": ["EXACT_LIFECYCLE_PROJECTION_MATCHED_BROKER_POSITION"],
                "broker_position": broker_position,
                "lifecycle_position": lifecycle_match,
                "trade_id": lifecycle_match.get("trade_id"),
                "lifecycle_id": lifecycle_match.get("lifecycle_id"),
                "position_key": key,
            }
            managed.append(exposure)
            resolved_lifecycle_positions.append(lifecycle_match)
            continue
        if len(registry_matches) == 1 and _registry_record_has_required_identity(registry_matches[0]):
            row = _lifecycle_position_from_registry_record(
                record=registry_matches[0],
                broker_position=broker_position,
                lifecycle_reports=lifecycle_reports,
            )
            exposure = {
                "classification": PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED,
                "reason_codes": [
                    "REGISTRY_OPEN_MANAGED_MATCHED_BROKER_POSITION",
                    "LIFECYCLE_PROJECTION_STALE_OR_EMPTY",
                ],
                "broker_position": broker_position,
                "lifecycle_position": row,
                "trade_id": row.get("trade_id"),
                "lifecycle_id": row.get("lifecycle_id"),
                "position_key": key,
            }
            managed.append(exposure)
            resolved_lifecycle_positions.append(row)
            continue
        if len(registry_matches) > 1:
            review.append(
                {
                    "classification": REVIEW_REQUIRED_AMBIGUOUS_MANAGED_EXPOSURE,
                    "reason_codes": ["MULTIPLE_REGISTRY_TRADE_IDS_MATCH_BROKER_POSITION"],
                    "broker_position": broker_position,
                    "matching_trade_ids": [record.trade_id for record in registry_matches],
                    "position_key": key,
                }
            )
            continue
        adoptable = _adoptable_lifecycle_report_for_broker_position(
            broker_position=broker_position,
            lifecycle_reports=lifecycle_reports,
        )
        if adoptable:
            row = _lifecycle_position_from_lifecycle_report(adoptable, broker_position=broker_position)
            managed.append(
                {
                    "classification": ADOPTABLE_BROKER_BACKED_EXPOSURE,
                    "reason_codes": ["EXACT_BROKER_BACKED_LIFECYCLE_REPORT_CAN_REPAIR_PROJECTION"],
                    "broker_position": broker_position,
                    "lifecycle_position": row,
                    "trade_id": row.get("trade_id"),
                    "lifecycle_id": row.get("lifecycle_id"),
                    "position_key": key,
                }
            )
            resolved_lifecycle_positions.append(row)
            continue
        review.append(
            {
                "classification": REVIEW_REQUIRED_UNMANAGED_BROKER_EXPOSURE,
                "reason_codes": ["NO_EXACT_REGISTRY_OR_BROKER_BACKED_LIFECYCLE_IDENTITY"],
                "broker_position": broker_position,
                "position_key": key,
            }
        )

    all_resolved = len(managed) == len(broker_rows) and not review
    classification = (
        MANAGED_EXPOSURE_RESOLVED
        if all(item["classification"] == MANAGED_EXPOSURE_RESOLVED for item in managed) and all_resolved
        else PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED
        if all_resolved
        else REVIEW_REQUIRED_UNMANAGED_BROKER_EXPOSURE
    )
    return {
        "classification": classification,
        "broker_position_count": len(broker_rows),
        "broker_open_order_count": len(list(broker_open_orders)),
        "resolved_managed_exposure_count": len(managed),
        "review_required_exposure_count": len(review),
        "resolved_lifecycle_positions": resolved_lifecycle_positions,
        "managed_exposures": managed,
        "review_required_exposures": review,
        "restart_with_owned_exposure_allowed": all_resolved and not list(broker_open_orders),
        "no_broad_flatten_generated": True,
        "read_only": True,
    }


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
        "source": "PRE_RESTART_EXPOSURE_RECONCILIATION_RESOLVER",
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
        "source": "PRE_RESTART_EXPOSURE_RECONCILIATION_RESOLVER",
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
