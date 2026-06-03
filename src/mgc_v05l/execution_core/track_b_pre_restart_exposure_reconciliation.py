"""Read-only exposure resolver for restart and managed-close decisions.

The resolver answers one narrow question: when broker truth shows exposure,
can we prove it is an exact registry-backed managed position before treating
it as unmanaged?  It never submits, cancels, flattens, or rewrites broker
state.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_current_exposure_owner_resolver import (
    AMBIGUOUS_EXPOSURE_OWNERSHIP,
    NO_OPEN_EXPOSURE as CURRENT_NO_OPEN_EXPOSURE,
    OWNED_MANAGED_EXIT_DUE,
    OWNED_MANAGED_EXPOSURE,
    UNMANAGED_BROKER_EXPOSURE,
    CurrentExposureOwnerResolverConfig,
    resolve_current_exposure_ownership,
)
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
    contract_resolver_status_path: Path = (
        Path("outputs") / "track_b_execution_core" / "contract_resolver" / "latest_contract_resolver_status.json"
    )

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


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

    owner_payload = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(
            repo_root=config.repo_root,
            contract_resolver_status_path=config.contract_resolver_status_path,
        ),
        broker_positions=broker_positions,
        broker_open_orders=broker_open_orders,
        registry_records=registry_records,
        lifecycle_positions=lifecycle_positions,
        lifecycle_reports=lifecycle_reports,
    )
    if owner_payload.get("classification") == CURRENT_NO_OPEN_EXPOSURE:
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
            "current_exposure_owner_resolution": owner_payload,
        }

    managed = [_legacy_managed_exposure(item) for item in owner_payload.get("owned_exposures") or []]
    review = [_legacy_review_exposure(item) for item in owner_payload.get("review_required_exposures") or []]
    resolved_lifecycle_positions = [dict(item) for item in owner_payload.get("resolved_lifecycle_positions") or []]
    all_resolved = bool(managed) and int(owner_payload.get("owned_exposure_count") or 0) == int(
        owner_payload.get("broker_position_count") or 0
    ) and not review
    if all_resolved:
        classification = (
            MANAGED_EXPOSURE_RESOLVED
            if all("EXACT_LIFECYCLE_PROJECTION_MATCHED_BROKER_POSITION" in item.get("reason_codes", []) for item in managed)
            else PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED
        )
    elif owner_payload.get("classification") == AMBIGUOUS_EXPOSURE_OWNERSHIP:
        classification = REVIEW_REQUIRED_AMBIGUOUS_MANAGED_EXPOSURE
    else:
        classification = REVIEW_REQUIRED_UNMANAGED_BROKER_EXPOSURE
    return {
        "classification": classification,
        "broker_position_count": owner_payload.get("broker_position_count", 0),
        "broker_open_order_count": len(list(broker_open_orders)),
        "resolved_managed_exposure_count": len(managed),
        "review_required_exposure_count": len(review),
        "resolved_lifecycle_positions": resolved_lifecycle_positions,
        "managed_exposures": managed,
        "review_required_exposures": review,
        "restart_with_owned_exposure_allowed": all_resolved and not list(broker_open_orders),
        "no_broad_flatten_generated": True,
        "read_only": True,
        "current_exposure_owner_resolution": owner_payload,
    }


def _legacy_managed_exposure(exposure: Mapping[str, Any]) -> dict[str, Any]:
    shared_classification = str(exposure.get("classification") or "")
    reason_codes = [str(item) for item in exposure.get("reason_codes") or []]
    lifecycle_position = dict(exposure.get("lifecycle_position") or {})
    if shared_classification in {OWNED_MANAGED_EXPOSURE, OWNED_MANAGED_EXIT_DUE}:
        classification = (
            MANAGED_EXPOSURE_RESOLVED
            if "EXACT_LIFECYCLE_PROJECTION_MATCHED_BROKER_POSITION" in reason_codes
            else PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED
        )
    else:
        classification = ADOPTABLE_BROKER_BACKED_EXPOSURE
    return {
        "classification": classification,
        "shared_owner_classification": shared_classification,
        "reason_codes": reason_codes,
        "broker_position": dict(exposure.get("broker_position") or {}),
        "canonical_broker_position": dict(exposure.get("canonical_broker_position") or {}),
        "canonical_identity_resolution": dict(exposure.get("canonical_identity_resolution") or {}),
        "lifecycle_position": lifecycle_position,
        "trade_id": exposure.get("trade_id") or lifecycle_position.get("trade_id"),
        "lifecycle_id": exposure.get("lifecycle_id") or lifecycle_position.get("lifecycle_id"),
        "position_key": exposure.get("position_key"),
        "exit_due": exposure.get("exit_due") is True,
    }


def _legacy_review_exposure(exposure: Mapping[str, Any]) -> dict[str, Any]:
    shared_classification = str(exposure.get("classification") or "")
    classification = (
        REVIEW_REQUIRED_AMBIGUOUS_MANAGED_EXPOSURE
        if shared_classification == AMBIGUOUS_EXPOSURE_OWNERSHIP
        else REVIEW_REQUIRED_UNMANAGED_BROKER_EXPOSURE
    )
    return {
        "classification": classification,
        "shared_owner_classification": shared_classification,
        "reason_codes": [str(item) for item in exposure.get("reason_codes") or []],
        "broker_position": dict(exposure.get("broker_position") or {}),
        "canonical_broker_position": dict(exposure.get("canonical_broker_position") or {}),
        "canonical_identity_resolution": dict(exposure.get("canonical_identity_resolution") or {}),
        "matching_trade_ids": list(exposure.get("matching_trade_ids") or []),
        "position_key": exposure.get("position_key"),
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


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}
