"""Read-only terminal-event publisher for managed close orders.

This module publishes evidence that an exact managed close order reached a
terminal broker effect. It never connects to a broker, submits, cancels,
modifies, closes, restarts services, or participates in trading authority.
Uncertain outcomes are diagnostic-only and must not block unrelated trading.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_jsonl import append_bounded_jsonl
from mgc_v05l.execution_core.models import to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic


SCHEMA_VERSION = "track_b_managed_close_terminal_event_v1"
SUMMARY_SCHEMA_VERSION = "track_b_managed_close_terminal_summary_v1"
BROKER_EFFECT_OBSERVED_FLAT = "BROKER_EFFECT_OBSERVED_FLAT"
BROKER_EFFECT_OBSERVED_REDUCED = "BROKER_EFFECT_OBSERVED_REDUCED"
TERMINAL_FILL_CONFIRMED = "TERMINAL_FILL_CONFIRMED"
TERMINAL_CANCEL_CONFIRMED = "TERMINAL_CANCEL_CONFIRMED"
ORDER_NOT_TERMINAL_STILL_OPEN = "ORDER_NOT_TERMINAL_STILL_OPEN"
INSUFFICIENT_EVIDENCE_NO_EVENT = "INSUFFICIENT_EVIDENCE_NO_EVENT"
BROKER_TRUTH_INCOMPLETE = "BROKER_TRUTH_INCOMPLETE"
UNKNOWN_ORDERS_PRESENT = "UNKNOWN_ORDERS_PRESENT"
IDENTITY_MISMATCH = "IDENTITY_MISMATCH"
AMBIGUOUS_CLOSE_ORDER = "AMBIGUOUS_CLOSE_ORDER"

TERMINAL_CLASSIFICATIONS = {
    BROKER_EFFECT_OBSERVED_FLAT,
    BROKER_EFFECT_OBSERVED_REDUCED,
    TERMINAL_FILL_CONFIRMED,
    TERMINAL_CANCEL_CONFIRMED,
}

DEFAULT_BROKER_POSITIONS_SNAPSHOT = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json"
)
DEFAULT_BROKER_OPEN_ORDERS_SNAPSHOT = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json"
)
DEFAULT_MANAGED_EXIT_ACTUATOR = (
    Path("outputs") / "track_b_execution_core" / "managed_exit_actuator" / "latest_managed_exit_actuator.json"
)
DEFAULT_TRADE_REGISTRY_EVENTS = (
    Path("outputs") / "track_b_execution_core" / "trade_registry" / "live_trade_events.jsonl"
)
DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core" / "managed_close_terminal_events"
DEFAULT_TERMINAL_EVENTS_JSONL = DEFAULT_OUTPUT_ROOT / "managed_close_terminal_events.jsonl"
DEFAULT_LATEST_SUMMARY = DEFAULT_OUTPUT_ROOT / "latest_managed_close_terminal_summary.json"
DEFAULT_ORDER_DIAGNOSTIC_ROOT = DEFAULT_OUTPUT_ROOT / "orders"


@dataclass(frozen=True)
class ManagedCloseTerminalPublisherConfig:
    repo_root: Path = Path(".")
    broker_positions_snapshot_path: Path = DEFAULT_BROKER_POSITIONS_SNAPSHOT
    broker_open_orders_snapshot_path: Path = DEFAULT_BROKER_OPEN_ORDERS_SNAPSHOT
    managed_exit_actuator_path: Path = DEFAULT_MANAGED_EXIT_ACTUATOR
    trade_registry_events_path: Path = DEFAULT_TRADE_REGISTRY_EVENTS
    terminal_events_path: Path = DEFAULT_TERMINAL_EVENTS_JSONL
    summary_path: Path = DEFAULT_LATEST_SUMMARY
    order_diagnostic_root: Path = DEFAULT_ORDER_DIAGNOSTIC_ROOT
    max_snapshot_age_seconds: float = 180.0
    write_order_diagnostics: bool = True

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def publish_managed_close_terminal_events(
    *,
    config: ManagedCloseTerminalPublisherConfig | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Publish terminal close-resolution events from fresh read-only artifacts."""

    actual_config = config or ManagedCloseTerminalPublisherConfig()
    generated_at = _ensure_utc(now or datetime.now(UTC))
    positions_snapshot = _read_json(actual_config.resolve(actual_config.broker_positions_snapshot_path))
    open_orders_snapshot = _read_json(actual_config.resolve(actual_config.broker_open_orders_snapshot_path))
    actuator = _read_json(actual_config.resolve(actual_config.managed_exit_actuator_path))
    registry_rows = _read_jsonl(actual_config.resolve(actual_config.trade_registry_events_path))
    submit_events = _submit_events_by_order(registry_rows)
    existing_event_ids = _existing_event_ids(actual_config.resolve(actual_config.terminal_events_path))

    attempts = _close_attempts_from_actuator(
        actuator=actuator,
        actuator_path=actual_config.resolve(actual_config.managed_exit_actuator_path),
        submit_events=submit_events,
    )
    attempts.extend(
        _close_attempts_from_registry(
            rows=registry_rows,
            source_path=actual_config.resolve(actual_config.trade_registry_events_path),
            existing_attempt_keys={_attempt_key(attempt) for attempt in attempts},
        )
    )

    diagnostics: list[dict[str, Any]] = []
    appended: list[dict[str, Any]] = []
    duplicate_count = 0
    for attempt in attempts:
        resolution = _resolve_attempt(
            attempt=attempt,
            positions_snapshot=positions_snapshot,
            open_orders_snapshot=open_orders_snapshot,
            generated_at=generated_at,
            max_snapshot_age_seconds=actual_config.max_snapshot_age_seconds,
        )
        if resolution["classification"] in TERMINAL_CLASSIFICATIONS:
            event = _terminal_event(attempt=attempt, resolution=resolution, generated_at=generated_at)
            if event["event_id"] in existing_event_ids:
                duplicate_count += 1
                diagnostics.append(_diagnostic(attempt, "DUPLICATE_TERMINAL_EVENT_SKIPPED", resolution))
            else:
                append_bounded_jsonl(actual_config.resolve(actual_config.terminal_events_path), event)
                existing_event_ids.add(str(event["event_id"]))
                appended.append(event)
        else:
            diagnostics.append(_diagnostic(attempt, str(resolution["classification"]), resolution))
        if actual_config.write_order_diagnostics:
            _write_order_diagnostic(
                config=actual_config,
                attempt=attempt,
                resolution=resolution,
                generated_at=generated_at,
            )

    summary = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "read_only_evidence_publisher": True,
        "broker_mutation_allowed": False,
        "runtime_authority": False,
        "entry_gating": False,
        "exit_gating": False,
        "profile_wide_authority": False,
        "attempt_count": len(attempts),
        "terminal_event_count": len(appended),
        "diagnostic_count": len(diagnostics),
        "duplicate_count": duplicate_count,
        "appended_events": [_event_summary(event) for event in appended],
        "diagnostics": diagnostics,
        "artifact_paths": {
            "terminal_events": str(actual_config.resolve(actual_config.terminal_events_path)),
            "summary": str(actual_config.resolve(actual_config.summary_path)),
            "broker_positions_snapshot": str(actual_config.resolve(actual_config.broker_positions_snapshot_path)),
            "broker_open_orders_snapshot": str(actual_config.resolve(actual_config.broker_open_orders_snapshot_path)),
            "managed_exit_actuator": str(actual_config.resolve(actual_config.managed_exit_actuator_path)),
            "trade_registry_events": str(actual_config.resolve(actual_config.trade_registry_events_path)),
        },
    }
    write_json_atomic(actual_config.resolve(actual_config.summary_path), to_jsonable(summary))
    return summary


def _resolve_attempt(
    *,
    attempt: Mapping[str, Any],
    positions_snapshot: Mapping[str, Any],
    open_orders_snapshot: Mapping[str, Any],
    generated_at: datetime,
    max_snapshot_age_seconds: float,
) -> dict[str, Any]:
    completeness = _snapshot_completeness(
        positions_snapshot=positions_snapshot,
        open_orders_snapshot=open_orders_snapshot,
        generated_at=generated_at,
        max_snapshot_age_seconds=max_snapshot_age_seconds,
    )
    if completeness["complete"] is not True:
        return {"classification": BROKER_TRUTH_INCOMPLETE, **completeness}
    unknown_orders = _unknown_open_orders(open_orders_snapshot)
    if unknown_orders:
        return {
            "classification": UNKNOWN_ORDERS_PRESENT,
            "unknown_orders": unknown_orders,
            **completeness,
        }
    identity_blockers = _attempt_identity_blockers(attempt)
    if identity_blockers:
        return {"classification": IDENTITY_MISMATCH, "identity_blockers": identity_blockers, **completeness}

    matching_orders, mismatched_orders = _matching_open_orders(open_orders_snapshot, attempt)
    if len(matching_orders) > 1:
        return {"classification": AMBIGUOUS_CLOSE_ORDER, "matching_open_orders": matching_orders, **completeness}
    if mismatched_orders:
        return {"classification": IDENTITY_MISMATCH, "mismatched_open_orders": mismatched_orders, **completeness}
    if matching_orders:
        return {"classification": ORDER_NOT_TERMINAL_STILL_OPEN, "matching_open_order": matching_orders[0], **completeness}

    terminal = _terminal_status_resolution(attempt)
    if terminal:
        return {**terminal, **completeness}

    current_qty = _current_signed_qty(positions_snapshot, attempt)
    pre_close_qty = _decimal(attempt.get("pre_close_signed_qty")) or _pre_close_signed_qty(attempt)
    if current_qty is None:
        return {"classification": IDENTITY_MISMATCH, "identity_blockers": ["current_position_identity_not_found"], **completeness}
    if pre_close_qty is None or pre_close_qty == 0:
        return {"classification": IDENTITY_MISMATCH, "identity_blockers": ["pre_close_signed_qty_missing"], **completeness}
    if current_qty == 0:
        return {
            "classification": BROKER_EFFECT_OBSERVED_FLAT,
            "resolution_source": "FRESH_BROKER_TRUTH",
            "confidence": "BROKER_TRUTH_CONFIRMED",
            "pre_close_signed_qty": _decimal_text(pre_close_qty),
            "post_close_signed_qty": "0",
            "open_order_present_after": False,
            **completeness,
        }
    if _same_direction(current_qty, pre_close_qty) and abs(current_qty) < abs(pre_close_qty):
        return {
            "classification": BROKER_EFFECT_OBSERVED_REDUCED,
            "resolution_source": "FRESH_BROKER_TRUTH",
            "confidence": "BROKER_TRUTH_CONFIRMED",
            "pre_close_signed_qty": _decimal_text(pre_close_qty),
            "post_close_signed_qty": _decimal_text(current_qty),
            "open_order_present_after": False,
            **completeness,
        }
    return {
        "classification": INSUFFICIENT_EVIDENCE_NO_EVENT,
        "pre_close_signed_qty": _decimal_text(pre_close_qty),
        "post_close_signed_qty": _decimal_text(current_qty),
        "open_order_present_after": False,
        **completeness,
    }


def _terminal_event(*, attempt: Mapping[str, Any], resolution: Mapping[str, Any], generated_at: datetime) -> dict[str, Any]:
    classification = str(resolution.get("classification") or "")
    event_id = _event_id(attempt=attempt, classification=classification)
    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": "MANAGED_CLOSE_TERMINAL_RESOLUTION",
        "event_id": event_id,
        "generated_at": generated_at.isoformat(),
        "classification": classification,
        "resolution_source": resolution.get("resolution_source") or "FRESH_BROKER_TRUTH",
        "confidence": resolution.get("confidence") or "BROKER_TRUTH_CONFIRMED",
        "read_only_evidence_publisher": True,
        "broker_mutation_allowed": False,
        "runtime_authority": False,
        "entry_gating": False,
        "exit_gating": False,
        "profile_wide_authority": False,
        "account_id": attempt.get("account_id"),
        "symbol": attempt.get("symbol"),
        "local_symbol": attempt.get("local_symbol"),
        "con_id": attempt.get("con_id"),
        "expiry": attempt.get("expiry"),
        "lane_id": attempt.get("lane_id"),
        "strategy_id": attempt.get("strategy_id"),
        "trade_id": attempt.get("trade_id"),
        "lifecycle_id": attempt.get("lifecycle_id"),
        "close_order_id": attempt.get("order_id"),
        "perm_id": attempt.get("perm_id"),
        "client_id": attempt.get("client_id"),
        "close_action": attempt.get("action"),
        "close_qty": _decimal_text(_decimal(attempt.get("quantity")) or Decimal("0")),
        "pre_close_signed_qty": resolution.get("pre_close_signed_qty") or _decimal_text(_pre_close_signed_qty(attempt) or Decimal("0")),
        "post_close_signed_qty": resolution.get("post_close_signed_qty"),
        "open_order_present_after": resolution.get("open_order_present_after", False),
        "unknown_orders_present": False,
        "terminal_status_observed": resolution.get("terminal_status_observed"),
        "exec_id": resolution.get("exec_id") or attempt.get("exec_id"),
        "fill_price": resolution.get("fill_price"),
        "fill_time": resolution.get("fill_time"),
        "source_refs": attempt.get("source_refs") or {},
        "notes": _event_notes(classification, resolution),
    }


def _close_attempts_from_actuator(
    *,
    actuator: Mapping[str, Any],
    actuator_path: Path,
    submit_events: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    attempts: list[dict[str, Any]] = []
    for row in _list(actuator.get("attempted_closes")):
        candidate = _mapping(row.get("close_candidate"))
        submit = _mapping(row.get("close_submit_attempt"))
        broker_order = _mapping(submit.get("broker_order"))
        fill = _mapping(submit.get("close_fill"))
        order_id = _text(row.get("order_id") or submit.get("broker_order_id") or broker_order.get("order_id"))
        registry = submit_events.get(order_id, {}) if order_id else {}
        action = _text(candidate.get("action") or registry.get("action") or broker_order.get("action"))
        side = _text(candidate.get("side") or registry.get("side"))
        attempt = {
            "account_id": _text(candidate.get("account_id") or registry.get("account_id")),
            "symbol": _text(candidate.get("symbol") or registry.get("symbol")),
            "local_symbol": _text(candidate.get("local_symbol") or registry.get("local_symbol")),
            "con_id": _int(candidate.get("con_id") or registry.get("con_id")),
            "expiry": _text(candidate.get("expiry") or registry.get("expiry")),
            "lane_id": _text(candidate.get("lane_id") or registry.get("lane_id")),
            "strategy_id": _text(candidate.get("strategy_id") or registry.get("thesis_strategy_id") or registry.get("strategy_id")),
            "trade_id": _text(candidate.get("trade_id") or registry.get("trade_id")),
            "lifecycle_id": _text(candidate.get("lifecycle_id") or registry.get("lifecycle_id")),
            "order_id": order_id,
            "perm_id": _text(row.get("perm_id") or fill.get("perm_id") or broker_order.get("perm_id") or registry.get("perm_id")),
            "client_id": _text(submit.get("client_id") or broker_order.get("client_id") or registry.get("client_id")),
            "action": action,
            "quantity": _decimal_text(_decimal(candidate.get("quantity") or registry.get("qty") or broker_order.get("quantity")) or Decimal("0")),
            "side": side,
            "pre_close_signed_qty": _pre_close_from_candidate(candidate, action=action),
            "broker_order_status": _text(broker_order.get("status")),
            "exec_id": _text(fill.get("exec_id") or fill.get("execution_id") or registry.get("exec_id")),
            "source_refs": {
                "managed_exit_actuator": str(actuator_path),
                **({"trade_registry_event": str(registry.get("_source_path"))} if registry.get("_source_path") else {}),
            },
        }
        if any(attempt.values()):
            attempts.append(attempt)
    return attempts


def _close_attempts_from_registry(
    *,
    rows: Sequence[Mapping[str, Any]],
    source_path: Path,
    existing_attempt_keys: set[tuple[str, str, str]],
) -> list[dict[str, Any]]:
    attempts: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("event_type") or "") != "EXIT_ORDER_SUBMITTED":
            continue
        attempt = {
            "account_id": _text(row.get("account_id")),
            "symbol": _text(row.get("symbol")),
            "local_symbol": _text(row.get("local_symbol")),
            "con_id": _int(row.get("con_id")),
            "expiry": _text(row.get("expiry")),
            "lane_id": _text(row.get("lane_id")),
            "strategy_id": _text(row.get("thesis_strategy_id")),
            "trade_id": _text(row.get("trade_id")),
            "lifecycle_id": _text(row.get("lifecycle_id")),
            "order_id": _text(row.get("order_id")),
            "perm_id": _text(row.get("perm_id")),
            "client_id": _text(row.get("client_id")),
            "action": _text(row.get("action")),
            "quantity": _decimal_text(_decimal(row.get("qty")) or Decimal("0")),
            "side": _text(row.get("side")),
            "pre_close_signed_qty": _pre_close_from_action(_text(row.get("action")), _decimal(row.get("qty")) or Decimal("0")),
            "source_refs": {"trade_registry_event": str(source_path)},
        }
        if _attempt_key(attempt) not in existing_attempt_keys:
            attempts.append(attempt)
    return attempts


def _submit_events_by_order(rows: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if str(row.get("event_type") or "") != "EXIT_ORDER_SUBMITTED":
            continue
        order_id = _text(row.get("order_id"))
        if order_id:
            result[order_id] = {**dict(row), "_source_path": "trade_registry/live_trade_events.jsonl"}
    return result


def _snapshot_completeness(
    *,
    positions_snapshot: Mapping[str, Any],
    open_orders_snapshot: Mapping[str, Any],
    generated_at: datetime,
    max_snapshot_age_seconds: float,
) -> dict[str, Any]:
    blockers: list[str] = []
    if positions_snapshot.get("positions_complete") is not True:
        blockers.append("positions_snapshot_incomplete")
    if open_orders_snapshot.get("open_orders_complete") is not True:
        blockers.append("open_orders_snapshot_incomplete")
    position_age = _age_seconds(positions_snapshot.get("generated_at"), generated_at)
    open_order_age = _age_seconds(open_orders_snapshot.get("generated_at"), generated_at)
    if position_age is None or position_age > max_snapshot_age_seconds:
        blockers.append("positions_snapshot_stale_or_missing_generated_at")
    if open_order_age is None or open_order_age > max_snapshot_age_seconds:
        blockers.append("open_orders_snapshot_stale_or_missing_generated_at")
    return {
        "complete": not blockers,
        "broker_positions_generated_at": positions_snapshot.get("generated_at"),
        "broker_open_orders_generated_at": open_orders_snapshot.get("generated_at"),
        "positions_age_seconds": position_age,
        "open_orders_age_seconds": open_order_age,
        "freshness_blockers": blockers,
    }


def _matching_open_orders(snapshot: Mapping[str, Any], attempt: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    matches: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    for order in _open_order_rows(snapshot):
        if _text(order.get("order_id") or order.get("broker_order_id")) != _text(attempt.get("order_id")):
            continue
        order_identity = _order_identity(order)
        blockers = _identity_mismatch_blockers(order_identity, attempt)
        if blockers:
            mismatches.append({"order": order_identity, "blockers": blockers})
        else:
            matches.append(order_identity)
    return matches, mismatches


def _current_signed_qty(snapshot: Mapping[str, Any], attempt: Mapping[str, Any]) -> Decimal | None:
    found_same_symbol = False
    for position in _list(snapshot.get("positions")):
        identity = _position_identity(position)
        if not _same_contract(identity, attempt):
            continue
        found_same_symbol = True
        return _decimal(identity.get("quantity")) or Decimal("0")
    return Decimal("0") if found_same_symbol or _attempt_identity_blockers(attempt) == [] else None


def _order_identity(order: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "account_id": _text(order.get("account_id") or order.get("account")),
        "symbol": _text(order.get("symbol")),
        "local_symbol": _text(order.get("local_symbol") or order.get("localSymbol")),
        "con_id": _int(order.get("con_id") or order.get("conId")),
        "expiry": _text(order.get("expiry") or order.get("lastTradeDateOrContractMonth")),
        "order_id": _text(order.get("order_id") or order.get("broker_order_id")),
        "perm_id": _text(order.get("perm_id") or order.get("permId")),
        "client_id": _text(order.get("client_id") or order.get("clientId")),
        "action": _text(order.get("action") or order.get("side")),
        "quantity": _decimal_text(_decimal(order.get("quantity") or order.get("totalQuantity")) or Decimal("0")),
        "status": _text(order.get("status")),
    }


def _position_identity(position: Mapping[str, Any]) -> dict[str, Any]:
    contract = _mapping(position.get("contract"))
    return {
        "account_id": _text(position.get("account_id") or position.get("account")),
        "symbol": _text(position.get("symbol") or contract.get("symbol")),
        "local_symbol": _text(position.get("local_symbol") or position.get("localSymbol") or contract.get("localSymbol")),
        "con_id": _int(position.get("con_id") or position.get("conId") or contract.get("conId")),
        "expiry": _text(position.get("expiry") or position.get("lastTradeDateOrContractMonth") or contract.get("lastTradeDateOrContractMonth")),
        "quantity": _decimal_text(_decimal(position.get("position") or position.get("quantity") or position.get("qty")) or Decimal("0")),
    }


def _identity_mismatch_blockers(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    for key in ("account_id", "local_symbol", "action"):
        if _text(expected.get(key)) and _text(actual.get(key)) and _text(actual.get(key)) != _text(expected.get(key)):
            blockers.append(f"{key}_mismatch")
    if expected.get("con_id") and actual.get("con_id") and int(actual["con_id"]) != int(expected["con_id"]):
        blockers.append("con_id_mismatch")
    expected_qty = _decimal(expected.get("quantity"))
    actual_qty = _decimal(actual.get("quantity"))
    if expected_qty is not None and actual_qty is not None and expected_qty != actual_qty:
        blockers.append("quantity_mismatch")
    return blockers


def _same_contract(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    if expected.get("con_id") and actual.get("con_id"):
        return int(actual["con_id"]) == int(expected["con_id"])
    expected_local = _text(expected.get("local_symbol"))
    actual_local = _text(actual.get("local_symbol"))
    if expected_local and actual_local:
        return expected_local == actual_local
    return False


def _attempt_identity_blockers(attempt: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    for key in ("account_id", "local_symbol", "action", "quantity", "order_id"):
        if not _text(attempt.get(key)):
            blockers.append(f"{key}_missing")
    if not attempt.get("con_id") and not _text(attempt.get("local_symbol")):
        blockers.append("contract_identity_missing")
    if _decimal(attempt.get("quantity")) is None or _decimal(attempt.get("quantity")) == 0:
        blockers.append("quantity_invalid")
    if _text(attempt.get("action")) not in {"BUY", "SELL"}:
        blockers.append("action_invalid")
    return blockers


def _unknown_open_orders(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    explicit = _list(snapshot.get("unknown_open_orders") or snapshot.get("unknown_orders"))
    if explicit:
        return explicit
    if int(snapshot.get("unknown_open_order_count") or snapshot.get("unknown_order_count") or 0) > 0:
        return [{"reason": "unknown_open_order_count"}]
    return []


def _open_order_rows(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    return _list(snapshot.get("open_orders") or snapshot.get("orders") or snapshot.get("broker_open_orders"))


def _terminal_status_resolution(attempt: Mapping[str, Any]) -> dict[str, Any] | None:
    status = _text(attempt.get("broker_order_status")).upper().replace("_", "")
    if status == "FILLED":
        return {
            "classification": TERMINAL_FILL_CONFIRMED,
            "resolution_source": "TERMINAL_ORDER_STATUS",
            "confidence": "TERMINAL_STATUS_CONFIRMED",
            "terminal_status_observed": "Filled",
            "exec_id": attempt.get("exec_id"),
        }
    if status in {"CANCELLED", "APICANCELLED"}:
        return {
            "classification": TERMINAL_CANCEL_CONFIRMED,
            "resolution_source": "TERMINAL_ORDER_STATUS",
            "confidence": "TERMINAL_STATUS_CONFIRMED",
            "terminal_status_observed": "Cancelled",
        }
    return None


def _write_order_diagnostic(
    *,
    config: ManagedCloseTerminalPublisherConfig,
    attempt: Mapping[str, Any],
    resolution: Mapping[str, Any],
    generated_at: datetime,
) -> None:
    order_id = _text(attempt.get("order_id")) or "unknown_order"
    perm_id = _text(attempt.get("perm_id")) or "unknown_perm"
    path = config.resolve(config.order_diagnostic_root) / f"{_safe_name(order_id)}_{_safe_name(perm_id)}.json"
    payload = {
        "schema_version": "track_b_managed_close_terminal_order_diagnostic_v1",
        "generated_at": generated_at.isoformat(),
        "read_only_evidence_publisher": True,
        "broker_mutation_allowed": False,
        "attempt": dict(attempt),
        "resolution": dict(resolution),
    }
    write_json_atomic(path, to_jsonable(payload))


def _diagnostic(attempt: Mapping[str, Any], classification: str, resolution: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "classification": classification,
        "order_id": attempt.get("order_id"),
        "perm_id": attempt.get("perm_id"),
        "symbol": attempt.get("symbol"),
        "local_symbol": attempt.get("local_symbol"),
        "lifecycle_id": attempt.get("lifecycle_id"),
        "diagnostic_only": True,
        "does_not_block_trading": True,
        "resolution": dict(resolution),
    }


def _event_summary(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: event.get(key)
        for key in (
            "event_id",
            "classification",
            "symbol",
            "local_symbol",
            "close_order_id",
            "perm_id",
            "lifecycle_id",
            "trade_id",
            "pre_close_signed_qty",
            "post_close_signed_qty",
        )
    }


def _event_notes(classification: str, resolution: Mapping[str, Any]) -> list[str]:
    if classification in {BROKER_EFFECT_OBSERVED_FLAT, BROKER_EFFECT_OBSERVED_REDUCED}:
        return ["No direct terminal order event is required; fresh complete broker truth proves close effect."]
    if classification == TERMINAL_FILL_CONFIRMED:
        return ["Terminal fill status was observed in local evidence."]
    if classification == TERMINAL_CANCEL_CONFIRMED:
        return ["Terminal cancel status was observed in local evidence."]
    return [str(resolution.get("classification") or classification)]


def _event_id(*, attempt: Mapping[str, Any], classification: str) -> str:
    parts = [
        classification,
        _text(attempt.get("account_id")),
        _text(attempt.get("local_symbol")),
        str(attempt.get("con_id") or ""),
        _text(attempt.get("order_id")),
        _text(attempt.get("perm_id")),
        _text(attempt.get("action")),
        _text(attempt.get("quantity")),
        _text(attempt.get("lifecycle_id")),
    ]
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"managed_close_terminal_{digest}"


def _existing_event_ids(path: Path) -> set[str]:
    return {str(row.get("event_id")) for row in _read_jsonl(path) if row.get("event_id")}


def _attempt_key(attempt: Mapping[str, Any]) -> tuple[str, str, str]:
    return (_text(attempt.get("order_id")), _text(attempt.get("local_symbol")), _text(attempt.get("action")))


def _pre_close_from_candidate(candidate: Mapping[str, Any], *, action: str) -> str:
    qty = _decimal(candidate.get("broker_quantity") or candidate.get("quantity")) or Decimal("0")
    signed = _pre_close_from_action(action, qty)
    return _decimal_text(signed)


def _pre_close_from_action(action: str, qty: Decimal) -> Decimal:
    normalized = _text(action)
    if normalized == "SELL":
        return abs(qty)
    if normalized == "BUY":
        return -abs(qty)
    return Decimal("0")


def _pre_close_signed_qty(attempt: Mapping[str, Any]) -> Decimal | None:
    existing = _decimal(attempt.get("pre_close_signed_qty"))
    if existing is not None and existing != 0:
        return existing
    qty = _decimal(attempt.get("quantity"))
    if qty is None:
        return None
    return _pre_close_from_action(_text(attempt.get("action")), qty)


def _same_direction(left: Decimal, right: Decimal) -> bool:
    return (left > 0 and right > 0) or (left < 0 and right < 0)


def _age_seconds(value: Any, now: datetime) -> float | None:
    parsed = _parse_time(value)
    if parsed is None:
        return None
    return round(max((now - parsed).total_seconds(), 0.0), 3)


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _parse_time(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping):
            rows.append(dict(payload))
    return rows


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _decimal(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_text(value: Decimal) -> str:
    if value == value.to_integral_value():
        return str(value.quantize(Decimal("1")))
    return format(value.normalize(), "f")


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value)[:120] or "unknown"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--broker-positions-snapshot", type=Path, default=DEFAULT_BROKER_POSITIONS_SNAPSHOT)
    parser.add_argument("--broker-open-orders-snapshot", type=Path, default=DEFAULT_BROKER_OPEN_ORDERS_SNAPSHOT)
    parser.add_argument("--managed-exit-actuator", type=Path, default=DEFAULT_MANAGED_EXIT_ACTUATOR)
    parser.add_argument("--trade-registry-events", type=Path, default=DEFAULT_TRADE_REGISTRY_EVENTS)
    parser.add_argument("--terminal-events", type=Path, default=DEFAULT_TERMINAL_EVENTS_JSONL)
    parser.add_argument("--summary", type=Path, default=DEFAULT_LATEST_SUMMARY)
    parser.add_argument("--max-snapshot-age-seconds", type=float, default=180.0)
    parser.add_argument("--no-order-diagnostics", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = ManagedCloseTerminalPublisherConfig(
        repo_root=args.repo_root,
        broker_positions_snapshot_path=args.broker_positions_snapshot,
        broker_open_orders_snapshot_path=args.broker_open_orders_snapshot,
        managed_exit_actuator_path=args.managed_exit_actuator,
        trade_registry_events_path=args.trade_registry_events,
        terminal_events_path=args.terminal_events,
        summary_path=args.summary,
        max_snapshot_age_seconds=args.max_snapshot_age_seconds,
        write_order_diagnostics=not args.no_order_diagnostics,
    )
    summary = publish_managed_close_terminal_events(config=config)
    print(
        "classification=MANAGED_CLOSE_TERMINAL_PUBLISHER_COMPLETE "
        f"terminal_event_count={summary['terminal_event_count']} diagnostic_count={summary['diagnostic_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
