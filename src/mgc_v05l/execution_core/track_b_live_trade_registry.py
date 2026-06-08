"""Append-only live Track B PAPER trade registry event writer.

This module is artifact plumbing only. It validates and appends central
``TradeEvent`` rows for the guarded PAPER lifecycle path; it does not query or
mutate broker state and it is not submit authority.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from .track_b_central_trade_registry import (
    TradeCurrentState,
    TradeEvent,
    TradeEventType,
    TradeRegistryRecord,
    reduce_trade_events,
)


DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL = (
    Path("outputs") / "track_b_execution_core" / "trade_registry" / "live_trade_events.jsonl"
)
DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_LATEST_EVENT_JSON = (
    Path("outputs") / "track_b_execution_core" / "trade_registry" / "latest_live_trade_event.json"
)


def append_live_trade_registry_event(
    *,
    repo_root: Path,
    event: TradeEvent,
    jsonl_path: Path = DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL,
    latest_path: Path = DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_LATEST_EVENT_JSON,
) -> dict[str, Any]:
    """Append one validated central trade event to the live PAPER registry."""

    event_payload = event.to_dict()
    if not _event_source_allowed(repo_root=repo_root, event=event):
        return {
            "persisted": False,
            "classification": "LIVE_REGISTRY_EVENT_REJECTED_OUT_OF_REPO_SOURCE",
            "event_type": event.event_type.value,
            "event_id": event.event_id,
            "trade_id": event.trade_id,
            "lifecycle_id": event.lifecycle_id,
            "source_artifact_path": event.source_artifact_path,
        }
    resolved_jsonl = _resolve(repo_root, jsonl_path)
    resolved_latest = _resolve(repo_root, latest_path)
    resolved_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with resolved_jsonl.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event_payload, sort_keys=True))
        handle.write("\n")
    latest_payload = {
        "schema_version": "track_b_live_trade_registry_latest_event_v1",
        "generated_at": _now().isoformat(),
        "append_only": True,
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "broker_mutation_allowed": False,
        "event": event_payload,
        "jsonl_path": str(resolved_jsonl),
    }
    resolved_latest.parent.mkdir(parents=True, exist_ok=True)
    resolved_latest.write_text(json.dumps(latest_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "persisted": True,
        "event_type": event.event_type.value,
        "event_id": event.event_id,
        "trade_id": event.trade_id,
        "lifecycle_id": event.lifecycle_id,
        "jsonl_path": str(resolved_jsonl),
        "latest_path": str(resolved_latest),
    }


def load_live_trade_registry_record(
    *,
    repo_root: Path,
    trade_id: str,
    jsonl_path: Path = DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL,
) -> TradeRegistryRecord | None:
    """Load and reduce one live registry trade chain by ``trade_id``.

    This is read-only reconstruction from the append-only registry event log.
    Malformed records are ignored here and should be classified by callers as
    review-required rather than inferred into ownership authority.
    """

    requested_trade_id = str(trade_id or "").strip()
    if not requested_trade_id:
        return None
    events: list[TradeEvent] = []
    path = _resolve(repo_root, jsonl_path)
    try:
        rows = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in rows:
        if not line.strip():
            continue
        try:
            event = TradeEvent.from_dict(json.loads(line))
        except Exception:
            continue
        if not _event_source_allowed(repo_root=repo_root, event=event):
            continue
        if event.trade_id == requested_trade_id:
            events.append(event)
    if not events:
        return None
    return reduce_trade_events(events)


def load_live_trade_registry_records(
    *,
    repo_root: Path,
    jsonl_path: Path = DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL,
) -> tuple[TradeRegistryRecord, ...]:
    """Load and reduce all valid live registry trade chains.

    The append-only registry may contain partial historical debris. Callers use
    the reduced records as authority only when they can map current broker or
    lifecycle truth to exactly one trade id.
    """

    path = _resolve(repo_root, jsonl_path)
    try:
        rows = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ()
    events_by_trade_id: dict[str, list[TradeEvent]] = {}
    for line in rows:
        if not line.strip():
            continue
        try:
            event = TradeEvent.from_dict(json.loads(line))
        except Exception:
            continue
        if not _event_source_allowed(repo_root=repo_root, event=event):
            continue
        events_by_trade_id.setdefault(event.trade_id, []).append(event)
    records: list[TradeRegistryRecord] = []
    for events in events_by_trade_id.values():
        try:
            records.append(reduce_trade_events(events))
        except Exception:
            continue
    return tuple(records)


def resolve_live_trade_id_for_lifecycle_id(
    *,
    repo_root: Path,
    lifecycle_id: str,
    jsonl_path: Path = DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL,
) -> str | None:
    """Return the unique live registry trade id for an exact lifecycle id.

    Lifecycle artifacts created before registry wiring can carry a placeholder
    trade id. Managed exits must prefer the append-only registry owner identity
    when exactly one current broker-backed open record maps to the lifecycle.
    """

    requested_lifecycle_id = str(lifecycle_id or "").strip()
    if not requested_lifecycle_id:
        return None
    matches: list[str] = []
    for record in load_live_trade_registry_records(repo_root=repo_root, jsonl_path=jsonl_path):
        owner = record.ownership_identity
        if owner is None or owner.lifecycle_id != requested_lifecycle_id:
            continue
        if record.current_state not in {TradeCurrentState.OPEN_MANAGED, TradeCurrentState.EXIT_DUE}:
            continue
        if record.broker_backed_entry is not True:
            continue
        matches.append(record.trade_id)
    unique_matches = sorted(set(matches))
    if len(unique_matches) == 1:
        return unique_matches[0]
    return None


def validate_registry_managed_exit_identity(
    *,
    repo_root: Path,
    trade_id: str | None,
    lifecycle_id: str | None,
    account_id: str | None,
    con_id: int | str | None,
    local_symbol: str | None,
    quantity: int | float | str | Decimal | None,
    action: str | None,
    phase1_reconciliation_gate: Mapping[str, Any],
    jsonl_path: Path = DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL,
) -> dict[str, Any]:
    """Validate a managed close against the central registry owner identity.

    The registry record is the ownership source of first resort. Phase-1
    broker/lifecycle reconciliation is then used to prove the registry owner
    still matches exact live broker-backed lifecycle truth.
    """

    blockers: list[str] = []
    requested_trade_id = str(trade_id or "").strip()
    requested_lifecycle_id = str(lifecycle_id or "").strip()
    if not requested_trade_id:
        blockers.append("missing_trade_id")
    if not requested_lifecycle_id:
        blockers.append("missing_lifecycle_id")
    registry_reconciliation_row = _exact_registry_reconciliation_row(
        phase1_reconciliation_gate=phase1_reconciliation_gate,
        trade_id=requested_trade_id,
        lifecycle_id=requested_lifecycle_id,
    )
    if not bool(phase1_reconciliation_gate.get("ready")) and not registry_reconciliation_row:
        blockers.append("broker_lifecycle_reconciliation_not_clean")
    if blockers:
        return _managed_exit_validation_result(blockers=blockers)

    record = load_live_trade_registry_record(repo_root=repo_root, trade_id=requested_trade_id, jsonl_path=jsonl_path)
    if record is None:
        return _managed_exit_validation_result(blockers=["trade_registry_record_missing"])
    if record.current_state not in {TradeCurrentState.OPEN_MANAGED, TradeCurrentState.EXIT_DUE}:
        current_scope_result = _current_scope_review_registry_managed_exit_result(
            record=record,
            requested_trade_id=requested_trade_id,
            requested_lifecycle_id=requested_lifecycle_id,
            account_id=account_id,
            con_id=con_id,
            local_symbol=local_symbol,
            quantity=quantity,
            action=action,
            phase1_reconciliation_gate=phase1_reconciliation_gate,
        )
        if current_scope_result.get("allowed") is True:
            return current_scope_result
        return _managed_exit_validation_result(
            blockers=["trade_registry_state_not_open_managed"],
            record=record,
        )
    if record.broker_backed_entry is not True:
        return _managed_exit_validation_result(blockers=["trade_registry_entry_not_broker_backed"], record=record)
    owner = record.ownership_identity
    if owner is None:
        return _managed_exit_validation_result(blockers=["trade_registry_owner_identity_missing"], record=record)
    if not owner.lifecycle_id:
        return _managed_exit_validation_result(blockers=["trade_registry_owner_lifecycle_id_missing"], record=record)
    if owner.lifecycle_id != requested_lifecycle_id:
        blockers.append("lifecycle_id_mismatch")

    lifecycle_row = _exact_lifecycle_row(phase1_reconciliation_gate, requested_lifecycle_id) or registry_reconciliation_row
    if not lifecycle_row:
        blockers.append("lifecycle_identity_row_missing")
    broker_row = _matching_broker_position(lifecycle_row, phase1_reconciliation_gate) if lifecycle_row else {}
    if not broker_row:
        blockers.append("broker_position_missing_for_managed_exit")

    requested_account = _valid_identity_text(account_id)
    owner_account = _valid_identity_text(owner.account_id)
    lifecycle_account = _valid_identity_text(lifecycle_row.get("account_id") if lifecycle_row else None)
    broker_account = _valid_identity_text(
        broker_row.get("account_id") or broker_row.get("account") if broker_row else None
    )
    exact_account = broker_account or lifecycle_account
    if requested_account and exact_account and requested_account != exact_account:
        blockers.append("account_id_mismatch")
    if owner_account and exact_account and owner_account != exact_account:
        blockers.append("registry_owner_account_id_mismatch")

    requested_con_id = str(con_id or "").strip()
    exact_con_id = str((lifecycle_row or {}).get("con_id") or (broker_row or {}).get("con_id") or (broker_row or {}).get("conId") or "").strip()
    if requested_con_id and exact_con_id and requested_con_id != exact_con_id:
        blockers.append("con_id_mismatch")
    if exact_con_id and str(owner.con_id) != exact_con_id:
        blockers.append("registry_owner_con_id_mismatch")

    requested_local = str(local_symbol or "").strip().upper()
    exact_local = str((lifecycle_row or {}).get("local_symbol") or (broker_row or {}).get("local_symbol") or (broker_row or {}).get("localSymbol") or "").strip().upper()
    if requested_local and exact_local and requested_local != exact_local:
        blockers.append("local_symbol_mismatch")
    if exact_local and owner.local_symbol.upper() != exact_local:
        blockers.append("registry_owner_local_symbol_mismatch")

    requested_qty = _decimal_or_none(quantity)
    exact_qty = _decimal_or_none((lifecycle_row or {}).get("quantity"))
    if requested_qty is not None and exact_qty is not None and requested_qty != exact_qty:
        blockers.append("quantity_mismatch")
    if exact_qty is not None and owner.qty != exact_qty:
        blockers.append("registry_owner_quantity_mismatch")

    expected_action = "SELL" if str(owner.side or "").upper() == "LONG" else "BUY"
    requested_action = str(action or "").strip().upper()
    if requested_action and requested_action != expected_action:
        blockers.append("close_action_mismatch")

    owner_payload = {
        "trade_id": record.trade_id,
        "current_state": record.current_state.value,
        "broker_backed_entry": record.broker_backed_entry,
        "lifecycle_id": owner.lifecycle_id,
        "lane_id": owner.lane_id,
        "strategy_id": owner.thesis_strategy_id,
        "account_id": exact_account or owner.account_id,
        "con_id": owner.con_id,
        "local_symbol": owner.local_symbol,
        "symbol": owner.symbol,
        "expiry": owner.expiry,
        "side": owner.side,
        "quantity": str(owner.qty),
        "entry_perm_id": _latest_event_value(record, "perm_id"),
        "entry_exec_id": _latest_event_value(record, "exec_id"),
    }
    return _managed_exit_validation_result(
        blockers=blockers,
        record=record,
        owner_identity=owner_payload,
        lifecycle_row=lifecycle_row,
        broker_position=broker_row,
    )


def _current_scope_review_registry_managed_exit_result(
    *,
    record: TradeRegistryRecord,
    requested_trade_id: str,
    requested_lifecycle_id: str,
    account_id: str | None,
    con_id: int | str | None,
    local_symbol: str | None,
    quantity: Any,
    action: str | None,
    phase1_reconciliation_gate: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if record.current_state != TradeCurrentState.REVIEW_REQUIRED:
        blockers.append("trade_registry_state_conflicts_current_scope")
    if phase1_reconciliation_gate.get("ready") is not True and phase1_reconciliation_gate.get("broker_reconciled") is not True:
        blockers.append("broker_lifecycle_reconciliation_not_clean")
    if _truthy(phase1_reconciliation_gate.get("live_money_eligible")):
        blockers.append("live_money_not_allowed")
    if _truthy(phase1_reconciliation_gate.get("paper_proof_invoked")):
        blockers.append("paper_proof_not_allowed")
    if _nonzero_count(
        phase1_reconciliation_gate.get("track_b_broker_open_order_count")
        or phase1_reconciliation_gate.get("broker_open_order_count")
    ) or list(phase1_reconciliation_gate.get("track_b_broker_open_orders") or []):
        blockers.append("broker_open_orders_not_zero")
    if _nonzero_count(
        phase1_reconciliation_gate.get("unknown_open_order_count")
        or phase1_reconciliation_gate.get("unknown_broker_open_order_count")
    ):
        blockers.append("unknown_open_orders_not_zero")
    registry_reconciliation = phase1_reconciliation_gate.get("registry_reconciliation")
    if isinstance(registry_reconciliation, Mapping):
        if registry_reconciliation.get("classification") != "REGISTRY_RECONCILIATION_MATCHED":
            blockers.append("registry_reconciliation_not_matched")
        if registry_reconciliation.get("blocking") is True:
            blockers.append("registry_reconciliation_blocking")
        review_trade_ids = {
            str(value or "").strip()
            for value in list(registry_reconciliation.get("review_required_trade_ids") or [])
            if str(value or "").strip()
        }
        if requested_trade_id in review_trade_ids:
            blockers.append("registry_current_scope_review_required")

    lifecycle_row = _exact_lifecycle_row(phase1_reconciliation_gate, requested_lifecycle_id)
    if not lifecycle_row:
        blockers.append("lifecycle_identity_row_missing")
    elif str(lifecycle_row.get("trade_id") or requested_trade_id).strip() != requested_trade_id:
        blockers.append("lifecycle_trade_id_mismatch")
    broker_row = _matching_broker_position(lifecycle_row, phase1_reconciliation_gate) if lifecycle_row else {}
    if not broker_row:
        blockers.append("broker_position_missing_for_managed_exit")

    requested_account = _valid_identity_text(account_id)
    lifecycle_account = _valid_identity_text(lifecycle_row.get("account_id") if lifecycle_row else None)
    broker_account = _valid_identity_text(
        broker_row.get("account_id") or broker_row.get("account") if broker_row else None
    )
    exact_account = broker_account or lifecycle_account
    if requested_account and exact_account and requested_account != exact_account:
        blockers.append("account_id_mismatch")

    requested_con_id = str(con_id or "").strip()
    exact_con_id = str((lifecycle_row or {}).get("con_id") or (broker_row or {}).get("con_id") or (broker_row or {}).get("conId") or "").strip()
    if requested_con_id and exact_con_id and requested_con_id != exact_con_id:
        blockers.append("con_id_mismatch")

    requested_local = str(local_symbol or "").strip().upper()
    exact_local = str((lifecycle_row or {}).get("local_symbol") or (broker_row or {}).get("local_symbol") or (broker_row or {}).get("localSymbol") or "").strip().upper()
    if requested_local and exact_local and requested_local != exact_local:
        blockers.append("local_symbol_mismatch")

    requested_qty = _decimal_or_none(quantity)
    exact_qty = _decimal_or_none((lifecycle_row or {}).get("quantity") or (broker_row or {}).get("quantity"))
    if requested_qty is not None and exact_qty is not None and requested_qty != exact_qty:
        blockers.append("quantity_mismatch")

    lifecycle_side = str((lifecycle_row or {}).get("side") or "").strip().upper()
    expected_action = "SELL" if lifecycle_side == "LONG" else "BUY" if lifecycle_side == "SHORT" else ""
    requested_action = str(action or "").strip().upper()
    if requested_action and expected_action and requested_action != expected_action:
        blockers.append("close_action_mismatch")
    if not expected_action:
        blockers.append("lifecycle_side_missing")
    entry_perm_id = (lifecycle_row or {}).get("entry_perm_id") or next(iter((lifecycle_row or {}).get("entry_perm_ids") or []), None)
    entry_exec_id = (lifecycle_row or {}).get("entry_exec_id") or next(iter((lifecycle_row or {}).get("entry_exec_ids") or []), None)
    if not entry_perm_id or not entry_exec_id:
        blockers.append("current_scope_entry_fill_identity_missing")

    owner_payload = {
        "trade_id": requested_trade_id,
        "current_state": "OPEN_MANAGED",
        "broker_backed_entry": True,
        "lifecycle_id": requested_lifecycle_id,
        "lane_id": (lifecycle_row or {}).get("lane_id"),
        "strategy_id": (lifecycle_row or {}).get("strategy_id"),
        "account_id": exact_account,
        "con_id": int(exact_con_id) if str(exact_con_id or "").isdigit() else exact_con_id,
        "local_symbol": exact_local,
        "symbol": (lifecycle_row or {}).get("track_b_root") or (lifecycle_row or {}).get("instrument_family") or (broker_row or {}).get("symbol"),
        "expiry": (lifecycle_row or {}).get("expiry") or (broker_row or {}).get("expiry"),
        "side": lifecycle_side,
        "quantity": str(exact_qty if exact_qty is not None else ""),
        "entry_perm_id": entry_perm_id,
        "entry_exec_id": entry_exec_id,
        "source": "CURRENT_SCOPE_BROKER_LIFECYCLE_RECONCILIATION",
        "superseded_registry_state": record.current_state.value,
    }
    return _managed_exit_validation_result(
        blockers=blockers,
        record=record,
        owner_identity=owner_payload,
        lifecycle_row=lifecycle_row,
        broker_position=broker_row,
    )


def repair_registry_identity_for_broker_backed_managed_position(
    *,
    repo_root: Path,
    trade_id: str,
    lifecycle_id: str,
    phase1_reconciliation_gate: Mapping[str, Any],
    lifecycle_report: Mapping[str, Any],
    jsonl_path: Path = DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_EVENTS_JSONL,
    latest_path: Path = DEFAULT_TRACK_B_LIVE_TRADE_REGISTRY_LATEST_EVENT_JSON,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Append a scoped identity repair for an exact broker-backed lifecycle.

    This is local artifact repair only. It never talks to the broker and never
    creates close/submit authority by itself; callers must still pass canonical
    managed-exit gates after the registry reduces to current truth.
    """

    blockers: list[str] = []
    requested_trade_id = str(trade_id or "").strip()
    requested_lifecycle_id = str(lifecycle_id or "").strip()
    if not requested_trade_id:
        blockers.append("missing_trade_id")
    if not requested_lifecycle_id:
        blockers.append("missing_lifecycle_id")
    if _truthy(phase1_reconciliation_gate.get("live_money_eligible")) or _truthy(
        lifecycle_report.get("live_money_eligible") or lifecycle_report.get("live_money_readiness")
    ):
        blockers.append("live_money_not_allowed")
    if _truthy(phase1_reconciliation_gate.get("paper_proof_invoked")) or _truthy(
        lifecycle_report.get("paper_proof_invoked") or lifecycle_report.get("paper_proof_cli_called")
    ):
        blockers.append("paper_proof_not_allowed")
    if _nonzero_count(
        phase1_reconciliation_gate.get("track_b_broker_open_order_count")
        or phase1_reconciliation_gate.get("broker_open_order_count")
    ) or list(phase1_reconciliation_gate.get("track_b_broker_open_orders") or []):
        blockers.append("broker_open_orders_not_zero")
    if phase1_reconciliation_gate.get("broker_reconciled") is not True and (
        phase1_reconciliation_gate.get("classification") != "TRACK_B_PAPER_BROKER_RECONCILED"
    ):
        blockers.append("broker_lifecycle_reconciliation_not_clean")
    if _nonzero_count(phase1_reconciliation_gate.get("current_scope_review_required_count")):
        blockers.append("current_scope_review_not_clean")

    registry_reconciliation = phase1_reconciliation_gate.get("registry_reconciliation")
    if isinstance(registry_reconciliation, Mapping):
        if registry_reconciliation.get("classification") != "REGISTRY_RECONCILIATION_MATCHED":
            blockers.append("registry_reconciliation_not_matched")
        if registry_reconciliation.get("blocking") is True:
            blockers.append("registry_reconciliation_blocking")

    record = (
        None
        if not requested_trade_id
        else load_live_trade_registry_record(repo_root=repo_root, trade_id=requested_trade_id, jsonl_path=jsonl_path)
    )
    if record is None:
        blockers.append("trade_registry_record_missing")
    else:
        if record.current_state != TradeCurrentState.REVIEW_REQUIRED:
            blockers.append("trade_registry_state_not_review_required")
        if record.ownership_identity is None:
            blockers.append("trade_registry_owner_identity_missing")
        elif record.ownership_identity.lifecycle_id:
            blockers.append("trade_registry_owner_lifecycle_id_already_present")

    lifecycle_row = _exact_lifecycle_row(phase1_reconciliation_gate, requested_lifecycle_id)
    if not lifecycle_row:
        blockers.append("lifecycle_identity_row_missing")
    if lifecycle_row and str(lifecycle_row.get("trade_id") or requested_trade_id).strip() != requested_trade_id:
        blockers.append("lifecycle_trade_id_mismatch")
    broker_position = _matching_broker_position(lifecycle_row, phase1_reconciliation_gate) if lifecycle_row else {}
    if not broker_position:
        blockers.append("broker_position_missing_for_repair")

    evidence = _entry_fill_identity_from_lifecycle_evidence(lifecycle_report=lifecycle_report, lifecycle_row=lifecycle_row)
    missing_fill_fields = [
        field
        for field in ("order_id", "client_id", "perm_id", "exec_id", "price")
        if not str(evidence.get(field) or "").strip()
    ]
    if missing_fill_fields:
        blockers.extend(f"entry_fill_{field}_missing" for field in missing_fill_fields)
    policy_id = _valid_identity_text(
        lifecycle_report.get("managed_exit_policy_id") or lifecycle_row.get("managed_exit_policy_id")
    )
    if not policy_id:
        blockers.append("managed_exit_policy_missing")
    if _lifecycle_has_terminal_or_mutating_close_evidence(lifecycle_report):
        blockers.append("lifecycle_already_has_close_evidence")

    target_identity = _repair_target_identity(
        trade_id=requested_trade_id,
        lifecycle_id=requested_lifecycle_id,
        lifecycle_report=lifecycle_report,
        lifecycle_row=lifecycle_row,
        broker_position=broker_position,
        entry_fill=evidence,
        managed_exit_policy_id=policy_id,
    )
    blockers.extend(_repair_identity_mismatches(target_identity))
    blockers.extend(
        _conflicting_registry_owner_blockers(
            repo_root=repo_root,
            jsonl_path=jsonl_path,
            trade_id=requested_trade_id,
            target_identity=target_identity,
            phase1_reconciliation_gate=phase1_reconciliation_gate,
        )
    )

    if blockers:
        return _registry_identity_repair_result(
            blockers=blockers,
            target_identity=target_identity,
            record=record,
            persisted_events=(),
        )

    now = _ensure_utc(generated_at or _now())
    source_path = str(
        lifecycle_report.get("report_json_path")
        or lifecycle_report.get("paper_lifecycle_report_path")
        or lifecycle_row.get("paper_lifecycle_report_path")
        or lifecycle_report.get("source_artifact_path")
        or "outputs/track_b_execution_core/trade_registry/scoped_registry_identity_repair.json"
    )
    common = {
        "trade_id": requested_trade_id,
        "lifecycle_id": requested_lifecycle_id,
        "lane_id": target_identity["lane_id"],
        "thesis_strategy_id": target_identity["strategy_id"],
        "account_id": target_identity["account_id"],
        "symbol": target_identity["symbol"],
        "con_id": target_identity["con_id"],
        "local_symbol": target_identity["local_symbol"],
        "expiry": target_identity["expiry"],
        "side": target_identity["side"],
        "action": "BUY" if target_identity["side"] == "LONG" else "SELL",
        "qty": target_identity["quantity"],
        "source_artifact_path": source_path,
        "metadata": {
            "repair_scope": "BROKER_BACKED_MANAGED_POSITION_REGISTRY_IDENTITY",
            "managed_exit_policy_id": policy_id,
            "broker_mutation_allowed": False,
            "broker_mutation_performed": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "preserves_prior_review_required_as_audit_history": True,
        },
    }
    events = (
        make_live_trade_registry_event(
            event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
            generated_at=now,
            order_id=evidence["order_id"],
            client_id=evidence["client_id"],
            perm_id=evidence["perm_id"],
            exec_id=evidence["exec_id"],
            price=evidence["price"],
            reason_codes=("SCOPED_REGISTRY_IDENTITY_REPAIR_BROKER_BACKED_ENTRY",),
            **common,
        ),
        make_live_trade_registry_event(
            event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
            generated_at=now + timedelta(microseconds=1),
            reason_codes=("SCOPED_REGISTRY_IDENTITY_REPAIR_LIFECYCLE_OPEN_MANAGED",),
            **common,
        ),
    )
    persisted = tuple(
        append_live_trade_registry_event(
            repo_root=repo_root,
            event=event,
            jsonl_path=jsonl_path,
            latest_path=latest_path,
        )
        for event in events
    )
    append_blockers = [
        str(item.get("classification") or "registry_repair_event_not_persisted")
        for item in persisted
        if item.get("persisted") is not True
    ]
    return _registry_identity_repair_result(
        blockers=append_blockers,
        target_identity=target_identity,
        record=load_live_trade_registry_record(repo_root=repo_root, trade_id=requested_trade_id, jsonl_path=jsonl_path),
        persisted_events=persisted,
    )


def make_live_trade_registry_event(
    *,
    event_type: TradeEventType,
    trade_id: str,
    lane_id: str,
    thesis_strategy_id: str,
    account_id: str,
    symbol: str,
    con_id: int | str | None,
    local_symbol: str,
    expiry: str,
    side: str,
    action: str,
    qty: int | float | str | Decimal | None,
    source_artifact_path: str,
    generated_at: datetime | None = None,
    lifecycle_id: str | None = None,
    order_id: str | int | None = None,
    client_id: str | int | None = None,
    perm_id: str | int | None = None,
    exec_id: str | None = None,
    price: int | float | str | Decimal | None = None,
    reason_codes: tuple[str, ...] = (),
    metadata: Mapping[str, Any] | None = None,
) -> TradeEvent:
    """Build a central trade event with strict identity validation."""

    return TradeEvent(
        event_id=f"live_{event_type.value.lower()}_{uuid.uuid4().hex}",
        event_type=event_type,
        generated_at=_ensure_utc(generated_at or _now()),
        trade_id=str(trade_id or "").strip(),
        lifecycle_id=_text_or_none(lifecycle_id),
        lane_id=str(lane_id or "").strip(),
        thesis_strategy_id=str(thesis_strategy_id or "").strip(),
        account_id=str(account_id or "").strip(),
        symbol=str(symbol or "").strip().upper(),
        con_id=int(con_id or 0),
        local_symbol=str(local_symbol or "").strip(),
        expiry=str(expiry or "").strip(),
        side=str(side or "").strip().upper(),
        action=str(action or "").strip().upper(),
        qty=_decimal(qty or 0),
        source_artifact_path=str(source_artifact_path or "").strip(),
        order_id=_text_or_none(order_id),
        client_id=_text_or_none(client_id),
        perm_id=_text_or_none(perm_id),
        exec_id=_text_or_none(exec_id),
        price=_decimal_or_none(price),
        reason_codes=tuple(str(item) for item in reason_codes if str(item or "").strip()),
        metadata=dict(metadata or {}),
    )


def trade_id_from_live_identity(
    *,
    explicit_trade_id: object = None,
    lifecycle_id: object = None,
    ownership_intent_id: object = None,
    order_intent_id: object = None,
    account_id: object = None,
    con_id: object = None,
    lane_id: object = None,
) -> str:
    """Return a stable live-path trade id from the best available identity."""

    for value in (explicit_trade_id,):
        text = str(value or "").strip()
        if text:
            return text
    lifecycle_text = str(lifecycle_id or "").strip()
    if lifecycle_text:
        return f"trade_{_slug(lifecycle_text)}"
    ownership_text = str(ownership_intent_id or "").strip()
    if ownership_text:
        return f"trade_{_slug(ownership_text)}"
    order_intent_text = str(order_intent_id or "").strip()
    if order_intent_text:
        return f"trade_{_slug(order_intent_text)}"
    identity = "|".join(
        str(value or "").strip()
        for value in (account_id, con_id, lane_id)
        if str(value or "").strip()
    )
    if identity:
        return f"trade_{_slug(identity)}"
    return ""


def broker_backed_fill_has_required_ids(*, perm_id: object = None, exec_id: object = None) -> bool:
    return bool(str(perm_id or "").strip() and str(exec_id or "").strip())


def _managed_exit_validation_result(
    *,
    blockers: list[str],
    record: TradeRegistryRecord | None = None,
    owner_identity: Mapping[str, Any] | None = None,
    lifecycle_row: Mapping[str, Any] | None = None,
    broker_position: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "classification": "REGISTRY_MANAGED_EXIT_ALLOWED" if not blockers else "REGISTRY_MANAGED_EXIT_BLOCKED",
        "allowed": not blockers,
        "block_reasons": list(dict.fromkeys(blockers)),
        "trade_id": None if record is None else record.trade_id,
        "registry_current_state": None if record is None else record.current_state.value,
        "broker_backed_entry": None if record is None else record.broker_backed_entry,
        "owner_identity": dict(owner_identity or {}),
        "lifecycle_row": dict(lifecycle_row or {}),
        "broker_position": dict(broker_position or {}),
    }


def _registry_identity_repair_result(
    *,
    blockers: list[str],
    target_identity: Mapping[str, Any],
    record: TradeRegistryRecord | None,
    persisted_events: tuple[Mapping[str, Any], ...],
) -> dict[str, Any]:
    return {
        "classification": "REGISTRY_IDENTITY_REPAIR_APPLIED" if not blockers else "REGISTRY_IDENTITY_REPAIR_BLOCKED",
        "repaired": not blockers,
        "block_reasons": list(dict.fromkeys(blockers)),
        "trade_id": target_identity.get("trade_id"),
        "lifecycle_id": target_identity.get("lifecycle_id"),
        "target_identity": dict(target_identity),
        "registry_current_state": None if record is None else record.current_state.value,
        "broker_backed_entry": None if record is None else record.broker_backed_entry,
        "broker_mutation_allowed": False,
        "broker_mutation_performed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "persisted_events": [dict(item) for item in persisted_events],
    }


def _exact_lifecycle_row(phase1_reconciliation_gate: Mapping[str, Any], lifecycle_id: str) -> dict[str, Any]:
    for row in list(phase1_reconciliation_gate.get("track_b_lifecycle_positions") or []):
        if isinstance(row, Mapping) and str(row.get("lifecycle_id") or "").strip() == lifecycle_id:
            return dict(row)
    return {}


def _exact_registry_reconciliation_row(
    *,
    phase1_reconciliation_gate: Mapping[str, Any],
    trade_id: str,
    lifecycle_id: str,
) -> dict[str, Any]:
    registry_reconciliation = phase1_reconciliation_gate.get("registry_reconciliation")
    if not isinstance(registry_reconciliation, Mapping):
        return {}
    if registry_reconciliation.get("classification") != "REGISTRY_RECONCILIATION_MATCHED":
        return {}
    if registry_reconciliation.get("blocking") is True:
        return {}
    for row in list(registry_reconciliation.get("mapped_records") or []):
        if not isinstance(row, Mapping):
            continue
        if str(row.get("trade_id") or "").strip() != trade_id:
            continue
        if str(row.get("lifecycle_id") or "").strip() != lifecycle_id:
            continue
        return {
            "lifecycle_id": row.get("lifecycle_id"),
            "account_id": row.get("account_id"),
            "lane_id": row.get("lane_id"),
            "strategy_id": row.get("strategy_id"),
            "track_b_root": row.get("instrument_family") or row.get("symbol"),
            "instrument_family": row.get("instrument_family") or row.get("symbol"),
            "local_symbol": row.get("local_symbol"),
            "con_id": row.get("con_id"),
            "quantity": row.get("quantity"),
            "side": row.get("side"),
            "entry_perm_id": row.get("entry_perm_id"),
            "entry_exec_id": row.get("entry_exec_id"),
            "source": "CENTRAL_TRADE_REGISTRY_RECONCILIATION",
        }
    return {}


def _entry_fill_identity_from_lifecycle_evidence(
    *,
    lifecycle_report: Mapping[str, Any],
    lifecycle_row: Mapping[str, Any],
) -> dict[str, str]:
    entry_fill = lifecycle_report.get("entry_fill")
    entry_fill = entry_fill if isinstance(entry_fill, Mapping) else {}
    entry_submit_attempt = lifecycle_report.get("entry_submit_attempt")
    entry_submit_attempt = entry_submit_attempt if isinstance(entry_submit_attempt, Mapping) else {}
    lifecycle_units = list(lifecycle_row.get("lifecycle_units") or lifecycle_report.get("lifecycle_units") or [])
    lifecycle_unit = lifecycle_units[0] if lifecycle_units and isinstance(lifecycle_units[0], Mapping) else {}
    return {
        "order_id": _first_text(
            entry_fill.get("order_id"),
            entry_fill.get("broker_order_id"),
            entry_fill.get("entry_order_id"),
            lifecycle_row.get("entry_order_id"),
            _first_item(lifecycle_row.get("entry_order_ids")),
            lifecycle_unit.get("entry_order_id"),
        ),
        "client_id": _first_text(
            entry_fill.get("client_id"),
            entry_fill.get("entry_client_id"),
            entry_submit_attempt.get("client_id"),
            lifecycle_row.get("entry_client_id"),
            lifecycle_unit.get("entry_client_id"),
        ),
        "perm_id": _first_text(
            entry_fill.get("perm_id"),
            entry_fill.get("entry_perm_id"),
            lifecycle_row.get("entry_perm_id"),
            _first_item(lifecycle_row.get("entry_perm_ids")),
            lifecycle_unit.get("entry_perm_id"),
        ),
        "exec_id": _first_text(
            entry_fill.get("exec_id"),
            entry_fill.get("execution_id"),
            entry_fill.get("entry_exec_id"),
            lifecycle_row.get("entry_exec_id"),
            _first_item(lifecycle_row.get("entry_exec_ids")),
            lifecycle_unit.get("entry_exec_id"),
        ),
        "price": _first_text(
            entry_fill.get("price"),
            entry_fill.get("fill_price"),
            entry_fill.get("entry_price"),
            lifecycle_row.get("avg_entry_price"),
            lifecycle_unit.get("entry_price"),
        ),
    }


def _repair_target_identity(
    *,
    trade_id: str,
    lifecycle_id: str,
    lifecycle_report: Mapping[str, Any],
    lifecycle_row: Mapping[str, Any],
    broker_position: Mapping[str, Any],
    entry_fill: Mapping[str, Any],
    managed_exit_policy_id: str,
) -> dict[str, Any]:
    return {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "account_id": _first_text(
            broker_position.get("account_id"),
            broker_position.get("account"),
            lifecycle_row.get("account_id"),
            lifecycle_report.get("account_id"),
        ),
        "lane_id": _first_text(lifecycle_row.get("lane_id"), lifecycle_report.get("lane_id")),
        "strategy_id": _first_text(
            lifecycle_row.get("strategy_id"),
            lifecycle_row.get("thesis_strategy_id"),
            lifecycle_report.get("strategy_id"),
            lifecycle_report.get("thesis_strategy_id"),
        ),
        "symbol": _first_text(
            broker_position.get("track_b_root"),
            broker_position.get("symbol"),
            lifecycle_row.get("instrument_family"),
            lifecycle_row.get("symbol"),
            lifecycle_report.get("instrument_family"),
            lifecycle_report.get("symbol"),
        ).upper(),
        "con_id": _first_text(broker_position.get("con_id"), broker_position.get("conId"), lifecycle_row.get("con_id")),
        "local_symbol": _first_text(
            broker_position.get("local_symbol"),
            broker_position.get("localSymbol"),
            lifecycle_row.get("local_symbol"),
            lifecycle_report.get("local_symbol"),
        ),
        "expiry": _first_text(
            broker_position.get("expiry"),
            lifecycle_row.get("expiry"),
            lifecycle_report.get("expiry"),
            lifecycle_report.get("contract_expiry"),
        ),
        "side": _first_text(lifecycle_row.get("side"), lifecycle_report.get("side")).upper(),
        "quantity": _first_text(lifecycle_row.get("quantity"), lifecycle_report.get("quantity"), entry_fill.get("quantity")),
        "managed_exit_policy_id": managed_exit_policy_id,
        "entry_order_id": _first_text(entry_fill.get("order_id")),
        "entry_client_id": _first_text(entry_fill.get("client_id")),
        "entry_perm_id": _first_text(entry_fill.get("perm_id")),
        "entry_exec_id": _first_text(entry_fill.get("exec_id")),
    }


def _repair_identity_mismatches(target_identity: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    required_fields = (
        "trade_id",
        "lifecycle_id",
        "account_id",
        "lane_id",
        "strategy_id",
        "symbol",
        "con_id",
        "local_symbol",
        "expiry",
        "side",
        "quantity",
    )
    for field in required_fields:
        if not _valid_identity_text(target_identity.get(field)):
            blockers.append(f"{field}_missing")
    if str(target_identity.get("side") or "").upper() not in {"LONG", "SHORT"}:
        blockers.append("side_invalid")
    if _decimal_or_none(target_identity.get("quantity")) is None:
        blockers.append("quantity_invalid")
    return blockers


def _conflicting_registry_owner_blockers(
    *,
    repo_root: Path,
    jsonl_path: Path,
    trade_id: str,
    target_identity: Mapping[str, Any],
    phase1_reconciliation_gate: Mapping[str, Any],
) -> list[str]:
    current_scope_trade_ids, current_scope_lifecycle_ids = _current_scope_identity_sets(phase1_reconciliation_gate)
    target_account = _valid_identity_text(target_identity.get("account_id"))
    target_con_id = _valid_identity_text(target_identity.get("con_id"))
    target_local = _valid_identity_text(target_identity.get("local_symbol")).upper()
    target_qty = _decimal_or_none(target_identity.get("quantity"))
    target_side = _valid_identity_text(target_identity.get("side")).upper()
    for record in load_live_trade_registry_records(repo_root=repo_root, jsonl_path=jsonl_path):
        if record.trade_id == trade_id:
            continue
        if record.current_state not in {TradeCurrentState.OPEN_MANAGED, TradeCurrentState.EXIT_DUE}:
            continue
        if record.broker_backed_entry is not True or record.ownership_identity is None:
            continue
        owner = record.ownership_identity
        if current_scope_trade_ids or current_scope_lifecycle_ids:
            owner_lifecycle = _valid_identity_text(owner.lifecycle_id)
            if record.trade_id not in current_scope_trade_ids and owner_lifecycle not in current_scope_lifecycle_ids:
                continue
        owner_account = _valid_identity_text(owner.account_id)
        same_account = not target_account or not owner_account or target_account == owner_account
        same_contract = (target_con_id and str(owner.con_id) == target_con_id) or (
            target_local and owner.local_symbol.upper() == target_local
        )
        same_qty = target_qty is not None and owner.qty == target_qty
        same_side = not target_side or owner.side.upper() == target_side
        if same_account and same_contract and same_qty and same_side:
            return ["conflicting_current_registry_owner"]
    return []


def _current_scope_identity_sets(phase1_reconciliation_gate: Mapping[str, Any]) -> tuple[set[str], set[str]]:
    trade_ids: set[str] = set()
    lifecycle_ids: set[str] = set()
    for row in list(phase1_reconciliation_gate.get("track_b_lifecycle_positions") or []):
        if not isinstance(row, Mapping):
            continue
        trade_id = _valid_identity_text(row.get("trade_id"))
        lifecycle_id = _valid_identity_text(row.get("lifecycle_id"))
        if trade_id:
            trade_ids.add(trade_id)
        if lifecycle_id:
            lifecycle_ids.add(lifecycle_id)
    registry_reconciliation = phase1_reconciliation_gate.get("registry_reconciliation")
    if isinstance(registry_reconciliation, Mapping):
        for row in list(registry_reconciliation.get("mapped_records") or []):
            if not isinstance(row, Mapping):
                continue
            trade_id = _valid_identity_text(row.get("trade_id"))
            lifecycle_id = _valid_identity_text(row.get("lifecycle_id"))
            if trade_id:
                trade_ids.add(trade_id)
            if lifecycle_id:
                lifecycle_ids.add(lifecycle_id)
        for trade_id in list(registry_reconciliation.get("mapped_trade_ids") or []):
            trade_id_text = _valid_identity_text(trade_id)
            if trade_id_text:
                trade_ids.add(trade_id_text)
    return trade_ids, lifecycle_ids


def _lifecycle_has_terminal_or_mutating_close_evidence(lifecycle_report: Mapping[str, Any]) -> bool:
    if lifecycle_report.get("close_fill"):
        return True
    if lifecycle_report.get("close_order"):
        return True
    if list(lifecycle_report.get("working_managed_exit_orders") or []):
        return True
    attempt = lifecycle_report.get("close_submit_attempt")
    if not isinstance(attempt, Mapping):
        return False
    if attempt.get("broker_state_mutated") is True:
        return True
    if attempt.get("submitted") is True:
        return True
    authorization = attempt.get("strategy_submit_authorization")
    if isinstance(authorization, Mapping) and authorization.get("broker_mutation_allowed") is True:
        return True
    return False


def _matching_broker_position(
    lifecycle_row: Mapping[str, Any],
    phase1_reconciliation_gate: Mapping[str, Any],
) -> dict[str, Any]:
    requested_con_id = str(lifecycle_row.get("con_id") or "").strip()
    requested_local = str(lifecycle_row.get("local_symbol") or "").strip().upper()
    requested_symbol = str(lifecycle_row.get("track_b_root") or lifecycle_row.get("instrument_family") or "").strip().upper()
    for row in list(phase1_reconciliation_gate.get("track_b_broker_positions") or []):
        if not isinstance(row, Mapping):
            continue
        broker_con_id = str(row.get("con_id") or row.get("conId") or "").strip()
        broker_local = str(row.get("local_symbol") or row.get("localSymbol") or "").strip().upper()
        broker_symbol = str(row.get("track_b_root") or row.get("symbol") or "").strip().upper()
        if requested_con_id and broker_con_id and requested_con_id == broker_con_id:
            return dict(row)
        if requested_local and broker_local and requested_local == broker_local:
            return dict(row)
        if requested_symbol and broker_symbol and requested_symbol == broker_symbol and not requested_local and not requested_con_id:
            return dict(row)
    return {}


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _nonzero_count(value: object) -> bool:
    if value in (None, ""):
        return False
    try:
        return Decimal(str(value)) != 0
    except (InvalidOperation, ValueError):
        return True


def _first_item(value: object) -> object:
    if isinstance(value, (list, tuple)) and value:
        return value[0]
    return None


def _first_text(*values: object) -> str:
    for value in values:
        text = _valid_identity_text(value)
        if text:
            return text
    return ""


def _valid_identity_text(value: object) -> str:
    text = str(value or "").strip()
    if text.upper() in {"", "MULTIPLE", "MISSING", "UNKNOWN", "NONE", "NULL"}:
        return ""
    return text


def _latest_event_value(record: TradeRegistryRecord, field: str) -> str | None:
    for event in reversed(record.event_chain):
        value = getattr(event, field, None)
        if value not in (None, ""):
            return str(value)
    return None


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else Path(repo_root) / path


def _event_source_allowed(*, repo_root: Path, event: TradeEvent) -> bool:
    source = str(event.source_artifact_path or "").strip()
    if not source:
        return True
    source_path = Path(source)
    if not source_path.is_absolute():
        return True
    try:
        source_path.resolve().relative_to(Path(repo_root).resolve())
        return True
    except (OSError, ValueError):
        return False


def _now() -> datetime:
    return datetime.now(UTC)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _decimal(value: object) -> Decimal:
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("trade registry quantity/price value is invalid") from exc


def _decimal_or_none(value: object) -> Decimal | None:
    if value in (None, ""):
        return None
    return _decimal(value)


def _text_or_none(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _slug(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", ".", "-"} else "_" for ch in value).strip("_")
