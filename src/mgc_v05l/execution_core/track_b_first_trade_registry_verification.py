"""Read-only first-trade registry verification report for Track B PAPER.

This module answers one narrow operator question after a PAPER trade appears:
did the trade get born into the central trade registry with the required
handoff events and broker-backed evidence? It reads artifacts only; it does not
submit, cancel, close, flatten, or mutate broker/runtime state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .track_b_canonical_truth_snapshot import (
    TrackBTruthSnapshotConfig,
    build_track_b_truth_snapshot,
)
from .track_b_central_trade_registry import TradeCurrentState, TradeEventType, TradeRegistryRecord
from .track_b_live_trade_registry import load_live_trade_registry_records


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_VERSION = "track_b_first_trade_registry_verification_v1"
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "trade_registry"
    / "latest_first_trade_registry_verification.json"
)

_ACTIVE_STATES = {
    TradeCurrentState.PENDING_ENTRY,
    TradeCurrentState.WORKING_ENTRY,
    TradeCurrentState.OPEN_MANAGED,
    TradeCurrentState.EXIT_DUE,
    TradeCurrentState.WORKING_EXIT,
}


@dataclass(frozen=True)
class FirstTradeRegistryVerificationConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_OUTPUT_PATH
    truth_config: TrackBTruthSnapshotConfig | None = None


def build_first_trade_registry_verification_report(
    *,
    config: FirstTradeRegistryVerificationConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    records = load_live_trade_registry_records(repo_root=config.repo_root)
    selected = _select_latest_active_or_latest_record(records)
    truth = build_track_b_truth_snapshot(
        config=config.truth_config or TrackBTruthSnapshotConfig(repo_root=config.repo_root),
        now=generated_at,
    )
    reconciliation = truth.reconciliation

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "read_only": True,
        "broker_mutation_allowed": False,
        "runtime_restart_allowed": False,
        "production_gate_wiring_allowed": False,
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "registry_event_count": sum(len(record.event_chain) for record in records),
        "registry_trade_count": len(records),
        "latest_active_trade_present": selected is not None and selected.current_state in _ACTIVE_STATES,
        "canonical_truth_classification": truth.classification,
        "reconciliation_status": reconciliation.classification,
        "reconciliation_reconciled": reconciliation.reconciled,
        "truth_reason_codes": list(truth.reason_codes),
        "truth_conflicts": [conflict.classification for conflict in truth.conflicts],
        "source_paths": dict(truth.source_paths),
    }
    if selected is None:
        report.update(
            {
                "classification": "FIRST_TRADE_REGISTRY_NO_TRADES",
                "latest_or_active_trade_id": None,
                "expected_next_lifecycle_action": "WAIT_FOR_FIRST_ENTRY_INTENT",
                "blockers": ["NO_REGISTRY_TRADES"],
                "reason_codes": ["NO_REGISTRY_TRADES"],
            }
        )
        return report

    events = list(selected.event_chain)
    event_types = {event.event_type for event in events}
    entry_fill = _latest_event(events, TradeEventType.ENTRY_FILL_BROKER_BACKED)
    row = _record_identity(selected)
    missing_required = _missing_required_events(selected, event_types)
    blockers = [*missing_required]
    if selected.current_state == TradeCurrentState.REVIEW_REQUIRED:
        blockers.append("REGISTRY_TRADE_REVIEW_REQUIRED")
    classification = "FIRST_TRADE_REGISTRY_VERIFIED"
    if selected.current_state == TradeCurrentState.CANCELLED:
        classification = "FIRST_TRADE_REGISTRY_ENTRY_CANCELLED"
    elif selected.current_state == TradeCurrentState.CLOSED_FLAT:
        classification = "FIRST_TRADE_REGISTRY_CLOSED_FLAT"
    elif blockers:
        classification = "FIRST_TRADE_REGISTRY_REVIEW_REQUIRED"

    report.update(
        {
            "classification": classification,
            "latest_or_active_trade_id": selected.trade_id,
            "trade_id": selected.trade_id,
            "lane_id": row.get("lane_id"),
            "strategy_id": row.get("strategy_id"),
            "symbol": row.get("symbol"),
            "con_id": row.get("con_id"),
            "local_symbol": row.get("local_symbol"),
            "lifecycle_id": row.get("lifecycle_id"),
            "entry_intent_event_present": TradeEventType.ENTRY_INTENT_CREATED in event_types,
            "entry_order_event_present": TradeEventType.ENTRY_ORDER_SUBMITTED in event_types,
            "entry_fill_broker_backed_event_present": entry_fill is not None,
            "entry_perm_id": entry_fill.perm_id if entry_fill else None,
            "entry_exec_id": entry_fill.exec_id if entry_fill else None,
            "lifecycle_open_managed_event_present": TradeEventType.LIFECYCLE_OPEN_MANAGED in event_types,
            "current_registry_state": selected.current_state.value,
            "broker_backed_entry": selected.broker_backed_entry,
            "broker_backed_exit": selected.broker_backed_exit,
            "open_qty": str(selected.open_qty),
            "expected_next_lifecycle_action": _expected_next_action(selected.current_state),
            "blockers": list(dict.fromkeys(blockers)),
            "reason_codes": list(dict.fromkeys([*selected.latest_reason_codes, *blockers])),
            "event_chain_count": len(events),
            "event_chain_summary_limit": 25,
            "event_chain_summary": [
                {
                    "event_type": event.event_type.value,
                    "generated_at": event.generated_at.isoformat(),
                    "order_id": event.order_id,
                    "client_id": event.client_id,
                    "perm_id": event.perm_id,
                    "exec_id": event.exec_id,
                    "reason_codes": list(event.reason_codes),
                }
                for event in events[-25:]
            ],
        }
    )
    return report


def write_first_trade_registry_verification_report(
    *,
    config: FirstTradeRegistryVerificationConfig,
    report: Mapping[str, Any],
) -> Path:
    path = _resolve(config.repo_root, config.output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _select_latest_active_or_latest_record(records: tuple[TradeRegistryRecord, ...]) -> TradeRegistryRecord | None:
    if not records:
        return None
    active = [record for record in records if record.current_state in _ACTIVE_STATES]
    return max(active or list(records), key=_record_last_event_time)


def _record_last_event_time(record: TradeRegistryRecord) -> datetime:
    return max((event.generated_at for event in record.event_chain), default=datetime.min.replace(tzinfo=timezone.utc))


def _record_identity(record: TradeRegistryRecord) -> dict[str, Any]:
    owner = record.ownership_identity
    latest = record.event_chain[-1]
    return {
        "lane_id": owner.lane_id if owner else latest.lane_id,
        "strategy_id": owner.thesis_strategy_id if owner else latest.thesis_strategy_id,
        "symbol": owner.symbol if owner else latest.symbol,
        "con_id": owner.con_id if owner else latest.con_id,
        "local_symbol": owner.local_symbol if owner else latest.local_symbol,
        "lifecycle_id": owner.lifecycle_id if owner else latest.lifecycle_id,
    }


def _latest_event(events: list[Any], event_type: TradeEventType) -> Any | None:
    matches = [event for event in events if event.event_type == event_type]
    return matches[-1] if matches else None


def _missing_required_events(record: TradeRegistryRecord, event_types: set[TradeEventType]) -> list[str]:
    missing: list[str] = []
    if TradeEventType.ENTRY_INTENT_CREATED not in event_types:
        missing.append("ENTRY_INTENT_EVENT_MISSING")
    if record.current_state not in {TradeCurrentState.PENDING_ENTRY, TradeCurrentState.CANCELLED}:
        if TradeEventType.ENTRY_ORDER_SUBMITTED not in event_types:
            missing.append("ENTRY_ORDER_EVENT_MISSING")
    if record.current_state in {
        TradeCurrentState.OPEN_MANAGED,
        TradeCurrentState.EXIT_DUE,
        TradeCurrentState.WORKING_EXIT,
        TradeCurrentState.CLOSED_FLAT,
    }:
        if not record.broker_backed_entry:
            missing.append("BROKER_BACKED_ENTRY_EVIDENCE_MISSING")
        if TradeEventType.LIFECYCLE_OPEN_MANAGED not in event_types:
            missing.append("LIFECYCLE_OPEN_MANAGED_EVENT_MISSING")
    return missing


def _expected_next_action(state: TradeCurrentState) -> str:
    if state == TradeCurrentState.PENDING_ENTRY:
        return "AWAIT_ENTRY_ORDER_SUBMITTED"
    if state == TradeCurrentState.WORKING_ENTRY:
        return "AWAIT_BROKER_BACKED_ENTRY_FILL"
    if state == TradeCurrentState.OPEN_MANAGED:
        return "AWAIT_MANAGED_HOLD_OR_EXIT_POLICY"
    if state == TradeCurrentState.EXIT_DUE:
        return "AWAIT_MANAGED_EXIT_ORDER_SUBMITTED"
    if state == TradeCurrentState.WORKING_EXIT:
        return "AWAIT_BROKER_BACKED_CLOSE_FILL"
    if state == TradeCurrentState.CANCELLED:
        return "NO_ACTION_ENTRY_CANCELLED"
    if state == TradeCurrentState.CLOSED_FLAT:
        return "NO_ACTION_CLOSED_FLAT"
    if state == TradeCurrentState.REVIEW_REQUIRED:
        return "OPERATOR_REVIEW_REQUIRED"
    return "UNKNOWN_REVIEW_REQUIRED"


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the read-only Track B first-trade registry verification report.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--no-write", action="store_true")
    args = parser.parse_args(argv)
    config = FirstTradeRegistryVerificationConfig(repo_root=args.repo_root, output_path=args.output)
    report = build_first_trade_registry_verification_report(config=config)
    if not args.no_write:
        write_first_trade_registry_verification_report(config=config, report=report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
