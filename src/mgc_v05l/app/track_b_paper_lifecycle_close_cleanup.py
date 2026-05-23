"""Supervised PAPER-only lifecycle close cleanup for proven bridge exits.

This command is intentionally offline. It consumes already-written broker
truth, bridge-fill, and compact-ledger artifacts to close a stale local
lifecycle row only when the broker is already flat and the exit fill identity is
exact. It never connects to IBKR and dry-run is the default.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_broker_truth_lease import DEFAULT_LEASE_ARTIFACT
from mgc_v05l.execution_core.track_b_managed_order_registry import (
    DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT,
    NO_MANAGED_ORDERS,
)
from mgc_v05l.execution_core.track_b_managed_position_registry import (
    DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT,
    NO_MANAGED_POSITIONS,
)
from mgc_v05l.execution_core.track_b_open_order_truth import DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT, NO_OPEN_ORDERS
from mgc_v05l.execution_core.track_b_position_truth_monitor import DEFAULT_POSITION_TRUTH_ARTIFACT
from mgc_v05l.execution_core.track_b_control_plane_snapshot import DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
from mgc_v05l.execution_core.track_b_lifecycle_local_repair_guard import (
    LIFECYCLE_LOCAL_REPAIR_VALID,
    TrackBLifecycleLocalRepairGuardConfig,
    validate_lifecycle_local_artifact_repair,
)
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_shared_truth_refresh_cli import DEFAULT_RECONCILIATION_ARTIFACT
from mgc_v05l.execution_core.track_b_paper_trade_ledger import (
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT,
    LEDGER_SCHEMA_VERSION,
    OPPOSITE_ENTRY_OFFSET_EXISTING_POSITION_RECLASSIFIED,
    build_track_b_paper_trade_summaries,
)


PAPER_ACCOUNT_ID = "DUM882026"
DEFAULT_LANE_ID = "mnq_1x_ny_early_core__us_late_long"
DEFAULT_STRATEGY_ID = "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long"
DEFAULT_SYMBOL = "MNQ"
DEFAULT_LOCAL_SYMBOL = "MNQM6"
DEFAULT_CON_ID = 770561201
DEFAULT_SIDE = "LONG"
DEFAULT_QUANTITY = Decimal("1")
DEFAULT_ENTRY_LIFECYCLE_ID = "bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN"
DEFAULT_ENTRY_FILL_TIME = "2026-05-12T19:05:26.191844Z"
DEFAULT_ENTRY_PRICE = Decimal("28981.25")
DEFAULT_EXIT_INTENT_ID = "MNQ|1m|2026-05-13T10:59:00Z|SELL_TO_CLOSE"
DEFAULT_EXIT_ACTION = "SELL"
DEFAULT_EXIT_PRICE = Decimal("29389.5")
DEFAULT_EXIT_FILL_TIME = "2026-05-13T11:00:38.198088Z"
DEFAULT_EXIT_CLIENT_ID = 10877
DEFAULT_EXIT_PERM_ID = 852752717
DEFAULT_OUTPUT_ROOT = Path("outputs") / "reports" / "track_b_paper_lifecycle_close_cleanup"
DEFAULT_LANE_ROOT = Path("outputs") / "probationary_pattern_engine" / "paper_session" / "lanes"
DEFAULT_BROKER_TRUTH_ROOT = Path("outputs") / "reports" / "ibkr_read_only_verification"
EVIDENCE_KIND_AUTO = "auto"
EVIDENCE_KIND_IBKR_POSITION_RECONCILED_FLAT = "ibkr-position-reconciled-flat"
POINT_VALUE_BY_SYMBOL = {
    "GC": Decimal("100"),
    "NQ": Decimal("20"),
    "ES": Decimal("50"),
    "MGC": Decimal("10"),
    "MNQ": Decimal("2"),
    "MES": Decimal("5"),
    "ZT": Decimal("2000"),
    "ZF": Decimal("1000"),
    "ZN": Decimal("1000"),
    "ZB": Decimal("1000"),
    "PL": Decimal("50"),
}
TICK_SIZE_BY_SYMBOL = {
    "GC": Decimal("0.1"),
    "NQ": Decimal("0.25"),
    "ES": Decimal("0.25"),
    "MGC": Decimal("0.1"),
    "MNQ": Decimal("0.25"),
    "MES": Decimal("0.25"),
    "ZT": Decimal("0.00390625"),
    "ZF": Decimal("0.0078125"),
    "ZN": Decimal("0.015625"),
    "ZB": Decimal("0.03125"),
    "PL": Decimal("0.1"),
}


@dataclass(frozen=True)
class LifecycleCloseCleanupConfig:
    repo_root: Path
    lane_id: str = DEFAULT_LANE_ID
    strategy_id: str = DEFAULT_STRATEGY_ID
    account_id: str = PAPER_ACCOUNT_ID
    symbol: str = DEFAULT_SYMBOL
    local_symbol: str = DEFAULT_LOCAL_SYMBOL
    con_id: int = DEFAULT_CON_ID
    side: str = DEFAULT_SIDE
    quantity: Decimal = DEFAULT_QUANTITY
    entry_lifecycle_id: str = DEFAULT_ENTRY_LIFECYCLE_ID
    entry_fill_time: str = DEFAULT_ENTRY_FILL_TIME
    entry_price: Decimal = DEFAULT_ENTRY_PRICE
    exit_intent_id: str = DEFAULT_EXIT_INTENT_ID
    exit_action: str = DEFAULT_EXIT_ACTION
    exit_price: Decimal = DEFAULT_EXIT_PRICE
    exit_fill_time: str = DEFAULT_EXIT_FILL_TIME
    exit_client_id: int = DEFAULT_EXIT_CLIENT_ID
    exit_perm_id: int = DEFAULT_EXIT_PERM_ID
    apply: bool = False
    refuse_on_ambiguous_mnq_rows: bool = True
    output_root: Path = DEFAULT_OUTPUT_ROOT
    lane_root: Path = DEFAULT_LANE_ROOT
    broker_truth_root: Path = DEFAULT_BROKER_TRUTH_ROOT
    ledger_root: Path = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT
    exit_bridge_report_path: Path | None = None
    evidence_kind: str = EVIDENCE_KIND_AUTO
    allow_ledger_entry_evidence: bool = False
    allow_offsetting_open_intent_as_close: bool = False
    offsetting_open_lifecycle_id: str | None = None
    require_shared_truth_evidence: bool = True
    shared_truth_max_age_seconds: float = 600.0
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    runtime_supervisor_authority_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    broker_lease_path: Path = DEFAULT_LEASE_ARTIFACT
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    require_control_plane_snapshot_for_apply: bool = True
    local_repair_snapshot_max_age_seconds: int = 300


@dataclass(frozen=True)
class LifecycleCloseCleanupResult:
    classification: str
    report: dict[str, Any]
    audit_path: Path
    latest_audit_path: Path


def run_track_b_paper_lifecycle_close_cleanup(
    *, config: LifecycleCloseCleanupConfig, now: datetime | None = None
) -> LifecycleCloseCleanupResult:
    actual_now = now or datetime.now(UTC)
    if actual_now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    mode = "APPLY" if config.apply else "DRY_RUN"
    repo_root = config.repo_root
    failures: list[str] = []

    ledger_jsonl = repo_root / config.ledger_root / "track_b_paper_trade_ledger.jsonl"
    lane_dir = repo_root / config.lane_root / config.lane_id
    bridge_results_path = lane_dir / "filled_bridge_results.jsonl"
    broker_positions_path = repo_root / config.broker_truth_root / "ibkr_positions_snapshot.json"
    broker_orders_path = repo_root / config.broker_truth_root / "ibkr_open_orders_snapshot.json"

    ledger_records = _read_jsonl(ledger_jsonl)
    bridge_rows = _read_jsonl(bridge_results_path)
    broker_positions = _read_json(broker_positions_path)
    broker_orders = _read_json(broker_orders_path)

    target = _select_target_open_row(config=config, rows=ledger_records, failures=failures)
    entry_evidence = _select_entry_bridge_evidence(config=config, rows=bridge_rows, target=target, failures=failures)
    exit_evidence = _select_exit_bridge_evidence(config=config, rows=bridge_rows, failures=failures)
    if exit_evidence is None and config.exit_bridge_report_path is not None:
        before_fallback_failures = list(failures)
        exit_evidence = _select_exit_bridge_report_evidence(config=config, failures=failures)
        if exit_evidence is not None:
            failures = [
                failure
                for failure in failures
                if failure not in before_fallback_failures
                or "matching SELL_TO_CLOSE bridge fill" not in failure
            ]
    broker_flat_evidence = _broker_flat_evidence(
        config=config,
        positions_snapshot=broker_positions,
        orders_snapshot=broker_orders,
        failures=failures,
    )
    shared_truth_evidence = _shared_truth_remediation_evidence(
        config=config,
        target=target,
        now=actual_now,
    )
    failures.extend(shared_truth_evidence["blockers"])
    ambiguous_rows = _ambiguous_open_mnq_rows(config=config, rows=ledger_records, target=target)
    if ambiguous_rows and config.refuse_on_ambiguous_mnq_rows:
        failures.append("Additional open MNQ lifecycle row has incomplete/non-matching identity evidence.")

    close_record = _build_close_record(config=config, target=target, exit_evidence=exit_evidence, now=actual_now)
    offsetting_reconciliation_record = _build_offsetting_entry_reconciliation_record(
        config=config,
        rows=ledger_records,
        exit_evidence=exit_evidence,
        now=actual_now,
    )
    already_applied = _already_has_matching_close(config=config, rows=ledger_records)
    offsetting_already_reconciled = _offsetting_entry_already_reconciled(config=config, rows=ledger_records)
    simulated_records = list(ledger_records)
    if close_record is not None and not already_applied:
        simulated_records.append(close_record)
    if offsetting_reconciliation_record is not None and not offsetting_already_reconciled:
        simulated_records.append(offsetting_reconciliation_record)
    summaries = _build_summaries(
        records=simulated_records,
        ledger_jsonl=ledger_jsonl,
        output_root=repo_root / config.ledger_root,
        now=actual_now,
    )
    reconciliation_prediction = _reconciliation_prediction(
        config=config,
        live_position_status=summaries["live_position_status"],
        positions_snapshot=broker_positions,
        orders_snapshot=broker_orders,
    )
    if close_record is not None and not reconciliation_prediction["would_clear"]:
        failures.append("Post-cleanup reconciliation prediction would not clear.")
    lifecycle_local_repair_guard = _lifecycle_local_repair_guard(
        config=config,
        close_record=close_record,
        broker_flat_evidence=broker_flat_evidence,
        now=actual_now,
    )
    if lifecycle_local_repair_guard["classification"] != LIFECYCLE_LOCAL_REPAIR_VALID:
        failures.append(f"Lifecycle State Matrix / Control Plane Snapshot guard blocked cleanup: {lifecycle_local_repair_guard['classification']}.")
    valid = not failures
    if valid and already_applied:
        classification = "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_ALREADY_APPLIED"
    elif valid and config.apply:
        classification = "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_APPLIED"
    elif valid:
        classification = "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_DRY_RUN_READY"
    else:
        classification = "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_REFUSED"

    report: dict[str, Any] = {
        "classification": classification,
        "mode": mode,
        "generated_at": actual_now.isoformat(),
        "paper_only": True,
        "live_money_eligible": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_order_attempted": False,
        "place_order_attempted": False,
        "paper_proof_invoked": False,
        "broker_mutated": False,
        "failures": failures,
        "expected_identity": _expected_identity(config),
        "ledger": {
            "path": str(ledger_jsonl),
            "target_open_row": target,
            "already_applied": already_applied,
            "ambiguous_open_mnq_rows": ambiguous_rows,
        },
        "bridge_evidence": {
            "path": str(bridge_results_path),
            "exit_bridge_report_path": str(config.exit_bridge_report_path) if config.exit_bridge_report_path else None,
            "entry": entry_evidence,
            "exit": exit_evidence,
        },
        "broker_flat_evidence": broker_flat_evidence,
        "shared_truth_evidence": shared_truth_evidence,
        "lifecycle_local_repair_guard": lifecycle_local_repair_guard,
        "control_plane_snapshot_id": lifecycle_local_repair_guard.get("control_plane_snapshot_id"),
        "shared_truth_refresh_generation_id": lifecycle_local_repair_guard.get("shared_truth_refresh_generation_id"),
        "write_plan": {
            "would_append_close_record": valid and not already_applied,
            "would_append_offsetting_entry_reconciliation": valid
            and offsetting_reconciliation_record is not None
            and not offsetting_already_reconciled,
            "would_update_compact_summaries": valid,
            "rows_that_would_change": [] if target is None else [_row_identity(target)],
            "ledger_path": str(ledger_jsonl),
            "summary_paths": {
                "trade_summary": str(repo_root / config.ledger_root / "latest_track_b_paper_trade_summary.json"),
                "live_position_status": str(repo_root / config.ledger_root / "latest_track_b_live_position_status.json"),
                "pnl_summary": str(repo_root / config.ledger_root / "latest_track_b_pnl_summary.json"),
            },
        },
        "close_record": close_record if valid else None,
        "post_cleanup_prediction": {
            "trade_summary": _summary_compact(summaries["trade_summary"]),
            "live_position_status": _position_compact(summaries["live_position_status"]),
            "reconciliation_would_clear": reconciliation_prediction["would_clear"],
            "reconciliation_prediction": reconciliation_prediction,
        },
    }
    audit_path = _write_audit(repo_root, config.output_root, report, actual_now=actual_now)
    report["audit_path"] = str(audit_path)
    latest_audit_path = _write_latest_audit(repo_root, config.output_root, report)

    if valid and config.apply and (
        (close_record is not None and not already_applied)
        or (offsetting_reconciliation_record is not None and not offsetting_already_reconciled)
    ):
        ledger_jsonl.parent.mkdir(parents=True, exist_ok=True)
        with ledger_jsonl.open("a", encoding="utf-8") as handle:
            if close_record is not None and not already_applied:
                handle.write(json.dumps(_jsonable(close_record), sort_keys=True) + "\n")
            if offsetting_reconciliation_record is not None and not offsetting_already_reconciled:
                handle.write(json.dumps(_jsonable(offsetting_reconciliation_record), sort_keys=True) + "\n")
        _write_summaries(summaries=summaries, output_root=repo_root / config.ledger_root)
        post_report = dict(report)
        post_report["classification"] = "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_APPLIED"
        post_report["post_apply"] = {
            "close_record_written": close_record is not None and not already_applied,
            "offsetting_entry_reconciliation_written": offsetting_reconciliation_record is not None
            and not offsetting_already_reconciled,
            "compact_summaries_updated": True,
            "reconciliation_would_clear": reconciliation_prediction["would_clear"],
        }
        post_report["audit_path"] = str(audit_path)
        audit_path = _write_audit(repo_root, config.output_root, post_report, actual_now=actual_now, suffix="post")
        latest_audit_path = _write_latest_audit(repo_root, config.output_root, post_report)
        report = post_report
    elif valid and config.apply and already_applied:
        _write_summaries(summaries=summaries, output_root=repo_root / config.ledger_root)

    return LifecycleCloseCleanupResult(
        classification=classification,
        report=report,
        audit_path=audit_path,
        latest_audit_path=latest_audit_path,
    )


def _select_target_open_row(
    *,
    config: LifecycleCloseCleanupConfig,
    rows: Sequence[Mapping[str, Any]],
    failures: list[str],
) -> dict[str, Any] | None:
    matches = [
        dict(row)
        for row in rows
        if str(row.get("lifecycle_id") or "") == config.entry_lifecycle_id
        and str(row.get("final_position_status") or "") == "OPEN_MANAGED"
    ]
    if not matches:
        failures.append("Target OPEN_MANAGED entry lifecycle id is missing from compact ledger.")
        return None
    latest = matches[-1]
    checks = {
        "strategy_id": str(latest.get("strategy_id") or "") == config.strategy_id,
        "symbol": str(latest.get("instrument_family") or "").upper() == config.symbol,
        "local_symbol": str(latest.get("local_symbol") or "").upper() == config.local_symbol,
        "con_id": _int(latest.get("con_id")) == config.con_id,
        "side": str(latest.get("side") or "").upper() == config.side,
        "quantity": _decimal(latest.get("quantity")) == config.quantity,
        "entry_fill_time": _same_time(latest.get("entry_timestamp"), config.entry_fill_time),
        "entry_price": _decimal(latest.get("entry_fill_price")) == config.entry_price,
        "open_managed": str(latest.get("final_position_status") or "") == "OPEN_MANAGED",
        "no_exit_fill": latest.get("exit_fill_price") in {None, ""},
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        failures.append(f"Target compact ledger row identity mismatch: {', '.join(failed)}.")
    return latest


def _select_entry_bridge_evidence(
    *,
    config: LifecycleCloseCleanupConfig,
    rows: Sequence[Mapping[str, Any]],
    target: Mapping[str, Any] | None,
    failures: list[str],
) -> dict[str, Any] | None:
    matches = [
        dict(row)
        for row in rows
        if str(row.get("order_intent_id") or "") == config.entry_lifecycle_id.removeprefix("bridge_fill_")
        and str(row.get("intent_type") or "").upper() == "BUY_TO_OPEN"
        and str(row.get("action") or "").upper() == "BUY"
        and str(row.get("symbol") or row.get("instrument") or "").upper() == config.symbol
        and str(row.get("local_symbol") or _nested(row, "contract", "local_symbol") or "").upper() == config.local_symbol
        and _int(row.get("con_id") or _nested(row, "contract", "qualified_contract_identifier")) == config.con_id
        and _decimal(row.get("quantity")) == config.quantity
        and _decimal(row.get("fill_price")) == config.entry_price
        and _same_time(row.get("fill_timestamp"), config.entry_fill_time)
    ]
    if len(matches) != 1:
        if config.allow_ledger_entry_evidence and target is not None:
            ledger_entry_checks = {
                "strategy_id": str(target.get("strategy_id") or "") == config.strategy_id,
                "symbol": str(target.get("instrument_family") or "").upper() == config.symbol,
                "local_symbol": str(target.get("local_symbol") or "").upper() == config.local_symbol,
                "con_id": _int(target.get("con_id")) == config.con_id,
                "quantity": _decimal(target.get("quantity")) == config.quantity,
                "entry_price": _decimal(target.get("entry_fill_price")) == config.entry_price,
                "entry_fill_time": _same_time(target.get("entry_timestamp"), config.entry_fill_time),
                "entry_broker_identity": isinstance(target.get("entry_broker_identity"), Mapping),
            }
            if all(ledger_entry_checks.values()):
                return {
                    "classification": "LEDGER_ENTRY_EVIDENCE_ACCEPTED",
                    "source": "TRACK_B_COMPACT_LEDGER_ENTRY_BROKER_IDENTITY",
                    "order_intent_id": config.entry_lifecycle_id.removeprefix("bridge_fill_"),
                    "symbol": config.symbol,
                    "local_symbol": config.local_symbol,
                    "con_id": config.con_id,
                    "quantity": _decimal_text(config.quantity),
                    "fill_price": _decimal_text(config.entry_price),
                    "fill_timestamp": _canonical_time(config.entry_fill_time),
                    "entry_broker_identity": target.get("entry_broker_identity"),
                }
        failures.append(f"Expected exactly one matching BUY_TO_OPEN bridge fill, found {len(matches)}.")
        return matches[0] if matches else None
    return matches[0]


def _select_exit_bridge_evidence(
    *,
    config: LifecycleCloseCleanupConfig,
    rows: Sequence[Mapping[str, Any]],
    failures: list[str],
) -> dict[str, Any] | None:
    matches = [
        dict(row)
        for row in rows
        if str(row.get("order_intent_id") or "") == config.exit_intent_id
        and str(row.get("intent_type") or "").upper() == "SELL_TO_CLOSE"
        and _exit_action_matches(row.get("action"), config.exit_action)
        and str(row.get("symbol") or row.get("instrument") or "").upper() == config.symbol
        and str(row.get("local_symbol") or _nested(row, "contract", "local_symbol") or "").upper() == config.local_symbol
        and _int(row.get("con_id") or _nested(row, "contract", "qualified_contract_identifier")) == config.con_id
        and _decimal(row.get("quantity")) == config.quantity
        and _decimal(row.get("fill_price")) == config.exit_price
        and _same_time(row.get("fill_timestamp"), config.exit_fill_time)
        and _int(row.get("client_id")) == config.exit_client_id
        and _int(row.get("perm_id")) == config.exit_perm_id
        and str(row.get("classification") or "") == "PAPER_STRATEGY_ORDER_FILLED_PERSISTED"
        and str(row.get("bridge_classification") or "") == "PAPER_STRATEGY_ORDER_FILLED"
    ]
    if len(matches) != 1 and config.allow_offsetting_open_intent_as_close:
        offsetting_matches = [
            dict(row)
            for row in rows
            if str(row.get("order_intent_id") or "") == config.exit_intent_id
            and _offsetting_open_intent_matches_close(row, config=config)
            and str(row.get("symbol") or row.get("instrument") or "").upper() == config.symbol
            and str(row.get("local_symbol") or _nested(row, "contract", "local_symbol") or "").upper()
            == config.local_symbol
            and _int(row.get("con_id") or _nested(row, "contract", "qualified_contract_identifier")) == config.con_id
            and _decimal(row.get("quantity")) == config.quantity
            and _decimal(row.get("fill_price")) == config.exit_price
            and _same_time(row.get("fill_timestamp"), config.exit_fill_time)
            and _int(row.get("client_id")) == config.exit_client_id
            and _int(row.get("perm_id")) == config.exit_perm_id
            and str(row.get("classification") or "") == "PAPER_STRATEGY_ORDER_FILLED_PERSISTED"
            and str(row.get("bridge_classification") or "") == "PAPER_STRATEGY_ORDER_FILLED"
        ]
        if len(offsetting_matches) == 1:
            return {
                **offsetting_matches[0],
                "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED_OFFSETTING_OPEN_RECLASSIFIED_AS_CLOSE",
                "original_intent_type": offsetting_matches[0].get("intent_type"),
                "intent_type": "SELL_TO_CLOSE" if config.side == "LONG" else "BUY_TO_CLOSE",
                "source": "OPPOSITE_OPEN_INTENT_OFFSET_EXISTING_MANAGED_POSITION",
                "reclassified_offsetting_open_intent": True,
            }
    if len(matches) != 1:
        failures.append(f"Expected exactly one matching SELL_TO_CLOSE bridge fill, found {len(matches)}.")
        return matches[0] if matches else None
    return matches[0]


def _select_exit_bridge_report_evidence(
    *,
    config: LifecycleCloseCleanupConfig,
    failures: list[str],
) -> dict[str, Any] | None:
    report_path = config.repo_root / config.exit_bridge_report_path if config.exit_bridge_report_path and not config.exit_bridge_report_path.is_absolute() else config.exit_bridge_report_path
    if report_path is None or not report_path.exists():
        failures.append("Exit bridge report path is missing.")
        return None
    report = _read_json(report_path)
    if config.evidence_kind == EVIDENCE_KIND_IBKR_POSITION_RECONCILED_FLAT:
        return _ibkr_position_reconciled_flat_evidence_from_report(
            config=config,
            report=report,
            report_path=report_path,
            failures=failures,
        )
    if config.evidence_kind not in {EVIDENCE_KIND_AUTO, ""}:
        failures.append(f"Unsupported close cleanup evidence kind: {config.evidence_kind}.")
        return None
    direct_evidence = _direct_filled_bridge_exit_evidence_from_report(
        config=config,
        report=report,
        report_path=report_path,
        failures=failures,
    )
    if direct_evidence is not None:
        return direct_evidence
    unattended_evidence = _unattended_close_evidence_from_report(
        config=config,
        report=report,
        report_path=report_path,
        failures=failures,
    )
    if unattended_evidence is not None:
        return unattended_evidence
    delegated = report.get("delegated_result") if isinstance(report.get("delegated_result"), Mapping) else {}
    delegated_report = delegated.get("report") if isinstance(delegated.get("report"), Mapping) else {}
    lifecycle = (
        delegated_report.get("submit_cancel_lifecycle")
        if isinstance(delegated_report.get("submit_cancel_lifecycle"), Mapping)
        else {}
    )
    latest_status = lifecycle.get("latest_order_status") if isinstance(lifecycle.get("latest_order_status"), Mapping) else {}
    fill_verification = lifecycle.get("fill_verification") if isinstance(lifecycle.get("fill_verification"), Mapping) else {}
    close_verification = (
        lifecycle.get("close_position_verification")
        if isinstance(lifecycle.get("close_position_verification"), Mapping)
        else {}
    )
    executions = fill_verification.get("executions_after_submit")
    execution_matches = [
        dict(row)
        for row in (executions if isinstance(executions, list) else [])
        if isinstance(row, Mapping)
        and str(row.get("symbol") or "").upper() == config.symbol
        and str(row.get("account_id") or "") == config.account_id
        and _decimal(row.get("quantity")) == config.quantity
        and _decimal(row.get("price")) == config.exit_price
    ]
    unique_by_exec_id: dict[str, dict[str, Any]] = {}
    for row in execution_matches:
        key = str(row.get("execution_id") or row.get("exec_id") or len(unique_by_exec_id))
        unique_by_exec_id.setdefault(key, row)
    execution_matches = list(unique_by_exec_id.values())
    if str(report.get("classification") or "") != "PAPER_STRATEGY_ORDER_FILLED":
        failures.append("Exit bridge report classification is not PAPER_STRATEGY_ORDER_FILLED.")
    if str(delegated.get("classification") or "") != "PAPER_CLOSE_FILLED_FLAT":
        failures.append("Delegated close report classification is not PAPER_CLOSE_FILLED_FLAT.")
    if str(latest_status.get("status") or "").upper() != "FILLED":
        failures.append("Exit bridge report latest order status is not Filled.")
    if _int(latest_status.get("client_id")) != config.exit_client_id:
        failures.append("Exit bridge report client id mismatch.")
    if _int(latest_status.get("perm_id")) != config.exit_perm_id:
        failures.append("Exit bridge report perm id mismatch.")
    if _decimal(latest_status.get("filled")) != config.quantity:
        failures.append("Exit bridge report filled quantity mismatch.")
    if _decimal(latest_status.get("avg_fill_price")) != config.exit_price:
        failures.append("Exit bridge report fill price mismatch.")
    if len(execution_matches) != 1:
        failures.append(f"Expected exactly one matching SELL_TO_CLOSE execution in bridge report, found {len(execution_matches)}.")
        return execution_matches[0] if execution_matches else None
    if not bool(fill_verification.get("verified")):
        failures.append("Exit bridge report fill verification is not marked verified.")
    if not bool(close_verification.get("verified")):
        failures.append("Exit bridge report close-position verification is not marked verified.")
    execution = execution_matches[0]
    if not _same_time(execution.get("executed_at"), config.exit_fill_time):
        failures.append("Exit bridge report execution fill time mismatch.")
    return {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED_FROM_BRIDGE_REPORT",
        "bridge_classification": report.get("classification"),
        "source": "IBKR_PAPER_STRATEGY_BRIDGE_REPORT",
        "source_path": str(report_path),
        "order_intent_id": config.exit_intent_id,
        "intent_type": "SELL_TO_CLOSE",
        "action": config.exit_action,
        "symbol": config.symbol,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "quantity": _decimal_text(config.quantity),
        "broker_order_id": str(latest_status.get("order_id") or lifecycle.get("submitted_order_id") or ""),
        "client_id": config.exit_client_id,
        "perm_id": config.exit_perm_id,
        "exec_id": execution.get("execution_id") or execution.get("exec_id"),
        "fill_price": _decimal_text(config.exit_price),
        "fill_timestamp": _canonical_time(config.exit_fill_time),
    }


def _ibkr_position_reconciled_flat_evidence_from_report(
    *,
    config: LifecycleCloseCleanupConfig,
    report: Mapping[str, Any],
    report_path: Path,
    failures: list[str],
) -> dict[str, Any] | None:
    initial_failure_count = len(failures)
    if str(report.get("classification") or "") != "IBKR_POSITION_RECONCILED_FLAT":
        failures.append("IBKR manual-close evidence classification is not IBKR_POSITION_RECONCILED_FLAT.")
    if report.get("read_only") is not True:
        failures.append("IBKR manual-close evidence is not marked read-only.")
    if str(report.get("account_id") or "") != config.account_id:
        failures.append("IBKR manual-close evidence account mismatch.")

    contract = _nested(report, "contract_report", "exact_contract")
    contract = contract if isinstance(contract, Mapping) else {}
    contract_symbol = contract.get("symbol") or contract.get("broker_symbol") or contract.get("internal_symbol")
    contract_checks = {
        "symbol": str(contract_symbol or "").upper() == config.symbol,
        "local_symbol": str(contract.get("local_symbol") or contract.get("localSymbol") or "").upper()
        == config.local_symbol,
        "con_id": _int(contract.get("con_id") or contract.get("conId")) == config.con_id,
    }
    failed_contract_checks = [name for name, passed in contract_checks.items() if not passed]
    if failed_contract_checks:
        failures.append(
            "IBKR manual-close evidence contract mismatch: " + ", ".join(failed_contract_checks) + "."
        )

    flat_quantity = _decimal(_nested(report, "diagnosis", "latest_exact_position_quantity"))
    if flat_quantity != Decimal("0"):
        failures.append("IBKR manual-close evidence current position is not flat.")

    open_orders, open_order_count = _ibkr_position_report_open_orders(report)
    if open_order_count != 0:
        failures.append("IBKR manual-close evidence open orders are not zero.")

    execution_rows = _nested(report, "execution_truth", "matching_execution_rows")
    execution_matches = [
        dict(row)
        for row in (execution_rows if isinstance(execution_rows, list) else [])
        if isinstance(row, Mapping)
        and _ibkr_manual_close_execution_matches(row, config=config)
    ]
    if len(execution_matches) != 1:
        failures.append(
            f"Expected exactly one matching IBKR manual closing execution, found {len(execution_matches)}."
        )
        return execution_matches[0] if execution_matches else None

    execution = execution_matches[0]
    execution_id = str(execution.get("execution_id") or execution.get("exec_id") or "")
    if not execution_id:
        failures.append("IBKR manual-close execution id is missing.")
    if _int(execution.get("perm_id")) != config.exit_perm_id:
        failures.append("IBKR manual-close execution perm id mismatch.")
    if _int(execution.get("client_id")) != config.exit_client_id:
        failures.append("IBKR manual-close execution client id mismatch.")
    if not _same_time(execution.get("executed_at"), config.exit_fill_time):
        failures.append("IBKR manual-close execution fill time mismatch.")
    if not _time_after(execution.get("executed_at"), config.entry_fill_time):
        failures.append("IBKR manual-close execution is not after lifecycle entry time.")

    completed_rows = _nested(report, "execution_truth", "matching_completed_order_rows")
    completed_matches = [
        dict(row)
        for row in (completed_rows if isinstance(completed_rows, list) else [])
        if isinstance(row, Mapping)
        and str(row.get("account_id") or "") == config.account_id
        and _int(row.get("perm_id")) == config.exit_perm_id
        and _int(row.get("client_id")) == config.exit_client_id
        and _int(row.get("con_id") or _nested(row, "contract", "con_id")) == config.con_id
        and str(row.get("symbol") or "").upper() == config.symbol
        and str(row.get("local_symbol") or "").upper() == config.local_symbol
    ]
    if len(completed_matches) != 1:
        failures.append(
            f"Expected exactly one matching IBKR completed order row, found {len(completed_matches)}."
        )
        completed = {}
    else:
        completed = completed_matches[0]
        if str(completed.get("status") or "").upper() != "FILLED":
            failures.append("IBKR manual-close completed order status is not Filled.")

    if len(failures) > initial_failure_count:
        return None

    return {
        "classification": "IBKR_POSITION_RECONCILED_FLAT_MANUAL_CLOSE_EVIDENCE",
        "ibkr_position_reconciliation_classification": report.get("classification"),
        "source": "IBKR_POSITION_RECONCILED_FLAT_READ_ONLY_EXECUTION_REPORT",
        "source_path": str(report_path),
        "operator_manual_paper_close": True,
        "close_reason": "Operator manual PAPER close with IBKR read-only execution evidence.",
        "close_reconciliation_source": "OPERATOR_MANUAL_PAPER_CLOSE_WITH_IBKR_READ_ONLY_EXECUTION_EVIDENCE",
        "order_intent_id": config.exit_intent_id,
        "intent_type": "SELL_TO_CLOSE" if config.side == "LONG" else "BUY_TO_CLOSE",
        "action": config.exit_action,
        "symbol": config.symbol,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "quantity": _decimal_text(config.quantity),
        "broker_order_id": str(execution.get("broker_order_id") or ""),
        "client_id": config.exit_client_id,
        "perm_id": config.exit_perm_id,
        "exec_id": execution_id,
        "execution_id": execution_id,
        "fill_price": _decimal_text(config.exit_price),
        "fill_timestamp": _canonical_time(config.exit_fill_time),
        "evidence_fields": {
            "account_id": report.get("account_id"),
            "classification": report.get("classification"),
            "read_only": report.get("read_only"),
            "generated_at": report.get("generated_at"),
            "contract": dict(contract),
            "current_position_quantity": _decimal_text(flat_quantity),
            "open_order_count": open_order_count,
            "open_orders": open_orders,
            "execution": execution,
            "completed_order": completed,
            "diagnosis": report.get("diagnosis"),
        },
    }


def _ibkr_manual_close_execution_matches(row: Mapping[str, Any], *, config: LifecycleCloseCleanupConfig) -> bool:
    closing_sides = {"SLD", "SELL", "SELL_TO_CLOSE"} if config.side == "LONG" else {"BOT", "BUY", "BUY_TO_CLOSE"}
    return (
        str(row.get("account_id") or "") == config.account_id
        and str(row.get("symbol") or "").upper() == config.symbol
        and str(row.get("local_symbol") or row.get("localSymbol") or "").upper() == config.local_symbol
        and _int(row.get("con_id") or row.get("conId")) == config.con_id
        and str(row.get("side") or row.get("action") or "").upper() in closing_sides
        and _decimal(row.get("quantity")) == config.quantity
        and _decimal(row.get("price")) == config.exit_price
        and _int(row.get("perm_id")) == config.exit_perm_id
    )


def _ibkr_position_report_open_orders(report: Mapping[str, Any]) -> tuple[list[Any], int | None]:
    provider_open_orders = _nested(report, "provider_snapshot", "open_orders")
    provider_open_order_ids = _nested(report, "provider_snapshot", "open_order_ids")
    orders_open_rows = _nested(report, "provider_snapshot", "orders", "open_rows")
    open_orders = []
    if isinstance(provider_open_orders, list):
        open_orders.extend(provider_open_orders)
    if isinstance(provider_open_order_ids, list):
        open_orders.extend(provider_open_order_ids)
    if isinstance(orders_open_rows, list):
        open_orders.extend(orders_open_rows)
    if provider_open_orders is None and provider_open_order_ids is None and orders_open_rows is None:
        return [], None
    return open_orders, len(open_orders)


def _unattended_close_evidence_from_report(
    *,
    config: LifecycleCloseCleanupConfig,
    report: Mapping[str, Any],
    report_path: Path,
    failures: list[str],
) -> dict[str, Any] | None:
    if str(report.get("classification") or "") != "IBKR_UNATTENDED_CLOSE_FILLED_FLAT":
        return None
    lifecycle = report.get("lifecycle") if isinstance(report.get("lifecycle"), Mapping) else {}
    latest_status = lifecycle.get("latest_order_status") if isinstance(lifecycle.get("latest_order_status"), Mapping) else {}
    close_verification = (
        lifecycle.get("close_position_verification")
        if isinstance(lifecycle.get("close_position_verification"), Mapping)
        else {}
    )
    executions = lifecycle.get("executions_after_submit")
    execution_matches = [
        dict(row)
        for row in (executions if isinstance(executions, list) else [])
        if isinstance(row, Mapping)
        and str(row.get("symbol") or "").upper() == config.symbol
        and str(row.get("account_id") or "") == config.account_id
        and _int(row.get("con_id")) == config.con_id
        and str(row.get("local_symbol") or "").upper() == config.local_symbol
        and _decimal(row.get("quantity")) == config.quantity
        and _decimal(row.get("price")) == config.exit_price
    ]
    unique_by_exec_id: dict[str, dict[str, Any]] = {}
    for row in execution_matches:
        key = str(row.get("execution_id") or row.get("exec_id") or len(unique_by_exec_id))
        unique_by_exec_id.setdefault(key, row)
    execution_matches = list(unique_by_exec_id.values())
    if str(latest_status.get("status") or "").upper() != "FILLED":
        failures.append("Unattended close report latest order status is not Filled.")
    if _int(latest_status.get("client_id")) != config.exit_client_id:
        failures.append("Unattended close report client id mismatch.")
    if _int(latest_status.get("perm_id")) != config.exit_perm_id:
        failures.append("Unattended close report perm id mismatch.")
    if _decimal(latest_status.get("filled")) != config.quantity:
        failures.append("Unattended close report filled quantity mismatch.")
    if _decimal(latest_status.get("avg_fill_price")) != config.exit_price:
        failures.append("Unattended close report fill price mismatch.")
    if len(execution_matches) != 1:
        failures.append(f"Expected exactly one matching SELL_TO_CLOSE execution in unattended close report, found {len(execution_matches)}.")
        return execution_matches[0] if execution_matches else None
    if not bool(close_verification.get("verified")):
        failures.append("Unattended close report close-position verification is not marked verified.")
    if _decimal(close_verification.get("exact_position_quantity")) != Decimal("0"):
        failures.append("Unattended close report exact position quantity is not flat.")
    execution = execution_matches[0]
    if not _same_time(execution.get("executed_at"), config.exit_fill_time):
        failures.append("Unattended close report execution fill time mismatch.")
    return {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED_FROM_UNATTENDED_CLOSE_REPORT",
        "bridge_classification": report.get("classification"),
        "source": "IBKR_UNATTENDED_PAPER_CLOSE_REPORT",
        "source_path": str(report_path),
        "order_intent_id": config.exit_intent_id,
        "intent_type": "SELL_TO_CLOSE",
        "action": config.exit_action,
        "symbol": config.symbol,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "quantity": _decimal_text(config.quantity),
        "broker_order_id": str(latest_status.get("order_id") or lifecycle.get("submitted_order_id") or ""),
        "client_id": config.exit_client_id,
        "perm_id": config.exit_perm_id,
        "exec_id": execution.get("execution_id") or execution.get("exec_id"),
        "fill_price": _decimal_text(config.exit_price),
        "fill_timestamp": _canonical_time(config.exit_fill_time),
    }


def _direct_filled_bridge_exit_evidence_from_report(
    *,
    config: LifecycleCloseCleanupConfig,
    report: Mapping[str, Any],
    report_path: Path,
    failures: list[str],
) -> dict[str, Any] | None:
    if str(report.get("artifact_type") or "") != "filled_bridge_result":
        return None
    checks = {
        "classification": str(report.get("classification") or "") == "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "bridge_classification": str(report.get("bridge_classification") or "") == "PAPER_STRATEGY_ORDER_FILLED",
        "intent_type": str(report.get("intent_type") or "").upper() == "SELL_TO_CLOSE",
        "order_intent_id": str(report.get("order_intent_id") or "") == config.exit_intent_id,
        "action": _exit_action_matches(report.get("action") or report.get("side"), config.exit_action),
        "symbol": str(report.get("symbol") or report.get("instrument") or "").upper() == config.symbol,
        "local_symbol": str(report.get("local_symbol") or _nested(report, "contract", "local_symbol") or "").upper()
        == config.local_symbol,
        "con_id": _int(report.get("con_id") or _nested(report, "contract", "qualified_contract_identifier")) == config.con_id,
        "quantity": _decimal(report.get("quantity")) == config.quantity,
        "fill_price": _decimal(report.get("fill_price")) == config.exit_price,
        "fill_timestamp": _same_time(report.get("fill_timestamp"), config.exit_fill_time),
        "client_id": _int(report.get("client_id")) == config.exit_client_id,
        "perm_id": _int(report.get("perm_id")) == config.exit_perm_id,
        "broker_flat": _decimal(report.get("broker_position_qty")) == Decimal("0")
        and _decimal(report.get("internal_position_qty")) == Decimal("0"),
    }
    if not all(checks.values()):
        failures.append(
            "Direct filled-bridge close artifact did not match expected exit identity: "
            + ", ".join(name for name, passed in checks.items() if not passed)
        )
        return None
    return {
        **dict(report),
        "source": "DIRECT_FILLED_BRIDGE_CLOSE_ARTIFACT",
        "source_path": str(report_path),
        "broker_order_id": str(report.get("broker_order_id") or ""),
        "exec_id": report.get("exec_id") or report.get("execution_id"),
        "fill_price": _decimal_text(config.exit_price),
        "fill_timestamp": _canonical_time(config.exit_fill_time),
    }


def _broker_flat_evidence(
    *,
    config: LifecycleCloseCleanupConfig,
    positions_snapshot: Mapping[str, Any],
    orders_snapshot: Mapping[str, Any],
    failures: list[str],
) -> dict[str, Any]:
    if not positions_snapshot:
        failures.append("Missing broker positions snapshot.")
    if not orders_snapshot:
        failures.append("Missing broker open-orders snapshot.")
    account_matches = str(
        positions_snapshot.get("account") or positions_snapshot.get("selected_account_id") or ""
    ) == config.account_id and str(orders_snapshot.get("account") or orders_snapshot.get("selected_account_id") or "") == config.account_id
    if not account_matches:
        failures.append("Broker truth account mismatch.")
    positions = positions_snapshot.get("positions") if isinstance(positions_snapshot.get("positions"), list) else []
    symbol_rows = [
        dict(row)
        for row in positions
        if str(row.get("symbol") or "").upper() == config.symbol
        and str(row.get("local_symbol") or row.get("localSymbol") or "").upper() == config.local_symbol
    ]
    flat_rows = [row for row in symbol_rows if _decimal(row.get("quantity")) == Decimal("0")]
    nonflat_rows = [row for row in symbol_rows if (_decimal(row.get("quantity")) or Decimal("0")) != Decimal("0")]
    if not flat_rows:
        failures.append(f"Broker truth does not include the required flat {config.symbol} row.")
    if nonflat_rows:
        failures.append(f"Broker truth reports non-flat {config.symbol} quantity.")
    open_orders = orders_snapshot.get("open_orders")
    open_order_count = orders_snapshot.get("open_order_count")
    if open_order_count is None and isinstance(open_orders, list):
        open_order_count = len(open_orders)
    if _int(open_order_count) != 0:
        failures.append("Broker truth open orders are not zero.")
    return {
        "positions_path_account": positions_snapshot.get("account") or positions_snapshot.get("selected_account_id"),
        "open_orders_path_account": orders_snapshot.get("account") or orders_snapshot.get("selected_account_id"),
        "positions_generated_at": positions_snapshot.get("generated_at"),
        "open_orders_generated_at": orders_snapshot.get("generated_at"),
        "matching_symbol_rows": symbol_rows,
        "broker_symbol_qty_flat": bool(flat_rows) and not nonflat_rows,
        "open_order_count": open_order_count,
        "open_orders_zero": _int(open_order_count) == 0,
    }


def _ambiguous_open_mnq_rows(
    *,
    config: LifecycleCloseCleanupConfig,
    rows: Sequence[Mapping[str, Any]],
    target: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    if config.symbol != "MNQ":
        return []
    latest_by_lifecycle: dict[str, dict[str, Any]] = {}
    for row in rows:
        lifecycle_id = str(row.get("lifecycle_id") or "")
        if not lifecycle_id:
            continue
        latest_by_lifecycle[lifecycle_id] = dict(row)
    ambiguous: list[dict[str, Any]] = []
    for row in latest_by_lifecycle.values():
        if target is not None and str(row.get("lifecycle_id") or "") == str(target.get("lifecycle_id") or ""):
            continue
        if str(row.get("instrument_family") or "").upper() != config.symbol:
            continue
        if str(row.get("local_symbol") or "").upper() != config.local_symbol:
            continue
        if _int(row.get("con_id")) != config.con_id:
            continue
        if not _is_open_managed(row):
            continue
        ambiguous.append(
            {
                **_row_identity(row),
                "classification": "STALE_AMBIGUOUS_OPEN_MNQ_ROW",
                "reason": "Open MNQ row shares contract identity but does not match the requested entry lifecycle/entry price/entry fill time.",
            }
        )
    return ambiguous


def _build_close_record(
    *,
    config: LifecycleCloseCleanupConfig,
    target: Mapping[str, Any] | None,
    exit_evidence: Mapping[str, Any] | None,
    now: datetime,
) -> dict[str, Any] | None:
    if target is None or exit_evidence is None:
        return None
    quantity = _decimal(target.get("quantity")) or config.quantity
    entry = _decimal(target.get("entry_fill_price")) or config.entry_price
    exit_price = _decimal(exit_evidence.get("fill_price")) or config.exit_price
    points = exit_price - entry if config.side == "LONG" else entry - exit_price
    realized = points * quantity * POINT_VALUE_BY_SYMBOL[config.symbol]
    ticks = points / TICK_SIZE_BY_SYMBOL[config.symbol]
    row = dict(target)
    row.update(
        {
            "ledger_schema_version": LEDGER_SCHEMA_VERSION,
            "exit_timestamp": _canonical_time(config.exit_fill_time),
            "exit_fill_time": _canonical_time(config.exit_fill_time),
            "exit_fill_price": _decimal_text(exit_price),
            "exit_price": _decimal_text(exit_price),
            "exit_order_id": str(exit_evidence.get("broker_order_id") or ""),
            "exit_perm_id": config.exit_perm_id,
            "exit_client_id": config.exit_client_id,
            "exit_exec_id": exit_evidence.get("exec_id") or exit_evidence.get("execution_id"),
            "exit_intent_id": config.exit_intent_id,
            "exit_action": config.exit_action,
            "exit_broker_identity": {
                "account_id": config.account_id,
                "broker_order_id": str(exit_evidence.get("broker_order_id") or ""),
                "client_id": config.exit_client_id,
                "perm_id": config.exit_perm_id,
                "con_id": config.con_id,
                "local_symbol": config.local_symbol,
            },
            "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT",
            "final_broker_state_classification": "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT",
            "final_position_status": "CLOSED_FLAT",
            "realized_pnl": _decimal_text(realized),
            "points_pnl": _decimal_text(points),
            "ticks_pnl": _decimal_text(ticks),
            "review_required": False,
            "broker_reconciled": False,
            "close_reconciliation_source": exit_evidence.get(
                "close_reconciliation_source",
                "VERIFIED_SELL_TO_CLOSE_BRIDGE_FILL_AND_BROKER_FLAT_TRUTH",
            ),
            "close_reason": exit_evidence.get(
                "close_reason", "Verified PAPER close fill with broker flat truth."
            ),
            "close_reconciliation_evidence_source": exit_evidence.get("source"),
            "close_reconciliation_evidence_path": exit_evidence.get("source_path"),
            "close_reconciliation_evidence_classification": exit_evidence.get("classification"),
            "close_reconciliation_evidence_fields": exit_evidence.get("evidence_fields"),
            "operator_manual_paper_close": bool(exit_evidence.get("operator_manual_paper_close")),
            "close_reconciliation_applied_at": now.isoformat(),
            "broker_mutation_attempted_by_cleanup": False,
            "submit_attempted_by_cleanup": False,
            "cancel_attempted_by_cleanup": False,
            "place_order_attempted_by_cleanup": False,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
            "source": target.get("source") or "TRACK_B_DIRECT_BRIDGE_FILL_ARTIFACT",
            "created_at": now.isoformat(),
        }
    )
    return row


def _build_offsetting_entry_reconciliation_record(
    *,
    config: LifecycleCloseCleanupConfig,
    rows: Sequence[Mapping[str, Any]],
    exit_evidence: Mapping[str, Any] | None,
    now: datetime,
) -> dict[str, Any] | None:
    if exit_evidence is None or exit_evidence.get("reclassified_offsetting_open_intent") is not True:
        return None
    lifecycle_id = config.offsetting_open_lifecycle_id or f"bridge_fill_{config.exit_intent_id}"
    target = next(
        (
            dict(row)
            for row in rows
            if str(row.get("lifecycle_id") or "") == lifecycle_id
            and str(row.get("final_position_status") or "") == "OPEN_MANAGED"
        ),
        None,
    )
    if target is None:
        return None
    return {
        "ledger_schema_version": LEDGER_SCHEMA_VERSION,
        "record_type": "ARTIFACT_RECONCILIATION",
        "reconciliation_schema_version": "track_b_artifact_reconciliation_v1",
        "trade_id": f"{target.get('trade_id')}:opposite_entry_offset_reclassified",
        "lifecycle_id": lifecycle_id,
        "strategy_id": target.get("strategy_id"),
        "instrument_family": target.get("instrument_family"),
        "contract_key": target.get("contract_key"),
        "local_symbol": target.get("local_symbol"),
        "con_id": target.get("con_id"),
        "account_id": target.get("account_id"),
        "prior_artifact_classification": target.get("paper_lifecycle_classification"),
        "prior_review_required": target.get("review_required"),
        "reconciliation_action": OPPOSITE_ENTRY_OFFSET_EXISTING_POSITION_RECLASSIFIED,
        "new_artifact_classification": OPPOSITE_ENTRY_OFFSET_EXISTING_POSITION_RECLASSIFIED,
        "final_position_status": OPPOSITE_ENTRY_OFFSET_EXISTING_POSITION_RECLASSIFIED,
        "review_required": False,
        "broker_reconciled": False,
        "offsetting_existing_lifecycle_id": config.entry_lifecycle_id,
        "offsetting_order_intent_id": config.exit_intent_id,
        "offsetting_broker_order_id": str(exit_evidence.get("broker_order_id") or ""),
        "offsetting_client_id": config.exit_client_id,
        "offsetting_perm_id": config.exit_perm_id,
        "offsetting_exec_id": exit_evidence.get("exec_id") or exit_evidence.get("execution_id"),
        "offsetting_fill_price": _decimal_text(config.exit_price),
        "offsetting_fill_time": _canonical_time(config.exit_fill_time),
        "broker_flat_confirmed": True,
        "broker_mutation_attempted": False,
        "submit_attempted": False,
        "paper_proof_cli_invoked": False,
        "source": "VERIFIED_OFFSETTING_OPEN_INTENT_AND_BROKER_FLAT_TRUTH",
        "created_at": now.isoformat(),
    }


def _offsetting_entry_already_reconciled(config: LifecycleCloseCleanupConfig, rows: Sequence[Mapping[str, Any]]) -> bool:
    lifecycle_id = config.offsetting_open_lifecycle_id or f"bridge_fill_{config.exit_intent_id}"
    for row in rows:
        if str(row.get("lifecycle_id") or "") != lifecycle_id:
            continue
        if str(row.get("new_artifact_classification") or "") == OPPOSITE_ENTRY_OFFSET_EXISTING_POSITION_RECLASSIFIED:
            return True
    return False


def _lifecycle_local_repair_guard(
    *,
    config: LifecycleCloseCleanupConfig,
    close_record: Mapping[str, Any] | None,
    broker_flat_evidence: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    evidence: dict[str, Any] = dict(close_record or {})
    evidence.update(
        {
            "requested_lifecycle_status": "CLOSED_FLAT",
            "broker_flat_proof": bool(
                broker_flat_evidence.get("broker_symbol_qty_flat") and broker_flat_evidence.get("open_orders_zero")
            ),
            "broker_flat_confirmed": bool(
                broker_flat_evidence.get("broker_symbol_qty_flat") and broker_flat_evidence.get("open_orders_zero")
            ),
            "broker_position_flat": bool(broker_flat_evidence.get("broker_symbol_qty_flat")),
            "open_order_count": broker_flat_evidence.get("open_order_count"),
        }
    )
    return validate_lifecycle_local_artifact_repair(
        config=TrackBLifecycleLocalRepairGuardConfig(
            repo_root=config.repo_root,
            control_plane_snapshot_path=config.control_plane_snapshot_path,
            max_snapshot_age_seconds=config.local_repair_snapshot_max_age_seconds,
            require_snapshot_for_apply=config.require_control_plane_snapshot_for_apply,
        ),
        current_state="OPEN_MANAGED",
        target_state="CLOSED_FLAT",
        evidence=evidence,
        apply=config.apply,
        active_state_affecting=True,
        target_identity=_expected_identity(config),
        now=now,
    )


def _already_has_matching_close(config: LifecycleCloseCleanupConfig, rows: Sequence[Mapping[str, Any]]) -> bool:
    for row in rows:
        if str(row.get("lifecycle_id") or "") != config.entry_lifecycle_id:
            continue
        if str(row.get("final_position_status") or "") != "CLOSED_FLAT":
            continue
        if _decimal(row.get("exit_fill_price")) != config.exit_price:
            continue
        if not _same_time(row.get("exit_timestamp") or row.get("exit_fill_time"), config.exit_fill_time):
            continue
        row_exit_intent_id = str(row.get("exit_intent_id") or "")
        if row_exit_intent_id == config.exit_intent_id:
            return True
        if row_exit_intent_id:
            continue
        row_perm_id = _int(row.get("exit_perm_id") or _nested(row, "exit_broker_identity", "perm_id"))
        row_client_id = _int(row.get("exit_client_id") or _nested(row, "exit_broker_identity", "client_id"))
        if row_perm_id == config.exit_perm_id and row_client_id == config.exit_client_id:
            return True
    return False


def _exit_action_matches(value: object, expected: str) -> bool:
    actual_action = str(value or "").strip().upper()
    expected_action = str(expected or "").strip().upper()
    if actual_action == expected_action:
        return True
    return expected_action == "SELL_TO_CLOSE" and actual_action == "SELL"


def _offsetting_open_intent_matches_close(row: Mapping[str, Any], *, config: LifecycleCloseCleanupConfig) -> bool:
    intent_type = str(row.get("intent_type") or "").strip().upper()
    action = str(row.get("action") or row.get("side") or "").strip().upper()
    if config.side == "LONG":
        return intent_type == "SELL_TO_OPEN" and action == "SELL" and config.exit_action.upper() in {"SELL", "SELL_TO_CLOSE"}
    if config.side == "SHORT":
        return intent_type == "BUY_TO_OPEN" and action == "BUY" and config.exit_action.upper() in {"BUY", "BUY_TO_CLOSE"}
    return False


def _build_summaries(
    *,
    records: Sequence[Mapping[str, Any]],
    ledger_jsonl: Path,
    output_root: Path,
    now: datetime,
) -> dict[str, dict[str, Any]]:
    return build_track_b_paper_trade_summaries(
        ledger_records=records,
        ledger_jsonl=ledger_jsonl,
        trade_summary_json=output_root / "latest_track_b_paper_trade_summary.json",
        live_position_status_json=output_root / "latest_track_b_live_position_status.json",
        pnl_summary_json=output_root / "latest_track_b_pnl_summary.json",
        now=now,
    )


def _write_summaries(*, summaries: Mapping[str, Mapping[str, Any]], output_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    for key, filename in (
        ("trade_summary", "latest_track_b_paper_trade_summary.json"),
        ("live_position_status", "latest_track_b_live_position_status.json"),
        ("pnl_summary", "latest_track_b_pnl_summary.json"),
    ):
        (output_root / filename).write_text(
            json.dumps(_jsonable(dict(summaries[key])), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


def _reconciliation_prediction(
    *,
    config: LifecycleCloseCleanupConfig,
    live_position_status: Mapping[str, Any],
    positions_snapshot: Mapping[str, Any],
    orders_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    lifecycle_positions = [
        dict(row)
        for row in (live_position_status.get("positions_by_instrument") or {}).values()
        if isinstance(row, Mapping)
        and str(row.get("instrument_family") or "").upper() in {config.symbol, "PL", "GC", "MGC", "NQ", "ES", "MES"}
    ]
    broker_positions = [
        dict(row)
        for row in positions_snapshot.get("positions", [])
        if isinstance(row, Mapping)
        and str(row.get("symbol") or "").upper() in {config.symbol, "PL", "GC", "MGC", "NQ", "ES", "MES"}
        and (_decimal(row.get("quantity")) or Decimal("0")) != Decimal("0")
    ]
    open_order_count = _int(orders_snapshot.get("open_order_count"))
    if open_order_count is None and isinstance(orders_snapshot.get("open_orders"), list):
        open_order_count = len(orders_snapshot.get("open_orders") or [])
    would_clear = len(lifecycle_positions) == len(broker_positions) and open_order_count == 0
    return {
        "would_clear": would_clear,
        "lifecycle_open_position_count": len(lifecycle_positions),
        "broker_open_position_count": len(broker_positions),
        "broker_open_order_count": open_order_count,
        "lifecycle_positions": lifecycle_positions,
        "broker_positions": broker_positions,
        "note": "Count-level prediction only; the normal broker reconciliation remains authoritative after apply.",
    }


def _is_open_managed(row: Mapping[str, Any]) -> bool:
    return (
        str(row.get("final_position_status") or "") == "OPEN_MANAGED"
        and row.get("entry_fill_price") not in {None, ""}
        and row.get("exit_fill_price") in {None, ""}
    )


def _expected_identity(config: LifecycleCloseCleanupConfig) -> dict[str, Any]:
    return {
        "account_id": config.account_id,
        "symbol": config.symbol,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "side": config.side,
        "quantity": _decimal_text(config.quantity),
        "entry_lifecycle_id": config.entry_lifecycle_id,
        "entry_fill_time": _canonical_time(config.entry_fill_time),
        "entry_price": _decimal_text(config.entry_price),
        "exit_intent_id": config.exit_intent_id,
        "exit_action": config.exit_action,
        "exit_fill_price": _decimal_text(config.exit_price),
        "exit_fill_time": _canonical_time(config.exit_fill_time),
        "exit_client_id": config.exit_client_id,
        "exit_perm_id": config.exit_perm_id,
    }


def _row_identity(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "trade_id": row.get("trade_id"),
        "strategy_id": row.get("strategy_id"),
        "lifecycle_id": row.get("lifecycle_id"),
        "instrument_family": row.get("instrument_family"),
        "contract_key": row.get("contract_key"),
        "local_symbol": row.get("local_symbol"),
        "con_id": row.get("con_id"),
        "side": row.get("side"),
        "quantity": row.get("quantity"),
        "entry_timestamp": row.get("entry_timestamp"),
        "entry_fill_price": row.get("entry_fill_price"),
        "final_position_status": row.get("final_position_status"),
    }


def _summary_compact(summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "open_position_count": summary.get("open_position_count"),
        "open_position_record_count": summary.get("open_position_record_count"),
        "completed_trade_count": summary.get("completed_trade_count"),
        "review_required_count": summary.get("review_required_count"),
        "total_realized_pnl_today": summary.get("total_realized_pnl_today"),
    }


def _position_compact(status: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "open_position_count": status.get("open_position_count"),
        "open_order_count": status.get("open_order_count"),
        "positions_by_instrument": status.get("positions_by_instrument"),
        "positions_by_strategy_keys": sorted((status.get("positions_by_strategy") or {}).keys()),
    }


def _shared_truth_remediation_evidence(
    *,
    config: LifecycleCloseCleanupConfig,
    target: Mapping[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    paths = {
        "open_order_truth": config.open_order_truth_path,
        "managed_order_registry": config.managed_order_registry_path,
        "position_truth": config.position_truth_path,
        "managed_position_registry": config.managed_position_registry_path,
        "runtime_supervisor_authority": config.runtime_supervisor_authority_path,
        "reconciliation": config.reconciliation_path,
        "broker_lease": config.broker_lease_path,
    }
    payloads = {name: _read_json(_resolve(repo_root=config.repo_root, path=path)) for name, path in paths.items()}
    classifications = {
        name: _shared_classification(name=name, payload=payload)
        for name, payload in payloads.items()
    }
    freshness = {
        name: _shared_freshness(payload=payload, now=now, max_age_seconds=config.shared_truth_max_age_seconds)
        for name, payload in payloads.items()
    }
    blockers: list[str] = []
    required_shared_truth = {
        "open_order_truth",
        "managed_order_registry",
        "position_truth",
        "managed_position_registry",
        "runtime_supervisor_authority",
    }
    if config.require_shared_truth_evidence:
        for name, payload in payloads.items():
            if name in required_shared_truth and not payload:
                blockers.append(f"Shared truth authority artifact missing: {name}.")
        for name, state in freshness.items():
            if name in required_shared_truth and state["stale_or_missing"]:
                blockers.append(f"Shared truth authority artifact stale/missing: {name}.")

    open_order_classification = classifications["open_order_truth"]
    if open_order_classification and open_order_classification != NO_OPEN_ORDERS:
        blockers.append(f"Open Order Truth is not clean: {open_order_classification}.")
    managed_order_classification = classifications["managed_order_registry"]
    if managed_order_classification and managed_order_classification != NO_MANAGED_ORDERS:
        blockers.append(f"Managed Order Registry is not clean: {managed_order_classification}.")

    supervisor_classification = classifications["runtime_supervisor_authority"]
    if supervisor_classification in {
        "SUPERVISOR_MANUAL_REVIEW_REQUIRED",
        "SUPERVISOR_RESTART_BLOCKED_OPERATOR_ACK",
        "SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP",
        "SUPERVISOR_SHARED_TRUTH_STALE",
        "SUPERVISOR_UNKNOWN_REVIEW_REQUIRED",
    }:
        blockers.append(f"Runtime Supervisor Authority blocks local cleanup: {supervisor_classification}.")

    managed_positions = _list(payloads["managed_position_registry"].get("managed_positions"))
    if managed_positions and not _any_shared_position_matches_target(config=config, rows=managed_positions, target=target):
        blockers.append("Managed Position Registry active rows do not match requested cleanup target.")

    position_states = _list(payloads["position_truth"].get("position_states"))
    if position_states and not _any_shared_position_matches_target(config=config, rows=position_states, target=target):
        blockers.append("Position Truth active rows do not match requested cleanup target.")

    return {
        "source_authority": "execution_core_authority",
        "dashboard_projection_consumed": False,
        "required": config.require_shared_truth_evidence,
        "max_age_seconds": config.shared_truth_max_age_seconds,
        "artifact_paths": {name: str(_resolve(repo_root=config.repo_root, path=path)) for name, path in paths.items()},
        "classifications": classifications,
        "freshness": freshness,
        "target_agreement": {
            "managed_position_registry_rows": len(managed_positions),
            "managed_position_registry_matches_target": (
                None
                if not managed_positions
                else _any_shared_position_matches_target(config=config, rows=managed_positions, target=target)
            ),
            "position_truth_rows": len(position_states),
            "position_truth_matches_target": (
                None
                if not position_states
                else _any_shared_position_matches_target(config=config, rows=position_states, target=target)
            ),
        },
        "blockers": blockers,
    }


def _shared_classification(*, name: str, payload: Mapping[str, Any]) -> str:
    if name == "position_truth":
        summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
        return str(payload.get("classification") or summary.get("overall_classification") or "")
    if name == "runtime_supervisor_authority":
        return str(payload.get("classification") or payload.get("supervisor_classification") or "")
    if name == "broker_lease":
        return str(payload.get("classification") or payload.get("lease_state") or "")
    return str(payload.get("classification") or "")


def _shared_freshness(*, payload: Mapping[str, Any], now: datetime, max_age_seconds: float) -> dict[str, Any]:
    generated_at = payload.get("generated_at") or payload.get("latest_refresh_time") or payload.get("last_success_at")
    age_seconds = _age_seconds(generated_at, now)
    return {
        "generated_at": generated_at,
        "age_seconds": age_seconds,
        "stale_or_missing": age_seconds is None or age_seconds > max_age_seconds,
    }


def _any_shared_position_matches_target(
    *,
    config: LifecycleCloseCleanupConfig,
    rows: Sequence[Mapping[str, Any]],
    target: Mapping[str, Any] | None,
) -> bool:
    return any(_shared_position_matches_target(config=config, row=row, target=target) for row in rows)


def _shared_position_matches_target(
    *,
    config: LifecycleCloseCleanupConfig,
    row: Mapping[str, Any],
    target: Mapping[str, Any] | None,
) -> bool:
    lifecycle_id = str(
        row.get("lifecycle_id")
        or row.get("entry_lifecycle_id")
        or _nested(row, "lifecycle", "lifecycle_id")
        or ""
    )
    if lifecycle_id and lifecycle_id == config.entry_lifecycle_id:
        return True
    symbol = str(row.get("symbol") or row.get("instrument_family") or row.get("instrument") or "").upper()
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").upper()
    con_id = _int(row.get("con_id") or row.get("conId"))
    quantity = _decimal(row.get("quantity") or row.get("broker_quantity") or row.get("qty"))
    if symbol == config.symbol and local_symbol == config.local_symbol and con_id == config.con_id:
        if quantity in {None, config.quantity, Decimal("0")}:
            return True
    if target is None:
        return False
    return str(row.get("trade_id") or "") == str(target.get("trade_id") or "")


def _resolve(*, repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _age_seconds(value: object, now: datetime) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, (now - datetime.fromisoformat(_canonical_time(value))).total_seconds())
    except (TypeError, ValueError):
        return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _write_audit(
    repo_root: Path,
    output_root: Path,
    report: Mapping[str, Any],
    *,
    actual_now: datetime,
    suffix: str | None = None,
) -> Path:
    root = repo_root / output_root
    root.mkdir(parents=True, exist_ok=True)
    stamp = actual_now.strftime("%Y%m%dT%H%M%S%fZ")
    suffix_text = f"_{suffix}" if suffix else ""
    path = root / f"track_b_paper_lifecycle_close_cleanup_{stamp}{suffix_text}.json"
    path.write_text(json.dumps(_jsonable(dict(report)), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _write_latest_audit(repo_root: Path, output_root: Path, report: Mapping[str, Any]) -> Path:
    root = repo_root / output_root
    root.mkdir(parents=True, exist_ok=True)
    path = root / "latest_track_b_paper_lifecycle_close_cleanup_audit.json"
    path.write_text(json.dumps(_jsonable(dict(report)), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _nested(payload: Mapping[str, Any], *keys: str) -> Any:
    value: Any = payload
    for key in keys:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    return value


def _canonical_time(value: object) -> str:
    raw = str(value or "")
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    return parsed.astimezone(UTC).isoformat()


def _same_time(left: object, right: object) -> bool:
    if not left or not right:
        return False
    try:
        return _canonical_time(left) == _canonical_time(right)
    except ValueError:
        return False


def _time_after(left: object, right: object) -> bool:
    if not left or not right:
        return False
    try:
        return datetime.fromisoformat(_canonical_time(left)) > datetime.fromisoformat(_canonical_time(right))
    except ValueError:
        return False


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_text(value: Any) -> str | None:
    decimal_value = _decimal(value)
    if decimal_value is None:
        return None
    return format(decimal_value.normalize(), "f")


def _int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="track-b-paper-lifecycle-close-cleanup",
        description="Supervised offline PAPER lifecycle close cleanup for a proven bridge exit.",
    )
    parser.add_argument("--repo-root", default=".", help="Repository root containing Track B artifacts.")
    parser.add_argument("--lane-id", default=DEFAULT_LANE_ID)
    parser.add_argument("--strategy-id", default=DEFAULT_STRATEGY_ID)
    parser.add_argument("--account-id", default=PAPER_ACCOUNT_ID)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--local-symbol", default=DEFAULT_LOCAL_SYMBOL)
    parser.add_argument("--con-id", type=int, default=DEFAULT_CON_ID)
    parser.add_argument("--side", default=DEFAULT_SIDE)
    parser.add_argument("--quantity", default=str(DEFAULT_QUANTITY))
    parser.add_argument("--entry-lifecycle-id", default=DEFAULT_ENTRY_LIFECYCLE_ID)
    parser.add_argument("--entry-fill-time", default=DEFAULT_ENTRY_FILL_TIME)
    parser.add_argument("--entry-price", default=str(DEFAULT_ENTRY_PRICE))
    parser.add_argument("--exit-intent-id", default=DEFAULT_EXIT_INTENT_ID)
    parser.add_argument("--exit-action", default=DEFAULT_EXIT_ACTION)
    parser.add_argument("--exit-price", default=str(DEFAULT_EXIT_PRICE))
    parser.add_argument("--exit-fill-time", default=DEFAULT_EXIT_FILL_TIME)
    parser.add_argument("--exit-client-id", type=int, default=DEFAULT_EXIT_CLIENT_ID)
    parser.add_argument("--exit-perm-id", type=int, default=DEFAULT_EXIT_PERM_ID)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--broker-truth-root", default=str(DEFAULT_BROKER_TRUTH_ROOT))
    parser.add_argument("--lane-root", default=str(DEFAULT_LANE_ROOT))
    parser.add_argument("--ledger-root", default=str(DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT))
    parser.add_argument("--exit-bridge-report-path", default=None)
    parser.add_argument(
        "--evidence-kind",
        default=EVIDENCE_KIND_AUTO,
        choices=(EVIDENCE_KIND_AUTO, EVIDENCE_KIND_IBKR_POSITION_RECONCILED_FLAT),
        help="Explicit non-bridge evidence mode for guarded close cleanup.",
    )
    parser.add_argument("--allow-ledger-entry-evidence", action="store_true")
    parser.add_argument("--allow-offsetting-open-intent-as-close", action="store_true")
    parser.add_argument("--offsetting-open-lifecycle-id", default=None)
    parser.add_argument("--allow-ambiguous-mnq-rows", action="store_true")
    parser.add_argument("--apply", action="store_true", help="Actually append lifecycle artifacts. Omit for dry-run.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    quantity = _decimal(args.quantity)
    entry_price = _decimal(args.entry_price)
    exit_price = _decimal(args.exit_price)
    if quantity is None:
        raise SystemExit("--quantity must be decimal")
    if entry_price is None:
        raise SystemExit("--entry-price must be decimal")
    if exit_price is None:
        raise SystemExit("--exit-price must be decimal")
    result = run_track_b_paper_lifecycle_close_cleanup(
        config=LifecycleCloseCleanupConfig(
            repo_root=Path(args.repo_root).expanduser().resolve(),
            lane_id=str(args.lane_id),
            strategy_id=str(args.strategy_id),
            account_id=str(args.account_id),
            symbol=str(args.symbol).upper(),
            local_symbol=str(args.local_symbol).upper(),
            con_id=int(args.con_id),
            side=str(args.side).upper(),
            quantity=quantity,
            entry_lifecycle_id=str(args.entry_lifecycle_id),
            entry_fill_time=str(args.entry_fill_time),
            entry_price=entry_price,
            exit_intent_id=str(args.exit_intent_id),
            exit_action=str(args.exit_action).upper(),
            exit_price=exit_price,
            exit_fill_time=str(args.exit_fill_time),
            exit_client_id=int(args.exit_client_id),
            exit_perm_id=int(args.exit_perm_id),
            apply=bool(args.apply),
            refuse_on_ambiguous_mnq_rows=not bool(args.allow_ambiguous_mnq_rows),
            output_root=Path(args.output_root),
            lane_root=Path(args.lane_root),
            broker_truth_root=Path(args.broker_truth_root),
            ledger_root=Path(args.ledger_root),
            exit_bridge_report_path=Path(args.exit_bridge_report_path) if args.exit_bridge_report_path else None,
            evidence_kind=str(args.evidence_kind),
            allow_ledger_entry_evidence=bool(args.allow_ledger_entry_evidence),
            allow_offsetting_open_intent_as_close=bool(args.allow_offsetting_open_intent_as_close),
            offsetting_open_lifecycle_id=str(args.offsetting_open_lifecycle_id) if args.offsetting_open_lifecycle_id else None,
        )
    )
    print(json.dumps({"classification": result.classification, "audit_path": str(result.audit_path)}, indent=2))
    return 0 if result.classification != "TRACK_B_PAPER_LIFECYCLE_CLOSE_CLEANUP_REFUSED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
