"""Read-only Track B PAPER lifecycle-to-broker-truth reconciliation.

This module consumes already-written IBKR read-only position/open-order
snapshots and Track B lifecycle summaries. It never connects to IBKR and never
mutates broker state.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.phase1_runtime_ticker_registry import PHASE1_RUNTIME_TICKER_ORDER
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_exit_safety import (
    DEFAULT_BRIDGE_TERMINAL_EVENT_GRACE_SECONDS,
    bridge_terminal_event_grace_state,
    classify_managed_exit_working_order,
)
from mgc_v05l.execution_core.track_b_lifecycle_state_transition import (
    is_registry_eligible,
    normalize_lifecycle_state,
)
from mgc_v05l.execution_core.track_b_open_order_truth import (
    DUPLICATE_CLOSE_ORDER,
    SUSPICIOUS_ORDER_STATE,
    TrackBOpenOrderTruthConfig,
    build_track_b_open_order_truth_from_reconciliation,
)
from mgc_v05l.execution_core.track_b_submit_intent_ownership import (
    DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_LATEST_JSON,
    DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL,
    load_unresolved_submit_intent_ownership_records,
)
from mgc_v05l.execution_core.track_b_broker_position_identity import (
    IDENTITY_READY,
    canonicalize_broker_position_identity,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import (
    TradeCurrentState,
    TradeEvent,
    TradeEventType,
    TradeRegistryRecord,
)
from mgc_v05l.execution_core.track_b_broker_fill_evidence_resolver import (
    BrokerFillEvidenceRequest,
    resolve_broker_backed_fill_evidence,
)
from mgc_v05l.execution_core.track_b_historical_reconciliation_debris_resolver import (
    RESOLVER_CLEAN,
    HistoricalReconciliationDebrisResolverConfig,
    resolve_historical_reconciliation_debris,
)
from mgc_v05l.execution_core.track_b_current_exposure_owner_resolver import (
    CurrentExposureOwnerResolverConfig,
    apply_current_exposure_owner_lifecycle_overlay,
    resolve_current_exposure_ownership,
)
from mgc_v05l.execution_core.track_b_terminal_registry_truth import (
    resolve_terminal_registry_truth,
)
from mgc_v05l.execution_core.track_b_live_trade_registry import (
    append_live_trade_registry_event,
    load_live_trade_registry_records,
    make_live_trade_registry_event,
    trade_id_from_live_identity,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LEDGER_ROOT = REPO_ROOT / "outputs" / "track_b_execution_core" / "paper_trade_ledger"
DEFAULT_BROKER_TRUTH_ROOT = REPO_ROOT / "outputs" / "reports" / "ibkr_read_only_verification"
DEFAULT_MARKET_DATA_ROOT = REPO_ROOT / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data"
DEFAULT_REPORT_PATH = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_KNOWN_MANAGED_EXIT_ORDERS_PATH = (
    REPO_ROOT
    / "outputs"
    / "track_b_execution_core"
    / "managed_exit_orders"
    / "latest_known_managed_exit_orders.json"
)
DEFAULT_KNOWN_LEAK_TEST_ENTRY_ORDERS_PATH = (
    REPO_ROOT
    / "outputs"
    / "track_b_execution_core"
    / "leak_test_entry_orders"
    / "latest_known_leak_test_entry_orders.json"
)
DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT = (
    REPO_ROOT
    / "outputs"
    / "track_b_execution_core"
    / "managed_orders"
    / "latest_managed_orders.json"
)
DEFAULT_SUBMIT_INTENT_OWNERSHIP_PATH = REPO_ROOT / DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_JSONL
DEFAULT_MAX_AGE_SECONDS = float(os.environ.get("TRACK_B_BROKER_TRUTH_MAX_AGE_SECONDS", "120"))
DEFAULT_BROKER_TRUTH_SETTLEMENT_SECONDS = float(os.environ.get("TRACK_B_PAPER_BROKER_TRUTH_SETTLEMENT_SECONDS", "300"))
DEFAULT_BROKER_TRUTH_SETTLEMENT_POLL_SECONDS = float(os.environ.get("TRACK_B_PAPER_BROKER_TRUTH_SETTLEMENT_POLL_SECONDS", "15"))
PAPER_ACCOUNT = "DUM882026"
DEFAULT_MIN_TICK_BY_ROOT = {
    "GC": 0.1,
    "MGC": 0.1,
    "NQ": 0.25,
    "MNQ": 0.25,
    "ES": 0.25,
    "MES": 0.25,
    "PL": 0.1,
    "ZT": 0.0078125,
    "ZF": 0.0078125,
    "ZN": 0.015625,
    "ZB": 0.03125,
}


@dataclass(frozen=True)
class ReconciliationConfig:
    repo_root: Path = REPO_ROOT
    ledger_root: Path = DEFAULT_LEDGER_ROOT
    broker_truth_root: Path = DEFAULT_BROKER_TRUTH_ROOT
    market_data_root: Path = DEFAULT_MARKET_DATA_ROOT
    report_path: Path = DEFAULT_REPORT_PATH
    max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS
    bridge_terminal_event_grace_seconds: float = DEFAULT_BRIDGE_TERMINAL_EVENT_GRACE_SECONDS
    broker_truth_settlement_seconds: float = DEFAULT_BROKER_TRUTH_SETTLEMENT_SECONDS
    broker_truth_settlement_poll_seconds: float = DEFAULT_BROKER_TRUTH_SETTLEMENT_POLL_SECONDS
    account: str = PAPER_ACCOUNT
    symbols: tuple[str, ...] = PHASE1_RUNTIME_TICKER_ORDER
    submit_intent_ownership_path: Path = DEFAULT_SUBMIT_INTENT_OWNERSHIP_PATH
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT

    @property
    def trade_summary_path(self) -> Path:
        return self.ledger_root / "latest_track_b_paper_trade_summary.json"

    @property
    def live_position_status_path(self) -> Path:
        return self.ledger_root / "latest_track_b_live_position_status.json"

    @property
    def pnl_summary_path(self) -> Path:
        return self.ledger_root / "latest_track_b_pnl_summary.json"

    @property
    def reconciled_trade_summary_path(self) -> Path:
        return self.ledger_root / "latest_track_b_broker_reconciled_paper_trade_summary.json"

    @property
    def reconciled_live_position_status_path(self) -> Path:
        return self.ledger_root / "latest_track_b_broker_reconciled_live_position_status.json"

    @property
    def reconciled_pnl_summary_path(self) -> Path:
        return self.ledger_root / "latest_track_b_broker_reconciled_pnl_summary.json"

    @property
    def broker_status_path(self) -> Path:
        return self.broker_truth_root / "ibkr_broker_truth_refresh_status.json"


def reconcile_track_b_paper_broker_truth(
    *,
    config: ReconciliationConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    trade_summary = _load_json(config.trade_summary_path)
    live_position_status = _load_json(config.live_position_status_path)
    pnl_summary = _load_json(config.pnl_summary_path)
    broker_status = _load_json(config.broker_status_path)
    positions_path = _path_from_payload(
        broker_status.get("positions_snapshot_path"),
        default=config.broker_truth_root / "ibkr_positions_snapshot.json",
    )
    open_orders_path = _path_from_payload(
        broker_status.get("open_orders_snapshot_path"),
        default=config.broker_truth_root / "ibkr_open_orders_snapshot.json",
    )
    positions_snapshot = _load_json(positions_path)
    open_orders_snapshot = _load_json(open_orders_path)

    blockers: list[dict[str, Any]] = []
    _validate_broker_truth(
        broker_status=broker_status,
        positions_snapshot=positions_snapshot,
        open_orders_snapshot=open_orders_snapshot,
        positions_path=positions_path,
        open_orders_path=open_orders_path,
        config=config,
        now=actual_now,
        blockers=blockers,
    )
    raw_review_required_count = _lifecycle_review_required_count(
        trade_summary=trade_summary,
        live_position_status=live_position_status,
        pnl_summary=pnl_summary,
    )
    lifecycle_blockers = _validate_lifecycle_read_model(
        trade_summary=trade_summary,
        live_position_status=live_position_status,
        pnl_summary=pnl_summary,
    )
    track_b_positions = _track_b_broker_positions(positions_snapshot, config.symbols)
    track_b_open_orders = _track_b_broker_open_orders(open_orders_snapshot, config.symbols)
    lifecycle_positions = _track_b_lifecycle_positions(live_position_status, config.symbols)
    lifecycle_projection_precedence = _closed_flat_lifecycle_projection_precedence(
        config=config,
        broker_positions=track_b_positions,
        broker_open_orders=track_b_open_orders,
        lifecycle_positions=lifecycle_positions,
    )
    lifecycle_positions = list(lifecycle_projection_precedence["current_scope_lifecycle_positions"])
    current_exposure_owner_resolution = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=config.repo_root),
        broker_positions=track_b_positions,
        broker_open_orders=track_b_open_orders,
        lifecycle_positions=lifecycle_positions,
        managed_position_registry={},
    )
    lifecycle_positions, owner_superseded_lifecycle_positions = apply_current_exposure_owner_lifecycle_overlay(
        lifecycle_positions=lifecycle_positions,
        owner_resolution=current_exposure_owner_resolution,
    )
    if owner_superseded_lifecycle_positions:
        lifecycle_projection_precedence = {
            **lifecycle_projection_precedence,
            "current_scope_lifecycle_positions": lifecycle_positions,
            "superseded_lifecycle_projections": [
                *list(lifecycle_projection_precedence.get("superseded_lifecycle_projections") or []),
                *owner_superseded_lifecycle_positions,
            ],
        }
    terminal_event_grace = _bridge_terminal_event_grace_for_flat_lifecycle(
        trade_summary=trade_summary,
        live_position_status=live_position_status,
        config=config,
        now=actual_now,
    )
    if terminal_event_grace.get("applied") is True:
        stale_codes = {"BROKER_TRUTH_STATUS_STALE", "BROKER_TRUTH_SNAPSHOT_STALE"}
        stale_blockers = [item for item in blockers if item.get("code") in stale_codes]
        blockers = [item for item in blockers if item.get("code") not in stale_codes]
        terminal_event_grace["downgraded_stale_blockers"] = stale_blockers
    position_match_report = _broker_lifecycle_position_match(
        broker_positions=track_b_positions,
        lifecycle_positions=lifecycle_positions,
        symbols=config.symbols,
    )
    if owner_superseded_lifecycle_positions:
        position_match_report = _append_owner_superseded_lifecycle_positions_to_match_report(
            position_match_report=position_match_report,
            owner_superseded_lifecycle_positions=owner_superseded_lifecycle_positions,
        )
    known_managed_exit_orders = _known_managed_exit_orders(
        broker_open_orders=track_b_open_orders,
        lifecycle_status=live_position_status,
        position_match_report=position_match_report,
        runtime_restore_orders=_runtime_restore_known_managed_exit_orders(config.repo_root, config.symbols),
        persisted_known_orders=_persisted_known_managed_exit_orders(config.repo_root, config.symbols),
        config=config,
        now=actual_now,
    )
    known_leak_test_entry_orders = _known_leak_test_entry_orders(
        broker_open_orders=track_b_open_orders,
        config=config,
        persisted_known_orders=_persisted_known_leak_test_entry_orders(config.repo_root, config.symbols),
        artifact_known_orders=_artifact_known_leak_test_entry_orders(config.repo_root, config.symbols),
    )
    unknown_track_b_open_orders = _unknown_track_b_open_orders(
        broker_open_orders=track_b_open_orders,
        known_managed_exit_orders=known_managed_exit_orders,
        known_leak_test_entry_orders=known_leak_test_entry_orders,
    )
    open_order_truth_evidence = _open_order_truth_evidence(
        config=config,
        now=actual_now,
        track_b_positions=track_b_positions,
        track_b_open_orders=track_b_open_orders,
        lifecycle_positions=lifecycle_positions,
        known_managed_exit_orders=known_managed_exit_orders,
        unknown_track_b_open_orders=unknown_track_b_open_orders,
    )
    managed_order_registry_evidence = _managed_order_registry_evidence(config=config, now=actual_now)
    unresolved_submit_intents = _unresolved_submit_intent_ownership_records(config)
    historical_debris_resolution = resolve_historical_reconciliation_debris(
        config=HistoricalReconciliationDebrisResolverConfig(
            repo_root=config.repo_root,
            submit_intent_ownership_path=_repo_scoped_path(config.repo_root, config.submit_intent_ownership_path),
            latest_submit_intent_ownership_path=config.repo_root
            / DEFAULT_TRACK_B_SUBMIT_INTENT_OWNERSHIP_LATEST_JSON,
            stale_after_seconds=config.broker_truth_settlement_seconds,
            apply=True,
        ),
        now=actual_now,
        broker_positions=track_b_positions,
        broker_open_orders=track_b_open_orders,
        lifecycle_positions=lifecycle_positions,
        unresolved_submit_intents=unresolved_submit_intents,
        lifecycle_review_required=any(row.get("code") == "LIFECYCLE_REVIEW_REQUIRED_PRESENT" for row in lifecycle_blockers),
        broker_flat_proof_path=positions_path,
        open_orders_proof_path=open_orders_path,
        source_artifact_path=config.report_path,
    )
    unresolved_submit_intents = _filter_historical_resolved_submit_intents(
        unresolved_submit_intents,
        historical_debris_resolution=historical_debris_resolution,
    )
    lifecycle_blockers = _filter_historical_resolved_lifecycle_blockers(
        lifecycle_blockers,
        historical_debris_resolution=historical_debris_resolution,
    )
    lifecycle_blockers = _filter_non_current_lifecycle_review_blockers(
        lifecycle_blockers,
        trade_summary=trade_summary,
        live_position_status=live_position_status,
        broker_positions=track_b_positions,
        lifecycle_positions=lifecycle_positions,
        position_match_report=position_match_report,
        symbols=config.symbols,
    )
    current_scope_review_required_count = _current_scope_review_required_count(lifecycle_blockers)
    historical_review_required_count = max(raw_review_required_count - current_scope_review_required_count, 0)
    blockers.extend(lifecycle_blockers)
    submit_intent_ownership_reconciliation = _submit_intent_ownership_reconciliation_state(
        unresolved_submit_intents=unresolved_submit_intents,
        broker_positions=track_b_positions,
        lifecycle_positions=lifecycle_positions,
        unknown_open_orders=unknown_track_b_open_orders,
        position_match_report=position_match_report,
        config=config,
        now=actual_now,
    )
    broker_truth_settlement = _broker_truth_settlement_state(
        position_match_report=position_match_report,
        broker_positions=track_b_positions,
        lifecycle_positions=lifecycle_positions,
        broker_open_orders=track_b_open_orders,
        unknown_open_orders=unknown_track_b_open_orders,
        trade_summary=trade_summary,
        live_position_status=live_position_status,
        broker_status=broker_status,
        config=config,
        now=actual_now,
    )
    broker_cost_basis_adjustments = _broker_cost_basis_adjustments_from_match_report(position_match_report)
    registry_reconciliation = _registry_reconciliation_state(
        config=config,
        broker_positions=track_b_positions,
        lifecycle_positions=lifecycle_positions,
        broker_open_orders=track_b_open_orders,
        position_match_report=position_match_report,
        current_exposure_owner_resolution=current_exposure_owner_resolution,
    )
    broker_backed_entry_adoption = _broker_backed_entry_adoption_remediation(
        config=config,
        submit_intent_ownership_reconciliation=submit_intent_ownership_reconciliation,
        registry_reconciliation=registry_reconciliation,
    )
    post_fill_lifecycle_adoption = _apply_post_fill_lifecycle_adoption_invariant(
        config=config,
        broker_backed_entry_adoption=broker_backed_entry_adoption,
        now=actual_now,
    )
    if registry_reconciliation.get("blocking") is True:
        blockers.append(
            {
                "code": registry_reconciliation.get("classification"),
                "detail": registry_reconciliation.get("detail"),
                "registry_reconciliation": registry_reconciliation,
            }
        )
    stale_managed_exit_orders = [
        row
        for row in known_managed_exit_orders
        if str(row.get("managed_order_policy", {}).get("stale_by_policy") or "").lower() == "true"
    ]
    hard_exit_order_not_marketable = [
        row
        for row in known_managed_exit_orders
        if row.get("managed_order_policy", {}).get("classification")
        in {
            "KNOWN_MANAGED_HARD_EXIT_ORDER_NOT_MARKETABLE",
            "KNOWN_MANAGED_HARD_EXIT_ORDER_REPRICE_REQUIRED",
        }
    ]
    if position_match_report["matched"] is not True:
        submit_intent_classification = str(submit_intent_ownership_reconciliation.get("classification") or "")
        settlement_classification = str(broker_truth_settlement.get("classification") or "")
        if submit_intent_classification in {
            "SUBMIT_INTENT_BROKER_POSITION_ADOPTION_REQUIRED",
            "SUBMIT_INTENT_BROKER_POSITIONS_ADOPTION_REQUIRED",
        }:
            if broker_backed_entry_adoption and broker_backed_entry_adoption.get("adoption_allowed") is not True:
                blockers.append(
                    {
                        "code": "REVIEW_REQUIRED_UNMANAGED_BROKER_EXPOSURE",
                        "detail": "Broker-backed PAPER exposure exists but no OPEN_MANAGED lifecycle state was created within the adoption grace path.",
                        "broker_backed_entry_adoption": broker_backed_entry_adoption,
                        "position_match_blocker": position_match_report.get("blocker"),
                    }
                )
            blockers.append(
                {
                    "code": submit_intent_classification,
                    "legacy_code": "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH",
                    "detail": submit_intent_ownership_reconciliation.get("detail")
                    or "Broker position is attributed to a durable unresolved Track B submit intent and requires lifecycle adoption.",
                    "submit_intent_ownership_reconciliation": submit_intent_ownership_reconciliation,
                    "broker_backed_entry_adoption": broker_backed_entry_adoption,
                    "position_match_blocker": position_match_report.get("blocker"),
                }
            )
        elif submit_intent_classification in {
            "SUBMIT_INTENT_COMPETING_UNRESOLVED_REVIEW_REQUIRED",
            "SUBMIT_INTENT_IDENTITY_MISMATCH_REVIEW_REQUIRED",
        }:
            blockers.append(
                {
                    "code": submit_intent_classification,
                    "detail": submit_intent_ownership_reconciliation.get("detail"),
                    "submit_intent_ownership_reconciliation": submit_intent_ownership_reconciliation,
                    "position_match_blocker": position_match_report.get("blocker"),
                }
            )
        elif settlement_classification == "WAITING_FOR_BROKER_TRUTH_SETTLEMENT":
            pass
        elif settlement_classification in {"BROKER_TRUTH_SETTLEMENT_TIMEOUT", "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE"}:
            blockers.append(
                {
                    "code": settlement_classification,
                    "detail": broker_truth_settlement.get("detail"),
                    "broker_truth_settlement": broker_truth_settlement,
                    "position_match_blocker": position_match_report.get("blocker"),
                }
            )
        else:
            blockers.append(position_match_report["blocker"])
    elif submit_intent_ownership_reconciliation.get("classification") in {
        "SUBMIT_INTENT_NO_BROKER_EFFECT_TIMEOUT",
        "SUBMIT_INTENT_COMPETING_UNRESOLVED_REVIEW_REQUIRED",
        "SUBMIT_INTENT_IDENTITY_MISMATCH_REVIEW_REQUIRED",
    }:
        blockers.append(
            {
                "code": submit_intent_ownership_reconciliation.get("classification"),
                "detail": submit_intent_ownership_reconciliation.get("detail"),
                "submit_intent_ownership_reconciliation": submit_intent_ownership_reconciliation,
            }
        )
    if unknown_track_b_open_orders:
        blockers.append(
            {
                "code": "UNKNOWN_BROKER_OPEN_ORDER",
                "legacy_code": "TRACK_B_BROKER_OPEN_ORDER_PRESENT",
                "detail": "IBKR broker truth reports Track B futures open orders that are not attributed to a known managed exit or leak-test entry.",
                "open_orders": unknown_track_b_open_orders,
                "open_order_truth": _open_order_truth_blocker_context(open_order_truth_evidence),
            }
        )
    open_order_truth_classification = str(open_order_truth_evidence.get("classification") or "")
    if open_order_truth_classification in {DUPLICATE_CLOSE_ORDER, SUSPICIOUS_ORDER_STATE}:
        blockers.append(
            {
                "code": open_order_truth_classification,
                "detail": "Open Order Truth reports duplicate or suspicious Track B PAPER open-order state.",
                "open_order_truth": _open_order_truth_blocker_context(open_order_truth_evidence),
            }
        )

    settlement_waiting = broker_truth_settlement.get("classification") == "WAITING_FOR_BROKER_TRUTH_SETTLEMENT"
    settlement_resolved = broker_truth_settlement.get("classification") == "BROKER_TRUTH_SETTLEMENT_RESOLVED"
    submit_intent_pending = (
        submit_intent_ownership_reconciliation.get("classification") == "SUBMIT_INTENT_NO_BROKER_EFFECT_PENDING_SETTLEMENT"
        and not unknown_track_b_open_orders
        and position_match_report["matched"] is True
    )
    reconciled = not blockers and position_match_report["matched"] is True and not submit_intent_pending
    if submit_intent_pending:
        classification = "SUBMIT_INTENT_NO_BROKER_EFFECT_PENDING_SETTLEMENT"
    elif settlement_waiting:
        classification = "WAITING_FOR_BROKER_TRUTH_SETTLEMENT"
    elif settlement_resolved:
        classification = "BROKER_TRUTH_SETTLEMENT_RESOLVED"
    elif blockers and broker_truth_settlement.get("classification") in {
        "BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE",
    }:
        classification = str(broker_truth_settlement.get("classification"))
    elif reconciled and known_managed_exit_orders:
        classification = "TRACK_B_PAPER_BROKER_RECONCILED_WITH_KNOWN_MANAGED_EXIT_ORDER"
    elif reconciled and known_leak_test_entry_orders:
        classification = "TRACK_B_PAPER_BROKER_RECONCILED_WITH_KNOWN_LEAK_TEST_ENTRY_ORDER"
    else:
        classification = "TRACK_B_PAPER_BROKER_RECONCILED" if reconciled else "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    report = {
        "schema_version": "track_b_paper_broker_reconciliation_v1",
        "generated_at": actual_now.isoformat(),
        "classification": classification,
        "broker_reconciled": reconciled,
        "source": "IBKR_READ_ONLY_BROKER_TRUTH",
        "account": config.account,
        "symbols": list(config.symbols),
        "max_age_seconds": config.max_age_seconds,
        "bridge_terminal_event_grace_seconds": config.bridge_terminal_event_grace_seconds,
        "broker_truth_settlement_seconds": config.broker_truth_settlement_seconds,
        "broker_truth_settlement_poll_seconds": config.broker_truth_settlement_poll_seconds,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "base_artifacts": {
            "trade_summary": str(config.trade_summary_path),
            "live_position_status": str(config.live_position_status_path),
            "pnl_summary": str(config.pnl_summary_path),
        },
        "broker_truth_artifacts": {
            "status": str(config.broker_status_path),
            "positions_snapshot": str(positions_path),
            "open_orders_snapshot": str(open_orders_path),
        },
        "last_successful_broker_truth": broker_status.get("last_successful_broker_truth")
        if isinstance(broker_status.get("last_successful_broker_truth"), Mapping)
        else _last_successful_broker_truth_from_status(broker_status),
        "latest_attempt_status": broker_status.get("latest_attempt_status")
        if isinstance(broker_status.get("latest_attempt_status"), Mapping)
        else {},
        "reconciled_artifacts": {
            "trade_summary": str(config.reconciled_trade_summary_path),
            "live_position_status": str(config.reconciled_live_position_status_path),
            "pnl_summary": str(config.reconciled_pnl_summary_path),
        },
        "track_b_broker_position_count": len(track_b_positions),
        "track_b_broker_open_order_count": len(track_b_open_orders),
        "known_managed_exit_order_count": len(known_managed_exit_orders),
        "known_leak_test_entry_order_count": len(known_leak_test_entry_orders),
        "unresolved_submit_intent_ownership_count": len(unresolved_submit_intents),
        "stale_managed_exit_order_count": len(stale_managed_exit_orders),
        "hard_exit_order_not_marketable_count": len(hard_exit_order_not_marketable),
        "unknown_broker_open_order_count": len(unknown_track_b_open_orders),
        "open_order_truth_classification": open_order_truth_evidence.get("classification"),
        "open_order_truth": _open_order_truth_report_context(open_order_truth_evidence),
        "managed_order_registry_classification": managed_order_registry_evidence.get("classification"),
        "managed_order_registry": managed_order_registry_evidence,
        "track_b_broker_positions": track_b_positions,
        "track_b_broker_open_orders": track_b_open_orders,
        "known_managed_exit_orders": known_managed_exit_orders,
        "known_leak_test_entry_orders": known_leak_test_entry_orders,
        "unresolved_submit_intent_ownership_records": unresolved_submit_intents,
        "submit_intent_ownership_reconciliation": submit_intent_ownership_reconciliation,
        "historical_reconciliation_debris_resolution": historical_debris_resolution,
        "broker_backed_entry_adoption": broker_backed_entry_adoption,
        "post_fill_lifecycle_adoption": post_fill_lifecycle_adoption,
        "stale_managed_exit_orders": stale_managed_exit_orders,
        "hard_exit_order_not_marketable_orders": hard_exit_order_not_marketable,
        "unknown_broker_open_orders": unknown_track_b_open_orders,
        "track_b_lifecycle_positions": lifecycle_positions,
        "current_exposure_owner_resolution": current_exposure_owner_resolution,
        "superseded_lifecycle_projections": lifecycle_projection_precedence["superseded_lifecycle_projections"],
        "lifecycle_projection_precedence": lifecycle_projection_precedence,
        "position_match_report": position_match_report,
        "broker_cost_basis_adjustments": broker_cost_basis_adjustments,
        "bridge_terminal_event_grace": terminal_event_grace,
        "broker_truth_settlement": broker_truth_settlement,
        "registry_reconciliation": registry_reconciliation,
        "lifecycle_open_position_count": _int_value(live_position_status.get("open_position_count")),
        "lifecycle_open_order_count": _int_value(live_position_status.get("open_order_count")),
        "review_required_count": current_scope_review_required_count,
        "current_scope_review_required_count": current_scope_review_required_count,
        "historical_review_required_count": historical_review_required_count,
        "raw_review_required_count": raw_review_required_count,
        "blockers": blockers,
    }
    if reconciled:
        _write_reconciled_summaries(
            config=config,
            now=actual_now,
            trade_summary=trade_summary,
            live_position_status=live_position_status,
            pnl_summary=pnl_summary,
            report=report,
            positions_path=positions_path,
            open_orders_path=open_orders_path,
            broker_positions=track_b_positions,
        )
    _write_json_atomic(config.report_path, report)
    _append_reconciliation_registry_events(
        config=config,
        report=report,
        registry_reconciliation=registry_reconciliation,
        broker_backed_entry_adoption=broker_backed_entry_adoption,
        now=actual_now,
    )
    return report


def _registry_reconciliation_state(
    *,
    config: ReconciliationConfig,
    broker_positions: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
    broker_open_orders: Sequence[Mapping[str, Any]],
    position_match_report: Mapping[str, Any],
    current_exposure_owner_resolution: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    records = load_live_trade_registry_records(repo_root=config.repo_root)
    owner_resolution = current_exposure_owner_resolution if isinstance(current_exposure_owner_resolution, Mapping) else {}
    owner_exposures = [
        dict(row)
        for row in owner_resolution.get("owned_exposures") or []
        if isinstance(row, Mapping)
        and str(row.get("trade_id") or "").strip()
        and _owner_exposure_has_lifecycle_report_authority(row)
    ]
    owner_trade_ids = {str(row.get("trade_id") or "").strip() for row in owner_exposures}
    owner_superseded_trade_ids = {
        str(row.get("trade_id") or "").strip()
        for row in owner_resolution.get("stale_superseded_full_audit_only") or []
        if owner_trade_ids and isinstance(row, Mapping) and str(row.get("trade_id") or "").strip()
    }
    records_by_trade_id = {record.trade_id: record for record in records}
    superseded_lifecycle_positions = [
        row
        for row in position_match_report.get("superseded_unmatched_lifecycle_positions", [])
        if isinstance(row, Mapping)
    ]
    superseded_lifecycle_keys = {
        _lifecycle_position_scope_key(row)
        for row in superseded_lifecycle_positions
        if _lifecycle_position_scope_key(row)
    }
    current_scope_lifecycle_positions = [
        dict(row)
        for row in lifecycle_positions
        if _lifecycle_position_scope_key(row) not in superseded_lifecycle_keys
    ]
    base = {
        "source": "CENTRAL_TRADE_REGISTRY_READ_ONLY",
        "registry_event_path": str(config.repo_root / "outputs/track_b_execution_core/trade_registry/live_trade_events.jsonl"),
        "record_count": len(records),
        "broker_position_count": len(broker_positions),
        "lifecycle_position_count": len(lifecycle_positions),
        "current_scope_lifecycle_position_count": len(current_scope_lifecycle_positions),
        "broker_open_order_count": len(broker_open_orders),
        "blocking": False,
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    if not records:
        return {
            **base,
            "classification": "REGISTRY_RECONCILIATION_NOT_AVAILABLE",
            "detail": "No central trade registry records exist yet; legacy broker/lifecycle reconciliation remains diagnostic authority.",
            "mapped_trade_ids": [],
            "review_required_trade_ids": [],
        }

    active_records = [
        record
        for record in records
        if record.current_state
        in {
            TradeCurrentState.OPEN_MANAGED,
            TradeCurrentState.EXIT_DUE,
            TradeCurrentState.WORKING_EXIT,
            TradeCurrentState.WORKING_ENTRY,
        }
    ]
    active_scope = _scope_superseded_lifecycle_only_registry_records(
        config=config,
        records=records,
        active_records=active_records,
        broker_positions=broker_positions,
        broker_open_orders=broker_open_orders,
        lifecycle_positions=current_scope_lifecycle_positions,
    )
    active_records = [
        record
        for record in active_scope["current_scope_active_records"]
        if record.trade_id not in owner_superseded_trade_ids
    ]
    blockers: list[dict[str, Any]] = []
    mapped_trade_ids: set[str] = set()
    mapped_records: dict[str, dict[str, Any]] = {}

    for broker_position in broker_positions:
        owner_exposure = _owner_exposure_for_broker_position(
            owner_exposures=owner_exposures,
            broker_position=broker_position,
        )
        if owner_exposure is not None:
            owner_trade_id = str(owner_exposure.get("trade_id") or "").strip()
            owner_record = records_by_trade_id.get(owner_trade_id)
            if owner_record is not None:
                mapped_trade_ids.add(owner_record.trade_id)
                mapped_records[owner_record.trade_id] = _registry_record_event_row(owner_record)
                continue
        matches = _registry_records_for_broker_position(active_records, broker_position)
        if len(matches) == 1:
            mapped_trade_ids.add(matches[0].trade_id)
            mapped_records[matches[0].trade_id] = _registry_record_event_row(matches[0])
        elif len(matches) > 1:
            lifecycle_matches = _registry_lifecycle_records_for_broker_position_match(
                active_records=active_records,
                broker_position=broker_position,
                position_match_report=position_match_report,
            )
            direct_lifecycle_trade_ids = _lifecycle_trade_ids_for_broker_position(
                broker_position=broker_position,
                lifecycle_positions=current_scope_lifecycle_positions,
            )
            lifecycle_trade_ids = direct_lifecycle_trade_ids or {
                *_lifecycle_trade_ids_for_broker_position_match(
                    broker_position=broker_position,
                    position_match_report=position_match_report,
                ),
                *(record.trade_id for record in lifecycle_matches),
            }
            narrowed_matches = [record for record in matches if record.trade_id in lifecycle_trade_ids]
            if len(narrowed_matches) == 1:
                mapped_trade_ids.add(narrowed_matches[0].trade_id)
                mapped_records[narrowed_matches[0].trade_id] = _registry_record_event_row(narrowed_matches[0])
            elif not lifecycle_trade_ids:
                latest_entry_matches = _narrow_registry_matches_by_latest_broker_backed_entry(matches)
                if len(latest_entry_matches) == 1:
                    mapped_trade_ids.add(latest_entry_matches[0].trade_id)
                    mapped_records[latest_entry_matches[0].trade_id] = _registry_record_event_row(latest_entry_matches[0])
                    continue
                blockers.append(
                    {
                        "code": "REGISTRY_AMBIGUOUS_BROKER_POSITION",
                        "broker_position": dict(broker_position),
                        "matching_trade_ids": [record.trade_id for record in matches],
                        "lifecycle_matching_trade_ids": sorted(lifecycle_trade_ids),
                    }
                )
            else:
                blockers.append(
                    {
                        "code": "REGISTRY_AMBIGUOUS_BROKER_POSITION",
                        "broker_position": dict(broker_position),
                        "matching_trade_ids": [record.trade_id for record in matches],
                        "lifecycle_matching_trade_ids": sorted(lifecycle_trade_ids),
                    }
                )
        else:
            blockers.append(
                {
                    "code": "REGISTRY_ORPHAN_BROKER_POSITION_REVIEW_REQUIRED",
                    "broker_position": dict(broker_position),
                }
            )

    for lifecycle_position in current_scope_lifecycle_positions:
        lifecycle_trade_ids = _trade_ids_from_lifecycle_position(lifecycle_position)
        authoritative_trade_ids = lifecycle_trade_ids.intersection(owner_trade_ids)
        if len(authoritative_trade_ids) == 1:
            owner_record = records_by_trade_id.get(next(iter(authoritative_trade_ids)))
            if owner_record is not None:
                mapped_trade_ids.add(owner_record.trade_id)
                mapped_records[owner_record.trade_id] = _registry_record_event_row(owner_record)
                continue
        matches = _registry_records_for_lifecycle_position(active_records, lifecycle_position)
        if len(matches) == 1:
            mapped_trade_ids.add(matches[0].trade_id)
            mapped_records[matches[0].trade_id] = _registry_record_event_row(matches[0])
            if not broker_positions:
                blockers.append(
                    {
                        "code": "REGISTRY_LIFECYCLE_OPEN_WITHOUT_BROKER_POSITION_REVIEW_REQUIRED",
                        "lifecycle_position": dict(lifecycle_position),
                        "trade_id": matches[0].trade_id,
                    }
                )
        elif len(matches) > 1:
            direct_trade_ids = _trade_ids_from_lifecycle_position(lifecycle_position)
            narrowed_matches = [record for record in matches if record.trade_id in direct_trade_ids]
            if len(narrowed_matches) == 1:
                mapped_trade_ids.add(narrowed_matches[0].trade_id)
                mapped_records[narrowed_matches[0].trade_id] = _registry_record_event_row(narrowed_matches[0])
            else:
                blockers.append(
                    {
                        "code": "REGISTRY_AMBIGUOUS_LIFECYCLE_POSITION",
                        "lifecycle_position": dict(lifecycle_position),
                        "matching_trade_ids": [record.trade_id for record in matches],
                        "lifecycle_trade_ids": sorted(direct_trade_ids),
                    }
                )
        else:
            blockers.append(
                {
                    "code": "REGISTRY_LIFECYCLE_OPEN_WITHOUT_TRADE_ID_REVIEW_REQUIRED",
                    "lifecycle_position": dict(lifecycle_position),
                }
            )

    for match in position_match_report.get("matches") or []:
        if not isinstance(match, Mapping):
            continue
        broker_position = match.get("broker_position") if isinstance(match.get("broker_position"), Mapping) else {}
        lifecycle_position = match.get("lifecycle_position") if isinstance(match.get("lifecycle_position"), Mapping) else {}
        broker_trade_ids = {record.trade_id for record in _registry_records_for_broker_position(active_records, broker_position)}
        lifecycle_trade_ids = {record.trade_id for record in _registry_records_for_lifecycle_position(active_records, lifecycle_position)}
        if broker_trade_ids and lifecycle_trade_ids and not lifecycle_trade_ids.issubset(broker_trade_ids):
            blockers.append(
                {
                    "code": "REGISTRY_BROKER_LIFECYCLE_TRADE_ID_CONFLICT",
                    "broker_trade_ids": sorted(broker_trade_ids),
                    "lifecycle_trade_ids": sorted(lifecycle_trade_ids),
                    "match": dict(match),
                }
            )

    if not broker_positions and not current_scope_lifecycle_positions:
        stale_open_records = [
            record
            for record in active_records
            if record.current_state in {TradeCurrentState.OPEN_MANAGED, TradeCurrentState.EXIT_DUE, TradeCurrentState.WORKING_EXIT}
        ]
        if stale_open_records:
            blockers.append(
                {
                    "code": "REGISTRY_OPEN_TRADE_WITH_FLAT_BROKER_LIFECYCLE_REVIEW_REQUIRED",
                    "trade_ids": [record.trade_id for record in stale_open_records],
                }
            )
        for record in records:
            if record.current_state == TradeCurrentState.CLOSED_FLAT:
                mapped_trade_ids.add(record.trade_id)
                mapped_records[record.trade_id] = _registry_record_event_row(record)

    working_order_trade_ids = [
        trade_id
        for order in broker_open_orders
        for trade_id in [_trade_id_from_row(order)]
        if trade_id
    ]
    if broker_open_orders and working_order_trade_ids:
        mapped_trade_ids.update(working_order_trade_ids)

    if blockers:
        review_trade_ids = _review_trade_ids_from_registry_blockers(blockers)
        review_records = [
            _registry_record_event_row(record)
            for record in records
            if record.trade_id in set(review_trade_ids)
        ]
        return {
            **base,
            "classification": "REGISTRY_RECONCILIATION_REVIEW_REQUIRED",
            "detail": "Central trade registry could not map current broker/lifecycle truth to exactly one trade chain.",
            "blocking": True,
            "blockers": blockers,
            "mapped_trade_ids": sorted(mapped_trade_ids),
            "mapped_records": [mapped_records[key] for key in sorted(mapped_records)],
            "review_required_trade_ids": review_trade_ids,
            "review_records": review_records,
            "superseded_lifecycle_only_records": active_scope["superseded_lifecycle_only_records"],
            "superseded_unmatched_lifecycle_positions": [dict(row) for row in superseded_lifecycle_positions],
        }
    return {
        **base,
        "classification": "REGISTRY_RECONCILIATION_MATCHED",
        "detail": "Current broker/lifecycle truth maps cleanly to central trade registry state.",
        "blockers": [],
        "mapped_trade_ids": sorted(mapped_trade_ids),
        "mapped_records": [mapped_records[key] for key in sorted(mapped_records)],
        "review_required_trade_ids": [],
        "working_order_trade_ids": sorted(set(working_order_trade_ids)),
        "superseded_lifecycle_only_records": active_scope["superseded_lifecycle_only_records"],
        "superseded_unmatched_lifecycle_positions": [dict(row) for row in superseded_lifecycle_positions],
    }


def _closed_flat_lifecycle_projection_precedence(
    *,
    config: ReconciliationConfig,
    broker_positions: Sequence[Mapping[str, Any]],
    broker_open_orders: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Scope stale lifecycle-open rows after broker-backed registry flat close.

    Managed lifecycle reports can be regenerated from stale open-state inputs.
    Once the append-only registry has exact broker-backed close evidence and
    broker truth no longer shows the position/order, that stale projection must
    remain visible for audit but must not reopen current reconciliation.
    """

    if not lifecycle_positions:
        return {
            "classification": "NO_LIFECYCLE_PROJECTION_SUPERSESSION_NEEDED",
            "current_scope_lifecycle_positions": [],
            "superseded_lifecycle_projections": [],
        }

    records = load_live_trade_registry_records(repo_root=config.repo_root)
    current_scope: list[dict[str, Any]] = []
    superseded: list[dict[str, Any]] = []
    for lifecycle_position in lifecycle_positions:
        terminal_truth = resolve_terminal_registry_truth(
            records=records,
            identity=lifecycle_position,
            broker_positions=broker_positions,
            broker_open_orders=broker_open_orders,
        )
        if terminal_truth.terminal_closed_flat and terminal_truth.record is not None:
            closed_record = terminal_truth.record
            superseded.append(
                {
                    "classification": "STALE_SUPERSEDED_LIFECYCLE_PROJECTION",
                    "reason_codes": list(terminal_truth.reason_codes),
                    "trade_id": closed_record.trade_id,
                    "lifecycle_id": closed_record.ownership_identity.lifecycle_id
                    if closed_record.ownership_identity
                    else lifecycle_position.get("lifecycle_id"),
                    "registry_current_state": closed_record.current_state.value,
                    "broker_backed_exit": closed_record.broker_backed_exit,
                    "open_qty": str(closed_record.open_qty),
                    "lifecycle_position": dict(lifecycle_position),
                    "registry_record": _registry_record_event_row(closed_record),
                    "terminal_registry_truth": terminal_truth.to_dict(),
                }
            )
            continue
        current_scope.append(dict(lifecycle_position))

    classification = (
        "STALE_LIFECYCLE_PROJECTIONS_SUPERSEDED_BY_BROKER_BACKED_CLOSED_FLAT"
        if superseded
        else "NO_LIFECYCLE_PROJECTION_SUPERSESSION_NEEDED"
    )
    return {
        "classification": classification,
        "current_scope_lifecycle_positions": current_scope,
        "superseded_lifecycle_projections": superseded,
    }


def _scope_superseded_lifecycle_only_registry_records(
    *,
    config: ReconciliationConfig,
    records: Sequence[TradeRegistryRecord],
    active_records: Sequence[TradeRegistryRecord],
    broker_positions: Sequence[Mapping[str, Any]],
    broker_open_orders: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if broker_positions or broker_open_orders:
        return {
            "current_scope_active_records": list(active_records),
            "superseded_lifecycle_only_records": [],
        }

    terminal_records = [
        record
        for record in records
        if _registry_record_has_broker_backed_flat_exit(record)
    ]
    remediation_terminal_records = _load_evidence_gated_remediation_terminal_records(config=config)
    current_scope: list[TradeRegistryRecord] = []
    superseded: list[dict[str, Any]] = []
    for record in active_records:
        if not _registry_record_is_supersedable_open_projection(record):
            current_scope.append(record)
            continue
        if _record_linked_to_current_lifecycle_position(record, lifecycle_positions):
            current_scope.append(record)
            continue

        superseding_record = _superseding_broker_backed_flat_record(record, terminal_records)
        if superseding_record is not None:
            superseded.append(
                _superseded_lifecycle_only_record_payload(
                    record=record,
                    classification="DUPLICATE_SUPERSEDED_FULL_AUDIT_ONLY",
                    reason_codes=[
                        "DUPLICATE_LIFECYCLE_ONLY_CHAIN_SUPERSEDED_BY_BROKER_BACKED_REGISTRY_CHAIN",
                        "BROKER_BACKED_CLOSED_FLAT_REGISTRY_CHAIN_FOUND",
                        "BROKER_FLAT_PROOF_CONFIRMED",
                        "NO_OPEN_ORDER_PROOF_CONFIRMED",
                    ],
                    superseding_record=superseding_record,
                )
            )
            continue

        remediation_terminal = _evidence_gated_remediation_terminal_for_record(record, remediation_terminal_records)
        if remediation_terminal is not None:
            superseded.append(
                _superseded_lifecycle_only_record_payload(
                    record=record,
                    classification="BROKER_FLAT_EVIDENCE_GATED_CLEANUP_TERMINAL_FULL_AUDIT_ONLY",
                    reason_codes=[
                        "EVIDENCE_GATED_BROKER_FLAT_REMEDIATION_SUPERSEDES_OPEN_REGISTRY_ROW",
                        "BROKER_FLAT_PROOF_CONFIRMED",
                        "NO_OPEN_ORDER_PROOF_CONFIRMED",
                        "BROKER_BACKED_EXIT_EVIDENCE_NOT_CLAIMED",
                    ],
                    remediation_terminal=remediation_terminal,
                )
            )
            continue

        stale_source = _stale_derived_lifecycle_source_context(config=config, record=record)
        if stale_source is not None:
            superseded.append(
                _superseded_lifecycle_only_record_payload(
                    record=record,
                    classification="STALE_DERIVED_REGISTRY_CHAIN_FULL_AUDIT_ONLY",
                    reason_codes=[
                        "STALE_DERIVED_LIFECYCLE_REPORT_WITHOUT_CURRENT_BROKER_LINKAGE",
                        "BROKER_FLAT_PROOF_CONFIRMED",
                        "NO_OPEN_ORDER_PROOF_CONFIRMED",
                    ],
                    stale_source=stale_source,
                )
            )
            continue

        current_scope.append(record)

    return {
        "current_scope_active_records": current_scope,
        "superseded_lifecycle_only_records": superseded,
    }


def _registry_record_is_supersedable_open_projection(record: TradeRegistryRecord) -> bool:
    if record.current_state not in {
        TradeCurrentState.OPEN_MANAGED,
        TradeCurrentState.EXIT_DUE,
        TradeCurrentState.WORKING_EXIT,
    }:
        return False
    return record.broker_backed_exit is False


def _registry_record_has_broker_backed_flat_exit(record: TradeRegistryRecord) -> bool:
    return record.broker_backed_exit is True and record.open_qty == 0


def _record_linked_to_current_lifecycle_position(
    record: TradeRegistryRecord,
    lifecycle_positions: Sequence[Mapping[str, Any]],
) -> bool:
    for lifecycle_position in lifecycle_positions:
        if record.trade_id in _trade_ids_from_lifecycle_position(lifecycle_position):
            return True
        if not (_record_contract_matches_row(record, lifecycle_position) and _record_quantity_matches_lifecycle_position(record, lifecycle_position)):
            continue
        lifecycle_id = _valid_registry_identity_text(lifecycle_position.get("lifecycle_id"))
        owner_lifecycle_id = _valid_registry_identity_text(record.ownership_identity.lifecycle_id if record.ownership_identity else None)
        if lifecycle_id and owner_lifecycle_id and lifecycle_id == owner_lifecycle_id:
            return True
    return False


def _superseding_broker_backed_flat_record(
    record: TradeRegistryRecord,
    terminal_records: Sequence[TradeRegistryRecord],
) -> TradeRegistryRecord | None:
    matches = [
        candidate
        for candidate in terminal_records
        if candidate.trade_id != record.trade_id
        and _registry_records_share_contract_account(record, candidate)
        and _registry_records_share_entry_order_context(record, candidate)
    ]
    return matches[0] if len(matches) == 1 else None


def _load_evidence_gated_remediation_terminal_records(*, config: ReconciliationConfig) -> tuple[dict[str, Any], ...]:
    ledger_path = config.ledger_root / "track_b_paper_trade_ledger.jsonl"
    if not ledger_path.exists():
        return ()
    records: list[dict[str, Any]] = []
    try:
        lines = ledger_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ()
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, Mapping):
            continue
        if _ledger_record_has_evidence_gated_remediation_terminal(payload):
            records.append(dict(payload))
    return tuple(records)


def _ledger_record_has_evidence_gated_remediation_terminal(payload: Mapping[str, Any]) -> bool:
    if payload.get("source") != "BROKER_POSITION_GUARDIAN_SCOPED_REMEDIATION_FILLED_AND_BROKER_FLAT_TRUTH":
        return False
    if payload.get("broker_flat") is not True or payload.get("open_orders_zero") is not True:
        return False
    if payload.get("historical_broker_backed_exposure_confirmed") is not True:
        return False
    if not _valid_registry_identity_text(payload.get("lifecycle_id")):
        return False
    if not _valid_registry_identity_text(payload.get("account_id")):
        return False
    if not _valid_registry_identity_text(payload.get("local_symbol")):
        return False
    if _int_or_none(payload.get("con_id")) is None:
        return False
    if not _valid_registry_identity_text(payload.get("broker_positions_snapshot_path")):
        return False
    if not _valid_registry_identity_text(payload.get("broker_open_orders_snapshot_path")):
        return False
    return all(
        _valid_registry_identity_text(payload.get(key))
        for key in (
            "remediation_broker_order_id",
            "remediation_perm_id",
            "remediation_execution_id",
            "remediation_fill_time",
        )
    )


def _evidence_gated_remediation_terminal_for_record(
    record: TradeRegistryRecord,
    remediation_records: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    matches = [
        remediation
        for remediation in remediation_records
        if _remediation_record_matches_registry_record(record=record, remediation=remediation)
    ]
    return matches[0] if len(matches) == 1 else None


def _remediation_record_matches_registry_record(
    *,
    record: TradeRegistryRecord,
    remediation: Mapping[str, Any],
) -> bool:
    owner = record.ownership_identity
    if owner is None:
        return False
    remediation_trade_id = _valid_registry_identity_text(remediation.get("trade_id"))
    if remediation_trade_id:
        base_trade_id = remediation_trade_id.split(":", 1)[0]
        if base_trade_id != record.trade_id:
            return False
    lifecycle_id = _valid_registry_identity_text(remediation.get("lifecycle_id"))
    if owner.lifecycle_id and lifecycle_id and lifecycle_id != owner.lifecycle_id:
        return False
    return (
        _valid_registry_identity_text(remediation.get("account_id")) == owner.account_id
        and _int_or_none(remediation.get("con_id")) == owner.con_id
        and _valid_registry_identity_text(remediation.get("local_symbol")).upper() == owner.local_symbol.upper()
    )


def _registry_records_share_contract_account(left: TradeRegistryRecord, right: TradeRegistryRecord) -> bool:
    left_owner = left.ownership_identity
    right_owner = right.ownership_identity
    if left_owner is None or right_owner is None:
        return False
    return (
        left_owner.account_id == right_owner.account_id
        and left_owner.con_id == right_owner.con_id
        and left_owner.local_symbol.upper() == right_owner.local_symbol.upper()
        and left_owner.symbol.upper() == right_owner.symbol.upper()
    )


def _registry_records_share_entry_order_context(left: TradeRegistryRecord, right: TradeRegistryRecord) -> bool:
    left_context = _entry_order_context_values(left)
    right_context = _entry_order_context_values(right)
    for key in ("perm_id", "client_id", "order_id"):
        left_value = left_context.get(key)
        right_value = right_context.get(key)
        if left_value and right_value and left_value != right_value:
            return False
    return bool(
        (left_context.get("perm_id") and right_context.get("perm_id"))
        or (
            left_context.get("client_id")
            and right_context.get("client_id")
            and left_context.get("order_id")
            and right_context.get("order_id")
        )
    )


def _entry_order_context_values(record: TradeRegistryRecord) -> dict[str, str]:
    values = {"perm_id": "", "client_id": "", "order_id": ""}
    for event in record.event_chain:
        if event.event_type not in {
            TradeEventType.ENTRY_FILL_BROKER_BACKED,
            TradeEventType.RECOVERY_ADOPTION_RECORDED,
            TradeEventType.LIFECYCLE_OPEN_MANAGED,
            TradeEventType.ENTRY_ORDER_SUBMITTED,
        }:
            continue
        if event.perm_id and not values["perm_id"]:
            values["perm_id"] = str(event.perm_id)
        if event.client_id and not values["client_id"]:
            values["client_id"] = str(event.client_id)
        if event.order_id and not values["order_id"]:
            values["order_id"] = str(event.order_id)
    return values


def _stale_derived_lifecycle_source_context(
    *,
    config: ReconciliationConfig,
    record: TradeRegistryRecord,
) -> dict[str, Any] | None:
    reports: list[dict[str, Any]] = []
    source_paths = sorted({event.source_artifact_path for event in record.event_chain if event.source_artifact_path})
    for path_text in source_paths:
        path = Path(path_text)
        if not path.is_absolute():
            path = config.repo_root / path
        if not path.exists() or path.suffix != ".json":
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        source_generated_at = _parse_datetime_value(payload.get("generated_at"))
        entry_fill = payload.get("entry_fill") if isinstance(payload.get("entry_fill"), Mapping) else {}
        close_fill = payload.get("close_fill") if isinstance(payload.get("close_fill"), Mapping) else {}
        entry_filled_at = _parse_datetime_value(entry_fill.get("filled_at"))
        close_filled_at = _parse_datetime_value(close_fill.get("filled_at"))
        impossible_timestamps = (
            source_generated_at is not None
            and (
                (entry_filled_at is not None and source_generated_at < entry_filled_at)
                or (close_filled_at is not None and source_generated_at < close_filled_at)
            )
        )
        generated_at_values = [event.generated_at for event in record.event_chain]
        stale_reemitted_report = (
            source_generated_at is not None
            and bool(generated_at_values)
            and max(generated_at_values) - source_generated_at > timedelta(hours=24)
        )
        if impossible_timestamps or stale_reemitted_report:
            reports.append(
                {
                    "source_artifact_path": str(path),
                    "source_generated_at": source_generated_at.isoformat() if source_generated_at else None,
                    "entry_filled_at": entry_filled_at.isoformat() if entry_filled_at else None,
                    "close_filled_at": close_filled_at.isoformat() if close_filled_at else None,
                    "impossible_timestamps": impossible_timestamps,
                    "stale_reemitted_report": stale_reemitted_report,
                }
            )
    if not reports:
        return None
    return {"source_reports": reports}


def _parse_datetime_value(value: object) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _superseded_lifecycle_only_record_payload(
    *,
    record: TradeRegistryRecord,
    classification: str,
    reason_codes: Sequence[str],
    superseding_record: TradeRegistryRecord | None = None,
    stale_source: Mapping[str, Any] | None = None,
    remediation_terminal: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "classification": classification,
        "reason_codes": list(reason_codes),
        "trade_id": record.trade_id,
        "lifecycle_id": record.ownership_identity.lifecycle_id if record.ownership_identity else None,
        "current_state": record.current_state.value,
        "broker_backed_entry": record.broker_backed_entry,
        "broker_backed_exit": record.broker_backed_exit,
        "open_qty": str(record.open_qty),
        "registry_record": _registry_record_event_row(record),
    }
    if superseding_record is not None:
        payload["superseding_trade_id"] = superseding_record.trade_id
        payload["superseding_record"] = _registry_record_event_row(superseding_record)
    if stale_source is not None:
        payload["stale_source"] = dict(stale_source)
    if remediation_terminal is not None:
        payload["remediation_terminal"] = dict(remediation_terminal)
    return payload


def _closed_flat_record_for_lifecycle_projection(
    records: Sequence[TradeRegistryRecord],
    lifecycle_position: Mapping[str, Any],
) -> TradeRegistryRecord | None:
    exact_matches = _registry_records_for_lifecycle_position(records, lifecycle_position)
    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(exact_matches) > 1:
        return None
    direct_trade_ids = _trade_ids_from_lifecycle_position(lifecycle_position)
    if direct_trade_ids:
        direct_matches = [record for record in records if record.trade_id in direct_trade_ids]
        if len(direct_matches) == 1:
            return direct_matches[0]
        return None
    contract_matches = [
        record
        for record in records
        if _record_contract_matches_row(record, lifecycle_position)
        and _record_account_matches_row(record, lifecycle_position)
        and _record_quantity_matches_lifecycle_position(record, lifecycle_position)
    ]
    return contract_matches[0] if len(contract_matches) == 1 else None


def _append_reconciliation_registry_events(
    *,
    config: ReconciliationConfig,
    report: Mapping[str, Any],
    registry_reconciliation: Mapping[str, Any],
    broker_backed_entry_adoption: Mapping[str, Any] | None,
    now: datetime,
) -> None:
    classification = str(report.get("classification") or "")
    registry_classification = str(registry_reconciliation.get("classification") or "")
    if "RECONCILED" in classification and report.get("broker_reconciled") is True:
        event_type = TradeEventType.RECONCILED_OPEN if report.get("track_b_broker_position_count") else TradeEventType.RECONCILED_FLAT
        reason = "BROKER_LIFECYCLE_RECONCILED"
    else:
        event_type = TradeEventType.REVIEW_REQUIRED
        reason = registry_classification or classification or "BROKER_LIFECYCLE_RECONCILIATION_REVIEW_REQUIRED"

    candidates = [
        row
        for bucket in (
            report.get("track_b_lifecycle_positions"),
            report.get("track_b_broker_positions"),
            report.get("unresolved_submit_intent_ownership_records"),
        )
        for row in list(bucket or [])
        if isinstance(row, Mapping)
    ]
    candidate_by_trade_id: dict[str, Mapping[str, Any]] = {}
    recovery_trade_ids = _recovery_adoption_trade_ids(broker_backed_entry_adoption)
    for row in candidates:
        trade_id = _trade_id_from_row(row)
        if trade_id:
            candidate_by_trade_id.setdefault(trade_id, row)
    for row in list(registry_reconciliation.get("mapped_records") or []):
        if isinstance(row, Mapping):
            trade_id = _trade_id_from_row(row)
            if trade_id:
                candidate_by_trade_id.setdefault(trade_id, row)
    for row in list(registry_reconciliation.get("review_records") or []):
        if isinstance(row, Mapping):
            trade_id = _trade_id_from_row(row)
            if trade_id:
                candidate_by_trade_id.setdefault(trade_id, row)
    for blocker in list(registry_reconciliation.get("blockers") or []):
        if not isinstance(blocker, Mapping):
            continue
        for row_key in ("broker_position", "lifecycle_position"):
            row = blocker.get(row_key)
            if isinstance(row, Mapping):
                trade_id = _trade_id_from_row(row)
                if not trade_id:
                    trade_id = trade_id_from_live_identity(
                        account_id=row.get("account_id") or row.get("account") or config.account,
                        con_id=row.get("con_id") or row.get("conId"),
                        lane_id=row.get("lane_id") or row.get("strategy_id") or row.get("track_b_root") or "review",
                    )
                if trade_id:
                    candidate_by_trade_id.setdefault(trade_id, row)
    for row in candidate_by_trade_id.values():
        extra = row.get("extra") if isinstance(row.get("extra"), Mapping) else {}
        trade_id = _trade_id_from_row(row)
        lifecycle_id = str(row.get("lifecycle_id") or "").strip()
        if not trade_id:
            trade_id = trade_id_from_live_identity(
                lifecycle_id=lifecycle_id,
                account_id=row.get("account_id") or row.get("account") or config.account,
                con_id=row.get("con_id") or row.get("conId"),
                lane_id=row.get("lane_id") or row.get("strategy_id") or row.get("track_b_root") or "review",
            )
        if not trade_id:
            continue
        if event_type == TradeEventType.REVIEW_REQUIRED and trade_id in recovery_trade_ids:
            continue
        try:
            event = make_live_trade_registry_event(
                event_type=event_type,
                generated_at=now,
                trade_id=trade_id,
                lifecycle_id=lifecycle_id or None,
                lane_id=str(row.get("lane_id") or row.get("strategy_id") or "UNKNOWN").strip(),
                thesis_strategy_id=str(row.get("strategy_id") or row.get("lane_id") or "UNKNOWN").strip(),
                account_id=str(row.get("account_id") or row.get("account") or config.account).strip(),
                symbol=str(row.get("symbol") or row.get("instrument_family") or row.get("root_symbol") or "UNKNOWN").strip().upper(),
                con_id=row.get("con_id") or row.get("conId") or 1,
                local_symbol=str(row.get("local_symbol") or row.get("localSymbol") or "UNKNOWN").strip(),
                expiry=str(row.get("expiry") or row.get("contract_month") or "UNKNOWN").strip(),
                side=str(row.get("side") or row.get("position_side") or row.get("action") or "UNKNOWN").strip().upper(),
                action=str(row.get("action") or row.get("order_action") or "RECONCILE").strip().upper(),
                qty=row.get("qty") or row.get("quantity") or row.get("position_qty") or 1,
                source_artifact_path=str(config.report_path),
                reason_codes=(reason,),
                metadata={
                    "source": "track_b_paper_broker_reconciliation",
                    "classification": classification,
                    "registry_reconciliation_classification": registry_classification,
                    "broker_reconciled": report.get("broker_reconciled") is True,
                    "paper_only": True,
                    "live_money_eligible": False,
                    "paper_proof_invoked": False,
                },
            )
            append_live_trade_registry_event(repo_root=config.repo_root, event=event)
        except Exception:
            continue
    _append_recovery_adoption_registry_events(
        config=config,
        broker_backed_entry_adoption=broker_backed_entry_adoption,
        now=now,
    )


def _narrow_registry_matches_by_latest_broker_backed_entry(
    records: Sequence[TradeRegistryRecord],
) -> list[TradeRegistryRecord]:
    """Resolve same-contract registry ambiguity only when one entry is newest.

    A current broker position may have no lifecycle projection yet during
    post-fill adoption.  In that gap, stale historical OPEN_MANAGED chains can
    match by account/contract/side/qty.  The only safe registry-side
    discriminator available is exact broker-backed entry evidence recency.
    Equal newest timestamps or missing broker-backed entry evidence still fail
    closed as ambiguity.
    """

    candidates: list[tuple[datetime, tuple[str, str, str, str], TradeRegistryRecord]] = []
    for record in records:
        latest_entry = _latest_broker_backed_entry_event(record)
        if latest_entry is None:
            continue
        identity = (
            str(latest_entry.order_id or ""),
            str(latest_entry.client_id or ""),
            str(latest_entry.perm_id or ""),
            str(latest_entry.exec_id or ""),
        )
        if not all(identity):
            continue
        candidates.append((latest_entry.generated_at, identity, record))
    if not candidates:
        return []
    newest = max(item[0] for item in candidates)
    newest_candidates = [item for item in candidates if item[0] == newest]
    newest_identities = {item[1] for item in newest_candidates}
    if len(newest_candidates) == 1 and len(newest_identities) == 1:
        return [newest_candidates[0][2]]
    return []


def _latest_broker_backed_entry_event(record: TradeRegistryRecord) -> TradeEvent | None:
    events = [
        event
        for event in record.event_chain
        if event.event_type == TradeEventType.ENTRY_FILL_BROKER_BACKED
        and event.order_id
        and event.client_id
        and event.perm_id
        and event.exec_id
    ]
    if not events:
        return None
    return max(events, key=lambda event: event.generated_at)


def _registry_records_for_broker_position(
    records: Sequence[TradeRegistryRecord],
    broker_position: Mapping[str, Any],
) -> list[TradeRegistryRecord]:
    canonical = canonicalize_broker_position_identity(
        broker_position=broker_position,
        registry_records=records,
    )
    if canonical.classification != IDENTITY_READY:
        return []
    canonical_position = canonical.canonical_position
    return [
        record
        for record in records
        if record.ownership_identity is not None
        and _record_contract_matches_row(record, canonical_position)
        and _record_account_matches_row(record, canonical_position)
        and _record_quantity_matches_broker_position(record, canonical_position)
    ]


def _registry_record_event_row(record: TradeRegistryRecord) -> dict[str, Any]:
    owner = record.ownership_identity
    if owner is None:
        return {"trade_id": record.trade_id}
    return {
        "trade_id": record.trade_id,
        "lifecycle_id": owner.lifecycle_id,
        "lane_id": owner.lane_id,
        "strategy_id": owner.thesis_strategy_id,
        "account_id": owner.account_id,
        "symbol": owner.symbol,
        "instrument_family": owner.symbol,
        "con_id": owner.con_id,
        "local_symbol": owner.local_symbol,
        "expiry": owner.expiry,
        "side": owner.side,
        "quantity": str(owner.qty),
        "action": _entry_action_for_side(owner.side),
        "current_state": record.current_state.value,
        "entry_order_id": _registry_record_latest_event_value(record, "order_id"),
        "entry_client_id": _registry_record_latest_event_value(record, "client_id"),
        "entry_perm_id": _registry_record_latest_event_value(record, "perm_id"),
        "entry_exec_id": _registry_record_latest_event_value(record, "exec_id"),
    }


def _registry_records_for_lifecycle_position(
    records: Sequence[TradeRegistryRecord],
    lifecycle_position: Mapping[str, Any],
) -> list[TradeRegistryRecord]:
    lifecycle_id = str(lifecycle_position.get("lifecycle_id") or "").strip()
    if lifecycle_id:
        exact = [
            record
            for record in records
            if record.ownership_identity is not None and record.ownership_identity.lifecycle_id == lifecycle_id
        ]
        return exact
    return [
        record
        for record in records
        if record.ownership_identity is not None
        and _record_contract_matches_row(record, lifecycle_position)
        and _record_account_matches_row(record, lifecycle_position)
        and _record_quantity_matches_lifecycle_position(record, lifecycle_position)
    ]


def _registry_lifecycle_records_for_broker_position_match(
    *,
    active_records: Sequence[TradeRegistryRecord],
    broker_position: Mapping[str, Any],
    position_match_report: Mapping[str, Any],
) -> list[TradeRegistryRecord]:
    records_by_trade_id: dict[str, TradeRegistryRecord] = {}
    for match in position_match_report.get("matches") or []:
        if not isinstance(match, Mapping):
            continue
        matched_broker = match.get("broker_position") if isinstance(match.get("broker_position"), Mapping) else {}
        if not _broker_position_rows_equivalent(broker_position, matched_broker):
            continue
        lifecycle_position = match.get("lifecycle_position") if isinstance(match.get("lifecycle_position"), Mapping) else {}
        for record in _registry_records_for_lifecycle_position(active_records, lifecycle_position):
            records_by_trade_id.setdefault(record.trade_id, record)
    return list(records_by_trade_id.values())


def _owner_exposure_for_broker_position(
    *,
    owner_exposures: Sequence[Mapping[str, Any]],
    broker_position: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    matches: list[Mapping[str, Any]] = []
    for exposure in owner_exposures:
        owner_position = exposure.get("canonical_broker_position")
        if not isinstance(owner_position, Mapping):
            owner_position = exposure.get("broker_position")
        if isinstance(owner_position, Mapping) and _broker_position_rows_equivalent(owner_position, broker_position):
            matches.append(exposure)
    return matches[0] if len(matches) == 1 else None


def _owner_exposure_has_lifecycle_report_authority(owner_exposure: Mapping[str, Any]) -> bool:
    reason_codes = {str(code or "") for code in owner_exposure.get("reason_codes") or []}
    if "NEWEST_EXACT_BROKER_BACKED_LIFECYCLE_REPORT_SELECTED" in reason_codes:
        return True
    lifecycle_position = owner_exposure.get("lifecycle_position")
    if not isinstance(lifecycle_position, Mapping):
        return False
    return str(lifecycle_position.get("projection_repair") or "") == "BROKER_BACKED_LIFECYCLE_REPORT_SUBMIT_INTENT_OWNER"


def _lifecycle_trade_ids_for_broker_position_match(
    *,
    broker_position: Mapping[str, Any],
    position_match_report: Mapping[str, Any],
) -> set[str]:
    trade_ids: set[str] = set()
    for match in position_match_report.get("matches") or []:
        if not isinstance(match, Mapping):
            continue
        matched_broker = match.get("broker_position") if isinstance(match.get("broker_position"), Mapping) else {}
        if not _broker_position_rows_equivalent(broker_position, matched_broker):
            continue
        lifecycle_position = match.get("lifecycle_position") if isinstance(match.get("lifecycle_position"), Mapping) else {}
        trade_id = str(lifecycle_position.get("trade_id") or "").strip()
        if trade_id:
            trade_ids.add(trade_id)
        trade_ids.update(str(item).strip() for item in lifecycle_position.get("trade_ids") or [] if str(item or "").strip())
    return trade_ids


def _lifecycle_trade_ids_for_broker_position(
    *,
    broker_position: Mapping[str, Any],
    lifecycle_positions: Sequence[Mapping[str, Any]],
) -> set[str]:
    trade_ids: set[str] = set()
    for lifecycle_position in lifecycle_positions:
        if not _lifecycle_position_matches_broker_position(lifecycle_position, broker_position):
            continue
        trade_ids.update(_trade_ids_from_lifecycle_position(lifecycle_position))
    return trade_ids


def _trade_ids_from_lifecycle_position(lifecycle_position: Mapping[str, Any]) -> set[str]:
    trade_ids: set[str] = set()
    trade_id = str(lifecycle_position.get("trade_id") or "").strip()
    if trade_id:
        trade_ids.add(trade_id)
    trade_ids.update(str(item).strip() for item in lifecycle_position.get("trade_ids") or [] if str(item or "").strip())
    return trade_ids


def _lifecycle_position_matches_broker_position(
    lifecycle_position: Mapping[str, Any],
    broker_position: Mapping[str, Any],
) -> bool:
    broker_con_id = _int_or_none(broker_position.get("con_id") or broker_position.get("conId"))
    lifecycle_con_id = _int_or_none(lifecycle_position.get("con_id") or lifecycle_position.get("conId"))
    if broker_con_id is not None and lifecycle_con_id is not None and broker_con_id != lifecycle_con_id:
        return False
    broker_local = str(broker_position.get("local_symbol") or broker_position.get("localSymbol") or "").strip().upper()
    lifecycle_local = str(lifecycle_position.get("local_symbol") or lifecycle_position.get("localSymbol") or "").strip().upper()
    if broker_local and lifecycle_local and broker_local != lifecycle_local:
        return False
    broker_symbol = str(broker_position.get("track_b_root") or broker_position.get("symbol") or "").strip().upper()
    lifecycle_symbol = str(lifecycle_position.get("track_b_root") or lifecycle_position.get("instrument_family") or lifecycle_position.get("symbol") or "").strip().upper()
    if broker_symbol and lifecycle_symbol and broker_symbol != lifecycle_symbol:
        return False
    return bool(broker_con_id is not None or lifecycle_con_id is not None or broker_local or lifecycle_local or broker_symbol or lifecycle_symbol)


def _broker_position_rows_equivalent(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_account = _valid_registry_identity_text(left.get("account_id") or left.get("account"))
    right_account = _valid_registry_identity_text(right.get("account_id") or right.get("account"))
    if left_account and right_account and left_account != right_account:
        return False
    left_con_id = _int_or_none(left.get("con_id") or left.get("conId"))
    right_con_id = _int_or_none(right.get("con_id") or right.get("conId"))
    if left_con_id is not None and right_con_id is not None and left_con_id != right_con_id:
        return False
    left_local = str(left.get("local_symbol") or left.get("localSymbol") or "").strip().upper()
    right_local = str(right.get("local_symbol") or right.get("localSymbol") or "").strip().upper()
    if left_local and right_local and left_local != right_local:
        return False
    left_symbol = str(left.get("track_b_root") or left.get("symbol") or left.get("instrument_family") or "").strip().upper()
    right_symbol = str(right.get("track_b_root") or right.get("symbol") or right.get("instrument_family") or "").strip().upper()
    if left_symbol and right_symbol and left_symbol != right_symbol:
        return False
    left_qty = _decimal_value(left.get("quantity"))
    right_qty = _decimal_value(right.get("quantity"))
    if left_qty is not None and right_qty is not None and left_qty != right_qty:
        return False
    return bool(left_con_id is not None or right_con_id is not None or left_local or right_local or left_symbol or right_symbol)


def _record_contract_matches_row(record: TradeRegistryRecord, row: Mapping[str, Any]) -> bool:
    owner = record.ownership_identity
    if owner is None:
        return False
    row_con_id = _int_or_none(row.get("con_id") or row.get("conId"))
    if row_con_id is not None and int(owner.con_id) != int(row_con_id):
        return False
    row_local = str(row.get("local_symbol") or row.get("localSymbol") or "").strip().upper()
    if row_local and owner.local_symbol.upper() != row_local:
        return False
    row_symbol = str(row.get("track_b_root") or row.get("symbol") or row.get("instrument_family") or "").strip().upper()
    if row_symbol and owner.symbol.upper() != row_symbol:
        return False
    return bool(row_con_id is not None or row_local or row_symbol)


def _record_account_matches_row(record: TradeRegistryRecord, row: Mapping[str, Any]) -> bool:
    owner = record.ownership_identity
    if owner is None:
        return False
    row_account = _valid_registry_identity_text(row.get("account_id") or row.get("account"))
    return not row_account or row_account == owner.account_id


def _record_quantity_matches_broker_position(record: TradeRegistryRecord, broker_position: Mapping[str, Any]) -> bool:
    owner = record.ownership_identity
    broker_qty = _decimal_value(broker_position.get("quantity"))
    if owner is None or broker_qty is None:
        return False
    expected_qty = owner.qty if owner.side.upper() == "LONG" else -owner.qty
    return broker_qty == expected_qty


def _record_quantity_matches_lifecycle_position(record: TradeRegistryRecord, lifecycle_position: Mapping[str, Any]) -> bool:
    owner = record.ownership_identity
    lifecycle_qty = _decimal_value(lifecycle_position.get("quantity"))
    if owner is None or lifecycle_qty is None:
        return False
    return abs(lifecycle_qty) == abs(owner.qty)


def _valid_registry_identity_text(value: object) -> str:
    text = str(value or "").strip()
    if text.upper() in {"", "MULTIPLE", "MISSING", "UNKNOWN", "NONE", "NULL"}:
        return ""
    return text


def _trade_id_from_row(row: Mapping[str, Any]) -> str:
    extra = row.get("extra") if isinstance(row.get("extra"), Mapping) else {}
    return str(row.get("trade_id") or extra.get("trade_id") or "").strip()


def _review_trade_ids_from_registry_blockers(blockers: Sequence[Mapping[str, Any]]) -> list[str]:
    trade_ids: list[str] = []
    for blocker in blockers:
        if str(blocker.get("trade_id") or "").strip():
            trade_ids.append(str(blocker.get("trade_id")))
        for key in ("matching_trade_ids", "trade_ids", "broker_trade_ids", "lifecycle_trade_ids"):
            value = blocker.get(key)
            if isinstance(value, list):
                trade_ids.extend(str(item) for item in value if str(item or "").strip())
    return sorted(set(trade_ids))


def _append_recovery_adoption_registry_events(
    *,
    config: ReconciliationConfig,
    broker_backed_entry_adoption: Mapping[str, Any] | None,
    now: datetime,
) -> None:
    if not isinstance(broker_backed_entry_adoption, Mapping):
        return
    classification = str(broker_backed_entry_adoption.get("classification") or "")
    adoption_rows = [
        row
        for row in broker_backed_entry_adoption.get("adoptions") or []
        if isinstance(row, Mapping)
    ]
    if not adoption_rows and broker_backed_entry_adoption.get("trade_id"):
        adoption_rows = [broker_backed_entry_adoption]
    for row in adoption_rows:
        event_type = (
            TradeEventType.RECOVERY_ADOPTION_RECORDED
            if row.get("adoption_allowed") is True and row.get("broker_backed_evidence_valid") is True
            else TradeEventType.REVIEW_REQUIRED
        )
        reason = (
            "RECOVERY_ADOPTION_REGISTRY_BACKED"
            if event_type == TradeEventType.RECOVERY_ADOPTION_RECORDED
            else "RECOVERY_ADOPTION_REVIEW_REQUIRED"
        )
        contract = row.get("contract") if isinstance(row.get("contract"), Mapping) else {}
        try:
            if event_type == TradeEventType.RECOVERY_ADOPTION_RECORDED:
                resolver_context = row.get("fill_evidence_resolver") if isinstance(row.get("fill_evidence_resolver"), Mapping) else {}
                append_live_trade_registry_event(
                    repo_root=config.repo_root,
                    event=make_live_trade_registry_event(
                        event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
                        generated_at=now,
                        trade_id=str(row.get("trade_id") or "").strip(),
                        lifecycle_id=str(row.get("lifecycle_id") or row.get("ownership_intent_id") or "").strip() or None,
                        lane_id=str(row.get("lane_id") or row.get("strategy_id") or "UNKNOWN").strip(),
                        thesis_strategy_id=str(row.get("strategy_id") or row.get("lane_id") or "UNKNOWN").strip(),
                        account_id=str(row.get("account_id") or config.account).strip(),
                        symbol=str(contract.get("symbol") or row.get("symbol") or "UNKNOWN").strip().upper(),
                        con_id=contract.get("con_id") or row.get("con_id") or 1,
                        local_symbol=str(contract.get("local_symbol") or row.get("local_symbol") or "UNKNOWN").strip(),
                        expiry=str(contract.get("expiry") or row.get("expiry") or "UNKNOWN").strip(),
                        side=str(row.get("side") or _side_for_entry_action(row.get("action")) or "LONG").strip().upper(),
                        action=str(row.get("action") or "BUY").strip().upper().replace("_TO_OPEN", ""),
                        qty=row.get("qty") or row.get("quantity") or 1,
                        order_id=row.get("broker_order_id") or row.get("order_id"),
                        client_id=row.get("client_id"),
                        perm_id=row.get("perm_id"),
                        exec_id=row.get("exec_id"),
                        price=row.get("fill_price"),
                        source_artifact_path=str(resolver_context.get("selected_source_artifact_path") or config.report_path),
                        reason_codes=("BROKER_BACKED_FILL_EVIDENCE_RESOLVED_FOR_ADOPTION",),
                        metadata={
                            "source": "track_b_paper_broker_reconciliation_fill_evidence_resolver",
                            "classification": classification,
                            "paper_only": True,
                            "live_money_eligible": False,
                            "paper_proof_invoked": False,
                        },
                    ),
                )
            append_live_trade_registry_event(
                repo_root=config.repo_root,
                event=make_live_trade_registry_event(
                    event_type=event_type,
                    generated_at=now,
                    trade_id=str(row.get("trade_id") or "").strip(),
                    lifecycle_id=str(row.get("lifecycle_id") or row.get("ownership_intent_id") or "").strip() or None,
                    lane_id=str(row.get("lane_id") or row.get("strategy_id") or "UNKNOWN").strip(),
                    thesis_strategy_id=str(row.get("strategy_id") or row.get("lane_id") or "UNKNOWN").strip(),
                    account_id=str(row.get("account_id") or config.account).strip(),
                    symbol=str(contract.get("symbol") or row.get("symbol") or "UNKNOWN").strip().upper(),
                    con_id=contract.get("con_id") or row.get("con_id") or 1,
                    local_symbol=str(contract.get("local_symbol") or row.get("local_symbol") or "UNKNOWN").strip(),
                    expiry=str(contract.get("expiry") or row.get("expiry") or "UNKNOWN").strip(),
                    side=str(row.get("side") or _side_for_entry_action(row.get("action")) or "LONG").strip().upper(),
                    action=str(row.get("action") or "BUY").strip().upper().replace("_TO_OPEN", ""),
                    qty=row.get("qty") or row.get("quantity") or 1,
                    order_id=row.get("broker_order_id") or row.get("order_id"),
                    client_id=row.get("client_id"),
                    perm_id=row.get("perm_id"),
                    exec_id=row.get("exec_id"),
                    source_artifact_path=str(config.report_path),
                    reason_codes=(reason,),
                    metadata={
                        "source": "track_b_paper_broker_reconciliation_recovery_adoption",
                        "classification": classification,
                        "paper_only": True,
                        "live_money_eligible": False,
                        "paper_proof_invoked": False,
                    },
                ),
            )
        except Exception:
            continue


def _apply_post_fill_lifecycle_adoption_invariant(
    *,
    config: ReconciliationConfig,
    broker_backed_entry_adoption: Mapping[str, Any] | None,
    now: datetime,
) -> dict[str, Any] | None:
    from mgc_v05l.execution_core.track_b_broker_backed_entry_auto_adoption import (
        auto_adopt_broker_backed_entry,
    )

    if not isinstance(broker_backed_entry_adoption, Mapping):
        return None
    adoption_rows = [
        row
        for row in broker_backed_entry_adoption.get("adoptions") or []
        if isinstance(row, Mapping)
    ]
    if not adoption_rows and broker_backed_entry_adoption.get("trade_id"):
        adoption_rows = [broker_backed_entry_adoption]
    if not adoption_rows:
        return None
    results: list[dict[str, Any]] = []
    for row in adoption_rows:
        if row.get("adoption_allowed") is not True or row.get("broker_backed_evidence_valid") is not True:
            results.append(
                {
                    "classification": "REVIEW_REQUIRED_UNMANAGED_BROKER_EXPOSURE",
                    "trade_id": row.get("trade_id"),
                    "lifecycle_id": row.get("lifecycle_id"),
                    "reason_codes": list((row.get("fill_evidence_resolver") or {}).get("reason_codes") or ()),
                    "fill_evidence_resolver": row.get("fill_evidence_resolver"),
                }
            )
            continue
        payload = _auto_adoption_payload_from_remediation(row)
        result = auto_adopt_broker_backed_entry(
            entry_fill_evidence=payload,
            evidence_path=Path(str((row.get("fill_evidence_resolver") or {}).get("selected_source_artifact_path") or config.report_path)),
            paper_trade_ledger_output_root=config.ledger_root,
            now=now,
        )
        if result.open_managed:
            _append_recovery_adoption_registry_events(
                config=config,
                broker_backed_entry_adoption={"classification": "POST_FILL_LIFECYCLE_ADOPTION_APPLIED", "adoptions": [row]},
                now=now,
            )
        results.append(
            {
                "classification": result.classification,
                "transition_classification": result.transition_classification,
                "trade_id": row.get("trade_id"),
                "lifecycle_id": row.get("lifecycle_id"),
                "open_managed": result.open_managed,
                "lifecycle_report_path": str(result.lifecycle_report_path) if result.lifecycle_report_path else None,
                "manifest_path": str(result.manifest_path) if result.manifest_path else None,
                "blockers": list(result.blockers),
            }
        )
    return {
        "classification": "POST_FILL_LIFECYCLE_ADOPTION_APPLIED"
        if results and all(item.get("open_managed") is True for item in results)
        else "POST_FILL_LIFECYCLE_ADOPTION_REVIEW_REQUIRED",
        "adoption_count": len(results),
        "results": results,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def _auto_adoption_payload_from_remediation(row: Mapping[str, Any]) -> dict[str, Any]:
    contract = row.get("contract") if isinstance(row.get("contract"), Mapping) else {}
    action = str(row.get("action") or "").strip().upper()
    intent_type = "SELL_TO_OPEN" if action == "SELL" else "BUY_TO_OPEN"
    symbol = str(contract.get("symbol") or row.get("symbol") or "").strip().upper()
    expiry = str(contract.get("expiry") or row.get("expiry") or "").strip()
    return {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "strategy_id": row.get("strategy_id") or row.get("lane_id"),
        "lane_id": row.get("lane_id") or row.get("strategy_id"),
        "instrument": symbol,
        "symbol": symbol,
        "action": action or ("SELL" if intent_type == "SELL_TO_OPEN" else "BUY"),
        "quantity": row.get("qty") or row.get("quantity") or 1,
        "order_intent_id": row.get("order_intent_id") or row.get("ownership_intent_id") or row.get("lifecycle_id"),
        "intent_type": intent_type,
        "decision_bar_timestamp": row.get("decision_bar_timestamp") or row.get("fill_timestamp"),
        "broker_order_id": row.get("broker_order_id") or row.get("order_id"),
        "account_id": row.get("account_id"),
        "perm_id": row.get("perm_id"),
        "client_id": row.get("client_id"),
        "exec_id": row.get("exec_id"),
        "local_symbol": contract.get("local_symbol") or row.get("local_symbol"),
        "con_id": contract.get("con_id") or row.get("con_id"),
        "contract_key": f"{symbol}-{expiry[:6]}" if symbol and expiry else symbol,
        "fill_price": row.get("fill_price"),
        "fill_timestamp": row.get("fill_timestamp"),
        "managed_exit_policy_id": row.get("managed_exit_policy_id") or "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        "trade_id": row.get("trade_id"),
        "lifecycle_id": row.get("lifecycle_id"),
        "paper_proof_invoked": False,
        "live_money_readiness": False,
        "review_required": False,
    }


def _recovery_adoption_trade_ids(broker_backed_entry_adoption: Mapping[str, Any] | None) -> set[str]:
    if not isinstance(broker_backed_entry_adoption, Mapping):
        return set()
    rows = [
        row
        for row in broker_backed_entry_adoption.get("adoptions") or []
        if isinstance(row, Mapping)
    ]
    if not rows and broker_backed_entry_adoption.get("trade_id"):
        rows = [broker_backed_entry_adoption]
    return {
        str(row.get("trade_id") or "").strip()
        for row in rows
        if row.get("adoption_allowed") is True and str(row.get("trade_id") or "").strip()
    }


def _registry_record_latest_event_value(record: TradeRegistryRecord, field_name: str) -> str | None:
    for event in reversed(record.event_chain):
        value = getattr(event, field_name, None)
        if value not in (None, ""):
            return str(value)
    return None


def _entry_action_for_side(side: object) -> str:
    return "SELL" if str(side or "").strip().upper() == "SHORT" else "BUY"


def _side_for_entry_action(action: object) -> str:
    normalized = str(action or "").strip().upper()
    return "SHORT" if normalized.startswith("SELL") else "LONG"


def _validate_broker_truth(
    *,
    broker_status: Mapping[str, Any],
    positions_snapshot: Mapping[str, Any],
    open_orders_snapshot: Mapping[str, Any],
    positions_path: Path,
    open_orders_path: Path,
    config: ReconciliationConfig,
    now: datetime,
    blockers: list[dict[str, Any]],
) -> None:
    if not broker_status:
        blockers.append({"code": "BROKER_TRUTH_STATUS_MISSING", "path": str(config.broker_status_path)})
        return
    status_generated_at = _parse_time(broker_status.get("generated_at") or broker_status.get("latest_refresh_time"))
    if status_generated_at is None:
        blockers.append({"code": "BROKER_TRUTH_STATUS_MISSING_GENERATED_AT", "path": str(config.broker_status_path)})
    else:
        age = max((now - status_generated_at).total_seconds(), 0.0)
        if age > config.max_age_seconds:
            blockers.append(
                {
                    "code": "BROKER_TRUTH_STATUS_STALE",
                    "path": str(config.broker_status_path),
                    "generated_at": status_generated_at.isoformat(),
                    "age_seconds": age,
                    "max_age_seconds": config.max_age_seconds,
                }
            )
    expected_status_flags = {
        "last_success": True,
        "read_only": True,
        "positions_complete": True,
        "open_orders_complete": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }
    for key, expected in expected_status_flags.items():
        if broker_status.get(key) is not expected:
            blockers.append({"code": "BROKER_TRUTH_STATUS_FLAG_MISMATCH", "field": key, "expected": expected, "actual": broker_status.get(key)})
    if str(broker_status.get("account") or "") != config.account:
        blockers.append({"code": "BROKER_TRUTH_ACCOUNT_MISMATCH", "expected": config.account, "actual": broker_status.get("account")})
    _validate_snapshot(
        payload=positions_snapshot,
        path=positions_path,
        complete_field="positions_complete",
        request_method="reqPositions",
        config=config,
        now=now,
        blockers=blockers,
    )
    _validate_snapshot(
        payload=open_orders_snapshot,
        path=open_orders_path,
        complete_field="open_orders_complete",
        request_method="reqAllOpenOrders",
        config=config,
        now=now,
        blockers=blockers,
    )
    if open_orders_snapshot.get("auto_open_orders_requested") is True:
        blockers.append({"code": "BROKER_TRUTH_UNSAFE_OPEN_ORDER_BINDING", "field": "auto_open_orders_requested", "path": str(open_orders_path)})
    if open_orders_snapshot.get("order_binding_requested") is True:
        blockers.append({"code": "BROKER_TRUTH_UNSAFE_OPEN_ORDER_BINDING", "field": "order_binding_requested", "path": str(open_orders_path)})


def _open_order_truth_evidence(
    *,
    config: ReconciliationConfig,
    now: datetime,
    track_b_positions: Sequence[Mapping[str, Any]],
    track_b_open_orders: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
    known_managed_exit_orders: Sequence[Mapping[str, Any]],
    unknown_track_b_open_orders: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Build Open Order Truth from the current in-memory reconciliation rows."""

    try:
        payload = build_track_b_open_order_truth_from_reconciliation(
            config=TrackBOpenOrderTruthConfig(
                repo_root=config.repo_root,
                dashboard_projection_path=None,
                reconciliation_path=config.report_path,
                live_position_status_path=config.live_position_status_path,
                market_data_root=config.market_data_root,
                artifact_max_age_seconds=config.max_age_seconds,
            ),
            reconciliation={
                "generated_at": now.isoformat(),
                "live_money_eligible": False,
                "paper_proof_invoked": False,
                "track_b_broker_positions": [dict(row) for row in track_b_positions],
                "track_b_broker_open_orders": [dict(row) for row in track_b_open_orders],
                "track_b_lifecycle_positions": [dict(row) for row in lifecycle_positions],
                "known_managed_exit_orders": [dict(row) for row in known_managed_exit_orders],
                "unknown_broker_open_orders": [dict(row) for row in unknown_track_b_open_orders],
                "unresolved_submit_intent_ownership_records": [],
            },
            now=now,
        )
        return {
            **payload,
            "broker_reconciliation_evidence_source": "OPEN_ORDER_TRUTH_BUILDER_DIRECT",
            "broker_reconciliation_open_order_truth_error": None,
        }
    except Exception as exc:  # noqa: BLE001 - reconciliation should preserve core blockers and expose evidence failure.
        return {
            "classification": "ORDER_TRUTH_STALE",
            "summary": {},
            "order_states": [],
            "broker_reconciliation_evidence_source": "OPEN_ORDER_TRUTH_BUILDER_ERROR",
            "broker_reconciliation_open_order_truth_error": str(exc),
        }


def _open_order_truth_report_context(open_order_truth: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "classification": open_order_truth.get("classification"),
        "summary": open_order_truth.get("summary") or {},
        "source": open_order_truth.get("broker_reconciliation_evidence_source"),
        "error": open_order_truth.get("broker_reconciliation_open_order_truth_error"),
        "order_states": open_order_truth.get("order_states") or [],
        "duplicate_close_order_groups": open_order_truth.get("duplicate_close_order_groups") or [],
        "broker_flat_with_open_close_order": open_order_truth.get("broker_flat_with_open_close_order") or [],
        "broker_positions_without_close_order": open_order_truth.get("broker_positions_without_close_order") or [],
    }


def _open_order_truth_blocker_context(open_order_truth: Mapping[str, Any]) -> dict[str, Any]:
    context = _open_order_truth_report_context(open_order_truth)
    context["order_states"] = [
        row
        for row in context.get("order_states", [])
        if isinstance(row, Mapping)
        and (
            row.get("suspicious") is True
            or row.get("classification") in {DUPLICATE_CLOSE_ORDER, SUSPICIOUS_ORDER_STATE}
            or row.get("unknown_open_order") is True
        )
    ]
    return context


def _managed_order_registry_evidence(*, config: ReconciliationConfig, now: datetime) -> dict[str, Any]:
    payload = _load_json(config.managed_order_registry_path)
    age_seconds = _age_seconds(payload.get("generated_at"), now) if payload else None
    return {
        "classification": payload.get("classification"),
        "summary": payload.get("summary") or {},
        "generated_at": payload.get("generated_at"),
        "artifact_path": str(config.managed_order_registry_path),
        "source": "MANAGED_ORDER_REGISTRY_AUTHORITY_ARTIFACT" if payload else "MANAGED_ORDER_REGISTRY_MISSING",
        "stale_or_missing": age_seconds is None or age_seconds > float(config.max_age_seconds),
        "age_seconds": age_seconds,
        "projection_only": payload.get("projection_only") is True,
    }


def _validate_snapshot(
    *,
    payload: Mapping[str, Any],
    path: Path,
    complete_field: str,
    request_method: str,
    config: ReconciliationConfig,
    now: datetime,
    blockers: list[dict[str, Any]],
) -> None:
    if not payload:
        blockers.append({"code": "BROKER_TRUTH_SNAPSHOT_MISSING", "path": str(path)})
        return
    generated_at = _parse_time(payload.get("generated_at"))
    if generated_at is None:
        blockers.append({"code": "BROKER_TRUTH_SNAPSHOT_MISSING_GENERATED_AT", "path": str(path)})
    else:
        age = max((now - generated_at).total_seconds(), 0.0)
        if age > config.max_age_seconds:
            blockers.append(
                {
                    "code": "BROKER_TRUTH_SNAPSHOT_STALE",
                    "path": str(path),
                    "generated_at": generated_at.isoformat(),
                    "age_seconds": age,
                    "max_age_seconds": config.max_age_seconds,
                }
            )
    if payload.get("read_only") is not True:
        blockers.append({"code": "BROKER_TRUTH_SNAPSHOT_NOT_READ_ONLY", "path": str(path)})
    if payload.get(complete_field) is not True:
        blockers.append({"code": "BROKER_TRUTH_SNAPSHOT_INCOMPLETE", "path": str(path), "field": complete_field})
    if str(payload.get("account") or payload.get("selected_account_id") or "") != config.account:
        blockers.append({"code": "BROKER_TRUTH_SNAPSHOT_ACCOUNT_MISMATCH", "path": str(path), "expected": config.account})
    if payload.get("request_method") != request_method:
        blockers.append(
            {
                "code": "BROKER_TRUTH_SNAPSHOT_REQUEST_METHOD_MISMATCH",
                "path": str(path),
                "expected": request_method,
                "actual": payload.get("request_method"),
            }
        )


def _last_successful_broker_truth_from_status(status: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "classification": status.get("classification"),
        "generated_at": status.get("generated_at"),
        "latest_refresh_time": status.get("latest_refresh_time"),
        "last_success_at": status.get("last_success_at"),
        "account": status.get("account"),
        "positions_complete": status.get("positions_complete") is True,
        "open_orders_complete": status.get("open_orders_complete") is True,
        "position_count": status.get("position_count"),
        "open_order_count": status.get("open_order_count"),
        "positions_snapshot_path": status.get("positions_snapshot_path"),
        "open_orders_snapshot_path": status.get("open_orders_snapshot_path"),
        "submit_authority": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def _validate_lifecycle_read_model(
    *,
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    pnl_summary: Mapping[str, Any],
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    if not trade_summary:
        blockers.append({"code": "LIFECYCLE_TRADE_SUMMARY_MISSING"})
    if not live_position_status:
        blockers.append({"code": "LIFECYCLE_POSITION_STATUS_MISSING"})
    if not pnl_summary:
        blockers.append({"code": "LIFECYCLE_PNL_SUMMARY_MISSING"})
    if _int_value(live_position_status.get("open_order_count")) != 0:
        blockers.append({"code": "LIFECYCLE_OPEN_ORDER_PRESENT", "count": live_position_status.get("open_order_count")})
    positions_by_instrument = live_position_status.get("positions_by_instrument")
    if positions_by_instrument not in ({}, None) and not isinstance(positions_by_instrument, Mapping):
        blockers.append({"code": "LIFECYCLE_POSITIONS_BY_INSTRUMENT_INVALID"})
    positions_by_strategy = live_position_status.get("positions_by_strategy")
    if positions_by_strategy not in ({}, None) and not isinstance(positions_by_strategy, Mapping):
        blockers.append({"code": "LIFECYCLE_POSITIONS_BY_STRATEGY_INVALID"})
    review_required_count = _lifecycle_review_required_count(
        trade_summary=trade_summary,
        live_position_status=live_position_status,
        pnl_summary=pnl_summary,
    )
    if review_required_count:
        blockers.append({"code": "LIFECYCLE_REVIEW_REQUIRED_PRESENT", "count": review_required_count})
    return blockers


def _lifecycle_review_required_count(
    *,
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    pnl_summary: Mapping[str, Any],
) -> int:
    return _max_int(
        trade_summary.get("review_required_count"),
        pnl_summary.get("review_required_count"),
        len(live_position_status.get("review_required_positions") or []),
    )


def _current_scope_review_required_count(blockers: Sequence[Mapping[str, Any]]) -> int:
    for row in blockers:
        if row.get("code") == "LIFECYCLE_REVIEW_REQUIRED_PRESENT":
            return _int_value(row.get("count"))
    return 0


def _bridge_terminal_event_grace_for_flat_lifecycle(
    *,
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    config: ReconciliationConfig,
    now: datetime,
) -> dict[str, Any]:
    if _int_value(live_position_status.get("open_position_count")) not in {0, None}:
        return {"applied": False, "reason": "lifecycle positions are not flat"}
    if _int_value(live_position_status.get("open_order_count")) not in {0, None}:
        return {"applied": False, "reason": "lifecycle open orders are not flat"}
    for trade in trade_summary.get("recent_trades", []) or []:
        if not isinstance(trade, Mapping):
            continue
        if not _terminal_trade_has_order_identity(trade):
            continue
        contract_key = str(trade.get("contract_key") or "")
        local_symbol = str(trade.get("local_symbol") or "")
        if _track_b_root({"contract_key": contract_key, "local_symbol": local_symbol}, config.symbols) is None:
            continue
        event = {
            "account_id": trade.get("account_id") or config.account,
            "contract_key": contract_key,
            "local_symbol": local_symbol,
            "con_id": trade.get("con_id"),
            "filled_at": trade.get("exit_timestamp"),
            "broker_order_id": trade.get("exit_order_id"),
            "event_type": "BRIDGE_TERMINAL_CLOSE",
        }
        grace = bridge_terminal_event_grace_state(
            event=event,
            now=now,
            account_id=config.account,
            contract_key=contract_key,
            local_symbol=local_symbol,
            con_id=_int_or_none(trade.get("con_id")),
            ttl_seconds=config.bridge_terminal_event_grace_seconds,
        )
        payload = grace.to_json_dict()
        payload["source"] = "RECENT_TRACK_B_TERMINAL_TRADE"
        if grace.applied:
            return payload
    return {"applied": False, "reason": "no exact recent bridge terminal event inside ttl"}


def _terminal_trade_has_order_identity(trade: Mapping[str, Any]) -> bool:
    return bool(trade.get("exit_timestamp") and trade.get("exit_order_id") and (trade.get("contract_key") or trade.get("local_symbol")))


def _broker_truth_settlement_state(
    *,
    position_match_report: Mapping[str, Any],
    broker_positions: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
    broker_open_orders: Sequence[Mapping[str, Any]],
    unknown_open_orders: Sequence[Mapping[str, Any]],
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    broker_status: Mapping[str, Any],
    config: ReconciliationConfig,
    now: datetime,
) -> dict[str, Any]:
    base = {
        "window_seconds": config.broker_truth_settlement_seconds,
        "poll_seconds": config.broker_truth_settlement_poll_seconds,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    if broker_status.get("live_money_eligible") is True:
        return {**base, "classification": "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE", "detail": "live_money_eligible=true blocks settlement waiting"}
    if broker_status.get("paper_proof_invoked") is True:
        return {**base, "classification": "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE", "detail": "paper_proof_invoked=true blocks settlement waiting"}
    if position_match_report.get("matched") is True:
        event = _previous_waiting_settlement_event(
            trade_summary=trade_summary,
            live_position_status=live_position_status,
        )
        if event and (_event_age_seconds(event, now) or config.broker_truth_settlement_seconds + 1) <= config.broker_truth_settlement_seconds:
            return {**base, "classification": "BROKER_TRUTH_SETTLEMENT_RESOLVED", "detail": "Broker truth and lifecycle are matched after a recent known broker-effect event.", "event": event}
        return {**base, "classification": "BROKER_TRUTH_SETTLEMENT_NOT_APPLICABLE", "detail": "broker and lifecycle already match"}
    if unknown_open_orders:
        return {
            **base,
            "classification": "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE",
            "detail": "unknown broker open orders block settlement waiting",
            "unknown_open_order_count": len(unknown_open_orders),
            "unknown_open_orders": [dict(row) for row in unknown_open_orders],
        }
    event = _settlement_event_for_mismatch(
        position_match_report=position_match_report,
        trade_summary=trade_summary,
        live_position_status=live_position_status,
        config=config,
        now=now,
    )
    if not event:
        return {**base, "classification": "BROKER_TRUTH_SETTLEMENT_NOT_APPLICABLE", "detail": "No exact known broker-effect event explains the mismatch."}
    event_age = _event_age_seconds(event, now)
    payload = {
        **base,
        "event": event,
        "event_age_seconds": event_age,
        "broker_position_count": len(broker_positions),
        "lifecycle_position_count": len(lifecycle_positions),
        "open_order_count": len(broker_open_orders),
    }
    if event_age is None:
        return {**payload, "classification": "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE", "detail": "Known broker-effect event is missing a usable timestamp."}
    if event_age <= config.broker_truth_settlement_seconds:
        return {**payload, "classification": "WAITING_FOR_BROKER_TRUTH_SETTLEMENT", "detail": "Mismatch is temporarily tolerated because it is explained by a recent attributed PAPER broker-effect event."}
    return {**payload, "classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT", "detail": "Broker truth did not settle inside the configured PAPER settlement window."}


def _unresolved_submit_intent_ownership_records(config: ReconciliationConfig) -> list[dict[str, Any]]:
    path = _repo_scoped_path(config.repo_root, config.submit_intent_ownership_path)
    rows = load_unresolved_submit_intent_ownership_records(path)
    allowed_symbols = {item.upper() for item in config.symbols}
    return [
        dict(row)
        for row in rows
        if str(row.get("symbol") or "").strip().upper() in allowed_symbols
    ]


def _filter_historical_resolved_submit_intents(
    rows: Sequence[Mapping[str, Any]],
    *,
    historical_debris_resolution: Mapping[str, Any],
) -> list[dict[str, Any]]:
    resolved_ids = {
        str(item.get("ownership_intent_id") or "").strip()
        for item in historical_debris_resolution.get("resolved_items") or []
        if isinstance(item, Mapping) and item.get("kind") == "submit_intent" and item.get("resolved") is True
    }
    resolved_trade_ids = {
        str(item.get("trade_id") or "").strip()
        for item in historical_debris_resolution.get("resolved_items") or []
        if isinstance(item, Mapping) and item.get("kind") == "submit_intent" and item.get("resolved") is True
    }
    if not resolved_ids and not resolved_trade_ids:
        return [dict(row) for row in rows]
    filtered: list[dict[str, Any]] = []
    for row in rows:
        extra = row.get("extra") if isinstance(row.get("extra"), Mapping) else {}
        ownership_id = str(row.get("ownership_intent_id") or "").strip()
        trade_id = str(extra.get("trade_id") or row.get("trade_id") or "").strip()
        if (ownership_id and ownership_id in resolved_ids) or (trade_id and trade_id in resolved_trade_ids):
            continue
        filtered.append(dict(row))
    return filtered


def _filter_historical_resolved_lifecycle_blockers(
    blockers: Sequence[Mapping[str, Any]],
    *,
    historical_debris_resolution: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if historical_debris_resolution.get("classification") != RESOLVER_CLEAN:
        return [dict(row) for row in blockers]
    lifecycle_resolved = any(
        isinstance(item, Mapping)
        and item.get("kind") in {"lifecycle_review", "lifecycle_review_summary"}
        and item.get("resolved") is True
        for item in historical_debris_resolution.get("resolved_items") or []
    )
    if not lifecycle_resolved:
        return [dict(row) for row in blockers]
    return [
        dict(row)
        for row in blockers
        if row.get("code") != "LIFECYCLE_REVIEW_REQUIRED_PRESENT"
    ]


def _filter_non_current_lifecycle_review_blockers(
    blockers: Sequence[Mapping[str, Any]],
    *,
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    broker_positions: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
    position_match_report: Mapping[str, Any],
    symbols: Sequence[str],
) -> list[dict[str, Any]]:
    current_count = _current_linked_lifecycle_review_required_count(
        trade_summary=trade_summary,
        live_position_status=live_position_status,
        broker_positions=broker_positions,
        lifecycle_positions=lifecycle_positions,
        position_match_report=position_match_report,
        symbols=symbols,
    )
    filtered: list[dict[str, Any]] = []
    for row in blockers:
        if row.get("code") != "LIFECYCLE_REVIEW_REQUIRED_PRESENT":
            filtered.append(dict(row))
            continue
        if current_count <= 0:
            continue
        updated = dict(row)
        updated["count"] = current_count
        filtered.append(updated)
    return filtered


def _current_linked_lifecycle_review_required_count(
    *,
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    broker_positions: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
    position_match_report: Mapping[str, Any],
    symbols: Sequence[str],
) -> int:
    review_positions = [
        dict(item)
        for item in live_position_status.get("review_required_positions") or []
        if isinstance(item, Mapping)
    ]
    if review_positions:
        return len(review_positions)

    review_trades = [
        dict(item)
        for item in trade_summary.get("recent_trades") or []
        if isinstance(item, Mapping) and item.get("review_required") is True
    ]
    if not review_trades:
        if broker_positions and position_match_report.get("matched") is not True:
            return _lifecycle_review_required_count(
                trade_summary=trade_summary,
                live_position_status=live_position_status,
                pnl_summary={},
            )
        return 0

    current_lifecycle_ids = _current_lifecycle_ids(lifecycle_positions)
    current_trade_ids = _current_trade_ids(lifecycle_positions)
    current_linked = [
        row
        for row in review_trades
        if _review_trade_links_to_current_lifecycle(
            row,
            current_lifecycle_ids=current_lifecycle_ids,
            current_trade_ids=current_trade_ids,
        )
    ]
    if current_linked:
        return len(current_linked)

    if position_match_report.get("matched") is True and lifecycle_positions:
        return 0

    return sum(
        1
        for row in review_trades
        if _review_trade_matches_unowned_current_broker_position(
            row,
            broker_positions=broker_positions,
            symbols=symbols,
        )
    )


def _current_lifecycle_ids(lifecycle_positions: Sequence[Mapping[str, Any]]) -> set[str]:
    ids: set[str] = set()
    for position in lifecycle_positions:
        for value in (position.get("lifecycle_id"), *(position.get("lifecycle_ids") or [])):
            text = str(value or "").strip()
            if text:
                ids.add(text)
        for unit in position.get("lifecycle_units") or []:
            if isinstance(unit, Mapping):
                text = str(unit.get("lifecycle_id") or "").strip()
                if text:
                    ids.add(text)
    return ids


def _current_trade_ids(lifecycle_positions: Sequence[Mapping[str, Any]]) -> set[str]:
    ids: set[str] = set()
    for position in lifecycle_positions:
        for value in (position.get("trade_id"), *(position.get("trade_ids") or [])):
            text = str(value or "").strip()
            if text:
                ids.add(text)
        for unit in position.get("lifecycle_units") or []:
            if isinstance(unit, Mapping):
                text = str(unit.get("trade_id") or "").strip()
                if text:
                    ids.add(text)
    return ids


def _review_trade_links_to_current_lifecycle(
    row: Mapping[str, Any],
    *,
    current_lifecycle_ids: set[str],
    current_trade_ids: set[str],
) -> bool:
    lifecycle_id = str(row.get("lifecycle_id") or "").strip()
    trade_id = str(row.get("trade_id") or "").strip()
    return bool((lifecycle_id and lifecycle_id in current_lifecycle_ids) or (trade_id and trade_id in current_trade_ids))


def _review_trade_matches_unowned_current_broker_position(
    row: Mapping[str, Any],
    *,
    broker_positions: Sequence[Mapping[str, Any]],
    symbols: Sequence[str],
) -> bool:
    review_root = _track_b_root(row, symbols)
    review_qty = _signed_review_quantity(row)
    if review_root is None or review_qty is None:
        return False
    for position in broker_positions:
        broker_root = _track_b_root(position, symbols)
        broker_qty = _decimal_value(position.get("quantity"))
        if broker_root == review_root and broker_qty == review_qty and _local_symbols_compatible(position, row):
            return True
    return False


def _signed_review_quantity(row: Mapping[str, Any]) -> Decimal | None:
    qty = _decimal_value(row.get("quantity") or row.get("qty"))
    if qty is None:
        return None
    side = str(row.get("side") or row.get("action") or "").upper()
    if "SHORT" in side or side.startswith("SELL"):
        return -abs(qty)
    if "LONG" in side or side.startswith("BUY"):
        return abs(qty)
    return qty


def _repo_scoped_path(repo_root: Path, path: Path) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        return repo_root / candidate
    try:
        return repo_root / candidate.relative_to(REPO_ROOT)
    except ValueError:
        return candidate


def _submit_intent_ownership_reconciliation_state(
    *,
    unresolved_submit_intents: Sequence[Mapping[str, Any]],
    broker_positions: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
    unknown_open_orders: Sequence[Mapping[str, Any]],
    position_match_report: Mapping[str, Any],
    config: ReconciliationConfig,
    now: datetime,
) -> dict[str, Any]:
    base = {
        "source": "TRACK_B_SUBMIT_INTENT_OWNERSHIP",
        "window_seconds": config.broker_truth_settlement_seconds,
        "unresolved_count": len(unresolved_submit_intents),
        "unresolved_records": [dict(row) for row in unresolved_submit_intents],
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    if not unresolved_submit_intents:
        return {**base, "classification": "SUBMIT_INTENT_OWNERSHIP_NOT_APPLICABLE", "detail": "No unresolved submit-intent ownership records."}
    unsafe = [
        dict(row)
        for row in unresolved_submit_intents
        if str(row.get("mode") or "").upper() != "PAPER"
        or str(row.get("account_id") or "") != config.account
        or row.get("live_money_eligible") is not False
        or row.get("paper_proof_invoked") is not False
    ]
    if unsafe:
        return {
            **base,
            "classification": "SUBMIT_INTENT_IDENTITY_MISMATCH_REVIEW_REQUIRED",
            "detail": "Unresolved submit-intent ownership contains unsafe or non-PAPER/account-mismatched records.",
            "mismatched_records": unsafe,
        }
    if unknown_open_orders:
        return {
            **base,
            "classification": "SUBMIT_INTENT_OPEN_ORDER_AMBIGUITY",
            "detail": "Unknown broker open orders block submit-intent settlement/adoption attribution.",
        }
    if position_match_report.get("matched") is True and lifecycle_positions:
        return {**base, "classification": "SUBMIT_INTENT_OWNERSHIP_NOT_APPLICABLE", "detail": "Broker and lifecycle are already matched."}

    unmatched_broker_positions = _unmatched_broker_positions_from_match_report(position_match_report)
    if unmatched_broker_positions:
        return _submit_intent_state_for_unmatched_broker_positions(
            base=base,
            unresolved_submit_intents=unresolved_submit_intents,
            unmatched_broker_positions=unmatched_broker_positions,
            config=config,
            now=now,
        )
    if not broker_positions and not lifecycle_positions:
        return _submit_intent_state_for_no_broker_effect(
            base=base,
            unresolved_submit_intents=unresolved_submit_intents,
            config=config,
            now=now,
        )
    return {**base, "classification": "SUBMIT_INTENT_OWNERSHIP_NOT_APPLICABLE", "detail": "Submit-intent ownership does not explain current broker/lifecycle state."}


def _broker_backed_entry_adoption_remediation(
    *,
    config: ReconciliationConfig,
    submit_intent_ownership_reconciliation: Mapping[str, Any],
    registry_reconciliation: Mapping[str, Any],
) -> dict[str, Any] | None:
    registry_classification = str(registry_reconciliation.get("classification") or "")
    if registry_classification == "REGISTRY_RECONCILIATION_REVIEW_REQUIRED":
        blockers = [row for row in registry_reconciliation.get("blockers") or [] if isinstance(row, Mapping)]
        orphan_broker_position_only = bool(blockers) and all(
            str(row.get("code") or "") == "REGISTRY_ORPHAN_BROKER_POSITION_REVIEW_REQUIRED"
            for row in blockers
        )
        if not orphan_broker_position_only:
            return {
                "classification": "BROKER_BACKED_ENTRY_ADOPTION_REVIEW_REQUIRED",
                "detail": "Central trade registry cannot safely identify the broker-backed position for recovery/adoption.",
                "registry_reconciliation": dict(registry_reconciliation),
                "adoption_allowed": False,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            }
    mapped_records = [row for row in registry_reconciliation.get("mapped_records") or [] if isinstance(row, Mapping)]
    mapped_trade_ids = [str(item).strip() for item in registry_reconciliation.get("mapped_trade_ids") or [] if str(item).strip()]
    broker_position_count = int(registry_reconciliation.get("broker_position_count") or 0)
    lifecycle_position_count = int(registry_reconciliation.get("lifecycle_position_count") or 0)
    classification = str(submit_intent_ownership_reconciliation.get("classification") or "")
    if classification == "SUBMIT_INTENT_BROKER_POSITIONS_ADOPTION_REQUIRED":
        remediations = []
        for item in submit_intent_ownership_reconciliation.get("matching_adoptions") or []:
            if not isinstance(item, Mapping):
                continue
            remediation = _broker_backed_entry_adoption_item(
                config=config,
                submit_intent=item.get("matching_submit_intent"),
                broker_position=item.get("broker_position"),
            )
            if remediation is not None:
                remediations.append(remediation)
        return {
            "classification": "BROKER_BACKED_ENTRIES_ADOPTION_REQUIRED",
            "detail": "Multiple broker-backed PAPER entries are each attributed to durable submit intents but no lifecycle OPEN_MANAGED records exist.",
            "adoption_count": len(remediations),
            "adoptions": remediations,
            "adoption_allowed": bool(remediations) and all(item.get("broker_backed_evidence_valid") is True for item in remediations),
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "required_action": "Run guarded PAPER lifecycle adoption for each exact ownership/order/contract before allowing submit.",
        }
    if classification == "SUBMIT_INTENT_BROKER_POSITION_ADOPTION_REQUIRED":
        submit_intent = submit_intent_ownership_reconciliation.get("matching_submit_intent")
        broker_position = submit_intent_ownership_reconciliation.get("broker_position")
        return _broker_backed_entry_adoption_item(config=config, submit_intent=submit_intent, broker_position=broker_position)
    open_resume_records = [
        row
        for row in mapped_records
        if str(row.get("current_state") or "") in {"OPEN_MANAGED", "EXIT_DUE", "WORKING_EXIT"}
    ]
    if open_resume_records and mapped_trade_ids and broker_position_count > 0 and lifecycle_position_count == 0:
        return {
            "classification": "BROKER_BACKED_ENTRY_REGISTRY_RESUME_REQUIRED",
            "detail": "Broker-backed PAPER position maps to exactly one central registry trade_id and should resume from that trade chain.",
            "adoption_allowed": True,
            "resume_existing_trade": True,
            "trade_id": mapped_trade_ids[0] if len(mapped_trade_ids) == 1 else None,
            "adoption_count": len(open_resume_records),
            "adoptions": [
                _registry_resume_adoption_item(row)
                for row in open_resume_records
            ],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "required_action": "Resume guarded PAPER lifecycle management from the exact central registry trade_id.",
        }
    if classification != "SUBMIT_INTENT_BROKER_POSITION_ADOPTION_REQUIRED":
        return None
    return None


def _broker_backed_entry_adoption_item(
    *,
    config: ReconciliationConfig,
    submit_intent: object,
    broker_position: object,
) -> dict[str, Any] | None:
    if not isinstance(submit_intent, Mapping) or not isinstance(broker_position, Mapping):
        return None
    perm_id = submit_intent.get("perm_id")
    extra = submit_intent.get("extra") if isinstance(submit_intent.get("extra"), Mapping) else {}
    caller_metadata = extra.get("caller_metadata") if isinstance(extra.get("caller_metadata"), Mapping) else {}
    resolver = resolve_broker_backed_fill_evidence(
        repo_root=config.repo_root,
        request=BrokerFillEvidenceRequest(
            trade_id=extra.get("trade_id") or submit_intent.get("trade_id"),
            submit_intent_id=submit_intent.get("ownership_intent_id") or submit_intent.get("order_intent_id"),
            order_id=submit_intent.get("broker_order_id"),
            client_id=submit_intent.get("client_id"),
            perm_id=perm_id,
            con_id=submit_intent.get("con_id") or broker_position.get("con_id") or broker_position.get("conId"),
            local_symbol=submit_intent.get("local_symbol") or broker_position.get("local_symbol") or broker_position.get("localSymbol"),
            account_id=submit_intent.get("account_id") or broker_position.get("account_id") or broker_position.get("account"),
            action=submit_intent.get("action"),
            qty=submit_intent.get("qty") or broker_position.get("quantity"),
            symbol=submit_intent.get("symbol") or broker_position.get("symbol"),
        ),
    )
    resolved_evidence = resolver.evidence or {}
    exec_id = (
        submit_intent.get("exec_id")
        or submit_intent.get("execution_id")
        or extra.get("exec_id")
        or resolved_evidence.get("exec_id")
        or resolved_evidence.get("execution_id")
    )
    if not perm_id:
        perm_id = resolved_evidence.get("perm_id")
    broker_backed_evidence_valid = bool(str(perm_id or "").strip() and str(exec_id or "").strip())
    trade_id = str(extra.get("trade_id") or submit_intent.get("trade_id") or "").strip()
    if not trade_id:
        trade_id = trade_id_from_live_identity(
            lifecycle_id=submit_intent.get("lifecycle_id"),
            ownership_intent_id=submit_intent.get("ownership_intent_id"),
            order_intent_id=submit_intent.get("order_intent_id"),
            account_id=submit_intent.get("account_id"),
            con_id=submit_intent.get("con_id") or broker_position.get("con_id"),
            lane_id=submit_intent.get("lane_id") or submit_intent.get("strategy_id"),
        )
    return {
        "classification": "BROKER_BACKED_ENTRY_ADOPTION_REQUIRED"
        if broker_backed_evidence_valid
        else "BROKER_BACKED_ENTRY_ADOPTION_REVIEW_REQUIRED",
        "detail": "Broker-backed PAPER entry is attributed to a durable submit intent but no lifecycle OPEN_MANAGED record exists."
        if broker_backed_evidence_valid
        else "Broker position is attributed to a submit intent, but broker-backed fill evidence is missing an exact exec_id.",
        "adoption_allowed": broker_backed_evidence_valid,
        "broker_backed_evidence_valid": broker_backed_evidence_valid,
        "fill_evidence_resolver": {
            "classification": resolver.classification,
            "broker_backed_evidence_valid": resolver.broker_backed_evidence_valid,
            "searched_paths": list(resolver.searched_paths),
            "reason_codes": list(resolver.reason_codes),
            "selected_source_artifact_path": resolved_evidence.get("source_artifact_path"),
            "matching_execution_count": len(resolver.matches),
            "matching_executions": list(resolver.matches),
            "rejected_execution_count": len(resolver.rejected),
            "rejected_executions": list(resolver.rejected),
        },
        "trade_id": trade_id,
        "lifecycle_id": submit_intent.get("lifecycle_id"),
        "ownership_intent_id": submit_intent.get("ownership_intent_id"),
        "order_intent_id": submit_intent.get("order_intent_id") or submit_intent.get("ownership_intent_id"),
        "broker_order_id": submit_intent.get("broker_order_id") or resolved_evidence.get("order_id"),
        "client_id": submit_intent.get("client_id") or resolved_evidence.get("client_id"),
        "perm_id": perm_id,
        "exec_id": exec_id,
        "lane_id": submit_intent.get("lane_id"),
        "strategy_id": submit_intent.get("strategy_id"),
        "managed_exit_policy_id": submit_intent.get("managed_exit_policy_id") or caller_metadata.get("managed_exit_policy_id"),
        "action": submit_intent.get("action"),
        "contract": {
            "symbol": submit_intent.get("symbol") or broker_position.get("symbol"),
            "local_symbol": submit_intent.get("local_symbol") or broker_position.get("local_symbol"),
            "expiry": submit_intent.get("expiry") or broker_position.get("expiry"),
            "con_id": submit_intent.get("con_id") or broker_position.get("con_id"),
        },
        "qty": submit_intent.get("qty") or broker_position.get("quantity"),
        "fill_price": resolved_evidence.get("price"),
        "fill_timestamp": resolved_evidence.get("fill_timestamp"),
        "broker_position_quantity": broker_position.get("quantity"),
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "required_action": "Run guarded PAPER lifecycle adoption for this exact ownership/order/contract before allowing submit.",
    }


def _registry_resume_adoption_item(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "classification": "BROKER_BACKED_ENTRY_REGISTRY_RESUME_READY",
        "adoption_allowed": True,
        "resume_existing_trade": True,
        "trade_id": row.get("trade_id"),
        "lifecycle_id": row.get("lifecycle_id"),
        "lane_id": row.get("lane_id"),
        "strategy_id": row.get("strategy_id"),
        "account_id": row.get("account_id"),
        "action": _entry_action_for_side(row.get("side")),
        "contract": {
            "symbol": row.get("symbol") or row.get("instrument_family"),
            "local_symbol": row.get("local_symbol"),
            "expiry": row.get("expiry"),
            "con_id": row.get("con_id"),
        },
        "qty": row.get("quantity"),
        "broker_order_id": row.get("entry_order_id"),
        "client_id": row.get("entry_client_id"),
        "perm_id": row.get("entry_perm_id"),
        "exec_id": row.get("entry_exec_id"),
        "broker_backed_evidence_valid": bool(str(row.get("entry_perm_id") or "").strip() and str(row.get("entry_exec_id") or "").strip()),
    }


def _unmatched_broker_positions_from_match_report(position_match_report: Mapping[str, Any]) -> list[dict[str, Any]]:
    mismatches = [row for row in position_match_report.get("mismatches", []) or [] if isinstance(row, Mapping)]
    positions = [
        dict(row.get("broker_position"))
        for row in mismatches
        if isinstance(row.get("broker_position"), Mapping)
    ]
    if positions:
        return positions
    broker_rows = position_match_report.get("broker")
    lifecycle_rows = position_match_report.get("lifecycle")
    if isinstance(broker_rows, list) and not lifecycle_rows:
        return [dict(row) for row in broker_rows if isinstance(row, Mapping)]
    return []


def _submit_intent_state_for_unmatched_broker_positions(
    *,
    base: Mapping[str, Any],
    unresolved_submit_intents: Sequence[Mapping[str, Any]],
    unmatched_broker_positions: Sequence[Mapping[str, Any]],
    config: ReconciliationConfig,
    now: datetime,
) -> dict[str, Any]:
    if len(unmatched_broker_positions) != 1:
        per_position = [
            _submit_intent_match_for_broker_position(
                broker_position=dict(position),
                unresolved_submit_intents=unresolved_submit_intents,
                config=config,
                now=now,
            )
            for position in unmatched_broker_positions
        ]
        if per_position and all(item.get("classification") == "SUBMIT_INTENT_BROKER_POSITION_ADOPTION_REQUIRED" for item in per_position):
            return {
                **base,
                "classification": "SUBMIT_INTENT_BROKER_POSITIONS_ADOPTION_REQUIRED",
                "detail": "Each unmatched broker position is exactly attributed to one unresolved Track B submit intent and needs lifecycle adoption.",
                "matching_adoptions": per_position,
                "unmatched_broker_positions": [dict(row) for row in unmatched_broker_positions],
            }
        return {
            **base,
            "classification": "SUBMIT_INTENT_IDENTITY_MISMATCH_REVIEW_REQUIRED",
            "detail": "Submit-intent attribution requires exactly one unmatched broker position.",
            "unmatched_broker_positions": [dict(row) for row in unmatched_broker_positions],
        }
    broker_position = dict(unmatched_broker_positions[0])
    return {
        **base,
        **_submit_intent_match_for_broker_position(
            broker_position=broker_position,
            unresolved_submit_intents=unresolved_submit_intents,
            config=config,
            now=now,
        ),
    }


def _submit_intent_match_for_broker_position(
    *,
    broker_position: Mapping[str, Any],
    unresolved_submit_intents: Sequence[Mapping[str, Any]],
    config: ReconciliationConfig,
    now: datetime,
) -> dict[str, Any]:
    broker_position = dict(broker_position)
    same_contract = [
        dict(row)
        for row in unresolved_submit_intents
        if _submit_intent_contract_matches_broker_position(row, broker_position, config.symbols)
    ]
    exact_matches = [
        row
        for row in same_contract
        if _submit_intent_matches_broker_position(row, broker_position, config=config)
    ]
    if len(exact_matches) > 1:
        return {
            "classification": "SUBMIT_INTENT_COMPETING_UNRESOLVED_REVIEW_REQUIRED",
            "detail": "Multiple unresolved submit-intent ownership records match the same broker position.",
            "broker_position": broker_position,
            "matching_submit_intents": exact_matches,
        }
    if len(exact_matches) == 1:
        event_age = _submit_intent_age_seconds(exact_matches[0], now)
        delayed_adoption = event_age is None or event_age > config.broker_truth_settlement_seconds
        if delayed_adoption and not _submit_intent_has_broker_effect_proof(exact_matches[0]):
            return {
                "classification": "SUBMIT_INTENT_IDENTITY_MISMATCH_REVIEW_REQUIRED",
                "detail": "Matching submit-intent ownership record is stale and lacks broker-effect/adoption evidence.",
                "broker_position": broker_position,
                "stale_or_unusable_submit_intents": exact_matches,
            }
        return {
            "classification": "SUBMIT_INTENT_BROKER_POSITION_ADOPTION_REQUIRED",
            "detail": "Broker position is exactly attributed to one unresolved Track B submit intent and needs lifecycle adoption.",
            "broker_position": broker_position,
            "matching_submit_intent": exact_matches[0],
            "event_age_seconds": event_age,
            "delayed_adoption": delayed_adoption,
        }
    if same_contract:
        return {
            "classification": "SUBMIT_INTENT_IDENTITY_MISMATCH_REVIEW_REQUIRED",
            "detail": "Unresolved submit-intent ownership records share the broker contract but fail side/qty/account identity.",
            "broker_position": broker_position,
            "mismatched_submit_intents": same_contract,
        }
    return {
        "classification": "SUBMIT_INTENT_NO_MATCHING_RECORD",
        "detail": "Broker-only Track B position has no matching unresolved submit-intent ownership record.",
        "broker_position": broker_position,
    }


def _submit_intent_state_for_no_broker_effect(
    *,
    base: Mapping[str, Any],
    unresolved_submit_intents: Sequence[Mapping[str, Any]],
    config: ReconciliationConfig,
    now: datetime,
) -> dict[str, Any]:
    comparable_records = [
        dict(row)
        for row in unresolved_submit_intents
        if _submit_intent_is_entry(row)
    ]
    if not comparable_records:
        return {**base, "classification": "SUBMIT_INTENT_OWNERSHIP_NOT_APPLICABLE", "detail": "No unresolved entry submit intents require broker-effect settlement."}
    grouped: dict[tuple[str, str, str, str, str], list[dict[str, Any]]] = {}
    for row in comparable_records:
        grouped.setdefault(_submit_intent_competition_key(row), []).append(row)
    competing = [rows for rows in grouped.values() if len(rows) > 1]
    if competing:
        return {
            **base,
            "classification": "SUBMIT_INTENT_COMPETING_UNRESOLVED_REVIEW_REQUIRED",
            "detail": "Multiple unresolved submit intents compete for the same account/contract/side window.",
            "competing_submit_intents": competing,
        }
    newest = max(comparable_records, key=lambda row: _submit_intent_age_seconds(row, now) or 0.0)
    age = _submit_intent_age_seconds(newest, now)
    if age is None:
        return {
            **base,
            "classification": "SUBMIT_INTENT_IDENTITY_MISMATCH_REVIEW_REQUIRED",
            "detail": "Unresolved submit intent is missing a usable timestamp.",
            "matching_submit_intent": newest,
        }
    payload = {
        **base,
        "matching_submit_intent": newest,
        "event_age_seconds": age,
    }
    if age <= config.broker_truth_settlement_seconds:
        return {
            **payload,
            "classification": "SUBMIT_INTENT_NO_BROKER_EFFECT_PENDING_SETTLEMENT",
            "detail": "No broker effect is visible yet, but the unresolved submit intent remains inside the uncertainty window.",
        }
    return {
        **payload,
        "classification": "SUBMIT_INTENT_NO_BROKER_EFFECT_TIMEOUT",
        "detail": "No broker effect appeared for the unresolved submit intent inside the configured uncertainty window.",
    }


def _submit_intent_contract_matches_broker_position(
    submit_intent: Mapping[str, Any],
    broker_position: Mapping[str, Any],
    symbols: Sequence[str],
) -> bool:
    intent_root = _track_b_root(submit_intent, symbols)
    broker_root = _track_b_root(broker_position, symbols)
    if intent_root is None or intent_root != broker_root:
        return False
    intent_local = str(submit_intent.get("local_symbol") or "").upper()
    broker_local = str(broker_position.get("local_symbol") or broker_position.get("localSymbol") or "").upper()
    if intent_local and broker_local and intent_local != broker_local:
        return False
    intent_con_id = _int_or_none(submit_intent.get("con_id"))
    broker_con_id = _int_or_none(broker_position.get("con_id") or broker_position.get("conId"))
    if intent_con_id is not None and broker_con_id is not None and intent_con_id != broker_con_id:
        return False
    intent_expiry = str(submit_intent.get("expiry") or "")
    broker_expiry = str(broker_position.get("expiry") or broker_position.get("lastTradeDateOrContractMonth") or "")
    if intent_expiry and broker_expiry and intent_expiry != broker_expiry:
        return False
    return True


def _submit_intent_matches_broker_position(
    submit_intent: Mapping[str, Any],
    broker_position: Mapping[str, Any],
    config: ReconciliationConfig,
) -> bool:
    if str(submit_intent.get("mode") or "").upper() != "PAPER":
        return False
    if str(submit_intent.get("account_id") or "") != config.account:
        return False
    if submit_intent.get("live_money_eligible") is not False or submit_intent.get("paper_proof_invoked") is not False:
        return False
    if not _submit_intent_contract_matches_broker_position(submit_intent, broker_position, config.symbols):
        return False
    broker_qty = _decimal_value(broker_position.get("quantity"))
    intent_qty = _decimal_value(submit_intent.get("qty") or submit_intent.get("quantity"))
    if broker_qty is None or intent_qty is None or abs(broker_qty) != abs(intent_qty):
        return False
    expected_action = "BUY" if broker_qty > 0 else "SELL"
    if str(submit_intent.get("action") or "").upper().replace("_TO_OPEN", "") != expected_action:
        return False
    return _submit_intent_is_entry(submit_intent)


def _submit_intent_is_entry(submit_intent: Mapping[str, Any]) -> bool:
    intent_type = str(submit_intent.get("intent_type") or "").upper()
    action = str(submit_intent.get("action") or "").upper()
    return intent_type in {"BUY_TO_OPEN", "SELL_TO_OPEN", "ENTRY"} or action in {"BUY", "SELL"}


def _submit_intent_has_broker_effect_proof(submit_intent: Mapping[str, Any]) -> bool:
    state = str(submit_intent.get("state") or "").strip().upper()
    if state in {"BROKER_POSITION_OBSERVED_ADOPTION_REQUIRED", "LIFECYCLE_OPEN_PERSISTED"}:
        return True
    extra = submit_intent.get("extra") if isinstance(submit_intent.get("extra"), Mapping) else {}
    return (
        str(extra.get("broker_effect_classification") or "").strip().upper() == "BROKER_EFFECT_CONFIRMED"
        or str(extra.get("bridge_classification") or "").strip().upper() in {"PAPER_STRATEGY_ORDER_FILLED", "PAPER_ORDER_FILLED"}
        or str(extra.get("delegated_status") or "").strip().lower() == "filled"
    )


def _submit_intent_competition_key(submit_intent: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(submit_intent.get("account_id") or ""),
        str(submit_intent.get("symbol") or "").upper(),
        str(submit_intent.get("local_symbol") or "").upper(),
        str(submit_intent.get("expiry") or ""),
        str(submit_intent.get("action") or "").upper().replace("_TO_OPEN", ""),
    )


def _submit_intent_age_seconds(submit_intent: Mapping[str, Any], now: datetime) -> float | None:
    timestamp = _parse_time(
        submit_intent.get("updated_at")
        or submit_intent.get("created_at")
        or submit_intent.get("submitted_at")
    )
    if timestamp is None:
        return None
    return max((now - timestamp).total_seconds(), 0.0)


def _previous_waiting_settlement_event(
    *,
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
) -> dict[str, Any] | None:
    for source in (live_position_status, trade_summary):
        settlement = source.get("broker_truth_settlement")
        if not isinstance(settlement, Mapping):
            continue
        if settlement.get("classification") != "WAITING_FOR_BROKER_TRUTH_SETTLEMENT":
            continue
        event = settlement.get("event")
        if isinstance(event, Mapping):
            return dict(event)
    return None


def _settlement_event_for_mismatch(
    *,
    position_match_report: Mapping[str, Any],
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    config: ReconciliationConfig,
    now: datetime,
) -> dict[str, Any] | None:
    unmatched_lifecycle = [row for row in position_match_report.get("unmatched_lifecycle_positions", []) if isinstance(row, Mapping)]
    mismatches = [row for row in position_match_report.get("mismatches", []) if isinstance(row, Mapping)]
    unmatched_broker = [row.get("broker_position") for row in mismatches if isinstance(row.get("broker_position"), Mapping)]
    if unmatched_lifecycle and not unmatched_broker:
        return _entry_settlement_event(unmatched_lifecycle[0], config=config)
    if unmatched_broker and not unmatched_lifecycle:
        for trade in trade_summary.get("recent_trades", []) or []:
            if not isinstance(trade, Mapping):
                continue
            event = _close_settlement_event(trade, config=config)
            if event and any(_event_matches_position(event, broker) for broker in unmatched_broker if isinstance(broker, Mapping)):
                return event
    return None


def _entry_settlement_event(lifecycle: Mapping[str, Any], *, config: ReconciliationConfig) -> dict[str, Any] | None:
    order_id = lifecycle.get("entry_order_id") or _nested_mapping(lifecycle, "entry_broker_identity").get("broker_order_id")
    timestamp = lifecycle.get("entry_timestamp") or lifecycle.get("entry_time") or lifecycle.get("as_of")
    if not order_id or not timestamp:
        return None
    return {
        "event_type": "ENTRY_FILL_EXPECTING_BROKER_POSITION",
        "account_id": lifecycle.get("account_id") or config.account,
        "contract_key": lifecycle.get("contract_key") or lifecycle.get("position_key"),
        "local_symbol": lifecycle.get("local_symbol") or lifecycle.get("localSymbol"),
        "con_id": lifecycle.get("con_id") or _nested_mapping(lifecycle, "entry_broker_identity").get("con_id"),
        "broker_order_id": str(order_id),
        "event_time": str(timestamp),
        "lifecycle_id": lifecycle.get("lifecycle_id"),
        "strategy_id": lifecycle.get("strategy_id"),
    }


def _close_settlement_event(trade: Mapping[str, Any], *, config: ReconciliationConfig) -> dict[str, Any] | None:
    if not _terminal_trade_has_order_identity(trade):
        return None
    return {
        "event_type": "EXIT_FILL_EXPECTING_BROKER_FLAT",
        "account_id": trade.get("account_id") or config.account,
        "contract_key": trade.get("contract_key"),
        "local_symbol": trade.get("local_symbol") or trade.get("localSymbol"),
        "con_id": trade.get("con_id"),
        "broker_order_id": str(trade.get("exit_order_id")),
        "event_time": str(trade.get("exit_timestamp")),
        "lifecycle_id": trade.get("lifecycle_id"),
        "strategy_id": trade.get("strategy_id"),
    }


def _event_age_seconds(event: Mapping[str, Any], now: datetime) -> float | None:
    event_time = _parse_time(event.get("event_time") or event.get("filled_at") or event.get("submitted_at"))
    if event_time is None:
        return None
    return max((now - event_time).total_seconds(), 0.0)


def _event_matches_position(event: Mapping[str, Any], position: Mapping[str, Any]) -> bool:
    event_con_id = _int_or_none(event.get("con_id"))
    position_con_id = _int_or_none(position.get("con_id") or position.get("conId"))
    if event_con_id is not None and position_con_id is not None and event_con_id != position_con_id:
        return False
    event_local = str(event.get("local_symbol") or "").upper()
    position_local = str(position.get("local_symbol") or position.get("localSymbol") or "").upper()
    if event_local and position_local and event_local != position_local:
        return False
    event_root = _track_b_root(event, PHASE1_RUNTIME_TICKER_ORDER)
    position_root = _track_b_root(position, PHASE1_RUNTIME_TICKER_ORDER)
    return event_root is not None and event_root == position_root


def _known_managed_exit_orders(
    *,
    broker_open_orders: Sequence[Mapping[str, Any]],
    lifecycle_status: Mapping[str, Any],
    position_match_report: Mapping[str, Any],
    config: ReconciliationConfig,
    now: datetime,
    runtime_restore_orders: Sequence[Mapping[str, Any]] = (),
    persisted_known_orders: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    declared_orders = _declared_known_managed_exit_orders(lifecycle_status)
    declared_orders.extend(_lifecycle_report_known_managed_exit_orders(lifecycle_status=lifecycle_status, repo_root=config.repo_root))
    declared_orders.extend(dict(row) for row in runtime_restore_orders if isinstance(row, Mapping))
    declared_orders.extend(dict(row) for row in persisted_known_orders if isinstance(row, Mapping))
    if not declared_orders:
        return []
    matched_positions = [
        dict(row)
        for row in position_match_report.get("matches", []) or []
        if isinstance(row, Mapping)
    ]
    known: list[dict[str, Any]] = []
    for broker_order in broker_open_orders:
        for declared in declared_orders:
            if not _broker_order_matches_declared_managed_exit(broker_order=broker_order, declared=declared):
                continue
            matched_position = _matched_position_for_declared_managed_exit(declared=declared, matched_positions=matched_positions)
            if matched_position is None:
                continue
            row = dict(broker_order)
            row["managed_order_status"] = "KNOWN_MANAGED_EXIT_ORDER_WORKING"
            row["lifecycle_id"] = declared.get("lifecycle_id")
            row["strategy_id"] = declared.get("strategy_id")
            row["lane_id"] = declared.get("lane_id")
            row["order_intent_id"] = declared.get("order_intent_id")
            _merge_missing_managed_exit_order_fields(row, declared)
            _merge_missing_managed_exit_order_fields(row, _managed_exit_identity_from_match(matched_position))
            _enrich_managed_exit_order_from_prepared_artifact(row, config.repo_root)
            market_reference = _runtime_market_reference(config=config, root=str(row.get("track_b_root") or row.get("symbol") or ""))
            policy = classify_managed_exit_working_order(
                order=row,
                now=now,
                runtime_market_reference=market_reference.get("price"),
                runtime_market_reference_source=market_reference.get("source_artifact_path"),
            )
            policy_payload = policy.to_json_dict()
            row["managed_order_policy"] = policy_payload
            row["managed_order_status"] = policy_payload["classification"]
            row["exit_urgency"] = policy_payload["exit_urgency"]
            row["order_age_seconds"] = policy_payload["order_age_seconds"]
            row["order_limit_price"] = policy_payload["limit_price"]
            row["runtime_market_reference"] = policy_payload["runtime_market_reference"]
            row["runtime_market_reference_source"] = policy_payload["runtime_market_reference_source"]
            row["distance_from_market_points"] = policy_payload["distance_from_market_points"]
            row["marketable_by_runtime_context"] = policy_payload["marketable_by_runtime_context"]
            row["stale_by_policy"] = policy_payload["stale_by_policy"]
            row["recommended_action"] = policy_payload["recommended_action"]
            proposal = _guarded_cancel_replace_proposal(row, policy_payload)
            if proposal:
                row["guarded_cancel_replace_proposal"] = proposal
            row["source"] = declared.get("source") or "TRACK_B_LIFECYCLE_PENDING_EXIT_ORDER"
            source_artifact_path = declared.get("source_artifact_path")
            if source_artifact_path:
                row["source_artifact_path"] = source_artifact_path
            known.append(row)
            break
    return known


def _runtime_restore_known_managed_exit_orders(repo_root: Path, symbols: Sequence[str]) -> list[dict[str, Any]]:
    lanes_root = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes"
    rows: list[dict[str, Any]] = []
    try:
        restore_paths = sorted(lanes_root.glob("*/restore_validation_latest.json"))
    except OSError:
        return rows
    for restore_path in restore_paths:
        payload = _load_json(restore_path)
        row = _managed_exit_order_from_restore_payload(payload, source_path=restore_path, symbols=symbols)
        if row is not None:
            rows.append(row)
    return rows


def _lifecycle_report_known_managed_exit_orders(*, lifecycle_status: Mapping[str, Any], repo_root: Path) -> list[dict[str, Any]]:
    report_paths: list[Path] = []
    for positions_key in ("positions_by_instrument", "positions_by_strategy"):
        positions = lifecycle_status.get(positions_key)
        if not isinstance(positions, Mapping):
            continue
        for position in positions.values():
            if not isinstance(position, Mapping):
                continue
            path_value = position.get("paper_lifecycle_report_path")
            if not path_value:
                continue
            path = Path(str(path_value))
            report_paths.append(path if path.is_absolute() else repo_root / path)
    rows: list[dict[str, Any]] = []
    seen_paths: set[Path] = set()
    for report_path in report_paths:
        if report_path in seen_paths:
            continue
        seen_paths.add(report_path)
        report = _load_json(report_path)
        for row in _declared_known_managed_exit_orders(report):
            row.setdefault("source", "TRACK_B_LIFECYCLE_REPORT_KNOWN_MANAGED_EXIT_ORDER")
            row.setdefault("source_artifact_path", str(report_path))
            rows.append(row)
    return rows


def _persisted_known_managed_exit_orders(repo_root: Path, symbols: Sequence[str]) -> list[dict[str, Any]]:
    path = repo_root / DEFAULT_KNOWN_MANAGED_EXIT_ORDERS_PATH.relative_to(REPO_ROOT)
    payload = _load_json(path)
    rows_payload = payload.get("known_managed_exit_orders") if isinstance(payload, Mapping) else None
    if not isinstance(rows_payload, list):
        return []
    allowed_symbols = {item.upper() for item in symbols}
    rows: list[dict[str, Any]] = []
    for item in rows_payload:
        if not isinstance(item, Mapping):
            continue
        symbol = str(item.get("symbol") or "").strip().upper()
        if symbol and symbol not in allowed_symbols:
            continue
        status = str(item.get("managed_order_status") or item.get("status") or "").strip().upper()
        if status in {"CANCELLED", "CANCELED", "REJECTED", "FILLED", "EXPIRED"}:
            continue
        rows.append(dict(item))
    return rows


def _persisted_known_leak_test_entry_orders(repo_root: Path, symbols: Sequence[str]) -> list[dict[str, Any]]:
    path = repo_root / DEFAULT_KNOWN_LEAK_TEST_ENTRY_ORDERS_PATH.relative_to(REPO_ROOT)
    payload = _load_json(path)
    rows_payload = payload.get("known_leak_test_entry_orders") if isinstance(payload, Mapping) else None
    if not isinstance(rows_payload, list):
        return []
    return _filter_known_leak_test_entry_orders(rows_payload, symbols)


def _artifact_known_leak_test_entry_orders(repo_root: Path, symbols: Sequence[str]) -> list[dict[str, Any]]:
    root = repo_root / "outputs" / "reports" / "track_b_paper_leak_test"
    try:
        report_paths = sorted(root.glob("*/ibkr_paper_strategy_bridge_report.json"))
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for report_path in report_paths:
        report = _load_json(report_path)
        row = _known_leak_test_entry_order_from_bridge_report(report, source_path=report_path)
        if row is not None:
            rows.append(row)
    return _filter_known_leak_test_entry_orders(rows, symbols)


def _filter_known_leak_test_entry_orders(rows_payload: Sequence[Mapping[str, Any]], symbols: Sequence[str]) -> list[dict[str, Any]]:
    allowed_symbols = {item.upper() for item in symbols}
    rows: list[dict[str, Any]] = []
    for item in rows_payload:
        if not isinstance(item, Mapping):
            continue
        symbol = str(item.get("symbol") or "").strip().upper()
        if symbol and symbol not in allowed_symbols:
            continue
        status = str(item.get("managed_order_status") or item.get("status") or "").strip().upper()
        if status in {"CANCELLED", "CANCELED", "REJECTED", "FILLED", "EXPIRED"}:
            continue
        action = str(item.get("action") or "").strip().upper()
        if action not in {"BUY", "SELL", "BUY_TO_OPEN", "SELL_TO_OPEN"}:
            continue
        rows.append(dict(item))
    return rows


def _known_leak_test_entry_order_from_bridge_report(
    report: Mapping[str, Any],
    *,
    source_path: Path,
) -> dict[str, Any] | None:
    if not isinstance(report, Mapping):
        return None
    metadata = dict(report.get("caller_metadata") or {})
    if metadata.get("leak_test") is not True and str(metadata.get("caller_type") or "") != "track_b_paper_leak_test":
        return None
    delegated = dict(report.get("delegated_result") or {})
    delegated_report = dict(delegated.get("report") or {})
    lifecycle = dict(
        delegated_report.get("submit_cancel_lifecycle")
        or delegated_report.get("lifecycle")
        or delegated.get("submit_cancel_lifecycle")
        or {}
    )
    broker_order_id = _int_or_none(
        lifecycle.get("submitted_order_id")
        or lifecycle.get("order_id")
        or lifecycle.get("broker_order_id")
        or delegated_report.get("submitted_order_id")
        or delegated_report.get("order_id")
        or delegated.get("submitted_order_id")
        or delegated.get("broker_order_id")
    )
    if broker_order_id is None:
        return None
    intent = dict(report.get("intent") or {})
    action = str(intent.get("action") or report.get("action") or metadata.get("intent_action") or "").strip().upper()
    if action not in {"BUY", "SELL", "BUY_TO_OPEN", "SELL_TO_OPEN"}:
        return None
    qualified = dict((report.get("qualified_contract_report") or {}).get("qualified_contract") or {})
    pricing = dict(report.get("entry_execution_pricing") or delegated.get("entry_execution_pricing") or {})
    return {
        "managed_order_status": "KNOWN_LEAK_TEST_ENTRY_ORDER_WORKING",
        "source": "TRACK_B_PAPER_LEAK_TEST_BRIDGE_REPORT",
        "source_artifact_path": str(source_path),
        "strategy_id": metadata.get("strategy_id"),
        "lane_id": metadata.get("lane_id") or report.get("strategy_id"),
        "account_id": metadata.get("account_id") or report.get("account_id") or PAPER_ACCOUNT,
        "symbol": str(qualified.get("symbol") or report.get("symbol") or intent.get("symbol") or "").strip().upper() or None,
        "local_symbol": metadata.get("local_symbol") or qualified.get("local_symbol"),
        "expiry": qualified.get("expiry") or report.get("contract_month") or intent.get("contract_month"),
        "con_id": _int_or_none(metadata.get("con_id") or qualified.get("con_id")),
        "action": action.replace("_TO_OPEN", ""),
        "qty": _float_or_none(intent.get("quantity") or report.get("quantity") or metadata.get("quantity")) or 1.0,
        "quantity": _float_or_none(intent.get("quantity") or report.get("quantity") or metadata.get("quantity")) or 1.0,
        "order_type": intent.get("order_type") or report.get("order_type"),
        "limit_price": pricing.get("limit_price") or intent.get("limit_price") or report.get("limit_price"),
        "tif": intent.get("time_in_force") or report.get("time_in_force") or "DAY",
        "broker_order_id": str(broker_order_id),
        "client_id": _int_or_none(
            lifecycle.get("client_id") or delegated_report.get("client_id") or delegated.get("client_id")
        ),
        "perm_id": _int_or_none(lifecycle.get("perm_id") or delegated_report.get("perm_id") or delegated.get("perm_id")),
        "submitted_at": lifecycle.get("submitted_at") or delegated_report.get("submitted_at") or report.get("generated_at"),
        "reason": report.get("reason") or intent.get("reason") or "LEAK_TEST_ENTRY",
        "entry_execution_intent": pricing.get("entry_execution_intent") or report.get("entry_execution_intent"),
        "execution_price_source": pricing.get("execution_price_source"),
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }


def _known_leak_test_entry_orders(
    *,
    broker_open_orders: Sequence[Mapping[str, Any]],
    config: ReconciliationConfig,
    persisted_known_orders: Sequence[Mapping[str, Any]] = (),
    artifact_known_orders: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    declared_orders: list[dict[str, Any]] = []
    declared_orders.extend(dict(row) for row in persisted_known_orders if isinstance(row, Mapping))
    declared_orders.extend(dict(row) for row in artifact_known_orders if isinstance(row, Mapping))
    if not declared_orders:
        return []
    known: list[dict[str, Any]] = []
    seen: set[str] = set()
    for broker_order in broker_open_orders:
        for declared in declared_orders:
            if not _broker_order_matches_declared_leak_test_entry(broker_order=broker_order, declared=declared):
                continue
            row = dict(broker_order)
            row["managed_order_status"] = "KNOWN_LEAK_TEST_ENTRY_ORDER_WORKING"
            for key in (
                "strategy_id",
                "lane_id",
                "source",
                "source_artifact_path",
                "entry_execution_intent",
                "execution_price_source",
                "reason",
                "submitted_at",
            ):
                if _missing_value(row.get(key)) and not _missing_value(declared.get(key)):
                    row[key] = declared.get(key)
            _merge_missing_managed_exit_order_fields(row, declared)
            row["source"] = declared.get("source") or "TRACK_B_PAPER_LEAK_TEST_KNOWN_ENTRY_ORDER"
            row["known_identity"] = True
            order_id = _order_id_text(row)
            if order_id and order_id in seen:
                break
            if order_id:
                seen.add(order_id)
            known.append(row)
            break
    return known


def _broker_order_matches_declared_leak_test_entry(
    *,
    broker_order: Mapping[str, Any],
    declared: Mapping[str, Any],
) -> bool:
    return (
        _order_id_text(broker_order) == _order_id_text(declared)
        and _optional_int_matches(broker_order, declared, "client_id")
        and _optional_int_matches(broker_order, declared, "perm_id")
        and _optional_text_matches(broker_order, declared, "account_id")
        and _optional_text_matches(broker_order, declared, "symbol")
        and _optional_text_matches(broker_order, declared, "local_symbol")
        and _optional_int_matches(broker_order, declared, "con_id")
        and _optional_action_matches(broker_order, declared)
        and _optional_quantity_matches(broker_order, declared)
    )


def _merge_missing_managed_exit_order_fields(row: dict[str, Any], declared: Mapping[str, Any]) -> None:
    for key in (
        "submitted_at",
        "acknowledged_at",
        "lifecycle_id",
        "exit_reason",
        "reason_code",
        "hard_exit",
        "exit_family",
        "action",
        "order_action",
        "order_type",
        "limit_price",
        "stop_price",
        "tif",
        "time_in_force",
        "quantity",
        "con_id",
        "conId",
    ):
        if _missing_value(row.get(key)) and not _missing_value(declared.get(key)):
            row[key] = declared.get(key)


def _enrich_managed_exit_order_from_prepared_artifact(row: dict[str, Any], repo_root: Path) -> None:
    lane_id = str(row.get("lane_id") or "").strip()
    if not lane_id:
        return
    base = (
        repo_root
        / "outputs"
        / "reports"
        / "ibkr_runtime_route_dispatch"
        / lane_id
        / "prepared_manual_harness"
    )
    for filename in ("ibkr_manual_paper_close_test_frozen_preview.json", "ibkr_manual_paper_close_test_report.json"):
        payload = _load_json(base / filename)
        if not payload:
            continue
        requested_order = payload.get("requested_order")
        if not isinstance(requested_order, Mapping):
            requested_order = _nested_mapping(payload, "frozen_preview", "requested_order")
        if not requested_order:
            continue
        action = str(requested_order.get("action") or "").strip().upper()
        row_action = str(row.get("action") or "").strip().upper()
        if action and row_action and action != row_action:
            continue
        symbol = str(requested_order.get("symbol") or "").strip().upper()
        row_symbol = str(row.get("symbol") or "").strip().upper()
        if symbol and row_symbol and symbol != row_symbol:
            continue
        for source_key, target_key in (
            ("order_type", "order_type"),
            ("limit_price", "limit_price"),
            ("stop_price", "stop_price"),
            ("time_in_force", "tif"),
            ("quantity", "quantity"),
        ):
            if _missing_value(row.get(target_key)) and not _missing_value(requested_order.get(source_key)):
                row[target_key] = requested_order.get(source_key)
        row["order_price_source_artifact_path"] = str(base / filename)
        return


def _runtime_market_reference(*, config: ReconciliationConfig, root: str) -> dict[str, Any]:
    normalized_root = str(root or "").strip().upper()
    if not normalized_root:
        return {}
    path = config.market_data_root / normalized_root / "1m" / "latest_runtime_candles.json"
    payload = _load_json(path)
    if not payload:
        return {}
    candle = _latest_runtime_candle(payload)
    if not candle:
        return {}
    price = _decimal_value(candle.get("close") or candle.get("last") or candle.get("price"))
    if price is None:
        return {}
    return {
        "price": float(price),
        "bar_start": candle.get("bar_start"),
        "bar_end": candle.get("bar_end"),
        "generated_at": payload.get("generated_at"),
        "source_artifact_path": str(path),
    }


def _guarded_cancel_replace_proposal(row: Mapping[str, Any], policy: Mapping[str, Any]) -> dict[str, Any] | None:
    if policy.get("recommended_action") != "PREPARE_EXACT_CANCEL_REPLACE_FOR_KNOWN_MANAGED_ORDER":
        return None
    root = str(row.get("track_b_root") or row.get("symbol") or "").strip().upper()
    market_reference = _decimal_value(policy.get("runtime_market_reference"))
    if market_reference is None:
        return None
    tick = Decimal(str(DEFAULT_MIN_TICK_BY_ROOT.get(root, 0.25)))
    action = str(row.get("action") or "").strip().upper()
    if action == "SELL":
        replacement_limit = market_reference - tick
    elif action == "BUY":
        replacement_limit = market_reference + tick
    else:
        return None
    replacement_limit = _round_decimal_to_tick(replacement_limit, tick)
    return {
        "enabled": False,
        "requires_explicit_operator_authorization": True,
        "broker_mutation_performed": False,
        "allowed_route": "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY",
        "forbidden_routes": ["broad_cancel", "reqGlobalCancel", "paper_proof", "live_money"],
        "cancel_identity": {
            "account_id": row.get("account_id") or PAPER_ACCOUNT,
            "broker_order_id": row.get("broker_order_id"),
            "client_id": row.get("client_id"),
            "perm_id": row.get("perm_id"),
            "symbol": row.get("symbol"),
            "local_symbol": row.get("local_symbol") or row.get("localSymbol"),
            "expiry": row.get("expiry"),
            "con_id": row.get("con_id") or row.get("conId"),
            "action": row.get("action"),
            "quantity": row.get("quantity"),
        },
        "replacement_order": {
            "account_id": row.get("account_id") or PAPER_ACCOUNT,
            "symbol": row.get("symbol"),
            "local_symbol": row.get("local_symbol") or row.get("localSymbol"),
            "expiry": row.get("expiry"),
            "con_id": row.get("con_id") or row.get("conId"),
            "action": row.get("action"),
            "quantity": row.get("quantity"),
            "order_type": "LMT",
            "tif": row.get("tif") or row.get("time_in_force") or "DAY",
            "limit_price": float(replacement_limit),
            "price_source": "RUNTIME_MARKET_REFERENCE_PLUS_HARD_EXIT_ONE_TICK",
            "runtime_market_reference": float(market_reference),
            "min_tick": float(tick),
        },
    }


def _round_decimal_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return price
    ticks = (price / tick).to_integral_value()
    return ticks * tick


def _latest_runtime_candle(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    for key in ("candles", "bars"):
        rows = payload.get(key)
        if isinstance(rows, list) and rows:
            last = rows[-1]
            return last if isinstance(last, Mapping) else {}
    return payload


def _missing_value(value: Any) -> bool:
    return value is None or value == ""


def _managed_exit_order_from_restore_payload(
    payload: Mapping[str, Any],
    *,
    source_path: Path,
    symbols: Sequence[str],
) -> dict[str, Any] | None:
    restored = payload.get("restored_state_summary")
    if not isinstance(restored, Mapping):
        return None
    pending_order_ids = [
        str(item).strip()
        for item in restored.get("pending_broker_order_ids", []) or []
        if str(item).strip()
    ]
    open_order_id = str(restored.get("open_broker_order_id") or "").strip()
    order_id = open_order_id or (pending_order_ids[0] if pending_order_ids else "")
    if not order_id:
        return None
    latest_intent_state = str(restored.get("latest_order_intent_state") or "").strip().upper()
    if latest_intent_state in {"CANCELLED", "CANCELED", "REJECTED", "FILLED", "EXPIRED"}:
        return None
    latest_intent = payload.get("pre_restore_state_summary", {})
    latest_intent = latest_intent.get("latest_order_intent") if isinstance(latest_intent, Mapping) else {}
    if not isinstance(latest_intent, Mapping):
        latest_intent = {}
    intent_type = str(latest_intent.get("intent_type") or restored.get("last_order_intent_id") or "").upper()
    if "SELL_TO_CLOSE" not in intent_type and "BUY_TO_CLOSE" not in intent_type:
        return None
    symbol = str(latest_intent.get("symbol") or latest_intent.get("instrument") or payload.get("symbol") or payload.get("instrument") or "").strip().upper()
    if symbol and symbol not in {item.upper() for item in symbols}:
        return None
    broker_position = _first_mapping(
        _nested_mapping(restored, "broker_snapshot", "broker_truth_position"),
        _nested_mapping(payload, "pre_restore_state_summary", "broker_snapshot", "broker_truth_position"),
    )
    local_symbol = str(broker_position.get("local_symbol") or broker_position.get("localSymbol") or "").strip()
    expiry = str(broker_position.get("expiry") or "").strip()
    con_id = broker_position.get("con_id") or broker_position.get("conId")
    action = "SELL" if "SELL_TO_CLOSE" in intent_type else "BUY"
    return {
        "managed_order_status": "KNOWN_MANAGED_EXIT_ORDER_WORKING",
        "source": "TRACK_B_RUNTIME_RESTORE_PENDING_EXIT_ORDER",
        "source_artifact_path": str(source_path),
        "lifecycle_id": _matched_lifecycle_id_for_restore(payload, local_symbol=local_symbol),
        "strategy_id": latest_intent.get("standalone_strategy_id") or latest_intent.get("strategy_id"),
        "lane_id": latest_intent.get("lane_id") or payload.get("lane_id"),
        "order_intent_id": latest_intent.get("order_intent_id") or restored.get("last_order_intent_id"),
        "broker_order_id": order_id,
        "symbol": symbol,
        "local_symbol": local_symbol,
        "expiry": expiry,
        "con_id": con_id,
        "action": action,
        "quantity": latest_intent.get("quantity") or "1",
        "submitted_at": latest_intent.get("submitted_at") or latest_intent.get("acknowledged_at"),
        "exit_reason": latest_intent.get("reason_code"),
    }


def _matched_lifecycle_id_for_restore(payload: Mapping[str, Any], *, local_symbol: str) -> str | None:
    # Runtime restore artifacts are lane-local, so they may not duplicate the
    # lifecycle id. Prefer a direct value, then fall back to the latest fill for
    # the same restored position; the final broker/lifecycle match check still
    # verifies the open lifecycle position before this can become known-managed.
    restored = payload.get("restored_state_summary")
    if isinstance(restored, Mapping):
        lifecycle_id = str(restored.get("lifecycle_id") or "").strip()
        if lifecycle_id:
            return lifecycle_id
    pre_restore = payload.get("pre_restore_state_summary")
    latest_fill = pre_restore.get("latest_fill") if isinstance(pre_restore, Mapping) else None
    if isinstance(latest_fill, Mapping):
        lifecycle_id = str(latest_fill.get("lifecycle_id") or "").strip()
        if lifecycle_id:
            return lifecycle_id
        fill_local_symbol = str(latest_fill.get("local_symbol") or latest_fill.get("localSymbol") or "").strip().upper()
        if local_symbol and fill_local_symbol and local_symbol.upper() != fill_local_symbol:
            return None
    return None


def _declared_known_managed_exit_orders(lifecycle_status: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidates: list[Any] = []
    for key in ("known_managed_exit_orders", "pending_managed_exit_orders", "working_managed_exit_orders"):
        value = lifecycle_status.get(key)
        if isinstance(value, list):
            candidates.extend(value)
    for positions_key in ("positions_by_instrument", "positions_by_strategy"):
        positions = lifecycle_status.get(positions_key)
        if not isinstance(positions, Mapping):
            continue
        for position in positions.values():
            if not isinstance(position, Mapping):
                continue
            for key in ("known_managed_exit_orders", "pending_managed_exit_orders", "working_exit_orders"):
                value = position.get(key)
                if isinstance(value, list):
                    candidates.extend(value)
            single = position.get("pending_managed_exit_order")
            if isinstance(single, Mapping):
                candidates.append(single)
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            continue
        row = dict(candidate)
        status = str(row.get("managed_order_status") or row.get("status") or "").strip().upper()
        if status in {"CANCELLED", "CANCELED", "REJECTED", "FILLED", "EXPIRED"}:
            continue
        if not _order_id_text(row):
            continue
        rows.append(row)
    return rows


def _broker_order_matches_declared_managed_exit(*, broker_order: Mapping[str, Any], declared: Mapping[str, Any]) -> bool:
    broker_order_id = _order_id_text(broker_order)
    declared_order_id = _order_id_text(declared)
    if broker_order_id and declared_order_id and broker_order_id != declared_order_id:
        return False
    for broker_key, declared_key in (("client_id", "client_id"), ("perm_id", "perm_id")):
        broker_value = str(broker_order.get(broker_key) or broker_order.get(_camel_case(broker_key)) or "").strip()
        declared_value = str(declared.get(declared_key) or declared.get(_camel_case(declared_key)) or "").strip()
        if broker_value and declared_value and broker_value != declared_value:
            return False
    for key in ("symbol", "local_symbol", "expiry"):
        broker_value = str(broker_order.get(key) or broker_order.get(_camel_case(key)) or "").strip().upper()
        declared_value = str(declared.get(key) or declared.get(_camel_case(key)) or "").strip().upper()
        if broker_value and declared_value and broker_value != declared_value:
            return False
    broker_con_id = str(broker_order.get("con_id") or broker_order.get("conId") or broker_order.get("qualified_contract_identifier") or "").strip()
    declared_con_id = str(declared.get("con_id") or declared.get("conId") or declared.get("qualified_contract_identifier") or "").strip()
    if broker_con_id and declared_con_id and broker_con_id != declared_con_id:
        return False
    broker_qty = _decimal_value(
        broker_order.get("remaining_quantity")
        or broker_order.get("remainingQuantity")
        or broker_order.get("total_quantity")
        or broker_order.get("totalQuantity")
        or broker_order.get("quantity")
    )
    declared_qty = _decimal_value(declared.get("qty") or declared.get("quantity"))
    if broker_qty is not None and declared_qty is not None and abs(broker_qty) != abs(declared_qty):
        return False
    broker_action = str(broker_order.get("action") or broker_order.get("order_action") or "").strip().upper()
    declared_action = str(declared.get("action") or declared.get("order_action") or "").strip().upper()
    if broker_action and declared_action and broker_action != declared_action:
        return False
    return True


def _matched_position_for_declared_managed_exit(
    *,
    declared: Mapping[str, Any],
    matched_positions: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    lifecycle_id = str(declared.get("lifecycle_id") or "").strip()
    for match in matched_positions:
        lifecycle_position = match.get("lifecycle_position") if isinstance(match.get("lifecycle_position"), Mapping) else {}
        if lifecycle_id and str(lifecycle_position.get("lifecycle_id") or "").strip() != lifecycle_id:
            continue
        local_symbol = str(declared.get("local_symbol") or declared.get("localSymbol") or "").strip().upper()
        position_local_symbol = str(lifecycle_position.get("local_symbol") or lifecycle_position.get("localSymbol") or "").strip().upper()
        if local_symbol and position_local_symbol and local_symbol != position_local_symbol:
            continue
        return dict(match)
    return None


def _managed_exit_identity_from_match(match: Mapping[str, Any]) -> dict[str, Any]:
    broker_position = match.get("broker_position") if isinstance(match.get("broker_position"), Mapping) else {}
    lifecycle_position = match.get("lifecycle_position") if isinstance(match.get("lifecycle_position"), Mapping) else {}
    return {
        "lifecycle_id": lifecycle_position.get("lifecycle_id"),
        "con_id": lifecycle_position.get("con_id") or lifecycle_position.get("conId") or broker_position.get("con_id") or broker_position.get("conId"),
        "local_symbol": lifecycle_position.get("local_symbol") or lifecycle_position.get("localSymbol") or broker_position.get("local_symbol") or broker_position.get("localSymbol"),
        "expiry": lifecycle_position.get("expiry") or broker_position.get("expiry"),
        "symbol": lifecycle_position.get("instrument_family") or broker_position.get("symbol"),
    }


def _unknown_track_b_open_orders(
    *,
    broker_open_orders: Sequence[Mapping[str, Any]],
    known_managed_exit_orders: Sequence[Mapping[str, Any]],
    known_leak_test_entry_orders: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    known_ids = {_order_id_text(row) for row in known_managed_exit_orders if _order_id_text(row)}
    known_ids.update(_order_id_text(row) for row in known_leak_test_entry_orders if _order_id_text(row))
    unknown: list[dict[str, Any]] = []
    for row in broker_open_orders:
        if _order_id_text(row) in known_ids:
            continue
        unknown.append(dict(row))
    return unknown


def _order_id_text(row: Mapping[str, Any]) -> str:
    return str(row.get("broker_order_id") or row.get("order_id") or row.get("orderId") or "").strip()


def _camel_case(key: str) -> str:
    parts = key.split("_")
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


def _write_reconciled_summaries(
    *,
    config: ReconciliationConfig,
    now: datetime,
    trade_summary: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    pnl_summary: Mapping[str, Any],
    report: Mapping[str, Any],
    positions_path: Path,
    open_orders_path: Path,
    broker_positions: Sequence[Mapping[str, Any]],
) -> None:
    broker_position_count = len(broker_positions)
    reconciled_state = "BROKER_AND_LIFECYCLE_OPEN_MATCHED" if broker_position_count else "BROKER_AND_LIFECYCLE_FLAT"
    broker_cost_basis_adjustments = [
        dict(item)
        for item in report.get("broker_cost_basis_adjustments", [])
        if isinstance(item, Mapping)
    ]
    broker_truth_warning = (
        "Broker read-only truth agrees with Track B lifecycle open-position state."
        if broker_position_count
        else "Broker read-only truth agrees with Track B lifecycle flat state."
    )
    common = {
        "source": "BROKER_RECONCILED",
        "broker_reconciled": True,
        "broker_reconciliation_classification": report.get("classification"),
        "broker_reconciliation_report_path": str(config.report_path),
        "broker_truth_status_path": str(config.broker_status_path),
        "broker_positions_snapshot_path": str(positions_path),
        "broker_open_orders_snapshot_path": str(open_orders_path),
        "last_broker_reconciliation_time": now.isoformat(),
        "base_source": "TRACK_B_LIFECYCLE_ARTIFACTS",
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }
    reconciled_trade = dict(trade_summary)
    reconciled_trade.update(common)
    reconciled_trade.update(
        {
            "as_of": now.isoformat(),
            "latest_trade_summary_path": str(config.reconciled_trade_summary_path),
            "latest_live_position_status_path": str(config.reconciled_live_position_status_path),
            "latest_pnl_summary_path": str(config.reconciled_pnl_summary_path),
            "broker_truth_warning": broker_truth_warning,
        }
    )
    reconciled_positions = dict(live_position_status)
    reconciled_positions.update(common)
    reconciled_positions.update(
        {
            "as_of": now.isoformat(),
            "account_id": config.account,
            "latest_live_position_status_path": str(config.reconciled_live_position_status_path),
            "broker_truth_warning": broker_truth_warning,
            "broker_track_b_position_count": broker_position_count,
            "broker_track_b_open_order_count": int(report.get("track_b_broker_open_order_count") or 0),
            "known_managed_exit_order_count": int(report.get("known_managed_exit_order_count") or 0),
            "known_leak_test_entry_order_count": int(report.get("known_leak_test_entry_order_count") or 0),
            "stale_managed_exit_order_count": int(report.get("stale_managed_exit_order_count") or 0),
            "hard_exit_order_not_marketable_count": int(report.get("hard_exit_order_not_marketable_count") or 0),
            "known_managed_exit_orders": [dict(item) for item in report.get("known_managed_exit_orders", []) if isinstance(item, Mapping)],
            "known_leak_test_entry_orders": [dict(item) for item in report.get("known_leak_test_entry_orders", []) if isinstance(item, Mapping)],
            "stale_managed_exit_orders": [dict(item) for item in report.get("stale_managed_exit_orders", []) if isinstance(item, Mapping)],
            "broker_reconciled_state": reconciled_state,
            "broker_track_b_positions": [dict(item) for item in broker_positions],
            "broker_cost_basis_adjustments": broker_cost_basis_adjustments,
        }
    )
    reconciled_pnl = dict(pnl_summary)
    reconciled_pnl.update(common)
    reconciled_pnl.update(
        {
            "as_of": now.isoformat(),
            "latest_pnl_summary_path": str(config.reconciled_pnl_summary_path),
        }
    )
    _write_json_atomic(config.reconciled_trade_summary_path, reconciled_trade)
    _write_json_atomic(config.reconciled_live_position_status_path, reconciled_positions)
    _write_json_atomic(config.reconciled_pnl_summary_path, reconciled_pnl)


def _track_b_lifecycle_positions(live_position_status: Mapping[str, Any], symbols: Sequence[str]) -> list[dict[str, Any]]:
    rows = live_position_status.get("positions_by_instrument")
    if not isinstance(rows, Mapping):
        return []
    matches: list[dict[str, Any]] = []
    for key, value in rows.items():
        if not isinstance(value, Mapping):
            continue
        item = dict(value)
        item.setdefault("position_key", key)
        root = _track_b_root(item, symbols)
        if root is None:
            root = _track_b_root({"instrument": key, "contract_key": key}, symbols)
        if root is None:
            continue
        qty = _decimal_value(item.get("quantity"))
        if qty is None or qty == 0:
            continue
        lifecycle_state = normalize_lifecycle_state(
            item.get("final_position_status")
            or item.get("lifecycle_status")
            or item.get("paper_lifecycle_classification")
            or item.get("strategy_managed_lifecycle_classification")
        )
        if lifecycle_state and not is_registry_eligible(lifecycle_state):
            continue
        item["track_b_root"] = root
        matches.append(item)
    return matches


def _broker_lifecycle_position_match(
    *,
    broker_positions: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
    symbols: Sequence[str],
) -> dict[str, Any]:
    if not broker_positions and not lifecycle_positions:
        return {"matched": True, "state": "BROKER_AND_LIFECYCLE_FLAT", "matches": []}
    count_mismatch = len(broker_positions) != len(lifecycle_positions)

    unmatched_lifecycle = [dict(item) for item in lifecycle_positions]
    matches: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    for broker_position in broker_positions:
        broker_root = _track_b_root(broker_position, symbols)
        broker_qty = _decimal_value(broker_position.get("quantity"))
        candidate_matches: list[tuple[int, dict[str, Any]]] = []
        for index, lifecycle_position in enumerate(unmatched_lifecycle):
            lifecycle_root = _track_b_root(lifecycle_position, symbols)
            lifecycle_qty = _decimal_value(lifecycle_position.get("quantity"))
            lifecycle_signed_qty = _signed_lifecycle_quantity(lifecycle_position, lifecycle_qty)
            root_matches = broker_root is not None and broker_root == lifecycle_root
            local_matches = _local_symbols_compatible(broker_position, lifecycle_position)
            quantity_matches = broker_qty is not None and lifecycle_signed_qty is not None and broker_qty == lifecycle_signed_qty
            if root_matches and local_matches and quantity_matches:
                match_detail = {
                    "root": broker_root,
                    "broker_local_symbol": broker_position.get("local_symbol") or broker_position.get("localSymbol"),
                    "lifecycle_local_symbol": lifecycle_position.get("local_symbol") or lifecycle_position.get("localSymbol"),
                    "quantity": str(broker_qty),
                    "lifecycle_aggregate_qty": lifecycle_position.get("aggregate_qty"),
                    "lifecycle_unit_count": lifecycle_position.get("lifecycle_unit_count")
                    or len(lifecycle_position.get("lifecycle_units") or []),
                    "lifecycle_ids": lifecycle_position.get("lifecycle_ids") or [],
                    "duplicate_same_lane_exposure": lifecycle_position.get("duplicate_same_lane_exposure") is True,
                    "pyramiding_allowed": lifecycle_position.get("pyramiding_allowed") is True,
                    "broker_position": dict(broker_position),
                    "lifecycle_position": dict(lifecycle_position),
                }
                cost_basis_adjustment = _broker_cost_basis_adjustment(
                    broker_position=broker_position,
                    lifecycle_position=lifecycle_position,
                    signed_quantity=broker_qty,
                    root=broker_root,
                )
                if cost_basis_adjustment is not None:
                    match_detail["broker_cost_basis_adjustment"] = cost_basis_adjustment
                candidate_matches.append((index, match_detail))
        if candidate_matches:
            match_index, match_detail = max(
                candidate_matches,
                key=lambda item: _broker_lifecycle_match_score(
                    broker_position=broker_position,
                    lifecycle_position=item[1]["lifecycle_position"],
                ),
            )
        else:
            match_index = None
            match_detail = None
        if match_index is None or match_detail is None:
            mismatches.append({"broker_position": dict(broker_position), "unmatched_lifecycle_positions": unmatched_lifecycle})
            continue
        matches.append(match_detail)
        unmatched_lifecycle.pop(match_index)

    superseded_unmatched_lifecycle = _superseded_unmatched_lifecycle_positions(
        unmatched_lifecycle=unmatched_lifecycle,
        matches=matches,
        symbols=symbols,
    )
    superseded_keys = {
        _lifecycle_position_scope_key(row)
        for row in superseded_unmatched_lifecycle
        if _lifecycle_position_scope_key(row)
    }
    current_unmatched_lifecycle = [
        row for row in unmatched_lifecycle if _lifecycle_position_scope_key(row) not in superseded_keys
    ]
    current_lifecycle_count = len(lifecycle_positions) - len(superseded_unmatched_lifecycle)
    count_mismatch = len(broker_positions) != current_lifecycle_count

    if count_mismatch or mismatches or current_unmatched_lifecycle:
        blocker_code = (
            "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH"
            if count_mismatch
            else "TRACK_B_BROKER_LIFECYCLE_POSITION_DETAIL_MISMATCH"
        )
        state = "BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" if count_mismatch else "BROKER_LIFECYCLE_POSITION_DETAIL_MISMATCH"
        detail = (
            "IBKR broker truth and Track B lifecycle report different Track B open-position counts."
            if count_mismatch
            else "IBKR broker truth open positions do not exactly match Track B lifecycle open positions."
        )
        return {
            "matched": False,
            "state": state,
            "broker": [dict(item) for item in broker_positions],
            "lifecycle": [dict(item) for item in lifecycle_positions],
            "current_scope_lifecycle": [
                dict(item)
                for item in lifecycle_positions
                if _lifecycle_position_scope_key(item) not in superseded_keys
            ],
            "matches": matches,
            "mismatches": mismatches,
            "unmatched_lifecycle_positions": current_unmatched_lifecycle,
            "superseded_unmatched_lifecycle_positions": superseded_unmatched_lifecycle,
            "blocker": {
                "code": blocker_code,
                "detail": detail,
                "broker_position_count": len(broker_positions),
                "lifecycle_position_count": len(lifecycle_positions),
                "current_scope_lifecycle_position_count": current_lifecycle_count,
                "broker_positions": [dict(item) for item in broker_positions],
                "lifecycle_positions": [dict(item) for item in lifecycle_positions],
                "matches": matches,
                "mismatches": mismatches,
                "unmatched_lifecycle_positions": current_unmatched_lifecycle,
                "superseded_unmatched_lifecycle_positions": superseded_unmatched_lifecycle,
            },
        }
    return {
        "matched": True,
        "state": "BROKER_AND_LIFECYCLE_OPEN_MATCHED",
        "matches": matches,
        "superseded_unmatched_lifecycle_positions": superseded_unmatched_lifecycle,
    }


def _append_owner_superseded_lifecycle_positions_to_match_report(
    *,
    position_match_report: Mapping[str, Any],
    owner_superseded_lifecycle_positions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    payload = dict(position_match_report)
    existing = [dict(row) for row in payload.get("superseded_unmatched_lifecycle_positions") or [] if isinstance(row, Mapping)]
    seen = {_lifecycle_position_scope_key(row) for row in existing if _lifecycle_position_scope_key(row)}
    for row in owner_superseded_lifecycle_positions:
        raw = row.get("raw_lifecycle_position") if isinstance(row.get("raw_lifecycle_position"), Mapping) else row
        if not isinstance(raw, Mapping):
            continue
        item = dict(raw)
        item.setdefault("classification", row.get("classification") or "STALE_SUPERSEDED_LIFECYCLE_PROJECTION")
        item.setdefault("reason_codes", list(row.get("reason_codes") or []))
        item.setdefault("superseding_lifecycle_id", row.get("owner_lifecycle_id"))
        item.setdefault("superseding_trade_id", row.get("owner_trade_id"))
        key = _lifecycle_position_scope_key(item)
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        existing.append(item)
    payload["superseded_unmatched_lifecycle_positions"] = existing
    if "blocker" in payload and isinstance(payload.get("blocker"), Mapping):
        blocker = dict(payload["blocker"])
        blocker["superseded_unmatched_lifecycle_positions"] = existing
        payload["blocker"] = blocker
    return payload


def _superseded_unmatched_lifecycle_positions(
    *,
    unmatched_lifecycle: Sequence[Mapping[str, Any]],
    matches: Sequence[Mapping[str, Any]],
    symbols: Sequence[str],
) -> list[dict[str, Any]]:
    superseded: list[dict[str, Any]] = []
    if not matches:
        return superseded
    for lifecycle_position in unmatched_lifecycle:
        for match in matches:
            broker_position = match.get("broker_position") if isinstance(match.get("broker_position"), Mapping) else {}
            matched_lifecycle = match.get("lifecycle_position") if isinstance(match.get("lifecycle_position"), Mapping) else {}
            if not broker_position or not matched_lifecycle:
                continue
            if not _lifecycle_rows_compete_for_same_broker_position(
                stale_lifecycle=lifecycle_position,
                matched_lifecycle=matched_lifecycle,
                broker_position=broker_position,
                symbols=symbols,
            ):
                continue
            matched_score = _broker_lifecycle_match_score(
                broker_position=broker_position,
                lifecycle_position=matched_lifecycle,
            )
            stale_score = _broker_lifecycle_match_score(
                broker_position=broker_position,
                lifecycle_position=lifecycle_position,
            )
            if matched_score <= stale_score:
                continue
            if not _lifecycle_position_has_broker_backed_identity(matched_lifecycle):
                continue
            if not _lifecycle_position_is_weaker_broker_identity(lifecycle_position, broker_position):
                continue
            payload = dict(lifecycle_position)
            payload["classification"] = "STALE_SUPERSEDED_LIFECYCLE_PROJECTION"
            payload["reason_codes"] = [
                "CURRENT_BROKER_POSITION_MATCHED_TO_STRONGER_BROKER_BACKED_LIFECYCLE",
                "WEAKER_LIFECYCLE_ROW_HAS_NO_CURRENT_BROKER_IDENTITY_MATCH",
            ]
            payload["superseding_lifecycle_id"] = matched_lifecycle.get("lifecycle_id")
            payload["superseding_trade_id"] = matched_lifecycle.get("trade_id")
            payload["broker_position"] = dict(broker_position)
            superseded.append(payload)
            break
    return superseded


def _lifecycle_rows_compete_for_same_broker_position(
    *,
    stale_lifecycle: Mapping[str, Any],
    matched_lifecycle: Mapping[str, Any],
    broker_position: Mapping[str, Any],
    symbols: Sequence[str],
) -> bool:
    stale_root = _track_b_root(stale_lifecycle, symbols)
    matched_root = _track_b_root(matched_lifecycle, symbols)
    broker_root = _track_b_root(broker_position, symbols)
    if broker_root and (stale_root != broker_root or matched_root != broker_root):
        return False
    if not (
        _local_symbols_compatible(broker_position, stale_lifecycle)
        and _local_symbols_compatible(broker_position, matched_lifecycle)
    ):
        return False
    broker_qty = _decimal_value(broker_position.get("quantity"))
    stale_qty = _signed_lifecycle_quantity(stale_lifecycle, _decimal_value(stale_lifecycle.get("quantity")))
    matched_qty = _signed_lifecycle_quantity(matched_lifecycle, _decimal_value(matched_lifecycle.get("quantity")))
    return broker_qty is not None and stale_qty == broker_qty and matched_qty == broker_qty


def _lifecycle_position_has_broker_backed_identity(lifecycle_position: Mapping[str, Any]) -> bool:
    identity = _nested_mapping(lifecycle_position, "entry_broker_identity")
    evidence_fields = (
        lifecycle_position.get("entry_exec_id"),
        lifecycle_position.get("exec_id"),
        identity.get("exec_id"),
        lifecycle_position.get("entry_perm_id"),
        lifecycle_position.get("perm_id"),
        identity.get("perm_id"),
        lifecycle_position.get("entry_order_id"),
        lifecycle_position.get("order_id"),
        identity.get("order_id"),
    )
    return any(str(value or "").strip() for value in evidence_fields)


def _lifecycle_position_is_weaker_broker_identity(
    lifecycle_position: Mapping[str, Any],
    broker_position: Mapping[str, Any],
) -> bool:
    if not _lifecycle_account_matches_broker(lifecycle_position, broker_position):
        return True
    if not _lifecycle_identity_matches_broker(lifecycle_position, broker_position):
        return True
    price_distance = -_negative_price_distance(broker_position=broker_position, lifecycle_position=lifecycle_position)
    return price_distance > Decimal("1")


def _lifecycle_position_scope_key(row: Mapping[str, Any]) -> str:
    lifecycle_id = str(row.get("lifecycle_id") or "").strip()
    trade_id = str(row.get("trade_id") or "").strip()
    if lifecycle_id or trade_id:
        return f"{trade_id}|{lifecycle_id}"
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").strip().upper()
    con_id = str(row.get("con_id") or row.get("conId") or "").strip()
    side = str(row.get("side") or row.get("position_side") or "").strip().upper()
    qty = str(row.get("quantity") or "").strip()
    entry = str(row.get("entry_timestamp") or row.get("as_of") or "").strip()
    return f"{con_id}|{local_symbol}|{side}|{qty}|{entry}"


def _broker_cost_basis_adjustments_from_match_report(match_report: Mapping[str, Any]) -> list[dict[str, Any]]:
    matches = match_report.get("matches")
    if not isinstance(matches, list):
        return []
    adjustments: list[dict[str, Any]] = []
    for match in matches:
        if not isinstance(match, Mapping):
            continue
        adjustment = match.get("broker_cost_basis_adjustment")
        if isinstance(adjustment, Mapping):
            adjustments.append(dict(adjustment))
    return adjustments


def _broker_cost_basis_adjustment(
    *,
    broker_position: Mapping[str, Any],
    lifecycle_position: Mapping[str, Any],
    signed_quantity: Decimal | None,
    root: str | None,
) -> dict[str, Any] | None:
    broker_average = _broker_average_price(broker_position)
    lifecycle_average = _decimal_value(
        lifecycle_position.get("avg_entry_price")
        or lifecycle_position.get("average_entry_price")
        or lifecycle_position.get("entry_price")
    )
    quantity = abs(signed_quantity) if signed_quantity is not None else _decimal_value(lifecycle_position.get("quantity"))
    if broker_average is None or lifecycle_average is None or quantity is None or quantity == 0:
        return None
    broker_minus_lifecycle = broker_average - lifecycle_average
    total_points = broker_minus_lifecycle * quantity
    return {
        "source": "IBKR_AVERAGE_PRICE_MINUS_TRACK_B_LIFECYCLE_ENTRY_PRICE",
        "root": root,
        "broker_local_symbol": broker_position.get("local_symbol") or broker_position.get("localSymbol"),
        "lifecycle_local_symbol": lifecycle_position.get("local_symbol") or lifecycle_position.get("localSymbol"),
        "quantity": str(quantity),
        "lifecycle_average_entry_price": str(lifecycle_average),
        "broker_average_price": str(broker_average),
        "broker_minus_lifecycle_points_per_contract": str(broker_minus_lifecycle),
        "broker_minus_lifecycle_points_total": str(total_points),
        "absolute_points_per_contract": str(abs(broker_minus_lifecycle)),
        "absolute_points_total": str(abs(total_points)),
        "note": "Captured for broker fee/cost-basis tracking only; IBKR broker truth remains authoritative for live PAPER position state.",
    }


def _broker_lifecycle_match_score(
    *,
    broker_position: Mapping[str, Any],
    lifecycle_position: Mapping[str, Any],
) -> tuple[int, int, Decimal, str]:
    return (
        1 if _lifecycle_account_matches_broker(lifecycle_position, broker_position) else 0,
        1 if _lifecycle_identity_matches_broker(lifecycle_position, broker_position) else 0,
        _negative_price_distance(broker_position=broker_position, lifecycle_position=lifecycle_position),
        str(lifecycle_position.get("as_of") or lifecycle_position.get("entry_timestamp") or ""),
    )


def _lifecycle_account_matches_broker(lifecycle_position: Mapping[str, Any], broker_position: Mapping[str, Any]) -> bool:
    expected = str(broker_position.get("account_id") or broker_position.get("account") or "").strip()
    if not expected:
        return False
    candidates = [
        lifecycle_position.get("account_id"),
        _nested_mapping(lifecycle_position, "entry_broker_identity").get("account_id"),
    ]
    for unit in lifecycle_position.get("lifecycle_units") or []:
        if isinstance(unit, Mapping):
            candidates.append(unit.get("account_id"))
    return any(str(candidate or "").strip() == expected for candidate in candidates)


def _lifecycle_identity_matches_broker(lifecycle_position: Mapping[str, Any], broker_position: Mapping[str, Any]) -> bool:
    broker_con_id = str(broker_position.get("con_id") or broker_position.get("conId") or "").strip()
    lifecycle_con_id = str(
        lifecycle_position.get("con_id")
        or lifecycle_position.get("conId")
        or _nested_mapping(lifecycle_position, "entry_broker_identity").get("con_id")
        or ""
    ).strip()
    broker_local = str(broker_position.get("local_symbol") or broker_position.get("localSymbol") or "").strip().upper()
    lifecycle_local = str(
        lifecycle_position.get("local_symbol")
        or lifecycle_position.get("localSymbol")
        or _nested_mapping(lifecycle_position, "entry_broker_identity").get("local_symbol")
        or ""
    ).strip().upper()
    return bool(broker_con_id and lifecycle_con_id and broker_con_id == lifecycle_con_id) or bool(
        broker_local and lifecycle_local and broker_local == lifecycle_local
    )


def _negative_price_distance(
    *,
    broker_position: Mapping[str, Any],
    lifecycle_position: Mapping[str, Any],
) -> Decimal:
    broker_average = _broker_average_price(broker_position)
    lifecycle_average = _decimal_value(
        lifecycle_position.get("avg_entry_price")
        or lifecycle_position.get("average_entry_price")
        or lifecycle_position.get("entry_price")
    )
    if broker_average is None or lifecycle_average is None:
        return Decimal("-999999")
    return -abs(broker_average - lifecycle_average)


def _broker_average_price(position: Mapping[str, Any]) -> Decimal | None:
    average_price = _decimal_value(position.get("average_price") or position.get("avg_entry_price"))
    if average_price is not None:
        return average_price
    average_cost = _decimal_value(position.get("average_cost"))
    multiplier = _decimal_value(position.get("multiplier"))
    if average_cost is None:
        return None
    if multiplier is None or multiplier == 0:
        return average_cost
    return average_cost / multiplier


def _signed_lifecycle_quantity(position: Mapping[str, Any], quantity: Decimal | None) -> Decimal | None:
    if quantity is None:
        return None
    side = str(position.get("side") or position.get("position_side") or "").strip().upper()
    if side == "SHORT" and quantity > 0:
        return -quantity
    return quantity


def _local_symbols_compatible(broker_position: Mapping[str, Any], lifecycle_position: Mapping[str, Any]) -> bool:
    broker_local = str(broker_position.get("local_symbol") or broker_position.get("localSymbol") or "").strip().upper()
    lifecycle_local = str(lifecycle_position.get("local_symbol") or lifecycle_position.get("localSymbol") or "").strip().upper()
    if broker_local and lifecycle_local:
        return broker_local == lifecycle_local
    return True


def _track_b_broker_positions(snapshot: Mapping[str, Any], symbols: Sequence[str]) -> list[dict[str, Any]]:
    rows = snapshot.get("positions") if isinstance(snapshot.get("positions"), list) else []
    matches: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        root = _track_b_root(row, symbols)
        if root is None:
            continue
        qty = _decimal_value(row.get("quantity"))
        if qty is None or qty == 0:
            continue
        item = dict(row)
        item["track_b_root"] = root
        matches.append(item)
    return matches


def _track_b_broker_open_orders(snapshot: Mapping[str, Any], symbols: Sequence[str]) -> list[dict[str, Any]]:
    rows = snapshot.get("open_orders") if isinstance(snapshot.get("open_orders"), list) else []
    matches: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        root = _track_b_root(row, symbols)
        if root is None:
            contract = row.get("contract")
            root = _track_b_root(contract, symbols) if isinstance(contract, Mapping) else None
        if root is None:
            continue
        item = dict(row)
        item["track_b_root"] = root
        matches.append(item)
    return matches


def _track_b_root(row: Mapping[str, Any], symbols: Sequence[str]) -> str | None:
    ordered = sorted((symbol.upper() for symbol in symbols), key=len, reverse=True)
    fields = (
        row.get("symbol"),
        row.get("root"),
        row.get("instrument"),
        row.get("contract_key"),
        row.get("local_symbol"),
        row.get("localSymbol"),
    )
    text_fields = [str(value).upper().replace(" ", "") for value in fields if value not in (None, "")]
    for symbol in ordered:
        for text in text_fields:
            if text == symbol or text.startswith(symbol):
                return symbol
    return None


def _parse_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_seconds(value: Any, now: datetime) -> float | None:
    parsed = _parse_time(value)
    if parsed is None:
        return None
    return round(max((now - parsed).total_seconds(), 0.0), 3)


def _decimal_value(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    decimal = _decimal_value(value)
    return None if decimal is None else float(decimal)


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_int_matches(left: Mapping[str, Any], right: Mapping[str, Any], key: str) -> bool:
    left_value = _int_or_none(left.get(key) or left.get(_camel_case(key)))
    right_value = _int_or_none(right.get(key) or right.get(_camel_case(key)))
    return left_value is None or right_value is None or left_value == right_value


def _optional_text_matches(left: Mapping[str, Any], right: Mapping[str, Any], key: str) -> bool:
    left_value = str(left.get(key) or left.get(_camel_case(key)) or "").strip().upper()
    right_value = str(right.get(key) or right.get(_camel_case(key)) or "").strip().upper()
    return not left_value or not right_value or left_value == right_value


def _optional_action_matches(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_action = str(left.get("action") or left.get("order_action") or "").strip().upper()
    right_action = str(right.get("action") or right.get("order_action") or "").strip().upper()
    right_action = right_action.replace("_TO_OPEN", "").replace("_TO_CLOSE", "")
    left_action = left_action.replace("_TO_OPEN", "").replace("_TO_CLOSE", "")
    return not left_action or not right_action or left_action == right_action


def _optional_quantity_matches(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_quantity = _decimal_value(
        left.get("quantity")
        or left.get("qty")
        or left.get("total_quantity")
        or left.get("totalQuantity")
    )
    right_quantity = _decimal_value(
        right.get("quantity")
        or right.get("qty")
        or right.get("total_quantity")
        or right.get("totalQuantity")
    )
    return left_quantity is None or right_quantity is None or abs(left_quantity) == abs(right_quantity)


def _max_int(*values: Any) -> int:
    return max(_int_value(value) for value in values)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _first_mapping(*values: object) -> dict[str, Any]:
    for value in values:
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _nested_mapping(value: object, *keys: str) -> dict[str, Any]:
    current: object = value
    for key in keys:
        if not isinstance(current, Mapping):
            return {}
        current = current.get(key)
    return dict(current) if isinstance(current, Mapping) else {}


def _path_from_payload(value: Any, *, default: Path) -> Path:
    if value in (None, ""):
        return default
    return Path(str(value))


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(Path(path), payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconcile Track B PAPER lifecycle summaries with read-only IBKR broker truth.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--ledger-root", default=None)
    parser.add_argument("--broker-truth-root", default=None)
    parser.add_argument("--market-data-root", default=None)
    parser.add_argument("--report-path", default=None)
    parser.add_argument("--max-age-seconds", type=float, default=DEFAULT_MAX_AGE_SECONDS)
    parser.add_argument("--bridge-terminal-event-grace-seconds", type=float, default=DEFAULT_BRIDGE_TERMINAL_EVENT_GRACE_SECONDS)
    parser.add_argument("--broker-truth-settlement-seconds", type=float, default=DEFAULT_BROKER_TRUTH_SETTLEMENT_SECONDS)
    parser.add_argument("--broker-truth-settlement-poll-seconds", type=float, default=DEFAULT_BROKER_TRUTH_SETTLEMENT_POLL_SECONDS)
    parser.add_argument("--account", default=PAPER_ACCOUNT)
    parser.add_argument("--symbols", default=",".join(PHASE1_RUNTIME_TICKER_ORDER))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root)
    config = ReconciliationConfig(
        repo_root=repo_root,
        ledger_root=Path(args.ledger_root) if args.ledger_root else repo_root / "outputs" / "track_b_execution_core" / "paper_trade_ledger",
        broker_truth_root=Path(args.broker_truth_root) if args.broker_truth_root else repo_root / "outputs" / "reports" / "ibkr_read_only_verification",
        market_data_root=Path(args.market_data_root)
        if args.market_data_root
        else repo_root / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data",
        report_path=Path(args.report_path)
        if args.report_path
        else repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        max_age_seconds=args.max_age_seconds,
        bridge_terminal_event_grace_seconds=args.bridge_terminal_event_grace_seconds,
        broker_truth_settlement_seconds=args.broker_truth_settlement_seconds,
        broker_truth_settlement_poll_seconds=args.broker_truth_settlement_poll_seconds,
        account=str(args.account),
        symbols=tuple(symbol.strip().upper() for symbol in str(args.symbols).split(",") if symbol.strip()),
    )
    report = reconcile_track_b_paper_broker_truth(config=config)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("broker_reconciled") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
