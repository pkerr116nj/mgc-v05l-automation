"""Guarded Track B strategy-managed PAPER lifecycle v1.

This module is the strategy lifecycle boundary for real Track B PAPER signals.
It is intentionally separate from paper-proof/canary flows: proof verifies
connectivity, while this lifecycle records strategy-owned entry/open/exit
state and refuses to mutate unless an explicit managed broker adapter and exit
policy are supplied.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping

from .ibkr_paper_adapter import IbkrPaperAdapter
from .models import require_aware_datetime, to_jsonable
from .models import IntentKind, OrderIntent, PositionState, SubmitAttempt, SubmitAttemptState
from .preflight import ReadOnlyPreflightConfig
from .track_b_paper_autonomous_recovery_planner import DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT
from .track_b_pre_action_snapshot_validator import (
    DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT,
    PRE_ACTION_BLOCKED_HARD_INVARIANT,
    PRE_ACTION_BLOCKED_PLAN_MISMATCH,
    PRE_ACTION_BLOCKED_SNAPSHOT_MISSING,
    PRE_ACTION_BLOCKED_SUPERVISOR_MISMATCH,
    PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH,
    PRE_ACTION_SNAPSHOT_VALID,
    TrackBPreActionSnapshotValidatorConfig,
    validate_track_b_pre_action_snapshot,
)
from .track_b_lifecycle_state_transition import validate_open_managed_evidence
from .track_b_open_order_truth import (
    BROKER_FLAT_WITH_OPEN_CLOSE_ORDER,
    CLOSE_ORDER_MARKETABLE_NOT_FILLED,
    CLOSE_ORDER_STALE,
    DUPLICATE_CLOSE_ORDER,
    OPEN_CLOSE_ORDER_WORKING,
    SUSPICIOUS_ORDER_STATE,
    TrackBOpenOrderTruthConfig,
    build_track_b_open_order_truth_from_reconciliation,
)
from .track_b_paper_trade_ledger import DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT
from .track_b_runtime_safe_state_envelope import DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
from .track_b_runtime_supervisor_authority import DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT


DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT = Path(
    "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
)
REPO_ROOT = Path(__file__).resolve().parents[3]
CLOSE_ORDER_ALREADY_WORKING = "CLOSE_ORDER_ALREADY_WORKING"

STRATEGY_SUBMIT_AUTHORIZED = "STRATEGY_SUBMIT_AUTHORIZED"
STRATEGY_SUBMIT_BLOCKED_NO_SNAPSHOT = "STRATEGY_SUBMIT_BLOCKED_NO_SNAPSHOT"
STRATEGY_SUBMIT_BLOCKED_SAFE_STATE = "STRATEGY_SUBMIT_BLOCKED_SAFE_STATE"
STRATEGY_SUBMIT_BLOCKED_SUPERVISOR = "STRATEGY_SUBMIT_BLOCKED_SUPERVISOR"
STRATEGY_SUBMIT_BLOCKED_RUNTIME_GENERATION = "STRATEGY_SUBMIT_BLOCKED_RUNTIME_GENERATION"
STRATEGY_SUBMIT_BLOCKED_ORDER_TRUTH = "STRATEGY_SUBMIT_BLOCKED_ORDER_TRUTH"
STRATEGY_SUBMIT_BLOCKED_POSITION_TRUTH = "STRATEGY_SUBMIT_BLOCKED_POSITION_TRUTH"
STRATEGY_SUBMIT_BLOCKED_LIVE_MONEY = "STRATEGY_SUBMIT_BLOCKED_LIVE_MONEY"
STRATEGY_SUBMIT_BLOCKED_PAPER_PROOF = "STRATEGY_SUBMIT_BLOCKED_PAPER_PROOF"
STRATEGY_SUBMIT_BLOCKED_TARGET_MISMATCH = "STRATEGY_SUBMIT_BLOCKED_TARGET_MISMATCH"

PLAN_STRATEGY_MANAGED_ENTRY_SUBMIT = "PLAN_STRATEGY_MANAGED_ENTRY_SUBMIT"
PLAN_STRATEGY_MANAGED_CLOSE_SUBMIT = "PLAN_STRATEGY_MANAGED_CLOSE_SUBMIT"
ACTION_STRATEGY_MANAGED_ENTRY_SUBMIT = "STRATEGY_MANAGED_ENTRY_SUBMIT"
ACTION_STRATEGY_MANAGED_CLOSE_SUBMIT = "STRATEGY_MANAGED_CLOSE_SUBMIT"


class TrackBManagedPaperLifecycleClassification(str, Enum):
    OPEN_MANAGED = "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED"
    EXIT_PENDING = "TRACK_B_STRATEGY_PAPER_EXIT_PENDING"
    CLOSED_FLAT = "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT"
    REVIEW_REQUIRED = "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED"
    BROKER_MISMATCH = "TRACK_B_STRATEGY_PAPER_BROKER_MISMATCH"
    EXIT_POLICY_MISSING = "TRACK_B_STRATEGY_PAPER_EXIT_POLICY_MISSING"
    LIFECYCLE_NOT_AVAILABLE = "TRACK_B_STRATEGY_PAPER_LIFECYCLE_NOT_AVAILABLE"
    BLOCKED_EXISTING_REVIEW_REQUIRED = "TRACK_B_STRATEGY_PAPER_BLOCKED_EXISTING_REVIEW_REQUIRED"


class TrackBManagedExitPolicy(str, Enum):
    EXIT_NOT_AVAILABLE = "EXIT_NOT_AVAILABLE"
    MANAGED_HOLD_REQUIRES_EXTERNAL_EXIT_SIGNAL = "MANAGED_HOLD_REQUIRES_EXTERNAL_EXIT_SIGNAL"
    DIAGNOSTIC_TIME_EXIT_IMMEDIATE = "DIAGNOSTIC_TIME_EXIT_IMMEDIATE"
    PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1 = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"


@dataclass(frozen=True)
class TrackBStrategyManagedPaperLifecycleConfig:
    mode: str
    account_id: str
    expected_account_id: str
    strategy_id: str
    instrument_family: str
    contract_key: str
    local_symbol: str
    con_id: int | None
    side: str
    quantity: int | None
    signal_timestamp: str | None = None
    signal_reason: str | None = None
    decision_bar_timestamp: str | None = None
    latest_decision_bar_source: str | None = None
    pricing_policy: str | None = None
    reference_price_source: str | None = None
    reference_price: str | Decimal | None = None
    entry_limit_price: str | Decimal | None = None
    close_limit_price: str | Decimal | None = None
    managed_exit_policy_id: str | None = None
    managed_exit_policy_max_completed_5m_bars: int = 3
    completed_5m_bars_since_entry: int | None = None
    completed_5m_bars_since_signal: int | None = None
    fill_timestamp_source: str | None = None
    data_freshness_state: str = "FRESH"
    broker_truth_state: str = "FRESH"
    suppress_discretionary_exits_due_to_stale_data: bool = False
    block_new_entries_due_to_stale_data: bool = False
    submit_enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 17086
    order_type: str = "LMT"
    time_in_force: str = "DAY"
    exchange: str = "COMEX"
    currency: str = "USD"
    tick_size: str = "0.1"
    source_id: str = "track_b_strategy_managed_paper_lifecycle"
    output_root: Path = DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT
    paper_trade_ledger_output_root: Path = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT
    live_position_status_json: Path | None = None
    live_money_readiness: bool = False
    broker_reconciled: bool = False
    repo_root: Path = REPO_ROOT
    lane_id: str | None = None
    runtime_generation_id: str | None = None
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    autonomous_recovery_plan_path: Path = DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT
    runtime_supervisor_authority_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT
    runtime_safe_state_envelope_path: Path = DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
    pre_action_snapshot_max_age_seconds: int = 300
    expected_control_plane_snapshot_id: str | None = None
    expected_shared_truth_generation_id: str | None = None


@dataclass(frozen=True)
class TrackBStrategyManagedPaperLifecycleStages:
    entry_submitter: Callable[[TrackBStrategyManagedPaperLifecycleConfig, Mapping[str, Any]], Mapping[str, Any]]
    exit_policy: Callable[
        [TrackBStrategyManagedPaperLifecycleConfig, Mapping[str, Any]],
        Mapping[str, Any] | None,
    ]
    close_submitter: Callable[[TrackBStrategyManagedPaperLifecycleConfig, Mapping[str, Any]], Mapping[str, Any]]


@dataclass(frozen=True)
class TrackBStrategyManagedPaperLifecycleResult:
    lifecycle_id: str
    classification: TrackBManagedPaperLifecycleClassification
    report_json: Path
    report: dict[str, Any]


def default_managed_lifecycle_stages() -> TrackBStrategyManagedPaperLifecycleStages:
    return TrackBStrategyManagedPaperLifecycleStages(
        entry_submitter=_default_entry_submitter,
        exit_policy=_default_exit_policy,
        close_submitter=_default_close_submitter,
    )


def run_track_b_strategy_managed_paper_lifecycle(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    stages: TrackBStrategyManagedPaperLifecycleStages | None = None,
    lifecycle_id: str | None = None,
    now: datetime | None = None,
) -> TrackBStrategyManagedPaperLifecycleResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_lifecycle_id = lifecycle_id or f"strategy_managed_{uuid.uuid4().hex}"
    report_json = Path(config.output_root) / actual_lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"
    actual_stages = stages or default_managed_lifecycle_stages()

    entry_intent = _entry_intent(config=config, lifecycle_id=actual_lifecycle_id, now=actual_now)
    close_intent: Mapping[str, Any] | None = None
    entry_submit: Mapping[str, Any] | None = None
    close_submit: Mapping[str, Any] | None = None
    entry_fill: Mapping[str, Any] | None = None
    close_fill: Mapping[str, Any] | None = None
    open_state: Mapping[str, Any] | None = None
    broker_state_mutated = False
    submit_attempted = False
    primary_blocker: str | None = None
    required_next_action = "No broker mutation occurred."

    guard_blocker = _guard_blocker(config)
    existing_review = _existing_review_required_blocker(config)
    stale_entry_blocker = _stale_new_entry_blocker(config)
    exit_policy_id = _normalized_exit_policy(config.managed_exit_policy_id)
    if guard_blocker:
        classification = TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
        primary_blocker = guard_blocker
        required_next_action = "Resolve managed PAPER lifecycle guard before retrying."
    elif existing_review:
        classification = TrackBManagedPaperLifecycleClassification.BLOCKED_EXISTING_REVIEW_REQUIRED
        primary_blocker = existing_review
        required_next_action = "Resolve the existing review-required PAPER position before new managed entries."
    elif stale_entry_blocker:
        classification = TrackBManagedPaperLifecycleClassification.LIFECYCLE_NOT_AVAILABLE
        primary_blocker = stale_entry_blocker
        required_next_action = "Wait for Track B market/broker truth freshness to recover before new managed entries."
    elif exit_policy_id in {"", TrackBManagedExitPolicy.EXIT_NOT_AVAILABLE.value}:
        classification = TrackBManagedPaperLifecycleClassification.EXIT_POLICY_MISSING
        primary_blocker = f"{config.strategy_id} has no managed PAPER exit policy."
        required_next_action = "Add an explicit managed exit policy before allowing strategy-managed PAPER entry."
    else:
        try:
            entry_submit = dict(actual_stages.entry_submitter(config, entry_intent))
            submit_attempted = bool(entry_submit.get("submitted") or entry_submit.get("submit_attempted"))
            broker_state_mutated = bool(entry_submit.get("broker_state_mutated"))
            entry_fill = _mapping(entry_submit.get("entry_fill") or entry_submit.get("fill"))
            if entry_submit.get("review_required") is True:
                classification = TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
                primary_blocker = str(entry_submit.get("primary_blocker") or "Managed entry submit requires review.")
                required_next_action = "Review managed entry submit diagnostics before retrying."
            elif not submit_attempted:
                classification = TrackBManagedPaperLifecycleClassification.LIFECYCLE_NOT_AVAILABLE
                primary_blocker = str(entry_submit.get("primary_blocker") or "Managed broker submit adapter is not configured.")
                required_next_action = "Configure the guarded managed PAPER lifecycle adapter before retrying."
            elif not entry_fill:
                classification = TrackBManagedPaperLifecycleClassification.EXIT_PENDING
                primary_blocker = str(entry_submit.get("primary_blocker") or "Managed entry submit has no fill yet.")
                required_next_action = "Monitor managed entry order state; do not submit another entry for this lifecycle."
            else:
                open_state = _open_state(config=config, lifecycle_id=actual_lifecycle_id, entry_intent=entry_intent, entry_fill=entry_fill)
                close_intent = actual_stages.exit_policy(config, open_state)
                if close_intent is None:
                    classification = TrackBManagedPaperLifecycleClassification.OPEN_MANAGED
                    required_next_action = "Position is open under strategy-managed PAPER state; wait for strategy exit policy."
                else:
                    close_submit = dict(actual_stages.close_submitter(config, close_intent))
                    submit_attempted = submit_attempted or bool(close_submit.get("submitted") or close_submit.get("submit_attempted"))
                    broker_state_mutated = broker_state_mutated or bool(close_submit.get("broker_state_mutated"))
                    close_fill = _mapping(close_submit.get("close_fill") or close_submit.get("fill"))
                    if close_fill:
                        classification = TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
                        required_next_action = "Managed PAPER lifecycle closed flat."
                    else:
                        classification = TrackBManagedPaperLifecycleClassification.EXIT_PENDING
                        primary_blocker = str(close_submit.get("primary_blocker") or "Managed close submit has no fill yet.")
                        required_next_action = "Monitor managed close order state and reconcile before another handoff."
        except Exception as exc:  # noqa: BLE001 - lifecycle errors must become artifacts.
            classification = TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
            primary_blocker = f"Managed PAPER lifecycle stage error: {exc}"
            required_next_action = "Review managed lifecycle diagnostics before retrying."

    report = _build_report(
        config=config,
        lifecycle_id=actual_lifecycle_id,
        now=actual_now,
        classification=classification,
        entry_intent=entry_intent,
        entry_submit=entry_submit,
        entry_fill=entry_fill,
        open_state=open_state,
        close_intent=close_intent,
        close_submit=close_submit,
        close_fill=close_fill,
        submit_attempted=submit_attempted,
        broker_state_mutated=broker_state_mutated,
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
        report_json=report_json,
    )
    _write_report(report_json, report)
    return TrackBStrategyManagedPaperLifecycleResult(
        lifecycle_id=actual_lifecycle_id,
        classification=classification,
        report_json=report_json,
        report=report,
    )


def maintain_open_track_b_strategy_managed_paper_lifecycle(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    existing_lifecycle_report: Mapping[str, Any],
    stages: TrackBStrategyManagedPaperLifecycleStages | None = None,
    now: datetime | None = None,
) -> TrackBStrategyManagedPaperLifecycleResult:
    """Re-evaluate exit policy for an existing OPEN_MANAGED lifecycle.

    This path never submits another entry. It only consumes an existing
    broker-confirmed entry fill and, if the configured managed exit policy is
    eligible, sends the close leg through the guarded strategy-managed lifecycle.
    """

    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    lifecycle_id = str(existing_lifecycle_report.get("lifecycle_id") or "")
    if not lifecycle_id:
        lifecycle_id = f"strategy_managed_{uuid.uuid4().hex}"
    report_json = Path(config.output_root) / lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"
    actual_stages = stages or default_managed_lifecycle_stages()

    entry_intent = _mapping(existing_lifecycle_report.get("entry_intent")) or _entry_intent(
        config=config,
        lifecycle_id=lifecycle_id,
        now=actual_now,
    )
    entry_submit = _mapping(existing_lifecycle_report.get("entry_submit_attempt"))
    entry_fill = _mapping(existing_lifecycle_report.get("entry_fill"))
    close_intent: Mapping[str, Any] | None = None
    close_submit: Mapping[str, Any] | None = None
    close_fill: Mapping[str, Any] | None = None
    open_state: Mapping[str, Any] | None = None
    submit_attempted = bool((entry_submit or {}).get("submitted") or (entry_submit or {}).get("submit_attempted"))
    broker_state_mutated = False
    primary_blocker: str | None = None
    required_next_action = "Position is open under strategy-managed PAPER state; wait for strategy exit policy."

    guard_blocker = _guard_blocker(config)
    exit_policy_id = _normalized_exit_policy(config.managed_exit_policy_id)
    if guard_blocker:
        classification = TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
        primary_blocker = guard_blocker
        required_next_action = "Resolve managed PAPER lifecycle guard before retrying close maintenance."
    elif exit_policy_id in {"", TrackBManagedExitPolicy.EXIT_NOT_AVAILABLE.value}:
        classification = TrackBManagedPaperLifecycleClassification.EXIT_POLICY_MISSING
        primary_blocker = f"{config.strategy_id} has no managed PAPER exit policy."
        required_next_action = "Add an explicit managed exit policy before allowing strategy-managed PAPER close maintenance."
    elif not entry_fill:
        classification = TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
        primary_blocker = "Existing OPEN_MANAGED lifecycle has no broker-confirmed entry fill."
        required_next_action = "Review lifecycle artifacts before attempting managed close."
    else:
        try:
            open_state = _open_state(
                config=config,
                lifecycle_id=lifecycle_id,
                entry_intent=entry_intent,
                entry_fill=entry_fill,
            )
            close_intent = actual_stages.exit_policy(config, open_state)
            if close_intent is None:
                classification = TrackBManagedPaperLifecycleClassification.OPEN_MANAGED
            else:
                close_submit = dict(actual_stages.close_submitter(config, close_intent))
                submit_attempted = submit_attempted or bool(close_submit.get("submitted") or close_submit.get("submit_attempted"))
                broker_state_mutated = bool(close_submit.get("broker_state_mutated"))
                close_fill = _mapping(close_submit.get("close_fill") or close_submit.get("fill"))
                if close_submit.get("review_required") is True:
                    classification = TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
                    primary_blocker = str(close_submit.get("primary_blocker") or "Managed close submit requires review.")
                    required_next_action = "Review managed close diagnostics before retrying."
                elif close_fill:
                    classification = TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
                    required_next_action = "Managed PAPER lifecycle closed flat."
                else:
                    classification = TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
                    primary_blocker = str(close_submit.get("primary_blocker") or "Managed close submit/fill was not confirmed.")
                    required_next_action = "Review managed close order state and reconcile before another handoff."
        except Exception as exc:  # noqa: BLE001 - maintenance errors must become artifacts.
            classification = TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
            primary_blocker = f"Managed PAPER lifecycle close maintenance error: {exc}"
            required_next_action = "Review managed lifecycle diagnostics before retrying."

    report = _build_report(
        config=config,
        lifecycle_id=lifecycle_id,
        now=actual_now,
        classification=classification,
        entry_intent=entry_intent,
        entry_submit=entry_submit,
        entry_fill=entry_fill,
        open_state=open_state,
        close_intent=close_intent,
        close_submit=close_submit,
        close_fill=close_fill,
        submit_attempted=submit_attempted,
        broker_state_mutated=broker_state_mutated,
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
        report_json=report_json,
    )
    report["maintenance_invoked"] = True
    report["maintenance_generated_at"] = actual_now.isoformat()
    _write_report(report_json, report)
    return TrackBStrategyManagedPaperLifecycleResult(
        lifecycle_id=lifecycle_id,
        classification=classification,
        report_json=report_json,
        report=report,
    )


def write_open_managed_lifecycle_report_from_filled_bridge_result(
    *,
    filled_bridge_result: Mapping[str, Any],
    output_root: Path = DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT,
    now: datetime | None = None,
) -> Path | None:
    """Persist an OPEN_MANAGED lifecycle report for a direct bridge entry fill.

    The runtime bridge can submit entries without invoking the managed lifecycle
    runner. Once the broker fill is known, this creates the lifecycle artifact
    that managed open-position maintenance uses for deterministic exits.
    """

    intent_type = str(filled_bridge_result.get("intent_type") or "").upper()
    if intent_type not in {"BUY_TO_OPEN", "SELL_TO_OPEN"}:
        return None
    if filled_bridge_result.get("paper_proof_invoked") is True:
        return None
    if filled_bridge_result.get("live_money_readiness") is True:
        return None
    policy_id = str(filled_bridge_result.get("managed_exit_policy_id") or "").strip()
    if not policy_id:
        return None
    order_intent_id = str(filled_bridge_result.get("order_intent_id") or "").strip()
    if not order_intent_id:
        return None
    contract = filled_bridge_result.get("contract") if isinstance(filled_bridge_result.get("contract"), Mapping) else {}
    instrument = str(
        filled_bridge_result.get("instrument")
        or filled_bridge_result.get("symbol")
        or contract.get("symbol")
        or ""
    ).upper()
    contract_key = str(
        filled_bridge_result.get("contract_key")
        or contract.get("contract_key")
        or _contract_key_from_bridge_payload(filled_bridge_result)
        or ""
    )
    local_symbol = str(filled_bridge_result.get("local_symbol") or contract.get("local_symbol") or "")
    con_id = _int_or_none(
        filled_bridge_result.get("con_id")
        or contract.get("qualified_contract_identifier")
        or contract.get("con_id")
    )
    quantity = _int_or_none(filled_bridge_result.get("quantity"))
    account_id = str(filled_bridge_result.get("account_id") or "DUM882026")
    if not all([instrument, contract_key, local_symbol, account_id]) or con_id is None or quantity is None:
        return None
    open_managed_transition = validate_open_managed_evidence(
        {
            "entry_intent_id": order_intent_id,
            "lane_id": filled_bridge_result.get("lane_id"),
            "strategy_id": filled_bridge_result.get("strategy_id") or filled_bridge_result.get("lane_id"),
            "contract_key": contract_key,
            "local_symbol": local_symbol,
            "con_id": con_id,
            "side": "LONG" if intent_type == "BUY_TO_OPEN" else "SHORT",
            "quantity": quantity,
            "lifecycle_id": filled_bridge_result.get("lifecycle_id") or f"bridge_fill_{order_intent_id}",
            "managed_exit_policy_id": policy_id,
            "broker_order_id": filled_bridge_result.get("broker_order_id"),
            "perm_id": filled_bridge_result.get("perm_id"),
            "fill_price": filled_bridge_result.get("fill_price") or filled_bridge_result.get("entry_fill_price"),
            "fill_timestamp": filled_bridge_result.get("fill_timestamp") or filled_bridge_result.get("entry_timestamp"),
        }
    )
    if not open_managed_transition.open_managed_allowed:
        return None
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    side = "LONG" if intent_type == "BUY_TO_OPEN" else "SHORT"
    lifecycle_id = str(filled_bridge_result.get("lifecycle_id") or f"bridge_fill_{order_intent_id}")
    report_json = Path(output_root) / lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"
    config = TrackBStrategyManagedPaperLifecycleConfig(
        mode="PAPER",
        account_id=account_id,
        expected_account_id=str(filled_bridge_result.get("expected_account_id") or account_id),
        strategy_id=str(filled_bridge_result.get("strategy_id") or filled_bridge_result.get("lane_id") or ""),
        instrument_family=instrument,
        contract_key=contract_key,
        local_symbol=local_symbol,
        con_id=con_id,
        side=side,
        quantity=quantity,
        signal_timestamp=str(filled_bridge_result.get("decision_bar_timestamp") or "") or None,
        decision_bar_timestamp=str(filled_bridge_result.get("decision_bar_timestamp") or "") or None,
        latest_decision_bar_source="DATABENTO_LIVE_ARTIFACT",
        managed_exit_policy_id=policy_id,
        managed_exit_policy_max_completed_5m_bars=3,
        completed_5m_bars_since_entry=0,
        completed_5m_bars_since_signal=0,
        fill_timestamp_source="BROKER_ENTRY_FILL",
        submit_enabled=True,
        output_root=Path(output_root),
        live_money_readiness=False,
        broker_reconciled=False,
    )
    entry_intent = _entry_intent(config=config, lifecycle_id=lifecycle_id, now=actual_now)
    entry_submit = {
        "submitted": True,
        "submit_attempted": True,
        "broker_state_mutated": True,
        "broker_order_id": filled_bridge_result.get("broker_order_id"),
        "perm_id": filled_bridge_result.get("perm_id"),
        "client_id": filled_bridge_result.get("client_id"),
        "submitted_at": filled_bridge_result.get("created_at") or actual_now.isoformat(),
        "submit_attempt_id": filled_bridge_result.get("submit_attempt_id"),
        "submit_diagnostics": {
            "canonical_broker_contract_fields": {
                "symbol": instrument,
                "contract_key": contract_key,
                "local_symbol": local_symbol,
                "con_id": con_id,
            },
        },
    }
    entry_fill = {
        "broker_order_id": filled_bridge_result.get("broker_order_id"),
        "perm_id": filled_bridge_result.get("perm_id"),
        "execution_id": filled_bridge_result.get("exec_id") or filled_bridge_result.get("execution_id"),
        "price": _decimal_text(filled_bridge_result.get("fill_price") or filled_bridge_result.get("entry_fill_price")),
        "quantity": _decimal_text(filled_bridge_result.get("quantity")),
        "filled_at": filled_bridge_result.get("fill_timestamp") or filled_bridge_result.get("entry_timestamp"),
    }
    open_state = _open_state(
        config=config,
        lifecycle_id=lifecycle_id,
        entry_intent=entry_intent,
        entry_fill=entry_fill,
    )
    report = _build_report(
        config=config,
        lifecycle_id=lifecycle_id,
        now=actual_now,
        classification=TrackBManagedPaperLifecycleClassification.OPEN_MANAGED,
        entry_intent=entry_intent,
        entry_submit=entry_submit,
        entry_fill=entry_fill,
        open_state=open_state,
        close_intent=None,
        close_submit=None,
        close_fill=None,
        submit_attempted=True,
        broker_state_mutated=True,
        primary_blocker=None,
        required_next_action="Position is open under strategy-managed PAPER state; wait for strategy exit policy.",
        report_json=report_json,
    )
    manifest_path = filled_bridge_result.get("position_management_manifest_path")
    if manifest_path:
        report["position_management_manifest_path"] = str(manifest_path)
    _write_report(report_json, report)
    return report_json


def _entry_intent(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    lifecycle_id: str,
    now: datetime,
) -> dict[str, Any]:
    action = _order_action(config.side)
    return {
        "intent_schema_version": "track_b_strategy_managed_paper_entry_intent_v1",
        "created_at": now.isoformat(),
        "lifecycle_id": lifecycle_id,
        "trade_id": f"{config.strategy_id}:{lifecycle_id}",
        "strategy_id": config.strategy_id,
        "instrument_family": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "account_id": config.account_id,
        "expected_account_id": config.expected_account_id,
        "side": _normalized_side(config.side),
        "order_action": action,
        "quantity": config.quantity,
        "signal_timestamp": config.signal_timestamp,
        "signal_reason": config.signal_reason,
        "decision_bar_timestamp": config.decision_bar_timestamp,
        "latest_decision_bar_source": config.latest_decision_bar_source,
        "pricing_policy": config.pricing_policy,
        "reference_price_source": config.reference_price_source,
        "reference_price": _decimal_text(config.reference_price),
        "entry_limit_price": _decimal_text(config.entry_limit_price),
        "managed_exit_policy_id": _normalized_exit_policy(config.managed_exit_policy_id),
        "source_id": config.source_id,
    }


def _open_state(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    lifecycle_id: str,
    entry_intent: Mapping[str, Any],
    entry_fill: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "state_schema_version": "track_b_strategy_managed_paper_open_state_v1",
        "lifecycle_id": lifecycle_id,
        "trade_id": entry_intent.get("trade_id"),
        "strategy_id": config.strategy_id,
        "instrument_family": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "side": entry_intent.get("side"),
        "quantity": config.quantity,
        "entry_price": _decimal_text(entry_fill.get("price") or entry_fill.get("avg_price")),
        "entry_timestamp": entry_fill.get("filled_at") or entry_fill.get("timestamp"),
        "current_state": TrackBManagedPaperLifecycleClassification.OPEN_MANAGED.value,
        "open_position_age_completed_5m_bars": int(config.completed_5m_bars_since_entry or 0),
        "bars_since_fill": int(config.completed_5m_bars_since_entry or 0),
        "bars_since_signal": None if config.completed_5m_bars_since_signal is None else int(config.completed_5m_bars_since_signal),
        "fill_timestamp_source": config.fill_timestamp_source or "BROKER_ENTRY_FILL",
        "data_freshness_state": str(config.data_freshness_state or "FRESH"),
        "broker_truth_state": str(config.broker_truth_state or "FRESH"),
        "suppressed_due_to_stale_data": False,
        "managed_exit_policy_id": _normalized_exit_policy(config.managed_exit_policy_id),
        "managed_exit_policy_max_completed_5m_bars": int(config.managed_exit_policy_max_completed_5m_bars),
        "expected_exit_condition": _expected_exit_condition(config),
        "broker_reconciled": bool(config.broker_reconciled),
        "review_required": False,
    }


def _build_report(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    lifecycle_id: str,
    now: datetime,
    classification: TrackBManagedPaperLifecycleClassification,
    entry_intent: Mapping[str, Any],
    entry_submit: Mapping[str, Any] | None,
    entry_fill: Mapping[str, Any] | None,
    open_state: Mapping[str, Any] | None,
    close_intent: Mapping[str, Any] | None,
    close_submit: Mapping[str, Any] | None,
    close_fill: Mapping[str, Any] | None,
    submit_attempted: bool,
    broker_state_mutated: bool,
    primary_blocker: str | None,
    required_next_action: str,
    report_json: Path,
) -> dict[str, Any]:
    realized = _realized_pnl(entry_intent.get("side"), config.quantity, entry_fill, close_fill)
    review_required = classification in {
        TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED,
        TrackBManagedPaperLifecycleClassification.BROKER_MISMATCH,
        TrackBManagedPaperLifecycleClassification.BLOCKED_EXISTING_REVIEW_REQUIRED,
    }
    return {
        "schema_version": "track_b_strategy_managed_paper_lifecycle_v1",
        "generated_at": now.isoformat(),
        "lifecycle_id": lifecycle_id,
        "trade_id": entry_intent.get("trade_id"),
        "strategy_id": config.strategy_id,
        "instrument_family": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "account_id": config.account_id,
        "expected_account_id": config.expected_account_id,
        "mode": config.mode,
        "managed_exit_policy_id": _normalized_exit_policy(config.managed_exit_policy_id),
        "managed_exit_policy_max_completed_5m_bars": int(config.managed_exit_policy_max_completed_5m_bars),
        "open_position_age_completed_5m_bars": None
        if open_state is None
        else open_state.get("open_position_age_completed_5m_bars"),
        "bars_since_fill": None if open_state is None else open_state.get("bars_since_fill"),
        "bars_since_signal": None if open_state is None else open_state.get("bars_since_signal"),
        "fill_timestamp_source": None if open_state is None else open_state.get("fill_timestamp_source"),
        "mfe": None,
        "mae": None,
        "data_freshness_state": str(config.data_freshness_state or "FRESH"),
        "broker_truth_state": str(config.broker_truth_state or "FRESH"),
        "suppressed_due_to_stale_data": bool(config.suppress_discretionary_exits_due_to_stale_data and close_intent is None),
        "expected_exit_condition": _expected_exit_condition(config),
        "close_intent_status": _close_intent_status(classification, close_intent),
        "strategy_managed_lifecycle_classification": classification.value,
        "paper_lifecycle_classification": classification.value,
        "submit_enabled": bool(config.submit_enabled),
        "managed_paper_submit_enabled": bool(config.submit_enabled),
        "managed_submit_blocked_reason": _managed_submit_blocked_reason(config, entry_submit, primary_blocker),
        "ibkr_adapter_available": True,
        "contract_fields_submitted_to_ibkr": _submit_diagnostic_value(entry_submit, "contract_fields_submitted_to_ibkr"),
        "canonical_broker_contract_fields": _submit_diagnostic_value(entry_submit, "canonical_broker_contract_fields"),
        "contract_consistency_check_passed": _submit_diagnostic_value(entry_submit, "contract_consistency_check_passed"),
        "contract_mismatch_reason": _submit_diagnostic_value(entry_submit, "contract_mismatch_reason"),
        "pre_submit_blocked": _submit_diagnostic_value(entry_submit, "pre_submit_blocked"),
        "ibkr_error_code": _submit_error_value(entry_submit, "ibkr_error_code"),
        "ibkr_error_message": _submit_error_value(entry_submit, "ibkr_error_message"),
        "entry_intent": dict(entry_intent),
        "entry_submit_attempt": dict(entry_submit) if entry_submit else None,
        "entry_fill": dict(entry_fill) if entry_fill else None,
        "close_intent": dict(close_intent) if close_intent else None,
        "close_submit_attempt": dict(close_submit) if close_submit else None,
        "close_fill": dict(close_fill) if close_fill else None,
        "realized_pnl": _decimal_text(realized),
        "pnl_currency": "USD",
        "final_position_status": _final_position_status(classification),
        "final_broker_state_classification": classification.value,
        "review_required": review_required,
        "broker_reconciled": bool(config.broker_reconciled),
        "source": "TRACK_B_STRATEGY_MANAGED_LIFECYCLE",
        "submit_allowed": bool(submit_attempted and not review_required),
        "submit_attempted": bool(submit_attempted),
        "paper_proof_invoked": False,
        "paper_proof_cli_called": False,
        "broker_state_mutated": bool(broker_state_mutated),
        "live_money_readiness": False,
        "ui_authority": False,
        "hidden_submit": False,
        "primary_blocker": primary_blocker,
        "required_next_action": required_next_action,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_track_b_strategy_managed_paper_lifecycle_report.json"),
    }


def _guard_blocker(config: TrackBStrategyManagedPaperLifecycleConfig) -> str | None:
    if str(config.mode).upper() != "PAPER":
        return "Strategy-managed PAPER lifecycle requires mode=PAPER."
    if config.live_money_readiness is True:
        return "Strategy-managed PAPER lifecycle requires live_money_readiness=false."
    if config.account_id != config.expected_account_id:
        return f"Account guard failed: {config.account_id} != expected {config.expected_account_id}."
    if str(config.latest_decision_bar_source or "") != "DATABENTO_LIVE_ARTIFACT":
        return "Strategy-managed PAPER lifecycle requires latest decision bar source DATABENTO_LIVE_ARTIFACT."
    if config.quantity is None or config.quantity <= 0:
        return "Strategy-managed PAPER lifecycle requires positive configured quantity."
    if not config.local_symbol:
        return "Strategy-managed PAPER lifecycle requires an allowlisted local symbol."
    return None


def _stale_new_entry_blocker(config: TrackBStrategyManagedPaperLifecycleConfig) -> str | None:
    if config.block_new_entries_due_to_stale_data is not True:
        return None
    return (
        "Track B staged stale-data model blocks new managed entries: "
        f"data_freshness_state={config.data_freshness_state}; broker_truth_state={config.broker_truth_state}."
    )


def _managed_submit_blocked_reason(
    config: TrackBStrategyManagedPaperLifecycleConfig,
    entry_submit: Mapping[str, Any] | None,
    primary_blocker: str | None,
) -> str | None:
    if config.submit_enabled is not True:
        return "SUBMIT_DISABLED"
    if entry_submit and entry_submit.get("managed_submit_blocked_reason"):
        return str(entry_submit.get("managed_submit_blocked_reason"))
    blocker = str(primary_blocker or "")
    if "review-required" in blocker:
        return "REVIEW_REQUIRED_BLOCKED_SUBMIT"
    if "Account guard" in blocker or "local symbol" in blocker or "conId" in blocker:
        return "ACCOUNT_OR_CONTRACT_GUARD_BLOCKED"
    if "not enabled" in blocker:
        return "SUBMIT_DISABLED"
    return None


def _submit_diagnostic_value(entry_submit: Mapping[str, Any] | None, key: str) -> Any:
    diagnostics = dict((entry_submit or {}).get("submit_diagnostics") or {})
    return diagnostics.get(key)


def _submit_error_value(entry_submit: Mapping[str, Any] | None, key: str) -> Any:
    if entry_submit is None:
        return None
    if key in entry_submit:
        return entry_submit.get(key)
    return _ibkr_error_summary(dict(entry_submit.get("submit_diagnostics") or {})).get(key)


def _ibkr_error_summary(diagnostics: Mapping[str, Any]) -> dict[str, Any]:
    errors = list(diagnostics.get("error_callbacks_after_submit") or [])
    if not errors:
        return {"ibkr_error_code": None, "ibkr_error_message": None}
    first = dict(errors[0])
    return {
        "ibkr_error_code": first.get("error_code"),
        "ibkr_error_message": first.get("error_string"),
    }


def _existing_review_required_blocker(config: TrackBStrategyManagedPaperLifecycleConfig) -> str | None:
    status_path = Path(config.live_position_status_json) if config.live_position_status_json else Path(config.paper_trade_ledger_output_root) / "latest_track_b_live_position_status.json"
    try:
        value = json.loads(status_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    if not isinstance(value, Mapping):
        return None
    if int(value.get("review_required_count") or 0) > 0:
        return f"Existing review-required PAPER lifecycle in {status_path} blocks new managed entries."
    positions = value.get("positions")
    if isinstance(positions, list):
        for item in positions:
            if isinstance(item, Mapping) and item.get("review_required") is True:
                return f"Existing review-required PAPER position in {status_path} blocks new managed entries."
    return None


def _default_entry_submitter(
    config: TrackBStrategyManagedPaperLifecycleConfig,
    entry_intent: Mapping[str, Any],
) -> Mapping[str, Any]:
    if config.submit_enabled is not True:
        return {
            "submitted": False,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "entry_intent": dict(entry_intent),
            "managed_submit_blocked_reason": "SUBMIT_DISABLED",
            "primary_blocker": "Managed PAPER submit is not enabled for this lifecycle invocation.",
        }
    return _submit_managed_limit_order(
        config=config,
        intent_payload=entry_intent,
        intent_kind=IntentKind.OPEN,
        limit_price=config.entry_limit_price,
        submit_index=1,
    )


def _default_exit_policy(
    config: TrackBStrategyManagedPaperLifecycleConfig,
    open_state: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    policy_id = _normalized_exit_policy(config.managed_exit_policy_id)
    if _discretionary_exits_suppressed(config, policy_id):
        return None
    if policy_id == TrackBManagedExitPolicy.DIAGNOSTIC_TIME_EXIT_IMMEDIATE.value:
        return {
            "intent_schema_version": "track_b_strategy_managed_paper_close_intent_v1",
            "lifecycle_id": open_state.get("lifecycle_id"),
            "trade_id": open_state.get("trade_id"),
            "strategy_id": config.strategy_id,
            "account_id": config.account_id,
            "contract_key": config.contract_key,
            "local_symbol": config.local_symbol,
            "con_id": config.con_id,
            "side": open_state.get("side"),
            "order_action": "SELL" if open_state.get("side") == "LONG" else "BUY",
            "quantity": config.quantity,
            "close_limit_price": _decimal_text(config.close_limit_price),
            "exit_family": "DIAGNOSTIC_TIME",
            "close_reason": "DIAGNOSTIC_TIME_EXIT_IMMEDIATE",
            "hard_exit": False,
            "discretionary_exit": True,
            "bars_since_fill": int(config.completed_5m_bars_since_entry or 0),
            "bars_since_signal": None if config.completed_5m_bars_since_signal is None else int(config.completed_5m_bars_since_signal),
            "fill_timestamp_source": config.fill_timestamp_source or "BROKER_ENTRY_FILL",
            "mfe": None,
            "mae": None,
            "data_freshness_state": str(config.data_freshness_state or "FRESH"),
            "broker_truth_state": str(config.broker_truth_state or "FRESH"),
            "suppressed_due_to_stale_data": False,
        }
    if policy_id == TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value:
        elapsed = int(config.completed_5m_bars_since_entry or 0)
        required = int(config.managed_exit_policy_max_completed_5m_bars)
        if elapsed < required:
            return None
        return {
            "intent_schema_version": "track_b_strategy_managed_paper_close_intent_v1",
            "lifecycle_id": open_state.get("lifecycle_id"),
            "trade_id": open_state.get("trade_id"),
            "strategy_id": config.strategy_id,
            "account_id": config.account_id,
            "contract_key": config.contract_key,
            "local_symbol": config.local_symbol,
            "con_id": config.con_id,
            "side": open_state.get("side"),
            "order_action": "SELL" if open_state.get("side") == "LONG" else "BUY",
            "quantity": config.quantity,
            "close_limit_price": _decimal_text(config.close_limit_price),
            "exit_family": "DIAGNOSTIC_TIME",
            "close_reason": "TIME_BOXED_EXIT",
            "hard_exit": False,
            "discretionary_exit": True,
            "managed_exit_policy_id": policy_id,
            "elapsed_completed_5m_bars": elapsed,
            "required_completed_5m_bars": required,
            "bars_since_fill": elapsed,
            "bars_since_signal": None if config.completed_5m_bars_since_signal is None else int(config.completed_5m_bars_since_signal),
            "fill_timestamp_source": config.fill_timestamp_source or "BROKER_ENTRY_FILL",
            "mfe": None,
            "mae": None,
            "data_freshness_state": str(config.data_freshness_state or "FRESH"),
            "broker_truth_state": str(config.broker_truth_state or "FRESH"),
            "suppressed_due_to_stale_data": False,
        }
    return None


def _discretionary_exits_suppressed(config: TrackBStrategyManagedPaperLifecycleConfig, policy_id: str) -> bool:
    if config.suppress_discretionary_exits_due_to_stale_data is not True:
        return False
    return policy_id in {
        TrackBManagedExitPolicy.DIAGNOSTIC_TIME_EXIT_IMMEDIATE.value,
        TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
    }


def _default_close_submitter(
    config: TrackBStrategyManagedPaperLifecycleConfig,
    close_intent: Mapping[str, Any],
) -> Mapping[str, Any]:
    if config.submit_enabled is not True:
        return {
            "submitted": False,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "close_intent": dict(close_intent),
            "primary_blocker": "Managed PAPER close submit is not enabled for this lifecycle invocation.",
        }
    return _submit_managed_limit_order(
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price=close_intent.get("close_limit_price") or config.close_limit_price,
        submit_index=2,
    )


def _submit_managed_limit_order(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    intent_payload: Mapping[str, Any],
    intent_kind: IntentKind,
    limit_price: object,
    submit_index: int,
) -> Mapping[str, Any]:
    if limit_price in {None, ""}:
        return {
            "submitted": False,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "primary_blocker": f"Managed PAPER {intent_kind.value.lower()} requires a derived limit price.",
        }
    authorization = build_strategy_managed_submit_authorization(
        config=config,
        intent_payload=intent_payload,
        intent_kind=intent_kind,
        limit_price=limit_price,
    )
    if authorization.get("authorization_classification") != STRATEGY_SUBMIT_AUTHORIZED:
        return {
            "submitted": False,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "review_required": True,
            "classification": authorization.get("authorization_classification"),
            "managed_submit_blocked_reason": authorization.get("authorization_classification"),
            "primary_blocker": authorization.get("reason") or "Strategy managed submit authorization blocked.",
            "strategy_submit_authorization": authorization,
        }
    adapter = IbkrPaperAdapter(
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        contract_allowlist={config.contract_key: _contract_allowlist_entry(config)},
        submit_enabled=True,
    )
    run_id = str(intent_payload.get("lifecycle_id") or f"strategy_managed_{uuid.uuid4().hex}")
    order_intent = OrderIntent(
        order_intent_id=f"managed_{intent_kind.value.lower()}_intent_{run_id}_{submit_index}",
        signal_event_id=str(intent_payload.get("signal_id") or intent_payload.get("lifecycle_id") or run_id),
        run_id=run_id,
        intent_kind=intent_kind,
        account_id=config.account_id,
        symbol=config.instrument_family,
        contract_key=config.contract_key,
        action=str(intent_payload.get("order_action") or ""),
        quantity=config.quantity or 0,
        order_type=config.order_type,
        limit_price=limit_price,
        time_in_force=config.time_in_force,
        paper_only=True,
        created_at=datetime.now(UTC),
        reason=str(intent_payload.get("close_reason") or config.signal_reason or config.strategy_id),
    )
    submit_attempt = SubmitAttempt(
        submit_attempt_id=f"managed_{intent_kind.value.lower()}_submit_{run_id}_{submit_index}",
        order_intent_id=order_intent.order_intent_id,
        run_id=run_id,
        account_id=config.account_id,
        broker="IBKR",
        environment={
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
            "broker": "IBKR",
            "environment": "PAPER",
            "lifecycle_mode": "STRATEGY_MANAGED",
        },
        pre_submit_reconciliation_id=f"managed_pre_submit_recon_{run_id}_{submit_index}",
        open_order_baseline_event_id=f"managed_open_orders_{run_id}_{submit_index}",
        request_digest=f"{order_intent.order_intent_id}:{order_intent.limit_price}",
        state=SubmitAttemptState.CREATED,
        submitted_at=datetime.now(UTC),
    )
    try:
        adapter.connect()
        adapter.managed_accounts()
        adapter.require_configured_account()
        open_orders = adapter.refresh_open_orders(contract_key=config.contract_key)
        if open_orders:
            open_order_truth = _managed_close_open_order_truth(
                config=config,
                close_intent=intent_payload,
                open_orders=open_orders,
            )
            existing_close = (
                _existing_working_close_order(
                    config=config,
                    close_intent=intent_payload,
                    open_orders=open_orders,
                    open_order_truth=open_order_truth,
                )
                if intent_kind is IntentKind.CLOSE
                else None
            )
            if existing_close is not None:
                return {
                    "submitted": False,
                    "submit_attempted": False,
                    "broker_state_mutated": False,
                    "classification": CLOSE_ORDER_ALREADY_WORKING,
                    "primary_blocker": "Existing working close order for exact contract/action/quantity blocks duplicate managed PAPER close submit.",
                    "working_order_count": len(open_orders),
                    "existing_working_close_order": existing_close,
                    "open_order_truth": open_order_truth,
                }
            return {
                "submitted": False,
                "submit_attempted": False,
                "broker_state_mutated": False,
                "primary_blocker": "Existing working order for exact contract blocks managed PAPER submit.",
                "working_order_count": len(open_orders),
                "open_order_truth": open_order_truth,
            }
        if intent_kind is IntentKind.CLOSE:
            position_blocker = _managed_close_position_guard(
                config=config,
                close_intent=intent_payload,
                adapter=adapter,
            )
            if position_blocker is not None:
                return position_blocker
        broker_order_id = adapter.submit_limit_order(submit_attempt=submit_attempt, order_intent=order_intent)
        broker_order = adapter.wait_for_broker_order(submit_attempt_id=submit_attempt.submit_attempt_id)
        fill = adapter.wait_for_fill(submit_attempt_id=submit_attempt.submit_attempt_id)
        field = "entry_fill" if intent_kind == IntentKind.OPEN else "close_fill"
        return {
            "submitted": True,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "broker_order_id": str(broker_order_id),
            "submit_attempt_id": submit_attempt.submit_attempt_id,
            "submitted_at": submit_attempt.submitted_at.isoformat(),
            "broker_order": broker_order.to_json_dict(),
            field: {
                "price": _decimal_text(fill.price),
                "quantity": _decimal_text(fill.quantity),
                "filled_at": fill.filled_at.isoformat(),
                "broker_order_id": fill.broker_order_id,
                "perm_id": fill.perm_id,
                "execution_id": fill.execution_id,
            },
            "submit_diagnostics": adapter.submit_diagnostics(submit_attempt.submit_attempt_id),
            "strategy_submit_authorization": authorization,
        }
    except Exception as exc:  # noqa: BLE001 - adapter stage failures must become artifacts.
        diagnostics = adapter.submit_diagnostics(submit_attempt.submit_attempt_id)
        place_order_called = bool(diagnostics.get("place_order_called"))
        broker_order_id = diagnostics.get("broker_order_id_allocated")
        error_summary = _ibkr_error_summary(diagnostics)
        primary_blocker = f"Managed PAPER adapter submit stage failed: {exc}"
        if error_summary.get("ibkr_error_code") == 478:
            primary_blocker = f"IBKR_CONTRACT_REJECTED: {error_summary.get('ibkr_error_message')}"
        return {
            "submitted": False,
            "submit_attempted": place_order_called or broker_order_id is not None,
            "broker_state_mutated": place_order_called,
            "review_required": place_order_called,
            "broker_order_id": str(broker_order_id) if broker_order_id is not None else None,
            "submit_attempt_id": submit_attempt.submit_attempt_id,
            "submitted_at": submit_attempt.submitted_at.isoformat(),
            "primary_blocker": primary_blocker,
            "adapter_exception": repr(exc),
            **error_summary,
            "submit_diagnostics": diagnostics,
            "strategy_submit_authorization": authorization,
        }
    finally:
        adapter.disconnect()


def build_strategy_managed_submit_authorization(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    intent_payload: Mapping[str, Any],
    intent_kind: IntentKind,
    limit_price: object,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    target_identity = _strategy_submit_target_identity(
        config=config,
        intent_payload=intent_payload,
        intent_kind=intent_kind,
        limit_price=limit_price,
    )
    plan_classification = (
        PLAN_STRATEGY_MANAGED_CLOSE_SUBMIT if intent_kind is IntentKind.CLOSE else PLAN_STRATEGY_MANAGED_ENTRY_SUBMIT
    )
    action_type = ACTION_STRATEGY_MANAGED_CLOSE_SUBMIT if intent_kind is IntentKind.CLOSE else ACTION_STRATEGY_MANAGED_ENTRY_SUBMIT
    validator_config = TrackBPreActionSnapshotValidatorConfig(
        repo_root=Path(config.repo_root),
        control_plane_snapshot_path=Path(config.control_plane_snapshot_path),
        autonomous_recovery_plan_path=Path(config.autonomous_recovery_plan_path),
        runtime_supervisor_authority_path=Path(config.runtime_supervisor_authority_path),
    )
    pre_action = validate_track_b_pre_action_snapshot(
        config=validator_config,
        expected_plan_classification=plan_classification,
        expected_action_type=action_type,
        expected_target_identity=target_identity,
        max_snapshot_age_seconds=int(config.pre_action_snapshot_max_age_seconds),
        expected_snapshot_id=config.expected_control_plane_snapshot_id,
        expected_shared_truth_generation_id=config.expected_shared_truth_generation_id,
        now=actual_now,
    )
    snapshot_path = validator_config.resolve(Path(config.control_plane_snapshot_path))
    safe_state_path = _resolve_path(config.repo_root, Path(config.runtime_safe_state_envelope_path))
    snapshot = _read_json_object(snapshot_path)
    safe_state = _read_json_object(safe_state_path)
    base = {
        "schema_version": "track_b_strategy_managed_submit_authorization_v1",
        "authorized_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only_validation": True,
        "paper_only": True,
        "execution_enabled": False,
        "dashboard_projection_consumed": False,
        "not_routing_authority": True,
        "control_plane_snapshot_id": pre_action.get("control_plane_snapshot_id") or snapshot.get("control_plane_snapshot_id"),
        "shared_truth_generation_id": pre_action.get("shared_truth_refresh_generation_id")
        or snapshot.get("shared_truth_refresh_generation_id"),
        "runtime_generation_id": config.runtime_generation_id,
        "strategy_id": config.strategy_id,
        "lane_id": config.lane_id,
        "symbol": config.instrument_family,
        "contract": config.local_symbol,
        "contract_key": config.contract_key,
        "con_id": config.con_id,
        "action": target_identity.get("action"),
        "quantity": target_identity.get("quantity"),
        "order_type": config.order_type,
        "limit_price": target_identity.get("limit_price"),
        "target_identity": target_identity,
        "pre_action_validation": pre_action,
        "safe_state_classification": safe_state.get("safe_state_classification") or safe_state.get("classification"),
        "supervisor_classification": pre_action.get("supervisor_classification")
        or snapshot.get("runtime_supervisor_classification"),
        "runtime_resume_action_policy": pre_action.get("runtime_resume_action_policy")
        or snapshot.get("runtime_resume_action_policy"),
        "runtime_resume_proposed_next_runtime_generation_id": pre_action.get(
            "runtime_resume_proposed_next_runtime_generation_id"
        )
        or snapshot.get("runtime_resume_proposed_next_runtime_generation_id"),
        "submit_allowed": False,
        "broker_mutation_allowed": False,
        "source_artifact_paths": {
            "control_plane_snapshot": str(snapshot_path),
            "runtime_safe_state_envelope": str(safe_state_path),
            "autonomous_recovery_plan": str(validator_config.resolve(Path(config.autonomous_recovery_plan_path))),
            "runtime_supervisor_authority": str(validator_config.resolve(Path(config.runtime_supervisor_authority_path))),
        },
    }
    classification, reason = _strategy_submit_authorization_blocker(
        config=config,
        pre_action=pre_action,
        snapshot=snapshot,
        safe_state=safe_state,
        target_identity=target_identity,
    )
    authorized = classification == STRATEGY_SUBMIT_AUTHORIZED
    return {
        **base,
        "authorization_classification": classification,
        "classification": classification,
        "authorized": authorized,
        "submit_allowed": authorized,
        "broker_mutation_allowed": authorized,
        "reason": reason,
    }


def _strategy_submit_authorization_blocker(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    pre_action: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    safe_state: Mapping[str, Any],
    target_identity: Mapping[str, Any],
) -> tuple[str, str]:
    if pre_action.get("classification") == PRE_ACTION_BLOCKED_SNAPSHOT_MISSING:
        return STRATEGY_SUBMIT_BLOCKED_NO_SNAPSHOT, "Control Plane Snapshot authority artifact is missing."
    if pre_action.get("classification") == PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH:
        return STRATEGY_SUBMIT_BLOCKED_TARGET_MISMATCH, str(pre_action.get("reason") or "Target identity mismatch.")
    if pre_action.get("classification") == PRE_ACTION_BLOCKED_HARD_INVARIANT and "live_money" in str(
        pre_action.get("reason") or ""
    ):
        return STRATEGY_SUBMIT_BLOCKED_LIVE_MONEY, str(pre_action.get("reason"))
    if pre_action.get("classification") in {PRE_ACTION_BLOCKED_SUPERVISOR_MISMATCH, PRE_ACTION_BLOCKED_PLAN_MISMATCH}:
        return STRATEGY_SUBMIT_BLOCKED_SUPERVISOR, str(pre_action.get("reason") or "Supervisor or planner mismatch.")
    if pre_action.get("classification") != PRE_ACTION_SNAPSHOT_VALID:
        return STRATEGY_SUBMIT_BLOCKED_NO_SNAPSHOT, str(pre_action.get("reason") or "Pre-action snapshot validation failed.")
    if _any_true(snapshot, safe_state, key="live_money_eligible"):
        return STRATEGY_SUBMIT_BLOCKED_LIVE_MONEY, "live_money_eligible=true blocks PAPER strategy submit."
    if _any_true(snapshot, safe_state, key="paper_proof_invoked"):
        return STRATEGY_SUBMIT_BLOCKED_PAPER_PROOF, "paper_proof_invoked=true blocks strategy-managed submit."
    safe_classification = str(safe_state.get("safe_state_classification") or safe_state.get("classification") or "")
    if not safe_state:
        return STRATEGY_SUBMIT_BLOCKED_SAFE_STATE, "Runtime Safe-State Envelope authority artifact is missing."
    if safe_state.get("submit_allowed") is not True:
        return STRATEGY_SUBMIT_BLOCKED_SAFE_STATE, "Runtime Safe-State Envelope does not permit submit."
    if safe_state.get("broker_mutation_allowed") is not True:
        return STRATEGY_SUBMIT_BLOCKED_SAFE_STATE, "Runtime Safe-State Envelope does not permit broker mutation."
    if safe_state.get("observe_only") is True or "HARD_HOLD" in safe_classification:
        return STRATEGY_SUBMIT_BLOCKED_SAFE_STATE, f"Runtime Safe-State Envelope blocks submit: {safe_classification}."
    if list(safe_state.get("tripped_limits") or []):
        return STRATEGY_SUBMIT_BLOCKED_SAFE_STATE, "Runtime Safe-State Envelope has tripped limits."
    supervisor_classification = str(pre_action.get("supervisor_classification") or "")
    supervisor_submit_allowed = supervisor_classification in {
        "SUPERVISOR_RUNTIME_START_ALLOWED",
        "SUPERVISOR_RUNTIME_ALREADY_HEALTHY",
    } and (
        pre_action.get("snapshot_safe_to_start_runtime") is True
        or snapshot.get("safe_to_leave_runtime_running") is True
        or safe_state.get("submit_allowed") is True
    )
    if not supervisor_submit_allowed:
        return (
            STRATEGY_SUBMIT_BLOCKED_SUPERVISOR,
            f"Runtime Supervisor does not permit submit/start posture: {supervisor_classification or 'UNKNOWN'}.",
        )
    runtime_generation_id = str(config.runtime_generation_id or "")
    if not runtime_generation_id:
        return STRATEGY_SUBMIT_BLOCKED_RUNTIME_GENERATION, "runtime_generation_id is required for strategy submit."
    proposed_generation = str(pre_action.get("runtime_resume_proposed_next_runtime_generation_id") or "")
    safe_generation = str(safe_state.get("runtime_generation_id") or "")
    if proposed_generation and proposed_generation != runtime_generation_id:
        return STRATEGY_SUBMIT_BLOCKED_RUNTIME_GENERATION, "Runtime Resume v2 proposed generation does not match submit authorization."
    if safe_generation and safe_generation != runtime_generation_id:
        return STRATEGY_SUBMIT_BLOCKED_RUNTIME_GENERATION, "Safe-State runtime generation does not match submit authorization."
    order_truth_blocker = _order_truth_blocker(snapshot=snapshot, safe_state=safe_state)
    if order_truth_blocker:
        return STRATEGY_SUBMIT_BLOCKED_ORDER_TRUTH, order_truth_blocker
    position_truth_blocker = _position_truth_blocker(snapshot=snapshot, safe_state=safe_state)
    if position_truth_blocker:
        return STRATEGY_SUBMIT_BLOCKED_POSITION_TRUTH, position_truth_blocker
    if not target_identity:
        return STRATEGY_SUBMIT_BLOCKED_TARGET_MISMATCH, "Strategy submit target identity is empty."
    return STRATEGY_SUBMIT_AUTHORIZED, "Strategy-managed PAPER submit authorization is valid."


def _strategy_submit_target_identity(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    intent_payload: Mapping[str, Any],
    intent_kind: IntentKind,
    limit_price: object,
) -> dict[str, str]:
    lifecycle_id = str(intent_payload.get("lifecycle_id") or "")
    return _compact_identity(
        {
            "account_id": config.account_id,
            "strategy_id": config.strategy_id,
            "lane_id": config.lane_id,
            "runtime_generation_id": config.runtime_generation_id,
            "lifecycle_id": lifecycle_id,
            "intent_kind": intent_kind.value,
            "symbol": config.instrument_family,
            "contract": config.local_symbol,
            "contract_key": config.contract_key,
            "con_id": config.con_id,
            "action": str(intent_payload.get("order_action") or ""),
            "quantity": config.quantity,
            "order_type": config.order_type,
            "limit_price": _decimal_text(limit_price),
            "managed_exit_policy_id": _normalized_exit_policy(config.managed_exit_policy_id),
        }
    )


def _compact_identity(values: Mapping[str, Any]) -> dict[str, str]:
    return {str(key): str(value) for key, value in values.items() if value not in {None, ""}}


def _order_truth_blocker(*, snapshot: Mapping[str, Any], safe_state: Mapping[str, Any]) -> str | None:
    text = " ".join(
        str(value or "")
        for value in (
            snapshot.get("open_order_truth_classification"),
            snapshot.get("managed_order_registry_classification"),
            safe_state.get("open_order_truth_classification"),
            safe_state.get("managed_order_registry_classification"),
        )
    ).upper()
    if any(token in text for token in ("SUSPICIOUS", "DUPLICATE", "UNKNOWN", "REVIEW_REQUIRED")):
        return f"Order truth blocks strategy submit: {text.strip()}."
    return None


def _position_truth_blocker(*, snapshot: Mapping[str, Any], safe_state: Mapping[str, Any]) -> str | None:
    text = " ".join(
        str(value or "")
        for value in (
            snapshot.get("position_truth_classification"),
            snapshot.get("managed_position_registry_classification"),
            safe_state.get("position_truth_classification"),
            safe_state.get("managed_position_registry_classification"),
        )
    ).upper()
    if any(token in text for token in ("SUSPICIOUS", "UNKNOWN", "CONFLICT", "REVIEW_REQUIRED")):
        return f"Position truth blocks strategy submit: {text.strip()}."
    return None


def _any_true(*payloads: Mapping[str, Any], key: str) -> bool:
    return any(payload.get(key) is True for payload in payloads)


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _resolve_path(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else Path(repo_root) / path


def _managed_close_position_guard(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    close_intent: Mapping[str, Any],
    adapter: IbkrPaperAdapter,
) -> dict[str, Any] | None:
    """Fail closed if broker truth does not show the position being closed."""

    try:
        position = adapter.refresh_positions(contract_key=config.contract_key)
    except Exception as exc:  # noqa: BLE001 - broker-position absence must block the close submit.
        return {
            "submitted": False,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "classification": "BROKER_POSITION_NOT_OPEN_FOR_MANAGED_CLOSE",
            "primary_blocker": f"Managed PAPER close blocked because broker position is not confirmed for exact contract: {exc}",
            "close_intent": dict(close_intent),
        }
    expected_sign = 1 if str(close_intent.get("side") or config.side).upper() == "LONG" else -1
    if int(position.signed_quantity) != expected_sign:
        return {
            "submitted": False,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "classification": "BROKER_POSITION_NOT_OPEN_FOR_MANAGED_CLOSE",
            "primary_blocker": (
                "Managed PAPER close blocked because broker position direction/quantity does not "
                f"match lifecycle side: expected_signed_quantity={expected_sign}; "
                f"observed_signed_quantity={position.signed_quantity}."
            ),
            "close_intent": dict(close_intent),
            "broker_position": position.to_json_dict(),
        }
    return None


def _contract_allowlist_entry(config: TrackBStrategyManagedPaperLifecycleConfig) -> dict[str, Any]:
    canonical = dict(ReadOnlyPreflightConfig().contract_allowlist.get(config.contract_key) or {})
    fallback = {
        "symbol": config.instrument_family,
        "security_type": "FUT",
        "exchange": config.exchange,
        "currency": config.currency,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "contract_month": _contract_month(config.contract_key),
        "expiry": _contract_month(config.contract_key),
        "tick_size": config.tick_size,
    }
    if not canonical:
        return fallback
    matches_config = (
        str(canonical.get("local_symbol") or "") == str(config.local_symbol or "")
        and int(canonical.get("con_id") or 0) == int(config.con_id or 0)
    )
    if not matches_config:
        return fallback
    merged = dict(fallback)
    merged.update({key: value for key, value in canonical.items() if value not in {None, ""}})
    return merged


def _contract_month(contract_key: str) -> str:
    raw = str(contract_key or "")
    if "-" not in raw:
        return ""
    return "".join(ch for ch in raw.split("-", 1)[1] if ch.isdigit())[:6]


def _existing_working_close_order(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    close_intent: Mapping[str, Any],
    open_orders: tuple[Any, ...],
    open_order_truth: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    expected_action = str(close_intent.get("order_action") or "").strip().upper()
    expected_qty = _int_or_none(close_intent.get("quantity") or config.quantity)
    truth = dict(open_order_truth or {})
    matching_truth_orders = [
        state
        for state in truth.get("order_states") or []
        if isinstance(state, Mapping)
        and state.get("is_close_order") is True
        and _order_matches_close_guard(
            config=config,
            expected_action=expected_action,
            expected_qty=expected_qty,
            order=state.get("order") if isinstance(state.get("order"), Mapping) else state,
        )
    ]
    if matching_truth_orders:
        snapshot = dict(matching_truth_orders[0].get("order") or {})
        snapshot["open_order_truth_classification"] = matching_truth_orders[0].get("classification")
        snapshot["open_order_truth_suspicious_reasons"] = matching_truth_orders[0].get("suspicious_reasons") or []
        snapshot["open_order_truth_condition_flags"] = matching_truth_orders[0].get("condition_flags") or []
        if truth.get("classification") in {
            BROKER_FLAT_WITH_OPEN_CLOSE_ORDER,
            CLOSE_ORDER_MARKETABLE_NOT_FILLED,
            CLOSE_ORDER_STALE,
            DUPLICATE_CLOSE_ORDER,
            OPEN_CLOSE_ORDER_WORKING,
            SUSPICIOUS_ORDER_STATE,
        }:
            snapshot["open_order_truth_overall_classification"] = truth.get("classification")
        return snapshot
    for order in open_orders:
        if not _order_matches_close_guard(
            config=config,
            expected_action=expected_action,
            expected_qty=expected_qty,
            order=_order_snapshot(order),
        ):
            continue
        return _order_snapshot(order)
    return None


def _managed_close_open_order_truth(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    close_intent: Mapping[str, Any],
    open_orders: tuple[Any, ...],
    include_expected_broker_position: bool = True,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    reconciliation = {
        "classification": "TRACK_B_PAPER_BROKER_RECONCILIATION_OPEN_ORDER_EVIDENCE",
        "generated_at": now.isoformat(),
        "broker_reconciled": False,
        "track_b_broker_positions": [_expected_broker_position(config)]
        if include_expected_broker_position
        else [],
        "track_b_broker_open_orders": [_managed_open_order_truth_row(order) for order in open_orders],
        "unknown_broker_open_orders": [],
        "known_managed_exit_orders": [_expected_close_order_marker(config=config, close_intent=close_intent)],
        "track_b_lifecycle_positions": [_expected_lifecycle_position(config)],
        "unresolved_submit_intent_ownership_records": [],
        "track_b_broker_open_order_count": len(open_orders),
        "track_b_broker_position_count": 1 if include_expected_broker_position else 0,
        "review_required_count": 0,
        "unresolved_submit_intent_ownership_count": 0,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    truth_config = TrackBOpenOrderTruthConfig(
        repo_root=Path.cwd(),
        dashboard_projection_path=None,
        lifecycle_root=config.output_root,
    )
    payload = build_track_b_open_order_truth_from_reconciliation(
        config=truth_config,
        reconciliation=reconciliation,
        now=now,
    )
    return {
        "schema_version": payload.get("schema_version"),
        "classification": payload.get("classification"),
        "summary": payload.get("summary") or {},
        "order_states": payload.get("order_states") or [],
        "duplicate_close_order_groups": payload.get("duplicate_close_order_groups") or [],
        "broker_flat_with_open_close_order": payload.get("broker_flat_with_open_close_order") or [],
        "broker_positions_without_close_order": payload.get("broker_positions_without_close_order") or [],
        "source": "TRACK_B_OPEN_ORDER_TRUTH_IN_MEMORY_PRE_SUBMIT_EVIDENCE",
        "authority_owner": "execution_core",
        "dashboard_projection_consumed": False,
    }


def _managed_open_order_truth_row(order: Any) -> dict[str, Any]:
    row = _order_snapshot(order)
    row.setdefault("symbol", _symbol_from_contract_key(str(row.get("contract_key") or "")))
    row.setdefault("track_b_root", row.get("symbol"))
    return row


def _expected_close_order_marker(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    close_intent: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "account_id": config.account_id,
        "symbol": config.instrument_family,
        "track_b_root": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "action": str(close_intent.get("order_action") or "").strip().upper(),
        "quantity": str(close_intent.get("quantity") or config.quantity or ""),
        "intent_type": "MANAGED_STRATEGY_CLOSE",
    }


def _expected_broker_position(config: TrackBStrategyManagedPaperLifecycleConfig) -> dict[str, Any]:
    quantity = _signed_position_quantity(config)
    return {
        "account_id": config.account_id,
        "symbol": config.instrument_family,
        "track_b_root": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "quantity": str(quantity),
    }


def _expected_lifecycle_position(config: TrackBStrategyManagedPaperLifecycleConfig) -> dict[str, Any]:
    return {
        "account_id": config.account_id,
        "strategy_id": config.strategy_id,
        "symbol": config.instrument_family,
        "track_b_root": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "side": _position_side(config),
        "quantity": str(abs(_signed_position_quantity(config))),
        "managed_exit_policy_id": config.managed_exit_policy_id,
    }


def _signed_position_quantity(config: TrackBStrategyManagedPaperLifecycleConfig) -> int:
    quantity = int(config.quantity or 0)
    side = _position_side(config)
    return -quantity if side == "SHORT" else quantity


def _position_side(config: TrackBStrategyManagedPaperLifecycleConfig) -> str:
    side = str(config.side or "").strip().upper()
    if side in {"SHORT", "SELL", "SELL_TO_OPEN"}:
        return "SHORT"
    return "LONG"


def _order_matches_close_guard(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    expected_action: str,
    expected_qty: int | None,
    order: Mapping[str, Any],
) -> bool:
    account_id = _order_field(order, "account_id")
    contract_key = _order_field(order, "contract_key")
    local_symbol = _order_field(order, "local_symbol")
    con_id = _order_field(order, "con_id")
    raw_action = _order_field(order, "action")
    action = str(getattr(raw_action, "value", raw_action) or "").strip().upper()
    quantity = _int_or_none(_order_field(order, "quantity"))
    if account_id and str(account_id) != str(config.account_id):
        return False
    if contract_key and str(contract_key) != str(config.contract_key):
        return False
    if not contract_key and local_symbol and str(local_symbol) != str(config.local_symbol):
        return False
    if not contract_key and not local_symbol and con_id and str(con_id) != str(config.con_id):
        return False
    if expected_action and action and action != expected_action:
        return False
    if expected_qty is not None and quantity is not None and quantity != expected_qty:
        return False
    return True


def _order_field(order: Any, key: str) -> Any:
    if isinstance(order, Mapping):
        return order.get(key)
    return getattr(order, key, None)


def _order_snapshot(order: Any) -> dict[str, Any]:
    if hasattr(order, "to_json_dict"):
        snapshot = order.to_json_dict()
        return dict(snapshot) if isinstance(snapshot, Mapping) else {"raw": snapshot}
    if isinstance(order, Mapping):
        return dict(order)
    return {
        "broker_order_id": _order_field(order, "broker_order_id"),
        "perm_id": _order_field(order, "perm_id"),
        "client_id": _order_field(order, "client_id"),
        "account_id": _order_field(order, "account_id"),
        "contract_key": _order_field(order, "contract_key"),
        "symbol": _order_field(order, "symbol"),
        "track_b_root": _order_field(order, "track_b_root"),
        "local_symbol": _order_field(order, "local_symbol"),
        "con_id": _order_field(order, "con_id"),
        "action": _order_field(order, "action"),
        "quantity": _order_field(order, "quantity"),
        "filled_quantity": _order_field(order, "filled_quantity"),
        "remaining_quantity": _order_field(order, "remaining_quantity"),
        "limit_price": _order_field(order, "limit_price"),
        "status": _order_field(order, "status"),
        "updated_at": _order_field(order, "updated_at"),
    }


def _symbol_from_contract_key(contract_key: str) -> str:
    raw = str(contract_key or "").strip().upper()
    if "-" in raw:
        return raw.split("-", 1)[0]
    return raw


def _contract_key_from_bridge_payload(payload: Mapping[str, Any]) -> str | None:
    contract = payload.get("contract") if isinstance(payload.get("contract"), Mapping) else {}
    symbol = str(payload.get("instrument") or payload.get("symbol") or contract.get("symbol") or "").upper()
    month = str(
        payload.get("contract_month")
        or contract.get("contract_month")
        or contract.get("expiry")
        or ""
    )
    digits = "".join(char for char in month if char.isdigit())[:6]
    if symbol and digits:
        return f"{symbol}-{digits}"
    return None


def _int_or_none(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _write_report(report_json: Path, report: Mapping[str, Any]) -> None:
    report_json.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(to_jsonable(dict(report)), indent=2, sort_keys=True)
    report_json.write_text(payload, encoding="utf-8")
    latest = Path(str(report["latest_report_json_path"]))
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(payload, encoding="utf-8")


def _normalized_exit_policy(value: str | None) -> str:
    return str(value or TrackBManagedExitPolicy.EXIT_NOT_AVAILABLE.value).strip().upper()


def _expected_exit_condition(config: TrackBStrategyManagedPaperLifecycleConfig) -> str:
    policy_id = _normalized_exit_policy(config.managed_exit_policy_id)
    if policy_id == TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value:
        return f"TIME_BOXED_EXIT_AFTER_{int(config.managed_exit_policy_max_completed_5m_bars)}_COMPLETED_5M_BARS"
    if policy_id == TrackBManagedExitPolicy.DIAGNOSTIC_TIME_EXIT_IMMEDIATE.value:
        return "DIAGNOSTIC_IMMEDIATE_CLOSE"
    if policy_id == TrackBManagedExitPolicy.MANAGED_HOLD_REQUIRES_EXTERNAL_EXIT_SIGNAL.value:
        return "EXTERNAL_STRATEGY_EXIT_SIGNAL_REQUIRED"
    return "EXIT_POLICY_MISSING"


def _close_intent_status(
    classification: TrackBManagedPaperLifecycleClassification,
    close_intent: Mapping[str, Any] | None,
) -> str:
    if close_intent is not None:
        return "CLOSE_INTENT_CREATED"
    if classification == TrackBManagedPaperLifecycleClassification.OPEN_MANAGED:
        return "WAITING_FOR_EXIT_POLICY_CONDITION"
    if classification == TrackBManagedPaperLifecycleClassification.EXIT_POLICY_MISSING:
        return "EXIT_POLICY_MISSING"
    return "NOT_APPLICABLE"


def _normalized_side(value: str | None) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"BUY", "LONG"}:
        return "LONG"
    if raw in {"SELL", "SHORT"}:
        return "SHORT"
    return raw or "UNKNOWN"


def _order_action(side: str | None) -> str:
    return "SELL" if _normalized_side(side) == "SHORT" else "BUY"


def _mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _decimal_text(value: object) -> str | None:
    parsed = _decimal(value)
    return None if parsed is None else format(parsed.normalize(), "f")


def _realized_pnl(
    side: object,
    quantity: int | None,
    entry_fill: Mapping[str, Any] | None,
    close_fill: Mapping[str, Any] | None,
) -> Decimal | None:
    if not entry_fill or not close_fill or quantity is None:
        return None
    entry = _decimal(entry_fill.get("price") or entry_fill.get("avg_price"))
    close = _decimal(close_fill.get("price") or close_fill.get("avg_price"))
    if entry is None or close is None:
        return None
    direction = Decimal("1") if _normalized_side(str(side)) == "LONG" else Decimal("-1")
    return (close - entry) * Decimal(quantity) * direction


def _final_position_status(classification: TrackBManagedPaperLifecycleClassification) -> str:
    if classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT:
        return "CLOSED_FLAT"
    if classification == TrackBManagedPaperLifecycleClassification.OPEN_MANAGED:
        return "OPEN_MANAGED"
    if classification == TrackBManagedPaperLifecycleClassification.EXIT_PENDING:
        return "EXIT_PENDING"
    return "REVIEW_REQUIRED" if "REVIEW" in classification.value or "MISMATCH" in classification.value else "NOT_OPENED"
