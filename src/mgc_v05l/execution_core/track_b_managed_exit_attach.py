"""Guarded managed-exit attach boundary for existing Track B PAPER positions.

This command plans, and only with explicit flags applies, the intended managed
close for an already OPEN_MANAGED strategy position. It is not a flatten tool
and it does not create unmanaged broker orders.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.models import require_aware_datetime, to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_broker_availability import (
    BrokerAvailabilityReportConfig,
    build_broker_availability_report,
)
from mgc_v05l.execution_core.track_b_control_plane_snapshot import (
    TrackBControlPlaneSnapshotConfig,
    build_track_b_control_plane_snapshot,
    write_track_b_control_plane_snapshot,
)
from mgc_v05l.execution_core.track_b_live_trade_registry import resolve_live_trade_id_for_lifecycle_id
from mgc_v05l.execution_core.track_b_exit_strategy_roster import (
    MNQ_SNAP_TURN_TIMEBOX_3X5M_V1,
    TIMEBOXED_3X5M_MANAGED_LIMIT_CLOSE_V1,
    close_action_for_position_side,
    close_limit_from_profile,
    resolve_track_b_exit_profile,
    resolve_track_b_exit_profile_for_position,
)
from mgc_v05l.execution_core.track_b_managed_order_registry import (
    DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT,
    POSITION_WITHOUT_CLOSE_ORDER,
)
from mgc_v05l.execution_core.track_b_managed_position_registry import DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_open_order_truth import (
    BROKER_POSITION_WITHOUT_CLOSE_ORDER,
    DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_paper_trade_ledger import (
    DEFAULT_TRACK_B_LIVE_POSITION_STATUS_JSON,
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT,
    DEFAULT_TRACK_B_PAPER_TRADE_SUMMARY_JSON,
    update_track_b_paper_trade_ledger_from_runner_report,
)
from mgc_v05l.execution_core.track_b_position_truth_monitor import DEFAULT_POSITION_TRUTH_ARTIFACT
from mgc_v05l.execution_core.track_b_runtime_safe_state_envelope import DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
from mgc_v05l.execution_core.track_b_broker_session_authority import DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT
from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import (
    DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT,
    TrackBManagedExitPolicy,
    TrackBStrategyManagedPaperLifecycleConfig,
    maintain_open_track_b_strategy_managed_paper_lifecycle,
)
from mgc_v05l.execution_core.track_b_exit_intent_dry_run_report import (
    TrackBExitIntentDryRunReportConfig,
    build_track_b_exit_intent_dry_run_report,
)
from mgc_v05l.execution_core.track_b_exit_authority_contract import (
    CloseQtySource,
    ExecutionDomain,
    ExitAuthorityCurrentState,
    ExitIntent,
    ExitType,
    ExitUrgency,
    validate_exit_authority,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PHASE1_RUNTIME_MARKET_DATA_ROOT = Path("outputs/track_b_execution_core/phase1_runtime_market_data")
DEFAULT_MANAGED_EXIT_ATTACH_PLAN = (
    Path("outputs") / "track_b_execution_core" / "managed_exit_attach" / "latest_managed_exit_attach_plan.json"
)

MANAGED_EXIT_ATTACH_PLAN_READY = "MANAGED_EXIT_ATTACH_PLAN_READY"
MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE = "MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE"
MANAGED_EXIT_NOT_YET_ELIGIBLE = "MANAGED_EXIT_NOT_YET_ELIGIBLE"
MANAGED_EXIT_BLOCKED_POSITION_MISMATCH = "MANAGED_EXIT_BLOCKED_POSITION_MISMATCH"
MANAGED_EXIT_BLOCKED_DUPLICATE_CLOSE_ORDER = "MANAGED_EXIT_BLOCKED_DUPLICATE_CLOSE_ORDER"
MANAGED_EXIT_BLOCKED_CONTROL_PLANE = "MANAGED_EXIT_BLOCKED_CONTROL_PLANE"
MANAGED_EXIT_BLOCKED_SAFE_STATE = "MANAGED_EXIT_BLOCKED_SAFE_STATE"
MANAGED_EXIT_APPLY_DISABLED = "MANAGED_EXIT_APPLY_DISABLED"
MANAGED_EXIT_APPLIED_OR_PENDING = "MANAGED_EXIT_APPLIED_OR_PENDING"
MANAGED_EXIT_DUE_READY_FOR_APPLY = "MANAGED_EXIT_DUE_READY_FOR_APPLY"
MANAGED_EXIT_BLOCKED_BROKER_UNAVAILABLE_RETRYABLE = "MANAGED_EXIT_BLOCKED_BROKER_UNAVAILABLE_RETRYABLE"
MANAGED_EXIT_BLOCKED_BROKER_UNAVAILABLE_FATAL = "MANAGED_EXIT_BLOCKED_BROKER_UNAVAILABLE_FATAL"
MANAGED_EXIT_BLOCKED_BROKER_AVAILABILITY_UNKNOWN = "MANAGED_EXIT_BLOCKED_BROKER_AVAILABILITY_UNKNOWN"


@dataclass(frozen=True)
class TrackBManagedExitAttachConfig:
    repo_root: Path = REPO_ROOT
    mode: str = "PAPER"
    account_id: str = "DUM882026"
    expected_account_id: str = "DUM882026"
    strategy_id: str = "MNQ_FIRST_BULL_SNAP_TURN_V1"
    lane_id: str = "mnq_first_bull_snap_turn"
    runtime_generation_id: str = "track-b-paper-runtime-generation-20260525T072612Z"
    lifecycle_id: str = (
        "strategy_managed_track_b_multi_strategy_runtime_cycle_36fa949343974202810c89297c817390_"
        "mnq_first_bull_snap_turn_v1"
    )
    instrument_family: str = "MNQ"
    contract_key: str = "MNQ-202606"
    local_symbol: str = "MNQM6"
    con_id: int = 770561201
    expiry: str = "20260618"
    side: str = "LONG"
    quantity: int = 1
    managed_exit_policy_id: str = TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value
    exit_strategy_id: str = TIMEBOXED_3X5M_MANAGED_LIMIT_CLOSE_V1
    exit_profile_id: str = MNQ_SNAP_TURN_TIMEBOX_3X5M_V1
    required_completed_5m_bars: int = 3
    close_limit_price: str | None = None
    exit_price_offset_ticks: int = 2
    tick_size: str = "0.25"
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 17086
    apply: bool = False
    operator_authorized_managed_exit: bool = False
    refresh_control_plane: bool = True
    control_plane_snapshot_path: Path = (
        Path("outputs") / "track_b_execution_core" / "control_plane" / "latest_control_plane_snapshot.json"
    )
    safe_state_path: Path = DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
    broker_session_authority_path: Path = DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    live_position_status_path: Path = DEFAULT_TRACK_B_LIVE_POSITION_STATUS_JSON
    paper_trade_summary_path: Path = DEFAULT_TRACK_B_PAPER_TRADE_SUMMARY_JSON
    phase1_market_data_root: Path = DEFAULT_PHASE1_RUNTIME_MARKET_DATA_ROOT
    lifecycle_output_root: Path = DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT
    paper_trade_ledger_output_root: Path = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT
    output_path: Path = DEFAULT_MANAGED_EXIT_ATTACH_PLAN
    auto_select_active_managed_position: bool = True
    aggregate_lifecycle_units: tuple[Mapping[str, Any], ...] = ()

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_managed_exit_attach_plan(
    *,
    config: TrackBManagedExitAttachConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    if config.refresh_control_plane:
        snapshot_config = TrackBControlPlaneSnapshotConfig(repo_root=config.repo_root, output_path=config.control_plane_snapshot_path)
        snapshot_payload = build_track_b_control_plane_snapshot(config=snapshot_config, now=actual_now)
        write_track_b_control_plane_snapshot(config=snapshot_config, payload=snapshot_payload)

    managed_position_registry = _read_json(config.resolve(config.managed_position_registry_path))
    selected_managed_position = _select_active_managed_exit_due_position(
        config=config,
        managed_position_registry=managed_position_registry,
    )
    config = _config_for_selected_managed_position(config=config, selected_position=selected_managed_position)
    snapshot = _read_json(config.resolve(config.control_plane_snapshot_path))
    safe_state = _read_json(config.resolve(config.safe_state_path))
    broker_session_authority = _read_json(config.resolve(config.broker_session_authority_path))
    open_order_truth = _read_json(config.resolve(config.open_order_truth_path))
    position_truth = _read_json(config.resolve(config.position_truth_path))
    managed_orders = _read_json(config.resolve(config.managed_order_registry_path))
    live_position_status = _read_json(config.resolve(config.live_position_status_path))
    lifecycle_path = _lifecycle_report_path(
        config=config,
        live_position_status=live_position_status,
        selected_position=selected_managed_position,
    )
    lifecycle_report = _read_json(lifecycle_path)
    config = _config_with_lifecycle_policy(
        config=config,
        lifecycle_report=lifecycle_report,
        selected_position=selected_managed_position,
    )
    exit_profile = _resolve_exit_profile_for_config(config)
    managed_exit_policy_id = exit_profile.managed_exit_policy_id
    required_completed_5m_bars = int(exit_profile.required_completed_5m_bars)

    bars_payload = _read_json(_phase1_path(config=config, timeframe="5m"))
    one_minute_payload = _read_json(_phase1_path(config=config, timeframe="1m"))
    entry_timestamp = _entry_timestamp(lifecycle_report=lifecycle_report, live_position_status=live_position_status, config=config)
    completed_bars = _completed_bar_timestamps_after_entry(payload=bars_payload, entry_timestamp=entry_timestamp)
    completed_bar_count = len(completed_bars)
    latest_price = _latest_price(one_minute_payload) or _latest_price(bars_payload)
    close_limit_price = config.close_limit_price or close_limit_from_profile(
        latest_price=latest_price,
        side=config.side,
        profile=exit_profile,
    )
    close_action = close_action_for_position_side(config.side)
    close_intent = _close_intent_preview(
        config=config,
        close_action=close_action,
        close_limit_price=close_limit_price,
        completed_bar_count=completed_bar_count,
        managed_exit_policy_id=managed_exit_policy_id,
        required_completed_5m_bars=required_completed_5m_bars,
        exit_strategy_id=exit_profile.exit_strategy_id,
        exit_profile_id=exit_profile.exit_profile_id,
    )
    exit_authority_candidate = _exit_authority_candidate_for_config(
        config=config,
        now=actual_now,
        close_action=close_action,
    )
    exit_authority_decision = (
        _mapping(exit_authority_candidate.get("authority_decision"))
        if exit_authority_candidate
        else {}
    )
    exit_authority_allows = str(exit_authority_decision.get("decision") or "") in {
        "ALLOWED",
        "DEGRADED_ALLOWED",
    }
    exit_authority_block_reasons = [
        str(item)
        for item in (exit_authority_decision.get("block_reasons") or [])
        if str(item or "").strip()
    ]

    blockers: list[str] = []
    control_plane_ok, control_plane_reason = _control_plane_allows_managed_exit(
        config=config,
        snapshot=snapshot,
        broker_session_authority=broker_session_authority,
        open_order_truth=open_order_truth,
        managed_orders=managed_orders,
        close_action=close_action,
    )
    safe_state_ok, safe_state_reason = _safe_state_allows_managed_exit(safe_state)
    position_ok, position_reason = _position_identity_matches(
        config=config,
        position_truth=position_truth,
        live_position_status=live_position_status,
        selected_position=selected_managed_position,
    )
    lifecycle_ok, lifecycle_reason = _lifecycle_matches(
        config=config,
        lifecycle_report=lifecycle_report,
        managed_exit_policy_id=managed_exit_policy_id,
        selected_position=selected_managed_position,
    )
    duplicate_close = _duplicate_close_order(config=config, managed_orders=managed_orders, open_order_truth=open_order_truth, close_action=close_action)
    raw_prior_lifecycle_close = _prior_lifecycle_close_submit_blocker(lifecycle_report)
    prior_lifecycle_close_is_stale_diagnostic = _prior_lifecycle_close_is_stale_diagnostic(
        prior_lifecycle_close=raw_prior_lifecycle_close,
        exit_authority_allows=exit_authority_allows,
        position_ok=position_ok,
        duplicate_close=duplicate_close,
    )
    prior_lifecycle_close = None if prior_lifecycle_close_is_stale_diagnostic else raw_prior_lifecycle_close
    aggregate_lifecycle_blocker = _aggregate_lifecycle_unit_blocker(
        config=config,
        managed_exit_policy_id=managed_exit_policy_id,
    )

    if exit_authority_candidate and not exit_authority_allows:
        blockers.extend(exit_authority_block_reasons or ["ExitAuthorityDecision V1.1 blocked exact close."])
        classification = _classification_for_exit_authority_blockers(exit_authority_block_reasons)
    elif not exit_authority_candidate:
        blockers.append("ExitAuthorityDecision V1.1 did not find a matching broker-scoped close intent.")
        classification = MANAGED_EXIT_BLOCKED_POSITION_MISMATCH
    elif not close_limit_price:
        blockers.append("Current executable close price is unavailable.")
        classification = MANAGED_EXIT_BLOCKED_POSITION_MISMATCH
    elif (
        snapshot.get("live_money_eligible") is True
        or snapshot.get("paper_proof_invoked") is True
        or broker_session_authority.get("live_money_eligible") is True
        or broker_session_authority.get("paper_proof_invoked") is True
        or safe_state.get("live_money_eligible") is True
        or safe_state.get("paper_proof_invoked") is True
    ):
        blockers.append("live_money/paper_proof safety flag blocks managed-exit attach.")
        classification = MANAGED_EXIT_BLOCKED_CONTROL_PLANE
    elif exit_authority_allows and _control_plane_has_explicit_hard_hold(snapshot):
        blockers.append("Control Plane reports an explicit hard safety hold.")
        classification = MANAGED_EXIT_BLOCKED_CONTROL_PLANE
    elif not control_plane_ok and not exit_authority_allows:
        blockers.append(control_plane_reason)
        classification = MANAGED_EXIT_BLOCKED_CONTROL_PLANE
    elif not safe_state_ok and not exit_authority_allows:
        blockers.append(safe_state_reason)
        classification = MANAGED_EXIT_BLOCKED_SAFE_STATE
    elif (not position_ok or not lifecycle_ok) and not exit_authority_allows:
        blockers.extend(reason for reason in (position_reason, lifecycle_reason) if reason)
        classification = MANAGED_EXIT_BLOCKED_POSITION_MISMATCH
    elif duplicate_close or prior_lifecycle_close or aggregate_lifecycle_blocker:
        blockers.append(
            duplicate_close
            or prior_lifecycle_close
            or aggregate_lifecycle_blocker
            or "Existing managed close state blocks duplicate attach."
        )
        classification = MANAGED_EXIT_BLOCKED_DUPLICATE_CLOSE_ORDER
    elif completed_bar_count < required_completed_5m_bars:
        classification = MANAGED_EXIT_NOT_YET_ELIGIBLE
    elif config.apply is not True and config.operator_authorized_managed_exit is not True:
        classification = MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    elif config.apply is not True or config.operator_authorized_managed_exit is not True:
        classification = MANAGED_EXIT_APPLY_DISABLED
    else:
        classification = MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE

    payload: dict[str, Any] = {
        "schema_version": "track_b_managed_exit_attach_plan_v1",
        "generated_at": actual_now.isoformat(),
        "classification": classification,
        "plan_classification": (
            MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
            if completed_bar_count >= required_completed_5m_bars and not blockers
            else MANAGED_EXIT_ATTACH_PLAN_READY
            if not blockers
            else classification
        ),
        "mode": config.mode,
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "broad_cancel_allowed": False,
        "global_flatten_allowed": False,
        "dashboard_projection_consumed": False,
        "apply_requested": config.apply is True,
        "operator_authorized_managed_exit": config.operator_authorized_managed_exit is True,
        "apply_enabled": config.apply is True and config.operator_authorized_managed_exit is True and not blockers,
        "apply_boundary_classification": None
        if config.apply is True and config.operator_authorized_managed_exit is True
        else MANAGED_EXIT_APPLY_DISABLED,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "blockers": blockers,
        "control_plane_snapshot_id": snapshot.get("control_plane_snapshot_id"),
        "shared_truth_generation_id": snapshot.get("shared_truth_refresh_generation_id"),
        "control_plane_classification": snapshot.get("classification"),
        "shared_truth_coherence_status": snapshot.get("shared_truth_coherence_status"),
        "runtime_supervisor_classification": snapshot.get("runtime_supervisor_classification"),
        "safe_state_classification": safe_state.get("safe_state_classification") or safe_state.get("classification"),
        "safe_state_submit_allowed": safe_state.get("submit_allowed") is True,
        "safe_state_broker_mutation_allowed": safe_state.get("broker_mutation_allowed") is True,
        "safe_state_managed_close_mutation_allowed": safe_state.get("managed_close_mutation_allowed") is True,
        "broker_session_authority_classification": broker_session_authority.get("classification"),
        "broker_session_allowed_uses": broker_session_authority.get("allowed_uses") if isinstance(broker_session_authority.get("allowed_uses"), Mapping) else {},
        "risk_reducing_close_connection_mode": broker_session_authority.get("risk_reducing_close_connection_mode"),
        "exit_authority_contract": {
            "schema_version": "track_b_managed_exit_attach_exit_authority_v1",
            "source": "ExitAuthorityDecision V1.1",
            "candidate": exit_authority_candidate,
            "decision": exit_authority_decision,
            "legacy_diagnostics": {
                "control_plane_ok": control_plane_ok,
                "control_plane_reason": control_plane_reason,
                "safe_state_ok": safe_state_ok,
                "safe_state_reason": safe_state_reason,
                "position_identity_ok": position_ok,
                "position_identity_reason": position_reason,
                "lifecycle_identity_ok": lifecycle_ok,
                "lifecycle_identity_reason": lifecycle_reason,
                "duplicate_close": duplicate_close,
                "prior_lifecycle_close": raw_prior_lifecycle_close,
                "prior_lifecycle_close_stale_diagnostic": prior_lifecycle_close_is_stale_diagnostic,
                "aggregate_lifecycle_blocker": aggregate_lifecycle_blocker,
            },
        },
        "open_order_truth_classification": open_order_truth.get("classification"),
        "managed_order_registry_classification": managed_orders.get("classification"),
        "managed_position_registry_classification": managed_position_registry.get("classification"),
        "selected_managed_position": selected_managed_position,
        "aggregate_exit_group": _aggregate_exit_group(config),
        "managed_exit_due_automation": {
            "scan_enabled": config.auto_select_active_managed_position,
            "classification": MANAGED_EXIT_DUE_READY_FOR_APPLY
            if selected_managed_position and completed_bar_count >= required_completed_5m_bars and not blockers
            else None,
            "active_due_count": _active_due_count(managed_position_registry),
        },
        "position_truth_classification": position_truth.get("classification"),
        "target_identity": _target_identity(
            config=config,
            action=close_action,
            close_limit_price=close_limit_price,
            managed_exit_policy_id=managed_exit_policy_id,
            exit_strategy_id=exit_profile.exit_strategy_id,
            exit_profile_id=exit_profile.exit_profile_id,
        ),
        "position_identity_verified": position_ok,
        "lifecycle_identity_verified": lifecycle_ok,
        "duplicate_close_order_detected": bool(duplicate_close or prior_lifecycle_close),
        "prior_lifecycle_close_submit_blocker": prior_lifecycle_close,
        "prior_lifecycle_close_stale_diagnostic": raw_prior_lifecycle_close
        if prior_lifecycle_close_is_stale_diagnostic
        else None,
        "managed_exit_policy_id": managed_exit_policy_id,
        "exit_profile": exit_profile.to_json_dict(),
        "exit_strategy_id": exit_profile.exit_strategy_id,
        "exit_profile_id": exit_profile.exit_profile_id,
        "requested_exit_strategy_id": config.exit_strategy_id,
        "requested_exit_profile_id": config.exit_profile_id,
        "exit_roster_compatible": True,
        "exit_roster_role": "strategy_managed_position_close",
        "required_completed_5m_bars": required_completed_5m_bars,
        "completed_5m_bars_since_entry": completed_bar_count,
        "completed_5m_bar_timestamps_since_entry": completed_bars,
        "timebox_exit_eligible": completed_bar_count >= required_completed_5m_bars,
        "entry_timestamp": entry_timestamp,
        "latest_price_evidence": latest_price,
        "close_intent_preview": close_intent,
        "expected_post_action_evidence": {
            "same_account": config.account_id,
            "same_contract": config.local_symbol,
            "same_con_id": config.con_id,
            "same_action": close_action,
            "same_quantity": str(config.quantity),
            "open_order_truth": "working close order or flat after fill",
            "managed_order_registry": "working close order or terminal close fill",
            "position_truth": "position protected by close order or closed flat",
            "reconciliation": "TRACK_B_PAPER_BROKER_RECONCILED",
        },
        "source_artifact_paths": {
            "control_plane_snapshot": str(config.resolve(config.control_plane_snapshot_path)),
            "runtime_safe_state_envelope": str(config.resolve(config.safe_state_path)),
            "broker_session_authority": str(config.resolve(config.broker_session_authority_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "position_truth": str(config.resolve(config.position_truth_path)),
            "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
            "live_position_status": str(config.resolve(config.live_position_status_path)),
            "lifecycle_report": str(lifecycle_path),
            "phase1_5m_candles": str(_phase1_path(config=config, timeframe="5m")),
            "phase1_1m_candles": str(_phase1_path(config=config, timeframe="1m")),
        },
    }
    if payload["apply_enabled"] is not True:
        payload["classification"] = classification
        payload["required_next_action"] = (
            "Review the plan and rerun with --apply --operator-authorized-managed-exit only if exact identity remains current."
            if classification in {MANAGED_EXIT_APPLY_DISABLED, MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE}
            else "Resolve blockers before attempting the managed-exit attach."
        )
        return payload

    broker_availability = _broker_availability_for_attach(config=config, now=actual_now)
    broker_availability_blocker = _broker_availability_boundary_blocker(broker_availability)
    payload["broker_availability"] = broker_availability
    payload["broker_availability_blocker"] = broker_availability_blocker
    if broker_availability_blocker:
        payload["classification"] = _managed_exit_classification_for_broker_availability_blocker(broker_availability_blocker)
        payload["apply_enabled"] = False
        payload["apply_boundary_classification"] = broker_availability_blocker
        payload["broker_state_mutated"] = False
        payload["submit_attempted"] = False
        payload["primary_blocker"] = broker_availability_blocker
        payload["required_next_action"] = "Retry managed-exit attach after BrokerAvailability returns BROKER_AVAILABLE."
        return payload

    apply_result = _apply_managed_exit(
        config=config,
        lifecycle_report=lifecycle_report,
        selected_position=selected_managed_position,
        completed_bar_count=completed_bar_count,
        close_limit_price=close_limit_price,
        managed_exit_policy_id=managed_exit_policy_id,
        required_completed_5m_bars=required_completed_5m_bars,
        now=actual_now,
    )
    payload["apply_result"] = apply_result
    payload["classification"] = str(apply_result.get("classification") or MANAGED_EXIT_APPLIED_OR_PENDING)
    payload["broker_state_mutated"] = bool(apply_result.get("broker_state_mutated"))
    payload["submit_attempted"] = bool(apply_result.get("submit_attempted"))
    payload["required_next_action"] = "Verify broker open order/fill and rebuild shared truth/control plane."
    return payload


def write_track_b_managed_exit_attach_plan(*, config: TrackBManagedExitAttachConfig, payload: Mapping[str, Any]) -> Path:
    return write_json_atomic(config.resolve(config.output_path), to_jsonable(dict(payload)))


def run_track_b_managed_exit_attach(
    *,
    config: TrackBManagedExitAttachConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    payload = build_track_b_managed_exit_attach_plan(config=config, now=now)
    write_track_b_managed_exit_attach_plan(config=config, payload=payload)
    return payload


def _broker_availability_for_attach(*, config: TrackBManagedExitAttachConfig, now: datetime) -> dict[str, Any]:
    return build_broker_availability_report(
        config=BrokerAvailabilityReportConfig(
            repo_root=config.repo_root,
            execution_domain=ExecutionDomain.TRACK_B_PAPER,
            account_id=config.account_id,
            endpoint_host=config.host,
            endpoint_port=config.port,
        ),
        now=now,
    )


def _broker_availability_boundary_blocker(broker_availability: Mapping[str, Any]) -> str | None:
    classification = str(broker_availability.get("classification") or "").strip().upper()
    if classification == "BROKER_AVAILABLE":
        return None
    if classification == "BROKER_UNAVAILABLE_RETRYABLE":
        return "broker_unavailable_retryable"
    if classification == "BROKER_UNAVAILABLE_FATAL":
        return "broker_unavailable_fatal"
    return "broker_availability_unknown"


def _managed_exit_classification_for_broker_availability_blocker(blocker: str) -> str:
    if blocker == "broker_unavailable_retryable":
        return MANAGED_EXIT_BLOCKED_BROKER_UNAVAILABLE_RETRYABLE
    if blocker == "broker_unavailable_fatal":
        return MANAGED_EXIT_BLOCKED_BROKER_UNAVAILABLE_FATAL
    return MANAGED_EXIT_BLOCKED_BROKER_AVAILABILITY_UNKNOWN


def _apply_managed_exit(
    *,
    config: TrackBManagedExitAttachConfig,
    lifecycle_report: Mapping[str, Any],
    selected_position: Mapping[str, Any],
    completed_bar_count: int,
    close_limit_price: str | None,
    managed_exit_policy_id: str,
    required_completed_5m_bars: int,
    now: datetime,
) -> dict[str, Any]:
    lifecycle_report = _with_registry_trade_id_for_managed_exit(
        config=config,
        lifecycle_report=lifecycle_report,
        selected_position=selected_position,
        managed_exit_policy_id=managed_exit_policy_id,
    )
    entry_intent = lifecycle_report.get("entry_intent") if isinstance(lifecycle_report.get("entry_intent"), Mapping) else {}
    lifecycle_config = TrackBStrategyManagedPaperLifecycleConfig(
        mode=config.mode,
        account_id=config.account_id,
        expected_account_id=config.expected_account_id,
        strategy_id=config.strategy_id,
        instrument_family=config.instrument_family,
        contract_key=config.contract_key,
        local_symbol=config.local_symbol,
        con_id=config.con_id,
        contract_expiry=config.expiry,
        side=config.side,
        quantity=config.quantity,
        close_limit_price=close_limit_price,
        managed_exit_policy_id=managed_exit_policy_id,
        managed_exit_policy_max_completed_5m_bars=required_completed_5m_bars,
        completed_5m_bars_since_entry=completed_bar_count,
        fill_timestamp_source="BROKER_ENTRY_FILL",
        signal_timestamp=entry_intent.get("signal_timestamp"),
        signal_reason=entry_intent.get("signal_reason"),
        decision_bar_timestamp=entry_intent.get("decision_bar_timestamp"),
        latest_decision_bar_source=str(entry_intent.get("latest_decision_bar_source") or "DATABENTO_LIVE_ARTIFACT"),
        data_freshness_state="FRESH",
        broker_truth_state="FRESH",
        submit_enabled=True,
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        order_type="LMT",
        time_in_force="DAY",
        exchange="CME" if config.instrument_family.upper() in {"MNQ", "NQ", "ES", "MES"} else "COMEX",
        currency="USD",
        tick_size=config.tick_size,
        source_id="track_b_managed_exit_attach",
        output_root=config.lifecycle_output_root,
        paper_trade_ledger_output_root=config.paper_trade_ledger_output_root,
        live_position_status_json=config.live_position_status_path,
        live_money_readiness=False,
        broker_reconciled=True,
        repo_root=config.repo_root,
        lane_id=config.lane_id,
        runtime_generation_id=config.runtime_generation_id,
        control_plane_snapshot_path=config.control_plane_snapshot_path,
        runtime_safe_state_envelope_path=config.safe_state_path,
        expected_control_plane_snapshot_id=None,
        expected_shared_truth_generation_id=None,
    )
    result = maintain_open_track_b_strategy_managed_paper_lifecycle(
        config=lifecycle_config,
        existing_lifecycle_report=lifecycle_report,
        now=now,
    )
    ledger_update: dict[str, Any] = {}
    if result.report.get("close_fill"):
        synced_reports = _sync_aggregate_lifecycle_close_reports(
            config=config,
            close_source_report=result.report,
            close_source_report_path=result.report_json,
            now=now,
        )
        ledger_results = []
        for report_path in synced_reports:
            report_payload = _read_json(report_path)
            ledger_result = update_track_b_paper_trade_ledger_from_runner_report(
                runner_report={
                    "managed_lifecycle_invoked": True,
                    "managed_lifecycle_report_path": str(report_path),
                    "strategy_id": report_payload.get("strategy_id") or config.strategy_id,
                    "contract_key": report_payload.get("contract_key") or config.contract_key,
                    "local_symbol": report_payload.get("local_symbol") or config.local_symbol,
                    "con_id": report_payload.get("con_id") or config.con_id,
                    "account_id": report_payload.get("account_id") or config.account_id,
                },
                runner_report_json=report_path,
                output_root=config.paper_trade_ledger_output_root,
                now=now,
            )
            ledger_results.append(ledger_result)
        ledger_update = {
            "aggregate_lifecycle_close_persistence": {
                "enabled": bool(config.aggregate_lifecycle_units),
                "synced_lifecycle_count": len(synced_reports),
                "synced_report_paths": [str(path) for path in synced_reports],
            },
            "trade_record_written": any(item.trade_record_written for item in ledger_results),
            "ledger_jsonl": str(ledger_results[-1].ledger_jsonl) if ledger_results else None,
            "trade_summary_json": str(ledger_results[-1].trade_summary_json) if ledger_results else None,
            "live_position_status_json": str(ledger_results[-1].live_position_status_json) if ledger_results else None,
            "pnl_summary_json": str(ledger_results[-1].pnl_summary_json) if ledger_results else None,
            "open_position_count": ledger_results[-1].trade_summary.get("open_position_count") if ledger_results else None,
            "realized_pnl_by_lifecycle": [
                None if item.trade_record is None else item.trade_record.get("realized_pnl")
                for item in ledger_results
            ],
        }
    return {
        "classification": result.report.get("paper_lifecycle_classification") or result.classification.value,
        "lifecycle_report_path": str(result.report_json),
        "submit_attempted": result.report.get("submit_attempted") is True,
        "broker_state_mutated": result.report.get("broker_state_mutated") is True,
        "close_intent": result.report.get("close_intent"),
        "close_submit_attempt": result.report.get("close_submit_attempt"),
        "close_fill": result.report.get("close_fill"),
        "paper_trade_ledger_update": ledger_update,
        "primary_blocker": result.report.get("primary_blocker"),
    }


def _with_registry_trade_id_for_managed_exit(
    *,
    config: TrackBManagedExitAttachConfig,
    lifecycle_report: Mapping[str, Any],
    selected_position: Mapping[str, Any] | None = None,
    managed_exit_policy_id: str | None = None,
) -> dict[str, Any]:
    report = dict(lifecycle_report)
    selected_trade_id = ""
    if _selected_current_scope_lifecycle_matches(
        config=config,
        lifecycle_report=lifecycle_report,
        managed_exit_policy_id=str(managed_exit_policy_id or report.get("managed_exit_policy_id") or ""),
        selected_position=selected_position,
    ):
        selected = _mapping(selected_position)
        lifecycle_position = _mapping(selected.get("lifecycle_position"))
        units = [
            _mapping(item)
            for item in (selected.get("lifecycle_units") or lifecycle_position.get("lifecycle_units") or [])
            if isinstance(item, Mapping)
        ]
        selected_trade_id = str(
            selected.get("trade_id")
            or lifecycle_position.get("trade_id")
            or next((unit.get("trade_id") for unit in units if unit.get("trade_id")), "")
            or ""
        ).strip()
    registry_trade_id = selected_trade_id or resolve_live_trade_id_for_lifecycle_id(
        repo_root=config.repo_root,
        lifecycle_id=config.lifecycle_id,
    )
    if not registry_trade_id and not selected_trade_id:
        return report
    report["lifecycle_id"] = config.lifecycle_id
    report["trade_id"] = registry_trade_id
    if selected_trade_id:
        report["current_scope_identity_source"] = "MANAGED_POSITION_REGISTRY_SELECTED_POSITION"
    for key in ("entry_intent", "open_state", "close_intent"):
        value = report.get(key)
        if isinstance(value, Mapping):
            nested = dict(value)
            nested["lifecycle_id"] = config.lifecycle_id
            nested["trade_id"] = registry_trade_id
            report[key] = nested
    return report


def _select_active_managed_exit_due_position(
    *,
    config: TrackBManagedExitAttachConfig,
    managed_position_registry: Mapping[str, Any],
) -> dict[str, Any]:
    if config.auto_select_active_managed_position is not True:
        return {}
    candidates = [
        dict(item)
        for item in managed_position_registry.get("managed_positions") or []
        if isinstance(item, Mapping)
        and str(item.get("classification") or "") == "OPEN_MANAGED_EXIT_DUE"
        and item.get("exit_due") is True
        and item.get("attention_required") is not True
    ]
    if not candidates:
        return {}
    exact_lifecycle = [item for item in candidates if str(item.get("lifecycle_id") or "") == config.lifecycle_id]
    if len(exact_lifecycle) == 1:
        return exact_lifecycle[0]
    exact_contract = [
        item
        for item in candidates
        if str(item.get("local_symbol") or "") == config.local_symbol
        and _int_or_none(item.get("con_id")) == config.con_id
        and str(item.get("contract_key") or "") == config.contract_key
    ]
    if len(exact_contract) == 1:
        return exact_contract[0]
    if _has_explicit_managed_exit_target(config):
        return {}
    if len(candidates) == 1:
        return candidates[0]
    return {}


def _has_explicit_managed_exit_target(config: TrackBManagedExitAttachConfig) -> bool:
    default = TrackBManagedExitAttachConfig(repo_root=config.repo_root)
    return any(
        (
            config.lifecycle_id != default.lifecycle_id,
            config.strategy_id != default.strategy_id,
            config.contract_key != default.contract_key,
            config.local_symbol != default.local_symbol,
            config.con_id != default.con_id,
        )
    )


def _resolve_exit_profile_for_config(config: TrackBManagedExitAttachConfig):
    requested = resolve_track_b_exit_profile(config.exit_profile_id)
    if (
        requested.instrument_family == str(config.instrument_family or "").upper()
        and requested.managed_exit_policy_id == config.managed_exit_policy_id
    ):
        return requested
    return resolve_track_b_exit_profile_for_position(
        instrument_family=config.instrument_family,
        managed_exit_policy_id=config.managed_exit_policy_id,
    )


def _config_with_lifecycle_policy(
    *,
    config: TrackBManagedExitAttachConfig,
    lifecycle_report: Mapping[str, Any],
    selected_position: Mapping[str, Any] | None = None,
) -> TrackBManagedExitAttachConfig:
    selected = _mapping(selected_position)
    selected_policy = str(
        selected.get("managed_exit_policy_id")
        or _mapping(selected.get("lifecycle_position")).get("managed_exit_policy_id")
        or ""
    ).strip()
    if selected_policy and selected_policy == config.managed_exit_policy_id:
        return config
    managed_exit_policy_id = str(lifecycle_report.get("managed_exit_policy_id") or "").strip()
    if not managed_exit_policy_id or managed_exit_policy_id == config.managed_exit_policy_id:
        return config
    exit_profile = resolve_track_b_exit_profile_for_position(
        instrument_family=config.instrument_family,
        managed_exit_policy_id=managed_exit_policy_id,
    )
    return replace(
        config,
        managed_exit_policy_id=managed_exit_policy_id,
        exit_strategy_id=exit_profile.exit_strategy_id,
        exit_profile_id=exit_profile.exit_profile_id,
        required_completed_5m_bars=int(exit_profile.required_completed_5m_bars),
        tick_size=str(exit_profile.tick_size),
    )


def _config_for_selected_managed_position(
    *,
    config: TrackBManagedExitAttachConfig,
    selected_position: Mapping[str, Any],
) -> TrackBManagedExitAttachConfig:
    if not selected_position:
        return config
    lifecycle_position = _mapping(selected_position.get("lifecycle_position"))
    broker_position = _mapping(selected_position.get("broker_position"))
    instrument_family = str(
        selected_position.get("symbol")
        or lifecycle_position.get("instrument_family")
        or lifecycle_position.get("track_b_root")
        or broker_position.get("track_b_root")
        or broker_position.get("symbol")
        or config.instrument_family
    ).upper()
    managed_exit_policy_id = str(selected_position.get("managed_exit_policy_id") or config.managed_exit_policy_id)
    exit_profile = resolve_track_b_exit_profile_for_position(
        instrument_family=instrument_family,
        managed_exit_policy_id=managed_exit_policy_id,
    )
    quantity = _int_or_none(selected_position.get("quantity") or lifecycle_position.get("quantity"))
    if quantity is None:
        decimal_quantity = _decimal(selected_position.get("quantity") or lifecycle_position.get("quantity"))
        quantity = int(abs(decimal_quantity)) if decimal_quantity is not None else config.quantity
    quantity = abs(quantity)
    strategy_id = str(
        selected_position.get("strategy_id")
        or lifecycle_position.get("strategy_id")
        or config.strategy_id
    )
    lane_id = str(selected_position.get("lane_id") or lifecycle_position.get("lane_id") or _lane_from_strategy_id(strategy_id) or config.lane_id)
    selected_account_id = _selected_position_account_id(
        selected_position=selected_position,
        lifecycle_position=lifecycle_position,
        broker_position=broker_position,
        fallback_account_id=config.account_id,
    )
    return replace(
        config,
        account_id=selected_account_id,
        expected_account_id=selected_account_id or config.expected_account_id,
        strategy_id=strategy_id,
        lane_id=lane_id,
        runtime_generation_id=str(
            lifecycle_position.get("runtime_generation_id")
            or selected_position.get("runtime_generation_id")
            or config.runtime_generation_id
            or ""
        ),
        lifecycle_id=str(selected_position.get("lifecycle_id") or lifecycle_position.get("lifecycle_id") or config.lifecycle_id),
        instrument_family=instrument_family,
        contract_key=str(selected_position.get("contract_key") or lifecycle_position.get("contract_key") or config.contract_key),
        local_symbol=str(selected_position.get("local_symbol") or lifecycle_position.get("local_symbol") or config.local_symbol),
        con_id=_int_or_none(selected_position.get("con_id") or lifecycle_position.get("con_id")) or config.con_id,
        expiry=str(broker_position.get("expiry") or lifecycle_position.get("expiry") or config.expiry),
        side=str(selected_position.get("side") or lifecycle_position.get("side") or config.side),
        quantity=quantity,
        managed_exit_policy_id=managed_exit_policy_id,
        exit_strategy_id=exit_profile.exit_strategy_id,
        exit_profile_id=exit_profile.exit_profile_id,
        required_completed_5m_bars=int(exit_profile.required_completed_5m_bars),
        tick_size=str(exit_profile.tick_size),
        aggregate_lifecycle_units=tuple(
            dict(item)
            for item in (selected_position.get("lifecycle_units") or lifecycle_position.get("lifecycle_units") or [])
            if isinstance(item, Mapping)
        ),
    )


def _selected_position_account_id(
    *,
    selected_position: Mapping[str, Any],
    lifecycle_position: Mapping[str, Any],
    broker_position: Mapping[str, Any],
    fallback_account_id: str,
) -> str:
    units = [
        _mapping(item)
        for item in (selected_position.get("lifecycle_units") or lifecycle_position.get("lifecycle_units") or [])
        if isinstance(item, Mapping)
    ]
    unit_accounts = {_valid_account_id(item.get("account_id") or item.get("account")) for item in units}
    unit_accounts.discard("")
    for raw in (
        broker_position.get("account_id"),
        broker_position.get("account"),
        next(iter(unit_accounts)) if len(unit_accounts) == 1 else "",
        lifecycle_position.get("account_id"),
        lifecycle_position.get("account"),
        selected_position.get("account_id"),
        selected_position.get("account"),
    ):
        account = _valid_account_id(raw)
        if account:
            return account
    return "" if selected_position else str(fallback_account_id or "")


def _valid_account_id(value: Any) -> str:
    text = str(value or "").strip()
    if text.upper() in {"", "MULTIPLE", "MISSING", "UNKNOWN", "NONE", "NULL"}:
        return ""
    return text


def _active_due_count(managed_position_registry: Mapping[str, Any]) -> int:
    return sum(
        1
        for item in managed_position_registry.get("managed_positions") or []
        if isinstance(item, Mapping)
        and str(item.get("classification") or "") == "OPEN_MANAGED_EXIT_DUE"
        and item.get("exit_due") is True
    )


def _lane_from_strategy_id(strategy_id: str) -> str:
    parts = [part for part in str(strategy_id or "").split("__") if part]
    return "__".join(parts[1:]) if len(parts) > 1 else ""


def _control_plane_allows_managed_exit(
    *,
    config: TrackBManagedExitAttachConfig,
    snapshot: Mapping[str, Any],
    broker_session_authority: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
    managed_orders: Mapping[str, Any],
    close_action: str,
) -> tuple[bool, str]:
    if not snapshot:
        return False, "Control Plane Snapshot is missing."
    if snapshot.get("live_money_eligible") is True:
        return False, "live_money_eligible=true blocks managed-exit attach."
    if snapshot.get("paper_proof_invoked") is True:
        return False, "paper_proof_invoked=true blocks managed-exit attach."
    if snapshot.get("agent_health_has_duplicate_writer") is True:
        return False, "Control Plane reports duplicate writer."
    classification = str(snapshot.get("classification") or "")
    supervisor = str(snapshot.get("runtime_supervisor_classification") or "")
    close_only_authority = _close_only_authority_allows_unhealthy_runtime(
        config=config,
        broker_session_authority=broker_session_authority,
        open_order_truth=open_order_truth,
        managed_orders=managed_orders,
        close_action=close_action,
    )
    if _control_plane_has_explicit_hard_hold(snapshot):
        return False, "Control Plane reports an explicit hard safety hold."
    if close_only_authority and classification in {
        "CONTROL_PLANE_SNAPSHOT_BLOCKED",
        "CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED",
        "CONTROL_PLANE_SNAPSHOT_START_BLOCKED",
        "CONTROL_PLANE_SNAPSHOT_BLOCKED_START_PREFLIGHT",
    }:
        return True, (
            "Close-only BSA authority permits exact managed-exit recovery while "
            "entry/runtime Control Plane authority is degraded."
        )
    if snapshot.get("shared_truth_coherence_status") != "COHERENT" and close_only_authority:
        return True, "Close-only BSA authority permits exact managed-exit recovery while entry Control Plane coherence is degraded."
    if snapshot.get("shared_truth_coherence_status") != "COHERENT":
        return False, "Control Plane Snapshot is not coherent."
    if classification == "CONTROL_PLANE_SNAPSHOT_READY":
        return True, "Control Plane Snapshot is ready."
    cleanup_state = supervisor == "SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME" or _snapshot_has_position_without_close(snapshot)
    if classification == "CONTROL_PLANE_SNAPSHOT_BLOCKED" and cleanup_state:
        return True, "Control Plane is blocked by the exact cleanup condition this managed-exit attach addresses."
    return False, f"Control Plane classification does not permit managed-exit attach: {classification or 'UNKNOWN'}."


def _safe_state_allows_managed_exit(safe_state: Mapping[str, Any]) -> tuple[bool, str]:
    if not safe_state:
        return False, "Runtime Safe-State Envelope is missing."
    if safe_state.get("live_money_eligible") is True:
        return False, "Safe-State reports live_money_eligible=true."
    if safe_state.get("paper_proof_invoked") is True:
        return False, "Safe-State reports paper_proof_invoked=true."
    classification = str(safe_state.get("safe_state_classification") or safe_state.get("classification") or "")
    managed_close_allowed = safe_state.get("managed_close_mutation_allowed") is True
    if classification != "SAFE_STATE_NORMAL" and not managed_close_allowed:
        return False, f"Safe-State classification is {classification or 'UNKNOWN'}."
    if safe_state.get("broker_mutation_allowed") is not True and not managed_close_allowed:
        return False, "Safe-State does not permit exact broker mutation for cleanup."
    if safe_state.get("observe_only") is True and not managed_close_allowed:
        return False, "Safe-State is observe-only."
    if list(safe_state.get("tripped_limits") or []) and not managed_close_allowed:
        return False, "Safe-State has tripped limits."
    return True, "Safe-State permits exact managed-exit cleanup."


def _close_only_authority_allows_unhealthy_runtime(
    *,
    config: TrackBManagedExitAttachConfig,
    broker_session_authority: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
    managed_orders: Mapping[str, Any],
    close_action: str,
) -> bool:
    allowed_uses = _mapping(broker_session_authority.get("allowed_uses"))
    if allowed_uses.get("managed_risk_reducing_close") is not True:
        return False
    if broker_session_authority.get("live_money_eligible") is True or broker_session_authority.get("paper_proof_invoked") is True:
        return False
    if broker_session_authority.get("broad_flatten_allowed") is True or broker_session_authority.get("global_flatten_allowed") is True:
        return False
    open_order_classification = str(open_order_truth.get("classification") or "")
    if open_order_classification != "NO_OPEN_ORDERS":
        return False
    if int(open_order_truth.get("unknown_open_order_count") or 0) != 0:
        return False
    managed_order_classification = str(managed_orders.get("classification") or "")
    if managed_order_classification not in {
        POSITION_WITHOUT_CLOSE_ORDER,
        BROKER_POSITION_WITHOUT_CLOSE_ORDER,
    } and not _managed_orders_have_exact_position_without_close_row(
        config=config,
        managed_orders=managed_orders,
        close_action=close_action,
    ):
        return False
    close_mode = str(broker_session_authority.get("risk_reducing_close_connection_mode") or "")
    if close_mode == "RISK_REDUCING_CLOSE_CAPABLE_ORDER_STATUS_DEGRADED":
        context = _mapping(broker_session_authority.get("degraded_exact_risk_reducing_close_context"))
        return context.get("ready") is True
    return True


def _classification_for_exit_authority_blockers(blockers: Sequence[str]) -> str:
    blocker_text = " ".join(str(item) for item in blockers)
    if "working_close" in blocker_text or "unknown_order" in blocker_text:
        return MANAGED_EXIT_BLOCKED_DUPLICATE_CLOSE_ORDER
    if "live_money" in blocker_text or "paper_proof" in blocker_text or "safe_state_hard_halt" in blocker_text:
        return MANAGED_EXIT_BLOCKED_CONTROL_PLANE
    return MANAGED_EXIT_BLOCKED_POSITION_MISMATCH


def _exit_authority_candidate_for_config(
    *,
    config: TrackBManagedExitAttachConfig,
    now: datetime,
    close_action: str,
) -> dict[str, Any]:
    report = build_track_b_exit_intent_dry_run_report(
        config=TrackBExitIntentDryRunReportConfig(repo_root=config.repo_root),
        now=now,
    )
    for candidate in report.get("candidate_exit_intents") or []:
        if not isinstance(candidate, Mapping):
            continue
        if (
            str(candidate.get("account_id") or "") == str(config.account_id)
            and str(candidate.get("local_symbol") or "") == str(config.local_symbol)
            and _int_or_none(candidate.get("con_id")) == config.con_id
            and str(candidate.get("candidate_close_action") or "") == str(close_action)
            and _decimal(candidate.get("candidate_close_qty")) == Decimal(str(config.quantity))
        ):
            return dict(candidate)
    return _synthetic_exit_authority_candidate_for_config(
        config=config,
        now=now,
        close_action=close_action,
    )


def _synthetic_exit_authority_candidate_for_config(
    *,
    config: TrackBManagedExitAttachConfig,
    now: datetime,
    close_action: str,
) -> dict[str, Any]:
    position_truth = _read_json(config.resolve(config.position_truth_path))
    safe_state = _read_json(config.resolve(config.safe_state_path))
    broker_session_authority = _read_json(config.resolve(config.broker_session_authority_path))
    open_order_truth = _read_json(config.resolve(config.open_order_truth_path))
    managed_orders = _read_json(config.resolve(config.managed_order_registry_path))
    broker_position = _exact_broker_position_for_config(config=config, position_truth=position_truth)
    if not broker_position:
        return {}
    qty = abs(_decimal(broker_position.get("quantity")) or Decimal("0"))
    if qty <= Decimal("0"):
        return {}
    side = "LONG" if (_decimal(broker_position.get("quantity")) or Decimal("0")) > 0 else "SHORT"
    intent = ExitIntent(
        exit_intent_id=f"exit_intent_attach_{config.account_id}_{config.local_symbol}_{config.con_id}".lower(),
        execution_domain=ExecutionDomain.TRACK_B_PAPER,
        account_id=config.account_id,
        instrument=config.instrument_family,
        local_symbol=config.local_symbol,
        con_id=config.con_id,
        position_side=side,
        owned_qty=str(qty),
        close_action=close_action,
        close_qty=str(config.quantity),
        remaining_qty_after=str(max(qty - Decimal(str(config.quantity)), Decimal("0"))),
        close_qty_source=CloseQtySource.RISK_POLICY,
        exit_type=ExitType.FULL_CLOSE if qty == Decimal(str(config.quantity)) else ExitType.PARTIAL_SCALE_OUT,
        exit_reason="managed_exit_attach_broker_scoped_risk_reduction",
        priority=50,
        urgency=ExitUrgency.NORMAL,
        price_policy={
            "type": "MANAGED_EXIT_ATTACH_LIMIT",
            "source": "track_b_managed_exit_attach",
            "requires_current_executable_price_before_apply": True,
        },
        idempotency_key="",
        allow_partial=qty != Decimal(str(config.quantity)),
        allow_reverse=False,
        source_policy_id="TRACK_B_MANAGED_EXIT_ATTACH_V1",
        generated_at=now,
        attribution={
            "lifecycle_id": config.lifecycle_id or None,
            "trade_id": None,
            "strategy_id": config.strategy_id or None,
            "lane_id": config.lane_id or None,
        },
        lifecycle_id=config.lifecycle_id or None,
        strategy_id=config.strategy_id or None,
        lane_id=config.lane_id or None,
        partial_policy_supported=qty != Decimal(str(config.quantity)),
        live_money_eligible=False,
        live_money_allowed=False,
        paper_proof_invoked=False,
        broad_flatten_allowed=False,
        global_flatten_allowed=False,
    )
    same_contract_unknown_orders = _same_contract_unknown_order_count_for_config(
        config=config,
        open_order_truth=open_order_truth,
    )
    state = ExitAuthorityCurrentState(
        execution_domain=ExecutionDomain.TRACK_B_PAPER,
        known_position=True,
        broker_position_side=side,
        broker_position_qty=str(qty),
        account_id=config.account_id,
        local_symbol=config.local_symbol,
        con_id=config.con_id,
        safe_state_hard_halt=_safe_state_has_hard_halt(safe_state),
        same_contract_working_close_qty=str(
            _same_contract_working_close_qty_for_config(
                config=config,
                managed_orders=managed_orders,
                open_order_truth=open_order_truth,
            )
        ),
        unrelated_unknown_order_count=max(_unknown_open_order_count(open_order_truth) - same_contract_unknown_orders, 0),
        same_contract_unknown_order_count=same_contract_unknown_orders,
        same_contract_unknown_order_could_over_close=same_contract_unknown_orders > 0,
        same_contract_unknown_order_over_close_ruled_out=False,
        reconciliation_clean=None,
        safe_state_allows_managed_close=_safe_state_allows_managed_exit(safe_state)[0],
        guardian_allows_exact_close=None,
        bsa_managed_risk_reducing_close=_mapping(broker_session_authority.get("allowed_uses")).get("managed_risk_reducing_close") is True,
        bsa_degraded_exact_close_ready=_mapping(
            broker_session_authority.get("degraded_exact_risk_reducing_close_context")
        ).get("ready")
        is True,
        live_money_eligible=broker_session_authority.get("live_money_eligible") is True
        or safe_state.get("live_money_eligible") is True,
        live_money_allowed=False,
        paper_proof_invoked=broker_session_authority.get("paper_proof_invoked") is True
        or safe_state.get("paper_proof_invoked") is True,
        broad_flatten_allowed=broker_session_authority.get("broad_flatten_allowed") is True,
        global_flatten_allowed=broker_session_authority.get("global_flatten_allowed") is True,
        attribution_status="PARTIALLY_ATTRIBUTED" if config.lifecycle_id else "UNATTRIBUTED",
        attribution_diagnostics={
            "lifecycle_id": config.lifecycle_id,
            "strategy_id": config.strategy_id,
            "lane_id": config.lane_id,
            "source": "track_b_managed_exit_attach_config",
            "blocks_authority": False,
        },
        diagnostics={
            "position_truth_classification": position_truth.get("classification"),
            "open_order_truth_classification": open_order_truth.get("classification"),
            "broker_session_authority_classification": broker_session_authority.get("classification"),
            "managed_order_registry_classification": managed_orders.get("classification"),
        },
    )
    decision = validate_exit_authority(intent=intent, current_state=state, validated_at=now)
    return {
        "instrument": intent.instrument,
        "local_symbol": intent.local_symbol,
        "con_id": intent.con_id,
        "account_id": intent.account_id,
        "position_side": intent.position_side.value,
        "broker_position_qty": str(qty),
        "candidate_close_action": intent.close_action.value,
        "candidate_close_qty": str(intent.close_qty),
        "price_policy": intent.price_policy,
        "attribution_status": decision.attribution_status.value,
        "attribution": intent.attribution.to_json_dict(),
        "exit_due": True,
        "exit_intent": intent.to_json_dict(),
        "authority_decision": decision.to_json_dict(),
        "block_reasons": list(decision.block_reasons),
    }


def _exact_broker_position_for_config(
    *,
    config: TrackBManagedExitAttachConfig,
    position_truth: Mapping[str, Any],
) -> dict[str, Any]:
    for row in position_truth.get("broker_positions") or []:
        if not isinstance(row, Mapping):
            continue
        account = _valid_account_id(row.get("account_id") or row.get("account"))
        if (
            account == config.account_id
            and str(row.get("local_symbol") or row.get("localSymbol") or "") == config.local_symbol
            and _int_or_none(row.get("con_id") or row.get("conId")) == config.con_id
        ):
            return dict(row)
    return {}


def _same_contract_working_close_qty_for_config(
    *,
    config: TrackBManagedExitAttachConfig,
    managed_orders: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
) -> Decimal:
    total = Decimal("0")
    rows = [*(managed_orders.get("managed_orders") or []), *(open_order_truth.get("broker_open_orders") or [])]
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("local_symbol") or row.get("contract") or "") != config.local_symbol:
            continue
        if row.get("working") is True or str(row.get("status") or "").upper() in {"SUBMITTED", "PRESUBMITTED", "PENDING_SUBMIT"}:
            total += abs(_decimal(row.get("remaining_quantity") or row.get("quantity")) or Decimal("0"))
    return total


def _same_contract_unknown_order_count_for_config(
    *,
    config: TrackBManagedExitAttachConfig,
    open_order_truth: Mapping[str, Any],
) -> int:
    return sum(
        1
        for row in open_order_truth.get("unknown_open_orders") or []
        if isinstance(row, Mapping)
        and str(row.get("local_symbol") or row.get("contract") or "") == config.local_symbol
    )


def _unknown_open_order_count(open_order_truth: Mapping[str, Any]) -> int:
    try:
        return int(open_order_truth.get("unknown_open_order_count") or 0)
    except (TypeError, ValueError):
        return len([row for row in open_order_truth.get("unknown_open_orders") or [] if isinstance(row, Mapping)])


def _safe_state_has_hard_halt(safe_state: Mapping[str, Any]) -> bool:
    classification = str(safe_state.get("safe_state_classification") or safe_state.get("classification") or "").upper()
    return "HARD" in classification or "HALT" in classification or "UNSAFE" in classification


def _managed_orders_have_exact_position_without_close_row(
    *,
    config: TrackBManagedExitAttachConfig,
    managed_orders: Mapping[str, Any],
    close_action: str,
) -> bool:
    rows = [row for row in managed_orders.get("managed_orders") or managed_orders.get("orders") or [] if isinstance(row, Mapping)]
    if not rows:
        return False
    exact_rows = []
    for row in rows:
        local_symbol = str(row.get("local_symbol") or row.get("contract") or "")
        row_action = str(row.get("required_close_action") or row.get("action") or "")
        quantity = _decimal(row.get("required_close_quantity") or row.get("quantity"))
        classification = str(row.get("classification") or "")
        if (
            local_symbol == config.local_symbol
            and row_action == close_action
            and quantity == Decimal(str(config.quantity))
            and classification in {POSITION_WITHOUT_CLOSE_ORDER, BROKER_POSITION_WITHOUT_CLOSE_ORDER}
            and row.get("working") is not True
            and not list(row.get("suspicious_reasons") or [])
        ):
            exact_rows.append(row)
    return len(exact_rows) == 1


def _control_plane_has_explicit_hard_hold(snapshot: Mapping[str, Any]) -> bool:
    hard_tokens = (
        "live_money",
        "paper_proof",
        "duplicate_writer",
        "hard_hold",
        "hard_unsafe",
        "broad_flatten",
        "global_flatten",
    )
    values: list[Any] = [
        snapshot.get("classification"),
        snapshot.get("top_line_classification"),
        snapshot.get("primary_blocking_agent_id"),
        snapshot.get("primary_blocking_reason"),
        snapshot.get("operator_explanation"),
        snapshot.get("paper_action_policy"),
    ]
    values.extend(snapshot.get("blockers") or [])
    values.extend(snapshot.get("prioritized_blockers") or [])
    for value in values:
        text = json.dumps(value, sort_keys=True) if isinstance(value, Mapping) else str(value or "")
        normalized = text.lower()
        if any(token in normalized for token in hard_tokens):
            return True
    return False


def _position_identity_matches(
    *,
    config: TrackBManagedExitAttachConfig,
    position_truth: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    selected_position: Mapping[str, Any] | None = None,
) -> tuple[bool, str]:
    candidates = list(position_truth.get("broker_positions") or [])
    reconciliation = _read_json(
        config.repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation_broker_positions = [
        dict(position)
        for position in list(reconciliation.get("track_b_broker_positions") or [])
        if isinstance(position, Mapping)
    ]
    candidates.extend(
        reconciliation_broker_positions
    )
    candidates.extend(
        _reconciled_lifecycle_position_identity_candidate(
            lifecycle_position=dict(position),
            broker_positions=reconciliation_broker_positions,
        )
        for position in list(reconciliation.get("track_b_lifecycle_positions") or [])
        if isinstance(position, Mapping)
    )
    selected = _mapping(selected_position)
    selected_broker = _mapping(selected.get("broker_position"))
    if selected_broker:
        candidates.append(
            {
                **selected_broker,
                "con_id": selected_broker.get("con_id") or selected.get("con_id"),
                "local_symbol": selected_broker.get("local_symbol") or selected.get("local_symbol"),
                "quantity": selected_broker.get("quantity") or selected.get("quantity"),
            }
        )
    for value in (live_position_status.get("positions_by_instrument") or {}).values():
        if isinstance(value, Mapping):
            candidates.append(dict(value))
    for position in candidates:
        if not isinstance(position, Mapping):
            continue
        local_symbol = str(position.get("local_symbol") or position.get("localSymbol") or "")
        con_id = _int_or_none(position.get("con_id") or position.get("conId"))
        account = _valid_account_id(position.get("account_id") or position.get("account"))
        quantity = _decimal(position.get("quantity"))
        if (
            account
            and account == config.account_id
            and local_symbol == config.local_symbol
            and con_id == config.con_id
            and quantity is not None
            and abs(quantity) == Decimal(str(config.quantity))
        ):
            return True, "Position identity matches."
    return False, "No exact active broker/lifecycle position matches account, contract, conId, and quantity."


def _reconciled_lifecycle_position_identity_candidate(
    *,
    lifecycle_position: Mapping[str, Any],
    broker_positions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    local_symbol = str(lifecycle_position.get("local_symbol") or lifecycle_position.get("localSymbol") or "").strip()
    symbol = str(
        lifecycle_position.get("track_b_root")
        or lifecycle_position.get("instrument_family")
        or lifecycle_position.get("symbol")
        or ""
    ).strip().upper()
    broker = _matching_reconciled_broker_position(
        local_symbol=local_symbol,
        symbol=symbol,
        broker_positions=broker_positions,
    )
    return {
        "account_id": broker.get("account_id") or broker.get("account") or lifecycle_position.get("account_id"),
        "local_symbol": local_symbol,
        "con_id": lifecycle_position.get("con_id") or lifecycle_position.get("conId") or broker.get("con_id") or broker.get("conId"),
        "quantity": lifecycle_position.get("quantity") or broker.get("quantity"),
    }


def _matching_reconciled_broker_position(
    *,
    local_symbol: str,
    symbol: str,
    broker_positions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    requested_local = str(local_symbol or "").strip().upper()
    requested_symbol = str(symbol or "").strip().upper()
    for position in broker_positions:
        broker_local = str(position.get("local_symbol") or position.get("localSymbol") or "").strip().upper()
        broker_symbol = str(position.get("track_b_root") or position.get("symbol") or "").strip().upper()
        if requested_local and broker_local and requested_local == broker_local:
            return dict(position)
        if requested_symbol and broker_symbol and requested_symbol == broker_symbol and not requested_local:
            return dict(position)
    return {}


def _lifecycle_matches(
    *,
    config: TrackBManagedExitAttachConfig,
    lifecycle_report: Mapping[str, Any],
    managed_exit_policy_id: str,
    selected_position: Mapping[str, Any] | None = None,
    retryable_close_quantity: int | None = None,
) -> tuple[bool, str]:
    checks = {
        "lifecycle_id": config.lifecycle_id,
        "strategy_id": config.strategy_id,
        "account_id": config.account_id,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
    }
    for key, expected in checks.items():
        if str(lifecycle_report.get(key) or "") != str(expected):
            selected_ok = _selected_current_scope_lifecycle_matches(
                config=config,
                lifecycle_report=lifecycle_report,
                managed_exit_policy_id=managed_exit_policy_id,
                selected_position=selected_position,
            )
            if selected_ok:
                break
            return False, f"Lifecycle {key} mismatch."
    if _int_or_none(lifecycle_report.get("con_id")) != config.con_id:
        selected_ok = _selected_current_scope_lifecycle_matches(
            config=config,
            lifecycle_report=lifecycle_report,
            managed_exit_policy_id=managed_exit_policy_id,
            selected_position=selected_position,
        )
        if not selected_ok:
            return False, "Lifecycle con_id mismatch."
    lifecycle_classification = str(lifecycle_report.get("paper_lifecycle_classification") or "")
    if lifecycle_classification != "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED":
        previous_attach_guard = (
            lifecycle_classification == "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED"
            and lifecycle_report.get("broker_state_mutated") is False
            and not lifecycle_report.get("close_intent")
            and not lifecycle_report.get("close_submit_attempt")
            and (
                "latest decision bar source DATABENTO_LIVE_ARTIFACT"
                in str(lifecycle_report.get("primary_blocker") or "")
                or "requires positive configured quantity"
                in str(lifecycle_report.get("primary_blocker") or "")
            )
        )
        retryable_unmutated_close_review = _retryable_unmutated_close_review(
            config=config,
            lifecycle_report=lifecycle_report,
            managed_exit_policy_id=managed_exit_policy_id,
            retryable_close_quantity=retryable_close_quantity,
        )
        if not previous_attach_guard and not retryable_unmutated_close_review:
            return False, "Lifecycle is not OPEN_MANAGED."
    if str(lifecycle_report.get("managed_exit_policy_id") or "") != managed_exit_policy_id and not _selected_current_scope_lifecycle_matches(
        config=config,
        lifecycle_report=lifecycle_report,
        managed_exit_policy_id=managed_exit_policy_id,
        selected_position=selected_position,
    ):
        return False, "Lifecycle managed exit policy mismatch."
    return True, "Lifecycle identity matches."


def _selected_current_scope_lifecycle_matches(
    *,
    config: TrackBManagedExitAttachConfig,
    lifecycle_report: Mapping[str, Any],
    managed_exit_policy_id: str,
    selected_position: Mapping[str, Any] | None,
) -> bool:
    selected = _mapping(selected_position)
    lifecycle_position = _mapping(selected.get("lifecycle_position"))
    units = [
        _mapping(item)
        for item in (selected.get("lifecycle_units") or lifecycle_position.get("lifecycle_units") or [])
        if isinstance(item, Mapping)
    ]
    selected_lifecycle_ids = {
        str(value or "").strip()
        for value in [
            selected.get("lifecycle_id"),
            lifecycle_position.get("lifecycle_id"),
            *(unit.get("lifecycle_id") for unit in units),
        ]
        if str(value or "").strip()
    }
    if config.lifecycle_id not in selected_lifecycle_ids:
        return False
    if str(selected.get("strategy_id") or lifecycle_position.get("strategy_id") or "") != config.strategy_id:
        return False
    if _valid_account_id(selected.get("account_id") or lifecycle_position.get("account_id")) not in {"", config.account_id}:
        return False
    if str(selected.get("local_symbol") or lifecycle_position.get("local_symbol") or "") != config.local_symbol:
        return False
    if _int_or_none(selected.get("con_id") or lifecycle_position.get("con_id")) != config.con_id:
        return False
    if str(lifecycle_report.get("strategy_id") or "") != config.strategy_id:
        return False
    if _valid_account_id(lifecycle_report.get("account_id")) not in {"", config.account_id}:
        return False
    if str(lifecycle_report.get("local_symbol") or "") != config.local_symbol:
        return False
    if _int_or_none(lifecycle_report.get("con_id")) != config.con_id:
        return False
    selected_policy = str(selected.get("managed_exit_policy_id") or lifecycle_position.get("managed_exit_policy_id") or "").strip()
    report_policy = str(lifecycle_report.get("managed_exit_policy_id") or "").strip()
    if selected_policy != managed_exit_policy_id and report_policy != managed_exit_policy_id:
        return False
    return str(lifecycle_report.get("paper_lifecycle_classification") or "") == "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED"


def _retryable_unmutated_close_review(
    *,
    config: TrackBManagedExitAttachConfig,
    lifecycle_report: Mapping[str, Any],
    managed_exit_policy_id: str,
    retryable_close_quantity: int | None = None,
) -> bool:
    if lifecycle_report.get("paper_lifecycle_classification") != "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED":
        return False
    if lifecycle_report.get("broker_state_mutated") is True:
        return False
    if lifecycle_report.get("close_fill"):
        return False
    close_intent = lifecycle_report.get("close_intent") if isinstance(lifecycle_report.get("close_intent"), Mapping) else {}
    close_submit = (
        lifecycle_report.get("close_submit_attempt")
        if isinstance(lifecycle_report.get("close_submit_attempt"), Mapping)
        else {}
    )
    if close_submit.get("submitted") is True or close_submit.get("broker_state_mutated") is True:
        return False
    return (
        str(close_intent.get("lifecycle_id") or "") == config.lifecycle_id
        and str(close_intent.get("strategy_id") or "") == config.strategy_id
        and str(close_intent.get("local_symbol") or "") == config.local_symbol
        and _int_or_none(close_intent.get("con_id")) == config.con_id
        and str(close_intent.get("order_action") or "") == close_action_for_position_side(config.side)
        and _decimal(close_intent.get("quantity")) == Decimal(str(retryable_close_quantity or config.quantity))
        and str(close_intent.get("managed_exit_policy_id") or "") == managed_exit_policy_id
    )


def _prior_lifecycle_close_submit_blocker(lifecycle_report: Mapping[str, Any]) -> str | None:
    close_fill = lifecycle_report.get("close_fill") if isinstance(lifecycle_report.get("close_fill"), Mapping) else {}
    if close_fill:
        return "Lifecycle already records a managed close fill; duplicate managed-exit attach is blocked."
    close_submit = (
        lifecycle_report.get("close_submit_attempt")
        if isinstance(lifecycle_report.get("close_submit_attempt"), Mapping)
        else {}
    )
    broker_order_id = str(close_submit.get("broker_order_id") or "").strip()
    if broker_order_id and close_submit.get("broker_state_mutated") is True:
        return (
            "Lifecycle already records managed close broker order "
            f"{broker_order_id}; duplicate managed-exit attach is blocked until broker/order truth converges."
        )
    return None


def _prior_lifecycle_close_is_stale_diagnostic(
    *,
    prior_lifecycle_close: str | None,
    exit_authority_allows: bool,
    position_ok: bool,
    duplicate_close: str | None,
) -> bool:
    if not prior_lifecycle_close:
        return False
    if "close fill" not in prior_lifecycle_close.lower():
        return False
    if duplicate_close:
        return False
    return exit_authority_allows and position_ok


def _aggregate_lifecycle_unit_blocker(
    *,
    config: TrackBManagedExitAttachConfig,
    managed_exit_policy_id: str,
) -> str | None:
    units = [dict(item) for item in config.aggregate_lifecycle_units if isinstance(item, Mapping)]
    if len(units) <= 1:
        return None
    lifecycle_ids = [str(item.get("lifecycle_id") or "").strip() for item in units]
    if len([item for item in lifecycle_ids if item]) != len(set(lifecycle_ids)):
        return "Aggregate lifecycle group has missing or duplicate lifecycle ids."
    total = sum((_decimal(item.get("quantity")) or Decimal("0") for item in units), Decimal("0"))
    if total != Decimal(str(config.quantity)):
        return "Aggregate lifecycle unit quantity does not match planned close quantity."
    for unit in units:
        report_path = _unit_lifecycle_report_path(config=config, unit=unit)
        report = _read_json(report_path)
        if not report:
            return f"Aggregate lifecycle unit report is missing: {report_path}."
        unit_config = replace(
            config,
            lifecycle_id=str(unit.get("lifecycle_id") or ""),
            quantity=int(abs(_decimal(unit.get("quantity")) or Decimal("0"))) or 1,
        )
        lifecycle_ok, lifecycle_reason = _lifecycle_matches(
            config=unit_config,
            lifecycle_report=report,
            managed_exit_policy_id=managed_exit_policy_id,
            retryable_close_quantity=config.quantity,
        )
        if not lifecycle_ok:
            return f"Aggregate lifecycle unit {unit_config.lifecycle_id} mismatch: {lifecycle_reason}"
        prior = _prior_lifecycle_close_submit_blocker(report)
        if prior:
            return f"Aggregate lifecycle unit {unit_config.lifecycle_id} already has close evidence: {prior}"
    return None


def _aggregate_exit_group(config: TrackBManagedExitAttachConfig) -> dict[str, Any]:
    units = [dict(item) for item in config.aggregate_lifecycle_units if isinstance(item, Mapping)]
    return {
        "enabled": bool(units),
        "unit_count": len(units),
        "lifecycle_ids": [item.get("lifecycle_id") for item in units],
        "entry_order_ids": [item.get("entry_order_id") for item in units],
        "entry_perm_ids": [item.get("entry_perm_id") for item in units if item.get("entry_perm_id")],
        "aggregate_close_quantity": str(config.quantity),
        "coordinated_lifecycle_close_required": len(units) > 1,
    }


def _sync_aggregate_lifecycle_close_reports(
    *,
    config: TrackBManagedExitAttachConfig,
    close_source_report: Mapping[str, Any],
    close_source_report_path: Path,
    now: datetime,
) -> list[Path]:
    units = [dict(item) for item in config.aggregate_lifecycle_units if isinstance(item, Mapping)]
    if not units:
        return [close_source_report_path]
    close_intent = _mapping(close_source_report.get("close_intent"))
    close_submit = _mapping(close_source_report.get("close_submit_attempt"))
    close_fill = _mapping(close_source_report.get("close_fill"))
    aggregate_group = _aggregate_exit_group(config)
    written: list[Path] = []
    for unit in units:
        report_path = _unit_lifecycle_report_path(config=config, unit=unit)
        report = _read_json(report_path)
        if not report:
            continue
        unit_qty = _decimal(unit.get("quantity")) or Decimal("1")
        unit_lifecycle_id = str(unit.get("lifecycle_id") or report.get("lifecycle_id") or "")
        unit_close_intent = {
            **close_intent,
            "lifecycle_id": unit_lifecycle_id,
            "quantity": _decimal_display(abs(unit_qty)),
            "aggregate_close_group": aggregate_group,
        }
        unit_close_submit = {
            **close_submit,
            "lifecycle_id": unit_lifecycle_id,
            "quantity": _decimal_display(abs(unit_qty)),
            "aggregate_order_quantity": str(config.quantity),
            "aggregate_close_group": aggregate_group,
        }
        unit_close_fill = {
            **close_fill,
            "lifecycle_id": unit_lifecycle_id,
            "quantity": _decimal_display(abs(unit_qty)),
            "aggregate_order_quantity": str(config.quantity),
            "aggregate_close_group": aggregate_group,
        }
        synced = {
            **report,
            "paper_lifecycle_classification": close_source_report.get("paper_lifecycle_classification"),
            "strategy_managed_lifecycle_classification": close_source_report.get(
                "strategy_managed_lifecycle_classification"
            )
            or close_source_report.get("paper_lifecycle_classification"),
            "final_position_status": close_source_report.get("final_position_status"),
            "final_broker_state_classification": close_source_report.get("final_broker_state_classification"),
            "broker_reconciled": close_source_report.get("broker_reconciled"),
            "close_intent": unit_close_intent,
            "close_submit_attempt": unit_close_submit,
            "close_fill": unit_close_fill,
            "aggregate_managed_exit_close": {
                **aggregate_group,
                "close_source_lifecycle_id": close_source_report.get("lifecycle_id"),
                "close_source_report_path": str(close_source_report_path),
                "synced_at": now.isoformat(),
            },
            "broker_state_mutated": close_source_report.get("broker_state_mutated"),
            "submit_attempted": close_source_report.get("submit_attempted"),
            "updated_at": now.isoformat(),
        }
        write_json_atomic(report_path, to_jsonable(synced))
        written.append(report_path)
    return written or [close_source_report_path]


def _unit_lifecycle_report_path(*, config: TrackBManagedExitAttachConfig, unit: Mapping[str, Any]) -> Path:
    path = unit.get("paper_lifecycle_report_path")
    if path:
        return config.resolve(Path(str(path)))
    lifecycle_id = str(unit.get("lifecycle_id") or "")
    return config.resolve(config.lifecycle_output_root) / lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"


def _duplicate_close_order(
    *,
    config: TrackBManagedExitAttachConfig,
    managed_orders: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
    close_action: str,
) -> str | None:
    text = " ".join(
        str(value or "")
        for value in (
            managed_orders.get("classification"),
            open_order_truth.get("classification"),
        )
    ).upper()
    if "DUPLICATE" in text or "WORKING_CLOSE_ORDER" in text or "OPEN_CLOSE_ORDER_WORKING" in text:
        return f"Existing/duplicate close order state blocks attach: {text.strip()}."
    for order in managed_orders.get("managed_orders") or []:
        if not isinstance(order, Mapping):
            continue
        if (
            order.get("working") is True
            and str(order.get("local_symbol") or order.get("contract") or "") == config.local_symbol
            and str(order.get("action") or "") == close_action
            and _decimal(order.get("quantity")) == Decimal(str(config.quantity))
        ):
            return "Exact working close order already exists."
    return None


def _snapshot_has_position_without_close(snapshot: Mapping[str, Any]) -> bool:
    text = " ".join(
        str(snapshot.get(key) or "")
        for key in (
            "open_order_truth_classification",
            "managed_order_registry_classification",
            "paper_action_policy",
            "operator_explanation",
        )
    )
    return BROKER_POSITION_WITHOUT_CLOSE_ORDER in text or POSITION_WITHOUT_CLOSE_ORDER in text


def _close_intent_preview(
    *,
    config: TrackBManagedExitAttachConfig,
    close_action: str,
    close_limit_price: str | None,
    completed_bar_count: int,
    managed_exit_policy_id: str,
    required_completed_5m_bars: int,
    exit_strategy_id: str,
    exit_profile_id: str,
) -> dict[str, Any]:
    return {
        "intent_schema_version": "track_b_strategy_managed_paper_close_intent_v1",
        "lifecycle_id": config.lifecycle_id,
        "strategy_id": config.strategy_id,
        "account_id": config.account_id,
        "contract_key": config.contract_key,
        "expiry": config.expiry,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "side": config.side,
        "order_action": close_action,
        "quantity": config.quantity,
        "order_type": "LMT",
        "close_limit_price": close_limit_price,
        "exit_family": "DIAGNOSTIC_TIME",
        "close_reason": "TIME_BOXED_EXIT",
        "managed_exit_policy_id": managed_exit_policy_id,
        "exit_strategy_id": exit_strategy_id,
        "exit_profile_id": exit_profile_id,
        "elapsed_completed_5m_bars": completed_bar_count,
        "required_completed_5m_bars": required_completed_5m_bars,
        "would_submit": False,
        "submit_allowed": False,
    }


def _target_identity(
    *,
    config: TrackBManagedExitAttachConfig,
    action: str,
    close_limit_price: str | None,
    managed_exit_policy_id: str,
    exit_strategy_id: str,
    exit_profile_id: str,
) -> dict[str, str]:
    return {
        "account_id": config.account_id,
        "strategy_id": config.strategy_id,
        "lane_id": config.lane_id,
        "runtime_generation_id": config.runtime_generation_id,
        "lifecycle_id": config.lifecycle_id,
        "symbol": config.instrument_family,
        "contract": config.local_symbol,
        "contract_key": config.contract_key,
        "con_id": str(config.con_id),
        "expiry": config.expiry,
        "action": action,
        "quantity": str(config.quantity),
        "order_type": "LMT",
        "limit_price": str(close_limit_price or ""),
        "managed_exit_policy_id": managed_exit_policy_id,
        "exit_strategy_id": exit_strategy_id,
        "exit_profile_id": exit_profile_id,
    }


def _lifecycle_report_path(
    *,
    config: TrackBManagedExitAttachConfig,
    live_position_status: Mapping[str, Any],
    selected_position: Mapping[str, Any] | None = None,
) -> Path:
    selected = _mapping(selected_position)
    lifecycle_position = _mapping(selected.get("lifecycle_position"))
    for source in (selected, lifecycle_position):
        path_value = source.get("paper_lifecycle_report_path")
        if path_value:
            return config.resolve(Path(str(path_value)))
    position = (live_position_status.get("positions_by_instrument") or {}).get(config.contract_key)
    if isinstance(position, Mapping) and position.get("paper_lifecycle_report_path"):
        return config.resolve(Path(str(position.get("paper_lifecycle_report_path"))))
    return config.resolve(config.lifecycle_output_root) / config.lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"


def _entry_timestamp(
    *,
    lifecycle_report: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    config: TrackBManagedExitAttachConfig,
) -> str | None:
    entry_fill = lifecycle_report.get("entry_fill") if isinstance(lifecycle_report.get("entry_fill"), Mapping) else {}
    position = (live_position_status.get("positions_by_instrument") or {}).get(config.contract_key)
    position_map = position if isinstance(position, Mapping) else {}
    return (
        entry_fill.get("filled_at")
        or lifecycle_report.get("entry_timestamp")
        or position_map.get("entry_timestamp")
        or position_map.get("as_of")
    )


def _phase1_path(*, config: TrackBManagedExitAttachConfig, timeframe: str) -> Path:
    return config.resolve(config.phase1_market_data_root) / config.instrument_family.upper() / timeframe / "latest_runtime_candles.json"


def _completed_bar_timestamps_after_entry(*, payload: Mapping[str, Any], entry_timestamp: str | None) -> list[str]:
    entry = _parse_time(entry_timestamp)
    values: list[str] = []
    for bar in _payload_bars(payload):
        timestamp = bar.get("bar_end") or bar.get("candle_timestamp") or bar.get("timestamp")
        parsed = _parse_time(timestamp)
        if parsed is not None and (entry is None or parsed > entry):
            values.append(parsed.isoformat())
    return values


def _latest_price(payload: Mapping[str, Any]) -> str | None:
    bars = _payload_bars(payload)
    if not bars:
        return None
    value = bars[-1].get("close") or bars[-1].get("last_price")
    return None if value in {None, ""} else str(value)


def _payload_bars(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw = payload.get("bars") or payload.get("candles") or payload.get("completed_5m_candles") or []
    return [item for item in raw if isinstance(item, Mapping)] if isinstance(raw, list) else []


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _parse_time(value: object) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001 - malformed artifact values fail closed elsewhere.
        return None


def _decimal_display(value: Decimal | None) -> str | None:
    if value is None:
        return None
    if value == value.to_integral_value():
        return str(value.quantize(Decimal("1")))
    return str(value.normalize())


def _int_or_none(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--account", default="DUM882026")
    parser.add_argument("--strategy-id", default="MNQ_FIRST_BULL_SNAP_TURN_V1")
    parser.add_argument("--lane-id", default="mnq_first_bull_snap_turn")
    parser.add_argument("--runtime-generation-id", default="track-b-paper-runtime-generation-20260525T072612Z")
    parser.add_argument(
        "--lifecycle-id",
        default=(
            "strategy_managed_track_b_multi_strategy_runtime_cycle_36fa949343974202810c89297c817390_"
            "mnq_first_bull_snap_turn_v1"
        ),
    )
    parser.add_argument("--instrument-family", default="MNQ")
    parser.add_argument("--contract-key", default="MNQ-202606")
    parser.add_argument("--local-symbol", default="MNQM6")
    parser.add_argument("--con-id", type=int, default=770561201)
    parser.add_argument("--expiry", default="20260618")
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument("--side", default="LONG")
    parser.add_argument("--close-limit-price")
    parser.add_argument("--exit-strategy-id", default="timeboxed_3x5m_managed_limit_close_v1")
    parser.add_argument("--exit-profile-id", default="MNQ_SNAP_TURN_TIMEBOX_3X5M_V1")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--operator-authorized-managed-exit", action="store_true")
    parser.add_argument("--skip-control-plane-refresh", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    config = TrackBManagedExitAttachConfig(
        repo_root=args.repo_root,
        account_id=args.account,
        expected_account_id=args.account,
        strategy_id=args.strategy_id,
        lane_id=args.lane_id,
        runtime_generation_id=args.runtime_generation_id,
        lifecycle_id=args.lifecycle_id,
        instrument_family=args.instrument_family,
        contract_key=args.contract_key,
        local_symbol=args.local_symbol,
        con_id=args.con_id,
        expiry=args.expiry,
        quantity=args.quantity,
        side=args.side,
        close_limit_price=args.close_limit_price,
        exit_strategy_id=args.exit_strategy_id,
        exit_profile_id=args.exit_profile_id,
        apply=args.apply,
        operator_authorized_managed_exit=args.operator_authorized_managed_exit,
        refresh_control_plane=not args.skip_control_plane_refresh,
    )
    payload = run_track_b_managed_exit_attach(config=config)
    if args.json:
        print(json.dumps(to_jsonable(payload), indent=2, sort_keys=True))
    else:
        print(f"classification={payload.get('classification')}")
        print(f"timebox_exit_eligible={payload.get('timebox_exit_eligible')}")
        print(f"close_intent={json.dumps(payload.get('close_intent_preview'), sort_keys=True)}")
        if payload.get("blockers"):
            print(f"blockers={payload.get('blockers')}")
    return 0 if payload.get("classification") in {MANAGED_EXIT_APPLY_DISABLED, MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE, MANAGED_EXIT_APPLIED_OR_PENDING} else 2


if __name__ == "__main__":
    raise SystemExit(main())
