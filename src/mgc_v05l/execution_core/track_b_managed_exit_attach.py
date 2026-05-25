"""Guarded managed-exit attach boundary for existing Track B PAPER positions.

This command plans, and only with explicit flags applies, the intended managed
close for an already OPEN_MANAGED strategy position. It is not a flatten tool
and it does not create unmanaged broker orders.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.models import require_aware_datetime, to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_control_plane_snapshot import (
    TrackBControlPlaneSnapshotConfig,
    build_track_b_control_plane_snapshot,
    write_track_b_control_plane_snapshot,
)
from mgc_v05l.execution_core.track_b_managed_order_registry import (
    DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT,
    POSITION_WITHOUT_CLOSE_ORDER,
)
from mgc_v05l.execution_core.track_b_open_order_truth import (
    BROKER_POSITION_WITHOUT_CLOSE_ORDER,
    DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_paper_trade_ledger import (
    DEFAULT_TRACK_B_LIVE_POSITION_STATUS_JSON,
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT,
    DEFAULT_TRACK_B_PAPER_TRADE_SUMMARY_JSON,
)
from mgc_v05l.execution_core.track_b_position_truth_monitor import DEFAULT_POSITION_TRUTH_ARTIFACT
from mgc_v05l.execution_core.track_b_runtime_safe_state_envelope import DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import (
    DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT,
    TrackBManagedExitPolicy,
    TrackBStrategyManagedPaperLifecycleConfig,
    maintain_open_track_b_strategy_managed_paper_lifecycle,
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
    exit_strategy_id: str = "timeboxed_3x5m_managed_limit_close_v1"
    exit_profile_id: str = "MNQ_SNAP_TURN_TIMEBOX_3X5M_V1"
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
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    live_position_status_path: Path = DEFAULT_TRACK_B_LIVE_POSITION_STATUS_JSON
    paper_trade_summary_path: Path = DEFAULT_TRACK_B_PAPER_TRADE_SUMMARY_JSON
    phase1_market_data_root: Path = DEFAULT_PHASE1_RUNTIME_MARKET_DATA_ROOT
    lifecycle_output_root: Path = DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT
    paper_trade_ledger_output_root: Path = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT
    output_path: Path = DEFAULT_MANAGED_EXIT_ATTACH_PLAN

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

    snapshot = _read_json(config.resolve(config.control_plane_snapshot_path))
    safe_state = _read_json(config.resolve(config.safe_state_path))
    open_order_truth = _read_json(config.resolve(config.open_order_truth_path))
    position_truth = _read_json(config.resolve(config.position_truth_path))
    managed_orders = _read_json(config.resolve(config.managed_order_registry_path))
    live_position_status = _read_json(config.resolve(config.live_position_status_path))
    lifecycle_path = _lifecycle_report_path(config=config, live_position_status=live_position_status)
    lifecycle_report = _read_json(lifecycle_path)

    bars_payload = _read_json(_phase1_path(config=config, timeframe="5m"))
    one_minute_payload = _read_json(_phase1_path(config=config, timeframe="1m"))
    entry_timestamp = _entry_timestamp(lifecycle_report=lifecycle_report, live_position_status=live_position_status, config=config)
    completed_bars = _completed_bar_timestamps_after_entry(payload=bars_payload, entry_timestamp=entry_timestamp)
    completed_bar_count = len(completed_bars)
    latest_price = _latest_price(one_minute_payload) or _latest_price(bars_payload)
    close_limit_price = config.close_limit_price or _derive_close_limit_price(
        latest_price=latest_price,
        side=config.side,
        tick_size=Decimal(str(config.tick_size)),
        offset_ticks=config.exit_price_offset_ticks,
    )
    close_action = "SELL" if config.side.upper() == "LONG" else "BUY"
    close_intent = _close_intent_preview(
        config=config,
        close_action=close_action,
        close_limit_price=close_limit_price,
        completed_bar_count=completed_bar_count,
    )

    blockers: list[str] = []
    control_plane_ok, control_plane_reason = _control_plane_allows_managed_exit(snapshot)
    safe_state_ok, safe_state_reason = _safe_state_allows_managed_exit(safe_state)
    position_ok, position_reason = _position_identity_matches(config=config, position_truth=position_truth, live_position_status=live_position_status)
    lifecycle_ok, lifecycle_reason = _lifecycle_matches(config=config, lifecycle_report=lifecycle_report)
    duplicate_close = _duplicate_close_order(config=config, managed_orders=managed_orders, open_order_truth=open_order_truth, close_action=close_action)

    if not control_plane_ok:
        blockers.append(control_plane_reason)
        classification = MANAGED_EXIT_BLOCKED_CONTROL_PLANE
    elif not safe_state_ok:
        blockers.append(safe_state_reason)
        classification = MANAGED_EXIT_BLOCKED_SAFE_STATE
    elif not position_ok or not lifecycle_ok:
        blockers.extend(reason for reason in (position_reason, lifecycle_reason) if reason)
        classification = MANAGED_EXIT_BLOCKED_POSITION_MISMATCH
    elif duplicate_close:
        blockers.append(duplicate_close)
        classification = MANAGED_EXIT_BLOCKED_DUPLICATE_CLOSE_ORDER
    elif completed_bar_count < int(config.required_completed_5m_bars):
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
            if completed_bar_count >= int(config.required_completed_5m_bars) and not blockers
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
        "open_order_truth_classification": open_order_truth.get("classification"),
        "managed_order_registry_classification": managed_orders.get("classification"),
        "position_truth_classification": position_truth.get("classification"),
        "target_identity": _target_identity(config=config, action=close_action, close_limit_price=close_limit_price),
        "position_identity_verified": position_ok,
        "lifecycle_identity_verified": lifecycle_ok,
        "duplicate_close_order_detected": bool(duplicate_close),
        "managed_exit_policy_id": config.managed_exit_policy_id,
        "exit_strategy_id": config.exit_strategy_id,
        "exit_profile_id": config.exit_profile_id,
        "exit_roster_compatible": True,
        "exit_roster_role": "strategy_managed_position_close",
        "required_completed_5m_bars": int(config.required_completed_5m_bars),
        "completed_5m_bars_since_entry": completed_bar_count,
        "completed_5m_bar_timestamps_since_entry": completed_bars,
        "timebox_exit_eligible": completed_bar_count >= int(config.required_completed_5m_bars),
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

    apply_result = _apply_managed_exit(config=config, lifecycle_report=lifecycle_report, completed_bar_count=completed_bar_count, close_limit_price=close_limit_price, now=actual_now)
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


def _apply_managed_exit(
    *,
    config: TrackBManagedExitAttachConfig,
    lifecycle_report: Mapping[str, Any],
    completed_bar_count: int,
    close_limit_price: str | None,
    now: datetime,
) -> dict[str, Any]:
    lifecycle_config = TrackBStrategyManagedPaperLifecycleConfig(
        mode=config.mode,
        account_id=config.account_id,
        expected_account_id=config.expected_account_id,
        strategy_id=config.strategy_id,
        instrument_family=config.instrument_family,
        contract_key=config.contract_key,
        local_symbol=config.local_symbol,
        con_id=config.con_id,
        side=config.side,
        quantity=config.quantity,
        close_limit_price=close_limit_price,
        managed_exit_policy_id=config.managed_exit_policy_id,
        managed_exit_policy_max_completed_5m_bars=config.required_completed_5m_bars,
        completed_5m_bars_since_entry=completed_bar_count,
        fill_timestamp_source="BROKER_ENTRY_FILL",
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
    return {
        "classification": result.report.get("paper_lifecycle_classification") or result.classification.value,
        "lifecycle_report_path": str(result.report_json),
        "submit_attempted": result.report.get("submit_attempted") is True,
        "broker_state_mutated": result.report.get("broker_state_mutated") is True,
        "close_intent": result.report.get("close_intent"),
        "close_submit_attempt": result.report.get("close_submit_attempt"),
        "close_fill": result.report.get("close_fill"),
        "primary_blocker": result.report.get("primary_blocker"),
    }


def _control_plane_allows_managed_exit(snapshot: Mapping[str, Any]) -> tuple[bool, str]:
    if not snapshot:
        return False, "Control Plane Snapshot is missing."
    if snapshot.get("shared_truth_coherence_status") != "COHERENT":
        return False, "Control Plane Snapshot is not coherent."
    if snapshot.get("live_money_eligible") is True:
        return False, "live_money_eligible=true blocks managed-exit attach."
    if snapshot.get("paper_proof_invoked") is True:
        return False, "paper_proof_invoked=true blocks managed-exit attach."
    classification = str(snapshot.get("classification") or "")
    supervisor = str(snapshot.get("runtime_supervisor_classification") or "")
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
    if classification != "SAFE_STATE_NORMAL":
        return False, f"Safe-State classification is {classification or 'UNKNOWN'}."
    if safe_state.get("broker_mutation_allowed") is not True:
        return False, "Safe-State does not permit exact broker mutation for cleanup."
    if safe_state.get("observe_only") is True:
        return False, "Safe-State is observe-only."
    if list(safe_state.get("tripped_limits") or []):
        return False, "Safe-State has tripped limits."
    return True, "Safe-State permits exact managed-exit cleanup."


def _position_identity_matches(
    *,
    config: TrackBManagedExitAttachConfig,
    position_truth: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
) -> tuple[bool, str]:
    candidates = list(position_truth.get("broker_positions") or [])
    for value in (live_position_status.get("positions_by_instrument") or {}).values():
        if isinstance(value, Mapping):
            candidates.append(dict(value))
    for position in candidates:
        if not isinstance(position, Mapping):
            continue
        local_symbol = str(position.get("local_symbol") or position.get("localSymbol") or "")
        con_id = _int_or_none(position.get("con_id") or position.get("conId"))
        account = str(position.get("account_id") or position.get("account") or config.account_id)
        quantity = _decimal(position.get("quantity"))
        if (
            account == config.account_id
            and local_symbol == config.local_symbol
            and con_id == config.con_id
            and quantity == Decimal(str(config.quantity))
        ):
            return True, "Position identity matches."
    return False, "No exact active broker/lifecycle position matches account, contract, conId, and quantity."


def _lifecycle_matches(*, config: TrackBManagedExitAttachConfig, lifecycle_report: Mapping[str, Any]) -> tuple[bool, str]:
    checks = {
        "lifecycle_id": config.lifecycle_id,
        "strategy_id": config.strategy_id,
        "account_id": config.account_id,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
    }
    for key, expected in checks.items():
        if str(lifecycle_report.get(key) or "") != str(expected):
            return False, f"Lifecycle {key} mismatch."
    if _int_or_none(lifecycle_report.get("con_id")) != config.con_id:
        return False, "Lifecycle con_id mismatch."
    if lifecycle_report.get("paper_lifecycle_classification") != "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED":
        return False, "Lifecycle is not OPEN_MANAGED."
    if str(lifecycle_report.get("managed_exit_policy_id") or "") != config.managed_exit_policy_id:
        return False, "Lifecycle managed exit policy mismatch."
    return True, "Lifecycle identity matches."


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
) -> dict[str, Any]:
    return {
        "intent_schema_version": "track_b_strategy_managed_paper_close_intent_v1",
        "lifecycle_id": config.lifecycle_id,
        "strategy_id": config.strategy_id,
        "account_id": config.account_id,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "side": config.side,
        "order_action": close_action,
        "quantity": config.quantity,
        "order_type": "LMT",
        "close_limit_price": close_limit_price,
        "exit_family": "DIAGNOSTIC_TIME",
        "close_reason": "TIME_BOXED_EXIT",
        "managed_exit_policy_id": config.managed_exit_policy_id,
        "exit_strategy_id": config.exit_strategy_id,
        "exit_profile_id": config.exit_profile_id,
        "elapsed_completed_5m_bars": completed_bar_count,
        "required_completed_5m_bars": int(config.required_completed_5m_bars),
        "would_submit": False,
        "submit_allowed": False,
    }


def _target_identity(*, config: TrackBManagedExitAttachConfig, action: str, close_limit_price: str | None) -> dict[str, str]:
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
        "managed_exit_policy_id": config.managed_exit_policy_id,
        "exit_strategy_id": config.exit_strategy_id,
        "exit_profile_id": config.exit_profile_id,
    }


def _lifecycle_report_path(*, config: TrackBManagedExitAttachConfig, live_position_status: Mapping[str, Any]) -> Path:
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


def _derive_close_limit_price(*, latest_price: str | None, side: str, tick_size: Decimal, offset_ticks: int) -> str | None:
    latest = _decimal(latest_price)
    if latest is None:
        return None
    offset = tick_size * Decimal(max(int(offset_ticks), 0))
    raw = latest - offset if side.upper() == "LONG" else latest + offset
    rounded = (raw / tick_size).to_integral_value(rounding=ROUND_HALF_UP) * tick_size
    return format(rounded.normalize(), "f")


def _payload_bars(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw = payload.get("bars") or payload.get("candles") or payload.get("completed_5m_candles") or []
    return [item for item in raw if isinstance(item, Mapping)] if isinstance(raw, list) else []


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


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
