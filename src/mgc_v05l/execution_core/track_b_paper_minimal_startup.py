"""Minimal Track B PAPER submit-capable startup contract.

This module is intentionally small and broker-risk focused. It does not grant
strategy authority, lifecycle authority, or live-money capability; it only
answers whether an explicitly selected Track B PAPER runtime has the current
facts needed to be submit-capable.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_broker_startup_authority import (
    classify_fresh_complete_clean_broker_truth,
)
from mgc_v05l.execution_core.track_b_current_state_authority import is_track_b_futures_position


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ACCOUNT_ID = "DUM882026"
DEFAULT_MAX_PAPER_ORDER_QTY = 1
DEFAULT_PRICE_MAX_AGE_SECONDS = 600.0

DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_minimal_startup"
    / "latest_paper_minimal_startup.json"
)
DEFAULT_BROKER_TRUTH_LEASE_PATH = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"
)
DEFAULT_BROKER_SESSION_AUTHORITY_PATH = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_broker_session_authority.json"
)
DEFAULT_OPEN_ORDER_TRUTH_PATH = (
    Path("outputs") / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
)
DEFAULT_RECONCILIATION_PATH = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_CONFIG_IN_FORCE_PATH = (
    Path("outputs")
    / "probationary_pattern_engine"
    / "paper_session"
    / "runtime"
    / "paper_config_in_force.json"
)
DEFAULT_CONFIG_PATHS_FILE = (
    Path("outputs")
    / "probationary_pattern_engine"
    / "paper_session"
    / "runtime"
    / "paper_runtime_config_paths.txt"
)
DEFAULT_PAPER_RUNTIME_TRUTH_PATH = (
    Path("outputs")
    / "probationary_pattern_engine"
    / "paper_session"
    / "runtime"
    / "paper_runtime_truth.json"
)
DEFAULT_PHASE1_MARKET_DATA_ROOT = (
    Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
)
DEFAULT_IBKR_POSITIONS_SNAPSHOT_PATH = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json"
)
DEFAULT_IBKR_OPEN_ORDERS_SNAPSHOT_PATH = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json"
)
DEFAULT_IBKR_BROKER_TRUTH_REFRESH_STATUS_PATH = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json"
)
DEFAULT_MANAGED_POSITION_REGISTRY_PATH = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
)
DEFAULT_APPROVED_PAPER_STACK_PROFILE_PATH = (
    Path("outputs") / "track_b_execution_core" / "runtime_recovery" / "approved_paper_stack_profile.json"
)


@dataclass(frozen=True)
class TrackBPaperMinimalStartupConfig:
    repo_root: Path = REPO_ROOT
    account_id: str = DEFAULT_ACCOUNT_ID
    max_paper_order_qty: int = DEFAULT_MAX_PAPER_ORDER_QTY
    price_max_age_seconds: float = DEFAULT_PRICE_MAX_AGE_SECONDS
    output_path: Path = DEFAULT_OUTPUT_PATH
    broker_truth_lease_path: Path = DEFAULT_BROKER_TRUTH_LEASE_PATH
    broker_session_authority_path: Path = DEFAULT_BROKER_SESSION_AUTHORITY_PATH
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_PATH
    reconciliation_path: Path = DEFAULT_RECONCILIATION_PATH
    config_in_force_path: Path = DEFAULT_CONFIG_IN_FORCE_PATH
    config_paths_file: Path = DEFAULT_CONFIG_PATHS_FILE
    paper_runtime_truth_path: Path = DEFAULT_PAPER_RUNTIME_TRUTH_PATH
    phase1_market_data_root: Path = DEFAULT_PHASE1_MARKET_DATA_ROOT
    ibkr_positions_snapshot_path: Path = DEFAULT_IBKR_POSITIONS_SNAPSHOT_PATH
    ibkr_open_orders_snapshot_path: Path = DEFAULT_IBKR_OPEN_ORDERS_SNAPSHOT_PATH
    ibkr_broker_truth_refresh_status_path: Path = DEFAULT_IBKR_BROKER_TRUTH_REFRESH_STATUS_PATH
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_PATH
    approved_paper_stack_profile_path: Path = DEFAULT_APPROVED_PAPER_STACK_PROFILE_PATH

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_paper_minimal_startup(
    *,
    config: TrackBPaperMinimalStartupConfig | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    config = config or TrackBPaperMinimalStartupConfig()
    actual_now = _ensure_utc(now or datetime.now(UTC))
    lease = _read_json(config.resolve(config.broker_truth_lease_path))
    bsa = _read_json(config.resolve(config.broker_session_authority_path))
    open_order_truth = _read_json(config.resolve(config.open_order_truth_path))
    reconciliation = _read_json(config.resolve(config.reconciliation_path))
    config_in_force = _read_json(config.resolve(config.config_in_force_path))
    runtime_truth = _read_json(config.resolve(config.paper_runtime_truth_path))
    config_paths = _read_text_lines(config.resolve(config.config_paths_file))
    ibkr_positions_snapshot = _read_json(config.resolve(config.ibkr_positions_snapshot_path))
    ibkr_open_orders_snapshot = _read_json(config.resolve(config.ibkr_open_orders_snapshot_path))
    ibkr_broker_truth_refresh_status = _read_json(config.resolve(config.ibkr_broker_truth_refresh_status_path))
    managed_positions = _read_json(config.resolve(config.managed_position_registry_path))
    approved_profile = _read_json(config.resolve(config.approved_paper_stack_profile_path))

    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    def block(code: str, detail: str, *, source: str) -> None:
        blockers.append({"code": code, "detail": detail, "source": source})

    def warn(code: str, detail: str, *, source: str) -> None:
        warnings.append({"code": code, "detail": detail, "source": source})

    broker_truth_authority = classify_track_b_paper_broker_truth_authority(
        config=config,
        lease=lease,
        bsa=bsa,
        open_order_truth=open_order_truth,
        reconciliation=reconciliation,
        config_in_force=config_in_force,
        runtime_truth=runtime_truth,
        ibkr_positions_snapshot=ibkr_positions_snapshot,
        ibkr_open_orders_snapshot=ibkr_open_orders_snapshot,
        ibkr_broker_truth_refresh_status=ibkr_broker_truth_refresh_status,
        managed_positions=managed_positions,
        now=actual_now,
    )
    blockers.extend(list(broker_truth_authority.get("blockers") or []))
    warnings.extend(list(broker_truth_authority.get("warnings") or []))
    account_id = str(broker_truth_authority.get("account_id") or "").strip()
    broker_positions_available = bool(broker_truth_authority.get("broker_positions_available"))
    broker_open_orders_available = bool(broker_truth_authority.get("broker_open_orders_available"))
    broker_position_count = _int_first(broker_truth_authority.get("broker_position_count"))
    broker_open_order_count = _int_first(broker_truth_authority.get("broker_open_order_count"))
    unknown_open_orders = _int_first(broker_truth_authority.get("unknown_open_order_count"))

    explicit_profile = _explicit_paper_profile(
        config_paths,
        approved_profile=approved_profile,
        runtime_dir=config.resolve(config.config_paths_file).parent,
        repo_root=config.repo_root,
    )
    if not explicit_profile:
        block("explicit_paper_profile_missing", "No explicit paper_stack_* profile overlay is selected.", source="config")

    active_lanes = _active_lanes(config_in_force)
    if not active_lanes:
        block("active_paper_lanes_missing", "No active PAPER lanes are configured.", source="config")
    bad_accounts = sorted(_lane_id(row) for row in active_lanes if _lane_account(row) not in {"", config.account_id})
    if bad_accounts:
        block("lane_account_not_allowed", f"Active lanes have non-PAPER account ids: {bad_accounts[:5]}.", source="config")
    not_paper_lanes = sorted(_lane_id(row) for row in active_lanes if row.get("paper_only") is not True)
    if not_paper_lanes:
        block("submit_route_not_paper_only", f"Active lanes are not paper_only: {not_paper_lanes[:5]}.", source="config")
    runtime_mode = str(runtime_truth.get("runtime_mode") or config_in_force.get("runtime_mode") or "PAPER").strip().upper()
    if runtime_mode != "PAPER":
        block("runtime_route_not_paper", f"Runtime mode is {runtime_mode or 'UNKNOWN'}, expected PAPER.", source="runtime")

    oversize_lanes = [
        {"lane_id": _lane_id(row), "max_position_quantity": _int_first(row.get("max_position_quantity"))}
        for row in active_lanes
        if _int_first(row.get("max_position_quantity")) > int(config.max_paper_order_qty)
    ]
    if oversize_lanes:
        block("paper_order_size_limit_exceeded", f"Active lane max quantity exceeds {config.max_paper_order_qty}.", source="config")

    instruments = _active_instruments(active_lanes)
    price_rows = _price_availability_rows(
        config=config,
        instruments=instruments,
        now=actual_now,
    )
    missing_price = [row for row in price_rows if row.get("available") is not True]
    if not instruments:
        block("configured_traded_contracts_missing", "No configured traded contracts could be inferred.", source="config")
    elif missing_price:
        warn(
            "current_market_price_unavailable_startup_diagnostic",
            "Current price/candle unavailable during startup; broker submit remains blocked until market truth is fresh "
            f"for {[row.get('instrument') for row in missing_price]}.",
            source="market_data",
        )

    _diagnostic_if_present(warnings, "stale_dashboard_backend_artifacts_diagnostic", "Dashboard/backend freshness is diagnostic under PAPER_MINIMAL_STARTUP_V1.", "diagnostics")
    _diagnostic_if_present(warnings, "stale_pid_runtime_truth_diagnostic", "Stale PID/runtime-truth artifacts are diagnostic unless they affect PAPER/account/broker/order/price/config/size invariants.", "diagnostics")
    _diagnostic_if_present(warnings, "control_plane_publication_diagnostic", "Control-plane/shared-services/ODS publication freshness is diagnostic unless it proves a hard PAPER invariant false.", "diagnostics")
    if reconciliation:
        recon_age = _age_seconds(reconciliation.get("generated_at"), actual_now)
        if recon_age is not None and recon_age > 180.0 and broker_positions_available and broker_open_orders_available:
            warn(
                "reconciliation_freshness_lag_diagnostic",
                "Reconciliation freshness lag is diagnostic because broker position/order truth is current and complete.",
                source="reconciliation",
            )

    classification = "PAPER_MINIMAL_STARTUP_ALLOWED" if not blockers else "PAPER_MINIMAL_STARTUP_BLOCKED"
    return {
        "schema_version": "track_b_paper_minimal_startup_v1",
        "generated_at": actual_now.isoformat(),
        "classification": classification,
        "allowed": not blockers,
        "account_id": account_id or None,
        "execution_domain": "TRACK_B_PAPER",
        "submit_route": "PAPER",
        "profile_overlay": explicit_profile,
        "configured_instruments": instruments,
        "max_paper_order_qty": int(config.max_paper_order_qty),
        "broker_positions_available": broker_positions_available,
        "broker_open_orders_available": broker_open_orders_available,
        "broker_position_count": broker_position_count,
        "broker_open_order_count": broker_open_order_count,
        "unknown_open_order_count": unknown_open_orders,
        "price_availability": price_rows,
        "blockers": blockers,
        "warnings": warnings,
        "diagnostics_only_categories": [
            "stale_dashboard_backend_artifacts",
            "stale_pid_runtime_truth_artifacts",
            "stale_ready_observation_publication",
            "stale_or_incoherent_control_plane_publication",
            "stale_shared_services_authority",
            "guarded_loop_status_without_hard_invariant_failure",
            "ods_publication_delay",
            "historical_registry_lifecycle_debris_contradicted_by_flat_broker_truth",
            "reconciliation_freshness_lag_with_current_flat_broker_truth",
        ],
        "source_artifact_refs": {
            "broker_truth_lease": str(config.resolve(config.broker_truth_lease_path)),
            "broker_session_authority": str(config.resolve(config.broker_session_authority_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "config_in_force": str(config.resolve(config.config_in_force_path)),
            "config_paths_file": str(config.resolve(config.config_paths_file)),
            "paper_runtime_truth": str(config.resolve(config.paper_runtime_truth_path)),
            "phase1_market_data_root": str(config.resolve(config.phase1_market_data_root)),
            "ibkr_positions_snapshot": str(config.resolve(config.ibkr_positions_snapshot_path)),
            "ibkr_open_orders_snapshot": str(config.resolve(config.ibkr_open_orders_snapshot_path)),
            "ibkr_broker_truth_refresh_status": str(config.resolve(config.ibkr_broker_truth_refresh_status_path)),
            "managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
            "approved_paper_stack_profile": str(config.resolve(config.approved_paper_stack_profile_path)),
        },
}


def classify_track_b_paper_broker_truth_authority(
    *,
    config: TrackBPaperMinimalStartupConfig,
    lease: Mapping[str, Any],
    bsa: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    config_in_force: Mapping[str, Any],
    runtime_truth: Mapping[str, Any],
    ibkr_positions_snapshot: Mapping[str, Any],
    ibkr_open_orders_snapshot: Mapping[str, Any],
    ibkr_broker_truth_refresh_status: Mapping[str, Any],
    managed_positions: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    """Classify PAPER startup/new-entry authority from broker truth first."""

    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    def block(code: str, detail: str, *, source: str) -> None:
        blockers.append({"code": code, "detail": detail, "source": source})

    def warn(code: str, detail: str, *, source: str) -> None:
        warnings.append({"code": code, "detail": detail, "source": source})

    account_id = str(
        lease.get("account_id")
        or bsa.get("account_id")
        or _config_account_id(config_in_force)
        or ""
    ).strip()
    if account_id != config.account_id:
        block("paper_account_not_allowed", f"Expected {config.account_id}, got {account_id or 'UNKNOWN'}.", source="account")

    if _any_true(lease, bsa, reconciliation, config_in_force, runtime_truth, key="live_money_eligible"):
        block("live_money_eligible_true", "live_money_eligible=true is forbidden for PAPER minimal startup.", source="paper_safety")
    if _any_true(lease, bsa, reconciliation, open_order_truth, runtime_truth, key="paper_proof_invoked"):
        block("paper_proof_invoked_true", "paper_proof=true/invoked is forbidden for PAPER minimal startup.", source="paper_safety")

    position_lease = _mapping(lease.get("broker_position_lease"))
    open_order_lease = _mapping(lease.get("broker_open_order_lease"))
    broker_positions_available = _lease_complete_and_not_stale(
        lease,
        lease_key="broker_position_lease",
        complete_key="broker_positions_complete",
        stale_block_code="broker_positions_stale",
        block=block,
    )
    broker_open_orders_available = _lease_complete_and_not_stale(
        lease,
        lease_key="broker_open_order_lease",
        complete_key="broker_open_orders_complete",
        stale_block_code="broker_open_orders_stale",
        block=block,
    )
    if not broker_positions_available:
        block("broker_positions_unavailable", "Broker positions are not available/complete.", source="broker_truth")
    if not broker_open_orders_available:
        block("broker_open_orders_unavailable", "Broker open orders are not available/complete.", source="broker_truth")

    fresh_read_only_truth = _fresh_ibkr_read_only_broker_truth(
        positions_snapshot=ibkr_positions_snapshot,
        open_orders_snapshot=ibkr_open_orders_snapshot,
        refresh_status=ibkr_broker_truth_refresh_status,
        account_id=config.account_id,
        now=now,
    )
    broker_position_count = _int_first(
        lease.get("track_b_broker_position_count"),
        position_lease.get("position_count"),
        position_lease.get("count"),
    )
    broker_open_order_count = max(
        _int_first(lease.get("track_b_broker_open_order_count"), open_order_lease.get("open_order_count"), open_order_lease.get("count")),
        _int_first(open_order_truth.get("open_order_count"), _mapping(open_order_truth.get("summary")).get("open_order_count")),
    )
    unknown_open_orders = max(
        _int_first(lease.get("unknown_broker_open_order_count")),
        _int_first(open_order_truth.get("unknown_open_order_count")),
        _int_first(_mapping(open_order_truth.get("summary")).get("unknown_open_order_count")),
    )
    if fresh_read_only_truth.get("available") is True:
        read_only_position_count = _int_first(fresh_read_only_truth.get("track_b_broker_position_count"))
        read_only_open_order_count = _int_first(fresh_read_only_truth.get("track_b_broker_open_order_count"))
        read_only_unknown_open_orders = _int_first(fresh_read_only_truth.get("unknown_open_order_count"))
        if broker_position_count != read_only_position_count:
            warn(
                "broker_position_lease_count_diagnostic",
                "Fresh IBKR read-only position truth outranks broker-truth lease aggregate position count "
                f"({broker_position_count} -> {read_only_position_count}).",
                source="broker_truth",
            )
        if broker_open_order_count != read_only_open_order_count:
            warn(
                "broker_open_order_lease_count_diagnostic",
                "Fresh IBKR read-only open-order truth outranks broker-truth lease aggregate open-order count "
                f"({broker_open_order_count} -> {read_only_open_order_count}).",
                source="broker_truth",
            )
        if unknown_open_orders != read_only_unknown_open_orders:
            warn(
                "unknown_open_order_lease_count_diagnostic",
                "Fresh IBKR read-only order truth outranks broker-truth lease/open-order aggregate unknown-order count "
                f"({unknown_open_orders} -> {read_only_unknown_open_orders}).",
                source="broker_truth",
            )
        broker_positions_available = True
        broker_open_orders_available = True
        broker_position_count = read_only_position_count
        broker_open_order_count = read_only_open_order_count
        unknown_open_orders = read_only_unknown_open_orders
        unrelated_open_order_count = _int_first(fresh_read_only_truth.get("unrelated_open_order_count"))
        if unrelated_open_order_count:
            warn(
                "unrelated_non_track_b_open_orders_diagnostic",
                f"Fresh IBKR read-only truth has {unrelated_open_order_count} non-Track-B/non-futures open order(s); "
                "diagnostic unless they become unknown or tied to Track B order-control scope.",
                source="broker_truth",
            )
    if broker_positions_available and broker_position_count != 0:
        startup_authority = classify_fresh_complete_clean_broker_truth(
            broker_truth_status={
                **dict(ibkr_broker_truth_refresh_status),
                "fresh": fresh_read_only_truth.get("available") is True,
                "positions_complete": fresh_read_only_truth.get("available") is True,
                "open_orders_complete": fresh_read_only_truth.get("available") is True,
                "open_order_count": broker_open_order_count,
                "unknown_open_order_count": unknown_open_orders,
                "live_money_eligible": _any_true(lease, bsa, reconciliation, config_in_force, runtime_truth, key="live_money_eligible"),
                "paper_proof_invoked": _any_true(lease, bsa, reconciliation, open_order_truth, runtime_truth, key="paper_proof_invoked"),
            },
            positions_snapshot=ibkr_positions_snapshot,
            open_orders_snapshot=ibkr_open_orders_snapshot,
            reconciliation=reconciliation,
            open_order_truth=open_order_truth,
            safety={"live_money_eligible": False, "paper_proof_invoked": False},
            managed_positions=managed_positions,
            allow_known_managed_positions=True,
            expected_account_id=config.account_id,
        )
        if startup_authority.broker_truth_clean is True:
            warn(
                "broker_positions_known_managed_startup_diagnostic",
                f"Broker has {broker_position_count} Track B PAPER position(s), all known/current managed exposure.",
                source="broker_truth",
            )
        else:
            detail = ", ".join(startup_authority.blockers) or f"Broker position count is {broker_position_count}."
            block("broker_positions_present", detail, source="broker_truth")

    if unknown_open_orders != 0:
        block("unknown_open_orders_present", f"Unknown open order count is {unknown_open_orders}.", source="broker_truth")

    if broker_open_orders_available and broker_open_order_count != 0:
        block("broker_open_orders_present", f"Broker open order count is {broker_open_order_count}.", source="broker_truth")

    lease_state = str(lease.get("lease_state") or "").strip().upper()
    if lease_state and lease_state != "ACTIVE" and broker_positions_available and broker_open_orders_available:
        warn(
            "broker_truth_aggregate_state_diagnostic",
            f"Aggregate broker truth lease_state is {lease_state}; current position/open-order evidence leases remain authoritative.",
            source="broker_truth",
        )

    open_order_classification = str(open_order_truth.get("classification") or "").strip().upper()
    if open_order_classification and open_order_classification != "NO_OPEN_ORDERS":
        if broker_open_order_count or unknown_open_orders or not broker_open_orders_available:
            block("open_order_truth_not_clean", f"Open Order Truth is {open_order_classification}.", source="open_order_truth")
        else:
            warn(
                "open_order_truth_stale_diagnostic",
                f"Open Order Truth is {open_order_classification}, but current broker open-order truth is zero/known.",
                source="open_order_truth",
            )

    if reconciliation:
        if reconciliation.get("broker_reconciled") is False:
            warn(
                "broker_reconciliation_dirty_diagnostic",
                "Legacy reconciliation reports broker_reconciled=false; broker truth is startup authority when current and safe.",
                source="reconciliation",
            )
        review_count = _int_first(
            reconciliation.get("current_scope_review_required_count"),
            reconciliation.get("review_required_count"),
        )
        if review_count != 0:
            warn(
                "review_required_current_scope_diagnostic",
                f"Legacy reconciliation review_required count is {review_count}; diagnostic unless broker truth shows current exposure/order risk.",
                source="reconciliation",
            )
        recon_position_count = _int_first(reconciliation.get("track_b_broker_position_count"))
        if recon_position_count != 0 and broker_position_count == 0:
            warn(
                "reconciliation_position_count_mismatch_diagnostic",
                f"Legacy reconciliation reports broker position count {recon_position_count}, but broker truth reports flat.",
                source="reconciliation",
            )
        recon_open_order_count = _int_first(reconciliation.get("track_b_broker_open_order_count"))
        if recon_open_order_count != 0 and broker_open_order_count == 0:
            warn(
                "reconciliation_open_order_count_mismatch_diagnostic",
                f"Legacy reconciliation reports open order count {recon_open_order_count}, but broker truth reports zero.",
                source="reconciliation",
            )

    bsa_classification = str(bsa.get("classification") or "").strip().upper()
    if bsa_classification and "ORDER_STATUS_UNRELIABLE" in bsa_classification and broker_open_orders_available:
        warn(
            "broker_session_authority_order_status_unreliable_diagnostic",
            "BSA order-status reliability is diagnostic when broker open-order truth is current and complete.",
            source="broker_session_authority",
        )

    return {
        "classification": "PAPER_BROKER_TRUTH_AUTHORITY_ALLOWED" if not blockers else "PAPER_BROKER_TRUTH_AUTHORITY_BLOCKED",
        "allowed": not blockers,
        "account_id": account_id or None,
        "broker_positions_available": broker_positions_available,
        "broker_open_orders_available": broker_open_orders_available,
        "broker_position_count": broker_position_count,
        "broker_open_order_count": broker_open_order_count,
        "unknown_open_order_count": unknown_open_orders,
        "blockers": blockers,
        "warnings": warnings,
    }


def write_track_b_paper_minimal_startup(
    *,
    config: TrackBPaperMinimalStartupConfig | None = None,
    payload: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> Path:
    config = config or TrackBPaperMinimalStartupConfig()
    payload = dict(payload or build_track_b_paper_minimal_startup(config=config, now=now))
    path = config.resolve(config.output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _active_lanes(config_in_force: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    active_ids = {str(value).strip() for value in list(config_in_force.get("active_lane_ids") or []) if str(value).strip()}
    rows = [row for row in list(config_in_force.get("lanes") or []) if isinstance(row, Mapping)]
    if not active_ids:
        return rows
    return [row for row in rows if _lane_id(row) in active_ids]


def _active_instruments(active_lanes: Sequence[Mapping[str, Any]]) -> list[str]:
    values: set[str] = set()
    for row in active_lanes:
        raw = row.get("instrument") or row.get("symbol") or row.get("contract_symbol") or ""
        text = str(raw).strip().upper()
        if not text:
            lane_id = _lane_id(row).upper()
            text = lane_id.split("_", 1)[0] if "_" in lane_id else ""
        if text:
            values.add(text)
    return sorted(values)


def _price_availability_rows(
    *,
    config: TrackBPaperMinimalStartupConfig,
    instruments: Sequence[str],
    now: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for instrument in instruments:
        path = config.resolve(config.phase1_market_data_root) / instrument / "1m" / "latest_runtime_candles.json"
        payload = _read_json(path)
        generated_at = _parse_datetime(payload.get("generated_at"))
        age_seconds = None if generated_at is None else max((now - generated_at).total_seconds(), 0.0)
        bars = list(payload.get("bars") or payload.get("candles") or [])
        latest_bar = bars[-1] if bars and isinstance(bars[-1], Mapping) else {}
        price = _float_first(latest_bar.get("close"), payload.get("latest_price"), payload.get("price"))
        blockers: list[str] = []
        if not payload:
            blockers.append("price_artifact_missing")
        if not bars:
            blockers.append("price_candles_missing")
        if price is None:
            blockers.append("price_missing")
        if generated_at is None:
            blockers.append("price_generated_at_missing")
        elif age_seconds is not None and age_seconds > float(config.price_max_age_seconds):
            blockers.append("price_artifact_stale")
        if payload and payload.get("live_money_eligible") is True:
            blockers.append("price_artifact_live_money_eligible_true")
        source_category = str(payload.get("source_category") or payload.get("source") or "").upper()
        if any(marker in source_category for marker in ("RESEARCH", "REPLAY", "OFFLINE")):
            blockers.append("price_artifact_not_runtime_source")
        rows.append(
            {
                "instrument": instrument,
                "available": not blockers,
                "artifact_path": str(path),
                "generated_at": payload.get("generated_at"),
                "age_seconds": None if age_seconds is None else round(age_seconds, 3),
                "latest_bar_end_ts": latest_bar.get("bar_end") or payload.get("last_completed_bar_ts"),
                "price": price,
                "blockers": blockers,
            }
        )
    return rows


def _fresh_ibkr_read_only_broker_truth(
    *,
    positions_snapshot: Mapping[str, Any],
    open_orders_snapshot: Mapping[str, Any],
    refresh_status: Mapping[str, Any],
    account_id: str,
    now: datetime,
) -> dict[str, Any]:
    if not positions_snapshot or not open_orders_snapshot:
        return {"available": False, "reason": "read_only_snapshot_missing"}
    positions_account = str(
        positions_snapshot.get("selected_account_id")
        or positions_snapshot.get("account")
        or positions_snapshot.get("account_id")
        or ""
    ).strip()
    orders_account = str(
        open_orders_snapshot.get("selected_account_id")
        or open_orders_snapshot.get("account")
        or open_orders_snapshot.get("account_id")
        or ""
    ).strip()
    if positions_account != account_id or orders_account != account_id:
        return {"available": False, "reason": "read_only_snapshot_wrong_account"}
    if positions_snapshot.get("ok") is not True or positions_snapshot.get("positions_complete") is not True:
        return {"available": False, "reason": "read_only_positions_incomplete"}
    if open_orders_snapshot.get("ok") is not True or open_orders_snapshot.get("open_orders_complete") is not True:
        return {"available": False, "reason": "read_only_open_orders_incomplete"}
    positions_age = _age_seconds(positions_snapshot.get("generated_at"), now)
    orders_age = _age_seconds(open_orders_snapshot.get("generated_at"), now)
    if positions_age is None or orders_age is None:
        return {"available": False, "reason": "read_only_snapshot_timestamp_missing"}
    max_age_seconds = 900.0
    if positions_age > max_age_seconds or orders_age > max_age_seconds:
        return {
            "available": False,
            "reason": "read_only_snapshot_stale",
            "positions_age_seconds": round(positions_age, 3),
            "open_orders_age_seconds": round(orders_age, 3),
        }
    track_b_positions = []
    for row in list(positions_snapshot.get("positions") or []):
        if not isinstance(row, Mapping):
            continue
        if not _is_track_b_futures_position_row(row):
            continue
        quantity = _float_first(row.get("quantity"), row.get("position"), row.get("position_quantity")) or 0.0
        if abs(quantity) > 0:
            track_b_positions.append(dict(row))
    open_orders = [dict(row) for row in list(open_orders_snapshot.get("open_orders") or []) if isinstance(row, Mapping)]
    track_b_open_orders = [row for row in open_orders if _is_track_b_futures_position_row(row)]
    unrelated_open_orders = [row for row in open_orders if not _is_track_b_futures_position_row(row)]
    open_order_count = _int_first(open_orders_snapshot.get("open_order_count"), len(open_orders))
    unknown_open_orders = 0
    for payload in (refresh_status, open_orders_snapshot):
        for key in ("unknown_open_order_count", "unknown_order_count", "unknown_broker_open_order_count", "suspicious_order_count"):
            unknown_open_orders = max(unknown_open_orders, _int_first(payload.get(key)))
    return {
        "available": True,
        "source": "ibkr_read_only_verification",
        "track_b_broker_position_count": len(track_b_positions),
        "broker_open_order_count": max(open_order_count, len(open_orders)),
        "track_b_broker_open_order_count": len(track_b_open_orders),
        "unrelated_open_order_count": len(unrelated_open_orders),
        "unknown_open_order_count": unknown_open_orders,
        "track_b_futures_positions": track_b_positions,
        "track_b_open_orders": track_b_open_orders,
        "open_orders": open_orders,
        "positions_generated_at": positions_snapshot.get("generated_at"),
        "open_orders_generated_at": open_orders_snapshot.get("generated_at"),
        "positions_age_seconds": round(positions_age, 3),
        "open_orders_age_seconds": round(orders_age, 3),
    }


def _is_track_b_futures_position_row(row: Mapping[str, Any]) -> bool:
    return is_track_b_futures_position(row)


def _explicit_paper_profile(
    config_paths: Sequence[str],
    *,
    approved_profile: Mapping[str, Any] | None = None,
    runtime_dir: Path | None = None,
    repo_root: Path | None = None,
) -> str | None:
    for raw_path in reversed(config_paths):
        name = Path(str(raw_path)).name
        if name.startswith("paper_stack_") and name.endswith(".yaml"):
            return str(raw_path)
    profile = _approved_paper_stack_profile_name(approved_profile or {})
    if not profile or runtime_dir is None:
        return None
    candidate = runtime_dir / f"paper_stack_{profile}.yaml"
    if candidate.exists():
        return str(candidate)
    if repo_root is not None:
        repo_candidate = repo_root / candidate
        if repo_candidate.exists():
            return str(repo_candidate)
    return None


def _approved_paper_stack_profile_name(payload: Mapping[str, Any]) -> str | None:
    if payload.get("paper_only") is not True:
        return None
    if payload.get("live_money_eligible") is True or payload.get("paper_proof_invoked") is True:
        return None
    if payload.get("recovery_profile_approved") is False:
        return None
    profile = str(payload.get("approved_profile") or payload.get("recovery_requested_profile") or "").strip()
    return profile or None


def _config_account_id(config_in_force: Mapping[str, Any]) -> str | None:
    for row in _active_lanes(config_in_force):
        account = _lane_account(row)
        if account:
            return account
    return None


def _lane_account(row: Mapping[str, Any]) -> str:
    overlay = _mapping(row.get("runtime_overlay_params"))
    return str(
        overlay.get("expected_account_id")
        or row.get("expected_account_id")
        or row.get("paper_account_id")
        or row.get("account_id")
        or ""
    ).strip()


def _lane_id(row: Mapping[str, Any]) -> str:
    return str(row.get("lane_id") or row.get("strategy_id") or row.get("id") or "").strip()


def _diagnostic_if_present(rows: list[dict[str, Any]], code: str, detail: str, source: str) -> None:
    rows.append({"code": code, "detail": detail, "source": source})


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_text_lines(path: Path) -> list[str]:
    try:
        return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except OSError:
        return []


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _lease_complete_and_not_stale(
    payload: Mapping[str, Any],
    *,
    lease_key: str,
    complete_key: str,
    stale_block_code: str,
    block: Any,
) -> bool:
    lease = _mapping(payload.get(lease_key))
    complete = bool(lease.get("complete") is True or payload.get(complete_key) is True)
    if not complete:
        return False
    if lease.get("fresh") is False or payload.get(f"{complete_key}_fresh") is False:
        block(stale_block_code, f"{lease_key} is explicitly stale.", source="broker_truth")
        return False
    return True


def _any_true(*payloads: Mapping[str, Any], key: str) -> bool:
    return any(payload.get(key) is True for payload in payloads if isinstance(payload, Mapping))


def _int_first(*values: Any) -> int:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(float(str(value)))
            except (TypeError, ValueError):
                continue
    return 0


def _float_first(*values: Any) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _age_seconds(value: Any, now: datetime) -> float | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return max((now - parsed).total_seconds(), 0.0)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Track B PAPER minimal startup classification.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--account-id", default=DEFAULT_ACCOUNT_ID)
    parser.add_argument("--max-paper-order-qty", type=int, default=DEFAULT_MAX_PAPER_ORDER_QTY)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    config = TrackBPaperMinimalStartupConfig(
        repo_root=args.repo_root,
        account_id=args.account_id,
        max_paper_order_qty=args.max_paper_order_qty,
    )
    payload = build_track_b_paper_minimal_startup(config=config)
    if not args.no_write:
        write_track_b_paper_minimal_startup(config=config, payload=payload)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload.get("allowed") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
