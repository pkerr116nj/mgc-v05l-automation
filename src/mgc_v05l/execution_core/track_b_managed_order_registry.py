"""Read-only Track B PAPER managed-order / order-intent authority.

Managed Order Registry authority lives in execution_core. Dashboard artifacts
are projections and must not be used as runtime, readiness, or routing
authority. This v1 service classifies order state only; it never submits,
modifies, cancels, replaces, or closes broker orders.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.bounded_jsonl import append_bounded_jsonl
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_live_trade_registry import load_live_trade_registry_records
from mgc_v05l.execution_core.track_b_pre_restart_exposure_reconciliation import (
    PreRestartExposureResolverConfig,
    resolve_pre_restart_exposure_reconciliation,
)
from mgc_v05l.execution_core.track_b_projection_metadata import build_projection_metadata
from mgc_v05l.execution_core.track_b_terminal_registry_truth import resolve_terminal_registry_truth


NO_MANAGED_ORDERS = "NO_MANAGED_ORDERS"
WORKING_ENTRY_ORDER = "WORKING_ENTRY_ORDER"
WORKING_CLOSE_ORDER = "WORKING_CLOSE_ORDER"
CLOSE_ORDER_MODIFIABLE = "CLOSE_ORDER_MODIFIABLE"
CLOSE_ORDER_CANCEL_REPLACE_REQUIRED = "CLOSE_ORDER_CANCEL_REPLACE_REQUIRED"
CLOSE_ORDER_NOT_MARKETABLE = "CLOSE_ORDER_NOT_MARKETABLE"
CLOSE_ORDER_SUSPICIOUS = "CLOSE_ORDER_SUSPICIOUS"
DUPLICATE_CLOSE_ORDER_BLOCKED = "DUPLICATE_CLOSE_ORDER_BLOCKED"
ORDER_TERMINAL_FILLED = "ORDER_TERMINAL_FILLED"
ORDER_TERMINAL_CANCELLED = "ORDER_TERMINAL_CANCELLED"
ORDER_STATE_UNKNOWN_REVIEW_REQUIRED = "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED"
BROKER_FLAT_WITH_WORKING_CLOSE = "BROKER_FLAT_WITH_WORKING_CLOSE"
POSITION_WITHOUT_CLOSE_ORDER = "POSITION_WITHOUT_CLOSE_ORDER"
ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING = "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING"

WAIT = "WAIT"
MODIFY_IN_PLACE_CANDIDATE = "MODIFY_IN_PLACE_CANDIDATE"
TARGETED_CANCEL_REPLACE_CANDIDATE = "TARGETED_CANCEL_REPLACE_CANDIDATE"
DO_NOT_REPLACE_DUPLICATE_RISK = "DO_NOT_REPLACE_DUPLICATE_RISK"
REVIEW_REQUIRED = "REVIEW_REQUIRED"

DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json"
)
DEFAULT_MANAGED_ORDER_EVENTS = (
    Path("outputs") / "track_b_execution_core" / "managed_orders" / "managed_order_events.jsonl"
)
DEFAULT_DASHBOARD_MANAGED_ORDER_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_managed_orders.json"
)
DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
)
DEFAULT_POSITION_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
)
DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
)
DEFAULT_RECONCILIATION_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_LIFECYCLE_ROOT = (
    Path("outputs") / "track_b_execution_core" / "track_b_strategy_managed_paper_lifecycle"
)
DEFAULT_MANIFEST_ROOT = Path("outputs") / "track_b_execution_core" / "position_management_manifests"
DEFAULT_SUBMIT_OWNERSHIP_JSONL = (
    Path("outputs") / "track_b_execution_core" / "submit_intent_ownership" / "track_b_submit_intent_ownership.jsonl"
)

_TERMINAL_FILLED_STATUSES = {"FILLED"}
_TERMINAL_CANCELLED_STATUSES = {"CANCELLED", "APICANCELLED", "INACTIVE"}


@dataclass(frozen=True)
class TrackBManagedOrderRegistryConfig:
    repo_root: Path
    output_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    event_log_path: Path = DEFAULT_MANAGED_ORDER_EVENTS
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_MANAGED_ORDER_PROJECTION
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    lifecycle_root: Path = DEFAULT_LIFECYCLE_ROOT
    manifest_root: Path = DEFAULT_MANIFEST_ROOT
    submit_ownership_jsonl_path: Path = DEFAULT_SUBMIT_OWNERSHIP_JSONL
    artifact_max_age_seconds: float = 180.0

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_managed_order_registry(
    *,
    config: TrackBManagedOrderRegistryConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    open_order_truth = _read_json(config.resolve(config.open_order_truth_path))
    position_truth = _read_json(config.resolve(config.position_truth_path))
    managed_positions = _read_json(config.resolve(config.managed_position_registry_path))
    reconciliation = _read_json(config.resolve(config.reconciliation_path))
    lifecycle_reports = _load_lifecycle_reports(config.resolve(config.lifecycle_root))
    terminal_records = load_live_trade_registry_records(repo_root=config.repo_root)
    manifests = _load_manifests(config.resolve(config.manifest_root))
    ownership_records = _load_submit_ownership_records(config.resolve(config.submit_ownership_jsonl_path))
    pre_restart_exposure_resolution = resolve_pre_restart_exposure_reconciliation(
        config=PreRestartExposureResolverConfig(repo_root=config.repo_root),
        broker_positions=_list(open_order_truth.get("broker_positions_without_close_order"))
        or _reconciliation_broker_positions(reconciliation),
        lifecycle_positions=_reconciliation_lifecycle_positions(reconciliation),
        broker_open_orders=_reconciliation_broker_open_orders(reconciliation),
        lifecycle_reports=lifecycle_reports,
    )
    owner_resolution = _mapping(reconciliation.get("current_exposure_owner_resolution")) or _mapping(
        pre_restart_exposure_resolution.get("current_exposure_owner_resolution")
    ) or pre_restart_exposure_resolution

    source_stale = _source_stale(
        now=actual_now,
        config=config,
        open_order_truth=open_order_truth,
        position_truth=position_truth,
        managed_positions=managed_positions,
        reconciliation=reconciliation,
    )
    raw_order_states = _list(open_order_truth.get("order_states"))
    order_states, current_truth_invalidated_orders = _active_order_states_from_current_truth(
        open_order_truth=open_order_truth,
        order_states=raw_order_states,
    )
    duplicate_order_ids = _duplicate_order_ids(_list(open_order_truth.get("duplicate_close_order_groups")))
    flat_close_order_ids = {
        _order_identity(state.get("order") or state)
        for state in _list(open_order_truth.get("broker_flat_with_open_close_order"))
    }
    managed_orders = [
        _managed_order_from_open_order_state(
            state=state,
            duplicate_order_ids=duplicate_order_ids,
            flat_close_order_ids=flat_close_order_ids,
            lifecycle_reports=lifecycle_reports,
            terminal_records=terminal_records,
            broker_positions=_list(reconciliation.get("track_b_broker_positions")),
            broker_open_orders=_list(reconciliation.get("track_b_broker_open_orders")),
            manifests=manifests,
            ownership_records=ownership_records,
        )
        for state in order_states
    ]
    canonical_managed_positions = _managed_positions_with_owner_overlay(
        managed_positions=managed_positions,
        resolver_payload=owner_resolution,
    )
    position_without_close_rows, terminal_superseded_rows = _position_without_close_rows(
        open_order_truth=open_order_truth,
        managed_positions=canonical_managed_positions,
        reconciliation=reconciliation,
        resolver_payload=pre_restart_exposure_resolution,
        lifecycle_reports=lifecycle_reports,
        terminal_records=terminal_records,
        manifests=manifests,
    )
    managed_orders.extend(position_without_close_rows)
    classification = _overall_classification(source_stale=source_stale, managed_orders=managed_orders)
    payload = {
        "schema_version": "track_b_managed_order_registry_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": reconciliation.get("live_money_eligible") is True,
        "classification": classification,
        "managed_orders": managed_orders,
        "terminal_registry_truth_overlay": {
            "enabled": True,
            "record_count": len(terminal_records),
            "superseded_full_audit_only_count": len(terminal_superseded_rows),
            "superseded_full_audit_only": terminal_superseded_rows,
            "source": "track_b_live_trade_registry",
        },
        "current_truth_invalidation": {
            "enabled": True,
            "invalidated_order_count": len(current_truth_invalidated_orders),
            "invalidated_orders": current_truth_invalidated_orders,
            "source": "latest_open_order_truth",
        },
        "pre_restart_exposure_resolution": pre_restart_exposure_resolution,
        "source_freshness": source_stale,
        "open_order_truth": _authority_summary(open_order_truth, config.resolve(config.open_order_truth_path)),
        "position_truth": _authority_summary(position_truth, config.resolve(config.position_truth_path)),
        "managed_position_registry": _authority_summary(
            managed_positions,
            config.resolve(config.managed_position_registry_path),
        ),
        "reconciliation": {
            "classification": reconciliation.get("classification"),
            "broker_reconciled": reconciliation.get("broker_reconciled"),
            "track_b_broker_open_order_count": reconciliation.get("track_b_broker_open_order_count"),
            "track_b_broker_position_count": reconciliation.get("track_b_broker_position_count"),
            "unresolved_submit_intent_ownership_count": reconciliation.get(
                "unresolved_submit_intent_ownership_count"
            ),
            "generated_at": reconciliation.get("generated_at"),
            "artifact_path": str(config.resolve(config.reconciliation_path)),
        },
        "summary": {
            "classification": classification,
            "managed_order_count": len(managed_orders),
            "working_entry_order_count": sum(1 for item in managed_orders if item.get("classification") == WORKING_ENTRY_ORDER),
            "working_close_order_count": sum(1 for item in managed_orders if item.get("classification") == WORKING_CLOSE_ORDER),
            "modifiable_close_order_count": sum(1 for item in managed_orders if item.get("classification") == CLOSE_ORDER_MODIFIABLE),
            "cancel_replace_candidate_count": sum(
                1 for item in managed_orders if item.get("recommended_next_action") == TARGETED_CANCEL_REPLACE_CANDIDATE
            ),
            "suspicious_order_count": sum(1 for item in managed_orders if item.get("classification") == CLOSE_ORDER_SUSPICIOUS),
            "duplicate_close_order_count": sum(
                1 for item in managed_orders if item.get("classification") == DUPLICATE_CLOSE_ORDER_BLOCKED
            ),
            "position_without_close_order_count": sum(
                1 for item in managed_orders if item.get("classification") == POSITION_WITHOUT_CLOSE_ORDER
            ),
            "active_hold_managed_timed_exit_pending_count": sum(
                1
                for item in managed_orders
                if item.get("classification") == ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING
            ),
        },
        "event_state": _event_state(classification=classification, managed_orders=managed_orders),
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "event_log": str(config.resolve(config.event_log_path)),
            "dashboard_projection": None
            if config.dashboard_projection_path is None
            else str(config.resolve(config.dashboard_projection_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "position_truth": str(config.resolve(config.position_truth_path)),
            "managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "lifecycle_root": str(config.resolve(config.lifecycle_root)),
            "manifest_root": str(config.resolve(config.manifest_root)),
            "submit_ownership_jsonl": str(config.resolve(config.submit_ownership_jsonl_path)),
        },
    }
    payload["dmc_metadata"] = _build_dmc_metadata(config=config, payload=payload, generated_at=actual_now)
    return payload


def write_track_b_managed_order_registry(
    *,
    config: TrackBManagedOrderRegistryConfig,
    payload: Mapping[str, Any],
    now: datetime | None = None,
) -> tuple[Path, list[dict[str, Any]]]:
    output_path = config.resolve(config.output_path)
    event_log_path = config.resolve(config.event_log_path)
    previous = _read_json(output_path)
    events = build_managed_order_events(previous=previous, current=payload, now=now)
    _write_json_atomic(output_path, dict(payload))
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_managed_order_projection(authority_payload=payload, authority_path=output_path),
        )
    if events:
        for event in events:
            append_bounded_jsonl(event_log_path, event)
    return output_path, events


def build_dashboard_managed_order_projection(*, authority_payload: Mapping[str, Any], authority_path: Path) -> dict[str, Any]:
    return {
        **dict(authority_payload),
        "schema_version": "track_b_managed_order_dashboard_projection_v1",
        **build_projection_metadata(source_authority_path=authority_path),
    }


def _build_dmc_metadata(
    *,
    config: TrackBManagedOrderRegistryConfig,
    payload: Mapping[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    artifact_paths = _mapping(payload.get("artifact_paths"))
    open_order_truth = _mapping(payload.get("open_order_truth"))
    position_truth = _mapping(payload.get("position_truth"))
    managed_position_registry = _mapping(payload.get("managed_position_registry"))
    reconciliation = _mapping(payload.get("reconciliation"))
    source_artifacts = [
        _dmc_source_artifact(
            artifact_family="open_order_truth",
            path=artifact_paths.get("open_order_truth") or str(config.resolve(config.open_order_truth_path)),
            observed_at=open_order_truth.get("generated_at"),
        ),
        _dmc_source_artifact(
            artifact_family="position_truth",
            path=artifact_paths.get("position_truth") or str(config.resolve(config.position_truth_path)),
            observed_at=position_truth.get("generated_at"),
        ),
        _dmc_source_artifact(
            artifact_family="managed_position_registry",
            path=artifact_paths.get("managed_position_registry")
            or str(config.resolve(config.managed_position_registry_path)),
            observed_at=managed_position_registry.get("generated_at"),
        ),
        _dmc_source_artifact(
            artifact_family="track_b_paper_broker_reconciliation",
            path=artifact_paths.get("reconciliation") or str(config.resolve(config.reconciliation_path)),
            observed_at=reconciliation.get("generated_at"),
        ),
    ]
    source_observed_at = _latest_observed_at(source_artifacts)
    return {
        "schema_version": "track_b_dmc_metadata_envelope_v1",
        "artifact_family": "latest_managed_orders",
        "authority_tier": "Tier 1 – Canonical",
        "publisher_id": "track_b_managed_order_registry.py",
        "owner_id": "Managed Order Registry",
        "generated_at": generated_at.isoformat(),
        "source_observed_at": source_observed_at,
        "source_artifacts": source_artifacts,
        "refresh_scope": {
            "scope_type": "GLOBAL_COMPLETE",
            "account_scope": "Track B PAPER",
            "symbols": "ALL_TRACK_B_FUTURES_FROM_OPEN_ORDER_TRUTH",
            "partial": False,
        },
        "retention_model": "rolling latest snapshot with append-only managed order event companion",
        "append_only": False,
        "diagnostic_only": False,
        "analytics_only": False,
        "can_influence_runtime": True,
        "can_influence_managed_exit": True,
    }


def _dmc_source_artifact(*, artifact_family: str, path: Any, observed_at: Any) -> dict[str, Any]:
    return {
        "artifact_family": artifact_family,
        "path": str(path or ""),
        "observed_at": observed_at,
    }


def _latest_observed_at(source_artifacts: list[dict[str, Any]]) -> str | None:
    latest: datetime | None = None
    for artifact in source_artifacts:
        parsed = _parse_time(artifact.get("observed_at"))
        if parsed is None:
            continue
        latest = parsed if latest is None or parsed > latest else latest
    return None if latest is None else latest.isoformat()


def build_managed_order_events(
    *,
    previous: Mapping[str, Any] | None,
    current: Mapping[str, Any],
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    previous_state = _mapping((previous or {}).get("event_state"))
    current_state = _mapping(current.get("event_state"))
    if previous_state == current_state:
        return []
    actual_now = _ensure_utc(now or datetime.now(UTC))
    return [
        {
            "schema_version": "track_b_managed_order_event_v1",
            "event_type": "MANAGED_ORDER_REGISTRY_CHANGED",
            "generated_at": actual_now.isoformat(),
            "previous_classification": previous_state.get("classification"),
            "classification": current_state.get("classification"),
            "previous_signature": previous_state.get("signature"),
            "signature": current_state.get("signature"),
            "read_only": True,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        }
    ]


def _managed_order_from_open_order_state(
    *,
    state: Mapping[str, Any],
    duplicate_order_ids: set[str],
    flat_close_order_ids: set[str],
    lifecycle_reports: list[dict[str, Any]],
    terminal_records: list[Any] | tuple[Any, ...],
    broker_positions: list[dict[str, Any]],
    broker_open_orders: list[dict[str, Any]],
    manifests: list[dict[str, Any]],
    ownership_records: list[dict[str, Any]],
) -> dict[str, Any]:
    order = _mapping(state.get("order"))
    order_identity = _order_identity(order or state)
    lifecycle_report = _lifecycle_report_for_order(
        order=order,
        lifecycle_reports=lifecycle_reports,
        terminal_records=terminal_records,
        broker_positions=broker_positions,
        broker_open_orders=broker_open_orders or [dict(order)],
    )
    manifest = _manifest_for_order(order=order, lifecycle_report=lifecycle_report, manifests=manifests)
    ownership = _ownership_for_order(order=order, lifecycle_report=lifecycle_report, ownership_records=ownership_records)
    classification = _classify_managed_order_state(
        state=state,
        order=order,
        order_identity=order_identity,
        duplicate_order_ids=duplicate_order_ids,
        flat_close_order_ids=flat_close_order_ids,
    )
    recommended = _recommended_next_action(classification=classification, state=state)
    return {
        "classification": classification,
        "recommended_next_action": recommended,
        "symbol": state.get("symbol") or _row_symbol(order),
        "contract": order.get("local_symbol") or state.get("local_symbol") or order.get("contract_key"),
        "local_symbol": order.get("local_symbol") or state.get("local_symbol"),
        "con_id": order.get("con_id") or lifecycle_report.get("con_id") or manifest.get("con_id"),
        "action": state.get("action") or _action(order),
        "side": _side_from_action(state.get("action") or _action(order)),
        "quantity": state.get("quantity") or _decimal_text(_quantity(order)),
        "broker_order_id": state.get("broker_order_id") or order.get("broker_order_id") or order.get("order_id"),
        "perm_id": state.get("perm_id") or order.get("perm_id"),
        "client_id": state.get("client_id") or order.get("client_id"),
        "order_type": order.get("order_type") or order.get("type"),
        "limit_price": state.get("limit_price") or order.get("limit_price") or order.get("order_limit_price"),
        "time_in_force": order.get("time_in_force") or order.get("tif"),
        "broker_status": state.get("status") or order.get("status"),
        "filled_quantity": order.get("filled_quantity") or order.get("filled"),
        "remaining_quantity": order.get("remaining_quantity") or order.get("remaining"),
        "lifecycle_id": _lifecycle_id(lifecycle_report) or order.get("lifecycle_id"),
        "manifest_id": manifest.get("entry_intent_id") or manifest.get("manifest_id"),
        "manifest_path": manifest.get("manifest_path"),
        "ownership_id": ownership.get("ownership_intent_id"),
        "lane_id": lifecycle_report.get("lane_id") or manifest.get("lane_id") or ownership.get("lane_id"),
        "strategy_id": lifecycle_report.get("strategy_id") or manifest.get("strategy_id") or ownership.get("strategy_id"),
        "is_close_order": state.get("is_close_order") is True,
        "is_entry_order": state.get("is_entry_order") is True,
        "working": state.get("working") is True,
        "marketability": {
            "marketable": state.get("marketable") is True,
            "market_reference": state.get("market_reference") or {},
            "age_seconds": state.get("age_seconds"),
        },
        "suspicious_reasons": state.get("suspicious_reasons") or [],
        "condition_flags": state.get("condition_flags") or [],
        "duplicate_key": state.get("duplicate_key"),
        "source_open_order_truth_classification": state.get("classification"),
        "source_order": order,
    }


def _classify_managed_order_state(
    *,
    state: Mapping[str, Any],
    order: Mapping[str, Any],
    order_identity: str,
    duplicate_order_ids: set[str],
    flat_close_order_ids: set[str],
) -> str:
    status = str(state.get("status") or order.get("status") or "").upper()
    if status in _TERMINAL_FILLED_STATUSES:
        return ORDER_TERMINAL_FILLED
    if status in _TERMINAL_CANCELLED_STATUSES:
        return ORDER_TERMINAL_CANCELLED
    if order_identity in duplicate_order_ids:
        return DUPLICATE_CLOSE_ORDER_BLOCKED
    if order_identity in flat_close_order_ids:
        return BROKER_FLAT_WITH_WORKING_CLOSE
    if state.get("suspicious") is True or state.get("classification") == "SUSPICIOUS_ORDER_STATE":
        return CLOSE_ORDER_SUSPICIOUS
    flags = set(str(flag) for flag in (state.get("condition_flags") or []))
    market_reference = _mapping(state.get("market_reference"))
    if state.get("is_close_order") is True and state.get("marketable") is False and market_reference:
        return CLOSE_ORDER_NOT_MARKETABLE
    if "close_order_stale" in flags:
        return CLOSE_ORDER_CANCEL_REPLACE_REQUIRED
    if "marketable_unfilled_beyond_threshold" in flags and state.get("is_close_order") is True:
        return CLOSE_ORDER_MODIFIABLE
    if state.get("classification") == "CLOSE_ORDER_STALE":
        return CLOSE_ORDER_CANCEL_REPLACE_REQUIRED
    if state.get("classification") == "CLOSE_ORDER_MARKETABLE_NOT_FILLED":
        return CLOSE_ORDER_MODIFIABLE
    if state.get("classification") == "UNKNOWN_OPEN_ORDER" or state.get("unknown_open_order") is True:
        return ORDER_STATE_UNKNOWN_REVIEW_REQUIRED
    if state.get("is_close_order") is True:
        return WORKING_CLOSE_ORDER
    return WORKING_ENTRY_ORDER


def _position_without_close_rows(
    *,
    open_order_truth: Mapping[str, Any],
    managed_positions: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    resolver_payload: Mapping[str, Any],
    lifecycle_reports: list[dict[str, Any]],
    terminal_records: list[Any] | tuple[Any, ...],
    manifests: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    terminal_superseded_rows: list[dict[str, Any]] = []
    broker_positions = _reconciliation_broker_positions(reconciliation)
    broker_open_orders = _reconciliation_broker_open_orders(reconciliation)
    registry_positions = [
        {**dict(item), "_managed_order_registry_position_source": "MANAGED_POSITION_AUTHORITY"}
        for item in _list(managed_positions.get("managed_positions"))
    ]
    registry_positions.extend(
        {**dict(item), "_managed_order_registry_position_source": "RESOLVED_LIFECYCLE_POSITION"}
        for item in _list(resolver_payload.get("resolved_lifecycle_positions"))
        if isinstance(item, Mapping)
    )
    positions_without_close = _canonical_positions_without_close_order(
        managed_positions=managed_positions,
        open_order_truth=open_order_truth,
    )
    for position in positions_without_close:
        terminal = resolve_terminal_registry_truth(
            records=terminal_records,
            identity=position,
            broker_positions=broker_positions,
            broker_open_orders=broker_open_orders,
        )
        if terminal.terminal_closed_flat:
            terminal_superseded_rows.append(
                {
                    "classification": "STALE_SUPERSEDED_LIFECYCLE_PROJECTION",
                    "full_audit_only": True,
                    "terminal_registry_truth": terminal.to_dict(),
                    "row": dict(position),
                }
            )
            continue
        registry_position = _mapping(position.get("canonical_managed_position")) or _registry_position_for_broker_position(
            position=position,
            registry_positions=registry_positions,
        )
        lifecycle_report = _lifecycle_report_for_registry_position(
            registry_position=registry_position,
            lifecycle_reports=lifecycle_reports,
            terminal_records=terminal_records,
            broker_positions=broker_positions,
            broker_open_orders=broker_open_orders,
        )
        if not lifecycle_report and not registry_position:
            lifecycle_report = _lifecycle_report_for_position(
                position=position,
                lifecycle_reports=lifecycle_reports,
                terminal_records=terminal_records,
                broker_positions=broker_positions,
                broker_open_orders=broker_open_orders,
            )
        manifest = _manifest_for_position(position=position, lifecycle_report=lifecycle_report, manifests=manifests)
        active_hold_pending = _position_without_close_can_remain_active_hold(
            position=position,
            registry_position=registry_position,
            reconciliation=reconciliation,
            resolver_payload=resolver_payload,
        )
        classification = ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING if active_hold_pending else POSITION_WITHOUT_CLOSE_ORDER
        rows.append(
            {
                "classification": classification,
                "recommended_next_action": WAIT if active_hold_pending else REVIEW_REQUIRED,
                "symbol": _row_symbol(position),
                "contract": position.get("local_symbol") or position.get("contract_key"),
                "local_symbol": position.get("local_symbol"),
                "con_id": position.get("con_id") or lifecycle_report.get("con_id") or manifest.get("con_id"),
                "action": _expected_close_action(position=position, lifecycle_report=lifecycle_report),
                "side": registry_position.get("side") or lifecycle_report.get("side"),
                "quantity": _decimal_text(abs(_quantity(position))),
                "trade_id": registry_position.get("trade_id") or lifecycle_report.get("trade_id"),
                "broker_order_id": None,
                "perm_id": None,
                "client_id": None,
                "order_type": None,
                "limit_price": None,
                "time_in_force": None,
                "broker_status": None,
                "filled_quantity": None,
                "remaining_quantity": None,
                "lifecycle_id": _lifecycle_id(lifecycle_report) or registry_position.get("lifecycle_id"),
                "required_close_action": _expected_close_action(position=position, lifecycle_report=lifecycle_report),
                "required_close_quantity": _decimal_text(abs(_quantity(position))),
                "manifest_id": manifest.get("entry_intent_id") or manifest.get("manifest_id"),
                "manifest_path": manifest.get("manifest_path"),
                "ownership_id": None,
                "lane_id": registry_position.get("lane_id") or lifecycle_report.get("lane_id") or manifest.get("lane_id"),
                "strategy_id": registry_position.get("strategy_id") or lifecycle_report.get("strategy_id") or manifest.get("strategy_id"),
                "is_close_order": False,
                "is_entry_order": False,
                "working": False,
                "marketability": {"marketable": False, "market_reference": {}, "age_seconds": None},
                "suspicious_reasons": [],
                "condition_flags": ["broker_position_without_close_order"],
                "duplicate_key": None,
                "source_open_order_truth_classification": "BROKER_POSITION_WITHOUT_CLOSE_ORDER",
                "source_order": None,
                "broker_position": dict(position),
                "managed_position_freshness_state": _managed_position_freshness_state(position),
                "exit_due_evidence_stale": _managed_position_exit_due_evidence_stale(position),
                "apply_authority_degraded": _managed_position_apply_authority_degraded(position),
                "stale_dependency_sources": _managed_position_stale_dependency_sources(position),
                "managed_exit_profile_present": active_hold_pending,
                "exit_not_yet_eligible": active_hold_pending,
                "close_order_required_now": not active_hold_pending,
                "managed_active_hold": active_hold_pending,
            }
        )
    return rows, terminal_superseded_rows


def _managed_positions_with_owner_overlay(
    *,
    managed_positions: Mapping[str, Any],
    resolver_payload: Mapping[str, Any],
) -> dict[str, Any]:
    payload = dict(managed_positions)
    positions = [dict(item) for item in _list(managed_positions.get("managed_positions"))]
    by_key = {
        _position_without_close_key(_mapping(item.get("broker_position")) or item): idx
        for idx, item in enumerate(positions)
        if _position_without_close_key(_mapping(item.get("broker_position")) or item)
    }
    by_dedupe_key: dict[str, int] = {}
    for idx, item in enumerate(positions):
        for dedupe_key in _position_without_close_dedupe_keys(_mapping(item.get("broker_position")) or item):
            by_dedupe_key.setdefault(dedupe_key, idx)
    for exposure in _list(resolver_payload.get("owned_exposures")):
        if not isinstance(exposure, Mapping):
            continue
        broker_position = _mapping(exposure.get("canonical_broker_position")) or _mapping(exposure.get("broker_position"))
        if _decimal_or_none(broker_position.get("quantity")) in {None, Decimal("0")}:
            continue
        lifecycle_position = _mapping(exposure.get("lifecycle_position"))
        key = _position_without_close_key(broker_position) or _position_without_close_key(lifecycle_position)
        if not key:
            continue
        replacement_idx = by_key.get(key)
        if replacement_idx is None:
            for dedupe_key in _position_without_close_dedupe_keys(broker_position) | _position_without_close_dedupe_keys(lifecycle_position):
                if dedupe_key in by_dedupe_key:
                    replacement_idx = by_dedupe_key[dedupe_key]
                    break
        existing_position = positions[replacement_idx] if replacement_idx is not None else {}
        owner_row = _managed_position_row_from_owner_exposure(
            exposure=exposure,
            broker_position=broker_position,
            lifecycle_position=lifecycle_position,
            existing_position=existing_position,
        )
        if replacement_idx is not None:
            positions[replacement_idx] = owner_row
        else:
            replacement_idx = len(positions)
            positions.append(owner_row)
        by_key[key] = replacement_idx
        for dedupe_key in _position_without_close_dedupe_keys(owner_row) | _position_without_close_dedupe_keys(broker_position):
            by_dedupe_key[dedupe_key] = replacement_idx
    payload["managed_positions"] = positions
    if positions and str(payload.get("classification") or "") == NO_MANAGED_ORDERS:
        payload["classification"] = positions[0].get("classification") or payload.get("classification")
    return payload


def _reconciliation_broker_positions(reconciliation: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _list(reconciliation.get("track_b_broker_positions"))
    if rows:
        return rows
    return [
        dict(position)
        for match in _list(_mapping(reconciliation.get("position_match_report")).get("matches"))
        for position in [_mapping(match.get("broker_position"))]
        if position
    ]


def _reconciliation_lifecycle_positions(reconciliation: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _list(reconciliation.get("track_b_lifecycle_positions"))
    if rows:
        return rows
    return [
        dict(position)
        for match in _list(_mapping(reconciliation.get("position_match_report")).get("matches"))
        for position in [_mapping(match.get("lifecycle_position"))]
        if position
    ]


def _reconciliation_broker_open_orders(reconciliation: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _list(reconciliation.get("track_b_broker_open_orders"))
    if rows:
        return rows
    return [
        dict(order)
        for match in _list(_mapping(reconciliation.get("position_match_report")).get("matches"))
        for order in _list(match.get("broker_open_orders"))
        if isinstance(order, Mapping)
    ]


def _managed_position_row_from_owner_exposure(
    *,
    exposure: Mapping[str, Any],
    broker_position: Mapping[str, Any],
    lifecycle_position: Mapping[str, Any],
    existing_position: Mapping[str, Any],
) -> dict[str, Any]:
    exit_due = (
        exposure.get("exit_due") is True
        or existing_position.get("exit_due") is True
        or str(exposure.get("classification") or "") == "OWNED_MANAGED_EXIT_DUE"
        or str(existing_position.get("exit_due_state") or "") == "EXIT_DUE"
        or str(lifecycle_position.get("exit_due_state") or "") == "EXIT_DUE"
    )
    classification = "OPEN_MANAGED_EXIT_DUE" if exit_due else "OPEN_MANAGED_MATCHED"
    signed_qty = _decimal_or_none(broker_position.get("quantity"))
    return {
        **dict(existing_position),
        "classification": classification,
        "symbol": broker_position.get("symbol") or lifecycle_position.get("symbol"),
        "local_symbol": broker_position.get("local_symbol") or broker_position.get("localSymbol") or lifecycle_position.get("local_symbol"),
        "con_id": broker_position.get("con_id") or broker_position.get("conId") or lifecycle_position.get("con_id"),
        "account_id": broker_position.get("account_id") or broker_position.get("account") or lifecycle_position.get("account_id"),
        "side": lifecycle_position.get("side") or ("SHORT" if signed_qty is not None and signed_qty < 0 else "LONG"),
        "quantity": _decimal_text(abs(signed_qty)) if signed_qty is not None else lifecycle_position.get("quantity"),
        "aggregate_qty": _decimal_text(signed_qty) if signed_qty is not None else lifecycle_position.get("aggregate_qty"),
        "signed_broker_qty": _decimal_text(signed_qty) if signed_qty is not None else lifecycle_position.get("signed_broker_qty"),
        "trade_id": exposure.get("trade_id") or lifecycle_position.get("trade_id"),
        "lifecycle_id": exposure.get("lifecycle_id") or lifecycle_position.get("lifecycle_id"),
        "lane_id": lifecycle_position.get("lane_id") or exposure.get("lane_id"),
        "strategy_id": lifecycle_position.get("strategy_id") or exposure.get("strategy_id"),
        "managed_exit_policy_id": lifecycle_position.get("managed_exit_policy_id") or exposure.get("managed_exit_policy_id"),
        "exit_due": exit_due,
        "exit_due_state": "EXIT_DUE" if exit_due else "NOT_DUE_OR_UNKNOWN",
        "freshness_state": existing_position.get("freshness_state"),
        "exit_due_evidence_stale": existing_position.get("exit_due_evidence_stale") is True,
        "apply_authority_degraded": existing_position.get("apply_authority_degraded") is True,
        "stale_dependency_sources": list(existing_position.get("stale_dependency_sources") or []),
        "attention_required": False,
        "working_close_qty": "0",
        "broker_position": dict(broker_position),
        "projection_authority_owner_confirmed": True,
        "projection_authority_source": "CURRENT_EXPOSURE_OWNER_RESOLVER",
    }


def _canonical_positions_without_close_order(
    *,
    managed_positions: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    seen_dedupe_keys: set[str] = set()
    for position in _list(managed_positions.get("managed_positions")):
        if str(position.get("classification") or "") not in {"OPEN_MANAGED_EXIT_DUE", "OPEN_MANAGED_MATCHED"}:
            continue
        exit_due = position.get("exit_due") is True or str(position.get("exit_due_state") or "") == "EXIT_DUE"
        if not exit_due:
            continue
        if _decimal_or_none(position.get("working_close_qty")) not in {None, Decimal("0")}:
            continue
        broker_position = _mapping(position.get("broker_position"))
        row = dict(broker_position or position)
        row["position_without_close_source"] = "MANAGED_POSITION_AUTHORITY"
        row.setdefault("symbol", position.get("symbol"))
        row.setdefault("local_symbol", position.get("local_symbol"))
        row.setdefault("con_id", position.get("con_id"))
        row.setdefault("quantity", position.get("signed_broker_qty") or position.get("aggregate_qty"))
        row["canonical_managed_position"] = dict(position)
        key = _position_without_close_key(row)
        dedupe_keys = _position_without_close_dedupe_keys(row)
        if key and key not in seen:
            seen.add(key)
            seen_dedupe_keys.update(dedupe_keys)
            rows.append(row)
    for position in _list(open_order_truth.get("broker_positions_without_close_order")):
        key = _position_without_close_key(position)
        dedupe_keys = _position_without_close_dedupe_keys(position)
        if key and key in seen:
            continue
        if dedupe_keys & seen_dedupe_keys:
            continue
        if key:
            seen.add(key)
        seen_dedupe_keys.update(dedupe_keys)
        rows.append({**dict(position), "position_without_close_source": "OPEN_ORDER_TRUTH_BROKER_POSITION_WITHOUT_CLOSE_ORDER"})
    return rows


def _position_without_close_key(row: Mapping[str, Any]) -> str:
    account = str(row.get("account_id") or row.get("account") or "").strip().upper()
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or row.get("contract_key") or "").strip().upper()
    con_id = str(row.get("con_id") or row.get("conId") or "").strip()
    return "|".join(part for part in (account, local_symbol, con_id) if part)


def _position_without_close_dedupe_keys(row: Mapping[str, Any]) -> set[str]:
    account = str(row.get("account_id") or row.get("account") or "").strip().upper()
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or row.get("contract_key") or "").strip().upper()
    con_id = str(row.get("con_id") or row.get("conId") or "").strip()
    keys: set[str] = set()
    if account and local_symbol:
        keys.add(f"account_local|{account}|{local_symbol}")
    if local_symbol and con_id:
        keys.add(f"local_con|{local_symbol}|{con_id}")
    if local_symbol:
        keys.add(f"local|{local_symbol}")
    return keys


def _overall_classification(*, source_stale: Mapping[str, Any], managed_orders: list[dict[str, Any]]) -> str:
    if not managed_orders:
        return NO_MANAGED_ORDERS
    if source_stale.get("stale") is True and not _has_due_position_without_close_order(managed_orders):
        return ORDER_STATE_UNKNOWN_REVIEW_REQUIRED
    priority = [
        DUPLICATE_CLOSE_ORDER_BLOCKED,
        CLOSE_ORDER_NOT_MARKETABLE,
        CLOSE_ORDER_SUSPICIOUS,
        BROKER_FLAT_WITH_WORKING_CLOSE,
        ORDER_STATE_UNKNOWN_REVIEW_REQUIRED,
        CLOSE_ORDER_CANCEL_REPLACE_REQUIRED,
        CLOSE_ORDER_MODIFIABLE,
        POSITION_WITHOUT_CLOSE_ORDER,
        WORKING_CLOSE_ORDER,
        WORKING_ENTRY_ORDER,
        ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING,
        ORDER_TERMINAL_FILLED,
        ORDER_TERMINAL_CANCELLED,
    ]
    classes = {str(item.get("classification") or "") for item in managed_orders}
    for classification in priority:
        if classification in classes:
            return classification
    return ORDER_STATE_UNKNOWN_REVIEW_REQUIRED


def _has_due_position_without_close_order(managed_orders: list[dict[str, Any]]) -> bool:
    return any(
        str(item.get("classification") or "") == POSITION_WITHOUT_CLOSE_ORDER
        and item.get("close_order_required_now") is True
        for item in managed_orders
    )


def _managed_position_freshness_state(position: Mapping[str, Any]) -> str | None:
    managed_position = _mapping(position.get("canonical_managed_position"))
    value = managed_position.get("freshness_state") or position.get("freshness_state")
    return str(value) if value is not None else None


def _managed_position_exit_due_evidence_stale(position: Mapping[str, Any]) -> bool:
    managed_position = _mapping(position.get("canonical_managed_position"))
    return managed_position.get("exit_due_evidence_stale") is True or position.get("exit_due_evidence_stale") is True


def _managed_position_apply_authority_degraded(position: Mapping[str, Any]) -> bool:
    managed_position = _mapping(position.get("canonical_managed_position"))
    return managed_position.get("apply_authority_degraded") is True or position.get("apply_authority_degraded") is True


def _managed_position_stale_dependency_sources(position: Mapping[str, Any]) -> list[Any]:
    managed_position = _mapping(position.get("canonical_managed_position"))
    return list(managed_position.get("stale_dependency_sources") or position.get("stale_dependency_sources") or [])


def _recommended_next_action(*, classification: str, state: Mapping[str, Any]) -> str:
    if classification == DUPLICATE_CLOSE_ORDER_BLOCKED:
        return DO_NOT_REPLACE_DUPLICATE_RISK
    if classification in {CLOSE_ORDER_SUSPICIOUS, CLOSE_ORDER_CANCEL_REPLACE_REQUIRED, CLOSE_ORDER_NOT_MARKETABLE}:
        return TARGETED_CANCEL_REPLACE_CANDIDATE
    if classification == CLOSE_ORDER_MODIFIABLE:
        return MODIFY_IN_PLACE_CANDIDATE
    if classification in {BROKER_FLAT_WITH_WORKING_CLOSE, ORDER_STATE_UNKNOWN_REVIEW_REQUIRED}:
        return REVIEW_REQUIRED
    return WAIT


def _active_order_states_from_current_truth(
    *,
    open_order_truth: Mapping[str, Any],
    order_states: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not order_states or not _open_order_truth_proves_no_current_orders(open_order_truth):
        return order_states, []
    invalidated = [
        _current_truth_invalidated_order_state(state=state, open_order_truth=open_order_truth)
        for state in order_states
    ]
    return [], invalidated


def _open_order_truth_proves_no_current_orders(open_order_truth: Mapping[str, Any]) -> bool:
    summary = _mapping(open_order_truth.get("summary"))
    open_order_count = _decimal_or_none(summary.get("open_order_count"))
    return (
        str(open_order_truth.get("canonical_refresh_scope") or "") == "GLOBAL_COMPLETE"
        and str(open_order_truth.get("classification") or "") == "NO_OPEN_ORDERS"
        and open_order_count == Decimal("0")
        and not _list(open_order_truth.get("duplicate_close_order_groups"))
    )


def _current_truth_invalidated_order_state(
    *,
    state: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
) -> dict[str, Any]:
    order = _mapping(state.get("order"))
    row = dict(state)
    row.update(
        {
            "historical_only": True,
            "diagnostic_only": True,
            "current_scope_active": False,
            "invalidated_by_current_truth": True,
            "invalidation_reason": "ORDER_ABSENT_FROM_GLOBAL_COMPLETE_OPEN_ORDER_TRUTH",
            "order_identity": _order_identity(order or state),
            "source_refs": {
                "open_order_truth_classification": open_order_truth.get("classification"),
                "open_order_truth_generated_at": open_order_truth.get("generated_at"),
                "canonical_refresh_scope": open_order_truth.get("canonical_refresh_scope"),
                "open_order_truth_summary": _mapping(open_order_truth.get("summary")),
            },
        }
    )
    return row


def _source_stale(
    *,
    now: datetime,
    config: TrackBManagedOrderRegistryConfig,
    open_order_truth: Mapping[str, Any],
    position_truth: Mapping[str, Any],
    managed_positions: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
) -> dict[str, Any]:
    sources = {
        "open_order_truth": open_order_truth,
        "position_truth": position_truth,
        "managed_position_registry": managed_positions,
        "reconciliation": reconciliation,
    }
    ages = {
        name: _age_seconds(payload.get("generated_at"), now)
        for name, payload in sources.items()
    }
    stale_sources = [
        name
        for name, age in ages.items()
        if age is None or age > float(config.artifact_max_age_seconds)
    ]
    return {
        "ttl_seconds": float(config.artifact_max_age_seconds),
        "ages_seconds": ages,
        "stale_sources": stale_sources,
        "stale": bool(stale_sources),
    }


def _authority_summary(payload: Mapping[str, Any], path: Path) -> dict[str, Any]:
    return {
        "artifact_path": str(path),
        "schema_version": payload.get("schema_version"),
        "classification": payload.get("classification") or _mapping(payload.get("summary")).get("overall_classification"),
        "generated_at": payload.get("generated_at"),
        "projection_only": payload.get("projection_only") is True,
    }


def _event_state(*, classification: str, managed_orders: list[dict[str, Any]]) -> dict[str, Any]:
    orders = [
        {
            "classification": item.get("classification"),
            "broker_order_id": str(item.get("broker_order_id") or ""),
            "perm_id": str(item.get("perm_id") or ""),
            "symbol": item.get("symbol"),
            "contract": item.get("contract"),
            "action": item.get("action"),
            "quantity": item.get("quantity"),
            "broker_status": item.get("broker_status"),
            "recommended_next_action": item.get("recommended_next_action"),
            "suspicious_reasons": item.get("suspicious_reasons") or [],
        }
        for item in managed_orders
    ]
    signature = json.dumps({"classification": classification, "orders": orders}, sort_keys=True)
    return {
        "classification": classification,
        "signature": signature,
        "orders": orders,
        "managed_order_count": len(managed_orders),
    }


def _duplicate_order_ids(duplicate_groups: list[dict[str, Any]]) -> set[str]:
    ids: set[str] = set()
    for group in duplicate_groups:
        for state in _list(group.get("orders")):
            identity = _order_identity(state.get("order") or state)
            if identity:
                ids.add(identity)
    return ids


def _order_identity(order: Mapping[str, Any]) -> str:
    order_id = str(order.get("broker_order_id") or order.get("order_id") or "").strip()
    perm_id = str(order.get("perm_id") or "").strip()
    if order_id:
        return f"order:{order_id}"
    if perm_id:
        return f"perm:{perm_id}"
    return ""


def _lifecycle_report_for_order(
    *,
    order: Mapping[str, Any],
    lifecycle_reports: list[dict[str, Any]],
    terminal_records: list[Any] | tuple[Any, ...],
    broker_positions: list[dict[str, Any]],
    broker_open_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    order_id = str(order.get("broker_order_id") or order.get("order_id") or "").strip()
    perm_id = str(order.get("perm_id") or "").strip()
    for report in lifecycle_reports:
        close_attempt = _mapping(report.get("close_submit_attempt"))
        close_fill = _mapping(report.get("close_fill"))
        if order_id and str(close_attempt.get("broker_order_id") or "") == order_id:
            return _terminal_scoped_lifecycle_report(
                report=report,
                terminal_records=terminal_records,
                broker_positions=broker_positions,
                broker_open_orders=broker_open_orders,
            )
        if order_id and str(close_fill.get("broker_order_id") or "") == order_id:
            return _terminal_scoped_lifecycle_report(
                report=report,
                terminal_records=terminal_records,
                broker_positions=broker_positions,
                broker_open_orders=broker_open_orders,
            )
        if perm_id and str(close_fill.get("perm_id") or "") == perm_id:
            return _terminal_scoped_lifecycle_report(
                report=report,
                terminal_records=terminal_records,
                broker_positions=broker_positions,
                broker_open_orders=broker_open_orders,
            )
    lifecycle_id = str(order.get("lifecycle_id") or "").strip()
    if lifecycle_id:
        for report in lifecycle_reports:
            if _lifecycle_id(report) == lifecycle_id:
                return _terminal_scoped_lifecycle_report(
                    report=report,
                    terminal_records=terminal_records,
                    broker_positions=broker_positions,
                    broker_open_orders=broker_open_orders,
                )
    return {}


def _lifecycle_report_for_position(
    *,
    position: Mapping[str, Any],
    lifecycle_reports: list[dict[str, Any]],
    terminal_records: list[Any] | tuple[Any, ...],
    broker_positions: list[dict[str, Any]],
    broker_open_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    for report in reversed(lifecycle_reports):
        if _same_contract(position, report) and str(report.get("final_position_status") or "").upper() == "OPEN_MANAGED":
            return _terminal_scoped_lifecycle_report(
                report=report,
                terminal_records=terminal_records,
                broker_positions=broker_positions,
                broker_open_orders=broker_open_orders,
            )
    return {}


def _lifecycle_report_for_registry_position(
    *,
    registry_position: Mapping[str, Any],
    lifecycle_reports: list[dict[str, Any]],
    terminal_records: list[Any] | tuple[Any, ...],
    broker_positions: list[dict[str, Any]],
    broker_open_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    lifecycle_id = str(registry_position.get("lifecycle_id") or "").strip()
    if not lifecycle_id:
        return {}
    for report in reversed(lifecycle_reports):
        if _lifecycle_id(report) == lifecycle_id:
            return _terminal_scoped_lifecycle_report(
                report=report,
                terminal_records=terminal_records,
                broker_positions=broker_positions,
                broker_open_orders=broker_open_orders,
            )
    return {}


def _terminal_scoped_lifecycle_report(
    *,
    report: Mapping[str, Any],
    terminal_records: list[Any] | tuple[Any, ...],
    broker_positions: list[dict[str, Any]],
    broker_open_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    terminal = resolve_terminal_registry_truth(
        records=terminal_records,
        identity=report,
        broker_positions=broker_positions,
        broker_open_orders=broker_open_orders,
    )
    if terminal.terminal_closed_flat:
        return {
            "classification": "STALE_SUPERSEDED_LIFECYCLE_PROJECTION",
            "full_audit_only": True,
            "terminal_registry_truth": terminal.to_dict(),
            "superseded_lifecycle_report": dict(report),
        }
    return dict(report)


def _registry_position_for_broker_position(
    *,
    position: Mapping[str, Any],
    registry_positions: list[dict[str, Any]],
) -> dict[str, Any]:
    for item in registry_positions:
        broker_position = _mapping(item.get("broker_position"))
        if broker_position and _same_contract(position, broker_position):
            return item
        if _same_contract(position, item):
            return item
    return {}


def _active_hold_managed_timed_exit_pending(*, registry_position: Mapping[str, Any]) -> bool:
    if str(registry_position.get("classification") or "") != "OPEN_MANAGED_MATCHED":
        return False
    if not str(registry_position.get("managed_exit_policy_id") or "").strip():
        return False
    if registry_position.get("exit_due") is True:
        return False
    if registry_position.get("attention_required") is True:
        return False
    if _mapping(registry_position.get("close_order_state")):
        return False
    return bool(registry_position.get("lifecycle_id"))


def _position_without_close_can_remain_active_hold(
    *,
    position: Mapping[str, Any],
    registry_position: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    resolver_payload: Mapping[str, Any],
) -> bool:
    if not _active_hold_managed_timed_exit_pending(registry_position=registry_position):
        return False
    if str(position.get("position_without_close_source") or "") != "OPEN_ORDER_TRUTH_BROKER_POSITION_WITHOUT_CLOSE_ORDER":
        return True
    if str(registry_position.get("_managed_order_registry_position_source") or "") != "MANAGED_POSITION_AUTHORITY":
        return False
    projection_source = str(registry_position.get("projection_authority_source") or "")
    if projection_source != "CURRENT_EXPOSURE_OWNER_RESOLVER":
        return True
    if not _reconciliation_clean_current_scope(reconciliation):
        return False
    if _resolver_has_unowned_or_review_exposure(resolver_payload):
        return False
    return _broker_backed_matched_position(registry_position)


def _reconciliation_clean_current_scope(reconciliation: Mapping[str, Any]) -> bool:
    if reconciliation.get("broker_reconciled") is not True:
        return False
    if str(reconciliation.get("classification") or "") != "TRACK_B_PAPER_BROKER_RECONCILED":
        return False
    if int(reconciliation.get("current_scope_review_required_count") or 0) != 0:
        return False
    if int(reconciliation.get("review_required_count") or 0) != 0:
        return False
    if reconciliation.get("blockers"):
        return False
    broker_position_count = int(reconciliation.get("track_b_broker_position_count") or 0)
    lifecycle_position_count = int(reconciliation.get("lifecycle_open_position_count") or 0)
    if broker_position_count > 0 and lifecycle_position_count != broker_position_count:
        return False
    return True


def _resolver_has_unowned_or_review_exposure(resolver_payload: Mapping[str, Any]) -> bool:
    if not resolver_payload:
        return False
    if _list(resolver_payload.get("unowned_exposures")):
        return True
    if _list(resolver_payload.get("review_required_exposures")):
        return True
    try:
        return int(resolver_payload.get("review_required_exposure_count") or 0) > 0
    except (TypeError, ValueError):
        return True


def _broker_backed_matched_position(registry_position: Mapping[str, Any]) -> bool:
    return registry_position.get("projection_authority_owner_confirmed") is True


def _manifest_for_order(
    *,
    order: Mapping[str, Any],
    lifecycle_report: Mapping[str, Any],
    manifests: list[dict[str, Any]],
) -> dict[str, Any]:
    lifecycle_id = _lifecycle_id(lifecycle_report) or str(order.get("lifecycle_id") or "")
    for manifest in manifests:
        if lifecycle_id and str(manifest.get("lifecycle_id") or "") == lifecycle_id:
            return manifest
    return _manifest_for_position(position=order, lifecycle_report=lifecycle_report, manifests=manifests)


def _manifest_for_position(
    *,
    position: Mapping[str, Any],
    lifecycle_report: Mapping[str, Any],
    manifests: list[dict[str, Any]],
) -> dict[str, Any]:
    lifecycle_id = _lifecycle_id(lifecycle_report)
    for manifest in manifests:
        if lifecycle_id and str(manifest.get("lifecycle_id") or "") == lifecycle_id:
            return manifest
    for manifest in manifests:
        if _same_contract(position, manifest):
            return manifest
    return {}


def _ownership_for_order(
    *,
    order: Mapping[str, Any],
    lifecycle_report: Mapping[str, Any],
    ownership_records: list[dict[str, Any]],
) -> dict[str, Any]:
    order_id = str(order.get("broker_order_id") or order.get("order_id") or "").strip()
    perm_id = str(order.get("perm_id") or "").strip()
    lifecycle_id = _lifecycle_id(lifecycle_report) or str(order.get("lifecycle_id") or "")
    for record in reversed(ownership_records):
        if lifecycle_id and str(record.get("lifecycle_id") or "") == lifecycle_id:
            return record
        if perm_id and str(record.get("perm_id") or "") == perm_id:
            return record
        if order_id and str(record.get("broker_order_id") or "") == order_id and _same_contract(order, record):
            return record
    return {}


def _load_lifecycle_reports(root: Path) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    if not root.exists():
        return reports
    for path in root.glob("*/track_b_strategy_managed_paper_lifecycle_report.json"):
        payload = _read_json(path)
        if payload:
            reports.append({**payload, "report_json_path": str(path)})
    latest = _read_json(root / "latest_track_b_strategy_managed_paper_lifecycle_report.json")
    if latest:
        reports.append({**latest, "report_json_path": str(root / "latest_track_b_strategy_managed_paper_lifecycle_report.json")})
    return reports


def _load_manifests(root: Path) -> list[dict[str, Any]]:
    manifests: list[dict[str, Any]] = []
    if not root.exists():
        return manifests
    for path in root.glob("*.json"):
        payload = _read_json(path)
        if payload:
            manifests.append({**payload, "manifest_path": str(path)})
    return manifests


def _load_submit_ownership_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            records.append(payload)
    return records


def _expected_close_action(*, position: Mapping[str, Any], lifecycle_report: Mapping[str, Any]) -> str | None:
    side = str(lifecycle_report.get("side") or "").upper()
    qty = _quantity(position)
    if side == "LONG" or qty > 0:
        return "SELL"
    if side == "SHORT" or qty < 0:
        return "BUY"
    return None


def _side_from_action(action: Any) -> str | None:
    raw = str(action or "").upper()
    if raw == "BUY":
        return "LONG_OR_BUY_TO_CLOSE"
    if raw == "SELL":
        return "SHORT_OR_SELL_TO_CLOSE"
    return None


def _lifecycle_id(report: Mapping[str, Any]) -> str:
    return str(report.get("lifecycle_id") or _mapping(report.get("entry_intent")).get("lifecycle_id") or "").strip()


def _same_contract(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_con = str(left.get("con_id") or "").strip()
    right_con = str(right.get("con_id") or "").strip()
    if left_con and right_con and left_con == right_con:
        return True
    left_local = str(left.get("local_symbol") or "").upper()
    right_local = str(right.get("local_symbol") or "").upper()
    if left_local and right_local and left_local == right_local:
        return True
    return bool(_row_symbol(left) and _row_symbol(left) == _row_symbol(right))


def _row_symbol(row: Mapping[str, Any]) -> str:
    return str(
        row.get("track_b_root")
        or row.get("instrument_family")
        or row.get("symbol")
        or _symbol_from_local(row.get("local_symbol"))
        or ""
    ).upper()


def _symbol_from_local(value: Any) -> str:
    raw = str(value or "").upper()
    return "".join(ch for ch in raw if ch.isalpha())[:3]


def _quantity(row: Mapping[str, Any]) -> Decimal:
    value = row.get("quantity") or row.get("qty") or row.get("signed_quantity") or "0"
    return _decimal_or_none(value) or Decimal("0")


def _action(row: Mapping[str, Any]) -> str:
    return str(row.get("action") or row.get("side") or "").upper()


def _age_seconds(value: Any, now: datetime) -> float | None:
    parsed = _parse_time(value)
    if parsed is None:
        return None
    return round(max((now - parsed).total_seconds(), 0.0), 3)


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _parse_time(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(path, payload)


def _list(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


__all__ = [
    "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING",
    "BROKER_FLAT_WITH_WORKING_CLOSE",
    "CLOSE_ORDER_CANCEL_REPLACE_REQUIRED",
    "CLOSE_ORDER_MODIFIABLE",
    "CLOSE_ORDER_NOT_MARKETABLE",
    "CLOSE_ORDER_SUSPICIOUS",
    "DUPLICATE_CLOSE_ORDER_BLOCKED",
    "NO_MANAGED_ORDERS",
    "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED",
    "ORDER_TERMINAL_CANCELLED",
    "ORDER_TERMINAL_FILLED",
    "POSITION_WITHOUT_CLOSE_ORDER",
    "WORKING_CLOSE_ORDER",
    "WORKING_ENTRY_ORDER",
    "TrackBManagedOrderRegistryConfig",
    "build_dashboard_managed_order_projection",
    "build_managed_order_events",
    "build_track_b_managed_order_registry",
    "write_track_b_managed_order_registry",
]
