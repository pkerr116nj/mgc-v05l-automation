"""Read-only Track B PAPER managed-position registry authority.

Managed Position Registry authority lives in execution_core; dashboard
artifacts are projections and must not be used as runtime, readiness, or
routing authority.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_jsonl import append_bounded_jsonl
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_contract_identity import normalize_track_b_contract_row
from mgc_v05l.execution_core.track_b_lifecycle_state_transition import (
    is_registry_eligible,
    normalize_lifecycle_state,
    requires_operator_action,
)
from mgc_v05l.execution_core.track_b_pre_restart_exposure_reconciliation import (
    PreRestartExposureResolverConfig,
    resolve_pre_restart_exposure_reconciliation,
)
from mgc_v05l.execution_core.track_b_current_exposure_owner_resolver import (
    apply_current_exposure_owner_lifecycle_overlay,
)
from mgc_v05l.execution_core.track_b_projection_metadata import build_projection_metadata
from mgc_v05l.execution_core.track_b_live_trade_registry import load_live_trade_registry_records
from mgc_v05l.execution_core.track_b_terminal_registry_truth import (
    filter_terminal_superseded_current_rows,
    resolve_terminal_registry_truth,
)
from mgc_v05l.execution_core.track_b_strategy_attrition_funnel import (
    events_from_managed_position_registry,
    try_record_strategy_funnel_events,
)


NO_MANAGED_POSITIONS = "NO_MANAGED_POSITIONS"
OPEN_MANAGED_MATCHED = "OPEN_MANAGED_MATCHED"
OPEN_MANAGED_EXIT_DUE = "OPEN_MANAGED_EXIT_DUE"
OPEN_MANAGED_CLOSE_WORKING = "OPEN_MANAGED_CLOSE_WORKING"
BROKER_BACKED_ADOPTION_REQUIRED = "BROKER_BACKED_ADOPTION_REQUIRED"
MANAGED_POSITION_METADATA_INCOMPLETE = "MANAGED_POSITION_METADATA_INCOMPLETE"
LIFECYCLE_WITHOUT_BROKER = "LIFECYCLE_WITHOUT_BROKER"
REVIEW_REQUIRED = "REVIEW_REQUIRED"
STALE_MANAGED_POSITION_EVIDENCE = "STALE_MANAGED_POSITION_EVIDENCE"
PROJECTION_AUTHORITY_DIVERGENCE = "PROJECTION_AUTHORITY_DIVERGENCE"
PROJECTION_AUTHORITY_COHERENT = "PROJECTION_AUTHORITY_COHERENT"

DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
)
DEFAULT_MANAGED_POSITION_EVENTS = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "managed_position_events.jsonl"
)
DEFAULT_DASHBOARD_MANAGED_POSITION_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_managed_positions.json"
)
DEFAULT_POSITION_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
)
DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
)
DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json"
)
DEFAULT_RECONCILIATION_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_BROKER_POSITIONS_SNAPSHOT = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json"
)
DEFAULT_BROKER_OPEN_ORDERS_SNAPSHOT = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json"
)
DEFAULT_LIVE_POSITION_STATUS_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_trade_ledger"
    / "latest_track_b_live_position_status.json"
)
DEFAULT_LIFECYCLE_ROOT = (
    Path("outputs") / "track_b_execution_core" / "track_b_strategy_managed_paper_lifecycle"
)
DEFAULT_MANIFEST_ROOT = Path("outputs") / "track_b_execution_core" / "position_management_manifests"
DEFAULT_PHASE1_RUNTIME_MARKET_DATA_ROOT = (
    Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
)
DEFAULT_POSITION_INTENT_CONTRACT_AUDIT = (
    Path("outputs") / "track_b_execution_core" / "diagnostics" / "latest_position_intent_contract_audit.json"
)
DEFAULT_HOLD_EXIT_SHADOW_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_shadow"
    / "latest_managed_position_hold_exit_shadow.json"
)
DEFAULT_HOLD_EXIT_SHADOW_EVENTS = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_shadow"
    / "managed_position_hold_exit_shadow_events.jsonl"
)


@dataclass(frozen=True)
class TrackBManagedPositionRegistryConfig:
    repo_root: Path
    output_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    event_log_path: Path = DEFAULT_MANAGED_POSITION_EVENTS
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_MANAGED_POSITION_PROJECTION
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    live_position_status_path: Path = DEFAULT_LIVE_POSITION_STATUS_ARTIFACT
    lifecycle_root: Path = DEFAULT_LIFECYCLE_ROOT
    manifest_root: Path = DEFAULT_MANIFEST_ROOT
    market_data_root: Path = DEFAULT_PHASE1_RUNTIME_MARKET_DATA_ROOT
    position_intent_audit_path: Path = DEFAULT_POSITION_INTENT_CONTRACT_AUDIT
    hold_exit_shadow_output_path: Path = DEFAULT_HOLD_EXIT_SHADOW_ARTIFACT
    hold_exit_shadow_event_log_path: Path = DEFAULT_HOLD_EXIT_SHADOW_EVENTS
    artifact_max_age_seconds: float = 180.0

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_managed_position_registry(
    *,
    config: TrackBManagedPositionRegistryConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    position_truth = _read_json(config.resolve(config.position_truth_path))
    open_order_truth = _read_json(config.resolve(config.open_order_truth_path))
    managed_order_registry = _read_json(config.resolve(config.managed_order_registry_path))
    reconciliation = _read_json(config.resolve(config.reconciliation_path))
    positions_snapshot = _read_json(config.resolve(DEFAULT_BROKER_POSITIONS_SNAPSHOT))
    open_orders_snapshot = _read_json(config.resolve(DEFAULT_BROKER_OPEN_ORDERS_SNAPSHOT))
    live_position_status = _read_json(config.resolve(config.live_position_status_path))
    lifecycle_reports = _load_lifecycle_reports(config.resolve(config.lifecycle_root))
    manifests = _load_manifests(config.resolve(config.manifest_root))
    terminal_records = load_live_trade_registry_records(repo_root=config.repo_root)

    fresh_broker_positions = _fresh_complete_broker_positions(positions_snapshot)
    broker_positions = (
        fresh_broker_positions
        if fresh_broker_positions is not None
        else [_normalize_contract_row(row) for row in _reconciliation_broker_positions(reconciliation)]
    )
    fresh_broker_open_orders = _fresh_complete_broker_open_orders(open_orders_snapshot)
    broker_open_orders = (
        fresh_broker_open_orders
        if fresh_broker_open_orders is not None
        else _list(reconciliation.get("track_b_broker_open_orders"))
    )
    lifecycle_positions = [
        _normalize_contract_row(item)
        for item in _reconciliation_lifecycle_positions(reconciliation)
        if _lifecycle_position_registry_eligible(item)
    ]
    lifecycle_positions = _merge_resolved_lifecycle_positions(
        lifecycle_positions=lifecycle_positions,
        resolved_lifecycle_positions=_registry_lifecycle_candidates_for_broker_positions(
            broker_positions=broker_positions,
            lifecycle_positions=lifecycle_positions,
            terminal_records=terminal_records,
        ),
    )
    pre_restart_exposure_resolution = resolve_pre_restart_exposure_reconciliation(
        config=PreRestartExposureResolverConfig(repo_root=config.repo_root),
        broker_positions=broker_positions,
        lifecycle_positions=lifecycle_positions,
        broker_open_orders=broker_open_orders,
        lifecycle_reports=lifecycle_reports,
    )
    owner_resolution = _current_owner_resolution_for_projection(
        pre_restart_exposure_resolution=pre_restart_exposure_resolution,
        reconciliation=reconciliation,
    )
    lifecycle_positions, owner_superseded_lifecycle_positions = apply_current_exposure_owner_lifecycle_overlay(
        lifecycle_positions=lifecycle_positions,
        owner_resolution=owner_resolution,
    )
    lifecycle_positions = _merge_resolved_lifecycle_positions(
        lifecycle_positions=lifecycle_positions,
        resolved_lifecycle_positions=_list(owner_resolution.get("resolved_lifecycle_positions"))
        or _list(pre_restart_exposure_resolution.get("resolved_lifecycle_positions")),
    )
    lifecycle_positions_tuple, superseded_lifecycle_positions = filter_terminal_superseded_current_rows(
        rows=lifecycle_positions,
        records=terminal_records,
        broker_positions=broker_positions,
        broker_open_orders=broker_open_orders,
    )
    lifecycle_positions = [dict(item) for item in lifecycle_positions_tuple if isinstance(item, Mapping)]
    unresolved_ownership = _list(reconciliation.get("unresolved_submit_intent_ownership_records"))
    open_order_states = _list(open_order_truth.get("order_states"))
    managed_order_states = _list(managed_order_registry.get("managed_orders"))
    review_scope = _review_required_position_scope(
        reconciliation=reconciliation,
        live_position_status=live_position_status,
        lifecycle_reports=lifecycle_reports,
        position_truth=position_truth,
        broker_positions=broker_positions,
        lifecycle_positions=lifecycle_positions,
        open_order_states=open_order_states,
        managed_order_states=managed_order_states,
        unresolved_ownership=unresolved_ownership,
    )
    review_positions = review_scope["current_scope"]
    historical_review_positions = review_scope["historical"]
    source_stale = _source_stale(
        now=actual_now,
        config=config,
        position_truth=position_truth,
        open_order_truth=open_order_truth,
        managed_order_registry=managed_order_registry,
        reconciliation=reconciliation,
    )
    if review_positions and _current_scope_flat_authority_clean(
        reconciliation=reconciliation,
        open_order_truth=open_order_truth,
    ) and _managed_order_registry_flat(managed_order_registry):
        historical_review_positions = [
            *historical_review_positions,
            *[_demote_review_position_to_historical_flat_diagnostic(row) for row in review_positions],
        ]
        review_positions = []
    managed_positions = _managed_positions(
        broker_positions=broker_positions,
        lifecycle_positions=lifecycle_positions,
        review_positions=review_positions,
        open_order_states=open_order_states,
        managed_order_states=managed_order_states,
        lifecycle_reports=lifecycle_reports,
        manifests=manifests,
        market_data_root=config.resolve(config.market_data_root),
        terminal_records=terminal_records,
        source_stale=source_stale,
    )
    managed_positions, projection_authority_diagnostics = _apply_current_owner_projection_overlay(
        managed_positions=managed_positions,
        owner_resolution=owner_resolution or pre_restart_exposure_resolution,
        broker_positions=broker_positions,
        broker_open_orders=broker_open_orders,
        open_order_states=open_order_states,
        managed_order_states=managed_order_states,
        lifecycle_reports=lifecycle_reports,
        manifests=manifests,
        market_data_root=config.resolve(config.market_data_root),
        terminal_records=terminal_records,
        source_stale=source_stale,
    )
    managed_positions, projection_authority_diagnostics = _enforce_owned_exposure_projection_invariant(
        managed_positions=managed_positions,
        projection_authority_diagnostics=projection_authority_diagnostics,
        owner_resolution=owner_resolution or pre_restart_exposure_resolution,
        broker_positions=broker_positions,
        broker_open_orders=broker_open_orders,
        open_order_states=open_order_states,
        managed_order_states=managed_order_states,
        lifecycle_reports=lifecycle_reports,
        manifests=manifests,
        market_data_root=config.resolve(config.market_data_root),
        terminal_records=terminal_records,
        source_stale=source_stale,
    )
    managed_positions = _repair_owner_confirmed_timebox_due_positions(managed_positions)
    classification = _overall_classification(
        managed_positions=managed_positions,
        broker_positions=broker_positions,
        lifecycle_positions=lifecycle_positions,
        review_positions=review_positions,
        source_stale=source_stale,
    )
    if projection_authority_diagnostics.get("classification") == PROJECTION_AUTHORITY_DIVERGENCE:
        classification = PROJECTION_AUTHORITY_DIVERGENCE
    from mgc_v05l.execution_core.track_b_managed_position_hold_exit_shadow import (
        ManagedPositionHoldExitShadowConfig,
        decorate_managed_positions_with_hold_exit_shadow,
    )

    hold_exit_shadow_config = ManagedPositionHoldExitShadowConfig(
        repo_root=config.repo_root,
        managed_position_registry_path=config.output_path,
        position_intent_audit_path=config.position_intent_audit_path,
        latest_output_path=config.hold_exit_shadow_output_path,
        events_path=config.hold_exit_shadow_event_log_path,
    )
    managed_positions = decorate_managed_positions_with_hold_exit_shadow(
        managed_positions=managed_positions,
        config=hold_exit_shadow_config,
        now=actual_now,
    )
    payload = {
        "schema_version": "track_b_managed_position_registry_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": reconciliation.get("live_money_eligible") is True,
        "classification": classification,
        "managed_positions": managed_positions,
        "broker_positions": broker_positions,
        "lifecycle_open_positions": lifecycle_positions,
        "review_required_positions": review_positions,
        "historical_review_positions": historical_review_positions,
        "superseded_lifecycle_projections": [
            *list(superseded_lifecycle_positions),
            *owner_superseded_lifecycle_positions,
        ],
        "projection_authority_diagnostics": projection_authority_diagnostics,
        "unresolved_submit_ownership": unresolved_ownership,
        "pre_restart_exposure_resolution": pre_restart_exposure_resolution,
        "source_freshness": source_stale,
        "position_truth": _authority_summary(position_truth, config.resolve(config.position_truth_path)),
        "open_order_truth": _authority_summary(open_order_truth, config.resolve(config.open_order_truth_path)),
        "managed_order_registry": _authority_summary(
            managed_order_registry,
            config.resolve(config.managed_order_registry_path),
        ),
        "reconciliation": {
            "classification": reconciliation.get("classification"),
            "broker_reconciled": reconciliation.get("broker_reconciled"),
            "review_required_count": reconciliation.get("review_required_count"),
            "unresolved_submit_intent_ownership_count": reconciliation.get(
                "unresolved_submit_intent_ownership_count"
            ),
            "generated_at": reconciliation.get("generated_at"),
            "artifact_path": str(config.resolve(config.reconciliation_path)),
        },
        "summary": {
            "classification": classification,
            "managed_position_count": len(managed_positions),
            "attention_required_count": sum(1 for item in managed_positions if item.get("attention_required") is True),
            "exit_due_count": sum(1 for item in managed_positions if item.get("exit_due") is True),
            "close_working_count": sum(1 for item in managed_positions if item.get("close_order_state")),
            "suspicious_managed_order_count": sum(
                1
                for item in managed_positions
                if str(_mapping(item.get("managed_order_state")).get("classification") or "") == "CLOSE_ORDER_SUSPICIOUS"
            ),
            "duplicate_close_risk_count": sum(
                1
                for item in managed_positions
                if str(_mapping(item.get("managed_order_state")).get("classification") or "")
                == "DUPLICATE_CLOSE_ORDER_BLOCKED"
            ),
            "broker_position_count": len(broker_positions),
            "lifecycle_position_count": len(lifecycle_positions),
            "review_required_count": len(review_positions),
            "historical_review_position_count": len(historical_review_positions),
            "pre_restart_resolved_managed_exposure_count": pre_restart_exposure_resolution.get(
                "resolved_managed_exposure_count"
            ),
            "pre_restart_review_required_exposure_count": pre_restart_exposure_resolution.get(
                "review_required_exposure_count"
            ),
        },
        "event_state": _event_state(classification=classification, managed_positions=managed_positions),
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "event_log": str(config.resolve(config.event_log_path)),
            "dashboard_projection": None
            if config.dashboard_projection_path is None
            else str(config.resolve(config.dashboard_projection_path)),
            "position_truth": str(config.resolve(config.position_truth_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "live_position_status": str(config.resolve(config.live_position_status_path)),
            "lifecycle_root": str(config.resolve(config.lifecycle_root)),
            "manifest_root": str(config.resolve(config.manifest_root)),
            "market_data_root": str(config.resolve(config.market_data_root)),
            "position_intent_audit": str(config.resolve(config.position_intent_audit_path)),
            "hold_exit_shadow": str(config.resolve(config.hold_exit_shadow_output_path)),
        },
    }
    return payload


def write_track_b_managed_position_registry(
    *,
    config: TrackBManagedPositionRegistryConfig,
    payload: Mapping[str, Any],
    now: datetime | None = None,
) -> tuple[Path, list[dict[str, Any]]]:
    output_path = config.resolve(config.output_path)
    event_log_path = config.resolve(config.event_log_path)
    previous = _read_json(output_path)
    events = build_managed_position_events(previous=previous, current=payload, now=now)
    _write_json_atomic(output_path, dict(payload))
    from mgc_v05l.execution_core.track_b_managed_position_hold_exit_shadow import (
        ManagedPositionHoldExitShadowConfig,
        build_managed_position_hold_exit_shadow,
        write_managed_position_hold_exit_shadow,
    )

    hold_exit_shadow_config = ManagedPositionHoldExitShadowConfig(
        repo_root=config.repo_root,
        managed_position_registry_path=config.output_path,
        position_intent_audit_path=config.position_intent_audit_path,
        latest_output_path=config.hold_exit_shadow_output_path,
        events_path=config.hold_exit_shadow_event_log_path,
    )
    hold_exit_shadow_payload = build_managed_position_hold_exit_shadow(
        config=hold_exit_shadow_config,
        managed_position_registry=payload,
        now=now,
    )
    write_managed_position_hold_exit_shadow(config=hold_exit_shadow_config, payload=hold_exit_shadow_payload)
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_managed_position_projection(authority_payload=payload, authority_path=output_path),
        )
    if events:
        for event in events:
            append_bounded_jsonl(event_log_path, event)
    try_record_strategy_funnel_events(
        events_from_managed_position_registry(payload),
        repo_root=config.repo_root,
    )
    return output_path, events


def build_dashboard_managed_position_projection(*, authority_payload: Mapping[str, Any], authority_path: Path) -> dict[str, Any]:
    return {
        **dict(authority_payload),
        "schema_version": "track_b_managed_position_dashboard_projection_v1",
        **build_projection_metadata(source_authority_path=authority_path),
    }


def build_managed_position_events(
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
            "schema_version": "track_b_managed_position_event_v1",
            "event_type": "MANAGED_POSITION_REGISTRY_CHANGED",
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


def _managed_positions(
    *,
    broker_positions: list[dict[str, Any]],
    lifecycle_positions: list[dict[str, Any]],
    review_positions: list[dict[str, Any]],
    open_order_states: list[dict[str, Any]],
    managed_order_states: list[dict[str, Any]],
    lifecycle_reports: list[dict[str, Any]],
    manifests: list[dict[str, Any]],
    market_data_root: Path,
    terminal_records: tuple[Any, ...],
    source_stale: Mapping[str, Any],
) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    keys = sorted(
        {
            _position_key(item)
            for item in [*broker_positions, *lifecycle_positions, *review_positions]
            if _position_key(item)
        }
    )
    for key in keys:
        broker = _first_match(broker_positions, key)
        lifecycle = _best_lifecycle_match(lifecycle_positions, key, broker)
        review = _first_match(review_positions, key)
        lifecycle_id = str((lifecycle or review or {}).get("lifecycle_id") or "")
        lifecycle_report = _lifecycle_report(lifecycle_id, lifecycle_reports)
        manifest = _manifest_for_position(
            lifecycle=lifecycle or review or {},
            lifecycle_report=lifecycle_report,
            manifests=manifests,
        )
        lifecycle = _recover_lifecycle_metadata(
            lifecycle=lifecycle,
            lifecycle_report=lifecycle_report,
            manifest=manifest,
            broker=broker,
            terminal_records=terminal_records,
        )
        close_order_state = _close_order_state(key=key, open_order_states=open_order_states)
        managed_order_state = _managed_order_state(key=key, managed_order_states=managed_order_states)
        effective_close_order_state = close_order_state or managed_order_state
        classification = _position_classification(
            broker=broker,
            lifecycle=lifecycle,
            review=review,
            lifecycle_report=lifecycle_report,
            manifest=manifest,
            close_order_state=effective_close_order_state,
            source_stale=source_stale,
        )
        bars_since_entry = _bars_since_entry(
            lifecycle=lifecycle,
            lifecycle_report=lifecycle_report,
            market_data_root=market_data_root,
            terminal_records=terminal_records,
        )
        due_classification = _exit_due_classification(
            classification=classification,
            broker=broker,
            lifecycle=lifecycle,
            review=review,
            lifecycle_report=lifecycle_report,
            close_order_state=effective_close_order_state,
            source_stale=source_stale,
        )
        exit_due = _exit_due(
            lifecycle=lifecycle,
            lifecycle_report=lifecycle_report,
            classification=due_classification,
            bars_since_entry=bars_since_entry,
        )
        effective_classification = (
            OPEN_MANAGED_EXIT_DUE
            if exit_due and due_classification == OPEN_MANAGED_MATCHED
            else classification
        )
        lifecycle_units = _list((lifecycle or {}).get("lifecycle_units"))
        aggregate_qty = (lifecycle or {}).get("aggregate_qty")
        signed_lifecycle_qty = _signed_lifecycle_quantity(lifecycle)
        signed_broker_qty = _decimal((broker or {}).get("quantity"))
        duplicate_excess_qty = _duplicate_excess_close_quantity(lifecycle, signed_broker_qty)
        broker_qty_match = (
            signed_lifecycle_qty is not None
            and signed_broker_qty is not None
            and signed_lifecycle_qty == signed_broker_qty
        )
        position = {
            "classification": effective_classification,
            "symbol": _symbol(broker or lifecycle or review),
            "contract_key": (lifecycle or review or {}).get("contract_key") or _contract_key_from_broker(broker or {}),
            "local_symbol": (lifecycle or review or broker or {}).get("local_symbol"),
            "con_id": (lifecycle or review or broker or {}).get("con_id"),
            "side": (lifecycle or review or {}).get("side") or _side_from_broker(broker or {}),
            "quantity": (lifecycle or review or broker or {}).get("quantity"),
            "aggregate_qty": aggregate_qty,
            "signed_lifecycle_qty": _decimal_display(signed_lifecycle_qty),
            "signed_broker_qty": _decimal_display(signed_broker_qty),
            "broker_qty_match": broker_qty_match if broker and lifecycle else None,
            "unit_count": (lifecycle or {}).get("unit_count") or (lifecycle or {}).get("lifecycle_unit_count") or len(lifecycle_units) or None,
            "lifecycle_unit_count": (lifecycle or {}).get("lifecycle_unit_count") or len(lifecycle_units) or None,
            "lifecycle_units": lifecycle_units,
            "lifecycle_ids": (lifecycle or {}).get("lifecycle_ids") or [
                str(item.get("lifecycle_id") or "") for item in lifecycle_units if item.get("lifecycle_id")
            ],
            "entry_intent_ids": [
                str(item.get("entry_intent_id") or "") for item in lifecycle_units if item.get("entry_intent_id")
            ],
            "entry_order_ids": (lifecycle or {}).get("entry_order_ids") or [
                str(item.get("entry_order_id") or "") for item in lifecycle_units if item.get("entry_order_id")
            ],
            "entry_perm_ids": (lifecycle or {}).get("entry_perm_ids") or [
                str(item.get("entry_perm_id") or "") for item in lifecycle_units if item.get("entry_perm_id")
            ],
            "duplicate_same_lane_exposure": (lifecycle or {}).get("duplicate_same_lane_exposure") is True,
            "duplicate_entry_count": (lifecycle or {}).get("duplicate_entry_count"),
            "duplicate_excess_qty": _decimal_display(duplicate_excess_qty),
            "accepted_managed_qty": (lifecycle or {}).get("accepted_managed_qty"),
            "duplicate_entry_exec_ids": (lifecycle or {}).get("duplicate_entry_exec_ids") or [],
            "duplicate_entry_perm_ids": (lifecycle or {}).get("duplicate_entry_perm_ids") or [],
            "pyramiding_allowed": (lifecycle or {}).get("pyramiding_allowed") is True,
            "pyramiding_policy": (lifecycle or {}).get("pyramiding_policy"),
            "working_close_qty": _working_close_qty(effective_close_order_state),
            "unmanaged_qty": None if broker_qty_match else _decimal_display(signed_broker_qty),
            "lane_id": (lifecycle or review or manifest or {}).get("lane_id"),
            "strategy_id": (lifecycle or review or lifecycle_report or manifest or {}).get("strategy_id"),
            "trade_id": (lifecycle or review or lifecycle_report or manifest or {}).get("trade_id"),
            "lifecycle_id": lifecycle_id or None,
            "manifest_id": (manifest or {}).get("entry_intent_id"),
            "manifest_path": _manifest_path(manifest),
            "entry_time": (lifecycle or review or lifecycle_report or {}).get("entry_timestamp")
            or _isoformat_or_none(
                _entry_time(lifecycle=lifecycle, lifecycle_report=lifecycle_report, terminal_records=terminal_records)
            ),
            "entry_price": (lifecycle or review or {}).get("avg_entry_price")
            or _mapping(lifecycle_report.get("entry_fill")).get("price"),
            "managed_exit_policy_id": _managed_exit_policy_id(lifecycle, review, lifecycle_report, manifest),
            "bars_since_entry": bars_since_entry,
            "exit_due": bool(exit_due),
            "exit_due_state": _exit_due_state(exit_due),
            "exit_due_evidence_stale": bool(exit_due and source_stale.get("diagnostic_stale") is True),
            "freshness_state": _position_freshness_state(source_stale=source_stale),
            "apply_authority_degraded": bool(exit_due and source_stale.get("stale") is True),
            "stale_dependency_sources": list(source_stale.get("stale_sources") or []),
            "required_close_action": _required_close_action(
                side=(lifecycle or review or {}).get("side") or _side_from_broker(broker or {}),
                signed_broker_qty=signed_broker_qty,
            )
            if exit_due
            else None,
            "required_close_quantity": _decimal_display(abs(signed_broker_qty))
            if exit_due and signed_broker_qty is not None and duplicate_excess_qty is None
            else _decimal_display(duplicate_excess_qty)
            if exit_due and duplicate_excess_qty is not None
            else None,
            "close_order_state": effective_close_order_state,
            "managed_order_state": managed_order_state,
            "reconciliation_status": _reconciliation_status(broker=broker, lifecycle=lifecycle, review=review),
            "attention_required": effective_classification
            in {
                BROKER_BACKED_ADOPTION_REQUIRED,
                MANAGED_POSITION_METADATA_INCOMPLETE,
                LIFECYCLE_WITHOUT_BROKER,
                REVIEW_REQUIRED,
                STALE_MANAGED_POSITION_EVIDENCE,
            },
            "recommended_operator_action": _recommended_action(
                classification=effective_classification
            ),
            "broker_position": broker,
            "lifecycle_position": lifecycle,
            "review_required_position": review,
        }
        positions.append(position)
    return positions


def _apply_current_owner_projection_overlay(
    *,
    managed_positions: list[dict[str, Any]],
    owner_resolution: Mapping[str, Any],
    broker_positions: list[dict[str, Any]],
    broker_open_orders: list[dict[str, Any]],
    open_order_states: list[dict[str, Any]],
    managed_order_states: list[dict[str, Any]],
    lifecycle_reports: list[dict[str, Any]],
    manifests: list[dict[str, Any]],
    market_data_root: Path,
    terminal_records: tuple[Any, ...],
    source_stale: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Keep managed-position authority aligned with the shared owner resolver."""

    positions = [dict(item) for item in managed_positions]
    diagnostics: dict[str, Any] = {
        "classification": PROJECTION_AUTHORITY_COHERENT,
        "authority_source": "CURRENT_EXPOSURE_OWNER_RESOLVER",
        "repaired_missing_owner_count": 0,
        "divergence_count": 0,
        "repairs": [],
        "divergences": [],
    }
    owned_exposures = [
        dict(item)
        for item in owner_resolution.get("owned_exposures") or []
        if isinstance(item, Mapping)
    ]
    if not owned_exposures:
        return positions, diagnostics

    by_key = {_position_key(item): idx for idx, item in enumerate(positions) if _position_key(item)}
    for exposure in owned_exposures:
        lifecycle = _mapping(exposure.get("lifecycle_position"))
        broker = _mapping(exposure.get("canonical_broker_position")) or _mapping(exposure.get("broker_position"))
        key = _position_key(broker) or _position_key(lifecycle)
        owner_lifecycle_id = str(exposure.get("lifecycle_id") or lifecycle.get("lifecycle_id") or "").strip()
        owner_trade_id = str(exposure.get("trade_id") or lifecycle.get("trade_id") or "").strip()
        if not key or not owner_lifecycle_id:
            diagnostics["classification"] = PROJECTION_AUTHORITY_DIVERGENCE
            diagnostics["divergence_count"] += 1
            diagnostics["divergences"].append(
                {
                    "classification": PROJECTION_AUTHORITY_DIVERGENCE,
                    "reason": "Owned exposure from resolver is missing contract key or lifecycle identity.",
                    "position_key": key,
                    "owner_trade_id": owner_trade_id,
                    "owner_lifecycle_id": owner_lifecycle_id,
                }
            )
            continue
        lifecycle = {
            **lifecycle,
            "trade_id": lifecycle.get("trade_id") or owner_trade_id,
            "lifecycle_id": lifecycle.get("lifecycle_id") or owner_lifecycle_id,
        }
        terminal_truth = resolve_terminal_registry_truth(
            records=terminal_records,
            identity=lifecycle or exposure,
            broker_positions=broker_positions,
            broker_open_orders=broker_open_orders,
        )
        if terminal_truth.terminal_closed_flat:
            diagnostics["repairs"].append(
                {
                    "classification": "CURRENT_OWNER_TERMINAL_CLOSED_SUPPRESSED",
                    "reason": "Terminal registry truth superseded stale current-owner lifecycle projection.",
                    "position_key": key,
                    "owner_trade_id": owner_trade_id,
                    "owner_lifecycle_id": owner_lifecycle_id,
                    "terminal_registry_truth": terminal_truth.to_dict(),
                }
            )
            continue

        existing_index = by_key.get(key)
        existing = positions[existing_index] if existing_index is not None else None
        existing_lifecycle_id = str((existing or {}).get("lifecycle_id") or "").strip()
        if existing and existing_lifecycle_id == owner_lifecycle_id:
            existing["projection_authority_owner_confirmed"] = True
            existing["projection_authority_source"] = "CURRENT_EXPOSURE_OWNER_RESOLVER"
            continue

        repaired = _managed_positions(
            broker_positions=[broker] if broker else [],
            lifecycle_positions=[lifecycle] if lifecycle else [],
            review_positions=[],
            open_order_states=open_order_states,
            managed_order_states=managed_order_states,
            lifecycle_reports=lifecycle_reports,
            manifests=manifests,
            market_data_root=market_data_root,
            terminal_records=terminal_records,
            source_stale=source_stale,
        )
        if not repaired:
            diagnostics["classification"] = PROJECTION_AUTHORITY_DIVERGENCE
            diagnostics["divergence_count"] += 1
            diagnostics["divergences"].append(
                {
                    "classification": PROJECTION_AUTHORITY_DIVERGENCE,
                    "reason": "Managed-position projection could not be built from resolver-proven owner.",
                    "position_key": key,
                    "owner_trade_id": owner_trade_id,
                    "owner_lifecycle_id": owner_lifecycle_id,
                }
            )
            continue
        owner_position = {
            **repaired[0],
            "projection_authority_owner_confirmed": True,
            "projection_authority_source": "CURRENT_EXPOSURE_OWNER_RESOLVER",
        }
        if existing_index is None:
            positions.append(owner_position)
            by_key[key] = len(positions) - 1
            diagnostics["repaired_missing_owner_count"] += 1
            diagnostics["repairs"].append(
                {
                    "classification": "CURRENT_OWNER_PROJECTION_REPAIRED",
                    "reason": "Resolver-proven current owner was missing from managed-position projection.",
                    "position_key": key,
                    "owner_trade_id": owner_trade_id,
                    "owner_lifecycle_id": owner_lifecycle_id,
                }
            )
            continue
        if not existing_lifecycle_id:
            positions[existing_index] = owner_position
            diagnostics["repaired_missing_owner_count"] += 1
            diagnostics["repairs"].append(
                {
                    "classification": "CURRENT_OWNER_PROJECTION_REPAIRED",
                    "reason": "Broker exposure projection lacked canonical lifecycle owner.",
                    "position_key": key,
                    "owner_trade_id": owner_trade_id,
                    "owner_lifecycle_id": owner_lifecycle_id,
                }
            )
            continue
        if _owner_exposure_supersedes_existing_projection(exposure):
            positions[existing_index] = {
                **owner_position,
                "superseded_projection_lifecycle_id": existing_lifecycle_id,
                "superseded_projection_trade_id": existing.get("trade_id"),
            }
            diagnostics["repaired_missing_owner_count"] += 1
            diagnostics["repairs"].append(
                {
                    "classification": "CURRENT_OWNER_PROJECTION_REPAIRED",
                    "reason": "Newest exact broker-backed owner superseded older same-contract managed-position projection.",
                    "position_key": key,
                    "owner_trade_id": owner_trade_id,
                    "owner_lifecycle_id": owner_lifecycle_id,
                    "superseded_trade_id": existing.get("trade_id"),
                    "superseded_lifecycle_id": existing_lifecycle_id,
                }
            )
            continue
        divergent = {
            **dict(existing),
            "classification": PROJECTION_AUTHORITY_DIVERGENCE,
            "attention_required": True,
            "projection_authority_divergence": {
                "classification": PROJECTION_AUTHORITY_DIVERGENCE,
                "reason": "Managed-position projection disagrees with shared current exposure owner resolver.",
                "position_key": key,
                "projected_trade_id": existing.get("trade_id"),
                "projected_lifecycle_id": existing_lifecycle_id,
                "owner_trade_id": owner_trade_id,
                "owner_lifecycle_id": owner_lifecycle_id,
            },
        }
        positions[existing_index] = divergent
        diagnostics["classification"] = PROJECTION_AUTHORITY_DIVERGENCE
        diagnostics["divergence_count"] += 1
        diagnostics["divergences"].append(divergent["projection_authority_divergence"])
    return positions, diagnostics


def _enforce_owned_exposure_projection_invariant(
    *,
    managed_positions: list[dict[str, Any]],
    projection_authority_diagnostics: Mapping[str, Any],
    owner_resolution: Mapping[str, Any],
    broker_positions: list[dict[str, Any]],
    broker_open_orders: list[dict[str, Any]],
    open_order_states: list[dict[str, Any]],
    managed_order_states: list[dict[str, Any]],
    lifecycle_reports: list[dict[str, Any]],
    manifests: list[dict[str, Any]],
    market_data_root: Path,
    terminal_records: tuple[Any, ...],
    source_stale: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    positions = [dict(item) for item in managed_positions]
    diagnostics = {
        **dict(projection_authority_diagnostics),
        "classification": projection_authority_diagnostics.get("classification") or PROJECTION_AUTHORITY_COHERENT,
        "divergence_count": int(projection_authority_diagnostics.get("divergence_count") or 0),
        "divergences": list(projection_authority_diagnostics.get("divergences") or []),
        "repairs": list(projection_authority_diagnostics.get("repairs") or []),
    }
    owned_exposures = _owned_current_exposures(owner_resolution)
    expected_count = _int_or_none(owner_resolution.get("owned_exposure_count")) or len(owned_exposures)
    if expected_count <= 0:
        return positions, diagnostics
    if not owned_exposures:
        diagnostics["classification"] = PROJECTION_AUTHORITY_DIVERGENCE
        diagnostics["divergence_count"] += 1
        diagnostics["divergences"].append(
            {
                "classification": PROJECTION_AUTHORITY_DIVERGENCE,
                "reason": "Owner resolver reported owned exposure count but did not publish owned exposure rows.",
                "owned_exposure_count": expected_count,
            }
        )
        return positions, diagnostics

    by_key = {_position_key(item): idx for idx, item in enumerate(positions) if _position_key(item)}
    for exposure in owned_exposures:
        broker = _mapping(exposure.get("canonical_broker_position")) or _mapping(exposure.get("broker_position"))
        if not _broker_position_nonzero(broker):
            continue
        lifecycle = _mapping(exposure.get("lifecycle_position"))
        key = _position_key(broker) or _position_key(lifecycle)
        owner_lifecycle_id = str(exposure.get("lifecycle_id") or lifecycle.get("lifecycle_id") or "").strip()
        owner_trade_id = str(exposure.get("trade_id") or lifecycle.get("trade_id") or "").strip()
        terminal_truth = resolve_terminal_registry_truth(
            records=terminal_records,
            identity=lifecycle or exposure,
            broker_positions=broker_positions,
            broker_open_orders=broker_open_orders,
        )
        if terminal_truth.terminal_closed_flat:
            diagnostics["repairs"].append(
                {
                    "classification": "OWNED_EXPOSURE_TERMINAL_CLOSED_SUPPRESSED",
                    "reason": "Invariant skipped stale owned exposure after terminal registry flat proof.",
                    "position_key": key,
                    "owner_trade_id": owner_trade_id,
                    "owner_lifecycle_id": owner_lifecycle_id,
                    "terminal_registry_truth": terminal_truth.to_dict(),
                }
            )
            continue
        existing = positions[by_key[key]] if key and key in by_key else None
        if existing and str(existing.get("lifecycle_id") or "").strip() == owner_lifecycle_id:
            continue
        repaired = _managed_positions(
            broker_positions=[broker] if broker else [],
            lifecycle_positions=[lifecycle] if lifecycle else [],
            review_positions=[],
            open_order_states=open_order_states,
            managed_order_states=managed_order_states,
            lifecycle_reports=lifecycle_reports,
            manifests=manifests,
            market_data_root=market_data_root,
            terminal_records=terminal_records,
            source_stale=source_stale,
        )
        if repaired:
            repaired_position = {
                **repaired[0],
                "projection_authority_owner_confirmed": True,
                "projection_authority_source": "CURRENT_EXPOSURE_OWNER_RESOLVER",
            }
            if existing and key:
                positions[by_key[key]] = repaired_position
            else:
                positions.append(repaired_position)
                if key:
                    by_key[key] = len(positions) - 1
            diagnostics["repairs"].append(
                {
                    "classification": "CURRENT_OWNER_PROJECTION_REPAIRED",
                    "reason": "Invariant repaired a resolver-proven owned broker exposure missing from managed-position authority.",
                    "position_key": key,
                    "owner_trade_id": owner_trade_id,
                    "owner_lifecycle_id": owner_lifecycle_id,
                }
            )
            continue
        divergence = {
            "classification": PROJECTION_AUTHORITY_DIVERGENCE,
            "reason": "Reconciled owned broker exposure was missing from managed-position authority.",
            "position_key": key,
            "owner_trade_id": owner_trade_id,
            "owner_lifecycle_id": owner_lifecycle_id,
        }
        diagnostics["classification"] = PROJECTION_AUTHORITY_DIVERGENCE
        diagnostics["divergence_count"] += 1
        diagnostics["divergences"].append(divergence)
        positions.append(
            {
                "classification": PROJECTION_AUTHORITY_DIVERGENCE,
                "attention_required": True,
                "projection_authority_divergence": divergence,
                "broker_position": broker,
                "lifecycle_position": lifecycle,
                "trade_id": owner_trade_id or None,
                "lifecycle_id": owner_lifecycle_id or None,
                "symbol": _symbol(broker or lifecycle),
                "local_symbol": (broker or lifecycle).get("local_symbol"),
                "con_id": (broker or lifecycle).get("con_id"),
                "side": lifecycle.get("side") or _side_from_broker(broker),
                "quantity": lifecycle.get("quantity") or broker.get("quantity"),
            }
        )
    return positions, diagnostics


def _owned_current_exposures(owner_resolution: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(item)
        for item in owner_resolution.get("owned_exposures") or []
        if isinstance(item, Mapping)
    ]


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


def _fresh_complete_broker_positions(snapshot: Mapping[str, Any]) -> list[dict[str, Any]] | None:
    if snapshot.get("positions_complete") is not True and snapshot.get("ok") is not True:
        return None
    positions: list[dict[str, Any]] = []
    for row in _list(snapshot.get("positions")):
        account_id = str(row.get("account_id") or row.get("account") or snapshot.get("account") or "").strip()
        if account_id and account_id != "DUM882026":
            continue
        normalized = _normalize_contract_row({**row, "account_id": account_id or row.get("account_id")})
        if not _validated_track_b_futures_position(normalized):
            continue
        qty = _decimal(normalized.get("quantity") or normalized.get("position") or normalized.get("signed_qty"))
        if qty is None or qty == 0:
            continue
        positions.append(normalized)
    return positions


def _fresh_complete_broker_open_orders(snapshot: Mapping[str, Any]) -> list[dict[str, Any]] | None:
    if snapshot.get("open_orders_complete") is not True and snapshot.get("ok") is not True:
        return None
    rows: list[dict[str, Any]] = []
    for row in _list(snapshot.get("open_orders")):
        account_id = str(row.get("account_id") or row.get("account") or snapshot.get("account") or "").strip()
        if account_id and account_id != "DUM882026":
            continue
        contract = _mapping(row.get("contract"))
        normalized_contract = _normalize_contract_row({**contract, "account_id": account_id or contract.get("account_id")})
        normalized = dict(row)
        normalized["contract"] = normalized_contract
        if normalized_contract.get("contract_identity", {}).get("resolved") is True:
            normalized.setdefault("symbol", normalized_contract.get("symbol"))
            normalized.setdefault("track_b_root", normalized_contract.get("track_b_root"))
            normalized.setdefault("instrument_family", normalized_contract.get("instrument_family"))
            normalized.setdefault("local_symbol", normalized_contract.get("local_symbol"))
            normalized.setdefault("con_id", normalized_contract.get("con_id"))
            normalized.setdefault("contract_key", normalized_contract.get("contract_key"))
        rows.append(normalized)
    return rows


def _validated_track_b_futures_position(row: Mapping[str, Any]) -> bool:
    sec_type = str(row.get("security_type") or row.get("secType") or "").strip().upper()
    if sec_type and sec_type != "FUT":
        return False
    identity = _mapping(row.get("contract_identity"))
    symbol = str(
        row.get("symbol")
        or row.get("track_b_root")
        or row.get("instrument_family")
        or identity.get("symbol")
        or ""
    ).strip().upper()
    return identity.get("resolved") is True and symbol in {
        "MGC",
        "GC",
        "ES",
        "NQ",
        "MNQ",
        "MES",
        "ZT",
        "ZF",
        "ZN",
        "ZB",
    }


def _registry_lifecycle_candidates_for_broker_positions(
    *,
    broker_positions: Sequence[Mapping[str, Any]],
    lifecycle_positions: Sequence[Mapping[str, Any]],
    terminal_records: tuple[Any, ...],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for broker in broker_positions:
        best_lifecycle = _best_lifecycle_match(
            [dict(item) for item in lifecycle_positions if isinstance(item, Mapping)],
            _position_key(broker),
            broker,
        )
        if best_lifecycle and _signed_lifecycle_quantity(best_lifecycle) == _decimal(broker.get("quantity")):
            continue
        candidate = _registry_lifecycle_candidate_for_broker_position(
            broker_position=broker,
            terminal_records=terminal_records,
        )
        if candidate:
            candidates.append(candidate)
    return candidates


def _registry_lifecycle_candidate_for_broker_position(
    *,
    broker_position: Mapping[str, Any],
    terminal_records: tuple[Any, ...],
) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    for record in terminal_records:
        state = str(getattr(getattr(record, "current_state", None), "value", getattr(record, "current_state", "")))
        if state == "CLOSED_FLAT":
            continue
        for event in getattr(record, "event_chain", ()) or ():
            if _event_type_value(getattr(event, "event_type", "")) != "ENTRY_FILL_BROKER_BACKED":
                continue
            if not _event_matches_broker_position(event, broker_position=broker_position):
                continue
            lane_id = str(getattr(event, "lane_id", None) or getattr(event, "thesis_strategy_id", None) or "").strip()
            signed_qty = _decimal(broker_position.get("quantity")) or Decimal("0")
            candidates.append(
                _normalize_contract_row(
                    {
                        "account_id": getattr(event, "account_id", None) or broker_position.get("account_id"),
                        "instrument_family": getattr(event, "symbol", None) or _symbol(broker_position),
                        "symbol": getattr(event, "symbol", None) or _symbol(broker_position),
                        "local_symbol": getattr(event, "local_symbol", None) or broker_position.get("local_symbol"),
                        "con_id": getattr(event, "con_id", None) or broker_position.get("con_id"),
                        "expiry": getattr(event, "expiry", None) or broker_position.get("expiry"),
                        "contract_key": broker_position.get("contract_key"),
                        "quantity": _decimal_display(abs(signed_qty)),
                        "aggregate_qty": _decimal_display(signed_qty),
                        "side": "LONG" if signed_qty > 0 else "SHORT",
                        "strategy_id": lane_id,
                        "lane_id": lane_id,
                        "lifecycle_id": getattr(event, "lifecycle_id", None),
                        "trade_id": getattr(event, "trade_id", None),
                        "entry_timestamp": getattr(event, "generated_at", None).isoformat()
                        if getattr(event, "generated_at", None) is not None
                        else None,
                        "avg_entry_price": str(getattr(event, "price", "")) if getattr(event, "price", None) is not None else None,
                        "managed_exit_policy_id": _managed_exit_policy_from_lane(lane_id),
                        "entry_order_ids": [str(getattr(event, "order_id", ""))] if getattr(event, "order_id", None) else [],
                        "entry_perm_ids": [str(getattr(event, "perm_id", ""))] if getattr(event, "perm_id", None) else [],
                        "entry_exec_ids": [str(getattr(event, "exec_id", ""))] if getattr(event, "exec_id", None) else [],
                        "source": "TRACK_B_LIVE_TRADE_REGISTRY_ENTRY_FILL",
                    }
                )
            )
    if not candidates:
        return None
    selected = max(candidates, key=lambda row: str(row.get("entry_timestamp") or ""))
    same_lane_side = [
        row
        for row in candidates
        if row.get("lane_id") == selected.get("lane_id") and row.get("side") == selected.get("side")
    ]
    broker_qty = abs(_decimal(broker_position.get("quantity")) or Decimal("0"))
    if len(same_lane_side) > 1 and broker_qty > Decimal("1"):
        selected = {
            **selected,
            "duplicate_same_lane_exposure": True,
            "duplicate_entry_count": len(same_lane_side),
            "accepted_managed_qty": "1",
            "duplicate_excess_qty": _decimal_display(max(broker_qty - Decimal("1"), Decimal("0"))),
            "duplicate_entry_exec_ids": [
                str(exec_id)
                for row in same_lane_side
                for exec_id in _list(row.get("entry_exec_ids"))
                if str(exec_id or "").strip()
            ],
            "duplicate_entry_perm_ids": [
                str(perm_id)
                for row in same_lane_side
                for perm_id in _list(row.get("entry_perm_ids"))
                if str(perm_id or "").strip()
            ],
        }
    return selected


def _event_matches_broker_position(event: Any, *, broker_position: Mapping[str, Any]) -> bool:
    broker = _normalize_contract_row(broker_position)
    event_row = _normalize_contract_row(
        {
            "symbol": getattr(event, "symbol", None),
            "local_symbol": getattr(event, "local_symbol", None),
            "con_id": getattr(event, "con_id", None),
            "expiry": getattr(event, "expiry", None),
            "account_id": getattr(event, "account_id", None),
        }
    )
    if not _account_matches(event_row, broker):
        return False
    return _contract_identity_matches(event_row, broker)


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


def _broker_position_nonzero(position: Mapping[str, Any]) -> bool:
    qty = _decimal(position.get("quantity"))
    return qty is not None and qty != 0


def _owner_exposure_supersedes_existing_projection(owner_exposure: Mapping[str, Any]) -> bool:
    reason_codes = {str(code or "") for code in owner_exposure.get("reason_codes") or []}
    return bool(
        {
            "NEWEST_EXACT_BROKER_BACKED_LIFECYCLE_REPORT_SELECTED",
            "NEWEST_EXACT_BROKER_BACKED_ENTRY_SELECTED",
            "REGISTRY_OPEN_MANAGED_MATCHED_BROKER_POSITION",
        }
        & reason_codes
    )


def _current_owner_resolution_for_projection(
    *,
    pre_restart_exposure_resolution: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
) -> dict[str, Any]:
    fresh = _mapping(pre_restart_exposure_resolution.get("current_exposure_owner_resolution"))
    cached = _mapping(reconciliation.get("current_exposure_owner_resolution"))
    if fresh and (_has_broker_backed_projection_owner(fresh) or not cached):
        return fresh
    return cached or fresh


def _has_broker_backed_projection_owner(owner_resolution: Mapping[str, Any]) -> bool:
    broker_backed_reasons = {
        "NEWEST_EXACT_BROKER_BACKED_LIFECYCLE_REPORT_SELECTED",
        "NEWEST_EXACT_BROKER_BACKED_ENTRY_SELECTED",
        "REGISTRY_OPEN_MANAGED_MATCHED_BROKER_POSITION",
        "EXACT_BROKER_BACKED_LIFECYCLE_REPORT_CAN_REPAIR_PROJECTION",
    }
    for exposure in owner_resolution.get("owned_exposures") or []:
        if not isinstance(exposure, Mapping):
            continue
        if broker_backed_reasons & {str(code or "") for code in exposure.get("reason_codes") or []}:
            return True
    return False


def _position_classification(
    *,
    broker: Mapping[str, Any] | None,
    lifecycle: Mapping[str, Any] | None,
    review: Mapping[str, Any] | None,
    lifecycle_report: Mapping[str, Any],
    manifest: Mapping[str, Any] | None,
    close_order_state: Mapping[str, Any] | None,
    source_stale: Mapping[str, Any],
) -> str:
    if source_stale.get("stale") is True and not _fresh_broker_position_overrides_stale_reconciliation(
        broker=broker,
        source_stale=source_stale,
    ):
        return STALE_MANAGED_POSITION_EVIDENCE
    lifecycle_review = (
        _truthy(lifecycle_report.get("review_required"))
        or _lifecycle_requires_operator_action(lifecycle, lifecycle_report)
    ) and not _retryable_unmutated_managed_close_review(lifecycle_report)
    if (review and not _retryable_unmutated_managed_close_review(review)) or lifecycle_review:
        return REVIEW_REQUIRED
    if broker and not lifecycle:
        return BROKER_BACKED_ADOPTION_REQUIRED
    if lifecycle and not _managed_exit_policy_id(lifecycle, None, lifecycle_report, manifest):
        return MANAGED_POSITION_METADATA_INCOMPLETE
    if lifecycle and not broker:
        return LIFECYCLE_WITHOUT_BROKER
    if close_order_state:
        return OPEN_MANAGED_CLOSE_WORKING
    if lifecycle and broker:
        return OPEN_MANAGED_MATCHED
    return NO_MANAGED_POSITIONS


def _exit_due_classification(
    *,
    classification: str,
    broker: Mapping[str, Any] | None,
    lifecycle: Mapping[str, Any] | None,
    review: Mapping[str, Any] | None,
    lifecycle_report: Mapping[str, Any],
    close_order_state: Mapping[str, Any] | None,
    source_stale: Mapping[str, Any],
) -> str:
    if classification == OPEN_MANAGED_MATCHED:
        return classification
    if classification != STALE_MANAGED_POSITION_EVIDENCE:
        return classification
    if source_stale.get("stale") is not True:
        return classification
    if not broker or not lifecycle:
        return classification
    if review:
        return classification
    if close_order_state:
        return classification
    if not str(lifecycle.get("lifecycle_id") or lifecycle_report.get("lifecycle_id") or "").strip():
        return classification
    if not str(lifecycle.get("trade_id") or lifecycle_report.get("trade_id") or "").strip():
        return classification
    if not _managed_exit_policy_id(lifecycle, None, lifecycle_report, None):
        return classification
    if _signed_lifecycle_quantity(lifecycle) is None or _decimal((broker or {}).get("quantity")) is None:
        return classification
    return OPEN_MANAGED_MATCHED


def _fresh_broker_position_overrides_stale_reconciliation(
    *,
    broker: Mapping[str, Any] | None,
    source_stale: Mapping[str, Any],
) -> bool:
    if not broker:
        return False
    stale_sources = {str(source or "") for source in source_stale.get("stale_sources") or []}
    if stale_sources != {"reconciliation"}:
        return False
    identity = _mapping(broker.get("contract_identity"))
    return identity.get("resolved") is True and _broker_position_nonzero(broker)


def _position_freshness_state(*, source_stale: Mapping[str, Any]) -> str:
    if source_stale.get("stale") is True:
        return "STALE_DEPENDENCY"
    if source_stale.get("diagnostic_stale") is True:
        return "DIAGNOSTIC_STALE_DEPENDENCY"
    return "FRESH"


def _required_close_action(*, side: Any, signed_broker_qty: Decimal | None) -> str | None:
    normalized_side = str(side or "").upper()
    if normalized_side == "LONG":
        return "SELL"
    if normalized_side == "SHORT":
        return "BUY"
    if signed_broker_qty is None or signed_broker_qty == 0:
        return None
    return "SELL" if signed_broker_qty > 0 else "BUY"


def _overall_classification(
    *,
    managed_positions: list[dict[str, Any]],
    broker_positions: list[dict[str, Any]],
    lifecycle_positions: list[dict[str, Any]],
    review_positions: list[dict[str, Any]],
    source_stale: Mapping[str, Any],
) -> str:
    if not broker_positions and not lifecycle_positions and not review_positions and not managed_positions:
        return NO_MANAGED_POSITIONS
    priority = [
        PROJECTION_AUTHORITY_DIVERGENCE,
        REVIEW_REQUIRED,
        BROKER_BACKED_ADOPTION_REQUIRED,
        MANAGED_POSITION_METADATA_INCOMPLETE,
        LIFECYCLE_WITHOUT_BROKER,
        OPEN_MANAGED_CLOSE_WORKING,
        OPEN_MANAGED_EXIT_DUE,
        OPEN_MANAGED_MATCHED,
    ]
    classifications = [str(item.get("classification") or "") for item in managed_positions]
    for item in priority:
        if item in classifications:
            return item
    if source_stale.get("stale") is True:
        return STALE_MANAGED_POSITION_EVIDENCE
    return classifications[0] if classifications else NO_MANAGED_POSITIONS


def _repair_owner_confirmed_timebox_due_positions(managed_positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    repaired: list[dict[str, Any]] = []
    for row in managed_positions:
        item = dict(row)
        if _owner_confirmed_timebox_due(item):
            signed_broker_qty = _decimal(
                _mapping(item.get("broker_position")).get("quantity") or item.get("signed_broker_qty")
            )
            item["classification"] = OPEN_MANAGED_EXIT_DUE
            item["exit_due"] = True
            item["exit_due_state"] = "EXIT_DUE"
            item["required_close_action"] = _required_close_action(side=item.get("side"), signed_broker_qty=signed_broker_qty)
            item["required_close_quantity"] = (
                _decimal_display(abs(signed_broker_qty)) if signed_broker_qty is not None else item.get("quantity")
            )
            item["stale_review_due_repair"] = {
                "classification": "OWNER_CONFIRMED_TIMEBOX_DUE_SUPERSEDES_STALE_REVIEW",
                "reason": (
                    "Current broker-backed owner projection has enough policy/bar evidence for timebox due; "
                    "stale lifecycle review remains diagnostic."
                ),
                "previous_classification": row.get("classification"),
            }
            item["attention_required"] = False
        repaired.append(item)
    return repaired


def _owner_confirmed_timebox_due(row: Mapping[str, Any]) -> bool:
    if row.get("projection_authority_owner_confirmed") is not True:
        return False
    if row.get("exit_due") is True or str(row.get("exit_due_state") or "").upper() == "EXIT_DUE":
        return False
    if str(row.get("classification") or "") not in {REVIEW_REQUIRED, STALE_MANAGED_POSITION_EVIDENCE}:
        return False
    if row.get("close_order_state"):
        return False
    broker = _mapping(row.get("broker_position"))
    lifecycle = _mapping(row.get("lifecycle_position"))
    if not broker or not lifecycle:
        return False
    if not _account_matches(lifecycle, broker) or not _contract_identity_matches(lifecycle, broker):
        return False
    signed_lifecycle_qty = _signed_lifecycle_quantity(lifecycle)
    signed_broker_qty = _decimal(broker.get("quantity"))
    if signed_lifecycle_qty is None or signed_broker_qty is None or signed_lifecycle_qty != signed_broker_qty:
        return False
    policy = str(row.get("managed_exit_policy_id") or _managed_exit_policy_id(lifecycle, None, {}, None) or "")
    required = _required_completed_5m_bars_for_policy(policy)
    bars_since_entry = _int_or_none(row.get("bars_since_entry"))
    return required is not None and bars_since_entry is not None and bars_since_entry >= required


def _required_completed_5m_bars_for_policy(policy: str) -> int | None:
    return {
        "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1": 3,
        "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1": 3,
        "CHANGEOVER_0300_LONG_TIMEBOX_6H_EXIT_V1": 72,
        "CHANGEOVER_0700_LONG_TIMEBOX_4H_EXIT_V1": 48,
        "US_SESSION_CONTINUATION_TIMEBOX_2H_EXIT_V1": 24,
        "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1": 12,
        "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1": 3,
        "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1": 3,
        "GLOBEX_REOPEN_FIRST_CANDLE_60M_TIMEBOX_SHADOW_EXIT_V1": 12,
    }.get(policy)


def _review_required_position_scope(
    *,
    reconciliation: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    lifecycle_reports: list[dict[str, Any]],
    position_truth: Mapping[str, Any],
    broker_positions: list[dict[str, Any]],
    lifecycle_positions: list[dict[str, Any]],
    open_order_states: list[dict[str, Any]],
    managed_order_states: list[dict[str, Any]],
    unresolved_ownership: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    current_scope: list[dict[str, Any]] = []
    historical: list[dict[str, Any]] = []
    for row in _review_required_positions(
        reconciliation=reconciliation,
        live_position_status=live_position_status,
        lifecycle_reports=lifecycle_reports,
        position_truth=position_truth,
        open_order_states=open_order_states,
        managed_order_states=managed_order_states,
        unresolved_ownership=unresolved_ownership,
    ):
        scoped = _classify_review_position_scope(
            row,
            reconciliation=reconciliation,
            broker_positions=broker_positions,
            lifecycle_positions=lifecycle_positions,
            open_order_states=open_order_states,
            managed_order_states=managed_order_states,
            unresolved_ownership=unresolved_ownership,
        )
        (current_scope if scoped["current_scope"] else historical).append(scoped["row"])
    return {"current_scope": current_scope, "historical": historical}


def _review_required_positions(
    *,
    reconciliation: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    lifecycle_reports: list[dict[str, Any]],
    position_truth: Mapping[str, Any],
    open_order_states: list[dict[str, Any]],
    managed_order_states: list[dict[str, Any]],
    unresolved_ownership: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    active_lifecycle_ids = {
        str(item.get("lifecycle_id") or "").strip()
        for item in [
            *_list(reconciliation.get("track_b_lifecycle_positions")),
        ]
        if str(item.get("lifecycle_id") or "").strip()
    }
    active_position_keys = {
        _position_key(item)
        for item in [
            *_list(reconciliation.get("track_b_broker_positions")),
            *_list(reconciliation.get("track_b_lifecycle_positions")),
            *_list(live_position_status.get("positions")),
            *_list(live_position_status.get("open_positions")),
        ]
        if _position_key(item)
    }
    values = _list(reconciliation.get("review_required_positions")) or _list(
        live_position_status.get("review_required_positions")
    )
    if values:
        return [value for value in values if not _retryable_unmutated_managed_close_review(value)]
    return [
        report
        for report in lifecycle_reports
        if (report.get("review_required") is True or "REVIEW" in str(report.get("paper_lifecycle_classification") or ""))
        and not _retryable_unmutated_managed_close_review(report)
        and _review_position_matches_active_context(
            report,
            active_lifecycle_ids=active_lifecycle_ids,
            active_position_keys=active_position_keys,
        )
    ]


def _classify_review_position_scope(
    row: Mapping[str, Any],
    *,
    reconciliation: Mapping[str, Any],
    broker_positions: list[dict[str, Any]],
    lifecycle_positions: list[dict[str, Any]],
    open_order_states: list[dict[str, Any]],
    managed_order_states: list[dict[str, Any]],
    unresolved_ownership: list[dict[str, Any]],
) -> dict[str, Any]:
    lifecycle_id = str(row.get("lifecycle_id") or "").strip()
    trade_id = str(row.get("trade_id") or "").strip()
    key = _position_key(row)
    current_ids = {
        str(item.get("lifecycle_id") or "").strip()
        for item in [
            *lifecycle_positions,
            *unresolved_ownership,
        ]
        if str(item.get("lifecycle_id") or "").strip()
    }
    current_trade_ids = {
        str(item.get("trade_id") or _mapping(item.get("extra")).get("trade_id") or "").strip()
        for item in [*lifecycle_positions, *unresolved_ownership]
        if str(item.get("trade_id") or _mapping(item.get("extra")).get("trade_id") or "").strip()
    }
    registry = _mapping(reconciliation.get("registry_reconciliation"))
    if registry.get("blocking") is True:
        current_trade_ids.update(str(item).strip() for item in registry.get("review_required_trade_ids") or [] if str(item).strip())
    current_exposure_keys = {
        key
        for item in [
            *broker_positions,
            *lifecycle_positions,
        ]
        for key in _current_linkage_keys(item)
    }
    explicit_linkage_keys = {
        key
        for item in [
            *open_order_states,
            *managed_order_states,
            *unresolved_ownership,
        ]
        for key in _current_linkage_keys(item)
    }
    identity_linked = bool(
        (lifecycle_id and lifecycle_id in current_ids) or (trade_id and trade_id in current_trade_ids)
    )
    reconciliation_review_ids = {
        str(item.get("lifecycle_id") or "").strip()
        for item in _list(reconciliation.get("review_required_positions"))
        if str(item.get("lifecycle_id") or "").strip()
    }
    reconciliation_review_trade_ids = {
        str(item.get("trade_id") or _mapping(item.get("extra")).get("trade_id") or "").strip()
        for item in _list(reconciliation.get("review_required_positions"))
        if str(item.get("trade_id") or _mapping(item.get("extra")).get("trade_id") or "").strip()
    }
    reconciliation_review_linked = bool(
        (lifecycle_id and lifecycle_id in reconciliation_review_ids)
        or (trade_id and trade_id in reconciliation_review_trade_ids)
    )
    same_key_current_lifecycle = bool(
        key and any(_position_key(item) == key for item in lifecycle_positions if isinstance(item, Mapping))
    )
    has_current_context = bool(current_ids or current_trade_ids or current_exposure_keys or explicit_linkage_keys)
    if same_key_current_lifecycle and not identity_linked:
        linked = False
    elif not has_current_context:
        linked = reconciliation_review_linked
    else:
        linked = bool(identity_linked or (key and (key in explicit_linkage_keys or key in current_exposure_keys)))
    scoped_row = dict(row)
    scoped_row["current_hot_path_scope"] = "CURRENT_SCOPE" if linked else _historical_scope_classification(row, registry)
    scoped_row["current_scope_linked"] = linked
    scoped_row["historical_only"] = not linked
    scoped_row["full_artifact_audit_visible"] = True
    return {"current_scope": linked, "row": scoped_row}


def _current_linkage_keys(row: Mapping[str, Any]) -> set[str]:
    keys = {_position_key(row)}
    nested_order = row.get("order")
    if isinstance(nested_order, Mapping):
        keys.add(_position_key(nested_order))
    nested_position = row.get("position")
    if isinstance(nested_position, Mapping):
        keys.add(_position_key(nested_position))
    return {key for key in keys if key}


def _historical_scope_classification(row: Mapping[str, Any], registry: Mapping[str, Any]) -> str:
    trade_id = str(row.get("trade_id") or "").strip()
    mapped = {str(item).strip() for item in registry.get("mapped_trade_ids") or [] if str(item).strip()}
    if trade_id and trade_id in mapped and registry.get("classification") == "REGISTRY_RECONCILIATION_MATCHED":
        return "HISTORICAL_RESOLVED"
    return "HISTORICAL_UNRESOLVED_FULL_AUDIT_ONLY"


def _review_position_matches_active_context(
    position: Mapping[str, Any],
    *,
    active_lifecycle_ids: set[str],
    active_position_keys: set[str],
) -> bool:
    lifecycle_id = str(position.get("lifecycle_id") or "").strip()
    if active_lifecycle_ids:
        return bool(lifecycle_id and lifecycle_id in active_lifecycle_ids)
    key = _position_key(position)
    if active_position_keys:
        return bool(key and key in active_position_keys)
    return True


def _active_lifecycle_report_evidence(
    *,
    reconciliation: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    position_truth: Mapping[str, Any],
    open_order_states: list[dict[str, Any]],
    managed_order_states: list[dict[str, Any]],
    unresolved_ownership: list[dict[str, Any]],
) -> bool:
    if _list(reconciliation.get("track_b_broker_positions")):
        return True
    if _list(reconciliation.get("track_b_lifecycle_positions")):
        return True
    if _list(reconciliation.get("unresolved_submit_intent_ownership_records")):
        return True
    if unresolved_ownership:
        return True
    if open_order_states or managed_order_states:
        return True
    if _current_scope_review_required_count(reconciliation):
        return True
    if _int_or_none(reconciliation.get("unresolved_submit_intent_ownership_count")):
        return True
    if _int_or_none(live_position_status.get("open_position_count")):
        return True
    if _list(live_position_status.get("positions")) or _list(live_position_status.get("open_positions")):
        return True
    position_summary = _mapping(position_truth.get("summary"))
    position_classification = str(position_truth.get("classification") or position_summary.get("overall_classification") or "")
    if position_classification in {"CLEAN_FLAT_READY", "FLAT_CLEAN"}:
        return False
    return position_summary.get("broker_exposure_present") is True


def _current_scope_review_required_count(reconciliation: Mapping[str, Any]) -> int:
    if "current_scope_review_required_count" in reconciliation:
        return int(reconciliation.get("current_scope_review_required_count") or 0)
    return int(reconciliation.get("review_required_count") or 0)


def _exit_due(
    *,
    lifecycle: Mapping[str, Any] | None,
    lifecycle_report: Mapping[str, Any],
    classification: str,
    bars_since_entry: int | None,
) -> bool:
    if classification != OPEN_MANAGED_MATCHED:
        return False
    policy = str(_managed_exit_policy_id(lifecycle, None, lifecycle_report, None) or "")
    required_by_policy = _required_completed_5m_bars_for_policy(policy)
    if required_by_policy is None:
        return False
    required = _int_or_none((lifecycle or {}).get("required_completed_5m_bars")) or required_by_policy
    return bars_since_entry is not None and bars_since_entry >= required


def _close_order_state(*, key: str, open_order_states: list[dict[str, Any]]) -> dict[str, Any] | None:
    for state in open_order_states:
        if state.get("is_close_order") is True and _position_key(state.get("order") or state) == key:
            return dict(state)
    return None


def _managed_order_state(*, key: str, managed_order_states: list[dict[str, Any]]) -> dict[str, Any] | None:
    for state in managed_order_states:
        if state.get("is_close_order") is True and _position_key(state) == key:
            return dict(state)
    return None


def _source_stale(
    *,
    now: datetime,
    config: TrackBManagedPositionRegistryConfig,
    position_truth: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
    managed_order_registry: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
) -> dict[str, Any]:
    sources = {
        "position_truth": position_truth.get("generated_at"),
        "open_order_truth": open_order_truth.get("generated_at"),
        "reconciliation": reconciliation.get("generated_at"),
    }
    ages = {name: _age_seconds(value, now) for name, value in sources.items()}
    stale_sources = [
        name
        for name, age in ages.items()
        if age is None or age > float(config.artifact_max_age_seconds)
    ]
    current_scope_flat_authority_clean = _current_scope_flat_authority_clean(
        reconciliation=reconciliation,
        open_order_truth=open_order_truth,
    )
    stale_diagnostic_only = bool(stale_sources) and current_scope_flat_authority_clean
    return {
        "stale": bool(stale_sources) and not stale_diagnostic_only,
        "diagnostic_stale": bool(stale_sources),
        "stale_diagnostic_only": stale_diagnostic_only,
        "current_scope_flat_authority_clean": current_scope_flat_authority_clean,
        "stale_sources": stale_sources,
        "ages_seconds": ages,
        "ttl_seconds": float(config.artifact_max_age_seconds),
    }


def _current_scope_flat_authority_clean(
    *,
    reconciliation: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
) -> bool:
    if reconciliation.get("broker_reconciled") is not True:
        return False
    reconciliation_classification = str(reconciliation.get("classification") or "")
    if reconciliation_classification not in {"BROKER_LIFECYCLE_RECONCILED", "TRACK_B_PAPER_BROKER_RECONCILED"}:
        return False
    if _current_scope_review_required_count(reconciliation) != 0:
        return False
    if _list(reconciliation.get("track_b_broker_positions")):
        return False
    if _list(reconciliation.get("track_b_lifecycle_positions")):
        return False
    if _list(reconciliation.get("review_required_positions")):
        return False
    if _list(reconciliation.get("track_b_broker_open_orders")):
        return False
    if _int_or_none(reconciliation.get("track_b_broker_position_count")) not in {None, 0}:
        return False
    if _int_or_none(reconciliation.get("track_b_broker_open_order_count")) not in {None, 0}:
        return False
    if _int_or_none(reconciliation.get("current_scope_lifecycle_open_position_count")) not in {None, 0}:
        return False
    if _int_or_none(reconciliation.get("lifecycle_open_position_count")) not in {None, 0}:
        return False
    if _int_or_none(reconciliation.get("lifecycle_open_order_count")) not in {None, 0}:
        return False
    if _int_or_none(reconciliation.get("unknown_broker_open_order_count")) not in {None, 0}:
        return False
    if _int_or_none(reconciliation.get("unresolved_submit_intent_ownership_count")) not in {None, 0}:
        return False
    if _list(reconciliation.get("unresolved_submit_intent_ownership_records")):
        return False
    registry = _mapping(reconciliation.get("registry_reconciliation"))
    if registry:
        if registry.get("blocking") is True:
            return False
        if _int_or_none(registry.get("broker_position_count")) not in {None, 0}:
            return False
        if _int_or_none(registry.get("lifecycle_position_count")) not in {None, 0}:
            return False
        if _int_or_none(registry.get("broker_open_order_count")) not in {None, 0}:
            return False
        if [str(item).strip() for item in registry.get("review_required_trade_ids") or [] if str(item).strip()]:
            return False
    open_order_classification = str(
        open_order_truth.get("classification")
        or _mapping(open_order_truth.get("summary")).get("classification")
        or ""
    )
    if open_order_classification in {"", "NO_OPEN_ORDERS"}:
        return True
    open_order_summary = _mapping(open_order_truth.get("summary"))
    return (
        open_order_classification == "ORDER_TRUTH_STALE"
        and not _list(open_order_truth.get("order_states"))
        and _int_or_none(open_order_truth.get("open_order_count") or open_order_summary.get("open_order_count")) in {None, 0}
        and _int_or_none(open_order_truth.get("unknown_open_order_count")) in {None, 0}
    )


def _managed_order_registry_flat(managed_order_registry: Mapping[str, Any]) -> bool:
    classification = str(managed_order_registry.get("classification") or "")
    if classification not in {"", "NO_MANAGED_ORDERS"}:
        return False
    if _list(managed_order_registry.get("managed_orders")):
        return False
    summary = _mapping(managed_order_registry.get("summary"))
    for key in (
        "managed_order_count",
        "working_close_order_count",
        "suspicious_order_count",
        "duplicate_close_order_count",
    ):
        if _int_or_none(summary.get(key)) not in {None, 0}:
            return False
    return True


def _demote_review_position_to_historical_flat_diagnostic(row: Mapping[str, Any]) -> dict[str, Any]:
    diagnostic = dict(row)
    diagnostic["current_hot_path_scope"] = "HISTORICAL_SUPERSEDED_BY_CURRENT_FLAT_AUTHORITY"
    diagnostic["current_scope_linked"] = False
    diagnostic["historical_only"] = True
    diagnostic["diagnostic_only"] = True
    diagnostic["full_artifact_audit_visible"] = True
    diagnostic["current_scope_demoted_reason"] = (
        "current broker, reconciliation, open-order, and managed-order authority is clean flat"
    )
    return diagnostic


def _authority_summary(payload: Mapping[str, Any], path: Path) -> dict[str, Any]:
    return {
        "classification": payload.get("classification")
        or _mapping(payload.get("summary")).get("overall_classification"),
        "generated_at": payload.get("generated_at"),
        "artifact_path": str(path),
        "projection_only": payload.get("projection_only") is True,
    }


def _event_state(*, classification: str, managed_positions: list[dict[str, Any]]) -> dict[str, Any]:
    signature_items = [
        "|".join(
            str(value or "")
            for value in (
                item.get("classification"),
                item.get("local_symbol"),
                item.get("side"),
                item.get("quantity"),
                item.get("lifecycle_id"),
                item.get("exit_due"),
            )
        )
        for item in managed_positions
    ]
    return {
        "classification": classification,
        "signature": ";".join(sorted(signature_items)) or classification,
    }


def _recommended_action(*, classification: str) -> str:
    return {
        NO_MANAGED_POSITIONS: "No managed PAPER positions require action.",
        OPEN_MANAGED_MATCHED: "Observe; position is broker/lifecycle matched.",
        OPEN_MANAGED_EXIT_DUE: "Observe runtime-managed exit path; do not manually interfere unless safety degrades.",
        OPEN_MANAGED_CLOSE_WORKING: "Monitor existing close order; do not submit a duplicate close.",
        BROKER_BACKED_ADOPTION_REQUIRED: "Run scoped broker-backed adoption before any close remediation.",
        MANAGED_POSITION_METADATA_INCOMPLETE: "Repair manifest/lifecycle management metadata before exit handling.",
        LIFECYCLE_WITHOUT_BROKER: "Review lifecycle artifact against broker-flat truth; local cleanup may be needed.",
        REVIEW_REQUIRED: "Review lifecycle diagnostics; do not restart trading until resolved.",
        STALE_MANAGED_POSITION_EVIDENCE: "Refresh authority artifacts before acting.",
        PROJECTION_AUTHORITY_DIVERGENCE: (
            "Refresh current exposure owner and managed-position projections; do not submit until they agree."
        ),
    }.get(classification, "Review managed position state.")


def _reconciliation_status(
    *,
    broker: Mapping[str, Any] | None,
    lifecycle: Mapping[str, Any] | None,
    review: Mapping[str, Any] | None,
) -> str:
    if review:
        return REVIEW_REQUIRED
    if broker and lifecycle:
        return OPEN_MANAGED_MATCHED
    if broker and not lifecycle:
        return BROKER_BACKED_ADOPTION_REQUIRED
    if lifecycle and not broker:
        return LIFECYCLE_WITHOUT_BROKER
    return NO_MANAGED_POSITIONS


def _signed_lifecycle_quantity(position: Mapping[str, Any] | None) -> Decimal | None:
    if not position:
        return None
    aggregate = _decimal(position.get("aggregate_qty"))
    if aggregate is not None:
        return aggregate
    quantity = _decimal(position.get("quantity"))
    if quantity is None:
        return None
    side = str(position.get("side") or "").upper()
    if side == "SHORT" and quantity > 0:
        return -quantity
    return quantity


def _duplicate_excess_close_quantity(
    lifecycle: Mapping[str, Any] | None,
    signed_broker_qty: Decimal | None,
) -> Decimal | None:
    if not lifecycle or lifecycle.get("duplicate_same_lane_exposure") is not True:
        return None
    excess = _decimal(lifecycle.get("duplicate_excess_qty"))
    if excess is None or excess <= 0:
        return None
    if signed_broker_qty is None or abs(signed_broker_qty) <= excess:
        return None
    return excess


def _working_close_qty(order_state: Mapping[str, Any] | None) -> str | None:
    if not order_state:
        return "0"
    quantity = _decimal(order_state.get("quantity") or _mapping(order_state.get("order")).get("quantity"))
    return _decimal_display(quantity)


def _decimal_display(value: Decimal | None) -> str | None:
    if value is None:
        return None
    if value == value.to_integral_value():
        return str(value.quantize(Decimal("1")))
    return str(value.normalize())


def _lifecycle_position_registry_eligible(position: Mapping[str, Any]) -> bool:
    state = _lifecycle_state_from_mapping(position)
    if not state:
        return True
    return is_registry_eligible(state)


def _merge_resolved_lifecycle_positions(
    *,
    lifecycle_positions: list[dict[str, Any]],
    resolved_lifecycle_positions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged = list(lifecycle_positions)
    existing_keys = {_position_key(item) for item in merged if _position_key(item)}
    existing_lifecycle_ids = {
        str(item.get("lifecycle_id") or "").strip()
        for item in merged
        if str(item.get("lifecycle_id") or "").strip()
    }
    for row in resolved_lifecycle_positions:
        row = _normalize_contract_row(row)
        key = _position_key(row)
        lifecycle_id = str(row.get("lifecycle_id") or "").strip()
        if lifecycle_id and lifecycle_id in existing_lifecycle_ids:
            continue
        if key and key in existing_keys:
            existing_index = next((idx for idx, item in enumerate(merged) if _position_key(item) == key), None)
            if existing_index is not None and _resolved_lifecycle_supersedes_existing(
                resolved=row,
                existing=merged[existing_index],
            ):
                existing_lifecycle_id = str(merged[existing_index].get("lifecycle_id") or "").strip()
                merged[existing_index] = {
                    **dict(row),
                    "superseded_lifecycle_id": existing_lifecycle_id or None,
                    "projection_repair_reason": "fresh_broker_backed_registry_owner_superseded_stale_same_contract_lifecycle",
                }
                if lifecycle_id:
                    existing_lifecycle_ids.add(lifecycle_id)
                continue
            continue
        merged.append(dict(row))
        if key:
            existing_keys.add(key)
        if lifecycle_id:
            existing_lifecycle_ids.add(lifecycle_id)
    return merged


def _resolved_lifecycle_supersedes_existing(
    *,
    resolved: Mapping[str, Any],
    existing: Mapping[str, Any],
) -> bool:
    source = str(resolved.get("source") or "").upper()
    if "LIVE_TRADE_REGISTRY" not in source:
        return False
    resolved_qty = _signed_lifecycle_quantity(resolved)
    existing_qty = _signed_lifecycle_quantity(existing)
    if resolved_qty is None or existing_qty is None or resolved_qty == existing_qty:
        return False
    if resolved_qty * existing_qty >= 0:
        return False
    if not _managed_exit_policy_id(resolved, None, {}, None):
        return False
    resolved_time = str(resolved.get("entry_timestamp") or "")
    existing_time = str(existing.get("entry_timestamp") or "")
    return not existing_time or resolved_time >= existing_time


def _lifecycle_requires_operator_action(
    lifecycle: Mapping[str, Any] | None,
    lifecycle_report: Mapping[str, Any],
) -> bool:
    state = _lifecycle_state_from_mapping(lifecycle or lifecycle_report)
    return bool(state and requires_operator_action(state))


def _retryable_unmutated_managed_close_review(payload: Mapping[str, Any]) -> bool:
    """Treat stale no-broker-effect close reviews as retryable evidence."""

    if str(payload.get("paper_lifecycle_classification") or "") != "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED":
        return False
    if payload.get("broker_state_mutated") is True:
        return False
    if payload.get("close_fill"):
        return False
    close_submit = payload.get("close_submit_attempt")
    if isinstance(close_submit, Mapping) and (
        close_submit.get("submitted") is True
        or close_submit.get("submit_attempted") is True
        or close_submit.get("broker_state_mutated") is True
        or str(close_submit.get("broker_order_id") or "").strip()
    ):
        return False
    if not isinstance(payload.get("close_intent"), Mapping):
        return False
    primary_blocker = str(payload.get("primary_blocker") or "")
    submit_diagnostics = close_submit.get("submit_diagnostics") if isinstance(close_submit, Mapping) else {}
    if not isinstance(submit_diagnostics, Mapping):
        submit_diagnostics = {}
    retryable_legacy_attach_guard = "quantity must be exactly 1 for milestone one" in primary_blocker
    retryable_pre_submit_no_broker_effect = (
        "PRE_SUBMIT" in primary_blocker
        and submit_diagnostics.get("pre_submit_blocked") is True
        and submit_diagnostics.get("place_order_called") is not True
    )
    if not retryable_legacy_attach_guard and not retryable_pre_submit_no_broker_effect:
        return False
    return bool(payload.get("entry_fill") or payload.get("entry_fill_confirmed") is True)


def _lifecycle_state_from_mapping(payload: Mapping[str, Any]) -> str:
    return normalize_lifecycle_state(
        payload.get("final_position_status")
        or payload.get("lifecycle_status")
        or payload.get("paper_lifecycle_classification")
        or payload.get("strategy_managed_lifecycle_classification")
        or payload.get("classification")
    )


def _load_lifecycle_reports(root: Path) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    reports: list[dict[str, Any]] = []
    for path in root.glob("*/track_b_strategy_managed_paper_lifecycle_report.json"):
        payload = _read_json(path)
        if payload:
            payload.setdefault("report_json_path", str(path))
            reports.append(payload)
    return reports


def _load_manifests(root: Path) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    manifests: list[dict[str, Any]] = []
    for path in root.glob("*.json"):
        payload = _read_json(path)
        if payload:
            payload.setdefault("manifest_path", str(path))
            manifests.append(payload)
    return manifests


def _lifecycle_report(lifecycle_id: str, lifecycle_reports: list[dict[str, Any]]) -> dict[str, Any]:
    if not lifecycle_id:
        return {}
    matches = [item for item in lifecycle_reports if str(item.get("lifecycle_id") or "") == lifecycle_id]
    return matches[-1] if matches else {}


def _manifest_for_position(
    *,
    lifecycle: Mapping[str, Any],
    lifecycle_report: Mapping[str, Any],
    manifests: list[dict[str, Any]],
) -> dict[str, Any] | None:
    explicit = str(
        lifecycle.get("position_management_manifest_path")
        or lifecycle_report.get("position_management_manifest_path")
        or ""
    )
    if explicit:
        for manifest in manifests:
            if str(manifest.get("manifest_path") or "").endswith(Path(explicit).name):
                return manifest
    lifecycle_id = str(lifecycle.get("lifecycle_id") or lifecycle_report.get("lifecycle_id") or "")
    for manifest in manifests:
        if lifecycle_id and str(manifest.get("lifecycle_id") or "") == lifecycle_id:
            return manifest
    return None


def _manifest_path(manifest: Mapping[str, Any] | None) -> str | None:
    if not manifest:
        return None
    return str(manifest.get("manifest_path") or "") or None


def _managed_exit_policy_id(
    lifecycle: Mapping[str, Any] | None,
    review: Mapping[str, Any] | None,
    lifecycle_report: Mapping[str, Any],
    manifest: Mapping[str, Any] | None,
) -> str:
    return str(
        (lifecycle or {}).get("managed_exit_policy_id")
        or (review or {}).get("managed_exit_policy_id")
        or lifecycle_report.get("managed_exit_policy_id")
        or (manifest or {}).get("managed_exit_policy_id")
        or _managed_exit_policy_from_lane(
            (lifecycle or {}).get("lane_id")
            or (lifecycle or {}).get("strategy_id")
            or (review or {}).get("lane_id")
            or (review or {}).get("strategy_id")
            or lifecycle_report.get("lane_id")
            or lifecycle_report.get("strategy_id")
            or (manifest or {}).get("lane_id")
            or (manifest or {}).get("strategy_id")
        )
        or ""
    )


def _recover_lifecycle_metadata(
    *,
    lifecycle: Mapping[str, Any] | None,
    lifecycle_report: Mapping[str, Any],
    manifest: Mapping[str, Any] | None,
    broker: Mapping[str, Any] | None,
    terminal_records: tuple[Any, ...],
) -> Mapping[str, Any] | None:
    if not lifecycle:
        return lifecycle
    recovered = dict(lifecycle)
    policy_id = _managed_exit_policy_id(recovered, None, lifecycle_report, manifest)
    if policy_id and not recovered.get("managed_exit_policy_id"):
        recovered["managed_exit_policy_id"] = policy_id
        recovered["metadata_repair"] = {
            **_mapping(recovered.get("metadata_repair")),
            "managed_exit_policy_id": "recovered_from_lane_or_manifest",
        }
        recovered["lifecycle_units"] = [
            {
                **dict(unit),
                "managed_exit_policy_id": unit.get("managed_exit_policy_id") or policy_id,
            }
            for unit in _list(recovered.get("lifecycle_units"))
            if isinstance(unit, Mapping)
        ] or recovered.get("lifecycle_units")

    entry_time = _entry_time(
        lifecycle=recovered,
        lifecycle_report=lifecycle_report,
        terminal_records=terminal_records,
    )
    if entry_time is not None and not recovered.get("entry_timestamp"):
        recovered["entry_timestamp"] = entry_time.isoformat()
        recovered["metadata_repair"] = {
            **_mapping(recovered.get("metadata_repair")),
            "entry_timestamp": "recovered_from_broker_fill_evidence",
        }
        recovered["lifecycle_units"] = [
            {
                **dict(unit),
                "entry_time": unit.get("entry_time") or entry_time.isoformat(),
            }
            for unit in _list(recovered.get("lifecycle_units"))
            if isinstance(unit, Mapping)
        ] or recovered.get("lifecycle_units")

    entry_fill = _entry_fill_event_from_registry(
        lifecycle=recovered,
        lifecycle_report=lifecycle_report,
        broker=broker,
        terminal_records=terminal_records,
    )
    if entry_fill is not None and not recovered.get("avg_entry_price"):
        price = getattr(entry_fill, "price", None)
        if price is not None:
            recovered["avg_entry_price"] = str(price)
    return recovered


def _managed_exit_policy_from_lane(lane_id: object) -> str | None:
    text = str(lane_id or "")
    if "_active_participation_" in text:
        return "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    return None


def _bars_since_entry(
    *,
    lifecycle: Mapping[str, Any] | None,
    lifecycle_report: Mapping[str, Any],
    market_data_root: Path,
    terminal_records: tuple[Any, ...] = (),
) -> int | None:
    from_phase1 = _phase1_completed_5m_bars_since_entry(
        lifecycle=lifecycle,
        lifecycle_report=lifecycle_report,
        market_data_root=market_data_root,
        terminal_records=terminal_records,
    )
    if from_phase1 is not None:
        return from_phase1
    for value in (
        (lifecycle or {}).get("bars_since_fill"),
        (lifecycle or {}).get("completed_bars_since_entry"),
        lifecycle_report.get("bars_since_fill"),
        lifecycle_report.get("open_position_age_completed_5m_bars"),
    ):
        parsed = _int_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _phase1_completed_5m_bars_since_entry(
    *,
    lifecycle: Mapping[str, Any] | None,
    lifecycle_report: Mapping[str, Any],
    market_data_root: Path,
    terminal_records: tuple[Any, ...] = (),
) -> int | None:
    symbol = _symbol(lifecycle or lifecycle_report)
    entry_time = _entry_time(
        lifecycle=lifecycle,
        lifecycle_report=lifecycle_report,
        terminal_records=terminal_records,
    )
    if not symbol or entry_time is None:
        return None
    payload = _read_json(market_data_root / symbol / "5m" / "latest_runtime_candles.json")
    bars = _runtime_bars(payload)
    if not bars:
        return None
    completed = 0
    for bar in bars:
        bar_time = _bar_end_time(bar)
        if bar_time is not None and bar_time > entry_time:
            completed += 1
    return completed


def _runtime_bars(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    for key in ("bars", "candles", "candle_history"):
        rows = _list(payload.get(key))
        if rows:
            return rows
    return []


def _bar_end_time(bar: Mapping[str, Any]) -> datetime | None:
    for key in ("bar_end", "end", "timestamp", "ts", "datetime", "time"):
        parsed = _parse_time(bar.get(key))
        if parsed is not None:
            return parsed
    return None


def _entry_time(
    *,
    lifecycle: Mapping[str, Any] | None,
    lifecycle_report: Mapping[str, Any],
    terminal_records: tuple[Any, ...] = (),
) -> datetime | None:
    explicit = _parse_time(
        (lifecycle or {}).get("entry_timestamp")
        or (lifecycle or {}).get("entry_time")
        or lifecycle_report.get("entry_timestamp")
        or _mapping(lifecycle_report.get("entry_fill")).get("filled_at")
    )
    if explicit is not None:
        return explicit
    entry_fill = _entry_fill_event_from_registry(
        lifecycle=lifecycle,
        lifecycle_report=lifecycle_report,
        broker=None,
        terminal_records=terminal_records,
    )
    return _ensure_utc(getattr(entry_fill, "generated_at", None)) if entry_fill is not None else None


def _entry_fill_event_from_registry(
    *,
    lifecycle: Mapping[str, Any] | None,
    lifecycle_report: Mapping[str, Any],
    broker: Mapping[str, Any] | None,
    terminal_records: tuple[Any, ...],
) -> Any | None:
    if not terminal_records:
        return None
    lifecycle = _mapping(lifecycle)
    lifecycle_units = [_mapping(item) for item in _list(lifecycle.get("lifecycle_units"))]
    lifecycle_ids = {
        str(value or "").strip()
        for value in [
            lifecycle.get("lifecycle_id"),
            lifecycle_report.get("lifecycle_id"),
            *[unit.get("lifecycle_id") for unit in lifecycle_units],
        ]
        if str(value or "").strip()
    }
    trade_ids = {
        str(value or "").strip()
        for value in [
            lifecycle.get("trade_id"),
            lifecycle_report.get("trade_id"),
            *[unit.get("trade_id") for unit in lifecycle_units],
        ]
        if str(value or "").strip()
    }
    exec_ids = _identity_set(lifecycle.get("entry_exec_ids"), lifecycle.get("entry_exec_id"), *[unit.get("entry_exec_id") for unit in lifecycle_units])
    perm_ids = _identity_set(lifecycle.get("entry_perm_ids"), lifecycle.get("entry_perm_id"), *[unit.get("entry_perm_id") for unit in lifecycle_units])
    order_ids = _identity_set(lifecycle.get("entry_order_ids"), lifecycle.get("entry_order_id"), *[unit.get("entry_order_id") for unit in lifecycle_units])
    con_id = _int_or_none(lifecycle.get("con_id") or _mapping(broker).get("con_id"))
    local_symbol = str(lifecycle.get("local_symbol") or _mapping(broker).get("local_symbol") or "").strip().upper()
    lane_id = str(lifecycle.get("lane_id") or lifecycle.get("strategy_id") or lifecycle_report.get("lane_id") or lifecycle_report.get("strategy_id") or "").strip()

    candidates: list[Any] = []
    for record in terminal_records:
        for event in getattr(record, "event_chain", ()) or ():
            if _event_type_value(getattr(event, "event_type", "")) != "ENTRY_FILL_BROKER_BACKED":
                continue
            event_lifecycle_id = str(getattr(event, "lifecycle_id", "") or "").strip()
            event_trade_id = str(getattr(event, "trade_id", "") or "").strip()
            event_exec_id = str(getattr(event, "exec_id", "") or "").strip()
            event_perm_id = str(getattr(event, "perm_id", "") or "").strip()
            event_order_id = str(getattr(event, "order_id", "") or "").strip()
            if lifecycle_ids and event_lifecycle_id in lifecycle_ids:
                candidates.append(event)
                continue
            if trade_ids and event_trade_id in trade_ids:
                candidates.append(event)
                continue
            if exec_ids and event_exec_id in exec_ids:
                candidates.append(event)
                continue
            if perm_ids and event_perm_id in perm_ids:
                candidates.append(event)
                continue
            if order_ids and event_order_id in order_ids and _event_contract_matches(event, con_id=con_id, local_symbol=local_symbol, lane_id=lane_id):
                candidates.append(event)
                continue
            if _event_contract_matches(event, con_id=con_id, local_symbol=local_symbol, lane_id=lane_id):
                candidates.append(event)
    if not candidates:
        return None
    return max(candidates, key=lambda event: getattr(event, "generated_at", datetime.min.replace(tzinfo=UTC)))


def _event_type_value(value: Any) -> str:
    return str(getattr(value, "value", value) or "")


def _event_contract_matches(event: Any, *, con_id: int | None, local_symbol: str, lane_id: str) -> bool:
    if con_id is not None and _int_or_none(getattr(event, "con_id", None)) != con_id:
        return False
    if local_symbol and str(getattr(event, "local_symbol", "") or "").strip().upper() != local_symbol:
        return False
    if lane_id and str(getattr(event, "lane_id", "") or "").strip() != lane_id:
        return False
    return bool(con_id or local_symbol or lane_id)


def _identity_set(*values: Any) -> set[str]:
    identities: set[str] = set()
    for value in values:
        if isinstance(value, (list, tuple, set)):
            identities.update(str(item or "").strip() for item in value if str(item or "").strip())
        elif str(value or "").strip():
            identities.add(str(value or "").strip())
    return identities


def _exit_due_state(exit_due: bool) -> str:
    return "EXIT_DUE" if exit_due else "NOT_DUE_OR_UNKNOWN"


def _position_key(row: Mapping[str, Any]) -> str:
    identity = _mapping(row.get("contract_identity"))
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or identity.get("local_symbol") or "").upper()
    if local_symbol:
        return local_symbol
    contract_key = str(row.get("contract_key") or row.get("position_key") or identity.get("contract_key") or "").upper()
    return contract_key


def _first_match(rows: list[dict[str, Any]], key: str) -> dict[str, Any] | None:
    matches = [row for row in rows if _position_key(row) == key]
    return matches[-1] if matches else None


def _best_lifecycle_match(
    rows: list[dict[str, Any]],
    key: str,
    broker: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    matches = [row for row in rows if _position_key(row) == key]
    if not matches:
        return None
    if not broker:
        return matches[-1]
    return max(matches, key=lambda row: _lifecycle_broker_match_score(row, broker))


def _lifecycle_broker_match_score(row: Mapping[str, Any], broker: Mapping[str, Any]) -> tuple[int, int, int, Decimal, str]:
    account_score = 1 if _account_matches(row, broker) else 0
    identity_score = 1 if _contract_identity_matches(row, broker) else 0
    quantity_score = 1 if _signed_lifecycle_quantity(row) == _decimal(broker.get("quantity")) else 0
    price_score = _negative_price_distance(row, broker)
    as_of = str(row.get("as_of") or row.get("entry_timestamp") or "")
    return (account_score, identity_score, quantity_score, price_score, as_of)


def _account_matches(row: Mapping[str, Any], broker: Mapping[str, Any]) -> bool:
    broker_identity = _mapping(broker.get("contract_identity"))
    expected = str(broker.get("account_id") or broker.get("account") or broker_identity.get("account_id") or "").strip()
    if not expected:
        return False
    row_identity = _mapping(row.get("contract_identity"))
    candidates = [
        row.get("account_id"),
        row.get("account"),
        row_identity.get("account_id"),
        _mapping(row.get("entry_broker_identity")).get("account_id"),
    ]
    candidates.extend(unit.get("account_id") for unit in _list(row.get("lifecycle_units")))
    return any(str(candidate or "").strip() == expected for candidate in candidates)


def _contract_identity_matches(row: Mapping[str, Any], broker: Mapping[str, Any]) -> bool:
    row_identity = _mapping(row.get("contract_identity"))
    broker_identity = _mapping(broker.get("contract_identity"))
    broker_con_id = str(broker.get("con_id") or broker.get("conId") or broker_identity.get("con_id") or "").strip()
    row_con_id = str(
        row.get("con_id")
        or row.get("conId")
        or row_identity.get("con_id")
        or _mapping(row.get("entry_broker_identity")).get("con_id")
        or ""
    ).strip()
    broker_local = str(
        broker.get("local_symbol") or broker.get("localSymbol") or broker_identity.get("local_symbol") or ""
    ).strip().upper()
    row_local = str(
        row.get("local_symbol")
        or row.get("localSymbol")
        or row_identity.get("local_symbol")
        or _mapping(row.get("entry_broker_identity")).get("local_symbol")
        or ""
    ).strip().upper()
    broker_key = str(broker.get("contract_key") or broker_identity.get("contract_key") or "").strip().upper()
    row_key = str(row.get("contract_key") or row_identity.get("contract_key") or "").strip().upper()
    con_id_matches = bool(broker_con_id and row_con_id and broker_con_id == row_con_id)
    local_matches = bool(broker_local and row_local and broker_local == row_local)
    key_matches = bool(broker_key and row_key and broker_key == row_key)
    return con_id_matches or local_matches or key_matches


def _negative_price_distance(row: Mapping[str, Any], broker: Mapping[str, Any]) -> Decimal:
    broker_price = _broker_average_price(broker)
    lifecycle_price = _decimal(row.get("avg_entry_price") or row.get("entry_price"))
    if broker_price is None or lifecycle_price is None:
        return Decimal("-999999")
    return -abs(broker_price - lifecycle_price)


def _broker_average_price(row: Mapping[str, Any]) -> Decimal | None:
    average_price = _decimal(row.get("average_price") or row.get("avg_entry_price"))
    if average_price is not None:
        return average_price
    average_cost = _decimal(row.get("average_cost"))
    multiplier = _decimal(row.get("multiplier"))
    if average_cost is None:
        return None
    if multiplier is None or multiplier == 0:
        return average_cost
    return average_cost / multiplier


def _symbol(row: Mapping[str, Any] | None) -> str | None:
    if not row:
        return None
    identity = _mapping(row.get("contract_identity"))
    return (
        str(
            row.get("symbol")
            or row.get("track_b_root")
            or row.get("instrument_family")
            or row.get("instrument")
            or identity.get("symbol")
            or identity.get("track_b_root")
            or ""
        ).upper()
        or None
    )


def _contract_key_from_broker(row: Mapping[str, Any]) -> str | None:
    identity = _mapping(row.get("contract_identity"))
    if identity.get("resolved") is True and identity.get("contract_key"):
        return str(identity.get("contract_key"))
    if row.get("contract_key"):
        return str(row.get("contract_key"))
    symbol = _symbol(row)
    expiry = str(row.get("expiry") or identity.get("expiry") or "").strip()
    return f"{symbol}-{expiry[:6]}" if symbol and expiry else None


def _normalize_contract_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return normalize_track_b_contract_row(
        row,
        account_id=str(row.get("account_id") or row.get("account") or "").strip() or None,
    )


def _side_from_broker(row: Mapping[str, Any]) -> str | None:
    quantity = _decimal(row.get("quantity"))
    if quantity is None or quantity == 0:
        return None
    return "LONG" if quantity > 0 else "SHORT"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(path, _to_jsonable(dict(payload)))


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: object) -> list[dict[str, Any]]:
    return [dict(item) for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _isoformat_or_none(value: datetime | None) -> str | None:
    return None if value is None else _ensure_utc(value).isoformat()


def _parse_time(value: object) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _age_seconds(value: object, now: datetime) -> float | None:
    parsed = _parse_time(value)
    return None if parsed is None else max(0.0, (now - parsed).total_seconds())


def _decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _int_or_none(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: object) -> bool:
    return value is True or str(value).strip().lower() == "true"


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(item) for item in value]
    return value
