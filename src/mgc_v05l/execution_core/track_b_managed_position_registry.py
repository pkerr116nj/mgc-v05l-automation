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
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
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
    live_position_status = _read_json(config.resolve(config.live_position_status_path))
    lifecycle_reports = _load_lifecycle_reports(config.resolve(config.lifecycle_root))
    manifests = _load_manifests(config.resolve(config.manifest_root))

    broker_positions = _reconciliation_broker_positions(reconciliation)
    lifecycle_positions = [
        item for item in _reconciliation_lifecycle_positions(reconciliation) if _lifecycle_position_registry_eligible(item)
    ]
    pre_restart_exposure_resolution = resolve_pre_restart_exposure_reconciliation(
        config=PreRestartExposureResolverConfig(repo_root=config.repo_root),
        broker_positions=broker_positions,
        lifecycle_positions=lifecycle_positions,
        broker_open_orders=_list(reconciliation.get("track_b_broker_open_orders")),
        lifecycle_reports=lifecycle_reports,
    )
    owner_resolution = _mapping(reconciliation.get("current_exposure_owner_resolution")) or _mapping(
        pre_restart_exposure_resolution.get("current_exposure_owner_resolution")
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
    broker_open_orders = _list(reconciliation.get("track_b_broker_open_orders"))
    terminal_records = load_live_trade_registry_records(repo_root=config.repo_root)
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
    managed_positions = _managed_positions(
        broker_positions=broker_positions,
        lifecycle_positions=lifecycle_positions,
        review_positions=review_positions,
        open_order_states=open_order_states,
        managed_order_states=managed_order_states,
        lifecycle_reports=lifecycle_reports,
        manifests=manifests,
        market_data_root=config.resolve(config.market_data_root),
        source_stale=source_stale,
    )
    managed_positions, projection_authority_diagnostics = _apply_current_owner_projection_overlay(
        managed_positions=managed_positions,
        owner_resolution=owner_resolution or pre_restart_exposure_resolution,
        open_order_states=open_order_states,
        managed_order_states=managed_order_states,
        lifecycle_reports=lifecycle_reports,
        manifests=manifests,
        market_data_root=config.resolve(config.market_data_root),
        source_stale=source_stale,
    )
    managed_positions, projection_authority_diagnostics = _enforce_owned_exposure_projection_invariant(
        managed_positions=managed_positions,
        projection_authority_diagnostics=projection_authority_diagnostics,
        owner_resolution=owner_resolution or pre_restart_exposure_resolution,
        open_order_states=open_order_states,
        managed_order_states=managed_order_states,
        lifecycle_reports=lifecycle_reports,
        manifests=manifests,
        market_data_root=config.resolve(config.market_data_root),
        source_stale=source_stale,
    )
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
        event_log_path.parent.mkdir(parents=True, exist_ok=True)
        with event_log_path.open("a", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event, sort_keys=True) + "\n")
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
        )
        exit_due = _exit_due(
            lifecycle=lifecycle,
            lifecycle_report=lifecycle_report,
            classification=classification,
            bars_since_entry=bars_since_entry,
        )
        lifecycle_units = _list((lifecycle or {}).get("lifecycle_units"))
        aggregate_qty = (lifecycle or {}).get("aggregate_qty")
        signed_lifecycle_qty = _signed_lifecycle_quantity(lifecycle)
        signed_broker_qty = _decimal((broker or {}).get("quantity"))
        broker_qty_match = (
            signed_lifecycle_qty is not None
            and signed_broker_qty is not None
            and signed_lifecycle_qty == signed_broker_qty
        )
        position = {
            "classification": OPEN_MANAGED_EXIT_DUE if exit_due and classification == OPEN_MANAGED_MATCHED else classification,
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
            or _mapping(lifecycle_report.get("entry_fill")).get("filled_at"),
            "entry_price": (lifecycle or review or {}).get("avg_entry_price")
            or _mapping(lifecycle_report.get("entry_fill")).get("price"),
            "managed_exit_policy_id": _managed_exit_policy_id(lifecycle, review, lifecycle_report, manifest),
            "bars_since_entry": bars_since_entry,
            "exit_due": bool(exit_due),
            "exit_due_state": _exit_due_state(exit_due),
            "close_order_state": effective_close_order_state,
            "managed_order_state": managed_order_state,
            "reconciliation_status": _reconciliation_status(broker=broker, lifecycle=lifecycle, review=review),
            "attention_required": classification
            in {
                BROKER_BACKED_ADOPTION_REQUIRED,
                MANAGED_POSITION_METADATA_INCOMPLETE,
                LIFECYCLE_WITHOUT_BROKER,
                REVIEW_REQUIRED,
                STALE_MANAGED_POSITION_EVIDENCE,
            },
            "recommended_operator_action": _recommended_action(
                classification=OPEN_MANAGED_EXIT_DUE if exit_due and classification == OPEN_MANAGED_MATCHED else classification
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
    open_order_states: list[dict[str, Any]],
    managed_order_states: list[dict[str, Any]],
    lifecycle_reports: list[dict[str, Any]],
    manifests: list[dict[str, Any]],
    market_data_root: Path,
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
    open_order_states: list[dict[str, Any]],
    managed_order_states: list[dict[str, Any]],
    lifecycle_reports: list[dict[str, Any]],
    manifests: list[dict[str, Any]],
    market_data_root: Path,
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
    return "NEWEST_EXACT_BROKER_BACKED_LIFECYCLE_REPORT_SELECTED" in reason_codes


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
    if source_stale.get("stale") is True:
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


def _overall_classification(
    *,
    managed_positions: list[dict[str, Any]],
    broker_positions: list[dict[str, Any]],
    lifecycle_positions: list[dict[str, Any]],
    review_positions: list[dict[str, Any]],
    source_stale: Mapping[str, Any],
) -> str:
    if source_stale.get("stale") is True:
        return STALE_MANAGED_POSITION_EVIDENCE
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
    return classifications[0] if classifications else NO_MANAGED_POSITIONS


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
            *_list(reconciliation.get("review_required_positions")),
        ]
        if str(item.get("lifecycle_id") or "").strip()
    }
    active_position_keys = {
        _position_key(item)
        for item in [
            *_list(reconciliation.get("track_b_broker_positions")),
            *_list(reconciliation.get("track_b_lifecycle_positions")),
            *_list(reconciliation.get("review_required_positions")),
            *_list(live_position_status.get("positions")),
            *_list(live_position_status.get("open_positions")),
        ]
        if _position_key(item)
    }
    values = _list(reconciliation.get("review_required_positions")) or _list(
        live_position_status.get("review_required_positions")
    )
    if values:
        return [
            value
            for value in values
            if not _retryable_unmutated_managed_close_review(value)
            if _review_position_matches_active_context(
                value,
                active_lifecycle_ids=active_lifecycle_ids,
                active_position_keys=active_position_keys,
            )
        ] or ([] if (active_lifecycle_ids or active_position_keys) else values)
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
            *_list(reconciliation.get("review_required_positions")),
            *unresolved_ownership,
        ]
        if str(item.get("lifecycle_id") or "").strip()
    }
    current_trade_ids = {
        str(item.get("trade_id") or _mapping(item.get("extra")).get("trade_id") or "").strip()
        for item in unresolved_ownership
        if str(item.get("trade_id") or _mapping(item.get("extra")).get("trade_id") or "").strip()
    }
    registry = _mapping(reconciliation.get("registry_reconciliation"))
    if registry.get("blocking") is True:
        current_trade_ids.update(str(item).strip() for item in registry.get("review_required_trade_ids") or [] if str(item).strip())
    current_keys = {
        key
        for item in [
            *broker_positions,
            *lifecycle_positions,
            *_list(reconciliation.get("review_required_positions")),
            *open_order_states,
            *managed_order_states,
            *unresolved_ownership,
        ]
        for key in _current_linkage_keys(item)
    }
    linked = bool(
        (lifecycle_id and lifecycle_id in current_ids)
        or (trade_id and trade_id in current_trade_ids)
        or (key and key in current_keys)
    )
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
    required_by_policy = {
        "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1": 3,
        "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1": 3,
        "CHANGEOVER_0300_LONG_TIMEBOX_6H_EXIT_V1": 72,
        "CHANGEOVER_0700_LONG_TIMEBOX_4H_EXIT_V1": 48,
        "US_SESSION_CONTINUATION_TIMEBOX_2H_EXIT_V1": 24,
        "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1": 12,
        "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1": 12,
        "GLOBEX_REOPEN_FIRST_CANDLE_60M_TIMEBOX_SHADOW_EXIT_V1": 12,
    }
    if policy not in required_by_policy:
        return False
    required = _int_or_none((lifecycle or {}).get("required_completed_5m_bars")) or required_by_policy[policy]
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
    if managed_order_registry:
        sources["managed_order_registry"] = managed_order_registry.get("generated_at")
    ages = {name: _age_seconds(value, now) for name, value in sources.items()}
    stale_sources = [
        name
        for name, age in ages.items()
        if age is None or age > float(config.artifact_max_age_seconds)
    ]
    return {
        "stale": bool(stale_sources),
        "stale_sources": stale_sources,
        "ages_seconds": ages,
        "ttl_seconds": float(config.artifact_max_age_seconds),
    }


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
        key = _position_key(row)
        lifecycle_id = str(row.get("lifecycle_id") or "").strip()
        if lifecycle_id and lifecycle_id in existing_lifecycle_ids:
            continue
        if key and key in existing_keys:
            continue
        merged.append(dict(row))
        if key:
            existing_keys.add(key)
        if lifecycle_id:
            existing_lifecycle_ids.add(lifecycle_id)
    return merged


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
        or ""
    )


def _bars_since_entry(
    *,
    lifecycle: Mapping[str, Any] | None,
    lifecycle_report: Mapping[str, Any],
    market_data_root: Path,
) -> int | None:
    from_phase1 = _phase1_completed_5m_bars_since_entry(
        lifecycle=lifecycle,
        lifecycle_report=lifecycle_report,
        market_data_root=market_data_root,
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
) -> int | None:
    symbol = _symbol(lifecycle or lifecycle_report)
    entry_time = _entry_time(lifecycle=lifecycle, lifecycle_report=lifecycle_report)
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


def _entry_time(*, lifecycle: Mapping[str, Any] | None, lifecycle_report: Mapping[str, Any]) -> datetime | None:
    return _parse_time(
        (lifecycle or {}).get("entry_timestamp")
        or lifecycle_report.get("entry_timestamp")
        or _mapping(lifecycle_report.get("entry_fill")).get("filled_at")
    )


def _exit_due_state(exit_due: bool) -> str:
    return "EXIT_DUE" if exit_due else "NOT_DUE_OR_UNKNOWN"


def _position_key(row: Mapping[str, Any]) -> str:
    local_symbol = str(row.get("local_symbol") or "").upper()
    if local_symbol:
        return local_symbol
    contract_key = str(row.get("contract_key") or row.get("position_key") or "").upper()
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
    expected = str(broker.get("account_id") or broker.get("account") or "").strip()
    if not expected:
        return False
    candidates = [
        row.get("account_id"),
        _mapping(row.get("entry_broker_identity")).get("account_id"),
    ]
    candidates.extend(unit.get("account_id") for unit in _list(row.get("lifecycle_units")))
    return any(str(candidate or "").strip() == expected for candidate in candidates)


def _contract_identity_matches(row: Mapping[str, Any], broker: Mapping[str, Any]) -> bool:
    broker_con_id = str(broker.get("con_id") or broker.get("conId") or "").strip()
    row_con_id = str(row.get("con_id") or row.get("conId") or _mapping(row.get("entry_broker_identity")).get("con_id") or "").strip()
    broker_local = str(broker.get("local_symbol") or broker.get("localSymbol") or "").strip().upper()
    row_local = str(row.get("local_symbol") or row.get("localSymbol") or _mapping(row.get("entry_broker_identity")).get("local_symbol") or "").strip().upper()
    con_id_matches = bool(broker_con_id and row_con_id and broker_con_id == row_con_id)
    local_matches = bool(broker_local and row_local and broker_local == row_local)
    return con_id_matches or local_matches


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
    return str(row.get("symbol") or row.get("track_b_root") or row.get("instrument_family") or "").upper() or None


def _contract_key_from_broker(row: Mapping[str, Any]) -> str | None:
    symbol = _symbol(row)
    expiry = str(row.get("expiry") or "").strip()
    return f"{symbol}-{expiry[:6]}" if symbol and expiry else None


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
