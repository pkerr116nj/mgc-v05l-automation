"""Read-only Track B PAPER position truth and trade-outcome monitor.

Position Truth authority lives in execution_core; dashboard artifacts are
projections and must not be used as runtime authority.
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
from mgc_v05l.execution_core.track_b_lifecycle_state_transition import normalize_lifecycle_state, requires_operator_action
from mgc_v05l.execution_core.track_b_projection_metadata import build_projection_metadata

from .track_b_open_order_truth import (
    BROKER_FLAT_WITH_OPEN_CLOSE_ORDER,
    BROKER_POSITION_WITHOUT_CLOSE_ORDER,
    CLOSE_ORDER_MARKETABLE_NOT_FILLED,
    CLOSE_ORDER_STALE,
    DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT,
    DUPLICATE_CLOSE_ORDER,
    OPEN_CLOSE_ORDER_WORKING,
    SUSPICIOUS_ORDER_STATE,
    TrackBOpenOrderTruthConfig,
    build_track_b_open_order_truth,
)
from .track_b_managed_order_registry import ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING, DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT

DEFAULT_POSITION_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
)
DEFAULT_TRADE_OUTCOME_EVENTS = (
    Path("outputs") / "track_b_execution_core" / "position_truth" / "track_b_trade_outcome_events.jsonl"
)
DEFAULT_DASHBOARD_POSITION_TRUTH_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_position_truth.json"
)
DEFAULT_RECONCILIATION_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_BROKER_LEASE_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"
)
DEFAULT_RUNTIME_TRUTH_ARTIFACT = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_runtime_truth.json"
)
DEFAULT_HEADLESS_STATUS_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "headless_supervised_paper_status.json"
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
DEFAULT_MARKET_DATA_ROOT = (
    Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
)

FLAT_CLEAN = "FLAT_CLEAN"
OPEN_MANAGED_MATCHED = "OPEN_MANAGED_MATCHED"
BROKER_POSITION_REQUIRES_ADOPTION = "BROKER_POSITION_REQUIRES_ADOPTION"
LIFECYCLE_POSITION_WITHOUT_BROKER = "LIFECYCLE_POSITION_WITHOUT_BROKER"
CLOSE_ORDER_WORKING = "CLOSE_ORDER_WORKING"
CLOSE_ORDER_SUSPICIOUS = "CLOSE_ORDER_SUSPICIOUS"
REVIEW_REQUIRED = "REVIEW_REQUIRED"
UNKNOWN_OPEN_ORDER = "UNKNOWN_OPEN_ORDER"
RECONCILIATION_BLOCKED = "RECONCILIATION_BLOCKED"

@dataclass(frozen=True)
class TrackBPositionTruthMonitorConfig:
    repo_root: Path
    output_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    event_log_path: Path = DEFAULT_TRADE_OUTCOME_EVENTS
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_POSITION_TRUTH_PROJECTION
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    broker_lease_path: Path = DEFAULT_BROKER_LEASE_ARTIFACT
    runtime_truth_path: Path = DEFAULT_RUNTIME_TRUTH_ARTIFACT
    headless_status_path: Path = DEFAULT_HEADLESS_STATUS_ARTIFACT
    live_position_status_path: Path = DEFAULT_LIVE_POSITION_STATUS_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    lifecycle_root: Path = DEFAULT_LIFECYCLE_ROOT
    market_data_root: Path = DEFAULT_MARKET_DATA_ROOT
    suspicious_marketable_seconds: float = 60.0

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_position_truth(
    *,
    config: TrackBPositionTruthMonitorConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build the latest read-only position truth payload from local artifacts."""

    actual_now = _ensure_utc(now or datetime.now(UTC))
    reconciliation = _read_json(config.resolve(config.reconciliation_path))
    broker_lease = _read_json(config.resolve(config.broker_lease_path))
    runtime_truth = _read_json(config.resolve(config.runtime_truth_path))
    headless_status = _read_json(config.resolve(config.headless_status_path))
    live_position_status = _read_json(config.resolve(config.live_position_status_path))
    lifecycle_reports = _load_lifecycle_reports(config.resolve(config.lifecycle_root))
    open_order_truth = _open_order_truth_evidence(config=config, now=actual_now)
    managed_order_registry = _managed_order_registry_evidence(config=config, now=actual_now)

    broker_positions = _list(reconciliation.get("track_b_broker_positions"))
    open_orders = _list(reconciliation.get("track_b_broker_open_orders"))
    lifecycle_positions = _list(reconciliation.get("track_b_lifecycle_positions"))
    review_required_positions = _review_required_positions(
        reconciliation=reconciliation,
        live_position_status=live_position_status,
        lifecycle_reports=lifecycle_reports,
    )
    unresolved_ownership = _list(reconciliation.get("unresolved_submit_intent_ownership_records"))
    unknown_orders = _list(reconciliation.get("unknown_broker_open_orders"))
    known_managed_exit_orders = _list(reconciliation.get("known_managed_exit_orders"))
    order_states = _list(open_order_truth.get("order_states"))

    symbols = _symbols(
        reconciliation=reconciliation,
        broker_positions=broker_positions,
        open_orders=open_orders,
        lifecycle_positions=lifecycle_positions,
        review_required_positions=review_required_positions,
        unresolved_ownership=unresolved_ownership,
    )
    position_states = [
        _classify_symbol(
            symbol=symbol,
            broker_positions=broker_positions,
            open_orders=open_orders,
            lifecycle_positions=lifecycle_positions,
            review_required_positions=review_required_positions,
            unresolved_ownership=unresolved_ownership,
            unknown_orders=unknown_orders,
            known_managed_exit_orders=known_managed_exit_orders,
            open_order_truth=open_order_truth,
            order_states=order_states,
            reconciliation=reconciliation,
        )
        for symbol in symbols
    ]
    runtime_status = _runtime_status(runtime_truth=runtime_truth, headless_status=headless_status, now=actual_now)
    payload = {
        "schema_version": "track_b_position_truth_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": reconciliation.get("live_money_eligible") is True,
        "reconciliation": {
            "classification": reconciliation.get("classification"),
            "broker_reconciled": reconciliation.get("broker_reconciled"),
            "blockers": reconciliation.get("blockers") or [],
            "generated_at": reconciliation.get("generated_at"),
            "artifact_path": str(config.resolve(config.reconciliation_path)),
        },
        "broker_lease": broker_lease,
        "runtime_status": runtime_status,
        "broker_positions": broker_positions,
        "open_broker_orders": open_orders,
        "lifecycle_open_positions": lifecycle_positions,
        "review_required_positions": review_required_positions,
        "unresolved_submit_ownership": unresolved_ownership,
        "known_managed_exit_orders": known_managed_exit_orders,
        "unknown_broker_open_orders": unknown_orders,
        "open_order_truth": {
            "classification": open_order_truth.get("classification"),
            "summary": open_order_truth.get("summary") or {},
            "source": open_order_truth.get("position_truth_evidence_source"),
            "generated_at": open_order_truth.get("generated_at"),
            "stale_or_missing": open_order_truth.get("position_truth_open_order_truth_stale_or_missing") is True,
            "artifact_path": str(config.resolve(config.open_order_truth_path)),
        },
        "managed_order_registry": {
            "classification": managed_order_registry.get("classification"),
            "summary": managed_order_registry.get("summary") or {},
            "generated_at": managed_order_registry.get("generated_at"),
            "stale_or_missing": managed_order_registry.get("position_truth_managed_order_registry_stale_or_missing")
            is True,
            "artifact_path": str(config.resolve(config.managed_order_registry_path)),
        },
        "position_states": position_states,
        "summary": _summary(
            position_states=position_states,
            reconciliation=reconciliation,
            runtime_status=runtime_status,
            open_order_truth=open_order_truth,
            managed_order_registry=managed_order_registry,
        ),
        "event_state": _event_state(position_states=position_states, reconciliation=reconciliation, runtime_status=runtime_status),
        "artifact_paths": {
            "latest": str(config.resolve(config.output_path)),
            "authority": str(config.resolve(config.output_path)),
            "event_log": str(config.resolve(config.event_log_path)),
            "dashboard_projection": None
            if config.dashboard_projection_path is None
            else str(config.resolve(config.dashboard_projection_path)),
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "broker_lease": str(config.resolve(config.broker_lease_path)),
            "runtime_truth": str(config.resolve(config.runtime_truth_path)),
            "headless_status": str(config.resolve(config.headless_status_path)),
            "live_position_status": str(config.resolve(config.live_position_status_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
        },
    }
    payload["dmc_metadata"] = _build_dmc_metadata(config=config, payload=payload, generated_at=actual_now)
    return payload


def write_track_b_position_truth(
    *,
    config: TrackBPositionTruthMonitorConfig,
    payload: Mapping[str, Any],
    now: datetime | None = None,
) -> tuple[Path, list[dict[str, Any]]]:
    """Write latest state and append trade-outcome events for meaningful changes."""

    output_path = config.resolve(config.output_path)
    event_log_path = config.resolve(config.event_log_path)
    previous = _read_json(output_path)
    events = build_trade_outcome_events(previous=previous, current=payload, now=now)
    _write_json_atomic(output_path, dict(payload))
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_position_truth_projection(authority_payload=payload, authority_path=output_path),
        )
    if events:
        for event in events:
            append_bounded_jsonl(event_log_path, event)
    return output_path, events


def build_dashboard_position_truth_projection(*, authority_payload: Mapping[str, Any], authority_path: Path) -> dict[str, Any]:
    """Build a dashboard-only projection of the execution-core authority payload."""

    return {
        **dict(authority_payload),
        "schema_version": "track_b_position_truth_dashboard_projection_v1",
        **build_projection_metadata(source_authority_path=authority_path),
    }


def _build_dmc_metadata(
    *,
    config: TrackBPositionTruthMonitorConfig,
    payload: Mapping[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    artifact_paths = _mapping(payload.get("artifact_paths"))
    reconciliation = _mapping(payload.get("reconciliation"))
    open_order_truth = _mapping(payload.get("open_order_truth"))
    managed_order_registry = _mapping(payload.get("managed_order_registry"))
    runtime_status = _mapping(payload.get("runtime_status"))
    source_artifacts = [
        _dmc_source_artifact(
            artifact_family="track_b_paper_broker_reconciliation",
            path=artifact_paths.get("reconciliation") or str(config.resolve(config.reconciliation_path)),
            observed_at=reconciliation.get("generated_at"),
        ),
        _dmc_source_artifact(
            artifact_family="open_order_truth",
            path=artifact_paths.get("open_order_truth") or str(config.resolve(config.open_order_truth_path)),
            observed_at=open_order_truth.get("generated_at"),
        ),
        _dmc_source_artifact(
            artifact_family="managed_order_registry",
            path=artifact_paths.get("managed_order_registry") or str(config.resolve(config.managed_order_registry_path)),
            observed_at=managed_order_registry.get("generated_at"),
        ),
        _dmc_source_artifact(
            artifact_family="runtime_truth",
            path=artifact_paths.get("runtime_truth") or str(config.resolve(config.runtime_truth_path)),
            observed_at=runtime_status.get("runtime_truth_generated_at"),
        ),
    ]
    source_observed_at = _latest_observed_at(source_artifacts)
    return {
        "schema_version": "track_b_dmc_metadata_envelope_v1",
        "artifact_family": "latest_position_truth",
        "authority_tier": "Tier 1 – Canonical",
        "publisher_id": "track_b_position_truth.py",
        "owner_id": "Position Truth",
        "generated_at": generated_at.isoformat(),
        "source_observed_at": source_observed_at,
        "source_artifacts": source_artifacts,
        "refresh_scope": {
            "scope_type": "GLOBAL_COMPLETE",
            "account_scope": "Track B PAPER",
            "symbols": "ALL_TRACK_B_FUTURES_FROM_RECONCILIATION",
            "partial": False,
        },
        "retention_model": "rolling latest snapshot with append-only trade outcome event companion",
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


def build_trade_outcome_events(
    *,
    previous: Mapping[str, Any] | None,
    current: Mapping[str, Any],
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    previous_state = _mapping((previous or {}).get("event_state"))
    current_state = _mapping(current.get("event_state"))
    current_symbols = _mapping(current_state.get("symbols"))
    previous_symbols = _mapping(previous_state.get("symbols"))
    baseline = not bool(previous_state)
    events: list[dict[str, Any]] = []

    prev_recon = previous_state.get("reconciliation_classification")
    cur_recon = current_state.get("reconciliation_classification")
    if cur_recon and (baseline and cur_recon != "TRACK_B_PAPER_BROKER_RECONCILED" or prev_recon and prev_recon != cur_recon):
        events.append(
            _event(
                actual_now,
                "RECONCILIATION_CLEAN" if cur_recon == "TRACK_B_PAPER_BROKER_RECONCILED" else "RECONCILIATION_BLOCKED",
                symbol=None,
                classification=str(cur_recon),
                detail=f"Reconciliation classification is {cur_recon}.",
            )
        )

    if current_state.get("runtime_stopped_with_broker_exposure") is True and (
        baseline or previous_state.get("runtime_stopped_with_broker_exposure") is not True
    ):
        events.append(
            _event(
                actual_now,
                "RUNTIME_STOPPED_WITH_BROKER_EXPOSURE",
                symbol=None,
                classification="RUNTIME_STOPPED_WITH_BROKER_EXPOSURE",
                detail="PAPER runtime is not running while broker exposure exists.",
            )
        )

    for symbol, current_symbol_state in current_symbols.items():
        current_symbol = _mapping(current_symbol_state)
        previous_symbol = _mapping(previous_symbols.get(symbol))
        event_type = _symbol_event_type(previous_symbol=previous_symbol, current_symbol=current_symbol, baseline=baseline)
        if event_type is None:
            continue
        events.append(
            _event(
                actual_now,
                event_type,
                symbol=str(symbol),
                classification=str(current_symbol.get("classification") or ""),
                detail=str(current_symbol.get("detail") or ""),
                state=current_symbol,
            )
        )
    return events


def _classify_symbol(
    *,
    symbol: str,
    broker_positions: list[dict[str, Any]],
    open_orders: list[dict[str, Any]],
    lifecycle_positions: list[dict[str, Any]],
    review_required_positions: list[dict[str, Any]],
    unresolved_ownership: list[dict[str, Any]],
    unknown_orders: list[dict[str, Any]],
    known_managed_exit_orders: list[dict[str, Any]],
    open_order_truth: Mapping[str, Any],
    order_states: list[dict[str, Any]],
    reconciliation: Mapping[str, Any],
) -> dict[str, Any]:
    broker_rows = [row for row in broker_positions if _row_symbol(row) == symbol and _quantity(row) != Decimal("0")]
    order_rows = [row for row in open_orders if _row_symbol(row) == symbol]
    lifecycle_rows = [row for row in lifecycle_positions if _row_symbol(row) == symbol]
    review_rows = [row for row in review_required_positions if _row_symbol(row) == symbol]
    ownership_rows = [row for row in unresolved_ownership if _row_symbol(row) == symbol]
    unknown_order_rows = [row for row in unknown_orders if _row_symbol(row) == symbol]
    known_exit_rows = [row for row in known_managed_exit_orders if _row_symbol(row) == symbol]
    symbol_order_states = [row for row in order_states if str(row.get("symbol") or "").upper() == symbol]
    suspicious = _suspicious_order_findings_from_open_order_truth(symbol_order_states)
    close_order_states = [row for row in symbol_order_states if row.get("is_close_order") is True]
    open_order_truth_classes = {str(row.get("classification") or "") for row in symbol_order_states}
    open_order_truth_classification = str(open_order_truth.get("classification") or "")
    broker_qty = sum((_quantity(row) for row in broker_rows), Decimal("0"))
    lifecycle_qty = sum((_quantity(row) for row in lifecycle_rows), Decimal("0"))
    has_close_order = bool(close_order_states or known_exit_rows)
    broker_lifecycle_match = _broker_lifecycle_match(broker_rows=broker_rows, lifecycle_rows=lifecycle_rows)

    if (
        suspicious
        or open_order_truth_classification in {DUPLICATE_CLOSE_ORDER, BROKER_FLAT_WITH_OPEN_CLOSE_ORDER}
        or open_order_truth_classes & {SUSPICIOUS_ORDER_STATE, CLOSE_ORDER_STALE, CLOSE_ORDER_MARKETABLE_NOT_FILLED}
    ):
        classification = CLOSE_ORDER_SUSPICIOUS
        detail = "Open Order Truth reports suspicious or contradictory close-order state."
    elif review_rows:
        classification = REVIEW_REQUIRED
        detail = "Track B lifecycle has review-required position state."
    elif unknown_order_rows or open_order_truth_classes & {"UNKNOWN_OPEN_ORDER"}:
        classification = UNKNOWN_OPEN_ORDER
        detail = "Broker reports an open order that Track B cannot attribute."
    elif has_close_order or open_order_truth_classes & {OPEN_CLOSE_ORDER_WORKING}:
        classification = CLOSE_ORDER_WORKING
        detail = "A managed close order is working."
    elif broker_rows and lifecycle_rows and broker_lifecycle_match:
        classification = OPEN_MANAGED_MATCHED
        detail = "Broker and lifecycle both report the managed position."
    elif broker_rows and not lifecycle_rows:
        classification = BROKER_POSITION_REQUIRES_ADOPTION
        detail = "Broker reports exposure without a matching lifecycle OPEN_MANAGED position."
    elif lifecycle_rows and not broker_rows:
        classification = LIFECYCLE_POSITION_WITHOUT_BROKER
        detail = "Lifecycle reports an open position but broker truth is flat."
    elif reconciliation.get("broker_reconciled") is not True and (broker_rows or order_rows or lifecycle_rows or ownership_rows):
        classification = RECONCILIATION_BLOCKED
        detail = "Reconciliation is blocked for this symbol."
    else:
        classification = FLAT_CLEAN
        detail = "No broker, order, lifecycle, or ownership exposure."
    return {
        "symbol": symbol,
        "classification": classification,
        "detail": detail,
        "broker_quantity": _decimal_text(broker_qty),
        "lifecycle_quantity": _decimal_text(lifecycle_qty),
        "broker_positions": broker_rows,
        "open_orders": order_rows,
        "lifecycle_positions": lifecycle_rows,
        "review_required_positions": review_rows,
        "unresolved_submit_ownership": ownership_rows,
        "known_managed_exit_orders": known_exit_rows,
        "unknown_broker_open_orders": unknown_order_rows,
        "suspicious_order_findings": suspicious,
        "open_order_truth_states": symbol_order_states,
        "open_order_truth_classification": open_order_truth_classification,
        "open_order_truth_summary": open_order_truth.get("summary") or {},
        "open_order_truth_stale_or_missing": open_order_truth.get("position_truth_open_order_truth_stale_or_missing") is True,
        "open_managed_valid": classification == OPEN_MANAGED_MATCHED,
    }


def _suspicious_order_findings_from_open_order_truth(order_states: list[dict[str, Any]]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for state in order_states:
        reasons = list(state.get("suspicious_reasons") or [])
        condition_flags = list(state.get("condition_flags") or [])
        classification = str(state.get("classification") or "")
        if classification in {SUSPICIOUS_ORDER_STATE, CLOSE_ORDER_STALE, CLOSE_ORDER_MARKETABLE_NOT_FILLED}:
            reasons.extend(flag for flag in condition_flags if flag not in reasons)
        if not reasons:
            continue
        order = _mapping(state.get("order"))
        findings.append(
            {
                "broker_order_id": state.get("broker_order_id") or order.get("broker_order_id") or order.get("order_id"),
                "client_id": state.get("client_id") or order.get("client_id"),
                "perm_id": state.get("perm_id") or order.get("perm_id"),
                "local_symbol": state.get("local_symbol") or order.get("local_symbol"),
                "action": state.get("action") or order.get("action"),
                "limit_price": state.get("limit_price") or order.get("limit_price") or order.get("order_limit_price"),
                "status": state.get("status") or order.get("status"),
                "reasons": reasons,
                "source": "OPEN_ORDER_TRUTH",
            }
        )
    return findings


def _review_required_positions(
    *,
    reconciliation: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    lifecycle_reports: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = _list(live_position_status.get("review_required_positions"))
    if rows:
        return rows
    if _current_scope_review_required_count(reconciliation) <= 0:
        return []
    reports = []
    for report in lifecycle_reports:
        state = normalize_lifecycle_state(
            report.get("final_position_status")
            or report.get("lifecycle_status")
            or report.get("paper_lifecycle_classification")
            or report.get("strategy_managed_lifecycle_classification")
        )
        if report.get("review_required") is True or requires_operator_action(state):
            reports.append(report)
    return reports


def _open_order_truth_evidence(*, config: TrackBPositionTruthMonitorConfig, now: datetime) -> dict[str, Any]:
    """Return execution_core Open Order Truth evidence without using dashboard projections."""

    try:
        payload = build_track_b_open_order_truth(
            config=TrackBOpenOrderTruthConfig(
                repo_root=config.repo_root,
                output_path=config.open_order_truth_path,
                dashboard_projection_path=None,
                reconciliation_path=config.reconciliation_path,
                position_truth_path=config.output_path,
                live_position_status_path=config.live_position_status_path,
                market_data_root=config.market_data_root,
                lifecycle_root=config.lifecycle_root,
                marketable_unfilled_seconds=config.suspicious_marketable_seconds,
            ),
            now=now,
        )
        return {
            **payload,
            "position_truth_evidence_source": "OPEN_ORDER_TRUTH_BUILDER_DIRECT",
            "position_truth_open_order_truth_stale_or_missing": False,
        }
    except Exception as exc:  # noqa: BLE001 - position truth must fail loud in-artifact.
        artifact = _read_json(config.resolve(config.open_order_truth_path))
        return {
            **artifact,
            "position_truth_evidence_source": "OPEN_ORDER_TRUTH_AUTHORITY_ARTIFACT_FALLBACK",
            "position_truth_open_order_truth_stale_or_missing": True,
            "position_truth_open_order_truth_error": str(exc),
        }


def _managed_order_registry_evidence(*, config: TrackBPositionTruthMonitorConfig, now: datetime) -> dict[str, Any]:
    artifact = _read_json(config.resolve(config.managed_order_registry_path))
    age_seconds = _age_seconds(artifact.get("generated_at"), now) if artifact else None
    stale_or_missing = age_seconds is None or age_seconds > 180.0
    return {
        **artifact,
        "position_truth_managed_order_registry_stale_or_missing": stale_or_missing,
        "position_truth_managed_order_registry_age_seconds": age_seconds,
    }


def _runtime_status(*, runtime_truth: Mapping[str, Any], headless_status: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    generated_at = _parse_time(runtime_truth.get("generated_at") or runtime_truth.get("last_success_at"))
    ttl_seconds = _float_or_none(runtime_truth.get("freshness_ttl_seconds")) or 180.0
    runtime_truth_age_seconds = None if generated_at is None else max((now - generated_at).total_seconds(), 0.0)
    runtime_truth_fresh = runtime_truth_age_seconds is not None and runtime_truth_age_seconds <= ttl_seconds
    runtime_running = (
        runtime_truth_fresh
        and (
            runtime_truth.get("runtime_running") is True
            or runtime_truth.get("heartbeat_state") == "HEALTHY"
            or headless_status.get("runtime_running") is True
        )
    )
    return {
        "runtime_running": runtime_running,
        "runtime_instance_id": runtime_truth.get("runtime_instance_id") or headless_status.get("runtime_instance_id"),
        "producer_pid": runtime_truth.get("producer_pid") or headless_status.get("runtime_pid"),
        "source_commit": runtime_truth.get("source_commit") or headless_status.get("source_commit"),
        "heartbeat_state": runtime_truth.get("heartbeat_state"),
        "freshness_state": runtime_truth.get("freshness_state"),
        "writer_authority": runtime_truth.get("writer_authority"),
        "lane_count": runtime_truth.get("lane_count") or headless_status.get("lane_count"),
        "paper_trade_allowed": runtime_truth.get("paper_trade_allowed") or headless_status.get("paper_trade_allowed"),
        "runtime_truth_generated_at": runtime_truth.get("generated_at"),
        "runtime_truth_age_seconds": None if runtime_truth_age_seconds is None else round(runtime_truth_age_seconds, 3),
        "runtime_truth_fresh": bool(runtime_truth_fresh),
        "runtime_truth_ttl_seconds": ttl_seconds,
        "headless_status_generated_at": headless_status.get("generated_at"),
    }


def _summary(
    *,
    position_states: list[dict[str, Any]],
    reconciliation: Mapping[str, Any],
    runtime_status: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
    managed_order_registry: Mapping[str, Any],
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for state in position_states:
        classification = str(state.get("classification") or "UNKNOWN")
        counts[classification] = counts.get(classification, 0) + 1
    open_order_summary = _mapping(open_order_truth.get("summary"))
    managed_order_summary = _mapping(managed_order_registry.get("summary"))
    all_flat = reconciliation.get("broker_reconciled") is True and all(
        s.get("classification") == FLAT_CLEAN for s in position_states
    )
    active_hold = (
        reconciliation.get("broker_reconciled") is True
        and str(managed_order_registry.get("classification") or "") == ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING
        and bool(position_states)
        and all(s.get("classification") in {FLAT_CLEAN, OPEN_MANAGED_MATCHED} for s in position_states)
    )
    return {
        "overall_classification": "CLEAN_FLAT_READY"
        if all_flat
        else ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING
        if active_hold
        else "ATTENTION_REQUIRED",
        "active_hold_managed_timed_exit_pending": active_hold,
        "classification_counts": counts,
        "broker_reconciled": reconciliation.get("broker_reconciled"),
        "runtime_running": runtime_status.get("runtime_running"),
        "position_count": sum(1 for state in position_states if _decimal_or_none(state.get("broker_quantity")) not in {None, Decimal("0")}),
        "open_order_count": int(open_order_summary.get("open_order_count") or 0),
        "suspicious_order_count": int(open_order_summary.get("suspicious_order_count") or 0),
        "working_close_order_count": int(open_order_summary.get("working_close_order_count") or 0),
        "duplicate_close_order_group_count": int(open_order_summary.get("duplicate_close_order_group_count") or 0),
        "broker_flat_with_open_close_order_count": int(
            open_order_summary.get("broker_flat_with_open_close_order_count") or 0
        ),
        "broker_position_without_close_order_count": int(
            open_order_summary.get("broker_position_without_close_order_count") or 0
        ),
        "open_order_truth_classification": open_order_truth.get("classification"),
        "managed_order_registry_classification": managed_order_registry.get("classification"),
        "managed_order_count": int(managed_order_summary.get("managed_order_count") or 0),
        "managed_order_suspicious_order_count": int(managed_order_summary.get("suspicious_order_count") or 0),
        "managed_order_duplicate_close_order_count": int(managed_order_summary.get("duplicate_close_order_count") or 0),
        "managed_order_working_close_order_count": int(managed_order_summary.get("working_close_order_count") or 0),
        "managed_order_modifiable_close_order_count": int(managed_order_summary.get("modifiable_close_order_count") or 0),
    }


def _current_scope_review_required_count(reconciliation: Mapping[str, Any]) -> int:
    if "current_scope_review_required_count" in reconciliation:
        return int(reconciliation.get("current_scope_review_required_count") or 0)
    return int(reconciliation.get("review_required_count") or 0)


def _event_state(*, position_states: list[dict[str, Any]], reconciliation: Mapping[str, Any], runtime_status: Mapping[str, Any]) -> dict[str, Any]:
    symbols = {}
    broker_exposure = False
    for state in position_states:
        broker_qty = _decimal_or_none(state.get("broker_quantity")) or Decimal("0")
        if broker_qty != Decimal("0"):
            broker_exposure = True
        symbols[str(state.get("symbol"))] = {
            "classification": state.get("classification"),
            "detail": state.get("detail"),
            "broker_quantity": state.get("broker_quantity"),
            "open_order_ids": [str(row.get("broker_order_id") or row.get("order_id") or "") for row in _list(state.get("open_orders"))],
            "lifecycle_ids": [str(row.get("lifecycle_id") or "") for row in _list(state.get("lifecycle_positions"))],
            "suspicious_order_reasons": [
                f"{item.get('broker_order_id')}:{','.join(item.get('reasons') or [])}"
                for item in _list(state.get("suspicious_order_findings"))
            ],
        }
    return {
        "reconciliation_classification": reconciliation.get("classification"),
        "runtime_stopped_with_broker_exposure": runtime_status.get("runtime_running") is False and broker_exposure,
        "symbols": symbols,
    }


def _symbol_event_type(*, previous_symbol: Mapping[str, Any], current_symbol: Mapping[str, Any], baseline: bool) -> str | None:
    previous_classification = str(previous_symbol.get("classification") or "")
    current_classification = str(current_symbol.get("classification") or "")
    previous_suspicious = set(previous_symbol.get("suspicious_order_reasons") or [])
    current_suspicious = set(current_symbol.get("suspicious_order_reasons") or [])
    if current_suspicious and (baseline or current_suspicious != previous_suspicious):
        return "SUSPICIOUS_ORDER_DETECTED"
    if not baseline and previous_classification == current_classification and previous_symbol == current_symbol:
        return None
    if baseline and current_classification == FLAT_CLEAN:
        return None
    if current_classification == OPEN_MANAGED_MATCHED and previous_classification != OPEN_MANAGED_MATCHED:
        return "ADOPTED_OPEN_MANAGED"
    if current_classification == BROKER_POSITION_REQUIRES_ADOPTION and previous_classification in {"", FLAT_CLEAN}:
        return "ENTRY_FILLED"
    if current_classification in {CLOSE_ORDER_WORKING, CLOSE_ORDER_SUSPICIOUS} and previous_classification not in {CLOSE_ORDER_WORKING, CLOSE_ORDER_SUSPICIOUS}:
        return "CLOSE_ORDER_SUBMITTED"
    if current_classification == FLAT_CLEAN and previous_classification and previous_classification != FLAT_CLEAN:
        return "LIFECYCLE_CLOSED_FLAT"
    if current_classification != previous_classification:
        return "POSITION_STATE_CHANGED"
    return None


def _event(
    generated_at: datetime,
    event_type: str,
    *,
    symbol: str | None,
    classification: str,
    detail: str,
    state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_trade_outcome_event_v1",
        "generated_at": generated_at.isoformat(),
        "event_type": event_type,
        "symbol": symbol,
        "classification": classification,
        "detail": detail,
        "state": dict(state or {}),
        "read_only": True,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }


def _symbols(
    *,
    reconciliation: Mapping[str, Any],
    broker_positions: list[dict[str, Any]],
    open_orders: list[dict[str, Any]],
    lifecycle_positions: list[dict[str, Any]],
    review_required_positions: list[dict[str, Any]],
    unresolved_ownership: list[dict[str, Any]],
) -> list[str]:
    values = {str(item).upper() for item in reconciliation.get("symbols") or [] if str(item).strip()}
    for rows in (broker_positions, open_orders, lifecycle_positions, review_required_positions, unresolved_ownership):
        for row in rows:
            symbol = _row_symbol(row)
            if symbol:
                values.add(symbol)
    return sorted(values or {"MGC", "MNQ"})


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


def _broker_lifecycle_match(*, broker_rows: list[dict[str, Any]], lifecycle_rows: list[dict[str, Any]]) -> bool:
    if not broker_rows or not lifecycle_rows:
        return False
    broker_symbols = {_row_contract(row) for row in broker_rows}
    lifecycle_symbols = {_row_contract(row) for row in lifecycle_rows}
    return bool(broker_symbols & lifecycle_symbols)


def _row_symbol(row: Mapping[str, Any]) -> str:
    return str(
        row.get("track_b_root")
        or row.get("instrument_family")
        or row.get("symbol")
        or _symbol_from_local(row.get("local_symbol"))
        or ""
    ).upper()


def _row_contract(row: Mapping[str, Any]) -> str:
    return str(row.get("local_symbol") or row.get("contract") or row.get("contract_key") or _row_symbol(row)).upper()


def _symbol_from_local(value: Any) -> str:
    raw = str(value or "").upper()
    return "".join(ch for ch in raw if ch.isalpha())[:3]


def _quantity(row: Mapping[str, Any]) -> Decimal:
    value = row.get("quantity") or row.get("qty") or row.get("signed_quantity") or "0"
    return _decimal_or_none(value) or Decimal("0")


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
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


def _age_seconds(value: Any, now: datetime) -> float | None:
    parsed = _parse_time(value)
    if parsed is None:
        return None
    return round(max((now - parsed).total_seconds(), 0.0), 3)


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


def _bool(*values: Any) -> bool:
    for value in values:
        if isinstance(value, bool):
            return value
    return False


__all__ = [
    "BROKER_POSITION_REQUIRES_ADOPTION",
    "CLOSE_ORDER_SUSPICIOUS",
    "CLOSE_ORDER_WORKING",
    "FLAT_CLEAN",
    "LIFECYCLE_POSITION_WITHOUT_BROKER",
    "OPEN_MANAGED_MATCHED",
    "RECONCILIATION_BLOCKED",
    "REVIEW_REQUIRED",
    "UNKNOWN_OPEN_ORDER",
    "TrackBPositionTruthMonitorConfig",
    "build_dashboard_position_truth_projection",
    "build_track_b_position_truth",
    "build_trade_outcome_events",
    "write_track_b_position_truth",
]
