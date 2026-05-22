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

_CLOSE_ACTIONS = {"SELL", "BUY"}
_SENTINEL_FILLED_QUANTITY = Decimal("1e100")


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
    market_refs = _market_refs(config=config)

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
            lifecycle_reports=lifecycle_reports,
            market_ref=market_refs.get(symbol, {}),
            reconciliation=reconciliation,
            now=actual_now,
            suspicious_marketable_seconds=config.suspicious_marketable_seconds,
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
        "position_states": position_states,
        "summary": _summary(position_states=position_states, reconciliation=reconciliation, runtime_status=runtime_status),
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
        },
    }
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
        event_log_path.parent.mkdir(parents=True, exist_ok=True)
        with event_log_path.open("a", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event, sort_keys=True) + "\n")
    return output_path, events


def build_dashboard_position_truth_projection(*, authority_payload: Mapping[str, Any], authority_path: Path) -> dict[str, Any]:
    """Build a dashboard-only projection of the execution-core authority payload."""

    return {
        **dict(authority_payload),
        "schema_version": "track_b_position_truth_dashboard_projection_v1",
        "projection_only": True,
        "not_routing_authority": True,
        "source_authority_path": str(authority_path),
        "authority_owner": "execution_core",
        "operator_dashboard_display_only": True,
    }


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
    lifecycle_reports: list[dict[str, Any]],
    market_ref: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    now: datetime,
    suspicious_marketable_seconds: float,
) -> dict[str, Any]:
    broker_rows = [row for row in broker_positions if _row_symbol(row) == symbol and _quantity(row) != Decimal("0")]
    order_rows = [row for row in open_orders if _row_symbol(row) == symbol]
    lifecycle_rows = [row for row in lifecycle_positions if _row_symbol(row) == symbol]
    review_rows = [row for row in review_required_positions if _row_symbol(row) == symbol]
    ownership_rows = [row for row in unresolved_ownership if _row_symbol(row) == symbol]
    unknown_order_rows = [row for row in unknown_orders if _row_symbol(row) == symbol]
    known_exit_rows = [row for row in known_managed_exit_orders if _row_symbol(row) == symbol]
    suspicious = _suspicious_orders(
        orders=order_rows,
        lifecycle_reports=lifecycle_reports,
        market_ref=market_ref,
        now=now,
        suspicious_marketable_seconds=suspicious_marketable_seconds,
    )
    broker_qty = sum((_quantity(row) for row in broker_rows), Decimal("0"))
    lifecycle_qty = sum((_quantity(row) for row in lifecycle_rows), Decimal("0"))
    has_close_order = any(_is_close_order(row, lifecycle_reports=lifecycle_reports) for row in order_rows)
    broker_lifecycle_match = _broker_lifecycle_match(broker_rows=broker_rows, lifecycle_rows=lifecycle_rows)

    if suspicious:
        classification = CLOSE_ORDER_SUSPICIOUS
        detail = "One or more open close orders have suspicious broker/order state."
    elif review_rows:
        classification = REVIEW_REQUIRED
        detail = "Track B lifecycle has review-required position state."
    elif unknown_order_rows:
        classification = UNKNOWN_OPEN_ORDER
        detail = "Broker reports an open order that Track B cannot attribute."
    elif has_close_order or known_exit_rows:
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
        "market_reference": dict(market_ref),
        "open_managed_valid": classification == OPEN_MANAGED_MATCHED,
    }


def _suspicious_orders(
    *,
    orders: list[dict[str, Any]],
    lifecycle_reports: list[dict[str, Any]],
    market_ref: Mapping[str, Any],
    now: datetime,
    suspicious_marketable_seconds: float,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for order in orders:
        reasons: list[str] = []
        filled = _decimal_or_none(order.get("filled_quantity") or order.get("filled"))
        if filled is not None and abs(filled) >= _SENTINEL_FILLED_QUANTITY:
            reasons.append("sentinel_filled_quantity")
        if order.get("remaining_quantity") in {None, ""}:
            reasons.append("missing_remaining_quantity")
        lifecycle = _lifecycle_report_for_order(order=order, lifecycle_reports=lifecycle_reports)
        close_attempt = _mapping(lifecycle.get("close_submit_attempt"))
        diagnostics = _mapping(close_attempt.get("submit_diagnostics"))
        if close_attempt and diagnostics.get("execDetails_seen") is False:
            reasons.append("open_close_order_without_execDetails")
        if _is_marketable_unfilled_beyond_threshold(
            order=order,
            lifecycle=close_attempt,
            market_ref=market_ref,
            now=now,
            threshold_seconds=suspicious_marketable_seconds,
        ):
            reasons.append("marketable_unfilled_beyond_threshold")
        if reasons:
            findings.append(
                {
                    "broker_order_id": order.get("broker_order_id") or order.get("order_id"),
                    "client_id": order.get("client_id"),
                    "perm_id": order.get("perm_id"),
                    "local_symbol": order.get("local_symbol"),
                    "action": order.get("action"),
                    "limit_price": order.get("limit_price") or order.get("order_limit_price"),
                    "status": order.get("status"),
                    "reasons": reasons,
                }
            )
    return findings


def _is_marketable_unfilled_beyond_threshold(
    *,
    order: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    market_ref: Mapping[str, Any],
    now: datetime,
    threshold_seconds: float,
) -> bool:
    limit_price = _decimal_or_none(order.get("limit_price") or order.get("order_limit_price"))
    reference = _decimal_or_none(market_ref.get("reference_price"))
    if limit_price is None or reference is None:
        return False
    action = str(order.get("action") or "").upper()
    marketable = (action == "SELL" and reference >= limit_price) or (action == "BUY" and reference <= limit_price)
    if not marketable:
        return False
    submitted_at = _parse_time(lifecycle.get("submitted_at") or order.get("submitted_at") or order.get("created_at"))
    if submitted_at is None:
        return False
    return (now - submitted_at).total_seconds() >= float(threshold_seconds)


def _lifecycle_report_for_order(*, order: Mapping[str, Any], lifecycle_reports: list[dict[str, Any]]) -> dict[str, Any]:
    order_id = str(order.get("broker_order_id") or order.get("order_id") or "")
    if not order_id:
        return {}
    for report in lifecycle_reports:
        close_attempt = _mapping(report.get("close_submit_attempt"))
        if str(close_attempt.get("broker_order_id") or "") == order_id:
            return report
    return {}


def _is_close_order(row: Mapping[str, Any], *, lifecycle_reports: list[dict[str, Any]]) -> bool:
    action = str(row.get("action") or "").upper()
    if action not in _CLOSE_ACTIONS:
        return False
    if _lifecycle_report_for_order(order=row, lifecycle_reports=lifecycle_reports):
        return True
    return str(row.get("track_b_root") or row.get("symbol") or "").upper() in {"MGC", "MNQ", "GC", "NQ", "ES", "MES"}


def _review_required_positions(
    *,
    reconciliation: Mapping[str, Any],
    live_position_status: Mapping[str, Any],
    lifecycle_reports: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = _list(live_position_status.get("review_required_positions"))
    if rows:
        return rows
    if int(reconciliation.get("review_required_count") or 0) <= 0:
        return []
    reports = []
    for report in lifecycle_reports:
        if report.get("review_required") is True or str(report.get("final_position_status") or "").upper() == "REVIEW_REQUIRED":
            reports.append(report)
    return reports


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


def _summary(*, position_states: list[dict[str, Any]], reconciliation: Mapping[str, Any], runtime_status: Mapping[str, Any]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for state in position_states:
        classification = str(state.get("classification") or "UNKNOWN")
        counts[classification] = counts.get(classification, 0) + 1
    return {
        "overall_classification": "CLEAN_FLAT_READY" if reconciliation.get("broker_reconciled") is True and all(s.get("classification") == FLAT_CLEAN for s in position_states) else "ATTENTION_REQUIRED",
        "classification_counts": counts,
        "broker_reconciled": reconciliation.get("broker_reconciled"),
        "runtime_running": runtime_status.get("runtime_running"),
        "position_count": sum(1 for state in position_states if _decimal_or_none(state.get("broker_quantity")) not in {None, Decimal("0")}),
        "open_order_count": sum(len(_list(state.get("open_orders"))) for state in position_states),
        "suspicious_order_count": sum(len(_list(state.get("suspicious_order_findings"))) for state in position_states),
    }


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


def _market_refs(*, config: TrackBPositionTruthMonitorConfig) -> dict[str, dict[str, Any]]:
    refs: dict[str, dict[str, Any]] = {}
    for symbol_dir in config.resolve(config.market_data_root).glob("*"):
        if not symbol_dir.is_dir():
            continue
        symbol = symbol_dir.name.upper()
        payload = _read_json(symbol_dir / "1m" / "latest_runtime_candles.json")
        bars = _list(payload.get("candles") or payload.get("bars"))
        if not bars:
            continue
        last = bars[-1]
        close = last.get("close") or last.get("last_price")
        refs[symbol] = {
            "reference_price": close,
            "reference_source": str(symbol_dir / "1m" / "latest_runtime_candles.json"),
            "bar_end": last.get("bar_end") or last.get("timestamp") or last.get("candle_timestamp"),
            "generated_at": payload.get("generated_at"),
        }
    return refs


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


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


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
