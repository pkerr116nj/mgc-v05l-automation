"""Read-only Track B PAPER open-order truth authority.

Open Order Truth authority lives in execution_core; dashboard artifacts are
projections and must not be used as runtime, readiness, or routing authority.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping


NO_OPEN_ORDERS = "NO_OPEN_ORDERS"
OPEN_CLOSE_ORDER_WORKING = "OPEN_CLOSE_ORDER_WORKING"
OPEN_ENTRY_ORDER_WORKING = "OPEN_ENTRY_ORDER_WORKING"
DUPLICATE_CLOSE_ORDER = "DUPLICATE_CLOSE_ORDER"
UNKNOWN_OPEN_ORDER = "UNKNOWN_OPEN_ORDER"
SUSPICIOUS_ORDER_STATE = "SUSPICIOUS_ORDER_STATE"
CLOSE_ORDER_STALE = "CLOSE_ORDER_STALE"
CLOSE_ORDER_MARKETABLE_NOT_FILLED = "CLOSE_ORDER_MARKETABLE_NOT_FILLED"
BROKER_FLAT_WITH_OPEN_CLOSE_ORDER = "BROKER_FLAT_WITH_OPEN_CLOSE_ORDER"
BROKER_POSITION_WITHOUT_CLOSE_ORDER = "BROKER_POSITION_WITHOUT_CLOSE_ORDER"
ORDER_TRUTH_STALE = "ORDER_TRUTH_STALE"

DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
)
DEFAULT_OPEN_ORDER_TRUTH_EVENTS = (
    Path("outputs") / "track_b_execution_core" / "open_order_truth" / "open_order_truth_events.jsonl"
)
DEFAULT_DASHBOARD_OPEN_ORDER_TRUTH_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_open_order_truth.json"
)
DEFAULT_RECONCILIATION_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_POSITION_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
)
DEFAULT_LIVE_POSITION_STATUS_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_trade_ledger"
    / "latest_track_b_live_position_status.json"
)
DEFAULT_MARKET_DATA_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
DEFAULT_LIFECYCLE_ROOT = (
    Path("outputs") / "track_b_execution_core" / "track_b_strategy_managed_paper_lifecycle"
)

_SENTINEL_FILLED_QUANTITY = Decimal("1e100")
_WORKING_ORDER_STATUSES = {"SUBMITTED", "PRESUBMITTED", "PENDING_SUBMIT", "APIPENDING"}
_TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELLED", "INACTIVE", "APICANCELLED"}


@dataclass(frozen=True)
class TrackBOpenOrderTruthConfig:
    repo_root: Path
    output_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    event_log_path: Path = DEFAULT_OPEN_ORDER_TRUTH_EVENTS
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_OPEN_ORDER_TRUTH_PROJECTION
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    live_position_status_path: Path = DEFAULT_LIVE_POSITION_STATUS_ARTIFACT
    market_data_root: Path = DEFAULT_MARKET_DATA_ROOT
    lifecycle_root: Path = DEFAULT_LIFECYCLE_ROOT
    artifact_max_age_seconds: float = 180.0
    close_order_stale_seconds: float = 900.0
    marketable_unfilled_seconds: float = 60.0

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_open_order_truth(
    *,
    config: TrackBOpenOrderTruthConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    reconciliation = _read_json(config.resolve(config.reconciliation_path))
    position_truth = _read_json(config.resolve(config.position_truth_path))
    live_position_status = _read_json(config.resolve(config.live_position_status_path))
    market_refs = _market_refs(config=config)
    lifecycle_reports = _load_lifecycle_reports(config.resolve(config.lifecycle_root))

    open_orders = _list(reconciliation.get("track_b_broker_open_orders"))
    broker_positions = _list(reconciliation.get("track_b_broker_positions"))
    lifecycle_positions = _list(reconciliation.get("track_b_lifecycle_positions"))
    unknown_orders = _list(reconciliation.get("unknown_broker_open_orders"))
    known_managed_exit_orders = _list(reconciliation.get("known_managed_exit_orders"))
    unresolved_ownership = _list(reconciliation.get("unresolved_submit_intent_ownership_records"))
    source_age = _age_seconds(reconciliation.get("generated_at"), actual_now)
    source_stale = source_age is None or source_age > float(config.artifact_max_age_seconds)

    order_states = [
        _classify_order(
            order=order,
            broker_positions=broker_positions,
            lifecycle_positions=lifecycle_positions,
            unknown_orders=unknown_orders,
            known_managed_exit_orders=known_managed_exit_orders,
            lifecycle_reports=lifecycle_reports,
            market_ref=market_refs.get(_row_symbol(order), {}),
            now=actual_now,
            close_order_stale_seconds=config.close_order_stale_seconds,
            marketable_unfilled_seconds=config.marketable_unfilled_seconds,
        )
        for order in open_orders
    ]
    duplicate_groups = _duplicate_close_groups(order_states)
    broker_positions_without_close = _broker_positions_without_close_order(
        broker_positions=broker_positions,
        order_states=order_states,
    )
    flat_with_close = [
        state
        for state in order_states
        if state.get("is_close_order") is True and not _matching_broker_positions(state.get("order") or {}, broker_positions)
    ]
    classification = _overall_classification(
        source_stale=source_stale,
        order_states=order_states,
        duplicate_groups=duplicate_groups,
        broker_positions_without_close=broker_positions_without_close,
        flat_with_close=flat_with_close,
    )
    payload = {
        "schema_version": "track_b_open_order_truth_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": reconciliation.get("live_money_eligible") is True,
        "classification": classification,
        "source_freshness": {
            "reconciliation_generated_at": reconciliation.get("generated_at"),
            "age_seconds": source_age,
            "ttl_seconds": float(config.artifact_max_age_seconds),
            "stale": bool(source_stale),
        },
        "broker_open_orders": open_orders,
        "broker_positions": broker_positions,
        "lifecycle_open_positions": lifecycle_positions,
        "unresolved_submit_ownership": unresolved_ownership,
        "order_states": order_states,
        "duplicate_close_order_groups": duplicate_groups,
        "broker_positions_without_close_order": broker_positions_without_close,
        "broker_flat_with_open_close_order": flat_with_close,
        "position_truth_summary": position_truth.get("summary") or {},
        "live_position_status_summary": {
            "open_position_count": live_position_status.get("open_position_count"),
            "review_required_count": len(_list(live_position_status.get("review_required_positions"))),
            "generated_at": live_position_status.get("as_of") or live_position_status.get("generated_at"),
        },
        "reconciliation": {
            "classification": reconciliation.get("classification"),
            "broker_reconciled": reconciliation.get("broker_reconciled"),
            "track_b_broker_open_order_count": reconciliation.get("track_b_broker_open_order_count"),
            "track_b_broker_position_count": reconciliation.get("track_b_broker_position_count"),
            "review_required_count": reconciliation.get("review_required_count"),
            "unresolved_submit_intent_ownership_count": reconciliation.get("unresolved_submit_intent_ownership_count"),
            "generated_at": reconciliation.get("generated_at"),
        },
        "summary": {
            "classification": classification,
            "open_order_count": len(open_orders),
            "working_close_order_count": sum(1 for state in order_states if state.get("is_close_order") is True),
            "working_entry_order_count": sum(1 for state in order_states if state.get("is_entry_order") is True),
            "suspicious_order_count": sum(1 for state in order_states if state.get("suspicious") is True),
            "duplicate_close_order_group_count": len(duplicate_groups),
            "broker_position_without_close_order_count": len(broker_positions_without_close),
            "broker_flat_with_open_close_order_count": len(flat_with_close),
        },
        "event_state": _event_state(
            classification=classification,
            order_states=order_states,
            duplicate_groups=duplicate_groups,
            broker_positions_without_close=broker_positions_without_close,
            flat_with_close=flat_with_close,
        ),
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "event_log": str(config.resolve(config.event_log_path)),
            "dashboard_projection": None
            if config.dashboard_projection_path is None
            else str(config.resolve(config.dashboard_projection_path)),
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "position_truth": str(config.resolve(config.position_truth_path)),
            "live_position_status": str(config.resolve(config.live_position_status_path)),
            "lifecycle_root": str(config.resolve(config.lifecycle_root)),
        },
    }
    return payload


def write_track_b_open_order_truth(
    *,
    config: TrackBOpenOrderTruthConfig,
    payload: Mapping[str, Any],
    now: datetime | None = None,
) -> tuple[Path, list[dict[str, Any]]]:
    output_path = config.resolve(config.output_path)
    event_log_path = config.resolve(config.event_log_path)
    previous = _read_json(output_path)
    events = build_open_order_truth_events(previous=previous, current=payload, now=now)
    _write_json_atomic(output_path, dict(payload))
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_open_order_truth_projection(authority_payload=payload, authority_path=output_path),
        )
    if events:
        event_log_path.parent.mkdir(parents=True, exist_ok=True)
        with event_log_path.open("a", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event, sort_keys=True) + "\n")
    return output_path, events


def build_dashboard_open_order_truth_projection(*, authority_payload: Mapping[str, Any], authority_path: Path) -> dict[str, Any]:
    return {
        **dict(authority_payload),
        "schema_version": "track_b_open_order_truth_dashboard_projection_v1",
        "projection_only": True,
        "not_routing_authority": True,
        "source_authority_path": str(authority_path),
        "authority_owner": "execution_core",
        "operator_dashboard_display_only": True,
    }


def build_open_order_truth_events(
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
            "schema_version": "track_b_open_order_truth_event_v1",
            "event_type": "OPEN_ORDER_TRUTH_CHANGED",
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


def _classify_order(
    *,
    order: Mapping[str, Any],
    broker_positions: list[dict[str, Any]],
    lifecycle_positions: list[dict[str, Any]],
    unknown_orders: list[dict[str, Any]],
    known_managed_exit_orders: list[dict[str, Any]],
    lifecycle_reports: list[dict[str, Any]],
    market_ref: Mapping[str, Any],
    now: datetime,
    close_order_stale_seconds: float,
    marketable_unfilled_seconds: float,
) -> dict[str, Any]:
    lifecycle_report = _lifecycle_report_for_order(order=order, lifecycle_reports=lifecycle_reports)
    reasons = _suspicious_reasons(order=order, lifecycle_report=lifecycle_report)
    is_close_order = _is_close_order(
        order=order,
        broker_positions=broker_positions,
        lifecycle_positions=lifecycle_positions,
        known_managed_exit_orders=known_managed_exit_orders,
    )
    is_unknown = _order_in(order, unknown_orders)
    age_seconds = _order_age_seconds(order, now)
    close_attempt = _mapping(lifecycle_report.get("close_submit_attempt"))
    close_attempt_age = _age_seconds(close_attempt.get("submitted_at"), now)
    if close_attempt_age is not None:
        age_seconds = max(age_seconds or 0.0, close_attempt_age)
    marketable = _is_marketable(order=order, market_ref=market_ref)
    stale = bool(is_close_order and age_seconds is not None and age_seconds >= float(close_order_stale_seconds))
    flat_with_close = bool(is_close_order and not _matching_broker_positions(order, broker_positions))
    condition_flags = []
    if marketable and age_seconds is not None and age_seconds >= float(marketable_unfilled_seconds):
        condition_flags.append("marketable_unfilled_beyond_threshold")
    if stale:
        condition_flags.append("close_order_stale")
    if flat_with_close:
        condition_flags.append("broker_flat_with_open_close_order")
    classification = OPEN_CLOSE_ORDER_WORKING if is_close_order else OPEN_ENTRY_ORDER_WORKING
    if is_unknown:
        classification = UNKNOWN_OPEN_ORDER
    if stale:
        classification = CLOSE_ORDER_STALE
    if marketable and age_seconds is not None and age_seconds >= float(marketable_unfilled_seconds):
        classification = CLOSE_ORDER_MARKETABLE_NOT_FILLED
    if reasons:
        classification = SUSPICIOUS_ORDER_STATE
    return {
        "classification": classification,
        "order": dict(order),
        "broker_order_id": order.get("broker_order_id") or order.get("order_id"),
        "perm_id": order.get("perm_id"),
        "client_id": order.get("client_id"),
        "account_id": order.get("account_id"),
        "symbol": _row_symbol(order),
        "local_symbol": order.get("local_symbol"),
        "action": _action(order),
        "quantity": _decimal_text(_quantity(order)),
        "status": order.get("status"),
        "limit_price": order.get("limit_price") or order.get("order_limit_price"),
        "age_seconds": age_seconds,
        "market_reference": dict(market_ref),
        "marketable": bool(marketable),
        "working": _order_working(order),
        "is_close_order": bool(is_close_order),
        "is_entry_order": not bool(is_close_order),
        "unknown_open_order": bool(is_unknown),
        "suspicious": bool(reasons),
        "suspicious_reasons": reasons,
        "condition_flags": condition_flags,
        "duplicate_key": _close_duplicate_key(order) if is_close_order else None,
    }


def _overall_classification(
    *,
    source_stale: bool,
    order_states: list[dict[str, Any]],
    duplicate_groups: list[dict[str, Any]],
    broker_positions_without_close: list[dict[str, Any]],
    flat_with_close: list[dict[str, Any]],
) -> str:
    if source_stale:
        return ORDER_TRUTH_STALE
    if duplicate_groups:
        return DUPLICATE_CLOSE_ORDER
    if any(state.get("classification") == SUSPICIOUS_ORDER_STATE for state in order_states):
        return SUSPICIOUS_ORDER_STATE
    if flat_with_close:
        return BROKER_FLAT_WITH_OPEN_CLOSE_ORDER
    if any(state.get("classification") == CLOSE_ORDER_MARKETABLE_NOT_FILLED for state in order_states):
        return CLOSE_ORDER_MARKETABLE_NOT_FILLED
    if any(state.get("classification") == CLOSE_ORDER_STALE for state in order_states):
        return CLOSE_ORDER_STALE
    if any(state.get("classification") == UNKNOWN_OPEN_ORDER for state in order_states):
        return UNKNOWN_OPEN_ORDER
    if any(state.get("is_close_order") is True for state in order_states):
        return OPEN_CLOSE_ORDER_WORKING
    if any(state.get("is_entry_order") is True for state in order_states):
        return OPEN_ENTRY_ORDER_WORKING
    if broker_positions_without_close:
        return BROKER_POSITION_WITHOUT_CLOSE_ORDER
    return NO_OPEN_ORDERS


def _suspicious_reasons(*, order: Mapping[str, Any], lifecycle_report: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    filled = _decimal_or_none(order.get("filled_quantity") or order.get("filled"))
    if filled is not None and abs(filled) >= _SENTINEL_FILLED_QUANTITY:
        reasons.append("sentinel_filled_quantity")
    if order.get("remaining_quantity") in {None, ""}:
        reasons.append("missing_remaining_quantity")
    close_attempt = _mapping(lifecycle_report.get("close_submit_attempt"))
    diagnostics = _mapping(close_attempt.get("submit_diagnostics"))
    if close_attempt and diagnostics.get("execDetails_seen") is False:
        reasons.append("open_close_order_without_execDetails")
    return reasons


def _duplicate_close_groups(order_states: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for state in order_states:
        if state.get("is_close_order") is not True or state.get("working") is not True:
            continue
        key = str(state.get("duplicate_key") or "")
        if key:
            groups.setdefault(key, []).append(state)
    return [
        {
            "duplicate_key": key,
            "count": len(rows),
            "orders": rows,
        }
        for key, rows in sorted(groups.items())
        if len(rows) > 1
    ]


def _broker_positions_without_close_order(
    *,
    broker_positions: list[dict[str, Any]],
    order_states: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for position in broker_positions:
        if _quantity(position) == Decimal("0"):
            continue
        if not any(state.get("is_close_order") is True and _same_contract(state.get("order") or {}, position) for state in order_states):
            rows.append(dict(position))
    return rows


def _event_state(
    *,
    classification: str,
    order_states: list[dict[str, Any]],
    duplicate_groups: list[dict[str, Any]],
    broker_positions_without_close: list[dict[str, Any]],
    flat_with_close: list[dict[str, Any]],
) -> dict[str, Any]:
    orders = [
        {
            "classification": state.get("classification"),
            "broker_order_id": str(state.get("broker_order_id") or ""),
            "perm_id": str(state.get("perm_id") or ""),
            "symbol": state.get("symbol"),
            "local_symbol": state.get("local_symbol"),
            "action": state.get("action"),
            "quantity": state.get("quantity"),
            "status": state.get("status"),
            "suspicious_reasons": state.get("suspicious_reasons") or [],
        }
        for state in order_states
    ]
    signature = json.dumps(
        {
            "classification": classification,
            "orders": orders,
            "duplicates": [
                {"duplicate_key": group.get("duplicate_key"), "count": group.get("count")} for group in duplicate_groups
            ],
            "broker_positions_without_close": [_contract_key(row) for row in broker_positions_without_close],
            "flat_with_close": [str(state.get("broker_order_id") or "") for state in flat_with_close],
        },
        sort_keys=True,
    )
    return {
        "classification": classification,
        "signature": signature,
        "orders": orders,
        "duplicate_close_order_group_count": len(duplicate_groups),
        "broker_position_without_close_order_count": len(broker_positions_without_close),
        "broker_flat_with_open_close_order_count": len(flat_with_close),
    }


def _is_close_order(
    *,
    order: Mapping[str, Any],
    broker_positions: list[dict[str, Any]],
    lifecycle_positions: list[dict[str, Any]],
    known_managed_exit_orders: list[dict[str, Any]],
) -> bool:
    if _order_in(order, known_managed_exit_orders):
        return True
    action = _action(order)
    if action not in {"BUY", "SELL"}:
        return False
    for position in broker_positions:
        if _same_contract(order, position):
            qty = _quantity(position)
            if qty > 0 and action == "SELL":
                return True
            if qty < 0 and action == "BUY":
                return True
    for position in lifecycle_positions:
        if _same_contract(order, position):
            side = str(position.get("side") or "").upper()
            if side == "LONG" and action == "SELL":
                return True
            if side == "SHORT" and action == "BUY":
                return True
    intent_type = str(order.get("intent_type") or order.get("order_intent_type") or "").upper()
    if "TO_CLOSE" in intent_type or "CLOSE" in intent_type:
        return True
    return False


def _matching_broker_positions(order: Mapping[str, Any], broker_positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [position for position in broker_positions if _same_contract(order, position) and _quantity(position) != Decimal("0")]


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


def _order_in(order: Mapping[str, Any], rows: list[dict[str, Any]]) -> bool:
    order_id = str(order.get("broker_order_id") or order.get("order_id") or "")
    perm_id = str(order.get("perm_id") or "")
    for row in rows:
        row_order_id = str(row.get("broker_order_id") or row.get("order_id") or "")
        row_perm_id = str(row.get("perm_id") or "")
        if order_id and row_order_id and order_id == row_order_id:
            return True
        if perm_id and row_perm_id and perm_id == row_perm_id:
            return True
    return False


def _close_duplicate_key(order: Mapping[str, Any]) -> str:
    return "|".join(
        [
            str(order.get("account_id") or ""),
            str(order.get("con_id") or order.get("local_symbol") or _row_symbol(order)).upper(),
            _action(order),
            _decimal_text(_quantity(order)),
        ]
    )


def _is_marketable(*, order: Mapping[str, Any], market_ref: Mapping[str, Any]) -> bool:
    limit_price = _decimal_or_none(order.get("limit_price") or order.get("order_limit_price") or order.get("lmt_price"))
    reference = _decimal_or_none(market_ref.get("reference_price"))
    if limit_price is None or reference is None:
        return False
    action = _action(order)
    return (action == "SELL" and reference >= limit_price) or (action == "BUY" and reference <= limit_price)


def _order_age_seconds(order: Mapping[str, Any], now: datetime) -> float | None:
    timestamp = _parse_time(
        order.get("submitted_at")
        or order.get("created_at")
        or order.get("updated_at")
        or order.get("last_update_at")
        or order.get("order_time")
    )
    if timestamp is None:
        return None
    return round(max((now - timestamp).total_seconds(), 0.0), 3)


def _order_working(order: Mapping[str, Any]) -> bool:
    status = str(order.get("status") or "").upper()
    if not status:
        return True
    if status in _TERMINAL_ORDER_STATUSES:
        return False
    return status in _WORKING_ORDER_STATUSES or status not in _TERMINAL_ORDER_STATUSES


def _market_refs(*, config: TrackBOpenOrderTruthConfig) -> dict[str, dict[str, Any]]:
    refs: dict[str, dict[str, Any]] = {}
    root = config.resolve(config.market_data_root)
    if not root.exists():
        return refs
    for symbol_dir in root.glob("*"):
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


def _lifecycle_report_for_order(*, order: Mapping[str, Any], lifecycle_reports: list[dict[str, Any]]) -> dict[str, Any]:
    order_id = str(order.get("broker_order_id") or order.get("order_id") or "")
    if not order_id:
        return {}
    for report in lifecycle_reports:
        close_attempt = _mapping(report.get("close_submit_attempt"))
        if str(close_attempt.get("broker_order_id") or "") == order_id:
            return report
    return {}


def _contract_key(row: Mapping[str, Any]) -> str:
    return str(row.get("con_id") or row.get("local_symbol") or row.get("contract_key") or _row_symbol(row)).upper()


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
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _list(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


__all__ = [
    "BROKER_FLAT_WITH_OPEN_CLOSE_ORDER",
    "BROKER_POSITION_WITHOUT_CLOSE_ORDER",
    "CLOSE_ORDER_MARKETABLE_NOT_FILLED",
    "CLOSE_ORDER_STALE",
    "DUPLICATE_CLOSE_ORDER",
    "NO_OPEN_ORDERS",
    "OPEN_CLOSE_ORDER_WORKING",
    "OPEN_ENTRY_ORDER_WORKING",
    "ORDER_TRUTH_STALE",
    "SUSPICIOUS_ORDER_STATE",
    "UNKNOWN_OPEN_ORDER",
    "TrackBOpenOrderTruthConfig",
    "build_dashboard_open_order_truth_projection",
    "build_open_order_truth_events",
    "build_track_b_open_order_truth",
    "write_track_b_open_order_truth",
]
