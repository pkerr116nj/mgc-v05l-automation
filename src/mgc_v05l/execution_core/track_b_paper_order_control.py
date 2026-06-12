"""Track B PAPER exact order-control authority.

This module is intentionally broker-transport agnostic.  It owns the durable
identity and exact-order resolution rules used before any PAPER modify/cancel.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from .models import to_jsonable

EXPECTED_PAPER_ACCOUNT_ID = "DUM882026"
SCHEMA_VERSION = "track_b_paper_order_control_v1"
DEFAULT_TRACK_B_PAPER_ORDER_CONTROL_ROOT = Path("outputs") / "track_b_execution_core" / "paper_order_control"
DEFAULT_TRACK_B_PAPER_ORDER_CONTROL_JSONL = DEFAULT_TRACK_B_PAPER_ORDER_CONTROL_ROOT / "track_b_paper_order_control.jsonl"
DEFAULT_TRACK_B_PAPER_ORDER_CONTROL_LATEST_JSON = DEFAULT_TRACK_B_PAPER_ORDER_CONTROL_ROOT / "latest_track_b_paper_order_control.json"
DEFAULT_TRACK_B_PAPER_ORDER_OPERATOR_REQUIRED_JSON = (
    DEFAULT_TRACK_B_PAPER_ORDER_CONTROL_ROOT / "operator_cancel_required_exact_order.json"
)

TERMINAL_STATUSES = {"CANCELLED", "CANCELED", "FILLED", "INACTIVE", "REJECTED", "EXPIRED"}
WORKING_STATUSES = {"SUBMITTED", "PRESUBMITTED", "PENDING_SUBMIT", "APIPENDING", "HELD", "PENDINGCANCEL", "PENDCANCEL"}


class PaperOrderControlError(ValueError):
    """Raised when exact PAPER order control cannot be proven safe."""


class PaperOrderControlTransport(Protocol):
    """Minimal transport expected by exact modify/cancel authority."""

    def cancel_order(self, *, order_id: int, client_id: int | None = None, perm_id: int | None = None) -> None:
        ...

    def modify_order(
        self,
        *,
        order_id: int,
        limit_price: float,
        client_id: int | None = None,
        perm_id: int | None = None,
    ) -> None:
        ...


@dataclass(frozen=True)
class TrackBPaperOrderRecord:
    order_id: int | str
    perm_id: int | str | None
    client_id: int | str | None
    account_id: str
    con_id: int | str
    local_symbol: str
    action: str
    quantity: int | float | str
    order_type: str
    limit_price: int | float | str | None
    order_ref: str | None
    originating_component: str
    lane_id: str | None
    timestamp: datetime | str
    mode: str = "PAPER"
    status: str | None = None
    source_artifact_path: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "mode": str(self.mode or "").strip().upper(),
            "order_id": _text(self.order_id),
            "perm_id": _text_or_none(self.perm_id),
            "client_id": _text_or_none(self.client_id),
            "account_id": _text(self.account_id),
            "con_id": _text(self.con_id),
            "local_symbol": _text(self.local_symbol),
            "action": _text(self.action).upper(),
            "quantity": _quantity_text(self.quantity),
            "order_type": _text(self.order_type).upper(),
            "limit_price": None if self.limit_price is None else _text(self.limit_price),
            "order_ref": _text_or_none(self.order_ref),
            "originating_component": _text(self.originating_component),
            "lane_id": _text_or_none(self.lane_id),
            "timestamp": _timestamp(self.timestamp),
            "status": _text_or_none(self.status),
            "source_artifact_path": _text_or_none(self.source_artifact_path),
            "extra": dict(self.extra or {}),
        }
        validate_paper_order_record(payload)
        return payload


@dataclass(frozen=True)
class OrderControlResolution:
    classification: str
    order: dict[str, Any] | None
    ownership: dict[str, Any] | None
    match_method: str | None
    reason: str | None = None

    @property
    def allowed(self) -> bool:
        return self.classification == "TRACK_B_PAPER_ORDER_CONTROL_RESOLVED"


def validate_paper_order_record(payload: Mapping[str, Any]) -> None:
    required = (
        "schema_version",
        "mode",
        "order_id",
        "account_id",
        "con_id",
        "local_symbol",
        "action",
        "quantity",
        "order_type",
        "originating_component",
        "timestamp",
    )
    missing = [field for field in required if _text(payload.get(field)) == ""]
    if missing:
        raise PaperOrderControlError(f"paper order-control record missing required fields: {', '.join(missing)}")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise PaperOrderControlError("paper order-control schema_version is unsupported")
    if _text(payload.get("mode")).upper() != "PAPER":
        raise PaperOrderControlError("paper order-control requires mode=PAPER")
    if _text(payload.get("account_id")) != EXPECTED_PAPER_ACCOUNT_ID:
        raise PaperOrderControlError("paper order-control requires account DUM882026")
    if _text(payload.get("action")).upper() not in {"BUY", "SELL"}:
        raise PaperOrderControlError("paper order-control action must be BUY or SELL")
    if _text(payload.get("order_type")).upper() not in {"LMT", "MKT", "STP", "STP LMT"}:
        raise PaperOrderControlError("paper order-control order_type is unsupported")
    if float(_quantity_text(payload.get("quantity"))) <= 0:
        raise PaperOrderControlError("paper order-control quantity must be positive")


def append_paper_order_control_record(
    record: TrackBPaperOrderRecord | Mapping[str, Any],
    *,
    jsonl_path: Path = DEFAULT_TRACK_B_PAPER_ORDER_CONTROL_JSONL,
    latest_path: Path = DEFAULT_TRACK_B_PAPER_ORDER_CONTROL_LATEST_JSON,
) -> dict[str, Any]:
    payload = record.to_payload() if isinstance(record, TrackBPaperOrderRecord) else dict(record)
    validate_paper_order_record(payload)
    payload = dict(payload)
    payload.setdefault("recorded_at", datetime.now(UTC).isoformat())
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(to_jsonable(payload), sort_keys=True) + "\n")
    records = load_paper_order_control_records(jsonl_path)
    latest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "record_count": len(records),
        "working_owned_order_count": len([row for row in records if _is_working(row)]),
        "latest_record": records[-1] if records else None,
        "latest_working_owned_orders": [row for row in records if _is_working(row)],
    }
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(to_jsonable(latest), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def load_paper_order_control_records(path: Path = DEFAULT_TRACK_B_PAPER_ORDER_CONTROL_JSONL) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def resolve_owned_working_order(
    *,
    request: Mapping[str, Any],
    broker_open_orders: Sequence[Mapping[str, Any]],
    ownership_records: Sequence[Mapping[str, Any]],
) -> OrderControlResolution:
    open_rows = [_normalize_order(row) for row in broker_open_orders if _is_working(row)]
    owned_rows = [_normalize_order(row) for row in ownership_records]
    request_row = _normalize_order(request)
    if request_row.get("account_id") and request_row.get("account_id") != EXPECTED_PAPER_ACCOUNT_ID:
        return OrderControlResolution("TRACK_B_PAPER_ORDER_CONTROL_BLOCKED", None, None, None, "wrong_account")

    matchers = (
        ("perm_id", _match_perm_id),
        ("order_id_client_id", _match_order_id_client_id),
        ("account_con_id_action_qty_order_ref", _match_composite_identity),
    )
    for method, matcher in matchers:
        open_match = _unique_match(open_rows, request_row, matcher)
        if open_match is None:
            continue
        owned_match = _unique_match(owned_rows, open_match, matcher) or _unique_match(owned_rows, request_row, matcher)
        if owned_match is None:
            return OrderControlResolution(
                "TRACK_B_PAPER_ORDER_CONTROL_BLOCKED",
                open_match,
                None,
                method,
                "open_order_is_not_owned_by_track_b",
            )
        if _identity_conflict(open_match, owned_match):
            return OrderControlResolution(
                "TRACK_B_PAPER_ORDER_CONTROL_BLOCKED",
                open_match,
                owned_match,
                method,
                "owned_order_identity_conflict",
            )
        return OrderControlResolution("TRACK_B_PAPER_ORDER_CONTROL_RESOLVED", open_match, owned_match, method)
    return OrderControlResolution("TRACK_B_PAPER_ORDER_CONTROL_BLOCKED", None, None, None, "exact_order_not_found")


def exact_cancel_owned_order(
    *,
    transport: PaperOrderControlTransport,
    request: Mapping[str, Any],
    broker_open_orders: Sequence[Mapping[str, Any]],
    ownership_records: Sequence[Mapping[str, Any]],
    confirmation_open_orders: Sequence[Mapping[str, Any]] | None = None,
    operator_required_path: Path = DEFAULT_TRACK_B_PAPER_ORDER_OPERATOR_REQUIRED_JSON,
) -> dict[str, Any]:
    resolution = resolve_owned_working_order(
        request=request,
        broker_open_orders=broker_open_orders,
        ownership_records=ownership_records,
    )
    if not resolution.allowed:
        return _control_result("EXACT_CANCEL_BLOCKED", resolution=resolution)
    order = dict(resolution.order or {})
    transport.cancel_order(
        order_id=int(order["order_id"]),
        client_id=_int_or_none(order.get("client_id")),
        perm_id=_int_or_none(order.get("perm_id")),
    )
    confirmed = confirmation_open_orders is not None and not _same_order_still_working(order, confirmation_open_orders)
    result = _control_result("EXACT_CANCEL_REQUESTED", resolution=resolution, confirmed=confirmed)
    if confirmation_open_orders is not None and not confirmed:
        emit_operator_cancel_required_exact_order(
            order=order,
            failed_paths=[result],
            path=operator_required_path,
        )
        result["classification"] = "OPERATOR_CANCEL_REQUIRED_EXACT_ORDER"
    return result


def exact_cancel_owned_order_with_fallbacks(
    *,
    cancel_paths: Sequence[Mapping[str, Any]],
    request: Mapping[str, Any],
    broker_open_orders: Sequence[Mapping[str, Any]],
    ownership_records: Sequence[Mapping[str, Any]],
    confirmation_supplier: Any,
    operator_required_path: Path = DEFAULT_TRACK_B_PAPER_ORDER_OPERATOR_REQUIRED_JSON,
) -> dict[str, Any]:
    """Try exact cancel through ordered client paths, never broad/global cancel."""

    resolution = resolve_owned_working_order(
        request=request,
        broker_open_orders=broker_open_orders,
        ownership_records=ownership_records,
    )
    if not resolution.allowed:
        return _control_result("EXACT_CANCEL_BLOCKED", resolution=resolution)
    order = dict(resolution.order or {})
    failed_paths: list[dict[str, Any]] = []
    for path in cancel_paths:
        path_name = str(path.get("name") or "unnamed_cancel_path")
        transport = path.get("transport")
        if transport is None:
            failed_paths.append({"path": path_name, "classification": "CANCEL_PATH_MISSING_TRANSPORT"})
            continue
        try:
            transport.cancel_order(
                order_id=int(order["order_id"]),
                client_id=_int_or_none(order.get("client_id")),
                perm_id=_int_or_none(order.get("perm_id")),
            )
        except Exception as exc:  # pragma: no cover - exact text is not important
            failed_paths.append(
                {
                    "path": path_name,
                    "classification": "EXACT_CANCEL_PATH_EXCEPTION",
                    "error": repr(exc),
                }
            )
            continue
        confirmation_rows = confirmation_supplier(path_name)
        if not _same_order_still_working(order, confirmation_rows):
            return _control_result(
                "EXACT_CANCEL_CONFIRMED",
                resolution=resolution,
                confirmed=True,
                cancel_path=path_name,
                failed_paths=failed_paths,
            )
        failed_paths.append(
            {
                "path": path_name,
                "classification": "EXACT_CANCEL_NOT_CONFIRMED",
                "matching_order_still_working": True,
            }
        )
    operator_payload = emit_operator_cancel_required_exact_order(
        order=order,
        failed_paths=failed_paths,
        path=operator_required_path,
    )
    return _control_result(
        "OPERATOR_CANCEL_REQUIRED_EXACT_ORDER",
        resolution=resolution,
        confirmed=False,
        failed_paths=failed_paths,
        operator_required_artifact=str(operator_required_path),
        operator_required=operator_payload,
    )


def exact_modify_owned_order(
    *,
    transport: PaperOrderControlTransport,
    request: Mapping[str, Any],
    broker_open_orders: Sequence[Mapping[str, Any]],
    ownership_records: Sequence[Mapping[str, Any]],
    new_limit_price: float,
) -> dict[str, Any]:
    resolution = resolve_owned_working_order(
        request=request,
        broker_open_orders=broker_open_orders,
        ownership_records=ownership_records,
    )
    if not resolution.allowed:
        return _control_result("EXACT_MODIFY_BLOCKED", resolution=resolution)
    order = dict(resolution.order or {})
    transport.modify_order(
        order_id=int(order["order_id"]),
        client_id=_int_or_none(order.get("client_id")),
        perm_id=_int_or_none(order.get("perm_id")),
        limit_price=float(new_limit_price),
    )
    return _control_result("EXACT_MODIFY_REQUESTED", resolution=resolution, new_limit_price=float(new_limit_price))


def emit_operator_cancel_required_exact_order(
    *,
    order: Mapping[str, Any],
    failed_paths: Sequence[Mapping[str, Any]],
    path: Path = DEFAULT_TRACK_B_PAPER_ORDER_OPERATOR_REQUIRED_JSON,
) -> dict[str, Any]:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "classification": "OPERATOR_CANCEL_REQUIRED_EXACT_ORDER",
        "generated_at": datetime.now(UTC).isoformat(),
        "paper_only": True,
        "broad_cancel_allowed": False,
        "submit_new_order_allowed": False,
        "target_order": dict(order),
        "failed_cancel_paths": [dict(row) for row in failed_paths],
        "detail": "Exact Track B PAPER cancel could not be confirmed; operator must cancel only the identified broker order.",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def _control_result(classification: str, *, resolution: OrderControlResolution, **extra: Any) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "classification": classification,
        "generated_at": datetime.now(UTC).isoformat(),
        "paper_only": True,
        "broad_cancel_allowed": False,
        "submit_new_order_allowed": False,
        "match_method": resolution.match_method,
        "reason": resolution.reason,
        "order": resolution.order,
        "ownership": resolution.ownership,
        **extra,
    }


def _same_order_still_working(order: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> bool:
    normalized = _normalize_order(order)
    return any(
        _is_working(row)
        and (
            _match_perm_id(_normalize_order(row), normalized)
            or _match_order_id_client_id(_normalize_order(row), normalized)
            or _match_composite_identity(_normalize_order(row), normalized)
        )
        for row in rows
    )


def _unique_match(rows: Sequence[dict[str, Any]], target: Mapping[str, Any], matcher: Any) -> dict[str, Any] | None:
    matches = [row for row in rows if matcher(row, target)]
    if len(matches) == 1:
        return matches[0]
    return None


def _match_perm_id(row: Mapping[str, Any], target: Mapping[str, Any]) -> bool:
    return bool(row.get("perm_id") and target.get("perm_id") and row.get("perm_id") == target.get("perm_id"))


def _match_order_id_client_id(row: Mapping[str, Any], target: Mapping[str, Any]) -> bool:
    return bool(
        row.get("order_id")
        and target.get("order_id")
        and row.get("client_id")
        and target.get("client_id")
        and row.get("order_id") == target.get("order_id")
        and row.get("client_id") == target.get("client_id")
    )


def _match_composite_identity(row: Mapping[str, Any], target: Mapping[str, Any]) -> bool:
    keys = ("account_id", "con_id", "action", "quantity", "order_ref")
    return all(row.get(key) and target.get(key) and row.get(key) == target.get(key) for key in keys)


def _identity_conflict(order: Mapping[str, Any], owned: Mapping[str, Any]) -> bool:
    for key in ("account_id", "con_id", "local_symbol", "action", "quantity", "order_ref"):
        if order.get(key) and owned.get(key) and order.get(key) != owned.get(key):
            return True
    return False


def _normalize_order(row: Mapping[str, Any]) -> dict[str, Any]:
    raw = dict(row or {})
    return {
        "order_id": _text_or_none(_first(raw, "order_id", "broker_order_id", "orderId")),
        "perm_id": _text_or_none(_first(raw, "perm_id", "permId")),
        "client_id": _text_or_none(_first(raw, "client_id", "clientId")),
        "account_id": _text_or_none(_first(raw, "account_id", "account", "acctNumber")),
        "con_id": _text_or_none(_first(raw, "con_id", "conId", "qualified_contract_identifier")),
        "local_symbol": _text_or_none(_first(raw, "local_symbol", "localSymbol")),
        "action": _text_or_none(_first(raw, "action", "side")),
        "quantity": _quantity_text(_first(raw, "quantity", "qty", "totalQuantity")) if _first(raw, "quantity", "qty", "totalQuantity") not in (None, "") else None,
        "order_type": _text_or_none(_first(raw, "order_type", "orderType")),
        "limit_price": _text_or_none(_first(raw, "limit_price", "lmtPrice")),
        "order_ref": _text_or_none(_first(raw, "order_ref", "orderRef")),
        "status": _text_or_none(_first(raw, "status", "order_status")),
        "raw": raw,
    }


def _is_working(row: Mapping[str, Any]) -> bool:
    status = _text(_first(row, "status", "order_status")).replace(" ", "").replace("_", "").upper()
    if status in {value.replace("_", "") for value in TERMINAL_STATUSES}:
        return False
    if status in {value.replace("_", "") for value in WORKING_STATUSES}:
        return True
    remaining = _first(row, "remaining_quantity", "remaining")
    if remaining not in (None, ""):
        try:
            return float(str(remaining)) > 0
        except ValueError:
            return True
    return bool(status)


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _text(value: Any) -> str:
    return str(value or "").strip()


def _text_or_none(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _quantity_text(value: Any) -> str:
    try:
        numeric = float(str(value))
    except (TypeError, ValueError) as exc:
        raise PaperOrderControlError("quantity must be numeric") from exc
    return str(int(numeric)) if numeric.is_integer() else str(numeric)


def _timestamp(value: datetime | str) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise PaperOrderControlError("timestamp must be timezone-aware")
        return value.astimezone(UTC).isoformat()
    text = _text(value)
    if not text:
        raise PaperOrderControlError("timestamp is required")
    return text


def _int_or_none(value: Any) -> int | None:
    text = _text(value)
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None
