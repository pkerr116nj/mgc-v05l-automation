"""Offline TradeStation futures dry-run ticket generation."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from .broker_requests import BrokerContractRequest, BrokerOrderRequest
from .execution_capability_matrix import resolve_stage2_futures_sim_capability
from .operator_ticket import DryRunTradeStationPayloadArtifact, DryRunValidationStatus, OperatorExecutionTicket
from .tradestation_account_router import TradeStationAccountRouteError, TradeStationFuturesAccountRouter


TRUTH_FRESHNESS_THRESHOLD_SECONDS = 120
STAGE2_PROVIDER_ID = "tradestation_futures_dry_run"
QUOTE_REQUIRED_SYMBOLS = {"NQ", "MNQ"}
QUOTE_OPTIONAL_SYMBOLS = {"ES", "MES"}
ELIGIBLE_DECISION_STATE = "TRADE_FAVORABLE"
ELIGIBLE_TIMING_BUCKET = "LATE_ASIA"


class TradeStationDryRunValidationError(RuntimeError):
    """Raised when a dry-run request cannot be validated."""


@dataclass(frozen=True)
class DryRunGenerationResult:
    validation_status: DryRunValidationStatus
    rejections: tuple[str, ...]
    warnings: tuple[str, ...]
    ticket: OperatorExecutionTicket | None
    dry_run_payload: DryRunTradeStationPayloadArtifact | None
    trace: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "validation_status": self.validation_status.value,
            "rejections": list(self.rejections),
            "warnings": list(self.warnings),
            "ticket": self.ticket.to_dict() if self.ticket is not None else None,
            "dry_run_payload": self.dry_run_payload.to_dict() if self.dry_run_payload is not None else None,
            "trace": dict(self.trace),
            "truth_freshness_threshold_seconds": TRUTH_FRESHNESS_THRESHOLD_SECONDS,
        }


def generate_stage2_futures_dry_run(
    *,
    signal_payload: dict[str, Any],
    truth_payload: dict[str, Any],
    broker_symbol_map_payload: dict[str, Any],
    pending_ticket_registry: dict[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> DryRunGenerationResult:
    now = generated_at or datetime.now(timezone.utc)
    symbol = _read_symbol(signal_payload)
    environment = str(signal_payload.get("environment") or truth_payload.get("environment") or "sim").strip().lower()
    signal_id = _read_text(signal_payload, "signal_id", "decision_id", required=True)
    trace = {"signal_id": signal_id}
    warnings: list[str] = []
    rejections: list[str] = []

    capability = resolve_stage2_futures_sim_capability(symbol, environment=environment)
    if capability is None:
        rejections.append(f"Symbol {symbol} is not execution-eligible for Stage 2 futures dry-run in environment {environment}.")

    decision_state = _read_text(signal_payload, "decision_state", "state_classification", required=True)
    timing_bucket = _read_text(signal_payload, "timing_bucket", "timing_within_asia", required=True)
    if decision_state != ELIGIBLE_DECISION_STATE or timing_bucket != ELIGIBLE_TIMING_BUCKET:
        rejections.append(
            f"Decision-state gating failed: require {ELIGIBLE_DECISION_STATE} + {ELIGIBLE_TIMING_BUCKET}, got {decision_state} + {timing_bucket}."
        )

    contract_metadata = _resolve_contract_metadata(symbol, broker_symbol_map_payload)
    missing_contract_fields = [field for field in ("broker_symbol", "expiry", "multiplier", "exchange") if not contract_metadata.get(field)]
    if missing_contract_fields:
        rejections.append(f"Contract metadata is incomplete for {symbol}: missing {', '.join(missing_contract_fields)}.")

    try:
        route = TradeStationFuturesAccountRouter().resolve_from_truth(truth_payload, environment=environment)
    except TradeStationAccountRouteError as exc:
        rejections.append(str(exc))
        route = None

    truth_generated_at = _parse_datetime(truth_payload.get("generated_at"))
    truth_age_seconds = None
    if truth_generated_at is None:
        rejections.append("Truth snapshot is missing generated_at.")
    else:
        truth_age_seconds = int((now - truth_generated_at).total_seconds())
        if truth_age_seconds < 0:
            rejections.append("Truth snapshot generated_at is in the future relative to dry-run generation time.")
        if truth_age_seconds > TRUTH_FRESHNESS_THRESHOLD_SECONDS:
            rejections.append(
                f"Truth snapshot is stale at {truth_age_seconds}s; threshold is {TRUTH_FRESHNESS_THRESHOLD_SECONDS}s."
            )

    if route is not None and _is_active_overlap(symbol, route.account_id, truth_payload, pending_ticket_registry):
        rejections.append(
            f"Active overlap detected for {symbol}: active means non-flat position or open order or pending ticket."
        )

    quote_snapshot = _coerce_quote_snapshot(signal_payload)
    if symbol in QUOTE_REQUIRED_SYMBOLS and quote_snapshot is None:
        rejections.append(f"Quote snapshot is required for {symbol} dry-run tickets.")
    if symbol in QUOTE_OPTIONAL_SYMBOLS and quote_snapshot is None:
        warnings.append(f"Quote snapshot is optional but missing for {symbol}.")

    quantity = _read_decimal(signal_payload, "quantity", required=True)
    entry_order_type = _read_text(signal_payload, "entry_order_type", "order_type", default="LIMIT").upper()
    if entry_order_type not in {"MARKET", "LIMIT"}:
        rejections.append(f"Unsupported Stage 2 entry_order_type {entry_order_type}; only MARKET and LIMIT are allowed.")

    side = _read_text(signal_payload, "side", required=True).upper()
    if side not in {"BUY", "SELL"}:
        rejections.append(f"Unsupported side {side}; expected BUY or SELL.")

    entry_limit_price = _read_decimal(signal_payload, "entry_limit_price")
    entry_reference_price = _resolve_entry_reference_price(signal_payload, quote_snapshot)
    if entry_order_type == "LIMIT" and entry_limit_price is None:
        rejections.append("LIMIT dry-run tickets require entry_limit_price.")

    validation_status = DryRunValidationStatus.REJECTED
    if not rejections and warnings:
        validation_status = DryRunValidationStatus.VALID_WITH_WARNINGS
    elif not rejections:
        validation_status = DryRunValidationStatus.VALID

    if validation_status == DryRunValidationStatus.REJECTED or route is None:
        return DryRunGenerationResult(
            validation_status=validation_status,
            rejections=tuple(rejections),
            warnings=tuple(warnings),
            ticket=None,
            dry_run_payload=None,
            trace=trace,
        )

    ticket_id = _stable_id(
        prefix="ticket",
        parts=(signal_id, symbol, environment, route.account_id, now.isoformat()),
    )
    trace["ticket_id"] = ticket_id
    ticket = OperatorExecutionTicket(
        ticket_id=ticket_id,
        signal_id=signal_id,
        strategy_id=_read_text(signal_payload, "strategy_id", default="asia_drift_continuation"),
        generated_at=now,
        environment=environment,
        asset_class="FUTURE",
        internal_symbol=symbol,
        broker_symbol=str(contract_metadata["broker_symbol"]),
        account_id=route.account_id,
        account_type=route.account_type,
        decision_state=decision_state,
        timing_bucket=timing_bucket,
        side=side,
        quantity=quantity,
        entry_order_type=entry_order_type,
        entry_reference_price=entry_reference_price,
        entry_limit_price=entry_limit_price,
        time_in_force="DAY",
        session="NORMAL",
        risk_profile=_read_text(signal_payload, "risk_profile", default=_default_risk_profile(symbol)),
        fixed_stop_points=_read_decimal(signal_payload, "fixed_stop_points", default=_default_stop(symbol)),
        fixed_target_points=_read_decimal(signal_payload, "fixed_target_points", default=_default_target(symbol)),
        time_stop_minutes=int(signal_payload.get("time_stop_minutes") or 120),
        contract_metadata=contract_metadata,
        quote_snapshot=quote_snapshot,
        truth_checked_at=truth_generated_at,
        truth_snapshot_age_seconds=truth_age_seconds,
        warnings=tuple(warnings),
        trace={"signal_id": signal_id, "ticket_id": ticket_id},
    )

    order_request = BrokerOrderRequest(
        account_id=route.account_id,
        contract=BrokerContractRequest(
            asset_class="FUTURE",
            symbol=symbol,
            broker_symbol=str(contract_metadata["broker_symbol"]),
            exchange=str(contract_metadata["exchange"]),
            expiry=str(contract_metadata["expiry"]),
            multiplier=str(contract_metadata["multiplier"]),
        ),
        side=side,
        quantity=quantity,
        order_type=entry_order_type,
        time_in_force="DAY",
        session="NORMAL",
        intent_type=side,
        limit_price=entry_limit_price,
        stop_price=None,
        client_order_id=ticket_id,
        pricing_source="quote_snapshot" if quote_snapshot is not None else "signal_payload",
        metadata={
            "signal_id": signal_id,
            "ticket_id": ticket_id,
            "truth_snapshot_age_seconds": truth_age_seconds,
        },
    )

    dry_run_payload_id = _stable_id(
        prefix="dryrun",
        parts=(ticket_id, STAGE2_PROVIDER_ID, now.isoformat()),
    )
    trace["dry_run_payload_id"] = dry_run_payload_id
    payload = _build_tradestation_sim_payload(order_request)
    dry_run_payload = DryRunTradeStationPayloadArtifact(
        dry_run_payload_id=dry_run_payload_id,
        ticket_id=ticket_id,
        provider_id=STAGE2_PROVIDER_ID,
        generated_at=now,
        validation_status=validation_status,
        warnings=tuple(warnings),
        payload=payload,
        trace={"signal_id": signal_id, "ticket_id": ticket_id, "dry_run_payload_id": dry_run_payload_id},
    )
    return DryRunGenerationResult(
        validation_status=validation_status,
        rejections=(),
        warnings=tuple(warnings),
        ticket=ticket,
        dry_run_payload=dry_run_payload,
        trace=trace,
    )


def _resolve_contract_metadata(symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
    row = dict((payload.get(symbol) or payload.get(symbol.upper()) or {}))
    return {
        "broker_symbol": _read_text(row, "broker_symbol"),
        "expiry": _read_text(row, "expiry"),
        "multiplier": _read_text(row, "multiplier"),
        "exchange": _read_text(row, "exchange"),
    }


def _is_active_overlap(
    symbol: str,
    account_id: str,
    truth_payload: dict[str, Any],
    pending_ticket_registry: dict[str, Any] | None,
) -> bool:
    normalized_symbol = symbol.upper()
    for row in list(truth_payload.get("positions") or []):
        if str(row.get("account_id") or "") == account_id and str(row.get("symbol") or "").upper() == normalized_symbol:
            quantity = _to_decimal(row.get("quantity"))
            if quantity is not None and quantity != 0:
                return True
    for row in list(truth_payload.get("open_orders") or []):
        if str(row.get("account_id") or "") == account_id and str(row.get("symbol") or "").upper() == normalized_symbol:
            return True
    if pending_ticket_registry:
        for row in list(pending_ticket_registry.get("pending_tickets") or []):
            if str(row.get("symbol") or "").upper() != normalized_symbol:
                continue
            if str(row.get("status") or "").strip().upper() in {"OPEN", "PENDING", "ACKNOWLEDGED", "SUBMITTED"}:
                return True
    return False


def _coerce_quote_snapshot(signal_payload: dict[str, Any]) -> dict[str, Any] | None:
    payload = signal_payload.get("quote_snapshot")
    if isinstance(payload, dict):
        return dict(payload)
    return None


def _resolve_entry_reference_price(signal_payload: dict[str, Any], quote_snapshot: dict[str, Any] | None) -> Decimal | None:
    explicit = _read_decimal(signal_payload, "entry_reference_price")
    if explicit is not None:
        return explicit
    if not quote_snapshot:
        return None
    for key in ("last_price", "mark_price", "ask_price", "bid_price"):
        value = _to_decimal(quote_snapshot.get(key))
        if value is not None:
            return value
    return None


def _build_tradestation_sim_payload(order_request: BrokerOrderRequest) -> dict[str, Any]:
    payload = {
        "Environment": "SIM",
        "AccountID": order_request.account_id,
        "Symbol": order_request.contract.broker_symbol,
        "AssetType": order_request.contract.asset_class,
        "Exchange": order_request.contract.exchange,
        "Quantity": str(order_request.quantity),
        "OrderType": order_request.order_type,
        "TradeAction": order_request.side,
        "TimeInForce": {"Duration": order_request.time_in_force},
        "Route": order_request.session,
        "ClientOrderID": order_request.client_order_id,
        "Metadata": dict(order_request.metadata or {}),
    }
    if order_request.limit_price is not None:
        payload["LimitPrice"] = str(order_request.limit_price)
    return payload


def _default_risk_profile(symbol: str) -> str:
    return "half_nq_unit" if symbol in {"NQ", "MNQ"} else "full_es_unit"


def _default_stop(symbol: str) -> Decimal:
    return Decimal("20") if symbol in {"NQ", "MNQ"} else Decimal("6")


def _default_target(symbol: str) -> Decimal:
    return Decimal("40") if symbol in {"NQ", "MNQ"} else Decimal("12")


def _read_symbol(payload: dict[str, Any]) -> str:
    return _read_text(payload, "internal_symbol", "symbol", "instrument", required=True).upper()


def _read_text(payload: dict[str, Any], *keys: str, required: bool = False, default: str | None = None) -> str:
    for key in keys:
        value = payload.get(key)
        text = str(value or "").strip()
        if text:
            return text
    if required:
        raise TradeStationDryRunValidationError(f"Missing required text field; looked for {', '.join(keys)}.")
    return str(default or "")


def _read_decimal(payload: dict[str, Any], key: str, *, required: bool = False, default: Decimal | None = None) -> Decimal | None:
    if key not in payload:
        if required:
            raise TradeStationDryRunValidationError(f"Missing required decimal field {key}.")
        return default
    return _to_decimal(payload.get(key), required=required, field_name=key)


def _to_decimal(value: Any, *, required: bool = False, field_name: str | None = None) -> Decimal | None:
    if value in (None, ""):
        if required:
            raise TradeStationDryRunValidationError(f"Missing required decimal field {field_name or 'value'}.")
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        if required:
            raise TradeStationDryRunValidationError(f"Field {field_name or 'value'} is not a valid decimal.") from exc
        return None


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _stable_id(*, prefix: str, parts: tuple[str, ...]) -> str:
    digest = hashlib.sha256("::".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"
