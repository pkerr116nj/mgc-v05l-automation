"""Paper-only IBKR read-only account-truth helpers."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ..brokers.ibkr import (
    IbkrBalanceRecord,
    IbkrClient,
    IbkrCompletedOrderRecord,
    IbkrContractDescriptor,
    IbkrExecutionRecord,
    IbkrOpenOrderRecord,
    IbkrPositionRecord,
    IbkrSession,
    IbkrTransportDependencyMissing,
    IbkrTwsTransport,
    IbkrTwsTransportConfig,
    build_default_ibkr_order_id_policy,
)
from .ibkr_execution_provider import IbkrExecutionProvider


class IbkrAccountTruthError(RuntimeError):
    """Base error for IBKR paper-truth capture."""


class IbkrPaperModeRequiredError(IbkrAccountTruthError):
    """Raised when a non-paper mode is requested."""


class IbkrRealConnectionBlocked(IbkrAccountTruthError):
    """Raised when a real IBKR connection is attempted without explicit approval."""


class IbkrManagedAccountsAmbiguousError(IbkrAccountTruthError):
    """Raised when multiple managed accounts are visible without explicit selection."""


class IbkrTransportMethodMissing(IbkrAccountTruthError):
    """Raised when the transport bridge lacks required read-only methods."""


@dataclass(frozen=True)
class IbkrAccountTruthCaptureConfig:
    mode: str
    host: str
    port: int
    client_id: int
    account_id: str | None = None
    timeout_seconds: float = 10.0


@dataclass(frozen=True)
class IbkrAccountTruthCaptureResult:
    snapshot: dict[str, Any]
    acceptance: dict[str, Any]
    audit: dict[str, Any]


def capture_fixture_ibkr_paper_truth(
    *,
    repo_root: Path,
    capture_config: IbkrAccountTruthCaptureConfig,
    fixture_payload: dict[str, Any],
    acceptance_evaluator: Callable[[dict[str, Any]], dict[str, Any]],
) -> IbkrAccountTruthCaptureResult:
    _require_paper_mode(capture_config.mode)
    provider = _provider_from_fixture(repo_root=repo_root, capture_config=capture_config, fixture_payload=fixture_payload)
    snapshot = provider.snapshot_state()
    acceptance = acceptance_evaluator(snapshot)
    audit = _build_audit_payload(
        capture_config=capture_config,
        transport_mode="fixture",
        status="ready",
        selected_account_id=snapshot.get("selected_account_id"),
        detail="Fixture-based paper truth capture completed without broker connectivity.",
    )
    return IbkrAccountTruthCaptureResult(snapshot=snapshot, acceptance=acceptance, audit=audit)


def capture_real_ibkr_paper_truth(
    *,
    repo_root: Path,
    capture_config: IbkrAccountTruthCaptureConfig,
    acceptance_evaluator: Callable[[dict[str, Any]], dict[str, Any]],
    allow_real_api: bool,
    transport_factory: Callable[..., IbkrTwsTransport] = IbkrTwsTransport,
    client_factory: Callable[[IbkrSession], IbkrClient] = IbkrClient,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> IbkrAccountTruthCaptureResult:
    _require_paper_mode(capture_config.mode)
    if not allow_real_api:
        raise IbkrRealConnectionBlocked(
            "Real IBKR API connection is blocked unless --allow-real-api is supplied. Use --fixture-json for safe local validation."
        )

    session = IbkrSession(
        host=capture_config.host,
        port=int(capture_config.port),
        client_id=int(capture_config.client_id),
        account_id=capture_config.account_id,
        gateway_mode="paper",
        read_only=True,
        order_id_policy=build_default_ibkr_order_id_policy(
            client_id=int(capture_config.client_id),
            live_orders_enabled=False,
        ),
    )
    client = client_factory(session)
    transport = transport_factory(
        client=client,
        config=IbkrTwsTransportConfig(
            host=capture_config.host,
            port=int(capture_config.port),
            client_id=int(capture_config.client_id),
            read_only=True,
        ),
    )
    try:
        bridge = transport.bridge()
    except IbkrTransportDependencyMissing:
        raise
    _require_bridge_method(bridge, "reqManagedAccts")
    _require_bridge_method(bridge, "reqAccountUpdates")
    _require_bridge_method(bridge, "reqPositions")
    _require_bridge_method(bridge, "reqAllOpenOrders")
    _require_bridge_method(bridge, "disconnect")

    transport.connect()
    run_loop_started = False
    try:
        try:
            thread = threading.Thread(target=transport.run_loop, name="ibkr_read_only_run_loop", daemon=True)
            thread.start()
            run_loop_started = True
        except NotImplementedError:
            run_loop_started = False

        client.request_managed_accounts()
        bridge.reqManagedAccts()
        _wait_for_event_types(
            client=client,
            required_event_types={"managed_accounts"},
            timeout_seconds=capture_config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        selected_account_id = _resolve_selected_account_id(
            requested_account_id=capture_config.account_id,
            managed_accounts=client.connection_state().managed_accounts,
        )

        client.request_balances()
        bridge.reqAccountUpdates(True, selected_account_id)
        client.request_positions()
        bridge.reqPositions()
        client.request_open_orders()
        bridge.reqAllOpenOrders()
        _wait_for_event_types(
            client=client,
            required_event_types={"balances", "positions", "open_orders"},
            timeout_seconds=capture_config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        try:
            bridge.reqAccountUpdates(False, selected_account_id)
        except Exception:
            pass

        session = client.session
        if session.state.account_id != selected_account_id:
            session.select_account(selected_account_id)
            session.record_heartbeat(occurred_at=session.state.last_heartbeat_at or datetime.now(timezone.utc))

        provider = IbkrExecutionProvider(repo_root, session=session, client=client)
        snapshot = provider.snapshot_state(force_refresh=True)
        acceptance = acceptance_evaluator(snapshot)
        audit = _build_audit_payload(
            capture_config=capture_config,
            transport_mode="real_api",
            status="ready",
            selected_account_id=selected_account_id,
            detail="Real IBKR paper read-only truth capture completed without order-capable calls.",
        )
        if run_loop_started:
            audit["run_loop_started"] = True
        return IbkrAccountTruthCaptureResult(snapshot=snapshot, acceptance=acceptance, audit=audit)
    finally:
        try:
            transport.disconnect()
        except Exception:
            pass


def _provider_from_fixture(
    *,
    repo_root: Path,
    capture_config: IbkrAccountTruthCaptureConfig,
    fixture_payload: dict[str, Any],
) -> IbkrExecutionProvider:
    managed_accounts = _coerce_managed_accounts(fixture_payload.get("managed_accounts"))
    selected_account_id = _resolve_selected_account_id(
        requested_account_id=capture_config.account_id or fixture_payload.get("selected_account_id"),
        managed_accounts=managed_accounts,
    )
    now = datetime.now(timezone.utc)
    session = IbkrSession(
        host=capture_config.host,
        port=int(capture_config.port),
        client_id=int(capture_config.client_id),
        account_id=selected_account_id,
        gateway_mode="paper",
        read_only=True,
        order_id_policy=build_default_ibkr_order_id_policy(
            client_id=int(capture_config.client_id),
            live_orders_enabled=False,
        ),
    )
    session.mark_connected(managed_accounts=managed_accounts, connected_at=now)
    session.record_heartbeat(occurred_at=now)
    return IbkrExecutionProvider(
        repo_root,
        session=session,
        balances=tuple(_coerce_balance_record(row) for row in fixture_payload.get("balances", [])),
        positions=tuple(_coerce_position_record(row) for row in fixture_payload.get("positions", [])),
        open_orders=tuple(_coerce_open_order_record(row) for row in fixture_payload.get("open_orders", [])),
        completed_orders=tuple(_coerce_completed_order_record(row) for row in fixture_payload.get("completed_orders", [])),
        executions=tuple(_coerce_execution_record(row) for row in fixture_payload.get("executions", [])),
    )


def _require_paper_mode(mode: str) -> None:
    if str(mode or "").strip().lower() != "paper":
        raise IbkrPaperModeRequiredError("IBKR account-truth Stage 1 supports paper mode only; live mode is rejected.")


def _resolve_selected_account_id(*, requested_account_id: str | None, managed_accounts: tuple[str, ...]) -> str:
    normalized_requested = str(requested_account_id or "").strip() or None
    if normalized_requested is not None:
        if normalized_requested not in managed_accounts:
            managed_list = ", ".join(managed_accounts) or "<none>"
            raise IbkrAccountTruthError(
                f"Selected IBKR account {normalized_requested} is not present in managed accounts: {managed_list}."
            )
        return normalized_requested
    if not managed_accounts:
        raise IbkrAccountTruthError("IBKR managed account discovery returned no accounts.")
    if len(managed_accounts) != 1:
        managed_list = ", ".join(managed_accounts)
        raise IbkrManagedAccountsAmbiguousError(
            f"IBKR managed account discovery is ambiguous. Provide --account-id. Managed accounts: {managed_list}."
        )
    return managed_accounts[0]


def _coerce_managed_accounts(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        rows = [part.strip() for part in value.split(",")]
    elif isinstance(value, (list, tuple)):
        rows = [str(part).strip() for part in value]
    else:
        rows = []
    return tuple(item for item in rows if item)


def _coerce_contract_descriptor(payload: dict[str, Any]) -> IbkrContractDescriptor:
    return IbkrContractDescriptor(
        con_id=_coerce_int(payload.get("con_id") or payload.get("conId")),
        symbol=str(payload.get("symbol") or "").strip(),
        local_symbol=_coerce_optional_text(payload.get("local_symbol") or payload.get("localSymbol")),
        security_type=str(payload.get("security_type") or payload.get("securityType") or "").strip(),
        exchange=str(payload.get("exchange") or "").strip(),
        currency=str(payload.get("currency") or "").strip(),
        expiry=_coerce_optional_text(payload.get("expiry") or payload.get("lastTradeDateOrContractMonth")),
        multiplier=_coerce_optional_text(payload.get("multiplier")),
        trading_class=_coerce_optional_text(payload.get("trading_class") or payload.get("tradingClass")),
        raw_payload=dict(payload),
    )


def _coerce_balance_record(payload: dict[str, Any]) -> IbkrBalanceRecord:
    return IbkrBalanceRecord(
        account_id=str(payload.get("account_id") or payload.get("accountId") or "").strip(),
        currency=_coerce_optional_text(payload.get("currency")),
        cash_balance=_coerce_optional_text(payload.get("cash_balance") or payload.get("cashBalance")),
        buying_power=_coerce_optional_text(payload.get("buying_power") or payload.get("buyingPower")),
        available_funds=_coerce_optional_text(payload.get("available_funds") or payload.get("availableFunds")),
        net_liquidation=_coerce_optional_text(payload.get("net_liquidation") or payload.get("netLiquidation")),
        maintenance_requirement=_coerce_optional_text(payload.get("maintenance_requirement") or payload.get("maintenanceRequirement")),
        updated_at=_coerce_datetime(payload.get("updated_at")),
        raw_payload=dict(payload),
    )


def _coerce_position_record(payload: dict[str, Any]) -> IbkrPositionRecord:
    return IbkrPositionRecord(
        account_id=str(payload.get("account_id") or payload.get("accountId") or "").strip(),
        contract=_coerce_contract_descriptor(dict(payload.get("contract") or {})),
        quantity=str(payload.get("quantity") or "").strip(),
        average_cost=_coerce_optional_text(payload.get("average_cost") or payload.get("averageCost")),
        market_price=_coerce_optional_text(payload.get("market_price") or payload.get("marketPrice")),
        market_value=_coerce_optional_text(payload.get("market_value") or payload.get("marketValue")),
        updated_at=_coerce_datetime(payload.get("updated_at")),
        raw_payload=dict(payload),
    )


def _coerce_open_order_record(payload: dict[str, Any]) -> IbkrOpenOrderRecord:
    return IbkrOpenOrderRecord(
        account_id=str(payload.get("account_id") or payload.get("accountId") or "").strip(),
        broker_order_id=int(payload.get("broker_order_id") or payload.get("brokerOrderId") or 0),
        client_id=int(payload.get("client_id") or payload.get("clientId") or 0),
        perm_id=_coerce_int(payload.get("perm_id") or payload.get("permId")),
        contract=_coerce_contract_descriptor(dict(payload.get("contract") or {})),
        status=str(payload.get("status") or "").strip(),
        quantity=str(payload.get("quantity") or "").strip(),
        filled_quantity=_coerce_optional_text(payload.get("filled_quantity") or payload.get("filledQuantity")),
        limit_price=_coerce_optional_text(payload.get("limit_price") or payload.get("limitPrice")),
        stop_price=_coerce_optional_text(payload.get("stop_price") or payload.get("stopPrice")),
        updated_at=_coerce_datetime(payload.get("updated_at")),
        raw_payload=dict(payload),
    )


def _coerce_completed_order_record(payload: dict[str, Any]) -> IbkrCompletedOrderRecord:
    return IbkrCompletedOrderRecord(
        account_id=str(payload.get("account_id") or payload.get("accountId") or "").strip(),
        broker_order_id=int(payload.get("broker_order_id") or payload.get("brokerOrderId") or 0),
        client_id=int(payload.get("client_id") or payload.get("clientId") or 0),
        perm_id=_coerce_int(payload.get("perm_id") or payload.get("permId")),
        contract=_coerce_contract_descriptor(dict(payload.get("contract") or {})),
        status=str(payload.get("status") or "").strip(),
        quantity=str(payload.get("quantity") or "").strip(),
        completed_at=_coerce_datetime(payload.get("completed_at")),
        raw_payload=dict(payload),
    )


def _coerce_execution_record(payload: dict[str, Any]) -> IbkrExecutionRecord:
    return IbkrExecutionRecord(
        account_id=str(payload.get("account_id") or payload.get("accountId") or "").strip(),
        execution_id=str(payload.get("execution_id") or payload.get("executionId") or "").strip(),
        broker_order_id=_coerce_int(payload.get("broker_order_id") or payload.get("brokerOrderId")),
        client_id=_coerce_int(payload.get("client_id") or payload.get("clientId")),
        perm_id=_coerce_int(payload.get("perm_id") or payload.get("permId")),
        contract=_coerce_contract_descriptor(dict(payload.get("contract") or {})),
        side=_coerce_optional_text(payload.get("side")),
        quantity=str(payload.get("quantity") or "").strip(),
        price=_coerce_optional_text(payload.get("price")),
        executed_at=_coerce_datetime(payload.get("executed_at")),
        raw_payload=dict(payload),
    )


def _coerce_optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _coerce_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    return datetime.fromisoformat(text)


def _require_bridge_method(bridge: Any, method_name: str) -> None:
    method = getattr(bridge, method_name, None)
    if method is None or not callable(method):
        raise IbkrTransportMethodMissing(
            f"IBKR transport bridge is missing required read-only method {method_name}."
        )


def _wait_for_event_types(
    *,
    client: IbkrClient,
    required_event_types: set[str],
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> None:
    deadline = time.monotonic() + float(timeout_seconds)
    seen: set[str] = set()
    while time.monotonic() < deadline:
        for event in client.drain_events():
            seen.add(event.event_type)
        if required_event_types.issubset(seen):
            return
        sleep_fn(0.05)
    missing = ", ".join(sorted(required_event_types - seen))
    raise IbkrAccountTruthError(f"Timed out waiting for IBKR read-only truth events: {missing}.")


def _build_audit_payload(
    *,
    capture_config: IbkrAccountTruthCaptureConfig,
    transport_mode: str,
    status: str,
    selected_account_id: str | None,
    detail: str,
) -> dict[str, Any]:
    return {
        "provider_id": "ibkr_execution",
        "mode": str(capture_config.mode).strip().lower(),
        "transport_mode": transport_mode,
        "status": status,
        "selected_account_id": selected_account_id,
        "host": capture_config.host,
        "port": int(capture_config.port),
        "client_id": int(capture_config.client_id),
        "read_only_only": True,
        "orders_allowed": False,
        "detail": detail,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
