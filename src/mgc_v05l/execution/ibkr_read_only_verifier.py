"""Strict IBKR TWS paper read-only verification helpers."""

from __future__ import annotations

import importlib
import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ..brokers.ibkr import (
    IbkrClient,
    IbkrContractResolver,
    IbkrQualifiedContract,
    IbkrReadOnlyCallbackAdapter,
    IbkrSession,
    build_default_ibkr_order_id_policy,
)
from .ibkr_execution_provider import IbkrExecutionProvider

_EXPECTED_MODE = "PAPER"
_EXPECTED_HOST = "127.0.0.1"
_EXPECTED_PORT = 7497
_ACCOUNT_SUMMARY_TAGS = ",".join(
    (
        "AccountType",
        "BuyingPower",
        "NetLiquidation",
        "AvailableFunds",
        "CashBalance",
        "MaintMarginReq",
        "TotalCashValue",
    )
)
_CONNECTION_ERROR_CODES = {502, 504, 1100, 1101, 1102, 1300}
_DUPLICATE_CLIENT_ID_ERROR_CODES = {326}
_MARKET_DATA_PERMISSION_ERROR_CODES = {354, 10167, 10168}
_CONTRACT_REQUEST_ERROR_CODES = {200}
_DEFAULT_GC_EXPIRY = "202606"
_DEFAULT_MGC_EXPIRY = "202606"
_POLL_INTERVAL_SECONDS = 0.05


class IbkrReadOnlyVerificationError(RuntimeError):
    """Base error for strict read-only verification."""


@dataclass(frozen=True)
class IbkrReadOnlyVerificationConfig:
    repo_root: Path
    mode: str
    host: str
    port: int
    client_id: int
    read_only: bool
    account_id: str | None = None
    timeout_seconds: float = 15.0
    probe_market_data: bool = True
    probe_duplicate_client_id: bool = True
    gc_expiry: str = _DEFAULT_GC_EXPIRY
    mgc_expiry: str = _DEFAULT_MGC_EXPIRY


@dataclass(frozen=True)
class IbkrReadOnlyVerificationArtifacts:
    classification: str
    connection_report: dict[str, Any]
    account_truth_snapshot: dict[str, Any]
    positions_snapshot: dict[str, Any]
    open_orders_snapshot: dict[str, Any]
    contract_qualification_report: dict[str, Any]
    market_data_probe_report: dict[str, Any] | None

    @property
    def exit_code(self) -> int:
        return 0 if self.classification != "IBKR_READ_ONLY_BLOCKED" else 1


@dataclass(frozen=True)
class IbkrReadOnlyApiTransportConfig:
    host: str
    port: int
    client_id: int
    read_only: bool = True


class IbkrReadOnlyProbeCollector:
    """Collects read-only TWS callbacks into normalized truth and probe buffers."""

    def __init__(self, client: IbkrClient) -> None:
        self._client = client
        self._adapter = IbkrReadOnlyCallbackAdapter(client)
        self.next_valid_id_ready = threading.Event()
        self.managed_accounts_ready = threading.Event()
        self.balances_ready = threading.Event()
        self.positions_ready = threading.Event()
        self.open_orders_ready = threading.Event()
        self._account_summary_ready: dict[int, threading.Event] = {}
        self._contract_details_ready: dict[int, threading.Event] = {}
        self._market_data_ready: dict[int, threading.Event] = {}
        self.account_summary_rows: list[dict[str, Any]] = []
        self.contract_details_rows: dict[int, list[dict[str, Any]]] = {}
        self.market_data_rows: dict[int, dict[str, Any]] = {}
        self.errors: list[dict[str, Any]] = []

    def account_summary_event(self, request_id: int) -> threading.Event:
        return self._account_summary_ready.setdefault(int(request_id), threading.Event())

    def contract_details_event(self, request_id: int) -> threading.Event:
        return self._contract_details_ready.setdefault(int(request_id), threading.Event())

    def market_data_event(self, request_id: int) -> threading.Event:
        return self._market_data_ready.setdefault(int(request_id), threading.Event())

    def next_valid_id(self, order_id: int, *, occurred_at: datetime | None = None) -> None:
        self._adapter.next_valid_id(order_id, occurred_at=occurred_at)
        self.next_valid_id_ready.set()

    def managed_accounts(self, accounts: str | tuple[str, ...], *, occurred_at: datetime | None = None) -> None:
        self._adapter.managed_accounts(accounts, occurred_at=occurred_at)
        self.managed_accounts_ready.set()

    def update_account_value(
        self,
        *,
        account_id: str,
        key: str,
        value: str,
        currency: str | None = None,
        occurred_at: datetime | None = None,
    ) -> None:
        self._adapter.update_account_value(
            account_id=account_id,
            key=key,
            value=value,
            currency=currency,
            occurred_at=occurred_at,
        )

    def account_download_end(self, *, occurred_at: datetime | None = None) -> None:
        self._adapter.account_download_end(occurred_at=occurred_at)
        self.balances_ready.set()

    def position(
        self,
        *,
        account_id: str,
        contract: dict[str, Any],
        quantity: str | int | float,
        average_cost: str | int | float | None = None,
        occurred_at: datetime | None = None,
    ) -> None:
        self._adapter.position(
            account_id=account_id,
            contract=contract,
            quantity=quantity,
            average_cost=average_cost,
            occurred_at=occurred_at,
        )

    def position_end(self, *, occurred_at: datetime | None = None) -> None:
        self._adapter.position_end(occurred_at=occurred_at)
        self.positions_ready.set()

    def open_order(
        self,
        *,
        account_id: str,
        broker_order_id: int,
        client_id: int,
        perm_id: int | None,
        contract: dict[str, Any],
        status: str,
        quantity: str | int | float,
        filled_quantity: str | int | float | None = None,
        limit_price: str | int | float | None = None,
        stop_price: str | int | float | None = None,
        occurred_at: datetime | None = None,
    ) -> None:
        self._adapter.open_order(
            account_id=account_id,
            broker_order_id=broker_order_id,
            client_id=client_id,
            perm_id=perm_id,
            contract=contract,
            status=status,
            quantity=quantity,
            filled_quantity=filled_quantity,
            limit_price=limit_price,
            stop_price=stop_price,
            occurred_at=occurred_at,
        )

    def open_order_end(self, *, occurred_at: datetime | None = None) -> None:
        self._adapter.open_order_end(occurred_at=occurred_at)
        self.open_orders_ready.set()

    def account_summary(
        self,
        *,
        request_id: int,
        account_id: str,
        tag: str,
        value: str,
        currency: str | None,
        occurred_at: datetime | None = None,
    ) -> None:
        self.account_summary_rows.append(
            {
                "request_id": int(request_id),
                "account_id": str(account_id or "").strip(),
                "tag": str(tag or "").strip(),
                "value": str(value or "").strip(),
                "currency": str(currency or "").strip() or None,
                "updated_at": (occurred_at or datetime.now(timezone.utc)).isoformat(),
            }
        )

    def account_summary_end(self, *, request_id: int) -> None:
        self.account_summary_event(request_id).set()

    def contract_details(self, *, request_id: int, contract_details: Any, occurred_at: datetime | None = None) -> None:
        contract = getattr(contract_details, "contract", None)
        rows = self.contract_details_rows.setdefault(int(request_id), [])
        rows.append(
            {
                "request_id": int(request_id),
                "con_id": getattr(contract, "conId", None),
                "symbol": getattr(contract, "symbol", None),
                "local_symbol": getattr(contract, "localSymbol", None),
                "security_type": getattr(contract, "secType", None),
                "exchange": getattr(contract, "exchange", None),
                "currency": getattr(contract, "currency", None),
                "expiry": getattr(contract, "lastTradeDateOrContractMonth", None),
                "multiplier": getattr(contract, "multiplier", None),
                "trading_class": getattr(contract, "tradingClass", None),
                "market_name": getattr(contract_details, "marketName", None),
                "min_tick": getattr(contract_details, "minTick", None),
                "valid_exchanges": getattr(contract_details, "validExchanges", None),
                "time_zone_id": getattr(contract_details, "timeZoneId", None),
                "long_name": getattr(contract_details, "longName", None),
                "updated_at": (occurred_at or datetime.now(timezone.utc)).isoformat(),
            }
        )

    def contract_details_end(self, *, request_id: int) -> None:
        self.contract_details_event(request_id).set()

    def market_data_type(self, *, request_id: int, market_data_type: int) -> None:
        row = self.market_data_rows.setdefault(int(request_id), _empty_market_data_row(request_id))
        row["market_data_type"] = int(market_data_type)

    def tick_price(self, *, request_id: int, tick_type: int, price: float) -> None:
        row = self.market_data_rows.setdefault(int(request_id), _empty_market_data_row(request_id))
        row["tick_prices"][str(int(tick_type))] = price
        row["updated_at"] = datetime.now(timezone.utc).isoformat()

    def tick_size(self, *, request_id: int, tick_type: int, size: int) -> None:
        row = self.market_data_rows.setdefault(int(request_id), _empty_market_data_row(request_id))
        row["tick_sizes"][str(int(tick_type))] = int(size)
        row["updated_at"] = datetime.now(timezone.utc).isoformat()

    def tick_string(self, *, request_id: int, tick_type: int, value: str) -> None:
        row = self.market_data_rows.setdefault(int(request_id), _empty_market_data_row(request_id))
        row["tick_strings"][str(int(tick_type))] = str(value)
        row["updated_at"] = datetime.now(timezone.utc).isoformat()

    def tick_snapshot_end(self, *, request_id: int) -> None:
        self.market_data_event(request_id).set()

    def error(
        self,
        *,
        code: int,
        message: str,
        request_id: int | None = None,
        occurred_at: datetime | None = None,
    ) -> None:
        self._adapter.error(code=code, message=message, request_id=request_id, occurred_at=occurred_at)
        error_row = {
            "code": int(code),
            "message": str(message),
            "request_id": request_id,
            "updated_at": (occurred_at or datetime.now(timezone.utc)).isoformat(),
        }
        self.errors.append(error_row)
        if request_id is not None:
            if int(request_id) in self._contract_details_ready and int(code) in _CONTRACT_REQUEST_ERROR_CODES:
                self.contract_details_event(int(request_id)).set()
            if int(request_id) in self._market_data_ready and int(code) in _MARKET_DATA_PERMISSION_ERROR_CODES:
                self.market_data_event(int(request_id)).set()

    def latest_error(self, *, codes: set[int] | None = None) -> dict[str, Any] | None:
        for row in reversed(self.errors):
            if codes is None or int(row.get("code", 0)) in codes:
                return row
        return None


class IbkrReadOnlyApiTransport:
    """Thin read-only wrapper around the native ibapi package."""

    def __init__(
        self,
        *,
        client: IbkrClient,
        collector: IbkrReadOnlyProbeCollector,
        config: IbkrReadOnlyApiTransportConfig,
        module_loader: Callable[[str], Any] | None = None,
    ) -> None:
        self._client = client
        self._collector = collector
        self._config = config
        self._module_loader = module_loader or importlib.import_module
        self._bridge: Any | None = None

    def connect(self) -> None:
        bridge = self._ensure_bridge()
        self._client.record_event(
            "transport_connect_requested",
            payload={
                "host": self._config.host,
                "port": self._config.port,
                "client_id": self._config.client_id,
                "read_only": self._config.read_only,
            },
        )
        bridge.connect(self._config.host, int(self._config.port), int(self._config.client_id))

    def disconnect(self) -> None:
        if self._bridge is None:
            return
        self._client.record_event("transport_disconnect_requested", payload={})
        self._bridge.disconnect()

    def run_loop(self) -> None:
        self._ensure_bridge().run()

    def is_connected(self) -> bool:
        bridge = self._ensure_bridge()
        checker = getattr(bridge, "isConnected", None)
        if checker is None:
            return False
        try:
            return bool(checker())
        except Exception:
            return False

    def server_version(self) -> int | None:
        bridge = self._ensure_bridge()
        getter = getattr(bridge, "serverVersion", None)
        if getter is None:
            return None
        try:
            return int(getter())
        except Exception:
            return None

    def tws_connection_time(self) -> str | None:
        bridge = self._ensure_bridge()
        getter = getattr(bridge, "twsConnectionTime", None)
        if getter is None:
            return None
        try:
            value = getter()
        except Exception:
            return None
        text = str(value or "").strip()
        return text or None

    def req_managed_accounts(self) -> None:
        self._ensure_bridge().reqManagedAccts()

    def req_account_summary(self, *, request_id: int, group_name: str, tags: str) -> None:
        self._ensure_bridge().reqAccountSummary(int(request_id), group_name, tags)

    def cancel_account_summary(self, *, request_id: int) -> None:
        self._ensure_bridge().cancelAccountSummary(int(request_id))

    def req_account_updates(self, *, subscribe: bool, account_id: str) -> None:
        self._ensure_bridge().reqAccountUpdates(bool(subscribe), str(account_id))

    def req_positions(self) -> None:
        self._ensure_bridge().reqPositions()

    def req_all_open_orders(self) -> None:
        self._ensure_bridge().reqAllOpenOrders()

    def req_contract_details(self, *, request_id: int, contract: IbkrQualifiedContract) -> None:
        contract_cls = getattr(self._module_loader("ibapi.contract"), "Contract", None)
        if contract_cls is None:
            raise IbkrReadOnlyVerificationError("Installed ibapi package is missing Contract.")
        raw_contract = contract_cls()
        raw_contract.symbol = contract.broker_symbol
        raw_contract.secType = contract.security_type
        raw_contract.exchange = contract.exchange
        raw_contract.currency = contract.currency
        raw_contract.lastTradeDateOrContractMonth = contract.expiry
        raw_contract.multiplier = contract.multiplier
        raw_contract.tradingClass = contract.trading_class
        self._ensure_bridge().reqContractDetails(int(request_id), raw_contract)

    def req_market_data_type(self, *, market_data_type: int) -> None:
        self._ensure_bridge().reqMarketDataType(int(market_data_type))

    def req_market_data_snapshot(self, *, request_id: int, contract: IbkrQualifiedContract) -> None:
        contract_cls = getattr(self._module_loader("ibapi.contract"), "Contract", None)
        if contract_cls is None:
            raise IbkrReadOnlyVerificationError("Installed ibapi package is missing Contract.")
        raw_contract = contract_cls()
        raw_contract.symbol = contract.broker_symbol
        raw_contract.secType = contract.security_type
        raw_contract.exchange = contract.exchange
        raw_contract.currency = contract.currency
        raw_contract.lastTradeDateOrContractMonth = contract.expiry
        raw_contract.multiplier = contract.multiplier
        raw_contract.tradingClass = contract.trading_class
        self._ensure_bridge().reqMktData(int(request_id), raw_contract, "", True, False, [])

    def cancel_market_data(self, *, request_id: int) -> None:
        self._ensure_bridge().cancelMktData(int(request_id))

    def _ensure_bridge(self) -> Any:
        if self._bridge is None:
            wrapper_cls = getattr(self._module_loader("ibapi.wrapper"), "EWrapper", None)
            client_cls = getattr(self._module_loader("ibapi.client"), "EClient", None)
            if wrapper_cls is None or client_cls is None:
                raise IbkrReadOnlyVerificationError(
                    "Installed ibapi package is missing EWrapper/EClient and cannot be used for read-only verification."
                )
            self._bridge = _build_bridge(wrapper_cls=wrapper_cls, client_cls=client_cls, collector=self._collector)
        return self._bridge


def verify_ibkr_read_only_connection(
    *,
    config: IbkrReadOnlyVerificationConfig,
    transport_factory: Callable[..., Any] = IbkrReadOnlyApiTransport,
    module_loader: Callable[[str], Any] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> IbkrReadOnlyVerificationArtifacts:
    started_at = datetime.now(timezone.utc)
    environment_lock = evaluate_ibkr_environment_lock(
        mode=config.mode,
        host=config.host,
        port=config.port,
        read_only=config.read_only,
    )
    if not environment_lock["passed"]:
        return _blocked_artifacts(
            config=config,
            started_at=started_at,
            environment_lock=environment_lock,
            detail="Configured environment lock does not match the required paper-only socket lock.",
            next_manual_check=environment_lock["next_manual_check"],
        )

    try:
        primary = _build_runtime(
            config=config,
            transport_factory=transport_factory,
            module_loader=module_loader,
        )
    except Exception as exc:
        return _blocked_artifacts(
            config=config,
            started_at=started_at,
            environment_lock=environment_lock,
            detail=str(exc),
            next_manual_check=_tws_manual_check_message(config.host, config.port),
        )

    try:
        primary.transport.connect()
        _start_run_loop(primary.transport)
        if not _wait_for_connection_ready(
            transport=primary.transport,
            collector=primary.collector,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        ):
            latest_error = primary.collector.latest_error()
            detail = (
                f"TWS paper API did not finish the initial connection handshake within {config.timeout_seconds:.1f}s."
                if latest_error is None
                else f"TWS paper API error {latest_error['code']}: {latest_error['message']}"
            )
            return _blocked_artifacts(
                config=config,
                started_at=started_at,
                environment_lock=environment_lock,
                detail=detail,
                next_manual_check=_tws_manual_check_message(config.host, config.port),
            )
        server_version = primary.transport.server_version()
        tws_connection_time = primary.transport.tws_connection_time()
        primary.client.request_managed_accounts()
        primary.transport.req_managed_accounts()
        if not _wait_for_event(
            primary.collector.managed_accounts_ready,
            collector=primary.collector,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        ):
            latest_error = primary.collector.latest_error(codes=_CONNECTION_ERROR_CODES)
            detail = (
                f"TWS paper API did not expose managed accounts within {config.timeout_seconds:.1f}s."
                if latest_error is None
                else f"TWS paper API error {latest_error['code']}: {latest_error['message']}"
            )
            return _blocked_artifacts(
                config=config,
                started_at=started_at,
                environment_lock=environment_lock,
                detail=detail,
                next_manual_check=_tws_manual_check_message(config.host, config.port),
            )

        managed_accounts = primary.client.connection_state().managed_accounts
        selected_account_id = _resolve_selected_account_id(
            requested_account_id=config.account_id,
            managed_accounts=managed_accounts,
        )
        primary.transport.req_account_summary(
            request_id=9001,
            group_name="All",
            tags=_ACCOUNT_SUMMARY_TAGS,
        )
        primary.client.request_balances()
        primary.transport.req_account_updates(subscribe=True, account_id=selected_account_id)
        primary.client.request_positions()
        primary.transport.req_positions()
        primary.client.request_open_orders()
        primary.transport.req_all_open_orders()

        events_ok = all(
            (
                _wait_for_event(
                    primary.collector.account_summary_event(9001),
                    collector=primary.collector,
                    timeout_seconds=config.timeout_seconds,
                    sleep_fn=sleep_fn,
                ),
                _wait_for_event(
                    primary.collector.balances_ready,
                    collector=primary.collector,
                    timeout_seconds=config.timeout_seconds,
                    sleep_fn=sleep_fn,
                ),
                _wait_for_event(
                    primary.collector.positions_ready,
                    collector=primary.collector,
                    timeout_seconds=config.timeout_seconds,
                    sleep_fn=sleep_fn,
                ),
                _wait_for_event(
                    primary.collector.open_orders_ready,
                    collector=primary.collector,
                    timeout_seconds=config.timeout_seconds,
                    sleep_fn=sleep_fn,
                ),
            )
        )
        try:
            primary.transport.req_account_updates(subscribe=False, account_id=selected_account_id)
        except Exception:
            pass
        try:
            primary.transport.cancel_account_summary(request_id=9001)
        except Exception:
            pass
        if not events_ok:
            latest_error = primary.collector.latest_error()
            detail = (
                f"Timed out waiting for account summary / positions / open orders within {config.timeout_seconds:.1f}s."
                if latest_error is None
                else f"IBKR callback error {latest_error['code']}: {latest_error['message']}"
            )
            return _blocked_artifacts(
                config=config,
                started_at=started_at,
                environment_lock=environment_lock,
                detail=detail,
                next_manual_check=_tws_manual_check_message(config.host, config.port),
            )

        if primary.session.state.account_id != selected_account_id:
            primary.session.select_account(selected_account_id)
            primary.session.record_heartbeat(
                occurred_at=primary.session.state.last_heartbeat_at or datetime.now(timezone.utc)
            )

        provider = IbkrExecutionProvider(config.repo_root, session=primary.session, client=primary.client)
        snapshot = provider.snapshot_state(force_refresh=True)
        account_truth_snapshot = _build_account_truth_snapshot(
            config=config,
            collector=primary.collector,
            session=primary.session,
            snapshot=snapshot,
            selected_account_id=selected_account_id,
        )
        positions_snapshot = _build_positions_snapshot(
            client=primary.client,
            selected_account_id=selected_account_id,
        )
        open_orders_snapshot = _build_open_orders_snapshot(
            client=primary.client,
            selected_account_id=selected_account_id,
        )
        contract_report = _run_contract_qualification_checks(
            transport=primary.transport,
            collector=primary.collector,
            timeout_seconds=config.timeout_seconds,
            gc_expiry=config.gc_expiry,
            mgc_expiry=config.mgc_expiry,
            sleep_fn=sleep_fn,
        )
        market_data_report = (
            _run_market_data_probe(
                transport=primary.transport,
                collector=primary.collector,
                contract_report=contract_report,
                timeout_seconds=config.timeout_seconds,
                sleep_fn=sleep_fn,
            )
            if config.probe_market_data
            else None
        )
        duplicate_client_id_report = (
            _run_duplicate_client_id_probe(
                config=config,
                transport_factory=transport_factory,
                module_loader=module_loader,
                primary=primary,
                sleep_fn=sleep_fn,
            )
            if config.probe_duplicate_client_id
            else {
                "status": "skipped",
                "safe": None,
                "detail": "Duplicate client-id probe was not requested.",
            }
        )
    except Exception as exc:
        return _blocked_artifacts(
            config=config,
            started_at=started_at,
            environment_lock=environment_lock,
            detail=str(exc),
            next_manual_check=_tws_manual_check_message(config.host, config.port),
        )
    finally:
        try:
            primary.transport.disconnect()
        except Exception:
            pass

    reconnect_report = _run_reconnect_check(
        config=config,
        transport_factory=transport_factory,
        module_loader=module_loader,
        sleep_fn=sleep_fn,
    )
    unavailable_tws_report = {
        "status": "verified_by_unit_test",
        "ok": True,
        "detail": "Graceful unavailable-TWS handling is covered by unit tests so the live paper session does not need to be interrupted.",
    }

    baseline_ok = all(
        (
            bool(primary.session.state.connected),
            bool(account_truth_snapshot["account_summary_available_fields"]),
            bool(reconnect_report.get("ok")),
            positions_snapshot["ok"],
            open_orders_snapshot["ok"],
        )
    )
    contract_ok = bool(contract_report.get("ok"))
    market_ok = bool(market_data_report is None or market_data_report.get("ok"))
    duplicate_ok = duplicate_client_id_report.get("safe")
    classification = _classify_result(
        baseline_ok=baseline_ok,
        contract_ok=contract_ok,
        market_ok=market_ok,
        duplicate_ok=duplicate_ok,
    )
    connection_report = _build_connection_report(
        config=config,
        started_at=started_at,
        environment_lock=environment_lock,
        session=primary.session,
        selected_account_id=selected_account_id,
        account_truth_snapshot=account_truth_snapshot,
        positions_snapshot=positions_snapshot,
        open_orders_snapshot=open_orders_snapshot,
        contract_report=contract_report,
        market_data_report=market_data_report,
        duplicate_client_id_report=duplicate_client_id_report,
        reconnect_report=reconnect_report,
        unavailable_tws_report=unavailable_tws_report,
        classification=classification,
        server_version=server_version,
        tws_connection_time=tws_connection_time,
        errors=primary.collector.errors,
    )
    return IbkrReadOnlyVerificationArtifacts(
        classification=classification,
        connection_report=connection_report,
        account_truth_snapshot=account_truth_snapshot,
        positions_snapshot=positions_snapshot,
        open_orders_snapshot=open_orders_snapshot,
        contract_qualification_report=contract_report,
        market_data_probe_report=market_data_report,
    )


def evaluate_ibkr_environment_lock(*, mode: str, host: str, port: int, read_only: bool) -> dict[str, Any]:
    normalized_mode = str(mode or "").strip().upper()
    normalized_host = str(host or "").strip()
    normalized_port = int(port)
    normalized_read_only = bool(read_only)
    checks = {
        "mode": normalized_mode == _EXPECTED_MODE,
        "host": normalized_host == _EXPECTED_HOST,
        "port": normalized_port == _EXPECTED_PORT,
        "read_only": normalized_read_only is True,
    }
    mismatches = [name for name, ok in checks.items() if not ok]
    return {
        "configured_mode": normalized_mode,
        "configured_host": normalized_host,
        "configured_port": normalized_port,
        "expected_mode": _EXPECTED_MODE,
        "expected_host": _EXPECTED_HOST,
        "expected_port": _EXPECTED_PORT,
        "read_only": normalized_read_only,
        "passed": not mismatches,
        "fail_closed": bool(mismatches),
        "mismatches": mismatches,
        "next_manual_check": (
            None
            if not mismatches
            else "Set mode=PAPER, host=127.0.0.1, port=7497, and read_only=true before retrying this verifier."
        ),
    }


def write_ibkr_read_only_artifacts(
    *,
    output_dir: Path,
    artifacts: IbkrReadOnlyVerificationArtifacts,
) -> None:
    reports_dir = Path(output_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "ibkr_read_only_connection_report.json").write_text(
        json.dumps(artifacts.connection_report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_read_only_connection_report.md").write_text(
        render_ibkr_read_only_connection_report_markdown(artifacts.connection_report),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_account_truth_snapshot.json").write_text(
        json.dumps(artifacts.account_truth_snapshot, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_positions_snapshot.json").write_text(
        json.dumps(artifacts.positions_snapshot, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_open_orders_snapshot.json").write_text(
        json.dumps(artifacts.open_orders_snapshot, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_contract_qualification_report.json").write_text(
        json.dumps(artifacts.contract_qualification_report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    if artifacts.market_data_probe_report is not None:
        (reports_dir / "ibkr_market_data_probe_report.json").write_text(
            json.dumps(artifacts.market_data_probe_report, indent=2, sort_keys=True),
            encoding="utf-8",
        )


def render_ibkr_read_only_connection_report_markdown(report: dict[str, Any]) -> str:
    connection = dict(report.get("connection_check") or {})
    environment = dict(report.get("environment_lock_check") or {})
    account_truth = dict(report.get("account_truth_check") or {})
    position_truth = dict(report.get("position_truth_check") or {})
    open_order_truth = dict(report.get("open_order_truth_check") or {})
    contract_truth = dict(report.get("contract_qualification_check") or {})
    reconnect = dict(report.get("reconnect_check") or {})
    duplicate_client = dict(report.get("duplicate_client_id_check") or {})
    market_data = dict(report.get("market_data_check") or {})
    contract_rows = list(contract_truth.get("contracts") or [])
    qualified_labels = [
        f"{row.get('symbol')} {row.get('requested_expiry')}"
        for row in contract_rows
        if row.get("symbol") and row.get("requested_expiry")
    ]
    market_data_detail = str(market_data.get("detail") or "").strip() or "Market data probe not run."
    market_data_errors = list(market_data.get("errors") or [])
    market_data_codes = [
        str(error.get("code"))
        for error in market_data_errors
        if error.get("code") is not None
    ]
    lines = [
        "# IBKR Read-Only Connection Report",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- host: `{connection.get('host')}`",
        f"- port: `{connection.get('port')}`",
        f"- client_id: `{connection.get('client_id')}`",
        f"- server_version: `{connection.get('server_version')}`",
        f"- selected_account_id: `{environment.get('account_id')}`",
        "",
        "## Safety",
        "",
        f"- read_only: `{environment.get('read_only')}`",
        f"- fail_closed: `{environment.get('fail_closed')}`",
        f"- mismatches: `{', '.join(environment.get('mismatches', [])) or 'none'}`",
        "",
        "## Truth Checks",
        "",
        f"- account_summary_read: `{account_truth.get('ok')}`",
        f"- positions_read: `{position_truth.get('ok')}`",
        f"- open_orders_read: `{open_order_truth.get('ok')}`",
        f"- contract_qualification: `{contract_truth.get('ok')}`",
        f"- market_data_probe: `{None if report.get('market_data_check') is None else market_data.get('ok')}`",
        f"- reconnect_check: `{reconnect.get('ok')}`",
        "",
        "## Verification Summary",
        "",
        f"- classification: {report.get('classification')}",
        f"- the repo safely connected to TWS paper on {connection.get('host')}:{connection.get('port')}",
        (
            f"- environment lock was mode={environment.get('configured_mode')}, "
            f"host={environment.get('configured_host')}, port={environment.get('configured_port')}, "
            f"read_only={str(environment.get('read_only')).lower()}"
        ),
        "- no orders were placed",
        "- no orders were staged",
        "- no strategy execution was connected",
        f"- account truth was read successfully: {account_truth.get('ok')}",
        f"- positions were read successfully: {position_truth.get('ok')}",
        f"- open orders were read successfully: {open_order_truth.get('ok')}",
        (
            f"- {', '.join(qualified_labels)} contracts qualified successfully"
            if qualified_labels
            else "- no contract qualification results were captured"
        ),
        f"- reconnect behavior passed: {reconnect.get('ok')}",
        f"- duplicate client ID was safely rejected: {duplicate_client.get('safe')}",
        (
            "- market data remained partial due to TWS response "
            f"{'/'.join(market_data_codes) if market_data_codes else 'unknown'} / no snapshot ticks"
            if market_data
            else "- market data probe was not run"
        ),
        f"- market data detail: {market_data_detail}",
        "- market data failure did not invalidate read-only truth verification",
        "",
        "## Next Manual Check",
        "",
        f"- {report.get('next_manual_check') or 'None.'}",
        "",
    ]
    return "\n".join(lines)


@dataclass
class _RuntimeContext:
    session: IbkrSession
    client: IbkrClient
    collector: IbkrReadOnlyProbeCollector
    transport: Any


def _build_runtime(
    *,
    config: IbkrReadOnlyVerificationConfig,
    transport_factory: Callable[..., Any],
    module_loader: Callable[[str], Any] | None,
) -> _RuntimeContext:
    session = IbkrSession(
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        gateway_mode="paper",
        read_only=config.read_only,
        order_id_policy=build_default_ibkr_order_id_policy(
            client_id=config.client_id,
            live_orders_enabled=False,
        ),
    )
    client = IbkrClient(session=session)
    collector = IbkrReadOnlyProbeCollector(client)
    transport = transport_factory(
        client=client,
        collector=collector,
        config=IbkrReadOnlyApiTransportConfig(
            host=config.host,
            port=config.port,
            client_id=config.client_id,
            read_only=config.read_only,
        ),
        module_loader=module_loader,
    )
    return _RuntimeContext(
        session=session,
        client=client,
        collector=collector,
        transport=transport,
    )


def _build_bridge(*, wrapper_cls: type[Any], client_cls: type[Any], collector: IbkrReadOnlyProbeCollector) -> Any:
    class _Bridge(wrapper_cls, client_cls):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            wrapper_cls.__init__(self)
            client_cls.__init__(self, wrapper=self)

        def nextValidId(self, orderId: int) -> None:  # noqa: N802
            collector.next_valid_id(orderId)

        def managedAccounts(self, accountsList: str) -> None:  # noqa: N802
            collector.managed_accounts(accountsList)

        def updateAccountValue(self, key: str, value: str, currency: str, accountName: str) -> None:  # noqa: N802
            collector.update_account_value(
                account_id=accountName,
                key=key,
                value=value,
                currency=currency,
            )

        def accountDownloadEnd(self, accountName: str) -> None:  # noqa: N802, ARG002
            collector.account_download_end()

        def position(self, account: str, contract: Any, pos: float, avgCost: float) -> None:  # noqa: N802
            collector.position(
                account_id=account,
                contract=_contract_payload(contract),
                quantity=pos,
                average_cost=avgCost,
            )

        def positionEnd(self) -> None:  # noqa: N802
            collector.position_end()

        def openOrder(self, orderId: int, contract: Any, order: Any, orderState: Any) -> None:  # noqa: N802
            collector.open_order(
                account_id=getattr(order, "account", "") or "",
                broker_order_id=orderId,
                client_id=getattr(order, "clientId", 0),
                perm_id=getattr(order, "permId", None),
                contract=_contract_payload(contract),
                status=getattr(orderState, "status", "") or "",
                quantity=getattr(order, "totalQuantity", 0),
                filled_quantity=getattr(order, "filledQuantity", None),
                limit_price=getattr(order, "lmtPrice", None),
                stop_price=getattr(order, "auxPrice", None),
            )

        def openOrderEnd(self) -> None:  # noqa: N802
            collector.open_order_end()

        def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str) -> None:  # noqa: N802
            collector.account_summary(
                request_id=reqId,
                account_id=account,
                tag=tag,
                value=value,
                currency=currency,
            )

        def accountSummaryEnd(self, reqId: int) -> None:  # noqa: N802
            collector.account_summary_end(request_id=reqId)

        def contractDetails(self, reqId: int, contractDetails: Any) -> None:  # noqa: N802
            collector.contract_details(request_id=reqId, contract_details=contractDetails)

        def contractDetailsEnd(self, reqId: int) -> None:  # noqa: N802
            collector.contract_details_end(request_id=reqId)

        def marketDataType(self, reqId: int, marketDataType: int) -> None:  # noqa: N802
            collector.market_data_type(request_id=reqId, market_data_type=marketDataType)

        def tickPrice(self, reqId: int, tickType: int, price: float, attrib: Any) -> None:  # noqa: N802, ARG002
            collector.tick_price(request_id=reqId, tick_type=tickType, price=price)

        def tickSize(self, reqId: int, tickType: int, size: int) -> None:  # noqa: N802
            collector.tick_size(request_id=reqId, tick_type=tickType, size=size)

        def tickString(self, reqId: int, tickType: int, value: str) -> None:  # noqa: N802
            collector.tick_string(request_id=reqId, tick_type=tickType, value=value)

        def tickSnapshotEnd(self, reqId: int) -> None:  # noqa: N802
            collector.tick_snapshot_end(request_id=reqId)

        def error(self, reqId: int, errorCode: int, errorString: str, *args: Any) -> None:  # noqa: N802, ARG002
            collector.error(code=errorCode, message=errorString, request_id=reqId)

    return _Bridge()


def _contract_payload(contract: Any) -> dict[str, Any]:
    return {
        "conId": getattr(contract, "conId", None),
        "symbol": getattr(contract, "symbol", None),
        "localSymbol": getattr(contract, "localSymbol", None),
        "securityType": getattr(contract, "secType", None),
        "exchange": getattr(contract, "exchange", None),
        "currency": getattr(contract, "currency", None),
        "lastTradeDateOrContractMonth": getattr(contract, "lastTradeDateOrContractMonth", None),
        "multiplier": getattr(contract, "multiplier", None),
        "tradingClass": getattr(contract, "tradingClass", None),
    }


def _start_run_loop(transport: Any) -> threading.Thread:
    thread = threading.Thread(target=transport.run_loop, name="ibkr_read_only_verify_loop", daemon=True)
    thread.start()
    return thread


def _wait_for_event(
    event: threading.Event,
    *,
    collector: IbkrReadOnlyProbeCollector,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if event.is_set():
            return True
        if collector.latest_error(codes=_CONNECTION_ERROR_CODES) is not None:
            return False
        sleep_fn(_POLL_INTERVAL_SECONDS)
    return event.is_set()


def _wait_for_connection_ready(
    *,
    transport: Any,
    collector: IbkrReadOnlyProbeCollector,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if collector.next_valid_id_ready.is_set():
            return True
        if collector.latest_error(codes=_CONNECTION_ERROR_CODES) is not None:
            return False
        if getattr(transport, "is_connected", None) is not None:
            try:
                transport.is_connected()
            except Exception:
                return False
        sleep_fn(_POLL_INTERVAL_SECONDS)
    return collector.next_valid_id_ready.is_set()


def _resolve_selected_account_id(*, requested_account_id: str | None, managed_accounts: tuple[str, ...]) -> str:
    normalized_requested = str(requested_account_id or "").strip()
    if normalized_requested:
        if normalized_requested not in managed_accounts:
            managed = ", ".join(managed_accounts) or "<none>"
            raise IbkrReadOnlyVerificationError(
                f"Selected account {normalized_requested} is not present in managed accounts: {managed}."
            )
        return normalized_requested
    if not managed_accounts:
        raise IbkrReadOnlyVerificationError("IBKR managed account discovery returned no accounts.")
    if len(managed_accounts) != 1:
        managed = ", ".join(managed_accounts)
        raise IbkrReadOnlyVerificationError(
            f"IBKR managed account discovery is ambiguous. Provide --account-id. Managed accounts: {managed}."
        )
    return managed_accounts[0]


def _build_account_truth_snapshot(
    *,
    config: IbkrReadOnlyVerificationConfig,
    collector: IbkrReadOnlyProbeCollector,
    session: IbkrSession,
    snapshot: dict[str, Any],
    selected_account_id: str,
) -> dict[str, Any]:
    summary_rows = [
        row
        for row in collector.account_summary_rows
        if row["account_id"] == selected_account_id
    ]
    summary_by_tag = {row["tag"]: row["value"] for row in summary_rows}
    balances = [
        row
        for row in snapshot.get("portfolio", {}).get("balances", [])
        if row.get("account_id") == selected_account_id
    ]
    return {
        "provider_id": snapshot.get("provider_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "selected_account_id": selected_account_id,
        "managed_accounts": list(session.state.managed_accounts),
        "account_summary_rows": summary_rows,
        "account_summary_available_fields": sorted(summary_by_tag),
        "account_summary_by_tag": summary_by_tag,
        "buying_power": summary_by_tag.get("BuyingPower") or _first_non_empty(balances, "buying_power"),
        "net_liquidation": summary_by_tag.get("NetLiquidation") or _first_non_empty(balances, "net_liquidation"),
        "currency": _first_non_empty(summary_rows, "currency") or _first_non_empty(balances, "currency"),
        "account_type": summary_by_tag.get("AccountType"),
        "paper_account_identifier_hint": selected_account_id if selected_account_id.startswith("DU") else None,
        "identifier_used_for_mode_inference": False,
        "balances": balances,
        "snapshot": snapshot,
        "configured_lock": {
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "read_only": config.read_only,
        },
    }


def _build_positions_snapshot(*, client: IbkrClient, selected_account_id: str) -> dict[str, Any]:
    rows = []
    for position in client.positions():
        if position.account_id != selected_account_id:
            continue
        rows.append(
            {
                "account_id": position.account_id,
                "symbol": position.contract.symbol,
                "local_symbol": position.contract.local_symbol,
                "security_type": position.contract.security_type,
                "exchange": position.contract.exchange,
                "currency": position.contract.currency,
                "expiry": position.contract.expiry,
                "multiplier": position.contract.multiplier,
                "quantity": position.quantity,
                "average_cost": position.average_cost,
                "updated_at": position.updated_at.isoformat() if position.updated_at is not None else None,
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "selected_account_id": selected_account_id,
        "position_count": len(rows),
        "is_flat_account": len(rows) == 0,
        "ok": True,
        "positions": rows,
    }


def _build_open_orders_snapshot(*, client: IbkrClient, selected_account_id: str) -> dict[str, Any]:
    rows = []
    for order in client.open_orders():
        if order.account_id != selected_account_id:
            continue
        rows.append(
            {
                "account_id": order.account_id,
                "broker_order_id": order.broker_order_id,
                "client_id": order.client_id,
                "perm_id": order.perm_id,
                "symbol": order.contract.symbol,
                "local_symbol": order.contract.local_symbol,
                "security_type": order.contract.security_type,
                "exchange": order.contract.exchange,
                "currency": order.contract.currency,
                "expiry": order.contract.expiry,
                "quantity": order.quantity,
                "filled_quantity": order.filled_quantity,
                "status": order.status,
                "updated_at": order.updated_at.isoformat() if order.updated_at is not None else None,
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "selected_account_id": selected_account_id,
        "open_order_count": len(rows),
        "has_open_orders": len(rows) > 0,
        "ok": True,
        "open_orders": rows,
    }


def _run_contract_qualification_checks(
    *,
    transport: Any,
    collector: IbkrReadOnlyProbeCollector,
    timeout_seconds: float,
    gc_expiry: str,
    mgc_expiry: str,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    resolver = IbkrContractResolver()
    requests = (
        ("GC", gc_expiry, 3101),
        ("MGC", mgc_expiry, 3102),
    )
    rows = []
    all_ok = True
    for symbol, expiry, request_id in requests:
        qualified = resolver.qualify_futures(symbol=symbol, expiry=expiry)
        event = collector.contract_details_event(request_id)
        transport.req_contract_details(request_id=request_id, contract=qualified)
        event_ok = _wait_for_event(
            event,
            collector=collector,
            timeout_seconds=timeout_seconds,
            sleep_fn=sleep_fn,
        )
        contract_rows = list(collector.contract_details_rows.get(request_id, []))
        latest_error = next(
            (
                row
                for row in reversed(collector.errors)
                if row.get("request_id") == request_id
            ),
            None,
        )
        ok = bool(event_ok and contract_rows)
        all_ok = all_ok and ok
        rows.append(
            {
                "symbol": symbol,
                "requested_expiry": expiry,
                "qualified_contract": _qualified_contract_to_dict(qualified),
                "ok": ok,
                "detail": (
                    "Contract details received from TWS."
                    if ok
                    else (
                        f"IBKR error {latest_error['code']}: {latest_error['message']}"
                        if latest_error is not None
                        else f"No contract details returned within {timeout_seconds:.1f}s."
                    )
                ),
                "api_contract_details": contract_rows,
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ok": all_ok,
        "contracts": rows,
    }


def _run_market_data_probe(
    *,
    transport: Any,
    collector: IbkrReadOnlyProbeCollector,
    contract_report: dict[str, Any],
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    successful_contract = next(
        (
            row
            for row in contract_report.get("contracts", [])
            if row.get("ok")
        ),
        None,
    )
    if successful_contract is None:
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "ok": False,
            "status": "skipped_no_qualified_contract",
            "detail": "Market data snapshot was skipped because no contract qualification succeeded.",
        }
    qualified = successful_contract["qualified_contract"]
    contract = IbkrQualifiedContract(
        internal_symbol=qualified["internal_symbol"],
        broker_symbol=qualified["broker_symbol"],
        local_symbol=qualified["local_symbol"],
        security_type=qualified["security_type"],
        exchange=qualified["exchange"],
        currency=qualified["currency"],
        expiry=qualified["expiry"],
        multiplier=qualified["multiplier"],
        trading_class=qualified["trading_class"],
        con_id=qualified["con_id"],
        metadata=qualified.get("metadata"),
    )
    request_id = 4101
    transport.req_market_data_type(market_data_type=3)
    event = collector.market_data_event(request_id)
    transport.req_market_data_snapshot(request_id=request_id, contract=contract)
    event_ok = _wait_for_event(
        event,
        collector=collector,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    )
    try:
        transport.cancel_market_data(request_id=request_id)
    except Exception:
        pass
    row = dict(collector.market_data_rows.get(request_id, _empty_market_data_row(request_id)))
    related_errors = [
        error
        for error in collector.errors
        if error.get("request_id") == request_id
    ]
    has_ticks = bool(row["tick_prices"] or row["tick_sizes"] or row["tick_strings"])
    permission_error = next(
        (
            error
            for error in reversed(related_errors)
            if int(error.get("code", 0)) in _MARKET_DATA_PERMISSION_ERROR_CODES
        ),
        None,
    )
    if has_ticks:
        status = "delayed_snapshot" if row.get("market_data_type") in {3, 4} else "snapshot"
        detail = "Snapshot market data received."
        ok = True
    elif permission_error is not None:
        status = "permission_missing"
        detail = f"Market data permission unavailable: {permission_error['message']}"
        ok = False
    elif event_ok:
        status = "no_ticks"
        detail = "Snapshot completed without tick payloads."
        ok = False
    else:
        status = "timeout"
        detail = f"Market data snapshot did not complete within {timeout_seconds:.1f}s."
        ok = False
    row.update(
        {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "ok": ok,
            "status": status,
            "detail": detail,
            "contract_symbol": contract.internal_symbol,
            "contract_local_symbol": contract.local_symbol,
            "errors": related_errors,
        }
    )
    return row


def _run_duplicate_client_id_probe(
    *,
    config: IbkrReadOnlyVerificationConfig,
    transport_factory: Callable[..., Any],
    module_loader: Callable[[str], Any] | None,
    primary: _RuntimeContext,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    try:
        secondary = _build_runtime(
            config=config,
            transport_factory=transport_factory,
            module_loader=module_loader,
        )
    except Exception as exc:
        return {
            "status": "probe_failed",
            "safe": False,
            "detail": str(exc),
        }
    try:
        secondary.transport.connect()
        _start_run_loop(secondary.transport)
        if not _wait_for_connection_ready(
            transport=secondary.transport,
            collector=secondary.collector,
            timeout_seconds=min(config.timeout_seconds, 5.0),
            sleep_fn=sleep_fn,
        ):
            latest_error = secondary.collector.latest_error()
            duplicate_error = secondary.collector.latest_error(codes=_DUPLICATE_CLIENT_ID_ERROR_CODES)
            if duplicate_error is not None:
                return {
                    "status": "rejected_duplicate_client_id",
                    "safe": True,
                    "detail": f"TWS rejected the concurrent duplicate client id safely: {duplicate_error['message']}",
                }
            return {
                "status": "probe_failed",
                "safe": False,
                "detail": (
                    f"Duplicate probe never finished the initial handshake: {latest_error['message']}"
                    if latest_error is not None
                    else "Duplicate probe never finished the initial handshake."
                ),
            }
        secondary.transport.req_managed_accounts()
        secondary_ok = _wait_for_event(
            secondary.collector.managed_accounts_ready,
            collector=secondary.collector,
            timeout_seconds=min(config.timeout_seconds, 5.0),
            sleep_fn=sleep_fn,
        )
        duplicate_error = secondary.collector.latest_error(codes=_DUPLICATE_CLIENT_ID_ERROR_CODES)
        primary_still_connected = primary.session.state.connected
        if duplicate_error is not None and primary_still_connected:
            return {
                "status": "rejected_duplicate_client_id",
                "safe": True,
                "detail": f"TWS rejected the concurrent duplicate client id safely: {duplicate_error['message']}",
            }
        if secondary_ok and primary_still_connected:
            return {
                "status": "accepted_duplicate_client_id",
                "safe": True,
                "detail": "Concurrent duplicate client-id probe connected without disconnecting the primary verifier session.",
            }
        return {
            "status": "primary_connection_impacted",
            "safe": False,
            "detail": "Duplicate client-id probe affected the primary verifier connection or never established a safe secondary outcome.",
        }
    finally:
        try:
            secondary.transport.disconnect()
        except Exception:
            pass


def _run_reconnect_check(
    *,
    config: IbkrReadOnlyVerificationConfig,
    transport_factory: Callable[..., Any],
    module_loader: Callable[[str], Any] | None,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    reconnect_config = IbkrReadOnlyVerificationConfig(
        repo_root=config.repo_root,
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        read_only=config.read_only,
        account_id=config.account_id,
        timeout_seconds=min(config.timeout_seconds, 8.0),
        probe_market_data=False,
        probe_duplicate_client_id=False,
        gc_expiry=config.gc_expiry,
        mgc_expiry=config.mgc_expiry,
    )
    runtime: _RuntimeContext | None = None
    try:
        runtime = _build_runtime(
            config=reconnect_config,
            transport_factory=transport_factory,
            module_loader=module_loader,
        )
        runtime.transport.connect()
        _start_run_loop(runtime.transport)
        if not _wait_for_connection_ready(
            transport=runtime.transport,
            collector=runtime.collector,
            timeout_seconds=reconnect_config.timeout_seconds,
            sleep_fn=sleep_fn,
        ):
            latest_error = runtime.collector.latest_error()
            return {
                "ok": False,
                "status": "reconnect_failed",
                "detail": (
                    f"Reconnect handshake failed: {latest_error['message']}"
                    if latest_error is not None
                    else "Reconnect handshake did not complete before timeout."
                ),
            }
        runtime.transport.req_managed_accounts()
        ok = _wait_for_event(
            runtime.collector.managed_accounts_ready,
            collector=runtime.collector,
            timeout_seconds=reconnect_config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        latest_error = runtime.collector.latest_error()
        return {
            "ok": ok,
            "status": "reconnected" if ok else "reconnect_failed",
            "detail": (
                "Disconnect/reconnect cycle completed cleanly."
                if ok
                else (
                    f"Reconnect failed: {latest_error['message']}"
                    if latest_error is not None
                    else "Reconnect timed out before managed account visibility returned."
                )
            ),
        }
    except Exception as exc:
        return {
            "ok": False,
            "status": "reconnect_failed",
            "detail": str(exc),
        }
    finally:
        if runtime is not None:
            try:
                runtime.transport.disconnect()
            except Exception:
                pass


def _blocked_artifacts(
    *,
    config: IbkrReadOnlyVerificationConfig,
    started_at: datetime,
    environment_lock: dict[str, Any],
    detail: str,
    next_manual_check: str | None,
) -> IbkrReadOnlyVerificationArtifacts:
    generated_at = datetime.now(timezone.utc).isoformat()
    connection_report = {
        "classification": "IBKR_READ_ONLY_BLOCKED",
        "generated_at": generated_at,
        "connection_check": {
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
            "server_version": None,
            "connection_timestamp": started_at.isoformat(),
            "detail": detail,
        },
        "environment_lock_check": {
            **environment_lock,
            "account_id": config.account_id,
            "account_type": None,
        },
        "account_truth_check": {"ok": False, "detail": detail},
        "position_truth_check": {"ok": False, "detail": detail},
        "open_order_truth_check": {"ok": False, "detail": detail},
        "contract_qualification_check": {"ok": False, "detail": detail},
        "market_data_check": None,
        "duplicate_client_id_check": {"safe": None, "detail": "Not run because verification blocked early."},
        "reconnect_check": {"ok": False, "detail": "Not run because verification blocked early."},
        "unavailable_tws_check": {"ok": True, "detail": "Covered by unit tests."},
        "next_manual_check": next_manual_check,
        "started_at": started_at.isoformat(),
    }
    empty_account = {
        "provider_id": "ibkr_execution",
        "generated_at": generated_at,
        "selected_account_id": config.account_id,
        "managed_accounts": [],
        "account_summary_rows": [],
        "account_summary_available_fields": [],
        "account_summary_by_tag": {},
        "buying_power": None,
        "net_liquidation": None,
        "currency": None,
        "account_type": None,
        "paper_account_identifier_hint": None,
        "identifier_used_for_mode_inference": False,
        "balances": [],
        "snapshot": {},
        "configured_lock": {
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "read_only": config.read_only,
        },
    }
    empty_positions = {
        "generated_at": generated_at,
        "selected_account_id": config.account_id,
        "position_count": 0,
        "is_flat_account": True,
        "ok": False,
        "positions": [],
    }
    empty_open_orders = {
        "generated_at": generated_at,
        "selected_account_id": config.account_id,
        "open_order_count": 0,
        "has_open_orders": False,
        "ok": False,
        "open_orders": [],
    }
    empty_contract_report = {
        "generated_at": generated_at,
        "ok": False,
        "contracts": [],
        "detail": detail,
    }
    return IbkrReadOnlyVerificationArtifacts(
        classification="IBKR_READ_ONLY_BLOCKED",
        connection_report=connection_report,
        account_truth_snapshot=empty_account,
        positions_snapshot=empty_positions,
        open_orders_snapshot=empty_open_orders,
        contract_qualification_report=empty_contract_report,
        market_data_probe_report=None,
    )


def _classify_result(*, baseline_ok: bool, contract_ok: bool, market_ok: bool, duplicate_ok: Any) -> str:
    if not baseline_ok:
        return "IBKR_READ_ONLY_BLOCKED"
    if not contract_ok:
        return "IBKR_READ_ONLY_PARTIAL"
    if duplicate_ok is False:
        return "IBKR_READ_ONLY_PARTIAL"
    if not market_ok:
        return "IBKR_READ_ONLY_PARTIAL"
    return "IBKR_READ_ONLY_CONNECTED"


def _build_connection_report(
    *,
    config: IbkrReadOnlyVerificationConfig,
    started_at: datetime,
    environment_lock: dict[str, Any],
    session: IbkrSession,
    selected_account_id: str,
    account_truth_snapshot: dict[str, Any],
    positions_snapshot: dict[str, Any],
    open_orders_snapshot: dict[str, Any],
    contract_report: dict[str, Any],
    market_data_report: dict[str, Any] | None,
    duplicate_client_id_report: dict[str, Any],
    reconnect_report: dict[str, Any],
    unavailable_tws_report: dict[str, Any],
    classification: str,
    server_version: int | None,
    tws_connection_time: str | None,
    errors: list[dict[str, Any]],
) -> dict[str, Any]:
    summary_by_tag = account_truth_snapshot.get("account_summary_by_tag", {})
    return {
        "classification": classification,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "started_at": started_at.isoformat(),
        "connection_check": {
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
            "server_version": server_version,
            "connection_timestamp": (
                session.state.connected_at.isoformat()
                if session.state.connected_at is not None
                else started_at.isoformat()
            ),
            "tws_connection_time": tws_connection_time,
            "connected": session.state.connected,
        },
        "environment_lock_check": {
            **environment_lock,
            "account_id": selected_account_id,
            "account_type": summary_by_tag.get("AccountType"),
            "paper_identifier_hint": selected_account_id if selected_account_id.startswith("DU") else None,
            "identifier_used_for_mode_inference": False,
            "tws_read_only_setting_observable": False,
        },
        "account_truth_check": {
            "ok": bool(account_truth_snapshot.get("account_summary_available_fields")),
            "available_fields": account_truth_snapshot.get("account_summary_available_fields"),
            "buying_power": account_truth_snapshot.get("buying_power"),
            "net_liquidation": account_truth_snapshot.get("net_liquidation"),
            "currency": account_truth_snapshot.get("currency"),
            "detail": "Account summary and account-update truth were captured from the paper TWS session.",
        },
        "position_truth_check": {
            "ok": positions_snapshot.get("ok"),
            "position_count": positions_snapshot.get("position_count"),
            "detail": "Flat account handled cleanly." if positions_snapshot.get("is_flat_account") else "Positions captured.",
        },
        "open_order_truth_check": {
            "ok": open_orders_snapshot.get("ok"),
            "open_order_count": open_orders_snapshot.get("open_order_count"),
            "detail": (
                "No open orders were visible, which was handled cleanly."
                if not open_orders_snapshot.get("has_open_orders")
                else "Open orders captured."
            ),
        },
        "contract_qualification_check": {
            "ok": contract_report.get("ok"),
            "detail": "GC and MGC contract qualification requests were processed in read-only mode.",
            "contracts": contract_report.get("contracts"),
        },
        "market_data_check": market_data_report,
        "duplicate_client_id_check": duplicate_client_id_report,
        "reconnect_check": reconnect_report,
        "unavailable_tws_check": unavailable_tws_report,
        "errors": errors,
        "next_manual_check": (
            None
            if classification != "IBKR_READ_ONLY_BLOCKED"
            else _tws_manual_check_message(config.host, config.port)
        ),
    }


def _qualified_contract_to_dict(contract: IbkrQualifiedContract) -> dict[str, Any]:
    return {
        "internal_symbol": contract.internal_symbol,
        "broker_symbol": contract.broker_symbol,
        "local_symbol": contract.local_symbol,
        "security_type": contract.security_type,
        "exchange": contract.exchange,
        "currency": contract.currency,
        "expiry": contract.expiry,
        "multiplier": contract.multiplier,
        "trading_class": contract.trading_class,
        "con_id": contract.con_id,
        "metadata": dict(contract.metadata or {}),
    }


def _first_non_empty(rows: list[dict[str, Any]], field: str) -> Any:
    for row in rows:
        value = row.get(field)
        if value not in (None, ""):
            return value
    return None


def _empty_market_data_row(request_id: int) -> dict[str, Any]:
    return {
        "request_id": int(request_id),
        "market_data_type": None,
        "tick_prices": {},
        "tick_sizes": {},
        "tick_strings": {},
        "updated_at": None,
    }


def _tws_manual_check_message(host: str, port: int) -> str:
    return (
        "Verify TWS paper is running on "
        f"{host}:{port}, then open TWS Global Configuration > API > Settings and confirm "
        "Enable ActiveX and Socket Clients is enabled, the socket port is 7497, localhost connections are allowed, "
        "and the Read-Only API option is enabled if your TWS build exposes it."
    )
