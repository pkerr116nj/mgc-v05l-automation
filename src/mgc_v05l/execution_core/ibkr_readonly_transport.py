"""Real read-only TWS transport for Track B preflight.

This module is deliberately limited to account, readiness, contract, position,
open-order, and quote observation requests. It does not expose submission
methods and does not construct broker order request objects.
"""

from __future__ import annotations

import importlib
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence

from .ibkr_paper_adapter import IbkrPaperConfigError
from .models import BrokerOrder, PositionSource, PositionState
from .pricing import QuoteObservation


MONTH_CODES = {
    "F": "01",
    "G": "02",
    "H": "03",
    "J": "04",
    "K": "05",
    "M": "06",
    "N": "07",
    "Q": "08",
    "U": "09",
    "V": "10",
    "X": "11",
    "Z": "12",
}


class IbkrReadOnlyTransportError(RuntimeError):
    """Base error for read-only TWS preflight transport failures."""


class IbkrReadOnlyDependencyError(IbkrReadOnlyTransportError):
    """Raised when optional IBKR API modules are unavailable."""


class IbkrReadOnlyTimeoutError(IbkrReadOnlyTransportError):
    """Raised when a required read-only callback is not observed."""


@dataclass(frozen=True)
class IbkrReadOnlyTransportConfig:
    request_timeout_seconds: float = 10.0
    quote_timeout_seconds: float = 3.0


class IbkrReadOnlyTwsTransport:
    """Synchronous adapter around read-only TWS API requests."""

    def __init__(
        self,
        *,
        config: IbkrReadOnlyTransportConfig | None = None,
        module_loader: Callable[[str], Any] | None = None,
    ) -> None:
        self.config = config or IbkrReadOnlyTransportConfig()
        self._module_loader = module_loader or importlib.import_module
        self._bridge: Any | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._request_id = 700000
        self._managed_accounts: tuple[str, ...] = ()
        self._next_valid_id: int | None = None
        self._next_valid_id_source: str | None = None
        self._next_valid_id_requested = False
        self._contract_details: dict[int, list[dict[str, Any]]] = {}
        self._positions: list[dict[str, Any]] = []
        self._open_orders: list[dict[str, Any]] = []
        self._quotes: dict[int, dict[str, Any]] = {}
        self._managed_accounts_ready = threading.Event()
        self._next_valid_id_ready = threading.Event()
        self._positions_ready = threading.Event()
        self._open_orders_ready = threading.Event()
        self._contract_ready: dict[int, threading.Event] = {}
        self._quote_ready: dict[int, threading.Event] = {}
        self._last_contract_key: str | None = None
        self._last_allowlist_entry: dict[str, Any] | None = None
        self._host: str | None = None
        self._port: int | None = None
        self._client_id: int | None = None
        self._socket_connected = False
        self._event_loop_started = False
        self._event_loop_error: str | None = None
        self._ibkr_errors: list[dict[str, Any]] = []
        self._last_error_raw_args: tuple[str, ...] = ()
        self._connect_started_at: datetime | None = None
        self._socket_connected_at: datetime | None = None
        self._event_loop_started_at: datetime | None = None
        self._event_loop_exited_at: datetime | None = None
        self._connection_closed_at: datetime | None = None
        self._connect_ack_received = False
        self._connect_ack_at: datetime | None = None
        self._disconnect_called_by_track_b = False
        self._is_connected_before_wait: bool | None = None
        self._is_connected_after_wait: bool | None = None
        self._is_connected_after_loop_exit: bool | None = None

    def connect(self, *, host: str, port: int, client_id: int, readonly: bool) -> None:
        if not readonly:
            raise IbkrPaperConfigError("read-only preflight transport requires readonly=True")
        if str(host) != "127.0.0.1":
            raise IbkrPaperConfigError("host must be 127.0.0.1")
        if int(port) != 7497:
            raise IbkrPaperConfigError("port must be 7497 for TWS paper")
        if int(client_id) <= 0:
            raise IbkrPaperConfigError("client_id must be an explicit positive integer")

        self._host = host
        self._port = int(port)
        self._client_id = int(client_id)
        self._connect_started_at = _utc_now()
        self._bridge = self._build_bridge()
        self._bridge.connect(host, int(port), int(client_id))
        self._socket_connected = self._is_connected()
        if self._socket_connected:
            self._socket_connected_at = _utc_now()
        self._thread = threading.Thread(target=self._run_loop, name="track-b-ibkr-readonly", daemon=True)
        self._thread.start()

    def disconnect(self) -> None:
        if self._bridge is not None:
            self._disconnect_called_by_track_b = True
            self._bridge.disconnect()

    def bridge_for_test(self) -> Any:
        return self._require_bridge()

    def diagnostics_report(self) -> dict[str, Any]:
        return {
            "host": self._host,
            "port": self._port,
            "client_id": self._client_id,
            "connected_socket": self._socket_connected,
            "connect_started_at": _iso_or_none(self._connect_started_at),
            "socket_connected_at": _iso_or_none(self._socket_connected_at),
            "event_loop_thread_started": self._event_loop_started,
            "event_loop_thread_alive": bool(self._thread and self._thread.is_alive()),
            "event_loop_error": self._event_loop_error,
            "event_loop_started_at": _iso_or_none(self._event_loop_started_at),
            "event_loop_exited_at": _iso_or_none(self._event_loop_exited_at),
            "connection_closed_at": _iso_or_none(self._connection_closed_at),
            "disconnect_called_by_track_b": self._disconnect_called_by_track_b,
            "is_connected_before_wait": self._is_connected_before_wait,
            "is_connected_after_wait": self._is_connected_after_wait,
            "is_connected_after_loop_exit": self._is_connected_after_loop_exit,
            "connect_ack_received": self._connect_ack_received,
            "connect_ack_at": _iso_or_none(self._connect_ack_at),
            "next_valid_id_received": self._next_valid_id is not None,
            "next_valid_id": self._next_valid_id,
            "next_valid_id_source": self._next_valid_id_source,
            "next_valid_id_requested": self._next_valid_id_requested,
            "handshake_timeout_seconds": self.config.request_timeout_seconds,
            "ibkr_errors": list(self._ibkr_errors),
            "suspected_causes": self._suspected_handshake_causes(),
        }

    def managed_accounts(self) -> Sequence[str]:
        bridge = self._require_bridge()
        self._ensure_api_ready()
        if not self._managed_accounts:
            bridge.reqManagedAccts()
        self._wait(self._managed_accounts_ready, "managedAccounts")
        return self._managed_accounts

    def next_valid_id(self) -> int | None:
        self._require_bridge()
        self._wait(self._next_valid_id_ready, "nextValidId")
        return self._next_valid_id

    def qualify_contract(self, *, contract_key: str, allowlist_entry: Mapping[str, Any]) -> Mapping[str, Any]:
        issues = detect_contract_allowlist_ambiguities(contract_key=contract_key, allowlist_entry=allowlist_entry)
        if issues:
            raise IbkrPaperConfigError("; ".join(issues))

        bridge = self._require_bridge()
        self._ensure_api_ready()
        request_id = self._next_request_id()
        self._last_contract_key = contract_key
        self._last_allowlist_entry = dict(allowlist_entry)
        self._contract_ready[request_id] = threading.Event()
        bridge.reqContractDetails(request_id, self._contract_from_allowlist(allowlist_entry))
        self._wait(self._contract_ready[request_id], "contractDetails")
        rows = self._contract_details.get(request_id, [])
        if not rows:
            raise IbkrReadOnlyTimeoutError("contractDetails returned no rows for exact allowlisted contract")
        return {"contract_key": contract_key, **rows[0]}

    def snapshot_position(
        self,
        *,
        run_id: str,
        account_id: str,
        contract_key: str,
        observed_at: datetime,
    ) -> PositionState:
        bridge = self._require_bridge()
        self._positions_ready.clear()
        self._positions = []
        bridge.reqPositions()
        self._wait(self._positions_ready, "positionEnd")

        matches = [row for row in self._positions if row.get("account_id") == account_id and self._contract_matches(contract_key, row)]
        if not matches:
            return PositionState(
                position_state_id=f"tws_position_{run_id}_{contract_key}",
                run_id=run_id,
                source=PositionSource.BROKER,
                account_id=account_id,
                contract_key=contract_key,
                signed_quantity=0,
                average_price=None,
                open_order_ids=(),
                observed_at=observed_at,
                raw={"source": "tws_position_snapshot_no_exact_row"},
            )
        total = sum(Decimal(str(row.get("signed_quantity", "0"))) for row in matches)
        average_price = matches[-1].get("average_price")
        return PositionState(
            position_state_id=f"tws_position_{run_id}_{contract_key}",
            run_id=run_id,
            source=PositionSource.BROKER,
            account_id=account_id,
            contract_key=contract_key,
            signed_quantity=int(total),
            average_price=average_price,
            open_order_ids=(),
            observed_at=observed_at,
            raw={"rows": matches},
        )

    def snapshot_open_orders(
        self,
        *,
        account_id: str,
        contract_key: str,
        observed_at: datetime,
    ) -> Sequence[BrokerOrder | Mapping[str, Any]]:
        bridge = self._require_bridge()
        self._open_orders_ready.clear()
        self._open_orders = []
        bridge.reqOpenOrders()
        self._wait(self._open_orders_ready, "openOrderEnd")
        return tuple(
            row
            for row in self._open_orders
            if row.get("account_id") == account_id and self._contract_matches(contract_key, row)
        )

    def observe_quote(
        self,
        *,
        run_id: str,
        contract_key: str,
        observed_at: datetime,
    ) -> QuoteObservation | None:
        bridge = self._require_bridge()
        if self._last_allowlist_entry is None or self._last_contract_key != contract_key:
            return None
        request_id = self._next_request_id()
        self._quotes[request_id] = {}
        self._quote_ready[request_id] = threading.Event()
        bridge.reqMktData(request_id, self._contract_from_allowlist(self._last_allowlist_entry), "", True, False, [])
        self._quote_ready[request_id].wait(float(self.config.quote_timeout_seconds))
        quote = self._quotes.get(request_id, {})
        cancel = getattr(bridge, "cancelMktData", None)
        if callable(cancel):
            cancel(request_id)
        bid = quote.get("bid")
        ask = quote.get("ask")
        last = quote.get("last")
        if bid is None and ask is None and last is None:
            return None
        return QuoteObservation(
            quote_id=f"tws_quote_{run_id}_{contract_key}_{request_id}",
            run_id=run_id,
            contract_key=contract_key,
            source="ibkr_tws_readonly_snapshot",
            bid=bid,
            ask=ask,
            last=last,
            observed_at=observed_at,
            raw={"request_id": request_id, "ticks": dict(quote)},
        )

    def _build_bridge(self) -> Any:
        wrapper_module = self._load_module("ibapi.wrapper")
        client_module = self._load_module("ibapi.client")
        contract_module = self._load_module("ibapi.contract")
        wrapper_cls = getattr(wrapper_module, "EWrapper")
        client_cls = getattr(client_module, "EClient")
        contract_cls = getattr(contract_module, "Contract")
        owner = self

        class ReadOnlyBridge(wrapper_cls, client_cls):  # type: ignore[misc, valid-type]
            def __init__(self) -> None:
                wrapper_cls.__init__(self)
                client_cls.__init__(self, self)

            def nextValidId(self, orderId: int) -> None:  # noqa: N802
                owner._record_next_valid_id(orderId)

            def managedAccounts(self, accountsList: str) -> None:  # noqa: N802
                owner._record_managed_accounts(accountsList)

            def connectAck(self) -> None:  # noqa: N802
                owner._record_connect_ack()

            def connectionClosed(self) -> None:  # noqa: N802
                owner._record_connection_closed()

            def contractDetails(self, reqId: int, contractDetails: Any) -> None:  # noqa: N802
                owner._record_contract_details(reqId, contractDetails)

            def contractDetailsEnd(self, reqId: int) -> None:  # noqa: N802
                owner._contract_ready.setdefault(reqId, threading.Event()).set()

            def position(self, account: str, contract: Any, pos: float, avgCost: float) -> None:  # noqa: N802
                owner._record_position(account, contract, pos, avgCost)

            def positionEnd(self) -> None:  # noqa: N802
                owner._positions_ready.set()

            def openOrder(self, orderId: int, contract: Any, order: Any, orderState: Any) -> None:  # noqa: N802
                owner._record_open_order(orderId, contract, order, orderState)

            def openOrderEnd(self) -> None:  # noqa: N802
                owner._open_orders_ready.set()

            def tickPrice(self, reqId: int, tickType: int, price: float, attrib: Any) -> None:  # noqa: N802, ARG002
                owner._record_tick_price(reqId, tickType, price)

            def tickSnapshotEnd(self, reqId: int) -> None:  # noqa: N802
                owner._quote_ready.setdefault(reqId, threading.Event()).set()

            def error(self, *args: Any) -> None:  # noqa: N802
                owner._record_error_from_callback(args)

        self._contract_cls = contract_cls
        return ReadOnlyBridge()

    def _contract_from_allowlist(self, allowlist_entry: Mapping[str, Any]) -> Any:
        contract = self._contract_cls()
        contract.symbol = str(allowlist_entry.get("symbol") or "")
        contract.secType = str(allowlist_entry.get("security_type") or allowlist_entry.get("secType") or "FUT")
        contract.exchange = str(allowlist_entry.get("exchange") or "")
        contract.currency = str(allowlist_entry.get("currency") or "USD")
        contract.lastTradeDateOrContractMonth = str(
            allowlist_entry.get("contract_month") or allowlist_entry.get("expiry") or ""
        )
        if allowlist_entry.get("local_symbol"):
            contract.localSymbol = str(allowlist_entry["local_symbol"])
        if allowlist_entry.get("con_id") is not None:
            contract.conId = int(allowlist_entry["con_id"])
        if allowlist_entry.get("multiplier") is not None:
            contract.multiplier = str(allowlist_entry["multiplier"])
        return contract

    def _load_module(self, name: str) -> Any:
        try:
            return self._module_loader(name)
        except ModuleNotFoundError as exc:
            raise IbkrReadOnlyDependencyError(f"optional IBKR API module is unavailable: {name}") from exc

    def _require_bridge(self) -> Any:
        if self._bridge is None:
            raise IbkrReadOnlyTransportError("transport is not connected")
        return self._bridge

    def _run_loop(self) -> None:
        self._event_loop_started = True
        self._event_loop_started_at = _utc_now()
        try:
            self._require_bridge().run()
        except Exception as exc:  # noqa: BLE001 - surface background reader failures in diagnostics.
            self._event_loop_error = str(exc)
        finally:
            self._event_loop_exited_at = _utc_now()
            self._is_connected_after_loop_exit = self._is_connected()

    def _next_request_id(self) -> int:
        with self._lock:
            self._request_id += 1
            return self._request_id

    def _ensure_api_ready(self) -> None:
        self._require_bridge()
        self._wait(self._next_valid_id_ready, "nextValidId")

    def _wait(self, event: threading.Event, callback_name: str) -> None:
        self._is_connected_before_wait = self._is_connected()
        completed = event.wait(float(self.config.request_timeout_seconds))
        self._is_connected_after_wait = self._is_connected()
        if not completed:
            raise IbkrReadOnlyTimeoutError(f"missing {callback_name} callback")

    def _record_next_valid_id(self, order_id: int) -> None:
        self._next_valid_id = int(order_id)
        self._next_valid_id_source = "requested" if self._next_valid_id_requested else "initial_passive"
        self._next_valid_id_ready.set()

    def _record_managed_accounts(self, accounts_list: str) -> None:
        self._managed_accounts = tuple(account.strip() for account in str(accounts_list or "").split(",") if account.strip())
        self._managed_accounts_ready.set()

    def _record_connect_ack(self) -> None:
        self._connect_ack_received = True
        self._connect_ack_at = _utc_now()

    def _record_connection_closed(self) -> None:
        self._connection_closed_at = _utc_now()

    def _record_contract_details(self, request_id: int, contract_details: Any) -> None:
        contract = getattr(contract_details, "contract", contract_details)
        row = {
            "con_id": getattr(contract, "conId", None),
            "symbol": getattr(contract, "symbol", None),
            "security_type": getattr(contract, "secType", None),
            "exchange": getattr(contract, "exchange", None),
            "currency": getattr(contract, "currency", None),
            "local_symbol": getattr(contract, "localSymbol", None),
            "contract_month": getattr(contract, "lastTradeDateOrContractMonth", None),
            "multiplier": getattr(contract, "multiplier", None),
            "trading_class": getattr(contract, "tradingClass", None),
        }
        self._contract_details.setdefault(request_id, []).append(row)

    def _record_position(self, account: str, contract: Any, pos: float, avg_cost: float) -> None:
        self._positions.append(
            {
                "account_id": account,
                "signed_quantity": pos,
                "average_price": avg_cost,
                **_contract_payload(contract),
            }
        )

    def _record_open_order(self, order_id: int, contract: Any, order: Any, order_state: Any) -> None:
        self._open_orders.append(
            {
                "broker_order_id": str(order_id),
                "perm_id": str(getattr(order, "permId", "")) or None,
                "client_id": getattr(order, "clientId", None),
                "account_id": getattr(order, "account", "") or "",
                "action": getattr(order, "action", "BUY") or "BUY",
                "quantity": getattr(order, "totalQuantity", 1) or 1,
                "order_type": getattr(order, "orderType", "LMT") or "LMT",
                "limit_price": getattr(order, "lmtPrice", 1) or 1,
                "status": getattr(order_state, "status", "") or "Submitted",
                "filled_quantity": 0,
                "remaining_quantity": getattr(order, "totalQuantity", 1) or 1,
                "average_fill_price": None,
                **_contract_payload(contract),
            }
        )

    def _record_tick_price(self, request_id: int, tick_type: int, price: float) -> None:
        field = {1: "bid", 2: "ask", 4: "last", 66: "bid", 67: "ask", 68: "last"}.get(int(tick_type))
        if field is None or price <= 0:
            return
        self._quotes.setdefault(request_id, {})[field] = str(price)
        if {"bid", "ask"}.issubset(self._quotes[request_id]):
            self._quote_ready.setdefault(request_id, threading.Event()).set()

    def _record_error_from_callback(self, args: tuple[Any, ...]) -> None:
        self._last_error_raw_args = tuple(repr(arg) for arg in args)
        request_id: int
        error_code: int
        error_string: str
        if len(args) >= 4 and isinstance(args[2], int):
            request_id = int(args[0])
            error_code = int(args[2])
            error_string = str(args[3])
        elif len(args) >= 3:
            request_id = int(args[0])
            error_code = int(args[1])
            error_string = str(args[2])
        else:
            request_id = -1
            error_code = -1
            error_string = "unknown IBKR error callback shape"
        self._record_error(request_id, error_code, error_string)

    def _record_error(self, request_id: int, error_code: int, error_string: str) -> None:
        self._ibkr_errors.append(
            {
                "request_id": request_id,
                "error_code": error_code,
                "error_string": error_string,
                "raw_args": list(getattr(self, "_last_error_raw_args", ())),
            }
        )
        if request_id in self._contract_ready and error_code >= 200:
            self._contract_ready[request_id].set()
        if request_id in self._quote_ready and error_code >= 200:
            self._quote_ready[request_id].set()

    def _suspected_handshake_causes(self) -> tuple[str, ...]:
        if self._next_valid_id is not None:
            return ()
        causes: list[str] = []
        codes = {int(error.get("error_code", -1)) for error in self._ibkr_errors}
        if 326 in codes:
            causes.append("client_id collision")
        if not self._socket_connected:
            causes.append("socket connection not established")
        if not self._event_loop_started:
            causes.append("event loop thread did not start")
        if self._event_loop_exited_at is not None:
            causes.append("IBKR API event loop exited before nextValidId")
        if self._connection_closed_at is not None:
            causes.append("IBKR connectionClosed callback received")
        if self._event_loop_error:
            causes.append("event loop thread error")
        causes.extend(
            [
                "TWS paper API disabled or not accepting clients",
                "TWS modal dialog/API-block condition",
                "TWS did not complete API handshake before timeout",
            ]
        )
        return tuple(dict.fromkeys(causes))

    def _is_connected(self) -> bool:
        if self._bridge is None:
            return False
        is_connected = getattr(self._bridge, "isConnected", None)
        return bool(is_connected()) if callable(is_connected) else True

    def _contract_matches(self, contract_key: str, row: Mapping[str, Any]) -> bool:
        if self._last_contract_key != contract_key or self._last_allowlist_entry is None:
            return False
        expected = self._last_allowlist_entry
        expected_con_id = expected.get("con_id")
        if expected_con_id is not None and row.get("con_id") is not None:
            return int(row["con_id"]) == int(expected_con_id)
        expected_local_symbol = str(expected.get("local_symbol") or "")
        if expected_local_symbol and row.get("local_symbol"):
            return str(row["local_symbol"]) == expected_local_symbol
        expected_symbol = str(expected.get("symbol") or "")
        return bool(expected_symbol and row.get("symbol") == expected_symbol)


def detect_contract_allowlist_ambiguities(*, contract_key: str, allowlist_entry: Mapping[str, Any]) -> tuple[str, ...]:
    issues: list[str] = []
    key_month = _month_from_contract_key(contract_key)
    configured_month = str(allowlist_entry.get("contract_month") or "")[:6]
    local_month = _month_from_local_symbol(str(allowlist_entry.get("local_symbol") or ""))

    if key_month and configured_month and key_month != configured_month:
        issues.append(
            f"contract_key {contract_key} implies {key_month}, but contract_month implies {configured_month}"
        )
    if key_month and local_month and key_month != local_month:
        issues.append(
            f"contract_key {contract_key} implies {key_month}, but local_symbol {allowlist_entry.get('local_symbol')} implies {local_month}"
        )
    return tuple(issues)


def _month_from_contract_key(contract_key: str) -> str | None:
    match = re.search(r"(\d{6})", contract_key)
    return match.group(1) if match else None


def _month_from_local_symbol(local_symbol: str) -> str | None:
    match = re.search(r"([FGHJKMNQUVXZ])(\d)$", local_symbol.upper())
    if not match:
        return None
    month = MONTH_CODES[match.group(1)]
    year = f"202{match.group(2)}"
    return f"{year}{month}"


def _contract_payload(contract: Any) -> dict[str, Any]:
    return {
        "con_id": getattr(contract, "conId", None),
        "symbol": getattr(contract, "symbol", None),
        "security_type": getattr(contract, "secType", None),
        "exchange": getattr(contract, "exchange", None),
        "currency": getattr(contract, "currency", None),
        "local_symbol": getattr(contract, "localSymbol", None),
        "contract_month": getattr(contract, "lastTradeDateOrContractMonth", None),
        "multiplier": getattr(contract, "multiplier", None),
    }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_or_none(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None
