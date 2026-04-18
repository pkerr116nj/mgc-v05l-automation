"""Optional TWS / IB Gateway transport bridge for read-only IBKR bring-up."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any, Callable

from .ibkr_callback_adapter import IbkrReadOnlyCallbackAdapter
from .ibkr_client import IbkrClient


class IbkrTransportDependencyMissing(RuntimeError):
    """Raised when optional IBKR transport dependencies are unavailable."""


@dataclass(frozen=True)
class IbkrTwsTransportConfig:
    host: str
    port: int
    client_id: int
    read_only: bool = True


class IbkrTwsTransport:
    """Lazy optional wrapper around the native IBKR TWS API package."""

    def __init__(
        self,
        *,
        client: IbkrClient,
        config: IbkrTwsTransportConfig,
        module_loader: Callable[[str], Any] | None = None,
    ) -> None:
        self._client = client
        self._config = config
        self._module_loader = module_loader or importlib.import_module
        self._bridge: Any | None = None

    @property
    def config(self) -> IbkrTwsTransportConfig:
        return self._config

    def is_available(self) -> bool:
        try:
            self._load_ibapi_classes()
        except IbkrTransportDependencyMissing:
            return False
        return True

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
        bridge = self._ensure_bridge()
        bridge.run()

    def bridge(self) -> Any:
        return self._ensure_bridge()

    def _ensure_bridge(self) -> Any:
        if self._bridge is None:
            wrapper_cls, client_cls = self._load_ibapi_classes()
            adapter = self._client.build_read_only_callback_adapter()
            self._bridge = _build_bridge(wrapper_cls=wrapper_cls, client_cls=client_cls, adapter=adapter)
        return self._bridge

    def _load_ibapi_classes(self) -> tuple[type[Any], type[Any]]:
        try:
            wrapper_module = self._module_loader("ibapi.wrapper")
            client_module = self._module_loader("ibapi.client")
        except ModuleNotFoundError as exc:
            raise IbkrTransportDependencyMissing(
                "Optional IBKR transport dependency 'ibapi' is not installed. "
                "Install the IBKR API package before attempting TWS/Gateway transport bring-up."
            ) from exc
        wrapper_cls = getattr(wrapper_module, "EWrapper", None)
        client_cls = getattr(client_module, "EClient", None)
        if wrapper_cls is None or client_cls is None:
            raise IbkrTransportDependencyMissing(
                "Installed ibapi package is missing EWrapper/EClient and cannot be used for TWS transport."
            )
        return wrapper_cls, client_cls


def _build_bridge(*, wrapper_cls: type[Any], client_cls: type[Any], adapter: IbkrReadOnlyCallbackAdapter) -> Any:
    class _Bridge(wrapper_cls, client_cls):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            wrapper_cls.__init__(self)
            client_cls.__init__(self, wrapper=self)
            self._adapter = adapter

        def nextValidId(self, orderId: int) -> None:  # noqa: N802
            self._adapter.next_valid_id(orderId)

        def managedAccounts(self, accountsList: str) -> None:  # noqa: N802
            self._adapter.managed_accounts(accountsList)

        def updateAccountValue(self, key: str, value: str, currency: str, accountName: str) -> None:  # noqa: N802
            self._adapter.update_account_value(
                account_id=accountName,
                key=key,
                value=value,
                currency=currency,
            )

        def accountDownloadEnd(self, accountName: str) -> None:  # noqa: N802, ARG002
            self._adapter.account_download_end()

        def position(  # noqa: N802
            self,
            account: str,
            contract: Any,
            pos: float,
            avgCost: float,
        ) -> None:
            self._adapter.position(
                account_id=account,
                contract={
                    "conId": getattr(contract, "conId", None),
                    "symbol": getattr(contract, "symbol", None),
                    "localSymbol": getattr(contract, "localSymbol", None),
                    "securityType": getattr(contract, "secType", None),
                    "exchange": getattr(contract, "exchange", None),
                    "currency": getattr(contract, "currency", None),
                    "lastTradeDateOrContractMonth": getattr(contract, "lastTradeDateOrContractMonth", None),
                    "multiplier": getattr(contract, "multiplier", None),
                    "tradingClass": getattr(contract, "tradingClass", None),
                },
                quantity=pos,
                average_cost=avgCost,
            )

        def positionEnd(self) -> None:  # noqa: N802
            self._adapter.position_end()

        def openOrder(  # noqa: N802
            self,
            orderId: int,
            contract: Any,
            order: Any,
            orderState: Any,
        ) -> None:
            self._adapter.open_order(
                account_id=getattr(order, "account", "") or "",
                broker_order_id=orderId,
                client_id=getattr(order, "clientId", 0),
                perm_id=getattr(order, "permId", None),
                contract={
                    "conId": getattr(contract, "conId", None),
                    "symbol": getattr(contract, "symbol", None),
                    "localSymbol": getattr(contract, "localSymbol", None),
                    "securityType": getattr(contract, "secType", None),
                    "exchange": getattr(contract, "exchange", None),
                    "currency": getattr(contract, "currency", None),
                    "lastTradeDateOrContractMonth": getattr(contract, "lastTradeDateOrContractMonth", None),
                    "multiplier": getattr(contract, "multiplier", None),
                    "tradingClass": getattr(contract, "tradingClass", None),
                },
                status=getattr(orderState, "status", "") or "",
                quantity=getattr(order, "totalQuantity", 0),
                filled_quantity=None,
                limit_price=getattr(order, "lmtPrice", None),
                stop_price=getattr(order, "auxPrice", None),
            )

        def openOrderEnd(self) -> None:  # noqa: N802
            self._adapter.open_order_end()

        def completedOrder(self, contract: Any, order: Any, orderState: Any) -> None:  # noqa: N802
            self._adapter.completed_order(
                account_id=getattr(order, "account", "") or "",
                broker_order_id=getattr(order, "orderId", 0),
                client_id=getattr(order, "clientId", 0),
                perm_id=getattr(order, "permId", None),
                contract={
                    "conId": getattr(contract, "conId", None),
                    "symbol": getattr(contract, "symbol", None),
                    "localSymbol": getattr(contract, "localSymbol", None),
                    "securityType": getattr(contract, "secType", None),
                    "exchange": getattr(contract, "exchange", None),
                    "currency": getattr(contract, "currency", None),
                    "lastTradeDateOrContractMonth": getattr(contract, "lastTradeDateOrContractMonth", None),
                    "multiplier": getattr(contract, "multiplier", None),
                    "tradingClass": getattr(contract, "tradingClass", None),
                },
                status=getattr(orderState, "status", "") or "",
                quantity=getattr(order, "totalQuantity", 0),
            )

        def completedOrdersEnd(self) -> None:  # noqa: N802
            self._adapter.completed_orders_end()

        def execDetails(self, reqId: int, contract: Any, execution: Any) -> None:  # noqa: N802, ARG002
            self._adapter.exec_details(
                account_id=getattr(execution, "acctNumber", "") or "",
                execution_id=getattr(execution, "execId", "") or "",
                broker_order_id=getattr(execution, "orderId", None),
                client_id=getattr(execution, "clientId", None),
                perm_id=getattr(execution, "permId", None),
                contract={
                    "conId": getattr(contract, "conId", None),
                    "symbol": getattr(contract, "symbol", None),
                    "localSymbol": getattr(contract, "localSymbol", None),
                    "securityType": getattr(contract, "secType", None),
                    "exchange": getattr(contract, "exchange", None),
                    "currency": getattr(contract, "currency", None),
                    "lastTradeDateOrContractMonth": getattr(contract, "lastTradeDateOrContractMonth", None),
                    "multiplier": getattr(contract, "multiplier", None),
                    "tradingClass": getattr(contract, "tradingClass", None),
                },
                side=getattr(execution, "side", None),
                quantity=getattr(execution, "shares", 0),
                price=getattr(execution, "price", None),
            )

        def execDetailsEnd(self, reqId: int) -> None:  # noqa: N802, ARG002
            self._adapter.exec_details_end()

        def error(self, reqId: int, errorCode: int, errorString: str, *args: Any) -> None:  # noqa: N802, ARG002
            self._adapter.error(code=errorCode, message=errorString, request_id=reqId)

    return _Bridge()

