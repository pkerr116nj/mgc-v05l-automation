from __future__ import annotations

from types import SimpleNamespace

import pytest

from mgc_v05l.brokers.ibkr import (
    IbkrClient,
    IbkrSession,
    IbkrTransportDependencyMissing,
    IbkrTwsTransport,
    IbkrTwsTransportConfig,
    build_default_ibkr_order_id_policy,
)


def _client() -> IbkrClient:
    return IbkrClient(
        session=IbkrSession(
            host="127.0.0.1",
            port=7497,
            client_id=7,
            account_id="DU1234567",
            gateway_mode="paper",
            read_only=True,
            order_id_policy=build_default_ibkr_order_id_policy(client_id=7, live_orders_enabled=False),
        )
    )


def test_ibkr_tws_transport_reports_missing_dependency_cleanly() -> None:
    transport = IbkrTwsTransport(
        client=_client(),
        config=IbkrTwsTransportConfig(host="127.0.0.1", port=7497, client_id=7),
        module_loader=lambda name: (_ for _ in ()).throw(ModuleNotFoundError(name)),
    )

    assert transport.is_available() is False
    with pytest.raises(IbkrTransportDependencyMissing, match="ibapi"):
        transport.connect()


def test_ibkr_tws_transport_builds_bridge_and_records_connect_request() -> None:
    class FakeEWrapper:
        pass

    class FakeEClient:
        def __init__(self, wrapper=None) -> None:
            self.wrapper = wrapper
            self.connected = None
            self.ran = False
            self.disconnected = False

        def connect(self, host: str, port: int, client_id: int) -> None:
            self.connected = (host, port, client_id)

        def run(self) -> None:
            self.ran = True

        def disconnect(self) -> None:
            self.disconnected = True

    def _loader(name: str):
        if name == "ibapi.wrapper":
            return SimpleNamespace(EWrapper=FakeEWrapper)
        if name == "ibapi.client":
            return SimpleNamespace(EClient=FakeEClient)
        raise ModuleNotFoundError(name)

    client = _client()
    transport = IbkrTwsTransport(
        client=client,
        config=IbkrTwsTransportConfig(host="127.0.0.1", port=7497, client_id=7),
        module_loader=_loader,
    )

    assert transport.is_available() is True
    transport.connect()
    bridge = transport.bridge()
    assert bridge.connected == ("127.0.0.1", 7497, 7)
    transport.run_loop()
    assert bridge.ran is True
    transport.disconnect()
    assert bridge.disconnected is True
    assert client.drain_events()[0].event_type == "transport_connect_requested"


def test_ibkr_tws_transport_bridge_feeds_callback_adapter() -> None:
    class FakeEWrapper:
        pass

    class FakeEClient:
        def __init__(self, wrapper=None) -> None:
            self.wrapper = wrapper

        def connect(self, host: str, port: int, client_id: int) -> None:  # pragma: no cover - not used here
            del host, port, client_id

        def run(self) -> None:  # pragma: no cover - not used here
            return None

        def disconnect(self) -> None:  # pragma: no cover - not used here
            return None

    def _loader(name: str):
        if name == "ibapi.wrapper":
            return SimpleNamespace(EWrapper=FakeEWrapper)
        if name == "ibapi.client":
            return SimpleNamespace(EClient=FakeEClient)
        raise ModuleNotFoundError(name)

    client = _client()
    transport = IbkrTwsTransport(
        client=client,
        config=IbkrTwsTransportConfig(host="127.0.0.1", port=7497, client_id=7),
        module_loader=_loader,
    )
    bridge = transport.bridge()

    contract = SimpleNamespace(
        conId=12345,
        symbol="MGC",
        localSymbol="MGCM26",
        secType="FUT",
        exchange="COMEX",
        currency="USD",
        lastTradeDateOrContractMonth="202606",
        multiplier="10",
        tradingClass="MGC",
    )
    order = SimpleNamespace(account="DU1234567", clientId=7, permId=1, totalQuantity="1", lmtPrice="2450.5", auxPrice=None)
    order_state = SimpleNamespace(status="Submitted")
    execution = SimpleNamespace(acctNumber="DU1234567", execId="exec-1", orderId=6998, clientId=7, permId=2, side="BOT", shares="1", price="2450.7")

    bridge.nextValidId(7001)
    bridge.managedAccounts("DU1234567")
    bridge.updateAccountValue("BuyingPower", "100000", "USD", "DU1234567")
    bridge.accountDownloadEnd("DU1234567")
    bridge.position("DU1234567", contract, "1", "2450.5")
    bridge.positionEnd()
    bridge.openOrder(7001, contract, order, order_state)
    bridge.openOrderEnd()
    bridge.completedOrder(contract, SimpleNamespace(orderId=6998, account="DU1234567", clientId=7, permId=2, totalQuantity="1"), SimpleNamespace(status="Filled"))
    bridge.completedOrdersEnd()
    bridge.execDetails(1, contract, execution)
    bridge.execDetailsEnd(1)

    assert client.connection_state().managed_accounts == ("DU1234567",)
    assert client.balances()[0].buying_power == "100000"
    assert client.positions()[0].contract.local_symbol == "MGCM26"
    assert client.open_orders()[0].broker_order_id == 7001
    assert client.completed_orders()[0].broker_order_id == 6998
    assert client.executions()[0].execution_id == "exec-1"
