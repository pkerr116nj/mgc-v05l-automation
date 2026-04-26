from __future__ import annotations

import inspect
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.execution.tradestation_auth import (
    TradeStationEnvironment,
    TradeStationSelectedAccounts,
    TradeStationSelectedAccountsStore,
    TradeStationTokenSet,
    TradeStationTokenStore,
)
from mgc_v05l.execution.tradestation_broker_family import (
    ACCOUNT_TYPE_FUTURES,
    ACCOUNT_TYPE_MARGIN_EQUITIES_OPTIONS,
    TradeStationAccountSelectionError,
    TradeStationBrokerFamilyService,
    classify_tradestation_account,
)
from mgc_v05l.execution.tradestation_client import TRADESTATION_LIVE_BASE_URL, TRADESTATION_SIM_BASE_URL, TradeStationClient


class _FakeTradeStationClient:
    def __init__(self, *, environment: TradeStationEnvironment, accounts: list[dict], balances: dict, positions: dict, orders: dict) -> None:
        self.environment = environment
        self._accounts = accounts
        self._balances = balances
        self._positions = positions
        self._orders = orders

    def list_accounts(self):
        return list(self._accounts)

    def get_balances(self, account_id: str):
        return list(self._balances.get(account_id, []))

    def get_positions(self, account_id: str):
        return list(self._positions.get(account_id, []))

    def get_open_orders(self, account_id: str):
        return list(self._orders.get(account_id, []))


def test_account_classification_distinguishes_margin_and_futures_accounts() -> None:
    margin = {"AccountID": "EQ-1", "AccountType": "Margin", "Alias": "Equities Margin"}
    futures = {"AccountID": "FU-1", "AccountTypeDescription": "Futures", "Alias": "Index Futures"}

    assert classify_tradestation_account(margin) == ACCOUNT_TYPE_MARGIN_EQUITIES_OPTIONS
    assert classify_tradestation_account(futures) == ACCOUNT_TYPE_FUTURES


def test_missing_selected_account_rejects_snapshot_build(tmp_path: Path) -> None:
    store = TradeStationSelectedAccountsStore(tmp_path / "selected_accounts.json")
    service = TradeStationBrokerFamilyService(
        sim_client=_FakeTradeStationClient(
            environment=TradeStationEnvironment.SIM,
            accounts=[
                {"AccountID": "EQ-1", "AccountType": "Margin"},
                {"AccountID": "FU-1", "AccountTypeDescription": "Futures"},
            ],
            balances={},
            positions={},
            orders={},
        ),
        live_client=_FakeTradeStationClient(
            environment=TradeStationEnvironment.LIVE,
            accounts=[],
            balances={},
            positions={},
            orders={},
        ),
        selected_accounts_store=store,
    )

    with pytest.raises(TradeStationAccountSelectionError, match="No selected TradeStation margin_equities_options account id"):
        service.snapshot_state(TradeStationEnvironment.SIM)


def test_ambiguous_account_rejects_when_selection_missing(tmp_path: Path) -> None:
    store = TradeStationSelectedAccountsStore(tmp_path / "selected_accounts.json")
    service = TradeStationBrokerFamilyService(
        sim_client=_FakeTradeStationClient(
            environment=TradeStationEnvironment.SIM,
            accounts=[
                {"AccountID": "FU-1", "AccountTypeDescription": "Futures Alpha"},
                {"AccountID": "FU-2", "AccountTypeDescription": "Futures Beta"},
                {"AccountID": "EQ-1", "AccountType": "Margin"},
            ],
            balances={},
            positions={},
            orders={},
        ),
        live_client=_FakeTradeStationClient(
            environment=TradeStationEnvironment.LIVE,
            accounts=[],
            balances={},
            positions={},
            orders={},
        ),
        selected_accounts_store=store,
    )
    store.save_environment(
        TradeStationEnvironment.SIM,
        TradeStationSelectedAccounts(selected_margin_account_id="EQ-1", selected_futures_account_id=None),
    )

    with pytest.raises(TradeStationAccountSelectionError, match="No selected TradeStation futures account id|Multiple TradeStation futures accounts"):
        service.snapshot_state(TradeStationEnvironment.SIM)


def test_sim_and_live_paths_remain_separate(tmp_path: Path) -> None:
    sim_store = TradeStationTokenStore(tmp_path / "sim_tokens.json")
    live_store = TradeStationTokenStore(tmp_path / "live_tokens.json")
    sim_store.save(TradeStationTokenSet(access_token="sim-token", refresh_token="sim-refresh"))
    live_store.save(TradeStationTokenSet(access_token="live-token", refresh_token="live-refresh"))

    selection_store = TradeStationSelectedAccountsStore(tmp_path / "selected_accounts.json")
    selection_store.save_environment(
        TradeStationEnvironment.SIM,
        TradeStationSelectedAccounts(selected_margin_account_id="EQ-SIM", selected_futures_account_id="FU-SIM"),
    )
    selection_store.save_environment(
        TradeStationEnvironment.LIVE,
        TradeStationSelectedAccounts(selected_margin_account_id="EQ-LIVE", selected_futures_account_id="FU-LIVE"),
    )

    sim_client = TradeStationClient(environment=TradeStationEnvironment.SIM, token_store=sim_store)
    live_client = TradeStationClient(environment=TradeStationEnvironment.LIVE, token_store=live_store)

    assert sim_client.base_url == TRADESTATION_SIM_BASE_URL
    assert live_client.base_url == TRADESTATION_LIVE_BASE_URL
    assert selection_store.load_environment(TradeStationEnvironment.SIM).selected_futures_account_id == "FU-SIM"
    assert selection_store.load_environment(TradeStationEnvironment.LIVE).selected_futures_account_id == "FU-LIVE"


def test_combined_summary_aggregates_positions_and_equity(tmp_path: Path) -> None:
    store = TradeStationSelectedAccountsStore(tmp_path / "selected_accounts.json")
    store.save_environment(
        TradeStationEnvironment.SIM,
        TradeStationSelectedAccounts(selected_margin_account_id="EQ-1", selected_futures_account_id="FU-1"),
    )
    now = datetime(2026, 4, 26, 10, 0, tzinfo=timezone.utc).isoformat()
    service = TradeStationBrokerFamilyService(
        sim_client=_FakeTradeStationClient(
            environment=TradeStationEnvironment.SIM,
            accounts=[
                {"AccountID": "EQ-1", "AccountType": "Margin", "Alias": "Equities Margin"},
                {"AccountID": "FU-1", "AccountTypeDescription": "Futures", "Alias": "Index Futures"},
            ],
            balances={
                "EQ-1": [{"NetLiquidation": "150000", "CashBalance": "50000", "BuyingPower": "200000", "UpdatedAt": now}],
                "FU-1": [{"NetLiquidation": "80000", "CashBalance": "30000", "BuyingPower": "90000", "UpdatedAt": now}],
            },
            positions={
                "EQ-1": [{"Symbol": "AAPL", "Quantity": "100", "AveragePrice": "180", "MarketValue": "18500", "AssetType": "STK", "UpdatedAt": now}],
                "FU-1": [{"Symbol": "NQ", "Quantity": "2", "AveragePrice": "18300", "MarketValue": "36600", "AssetType": "FUT", "UpdatedAt": now}],
            },
            orders={"EQ-1": [], "FU-1": []},
        ),
        live_client=_FakeTradeStationClient(
            environment=TradeStationEnvironment.LIVE,
            accounts=[],
            balances={},
            positions={},
            orders={},
        ),
        selected_accounts_store=store,
    )

    payload = service.snapshot_state(TradeStationEnvironment.SIM)

    assert payload["broker_family"] == "tradestation"
    assert payload["environment"] == "sim"
    assert payload["selected_accounts"]["selected_margin_account_id"] == "EQ-1"
    assert payload["selected_accounts"]["selected_futures_account_id"] == "FU-1"
    assert payload["combined_summary"]["application_level_aggregation"] is True
    assert payload["combined_summary"]["total_net_liquidation"] == "230000"
    assert payload["combined_summary"]["total_cash_balance"] == "80000"
    assert payload["combined_summary"]["positions_by_symbol"]["NQ"]["asset_class"] == "FUTURE"
    assert payload["combined_summary"]["exposure_by_asset_class"]["FUTURE"]["gross_quantity"] == "2"
    assert payload["per_account"]["FU-1"]["account_type"] == "futures"


def test_forward_tradestation_path_has_no_schwab_dependency() -> None:
    from mgc_v05l.execution import tradestation_auth, tradestation_broker_family, tradestation_client

    source = "\n".join(
        [
            inspect.getsource(tradestation_auth),
            inspect.getsource(tradestation_client),
            inspect.getsource(tradestation_broker_family),
        ]
    ).lower()

    assert "schwab" not in source
