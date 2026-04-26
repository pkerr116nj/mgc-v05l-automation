"""TradeStation broker-family discovery and normalized truth scaffolding."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from .broker_truth import (
    BrokerAccountSnapshot,
    BrokerBalanceSnapshot,
    BrokerHealthSnapshot,
    BrokerOpenOrderSnapshot,
    BrokerPositionSnapshot,
    BrokerTruthSnapshot,
)
from .tradestation_auth import (
    TradeStationEnvironment,
    TradeStationSelectedAccounts,
    TradeStationSelectedAccountsStore,
)

TRADESTATION_BROKER_FAMILY_PROVIDER_ID = "tradestation_broker_family"
ACCOUNT_TYPE_MARGIN_EQUITIES_OPTIONS = "margin_equities_options"
ACCOUNT_TYPE_FUTURES = "futures"
ACCOUNT_TYPE_UNKNOWN = "unknown"


class TradeStationAccountSelectionError(RuntimeError):
    """Raised when selected-account requirements are not satisfied."""


class TradeStationBrokerClient(Protocol):
    environment: TradeStationEnvironment

    def list_accounts(self) -> Any:
        """Return accessible broker accounts."""

    def get_balances(self, account_id: str) -> Any:
        """Return balances for an account."""

    def get_positions(self, account_id: str) -> Any:
        """Return positions for an account."""

    def get_open_orders(self, account_id: str) -> Any:
        """Return open orders for an account."""


@dataclass(frozen=True)
class TradeStationDiscoveredAccount:
    account_id: str
    account_number: str | None
    display_name: str
    account_type: str
    environment: str
    selected: bool
    raw_payload: dict[str, Any]


class TradeStationBrokerFamilyService:
    """Discovers TradeStation accounts and builds a two-account truth snapshot."""

    def __init__(
        self,
        *,
        sim_client: TradeStationBrokerClient,
        live_client: TradeStationBrokerClient,
        selected_accounts_store: TradeStationSelectedAccountsStore,
    ) -> None:
        self._clients = {
            TradeStationEnvironment.SIM: sim_client,
            TradeStationEnvironment.LIVE: live_client,
        }
        self._selected_accounts_store = selected_accounts_store

    def discover_accounts(self, environment: TradeStationEnvironment) -> list[TradeStationDiscoveredAccount]:
        client = self._clients[environment]
        selection = self._selected_accounts_store.load_environment(environment)
        selected_ids = {
            selection.selected_margin_account_id,
            selection.selected_futures_account_id,
        }
        rows = _coerce_rows(client.list_accounts())
        accounts: list[TradeStationDiscoveredAccount] = []
        for raw in rows:
            account_id = _first_text(raw, "AccountID", "accountId", "account_id", "Id", "id")
            if account_id is None:
                continue
            account_number = _first_text(raw, "AccountNumber", "accountNumber", "account_number")
            display_name = (
                _first_text(raw, "Alias", "alias", "Name", "name", "Description", "description")
                or account_number
                or account_id
            )
            account_type = classify_tradestation_account(raw)
            accounts.append(
                TradeStationDiscoveredAccount(
                    account_id=account_id,
                    account_number=account_number,
                    display_name=display_name,
                    account_type=account_type,
                    environment=environment.value,
                    selected=account_id in selected_ids,
                    raw_payload=dict(raw),
                )
            )
        return accounts

    def snapshot_state(self, environment: TradeStationEnvironment, *, force_refresh: bool = False) -> dict[str, Any]:
        del force_refresh
        generated_at = datetime.now(timezone.utc)
        client = self._clients[environment]
        accounts = self.discover_accounts(environment)
        selected_accounts = self._resolve_selected_accounts(environment, accounts)
        truth = self._build_truth_snapshot(
            client=client,
            environment=environment,
            accounts=accounts,
            selected_accounts=selected_accounts,
            generated_at=generated_at,
        )
        payload = _truth_snapshot_to_payload(truth)
        payload["broker_family"] = "tradestation"
        payload["environment"] = environment.value
        payload["selected_accounts"] = selected_accounts.to_payload()
        payload["per_account"] = _build_per_account_payload(
            accounts=truth.accounts,
            balances=truth.balances,
            positions=truth.positions,
            open_orders=truth.open_orders,
            environment=environment.value,
            selected_accounts=selected_accounts,
            generated_at=generated_at,
        )
        payload["combined_summary"] = _build_combined_summary(
            balances=truth.balances,
            positions=truth.positions,
            environment=environment.value,
            selected_accounts=selected_accounts,
            generated_at=generated_at,
        )
        return payload

    def _resolve_selected_accounts(
        self,
        environment: TradeStationEnvironment,
        accounts: list[TradeStationDiscoveredAccount],
    ) -> TradeStationSelectedAccounts:
        selection = self._selected_accounts_store.load_environment(environment)
        margin_candidates = [account for account in accounts if account.account_type == ACCOUNT_TYPE_MARGIN_EQUITIES_OPTIONS]
        futures_candidates = [account for account in accounts if account.account_type == ACCOUNT_TYPE_FUTURES]
        self._validate_selected_account(
            account_type=ACCOUNT_TYPE_MARGIN_EQUITIES_OPTIONS,
            selected_id=selection.selected_margin_account_id,
            candidates=margin_candidates,
        )
        self._validate_selected_account(
            account_type=ACCOUNT_TYPE_FUTURES,
            selected_id=selection.selected_futures_account_id,
            candidates=futures_candidates,
        )
        return selection

    def _validate_selected_account(
        self,
        *,
        account_type: str,
        selected_id: str | None,
        candidates: list[TradeStationDiscoveredAccount],
    ) -> None:
        if not candidates:
            raise TradeStationAccountSelectionError(
                f"No accessible TradeStation {account_type} account was discovered."
            )
        if selected_id is None:
            if len(candidates) > 1:
                raise TradeStationAccountSelectionError(
                    f"Multiple TradeStation {account_type} accounts are accessible; an explicit selection is required."
                )
            raise TradeStationAccountSelectionError(
                f"No selected TradeStation {account_type} account id is persisted."
            )
        if not any(account.account_id == selected_id for account in candidates):
            raise TradeStationAccountSelectionError(
                f"Selected TradeStation {account_type} account {selected_id} was not discovered in the current environment."
            )

    def _build_truth_snapshot(
        self,
        *,
        client: TradeStationBrokerClient,
        environment: TradeStationEnvironment,
        accounts: list[TradeStationDiscoveredAccount],
        selected_accounts: TradeStationSelectedAccounts,
        generated_at: datetime,
    ) -> BrokerTruthSnapshot:
        selected_ids = {
            selected_accounts.selected_margin_account_id,
            selected_accounts.selected_futures_account_id,
        }
        account_snapshots = tuple(
            BrokerAccountSnapshot(
                provider_id=TRADESTATION_BROKER_FAMILY_PROVIDER_ID,
                account_id=account.account_id,
                account_number=account.account_number,
                display_name=account.display_name,
                account_type=account.account_type,
                selected=account.account_id in selected_ids,
                updated_at=generated_at,
                raw_payload={**account.raw_payload, "environment": environment.value},
            )
            for account in accounts
        )
        balance_snapshots: list[BrokerBalanceSnapshot] = []
        position_snapshots: list[BrokerPositionSnapshot] = []
        order_snapshots: list[BrokerOpenOrderSnapshot] = []
        for account in accounts:
            balance_snapshots.extend(_normalize_balances(client.get_balances(account.account_id), account, generated_at))
            position_snapshots.extend(_normalize_positions(client.get_positions(account.account_id), account, generated_at))
            try:
                open_orders_payload = client.get_open_orders(account.account_id)
            except NotImplementedError:
                open_orders_payload = []
            order_snapshots.extend(_normalize_open_orders(open_orders_payload, account, generated_at))
        return BrokerTruthSnapshot(
            provider_id=TRADESTATION_BROKER_FAMILY_PROVIDER_ID,
            selected_account_id=None,
            accounts=account_snapshots,
            balances=tuple(balance_snapshots),
            positions=tuple(position_snapshots),
            open_orders=tuple(order_snapshots),
            health=BrokerHealthSnapshot(
                provider_id=TRADESTATION_BROKER_FAMILY_PROVIDER_ID,
                connected=True,
                checked_at=generated_at,
                details={
                    "environment": environment.value,
                    "selected_margin_account_id": selected_accounts.selected_margin_account_id,
                    "selected_futures_account_id": selected_accounts.selected_futures_account_id,
                    "combined_summary_is_application_level": True,
                },
            ),
            generated_at=generated_at,
            metadata={
                "broker_family": "tradestation",
                "environment": environment.value,
            },
        )


def classify_tradestation_account(payload: dict[str, Any]) -> str:
    signals = " ".join(
        [
            _first_text(
                payload,
                "Type",
                "type",
                "AccountType",
                "accountType",
                "AccountTypeDescription",
                "accountTypeDescription",
                "Alias",
                "alias",
                "Name",
                "name",
                "Description",
                "description",
            )
            or "",
            _first_text(payload, "MarginOrCash", "marginOrCash", "TradingType", "tradingType") or "",
        ]
    ).upper()
    if "FUTURE" in signals:
        return ACCOUNT_TYPE_FUTURES
    if any(token in signals for token in ("MARGIN", "EQUITY", "SECURIT", "OPTION", "STOCK")):
        return ACCOUNT_TYPE_MARGIN_EQUITIES_OPTIONS
    return ACCOUNT_TYPE_UNKNOWN


def _normalize_balances(
    payload: Any,
    account: TradeStationDiscoveredAccount,
    generated_at: datetime,
) -> list[BrokerBalanceSnapshot]:
    rows = _coerce_rows(payload)
    if not rows and isinstance(payload, dict):
        rows = [payload]
    normalized: list[BrokerBalanceSnapshot] = []
    for raw in rows:
        normalized.append(
            BrokerBalanceSnapshot(
                provider_id=TRADESTATION_BROKER_FAMILY_PROVIDER_ID,
                account_id=account.account_id,
                currency=_first_text(raw, "Currency", "currency"),
                cash_balance=_first_decimal(raw, "CashBalance", "cashBalance", "Cash", "cash"),
                buying_power=_first_decimal(raw, "BuyingPower", "buyingPower"),
                available_funds=_first_decimal(raw, "AvailableFunds", "availableFunds"),
                net_liquidation=_first_decimal(raw, "NetLiquidation", "netLiquidation", "Equity", "equity"),
                maintenance_requirement=_first_decimal(
                    raw, "MaintenanceRequirement", "maintenanceRequirement", "MaintenanceMargin", "maintenanceMargin"
                ),
                updated_at=_first_datetime(raw, "UpdatedAt", "updatedAt", "AsOf", "asOf") or generated_at,
                raw_payload={**raw, "account_type": account.account_type, "environment": account.environment},
            )
        )
    return normalized


def _normalize_positions(
    payload: Any,
    account: TradeStationDiscoveredAccount,
    generated_at: datetime,
) -> list[BrokerPositionSnapshot]:
    rows = _coerce_rows(payload)
    normalized: list[BrokerPositionSnapshot] = []
    for raw in rows:
        symbol = _first_text(raw, "Symbol", "symbol")
        quantity = _first_decimal(raw, "Quantity", "quantity", "NetQuantity", "netQuantity")
        if symbol is None or quantity is None:
            continue
        side = _position_side(quantity)
        normalized.append(
            BrokerPositionSnapshot(
                provider_id=TRADESTATION_BROKER_FAMILY_PROVIDER_ID,
                account_id=account.account_id,
                symbol=symbol,
                asset_class=_infer_asset_class(account.account_type, raw),
                quantity=abs(quantity),
                side=side,
                average_cost=_first_decimal(raw, "AveragePrice", "averagePrice", "AverageCost", "averageCost"),
                mark_price=_first_decimal(raw, "Last", "last", "Mark", "mark", "MarketPrice", "marketPrice"),
                market_value=_first_decimal(raw, "MarketValue", "marketValue"),
                updated_at=_first_datetime(raw, "UpdatedAt", "updatedAt", "AsOf", "asOf") or generated_at,
                raw_payload={**raw, "account_type": account.account_type, "environment": account.environment},
            )
        )
    return normalized


def _normalize_open_orders(
    payload: Any,
    account: TradeStationDiscoveredAccount,
    generated_at: datetime,
) -> list[BrokerOpenOrderSnapshot]:
    rows = _coerce_rows(payload)
    normalized: list[BrokerOpenOrderSnapshot] = []
    for raw in rows:
        broker_order_id = _first_text(raw, "OrderID", "orderID", "orderId", "id")
        symbol = _first_text(raw, "Symbol", "symbol")
        quantity = _first_decimal(raw, "Quantity", "quantity")
        if broker_order_id is None or symbol is None or quantity is None:
            continue
        normalized.append(
            BrokerOpenOrderSnapshot(
                provider_id=TRADESTATION_BROKER_FAMILY_PROVIDER_ID,
                account_id=account.account_id,
                broker_order_id=broker_order_id,
                symbol=symbol,
                status=_first_text(raw, "Status", "status") or "UNKNOWN",
                quantity=quantity,
                filled_quantity=_first_decimal(raw, "FilledQuantity", "filledQuantity"),
                updated_at=_first_datetime(raw, "UpdatedAt", "updatedAt", "OpenedDateTime", "openedDateTime")
                or generated_at,
                raw_payload={**raw, "account_type": account.account_type, "environment": account.environment},
            )
        )
    return normalized


def _build_per_account_payload(
    *,
    accounts: tuple[BrokerAccountSnapshot, ...],
    balances: tuple[BrokerBalanceSnapshot, ...],
    positions: tuple[BrokerPositionSnapshot, ...],
    open_orders: tuple[BrokerOpenOrderSnapshot, ...],
    environment: str,
    selected_accounts: TradeStationSelectedAccounts,
    generated_at: datetime,
) -> dict[str, Any]:
    by_account: dict[str, Any] = {}
    for account in accounts:
        by_account[account.account_id] = {
            "account_id": account.account_id,
            "display_name": account.display_name,
            "account_type": account.account_type,
            "environment": environment,
            "selected": account.selected,
            "selected_role": (
                "margin_equities_options"
                if account.account_id == selected_accounts.selected_margin_account_id
                else "futures"
                if account.account_id == selected_accounts.selected_futures_account_id
                else None
            ),
            "freshness_timestamp": generated_at.isoformat(),
            "balances": [],
            "positions": [],
            "open_orders": [],
        }
    for balance in balances:
        by_account[balance.account_id]["balances"].append(_dataclass_payload(balance))
    for position in positions:
        by_account[position.account_id]["positions"].append(_dataclass_payload(position))
    for order in open_orders:
        by_account[order.account_id]["open_orders"].append(_dataclass_payload(order))
    return by_account


def _build_combined_summary(
    *,
    balances: tuple[BrokerBalanceSnapshot, ...],
    positions: tuple[BrokerPositionSnapshot, ...],
    environment: str,
    selected_accounts: TradeStationSelectedAccounts,
    generated_at: datetime,
) -> dict[str, Any]:
    total_net_liq = _sum_decimals(balance.net_liquidation for balance in balances)
    total_cash = _sum_decimals(balance.cash_balance for balance in balances)
    total_buying_power = _sum_decimals(balance.buying_power for balance in balances)
    positions_by_account: dict[str, list[dict[str, Any]]] = {}
    positions_by_symbol: dict[str, dict[str, Any]] = {}
    exposure_by_asset_class: dict[str, dict[str, Any]] = {}
    for position in positions:
        positions_by_account.setdefault(position.account_id, []).append(_dataclass_payload(position))
        row = positions_by_symbol.setdefault(
            position.symbol,
            {
                "symbol": position.symbol,
                "accounts": [],
                "asset_class": position.asset_class,
                "gross_quantity": Decimal("0"),
                "net_signed_quantity": Decimal("0"),
            },
        )
        signed = position.quantity if position.side == "LONG" else -position.quantity
        row["accounts"].append(position.account_id)
        row["gross_quantity"] += position.quantity
        row["net_signed_quantity"] += signed
        asset_row = exposure_by_asset_class.setdefault(
            position.asset_class,
            {
                "asset_class": position.asset_class,
                "gross_quantity": Decimal("0"),
                "net_signed_quantity": Decimal("0"),
                "symbols": set(),
            },
        )
        asset_row["gross_quantity"] += position.quantity
        asset_row["net_signed_quantity"] += signed
        asset_row["symbols"].add(position.symbol)
    return {
        "environment": environment,
        "generated_at": generated_at.isoformat(),
        "selected_accounts": selected_accounts.to_payload(),
        "application_level_aggregation": True,
        "total_net_liquidation": str(total_net_liq) if total_net_liq is not None else None,
        "total_cash_balance": str(total_cash) if total_cash is not None else None,
        "total_buying_power": str(total_buying_power) if total_buying_power is not None else None,
        "positions_by_account": positions_by_account,
        "positions_by_symbol": {
            symbol: {
                **value,
                "accounts": sorted(set(value["accounts"])),
                "gross_quantity": str(value["gross_quantity"]),
                "net_signed_quantity": str(value["net_signed_quantity"]),
            }
            for symbol, value in positions_by_symbol.items()
        },
        "exposure_by_asset_class": {
            asset_class: {
                "asset_class": asset_class,
                "gross_quantity": str(value["gross_quantity"]),
                "net_signed_quantity": str(value["net_signed_quantity"]),
                "symbols": sorted(value["symbols"]),
            }
            for asset_class, value in exposure_by_asset_class.items()
        },
    }


def _truth_snapshot_to_payload(snapshot: BrokerTruthSnapshot) -> dict[str, Any]:
    return {
        "provider_id": snapshot.provider_id,
        "selected_account_id": snapshot.selected_account_id,
        "generated_at": snapshot.generated_at.isoformat() if snapshot.generated_at is not None else None,
        "health": {
            "connected": bool(snapshot.health and snapshot.health.connected),
            "checked_at": snapshot.health.checked_at.isoformat() if snapshot.health and snapshot.health.checked_at else None,
            "details": dict(snapshot.health.details) if snapshot.health else {},
        },
        "accounts": [_dataclass_payload(row) for row in snapshot.accounts],
        "balances": [_dataclass_payload(row) for row in snapshot.balances],
        "positions": [_dataclass_payload(row) for row in snapshot.positions],
        "open_orders": [_dataclass_payload(row) for row in snapshot.open_orders],
        "completed_orders": [],
        "executions": [],
        "truth_complete": True,
        "metadata": dict(snapshot.metadata),
    }


def _dataclass_payload(value: Any) -> dict[str, Any]:
    payload = asdict(value)
    for key, item in list(payload.items()):
        if isinstance(item, Decimal):
            payload[key] = str(item)
        elif isinstance(item, datetime):
            payload[key] = item.isoformat()
    return payload


def _coerce_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("Accounts", "accounts", "Balances", "balances", "Positions", "positions", "Orders", "orders"):
            value = payload.get(key)
            if isinstance(value, list):
                return [dict(row) for row in value if isinstance(row, dict)]
        return [dict(payload)]
    return []


def _first_text(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        text = str(value or "").strip()
        if text:
            return text
    return None


def _first_decimal(payload: dict[str, Any], *keys: str) -> Decimal | None:
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            continue
    return None


def _first_datetime(payload: dict[str, Any], *keys: str) -> datetime | None:
    for key in keys:
        text = _first_text(payload, key)
        if text is None:
            continue
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    return None


def _position_side(quantity: Decimal) -> str:
    return "LONG" if quantity >= 0 else "SHORT"


def _infer_asset_class(account_type: str, payload: dict[str, Any]) -> str:
    explicit = _first_text(payload, "AssetType", "assetType", "SecurityType", "securityType")
    if explicit:
        upper = explicit.upper()
        if "FUT" in upper:
            return "FUTURE"
        if "OPT" in upper:
            return "OPTION"
        if "STK" in upper or "EQUITY" in upper or "STOCK" in upper:
            return "STOCK"
    if account_type == ACCOUNT_TYPE_FUTURES:
        return "FUTURE"
    if account_type == ACCOUNT_TYPE_MARGIN_EQUITIES_OPTIONS:
        return "STOCK"
    return "UNKNOWN"


def _sum_decimals(values: Any) -> Decimal | None:
    total = Decimal("0")
    seen = False
    for value in values:
        if value is None:
            continue
        total += value
        seen = True
    return total if seen else None
