"""Broker-facing models for the production-link layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ProductionFeatureFlags:
    production_connectivity_enabled: bool = False
    manual_order_ticket_enabled: bool = False
    manual_live_pilot_enabled: bool = False
    futures_pilot_enabled: bool = False
    live_order_submit_enabled: bool = False
    stock_market_live_submit_enabled: bool = False
    stock_limit_live_submit_enabled: bool = False
    stock_stop_live_submit_enabled: bool = False
    stock_stop_limit_live_submit_enabled: bool = False
    advanced_tif_enabled: bool = False
    ext_exto_ticket_support_enabled: bool = False
    oco_ticket_support_enabled: bool = False
    ext_exto_live_submit_enabled: bool = False
    oco_live_submit_enabled: bool = False
    trailing_live_submit_enabled: bool = False
    close_order_live_submit_enabled: bool = False
    futures_live_submit_enabled: bool = False
    portfolio_statement_enabled: bool = False
    analytics_overlays_enabled: bool = False
    replace_order_enabled: bool = False
    sell_short_enabled: bool = False
    supported_manual_asset_classes: tuple[str, ...] = ("STOCK",)
    supported_manual_order_types: tuple[str, ...] = ("LIMIT",)
    supported_manual_dry_run_order_types: tuple[str, ...] = (
        "MARKET",
        "LIMIT",
        "STOP",
        "STOP_LIMIT",
        "TRAIL_STOP",
        "TRAIL_STOP_LIMIT",
        "MARKET_ON_CLOSE",
        "LIMIT_ON_CLOSE",
    )
    supported_manual_time_in_force_values: tuple[str, ...] = ("DAY", "GTC")
    supported_manual_session_values: tuple[str, ...] = ("NORMAL",)
    live_verified_order_keys: tuple[str, ...] = ()
    manual_symbol_whitelist: tuple[str, ...] = ()
    manual_max_quantity: Decimal = Decimal("1")
    futures_symbol_whitelist: tuple[str, ...] = ()
    futures_supported_asset_classes: tuple[str, ...] = ("FUTURE",)
    futures_supported_order_types: tuple[str, ...] = ("MARKET",)
    futures_supported_time_in_force_values: tuple[str, ...] = ("DAY",)
    futures_supported_session_values: tuple[str, ...] = ("NORMAL",)
    futures_max_quantity: Decimal = Decimal("1")
    futures_market_data_symbol_map: dict[str, str] = field(default_factory=dict)
    broker_freshness_max_age_seconds: int = 120


@dataclass(frozen=True)
class SchwabProductionLinkConfig:
    repo_root: Path
    enabled: bool
    broker_provider_id: str
    market_data_provider_id: str
    features: ProductionFeatureFlags
    trader_api_base_url: str
    market_data_config_path: Path
    request_timeout_seconds: int
    cache_ttl_seconds: int
    database_path: Path
    snapshot_path: Path
    selected_account_path: Path
    open_orders_lookback_days: int
    recent_fills_lookback_days: int
    manual_order_ack_timeout_seconds: int
    manual_order_fill_timeout_seconds: int
    manual_order_reconcile_grace_seconds: int
    manual_order_post_ack_grace_seconds: int
    default_account_hash: str | None = None
    default_account_number: str | None = None
    config_path: Path | None = None


@dataclass(frozen=True)
class BrokerAccountIdentity:
    broker_name: str
    account_hash: str
    account_number: str | None
    display_name: str
    account_type: str | None
    selected: bool
    source: str
    updated_at: datetime
    raw_payload: dict[str, Any]

    @property
    def account_id(self) -> str:
        return self.account_hash


@dataclass(frozen=True)
class BrokerBalanceSnapshot:
    account_hash: str
    currency: str | None
    liquidation_value: Decimal | None
    buying_power: Decimal | None
    available_funds: Decimal | None
    cash_balance: Decimal | None
    long_market_value: Decimal | None
    short_market_value: Decimal | None
    day_trading_buying_power: Decimal | None
    maintenance_requirement: Decimal | None
    margin_balance: Decimal | None
    fetched_at: datetime
    raw_payload: dict[str, Any]

    @property
    def account_id(self) -> str:
        return self.account_hash


@dataclass(frozen=True)
class BrokerPositionSnapshot:
    account_hash: str
    position_key: str
    symbol: str
    description: str | None
    asset_class: str
    quantity: Decimal
    side: str
    average_cost: Decimal | None
    mark_price: Decimal | None
    market_value: Decimal | None
    current_day_pnl: Decimal | None
    open_pnl: Decimal | None
    ytd_pnl: Decimal | None
    margin_impact: Decimal | None
    broker_position_id: str | None
    fetched_at: datetime
    raw_payload: dict[str, Any]

    @property
    def account_id(self) -> str:
        return self.account_hash


@dataclass(frozen=True)
class BrokerQuoteSnapshot:
    account_hash: str
    symbol: str
    external_symbol: str
    bid_price: Decimal | None
    ask_price: Decimal | None
    last_price: Decimal | None
    mark_price: Decimal | None
    close_price: Decimal | None
    net_change: Decimal | None
    net_percent_change: Decimal | None
    delayed: bool | None
    quote_time: datetime | None
    fetched_at: datetime
    source: str
    raw_payload: dict[str, Any]

    @property
    def account_id(self) -> str:
        return self.account_hash


@dataclass(frozen=True)
class BrokerOrderRecord:
    broker_order_id: str
    account_hash: str
    client_order_id: str | None
    symbol: str
    description: str | None
    asset_class: str
    instruction: str
    quantity: Decimal
    filled_quantity: Decimal | None
    order_type: str
    duration: str | None
    session: str | None
    status: str
    entered_at: datetime | None
    closed_at: datetime | None
    updated_at: datetime
    limit_price: Decimal | None
    stop_price: Decimal | None
    source: str
    raw_payload: dict[str, Any]

    @property
    def account_id(self) -> str:
        return self.account_hash


@dataclass(frozen=True)
class BrokerOrderEvent:
    account_hash: str
    broker_order_id: str | None
    client_order_id: str | None
    event_type: str
    status: str | None
    occurred_at: datetime
    message: str | None
    request_payload: dict[str, Any] | None
    response_payload: dict[str, Any] | None
    source: str

    @property
    def account_id(self) -> str:
        return self.account_hash


@dataclass(frozen=True)
class BrokerReconciliationRecord:
    account_hash: str | None
    classification: str
    status: str
    detail: str
    mismatch_count: int
    created_at: datetime
    payload: dict[str, Any]

    @property
    def account_id(self) -> str | None:
        return self.account_hash


@dataclass(frozen=True)
class ManualOrderRequest:
    account_hash: str
    symbol: str
    asset_class: str
    structure_type: str
    intent_type: str | None
    side: str
    quantity: Decimal
    order_type: str
    limit_price: Decimal | None
    stop_price: Decimal | None
    trail_value_type: str | None
    trail_value: Decimal | None
    trail_trigger_basis: str | None
    trail_limit_offset: Decimal | None
    time_in_force: str
    session: str
    review_confirmed: bool
    operator_note: str | None = None
    client_order_id: str | None = None
    broker_account_number: str | None = None
    oco_group_id: str | None = None
    oco_legs: tuple["ManualOcoLegRequest", ...] = ()
    operator_authenticated: bool = False
    operator_reduce_only_authorized: bool = False
    operator_auth_policy: str | None = None
    operator_auth_risk_bucket: str | None = None
    local_operator_identity: str | None = None
    auth_session_id: str | None = None
    auth_method: str | None = None
    authenticated_at: str | None = None

    @property
    def account_id(self) -> str:
        return self.account_hash


@dataclass(frozen=True)
class ManualFlattenRequest:
    account_hash: str
    symbol: str
    asset_class: str
    quantity: Decimal
    side: str
    time_in_force: str = "DAY"
    session: str = "NORMAL"
    operator_authenticated: bool = False
    operator_reduce_only_authorized: bool = False
    operator_auth_policy: str | None = None
    operator_auth_risk_bucket: str | None = None
    local_operator_identity: str | None = None
    auth_session_id: str | None = None
    auth_method: str | None = None
    authenticated_at: str | None = None

    @property
    def account_id(self) -> str:
        return self.account_hash


@dataclass(frozen=True)
class ManualOcoLegRequest:
    leg_label: str
    side: str
    quantity: Decimal
    order_type: str
    limit_price: Decimal | None
    stop_price: Decimal | None
    trail_value_type: str | None
    trail_value: Decimal | None
    trail_trigger_basis: str | None
    trail_limit_offset: Decimal | None


ProductionLinkConfig = SchwabProductionLinkConfig
