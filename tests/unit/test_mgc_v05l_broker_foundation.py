from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from mgc_v05l.config_models import (
    BrokerProvider,
    ExecutionPricingPolicy,
    MarketDataProvider,
    RuntimeMode,
    load_settings_from_files,
)
from mgc_v05l.domain.enums import OrderIntentType
from mgc_v05l.execution.broker_requests import BrokerContractRequest, BrokerOrderRequest
from mgc_v05l.execution.live_strategy_broker import LiveStrategyPilotBroker
from mgc_v05l.execution.order_models import OrderIntent
from mgc_v05l.market_data.provider_models import QuoteSnapshot
from mgc_v05l.production_link.models import BrokerAccountIdentity
from mgc_v05l.production_link.store import ProductionLinkStore


def test_strategy_settings_expose_provider_aware_defaults(tmp_path: Path) -> None:
    override_path = tmp_path / "provider_defaults.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "foundation.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "artifacts"}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    settings = load_settings_from_files([Path("config/base.yaml"), override_path])

    assert settings.broker_provider == BrokerProvider.SCHWAB
    assert settings.market_data_provider == MarketDataProvider.DATABENTO
    assert settings.execution_pricing_policy == ExecutionPricingPolicy.MARKET_DATA
    assert settings.broker_quote_fallback_enabled is False


def test_strategy_settings_accept_provider_overrides(tmp_path: Path) -> None:
    override_path = tmp_path / "provider_overrides.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "provider.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "artifacts"}"',
                "mode: live",
                "broker_provider: ibkr",
                "market_data_provider: databento",
                "execution_pricing_policy: broker_quote_fallback",
                "broker_quote_fallback_enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    settings = load_settings_from_files([Path("config/base.yaml"), override_path])

    assert settings.mode == RuntimeMode.LIVE
    assert settings.broker_provider == BrokerProvider.IBKR
    assert settings.market_data_provider == MarketDataProvider.DATABENTO
    assert settings.execution_pricing_policy == ExecutionPricingPolicy.BROKER_QUOTE_FALLBACK
    assert settings.broker_quote_fallback_enabled is True


def test_live_strategy_broker_uses_execution_provider_request_builder(tmp_path: Path) -> None:
    override_path = tmp_path / "live_broker.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "live.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "artifacts"}"',
                "mode: live",
                "symbol: MGC",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    settings = load_settings_from_files([Path("config/base.yaml"), override_path])

    class StubMarketDataProvider:
        def fetch_quotes(self, symbols: tuple[str, ...]):
            assert symbols == ("MGC",)
            return [
                QuoteSnapshot(
                    internal_symbol="MGC",
                    external_symbol="/MGC",
                    ask_price=Decimal("2450.1"),
                    bid_price=Decimal("2449.9"),
                    last_price=Decimal("2450.0"),
                    provider="databento",
                )
            ]

    class StubExecutionProvider:
        provider_id = "stub_execution"

        def __init__(self) -> None:
            self.built_order_request: BrokerOrderRequest | None = None
            self.submitted_account_hash: str | None = None

        def snapshot_state(self, *, force_refresh: bool = False) -> dict:
            return {
                "connection": {"selected_account_id": "hash-123", "selected_account_hash": "hash-123"},
                "accounts": {"selected_account_id": "hash-123", "selected_account_hash": "hash-123"},
            }

        def selected_account_id(self, snapshot: dict | None = None) -> str | None:
            return "hash-123"

        def selected_account_hash(self, snapshot: dict | None = None) -> str | None:
            return "hash-123"

        def build_order_request(self, *, order_intent: OrderIntent, quote_snapshot=None) -> BrokerOrderRequest:
            self.built_order_request = BrokerOrderRequest(
                account_id=None,
                contract=BrokerContractRequest(asset_class="FUTURE", symbol=order_intent.symbol, broker_symbol="/MGC"),
                side="BUY",
                quantity=Decimal(order_intent.quantity),
                order_type="LIMIT",
                time_in_force="DAY",
                session="NORMAL",
                intent_type=order_intent.intent_type.value,
                limit_price=Decimal("2450.1"),
                pricing_source="market_data:databento",
                metadata={"quote_symbol": getattr(quote_snapshot, "external_symbol", None)},
            )
            return self.built_order_request

        def submit_order(self, account_hash: str, order_request: BrokerOrderRequest) -> dict:
            self.submitted_account_hash = account_hash
            assert order_request == self.built_order_request
            return {"broker_order_id": "broker-1", "status_code": 201}

        def cancel_order(self, account_hash: str, broker_order_id: str) -> None:  # pragma: no cover - unused in test
            raise NotImplementedError

        def get_order_status(self, account_hash: str, broker_order_id: str) -> dict:  # pragma: no cover - unused in test
            raise NotImplementedError

    provider = StubExecutionProvider()
    broker = LiveStrategyPilotBroker(
        settings=settings,
        repo_root=Path.cwd(),
        market_data_provider=StubMarketDataProvider(),
        execution_provider=provider,
    )
    broker.connect()

    broker_order_id = broker.submit_order(
        OrderIntent(
            order_intent_id="intent-1",
            bar_id="bar-1",
            symbol="MGC",
            intent_type=OrderIntentType.BUY_TO_OPEN,
            quantity=1,
            created_at=datetime.now(timezone.utc),
            reason_code="test_reason",
        )
    )

    assert broker_order_id == "broker-1"
    assert provider.submitted_account_hash == "hash-123"
    assert provider.built_order_request is not None
    assert provider.built_order_request.pricing_source == "market_data:databento"
    assert broker.last_submit_context()["request_payload"]["contract"]["broker_symbol"] == "/MGC"


def test_production_link_store_surfaces_provider_context(tmp_path: Path) -> None:
    store = ProductionLinkStore(tmp_path / "production_link.sqlite3")
    store.save_provider_context(broker_provider_id="schwab", market_data_provider_id="databento")

    snapshot = store.build_snapshot()

    assert snapshot["provider_context"]["broker_provider_id"] == "schwab"
    assert snapshot["provider_context"]["market_data_provider_id"] == "databento"


def test_production_link_store_surfaces_account_id_aliases(tmp_path: Path) -> None:
    store = ProductionLinkStore(tmp_path / "production_link.sqlite3")
    store.save_accounts(
        [
            BrokerAccountIdentity(
                broker_name="Schwab",
                account_hash="hash-123",
                account_number="12345678",
                display_name="MARGIN 12345678",
                account_type="MARGIN",
                selected=True,
                source="schwab_live",
                updated_at=datetime.now(timezone.utc),
                raw_payload={},
            )
        ],
        selected_account_hash="hash-123",
    )

    snapshot = store.build_snapshot()

    assert snapshot["accounts"]["selected_account_id"] == "hash-123"
    assert snapshot["accounts"]["rows"][0]["account_id"] == "hash-123"
