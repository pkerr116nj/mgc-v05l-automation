"""Execution-provider wrapper around the existing Schwab production link."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..market_data import SchwabOAuthClient, SchwabTokenStore, UrllibJsonTransport, load_schwab_market_data_config
from ..production_link import SchwabProductionLinkService
from ..production_link.client import SchwabBrokerHttpClient
from .provider_interfaces import ExecutionProvider


class SchwabExecutionProvider(ExecutionProvider):
    """Schwab execution/account-truth provider kept separate from market-data providers."""

    provider_id = "schwab_execution"

    def __init__(
        self,
        repo_root: Path,
        *,
        production_link_service: SchwabProductionLinkService | None = None,
        broker_client: SchwabBrokerHttpClient | None = None,
    ) -> None:
        self._repo_root = Path(repo_root).resolve(strict=False)
        self._production_link_service = production_link_service or SchwabProductionLinkService(self._repo_root)
        schwab_config = load_schwab_market_data_config(self._production_link_service.config.market_data_config_path)
        oauth_client = SchwabOAuthClient(
            config=schwab_config.auth,
            transport=UrllibJsonTransport(timeout_seconds=self._production_link_service.config.request_timeout_seconds),
            token_store=SchwabTokenStore(schwab_config.auth.token_store_path),
        )
        self._client = broker_client or SchwabBrokerHttpClient(
            oauth_client=oauth_client,
            base_url=self._production_link_service.config.trader_api_base_url,
            timeout_seconds=self._production_link_service.config.request_timeout_seconds,
        )

    def snapshot_state(self, *, force_refresh: bool = False) -> dict[str, Any]:
        payload = self._production_link_service.snapshot(force_refresh=force_refresh)
        return dict(payload or {})

    def selected_account_hash(self, snapshot: dict[str, Any] | None = None) -> str | None:
        payload = snapshot or {}
        connection = dict(payload.get("connection") or {})
        accounts = dict(payload.get("accounts") or {})
        account_hash = str(
            connection.get("selected_account_hash")
            or accounts.get("selected_account_hash")
            or ""
        ).strip()
        return account_hash or None

    def submit_order(self, account_hash: str, order_payload: dict[str, Any]) -> dict[str, Any]:
        return self._client.submit_order(account_hash, order_payload)

    def cancel_order(self, account_hash: str, broker_order_id: str) -> None:
        self._client.cancel_order(account_hash, broker_order_id)

    def get_order_status(self, account_hash: str, broker_order_id: str) -> dict[str, Any]:
        payload = self._client.get_order_status(account_hash, broker_order_id)
        return dict(payload or {})
