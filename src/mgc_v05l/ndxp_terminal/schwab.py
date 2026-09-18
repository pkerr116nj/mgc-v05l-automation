"""Read-only Schwab adapter used by the NDXP terminal poller."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..market_data import (
    SchwabOAuthClient,
    SchwabTokenStore,
    UrllibJsonTransport,
    load_schwab_auth_config_from_env,
    load_schwab_market_data_config,
)
from ..market_data.schwab_models import HttpRequest
from ..production_link.client import SchwabBrokerHttpClient


NDXP_CHAIN_STRIKE_COUNT = 120


class NdxpSchwabAdapter:
    mode = "SCHWAB_LIVE_READ_ONLY"

    def __init__(self, repo_root: Path) -> None:
        self._repo_root = repo_root
        config_path = repo_root / "config" / "schwab.local.json"
        market_config = load_schwab_market_data_config(config_path if config_path.exists() else None)
        auth_config = load_schwab_auth_config_from_env()
        transport = UrllibJsonTransport(timeout_seconds=15)
        self.oauth = SchwabOAuthClient(
            config=auth_config,
            transport=transport,
            token_store=SchwabTokenStore(auth_config.token_store_path),
        )
        self.market_config = market_config
        self.transport = transport
        self.broker = SchwabBrokerHttpClient(
            oauth_client=self.oauth,
            base_url="https://api.schwabapi.com/trader/v1",
            timeout_seconds=15,
        )

    def fetch_market(self, *, chain_symbol: str, quote_symbol: str) -> dict[str, Any]:
        started = time.monotonic()
        chain = self.fetch_chain(chain_symbol=chain_symbol)
        quote = self.fetch_quote(quote_symbol=quote_symbol)
        return {
            "chain": chain,
            "quote": quote,
            "latency_ms": round((time.monotonic() - started) * 1000, 1),
            "received_at": datetime.now(timezone.utc).isoformat(),
        }

    def fetch_chain(self, *, chain_symbol: str) -> dict[str, Any]:
        access_token = self.oauth.get_access_token()
        headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
        return self.transport.request_json(
            HttpRequest(
                method="GET",
                url=f"{self.market_config.market_data_base_url.rstrip('/')}/chains",
                headers=headers,
                query={
                    "symbol": chain_symbol,
                    "contractType": "ALL",
                    "strikeCount": NDXP_CHAIN_STRIKE_COUNT,
                    "includeUnderlyingQuote": True,
                    "strategy": "SINGLE",
                },
            )
        )

    def fetch_quote(self, *, quote_symbol: str) -> dict[str, Any]:
        access_token = self.oauth.get_access_token()
        headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
        return self.transport.request_json(
            HttpRequest(
                method="GET",
                url=f"{self.market_config.market_data_base_url.rstrip('/')}/quotes",
                headers=headers,
                query={"symbols": quote_symbol, "indicative": False},
            )
        )

    def fetch_broker_truth(self) -> dict[str, Any]:
        started = time.monotonic()
        account_numbers = self.broker.list_account_numbers()
        accounts = self.broker.list_accounts(fields=["positions"])
        selected_hash = self._selected_account_hash(account_numbers)
        working_orders = self.broker.get_orders(selected_hash, status="WORKING", max_results=100) if selected_hash else []
        return {
            "account_numbers": account_numbers,
            "accounts": accounts,
            "selected_account_hash": selected_hash,
            "working_orders": working_orders,
            "latency_ms": round((time.monotonic() - started) * 1000, 1),
            "received_at": datetime.now(timezone.utc).isoformat(),
        }

    def _selected_account_hash(self, account_numbers: list[dict[str, Any]]) -> str:
        available = {str(row.get("hashValue") or "") for row in account_numbers}
        configured = str(os.environ.get("MGC_NDXP_ACCOUNT_HASH") or "").strip()
        if configured:
            if configured not in available:
                raise ValueError("MGC_NDXP_ACCOUNT_HASH is not present in the current Schwab token's accounts.")
            return configured
        selected_path = self._repo_root / "outputs" / "production_link" / "selected_account.json"
        if selected_path.exists():
            try:
                payload = json.loads(selected_path.read_text(encoding="utf-8"))
                persisted = str(payload.get("account_hash") or payload.get("selected_account_hash") or "").strip()
                if persisted in available:
                    return persisted
            except (OSError, json.JSONDecodeError):
                pass
        return str(account_numbers[0].get("hashValue") or "") if account_numbers else ""

    def access_check(self) -> dict[str, Any]:
        broker_truth = self.fetch_broker_truth()
        market = self.fetch_market(chain_symbol="NDX", quote_symbol="$NDX")
        chain = market.get("chain") if isinstance(market.get("chain"), dict) else {}
        return {
            "ok": True,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "account_count": len(broker_truth.get("account_numbers") or []),
            "account_and_trading_access": bool(broker_truth.get("account_numbers")),
            "ndx_chain_access": bool(chain.get("callExpDateMap") or chain.get("putExpDateMap")),
            "market_latency_ms": market.get("latency_ms"),
            "broker_latency_ms": broker_truth.get("latency_ms"),
            "mutation_attempted": False,
        }
