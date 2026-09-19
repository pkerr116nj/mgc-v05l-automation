"""Read-only Schwab adapter used by the NDXP terminal poller."""

from __future__ import annotations

import json
import os
import time
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

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
CHAIN_EXPIRATION_LOOKAHEAD_DAYS = 10
GAMMA_CHAIN_LOOKAHEAD_DAYS = 45
GAMMA_CHAIN_STRIKE_COUNT = 160
GAMMA_CHAIN_REFRESH_SECONDS = 60.0
WORKING_ORDERS_LOOKBACK_DAYS = 60
RECENT_ORDERS_LOOKBACK_DAYS = 1
ACCESS_CHECK_QUOTE_SYMBOL = "AAPL"
EASTERN = ZoneInfo("America/New_York")


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
        self._chain_expiration: dict[str, Any] = {}
        self._selected_expiration: str | None = None
        self._gamma_chain_cache: dict[str, dict[str, Any]] = {}
        self._gamma_chain_refresh: dict[str, float] = {}
        self.broker = SchwabBrokerHttpClient(
            oauth_client=self.oauth,
            base_url="https://api.schwabapi.com/trader/v1",
            timeout_seconds=15,
        )

    def fetch_market(self, *, chain_symbol: str, quote_symbol: str) -> dict[str, Any]:
        started = time.monotonic()
        chain = self.fetch_chain(chain_symbol=chain_symbol)
        gamma_chain = self.fetch_gamma_chain(chain_symbol=chain_symbol)
        quote = self.fetch_quote(quote_symbol=quote_symbol)
        return {
            "chain": _merge_chain_payloads(gamma_chain, chain),
            "gamma_chain": gamma_chain,
            "quote": quote,
            "latency_ms": round((time.monotonic() - started) * 1000, 1),
            "received_at": datetime.now(timezone.utc).isoformat(),
        }

    def set_selected_expiration(self, expiration: str | None) -> None:
        self._selected_expiration = expiration or None

    def fetch_chain(self, *, chain_symbol: str) -> dict[str, Any]:
        access_token = self.oauth.get_access_token()
        headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
        resolved_chain_symbol = _resolved_index_symbol(chain_symbol)
        today = datetime.now(EASTERN).date()
        cache = getattr(self, "_chain_expiration", {})
        if not isinstance(cache, dict):
            cache = {resolved_chain_symbol: cache} if cache else {}
        self._chain_expiration = cache
        selected = getattr(self, "_selected_expiration", None)
        cached_expiration = cache.get(resolved_chain_symbol)
        selected_date = date.fromisoformat(selected) if selected else None
        first_candidate = selected_date or (cached_expiration if cached_expiration and cached_expiration >= today else today)
        last_payload: dict[str, Any] = {}
        for offset in range(CHAIN_EXPIRATION_LOOKAHEAD_DAYS + 1):
            candidate = first_candidate + timedelta(days=offset)
            expiration = candidate.isoformat()
            payload = self.transport.request_json(
                HttpRequest(
                    method="GET",
                    url=f"{self.market_config.market_data_base_url.rstrip('/')}/chains",
                    headers=headers,
                    query={
                        "symbol": resolved_chain_symbol,
                        "contractType": "ALL",
                        "strikeCount": NDXP_CHAIN_STRIKE_COUNT,
                        "fromDate": expiration,
                        "toDate": expiration,
                        "includeUnderlyingQuote": True,
                        "strategy": "SINGLE",
                    },
                )
            )
            last_payload = payload if isinstance(payload, dict) else {}
            if _chain_has_contracts(last_payload):
                cache[resolved_chain_symbol] = candidate
                return last_payload
        cache.pop(resolved_chain_symbol, None)
        return last_payload

    def fetch_gamma_chain(self, *, chain_symbol: str) -> dict[str, Any]:
        resolved_chain_symbol = _resolved_index_symbol(chain_symbol)
        now = time.monotonic()
        cache = getattr(self, "_gamma_chain_cache", {})
        refresh = getattr(self, "_gamma_chain_refresh", {})
        if not isinstance(cache, dict):
            cache = {}
        if not isinstance(refresh, dict):
            refresh = {}
        self._gamma_chain_cache = cache
        self._gamma_chain_refresh = refresh
        cached = cache.get(resolved_chain_symbol)
        refreshed = refresh.get(resolved_chain_symbol, 0.0)
        if cached is not None and now - refreshed < GAMMA_CHAIN_REFRESH_SECONDS:
            return deepcopy(cached)
        access_token = self.oauth.get_access_token()
        headers = {"Accept": "application/json", "Authorization": f"Bearer {access_token}"}
        today = datetime.now(EASTERN).date()
        try:
            payload = self.transport.request_json(
                HttpRequest(
                    method="GET",
                    url=f"{self.market_config.market_data_base_url.rstrip('/')}/chains",
                    headers=headers,
                    query={
                        "symbol": resolved_chain_symbol,
                        "contractType": "ALL",
                        "strikeCount": GAMMA_CHAIN_STRIKE_COUNT,
                        "fromDate": today.isoformat(),
                        "toDate": (today + timedelta(days=GAMMA_CHAIN_LOOKAHEAD_DAYS)).isoformat(),
                        "includeUnderlyingQuote": False,
                        "strategy": "SINGLE",
                    },
                )
            )
            normalized = payload if isinstance(payload, dict) else {}
            if _chain_has_contracts(normalized):
                cache[resolved_chain_symbol] = deepcopy(normalized)
                refresh[resolved_chain_symbol] = now
                return normalized
        except Exception:
            if cached is not None:
                return deepcopy(cached)
            return {}
        return deepcopy(cached) if cached is not None else {}

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
        now = datetime.now(timezone.utc)
        working_orders = (
            self.broker.get_orders(
                selected_hash,
                from_entered_time=_schwab_zoned_datetime(
                    now - timedelta(days=WORKING_ORDERS_LOOKBACK_DAYS)
                ),
                to_entered_time=_schwab_zoned_datetime(now),
                status="WORKING",
                max_results=100,
            )
            if selected_hash
            else []
        )
        recent_orders = (
            self.broker.get_orders(
                selected_hash,
                from_entered_time=_schwab_zoned_datetime(
                    now - timedelta(days=RECENT_ORDERS_LOOKBACK_DAYS)
                ),
                to_entered_time=_schwab_zoned_datetime(now),
                max_results=100,
            )
            if selected_hash
            else []
        )
        return {
            "account_numbers": account_numbers,
            "accounts": accounts,
            "selected_account_hash": selected_hash,
            "working_orders": working_orders,
            "recent_orders": recent_orders,
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

    def access_check(self, *, chain_symbol: str = "$NDX") -> dict[str, Any]:
        broker_truth = self.fetch_broker_truth()
        market_started = time.monotonic()
        resolved_chain_symbol = _resolved_index_symbol(chain_symbol)
        chain = self.fetch_chain(chain_symbol=resolved_chain_symbol)
        quote = self.fetch_quote(quote_symbol=ACCESS_CHECK_QUOTE_SYMBOL)
        chain_access = bool(chain.get("callExpDateMap") or chain.get("putExpDateMap"))
        return {
            "ok": True,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "account_count": len(broker_truth.get("account_numbers") or []),
            "account_and_trading_access": bool(broker_truth.get("account_numbers")),
            "index_chain_symbol": resolved_chain_symbol,
            "index_chain_access": chain_access,
            "ndx_chain_access": chain_access if resolved_chain_symbol == "$NDX" else None,
            "quote_probe_symbol": ACCESS_CHECK_QUOTE_SYMBOL,
            "quote_access": bool(quote),
            "market_latency_ms": round((time.monotonic() - market_started) * 1000, 1),
            "broker_latency_ms": broker_truth.get("latency_ms"),
            "mutation_attempted": False,
        }


def _schwab_zoned_datetime(value: datetime) -> str:
    """Render the exact millisecond UTC form required by Schwab's orders API."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _chain_has_contracts(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    for key in ("callExpDateMap", "putExpDateMap"):
        expiration_map = payload.get(key)
        if not isinstance(expiration_map, dict):
            continue
        for strike_map in expiration_map.values():
            if not isinstance(strike_map, dict):
                continue
            if any(isinstance(contracts, list) and bool(contracts) for contracts in strike_map.values()):
                return True
    return False


def _resolved_index_symbol(value: str) -> str:
    normalized = str(value or "").strip().upper().lstrip("$")
    return f"${normalized}" if normalized in {"NDX", "SPX", "RUT"} else str(value)


def _merge_chain_payloads(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base) if isinstance(base, dict) else {}
    source = overlay if isinstance(overlay, dict) else {}
    for key, value in source.items():
        if key not in {"callExpDateMap", "putExpDateMap"}:
            merged[key] = deepcopy(value)
    for key in ("callExpDateMap", "putExpDateMap"):
        target_map = merged.setdefault(key, {})
        if not isinstance(target_map, dict):
            target_map = {}
            merged[key] = target_map
        source_map = source.get(key) if isinstance(source.get(key), dict) else {}
        for expiration_key, strikes in source_map.items():
            target_map[expiration_key] = deepcopy(strikes)
    return merged
