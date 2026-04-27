"""TradeStation client skeleton with explicit SIM/LIVE separation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import json

from .tradestation_auth import TradeStationEnvironment, TradeStationTokenStore

TRADESTATION_SIM_BASE_URL = "https://sim-api.tradestation.com/v3"
TRADESTATION_LIVE_BASE_URL = "https://api.tradestation.com/v3"


class TradeStationClientConfigurationError(RuntimeError):
    """Raised when the client lacks the required local configuration."""


class TradeStationHttpRequestError(RuntimeError):
    """Raised when a TradeStation HTTP request fails."""


@dataclass(frozen=True)
class TradeStationHttpRequest:
    method: str
    url: str
    headers: dict[str, str]
    params: dict[str, Any] | None = None
    json_body: dict[str, Any] | None = None


class TradeStationJsonTransport(Protocol):
    def request_json(self, request: TradeStationHttpRequest) -> Any:
        """Perform a JSON request and return the decoded payload."""


class UrllibTradeStationJsonTransport:
    """Small urllib-backed JSON transport for read-only account truth."""

    def __init__(self, *, timeout_seconds: int = 30) -> None:
        self._timeout_seconds = int(timeout_seconds)

    def request_json(self, request: TradeStationHttpRequest) -> Any:
        url = request.url
        if request.params:
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}{urlencode(request.params)}"
        data = None
        if request.json_body is not None:
            data = json.dumps(request.json_body).encode("utf-8")
        raw_request = Request(url=url, data=data, method=request.method.upper())
        for key, value in request.headers.items():
            raw_request.add_header(key, value)
        try:
            with urlopen(raw_request, timeout=self._timeout_seconds) as response:
                payload = response.read().decode("utf-8")
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise TradeStationHttpRequestError(
                f"TradeStation HTTP {exc.code} for {request.method} {request.url}: {detail}"
            ) from exc
        except URLError as exc:
            raise TradeStationHttpRequestError(
                f"TradeStation transport error for {request.method} {request.url}: {exc.reason}"
            ) from exc
        try:
            return json.loads(payload)
        except json.JSONDecodeError as exc:
            raise TradeStationHttpRequestError(
                f"TradeStation response for {request.method} {request.url} is not valid JSON."
            ) from exc


class TradeStationClient:
    """Thin HTTP skeleton for account-truth calls.

    Network-backed usage is intentionally deferred; Stage 1 relies on injected fake
    clients in tests rather than real API calls.
    """

    def __init__(
        self,
        *,
        environment: TradeStationEnvironment,
        token_store: TradeStationTokenStore | None = None,
        transport: TradeStationJsonTransport | None = None,
        request_timeout_seconds: int = 30,
        base_url: str | None = None,
    ) -> None:
        self._environment = environment
        self._token_store = token_store
        self._transport = transport
        self._request_timeout_seconds = int(request_timeout_seconds)
        self._base_url = base_url or (
            TRADESTATION_SIM_BASE_URL if environment == TradeStationEnvironment.SIM else TRADESTATION_LIVE_BASE_URL
        )

    @property
    def environment(self) -> TradeStationEnvironment:
        return self._environment

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def request_timeout_seconds(self) -> int:
        return self._request_timeout_seconds

    def list_accounts(self) -> Any:
        return self._request_json("GET", "/brokerage/accounts")

    def get_balances(self, account_id: str) -> Any:
        return self._request_json("GET", f"/brokerage/accounts/{account_id}/balances")

    def get_positions(self, account_id: str) -> Any:
        return self._request_json("GET", f"/brokerage/accounts/{account_id}/positions")

    def get_open_orders(self, account_id: str) -> Any:
        return self._request_json("GET", f"/brokerage/accounts/{account_id}/orders")

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        if self._transport is None:
            raise TradeStationClientConfigurationError(
                "TradeStationClient transport is not configured; Stage 1 does not connect to the live API."
            )
        request = TradeStationHttpRequest(
            method=method,
            url=f"{self._base_url}{path}",
            headers=self._build_headers(),
            params=params,
            json_body=json_body,
        )
        return self._transport.request_json(request)

    def _build_headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-TradeStation-Environment": self._environment.value,
        }
        if self._token_store is not None:
            token_set = self._token_store.load()
            if token_set is not None and token_set.access_token:
                headers["Authorization"] = f"Bearer {token_set.access_token}"
        return headers


def default_token_store_path(root: Path, environment: TradeStationEnvironment) -> Path:
    root = Path(root)
    filename = "sim_tokens.json" if environment == TradeStationEnvironment.SIM else "live_tokens.json"
    return root / "var" / "tradestation" / filename
