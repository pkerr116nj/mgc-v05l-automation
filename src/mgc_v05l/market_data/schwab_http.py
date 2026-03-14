"""HTTP transport and confirmed Schwab market-data clients."""

from __future__ import annotations

import json
from typing import Any, Optional, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .schwab_auth import SchwabOAuthClient
from .schwab_models import (
    HttpRequest,
    JsonHttpTransport,
    SchwabHistoricalClient,
    SchwabHistoricalRequest,
    SchwabMarketDataConfig,
    SchwabPriceHistoryFrequency,
    SchwabQuoteClient,
)


class SchwabHttpError(RuntimeError):
    """Raised when the Schwab HTTP layer fails."""


class UrllibJsonTransport(JsonHttpTransport):
    """Small stdlib transport so tests can stay network-free by injecting fakes."""

    def __init__(self, timeout_seconds: int = 30) -> None:
        self._timeout_seconds = timeout_seconds

    def request_json(self, request: HttpRequest) -> dict[str, Any]:
        body: Optional[bytes] = None
        url = request.url
        if request.query:
            url = f"{url}?{urlencode({key: _encode_http_value(value) for key, value in request.query.items()})}"
        if request.form:
            body = urlencode({key: _encode_http_value(value) for key, value in request.form.items()}).encode("utf-8")

        try:
            with urlopen(
                Request(url=url, method=request.method, headers=request.headers, data=body),
                timeout=self._timeout_seconds,
            ) as response:
                payload = response.read().decode("utf-8")
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise SchwabHttpError(f"Schwab HTTP error {exc.code}: {detail}") from exc
        except URLError as exc:
            raise SchwabHttpError(f"Schwab transport error: {exc}") from exc

        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SchwabHttpError(f"Expected JSON response from Schwab, received: {payload[:200]!r}") from exc
        if not isinstance(raw, dict):
            raise SchwabHttpError("Expected top-level JSON object from Schwab.")
        return raw


class SchwabHistoricalHttpClient(SchwabHistoricalClient):
    """Confirmed GET /pricehistory client using stored OAuth tokens."""

    def __init__(
        self,
        oauth_client: SchwabOAuthClient,
        market_data_config: SchwabMarketDataConfig,
        transport: JsonHttpTransport,
    ) -> None:
        self._oauth_client = oauth_client
        self._market_data_config = market_data_config
        self._transport = transport

    def fetch_price_history(
        self,
        external_symbol: str,
        request: SchwabHistoricalRequest,
        default_frequency: Optional[SchwabPriceHistoryFrequency],
    ) -> dict[str, Any]:
        query: dict[str, Any] = {
            "symbol": external_symbol,
            "periodType": request.period_type,
            "needExtendedHoursData": request.need_extended_hours_data,
            "needPreviousClose": request.need_previous_close,
        }
        if request.period is not None:
            query["period"] = request.period

        frequency_type = request.frequency_type
        frequency = request.frequency
        if frequency_type is None or frequency is None:
            if default_frequency is None:
                raise ValueError(
                    "frequencyType/frequency must be provided in the request or via explicit timeframe mapping."
                )
            frequency_type = default_frequency.frequency_type
            frequency = default_frequency.frequency

        query["frequencyType"] = frequency_type
        query["frequency"] = frequency

        if request.start_date_ms is not None:
            query["startDate"] = request.start_date_ms
        if request.end_date_ms is not None:
            query["endDate"] = request.end_date_ms

        return self._transport.request_json(
            HttpRequest(
                method="GET",
                url=f"{self._market_data_config.market_data_base_url.rstrip('/')}/pricehistory",
                headers=self._auth_headers(),
                query=query,
            )
        )

    def _auth_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._oauth_client.get_access_token()}",
        }


class SchwabQuoteHttpClient(SchwabQuoteClient):
    """Confirmed GET /quotes client kept separate from strategy decisions."""

    def __init__(
        self,
        oauth_client: SchwabOAuthClient,
        market_data_config: SchwabMarketDataConfig,
        transport: JsonHttpTransport,
    ) -> None:
        self._oauth_client = oauth_client
        self._market_data_config = market_data_config
        self._transport = transport

    def fetch_quotes(self, external_symbols: Sequence[str]) -> dict[str, Any]:
        query = {
            self._market_data_config.quotes_symbol_query_param: ",".join(external_symbols),
        }
        return self._transport.request_json(
            HttpRequest(
                method="GET",
                url=f"{self._market_data_config.market_data_base_url.rstrip('/')}/quotes",
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self._oauth_client.get_access_token()}",
                },
                query=query,
            )
        )


def _encode_http_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
