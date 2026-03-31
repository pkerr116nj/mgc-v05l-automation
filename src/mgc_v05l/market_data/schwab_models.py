"""Typed Schwab auth, mapping, and market-data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Protocol, Sequence
from urllib.parse import urlencode


class TimestampSemantics(str, Enum):
    START = "start"
    END = "end"


@dataclass(frozen=True)
class SchwabAuthConfig:
    """OAuth configuration for Schwab auth and market-data access."""

    app_key: str
    app_secret: str
    callback_url: str
    api_host: str = "https://api.schwabapi.com"
    authorize_path: str = "/v1/oauth/authorize"
    token_path: str = "/v1/oauth/token"
    token_store_path: Path = Path(".local/schwab/tokens.json")

    @property
    def authorize_url(self) -> str:
        return f"{self.api_host.rstrip('/')}{self.authorize_path}"

    @property
    def token_url(self) -> str:
        return f"{self.api_host.rstrip('/')}{self.token_path}"


@dataclass(frozen=True)
class SchwabTokenSet:
    """Parsed token response plus local issue timestamp."""

    access_token: str
    refresh_token: Optional[str]
    token_type: str
    expires_in: Optional[int]
    scope: Optional[str]
    issued_at: datetime

    @classmethod
    def from_token_response(
        cls,
        payload: dict[str, Any],
        issued_at: Optional[datetime] = None,
    ) -> "SchwabTokenSet":
        return cls(
            access_token=str(payload["access_token"]),
            refresh_token=(
                str(payload["refresh_token"])
                if payload.get("refresh_token") not in (None, "")
                else None
            ),
            token_type=str(payload.get("token_type", "Bearer")),
            expires_in=int(payload["expires_in"]) if payload.get("expires_in") is not None else None,
            scope=str(payload["scope"]) if payload.get("scope") is not None else None,
            issued_at=issued_at or datetime.now(timezone.utc),
        )

    @property
    def expires_at(self) -> Optional[datetime]:
        if self.expires_in is None:
            return None
        return self.issued_at + timedelta(seconds=self.expires_in)

    def is_expired(self, now: Optional[datetime] = None, skew_seconds: int = 30) -> bool:
        if self.expires_in is None:
            return False
        current = now or datetime.now(timezone.utc)
        assert self.expires_at is not None
        return current >= (self.expires_at - timedelta(seconds=skew_seconds))

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_type": self.token_type,
            "expires_in": self.expires_in,
            "scope": self.scope,
            "issued_at": self.issued_at.astimezone(timezone.utc).isoformat(),
        }

    @classmethod
    def from_json_dict(cls, payload: dict[str, Any]) -> "SchwabTokenSet":
        return cls(
            access_token=str(payload["access_token"]),
            refresh_token=(
                str(payload["refresh_token"])
                if payload.get("refresh_token") not in (None, "")
                else None
            ),
            token_type=str(payload.get("token_type", "Bearer")),
            expires_in=int(payload["expires_in"]) if payload.get("expires_in") is not None else None,
            scope=str(payload["scope"]) if payload.get("scope") is not None else None,
            issued_at=datetime.fromisoformat(str(payload["issued_at"])),
        )


@dataclass(frozen=True)
class SchwabBarFieldMap:
    """Field map used to normalize confirmed Schwab payloads into internal bars."""

    timestamp_field: str
    open_field: str
    high_field: str
    low_field: str
    close_field: str
    volume_field: str
    is_final_field: Optional[str] = None
    timestamp_semantics: TimestampSemantics = TimestampSemantics.END


@dataclass(frozen=True)
class SchwabPriceHistoryFrequency:
    """Explicit Schwab pricehistory frequency mapping."""

    frequency_type: str
    frequency: int


@dataclass(frozen=True)
class SchwabMarketDataConfig:
    """Config for auth, mapping, and response normalization."""

    auth: SchwabAuthConfig
    historical_symbol_map: dict[str, str]
    quote_symbol_map: dict[str, str]
    timeframe_map: dict[str, SchwabPriceHistoryFrequency]
    field_map: SchwabBarFieldMap
    market_context_quote_symbols: dict[str, str] = field(default_factory=dict)
    treasury_context_quote_symbols: dict[str, str] = field(default_factory=dict)
    market_data_base_url: str = "https://api.schwabapi.com/marketdata/v1"
    quotes_symbol_query_param: str = "symbols"


@dataclass(frozen=True)
class SchwabHistoricalRequest:
    internal_symbol: str
    period_type: str
    period: Optional[int] = None
    frequency_type: Optional[str] = None
    frequency: Optional[int] = None
    start_date_ms: Optional[int] = None
    end_date_ms: Optional[int] = None
    need_extended_hours_data: bool = False
    need_previous_close: bool = False


@dataclass(frozen=True)
class SchwabLivePollRequest:
    internal_symbol: str
    since: Optional[datetime] = None


@dataclass(frozen=True)
class SchwabQuoteRequest:
    internal_symbols: tuple[str, ...]


@dataclass(frozen=True)
class SchwabQuoteResult:
    """Normalized quote result kept separate from strategy decisions."""

    internal_symbol: str
    external_symbol: str
    quote_future: Optional[dict[str, Any]]
    reference_future: Optional[dict[str, Any]]
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class HttpRequest:
    method: str
    url: str
    headers: dict[str, str]
    query: Optional[dict[str, Any]] = None
    form: Optional[dict[str, Any]] = None

    def rendered_url(self) -> str:
        if not self.query:
            return self.url
        return f"{self.url}?{urlencode({key: _encode_http_value(value) for key, value in self.query.items()})}"


class JsonHttpTransport(Protocol):
    def request_json(self, request: HttpRequest) -> dict[str, Any]:
        """Execute an HTTP request and return a parsed JSON payload."""


class SchwabHistoricalClient(Protocol):
    def fetch_price_history(
        self,
        external_symbol: str,
        request: SchwabHistoricalRequest,
        default_frequency: Optional[SchwabPriceHistoryFrequency],
    ) -> dict[str, Any]:
        """Return the raw Schwab /pricehistory payload."""


class SchwabQuoteClient(Protocol):
    def fetch_quotes(self, external_symbols: Sequence[str]) -> dict[str, Any]:
        """Return the raw Schwab /quotes payload."""


class SchwabLivePollingClient(Protocol):
    def poll_live_bars(
        self,
        external_symbol: str,
        external_timeframe: str,
        request: SchwabLivePollRequest,
    ) -> Sequence[dict]:
        """Return raw live bar payload records."""


class SchwabLiveStreamClient(Protocol):
    def subscribe_live_bars(
        self,
        external_symbol: str,
        external_timeframe: str,
    ) -> Sequence[dict]:
        """Return raw streamed bar payload records."""


def _encode_http_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
