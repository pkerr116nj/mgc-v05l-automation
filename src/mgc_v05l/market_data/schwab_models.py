"""Schwab market-data configuration and request models.

Exact endpoint names and payload contracts are intentionally deferred until official
Schwab API documentation is confirmed for this integration stage.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Protocol, Sequence


class TimestampSemantics(str, Enum):
    START = "start"
    END = "end"


@dataclass(frozen=True)
class SchwabAuthConfig:
    """Placeholder auth/config contract pending official Schwab API confirmation."""

    client_id: str
    client_secret: Optional[str] = None
    refresh_token: Optional[str] = None
    access_token: Optional[str] = None
    api_base_url: Optional[str] = None
    note: str = "Placeholder only. Confirm exact Schwab auth fields against official docs before use."


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
class SchwabMarketDataConfig:
    """Placeholder config for symbol/timeframe mapping and payload normalization."""

    auth: SchwabAuthConfig
    symbol_map: dict[str, str]
    timeframe_map: dict[str, str]
    field_map: SchwabBarFieldMap


@dataclass(frozen=True)
class SchwabHistoricalRequest:
    internal_symbol: str
    start_at: datetime
    end_at: datetime
    max_bars: Optional[int] = None


@dataclass(frozen=True)
class SchwabLivePollRequest:
    internal_symbol: str
    since: Optional[datetime] = None


class SchwabHistoricalClient(Protocol):
    """Placeholder raw historical client contract pending exact endpoint confirmation."""

    def fetch_historical_bars(
        self,
        external_symbol: str,
        external_timeframe: str,
        request: SchwabHistoricalRequest,
    ) -> Sequence[dict]:
        """Return raw historical bar payload records."""


class SchwabLivePollingClient(Protocol):
    """Placeholder raw live-poll client contract pending exact endpoint confirmation."""

    def poll_live_bars(
        self,
        external_symbol: str,
        external_timeframe: str,
        request: SchwabLivePollRequest,
    ) -> Sequence[dict]:
        """Return raw live bar payload records."""


class SchwabLiveStreamClient(Protocol):
    """Placeholder raw live-stream contract pending exact endpoint confirmation."""

    def subscribe_live_bars(
        self,
        external_symbol: str,
        external_timeframe: str,
    ) -> Sequence[dict]:
        """Return raw streamed bar payload records."""
