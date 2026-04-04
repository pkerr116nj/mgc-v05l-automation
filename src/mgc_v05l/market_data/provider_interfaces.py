"""Provider interfaces separating market data from execution truth."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, Protocol

from .provider_models import HistoricalBarsRequest, HistoricalBarsResult, QuoteSnapshot


class MarketDataProvider(Protocol):
    provider_id: str

    def fetch_historical_bars(self, request: HistoricalBarsRequest) -> HistoricalBarsResult:
        """Return normalized historical bars plus provenance metadata."""

    def fetch_quotes(self, internal_symbols: Sequence[str]) -> list[QuoteSnapshot]:
        """Return normalized quote snapshots for one or more internal symbols."""

    def describe_symbol(self, internal_symbol: str) -> dict[str, Any]:
        """Return provider-side reference metadata for an internal symbol."""

    def subscribe_live_quotes(self, internal_symbols: Sequence[str]) -> Iterable[QuoteSnapshot]:
        """Return a live quote stream or raise when unsupported."""
