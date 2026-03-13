"""Historical backfill scaffolding for Schwab market data."""

from __future__ import annotations

from typing import Optional

from ..domain.models import Bar
from ..persistence.repositories import RepositorySet
from .schwab_adapter import SchwabMarketDataAdapter
from .schwab_models import SchwabHistoricalClient, SchwabHistoricalRequest


class HistoricalBackfillService:
    """Fetches historical bars and normalizes them into the shared internal bar model."""

    def __init__(
        self,
        adapter: SchwabMarketDataAdapter,
        client: Optional[SchwabHistoricalClient] = None,
        repositories: Optional[RepositorySet] = None,
    ) -> None:
        self._adapter = adapter
        self._client = client
        self._repositories = repositories

    def fetch_bars(self, request: SchwabHistoricalRequest, internal_timeframe: str) -> list[Bar]:
        if self._client is None:
            raise NotImplementedError(
                "Schwab historical endpoint integration is pending official API confirmation. "
                "Fill in the SchwabHistoricalClient once docs are confirmed."
            )

        external_symbol = self._adapter.map_symbol(request.internal_symbol)
        external_timeframe = self._adapter.map_timeframe(internal_timeframe)
        raw_records = self._client.fetch_historical_bars(external_symbol, external_timeframe, request)
        bars = self._adapter.normalize_historical_records(raw_records, request.internal_symbol, internal_timeframe)
        self._persist_bars(bars)
        return bars

    def _persist_bars(self, bars: list[Bar]) -> None:
        if self._repositories is None:
            return
        for bar in bars:
            self._repositories.bars.save(bar)
