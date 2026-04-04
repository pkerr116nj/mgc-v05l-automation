"""Historical backfill service for confirmed Schwab /pricehistory access."""

from __future__ import annotations

from typing import Optional

from ..domain.models import Bar
from ..persistence.repositories import RepositorySet
from .canonical_maintenance import CanonicalMarketDataMaintenanceService
from .schwab_adapter import SchwabMarketDataAdapter
from .schwab_models import SchwabHistoricalClient, SchwabHistoricalRequest


class HistoricalBackfillService:
    """Fetches historical bars and normalizes them into the shared internal bar model."""

    def __init__(
        self,
        adapter: SchwabMarketDataAdapter,
        client: Optional[SchwabHistoricalClient] = None,
        repositories: Optional[RepositorySet] = None,
        canonical_maintenance: CanonicalMarketDataMaintenanceService | None = None,
    ) -> None:
        self._adapter = adapter
        self._client = client
        self._repositories = repositories
        self._canonical_maintenance = canonical_maintenance

    def fetch_bars(self, request: SchwabHistoricalRequest, internal_timeframe: str) -> list[Bar]:
        if self._client is None:
            raise NotImplementedError("No Schwab historical client is configured.")

        external_symbol = self._adapter.map_historical_symbol(request.internal_symbol)
        frequency = self._adapter.resolve_history_frequency(request, internal_timeframe)
        payload = self._client.fetch_price_history(external_symbol, request, frequency)
        bars = self._adapter.normalize_pricehistory_response(payload, request.internal_symbol, internal_timeframe)
        self._persist_bars(bars, internal_timeframe=internal_timeframe)
        return bars

    def _persist_bars(self, bars: list[Bar], *, internal_timeframe: str) -> None:
        if self._repositories is None:
            return
        for bar in bars:
            self._repositories.bars.save(bar, data_source="schwab_history")
        if self._canonical_maintenance is not None and str(internal_timeframe).lower() == "1m":
            self._canonical_maintenance.persist_completed_1m_bars(
                bars=bars,
                raw_data_source="schwab_history",
                provider="schwab_market_data",
                provenance_tag="schwab_market_data_historical",
                dataset="schwab_pricehistory",
                schema_name="ohlcv-1m",
                provider_metadata={"ingest_mode": "historical_backfill"},
            )
