"""Live ingestion scaffolding for Schwab market data."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Optional

from ..domain.models import Bar
from ..persistence.repositories import RepositorySet
from .schwab_adapter import SchwabMarketDataAdapter
from .schwab_models import SchwabLivePollRequest, SchwabLivePollingClient, SchwabLiveStreamClient


class LivePollingService:
    """Polls live bar data and normalizes it into the shared internal bar model."""

    def __init__(
        self,
        adapter: SchwabMarketDataAdapter,
        client: Optional[SchwabLivePollingClient] = None,
        repositories: Optional[RepositorySet] = None,
    ) -> None:
        self._adapter = adapter
        self._client = client
        self._repositories = repositories

    def poll_bars(
        self,
        request: SchwabLivePollRequest,
        internal_timeframe: str,
        default_is_final: bool = False,
    ) -> list[Bar]:
        if self._client is None:
            raise NotImplementedError(
                "Schwab live polling integration is pending official API confirmation. "
                "Fill in the SchwabLivePollingClient once docs are confirmed."
            )

        external_symbol = self._adapter.map_symbol(request.internal_symbol)
        external_timeframe = self._adapter.map_timeframe(internal_timeframe)
        raw_records = self._client.poll_live_bars(external_symbol, external_timeframe, request)
        bars = self._adapter.normalize_live_records(
            raw_records,
            request.internal_symbol,
            internal_timeframe,
            default_is_final=default_is_final,
        )
        self._persist_bars(bars)
        return bars

    def _persist_bars(self, bars: list[Bar]) -> None:
        if self._repositories is None:
            return
        for bar in bars:
            self._repositories.bars.save(bar)


class LiveStreamService:
    """Placeholder live-stream service targeting the same internal Bar model."""

    def __init__(
        self,
        adapter: SchwabMarketDataAdapter,
        client: Optional[SchwabLiveStreamClient] = None,
    ) -> None:
        self._adapter = adapter
        self._client = client

    def subscribe_bars(self, internal_symbol: str, internal_timeframe: str) -> Iterable[Bar]:
        if self._client is None:
            raise NotImplementedError(
                "Schwab live streaming integration is pending official API confirmation. "
                "Fill in the SchwabLiveStreamClient once docs are confirmed."
            )

        external_symbol = self._adapter.map_symbol(internal_symbol)
        external_timeframe = self._adapter.map_timeframe(internal_timeframe)
        raw_records = self._client.subscribe_live_bars(external_symbol, external_timeframe)
        return self._adapter.normalize_live_records(raw_records, internal_symbol, internal_timeframe, default_is_final=False)
