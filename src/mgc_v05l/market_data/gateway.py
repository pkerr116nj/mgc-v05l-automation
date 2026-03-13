"""Shared market-data gateway for replay, historical backfill, and live ingestion."""

from __future__ import annotations

from collections.abc import Iterable

from ..domain.models import Bar
from .historical_service import HistoricalBackfillService
from .live_feed import LivePollingService, LiveStreamService
from .replay_feed import ReplayFeed
from .schwab_models import SchwabHistoricalRequest, SchwabLivePollRequest


class MarketDataGateway:
    """Coordinates replay, historical backfill, and live ingestion into the same Bar model."""

    def __init__(
        self,
        replay_feed: ReplayFeed | None = None,
        historical_service: HistoricalBackfillService | None = None,
        live_polling_service: LivePollingService | None = None,
        live_stream_service: LiveStreamService | None = None,
    ) -> None:
        self._replay_feed = replay_feed
        self._historical_service = historical_service
        self._live_polling_service = live_polling_service
        self._live_stream_service = live_stream_service

    def iter_replay_bars(self, csv_path: str) -> Iterable[Bar]:
        if self._replay_feed is None:
            raise ValueError("ReplayFeed is not configured.")
        return self._replay_feed.iter_csv(csv_path)

    def fetch_historical_bars(self, request: SchwabHistoricalRequest, internal_timeframe: str) -> list[Bar]:
        if self._historical_service is None:
            raise ValueError("HistoricalBackfillService is not configured.")
        return self._historical_service.fetch_bars(request, internal_timeframe)

    def poll_live_bars(
        self,
        request: SchwabLivePollRequest,
        internal_timeframe: str,
        default_is_final: bool = False,
    ) -> list[Bar]:
        if self._live_polling_service is None:
            raise ValueError("LivePollingService is not configured.")
        return self._live_polling_service.poll_bars(request, internal_timeframe, default_is_final=default_is_final)

    def subscribe_live_bars(self, internal_symbol: str, internal_timeframe: str) -> Iterable[Bar]:
        if self._live_stream_service is None:
            raise ValueError("LiveStreamService is not configured.")
        return self._live_stream_service.subscribe_bars(internal_symbol, internal_timeframe)
