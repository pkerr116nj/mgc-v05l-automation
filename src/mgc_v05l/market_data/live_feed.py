"""Live ingestion scaffolding for Schwab market data."""

from __future__ import annotations

from datetime import datetime, timedelta
from collections.abc import Iterable
from typing import Optional

from ..domain.models import Bar
from ..persistence.repositories import RepositorySet
from .schwab_adapter import SchwabMarketDataAdapter
from .schwab_models import (
    SchwabHistoricalClient,
    SchwabHistoricalRequest,
    SchwabLivePollRequest,
    SchwabLivePollingClient,
    SchwabLiveStreamClient,
)
from .timeframes import timeframe_minutes


class HistoricalPollingLiveClient:
    """Uses recent Schwab price-history bars as a completed-bar live polling source."""

    def __init__(
        self,
        adapter: SchwabMarketDataAdapter,
        historical_client: SchwabHistoricalClient,
        lookback_minutes: int = 180,
    ) -> None:
        self._adapter = adapter
        self._historical_client = historical_client
        self._lookback_minutes = lookback_minutes

    def poll_live_bars(
        self,
        external_symbol: str,
        external_timeframe: str,
        request: SchwabLivePollRequest,
    ) -> list[dict]:
        now = datetime.now(self._adapter._settings.timezone_info)  # noqa: SLF001 - adapter already owns runtime tz
        timeframe_duration = timedelta(minutes=timeframe_minutes(external_timeframe))
        start_dt = request.since - timeframe_duration if request.since is not None else (
            now - timedelta(minutes=self._lookback_minutes)
        )
        payload = self._historical_client.fetch_price_history(
            external_symbol,
            SchwabHistoricalRequest(
                internal_symbol=request.internal_symbol,
                period_type="day",
                frequency_type=self._adapter.map_timeframe(external_timeframe).frequency_type,
                frequency=self._adapter.map_timeframe(external_timeframe).frequency,
                start_date_ms=int(start_dt.timestamp() * 1000),
                end_date_ms=int(now.timestamp() * 1000),
                need_extended_hours_data=True,
                need_previous_close=False,
            ),
            default_frequency=self._adapter.map_timeframe(external_timeframe),
        )
        records = payload.get("candles", [])
        if not isinstance(records, list):
            raise ValueError("Schwab live polling payload must expose a candle list.")
        return list(records)


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
        default_is_final: bool = True,
    ) -> list[Bar]:
        if self._client is None:
            raise NotImplementedError(
                "Schwab live polling integration is pending official API confirmation. "
                "Fill in the SchwabLivePollingClient once docs are confirmed."
            )

        external_symbol = self._adapter.map_historical_symbol(request.internal_symbol)
        raw_records = self._client.poll_live_bars(external_symbol, internal_timeframe, request)
        bars = self._adapter.normalize_live_records(
            raw_records,
            request.internal_symbol,
            internal_timeframe,
            default_is_final=default_is_final,
        )
        bars = self._filter_completed_bars(bars, request=request, internal_timeframe=internal_timeframe)
        self._persist_bars(bars)
        return bars

    def _filter_completed_bars(
        self,
        bars: list[Bar],
        request: SchwabLivePollRequest,
        internal_timeframe: str,
    ) -> list[Bar]:
        if not bars:
            return []
        latest_completed_end = _latest_completed_bar_end(datetime.now(bars[-1].end_ts.tzinfo), internal_timeframe)
        filtered = [bar for bar in bars if bar.is_final and bar.end_ts <= latest_completed_end]
        if request.since is not None:
            filtered = [bar for bar in filtered if bar.end_ts > request.since]
        return filtered

    def _persist_bars(self, bars: list[Bar]) -> None:
        if self._repositories is None:
            return
        for bar in bars:
            self._repositories.bars.save(bar, data_source="schwab_live_poll")


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

        external_symbol = self._adapter.map_historical_symbol(internal_symbol)
        raw_records = self._client.subscribe_live_bars(external_symbol, internal_timeframe)
        return self._adapter.normalize_live_records(raw_records, internal_symbol, internal_timeframe, default_is_final=False)


def _latest_completed_bar_end(now: datetime, internal_timeframe: str) -> datetime:
    minutes = timeframe_minutes(internal_timeframe)
    floored_minute = now.minute - (now.minute % minutes)
    return now.replace(minute=floored_minute, second=0, microsecond=0)
