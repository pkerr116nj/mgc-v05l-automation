"""Provider wrapper around the existing Schwab market-data services."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..config_models import StrategySettings
from .historical_service import HistoricalBackfillService
from .provider_config import load_market_data_providers_config
from .provider_interfaces import MarketDataProvider
from .provider_models import HistoricalBarProvenance, HistoricalBarsRequest, HistoricalBarsResult, QuoteSnapshot
from .quote_service import QuoteService
from .schwab_adapter import SchwabMarketDataAdapter
from .schwab_auth import SchwabOAuthClient, SchwabTokenStore
from .schwab_config import load_schwab_market_data_config
from .schwab_http import SchwabHistoricalHttpClient, SchwabQuoteHttpClient, UrllibJsonTransport
from .schwab_models import SchwabHistoricalRequest, SchwabQuoteRequest


class SchwabMarketDataProvider(MarketDataProvider):
    """Provider implementation for Schwab market data."""

    provider_id = "schwab_market_data"

    def __init__(
        self,
        settings: StrategySettings,
        *,
        repo_root: Path | None = None,
        provider_config_path: str | Path | None = None,
        schwab_config_path: str | Path | None = None,
        quote_service: QuoteService | None = None,
        historical_service: HistoricalBackfillService | None = None,
        adapter: SchwabMarketDataAdapter | None = None,
    ) -> None:
        self._settings = settings
        self._repo_root = (repo_root or Path.cwd()).resolve(strict=False)
        providers_config = load_market_data_providers_config(provider_config_path)
        self._config = providers_config.schwab_market_data
        self._schwab_config = load_schwab_market_data_config(
            schwab_config_path or (self._repo_root / self._config.config_path)
        )
        self._adapter = adapter or SchwabMarketDataAdapter(settings, self._schwab_config)
        oauth_client = SchwabOAuthClient(
            config=self._schwab_config.auth,
            transport=UrllibJsonTransport(),
            token_store=SchwabTokenStore(self._schwab_config.auth.token_store_path),
        )
        self._quote_service = quote_service or QuoteService(
            adapter=self._adapter,
            client=SchwabQuoteHttpClient(
                oauth_client=oauth_client,
                market_data_config=self._schwab_config,
                transport=UrllibJsonTransport(),
            ),
        )
        self._historical_service = historical_service or HistoricalBackfillService(
            adapter=self._adapter,
            client=SchwabHistoricalHttpClient(
                oauth_client=oauth_client,
                market_data_config=self._schwab_config,
                transport=UrllibJsonTransport(),
            ),
            repositories=None,
        )

    def fetch_historical_bars(self, request: HistoricalBarsRequest) -> HistoricalBarsResult:
        timeframe = request.timeframe
        bars = self._historical_service.fetch_bars(
            SchwabHistoricalRequest(
                internal_symbol=request.internal_symbol,
                period_type="day",
                frequency_type="minute",
                frequency=1,
                start_date_ms=int(request.start.astimezone(UTC).timestamp() * 1000),
                end_date_ms=int(request.end.astimezone(UTC).timestamp() * 1000) if request.end is not None else None,
                need_extended_hours_data=True,
                need_previous_close=False,
            ),
            internal_timeframe=timeframe,
        )
        ingest_time = datetime.now(UTC)
        coverage_start = bars[0].start_ts if bars else None
        coverage_end = bars[-1].end_ts if bars else None
        external_symbol = self._adapter.map_historical_symbol(request.internal_symbol)
        data_source = self._config.canonical_data_source_by_timeframe.get(timeframe, "historical_1m_canonical")
        provenance = {
            bar.bar_id: HistoricalBarProvenance(
                provider=self.provider_id,
                dataset=None,
                schema_name="pricehistory",
                raw_symbol=external_symbol,
                stype_in="raw_symbol",
                stype_out="raw_symbol",
                interval=timeframe,
                ingest_time=ingest_time,
                coverage_start=coverage_start,
                coverage_end=coverage_end,
                provenance_tag=self._config.provenance_tag,
                request_symbol=external_symbol,
            )
            for bar in bars
        }
        return HistoricalBarsResult(
            provider=self.provider_id,
            data_source=data_source,
            internal_symbol=request.internal_symbol,
            timeframe=timeframe,
            bars=bars,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            ingest_time=ingest_time,
            dataset="schwab_marketdata_v1",
            schema_name="pricehistory",
            stype_in="raw_symbol",
            stype_out="raw_symbol",
            request_symbol=external_symbol,
            provenance_tag=self._config.provenance_tag,
            metadata={"external_symbol": external_symbol},
            bar_provenance=provenance,
        )

    def fetch_quotes(self, internal_symbols: list[str] | tuple[str, ...]) -> list[QuoteSnapshot]:
        results = self._quote_service.fetch_quotes(SchwabQuoteRequest(internal_symbols=tuple(internal_symbols)))
        snapshots: list[QuoteSnapshot] = []
        for result in results:
            quote = dict(result.quote_future or {})
            reference = dict(result.reference_future or {})
            snapshots.append(
                QuoteSnapshot(
                    internal_symbol=result.internal_symbol,
                    external_symbol=result.external_symbol,
                    bid_price=_decimal_or_none(quote.get("bidPrice") or quote.get("bid")),
                    ask_price=_decimal_or_none(quote.get("askPrice") or quote.get("ask")),
                    last_price=_decimal_or_none(quote.get("lastPrice") or quote.get("last")),
                    mark_price=_decimal_or_none(quote.get("mark") or quote.get("markPrice")),
                    close_price=_decimal_or_none(quote.get("closePrice") or reference.get("closePrice")),
                    delayed=_bool_or_none(quote.get("delayed")),
                    provider=self.provider_id,
                    raw_payload=result.raw_payload,
                )
            )
        return snapshots

    def describe_symbol(self, internal_symbol: str) -> dict[str, Any]:
        return {
            "internal_symbol": internal_symbol,
            "historical_symbol": self._adapter.map_historical_symbol(internal_symbol),
            "quote_symbol": self._adapter.map_quote_symbol(internal_symbol),
        }

    def subscribe_live_quotes(self, internal_symbols: list[str] | tuple[str, ...]):
        raise NotImplementedError("Schwab live streaming stays outside this pass.")


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)
