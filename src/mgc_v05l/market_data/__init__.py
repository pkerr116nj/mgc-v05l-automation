"""Market-data package for replay and external-ingestion adapters.

This package exposes a broad convenience surface. Keep imports lazy so callers
that only need narrow utilities, such as timeframe normalization, do not pull
in the entire runtime graph during package initialization.
"""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "Bar",
    "BarBuilder",
    "build_auth_metadata",
    "CanonicalMarketDataMaintenanceService",
    "HistoricalBackfillService",
    "HistoricalMarketDataIngestionService",
    "HistoricalPollingLiveClient",
    "LivePollingService",
    "LiveStreamService",
    "DatabentoMarketDataProvider",
    "MarketDataGateway",
    "MarketDataProvider",
    "MarketDataUseCase",
    "HistoricalBarsRequest",
    "HistoricalBarsResult",
    "HistoricalIngestAudit",
    "QuoteSnapshot",
    "QuoteService",
    "ReplayFeed",
    "SchwabAuthConfig",
    "SchwabBarFieldMap",
    "SchwabAuthError",
    "SchwabHistoricalRequest",
    "SchwabHistoricalHttpClient",
    "SchwabLivePollRequest",
    "SchwabMarketDataAdapter",
    "SchwabMarketDataProvider",
    "SchwabMarketDataConfig",
    "SchwabOAuthClient",
    "SchwabPriceHistoryFrequency",
    "SchwabQuoteHttpClient",
    "SchwabQuoteRequest",
    "SchwabQuoteResult",
    "SchwabTokenSet",
    "SchwabTokenStore",
    "TimestampSemantics",
    "UrllibJsonTransport",
    "build_bar_id",
    "json_ready_loopback_result",
    "load_schwab_auth_config_from_env",
    "load_schwab_market_data_config",
    "normalize_timeframe_label",
    "run_loopback_authorization",
    "timeframe_aliases",
    "timeframe_minutes",
]

_EXPORT_MAP = {
    "BarBuilder": (".bar_builder", "BarBuilder"),
    "Bar": (".bar_models", "Bar"),
    "build_bar_id": (".bar_models", "build_bar_id"),
    "CanonicalMarketDataMaintenanceService": (".canonical_maintenance", "CanonicalMarketDataMaintenanceService"),
    "MarketDataGateway": (".gateway", "MarketDataGateway"),
    "HistoricalBackfillService": (".historical_service", "HistoricalBackfillService"),
    "HistoricalMarketDataIngestionService": (".provider_ingest", "HistoricalMarketDataIngestionService"),
    "HistoricalPollingLiveClient": (".live_feed", "HistoricalPollingLiveClient"),
    "LivePollingService": (".live_feed", "LivePollingService"),
    "LiveStreamService": (".live_feed", "LiveStreamService"),
    "DatabentoMarketDataProvider": (".databento_provider", "DatabentoMarketDataProvider"),
    "QuoteService": (".quote_service", "QuoteService"),
    "ReplayFeed": (".replay_feed", "ReplayFeed"),
    "MarketDataProvider": (".provider_interfaces", "MarketDataProvider"),
    "MarketDataUseCase": (".provider_models", "MarketDataUseCase"),
    "HistoricalBarsRequest": (".provider_models", "HistoricalBarsRequest"),
    "HistoricalBarsResult": (".provider_models", "HistoricalBarsResult"),
    "HistoricalIngestAudit": (".provider_models", "HistoricalIngestAudit"),
    "QuoteSnapshot": (".provider_models", "QuoteSnapshot"),
    "SchwabAuthError": (".schwab_auth", "SchwabAuthError"),
    "SchwabOAuthClient": (".schwab_auth", "SchwabOAuthClient"),
    "SchwabTokenStore": (".schwab_auth", "SchwabTokenStore"),
    "build_auth_metadata": (".schwab_auth", "build_auth_metadata"),
    "load_schwab_auth_config_from_env": (".schwab_auth", "load_schwab_auth_config_from_env"),
    "SchwabMarketDataAdapter": (".schwab_adapter", "SchwabMarketDataAdapter"),
    "SchwabMarketDataProvider": (".schwab_provider", "SchwabMarketDataProvider"),
    "load_schwab_market_data_config": (".schwab_config", "load_schwab_market_data_config"),
    "SchwabHistoricalHttpClient": (".schwab_http", "SchwabHistoricalHttpClient"),
    "SchwabQuoteHttpClient": (".schwab_http", "SchwabQuoteHttpClient"),
    "UrllibJsonTransport": (".schwab_http", "UrllibJsonTransport"),
    "json_ready_loopback_result": (".schwab_local_auth", "json_ready_loopback_result"),
    "run_loopback_authorization": (".schwab_local_auth", "run_loopback_authorization"),
    "SchwabAuthConfig": (".schwab_models", "SchwabAuthConfig"),
    "SchwabBarFieldMap": (".schwab_models", "SchwabBarFieldMap"),
    "SchwabHistoricalRequest": (".schwab_models", "SchwabHistoricalRequest"),
    "SchwabLivePollRequest": (".schwab_models", "SchwabLivePollRequest"),
    "SchwabMarketDataConfig": (".schwab_models", "SchwabMarketDataConfig"),
    "SchwabPriceHistoryFrequency": (".schwab_models", "SchwabPriceHistoryFrequency"),
    "SchwabQuoteRequest": (".schwab_models", "SchwabQuoteRequest"),
    "SchwabQuoteResult": (".schwab_models", "SchwabQuoteResult"),
    "SchwabTokenSet": (".schwab_models", "SchwabTokenSet"),
    "TimestampSemantics": (".schwab_models", "TimestampSemantics"),
    "normalize_timeframe_label": (".timeframes", "normalize_timeframe_label"),
    "timeframe_aliases": (".timeframes", "timeframe_aliases"),
    "timeframe_minutes": (".timeframes", "timeframe_minutes"),
}


def __getattr__(name: str):
    module_name, export_name = _EXPORT_MAP[name]
    module = import_module(module_name, __name__)
    value = getattr(module, export_name)
    globals()[name] = value
    return value
