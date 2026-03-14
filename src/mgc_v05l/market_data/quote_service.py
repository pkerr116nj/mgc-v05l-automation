"""Quote service kept outside the strategy decision path."""

from __future__ import annotations

from .schwab_adapter import SchwabMarketDataAdapter
from .schwab_models import SchwabQuoteClient, SchwabQuoteRequest, SchwabQuoteResult


class QuoteService:
    """Fetch and normalize quotes without feeding strategy decisions."""

    def __init__(self, adapter: SchwabMarketDataAdapter, client: SchwabQuoteClient) -> None:
        self._adapter = adapter
        self._client = client

    def fetch_quotes(self, request: SchwabQuoteRequest) -> list[SchwabQuoteResult]:
        external_symbols = [self._adapter.map_quote_symbol(symbol) for symbol in request.internal_symbols]
        payload = self._client.fetch_quotes(external_symbols)
        return self._adapter.normalize_quote_response(payload, request.internal_symbols)
