"""Helpers for loading explicit Schwab market-data mapping config."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from .schwab_auth import load_schwab_auth_config_from_env
from .schwab_models import (
    SchwabBarFieldMap,
    SchwabMarketDataConfig,
    SchwabPriceHistoryFrequency,
    TimestampSemantics,
)
from .timeframes import normalize_timeframe_label


def load_schwab_market_data_config(path: str | Path | None = None) -> SchwabMarketDataConfig:
    """Load explicit Schwab symbol/timeframe mapping plus env-backed auth config."""
    payload: dict[str, Any] = {}
    if path is not None:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))

    timeframe_map = {
        normalize_timeframe_label(internal_timeframe): SchwabPriceHistoryFrequency(
            frequency_type=str(spec["frequency_type"]),
            frequency=int(spec["frequency"]),
        )
        for internal_timeframe, spec in payload.get("timeframe_map", {}).items()
    }

    return SchwabMarketDataConfig(
        auth=load_schwab_auth_config_from_env(payload.get("token_store_path")),
        historical_symbol_map={
            str(key): str(value) for key, value in payload.get("historical_symbol_map", {}).items()
        },
        quote_symbol_map={str(key): str(value) for key, value in payload.get("quote_symbol_map", {}).items()},
        timeframe_map=timeframe_map,
        field_map=SchwabBarFieldMap(
            timestamp_field="datetime",
            open_field="open",
            high_field="high",
            low_field="low",
            close_field="close",
            volume_field="volume",
            is_final_field=None,
            timestamp_semantics=TimestampSemantics.END,
        ),
        market_context_quote_symbols={
            str(key): str(value) for key, value in payload.get("market_context_quote_symbols", {}).items()
        },
        treasury_context_quote_symbols={
            str(key): str(value) for key, value in payload.get("treasury_context_quote_symbols", {}).items()
        },
        market_data_base_url=str(
            payload.get("market_data_base_url", "https://api.schwabapi.com/marketdata/v1")
        ),
        quotes_symbol_query_param=str(payload.get("quotes_symbol_query_param", "symbols")),
    )
