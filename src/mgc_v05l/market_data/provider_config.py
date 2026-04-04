"""Config loader for provider routing, precedence, and Databento symbol metadata."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from .provider_models import MarketDataUseCase
from .timeframes import normalize_timeframe_label

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PROVIDER_CONFIG_PATH = REPO_ROOT / "config" / "market_data_providers.json"


class ProviderSelectionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    historical_research: tuple[str, ...]
    live_market_data: tuple[str, ...]
    execution_truth: tuple[str, ...]


class SchwabMarketDataProviderConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    config_path: str = "config/schwab.local.json"
    provenance_tag: str = "schwab_market_data"
    canonical_data_source_by_timeframe: dict[str, str] = Field(default_factory=dict)


class DatabentoSymbolConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_symbol: str
    dataset: str = "GLBX.MDP3"
    stype_in: str = "continuous"
    stype_out: str = "raw_symbol"
    schema_by_timeframe: dict[str, str]
    asset_class: str = "future"
    exchange: str | None = None
    description: str | None = None


class DatabentoProviderConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    api_key_env: str = "DATABENTO_API_KEY"
    historical_base_url: str = "https://hist.databento.com/v0"
    encoding: str = "json"
    compression: str = "none"
    pretty_px: bool = True
    pretty_ts: bool = True
    map_symbols: bool = True
    provenance_tag: str = "databento_historical"
    canonical_data_source_by_timeframe: dict[str, str] = Field(default_factory=dict)
    pilot_symbols: dict[str, DatabentoSymbolConfig]


class MarketDataProvidersConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_selection: ProviderSelectionConfig
    data_source_precedence: dict[str, dict[str, tuple[str, ...]]] = Field(default_factory=dict)
    schwab_market_data: SchwabMarketDataProviderConfig
    databento: DatabentoProviderConfig

    def preferred_providers(self, use_case: MarketDataUseCase | str) -> tuple[str, ...]:
        key = str(use_case.value if isinstance(use_case, MarketDataUseCase) else use_case)
        return tuple(getattr(self.provider_selection, key))

    def preferred_data_sources(self, use_case: MarketDataUseCase | str, timeframe: str) -> tuple[str, ...]:
        use_case_key = str(use_case.value if isinstance(use_case, MarketDataUseCase) else use_case)
        return tuple((self.data_source_precedence.get(use_case_key) or {}).get(normalize_timeframe_label(timeframe), ()))


def load_market_data_providers_config(path: str | Path | None = None) -> MarketDataProvidersConfig:
    resolved = _resolve_path(path or DEFAULT_PROVIDER_CONFIG_PATH)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    return MarketDataProvidersConfig.model_validate(payload)


def provider_config_path(path: str | Path | None = None) -> Path:
    return _resolve_path(path or DEFAULT_PROVIDER_CONFIG_PATH)


def _resolve_path(path: str | Path) -> Path:
    raw = Path(path)
    if raw.is_absolute():
        return raw.resolve(strict=False)
    return (REPO_ROOT / raw).resolve(strict=False)
