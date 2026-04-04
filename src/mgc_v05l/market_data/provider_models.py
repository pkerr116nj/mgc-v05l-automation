"""Provider-agnostic market-data request, response, and provenance models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from ..domain.models import Bar


class MarketDataUseCase(str, Enum):
    HISTORICAL_RESEARCH = "historical_research"
    LIVE_MARKET_DATA = "live_market_data"


class CoverageChange(str, Enum):
    INITIAL = "initial"
    WIDENED = "widened"
    MATCHED = "matched"
    APPENDED = "appended"
    NARROWED = "narrowed"


@dataclass(frozen=True)
class HistoricalBarsRequest:
    internal_symbol: str
    timeframe: str
    start: datetime
    end: datetime | None = None
    limit: int | None = None


@dataclass(frozen=True)
class QuoteSnapshot:
    internal_symbol: str
    external_symbol: str
    bid_price: Decimal | None = None
    ask_price: Decimal | None = None
    last_price: Decimal | None = None
    mark_price: Decimal | None = None
    close_price: Decimal | None = None
    quote_time: datetime | None = None
    delayed: bool | None = None
    provider: str = ""
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class HistoricalBarProvenance:
    provider: str
    dataset: str | None
    schema_name: str | None
    raw_symbol: str | None
    stype_in: str | None
    stype_out: str | None
    interval: str
    ingest_time: datetime
    coverage_start: datetime | None
    coverage_end: datetime | None
    provenance_tag: str
    request_symbol: str | None = None
    provider_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class HistoricalBarsResult:
    provider: str
    data_source: str
    internal_symbol: str
    timeframe: str
    bars: list[Bar]
    coverage_start: datetime | None
    coverage_end: datetime | None
    ingest_time: datetime
    dataset: str | None = None
    schema_name: str | None = None
    stype_in: str | None = None
    stype_out: str | None = None
    request_symbol: str | None = None
    provenance_tag: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    bar_provenance: dict[str, HistoricalBarProvenance] = field(default_factory=dict)


@dataclass(frozen=True)
class CoverageSnapshot:
    symbol: str
    timeframe: str
    data_source: str
    bar_count: int
    earliest: str | None
    latest: str | None


@dataclass(frozen=True)
class HistoricalIngestAudit:
    provider: str
    internal_symbol: str
    timeframe: str
    data_source: str
    before: CoverageSnapshot
    after: CoverageSnapshot
    change: CoverageChange
    inserted_bar_count: int
    skipped_existing_count: int
    ingest_run_id: str
    report_path: str | None = None
