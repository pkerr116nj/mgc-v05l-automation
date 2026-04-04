"""Databento historical provider for deep replay/research backfills."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ..config_models import StrategySettings
from ..domain.models import Bar
from .bar_builder import BarBuilder
from .bar_models import build_bar_id
from .provider_config import DatabentoProviderConfig, load_market_data_providers_config
from .provider_interfaces import MarketDataProvider
from .provider_models import HistoricalBarProvenance, HistoricalBarsRequest, HistoricalBarsResult, QuoteSnapshot
from .timeframes import normalize_timeframe_label, timeframe_minutes


class DatabentoHttpError(RuntimeError):
    """Raised when the Databento HTTP layer fails."""


class DatabentoTransport(Protocol):
    def request_lines(self, *, url: str, headers: dict[str, str], form: dict[str, Any]) -> list[str]:
        """Execute a Databento request and return decoded lines."""


class UrllibDatabentoTransport:
    """Small stdlib-only transport for Databento historical requests."""

    def __init__(self, timeout_seconds: int = 60) -> None:
        self._timeout_seconds = timeout_seconds

    def request_lines(self, *, url: str, headers: dict[str, str], form: dict[str, Any]) -> list[str]:
        body = urlencode({key: _encode_form_value(value) for key, value in form.items()}).encode("utf-8")
        request = Request(url=url, method="POST", headers=headers, data=body)
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                payload = response.read().decode("utf-8")
        except HTTPError as exc:  # pragma: no cover - exercised in integration only
            detail = exc.read().decode("utf-8", errors="replace")
            raise DatabentoHttpError(f"Databento HTTP error {exc.code}: {detail}") from exc
        except URLError as exc:  # pragma: no cover - exercised in integration only
            raise DatabentoHttpError(f"Databento transport error: {exc}") from exc
        return [line for line in payload.splitlines() if line.strip()]


@dataclass(frozen=True)
class DatabentoHistoricalHttpClient:
    api_key: str
    base_url: str
    transport: DatabentoTransport

    def get_range_json_lines(
        self,
        *,
        dataset: str,
        request_symbol: str,
        schema_name: str,
        start: datetime,
        end: datetime | None,
        stype_in: str,
        stype_out: str,
        encoding: str,
        compression: str,
        pretty_px: bool,
        pretty_ts: bool,
        map_symbols: bool,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        headers = {
            "Accept": "application/x-ndjson",
            "Authorization": _basic_auth_header(self.api_key),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        form: dict[str, Any] = {
            "dataset": dataset,
            "symbols": request_symbol,
            "schema": schema_name,
            "start": start.astimezone(UTC).isoformat(),
            "stype_in": stype_in,
            "stype_out": stype_out,
            "encoding": encoding,
            "compression": compression,
            "pretty_px": pretty_px,
            "pretty_ts": pretty_ts,
            "map_symbols": map_symbols,
        }
        if end is not None:
            form["end"] = end.astimezone(UTC).isoformat()
        if limit is not None:
            form["limit"] = int(limit)
        lines = self.transport.request_lines(
            url=f"{self.base_url.rstrip('/')}/timeseries.get_range",
            headers=headers,
            form=form,
        )
        return [json.loads(line) for line in lines]


class DatabentoMarketDataProvider(MarketDataProvider):
    """Provider implementation for Databento historical bars."""

    provider_id = "databento"

    def __init__(
        self,
        settings: StrategySettings,
        *,
        repo_root: Path | None = None,
        config_path: str | Path | None = None,
        api_key: str | None = None,
        client: DatabentoHistoricalHttpClient | None = None,
    ) -> None:
        self._settings = settings
        self._repo_root = (repo_root or Path.cwd()).resolve(strict=False)
        self._providers_config = load_market_data_providers_config(config_path)
        self._config: DatabentoProviderConfig = self._providers_config.databento
        self._bar_builder = BarBuilder(settings)
        resolved_api_key = api_key or __import__("os").environ.get(self._config.api_key_env)
        self._api_key = str(resolved_api_key or "").strip()
        self._client = client or DatabentoHistoricalHttpClient(
            api_key=self._api_key,
            base_url=self._config.historical_base_url,
            transport=UrllibDatabentoTransport(),
        )

    def fetch_historical_bars(self, request: HistoricalBarsRequest) -> HistoricalBarsResult:
        if not self._api_key:
            raise RuntimeError(
                f"Databento historical access requires {self._config.api_key_env} to be set in the environment."
            )
        normalized_timeframe = normalize_timeframe_label(request.timeframe)
        symbol_config = self._config.pilot_symbols.get(request.internal_symbol)
        if symbol_config is None:
            raise ValueError(f"No Databento pilot symbol mapping configured for {request.internal_symbol!r}.")
        schema_name = symbol_config.schema_by_timeframe.get(normalized_timeframe)
        if schema_name is None:
            raise ValueError(
                f"No Databento schema is configured for {request.internal_symbol!r} {normalized_timeframe!r}."
            )
        records = self._client.get_range_json_lines(
            dataset=symbol_config.dataset,
            request_symbol=symbol_config.request_symbol,
            schema_name=schema_name,
            start=request.start,
            end=request.end,
            stype_in=symbol_config.stype_in,
            stype_out=symbol_config.stype_out,
            encoding=self._config.encoding,
            compression=self._config.compression,
            pretty_px=self._config.pretty_px,
            pretty_ts=self._config.pretty_ts,
            map_symbols=self._config.map_symbols,
            limit=request.limit,
        )
        ingest_time = datetime.now(UTC)
        bars: list[Bar] = []
        raw_symbols_by_bar_id: dict[str, str | None] = {}
        provider_metadata_by_bar_id: dict[str, dict[str, Any]] = {}
        interval = normalize_timeframe_label(request.timeframe)
        for record in records:
            if not _looks_like_ohlcv_record(record):
                continue
            header = _record_header(record)
            start_ts = _parse_timestamp(_record_timestamp(record), settings=self._settings)
            end_ts = start_ts + timedelta(minutes=timeframe_minutes(interval))
            bar = self._bar_builder.require_finalized(
                self._bar_builder.normalize(
                    Bar(
                        bar_id=build_bar_id(request.internal_symbol, interval, end_ts),
                        symbol=request.internal_symbol,
                        timeframe=interval,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        open=Decimal(str(record["open"])),
                        high=Decimal(str(record["high"])),
                        low=Decimal(str(record["low"])),
                        close=Decimal(str(record["close"])),
                        volume=int(record.get("volume") or 0),
                        is_final=True,
                        session_asia=False,
                        session_london=False,
                        session_us=False,
                        session_allowed=False,
                    )
                )
            )
            bars.append(bar)
            raw_symbols_by_bar_id[bar.bar_id] = _record_raw_symbol(
                record,
                stype_out=symbol_config.stype_out,
                request_symbol=symbol_config.request_symbol,
            )
            provider_metadata_by_bar_id[bar.bar_id] = {
                "instrument_id": header.get("instrument_id"),
                "publisher_id": header.get("publisher_id"),
                "response_symbol": str(record.get("symbol") or "").strip() or None,
            }
        bars.sort(key=lambda item: item.end_ts)
        coverage_start = bars[0].start_ts if bars else None
        coverage_end = bars[-1].end_ts if bars else None
        data_source = self._config.canonical_data_source_by_timeframe.get(interval, f"databento_{interval}_canonical")
        provenance = {
            bar.bar_id: HistoricalBarProvenance(
                provider=self.provider_id,
                dataset=symbol_config.dataset,
                schema_name=schema_name,
                raw_symbol=raw_symbols_by_bar_id.get(bar.bar_id),
                stype_in=symbol_config.stype_in,
                stype_out=symbol_config.stype_out,
                interval=interval,
                ingest_time=ingest_time,
                coverage_start=coverage_start,
                coverage_end=coverage_end,
                provenance_tag=self._config.provenance_tag,
                request_symbol=symbol_config.request_symbol,
                provider_metadata=provider_metadata_by_bar_id.get(bar.bar_id) or {},
            )
            for bar in bars
        }
        return HistoricalBarsResult(
            provider=self.provider_id,
            data_source=data_source,
            internal_symbol=request.internal_symbol,
            timeframe=interval,
            bars=bars,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            ingest_time=ingest_time,
            dataset=symbol_config.dataset,
            schema_name=schema_name,
            stype_in=symbol_config.stype_in,
            stype_out=symbol_config.stype_out,
            request_symbol=symbol_config.request_symbol,
            provenance_tag=self._config.provenance_tag,
            metadata={
                "record_count": len(bars),
                "description": symbol_config.description,
                "exchange": symbol_config.exchange,
                "api_base_url": self._config.historical_base_url,
            },
            bar_provenance=provenance,
        )

    def fetch_quotes(self, internal_symbols: list[str] | tuple[str, ...]) -> list[QuoteSnapshot]:
        raise NotImplementedError("Databento live quotes are not wired in this pass.")

    def describe_symbol(self, internal_symbol: str) -> dict[str, Any]:
        symbol_config = self._config.pilot_symbols.get(internal_symbol)
        if symbol_config is None:
            raise ValueError(f"No Databento pilot symbol mapping configured for {internal_symbol!r}.")
        return symbol_config.model_dump(mode="json")

    def subscribe_live_quotes(self, internal_symbols: list[str] | tuple[str, ...]):
        raise NotImplementedError("Databento live streaming is reserved for a later pass.")


def _basic_auth_header(api_key: str) -> str:
    token = base64.b64encode(f"{api_key}:".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def _encode_form_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _parse_timestamp(value: Any, *, settings: StrategySettings) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=settings.timezone_info)
    return parsed.astimezone(settings.timezone_info)


def _looks_like_ohlcv_record(record: dict[str, Any]) -> bool:
    required = {"ts_event", "open", "high", "low", "close", "volume"}
    if required.issubset(set(record)):
        return True
    header = record.get("hd")
    return isinstance(header, dict) and required.difference({"ts_event"}).issubset(set(record)) and "ts_event" in header


def _record_header(record: dict[str, Any]) -> dict[str, Any]:
    header = record.get("hd")
    if isinstance(header, dict):
        return header
    return record


def _record_timestamp(record: dict[str, Any]) -> Any:
    if "ts_event" in record:
        return record["ts_event"]
    header = _record_header(record)
    return header["ts_event"]


def _record_raw_symbol(record: dict[str, Any], *, stype_out: str, request_symbol: str) -> str | None:
    symbol = str(record.get("symbol") or "").strip() or None
    if symbol is None:
        return None
    if stype_out == "raw_symbol":
        return symbol
    if symbol != request_symbol:
        return symbol
    return None
