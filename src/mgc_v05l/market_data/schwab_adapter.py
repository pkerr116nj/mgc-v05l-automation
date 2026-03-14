"""Schwab market-data adapter boundary and response normalization."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Iterable, Optional, Sequence

from ..config_models import StrategySettings
from ..domain.models import Bar
from .bar_builder import BarBuilder
from .bar_models import build_bar_id
from .schwab_models import (
    SchwabHistoricalRequest,
    SchwabMarketDataConfig,
    SchwabPriceHistoryFrequency,
    SchwabQuoteResult,
    TimestampSemantics,
)


class SchwabMarketDataAdapter:
    """Maps Schwab symbols/timeframes and normalizes confirmed payloads."""

    def __init__(self, settings: StrategySettings, config: SchwabMarketDataConfig) -> None:
        self._settings = settings
        self._config = config
        self._bar_builder = BarBuilder(settings)

    def map_symbol(self, internal_symbol: str) -> str:
        return self.map_historical_symbol(internal_symbol)

    def map_historical_symbol(self, internal_symbol: str) -> str:
        try:
            return self._config.historical_symbol_map[internal_symbol]
        except KeyError as exc:
            raise ValueError(
                f"No Schwab historical symbol mapping configured for {internal_symbol!r}."
            ) from exc

    def map_quote_symbol(self, internal_symbol: str) -> str:
        try:
            return self._config.quote_symbol_map[internal_symbol]
        except KeyError as exc:
            raise ValueError(f"No Schwab quote symbol mapping configured for {internal_symbol!r}.") from exc

    def map_timeframe(self, internal_timeframe: str) -> SchwabPriceHistoryFrequency:
        try:
            return self._config.timeframe_map[internal_timeframe]
        except KeyError as exc:
            raise ValueError(f"No Schwab timeframe mapping configured for {internal_timeframe!r}.") from exc

    def resolve_history_frequency(
        self,
        request: SchwabHistoricalRequest,
        internal_timeframe: str,
    ) -> Optional[SchwabPriceHistoryFrequency]:
        if request.frequency_type is not None and request.frequency is not None:
            return SchwabPriceHistoryFrequency(
                frequency_type=request.frequency_type,
                frequency=request.frequency,
            )
        return self._config.timeframe_map.get(internal_timeframe)

    def normalize_pricehistory_response(
        self,
        payload: dict[str, Any],
        internal_symbol: str,
        internal_timeframe: str,
    ) -> list[Bar]:
        if "candles" not in payload:
            raise ValueError("Schwab /pricehistory response is missing the 'candles' field.")
        records = payload["candles"]
        if not isinstance(records, list):
            raise ValueError("Schwab /pricehistory 'candles' field must be a list.")
        bars = self.normalize_historical_records(records, internal_symbol, internal_timeframe)
        self._validate_bar_sequence(bars)
        return bars

    def normalize_historical_records(
        self,
        records: Sequence[dict[str, Any]],
        internal_symbol: str,
        internal_timeframe: str,
    ) -> list[Bar]:
        bars = [
            self._bar_builder.require_finalized(
                self._normalize_record(record, internal_symbol, internal_timeframe, default_is_final=True)
            )
            for record in records
        ]
        bars = sorted(bars, key=lambda bar: bar.end_ts)
        self._validate_bar_sequence(bars)
        return bars

    def normalize_live_records(
        self,
        records: Sequence[dict[str, Any]],
        internal_symbol: str,
        internal_timeframe: str,
        default_is_final: bool = False,
    ) -> list[Bar]:
        bars = [
            self._normalize_record(record, internal_symbol, internal_timeframe, default_is_final=default_is_final)
            for record in records
        ]
        bars = sorted(bars, key=lambda bar: bar.end_ts)
        self._validate_bar_sequence(bars)
        return bars

    def normalize_quote_response(
        self,
        payload: dict[str, Any],
        internal_symbols: Sequence[str],
    ) -> list[SchwabQuoteResult]:
        results: list[SchwabQuoteResult] = []
        for internal_symbol in internal_symbols:
            external_symbol = self.map_quote_symbol(internal_symbol)
            raw_payload = payload.get(external_symbol)
            if not isinstance(raw_payload, dict):
                raise ValueError(f"Schwab /quotes response did not include payload for {external_symbol!r}.")
            results.append(
                SchwabQuoteResult(
                    internal_symbol=internal_symbol,
                    external_symbol=external_symbol,
                    quote_future=_extract_dict(raw_payload, "quote"),
                    reference_future=_extract_dict(raw_payload, "reference"),
                    raw_payload=raw_payload,
                )
            )
        return results

    def _normalize_record(
        self,
        record: dict[str, Any],
        internal_symbol: str,
        internal_timeframe: str,
        default_is_final: bool,
    ) -> Bar:
        field_map = self._config.field_map
        _require_fields(
            record,
            (
                field_map.timestamp_field,
                field_map.open_field,
                field_map.high_field,
                field_map.low_field,
                field_map.close_field,
                field_map.volume_field,
            ),
        )
        timestamp = self._parse_timestamp(record[field_map.timestamp_field])
        duration = _timeframe_duration(internal_timeframe)
        if field_map.timestamp_semantics is TimestampSemantics.END:
            end_ts = timestamp
            start_ts = end_ts - duration
        else:
            start_ts = timestamp
            end_ts = start_ts + duration

        is_final = default_is_final
        if field_map.is_final_field is not None and field_map.is_final_field in record:
            is_final = _coerce_bool(record[field_map.is_final_field])

        bar = Bar(
            bar_id=build_bar_id(internal_symbol, internal_timeframe, end_ts),
            symbol=internal_symbol,
            timeframe=internal_timeframe,
            start_ts=start_ts,
            end_ts=end_ts,
            open=Decimal(str(record[field_map.open_field])),
            high=Decimal(str(record[field_map.high_field])),
            low=Decimal(str(record[field_map.low_field])),
            close=Decimal(str(record[field_map.close_field])),
            volume=int(record[field_map.volume_field]),
            is_final=is_final,
            session_asia=False,
            session_london=False,
            session_us=False,
            session_allowed=False,
        )
        return self._bar_builder.normalize(bar)

    def _parse_timestamp(self, raw_timestamp: Any) -> datetime:
        if isinstance(raw_timestamp, datetime):
            timestamp = raw_timestamp
        elif isinstance(raw_timestamp, (int, float)):
            timestamp = datetime.fromtimestamp(float(raw_timestamp) / 1000.0, tz=self._settings.timezone_info)
        else:
            text = str(raw_timestamp)
            if text.lstrip("-").isdigit():
                timestamp = datetime.fromtimestamp(int(text) / 1000.0, tz=self._settings.timezone_info)
            else:
                timestamp = datetime.fromisoformat(text.replace("Z", "+00:00"))

        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            return timestamp.replace(tzinfo=self._settings.timezone_info)
        return timestamp.astimezone(self._settings.timezone_info)

    @staticmethod
    def _validate_bar_sequence(bars: Iterable[Bar]) -> None:
        previous_end_ts: Optional[datetime] = None
        for bar in bars:
            if previous_end_ts is not None and bar.end_ts <= previous_end_ts:
                raise ValueError("Normalized Schwab bars must be strictly increasing by end timestamp.")
            previous_end_ts = bar.end_ts


def _timeframe_duration(internal_timeframe: str) -> timedelta:
    if internal_timeframe != "5m":
        raise ValueError(f"Unsupported internal timeframe for this build: {internal_timeframe}")
    return timedelta(minutes=5)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return bool(value)


def _extract_dict(payload: dict[str, Any], key: str) -> Optional[dict[str, Any]]:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError(f"Expected Schwab quote field {key!r} to be an object.")
    return value


def _require_fields(record: dict[str, Any], required_fields: Sequence[str]) -> None:
    missing = [field for field in required_fields if field not in record]
    if missing:
        raise ValueError(f"Schwab payload record is missing required fields: {', '.join(missing)}")
