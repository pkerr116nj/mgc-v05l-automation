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
from .timeframes import normalize_timeframe_label, timeframe_minutes


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
        internal_timeframe = normalize_timeframe_label(internal_timeframe)
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
        return self._config.timeframe_map.get(normalize_timeframe_label(internal_timeframe))

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
        bars = self._dedupe_exact_live_bars(bars)
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
            raw_payload = _resolve_quote_payload(payload, external_symbol)
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

        open_price = Decimal(str(record[field_map.open_field]))
        high_price = Decimal(str(record[field_map.high_field]))
        low_price = Decimal(str(record[field_map.low_field]))
        close_price = Decimal(str(record[field_map.close_field]))
        envelope_high = max(open_price, high_price, low_price, close_price)
        envelope_low = min(open_price, high_price, low_price, close_price)

        bar = Bar(
            bar_id=build_bar_id(internal_symbol, internal_timeframe, end_ts),
            symbol=internal_symbol,
            timeframe=internal_timeframe,
            start_ts=start_ts,
            end_ts=end_ts,
            open=open_price,
            high=envelope_high,
            low=envelope_low,
            close=close_price,
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

    @staticmethod
    def _dedupe_exact_live_bars(bars: Sequence[Bar]) -> list[Bar]:
        deduped: list[Bar] = []
        for bar in bars:
            if deduped and bar.bar_id == deduped[-1].bar_id:
                if bar == deduped[-1]:
                    continue
                raise ValueError("Normalized Schwab live bars contained conflicting duplicate end timestamps.")
            deduped.append(bar)
        return deduped


def _timeframe_duration(internal_timeframe: str) -> timedelta:
    return timedelta(minutes=timeframe_minutes(internal_timeframe))


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


def _resolve_quote_payload(payload: dict[str, Any], external_symbol: str) -> dict[str, Any] | None:
    if external_symbol in payload and isinstance(payload[external_symbol], dict):
        return payload[external_symbol]

    candidate_keys = _quote_symbol_aliases(external_symbol)
    for candidate in candidate_keys:
        if candidate in payload and isinstance(payload[candidate], dict):
            return payload[candidate]

    for key, value in payload.items():
        if not isinstance(value, dict):
            continue
        reference = value.get("reference")
        if not isinstance(reference, dict):
            reference = {}
        reference_symbol = reference.get("symbol")
        reference_product = reference.get("product")
        payload_symbol = value.get("symbol")
        if isinstance(reference_symbol, str) and reference_symbol in candidate_keys:
            return value
        if isinstance(reference_product, str) and reference_product in candidate_keys:
            return value
        if isinstance(payload_symbol, str) and payload_symbol in candidate_keys:
            return value
        if isinstance(key, str) and key in candidate_keys:
            return value
    return None


def _quote_symbol_aliases(external_symbol: str) -> set[str]:
    aliases = {external_symbol}
    stripped = external_symbol.lstrip("/")
    if stripped:
        aliases.add(stripped)
        aliases.add(f"/{stripped}")
    aliases.add(external_symbol.replace("/", ""))
    return {alias for alias in aliases if alias}


def _require_fields(record: dict[str, Any], required_fields: Sequence[str]) -> None:
    missing = [field for field in required_fields if field not in record]
    if missing:
        raise ValueError(f"Schwab payload record is missing required fields: {', '.join(missing)}")
