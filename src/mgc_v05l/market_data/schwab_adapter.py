"""Schwab market-data adapter boundary.

This module intentionally avoids guessing undocumented Schwab endpoint details.
It only covers symbol/timeframe mapping plus raw-record normalization into the
shared internal Bar model.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Sequence

from ..config_models import StrategySettings
from ..domain.models import Bar
from .bar_builder import BarBuilder
from .bar_models import build_bar_id
from .schwab_models import SchwabMarketDataConfig, TimestampSemantics


class SchwabMarketDataAdapter:
    """Maps Schwab-facing symbols/timeframes and normalizes raw records into Bar objects."""

    def __init__(self, settings: StrategySettings, config: SchwabMarketDataConfig) -> None:
        self._settings = settings
        self._config = config
        self._bar_builder = BarBuilder(settings)

    def map_symbol(self, internal_symbol: str) -> str:
        try:
            return self._config.symbol_map[internal_symbol]
        except KeyError as exc:
            raise ValueError(f"No Schwab symbol mapping configured for {internal_symbol!r}.") from exc

    def map_timeframe(self, internal_timeframe: str) -> str:
        try:
            return self._config.timeframe_map[internal_timeframe]
        except KeyError as exc:
            raise ValueError(f"No Schwab timeframe mapping configured for {internal_timeframe!r}.") from exc

    def normalize_historical_records(
        self,
        records: Sequence[dict[str, Any]],
        internal_symbol: str,
        internal_timeframe: str,
    ) -> list[Bar]:
        return sorted(
            [
                self._bar_builder.require_finalized(
                    self._normalize_record(record, internal_symbol, internal_timeframe, default_is_final=True)
                )
                for record in records
            ],
            key=lambda bar: bar.end_ts,
        )

    def normalize_live_records(
        self,
        records: Sequence[dict[str, Any]],
        internal_symbol: str,
        internal_timeframe: str,
        default_is_final: bool = False,
    ) -> list[Bar]:
        return sorted(
            [
                self._normalize_record(record, internal_symbol, internal_timeframe, default_is_final=default_is_final)
                for record in records
            ],
            key=lambda bar: bar.end_ts,
        )

    def _normalize_record(
        self,
        record: dict[str, Any],
        internal_symbol: str,
        internal_timeframe: str,
        default_is_final: bool,
    ) -> Bar:
        field_map = self._config.field_map
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
        else:
            timestamp = datetime.fromisoformat(str(raw_timestamp).replace("Z", "+00:00"))

        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            return timestamp.replace(tzinfo=self._settings.timezone_info)
        return timestamp.astimezone(self._settings.timezone_info)


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
