"""Replay CSV ingestion for deterministic bar playback."""

from __future__ import annotations

import csv
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from ..config_models import StrategySettings
from ..domain.models import Bar
from .bar_models import build_bar_id

REPLAY_DATA_COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")
REPLAY_DATA_COLUMNS_WITH_SYMBOL = ("symbol", "timestamp", "open", "high", "low", "close", "volume")
REPLAY_DATA_COLUMNS_WITH_SYMBOL_AND_TIMEFRAME = (
    "symbol",
    "timeframe",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
)


class ReplayFeed:
    """Loads CSV bars and emits them in timestamp order for replay mode."""

    def __init__(self, settings: StrategySettings) -> None:
        self._settings = settings

    def load_csv(self, csv_path: str | Path) -> list[Bar]:
        path = Path(csv_path)
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = tuple(reader.fieldnames or ())
            if fieldnames not in (
                REPLAY_DATA_COLUMNS,
                REPLAY_DATA_COLUMNS_WITH_SYMBOL,
                REPLAY_DATA_COLUMNS_WITH_SYMBOL_AND_TIMEFRAME,
            ):
                raise ValueError(
                    "Replay CSV columns must exactly match one of: "
                    "timestamp,open,high,low,close,volume | "
                    "symbol,timestamp,open,high,low,close,volume | "
                    "symbol,timeframe,timestamp,open,high,low,close,volume"
                )
            bars = [self._row_to_bar(row) for row in reader]
        bars.sort(key=lambda bar: (bar.end_ts, bar.symbol, bar.timeframe, bar.bar_id))
        return bars

    def iter_csv(self, csv_path: str | Path) -> Iterable[Bar]:
        for bar in self.load_csv(csv_path):
            yield bar

    def _row_to_bar(self, row: dict[str, str]) -> Bar:
        symbol = str(row.get("symbol") or self._settings.symbol).strip().upper()
        timeframe = str(row.get("timeframe") or self._settings.timeframe).strip()
        end_ts = self._parse_timestamp(row["timestamp"])
        minutes = int(timeframe.removesuffix("m"))
        start_ts = end_ts - timedelta(minutes=minutes)
        return Bar(
            bar_id=build_bar_id(symbol, timeframe, end_ts),
            symbol=symbol,
            timeframe=timeframe,
            start_ts=start_ts,
            end_ts=end_ts,
            open=Decimal(row["open"]),
            high=Decimal(row["high"]),
            low=Decimal(row["low"]),
            close=Decimal(row["close"]),
            volume=int(row["volume"]),
            is_final=True,
            session_asia=False,
            session_london=False,
            session_us=False,
            session_allowed=False,
        )

    def _parse_timestamp(self, raw_value: str) -> datetime:
        normalized = raw_value.replace("Z", "+00:00")
        timestamp = datetime.fromisoformat(normalized)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            return timestamp.replace(tzinfo=self._settings.timezone_info)
        return timestamp.astimezone(self._settings.timezone_info)
