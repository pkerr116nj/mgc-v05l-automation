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


class ReplayFeed:
    """Loads CSV bars and emits them in timestamp order for replay mode."""

    def __init__(self, settings: StrategySettings) -> None:
        self._settings = settings

    def load_csv(self, csv_path: str | Path) -> list[Bar]:
        path = Path(csv_path)
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != list(REPLAY_DATA_COLUMNS):
                raise ValueError(
                    "Replay CSV columns must exactly match: timestamp,open,high,low,close,volume"
                )
            bars = [self._row_to_bar(row) for row in reader]
        bars.sort(key=lambda bar: bar.end_ts)
        return bars

    def iter_csv(self, csv_path: str | Path) -> Iterable[Bar]:
        for bar in self.load_csv(csv_path):
            yield bar

    def _row_to_bar(self, row: dict[str, str]) -> Bar:
        end_ts = self._parse_timestamp(row["timestamp"])
        start_ts = end_ts - timedelta(minutes=5)
        return Bar(
            bar_id=build_bar_id(self._settings.symbol, self._settings.timeframe, end_ts),
            symbol=self._settings.symbol,
            timeframe=self._settings.timeframe,
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
