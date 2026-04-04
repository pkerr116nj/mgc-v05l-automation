"""SQLite-backed historical bar loading for playback runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

from sqlalchemy import distinct, select
from sqlalchemy.engine import Engine

from ..config_models import StrategySettings
from ..domain.models import Bar
from ..persistence import build_engine
from ..persistence.tables import bars_table
from ..research.bar_resampling import build_resampled_bars
from .bar_builder import BarBuilder
from .provider_config import load_market_data_providers_config
from .provider_models import MarketDataUseCase
from .timeframes import normalize_timeframe_label


@dataclass(frozen=True)
class HistoricalPlaybackBars:
    symbol: str
    source_timeframe: str
    target_timeframe: str
    data_source: str
    source_bar_count: int
    source_bars: list[Bar]
    playback_bars: list[Bar]
    skipped_incomplete_buckets: int


class SQLiteHistoricalBarSource:
    """Loads persisted bars from SQLite for deterministic playback."""

    def __init__(
        self,
        database_path: str | Path,
        settings: StrategySettings,
        *,
        engine: Engine | None = None,
        bar_builder: BarBuilder | None = None,
    ) -> None:
        self._database_path = Path(database_path)
        self._engine = engine or build_engine(f"sqlite:///{self._database_path}")
        self._settings = settings
        self._bar_builder = bar_builder or BarBuilder(settings)

    def load_bars(
        self,
        *,
        symbol: str,
        source_timeframe: str,
        target_timeframe: str,
        start_timestamp: datetime | None = None,
        end_timestamp: datetime | None = None,
        data_source: str | None = None,
    ) -> HistoricalPlaybackBars:
        canonical_source = normalize_timeframe_label(source_timeframe)
        canonical_target = normalize_timeframe_label(target_timeframe)
        selected_data_source = self._resolve_data_source(
            symbol=symbol,
            timeframe=canonical_source,
            explicit_data_source=data_source,
        )
        source_bars = self._load_source_bars(
            symbol=symbol,
            timeframe=canonical_source,
            data_source=selected_data_source,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )

        if canonical_source == canonical_target:
            return HistoricalPlaybackBars(
                symbol=symbol,
                source_timeframe=canonical_source,
                target_timeframe=canonical_target,
                data_source=selected_data_source,
                source_bar_count=len(source_bars),
                source_bars=source_bars,
                playback_bars=source_bars,
                skipped_incomplete_buckets=0,
            )

        source_minutes = _timeframe_minutes_or_none(canonical_source)
        target_minutes = _timeframe_minutes_or_none(canonical_target)
        if (
            canonical_source == "1m"
            and source_minutes is not None
            and target_minutes is not None
            and target_minutes > source_minutes
            and target_minutes % source_minutes == 0
        ):
            resampled = build_resampled_bars(
                source_bars,
                target_timeframe=canonical_target,
                bar_builder=self._bar_builder,
            )
            return HistoricalPlaybackBars(
                symbol=symbol,
                source_timeframe=canonical_source,
                target_timeframe=canonical_target,
                data_source=selected_data_source,
                source_bar_count=len(source_bars),
                source_bars=source_bars,
                playback_bars=resampled.bars,
                skipped_incomplete_buckets=resampled.skipped_bucket_count,
            )

        raise ValueError(
            "Historical playback supports direct streaming when source_timeframe == target_timeframe "
            "or finalized 1m -> larger whole-minute aggregation."
        )

    def _resolve_data_source(
        self,
        *,
        symbol: str,
        timeframe: str,
        explicit_data_source: str | None,
    ) -> str:
        if explicit_data_source is not None:
            return explicit_data_source

        with self._engine.begin() as connection:
            rows = connection.execute(
                select(distinct(bars_table.c.data_source))
                .where(
                    bars_table.c.ticker == symbol,
                    bars_table.c.timeframe == timeframe,
                    bars_table.c.is_final.is_(True),
                )
                .order_by(bars_table.c.data_source.asc())
            ).all()

        available = [str(row[0]) for row in rows]
        if not available:
            raise ValueError(f"No finalized persisted bars found for {symbol} {timeframe}.")

        preferred: list[str]
        providers_config = load_market_data_providers_config()
        preferred = list(providers_config.preferred_data_sources(MarketDataUseCase.HISTORICAL_RESEARCH, timeframe))

        for candidate in preferred:
            if candidate in available:
                return candidate

        if len(available) == 1:
            return available[0]

        raise ValueError(
            f"Multiple persisted data sources found for {symbol} {timeframe}: {available}. "
            "Pass an explicit data_source to historical playback."
        )

    def _load_source_bars(
        self,
        *,
        symbol: str,
        timeframe: str,
        data_source: str,
        start_timestamp: datetime | None,
        end_timestamp: datetime | None,
    ) -> list[Bar]:
        statement = (
            select(bars_table)
            .where(
                bars_table.c.ticker == symbol,
                bars_table.c.timeframe == timeframe,
                bars_table.c.data_source == data_source,
                bars_table.c.is_final.is_(True),
            )
            .order_by(bars_table.c.timestamp.asc(), bars_table.c.bar_id.asc())
        )
        if start_timestamp is not None:
            statement = statement.where(bars_table.c.end_ts >= start_timestamp.isoformat())
        if end_timestamp is not None:
            statement = statement.where(bars_table.c.end_ts <= end_timestamp.isoformat())

        with self._engine.begin() as connection:
            rows = connection.execute(statement).mappings().all()

        loaded = [self._row_to_bar(row) for row in rows]
        return validate_playback_bars(loaded)

    def _row_to_bar(self, row: dict[str, object]) -> Bar:
        return self._bar_builder.require_finalized(
            Bar(
                bar_id=str(row["bar_id"]),
                symbol=str(row["symbol"]),
                timeframe=str(row["timeframe"]),
                start_ts=datetime.fromisoformat(str(row["start_ts"])),
                end_ts=datetime.fromisoformat(str(row["end_ts"])),
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                volume=int(row["volume"]),
                is_final=bool(row["is_final"]),
                session_asia=bool(row["session_asia"]),
                session_london=bool(row["session_london"]),
                session_us=bool(row["session_us"]),
                session_allowed=bool(row["session_allowed"]),
            )
        )


def validate_playback_bars(bars: list[Bar]) -> list[Bar]:
    """Return bars with duplicate bar IDs removed and strict timestamp order enforced."""
    ordered: list[Bar] = []
    seen_bar_ids: set[str] = set()
    latest_end_ts: Optional[datetime] = None

    for bar in bars:
        if bar.bar_id in seen_bar_ids:
            continue
        if latest_end_ts is not None and bar.end_ts < latest_end_ts:
            raise ValueError(f"Out-of-order historical playback bar rejected: {bar.bar_id}")
        if latest_end_ts is not None and bar.end_ts == latest_end_ts and ordered and bar.bar_id != ordered[-1].bar_id:
            raise ValueError(f"Duplicate timestamp with different bar identity rejected: {bar.end_ts.isoformat()}")
        ordered.append(bar)
        seen_bar_ids.add(bar.bar_id)
        latest_end_ts = bar.end_ts

    return ordered


def _timeframe_minutes_or_none(timeframe: str) -> int | None:
    try:
        return int(normalize_timeframe_label(timeframe).removesuffix("m"))
    except ValueError:
        return None
