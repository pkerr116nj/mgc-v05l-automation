"""Live trade aggregation into canonical 1m OHLCV bars."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterable

from ..config_models import StrategySettings
from ..domain.models import Bar
from .bar_builder import BarBuilder
from .bar_models import build_bar_id
from .canonical_maintenance import CanonicalMarketDataMaintenanceService
from .provider_models import TradePrint
from .timeframes import normalize_timeframe_label, timeframe_minutes


@dataclass(frozen=True)
class LiveTradeCaptureResult:
    finalized_bars: tuple[Bar, ...]
    persisted_bar_count: int
    derived_timeframes: tuple[str, ...]


@dataclass
class _OpenMinuteBucket:
    symbol: str
    start_ts: datetime
    end_ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int

    def apply_trade(self, trade: TradePrint) -> None:
        self.high = max(self.high, trade.price)
        self.low = min(self.low, trade.price)
        self.close = trade.price
        self.volume += trade.size


class LiveTradeOhlcvAggregator:
    """Aggregate authoritative live trade prints into finalized 1m bars."""

    def __init__(self, settings: StrategySettings) -> None:
        self._settings = settings
        self._bar_builder = BarBuilder(settings)
        self._open_buckets: dict[str, _OpenMinuteBucket] = {}
        self._last_finalized_end_by_symbol: dict[str, datetime] = {}

    def ingest_trade(self, trade: TradePrint) -> list[Bar]:
        localized_ts = self._localize(trade.occurred_at)
        if trade.size <= 0:
            raise ValueError("TradePrint.size must be > 0 for OHLCV aggregation.")

        symbol = trade.internal_symbol
        finalized: list[Bar] = []
        bucket = self._open_buckets.get(symbol)
        trade_bucket_start = localized_ts.replace(second=0, microsecond=0)
        trade_bucket_end = trade_bucket_start + timedelta(minutes=1)

        last_finalized_end = self._last_finalized_end_by_symbol.get(symbol)
        if last_finalized_end is not None and trade_bucket_end <= last_finalized_end:
            raise ValueError(
                f"Out-of-order trade for {symbol}: {trade_bucket_end.isoformat()} "
                f"arrived after {last_finalized_end.isoformat()} was already finalized."
            )

        if bucket is not None and trade_bucket_start < bucket.start_ts:
            raise ValueError(
                f"Out-of-order trade for {symbol}: {trade_bucket_start.isoformat()} "
                f"arrived before the active bucket {bucket.start_ts.isoformat()}."
            )

        if bucket is None:
            self._open_buckets[symbol] = self._new_bucket(trade, trade_bucket_start)
            return []

        if trade_bucket_start == bucket.start_ts:
            bucket.apply_trade(trade)
            return []

        finalized.append(self._finalize_bucket(bucket))
        self._last_finalized_end_by_symbol[symbol] = bucket.end_ts
        self._open_buckets[symbol] = self._new_bucket(trade, trade_bucket_start)
        return finalized

    def flush_completed(self, now: datetime) -> list[Bar]:
        watermark = self._localize(now).replace(second=0, microsecond=0)
        finalized: list[Bar] = []
        completed_symbols = [
            symbol
            for symbol, bucket in self._open_buckets.items()
            if bucket.end_ts <= watermark
        ]
        for symbol in completed_symbols:
            bucket = self._open_buckets.pop(symbol)
            finalized.append(self._finalize_bucket(bucket))
            self._last_finalized_end_by_symbol[symbol] = bucket.end_ts
        finalized.sort(key=lambda bar: (bar.end_ts, bar.symbol))
        return finalized

    def flush_all(self) -> list[Bar]:
        finalized = [self._finalize_bucket(bucket) for bucket in self._open_buckets.values()]
        for bucket in self._open_buckets.values():
            self._last_finalized_end_by_symbol[bucket.symbol] = bucket.end_ts
        self._open_buckets.clear()
        finalized.sort(key=lambda bar: (bar.end_ts, bar.symbol))
        return finalized

    def _new_bucket(self, trade: TradePrint, start_ts: datetime) -> _OpenMinuteBucket:
        return _OpenMinuteBucket(
            symbol=trade.internal_symbol,
            start_ts=start_ts,
            end_ts=start_ts + timedelta(minutes=1),
            open=trade.price,
            high=trade.price,
            low=trade.price,
            close=trade.price,
            volume=trade.size,
        )

    def _finalize_bucket(self, bucket: _OpenMinuteBucket) -> Bar:
        return self._bar_builder.require_finalized(
            self._bar_builder.normalize(
                Bar(
                    bar_id=build_bar_id(bucket.symbol, "1m", bucket.end_ts),
                    symbol=bucket.symbol,
                    timeframe="1m",
                    start_ts=bucket.start_ts,
                    end_ts=bucket.end_ts,
                    open=bucket.open,
                    high=bucket.high,
                    low=bucket.low,
                    close=bucket.close,
                    volume=bucket.volume,
                    is_final=True,
                    session_asia=False,
                    session_london=False,
                    session_us=False,
                    session_allowed=False,
                )
            )
        )

    def _localize(self, timestamp: datetime) -> datetime:
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("Live trade timestamps must be timezone-aware.")
        return timestamp.astimezone(self._settings.timezone_info)


class CanonicalLiveTradeCaptureService:
    """Persist finalized live-trade minute bars into the canonical research base."""

    def __init__(
        self,
        settings: StrategySettings,
        *,
        canonical_maintenance: CanonicalMarketDataMaintenanceService,
        raw_data_source: str = "live_trade_capture",
        provider: str = "live_trade_capture",
        provenance_tag: str = "live_trade_capture",
        dataset: str | None = "live_trade_stream",
        schema_name: str | None = "trade_print",
        derive_timeframes: tuple[str, ...] = (),
    ) -> None:
        self._settings = settings
        self._aggregator = LiveTradeOhlcvAggregator(settings)
        self._canonical_maintenance = canonical_maintenance
        self._raw_data_source = raw_data_source
        self._provider = provider
        self._provenance_tag = provenance_tag
        self._dataset = dataset
        self._schema_name = schema_name
        self._derive_timeframes = tuple(
            sorted(
                {normalize_timeframe_label(timeframe) for timeframe in derive_timeframes},
                key=timeframe_minutes,
            )
        )

    def capture_trade(self, trade: TradePrint) -> LiveTradeCaptureResult:
        finalized = self._aggregator.ingest_trade(trade)
        return self._persist_and_derive(finalized)

    def capture_trades(self, trades: Iterable[TradePrint]) -> LiveTradeCaptureResult:
        finalized: list[Bar] = []
        for trade in trades:
            finalized.extend(self._aggregator.ingest_trade(trade))
        return self._persist_and_derive(finalized)

    def flush_completed(self, now: datetime) -> LiveTradeCaptureResult:
        finalized = self._aggregator.flush_completed(now)
        return self._persist_and_derive(finalized)

    def flush_all(self) -> LiveTradeCaptureResult:
        finalized = self._aggregator.flush_all()
        return self._persist_and_derive(finalized)

    def _persist_and_derive(self, bars: list[Bar]) -> LiveTradeCaptureResult:
        if not bars:
            return LiveTradeCaptureResult(finalized_bars=(), persisted_bar_count=0, derived_timeframes=self._derive_timeframes)

        persisted_count = 0
        bars_by_symbol: dict[str, list[Bar]] = defaultdict(list)
        for bar in bars:
            bars_by_symbol[bar.symbol].append(bar)

        for symbol, symbol_bars in bars_by_symbol.items():
            ordered_bars = sorted(symbol_bars, key=lambda item: item.end_ts)
            audit = self._canonical_maintenance.persist_completed_1m_bars(
                bars=ordered_bars,
                raw_data_source=self._raw_data_source,
                provider=self._provider,
                provenance_tag=self._provenance_tag,
                dataset=self._dataset,
                schema_name=self._schema_name,
                request_symbol=symbol,
                provider_metadata={"capture_mode": "live_trade_aggregation"},
            )
            persisted_count += len(ordered_bars)
            if audit is None:
                continue
            earliest_end = ordered_bars[0].end_ts
            latest_end = ordered_bars[-1].end_ts
            for timeframe in self._derive_timeframes:
                lookback_minutes = max(1, timeframe_minutes(timeframe) - 1)
                self._canonical_maintenance.derive_timeframe(
                    symbol=symbol,
                    target_timeframe=timeframe,
                    start=earliest_end - timedelta(minutes=lookback_minutes),
                    end=latest_end,
                )

        ordered = tuple(sorted(bars, key=lambda item: (item.end_ts, item.symbol)))
        return LiveTradeCaptureResult(
            finalized_bars=ordered,
            persisted_bar_count=persisted_count,
            derived_timeframes=self._derive_timeframes,
        )
