"""Runnable live-trade capture engine for canonical 1m OHLCV persistence."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from ..config_models import StrategySettings
from ..market_data import CanonicalLiveTradeCaptureService, CanonicalMarketDataMaintenanceService, MarketDataProvider
from ..market_data.provider_models import TradePrint


@dataclass(frozen=True)
class LiveTradeCaptureSummary:
    mode: str
    provider: str | None
    input_jsonl: str | None
    symbols: tuple[str, ...]
    trade_count: int
    finalized_bar_count: int
    per_symbol_trade_count: dict[str, int]
    per_symbol_finalized_bars: dict[str, int]
    derived_timeframes: tuple[str, ...]
    raw_data_source: str
    started_at: str
    completed_at: str


def run_live_trade_capture(
    *,
    settings: StrategySettings,
    symbols: list[str] | tuple[str, ...],
    provider: MarketDataProvider | None = None,
    input_jsonl: str | Path | None = None,
    derive_timeframes: tuple[str, ...] = ("5m", "10m"),
    max_events: int | None = None,
    raw_data_source: str = "live_trade_capture",
    provider_label: str | None = None,
) -> LiveTradeCaptureSummary:
    normalized_symbols = tuple(sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}))
    if not normalized_symbols:
        raise ValueError("At least one symbol is required for live trade capture.")
    if input_jsonl is None and provider is None:
        raise ValueError("A live-trade provider or --input-jsonl source is required.")

    maintenance = CanonicalMarketDataMaintenanceService(database_url=settings.database_url)
    capture = CanonicalLiveTradeCaptureService(
        settings,
        canonical_maintenance=maintenance,
        raw_data_source=raw_data_source,
        provider=provider_label or (getattr(provider, "provider_id", None) or "live_trade_capture"),
        provenance_tag=raw_data_source,
        dataset="live_trade_stream",
        schema_name="trade_print",
        derive_timeframes=derive_timeframes,
    )

    allowed_symbols = set(normalized_symbols)
    trade_count = 0
    finalized_bar_count = 0
    per_symbol_trade_count = {symbol: 0 for symbol in normalized_symbols}
    per_symbol_finalized_bars = {symbol: 0 for symbol in normalized_symbols}
    started_at = datetime.now(settings.timezone_info)

    if input_jsonl is not None:
        stream = _iter_jsonl_trade_prints(Path(input_jsonl), settings=settings, allowed_symbols=allowed_symbols)
        mode = "jsonl_replay"
    else:
        assert provider is not None
        stream = provider.subscribe_live_trades(normalized_symbols)
        mode = "provider_stream"

    for trade in stream:
        normalized_trade = _normalize_trade_print(trade, settings=settings)
        if normalized_trade.internal_symbol not in allowed_symbols:
            continue
        capture_result = capture.capture_trade(normalized_trade)
        _record_bars(capture_result.finalized_bars, per_symbol_finalized_bars)
        finalized_bar_count += len(capture_result.finalized_bars)
        trade_count += 1
        per_symbol_trade_count[normalized_trade.internal_symbol] += 1

        watermark_result = capture.flush_completed(normalized_trade.occurred_at)
        _record_bars(watermark_result.finalized_bars, per_symbol_finalized_bars)
        finalized_bar_count += len(watermark_result.finalized_bars)

        if max_events is not None and trade_count >= max_events:
            break

    final_result = capture.flush_all()
    _record_bars(final_result.finalized_bars, per_symbol_finalized_bars)
    finalized_bar_count += len(final_result.finalized_bars)
    completed_at = datetime.now(settings.timezone_info)

    return LiveTradeCaptureSummary(
        mode=mode,
        provider=provider_label or getattr(provider, "provider_id", None),
        input_jsonl=str(Path(input_jsonl).resolve(strict=False)) if input_jsonl is not None else None,
        symbols=normalized_symbols,
        trade_count=trade_count,
        finalized_bar_count=finalized_bar_count,
        per_symbol_trade_count=per_symbol_trade_count,
        per_symbol_finalized_bars=per_symbol_finalized_bars,
        derived_timeframes=tuple(final_result.derived_timeframes),
        raw_data_source=raw_data_source,
        started_at=started_at.isoformat(),
        completed_at=completed_at.isoformat(),
    )


def _iter_jsonl_trade_prints(
    path: Path,
    *,
    settings: StrategySettings,
    allowed_symbols: set[str],
) -> Iterable[TradePrint]:
    if not path.exists():
        raise FileNotFoundError(f"Trade JSONL input does not exist: {path}")
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            symbol = str(payload.get("internal_symbol") or payload.get("symbol") or "").strip().upper()
            if not symbol or symbol not in allowed_symbols:
                continue
            timestamp_raw = payload.get("occurred_at", payload.get("timestamp"))
            if timestamp_raw in (None, ""):
                raise ValueError(f"Trade JSONL line {line_number} is missing occurred_at/timestamp.")
            trade = TradePrint(
                internal_symbol=symbol,
                price=_as_decimal(payload.get("price")),
                size=int(payload.get("size", payload.get("volume", 0))),
                occurred_at=_parse_timestamp(str(timestamp_raw), settings=settings),
                provider=str(payload.get("provider") or "jsonl_replay"),
                external_symbol=(
                    str(payload.get("external_symbol")).strip()
                    if payload.get("external_symbol") not in (None, "")
                    else None
                ),
                raw_payload=dict(payload),
            )
            yield trade


def _normalize_trade_print(trade: TradePrint, *, settings: StrategySettings) -> TradePrint:
    symbol = str(trade.internal_symbol).strip().upper()
    if not symbol:
        raise ValueError("TradePrint.internal_symbol is required.")
    return TradePrint(
        internal_symbol=symbol,
        price=_as_decimal(trade.price),
        size=int(trade.size),
        occurred_at=_parse_timestamp(trade.occurred_at, settings=settings),
        provider=str(trade.provider or ""),
        external_symbol=(str(trade.external_symbol).strip() if trade.external_symbol else None),
        raw_payload=dict(trade.raw_payload),
    )


def _record_bars(bars, per_symbol_finalized_bars: dict[str, int]) -> None:
    for bar in bars:
        per_symbol_finalized_bars.setdefault(bar.symbol, 0)
        per_symbol_finalized_bars[bar.symbol] += 1


def _parse_timestamp(raw_value: datetime | str, *, settings: StrategySettings) -> datetime:
    if isinstance(raw_value, datetime):
        parsed = raw_value
    else:
        parsed = datetime.fromisoformat(str(raw_value))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=settings.timezone_info)
    return parsed.astimezone(settings.timezone_info)


def _as_decimal(value) -> object:
    if value in (None, ""):
        raise ValueError("Trade price is required.")
    from decimal import Decimal

    return Decimal(str(value))
