"""Pattern Engine v1 bar-context loader."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from ...app.session_phase_labels import label_session_phase


@dataclass(frozen=True)
class PatternEngineContext:
    bar_id: str
    timestamp: datetime
    symbol: str
    timeframe: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    session_phase: str
    atr: Decimal
    vol_ratio: Decimal
    vwap: Decimal | None
    turn_ema_fast: Decimal | None
    turn_ema_slow: Decimal | None
    normalized_slope: Decimal
    normalized_curvature: Decimal
    range_expansion_ratio: Decimal
    body_to_range: Decimal
    close_location: Decimal
    vwap_distance_atr: Decimal | None
    rolling_high_10: Decimal | None
    rolling_low_10: Decimal | None
    distance_from_high_10_atr: Decimal | None
    distance_from_low_10_atr: Decimal | None


def load_pattern_engine_contexts(*, replay_db_path: Path, ticker: str = "MGC", timeframe: str = "5m") -> list[PatternEngineContext]:
    connection = sqlite3.connect(replay_db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            select
              b.bar_id,
              b.ticker,
              b.timeframe,
              b.open,
              b.high,
              b.low,
              b.close,
              b.volume,
              b.end_ts,
              f.payload_json
            from bars b
            join features f on f.bar_id = b.bar_id
            where b.ticker = ? and b.timeframe = ?
            order by b.end_ts asc
            """,
            (ticker, timeframe),
        ).fetchall()
    finally:
        connection.close()

    contexts: list[PatternEngineContext] = []
    rolling_highs: list[Decimal] = []
    rolling_lows: list[Decimal] = []
    for row in rows:
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        open_ = Decimal(str(row["open"]))
        high = Decimal(str(row["high"]))
        low = Decimal(str(row["low"]))
        close = Decimal(str(row["close"]))
        atr = _payload_decimal(payload.get("atr")) or Decimal("0")
        rolling_highs.append(high)
        rolling_lows.append(low)
        recent_high = max(rolling_highs[-10:]) if rolling_highs else None
        recent_low = min(rolling_lows[-10:]) if rolling_lows else None
        vwap = _payload_decimal(payload.get("vwap"))
        contexts.append(
            PatternEngineContext(
                bar_id=row["bar_id"],
                timestamp=datetime.fromisoformat(row["end_ts"]),
                symbol=row["ticker"],
                timeframe=row["timeframe"],
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=int(row["volume"]),
                session_phase=label_session_phase(datetime.fromisoformat(row["end_ts"])),
                atr=atr,
                vol_ratio=_payload_decimal(payload.get("vol_ratio")) or Decimal("0"),
                vwap=vwap,
                turn_ema_fast=_payload_decimal(payload.get("turn_ema_fast")),
                turn_ema_slow=_payload_decimal(payload.get("turn_ema_slow")),
                normalized_slope=_safe_div(_payload_decimal(payload.get("velocity")) or Decimal("0"), atr) or Decimal("0"),
                normalized_curvature=_safe_div(_payload_decimal(payload.get("velocity_delta")) or Decimal("0"), atr) or Decimal("0"),
                range_expansion_ratio=_safe_div(high - low, atr) or Decimal("0"),
                body_to_range=_safe_div(abs(close - open_), high - low) or Decimal("0"),
                close_location=_safe_div(close - low, high - low) or Decimal("0"),
                vwap_distance_atr=_safe_div(close - vwap, atr) if vwap is not None else None,
                rolling_high_10=recent_high,
                rolling_low_10=recent_low,
                distance_from_high_10_atr=_safe_div(recent_high - close, atr) if recent_high is not None else None,
                distance_from_low_10_atr=_safe_div(close - recent_low, atr) if recent_low is not None else None,
            )
        )
    return contexts


def _payload_decimal(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, dict) and value.get("__type__") == "decimal":
        return Decimal(str(value.get("value")))
    return Decimal(str(value))


def _safe_div(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator
