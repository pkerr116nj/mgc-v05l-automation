"""Research-only minute bar resampling from persisted base data."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

from ..domain.models import Bar
from ..market_data.bar_builder import BarBuilder
from ..market_data.bar_models import build_bar_id
from ..market_data.timeframes import normalize_timeframe_label, timeframe_minutes


@dataclass(frozen=True)
class ResampledBars:
    bars: list[Bar]
    skipped_bucket_count: int


def build_resampled_bars(
    source_bars: Sequence[Bar],
    *,
    target_timeframe: str,
    bar_builder: BarBuilder,
) -> ResampledBars:
    if not source_bars:
        return ResampledBars(bars=[], skipped_bucket_count=0)

    canonical_source_timeframe = normalize_timeframe_label(source_bars[0].timeframe)
    canonical_target_timeframe = normalize_timeframe_label(target_timeframe)
    source_minutes = timeframe_minutes(canonical_source_timeframe)
    target_minutes = timeframe_minutes(canonical_target_timeframe)
    if target_minutes <= source_minutes or target_minutes % source_minutes != 0:
        raise ValueError("target_timeframe must be a larger whole-minute multiple of the source timeframe.")

    ratio = target_minutes // source_minutes
    buckets: dict[int, list[Bar]] = {}
    for bar in source_bars:
        if normalize_timeframe_label(bar.timeframe) != canonical_source_timeframe:
            raise ValueError("All source bars must share the same timeframe.")
        bucket_key = _bucket_key(bar.end_ts, target_minutes)
        buckets.setdefault(bucket_key, []).append(bar)

    resampled: list[Bar] = []
    skipped_bucket_count = 0
    for key in sorted(buckets):
        bucket_bars = sorted(buckets[key], key=lambda bar: bar.end_ts)
        if not _is_complete_bucket(bucket_bars, ratio=ratio, source_minutes=source_minutes):
            skipped_bucket_count += 1
            continue
        first = bucket_bars[0]
        last = bucket_bars[-1]
        resampled.append(
            bar_builder.normalize(
                Bar(
                    bar_id=build_bar_id(first.symbol, canonical_target_timeframe, last.end_ts),
                    symbol=first.symbol,
                    timeframe=canonical_target_timeframe,
                    start_ts=first.start_ts,
                    end_ts=last.end_ts,
                    open=first.open,
                    high=max(bar.high for bar in bucket_bars),
                    low=min(bar.low for bar in bucket_bars),
                    close=last.close,
                    volume=sum(bar.volume for bar in bucket_bars),
                    is_final=all(bar.is_final for bar in bucket_bars),
                    session_asia=first.session_asia,
                    session_london=first.session_london,
                    session_us=first.session_us,
                    session_allowed=first.session_allowed,
                )
            )
        )

    return ResampledBars(bars=resampled, skipped_bucket_count=skipped_bucket_count)


def write_resampled_bars_csv(bars: Sequence[Bar], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "bar_id",
                "symbol",
                "timeframe",
                "start_ts",
                "end_ts",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "is_final",
                "session_asia",
                "session_london",
                "session_us",
                "session_allowed",
            ],
        )
        writer.writeheader()
        for bar in bars:
            writer.writerow(
                {
                    "bar_id": bar.bar_id,
                    "symbol": bar.symbol,
                    "timeframe": bar.timeframe,
                    "start_ts": bar.start_ts.isoformat(),
                    "end_ts": bar.end_ts.isoformat(),
                    "open": str(bar.open),
                    "high": str(bar.high),
                    "low": str(bar.low),
                    "close": str(bar.close),
                    "volume": bar.volume,
                    "is_final": bar.is_final,
                    "session_asia": bar.session_asia,
                    "session_london": bar.session_london,
                    "session_us": bar.session_us,
                    "session_allowed": bar.session_allowed,
                }
            )
    return path

def _bucket_key(end_ts: datetime, target_minutes: int) -> int:
    epoch_minutes = int(end_ts.astimezone(timezone.utc).timestamp() // 60)
    return (epoch_minutes - 1) // target_minutes


def _is_complete_bucket(bucket_bars: Sequence[Bar], *, ratio: int, source_minutes: int) -> bool:
    if len(bucket_bars) != ratio:
        return False
    expected_gap = timedelta(minutes=source_minutes)
    prior_end_ts: datetime | None = None
    for bar in bucket_bars:
        if prior_end_ts is not None and bar.end_ts - prior_end_ts != expected_gap:
            return False
        prior_end_ts = bar.end_ts
    return True
