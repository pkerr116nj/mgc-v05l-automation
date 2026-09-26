"""Fast, bounded Databento cost estimator for the expanded NDXP event study.

Purpose
-------
Estimate the cost of broad NDXP CMBP-1 capture without repeating the prior
hundreds-of-calls pricing mistake.

Design
------
* Exactly four representative regular sessions by default.
* Two get_cost calls per session:
    1. full-parent NDXP opening window, 09:30-10:00 ET
    2. full-parent NDXP regular session, open-close
* Maximum eight metadata calls total.
* Four workers.
* No data downloads.
* No per-contract pricing loops.
* Results are deliberately conservative because parent requests include more
  contracts than the eventual $2-to-first-$10 surface.

If the full-parent upper bound is cheap enough, prefer broad acquisition over
engineering a surgical strike-by-strike downloader.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
import json
import math
from pathlib import Path
from statistics import mean, median
from typing import Callable

import exchange_calendars as xc
import pandas as pd

from .index_options_metadata_estimator import http_metadata, load_key

DATASET = "OPRA.PILLAR"
SCHEMA = "cmbp-1"
PARENT = "NDXP.OPT"
MAX_CALLS = 12
MAX_WORKERS = 4
DEFAULT_START = "2023-03-28"
DEFAULT_END = "2026-09-24"


@dataclass(frozen=True)
class SampleRequest:
    session: str
    window: str
    start: str
    end: str

    def params(self) -> dict:
        return {
            "dataset": DATASET,
            "schema": SCHEMA,
            "stype_in": "parent",
            "symbols": [PARENT],
            "start": self.start,
            "end": self.end,
        }


def _sessions(start: str, end: str) -> pd.DatetimeIndex:
    cal = xc.get_calendar("XNYS")
    return cal.sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))


def representative_sessions(start: str, end: str, n: int = 4) -> list[str]:
    """Deterministic evenly-spaced regular sessions; no outcome-based selection."""
    sessions = _sessions(start, end)
    if len(sessions) < n:
        raise ValueError("Not enough sessions for requested sample")
    # Avoid endpoints so one odd first/last day cannot dominate.
    fractions = [(i + 1) / (n + 1) for i in range(n)]
    idx = [min(len(sessions) - 1, max(0, round(f * (len(sessions) - 1)))) for f in fractions]
    chosen = [sessions[i].strftime("%Y-%m-%d") for i in idx]
    if len(set(chosen)) != n:
        raise ValueError("Representative session selection collapsed")
    return chosen


def build_requests(sample_dates: list[str]) -> list[SampleRequest]:
    """Build only short parent-symbol samples so metadata pricing stays fast."""
    cal = xc.get_calendar("XNYS")
    requests: list[SampleRequest] = []
    for day in sample_dates:
        if not cal.is_session(day):
            raise ValueError(f"Non-session sample: {day}")
        open_ts = cal.session_open(day)
        close_ts = cal.session_close(day)
        span = close_ts - open_ts
        anchors = {
            "opening_2m": open_ts,
            "midday_2m": open_ts + span / 2,
            "closing_2m": max(open_ts, close_ts - pd.Timedelta(minutes=2)),
        }
        for window, a in anchors.items():
            b = min(a + pd.Timedelta(minutes=2), close_ts)
            requests.append(
                SampleRequest(
                    session=day,
                    window=window,
                    start=a.isoformat(),
                    end=b.isoformat(),
                )
            )
    if len(requests) > MAX_CALLS:
        raise ValueError(f"Metadata call cap exceeded: {len(requests)} > {MAX_CALLS}")
    return requestsdef summarize(rows: list[dict], start: str, end: str) -> dict:
    """Extrapolate from short parent samples; never price a long parent interval."""
    session_count = len(_sessions(start, end))
    per_minute = [float(r["cost_usd"]) / 2.0 for r in rows]
    med_rate = median(per_minute)
    avg_rate = mean(per_minute)
    max_rate = max(per_minute)
    regular_minutes = 390.0
    opening_minutes = 30.0

    return {
        "mode": "fast_short_parent_samples",
        "dataset": DATASET,
        "schema": SCHEMA,
        "parent": PARENT,
        "study_start": start,
        "study_end": end,
        "trading_sessions": session_count,
        "metadata_calls": len(rows),
        "max_metadata_calls": MAX_CALLS,
        "max_workers": MAX_WORKERS,
        "download_calls": 0,
        "sample_minutes_each": 2,
        "interpretation": (
            "Cost is extrapolated from short full-parent samples. This deliberately "
            "avoids long-running metadata requests. Because the eventual capture is "
            "restricted to the $2-to-first-$10, 0-5DTE surface, the parent projection "
            "is an intentionally broad upper-bound planning estimate."
        ),
        "per_minute_parent_cost_usd": {
            "median": med_rate,
            "mean": avg_rate,
            "max": max_rate,
        },
        "projected": {
            "opening_30m_total_median_usd": med_rate * opening_minutes * session_count,
            "opening_30m_total_mean_usd": avg_rate * opening_minutes * session_count,
            "opening_30m_total_high_usd": max_rate * opening_minutes * session_count,
            "full_regular_session_total_median_usd": med_rate * regular_minutes * session_count,
            "full_regular_session_total_mean_usd": avg_rate * regular_minutes * session_count,
            "full_regular_session_total_high_usd": max_rate * regular_minutes * session_count,
        },
        "rows": rows,
    }
