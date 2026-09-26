"""Fast bounded Databento cost estimator for expanded NDXP CMBP-1 research.

Uses four representative sessions and three two-minute parent-symbol samples
per session. Maximum: 12 metadata calls, four workers, zero downloads.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
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
    return xc.get_calendar("XNYS").sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))


def representative_sessions(start: str, end: str, n: int = 4) -> list[str]:
    sessions = _sessions(start, end)
    if len(sessions) < n:
        raise ValueError("Not enough sessions")
    idx = [round(((i + 1) / (n + 1)) * (len(sessions) - 1)) for i in range(n)]
    result = [sessions[i].strftime("%Y-%m-%d") for i in idx]
    if len(set(result)) != n:
        raise ValueError("Sample dates collapsed")
    return result


def build_requests(sample_dates: list[str]) -> list[SampleRequest]:
    cal = xc.get_calendar("XNYS")
    out = []
    for day in sample_dates:
        if not cal.is_session(day):
            raise ValueError(f"Non-session: {day}")
        op = cal.session_open(day)
        cl = cal.session_close(day)
        span = cl - op
        anchors = (
            ("opening_2m", op),
            ("midday_2m", op + span / 2),
            ("closing_2m", cl - pd.Timedelta(minutes=2)),
        )
        for window, start in anchors:
            end = min(start + pd.Timedelta(minutes=2), cl)
            out.append(SampleRequest(day, window, start.isoformat(), end.isoformat()))
    if len(out) > MAX_CALLS:
        raise ValueError("Metadata call cap exceeded")
    return out


def quote_requests(
    requests: list[SampleRequest],
    call: Callable[[str, dict], float],
    workers: int = MAX_WORKERS,
) -> list[dict]:
    if len(requests) > MAX_CALLS:
        raise ValueError("Metadata call cap exceeded")
    if not 1 <= workers <= MAX_WORKERS:
        raise ValueError("Worker cap exceeded")

    def one(req: SampleRequest) -> dict:
        value = call("get_cost", req.params())
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError("Invalid provider cost")
        return {
            "session": req.session,
            "window": req.window,
            "start": req.start,
            "end": req.end,
            "cost_usd": float(value),
        }

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(one, requests))


def summarize(rows: list[dict], start: str, end: str) -> dict:
    session_count = len(_sessions(start, end))
    rates = [float(r["cost_usd"]) / 2.0 for r in rows]
    med, avg, high = median(rates), mean(rates), max(rates)
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
        "per_minute_parent_cost_usd": {"median": med, "mean": avg, "max": high},
        "projected": {
            "opening_30m_total_median_usd": med * 30 * session_count,
            "opening_30m_total_mean_usd": avg * 30 * session_count,
            "opening_30m_total_high_usd": high * 30 * session_count,
            "full_regular_session_total_median_usd": med * 390 * session_count,
            "full_regular_session_total_mean_usd": avg * 390 * session_count,
            "full_regular_session_total_high_usd": high * 390 * session_count,
        },
        "interpretation": (
            "Broad parent-symbol planning estimate from short samples. The eventual "
            "$2-to-first-$10, 0-5DTE surface should be narrower."
        ),
        "rows": rows,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", default=DEFAULT_START)
    p.add_argument("--end", default=DEFAULT_END)
    p.add_argument("--samples", type=int, default=4, choices=[2, 3, 4])
    p.add_argument("--env-file", type=Path, default=Path(".env.local"))
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--offline", action="store_true")
    args = p.parse_args()

    if args.output.exists():
        raise FileExistsError("Choose a new output path")
    dates = representative_sessions(args.start, args.end, args.samples)
    requests = build_requests(dates)
    args.output.parent.mkdir(parents=True, exist_ok=True)

    if args.offline:
        result = {
            "mode": "offline_preview",
            "sample_dates": dates,
            "planned_metadata_calls": len(requests),
            "metadata_calls": 0,
            "download_calls": 0,
            "requests": [{"session": r.session, "window": r.window, "params": r.params()} for r in requests],
        }
    else:
        rows = quote_requests(requests, http_metadata(load_key(args.env_file)))
        result = summarize(rows, args.start, args.end)
        result["sample_dates"] = dates

    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
