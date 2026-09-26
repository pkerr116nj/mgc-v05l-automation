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
MAX_CALLS = 8
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
    cal = xc.get_calendar("XNYS")
    requests: list[SampleRequest] = []
    for day in sample_dates:
        if not cal.is_session(day):
            raise ValueError(f"Non-session sample: {day}")
        open_ts = cal.session_open(day)
        close_ts = cal.session_close(day)
        opening_end = min(open_ts + pd.Timedelta(minutes=30), close_ts)
        requests.append(
            SampleRequest(
                session=day,
                window="opening_30m",
                start=open_ts.isoformat(),
                end=opening_end.isoformat(),
            )
        )
        requests.append(
            SampleRequest(
                session=day,
                window="full_regular_session",
                start=open_ts.isoformat(),
                end=close_ts.isoformat(),
            )
        )
    if len(requests) > MAX_CALLS:
        raise ValueError(f"Metadata call cap exceeded: {len(requests)} > {MAX_CALLS}")
    return requests


def quote_requests(
    requests: list[SampleRequest],
    call: Callable[[str, dict], float],
    workers: int = MAX_WORKERS,
) -> list[dict]:
    if not 1 <= workers <= MAX_WORKERS:
        raise ValueError("Worker cap exceeded")
    if len(requests) > MAX_CALLS:
        raise ValueError("Metadata call cap exceeded")

    def one(req: SampleRequest) -> dict:
        value = call("get_cost", req.params())
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError(f"Invalid provider cost for {req.session} {req.window}")
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
    by_window: dict[str, list[float]] = {}
    for row in rows:
        by_window.setdefault(row["window"], []).append(float(row["cost_usd"]))

    windows = {}
    for window, values in sorted(by_window.items()):
        med = median(values)
        avg = mean(values)
        mx = max(values)
        windows[window] = {
            "sample_count": len(values),
            "sample_costs_usd": values,
            "per_session_median_usd": med,
            "per_session_mean_usd": avg,
            "per_session_max_usd": mx,
            "projected_sessions": session_count,
            "projected_total_median_usd": med * session_count,
            "projected_total_mean_usd": avg * session_count,
            "projected_total_high_usd": mx * session_count,
        }

    opening = windows["opening_30m"]
    full = windows["full_regular_session"]
    return {
        "mode": "fast_parent_upper_bound",
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
        "interpretation": (
            "These are parent-symbol upper bounds. They include more strikes/expirations "
            "than the intended $2-to-first-$10, 0-5DTE call/put surface. If the upper "
            "bound is affordable, prefer broad coarse retrieval instead of surgical "
            "per-contract acquisition."
        ),
        "windows": windows,
        "derived": {
            "projected_rest_of_day_median_usd": max(
                0.0,
                full["projected_total_median_usd"] - opening["projected_total_median_usd"],
            ),
            "projected_rest_of_day_high_usd": max(
                0.0,
                full["projected_total_high_usd"] - opening["projected_total_high_usd"],
            ),
        },
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", default=DEFAULT_START)
    p.add_argument("--end", default=DEFAULT_END)
    p.add_argument("--samples", type=int, default=4, choices=[2, 3, 4])
    p.add_argument("--env-file", type=Path, default=Path(".env.local"))
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--offline", action="store_true", help="Print request plan only; make zero metadata calls")
    args = p.parse_args()

    dates = representative_sessions(args.start, args.end, args.samples)
    requests = build_requests(dates)

    if args.output.exists():
        raise FileExistsError("Choose a new output path; estimator never overwrites evidence")

    args.output.parent.mkdir(parents=True, exist_ok=True)

    if args.offline:
        result = {
            "mode": "offline_preview",
            "sample_dates": dates,
            "metadata_calls": 0,
            "planned_metadata_calls": len(requests),
            "download_calls": 0,
            "requests": [{"session": r.session, "window": r.window, "params": r.params()} for r in requests],
        }
    else:
        call = http_metadata(load_key(args.env_file))
        rows = quote_requests(requests, call)
        result = summarize(rows, args.start, args.end)
        result["sample_dates"] = dates
        result["rows"] = rows

    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k not in {"rows", "requests", "windows"}}, indent=2))
    if "windows" in result:
        print(json.dumps(result["windows"], indent=2))


if __name__ == "__main__":
    main()
