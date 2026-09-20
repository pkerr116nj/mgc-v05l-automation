from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from mgc_v05l.research.ndxp_naive_data import (
    _normalized_rows,
    estimate_cost,
    parse_osi_symbol,
    weekday_sessions,
)


def test_parse_osi_symbol_preserves_ndxp_contract_details() -> None:
    assert parse_osi_symbol("NDXP  260917P29460000") == ("NDXP", date(2026, 9, 17), "P", 29460.0)


def test_weekdays_are_inclusive_and_exclude_weekend() -> None:
    assert weekday_sessions(date(2026, 9, 18), date(2026, 9, 21)) == [date(2026, 9, 18), date(2026, 9, 21)]


def test_normalizer_keeps_only_same_day_valid_quotes() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["NDXP  260917P29460000", "NDXP  260918P29460000", "NDXP  260917C29460000"],
            "bid_px_00": [5.0, 6.0, float("nan")],
            "ask_px_00": [5.5, 6.5, 3.0],
        },
        index=pd.to_datetime(["2026-09-17T13:30:00Z", "2026-09-17T13:30:00Z", "2026-09-17T13:30:00Z"]),
    )
    frame.index.name = "ts_recv"
    rows = _normalized_rows(frame, date(2026, 9, 17))
    assert len(rows) == 1
    assert rows[0]["strike"] == 29460.0
    assert rows[0]["quote_time"].startswith("2026-09-17T09:30:00")


def test_cost_estimator_uses_one_minute_parent_requests() -> None:
    calls = []

    def get_cost(**kwargs):
        calls.append(kwargs)
        return 0.25

    client = SimpleNamespace(metadata=SimpleNamespace(get_cost=get_cost))
    total, details = estimate_cost(client, [date(2026, 9, 17), date(2026, 9, 18)])
    assert total == pytest.approx(0.50)
    assert len(details) == 2
    assert all(call["schema"] == "cbbo-1s" for call in calls)
    assert all(call["symbols"] == ["NDXP.OPT"] for call in calls)
    assert all((call["end"] - call["start"]).total_seconds() == 60 for call in calls)
