from __future__ import annotations

from datetime import datetime

from mgc_v05l.research.regime.vix_join import build_vix_asof_lookup, lookup_vix_asof


def test_lookup_vix_asof_respects_weekend_and_same_day_availability() -> None:
    lookup = build_vix_asof_lookup(
        [
            {
                "vix_trade_date": "2026-03-13",
                "vix_asof_ts": "2026-03-13T20:15:00+00:00",
                "vix_close": 21.0,
            },
            {
                "vix_trade_date": "2026-03-16",
                "vix_asof_ts": "2026-03-16T20:15:00+00:00",
                "vix_close": 23.0,
            },
        ]
    )

    sunday_evening = datetime.fromisoformat("2026-03-15T18:00:00-04:00")
    monday_before_close = datetime.fromisoformat("2026-03-16T15:00:00-04:00")
    monday_evening = datetime.fromisoformat("2026-03-16T18:00:00-04:00")

    assert lookup_vix_asof(lookup, sunday_evening)["vix_trade_date"] == "2026-03-13"
    assert lookup_vix_asof(lookup, monday_before_close)["vix_trade_date"] == "2026-03-13"
    assert lookup_vix_asof(lookup, monday_evening)["vix_trade_date"] == "2026-03-16"


def test_lookup_vix_asof_carries_forward_last_known_value_when_day_missing() -> None:
    lookup = build_vix_asof_lookup(
        [
            {
                "vix_trade_date": "2026-01-16",
                "vix_asof_ts": "2026-01-16T21:15:00+00:00",
                "vix_close": 19.0,
            }
        ]
    )

    holiday_evening = datetime.fromisoformat("2026-01-19T18:00:00-05:00")
    assert lookup_vix_asof(lookup, holiday_evening)["vix_trade_date"] == "2026-01-16"

