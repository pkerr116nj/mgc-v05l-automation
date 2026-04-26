from __future__ import annotations

from datetime import UTC, date, datetime

from mgc_v05l.market_data.vix_daily_ingest import parse_cboe_vix_history_csv


def test_parse_cboe_vix_history_csv_filters_range_and_assigns_asof_ts() -> None:
    csv_text = "\n".join(
        [
            "DATE,OPEN,HIGH,LOW,CLOSE",
            "12/31/2019,13.0,14.0,12.5,13.2",
            "01/02/2020,13.5,14.5,13.1,14.0",
            "01/03/2020,14.0,15.0,13.8,14.5",
        ]
    )

    rows = parse_cboe_vix_history_csv(
        csv_text,
        start_date=date.fromisoformat("2020-01-01"),
        end_date=date.fromisoformat("2020-01-03"),
        asof_time_et="16:15:00",
        source="cboe_official_daily_history",
        loaded_at="2026-04-25T12:00:00+00:00",
    )

    assert [row["vix_trade_date"] for row in rows] == ["2020-01-02", "2020-01-03"]
    assert rows[0]["vix_close"] == 14.0
    assert rows[0]["vix_open"] == 13.5
    assert datetime.fromisoformat(rows[0]["vix_asof_ts"]).astimezone(UTC).hour == 21

