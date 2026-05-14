from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from mgc_v05l.app.operator_dashboard import _ny_session_date_key as dashboard_session_date_key
from mgc_v05l.app.operator_dashboard import _timestamp_matches_session
from mgc_v05l.app.operator_dashboard import _broad_trading_session_for_timestamp
from mgc_v05l.app.session_phase_labels import label_session_phase, session_restriction_matches_timestamp
from mgc_v05l.app.tracked_paper_strategies import _ny_session_date_key as tracked_session_date_key
from mgc_v05l.domain.models import Bar
from mgc_v05l.strategy.strategy_engine import label_session_phase_for_bar


def test_operator_dashboard_session_date_uses_new_york_timezone() -> None:
    assert dashboard_session_date_key("2026-04-15T00:30:00+00:00") == "2026-04-14"
    assert dashboard_session_date_key("2026-04-15T05:30:00+00:00") == "2026-04-15"


def test_operator_dashboard_timestamp_matches_session_uses_new_york_timezone() -> None:
    assert _timestamp_matches_session("2026-04-15T00:30:00+00:00", "2026-04-14") is True
    assert _timestamp_matches_session("2026-04-15T00:30:00+00:00", "2026-04-15") is False
    assert _timestamp_matches_session("2026-04-15T05:30:00+00:00", "2026-04-15") is True


def test_tracked_paper_strategy_day_pnl_uses_new_york_timezone() -> None:
    assert tracked_session_date_key("2026-04-15T00:30:00+00:00") == "2026-04-14"
    assert tracked_session_date_key("2026-04-15T05:30:00+00:00") == "2026-04-15"


def test_session_restriction_matches_midday_and_late_windows() -> None:
    assert session_restriction_matches_timestamp(datetime.fromisoformat("2026-04-29T11:05:00-04:00"), "US_MIDDAY") is True
    assert session_restriction_matches_timestamp(datetime.fromisoformat("2026-04-29T13:35:00-04:00"), "US_LATE") is True
    assert session_restriction_matches_timestamp(datetime.fromisoformat("2026-04-29T10:45:00-04:00"), "US_EARLY") is True


def test_broad_session_matching_survives_unclassified_phase_gaps() -> None:
    assert label_session_phase(datetime.fromisoformat("2026-04-29T08:45:00-04:00")) == "US_EARLY"
    assert session_restriction_matches_timestamp(datetime.fromisoformat("2026-04-29T08:45:00-04:00"), "US") is True
    assert session_restriction_matches_timestamp(datetime.fromisoformat("2026-04-29T10:45:00-04:00"), "US") is True
    assert session_restriction_matches_timestamp(datetime.fromisoformat("2026-04-29T17:15:00-04:00"), "US") is False


def test_session_open_is_standalone_sunday_through_thursday() -> None:
    reopen_dates = [
        "2026-05-10",  # Sunday
        "2026-05-11",
        "2026-05-12",
        "2026-05-13",
        "2026-05-14",  # Thursday
    ]
    friday_open = datetime.fromisoformat("2026-05-15T18:30:00-04:00")

    for reopen_date in reopen_dates:
        session_open = datetime.fromisoformat(f"{reopen_date}T18:30:00-04:00")
        assert label_session_phase(session_open) == "SESSION_OPEN"
        assert session_restriction_matches_timestamp(session_open, "SESSION_OPEN") is True
        assert session_restriction_matches_timestamp(session_open, "ASIA_EARLY") is False
    assert label_session_phase(datetime.fromisoformat("2026-05-10T19:00:00-04:00")) == "ASIA_EARLY"
    assert session_restriction_matches_timestamp(friday_open, "SESSION_OPEN") is False


def test_late_asia_covers_overnight_before_london_open() -> None:
    late_asia = datetime.fromisoformat("2026-05-14T01:15:00-04:00")
    london_open = datetime.fromisoformat("2026-05-14T03:00:00-04:00")

    assert label_session_phase(late_asia) == "ASIA_LATE"
    assert session_restriction_matches_timestamp(late_asia, "ASIA_LATE") is True
    assert session_restriction_matches_timestamp(late_asia, "ASIA") is True
    assert session_restriction_matches_timestamp(late_asia, "LONDON_EARLY") is False
    assert label_session_phase(london_open) == "LONDON_OPEN"
    assert _broad_trading_session_for_timestamp(late_asia) == "ASIA_LATE"


def test_london_late_hands_off_to_us_sessions_at_comex_open() -> None:
    london_late = datetime.fromisoformat("2026-04-29T08:19:00-04:00")
    us_early_start = datetime.fromisoformat("2026-04-29T08:20:00-04:00")
    us_preopen = datetime.fromisoformat("2026-04-29T09:15:00-04:00")
    us_cash_impulse = datetime.fromisoformat("2026-04-29T09:45:00-04:00")
    us_open_late = datetime.fromisoformat("2026-04-29T10:15:00-04:00")
    us_early_after_open_late = datetime.fromisoformat("2026-04-29T10:45:00-04:00")
    us_midday = datetime.fromisoformat("2026-04-29T11:00:00-04:00")
    us_late = datetime.fromisoformat("2026-04-29T13:30:00-04:00")

    assert label_session_phase(london_late) == "LONDON_LATE"
    assert session_restriction_matches_timestamp(london_late, "LONDON_LATE") is True
    assert _broad_trading_session_for_timestamp(london_late) == "LONDON_LATE"

    assert label_session_phase(us_early_start) == "US_EARLY"
    assert session_restriction_matches_timestamp(us_early_start, "LONDON_LATE") is False
    assert session_restriction_matches_timestamp(us_early_start, "US_EARLY") is True
    assert session_restriction_matches_timestamp(us_early_start, "NY_EARLY") is True
    assert _broad_trading_session_for_timestamp(us_early_start) == "US_EARLY"

    assert label_session_phase(us_preopen) == "US_PREOPEN_OPENING"
    assert session_restriction_matches_timestamp(us_preopen, "US_EARLY") is True
    assert session_restriction_matches_timestamp(us_preopen, "US_EARLY_OBSERVATION") is True
    assert label_session_phase(us_cash_impulse) == "US_CASH_OPEN_IMPULSE"
    assert session_restriction_matches_timestamp(us_cash_impulse, "US_EARLY") is True
    assert label_session_phase(us_open_late) == "US_OPEN_LATE"
    assert session_restriction_matches_timestamp(us_open_late, "US_EARLY") is True
    assert label_session_phase(us_early_after_open_late) == "US_EARLY"
    assert session_restriction_matches_timestamp(us_early_after_open_late, "US_EARLY") is True

    assert label_session_phase(us_midday) == "US_MIDDAY"
    assert session_restriction_matches_timestamp(us_midday, "US_MIDDAY") is True
    assert session_restriction_matches_timestamp(us_midday, "NY_LATE") is True
    assert _broad_trading_session_for_timestamp(us_midday) == "US_MIDDAY"

    assert label_session_phase(us_late) == "US_LATE"
    assert session_restriction_matches_timestamp(us_late, "US_LATE") is True
    assert _broad_trading_session_for_timestamp(us_late) == "US_LATE"


def test_strategy_runtime_bar_label_uses_late_asia_until_london_open() -> None:
    end_ts = datetime.fromisoformat("2026-05-14T01:15:00-04:00")
    bar = Bar(
        bar_id="GC|1m|2026-05-14T05:15:00Z",
        symbol="GC",
        timeframe="1m",
        start_ts=datetime.fromisoformat("2026-05-14T01:14:00-04:00"),
        end_ts=end_ts,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=1,
        is_final=True,
        session_asia=True,
        session_london=False,
        session_us=False,
        session_allowed=True,
    )

    assert label_session_phase_for_bar(bar, ZoneInfo("America/New_York")) == "ASIA_LATE"


def test_strategy_runtime_bar_label_uses_us_early_around_comex_open() -> None:
    for end_ts, expected in (
        ("2026-04-29T08:19:00-04:00", "LONDON_LATE"),
        ("2026-04-29T08:20:00-04:00", "US_EARLY"),
        ("2026-04-29T10:45:00-04:00", "US_EARLY"),
        ("2026-04-29T11:00:00-04:00", "US_MIDDAY"),
        ("2026-04-29T13:30:00-04:00", "US_LATE"),
    ):
        parsed_end = datetime.fromisoformat(end_ts)
        bar = Bar(
            bar_id=f"GC|1m|{parsed_end.astimezone(ZoneInfo('UTC')).isoformat()}",
            symbol="GC",
            timeframe="1m",
            start_ts=parsed_end - timedelta(minutes=1),
            end_ts=parsed_end,
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100.5"),
            volume=1,
            is_final=True,
            session_asia=False,
            session_london=expected.startswith("LONDON"),
            session_us=expected.startswith("US"),
            session_allowed=True,
        )

        assert label_session_phase_for_bar(bar, ZoneInfo("America/New_York")) == expected
