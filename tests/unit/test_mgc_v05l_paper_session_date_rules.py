from datetime import datetime

from mgc_v05l.app.operator_dashboard import _ny_session_date_key as dashboard_session_date_key
from mgc_v05l.app.operator_dashboard import _timestamp_matches_session
from mgc_v05l.app.session_phase_labels import session_restriction_matches_timestamp
from mgc_v05l.app.tracked_paper_strategies import _ny_session_date_key as tracked_session_date_key


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
    assert session_restriction_matches_timestamp(datetime.fromisoformat("2026-04-29T08:45:00-04:00"), "US") is True
    assert session_restriction_matches_timestamp(datetime.fromisoformat("2026-04-29T10:45:00-04:00"), "US") is True
    assert session_restriction_matches_timestamp(datetime.fromisoformat("2026-04-29T17:15:00-04:00"), "US") is False
