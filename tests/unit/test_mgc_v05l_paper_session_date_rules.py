from mgc_v05l.app.operator_dashboard import _ny_session_date_key as dashboard_session_date_key
from mgc_v05l.app.operator_dashboard import _timestamp_matches_session
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
