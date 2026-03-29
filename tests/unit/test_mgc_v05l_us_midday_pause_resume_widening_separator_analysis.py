from decimal import Decimal

from mgc_v05l.app.us_midday_pause_resume_widening_separator_analysis import (
    _rebound_depth_1bar,
    _rebound_depth_2bar,
    _strict_monotone,
)


def test_rebound_depth_helpers_measure_recent_rebound() -> None:
    prior = [
        {"close": Decimal("100")},
        {"close": Decimal("101")},
        {"close": Decimal("103")},
    ]

    assert _rebound_depth_1bar(prior) == Decimal("2")
    assert _rebound_depth_2bar(prior) == Decimal("3")


def test_strict_monotone_supports_contracting_and_rising_shapes() -> None:
    assert _strict_monotone([Decimal("5"), Decimal("4"), Decimal("3")], decreasing=True) is True
    assert _strict_monotone([Decimal("1"), Decimal("2"), Decimal("3")], decreasing=False) is True
    assert _strict_monotone([Decimal("5"), Decimal("5"), Decimal("3")], decreasing=True) is False
