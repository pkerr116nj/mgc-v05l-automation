from decimal import Decimal

from mgc_v05l.app.open_late_additive_sequence_shape_analysis import (
    _one_bar_rebound,
    _return_sign_pattern,
    _sign_pattern,
    _slope_getting_less_negative,
)


def test_return_sign_pattern_uses_bar_direction() -> None:
    rows = [
        {"open": Decimal("10"), "close": Decimal("11")},
        {"open": Decimal("11"), "close": Decimal("9")},
        {"open": Decimal("9"), "close": Decimal("9")},
    ]

    assert _return_sign_pattern(rows) == "UDF"


def test_sequence_sign_pattern_formats_numeric_signs() -> None:
    assert _sign_pattern([Decimal("-1"), Decimal("0"), Decimal("2"), None]) == "-0+?"


def test_one_bar_rebound_detects_last_bar_rebound() -> None:
    rows = [
        {"close": Decimal("10")},
        {"close": Decimal("9")},
        {"close": Decimal("9.5")},
    ]

    assert _one_bar_rebound(rows) is True


def test_slope_getting_less_negative_requires_monotonic_improvement() -> None:
    improving = [
        {"normalized_slope": Decimal("-0.8")},
        {"normalized_slope": Decimal("-0.4")},
        {"normalized_slope": Decimal("-0.1")},
    ]
    not_improving = [
        {"normalized_slope": Decimal("-0.2")},
        {"normalized_slope": Decimal("-0.3")},
        {"normalized_slope": Decimal("-0.1")},
    ]

    assert _slope_getting_less_negative(improving) is True
    assert _slope_getting_less_negative(not_improving) is False
