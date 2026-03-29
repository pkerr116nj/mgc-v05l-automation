from decimal import Decimal

from mgc_v05l.app.us_midday_pause_resume_long_bind_analysis import _ratio


def test_ratio_formats_decimal_share() -> None:
    assert _ratio(2, 112) == str(Decimal(2) / Decimal(112))
