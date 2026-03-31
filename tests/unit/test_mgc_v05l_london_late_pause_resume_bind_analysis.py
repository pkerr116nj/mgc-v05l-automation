from decimal import Decimal

from mgc_v05l.app.london_late_pause_resume_bind_analysis import _ratio


def test_ratio_formats_decimal_share() -> None:
    assert _ratio(17, 121) == str(Decimal(17) / Decimal(121))
