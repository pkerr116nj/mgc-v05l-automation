from decimal import Decimal

from mgc_v05l.app.derivative_state_separator_research import _bucket_from_values


def test_bucket_from_values_matches_existing_turn_research_labels() -> None:
    assert _bucket_from_values(str(Decimal("-0.20")), str(Decimal("-0.30"))) == "SLOPE_NEG|CURVATURE_NEG"
    assert _bucket_from_values(str(Decimal("-0.70")), str(Decimal("-0.70"))) == "SLOPE_STRONG_NEG|CURVATURE_STRONG_NEG"
    assert _bucket_from_values(str(Decimal("-0.05")), str(Decimal("0.20"))) == "SLOPE_FLAT|CURVATURE_POS"
