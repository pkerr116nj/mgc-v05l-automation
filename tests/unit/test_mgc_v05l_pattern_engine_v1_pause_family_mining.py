from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_pause_family_mining import _promotion_risk


def test_promotion_risk_marks_candidate_when_cluster_is_repeatable() -> None:
    assert _promotion_risk(match_count=20, favorable_rate=Decimal("0.60"), mfe_mae_ratio=Decimal("1.20")) == "candidate"


def test_promotion_risk_marks_broad_or_noisy_when_quality_is_weak() -> None:
    assert _promotion_risk(match_count=10, favorable_rate=Decimal("0.45"), mfe_mae_ratio=Decimal("0.90")) == "broad_or_noisy"
