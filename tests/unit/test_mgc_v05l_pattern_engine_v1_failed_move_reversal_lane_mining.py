from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_failed_move_reversal_lane_mining import (
    _cluster_label,
    _separator_followup_warranted,
)


def test_failed_move_reversal_lane_followup_requires_positive_directional_quality() -> None:
    assert (
        _separator_followup_warranted(
            match_count=140,
            directional_avg_move=Decimal("1.2"),
            aligned_rate=Decimal("0.54"),
            mfe_mae_ratio=Decimal("1.14"),
        )
        == "yes"
    )


def test_failed_move_reversal_cluster_label_rejects_broad_negative_directional_group() -> None:
    assert (
        _cluster_label(
            match_count=34,
            directional_avg_move=Decimal("-0.3"),
            aligned_rate=Decimal("0.58"),
            mfe_mae_ratio=Decimal("1.22"),
        )
        == "broad_or_noisy"
    )
