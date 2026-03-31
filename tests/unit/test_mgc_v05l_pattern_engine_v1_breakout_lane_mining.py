from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_breakout_lane_mining import (
    _cluster_label,
    _coherence_label,
    _lane_selection_score,
    _separator_followup_warranted,
)


def test_breakout_lane_scoring_rewards_directional_quality_and_coherence() -> None:
    stronger = _lane_selection_score(
        estimated_value=Decimal("800"),
        directional_avg_move=Decimal("1.5"),
        aligned_rate=Decimal("0.58"),
        mfe_mae_ratio=Decimal("1.25"),
        top3_signature_share=Decimal("0.22"),
    )
    weaker = _lane_selection_score(
        estimated_value=Decimal("800"),
        directional_avg_move=Decimal("0.2"),
        aligned_rate=Decimal("0.49"),
        mfe_mae_ratio=Decimal("1.01"),
        top3_signature_share=Decimal("0.10"),
    )

    assert stronger > weaker


def test_breakout_lane_followup_gate_requires_width_and_quality() -> None:
    assert (
        _separator_followup_warranted(
            match_count=160,
            directional_avg_move=Decimal("0.8"),
            aligned_rate=Decimal("0.55"),
            mfe_mae_ratio=Decimal("1.18"),
        )
        == "yes"
    )
    assert (
        _separator_followup_warranted(
            match_count=50,
            directional_avg_move=Decimal("1.2"),
            aligned_rate=Decimal("0.60"),
            mfe_mae_ratio=Decimal("1.30"),
        )
        == "no"
    )


def test_breakout_lane_labels_distinguish_coherent_vs_broad() -> None:
    assert (
        _coherence_label(
            match_count=220,
            directional_avg_move=Decimal("1.1"),
            aligned_rate=Decimal("0.57"),
            mfe_mae_ratio=Decimal("1.16"),
            top3_signature_share=Decimal("0.20"),
        )
        == "coherent"
    )
    assert (
        _coherence_label(
            match_count=220,
            directional_avg_move=Decimal("-0.2"),
            aligned_rate=Decimal("0.47"),
            mfe_mae_ratio=Decimal("0.98"),
            top3_signature_share=Decimal("0.10"),
        )
        == "likely_too_broad"
    )
    assert (
        _cluster_label(
            match_count=18,
            directional_avg_move=Decimal("1.2"),
            aligned_rate=Decimal("0.61"),
            mfe_mae_ratio=Decimal("1.20"),
        )
        == "candidate"
    )
