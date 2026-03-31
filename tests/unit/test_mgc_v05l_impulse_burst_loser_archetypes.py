from mgc_v05l.app.mgc_impulse_burst_loser_archetypes import (
    _body_to_range_quality,
    _quantile_slice,
)
from mgc_v05l.app.mgc_impulse_burst_loser_archetypes import FeatureRow
from mgc_v05l.app.mgc_impulse_burst_continuation_research import Bar


def _row(pnl: float) -> FeatureRow:
    return FeatureRow(
        pnl=pnl,
        subclass_bucket="SPIKE_DOMINATED_OTHER",
        time_of_day_bucket="US_LATE",
        prior_10_bar_net_move_normalized=0.0,
        prior_20_bar_net_move_normalized=0.0,
        pre_burst_range_compression_or_expansion=1.0,
        local_micro_range_breakout_flag=1.0,
        largest_bar_concentration_metric=0.5,
        contributing_bar_breadth_metric=0.5,
        same_direction_share=0.75,
        body_dominance=0.7,
        path_efficiency=0.5,
        normalized_move=1.5,
        acceleration_ratio=1.0,
        late_extension_share=0.3,
        body_to_range_quality=0.6,
        first_1_bar_continuation_amount=5.0,
        first_2_bars_continuation_amount=8.0,
        first_1_bar_retrace=3.0,
        first_2_bars_max_retrace=4.0,
        new_extension_within_2_bars_flag=1.0,
    )


def test_quantile_slice_picks_worst_losers() -> None:
    rows = [_row(-10.0), _row(-50.0), _row(-20.0), _row(-80.0)]
    worst = _quantile_slice(rows, ascending=True, fraction=0.25)
    assert len(worst) == 1
    assert worst[0].pnl == -80.0


def test_quantile_slice_picks_best_winners() -> None:
    rows = [_row(10.0), _row(50.0), _row(20.0), _row(80.0)]
    best = _quantile_slice(rows, ascending=False, fraction=0.25)
    assert len(best) == 1
    assert best[0].pnl == 80.0


def test_body_to_range_quality_uses_body_over_range() -> None:
    bars = [
        Bar(timestamp=None, open=1.0, high=2.0, low=0.0, close=2.0, volume=1.0),
        Bar(timestamp=None, open=2.0, high=3.0, low=1.0, close=3.0, volume=1.0),
        Bar(timestamp=None, open=3.0, high=4.0, low=2.0, close=4.0, volume=1.0),
        Bar(timestamp=None, open=4.0, high=5.0, low=3.0, close=5.0, volume=1.0),
        Bar(timestamp=None, open=5.0, high=6.0, low=4.0, close=6.0, volume=1.0),
        Bar(timestamp=None, open=6.0, high=7.0, low=5.0, close=7.0, volume=1.0),
        Bar(timestamp=None, open=7.0, high=8.0, low=6.0, close=8.0, volume=1.0),
        Bar(timestamp=None, open=8.0, high=9.0, low=7.0, close=9.0, volume=1.0),
    ]
    assert _body_to_range_quality(bars, signal_index=7) == 0.5
