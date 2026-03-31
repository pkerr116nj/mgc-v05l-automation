from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from mgc_v05l.domain.models import Bar
from mgc_v05l.research.quant_futures import (
    _FrameSeries,
    _candidate_id_is_gated,
    _rank_score,
    _resolve_exit,
)


def _bar(*, minute_offset: int, open_: str, high_: str, low: str, close: str) -> Bar:
    start = datetime(2026, 1, 5, 14, 0, tzinfo=UTC) + timedelta(minutes=5 * minute_offset)
    end = start + timedelta(minutes=5)
    return Bar(
        bar_id=f"TEST|5m|{minute_offset}",
        symbol="TEST",
        timeframe="5m",
        start_ts=start,
        end_ts=end,
        open=Decimal(open_),
        high=Decimal(high_),
        low=Decimal(low),
        close=Decimal(close),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )


def test_candidate_id_is_gated_distinguishes_ungated() -> None:
    assert _candidate_id_is_gated("breakout_acceptance.long.gated.tight") is True
    assert _candidate_id_is_gated("breakout_acceptance.long.ungated.tight") is False


def test_resolve_exit_prefers_stop_when_stop_and_target_hit_same_bar() -> None:
    execution = _FrameSeries.from_bars(
        [
            _bar(minute_offset=0, open_="100", high_="101", low="99", close="100.5"),
            _bar(minute_offset=1, open_="100", high_="102.5", low="98.5", close="101"),
            _bar(minute_offset=2, open_="101", high_="103", low="100", close="102"),
        ]
    )
    exit_index, exit_price, exit_reason = _resolve_exit(
        direction="LONG",
        execution=execution,
        entry_index=1,
        hold_bars=2,
        stop_price=99.0,
        target_price=102.0,
    )
    assert exit_index == 1
    assert exit_price == 99.0
    assert exit_reason == "stop_first_conflict"


def test_rank_score_rewards_better_expectancy_and_stability() -> None:
    strong = _rank_score(
        expectancy=0.10,
        positive_symbol_share=0.60,
        walk_forward_positive_ratio=0.75,
        trade_count=120,
        max_drawdown=6.0,
        cost_005=0.05,
        top_3_positive_share=0.35,
        parameter_neighbor_stability=0.80,
        gated=True,
    )
    weak = _rank_score(
        expectancy=-0.02,
        positive_symbol_share=0.20,
        walk_forward_positive_ratio=0.25,
        trade_count=20,
        max_drawdown=14.0,
        cost_005=-0.04,
        top_3_positive_share=0.85,
        parameter_neighbor_stability=0.20,
        gated=False,
    )
    assert strong > weak
