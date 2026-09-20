from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from mgc_v05l.research.ndxp_naive_baseline import (
    OptionQuote,
    build_opening_candidates,
    evaluate_baselines,
    summarize,
)


ET = ZoneInfo("America/New_York")


def quote(second: int, option_type: str, strike: float, bid: float, ask: float, *, spot: float = 100.0) -> OptionQuote:
    return OptionQuote(
        quote_time=datetime(2026, 9, 17, 9, 30, second, tzinfo=ET),
        expiration=date(2026, 9, 17),
        option_type=option_type,
        strike=strike,
        bid=bid,
        ask=ask,
        spot=spot,
    )


def test_builds_first_synchronized_ten_point_put_spread() -> None:
    quotes = [
        quote(0, "P", 90, 1.0, 1.2),
        quote(1, "P", 100, 5.0, 5.4),
        quote(3, "P", 110, 10.2, 10.6),  # too stale relative to 100
        quote(4, "P", 100, 5.2, 5.5),
    ]
    candidates, exclusions = build_opening_candidates(quotes, max_leg_skew_seconds=1.5)
    assert exclusions == {}
    assert [(row.short_strike, row.long_strike) for row in candidates] == [(100, 90), (110, 100)]
    assert candidates[0].mid_credit == pytest.approx(4.1)
    assert candidates[1].quote_skew_seconds == 1.0


def test_itm_credit_reduces_maximum_loss_but_can_lose_without_decline() -> None:
    quotes = [
        quote(0, "P", 90, 0.9, 1.1),
        quote(0, "P", 100, 4.9, 5.1),
        quote(0, "P", 110, 10.9, 11.1),
        quote(0, "P", 120, 18.9, 19.1),
    ]
    candidates, _ = build_opening_candidates(quotes)
    outcomes, _ = evaluate_baselines(
        candidates,
        {date(2026, 9, 17): 100.0},
        offsets=(0.0, 20.0),
        credit_targets=(),
        fill_haircuts=(0.0,),
        fee_per_spread=0.0,
    )
    atm = next(row for row in outcomes if row.selector_value == 0.0)
    itm = next(row for row in outcomes if row.selector_value == 20.0)
    assert atm.entry_credit == pytest.approx(4.0)
    assert itm.entry_credit == pytest.approx(8.0)
    assert atm.max_loss == pytest.approx(600.0)
    assert itm.max_loss == pytest.approx(200.0)
    assert atm.net_pnl == pytest.approx(400.0)
    assert itm.net_pnl == pytest.approx(-200.0)
    assert itm.full_loss is True


def test_summary_reports_tail_and_settled_equity_drawdown() -> None:
    sessions = [date(2026, 9, day) for day in (14, 15, 16, 17)]
    all_outcomes = []
    for session, settlement in zip(sessions, (110.0, 110.0, 80.0, 110.0), strict=True):
        base = [
            OptionQuote(datetime.combine(session, datetime.min.time(), ET).replace(hour=9, minute=30), session, "P", 90, 1, 1.2, spot=100),
            OptionQuote(datetime.combine(session, datetime.min.time(), ET).replace(hour=9, minute=30), session, "P", 100, 5, 5.2, spot=100),
        ]
        candidates, _ = build_opening_candidates(base)
        outcomes, _ = evaluate_baselines(candidates, {session: settlement}, offsets=(0,), credit_targets=(), fill_haircuts=(0,), fee_per_spread=0)
        all_outcomes.extend(outcomes)
    row = summarize(all_outcomes)[0]
    assert row["trades"] == 4
    assert row["win_rate"] == 0.75
    assert row["full_loss_rate"] == 0.25
    assert row["max_drawdown"] == pytest.approx(-600.0)


def test_parity_reference_uses_only_quotes_available_at_candidate_time() -> None:
    quotes = []
    markets = {90: (15.0, 5.0), 100: (8.0, 8.0), 110: (5.0, 15.0)}
    for strike, (call_mid, put_mid) in markets.items():
        quotes.extend(
            [
                quote(0, "C", strike, call_mid - 0.1, call_mid + 0.1, spot=None),
                quote(0, "P", strike, put_mid - 0.1, put_mid + 0.1, spot=None),
            ]
        )
    candidates, exclusions = build_opening_candidates(quotes)
    assert exclusions == {}
    assert candidates
    assert all(candidate.reference_level == pytest.approx(100.0) for candidate in candidates)
