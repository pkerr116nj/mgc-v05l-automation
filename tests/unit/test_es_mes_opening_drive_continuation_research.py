from __future__ import annotations

from datetime import date, datetime, timedelta

from mgc_v05l.app.es_mes_opening_drive_continuation_research import (
    OpeningDriveSpec,
    _evaluate_session,
    build_variant_spec,
)
from mgc_v05l.research.trend_participation.models import ResearchBar


def _bar(
    end_ts: datetime,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: int = 1000,
    timeframe: str = "5m",
) -> ResearchBar:
    return ResearchBar(
        instrument="ES",
        timeframe=timeframe,
        start_ts=end_ts - timedelta(minutes=5 if timeframe == "5m" else 1),
        end_ts=end_ts,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        session_label="US",
        session_segment="US",
        source="test",
    )


def _preopen_bars() -> list[ResearchBar]:
    start = datetime.fromisoformat("2026-04-17T09:01:00-04:00")
    closes = [7121.0, 7123.0, 7122.5, 7124.0, 7126.5, 7125.5, 7127.0, 7128.5, 7129.0]
    rows: list[ResearchBar] = []
    current = start
    previous = 7120.5
    for close in closes:
        rows.append(
            _bar(
                current,
                previous,
                max(previous, close) + 0.25,
                min(previous, close) - 0.25,
                close,
                200,
                "1m",
            )
        )
        previous = close
        current += timedelta(minutes=1)
    return rows


def _cash_bars() -> list[ResearchBar]:
    start = datetime.fromisoformat("2026-04-17T09:35:00-04:00")
    values = [
        (7124.0, 7133.0, 7123.5, 7132.0),
        (7132.0, 7134.0, 7127.5, 7127.75),
        (7127.75, 7133.25, 7127.5, 7132.75),
        (7132.75, 7133.0, 7129.75, 7131.5),
        (7131.5, 7138.25, 7131.0, 7137.0),
        (7137.0, 7146.0, 7136.75, 7145.25),
        (7145.25, 7150.0, 7143.75, 7149.0),
        (7149.0, 7154.0, 7148.5, 7153.75),
        (7153.75, 7154.25, 7151.5, 7153.25),
        (7153.25, 7153.5, 7151.25, 7152.25),
        (7152.25, 7154.0, 7151.75, 7153.25),
        (7153.25, 7154.5, 7152.75, 7153.5),
        (7153.5, 7160.0, 7153.0, 7159.25),
        (7159.25, 7167.0, 7158.5, 7166.75),
        (7166.75, 7171.5, 7165.75, 7171.0),
        (7171.0, 7171.25, 7168.5, 7169.5),
        (7169.5, 7170.0, 7162.75, 7163.25),
        (7163.25, 7166.0, 7162.75, 7165.25),
    ]
    rows: list[ResearchBar] = []
    current = start
    for open_, high, low, close in values:
        rows.append(_bar(current, open_, high, low, close, 2500))
        current += timedelta(minutes=5)
    return rows


def test_evaluate_session_detects_april_17_style_opening_drive() -> None:
    trade = _evaluate_session(
        symbol="ES",
        session_day=date(2026, 4, 17),
        preopen_bars=_preopen_bars(),
        cash_bars=_cash_bars(),
        spec=OpeningDriveSpec(),
    )

    assert trade.qualified is True
    assert trade.entered is True
    assert trade.entry_bar_number == 6
    assert trade.exit_bar_number == 17
    assert trade.exit_reason == "ema_and_structure_break"
    assert trade.pnl_points is not None and trade.pnl_points > 20.0
    assert trade.net_pnl_points is not None and trade.net_pnl_points < trade.pnl_points
    assert trade.gross_r_multiple is not None and trade.gross_r_multiple > 1.0
    assert trade.net_r_multiple is not None and trade.net_r_multiple > 0.5
    assert trade.mfe_points is not None and trade.mfe_points >= trade.pnl_points
    assert trade.mae_points is not None and trade.mae_points >= 0.0


def test_evaluate_session_rejects_weak_setup_bar() -> None:
    damaged_cash = _cash_bars()
    damaged_cash[4] = _bar(
        damaged_cash[4].end_ts,
        damaged_cash[4].open,
        damaged_cash[4].high,
        7124.0,
        7131.75,
        damaged_cash[4].volume,
    )

    trade = _evaluate_session(
        symbol="ES",
        session_day=date(2026, 4, 17),
        preopen_bars=_preopen_bars(),
        cash_bars=damaged_cash,
        spec=OpeningDriveSpec(),
    )

    assert trade.qualified is False
    assert trade.entered is False
    assert "pause_breakout_ok" in trade.notes or "setup_close_ok" in trade.notes


def test_v2_enters_earlier_and_captures_more_of_april_17() -> None:
    v1 = _evaluate_session(
        symbol="ES",
        session_day=date(2026, 4, 17),
        preopen_bars=_preopen_bars(),
        cash_bars=_cash_bars(),
        spec=OpeningDriveSpec(),
    )
    v2 = _evaluate_session(
        symbol="ES",
        session_day=date(2026, 4, 17),
        preopen_bars=_preopen_bars(),
        cash_bars=_cash_bars(),
        spec=build_variant_spec(
            variant="v2",
            slippage_ticks_per_side=1.0,
            es_round_turn_commission_dollars=4.5,
            mes_round_turn_commission_dollars=1.5,
        ),
    )

    assert v2.entered is True
    assert v2.entry_bar_number is not None and v1.entry_bar_number is not None
    assert v2.entry_bar_number < v1.entry_bar_number
    assert v2.entry_price is not None and v1.entry_price is not None and v2.entry_price < v1.entry_price
    assert v2.pnl_points is not None and v1.pnl_points is not None and v2.pnl_points > v1.pnl_points


def test_v3_keeps_earlier_capture_but_remains_filtered() -> None:
    v1 = _evaluate_session(
        symbol="ES",
        session_day=date(2026, 4, 17),
        preopen_bars=_preopen_bars(),
        cash_bars=_cash_bars(),
        spec=OpeningDriveSpec(),
    )
    v3 = _evaluate_session(
        symbol="ES",
        session_day=date(2026, 4, 17),
        preopen_bars=_preopen_bars(),
        cash_bars=_cash_bars(),
        spec=build_variant_spec(
            variant="v3",
            slippage_ticks_per_side=1.0,
            es_round_turn_commission_dollars=4.5,
            mes_round_turn_commission_dollars=1.5,
        ),
    )

    assert v3.entered is True
    assert v3.entry_bar_number is not None and v1.entry_bar_number is not None
    assert v3.entry_bar_number <= v1.entry_bar_number
    assert v3.entry_price is not None and v1.entry_price is not None and v3.entry_price <= v1.entry_price
    assert v3.net_pnl_points is not None and v1.net_pnl_points is not None and v3.net_pnl_points >= v1.net_pnl_points
