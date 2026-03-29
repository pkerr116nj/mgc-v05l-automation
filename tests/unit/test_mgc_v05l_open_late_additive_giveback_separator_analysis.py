from decimal import Decimal

from mgc_v05l.app.open_late_additive_giveback_separator_analysis import GivebackPathRow, _first_bar


def test_first_bar_returns_first_trigger_bar_end_ts() -> None:
    rows = [
        GivebackPathRow(
            trade_id="1",
            cohort="good_recent",
            entry_ts="2026-02-10T10:25:00-05:00",
            exit_ts="2026-02-10T10:40:00-05:00",
            bar_index=1,
            bar_start_ts="2026-02-10T10:25:00-05:00",
            bar_end_ts="2026-02-10T10:30:00-05:00",
            bar_low=Decimal("99"),
            bar_close=Decimal("100"),
            short_risk=Decimal("4"),
            current_favorable_excursion=Decimal("2"),
            max_favorable_excursion=Decimal("2"),
            giveback_from_peak=Decimal("0"),
            reached_0_5r=True,
            reached_0_75r=False,
            reached_1_0r=False,
            fire_0_5r_25pct=False,
            fire_0_5r_33pct=False,
            fire_0_5r_50pct=False,
            fire_0_75r_25pct=False,
            fire_0_75r_33pct=False,
            fire_0_75r_50pct=False,
            fire_1_0r_25pct=False,
            fire_1_0r_33pct=False,
            fire_1_0r_50pct=False,
        ),
        GivebackPathRow(
            trade_id="1",
            cohort="good_recent",
            entry_ts="2026-02-10T10:25:00-05:00",
            exit_ts="2026-02-10T10:40:00-05:00",
            bar_index=2,
            bar_start_ts="2026-02-10T10:30:00-05:00",
            bar_end_ts="2026-02-10T10:35:00-05:00",
            bar_low=Decimal("98"),
            bar_close=Decimal("101"),
            short_risk=Decimal("4"),
            current_favorable_excursion=Decimal("3"),
            max_favorable_excursion=Decimal("3"),
            giveback_from_peak=Decimal("1"),
            reached_0_5r=True,
            reached_0_75r=True,
            reached_1_0r=False,
            fire_0_5r_25pct=True,
            fire_0_5r_33pct=True,
            fire_0_5r_50pct=False,
            fire_0_75r_25pct=True,
            fire_0_75r_33pct=True,
            fire_0_75r_50pct=False,
            fire_1_0r_25pct=False,
            fire_1_0r_33pct=False,
            fire_1_0r_50pct=False,
        ),
    ]

    assert _first_bar(rows, "fire_0_75r_33pct") == "2026-02-10T10:35:00-05:00"
