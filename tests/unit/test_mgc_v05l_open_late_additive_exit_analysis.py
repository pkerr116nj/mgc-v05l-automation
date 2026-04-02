from decimal import Decimal

from mgc_v05l.app.open_late_additive_exit_analysis import _classify_trade


def test_classify_trade_flags_low_capture_fast_mfe_as_bad_exit() -> None:
    assessment, takeaway = _classify_trade(
        net_pnl=Decimal("19"),
        mfe=Decimal("9.6"),
        mfe_capture_pct=Decimal("19.79"),
        bars_held=2,
        time_to_mfe=1,
        exit_reason="SHORT_INTEGRITY_FAIL",
    )

    assert assessment == "tolerable_entry_bad_exit"
    assert "monetization" in takeaway.lower()


def test_classify_trade_flags_sub_half_capture_as_exit_left_money() -> None:
    assessment, takeaway = _classify_trade(
        net_pnl=Decimal("58"),
        mfe=Decimal("14.9"),
        mfe_capture_pct=Decimal("38.9"),
        bars_held=5,
        time_to_mfe=4,
        exit_reason="SHORT_INTEGRITY_FAIL",
    )

    assert assessment == "good_entry_exit_left_money"
    assert "less than half" in takeaway.lower()


def test_classify_trade_flags_high_capture_as_reasonable_exit() -> None:
    assessment, takeaway = _classify_trade(
        net_pnl=Decimal("178"),
        mfe=Decimal("32.8"),
        mfe_capture_pct=Decimal("54.26"),
        bars_held=3,
        time_to_mfe=2,
        exit_reason="SHORT_INTEGRITY_FAIL",
    )

    assert assessment == "good_entry_reasonable_exit"
    assert "useful share" in takeaway.lower()
