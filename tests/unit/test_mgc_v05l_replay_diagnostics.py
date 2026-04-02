from datetime import datetime, timedelta
from decimal import Decimal

from mgc_v05l.app.replay_diagnostics import (
    DiagnosticBar,
    LedgerRow,
    build_bad_entry_rows,
    build_bad_entry_summary,
    build_missed_turn_rows,
    build_missed_turn_summary,
)


def _bar(
    ts: datetime,
    open_px: str,
    high_px: str,
    low_px: str,
    close_px: str,
    session: str = "ASIA",
    atr: str = "5",
    bar_range: str = "5",
    vol_ratio: str = "1.1",
) -> DiagnosticBar:
    return DiagnosticBar(
        bar_id=f"MGC|5m|{ts.isoformat()}",
        timestamp=ts,
        open=Decimal(open_px),
        high=Decimal(high_px),
        low=Decimal(low_px),
        close=Decimal(close_px),
        session=session,
        atr=Decimal(atr),
        bar_range=Decimal(bar_range),
        vol_ratio=Decimal(vol_ratio),
    )


def test_replay_diagnostics_builds_missed_turns_and_bad_entries() -> None:
    base = datetime.fromisoformat("2026-03-13T18:00:00-04:00")
    bars = [
        _bar(base + timedelta(minutes=5 * idx), "100", "101", "99", "100")
        for idx in range(30)
    ]
    bars[4] = _bar(base + timedelta(minutes=20), "99", "100", "95", "99")
    bars[5] = _bar(base + timedelta(minutes=25), "96", "101", "94", "100")
    bars[6] = _bar(base + timedelta(minutes=30), "100", "103", "99", "102")
    bars[7] = _bar(base + timedelta(minutes=35), "102", "106", "101", "105")
    bars[8] = _bar(base + timedelta(minutes=40), "105", "108", "104", "107")
    bars[14] = _bar(base + timedelta(minutes=70), "108", "109", "104", "105", session="US")
    bars[15] = _bar(base + timedelta(minutes=75), "105", "106", "100", "101", session="US")
    bars[16] = _bar(base + timedelta(minutes=80), "101", "102", "95", "96", session="US")

    ledger = [
        LedgerRow(
            trade_id=1,
            direction="LONG",
            entry_ts=bars[8].timestamp,
            entry_px=Decimal("107"),
            exit_ts=bars[10].timestamp,
            exit_px=Decimal("103"),
            qty=1,
            gross_pnl=Decimal("-20"),
            fees=Decimal("0"),
            slippage=Decimal("0"),
            net_pnl=Decimal("-20"),
            exit_reason="LONG_INTEGRITY_FAIL",
            setup_family="firstBullSnapTurn",
            entry_session="ASIA",
            exit_session="ASIA",
        )
    ]

    missed_turns = build_missed_turn_rows(bars, ledger)
    assert missed_turns
    assert any(row.classifier in {"late_entry", "poor_entry", "no_trade"} for row in missed_turns)

    bad_entries = build_bad_entry_rows(bars, ledger)
    assert len(bad_entries) == 1
    assert bad_entries[0].signal_family == "firstBullSnapTurn"
    assert bad_entries[0].entry_efficiency_pct < Decimal("35")

    missed_summary = build_missed_turn_summary(missed_turns)
    bad_summary = build_bad_entry_summary(bad_entries)
    assert missed_summary["missed_turn_count"] >= 1
    assert "ASIA" in missed_summary["missed_turn_count_by_session"]
    assert bad_summary["bad_entry_count"] == 1
    assert bad_summary["bad_entry_count_by_signal_family"]["firstBullSnapTurn"] == 1
