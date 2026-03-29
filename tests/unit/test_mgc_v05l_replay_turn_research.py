from datetime import datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import sqlite3

from mgc_v05l.app.replay_turn_research import (
    TurnLedgerRow,
    TurnResearchBar,
    build_and_write_replay_turn_research,
    build_derivative_bin_rows,
    build_entry_quality_by_derivative_bucket_rows,
    build_missed_turns_by_derivative_bucket_rows,
    build_turn_dataset_rows,
    build_turn_summary,
)


def _bar(
    ts: datetime,
    open_px: str,
    high_px: str,
    low_px: str,
    close_px: str,
    *,
    session: str = "ASIA",
    atr: str = "5",
    vwap: str = "100",
    ema_fast: str = "100",
    ema_slow: str = "99.5",
    velocity: str = "0.5",
    velocity_delta: str = "0.1",
    bar_range: str = "5",
    vol_ratio: str = "1.1",
) -> TurnResearchBar:
    return TurnResearchBar(
        bar_id=f"MGC|5m|{ts.isoformat()}",
        timestamp=ts,
        open=Decimal(open_px),
        high=Decimal(high_px),
        low=Decimal(low_px),
        close=Decimal(close_px),
        session=session,
        atr=Decimal(atr),
        vwap=Decimal(vwap),
        turn_ema_fast=Decimal(ema_fast),
        turn_ema_slow=Decimal(ema_slow),
        velocity=Decimal(velocity),
        velocity_delta=Decimal(velocity_delta),
        bar_range=Decimal(bar_range),
        vol_ratio=Decimal(vol_ratio),
    )


def test_turn_research_builds_dataset_and_summaries() -> None:
    base = datetime.fromisoformat("2026-03-13T18:00:00-04:00")
    bars = [_bar(base + timedelta(minutes=5 * idx), "100", "101", "99", "100") for idx in range(35)]

    bars[4] = _bar(base + timedelta(minutes=20), "99", "100", "95", "99", session="ASIA")
    bars[5] = _bar(base + timedelta(minutes=25), "96", "101", "94", "100", session="ASIA")
    bars[6] = _bar(base + timedelta(minutes=30), "100", "103", "99", "102", session="ASIA")
    bars[7] = _bar(base + timedelta(minutes=35), "102", "106", "101", "105", session="ASIA")
    bars[8] = _bar(base + timedelta(minutes=40), "105", "108", "104", "107", session="ASIA")
    bars[18] = _bar(base + timedelta(minutes=90), "108", "109", "104", "105", session="US")
    bars[19] = _bar(base + timedelta(minutes=95), "105", "106", "100", "101", session="US")
    bars[20] = _bar(base + timedelta(minutes=100), "101", "102", "95", "96", session="US")

    ledger = [
        TurnLedgerRow(
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

    rows = build_turn_dataset_rows(bars, ledger)
    assert rows
    assert any(row.material_turn for row in rows)
    assert any(row.participation_classification in {"no_trade", "late_entry", "poor_entry", "good_entry"} for row in rows)
    assert all(row.derivative_bucket for row in rows)
    assert all(row.session_phase for row in rows)
    assert all(row.atr >= 0 for row in rows)

    summary = build_turn_summary(rows)
    assert "missed_turn_count" in summary
    assert "first_plus_second_derivative_materially_better" in summary

    derivative_rows = build_derivative_bin_rows(rows)
    missed_rows = build_missed_turns_by_derivative_bucket_rows(rows)
    entry_quality_rows = build_entry_quality_by_derivative_bucket_rows(rows)
    assert isinstance(derivative_rows, list)
    assert isinstance(missed_rows, list)
    assert isinstance(entry_quality_rows, list)


def test_replay_turn_research_writes_mode_aware_summary_metadata(tmp_path: Path) -> None:
    replay_db_path = tmp_path / "replay.sqlite3"
    ledger_path = tmp_path / "trade_ledger.csv"
    summary_path = tmp_path / "historical_playback_test.summary.json"
    connection = sqlite3.connect(replay_db_path)
    try:
        connection.executescript(
            """
            create table bars (
              bar_id text primary key,
              timestamp text not null,
              open text not null,
              high text not null,
              low text not null,
              close text not null,
              session_asia integer not null,
              session_london integer not null,
              session_us integer not null,
              ticker text not null,
              timeframe text not null
            );
            create table features (
              bar_id text primary key,
              payload_json text
            );
            """
        )
        base = datetime.fromisoformat("2026-03-13T18:00:00-04:00")
        bars = [_bar(base + timedelta(minutes=5 * idx), "100", "101", "99", "100") for idx in range(35)]
        for bar in bars:
            connection.execute(
                "insert into bars (bar_id, timestamp, open, high, low, close, session_asia, session_london, session_us, ticker, timeframe) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    bar.bar_id,
                    bar.timestamp.isoformat(),
                    str(bar.open),
                    str(bar.high),
                    str(bar.low),
                    str(bar.close),
                    1 if bar.session == "ASIA" else 0,
                    1 if bar.session == "LONDON" else 0,
                    1 if bar.session == "US" else 0,
                    "MGC",
                    "15m",
                ),
            )
            connection.execute(
                "insert into features (bar_id, payload_json) values (?, ?)",
                (
                    bar.bar_id,
                    json.dumps(
                        {
                            "atr": "5",
                            "vwap": "100",
                            "turn_ema_fast": "100",
                            "turn_ema_slow": "99.5",
                            "velocity": "0.5",
                            "velocity_delta": "0.1",
                            "bar_range": "5",
                            "vol_ratio": "1.1",
                        }
                    ),
                ),
            )
        connection.commit()
    finally:
        connection.close()

    ledger_path.write_text(
        "\n".join(
            [
                "trade_id,direction,entry_ts,entry_px,exit_ts,exit_px,qty,gross_pnl,fees,slippage,net_pnl,exit_reason,setup_family,entry_session,exit_session",
                "1,LONG,2026-03-13T18:40:00-04:00,107,2026-03-13T18:50:00-04:00,103,1,-20,0,0,-20,LONG_INTEGRITY_FAIL,firstBullSnapTurn,ASIA,ASIA",
            ]
        ),
        encoding="utf-8",
    )
    summary_path.write_text(
        json.dumps(
            {
                "replay_db_path": str(replay_db_path),
                "trade_ledger_path": str(ledger_path),
                "environment_mode": "research_execution_mode",
                "structural_signal_timeframe": "15m",
                "execution_timeframe": "1m",
                "artifact_timeframe": "15m",
            }
        ),
        encoding="utf-8",
    )

    outputs = build_and_write_replay_turn_research(summary_path)
    turn_summary = json.loads(Path(outputs["turn_summary_path"]).read_text(encoding="utf-8"))

    assert turn_summary["study_mode"] == "research_execution_mode"
    assert turn_summary["timeframe_truth"]["structural_signal_timeframe"] == "15m"
    assert turn_summary["timeframe_truth"]["execution_timeframe"] == "1m"
    assert turn_summary["timeframe_truth"]["artifact_timeframe"] == "15m"
    assert turn_summary["source_replay_timeframe"] == "15m"
