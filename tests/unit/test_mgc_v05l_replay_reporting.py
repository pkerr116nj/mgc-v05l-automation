from datetime import datetime, timedelta
from decimal import Decimal

from mgc_v05l.app.replay_reporting import (
    build_breakdown_rows,
    build_drawdown_curve_rows,
    build_equity_curve_rows,
    build_exit_reason_breakdown_rows,
    build_hold_time_summary,
    build_mae_mfe_summary,
    build_rolling_performance_rows,
    build_session_lookup,
    build_summary_metrics,
    build_trade_efficiency_rows,
    ReplayFeatureContext,
    build_trade_ledger,
    write_breakdown_csv,
    write_dict_rows_csv,
    write_drawdown_curve_csv,
    write_equity_curve_csv,
    write_hold_time_summary_json,
    write_mae_mfe_summary_json,
    write_rolling_performance_csv,
    write_summary_metrics_json,
    write_trade_ledger_csv,
)
from mgc_v05l.app.session_phase_labels import label_session_phase
from mgc_v05l.domain.models import Bar


def _build_bar(start_ts: datetime, session: str) -> Bar:
    return Bar(
        bar_id=f"MGC|5m|{(start_ts + timedelta(minutes=5)).astimezone().isoformat()}",
        symbol="MGC",
        timeframe="5m",
        start_ts=start_ts,
        end_ts=start_ts + timedelta(minutes=5),
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=100,
        is_final=True,
        session_asia=session == "ASIA",
        session_london=session == "LONDON",
        session_us=session == "US",
        session_allowed=session in {"ASIA", "LONDON", "US"},
    )


def test_replay_reporting_builds_trade_ledger_summary_and_artifacts(tmp_path) -> None:
    entry_1 = datetime.fromisoformat("2026-03-13T18:00:00-04:00")
    exit_1 = datetime.fromisoformat("2026-03-13T18:05:00-04:00")
    entry_2 = datetime.fromisoformat("2026-03-13T19:30:00-04:00")
    exit_2 = datetime.fromisoformat("2026-03-13T19:35:00-04:00")
    bars = [
        _build_bar(entry_1, "ASIA"),
        _build_bar(exit_1, "ASIA"),
        _build_bar(entry_2, "US"),
        _build_bar(exit_2, "US"),
    ]
    session_lookup = build_session_lookup(bars)

    order_intents = [
        {
            "order_intent_id": "entry-1",
            "bar_id": "bar-1",
            "symbol": "MGC",
            "intent_type": "BUY_TO_OPEN",
            "quantity": 1,
            "created_at": entry_1.isoformat(),
            "reason_code": "firstBullSnapTurn",
            "broker_order_id": "broker-1",
            "order_status": "FILLED",
        },
        {
            "order_intent_id": "exit-1",
            "bar_id": "bar-2",
            "symbol": "MGC",
            "intent_type": "SELL_TO_CLOSE",
            "quantity": 1,
            "created_at": exit_1.isoformat(),
            "reason_code": "LONG_TIME_EXIT",
            "broker_order_id": "broker-2",
            "order_status": "FILLED",
        },
        {
            "order_intent_id": "entry-2",
            "bar_id": "bar-3",
            "symbol": "MGC",
            "intent_type": "SELL_TO_OPEN",
            "quantity": 1,
            "created_at": entry_2.isoformat(),
            "reason_code": "firstBearSnapTurn",
            "broker_order_id": "broker-3",
            "order_status": "FILLED",
        },
        {
            "order_intent_id": "exit-2",
            "bar_id": "bar-4",
            "symbol": "MGC",
            "intent_type": "BUY_TO_CLOSE",
            "quantity": 1,
            "created_at": exit_2.isoformat(),
            "reason_code": "SHORT_STOP",
            "broker_order_id": "broker-4",
            "order_status": "FILLED",
        },
    ]
    fills = [
        {
            "order_intent_id": "entry-1",
            "intent_type": "BUY_TO_OPEN",
            "order_status": "FILLED",
            "fill_timestamp": entry_1.isoformat(),
            "fill_price": "100",
            "broker_order_id": "broker-1",
        },
        {
            "order_intent_id": "exit-1",
            "intent_type": "SELL_TO_CLOSE",
            "order_status": "FILLED",
            "fill_timestamp": exit_1.isoformat(),
            "fill_price": "102",
            "broker_order_id": "broker-2",
        },
        {
            "order_intent_id": "entry-2",
            "intent_type": "SELL_TO_OPEN",
            "order_status": "FILLED",
            "fill_timestamp": entry_2.isoformat(),
            "fill_price": "105",
            "broker_order_id": "broker-3",
        },
        {
            "order_intent_id": "exit-2",
            "intent_type": "BUY_TO_CLOSE",
            "order_status": "FILLED",
            "fill_timestamp": exit_2.isoformat(),
            "fill_price": "106",
            "broker_order_id": "broker-4",
        },
    ]

    ledger = build_trade_ledger(order_intents, fills, session_lookup, point_value=Decimal("10"))
    feature_context = {
        "bar-1": ReplayFeatureContext(
            atr=Decimal("2"),
            turn_ema_fast=Decimal("99"),
            turn_ema_slow=Decimal("98"),
            vwap=Decimal("100"),
        ),
        "bar-3": ReplayFeatureContext(
            atr=Decimal("2"),
            turn_ema_fast=Decimal("106"),
            turn_ema_slow=Decimal("107"),
            vwap=Decimal("105"),
        ),
    }
    ledger = build_trade_ledger(
        order_intents,
        fills,
        session_lookup,
        point_value=Decimal("10"),
        bars=bars,
        feature_context_by_bar_id=feature_context,
    )
    assert len(ledger) == 2
    assert ledger[0].direction == "LONG"
    assert ledger[0].gross_pnl == Decimal("20")
    assert ledger[0].entry_session == "ASIA"
    assert ledger[0].entry_session_phase == "SESSION_RESET_1800"
    assert ledger[0].bars_held == 1
    assert ledger[0].entry_distance_fast_ema_atr == Decimal("0.5")
    assert ledger[1].direction == "SHORT"
    assert ledger[1].gross_pnl == Decimal("-10")
    assert ledger[1].exit_reason == "SHORT_STOP"
    assert ledger[1].entry_session_phase == "ASIA_EARLY"
    assert ledger[1].entry_distance_vwap_atr == Decimal("0")

    summary = build_summary_metrics(ledger)
    mae_mfe_summary = build_mae_mfe_summary(ledger)
    hold_time_summary = build_hold_time_summary(ledger)
    assert summary.total_net_pnl == Decimal("10")
    assert summary.win_rate == Decimal("0.5")
    assert summary.avg_winner == Decimal("20")
    assert summary.avg_loser == Decimal("-10")
    assert summary.expectancy == Decimal("5")
    assert summary.max_drawdown == Decimal("10")
    assert summary.number_of_trades == 2
    assert summary.pnl_by_signal_family["firstBullSnapTurn"] == Decimal("20")
    assert summary.pnl_by_session["ASIA"] == Decimal("20")
    assert summary.pnl_by_session["US"] == Decimal("-10")
    assert mae_mfe_summary.average_mfe >= Decimal("0")
    assert hold_time_summary.average_bars_held == Decimal("1")

    equity_curve = build_equity_curve_rows(ledger)
    assert equity_curve[-1]["cumulative_net_pnl"] == "10"
    assert equity_curve[-1]["drawdown"] == "10"
    drawdown_curve = build_drawdown_curve_rows(ledger)
    assert drawdown_curve[-1]["peak_equity"] == "20"
    assert drawdown_curve[-1]["drawdown"] == "10"
    rolling_rows = build_rolling_performance_rows(ledger, window_size=2)
    assert rolling_rows[-1]["rolling_win_rate"] == "0.5"
    assert rolling_rows[-1]["rolling_expectancy"] == "5"
    signal_rows = build_breakdown_rows(ledger, key_name="setup_family")
    session_rows = build_breakdown_rows(ledger, key_name="entry_session")
    direction_rows = build_breakdown_rows(ledger, key_name="direction")
    exit_rows = build_exit_reason_breakdown_rows(ledger)
    efficiency_rows = build_trade_efficiency_rows(ledger, key_name="setup_family")
    assert signal_rows[0].bucket == "firstBearSnapTurn"
    assert session_rows[0].bucket == "ASIA"
    assert direction_rows[0].bucket == "LONG"
    assert exit_rows[0].bucket == "LONG_TIME_EXIT"
    assert efficiency_rows[0]["bucket"] == "firstBearSnapTurn"

    ledger_path = write_trade_ledger_csv(ledger, tmp_path / "trade_ledger.csv")
    summary_path = write_summary_metrics_json(
        summary,
        tmp_path / "summary_metrics.json",
        point_value=Decimal("10"),
        fee_per_fill=Decimal("0"),
        slippage_per_fill=Decimal("0"),
    )
    equity_path = write_equity_curve_csv(equity_curve, tmp_path / "equity_curve.csv")
    signal_path = write_breakdown_csv(signal_rows, tmp_path / "pnl_by_signal_family.csv")
    session_path = write_breakdown_csv(session_rows, tmp_path / "pnl_by_session.csv")
    direction_path = write_breakdown_csv(direction_rows, tmp_path / "pnl_by_direction.csv")
    drawdown_path = write_drawdown_curve_csv(drawdown_curve, tmp_path / "drawdown_curve.csv")
    rolling_path = write_rolling_performance_csv(rolling_rows, tmp_path / "rolling_performance.csv")
    mae_mfe_path = write_mae_mfe_summary_json(mae_mfe_summary, tmp_path / "mae_mfe_summary.json")
    hold_time_path = write_hold_time_summary_json(hold_time_summary, tmp_path / "hold_time_summary.json")
    exit_path = write_breakdown_csv(exit_rows, tmp_path / "exit_reason_breakdown.csv")
    efficiency_path = write_dict_rows_csv(efficiency_rows, tmp_path / "trade_efficiency_by_signal_family.csv")

    assert ledger_path.exists()
    assert summary_path.exists()
    assert equity_path.exists()
    assert signal_path.exists()
    assert session_path.exists()
    assert direction_path.exists()
    assert drawdown_path.exists()
    assert rolling_path.exists()
    assert mae_mfe_path.exists()
    assert hold_time_path.exists()
    assert exit_path.exists()
    assert efficiency_path.exists()
    assert "entry_session_phase" in ledger_path.read_text(encoding="utf-8")
    assert '"total_net_pnl": 10.0' in summary_path.read_text(encoding="utf-8")
    assert "trade_id,exit_ts,net_pnl,cumulative_net_pnl,drawdown" in equity_path.read_text(encoding="utf-8")
    assert "bucket,trade_count,wins,losses,win_rate,total_net_pnl" in signal_path.read_text(encoding="utf-8")
    assert "trade_id,exit_ts,cumulative_net_pnl,peak_equity,drawdown" in drawdown_path.read_text(encoding="utf-8")
    assert "trade_id,exit_ts,window_size,rolling_win_rate,rolling_expectancy" in rolling_path.read_text(encoding="utf-8")
    assert '"average_mfe"' in mae_mfe_path.read_text(encoding="utf-8")
    assert '"average_bars_held"' in hold_time_path.read_text(encoding="utf-8")


def test_session_phase_labels_cover_requested_research_windows() -> None:
    assert label_session_phase(datetime.fromisoformat("2026-03-13T18:00:00-04:00")) == "SESSION_RESET_1800"
    assert label_session_phase(datetime.fromisoformat("2026-03-13T19:00:00-04:00")) == "ASIA_EARLY"
    assert label_session_phase(datetime.fromisoformat("2026-03-13T21:00:00-04:00")) == "ASIA_LATE"
    assert label_session_phase(datetime.fromisoformat("2026-03-13T03:30:00-04:00")) == "LONDON_OPEN"
    assert label_session_phase(datetime.fromisoformat("2026-03-13T06:00:00-04:00")) == "LONDON_LATE"
    assert label_session_phase(datetime.fromisoformat("2026-03-13T09:15:00-04:00")) == "US_PREOPEN_OPENING"
    assert label_session_phase(datetime.fromisoformat("2026-03-13T09:45:00-04:00")) == "US_CASH_OPEN_IMPULSE"
    assert label_session_phase(datetime.fromisoformat("2026-03-13T10:15:00-04:00")) == "US_OPEN_LATE"
    assert label_session_phase(datetime.fromisoformat("2026-03-13T11:00:00-04:00")) == "US_MIDDAY"
    assert label_session_phase(datetime.fromisoformat("2026-03-13T14:30:00-04:00")) == "US_LATE"
