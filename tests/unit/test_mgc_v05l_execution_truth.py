from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from mgc_v05l.app.execution_truth import (
    AUTHORITATIVE_INTRABAR_ENTRY_ONLY,
    BASELINE_PARITY_ONLY,
    CURRENT_CANDLE_VWAP,
    FULL_AUTHORITATIVE_LIFECYCLE,
    HYBRID_ENTRY_BASELINE_EXIT_TRUTH,
    HYBRID_AUTHORITATIVE_ENTRY_BASELINE_EXIT,
    UNSUPPORTED_ENTRY_MODEL,
    ExecutionTruthEmitterContext,
    resolve_execution_truth,
)
from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.config_models.settings import EnvironmentMode, ExecutionTimeframeRole
from mgc_v05l.domain.models import Bar


def _bar(
    *,
    bar_id: str,
    timeframe: str,
    start_ts: datetime,
    end_ts: datetime,
    open_price: str,
    high_price: str,
    low_price: str,
    close_price: str,
    session_asia: bool = True,
) -> Bar:
    return Bar(
        bar_id=bar_id,
        symbol="MGC",
        timeframe=timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        open=Decimal(open_price),
        high=Decimal(high_price),
        low=Decimal(low_price),
        close=Decimal(close_price),
        volume=100,
        is_final=True,
        session_asia=session_asia,
        session_london=False,
        session_us=not session_asia,
        session_allowed=True,
    )


def test_asia_vwap_execution_truth_emitter_emits_authoritative_intrabar_entry_and_hybrid_lifecycle() -> None:
    settings = load_settings_from_files([Path("config/base.yaml")]).model_copy(
        update={
            "symbol": "MGC",
            "timeframe": "5m",
            "environment_mode": EnvironmentMode.RESEARCH_EXECUTION,
            "structural_signal_timeframe": "5m",
            "execution_timeframe": "1m",
            "artifact_timeframe": "5m",
            "execution_timeframe_role": ExecutionTimeframeRole.EXECUTION_DETAIL_ONLY,
            "enable_asia_vwap_longs": True,
            "require_acceptance_close_above_vwap": True,
        }
    )
    start = datetime.fromisoformat("2026-03-18T01:00:00+00:00")
    bars = [
        _bar(
            bar_id="reclaim",
            timeframe="5m",
            start_ts=start,
            end_ts=start + timedelta(minutes=5),
            open_price="100.0",
            high_price="100.6",
            low_price="99.7",
            close_price="100.5",
        ),
        _bar(
            bar_id="hold",
            timeframe="5m",
            start_ts=start + timedelta(minutes=5),
            end_ts=start + timedelta(minutes=10),
            open_price="100.4",
            high_price="100.7",
            low_price="100.2",
            close_price="100.45",
        ),
        _bar(
            bar_id="acceptance",
            timeframe="5m",
            start_ts=start + timedelta(minutes=10),
            end_ts=start + timedelta(minutes=15),
            open_price="100.35",
            high_price="100.95",
            low_price="100.3",
            close_price="100.9",
        ),
        _bar(
            bar_id="exit",
            timeframe="5m",
            start_ts=start + timedelta(minutes=15),
            end_ts=start + timedelta(minutes=20),
            open_price="101.0",
            high_price="101.4",
            low_price="100.8",
            close_price="101.3",
        ),
    ]
    source_bars = [
        _bar(
            bar_id=f"acceptance-1m-{index}",
            timeframe="1m",
            start_ts=start + timedelta(minutes=10 + index),
            end_ts=start + timedelta(minutes=11 + index),
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
        )
        for index, (open_price, high_price, low_price, close_price) in enumerate(
            [
                ("100.35", "100.5", "100.3", "100.45"),
                ("100.45", "100.65", "100.4", "100.58"),
                ("100.58", "100.9", "100.55", "100.82"),
                ("100.82", "100.93", "100.78", "100.9"),
                ("100.9", "100.95", "100.84", "100.88"),
            ]
        )
    ]
    rows = [
        {
            "bar_id": "reclaim",
            "timestamp": (start + timedelta(minutes=5)).isoformat(),
            "start_timestamp": start.isoformat(),
            "end_timestamp": (start + timedelta(minutes=5)).isoformat(),
            "entry_source_family": "asiaVWAPLongSignal",
            "fill_markers": [],
        },
        {
            "bar_id": "hold",
            "timestamp": (start + timedelta(minutes=10)).isoformat(),
            "start_timestamp": (start + timedelta(minutes=5)).isoformat(),
            "end_timestamp": (start + timedelta(minutes=10)).isoformat(),
            "entry_source_family": "asiaVWAPLongSignal",
            "fill_markers": [],
        },
        {
            "bar_id": "acceptance",
            "timestamp": (start + timedelta(minutes=15)).isoformat(),
            "start_timestamp": (start + timedelta(minutes=10)).isoformat(),
            "end_timestamp": (start + timedelta(minutes=15)).isoformat(),
            "entry_source_family": "asiaVWAPLongSignal",
            "fill_markers": [],
        },
        {
            "bar_id": "exit",
            "timestamp": (start + timedelta(minutes=20)).isoformat(),
            "start_timestamp": (start + timedelta(minutes=15)).isoformat(),
            "end_timestamp": (start + timedelta(minutes=20)).isoformat(),
            "entry_source_family": "asiaVWAPLongSignal",
            "fill_markers": [
                {
                    "kind": "fill",
                    "intent_type": "SELL_TO_CLOSE",
                    "side": "LONG",
                    "price": "101.25",
                    "timestamp": (start + timedelta(minutes=20)).isoformat(),
                    "is_entry": False,
                    "is_exit": True,
                }
            ],
        },
    ]
    signal_by_bar_id = {
        "reclaim": {"asia_reclaim_bar_raw": True},
        "hold": {"asia_hold_bar_ok": True, "asia_hold_bar": True},
        "acceptance": {
            "asia_acceptance_bar": True,
            "asia_acceptance_bar_ok": True,
            "asia_vwap_long_signal": True,
            "long_entry_source": "asiaVWAPLongSignal",
        },
    }
    feature_by_bar_id = {"reclaim": {"vwap": Decimal("100.4")}}

    result = resolve_execution_truth(
        ExecutionTruthEmitterContext(
            settings=settings,
            bars=bars,
            source_bars=source_bars,
            rows=rows,
            signal_by_bar_id=signal_by_bar_id,
            feature_by_bar_id=feature_by_bar_id,
            point_value=None,
            strategy_family="LEGACY_RUNTIME",
            standalone_strategy_id="legacy_runtime__MGC",
            instrument="MGC",
            requested_entry_model=CURRENT_CANDLE_VWAP,
        )
    )

    assert result.execution_truth_emitter == "asia_vwap_reclaim_emitter"
    assert result.entry_model_supported is True
    assert result.authoritative_intrabar_available is True
    assert result.authoritative_entry_truth_available is True
    assert result.authoritative_exit_truth_available is True
    assert result.authoritative_trade_lifecycle_available is True
    assert result.pnl_truth_basis == HYBRID_ENTRY_BASELINE_EXIT_TRUTH
    assert result.lifecycle_truth_class == HYBRID_AUTHORITATIVE_ENTRY_BASELINE_EXIT
    assert {event["execution_event_type"] for event in result.authoritative_execution_events} >= {
        "ENTRY_ARMED",
        "ENTRY_CONFIRMED",
        "ENTRY_EXECUTED",
        "EXIT_TRIGGERED",
    }
    assert result.authoritative_trade_lifecycle_records
    assert result.authoritative_trade_lifecycle_records[0]["family"] == "asiaVWAPLongSignal"


def test_execution_truth_marks_unsupported_non_atp_current_candle_requests_without_fallback() -> None:
    settings = load_settings_from_files([Path("config/base.yaml")]).model_copy(
        update={
            "symbol": "MGC",
            "timeframe": "5m",
            "environment_mode": EnvironmentMode.RESEARCH_EXECUTION,
            "structural_signal_timeframe": "5m",
            "execution_timeframe": "1m",
            "artifact_timeframe": "5m",
            "execution_timeframe_role": ExecutionTimeframeRole.EXECUTION_DETAIL_ONLY,
            "enable_asia_vwap_longs": False,
        }
    )
    start = datetime.fromisoformat("2026-03-18T13:00:00+00:00")
    bars = [
        _bar(
            bar_id="k-setup",
            timeframe="5m",
            start_ts=start,
            end_ts=start + timedelta(minutes=5),
            open_price="100",
            high_price="101",
            low_price="99.5",
            close_price="100.9",
            session_asia=False,
        )
    ]
    rows = [
        {
            "bar_id": "k-setup",
            "timestamp": (start + timedelta(minutes=5)).isoformat(),
            "start_timestamp": start.isoformat(),
            "end_timestamp": (start + timedelta(minutes=5)).isoformat(),
            "entry_source_family": "firstBullSnapTurn",
            "fill_markers": [],
        }
    ]

    result = resolve_execution_truth(
        ExecutionTruthEmitterContext(
            settings=settings,
            bars=bars,
            source_bars=[],
            rows=rows,
            signal_by_bar_id={},
            feature_by_bar_id={},
            point_value=None,
            strategy_family="LEGACY_RUNTIME",
            standalone_strategy_id="legacy_runtime__MGC",
            instrument="MGC",
            requested_entry_model=CURRENT_CANDLE_VWAP,
        )
    )

    assert result.entry_model_supported is False
    assert result.pnl_truth_basis == UNSUPPORTED_ENTRY_MODEL
    assert result.lifecycle_truth_class == UNSUPPORTED_ENTRY_MODEL
    assert result.authoritative_entry_truth_available is False
    assert result.authoritative_exit_truth_available is False
    assert result.authoritative_trade_lifecycle_available is False
    assert result.authoritative_execution_events == ()
    assert result.active_entry_model == CURRENT_CANDLE_VWAP
    assert result.unsupported_reason
    first_bull_snap_capability = next(
        row for row in result.capability_rows if row["subject"] == "firstBullSnapTurn"
    )
    assert first_bull_snap_capability["supported_entry_models"] == ["BASELINE_NEXT_BAR_OPEN"]
