from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import mgc_v05l.app.asia_london_participation_runtime as asia_runtime
import mgc_v05l.app.gc_mgc_forced_session_runtime as gold_runtime
import mgc_v05l.app.index_futures_forced_session_runtime as index_runtime
from mgc_v05l.domain.models import Bar


def _bar(symbol: str, end_ts: datetime, *, timeframe: str = "3m", close: str = "100.20") -> Bar:
    minutes = int(timeframe.removesuffix("m"))
    return Bar(
        bar_id=f"{symbol}|{timeframe}|{end_ts.astimezone(ZoneInfo('UTC')).isoformat()}",
        symbol=symbol,
        timeframe=timeframe,
        start_ts=end_ts - timedelta(minutes=minutes),
        end_ts=end_ts,
        open=Decimal("100.10"),
        high=Decimal("100.90"),
        low=Decimal("100.00"),
        close=Decimal(close),
        volume=100,
        is_final=True,
        session_asia=True,
        session_london=False,
        session_us=False,
        session_allowed=True,
    )


def _op_params() -> dict[str, object]:
    return {
        "operational_maturation_mode": True,
        "operational_maturation_profile": "PAPER_ONLY_OPERATIONAL_MATURATION_V1",
        "operational_maturation_forced_entry_bar": 5,
        "operational_maturation_entry_catchup_bars": 2,
        "operational_maturation_min_setup_range_ticks": 2,
    }


def _packet(bar: Bar) -> SimpleNamespace:
    return SimpleNamespace(bar_id=bar.bar_id)


def test_gold_operational_maturation_bar5_entry_is_opt_in() -> None:
    start = datetime(2026, 5, 19, 19, 0, tzinfo=ZoneInfo("America/New_York"))
    bars = [_bar("MGC", start + timedelta(minutes=3 * index)) for index in range(1, 6)]

    default_engine = object.__new__(gold_runtime.GcMgcForcedSessionStrategyEngine)
    default_engine._lane_spec = SimpleNamespace(symbol="MGC", runtime_overlay_params={}, paper_only=True, live_money_eligible=False)
    default_engine._runtime_definition = gold_runtime.FORCED_SESSION_RUNTIME_BY_SOURCE[gold_runtime.ASIA_EARLY_LONG_SOURCE]
    default_engine._bar_history = bars

    default_signal = default_engine._evaluate_signals(_packet(bars[-1]), [])  # noqa: SLF001
    assert default_signal.long_entry is False

    operational_engine = object.__new__(gold_runtime.GcMgcForcedSessionStrategyEngine)
    operational_engine._lane_spec = SimpleNamespace(
        symbol="MGC",
        runtime_overlay_params=_op_params(),
        paper_only=True,
        live_money_eligible=False,
    )
    operational_engine._runtime_definition = gold_runtime.FORCED_SESSION_RUNTIME_BY_SOURCE[gold_runtime.ASIA_EARLY_LONG_SOURCE]
    operational_engine._bar_history = bars

    operational_signal = operational_engine._evaluate_signals(_packet(bars[-1]), [])  # noqa: SLF001
    assert operational_signal.long_entry is True
    assert operational_signal.long_entry_source == gold_runtime.ASIA_EARLY_LONG_SOURCE


def test_index_operational_maturation_bar5_entry_is_session_scoped() -> None:
    start = datetime(2026, 5, 20, 8, 20, tzinfo=ZoneInfo("America/New_York"))
    bars = [_bar("MNQ", start + timedelta(minutes=3 * index)) for index in range(1, 6)]
    engine = object.__new__(index_runtime.IndexFuturesForcedSessionStrategyEngine)
    engine._lane_spec = SimpleNamespace(
        symbol="MNQ",
        runtime_overlay_params=_op_params(),
        paper_only=True,
        live_money_eligible=False,
    )
    engine._runtime_definition = index_runtime.INDEX_FORCED_SESSION_RUNTIME_BY_SOURCE[index_runtime.INDEX_NY_EARLY_LONG_SOURCE]
    engine._bar_history = bars

    signal = engine._evaluate_signals(_packet(bars[-1]), [])  # noqa: SLF001
    assert signal.long_entry is True
    assert signal.long_entry_source == index_runtime.INDEX_NY_EARLY_LONG_SOURCE

    outside_session_bar = _bar("MNQ", datetime(2026, 5, 20, 13, 0, tzinfo=ZoneInfo("America/New_York")))
    engine._bar_history = [outside_session_bar]
    outside_signal = engine._evaluate_signals(_packet(outside_session_bar), [])  # noqa: SLF001
    assert outside_signal.long_entry is False


def test_asia_london_operational_maturation_accepts_bar5_but_not_session_open() -> None:
    start = datetime(2026, 5, 19, 19, 0, tzinfo=ZoneInfo("America/New_York"))
    bars = [_bar("MNQ", start + timedelta(minutes=3 * index)) for index in range(1, 6)]
    engine = object.__new__(asia_runtime.AsiaLondonParticipationStrategyEngine)
    engine._lane_spec = SimpleNamespace(
        symbol="MNQ",
        lane_id="mnq_test",
        runtime_overlay_params=_op_params(),
        paper_only=True,
        live_money_eligible=False,
    )
    engine._runtime_definition = asia_runtime.ASIA_LONDON_RUNTIME_BY_SOURCE[asia_runtime.NQ_ASIA_LONDON_LONG_V6_SOURCE]
    engine._bar_history = bars

    signal = engine._evaluate_signals(_packet(bars[-1]), [])  # noqa: SLF001
    assert signal.long_entry is True
    assert signal.long_entry_source == asia_runtime.NQ_ASIA_LONDON_LONG_V6_SOURCE

    session_open_start = datetime(2026, 5, 19, 18, 0, tzinfo=ZoneInfo("America/New_York"))
    session_open_bars = [_bar("MNQ", session_open_start + timedelta(minutes=3 * index)) for index in range(1, 6)]
    engine._bar_history = session_open_bars
    session_open_signal = engine._evaluate_signals(_packet(session_open_bars[-1]), [])  # noqa: SLF001
    assert session_open_signal.long_entry is False


def test_operational_maturation_fails_closed_for_live_money_eligible_lane() -> None:
    start = datetime(2026, 5, 19, 19, 0, tzinfo=ZoneInfo("America/New_York"))
    bars = [_bar("MGC", start + timedelta(minutes=3 * index)) for index in range(1, 6)]
    engine = object.__new__(gold_runtime.GcMgcForcedSessionStrategyEngine)
    engine._lane_spec = SimpleNamespace(
        symbol="MGC",
        runtime_overlay_params=_op_params(),
        paper_only=True,
        live_money_eligible=True,
    )
    engine._runtime_definition = gold_runtime.FORCED_SESSION_RUNTIME_BY_SOURCE[gold_runtime.ASIA_EARLY_LONG_SOURCE]
    engine._bar_history = bars

    signal = engine._evaluate_signals(_packet(bars[-1]), [])  # noqa: SLF001
    assert signal.long_entry is False
