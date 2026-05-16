from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import mgc_v05l.app.asia_london_participation_runtime as asia_runtime
from mgc_v05l.domain.enums import LongEntryFamily, PositionSide, ShortEntryFamily, StrategyStatus
from mgc_v05l.domain.models import Bar
from mgc_v05l.domain.models import StrategyState


def _bar(symbol: str, end_ts: datetime, *, open_px: str, high_px: str, low_px: str, close_px: str, volume: int = 100) -> Bar:
    return Bar(
        bar_id=f"{symbol}|5m|{end_ts.astimezone(ZoneInfo('UTC')).isoformat()}",
        symbol=symbol,
        timeframe="5m",
        start_ts=end_ts - timedelta(minutes=5),
        end_ts=end_ts,
        open=Decimal(open_px),
        high=Decimal(high_px),
        low=Decimal(low_px),
        close=Decimal(close_px),
        volume=volume,
        is_final=True,
        session_asia=True,
        session_london=False,
        session_us=False,
        session_allowed=True,
    )


def _long_state(*, entry_bar_id: str, entry_timestamp: datetime) -> StrategyState:
    return StrategyState(
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        broker_position_qty=1,
        internal_position_qty=1,
        entry_price=Decimal("100.0"),
        entry_timestamp=entry_timestamp,
        entry_bar_id=entry_bar_id,
        long_entry_family=LongEntryFamily.K,
        bars_in_trade=1,
        long_be_armed=False,
        short_be_armed=False,
        last_swing_low=None,
        last_swing_high=None,
        asia_reclaim_bar_low=None,
        asia_reclaim_bar_high=None,
        asia_reclaim_bar_vwap=None,
        bars_since_bull_snap=None,
        bars_since_bear_snap=None,
        bars_since_asia_reclaim=None,
        bars_since_asia_vwap_signal=None,
        bars_since_long_setup=None,
        bars_since_short_setup=None,
        last_signal_bar_id=entry_bar_id,
        last_order_intent_id=None,
        open_broker_order_id=None,
        entries_enabled=True,
        exits_enabled=True,
        operator_halt=False,
        same_underlying_entry_hold=False,
        same_underlying_hold_reason=None,
        reconcile_required=False,
        fault_code=None,
        updated_at=entry_timestamp,
        short_entry_family=ShortEntryFamily.NONE,
    )


def _participation_engine(*, symbol: str = "GC", adopted: bool = False):
    engine = object.__new__(asia_runtime.AsiaLondonParticipationStrategyEngine)
    engine._lane_spec = SimpleNamespace(symbol=symbol)
    engine._runtime_definition = asia_runtime.ASIA_LONDON_RUNTIME_BY_SOURCE[asia_runtime.GC_ASIA_LONDON_LONG_V5_SOURCE]
    engine._primary_context_timeframe = "5m"
    if adopted:
        engine._repositories = SimpleNamespace(
            fills=SimpleNamespace(
                list_all=lambda: [
                    {
                        "broker_order_id": "adopted-broker-truth-GC-GCM6",
                        "intent_type": "BUY_TO_OPEN",
                        "order_intent_id": "GC|5m|2026-05-12T23:20:00+00:00|BUY_TO_OPEN",
                    }
                ]
            )
        )
    else:
        engine._repositories = None
    return engine


def _long_hold_bars() -> list[Bar]:
    start = datetime(2026, 5, 12, 19, 0, tzinfo=ZoneInfo("America/New_York"))
    bars = [
        _bar("GC", start + timedelta(minutes=5 * index), open_px="100.4", high_px="100.8", low_px="100.0", close_px="100.5")
        for index in range(4)
    ]
    bars.extend(
        [
            _bar("GC", start + timedelta(minutes=20), open_px="100.5", high_px="101.0", low_px="100.4", close_px="100.8"),
            _bar("GC", start + timedelta(minutes=25), open_px="100.8", high_px="101.0", low_px="99.8", close_px="99.8"),
            _bar("GC", start + timedelta(minutes=30), open_px="100.3", high_px="101.0", low_px="100.2", close_px="100.5"),
            _bar("GC", start + timedelta(minutes=35), open_px="100.5", high_px="100.7", low_px="99.7", close_px="99.7"),
        ]
    )
    return bars


def test_asia_london_live_observation_writes_predicate_and_near_miss_artifacts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("MGC_ASIA_LONDON_INSTRUMENTATION_ENABLED", "1")
    monkeypatch.setenv("MGC_ASIA_LONDON_INSTRUMENTATION_DIR", str(tmp_path))
    definition = asia_runtime.ASIA_LONDON_RUNTIME_BY_SOURCE[asia_runtime.GC_ASIA_LONDON_LONG_V5_SOURCE]
    start = datetime(2026, 4, 29, 18, 5, tzinfo=ZoneInfo("America/New_York"))
    segment_bars = [
        _bar("GC", start + timedelta(minutes=5 * index), open_px="100.0", high_px="100.5", low_px="99.8", close_px="100.2")
        for index in range(9)
    ]
    # Make the final bar a dip-reclaim candidate on the strict latest-bar decision.
    segment_bars[-2] = _bar("GC", start + timedelta(minutes=35), open_px="100.2", high_px="100.4", low_px="99.9", close_px="100.0")
    segment_bars[-1] = _bar("GC", start + timedelta(minutes=40), open_px="100.0", high_px="100.8", low_px="100.0", close_px="100.7")
    row = asia_runtime._build_asia_london_live_observation(  # noqa: SLF001
        lane_id="gc_1x_asia_london_participation_long",
        definition=definition,
        current_bar=segment_bars[-1],
        session_label=asia_runtime.ENTRY_SEGMENT,
        segment_bars=segment_bars,
        strict_gate_pass=True,
        strict_gate_fail_reason=None,
        entry_index=len(segment_bars) - 1,
        entry_reason="dip_reclaim_or_bar8",
        floor_reason=None,
    )

    asia_runtime._record_asia_london_live_observation(row)  # noqa: SLF001

    predicate_rows = [
        json.loads(line)
        for line in (tmp_path / "asia_london_live_predicate_trace.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert predicate_rows[-1]["candidate_score_bucket"] == "A+"
    assert predicate_rows[-1]["current_strict_candidate"] is True
    near_miss_rows = list(csv.DictReader((tmp_path / "asia_london_live_near_miss_trace.csv").open(encoding="utf-8")))
    assert near_miss_rows[-1]["candidate_score_bucket"] == "A+"
    score_rows = list(csv.DictReader((tmp_path / "asia_london_score_bucket_live_observation.csv").open(encoding="utf-8")))
    assert score_rows[-1]["research_score_candidate"] == "True"


def test_asia_london_live_observation_ignores_instrumentation_write_timeouts(monkeypatch) -> None:
    monkeypatch.setenv("MGC_ASIA_LONDON_INSTRUMENTATION_ENABLED", "1")
    row = {
        "timestamp": "2026-04-30T15:54:08.920238+00:00",
        "lane_id": "gc_1x_asia_london_participation_long",
        "instrument": "GC",
        "session_label": asia_runtime.ENTRY_SEGMENT,
        "strict_gate_pass": False,
        "strict_gate_fail_reason": "timeout_probe",
        "predicates_passed": 1,
        "predicate_count": 3,
        "near_miss_score": 0.333333,
        "candidate_score_bucket": "rejected",
        "current_strict_candidate": False,
        "research_score_candidate": False,
        "entry_reason": "no_entry",
        "floor_reason": None,
        "predicate_results": {"in_entry_segment": True, "setup_bars_ready": False},
    }

    def _raise_timeout(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise TimeoutError("disk write stalled")

    monkeypatch.setattr(asia_runtime, "_append_jsonl_record", _raise_timeout)
    monkeypatch.setattr(asia_runtime, "_append_csv_record", _raise_timeout)

    asia_runtime._record_asia_london_live_observation(row)  # noqa: SLF001


def test_participation_exit_uses_current_bar_when_prior_stop_was_stale() -> None:
    engine = _participation_engine()
    bars = _long_hold_bars()
    engine._bar_history = bars
    state = _long_state(entry_bar_id=bars[4].bar_id, entry_timestamp=bars[4].end_ts)

    intent = engine._participation_exit_intent(bar=bars[-1], state=state)  # noqa: SLF001

    assert intent is not None
    assert intent.intent_type.value == "SELL_TO_CLOSE"
    assert intent.reason_code == "asia_london_initial_stop"
    assert intent.bar_id == bars[-1].bar_id


def test_participation_exit_catches_up_adopted_prior_stop() -> None:
    engine = _participation_engine(adopted=True)
    bars = _long_hold_bars()[:-1]
    bars.append(
        _bar(
            "GC",
            bars[-1].end_ts + timedelta(minutes=5),
            open_px="100.6",
            high_px="101.0",
            low_px="100.4",
            close_px="100.7",
        )
    )
    engine._bar_history = bars
    state = _long_state(entry_bar_id=bars[4].bar_id, entry_timestamp=bars[4].end_ts)

    intent = engine._participation_exit_intent(bar=bars[-1], state=state)  # noqa: SLF001

    assert intent is not None
    assert intent.intent_type.value == "SELL_TO_CLOSE"
    assert intent.reason_code == "asia_london_adopted_initial_stop_catchup"


def test_participation_exit_flattens_segment_overrun() -> None:
    engine = _participation_engine()
    bars = _long_hold_bars()[:-1]
    current = _bar(
        "GC",
        datetime(2026, 5, 13, 8, 25, tzinfo=ZoneInfo("America/New_York")),
        open_px="101.0",
        high_px="101.3",
        low_px="100.8",
        close_px="101.1",
    )
    engine._bar_history = [*bars, current]
    state = _long_state(entry_bar_id=bars[4].bar_id, entry_timestamp=bars[4].end_ts)

    intent = engine._participation_exit_intent(bar=current, state=state)  # noqa: SLF001

    assert intent is not None
    assert intent.intent_type.value == "SELL_TO_CLOSE"
    assert intent.reason_code == "asia_london_segment_overrun"
