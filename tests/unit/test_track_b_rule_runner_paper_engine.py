from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from mgc_v05l.app.track_b_rule_runner_paper_engine import (
    CHANGEOVER_0300_LONG_CONTINUATION_ID,
    CHANGEOVER_0700_MNQ_LONG_CONTINUATION_ID,
    CHANGEOVER_CONTINUATION_SPECS,
    PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_SHORT_ID,
    PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_LONG_ID,
    PAPER_ACTIVE_EVIDENCE_MNQ_US_LONG_ID,
    PAPER_ACTIVE_EVIDENCE_MNQ_US_SHORT_ID,
    PAPER_ACTIVE_EVIDENCE_SPECS,
    TrackBRuleRunnerPaperStrategyEngine,
    US_SESSION_CONTINUATION_SPECS,
    US_SESSION_MNQ_LONG_CONTINUATION_ID,
    _changeover_0300_long_continuation_decision,
    _changeover_long_continuation_decision,
    _is_changeover_0300_long_rule,
    _native_runtime_fallback_spec,
    _paper_active_evidence_decision,
    _promoted_signal_packet_from_native,
    _state_freshness_blocker,
    _us_session_long_continuation_decision,
)
from mgc_v05l.domain.models import Bar
from mgc_v05l.domain.enums import OrderIntentType, PositionSide, ShortEntryFamily, StrategyStatus
from mgc_v05l.domain.models import SignalPacket
from mgc_v05l.strategy.strategy_engine import _empty_signal_packet_payload


def test_rule_runner_paper_engine_requires_timestamp_coherent_input() -> None:
    current = datetime(2026, 5, 27, 8, 0, tzinfo=UTC)

    blocker = _state_freshness_blocker(
        {"candle_timestamp": (current - timedelta(minutes=20)).isoformat()},
        current_bar_end=current,
        config={"timestamp_tolerance_seconds": 360},
    )

    assert blocker == "track_b_rule_runner_input_event_not_timestamp_coherent"


def test_rule_runner_paper_engine_accepts_fresh_timestamp_coherent_input() -> None:
    current = datetime(2026, 5, 27, 8, 0, tzinfo=UTC)

    blocker = _state_freshness_blocker(
        {"candle_timestamp": (current - timedelta(minutes=1)).isoformat()},
        current_bar_end=current,
        config={"timestamp_tolerance_seconds": 360},
    )

    assert blocker is None


class _LaneSpec:
    strategy_family = "LONDON_LATE_PAUSE_RESUME_SHORT_V1"


class _LongLaneSpec:
    strategy_family = "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"


class _ChangeoverLaneSpec:
    strategy_family = CHANGEOVER_0300_LONG_CONTINUATION_ID
    standalone_strategy_id = CHANGEOVER_0300_LONG_CONTINUATION_ID


def _bar(end_et: datetime, *, open_: str, close: str) -> Bar:
    end = end_et.astimezone(UTC)
    start = end - timedelta(minutes=5)
    high = max(Decimal(open_), Decimal(close))
    low = min(Decimal(open_), Decimal(close))
    return Bar(
        bar_id=end.isoformat(),
        symbol="MNQ",
        timeframe="5m",
        start_ts=start,
        end_ts=end,
        open=Decimal(open_),
        high=high,
        low=low,
        close=Decimal(close),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=True,
        session_us=False,
        session_allowed=True,
    )


def test_rule_runner_native_fallback_maps_promoted_short_source() -> None:
    fallback = _native_runtime_fallback_spec({}, _LaneSpec())
    assert fallback is not None
    assert fallback.short_family is ShortEntryFamily.LONDON_LATE_PAUSE_RESUME_SHORT

    payload = _empty_signal_packet_payload("bar-1")
    payload.update(
        {
            "short_entry_raw": True,
            "recent_short_setup": True,
            "short_entry": True,
            "short_entry_source": "londonLatePauseResumeShortTurn",
        }
    )
    native_packet = SignalPacket(**payload)

    promoted_packet = _promoted_signal_packet_from_native("bar-1", native_packet, fallback)

    assert promoted_packet.short_entry is True
    assert promoted_packet.short_entry_source == "LONDON_LATE_PAUSE_RESUME_SHORT_V1"
    assert promoted_packet.long_entry is False


def test_rule_runner_native_fallback_maps_promoted_long_source() -> None:
    fallback = _native_runtime_fallback_spec({}, _LongLaneSpec())
    assert fallback is not None
    assert fallback.direction == "LONG"

    payload = _empty_signal_packet_payload("bar-1")
    payload.update(
        {
            "long_entry_raw": True,
            "recent_long_setup": True,
            "long_entry": True,
            "long_entry_source": "asiaEarlyNormalBreakoutRetestHoldTurn",
        }
    )
    native_packet = SignalPacket(**payload)

    promoted_packet = _promoted_signal_packet_from_native("bar-1", native_packet, fallback)

    assert promoted_packet.long_entry is True
    assert promoted_packet.long_entry_source == "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    assert promoted_packet.short_entry is False


def test_rule_runner_native_fallback_does_not_emit_without_matching_native_source() -> None:
    fallback = _native_runtime_fallback_spec({"rule_mode": "US_DERIVATIVE_BEAR_TURN_V1"}, _LaneSpec())
    assert fallback is not None

    payload = _empty_signal_packet_payload("bar-1")
    payload.update(
        {
            "short_entry_raw": True,
            "recent_short_setup": True,
            "short_entry": True,
            "short_entry_source": "londonLatePauseResumeShortTurn",
        }
    )
    native_packet = SignalPacket(**payload)

    promoted_packet = _promoted_signal_packet_from_native("bar-1", native_packet, fallback)

    assert promoted_packet.short_entry is False
    assert promoted_packet.short_entry_source is None


def test_changeover_0300_rule_is_recognized_from_promoted_strategy_id() -> None:
    assert _is_changeover_0300_long_rule({}, _ChangeoverLaneSpec()) is True
    assert _is_changeover_0300_long_rule({"rule_mode": CHANGEOVER_0300_LONG_CONTINUATION_ID}, object()) is True


def test_changeover_0300_long_accepts_close_above_session_open() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 27, 18, 5, tzinfo=ny), open_="21000", close="21005"),
        _bar(datetime(2026, 5, 28, 3, 0, tzinfo=ny), open_="21025", close="21050"),
    ]

    decision = _changeover_0300_long_continuation_decision(history)

    assert decision["accepted"] is True
    assert decision["primary_blocker"] is None
    assert decision["session_open_price"] == "21000"


def test_changeover_0300_long_rejects_non_changeover_bar() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 27, 18, 5, tzinfo=ny), open_="21000", close="21005"),
        _bar(datetime(2026, 5, 28, 4, 5, tzinfo=ny), open_="21025", close="21050"),
    ]

    decision = _changeover_0300_long_continuation_decision(history)

    assert decision["accepted"] is False
    assert decision["primary_blocker"] == "not_in_changeover_entry_window"


def test_changeover_0300_long_accepts_later_window_bar_with_simple_continuation() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 27, 18, 5, tzinfo=ny), open_="21000", close="21005"),
        _bar(datetime(2026, 5, 28, 3, 5, tzinfo=ny), open_="21025", close="21040"),
        _bar(datetime(2026, 5, 28, 3, 10, tzinfo=ny), open_="21040", close="21050"),
    ]

    decision = _changeover_0300_long_continuation_decision(history)

    assert decision["accepted"] is True
    assert decision["primary_blocker"] is None


def test_changeover_0700_long_accepts_close_above_0300_reference() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 27, 18, 5, tzinfo=ny), open_="21000", close="21005"),
        _bar(datetime(2026, 5, 28, 3, 0, tzinfo=ny), open_="20980", close="20990"),
        _bar(datetime(2026, 5, 28, 7, 0, tzinfo=ny), open_="21010", close="21020"),
    ]

    decision = _changeover_long_continuation_decision(
        history,
        spec=CHANGEOVER_CONTINUATION_SPECS[CHANGEOVER_0700_MNQ_LONG_CONTINUATION_ID],
    )

    assert decision["accepted"] is True
    assert decision["primary_blocker"] is None
    assert decision["changeover_reference_price"] == "20990"


def test_changeover_0700_entry_intent_uses_minimal_rule_context_without_generic_warmup() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 27, 18, 5, tzinfo=ny), open_="21000", close="21005"),
        _bar(datetime(2026, 5, 28, 3, 0, tzinfo=ny), open_="20980", close="20990"),
        _bar(datetime(2026, 5, 28, 7, 0, tzinfo=ny), open_="21010", close="21020"),
    ]
    engine = object.__new__(TrackBRuleRunnerPaperStrategyEngine)
    engine._bar_history = history
    engine._settings = SimpleNamespace(symbol="MNQ", trade_size=1)

    payload = _empty_signal_packet_payload(history[-1].bar_id)
    payload.update(
        {
            "long_entry_raw": True,
            "recent_long_setup": True,
            "long_entry": True,
            "long_entry_source": CHANGEOVER_0700_MNQ_LONG_CONTINUATION_ID,
        }
    )
    state = SimpleNamespace(
        strategy_status=StrategyStatus.READY,
        position_side=PositionSide.FLAT,
        open_broker_order_id=None,
        entries_enabled=True,
        operator_halt=False,
        same_underlying_entry_hold=False,
        same_underlying_hold_reason=None,
    )

    intent = engine._maybe_create_order_intent(history[-1], SignalPacket(**payload), state, SimpleNamespace())

    assert intent is not None
    assert intent.intent_type is OrderIntentType.BUY_TO_OPEN
    assert intent.reason_code == CHANGEOVER_0700_MNQ_LONG_CONTINUATION_ID
    assert intent.quantity == 1


def test_us_session_continuation_accepts_close_above_us_open_with_simple_continuation() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 28, 9, 30, tzinfo=ny), open_="21000", close="21005"),
        _bar(datetime(2026, 5, 28, 10, 15, tzinfo=ny), open_="21020", close="21030"),
    ]

    decision = _us_session_long_continuation_decision(
        history,
        spec=US_SESSION_CONTINUATION_SPECS[US_SESSION_MNQ_LONG_CONTINUATION_ID],
    )

    assert decision["accepted"] is True
    assert decision["primary_blocker"] is None
    assert decision["session_open_price"] == "21000"


def test_us_session_continuation_rejects_outside_entry_window() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 28, 9, 30, tzinfo=ny), open_="21000", close="21005"),
        _bar(datetime(2026, 5, 28, 12, 5, tzinfo=ny), open_="21020", close="21030"),
    ]

    decision = _us_session_long_continuation_decision(
        history,
        spec=US_SESSION_CONTINUATION_SPECS[US_SESSION_MNQ_LONG_CONTINUATION_ID],
    )

    assert decision["accepted"] is False
    assert decision["primary_blocker"] == "not_in_us_session_continuation_entry_window"


def test_us_session_continuation_entry_intent_uses_minimal_runtime_bar_context() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 28, 9, 30, tzinfo=ny), open_="21000", close="21005"),
        _bar(datetime(2026, 5, 28, 10, 15, tzinfo=ny), open_="21020", close="21030"),
    ]
    engine = object.__new__(TrackBRuleRunnerPaperStrategyEngine)
    engine._bar_history = history
    engine._settings = SimpleNamespace(symbol="MNQ", trade_size=1)

    payload = _empty_signal_packet_payload(history[-1].bar_id)
    payload.update(
        {
            "long_entry_raw": True,
            "recent_long_setup": True,
            "long_entry": True,
            "long_entry_source": US_SESSION_MNQ_LONG_CONTINUATION_ID,
        }
    )
    state = SimpleNamespace(
        strategy_status=StrategyStatus.READY,
        position_side=PositionSide.FLAT,
        open_broker_order_id=None,
        entries_enabled=True,
        operator_halt=False,
        same_underlying_entry_hold=False,
        same_underlying_hold_reason=None,
    )

    intent = engine._maybe_create_order_intent(history[-1], SignalPacket(**payload), state, SimpleNamespace())

    assert intent is not None
    assert intent.intent_type is OrderIntentType.BUY_TO_OPEN
    assert intent.reason_code == US_SESSION_MNQ_LONG_CONTINUATION_ID
    assert intent.quantity == 1


def test_us_session_continuation_can_use_phase1_1m_artifact_for_restart_reference(tmp_path) -> None:
    ny = ZoneInfo("America/New_York")
    artifact_dir = tmp_path / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data" / "MNQ" / "1m"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "latest_runtime_candles.json").write_text(
        """
{
  "bars": [
    {
      "bar_start": "2026-05-28T13:29:00+00:00",
      "bar_end": "2026-05-28T13:30:00+00:00",
      "open": 21000,
      "high": 21006,
      "low": 20998,
      "close": 21005,
      "completed": true
    },
    {
      "bar_start": "2026-05-28T14:14:00+00:00",
      "bar_end": "2026-05-28T14:15:00+00:00",
      "open": 21020,
      "high": 21031,
      "low": 21018,
      "close": 21030,
      "completed": true
    }
  ]
}
""",
        encoding="utf-8",
    )
    engine = object.__new__(TrackBRuleRunnerPaperStrategyEngine)
    engine._bar_history = [_bar(datetime(2026, 5, 28, 10, 15, tzinfo=ny), open_="21020", close="21030")]
    engine._settings = SimpleNamespace(symbol="MNQ", trade_size=1)
    engine._track_b_repo_root = tmp_path

    decision = engine._paper_only_long_continuation_decision_for_source(US_SESSION_MNQ_LONG_CONTINUATION_ID)

    assert decision["accepted"] is True
    assert decision["primary_blocker"] is None
    assert decision["session_open_price"] == "21000"


def test_paper_active_evidence_long_accepts_vwap_or_session_open_reference() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 28, 9, 30, tzinfo=ny), open_="21000", close="20990"),
        _bar(datetime(2026, 5, 28, 10, 15, tzinfo=ny), open_="20984", close="20990"),
    ]

    decision = _paper_active_evidence_decision(
        history,
        source=PAPER_ACTIVE_EVIDENCE_MNQ_US_LONG_ID,
        vwap=Decimal("20980"),
    )

    assert decision["accepted"] is True
    assert decision["primary_blocker"] is None
    assert decision["vwap_price"] == "20980"


def test_paper_active_evidence_short_accepts_downward_reference_and_recent_close() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 28, 9, 30, tzinfo=ny), open_="21000", close="21010"),
        _bar(datetime(2026, 5, 28, 10, 15, tzinfo=ny), open_="21002", close="20990"),
    ]

    decision = _paper_active_evidence_decision(
        history,
        source=PAPER_ACTIVE_EVIDENCE_MNQ_US_SHORT_ID,
        vwap=Decimal("21005"),
    )

    assert decision["accepted"] is True
    assert decision["primary_blocker"] is None


def test_paper_active_evidence_long_treats_recent_close_direction_as_soft_evidence() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 28, 9, 30, tzinfo=ny), open_="21000", close="21050"),
        _bar(datetime(2026, 5, 28, 10, 15, tzinfo=ny), open_="21060", close="21030"),
    ]

    decision = _paper_active_evidence_decision(
        history,
        source=PAPER_ACTIVE_EVIDENCE_MNQ_US_LONG_ID,
    )

    assert decision["accepted"] is True
    assert decision["primary_blocker"] is None
    assert decision["continuation_confirmed"] is False
    assert decision["recent_close_direction_tag"] == "RECENT_CLOSE_DOWN"
    assert decision["paper_only_soft_warnings"] == ["simple_long_continuation_not_confirmed"]


def test_paper_active_evidence_short_treats_recent_close_direction_as_soft_evidence() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 28, 9, 30, tzinfo=ny), open_="21000", close="20950"),
        _bar(datetime(2026, 5, 28, 10, 15, tzinfo=ny), open_="20940", close="20970"),
    ]

    decision = _paper_active_evidence_decision(
        history,
        source=PAPER_ACTIVE_EVIDENCE_MNQ_US_SHORT_ID,
    )

    assert decision["accepted"] is True
    assert decision["primary_blocker"] is None
    assert decision["continuation_confirmed"] is False
    assert decision["recent_close_direction_tag"] == "RECENT_CLOSE_UP"
    assert decision["paper_only_soft_warnings"] == ["simple_short_continuation_not_confirmed"]


def test_globex_active_evidence_long_accepts_cross_midnight_window_with_vwap_reference() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 28, 23, 58, tzinfo=ny), open_="21000", close="21002"),
        _bar(datetime(2026, 5, 28, 23, 59, tzinfo=ny), open_="21002", close="21005"),
    ]

    decision = _paper_active_evidence_decision(
        history,
        source=PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_LONG_ID,
        vwap=Decimal("21001"),
    )

    assert decision["accepted"] is True
    assert decision["primary_blocker"] is None
    assert decision["vwap_price"] == "21001"
    assert decision["session_open_price"] is None
    assert decision["recent_close_direction_tag"] == "RECENT_CLOSE_UP"


def test_globex_active_evidence_long_uses_rolling_vwap_when_session_open_not_available() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 28, 23, 58, tzinfo=ny), open_="21000", close="21002"),
        _bar(datetime(2026, 5, 28, 23, 59, tzinfo=ny), open_="21002", close="21006"),
    ]

    decision = _paper_active_evidence_decision(
        history,
        source=PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_LONG_ID,
    )

    assert decision["accepted"] is True
    assert decision["primary_blocker"] is None
    assert decision["session_open_price"] is None
    assert decision["reference_source"] == "ROLLING_VWAP_REFERENCE"
    assert Decimal(decision["vwap_price"]) == Decimal("21004")


def test_globex_active_evidence_short_uses_18_et_open_when_available() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 28, 18, 1, tzinfo=ny), open_="5300", close="5298"),
        _bar(datetime(2026, 5, 28, 18, 8, tzinfo=ny), open_="5299", close="5295"),
    ]

    decision = _paper_active_evidence_decision(
        history,
        source=PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_SHORT_ID,
    )

    assert decision["accepted"] is True
    assert decision["primary_blocker"] is None
    assert decision["session_open_price"] == "5300"


def test_globex_active_evidence_blocks_outside_evening_window() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 28, 10, 15, tzinfo=ny), open_="21000", close="21030"),
    ]

    decision = _paper_active_evidence_decision(
        history,
        source=PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_LONG_ID,
        vwap=Decimal("21000"),
    )

    assert decision["accepted"] is False
    assert decision["primary_blocker"] == "not_in_paper_active_evidence_entry_window"


def test_paper_active_evidence_recovers_us_open_from_phase1_1m_gap_backfill(tmp_path) -> None:
    ny = ZoneInfo("America/New_York")
    live_path = (
        tmp_path
        / "outputs/track_b_execution_core/phase1_runtime_market_data/MNQ/1m/latest_runtime_candles.json"
    )
    gap_path = (
        tmp_path
        / "outputs/track_b_execution_core/phase1_runtime_market_data_gap_backfill/us_session_reference/MNQ/1m/latest_runtime_candles.json"
    )
    live_path.parent.mkdir(parents=True, exist_ok=True)
    gap_path.parent.mkdir(parents=True, exist_ok=True)
    live_path.write_text(
        json.dumps(
            {
                "source": "DATABENTO_REALTIME_PHASE1",
                "bars": [
                    {
                        "bar_start": datetime(2026, 5, 28, 10, 14, tzinfo=ny).astimezone(UTC).isoformat(),
                        "bar_end": datetime(2026, 5, 28, 10, 15, tzinfo=ny).astimezone(UTC).isoformat(),
                        "open": "21010",
                        "high": "21030",
                        "low": "21010",
                        "close": "21030",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    gap_path.write_text(
        json.dumps(
            {
                "source": "DATABENTO_HISTORICAL_SEED",
                "can_submit": False,
                "live_money_eligible": False,
                "bars": [
                    {
                        "bar_start": datetime(2026, 5, 28, 9, 29, tzinfo=ny).astimezone(UTC).isoformat(),
                        "bar_end": datetime(2026, 5, 28, 9, 30, tzinfo=ny).astimezone(UTC).isoformat(),
                        "open": "21000",
                        "high": "21002",
                        "low": "20998",
                        "close": "21001",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    engine = object.__new__(TrackBRuleRunnerPaperStrategyEngine)
    engine._bar_history = [_bar(datetime(2026, 5, 28, 10, 15, tzinfo=ny), open_="21010", close="21030")]
    engine._settings = SimpleNamespace(symbol="MNQ", trade_size=1)
    engine._track_b_repo_root = tmp_path

    decision = engine._paper_active_evidence_decision_for_source(PAPER_ACTIVE_EVIDENCE_MNQ_US_LONG_ID)

    assert decision["accepted"] is True
    assert decision["primary_blocker"] is None
    assert decision["session_open_price"] == "21000"
    assert decision["reference_source"] == "RECOVERED_PHASE1_1M"
    assert decision["reference_recovery_blocker"] is None
    assert decision["session_anchor_status"] == "READY"
    assert decision["session_anchor_reason_code"] == "ANCHOR_RECOVERED_FROM_PHASE1_GAP_BACKFILL"


def test_paper_active_evidence_keeps_missing_us_open_hard_after_failed_recovery(tmp_path) -> None:
    ny = ZoneInfo("America/New_York")
    live_path = (
        tmp_path
        / "outputs/track_b_execution_core/phase1_runtime_market_data/MNQ/1m/latest_runtime_candles.json"
    )
    live_path.parent.mkdir(parents=True, exist_ok=True)
    live_path.write_text(
        json.dumps(
            {
                "source": "DATABENTO_REALTIME_PHASE1",
                "bars": [
                    {
                        "bar_start": datetime(2026, 5, 28, 10, 14, tzinfo=ny).astimezone(UTC).isoformat(),
                        "bar_end": datetime(2026, 5, 28, 10, 15, tzinfo=ny).astimezone(UTC).isoformat(),
                        "open": "21010",
                        "high": "21030",
                        "low": "21010",
                        "close": "21030",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    engine = object.__new__(TrackBRuleRunnerPaperStrategyEngine)
    engine._bar_history = [_bar(datetime(2026, 5, 28, 10, 15, tzinfo=ny), open_="21010", close="21030")]
    engine._settings = SimpleNamespace(symbol="MNQ", trade_size=1)
    engine._track_b_repo_root = tmp_path

    decision = engine._paper_active_evidence_decision_for_source(PAPER_ACTIVE_EVIDENCE_MNQ_US_LONG_ID)

    assert decision["accepted"] is False
    assert decision["primary_blocker"] == "SESSION_ANCHOR_NOT_READY"
    assert decision["reference_recovery_blocker"] == "ANCHOR_BAR_NOT_FOUND"
    assert decision["session_anchor_status"] == "NOT_READY"
    assert decision["session_anchor_reason_code"] == "ANCHOR_BAR_NOT_FOUND"


def test_paper_active_evidence_uses_session_anchor_intraday_backfill_after_runtime_roll(tmp_path) -> None:
    ny = ZoneInfo("America/New_York")
    backfill_path = (
        tmp_path
        / "outputs/track_b_execution_core/phase1_runtime_market_data_intraday_backfill/MNQ/1m/latest_runtime_candles.json"
    )
    backfill_path.parent.mkdir(parents=True, exist_ok=True)
    backfill_path.write_text(
        json.dumps(
            {
                "source": "PHASE1_CURRENT_DAY_INTRADAY_BACKFILL",
                "bars": [
                    {
                        "bar_start": datetime(2026, 5, 28, 9, 30, tzinfo=ny).astimezone(UTC).isoformat(),
                        "bar_end": datetime(2026, 5, 28, 9, 31, tzinfo=ny).astimezone(UTC).isoformat(),
                        "open": "21000",
                        "high": "21002",
                        "low": "20998",
                        "close": "21001",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    engine = object.__new__(TrackBRuleRunnerPaperStrategyEngine)
    engine._bar_history = [_bar(datetime(2026, 5, 28, 10, 15, tzinfo=ny), open_="21010", close="21030")]
    engine._settings = SimpleNamespace(symbol="MNQ", trade_size=1)
    engine._track_b_repo_root = tmp_path

    decision = engine._paper_active_evidence_decision_for_source(PAPER_ACTIVE_EVIDENCE_MNQ_US_LONG_ID)

    assert decision["accepted"] is True
    assert decision["primary_blocker"] is None
    assert decision["session_open_price"] == "21000"
    assert decision["session_anchor_status"] == "READY"
    assert decision["session_anchor_source"] == "RECOVERED_PHASE1_1M"
    assert decision["reference_source"] == "RECOVERED_PHASE1_1M"


def test_paper_active_evidence_blocks_stale_prior_day_session_anchor(tmp_path) -> None:
    ny = ZoneInfo("America/New_York")
    anchor_path = tmp_path / "outputs/track_b_execution_core/session_anchors/MNQ/2026-05-28/US_0930_OPEN.json"
    anchor_path.parent.mkdir(parents=True, exist_ok=True)
    anchor_path.write_text(
        json.dumps(
            {
                "status": "READY",
                "session_date_et": "2026-05-27",
                "anchor_time_utc": datetime(2026, 5, 28, 13, 30, tzinfo=UTC).isoformat(),
                "timeframe": "1m",
                "reference_price": "21000",
                "bar": {
                    "bar_start": datetime(2026, 5, 28, 9, 30, tzinfo=ny).astimezone(UTC).isoformat(),
                    "bar_end": datetime(2026, 5, 28, 9, 31, tzinfo=ny).astimezone(UTC).isoformat(),
                    "open": "21000",
                    "high": "21002",
                    "low": "20998",
                    "close": "21001",
                },
            }
        ),
        encoding="utf-8",
    )
    engine = object.__new__(TrackBRuleRunnerPaperStrategyEngine)
    engine._bar_history = [_bar(datetime(2026, 5, 28, 10, 15, tzinfo=ny), open_="21010", close="21030")]
    engine._settings = SimpleNamespace(symbol="MNQ", trade_size=1)
    engine._track_b_repo_root = tmp_path

    decision = engine._paper_active_evidence_decision_for_source(PAPER_ACTIVE_EVIDENCE_MNQ_US_LONG_ID)

    assert decision["accepted"] is False
    assert decision["primary_blocker"] == "SESSION_ANCHOR_NOT_READY"
    assert decision["session_anchor_status"] == "STALE"
    assert decision["session_anchor_reason_code"] == "ANCHOR_SESSION_DATE_MISMATCH"


def test_paper_active_evidence_blocks_ambiguous_session_anchor_sources(tmp_path) -> None:
    ny = ZoneInfo("America/New_York")
    live_path = (
        tmp_path
        / "outputs/track_b_execution_core/phase1_runtime_market_data/MNQ/1m/latest_runtime_candles.json"
    )
    backfill_path = (
        tmp_path
        / "outputs/track_b_execution_core/phase1_runtime_market_data_intraday_backfill/MNQ/1m/latest_runtime_candles.json"
    )
    for path, open_price in ((live_path, "21000"), (backfill_path, "21025")):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "bars": [
                        {
                            "bar_start": datetime(2026, 5, 28, 9, 30, tzinfo=ny).astimezone(UTC).isoformat(),
                            "bar_end": datetime(2026, 5, 28, 9, 31, tzinfo=ny).astimezone(UTC).isoformat(),
                            "open": open_price,
                            "high": "21030",
                            "low": "20990",
                            "close": "21010",
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
    engine = object.__new__(TrackBRuleRunnerPaperStrategyEngine)
    engine._bar_history = [_bar(datetime(2026, 5, 28, 10, 15, tzinfo=ny), open_="21010", close="21030")]
    engine._settings = SimpleNamespace(symbol="MNQ", trade_size=1)
    engine._track_b_repo_root = tmp_path

    decision = engine._paper_active_evidence_decision_for_source(PAPER_ACTIVE_EVIDENCE_MNQ_US_LONG_ID)

    assert decision["accepted"] is False
    assert decision["primary_blocker"] == "SESSION_ANCHOR_NOT_READY"
    assert decision["session_anchor_status"] == "AMBIGUOUS"
    assert decision["session_anchor_reason_code"] == "ANCHOR_AMBIGUOUS"


def test_paper_active_evidence_short_entry_intent_uses_minimal_runtime_context() -> None:
    ny = ZoneInfo("America/New_York")
    history = [
        _bar(datetime(2026, 5, 28, 9, 30, tzinfo=ny), open_="21000", close="21010"),
        _bar(datetime(2026, 5, 28, 10, 15, tzinfo=ny), open_="21002", close="20990"),
    ]
    engine = object.__new__(TrackBRuleRunnerPaperStrategyEngine)
    engine._bar_history = history
    engine._settings = SimpleNamespace(symbol="MNQ", trade_size=1)
    engine._latest_track_b_rule_report = {
        "classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_ACCEPTED",
        "entry_source": PAPER_ACTIVE_EVIDENCE_MNQ_US_SHORT_ID,
        "direction": PAPER_ACTIVE_EVIDENCE_SPECS[PAPER_ACTIVE_EVIDENCE_MNQ_US_SHORT_ID].direction,
        "primary_blocker": None,
    }

    payload = _empty_signal_packet_payload(history[-1].bar_id)
    payload.update(
        {
            "short_entry_raw": True,
            "recent_short_setup": True,
            "short_entry": True,
            "short_entry_source": PAPER_ACTIVE_EVIDENCE_MNQ_US_SHORT_ID,
        }
    )
    state = SimpleNamespace(
        strategy_status=StrategyStatus.READY,
        position_side=PositionSide.FLAT,
        open_broker_order_id=None,
        entries_enabled=True,
        operator_halt=False,
        same_underlying_entry_hold=False,
        same_underlying_hold_reason=None,
    )

    intent = engine._maybe_create_order_intent(history[-1], SignalPacket(**payload), state, SimpleNamespace())

    assert intent is not None
    assert intent.intent_type is OrderIntentType.SELL_TO_OPEN
    assert intent.reason_code == PAPER_ACTIVE_EVIDENCE_MNQ_US_SHORT_ID
    assert intent.quantity == 1
