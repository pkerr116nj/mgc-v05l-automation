from __future__ import annotations

import ast
import json
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import mgc_v05l.app.asia_london_participation_runtime as asia_runtime
import mgc_v05l.app.gc_mgc_forced_session_runtime as gold_runtime
import mgc_v05l.app.index_futures_forced_session_runtime as index_runtime
from mgc_v05l.app.operational_maturation_runtime import (
    b_plus_diagnostic_payload,
    b_plus_setup_score,
    emit_b_plus_diagnostic,
)
from mgc_v05l.domain.enums import AddDirectionPolicy, OrderIntentType, ParticipationPolicy, StrategyStatus
from mgc_v05l.domain.models import Bar
from mgc_v05l.strategy.trade_state import build_initial_state


REPO_ROOT = Path(__file__).resolve().parents[2]
ACTIVE_15_LANE_OVERLAY = REPO_ROOT / "config" / "probationary_pattern_engine_paper_mnq_mgc_plus_mnq_us_intraday_review.yaml"


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


def _b_plus_params() -> dict[str, object]:
    return {
        **_op_params(),
        "operational_maturation_timed_entry_enabled": False,
        "operational_maturation_b_plus_enabled": True,
        "operational_maturation_entry_acceptance_level": "B_PLUS",
        "operational_maturation_b_plus_threshold": 0.80,
        "operational_maturation_b_plus_max_entry_bar": 10,
    }


def _packet(bar: Bar) -> SimpleNamespace:
    return SimpleNamespace(bar_id=bar.bar_id)


def _b_plus_boundary_short_bars(start: datetime) -> list[Bar]:
    bars = [_bar("MNQ", start + timedelta(minutes=3 * index), close="100.20") for index in range(1, 12)]
    bars[-2] = _bar("MNQ", start + timedelta(minutes=30), close="100.05")
    bars[-1] = _bar("MNQ", start + timedelta(minutes=33), close="100.20")
    return bars


def _active_overlay_lanes() -> list[dict[str, object]]:
    raw = next(
        line.split(": ", 1)[1]
        for line in ACTIVE_15_LANE_OVERLAY.read_text(encoding="utf-8").splitlines()
        if line.startswith("probationary_paper_lanes_json: ")
    )
    return list(json.loads(ast.literal_eval(raw)))


def test_active_overlay_enables_paper_execution_test_mule_switch() -> None:
    payload = ACTIVE_15_LANE_OVERLAY.read_text(encoding="utf-8")
    assert "probationary_paper_execution_test_mule_enabled: true" in payload


def _ready_state(at: datetime):
    return replace(build_initial_state(at), strategy_status=StrategyStatus.READY)


def _intent_settings(symbol: str = "MNQ") -> SimpleNamespace:
    return SimpleNamespace(
        symbol=symbol,
        trade_size=1,
        warmup_bars_required=lambda: 1,
        add_direction_policy=AddDirectionPolicy.SAME_DIRECTION_ONLY,
        participation_policy=ParticipationPolicy.SINGLE_ENTRY_ONLY,
        max_concurrent_entries=1,
        max_adds_after_entry=0,
        max_position_quantity=1,
    )


def _exit_decision() -> SimpleNamespace:
    return SimpleNamespace(long_exit=False, short_exit=False, primary_reason=None)


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


def test_b_plus_setup_score_uses_upper_near_acceptance_level() -> None:
    start = datetime(2026, 5, 19, 19, 0, tzinfo=ZoneInfo("America/New_York"))
    bars = [_bar("MGC", start + timedelta(minutes=3 * index), close="100.70") for index in range(1, 6)]
    lane_spec = SimpleNamespace(
        symbol="MGC",
        required_market_data_provenance="DATABENTO_REALTIME_PHASE1",
        market_data_source="phase1_runtime_artifact",
        runtime_overlay_params=_b_plus_params(),
        paper_only=True,
        live_money_eligible=False,
    )

    result = b_plus_setup_score(
        lane_spec=lane_spec,
        segment_bars=bars,
        current_index=4,
        setup_bar_count=4,
        tick_size=0.1,
        side="LONG",
        exact_match=False,
        preferred_or_near_trigger=True,
        fallback_entry_bar=8,
    )

    assert result.b_plus_match is True
    assert result.acceptance_level == "B_PLUS"
    assert result.acceptance_class == "NEAR_STRUCTURAL_MATCH"
    assert result.score >= 0.80
    assert result.dimension_scores["structural_similarity"] >= 0.72


def test_b_plus_score_below_threshold_cannot_route() -> None:
    start = datetime(2026, 5, 19, 19, 0, tzinfo=ZoneInfo("America/New_York"))
    bars = [_bar("MGC", start + timedelta(minutes=3 * index), close="100.05") for index in range(1, 6)]
    lane_spec = SimpleNamespace(
        symbol="MGC",
        required_market_data_provenance="DATABENTO_REALTIME_PHASE1",
        market_data_source="phase1_runtime_artifact",
        runtime_overlay_params={**_b_plus_params(), "operational_maturation_b_plus_threshold": 0.84},
        paper_only=True,
        live_money_eligible=False,
    )

    result = b_plus_setup_score(
        lane_spec=lane_spec,
        segment_bars=bars,
        current_index=4,
        setup_bar_count=4,
        tick_size=0.1,
        side="LONG",
        exact_match=False,
        preferred_or_near_trigger=False,
        fallback_entry_bar=8,
    )

    assert result.b_plus_match is False
    assert result.reason.startswith("b_plus_score_below_threshold")


def test_active_overlay_sets_paper_b_plus_threshold_to_0775_for_all_lanes() -> None:
    lanes = _active_overlay_lanes()

    assert len(lanes) == 15
    assert {
        lane["runtime_overlay_params"]["operational_maturation_b_plus_threshold"]  # type: ignore[index]
        for lane in lanes
    } == {0.775}
    assert all(lane["runtime_overlay_params"]["live_money_eligible"] is False for lane in lanes)  # type: ignore[index]


def test_b_plus_score_07775_routes_with_paper_operational_threshold() -> None:
    start = datetime(2026, 5, 20, 8, 20, tzinfo=ZoneInfo("America/New_York"))
    bars = _b_plus_boundary_short_bars(start)
    lane_spec = SimpleNamespace(
        symbol="MNQ",
        required_market_data_provenance="DATABENTO_REALTIME_PHASE1",
        market_data_source="phase1_runtime_artifact",
        runtime_overlay_params={**_b_plus_params(), "operational_maturation_b_plus_threshold": 0.775},
        paper_only=True,
        live_money_eligible=False,
    )

    result = b_plus_setup_score(
        lane_spec=lane_spec,
        segment_bars=bars,
        current_index=10,
        setup_bar_count=4,
        tick_size=0.1,
        side="SHORT",
        exact_match=False,
        preferred_or_near_trigger=False,
        fallback_entry_bar=8,
    )

    assert result.score == 0.7775
    assert result.threshold == 0.775
    assert result.b_plus_match is True
    assert result.live_money_eligible is False
    assert result.mandatory_gate_failures == ()


def test_b_plus_score_below_0775_cannot_route() -> None:
    start = datetime(2026, 5, 20, 8, 20, tzinfo=ZoneInfo("America/New_York"))
    bars = _b_plus_boundary_short_bars(start)
    bars[-1] = _bar("MNQ", start + timedelta(minutes=33), close="100.70")
    lane_spec = SimpleNamespace(
        symbol="MNQ",
        required_market_data_provenance="DATABENTO_REALTIME_PHASE1",
        market_data_source="phase1_runtime_artifact",
        runtime_overlay_params={**_b_plus_params(), "operational_maturation_b_plus_threshold": 0.775},
        paper_only=True,
        live_money_eligible=False,
    )

    result = b_plus_setup_score(
        lane_spec=lane_spec,
        segment_bars=bars,
        current_index=10,
        setup_bar_count=4,
        tick_size=0.1,
        side="SHORT",
        exact_match=False,
        preferred_or_near_trigger=False,
        fallback_entry_bar=8,
    )

    assert result.score < 0.775
    assert result.threshold == 0.775
    assert result.b_plus_match is False
    assert result.reason.startswith("b_plus_score_below_threshold")


def test_b_plus_wrong_direction_and_wrong_provenance_fail_closed() -> None:
    start = datetime(2026, 5, 19, 19, 0, tzinfo=ZoneInfo("America/New_York"))
    bars = [_bar("MGC", start + timedelta(minutes=3 * index)) for index in range(1, 6)]
    wrong_direction = b_plus_setup_score(
        lane_spec=SimpleNamespace(
            symbol="MGC",
            required_market_data_provenance="DATABENTO_REALTIME_PHASE1",
            market_data_source="phase1_runtime_artifact",
            runtime_overlay_params=_b_plus_params(),
            paper_only=True,
            live_money_eligible=False,
        ),
        segment_bars=bars,
        current_index=4,
        setup_bar_count=4,
        tick_size=0.1,
        side="SIDEWAYS",
        exact_match=False,
        preferred_or_near_trigger=True,
        fallback_entry_bar=8,
    )
    wrong_provenance = b_plus_setup_score(
        lane_spec=SimpleNamespace(
            symbol="MGC",
            required_market_data_provenance="RESEARCH_BACKTEST",
            market_data_source="phase1_runtime_artifact",
            runtime_overlay_params=_b_plus_params(),
            paper_only=True,
            live_money_eligible=False,
        ),
        segment_bars=bars,
        current_index=4,
        setup_bar_count=4,
        tick_size=0.1,
        side="LONG",
        exact_match=False,
        preferred_or_near_trigger=True,
        fallback_entry_bar=8,
    )
    stale_provenance = b_plus_setup_score(
        lane_spec=SimpleNamespace(
            symbol="MGC",
            required_market_data_provenance="DATABENTO_REALTIME_PHASE1",
            market_data_source="phase1_runtime_artifact",
            runtime_overlay_params={**_b_plus_params(), "operational_maturation_b_plus_phase1_artifact_fresh": False},
            paper_only=True,
            live_money_eligible=False,
        ),
        segment_bars=bars,
        current_index=4,
        setup_bar_count=4,
        tick_size=0.1,
        side="LONG",
        exact_match=False,
        preferred_or_near_trigger=True,
        fallback_entry_bar=8,
    )

    assert wrong_direction.b_plus_match is False
    assert "direction_unknown" in wrong_direction.mandatory_gate_failures
    assert wrong_provenance.b_plus_match is False
    assert "wrong_provenance" in wrong_provenance.mandatory_gate_failures
    assert stale_provenance.b_plus_match is False
    assert "stale_phase1_artifact" in stale_provenance.mandatory_gate_failures


def test_b_plus_runtime_respects_session_gate_before_route() -> None:
    engine = object.__new__(index_runtime.IndexFuturesForcedSessionStrategyEngine)
    engine._lane_spec = SimpleNamespace(
        symbol="MNQ",
        lane_id="mnq_test",
        runtime_overlay_params=_b_plus_params(),
        paper_only=True,
        live_money_eligible=False,
        required_market_data_provenance="DATABENTO_REALTIME_PHASE1",
        market_data_source="phase1_runtime_artifact",
    )
    engine._runtime_definition = index_runtime.INDEX_FORCED_SESSION_RUNTIME_BY_SOURCE[index_runtime.INDEX_NY_EARLY_LONG_SOURCE]
    outside_session_bar = _bar("MNQ", datetime(2026, 5, 20, 13, 0, tzinfo=ZoneInfo("America/New_York")))
    engine._bar_history = [outside_session_bar]

    signal = engine._evaluate_signals(_packet(outside_session_bar), [])  # noqa: SLF001

    assert signal.long_entry is False
    assert not hasattr(engine, "_latest_b_plus_setup_score")


def test_b_plus_runtime_can_route_near_miss_when_paper_only() -> None:
    start = datetime(2026, 5, 20, 8, 20, tzinfo=ZoneInfo("America/New_York"))
    bars = [_bar("MNQ", start + timedelta(minutes=3 * index), close="100.70") for index in range(1, 6)]
    engine = object.__new__(index_runtime.IndexFuturesForcedSessionStrategyEngine)
    engine._lane_spec = SimpleNamespace(
        symbol="MNQ",
        lane_id="mnq_test",
        runtime_overlay_params=_b_plus_params(),
        paper_only=True,
        live_money_eligible=False,
        required_market_data_provenance="DATABENTO_REALTIME_PHASE1",
        market_data_source="phase1_runtime_artifact",
    )
    engine._runtime_definition = index_runtime.INDEX_FORCED_SESSION_RUNTIME_BY_SOURCE[index_runtime.INDEX_NY_EARLY_LONG_SOURCE]
    engine._bar_history = bars

    signal = engine._evaluate_signals(_packet(bars[-1]), [])  # noqa: SLF001

    assert signal.long_entry is True
    assert signal.long_entry_source == index_runtime.INDEX_NY_EARLY_LONG_SOURCE
    assert engine._latest_b_plus_setup_score["b_plus_match"] is True
    assert engine._latest_b_plus_setup_score["live_money_eligible"] is False


def test_b_plus_runtime_fails_closed_for_live_money_eligible_lane() -> None:
    start = datetime(2026, 5, 20, 8, 20, tzinfo=ZoneInfo("America/New_York"))
    bars = [_bar("MNQ", start + timedelta(minutes=3 * index), close="100.70") for index in range(1, 6)]
    engine = object.__new__(index_runtime.IndexFuturesForcedSessionStrategyEngine)
    engine._lane_spec = SimpleNamespace(
        symbol="MNQ",
        lane_id="mnq_test",
        runtime_overlay_params=_b_plus_params(),
        paper_only=True,
        live_money_eligible=True,
        required_market_data_provenance="DATABENTO_REALTIME_PHASE1",
        market_data_source="phase1_runtime_artifact",
    )
    engine._runtime_definition = index_runtime.INDEX_FORCED_SESSION_RUNTIME_BY_SOURCE[index_runtime.INDEX_NY_EARLY_LONG_SOURCE]
    engine._bar_history = bars

    signal = engine._evaluate_signals(_packet(bars[-1]), [])  # noqa: SLF001

    assert signal.long_entry is False


def test_b_plus_rejected_score_diagnostic_is_persisted_without_route_authority(tmp_path) -> None:
    start = datetime(2026, 5, 20, 8, 20, tzinfo=ZoneInfo("America/New_York"))
    bars = [_bar("MNQ", start + timedelta(minutes=3 * index), close="100.05") for index in range(1, 6)]
    lane_spec = SimpleNamespace(
        symbol="MNQ",
        lane_id="mnq_test",
        artifacts_dir=str(tmp_path / "lanes" / "mnq_test"),
        required_market_data_provenance="DATABENTO_REALTIME_PHASE1",
        market_data_source="phase1_runtime_artifact",
        runtime_overlay_params={**_b_plus_params(), "operational_maturation_b_plus_threshold": 0.84},
        paper_only=True,
        live_money_eligible=False,
    )
    result = b_plus_setup_score(
        lane_spec=lane_spec,
        segment_bars=bars,
        current_index=4,
        setup_bar_count=4,
        tick_size=0.1,
        side="LONG",
        exact_match=False,
        preferred_or_near_trigger=False,
        fallback_entry_bar=8,
    )
    payload = b_plus_diagnostic_payload(
        result=result,
        bar=bars[-1],
        lane_id="mnq_test",
        source_id=index_runtime.INDEX_NY_EARLY_LONG_SOURCE,
        side="LONG",
    )

    written = emit_b_plus_diagnostic(lane_spec=lane_spec, payload=payload)

    assert result.b_plus_match is False
    assert len(written) == 3
    latest = (tmp_path / "lanes" / "mnq_test" / "b_plus_setup_score_latest.json").read_text()
    history = (tmp_path / "lanes" / "mnq_test" / "b_plus_setup_score_events.jsonl").read_text()
    aggregate = (tmp_path / "b_plus_setup_score_events.jsonl").read_text()
    for text in (latest, history, aggregate):
        assert '"b_plus_match": false' in text
        assert '"diagnostic_only": true' in text
        assert '"route_authority_changed": false' in text
        assert '"order_intent_created_by_diagnostic": false' in text
        assert '"route_attempted_by_diagnostic": false' in text
        assert '"broker_mutation": false' in text
        assert '"lifecycle_mutation": false' in text


def test_accepted_b_plus_match_creates_normal_paper_order_intent_candidate(tmp_path) -> None:
    start = datetime(2026, 5, 20, 8, 20, tzinfo=ZoneInfo("America/New_York"))
    bars = [_bar("MNQ", start + timedelta(minutes=3 * index), close="100.70") for index in range(1, 6)]
    engine = object.__new__(index_runtime.IndexFuturesForcedSessionStrategyEngine)
    engine._lane_spec = SimpleNamespace(
        symbol="MNQ",
        lane_id="mnq_test",
        artifacts_dir=str(tmp_path / "lanes" / "mnq_test"),
        runtime_overlay_params=_b_plus_params(),
        paper_only=True,
        live_money_eligible=False,
        required_market_data_provenance="DATABENTO_REALTIME_PHASE1",
        market_data_source="phase1_runtime_artifact",
    )
    engine._runtime_definition = index_runtime.INDEX_FORCED_SESSION_RUNTIME_BY_SOURCE[index_runtime.INDEX_NY_EARLY_LONG_SOURCE]
    engine._bar_history = bars
    engine._settings = _intent_settings("MNQ")

    signal = engine._evaluate_signals(_packet(bars[-1]), [])  # noqa: SLF001
    intent = engine._maybe_create_order_intent(bars[-1], signal, _ready_state(bars[-1].end_ts), _exit_decision())  # noqa: SLF001

    assert signal.long_entry is True
    assert signal.long_entry_source == index_runtime.INDEX_NY_EARLY_LONG_SOURCE
    assert intent is not None
    assert intent.intent_type == OrderIntentType.BUY_TO_OPEN
    assert intent.symbol == "MNQ"
    assert intent.reason_code == index_runtime.INDEX_NY_EARLY_LONG_SOURCE
    assert engine._latest_b_plus_setup_score["b_plus_match"] is True
    assert engine._latest_b_plus_setup_score["diagnostic_only"] is True
    assert engine._latest_b_plus_setup_score["route_attempted_by_diagnostic"] is False


def test_rejected_b_plus_match_does_not_create_order_intent_candidate(tmp_path) -> None:
    start = datetime(2026, 5, 20, 8, 20, tzinfo=ZoneInfo("America/New_York"))
    bars = [_bar("MNQ", start + timedelta(minutes=3 * index), close="100.05") for index in range(1, 6)]
    engine = object.__new__(index_runtime.IndexFuturesForcedSessionStrategyEngine)
    engine._lane_spec = SimpleNamespace(
        symbol="MNQ",
        lane_id="mnq_test",
        artifacts_dir=str(tmp_path / "lanes" / "mnq_test"),
        runtime_overlay_params={**_b_plus_params(), "operational_maturation_b_plus_threshold": 0.84},
        paper_only=True,
        live_money_eligible=False,
        required_market_data_provenance="DATABENTO_REALTIME_PHASE1",
        market_data_source="phase1_runtime_artifact",
    )
    engine._runtime_definition = index_runtime.INDEX_FORCED_SESSION_RUNTIME_BY_SOURCE[index_runtime.INDEX_NY_EARLY_LONG_SOURCE]
    engine._bar_history = bars
    engine._settings = _intent_settings("MNQ")

    signal = engine._evaluate_signals(_packet(bars[-1]), [])  # noqa: SLF001
    intent = engine._maybe_create_order_intent(bars[-1], signal, _ready_state(bars[-1].end_ts), _exit_decision())  # noqa: SLF001

    assert engine._latest_b_plus_setup_score["b_plus_match"] is False
    assert signal.long_entry is False
    assert intent is None


def test_startup_restore_b_plus_is_diagnostic_only_and_does_not_consume_session_key(tmp_path) -> None:
    start = datetime(2026, 5, 20, 8, 20, tzinfo=ZoneInfo("America/New_York"))
    bars = [_bar("MNQ", start + timedelta(minutes=3 * index), close="100.70") for index in range(1, 6)]
    engine = object.__new__(index_runtime.IndexFuturesForcedSessionStrategyEngine)
    engine._lane_spec = SimpleNamespace(
        symbol="MNQ",
        lane_id="mnq_test",
        artifacts_dir=str(tmp_path / "lanes" / "mnq_test"),
        runtime_overlay_params=_b_plus_params(),
        paper_only=True,
        live_money_eligible=False,
        required_market_data_provenance="DATABENTO_REALTIME_PHASE1",
        market_data_source="phase1_runtime_artifact",
    )
    engine._runtime_definition = index_runtime.INDEX_FORCED_SESSION_RUNTIME_BY_SOURCE[index_runtime.INDEX_NY_EARLY_LONG_SOURCE]
    engine._bar_history = bars
    engine._startup_restore_in_progress = True

    restore_signal = engine._evaluate_signals(_packet(bars[-1]), [])  # noqa: SLF001

    assert engine._latest_b_plus_setup_score["b_plus_match"] is True
    assert restore_signal.long_entry is False
    assert not getattr(engine, "_b_plus_session_keys", set())

    engine._startup_restore_in_progress = False
    live_signal = engine._evaluate_signals(_packet(bars[-1]), [])  # noqa: SLF001

    assert live_signal.long_entry is True
    assert getattr(engine, "_b_plus_session_keys", set())
