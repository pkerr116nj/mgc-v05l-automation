from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.app import strategy_universe_retest as retest
from mgc_v05l.domain.models import Bar
from mgc_v05l.research.trend_participation.models import ResearchBar


@dataclass(frozen=True)
class _FeatureStub:
    bar_id: str
    atr: Decimal


def test_discover_best_sources_prefers_canonical_historical_1m(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / "sources.sqlite3"
    connection = sqlite3.connect(database_path)
    try:
        connection.executescript(
            """
            create table bars (
              symbol text not null,
              timeframe text not null,
              data_source text not null,
              end_ts text not null
            );
            """
        )
        canonical_rows = [
            ("MGC", "1m", "historical_1m_canonical", f"2026-03-13T18:{minute:02d}:00+00:00")
            for minute in range(1, 6)
        ]
        larger_schwab_rows = [
            ("MGC", "1m", "schwab_history", f"2026-03-13T18:{minute:02d}:00+00:00")
            for minute in range(1, 11)
        ]
        connection.executemany(
            "insert into bars (symbol, timeframe, data_source, end_ts) values (?, ?, ?, ?)",
            canonical_rows + larger_schwab_rows,
        )
        connection.commit()
    finally:
        connection.close()

    monkeypatch.setattr(retest, "REPO_ROOT", tmp_path)
    selections = retest._discover_best_sources(symbols={"MGC"}, timeframes={"1m"})

    assert selections["MGC"]["1m"].data_source == "historical_1m_canonical"
    assert selections["MGC"]["1m"].sqlite_path == database_path.resolve()


def test_build_symbol_windows_applies_quarter_shards_with_warmup() -> None:
    bar_source_index = {
        "MGC": {
            "1m": retest.SourceSelection(
                symbol="MGC",
                timeframe="1m",
                data_source="historical_1m_canonical",
                sqlite_path=Path("/tmp/source.sqlite3"),
                row_count=10,
                start_ts="2024-01-01T00:00:00+00:00",
                end_ts="2024-08-15T00:00:00+00:00",
            )
        }
    }

    windows = retest._build_symbol_windows(
        symbols={"MGC"},
        bar_source_index=bar_source_index,
        start_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        end_timestamp=datetime(2024, 7, 1, tzinfo=UTC),
        shard_config=retest.RetestShardConfig(shard_months=3, warmup_days=14),
    )

    assert len(windows) == 3
    assert windows[0].evaluation_start == datetime(2024, 1, 1, tzinfo=UTC)
    assert windows[0].load_start == datetime(2024, 1, 1, tzinfo=UTC)
    assert windows[1].evaluation_start == datetime(2024, 3, 31, 23, 59, tzinfo=UTC) + timedelta(minutes=1)
    assert windows[1].load_start == windows[1].evaluation_start - timedelta(days=14)


def test_run_sharded_universe_filters_overlap_by_entry_timestamp(monkeypatch) -> None:
    strategy_id = "atp_companion_v1__benchmark_mgc_asia_us"
    universe = {
        "atp_lanes": [
            {
                "strategy_id": strategy_id,
                "study_id": strategy_id,
                "symbol": "MGC",
                "display_name": "ATP",
                "lane_status": "approved",
                "study_mode": "baseline_parity_mode",
                "family": "active_trend_participation_engine",
                "cohort": "ATP_CORE",
                "allowed_sessions": {"ASIA", "US"},
                "point_value": 10,
                "reference_lane": True,
                "lane_type": "atp_core",
            }
        ],
        "atp_promotion_candidates": [],
        "approved_quant_lanes": [],
        "probationary_lanes": [],
        "expanded_universe": [],
    }
    bar_source_index = {
        "MGC": {
            "1m": retest.SourceSelection(
                symbol="MGC",
                timeframe="1m",
                data_source="historical_1m_canonical",
                sqlite_path=Path("/tmp/source.sqlite3"),
                row_count=10,
                start_ts="2024-01-01T00:00:00+00:00",
                end_ts="2024-04-30T00:00:00+00:00",
            ),
            "5m": retest.SourceSelection(
                symbol="MGC",
                timeframe="5m",
                data_source="historical_5m_canonical",
                sqlite_path=Path("/tmp/source.sqlite3"),
                row_count=10,
                start_ts="2024-01-01T00:00:00+00:00",
                end_ts="2024-04-30T00:00:00+00:00",
            ),
        }
    }
    jan_bar = ResearchBar(
        instrument="MGC",
        timeframe="1m",
        start_ts=datetime(2024, 1, 20, 14, 29, tzinfo=UTC),
        end_ts=datetime(2024, 1, 20, 14, 30, tzinfo=UTC),
        open=1.0,
        high=1.1,
        low=0.9,
        close=1.0,
        volume=1,
        session_label="US",
        session_segment="US",
        source="test",
    )
    feb_bar = ResearchBar(
        instrument="MGC",
        timeframe="1m",
        start_ts=datetime(2024, 2, 20, 14, 29, tzinfo=UTC),
        end_ts=datetime(2024, 2, 20, 14, 30, tzinfo=UTC),
        open=1.0,
        high=1.1,
        low=0.9,
        close=1.0,
        volume=1,
        session_label="US",
        session_segment="US",
        source="test",
    )

    monkeypatch.setattr(
        retest,
        "_load_symbol_context",
        lambda **kwargs: {
            "bars_1m": [jan_bar, feb_bar],
            "rolling_5m": [],
            "combined_rolling_5m": [],
            "window_completed_5m": [],
            "completed_5m_history": [],
        },
    )
    monkeypatch.setattr(
        retest,
        "_evaluate_atp_lane",
        lambda **kwargs: {
            "bars_1m": [jan_bar, feb_bar],
            "trade_rows": [
                {
                    "trade_id": "jan",
                    "entry_timestamp": "2024-01-20T14:30:00+00:00",
                    "exit_timestamp": "2024-01-20T14:31:00+00:00",
                    "entry_price": 1.0,
                    "exit_price": 1.1,
                    "side": "LONG",
                    "family": "atp",
                    "entry_session_phase": "US",
                    "exit_reason": "target",
                    "realized_pnl": 1.0,
                    "vwap_price_quality_state": "VWAP_FAVORABLE",
                    "trade_record": object(),
                },
                {
                    "trade_id": "feb",
                    "entry_timestamp": "2024-02-20T14:30:00+00:00",
                    "exit_timestamp": "2024-02-20T14:31:00+00:00",
                    "entry_price": 1.0,
                    "exit_price": 1.1,
                    "side": "LONG",
                    "family": "atp",
                    "entry_session_phase": "US",
                    "exit_reason": "target",
                    "realized_pnl": 1.0,
                    "vwap_price_quality_state": "VWAP_FAVORABLE",
                    "trade_record": object(),
                },
            ],
            "prior_trade_rows": [],
            "summary": retest._empty_summary(),
            "prior_summary": retest._empty_summary(),
        },
    )

    windows = [
        retest.RetestShardWindow(
            symbol="MGC",
            shard_id="MGC:0",
            evaluation_start=datetime(2024, 1, 1, tzinfo=UTC),
            evaluation_end=datetime(2024, 1, 31, 23, 59, tzinfo=UTC),
            load_start=datetime(2024, 1, 1, tzinfo=UTC),
            load_end=datetime(2024, 1, 31, 23, 59, tzinfo=UTC),
        ),
        retest.RetestShardWindow(
            symbol="MGC",
            shard_id="MGC:1",
            evaluation_start=datetime(2024, 2, 1, tzinfo=UTC),
            evaluation_end=datetime(2024, 2, 29, 23, 59, tzinfo=UTC),
            load_start=datetime(2024, 1, 18, tzinfo=UTC),
            load_end=datetime(2024, 2, 29, 23, 59, tzinfo=UTC),
        ),
    ]

    aggregates = retest._run_sharded_universe(
        universe=universe,
        symbol_windows=windows,
        bar_source_index=bar_source_index,
        shard_config=retest.RetestShardConfig(),
        timing=retest.TimingBreakdown(),
    )

    assert [row["trade_id"] for row in aggregates[strategy_id]["current_trade_rows"]] == ["jan", "feb"]


def test_should_emit_rich_artifact_defaults_to_buckets_and_reference_lanes() -> None:
    config = retest.RetestShardConfig()

    assert retest._should_emit_rich_artifact({"bucket": "promotable_now", "status": "retained_candidate"}, shard_config=config)
    assert retest._should_emit_rich_artifact({"bucket": "interesting_but_not_clean_enough", "status": "approved", "reference_lane": True}, shard_config=config)
    assert not retest._should_emit_rich_artifact({"bucket": "still_reject", "status": "retained_candidate"}, shard_config=config)


def test_clip_research_bars_to_exact_window_respects_true_datetimes() -> None:
    bars = [
        ResearchBar(
            instrument="MGC",
            timeframe="1m",
            start_ts=datetime.fromisoformat("2026-03-01T22:59:00+00:00"),
            end_ts=datetime.fromisoformat("2026-03-01T23:00:00+00:00"),
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            volume=1,
            session_label="US",
            session_segment="US",
            source="test",
        ),
        ResearchBar(
            instrument="MGC",
            timeframe="1m",
            start_ts=datetime.fromisoformat("2026-03-02T03:59:00+00:00"),
            end_ts=datetime.fromisoformat("2026-03-02T04:00:00+00:00"),
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            volume=1,
            session_label="ASIA",
            session_segment="ASIA",
            source="test",
        ),
    ]

    clipped = retest._clip_research_bars_to_exact_window(
        bars,
        start_timestamp=datetime.fromisoformat("2026-03-01T18:00:00-05:00"),
        end_timestamp=datetime.fromisoformat("2026-03-01T20:00:00-05:00"),
    )

    assert len(clipped) == 1
    assert clipped[0].end_ts == datetime.fromisoformat("2026-03-01T23:00:00+00:00")


def test_probationary_detector_limits_full_replay_to_triggered_lanes(monkeypatch) -> None:
    lane_a = {
        "strategy_id": "gc_us_late_pause_resume_long_turn__GC",
        "study_id": "gc_us_late_pause_resume_long_turn__GC",
        "symbol": "GC",
        "display_name": "GC / usLatePauseResumeLongTurn",
        "lane_status": "retained_candidate",
        "study_mode": "research_execution_mode",
        "family": "usLatePauseResumeLongTurn",
        "cohort": "NEXT_TIER",
        "point_value": 100,
        "lane_type": "probationary",
        "data_limit_status": "none",
        "reference_lane": False,
    }
    lane_b = {
        "strategy_id": "gc_asia_early_normal_breakout_retest_hold_turn__GC",
        "study_id": "gc_asia_early_normal_breakout_retest_hold_turn__GC",
        "symbol": "GC",
        "display_name": "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
        "lane_status": "approved_probationary",
        "study_mode": "research_execution_mode",
        "family": "asiaEarlyNormalBreakoutRetestHoldTurn",
        "cohort": "ADMITTED_COMPARATOR",
        "point_value": 100,
        "lane_type": "probationary",
        "data_limit_status": "none",
        "reference_lane": True,
    }
    universe = {
        "atp_lanes": [],
        "atp_promotion_candidates": [],
        "approved_quant_lanes": [],
        "probationary_lanes": [lane_a, lane_b],
        "expanded_universe": [],
    }
    bar_source_index = {
        "GC": {
            "1m": retest.SourceSelection(
                symbol="GC",
                timeframe="1m",
                data_source="historical_1m_canonical",
                sqlite_path=Path("/tmp/source.sqlite3"),
                row_count=100,
                start_ts="2024-01-01T00:00:00+00:00",
                end_ts="2024-01-31T00:00:00+00:00",
            ),
            "5m": retest.SourceSelection(
                symbol="GC",
                timeframe="5m",
                data_source="historical_5m_canonical",
                sqlite_path=Path("/tmp/source.sqlite3"),
                row_count=100,
                start_ts="2024-01-01T00:00:00+00:00",
                end_ts="2024-01-31T00:00:00+00:00",
            ),
        }
    }
    bars = [
        Bar(
            bar_id="GC|1m|2024-01-10T15:01:00+00:00",
            symbol="GC",
            timeframe="1m",
            start_ts=datetime(2024, 1, 10, 15, 0, tzinfo=UTC),
            end_ts=datetime(2024, 1, 10, 15, 1, tzinfo=UTC),
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
            volume=1,
            is_final=True,
            session_asia=False,
            session_london=False,
            session_us=True,
            session_allowed=True,
        )
    ]
    monkeypatch.setattr(retest, "load_settings_from_files", lambda paths: object())
    monkeypatch.setattr(
        retest,
        "_prepare_probationary_playback_bundle",
        lambda **kwargs: {
            "current_bars": bars,
            "prior_bars": bars,
            "structural_bars_5m": bars,
            "load_seconds": 0.0,
            "resample_seconds": 0.0,
        },
    )
    monkeypatch.setattr(
        retest,
        "_run_probationary_trigger_detector_from_bars",
        lambda **kwargs: {
            lane_a["strategy_id"]: {"triggered": True},
            lane_b["strategy_id"]: {"triggered": False},
        },
    )
    replay_calls: list[list[str]] = []

    def _fake_replay(**kwargs):
        replay_calls.append([str(lane["strategy_id"]) for lane in kwargs["lanes"]])
        return {}

    monkeypatch.setattr(retest, "_run_probationary_registry_playback_from_bars", _fake_replay)

    windows = [
        retest.RetestShardWindow(
            symbol="GC",
            shard_id="GC:0",
            evaluation_start=datetime(2024, 1, 1, tzinfo=UTC),
            evaluation_end=datetime(2024, 1, 31, 23, 59, tzinfo=UTC),
            load_start=datetime(2024, 1, 1, tzinfo=UTC),
            load_end=datetime(2024, 1, 31, 23, 59, tzinfo=UTC),
        )
    ]

    timing = retest.TimingBreakdown()
    retest._run_sharded_universe(
        universe=universe,
        symbol_windows=windows,
        bar_source_index=bar_source_index,
        shard_config=retest.RetestShardConfig(probationary_fast_path_mode="compiled_family_events"),
        timing=timing,
    )

    assert replay_calls == [[lane_a["strategy_id"]], [lane_a["strategy_id"]]]
    assert timing.detector_triggered_lane_count == 1
    assert timing.detector_skipped_lane_count == 1


def test_prepare_probationary_playback_bundle_falls_back_to_1m_resample_when_5m_source_is_shallow(
    monkeypatch,
) -> None:
    calls: list[tuple[str, str, str]] = []

    class _FakeHistoricalBarSource:
        def __init__(self, sqlite_path, settings) -> None:
            self.sqlite_path = sqlite_path
            self.settings = settings

        def load_bars(self, *, symbol, source_timeframe, target_timeframe, data_source, start_timestamp, end_timestamp):
            calls.append((source_timeframe, target_timeframe, data_source))
            return SimpleNamespace(playback_bars=[])

    monkeypatch.setattr(retest, "build_standalone_strategy_definitions", lambda *args, **kwargs: [object()])
    monkeypatch.setattr(retest, "build_runtime_settings", lambda *args, **kwargs: object())
    monkeypatch.setattr(retest, "SQLiteHistoricalBarSource", _FakeHistoricalBarSource)
    retest._PROBATIONARY_PLAYBACK_CACHE.clear()

    bundle = retest._prepare_probationary_playback_bundle(
        symbol="GC",
        base_settings=object(),
        bar_source_index={
            "GC": {
                "1m": retest.SourceSelection(
                    symbol="GC",
                    timeframe="1m",
                    data_source="historical_1m_canonical",
                    sqlite_path=Path("/tmp/gc_1m.sqlite3"),
                    row_count=100,
                    start_ts="2024-01-01T00:00:00+00:00",
                    end_ts="2024-01-31T23:59:00+00:00",
                ),
                "5m": retest.SourceSelection(
                    symbol="GC",
                    timeframe="5m",
                    data_source="internal",
                    sqlite_path=Path("/tmp/gc_5m.sqlite3"),
                    row_count=100,
                    start_ts="2026-03-01T00:00:00+00:00",
                    end_ts="2026-03-31T23:59:00+00:00",
                ),
            }
        },
        start_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        end_timestamp=datetime(2024, 1, 31, 23, 59, tzinfo=UTC),
    )

    assert bundle is not None
    assert calls == [
        ("1m", "1m", "historical_1m_canonical"),
        ("1m", "5m", "historical_1m_canonical"),
    ]


def test_execution_model_labels_keep_atp_separate_from_legacy_next_bar_open() -> None:
    assert retest._current_execution_model_label("atp_core") == retest.EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP
    assert retest._prior_execution_model_label("atp_core") == retest.EXECUTION_MODEL_ATP_COMPLETED_5M_1M_EXECUTABLE_VWAP
    assert retest._current_execution_model_label("probationary") == retest.EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP
    assert retest._prior_execution_model_label("probationary") == retest.EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_NEXT_BAR_OPEN_DEPRECATED
    assert retest._current_execution_model_label("atp_core") != retest.EXECUTION_MODEL_LEGACY_NEXT_BAR_OPEN


def test_build_synthetic_strategy_study_records_execution_model_label() -> None:
    bar = ResearchBar(
        instrument="MGC",
        timeframe="1m",
        start_ts=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        end_ts=datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        volume=1,
        session_label="ASIA",
        session_segment="ASIA",
        source="test",
    )
    payload = retest._build_synthetic_strategy_study(
        symbol="MGC",
        study_id="atp_companion_v1__benchmark_mgc_asia_us",
        display_name="ATP",
        strategy_family="active_trend_participation_engine",
        study_mode="research_execution_mode",
        bars_1m=[bar],
        trade_rows=[],
        point_value=Decimal("10"),
        candidate_id=None,
        entry_model="CURRENT_CANDLE_VWAP",
        execution_model_label=retest.EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP,
        pnl_truth_basis="ENRICHED_EXECUTION_TRUTH",
        lifecycle_truth_class="AUTHORITATIVE_INTRABAR_ENTRY_ONLY",
    )

    assert payload["meta"]["execution_model"] == retest.EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP


def test_execution_contracts_payload_declares_probationary_executable_vwap_current() -> None:
    payload = retest._execution_contracts_payload()

    assert payload["probationary_current"]["label"] == retest.EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP
    assert payload["probationary_current"]["fill_policy"] == "CURRENT_MINUTE_CLOSE_PROXY_WITH_VWAP_QUALITY_GATE"
    assert payload["probationary_deprecated_comparison"]["label"] == retest.EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_NEXT_BAR_OPEN_DEPRECATED
    assert payload["atp_companion_current"]["label"] == retest.EXECUTION_MODEL_ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP


def _domain_bar(*, minute: int, close: str, high: str | None = None, low: str | None = None) -> Bar:
    close_decimal = Decimal(close)
    high_decimal = Decimal(high if high is not None else close)
    low_decimal = Decimal(low if low is not None else close)
    return Bar(
        bar_id=f"MGC-1m-{minute}",
        symbol="MGC",
        timeframe="1m",
        start_ts=datetime(2024, 1, 1, 0, minute, tzinfo=UTC),
        end_ts=datetime(2024, 1, 1, 0, minute + 1, tzinfo=UTC),
        open=close_decimal,
        high=high_decimal,
        low=low_decimal,
        close=close_decimal,
        volume=10,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )


def test_probationary_same_bar_fill_blocks_chase_risk_and_fills_at_current_close() -> None:
    bar = _domain_bar(minute=0, close="100.2")
    intent = retest.OrderIntent(
        order_intent_id="entry",
        bar_id=bar.bar_id,
        symbol="MGC",
        intent_type=retest.OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=bar.end_ts,
        reason_code="usLatePauseResumeLongTurn",
    )
    feature_packet = _FeatureStub(bar_id=bar.bar_id, atr=Decimal("1.0"))

    fill = retest._build_probationary_same_bar_fill(intent=intent, bar=bar, feature_packet=feature_packet)

    assert fill is not None
    assert fill.fill_timestamp == bar.start_ts
    assert fill.fill_price == bar.close

    chase_bar = _domain_bar(minute=1, close="101.45", high="101.5", low="99.5")
    assert retest._build_probationary_same_bar_fill(intent=intent, bar=chase_bar, feature_packet=feature_packet) is None


def test_probationary_historical_executor_defaults_to_same_bar_executable_vwap(monkeypatch) -> None:
    bars = [_domain_bar(minute=index, close=str(Decimal("100.2") + Decimal(index))) for index in range(3)]
    payload = retest._empty_signal_packet_payload("structural")
    payload.update(
        {
            "long_entry": True,
            "long_entry_source": "usLatePauseResumeLongTurn",
            "short_entry": False,
            "short_entry_source": None,
        }
    )
    signal_packet = retest.SignalPacket(**payload)
    shared_context = {
        "structural_bars": [bars[0]],
        "signals_by_bar_id": {bars[0].bar_id: signal_packet},
        "features_by_bar_id": {bars[0].bar_id: _FeatureStub(bar_id=bars[0].bar_id, atr=Decimal("1.0"))},
        "bar_index_by_id": {bars[0].bar_id: 0},
    }

    monkeypatch.setattr(retest, "_advance_probationary_signal_state", lambda **kwargs: kwargs["state"])
    monkeypatch.setattr(retest, "_apply_probationary_runtime_entry_controls", lambda **kwargs: kwargs["signal_packet"])
    monkeypatch.setattr(
        retest,
        "compute_risk_context",
        lambda *args, **kwargs: SimpleNamespace(long_break_even_armed=False, short_break_even_armed=False),
    )
    monkeypatch.setattr(retest, "update_additive_short_peak_state", lambda state, *args, **kwargs: state)
    monkeypatch.setattr(
        retest,
        "evaluate_exits",
        lambda *args, **kwargs: SimpleNamespace(long_exit=False, short_exit=False, primary_reason=None),
    )

    def _intent_for_bar(*, bar, state, **kwargs):
        if state.position_side == retest.PositionSide.FLAT and bar.bar_id == bars[0].bar_id:
            return retest.OrderIntent(
                order_intent_id=f"{bar.bar_id}|entry",
                bar_id=bar.bar_id,
                symbol="MGC",
                intent_type=retest.OrderIntentType.BUY_TO_OPEN,
                quantity=1,
                created_at=bar.end_ts,
                reason_code="usLatePauseResumeLongTurn",
            )
        if state.position_side == retest.PositionSide.LONG and bar.bar_id == bars[1].bar_id:
            return retest.OrderIntent(
                order_intent_id=f"{bar.bar_id}|exit",
                bar_id=bar.bar_id,
                symbol="MGC",
                intent_type=retest.OrderIntentType.SELL_TO_CLOSE,
                quantity=1,
                created_at=bar.end_ts,
                reason_code="time_exit",
            )
        return None

    monkeypatch.setattr(retest, "_maybe_create_probationary_order_intent", _intent_for_bar)

    current_rows = retest._run_probationary_historical_executor(
        lane={"strategy_id": "lane", "symbol": "MGC"},
        settings=SimpleNamespace(symbol="MGC", trade_size=1, warmup_bars_required=lambda: 0),
        point_value=Decimal("10"),
        execution_bars=bars,
        shared_context=shared_context,
        execution_model_label=retest.EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_EXECUTABLE_VWAP,
    )
    deprecated_rows = retest._run_probationary_historical_executor(
        lane={"strategy_id": "lane", "symbol": "MGC"},
        settings=SimpleNamespace(symbol="MGC", trade_size=1, warmup_bars_required=lambda: 0),
        point_value=Decimal("10"),
        execution_bars=bars,
        shared_context=shared_context,
        execution_model_label=retest.EXECUTION_MODEL_PROBATIONARY_5M_CONTEXT_1M_NEXT_BAR_OPEN_DEPRECATED,
    )

    assert current_rows[0]["entry_timestamp"] == bars[0].end_ts.isoformat()
    assert deprecated_rows[0]["entry_timestamp"] == bars[1].end_ts.isoformat()
