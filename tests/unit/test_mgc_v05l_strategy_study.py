"""Strategy-study artifact coverage."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from mgc_v05l.app.historical_playback import run_historical_playback
from mgc_v05l.app.strategy_study import build_strategy_study, build_strategy_study_v3, normalize_strategy_study_payload
from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.config_models.settings import EnvironmentMode, ExecutionTimeframeRole
from mgc_v05l.domain.enums import OrderIntentType
from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.replay_feed import ReplayFeed
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.repositories import RepositorySet


def _persist_bars(database_path: Path, bars: list[Bar], *, data_source: str) -> None:
    repositories = RepositorySet(build_engine(f"sqlite:///{database_path}"))
    for bar in bars:
        repositories.bars.save(bar, data_source=data_source)


def _write_replay_config_files(tmp_path: Path, replay_db_path: Path) -> tuple[Path, Path]:
    base_config = tmp_path / "base.yaml"
    replay_config = tmp_path / "replay.yaml"
    base_config.write_text(Path("config/base.yaml").read_text(encoding="utf-8"), encoding="utf-8")
    replay_config.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{replay_db_path}"\n'
        "enable_bear_snap_shorts: false\n"
        "enable_asia_vwap_longs: false\n"
        "atr_len: 2\n"
        "max_bars_long: 2\n"
        "max_bars_short: 2\n"
        "anti_churn_bars: 1\n"
        "turn_fast_len: 1\n"
        "turn_slow_len: 3\n"
        "turn_stretch_lookback: 2\n"
        "min_snap_down_stretch_atr: 0.10\n"
        "min_snap_bar_range_atr: 0.10\n"
        "min_snap_body_atr: 0.10\n"
        "min_snap_close_location: 0.50\n"
        "min_snap_velocity_delta_atr: 0.00\n"
        "snap_cooldown_bars: 1\n"
        "use_asia_bull_snap_thresholds: false\n"
        "asia_min_snap_bar_range_atr: 0.10\n"
        "asia_min_snap_body_atr: 0.10\n"
        "asia_min_snap_velocity_delta_atr: 0.00\n"
        "use_bull_snap_location_filter: false\n"
        "bull_snap_max_close_vs_slow_ema_atr: 10.0\n"
        "bull_snap_require_close_below_slow_ema: false\n"
        "use_bear_snap_location_filter: false\n"
        "bear_snap_min_close_vs_slow_ema_atr: 0.0\n"
        "bear_snap_require_close_above_slow_ema: false\n"
        "below_vwap_lookback: 1\n"
        "require_green_reclaim_bar: false\n"
        "reclaim_close_buffer_atr: 0.0\n"
        "min_vwap_bar_range_atr: 0.10\n"
        "require_hold_close_above_vwap: false\n"
        "require_hold_not_break_reclaim_low: false\n"
        "require_acceptance_close_above_reclaim_high: false\n"
        "require_acceptance_close_above_vwap: false\n"
        "vwap_long_max_bars: 2\n"
        "vwap_weak_close_lookback_bars: 1\n"
        "vol_len: 1\n"
        "show_debug_labels: false\n",
        encoding="utf-8",
    )
    return base_config, replay_config


def _replay_bars(base_config: Path, replay_config: Path) -> list[Bar]:
    feed_settings = load_settings_from_files([base_config, replay_config])
    csv_rows = (
        "timestamp,open,high,low,close,volume\n"
        "2026-03-13T17:20:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:25:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:30:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:35:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:40:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:45:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:50:00-04:00,100,101,99,100,100\n"
        "2026-03-13T17:55:00-04:00,100,101,99,100,100\n"
        "2026-03-13T18:00:00-04:00,99,100,97,98,100\n"
        "2026-03-13T18:05:00-04:00,95,100,94,99,100\n"
        "2026-03-13T18:10:00-04:00,100,100.5,99,100.4,100\n"
        "2026-03-13T18:15:00-04:00,100.2,100.6,99.8,100.1,100\n"
    )
    csv_path = Path("/tmp/strategy-study-fixture.csv")
    csv_path.write_text(csv_rows, encoding="utf-8")
    return ReplayFeed(feed_settings).load_csv(csv_path)


def _build_atp_timing_fixture_bars() -> tuple[list[Bar], list[Bar]]:
    source_bars: list[Bar] = []
    start = datetime.fromisoformat("2026-03-13T18:00:00+00:00")
    price = 100.0
    for index in range(120):
        bar_start = start + timedelta(minutes=index)
        open_price = price
        if index < 40:
            close_price = price + 0.12
        elif index < 55:
            close_price = price - 0.08
        elif index < 75:
            close_price = price + 0.16
        elif index < 90:
            close_price = price - 0.04
        else:
            close_price = price + 0.10
        high_price = max(open_price, close_price) + 0.06
        low_price = min(open_price, close_price) - 0.06
        price = close_price
        source_bars.append(
            Bar(
                bar_id=f"MGC|1m|{(bar_start + timedelta(minutes=1)).isoformat()}",
                symbol="MGC",
                timeframe="1m",
                start_ts=bar_start,
                end_ts=bar_start + timedelta(minutes=1),
                open=Decimal(str(round(open_price, 4))),
                high=Decimal(str(round(high_price, 4))),
                low=Decimal(str(round(low_price, 4))),
                close=Decimal(str(round(close_price, 4))),
                volume=100,
                is_final=True,
                session_asia=False,
                session_london=False,
                session_us=True,
                session_allowed=True,
            )
        )

    playback_bars: list[Bar] = []
    for index in range(0, len(source_bars), 5):
        chunk = source_bars[index : index + 5]
        if len(chunk) < 5:
            break
        playback_bars.append(
            Bar(
                bar_id=f"MGC|5m|{chunk[-1].end_ts.isoformat()}",
                symbol="MGC",
                timeframe="5m",
                start_ts=chunk[0].start_ts,
                end_ts=chunk[-1].end_ts,
                open=chunk[0].open,
                high=max(bar.high for bar in chunk),
                low=min(bar.low for bar in chunk),
                close=chunk[-1].close,
                volume=sum(bar.volume for bar in chunk),
                is_final=True,
                session_asia=False,
                session_london=False,
                session_us=True,
                session_allowed=True,
            )
        )
    return source_bars, playback_bars


def test_strategy_study_rows_align_markers_and_pnl_to_persisted_truth(tmp_path: Path) -> None:
    source_db = tmp_path / "source.sqlite3"
    replay_db = tmp_path / "replay.sqlite3"
    base_config, replay_config = _write_replay_config_files(tmp_path, replay_db)
    bars = _replay_bars(base_config, replay_config)
    _persist_bars(source_db, bars, data_source="schwab_history")

    result = run_historical_playback(
        config_paths=[base_config, replay_config],
        source_db_path=source_db,
        symbols=["MGC"],
        source_timeframe="5m",
        target_timeframe="5m",
        output_dir=tmp_path / "outputs",
        run_stamp="studytest",
    )

    symbol_result = result.symbols[0]
    repositories = RepositorySet(build_engine(f"sqlite:///{symbol_result.replay_db_path}"))
    settings = load_settings_from_files([base_config, replay_config]).model_copy(update={"symbol": "MGC", "timeframe": "5m"})
    study = build_strategy_study(
        repositories=repositories,
        settings=settings,
        bars=bars,
        source_bars=bars,
        point_value=Decimal("10"),
        standalone_strategy_id=None,
        strategy_family="LEGACY_RUNTIME",
        instrument="MGC",
        run_metadata={"mode": "REPLAY", "run_stamp": "studytest"},
    )

    rows = list(study["rows"])
    rows_by_bar_id = {str(row["bar_id"]): row for row in rows}
    rows_by_start_timestamp = {str(row["start_timestamp"]): row for row in rows}
    order_rows = repositories.order_intents.list_all()
    fill_rows = repositories.fills.list_all()

    assert [row["timestamp"] for row in rows] == [bar.end_ts.isoformat() for bar in bars]
    assert study["summary"]["bar_count"] == len(bars)
    assert len(rows) == len(bars)

    entry_bar_ids = {
        str(row["bar_id"])
        for row in order_rows
        if OrderIntentType(str(row["intent_type"])) in {OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN}
    }
    exit_bar_ids = {
        str(row["bar_id"])
        for row in order_rows
        if OrderIntentType(str(row["intent_type"])) in {OrderIntentType.SELL_TO_CLOSE, OrderIntentType.BUY_TO_CLOSE}
    }
    assert entry_bar_ids
    assert exit_bar_ids
    assert all(rows_by_bar_id[bar_id]["entry_marker"] is True for bar_id in entry_bar_ids)
    assert all(rows_by_bar_id[bar_id]["exit_marker"] is True for bar_id in exit_bar_ids)
    assert all(rows_by_start_timestamp[str(row["fill_timestamp"])]["fill_marker"] is True for row in fill_rows)

    entry_fill = next(row for row in fill_rows if str(row["intent_type"]) == OrderIntentType.BUY_TO_OPEN.value)
    exit_fill = next(row for row in fill_rows if str(row["intent_type"]) == OrderIntentType.SELL_TO_CLOSE.value)
    expected_realized = (Decimal(str(exit_fill["fill_price"])) - Decimal(str(entry_fill["fill_price"]))) * Decimal("10")

    assert study["summary"]["total_trades"] == 1
    assert study["summary"]["long_trades"] == 1
    assert study["summary"]["short_trades"] == 0
    assert study["summary"]["profit_factor"] == "999"
    assert study["summary"]["cumulative_realized_pnl"] == str(expected_realized)
    assert study["summary"]["closed_trade_breakdown"][0]["realized_pnl"] == str(expected_realized)
    assert study["summary"]["latest_trade_summary"]["realized_pnl"] == str(expected_realized)
    assert study["summary"]["session_trade_breakdown"][0]["trade_count"] == 1
    assert rows[-1]["cumulative_realized_pnl"] == str(expected_realized)
    assert rows[-1]["cumulative_total_pnl"] == str(expected_realized)
    assert study["summary"]["cumulative_total_pnl"] == str(expected_realized)

    assert any(row["current_bias_state"] is not None for row in rows)
    assert any(row["current_pullback_state"] is not None for row in rows)
    assert any(row["continuation_state"] is not None for row in rows)
    assert all(row["atp_timing_state"] is None for row in rows)
    assert study["summary"]["atp_summary"]["available"] is True
    assert study["summary"]["atp_summary"]["timing_available"] is False
    assert rows[0]["legacy_blocker_code"] is None
    assert rows[0]["legacy_latest_signal_state"] == "NO_SIGNAL"
    assert rows[0]["atp_entry_blocker_code"] == "ATP_WARMUP_INCOMPLETE"
    assert rows[0]["legacy_latest_signal_state"] != rows[0]["atp_entry_blocker_code"]

    persisted_study_payload = json.loads(Path(symbol_result.strategy_study_json_path).read_text(encoding="utf-8"))
    assert persisted_study_payload["contract_version"] == "strategy_study_v3"
    assert len(persisted_study_payload["bars"]) == len(bars)
    assert len(persisted_study_payload["pnl_points"]) == len(bars)
    assert persisted_study_payload["meta"]["study_mode"] == "baseline_parity_mode"
    assert persisted_study_payload["meta"]["timeframe_truth"]["structural_signal_timeframe"] == "5m"
    assert persisted_study_payload["meta"]["timeframe_truth"]["execution_timeframe"] == "5m"
    assert persisted_study_payload["meta"]["timeframe_truth"]["artifact_timeframe"] == "5m"
    assert persisted_study_payload["summary"]["bar_count"] == len(bars)
    assert persisted_study_payload["summary"]["total_trades"] == 1
    assert len(persisted_study_payload["summary"]["closed_trade_breakdown"]) == 1
    assert persisted_study_payload["summary"]["atp_summary"]["available"] is True
    assert persisted_study_payload["summary"]["atp_summary"]["timing_available"] is False


def test_strategy_study_computes_atp_timing_from_persisted_1m_source_truth(tmp_path: Path) -> None:
    repositories = RepositorySet(build_engine(f"sqlite:///{tmp_path / 'timing.sqlite3'}"))
    source_bars, playback_bars = _build_atp_timing_fixture_bars()
    settings = load_settings_from_files([Path("config/base.yaml")]).model_copy(
        update={
            "symbol": "MGC",
            "timeframe": "5m",
            "environment_mode": EnvironmentMode.RESEARCH_EXECUTION,
            "structural_signal_timeframe": "5m",
            "execution_timeframe": "1m",
            "artifact_timeframe": "5m",
            "execution_timeframe_role": ExecutionTimeframeRole.EXECUTION_DETAIL_ONLY,
        }
    )

    study = build_strategy_study(
        repositories=repositories,
        settings=settings,
        bars=playback_bars,
        source_bars=source_bars,
        point_value=None,
        standalone_strategy_id=None,
        strategy_family="LEGACY_RUNTIME",
        instrument="MGC",
        run_metadata={"mode": "REPLAY", "run_stamp": "timing-fixture"},
    )

    rows = list(study["rows"])
    confirmed_rows = [row for row in rows if row["atp_timing_state"] == "ATP_TIMING_CONFIRMED"]

    assert confirmed_rows
    assert all(row["atp_timing_confirmed"] is True for row in confirmed_rows)
    assert all(row["vwap_entry_quality_state"] is not None for row in confirmed_rows)
    assert study["summary"]["atp_summary"]["available"] is True
    assert study["summary"]["atp_summary"]["timing_available"] is True
    assert study["summary"]["atp_summary"]["ready_to_timing_confirmed_percent"] > 0
    assert study["summary"]["atp_summary"]["timing_confirmed_to_executed_percent"] == 0.0


def test_strategy_study_v3_splits_context_events_pnl_and_execution_slices(tmp_path: Path) -> None:
    repositories = RepositorySet(build_engine(f"sqlite:///{tmp_path / 'study-v3.sqlite3'}"))
    source_bars, playback_bars = _build_atp_timing_fixture_bars()
    settings = load_settings_from_files([Path("config/base.yaml")]).model_copy(
        update={
            "symbol": "MGC",
            "timeframe": "5m",
            "environment_mode": EnvironmentMode.RESEARCH_EXECUTION,
            "structural_signal_timeframe": "5m",
            "execution_timeframe": "1m",
            "artifact_timeframe": "5m",
            "execution_timeframe_role": ExecutionTimeframeRole.EXECUTION_DETAIL_ONLY,
        }
    )

    study = build_strategy_study_v3(
        repositories=repositories,
        settings=settings,
        bars=playback_bars,
        source_bars=source_bars,
        point_value=None,
        standalone_strategy_id="mgc_vwap_candidate",
        strategy_family="ACTIVE_TREND_PARTICIPATION",
        instrument="MGC",
        run_metadata={"mode": "REPLAY", "run_stamp": "study-v3"},
    )

    assert study["contract_version"] == "strategy_study_v3"
    assert len(study["bars"]) == len(playback_bars)
    assert len(study["rows"]) == len(playback_bars)
    assert len(study["pnl_points"]) == len(playback_bars)
    assert study["lifecycle_records"]
    assert study["authoritative_trade_lifecycle_records"] == study["lifecycle_records"]
    assert study["trade_events"]
    assert study["execution_slices"]
    assert study["meta"]["context_resolution"] == "5m"
    assert study["meta"]["execution_resolution"] == "1m"
    assert study["meta"]["study_mode"] == "research_execution_mode"
    assert study["meta"]["coverage_start"] == study["meta"]["coverage_range"]["start_timestamp"]
    assert study["meta"]["coverage_end"] == study["meta"]["coverage_range"]["end_timestamp"]
    assert study["meta"]["timeframe_truth"]["structural_signal_timeframe"] == "5m"
    assert study["meta"]["timeframe_truth"]["execution_timeframe"] == "1m"
    assert study["meta"]["timeframe_truth"]["execution_timeframe_role"] == "execution_detail_only"
    assert study["meta"]["entry_model"] == "CURRENT_CANDLE_VWAP"
    assert study["meta"]["active_entry_model"] == "CURRENT_CANDLE_VWAP"
    assert study["meta"]["execution_truth_emitter"] == "atp_phase3_timing_emitter"
    assert study["meta"]["supported_entry_models"] == ["BASELINE_NEXT_BAR_OPEN", "CURRENT_CANDLE_VWAP"]
    assert study["meta"]["entry_model_supported"] is True
    assert study["meta"]["intrabar_execution_authoritative"] is True
    assert study["meta"]["authoritative_intrabar_available"] is True
    assert study["meta"]["authoritative_entry_truth_available"] is True
    assert study["meta"]["authoritative_exit_truth_available"] is True
    assert study["meta"]["authoritative_trade_lifecycle_available"] is True
    assert study["meta"]["pnl_truth_basis"] == "ENRICHED_EXECUTION_TRUTH"
    assert study["meta"]["lifecycle_truth_class"] == "FULL_AUTHORITATIVE_LIFECYCLE"
    assert study["meta"]["truth_provenance"]["runtime_context"] == "REPLAY"
    assert study["meta"]["truth_provenance"]["run_lane"] == "BENCHMARK_REPLAY"
    assert study["meta"]["truth_provenance"]["persistence_origin"] == "PERSISTED_RUNTIME_TRUTH"
    assert study["meta"]["available_overlay_flags"]["execution_detail"] is True
    assert {slice_row["linked_bar_id"] for slice_row in study["execution_slices"]}.issubset(
        {bar["bar_id"] for bar in study["bars"]}
    )
    assert any(event["execution_event_type"] == "ENTRY_CONFIRMED" for event in study["trade_events"])
    assert any(event["execution_event_type"] == "ENTRY_EXECUTED" for event in study["trade_events"])
    assert any(event["execution_event_type"] == "EXIT_TRIGGERED" for event in study["trade_events"])
    assert all(
        event["source_resolution"] == "INTRABAR"
        for event in study["trade_events"]
        if event.get("execution_event_type")
    )
    assert all(
        event["event_timestamp"] >= event["decision_context_timestamp"]
        for event in study["trade_events"]
        if event.get("source_resolution") == "INTRABAR"
        and event.get("decision_context_timestamp")
        and event.get("event_timestamp")
    )
    assert any(point["pnl_truth_basis"] == "ENRICHED_EXECUTION_TRUTH" for point in study["pnl_points"])
    assert study["summary"]["pnl_truth_basis"] == "ENRICHED_EXECUTION_TRUTH"
    assert study["summary"]["total_trades"] >= 1
    assert study["summary"]["closed_trade_breakdown"]
    assert study["summary"]["latest_trade_summary"] is not None
    assert "trade_count" in study["summary"]["trade_family_breakdown"][0]
    assert study["summary"]["atp_summary"]["timing_confirmed_to_executed_percent"] > 0.0
    assert {
        "trade_id",
        "decision_id",
        "decision_ts",
        "entry_ts",
        "exit_ts",
        "decision_context_linkage_available",
        "decision_context_linkage_status",
        "entry_model",
        "pnl_truth_basis",
        "lifecycle_truth_class",
        "truth_provenance",
    }.issubset(study["lifecycle_records"][0])
    assert study["lifecycle_records"][0]["decision_context_linkage_available"] is True


def test_strategy_study_v3_baseline_parity_does_not_emit_execution_slices_without_explicit_research_mode(tmp_path: Path) -> None:
    repositories = RepositorySet(build_engine(f"sqlite:///{tmp_path / 'study-v3-baseline.sqlite3'}"))
    source_bars, playback_bars = _build_atp_timing_fixture_bars()
    settings = load_settings_from_files([Path("config/base.yaml")]).model_copy(
        update={
            "symbol": "MGC",
            "timeframe": "5m",
            "environment_mode": EnvironmentMode.BASELINE_PARITY,
            "structural_signal_timeframe": "5m",
            "execution_timeframe": "5m",
            "artifact_timeframe": "5m",
            "execution_timeframe_role": ExecutionTimeframeRole.MATCHES_SIGNAL_EVALUATION,
        }
    )

    study = build_strategy_study_v3(
        repositories=repositories,
        settings=settings,
        bars=playback_bars,
        source_bars=source_bars,
        point_value=None,
        standalone_strategy_id="legacy_runtime__MGC",
        strategy_family="LEGACY_RUNTIME",
        instrument="MGC",
        run_metadata={"mode": "REPLAY", "run_stamp": "study-v3-baseline"},
    )

    assert study["meta"]["study_mode"] == "baseline_parity_mode"
    assert study["meta"]["execution_resolution"] == "5m"
    assert study["meta"]["entry_model"] == "BASELINE_NEXT_BAR_OPEN"
    assert study["meta"]["active_entry_model"] == "BASELINE_NEXT_BAR_OPEN"
    assert study["meta"]["entry_model_supported"] is True
    assert study["meta"]["intrabar_execution_authoritative"] is False
    assert study["meta"]["authoritative_intrabar_available"] is False
    assert study["meta"]["authoritative_entry_truth_available"] is False
    assert study["meta"]["authoritative_exit_truth_available"] is False
    assert study["meta"]["authoritative_trade_lifecycle_available"] is False
    assert study["meta"]["pnl_truth_basis"] == "BASELINE_FILL_TRUTH"
    assert study["meta"]["lifecycle_truth_class"] == "BASELINE_PARITY_ONLY"
    assert study["execution_slices"] == []
    assert study["meta"]["available_overlay_flags"]["execution_detail"] is False
    assert all(event.get("execution_event_type") in {None, ""} for event in study["trade_events"])


def test_legacy_strategy_study_normalization_defaults_to_baseline_parity_truth() -> None:
    legacy_payload = {
        "contract_version": "strategy_study_v2",
        "generated_at": "2026-03-28T12:00:00+00:00",
        "symbol": "MGC",
        "timeframe": "5m",
        "rows": [
            {
                "bar_id": "MGC|5m|2026-03-28T12:05:00+00:00",
                "timestamp": "2026-03-28T12:05:00+00:00",
                "start_timestamp": "2026-03-28T12:00:00+00:00",
                "end_timestamp": "2026-03-28T12:05:00+00:00",
                "open": "100",
                "high": "101",
                "low": "99",
                "close": "100.5",
            }
        ],
        "summary": {"bar_count": 1, "atp_summary": {"available": False, "timing_available": False}},
        "run_metadata": {"run_stamp": "legacy"},
    }

    normalized = normalize_strategy_study_payload(legacy_payload)

    assert normalized is not None
    assert normalized["contract_version"] == "strategy_study_v3"
    assert normalized["meta"]["study_mode"] == "baseline_parity_mode"
    assert normalized["meta"]["context_resolution"] == "5m"
    assert normalized["meta"]["execution_resolution"] == "5m"
    assert normalized["meta"]["coverage_start"] == "2026-03-28T12:00:00+00:00"
    assert normalized["meta"]["coverage_end"] == "2026-03-28T12:05:00+00:00"
    assert normalized["meta"]["timeframe_truth"]["structural_signal_timeframe"] == "5m"
    assert normalized["meta"]["timeframe_truth"]["execution_timeframe"] == "5m"
    assert normalized["meta"]["timeframe_truth"]["artifact_timeframe"] == "5m"
    assert normalized["meta"]["timeframe_truth"]["execution_timeframe_role"] == "matches_signal_evaluation"
    assert normalized["meta"]["entry_model"] == "BASELINE_NEXT_BAR_OPEN"
    assert normalized["meta"]["active_entry_model"] == "BASELINE_NEXT_BAR_OPEN"
    assert normalized["meta"]["supported_entry_models"] == ["BASELINE_NEXT_BAR_OPEN"]
    assert normalized["meta"]["execution_truth_emitter"] == "baseline_parity_emitter"
    assert normalized["meta"]["entry_model_supported"] is True
    assert normalized["meta"]["intrabar_execution_authoritative"] is False
    assert normalized["meta"]["authoritative_entry_truth_available"] is False
    assert normalized["meta"]["authoritative_exit_truth_available"] is False
    assert normalized["meta"]["authoritative_trade_lifecycle_available"] is False
    assert normalized["meta"]["pnl_truth_basis"] == "BASELINE_FILL_TRUTH"
    assert normalized["meta"]["lifecycle_truth_class"] == "BASELINE_PARITY_ONLY"
    assert normalized["execution_slices"] == []


def test_strategy_study_v3_authoritative_intrabar_events_include_invalidation_without_execution(tmp_path: Path) -> None:
    study = normalize_strategy_study_payload(
        {
            "contract_version": "strategy_study_v2",
            "generated_at": "2026-03-28T12:00:00+00:00",
            "symbol": "MGC",
            "timeframe": "5m",
            "rows": [
                {
                    "bar_id": "MGC|5m|2026-03-28T12:05:00+00:00",
                    "timestamp": "2026-03-28T12:05:00+00:00",
                    "start_timestamp": "2026-03-28T12:00:00+00:00",
                    "end_timestamp": "2026-03-28T12:05:00+00:00",
                    "open": "100",
                    "high": "101",
                    "low": "99",
                    "close": "100.5",
                }
            ],
            "summary": {"bar_count": 1, "atp_summary": {"available": True, "timing_available": True}},
            "run_metadata": {"run_stamp": "invalidated"},
            "meta": {
                "study_mode": "research_execution_mode",
                "entry_model": "CURRENT_CANDLE_VWAP",
                "intrabar_execution_authoritative": True,
                "pnl_truth_basis": "ENRICHED_EXECUTION_TRUTH",
                "authoritative_intrabar_timing_states": [
                    {
                        "bar_id": "MGC|5m|2026-03-28T12:05:00+00:00",
                        "decision_ts": "2026-03-28T12:05:00+00:00",
                        "family_name": "atp_v1_long_pullback_continuation",
                        "context_entry_state": "ENTRY_ELIGIBLE",
                        "timing_state": "ATP_TIMING_INVALIDATED",
                        "vwap_price_quality_state": "VWAP_NEUTRAL",
                        "primary_blocker": "ATP_TIMING_INVALIDATED_BEFORE_ENTRY",
                        "setup_armed": True,
                        "timing_confirmed": False,
                        "executable_entry": False,
                        "invalidated_before_entry": True,
                        "setup_armed_but_not_executable": False,
                        "entry_executed": False,
                        "timing_bar_ts": "2026-03-28T12:03:00+00:00",
                        "entry_ts": None,
                        "entry_price": None,
                        "feature_snapshot": {"timing_checks": {"bar_vwap": 100.25}},
                        "side": "LONG",
                    }
                ],
                "authoritative_intrabar_trades": [],
            },
        }
    )

    assert study is not None
    assert study["meta"]["authoritative_entry_truth_available"] is True
    assert study["meta"]["authoritative_exit_truth_available"] is False
    assert study["meta"]["authoritative_trade_lifecycle_available"] is False
    assert study["meta"]["lifecycle_truth_class"] == "AUTHORITATIVE_INTRABAR_ENTRY_ONLY"
    invalidated_events = [event for event in study["trade_events"] if event.get("event_type") == "ATP_ENTRY_INVALIDATED"]
    assert invalidated_events
    for event in invalidated_events:
        matching_executions = [
            candidate
            for candidate in study["trade_events"]
            if candidate.get("event_type") == "ATP_ENTRY_EXECUTED"
            and candidate.get("decision_context_timestamp") == event.get("decision_context_timestamp")
        ]
        assert matching_executions == []


def test_strategy_study_v3_surfaces_unsupported_current_candle_model_without_silent_baseline_fallback(tmp_path: Path) -> None:
    repositories = RepositorySet(build_engine(f"sqlite:///{tmp_path / 'study-v3-unsupported.sqlite3'}"))
    source_bars, playback_bars = _build_atp_timing_fixture_bars()
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

    study = build_strategy_study_v3(
        repositories=repositories,
        settings=settings,
        bars=playback_bars,
        source_bars=source_bars,
        point_value=None,
        standalone_strategy_id="legacy_runtime__MGC",
        strategy_family="LEGACY_RUNTIME",
        instrument="MGC",
        run_metadata={"mode": "REPLAY", "run_stamp": "study-v3-unsupported"},
    )

    assert study["meta"]["entry_model"] == "CURRENT_CANDLE_VWAP"
    assert study["meta"]["active_entry_model"] == "CURRENT_CANDLE_VWAP"
    assert study["meta"]["entry_model_supported"] is False
    assert study["meta"]["execution_truth_emitter"] == "unsupported"
    assert study["meta"]["pnl_truth_basis"] == "UNSUPPORTED_ENTRY_MODEL"
    assert study["meta"]["lifecycle_truth_class"] == "UNSUPPORTED_ENTRY_MODEL"
    assert study["meta"]["unsupported_reason"]
