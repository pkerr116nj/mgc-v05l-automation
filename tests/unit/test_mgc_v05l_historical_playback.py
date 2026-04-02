"""Historical playback coverage for persisted SQLite bars."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import select

from mgc_v05l.app.historical_playback import build_trigger_report, ensure_strategy_study_artifacts, run_historical_playback
from mgc_v05l.app.main import main
from mgc_v05l.config_models.settings import RuntimeMode, StrategySettings
from mgc_v05l.domain.enums import ReplayFillPolicy, VwapPolicy
from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.bar_models import build_bar_id
from mgc_v05l.market_data.replay_feed import ReplayFeed
from mgc_v05l.market_data.sqlite_playback import SQLiteHistoricalBarSource, validate_playback_bars
from mgc_v05l.persistence.db import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.persistence.tables import fills_table


def _build_settings(database_path: Path, **overrides) -> StrategySettings:
    settings = StrategySettings(
        symbol="MGC",
        timeframe="5m",
        timezone="America/New_York",
        mode=RuntimeMode.REPLAY,
        database_url=f"sqlite:///{database_path}",
        replay_fill_policy=ReplayFillPolicy.NEXT_BAR_OPEN,
        vwap_policy=VwapPolicy.SESSION_RESET,
        trade_size=1,
        enable_bull_snap_longs=True,
        enable_bear_snap_shorts=False,
        enable_asia_vwap_longs=False,
        atr_len=2,
        stop_atr_mult=Decimal("0.10"),
        breakeven_at_r=Decimal("1.0"),
        max_bars_long=2,
        max_bars_short=2,
        allow_asia=True,
        allow_london=True,
        allow_us=True,
        asia_start=datetime.strptime("18:00", "%H:%M").time(),
        asia_end=datetime.strptime("23:00", "%H:%M").time(),
        london_start=datetime.strptime("03:00", "%H:%M").time(),
        london_end=datetime.strptime("08:30", "%H:%M").time(),
        us_start=datetime.strptime("08:30", "%H:%M").time(),
        us_end=datetime.strptime("17:00", "%H:%M").time(),
        anti_churn_bars=1,
        use_turn_family=True,
        turn_fast_len=1,
        turn_slow_len=3,
        turn_signal_len=2,
        turn_stretch_lookback=2,
        min_snap_down_stretch_atr=Decimal("0.10"),
        min_snap_bar_range_atr=Decimal("0.10"),
        min_snap_body_atr=Decimal("0.10"),
        min_snap_close_location=Decimal("0.50"),
        min_snap_velocity_delta_atr=Decimal("0.00"),
        snap_cooldown_bars=1,
        use_asia_bull_snap_thresholds=False,
        asia_min_snap_bar_range_atr=Decimal("0.10"),
        asia_min_snap_body_atr=Decimal("0.10"),
        asia_min_snap_velocity_delta_atr=Decimal("0.00"),
        use_bull_snap_location_filter=False,
        bull_snap_max_close_vs_slow_ema_atr=Decimal("10.0"),
        bull_snap_require_close_below_slow_ema=False,
        min_bear_snap_up_stretch_atr=Decimal("1.00"),
        min_bear_snap_bar_range_atr=Decimal("0.90"),
        min_bear_snap_body_atr=Decimal("0.40"),
        max_bear_snap_close_location=Decimal("0.28"),
        min_bear_snap_velocity_delta_atr=Decimal("0.16"),
        bear_snap_cooldown_bars=5,
        use_bear_snap_location_filter=False,
        bear_snap_min_close_vs_slow_ema_atr=Decimal("0.0"),
        bear_snap_require_close_above_slow_ema=False,
        below_vwap_lookback=1,
        require_green_reclaim_bar=False,
        reclaim_close_buffer_atr=Decimal("0.0"),
        min_vwap_bar_range_atr=Decimal("0.10"),
        use_vwap_volume_filter=False,
        min_vwap_vol_ratio=Decimal("1.00"),
        require_hold_close_above_vwap=False,
        require_hold_not_break_reclaim_low=False,
        require_acceptance_close_above_reclaim_high=False,
        require_acceptance_close_above_vwap=False,
        vwap_long_stop_atr_mult=Decimal("0.05"),
        vwap_long_breakeven_at_r=Decimal("0.50"),
        vwap_long_max_bars=2,
        use_vwap_hard_loss_exit=True,
        vwap_weak_close_lookback_bars=1,
        vol_len=1,
        show_debug_labels=False,
    )
    return settings.model_copy(update=overrides)


def _build_bar(*, symbol: str, timeframe: str, end_ts: datetime, open_price: str, high_price: str, low_price: str, close_price: str, volume: int = 100) -> Bar:
    minutes = int(timeframe.removesuffix("m"))
    start_ts = end_ts - timedelta(minutes=minutes)
    return Bar(
        bar_id=build_bar_id(symbol, timeframe, end_ts),
        symbol=symbol,
        timeframe=timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        open=Decimal(open_price),
        high=Decimal(high_price),
        low=Decimal(low_price),
        close=Decimal(close_price),
        volume=volume,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=False,
        session_allowed=False,
    )


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


def _replay_bars() -> list[Bar]:
    settings = _build_settings(Path("/tmp/unused.sqlite3"))
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
    csv_path = Path("/tmp/historical-playback-fixture.csv")
    csv_path.write_text(csv_rows, encoding="utf-8")
    return ReplayFeed(settings).load_csv(csv_path)


def test_validate_playback_bars_rejects_out_of_order_sequence() -> None:
    ny = ZoneInfo("America/New_York")
    first = _build_bar(symbol="MGC", timeframe="5m", end_ts=datetime(2026, 3, 13, 18, 5, tzinfo=ny), open_price="99", high_price="100", low_price="94", close_price="99")
    second = _build_bar(symbol="MGC", timeframe="5m", end_ts=datetime(2026, 3, 13, 18, 0, tzinfo=ny), open_price="100", high_price="101", low_price="99", close_price="100")

    try:
        validate_playback_bars([first, second])
    except ValueError as exc:
        assert "Out-of-order historical playback bar rejected" in str(exc)
    else:
        raise AssertionError("Expected out-of-order playback bars to be rejected.")


def test_sqlite_historical_bar_source_reads_persisted_5m_bars_in_order(tmp_path: Path) -> None:
    source_db = tmp_path / "source.sqlite3"
    settings = _build_settings(tmp_path / "playback.sqlite3")
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(symbol="MGC", timeframe="5m", end_ts=datetime(2026, 3, 13, 18, 0, tzinfo=ny), open_price="100", high_price="101", low_price="99", close_price="100"),
        _build_bar(symbol="MGC", timeframe="5m", end_ts=datetime(2026, 3, 13, 18, 5, tzinfo=ny), open_price="99", high_price="100", low_price="94", close_price="99"),
    ]
    _persist_bars(source_db, bars, data_source="schwab_history")

    loaded = SQLiteHistoricalBarSource(source_db, settings).load_bars(
        symbol="MGC",
        source_timeframe="5m",
        target_timeframe="5m",
    )

    assert loaded.data_source == "schwab_history"
    assert loaded.source_bar_count == 2
    assert [bar.end_ts for bar in loaded.playback_bars] == [bars[0].end_ts, bars[1].end_ts]


def test_sqlite_historical_bar_source_aggregates_1m_into_completed_5m_bars(tmp_path: Path) -> None:
    source_db = tmp_path / "source.sqlite3"
    settings = _build_settings(tmp_path / "playback.sqlite3")
    ny = ZoneInfo("America/New_York")
    one_minute_bars = [
        _build_bar(symbol="MGC", timeframe="1m", end_ts=datetime(2026, 3, 13, 18, minute, tzinfo=ny), open_price=str(open_price), high_price=str(high_price), low_price=str(low_price), close_price=str(close_price), volume=volume)
        for minute, open_price, high_price, low_price, close_price, volume in [
            (1, 100, 101, 99, 100, 10),
            (2, 100, 102, 99, 101, 11),
            (3, 101, 103, 100, 102, 12),
            (4, 102, 104, 101, 103, 13),
            (5, 103, 105, 102, 104, 14),
        ]
    ]
    _persist_bars(source_db, one_minute_bars, data_source="schwab_history")

    loaded = SQLiteHistoricalBarSource(source_db, settings).load_bars(
        symbol="MGC",
        source_timeframe="1m",
        target_timeframe="5m",
    )

    assert loaded.source_bar_count == 5
    assert loaded.skipped_incomplete_buckets == 0
    assert len(loaded.playback_bars) == 1
    aggregated = loaded.playback_bars[0]
    assert aggregated.open == Decimal("100")
    assert aggregated.high == Decimal("105")
    assert aggregated.low == Decimal("99")
    assert aggregated.close == Decimal("104")
    assert aggregated.volume == 60
    assert aggregated.end_ts == datetime(2026, 3, 13, 18, 5, tzinfo=ny)


def test_sqlite_historical_bar_source_aggregates_1m_into_completed_larger_timeframe_bars(tmp_path: Path) -> None:
    source_db = tmp_path / "source.sqlite3"
    settings = _build_settings(tmp_path / "playback.sqlite3")
    ny = ZoneInfo("America/New_York")
    one_minute_bars = [
        _build_bar(
            symbol="MGC",
            timeframe="1m",
            end_ts=datetime(2026, 3, 13, 18, minute, tzinfo=ny),
            open_price=str(100 + (minute - 1)),
            high_price=str(101 + (minute - 1)),
            low_price=str(99 + (minute - 1)),
            close_price=str(100.5 + (minute - 1)),
            volume=10,
        )
        for minute in range(1, 16)
    ]
    _persist_bars(source_db, one_minute_bars, data_source="schwab_history")

    loaded = SQLiteHistoricalBarSource(source_db, settings).load_bars(
        symbol="MGC",
        source_timeframe="1m",
        target_timeframe="15m",
    )

    assert loaded.source_bar_count == 15
    assert loaded.skipped_incomplete_buckets == 0
    assert len(loaded.playback_bars) == 1
    aggregated = loaded.playback_bars[0]
    assert aggregated.timeframe == "15m"
    assert aggregated.open == Decimal("100")
    assert aggregated.high == Decimal("115")
    assert aggregated.low == Decimal("99")
    assert aggregated.close == Decimal("114.5")
    assert aggregated.volume == 150
    assert aggregated.end_ts == datetime(2026, 3, 13, 18, 15, tzinfo=ny)


def test_historical_playback_runs_persisted_bars_and_writes_trigger_report(tmp_path: Path) -> None:
    source_db = tmp_path / "source.sqlite3"
    replay_db = tmp_path / "unused.sqlite3"
    output_dir = tmp_path / "outputs"
    base_config, replay_config = _write_replay_config_files(tmp_path, replay_db)
    bars = _replay_bars()
    _persist_bars(source_db, bars, data_source="schwab_history")

    result = run_historical_playback(
        config_paths=[base_config, replay_config],
        source_db_path=source_db,
        symbols=["MGC"],
        source_timeframe="5m",
        target_timeframe="5m",
        output_dir=output_dir,
        run_stamp="testplayback",
    )

    symbol_result = result.symbols[0]
    trigger_rows = json.loads(Path(symbol_result.trigger_report_json_path).read_text(encoding="utf-8"))
    summary_payload = json.loads(Path(symbol_result.summary_path).read_text(encoding="utf-8"))
    study_payload = json.loads(Path(symbol_result.strategy_study_json_path).read_text(encoding="utf-8"))
    repositories = RepositorySet(build_engine(f"sqlite:///{symbol_result.replay_db_path}"))

    assert symbol_result.processed_bars == 12
    assert symbol_result.order_intents == 2
    assert symbol_result.fills == 2
    assert repositories.processed_bars.count() == 12
    assert len(repositories.order_intents.list_all()) == 2
    assert len(repositories.fills.list_all()) == 2
    assert Path(symbol_result.trigger_report_markdown_path).exists()
    assert Path(symbol_result.strategy_study_markdown_path).exists()
    assert summary_payload["trigger_report_json_path"] == symbol_result.trigger_report_json_path
    assert summary_payload["strategy_study_json_path"] == symbol_result.strategy_study_json_path
    assert summary_payload["environment_mode"] == "baseline_parity_mode"
    assert summary_payload["structural_signal_timeframe"] == "5m"
    assert summary_payload["execution_timeframe"] == "5m"
    assert summary_payload["artifact_timeframe"] == "5m"
    assert summary_payload["execution_timeframe_role"] == "matches_signal_evaluation"
    assert summary_payload["study_contract_version"] == "strategy_study_v3"
    assert summary_payload["entry_model"] == "BASELINE_NEXT_BAR_OPEN"
    assert summary_payload["active_entry_model"] == "BASELINE_NEXT_BAR_OPEN"
    assert summary_payload["entry_model_supported"] is True
    assert summary_payload["execution_truth_emitter"] == "baseline_parity_emitter"
    assert summary_payload["authoritative_intrabar_available"] is False
    assert summary_payload["authoritative_entry_truth_available"] is False
    assert summary_payload["authoritative_exit_truth_available"] is False
    assert summary_payload["authoritative_trade_lifecycle_available"] is False
    assert summary_payload["pnl_truth_basis"] == "BASELINE_FILL_TRUTH"
    assert summary_payload["lifecycle_truth_class"] == "BASELINE_PARITY_ONLY"
    assert summary_payload["truth_provenance"]["runtime_context"] == "REPLAY"
    assert summary_payload["truth_provenance"]["run_lane"] == "BENCHMARK_REPLAY"
    assert trigger_rows[0]["lane_family"] == "firstBullSnapTurn"
    assert trigger_rows[0]["signals_seen"] == 1
    assert trigger_rows[0]["intents_created"] == 1
    assert trigger_rows[0]["fills_created"] == 1
    assert trigger_rows[0]["block_or_fault_reason"] is None
    assert study_payload["summary"]["bar_count"] == 12
    assert len(study_payload["rows"]) == 12
    assert study_payload["rows"][0]["timestamp"] == bars[0].end_ts.isoformat()
    assert study_payload["rows"][0]["bar_id"] == bars[0].bar_id
    assert study_payload["summary"]["atp_summary"]["available"] is True
    assert study_payload["summary"]["atp_summary"]["timing_available"] is False
    assert study_payload["rows"][0]["legacy_blocker_code"] is None
    assert study_payload["rows"][0]["atp_entry_blocker_code"] == "ATP_WARMUP_INCOMPLETE"

    with repositories.engine.begin() as connection:
        fill_row = connection.execute(select(fills_table).order_by(fills_table.c.fill_timestamp.asc())).mappings().first()
    assert fill_row is not None
    assert fill_row["fill_timestamp"] == "2026-03-13T18:05:00-04:00"
    assert fill_row["fill_price"] == "100.0000000000"


def test_historical_playback_can_rebuild_missing_strategy_study_artifacts_from_summary(tmp_path: Path) -> None:
    source_db = tmp_path / "source.sqlite3"
    replay_db = tmp_path / "unused.sqlite3"
    output_dir = tmp_path / "outputs"
    base_config, replay_config = _write_replay_config_files(tmp_path, replay_db)
    _persist_bars(source_db, _replay_bars(), data_source="schwab_history")

    result = run_historical_playback(
        config_paths=[base_config, replay_config],
        source_db_path=source_db,
        symbols=["MGC"],
        source_timeframe="5m",
        target_timeframe="5m",
        output_dir=output_dir,
        run_stamp="backfill_test",
    )

    symbol_result = result.symbols[0]
    summary_path = Path(symbol_result.summary_path)
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    study_json_path = Path(symbol_result.strategy_study_json_path)
    study_markdown_path = Path(symbol_result.strategy_study_markdown_path)
    study_json_path.unlink()
    study_markdown_path.unlink()
    summary_payload.pop("strategy_study_json_path", None)
    summary_payload.pop("strategy_study_markdown_path", None)
    summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    rebuilt_json_path, rebuilt_markdown_path = ensure_strategy_study_artifacts(
        summary_path=summary_path,
        summary_payload=summary_payload,
    )

    assert rebuilt_json_path == study_json_path
    assert rebuilt_markdown_path == study_markdown_path
    assert rebuilt_json_path is not None and rebuilt_json_path.exists()
    assert rebuilt_markdown_path is not None and rebuilt_markdown_path.exists()
    rebuilt_payload = json.loads(rebuilt_json_path.read_text(encoding="utf-8"))
    assert rebuilt_payload["summary"]["bar_count"] == 12
    assert rebuilt_payload["run_metadata"]["artifact_rebuilt"] is True


def test_historical_playback_cli_writes_manifest(tmp_path: Path, capsys) -> None:
    source_db = tmp_path / "source.sqlite3"
    replay_db = tmp_path / "unused.sqlite3"
    output_dir = tmp_path / "outputs"
    base_config, replay_config = _write_replay_config_files(tmp_path, replay_db)
    _persist_bars(source_db, _replay_bars(), data_source="schwab_history")

    exit_code = main(
        [
            "historical-playback",
            "--config",
            str(base_config),
            "--config",
            str(replay_config),
            "--database",
            str(source_db),
            "--symbol",
            "MGC",
            "--source-timeframe",
            "5m",
            "--output-dir",
            str(output_dir),
            "--run-stamp",
            "cli_test",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert Path(payload["manifest_path"]).exists()
    assert payload["symbols"][0]["processed_bars"] == 12
    assert payload["symbols"][0]["strategy_study_json_path"].endswith(".strategy_study.json")
    manifest_payload = json.loads(Path(payload["manifest_path"]).read_text(encoding="utf-8"))
    assert manifest_payload["symbols"][0]["strategy_study_json_path"].endswith(".strategy_study.json")
    assert manifest_payload["symbols"][0]["strategy_study_markdown_path"].endswith(".strategy_study.md")
