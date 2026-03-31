"""Tests for standalone strategy runtime registry behavior."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import select

from mgc_v05l.app.approved_quant_lanes.engine import ApprovedQuantStrategyEngine
from mgc_v05l.app.container import ApplicationContainer
from mgc_v05l.app.runner import StrategyServiceRunner
from mgc_v05l.app.strategy_runtime_registry import (
    build_standalone_strategy_definitions,
    build_strategy_runtime_registry,
)
from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.domain.enums import LongEntryFamily, OrderStatus
from mgc_v05l.domain.models import Bar
from mgc_v05l.execution.order_models import FillEvent
from mgc_v05l.market_data.replay_feed import ReplayFeed
from mgc_v05l.persistence.tables import fills_table, order_intents_table, processed_bars_table, strategy_state_snapshots_table


def _load_runtime_settings(tmp_path: Path, standalone_strategy_definitions_json: str) -> object:
    override_path = tmp_path / "runtime_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "runtime.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "artifacts"}"',
                f"standalone_strategy_definitions_json: '{standalone_strategy_definitions_json}'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            override_path,
        ]
    )


def _build_bar(symbol: str, end_ts: datetime) -> Bar:
    return Bar(
        bar_id=f"{symbol}|5m|{end_ts.astimezone(ZoneInfo('UTC')).isoformat()}",
        symbol=symbol,
        timeframe="5m",
        start_ts=end_ts - timedelta(minutes=5),
        end_ts=end_ts,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )


def test_build_standalone_strategy_definitions_from_explicit_runtime_rows(tmp_path: Path) -> None:
    settings = _load_runtime_settings(
        tmp_path,
        '[{"lane_id":"gc_lane","display_name":"GC / usLatePauseResumeLongTurn","symbol":"GC","strategy_family":"usLatePauseResumeLongTurn","long_sources":["usLatePauseResumeLongTurn"],"session_restriction":"US_LATE","trade_size":1},{"lane_id":"pl_lane","display_name":"PL / asiaEarlyPauseResumeShortTurn","symbol":"PL","strategy_family":"asiaEarlyPauseResumeShortTurn","short_sources":["asiaEarlyPauseResumeShortTurn"],"session_restriction":"ASIA_EARLY","trade_size":1}]',
    )

    definitions = build_standalone_strategy_definitions(settings)
    keyed = {definition.lane_id: definition for definition in definitions}

    assert keyed["gc_lane"].standalone_strategy_id == "gc_us_late_pause_resume_long_turn__GC"
    assert keyed["gc_lane"].instrument == "GC"
    assert keyed["pl_lane"].standalone_strategy_id == "pl_asia_early_pause_resume_short_turn__PL"
    assert keyed["pl_lane"].strategy_family == "asiaEarlyPauseResumeShortTurn"


def test_runtime_registry_routes_bars_by_instrument_without_collision(tmp_path: Path) -> None:
    settings = _load_runtime_settings(
        tmp_path,
        '[{"lane_id":"gc_lane","display_name":"GC / usLatePauseResumeLongTurn","symbol":"GC","strategy_family":"usLatePauseResumeLongTurn","long_sources":["usLatePauseResumeLongTurn"],"session_restriction":"US_LATE","trade_size":1},{"lane_id":"pl_lane","display_name":"PL / asiaEarlyPauseResumeShortTurn","symbol":"PL","strategy_family":"asiaEarlyPauseResumeShortTurn","short_sources":["asiaEarlyPauseResumeShortTurn"],"session_restriction":"ASIA_EARLY","trade_size":1}]',
    )
    registry = build_strategy_runtime_registry(settings)

    gc_bar = _build_bar("GC", datetime(2026, 3, 23, 15, 0, tzinfo=ZoneInfo("America/New_York")))
    pl_bar = _build_bar("PL", datetime(2026, 3, 23, 15, 5, tzinfo=ZoneInfo("America/New_York")))

    gc_events = registry.process_bar(gc_bar)
    assert list(gc_events) == ["gc_us_late_pause_resume_long_turn__GC"]

    instances = {instance.definition.instrument: instance for instance in registry.instances if instance.repositories is not None}
    assert instances["GC"].repositories.processed_bars.count() == 1
    assert instances["PL"].repositories.processed_bars.count() == 0

    registry.process_bar(pl_bar)
    assert instances["GC"].repositories.processed_bars.count() == 1
    assert instances["PL"].repositories.processed_bars.count() == 1


def test_runtime_registry_summary_marks_runtime_state_loaded_after_processed_bars(tmp_path: Path) -> None:
    settings = _load_runtime_settings(
        tmp_path,
        '[{"lane_id":"gc_lane","display_name":"GC / usLatePauseResumeLongTurn","symbol":"GC","strategy_family":"usLatePauseResumeLongTurn","long_sources":["usLatePauseResumeLongTurn"],"session_restriction":"US_LATE","trade_size":1}]',
    )
    registry = build_strategy_runtime_registry(settings)
    gc_bar = _build_bar("GC", datetime(2026, 3, 23, 15, 0, tzinfo=ZoneInfo("America/New_York")))

    registry.process_bar(gc_bar)

    rows = {row["standalone_strategy_id"]: row for row in registry.summary_rows()}
    assert rows["gc_us_late_pause_resume_long_turn__GC"]["processed_bar_count"] == 1
    assert rows["gc_us_late_pause_resume_long_turn__GC"]["runtime_state_loaded"] is True


def test_runtime_registry_persists_standalone_identity_on_state_intents_and_fills(tmp_path: Path) -> None:
    settings = _load_runtime_settings(
        tmp_path,
        '[{"lane_id":"gc_lane","display_name":"GC / usLatePauseResumeLongTurn","symbol":"GC","strategy_family":"usLatePauseResumeLongTurn","long_sources":["usLatePauseResumeLongTurn"],"session_restriction":"US_LATE","trade_size":1}]',
    )
    registry = build_strategy_runtime_registry(settings)
    instance = registry.primary_engine_instance()
    assert instance is not None
    assert instance.repositories is not None
    assert instance.strategy_engine is not None

    occurred_at = datetime(2026, 3, 23, 15, 0, tzinfo=ZoneInfo("America/New_York"))
    instance.strategy_engine.set_operator_halt(occurred_at, False)
    bar = _build_bar("GC", occurred_at)
    instance.repositories.processed_bars.mark_processed(bar)
    intent = instance.strategy_engine.submit_paper_canary_entry_intent(bar)
    assert intent is not None

    fill = FillEvent(
        order_intent_id=intent.order_intent_id,
        intent_type=intent.intent_type,
        order_status=OrderStatus.FILLED,
        fill_timestamp=occurred_at + timedelta(minutes=5),
        fill_price=bar.open,
        broker_order_id="paper-gc-1",
    )
    instance.strategy_engine.apply_fill(
        fill_event=fill,
        signal_bar_id=bar.bar_id,
        long_entry_family=LongEntryFamily.K,
    )

    with instance.repositories.engine.begin() as connection:
        processed_row = connection.execute(select(processed_bars_table)).mappings().first()
        intent_row = connection.execute(select(order_intents_table)).mappings().first()
        fill_row = connection.execute(select(fills_table)).mappings().first()
        state_row = connection.execute(select(strategy_state_snapshots_table)).mappings().first()

    assert processed_row["standalone_strategy_id"] == "gc_us_late_pause_resume_long_turn__GC"
    assert intent_row["standalone_strategy_id"] == "gc_us_late_pause_resume_long_turn__GC"
    assert fill_row["standalone_strategy_id"] == "gc_us_late_pause_resume_long_turn__GC"
    assert state_row["standalone_strategy_id"] == "gc_us_late_pause_resume_long_turn__GC"
    assert intent_row["instrument"] == "GC"
    assert fill_row["instrument"] == "GC"


def test_legacy_single_symbol_config_builds_one_default_runtime_instance(tmp_path: Path) -> None:
    override_path = tmp_path / "legacy_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "legacy.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "legacy_artifacts"}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    settings = load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            override_path,
        ]
    )

    definitions = build_standalone_strategy_definitions(settings)
    registry = build_strategy_runtime_registry(settings)

    assert len(definitions) == 1
    assert definitions[0].legacy_derived_identity is True
    assert definitions[0].standalone_strategy_id == "legacy_runtime__MGC"
    assert len(registry.instances) == 1


def test_registry_can_include_first_class_approved_quant_runtime_identities(tmp_path: Path) -> None:
    override_path = tmp_path / "quant_runtime_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "quant.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "quant_artifacts"}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    settings = load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            override_path,
        ]
    )

    registry = build_strategy_runtime_registry(settings, include_approved_quant_runtime_rows=True)
    quant_instances = [
        instance for instance in registry.instances if instance.definition.runtime_kind == "approved_quant_strategy_engine"
    ]

    assert len(quant_instances) == 9
    assert {instance.definition.standalone_strategy_id for instance in quant_instances} >= {
        "breakout_metals_us_unknown_continuation__GC",
        "breakout_metals_us_unknown_continuation__MGC",
        "failed_move_no_us_reversal_short__CL",
        "failed_move_no_us_reversal_short__6E",
    }
    assert all(instance.runtime_instance_present for instance in quant_instances)
    assert all(instance.can_process_bars is True for instance in quant_instances)
    assert all(isinstance(instance.strategy_engine, ApprovedQuantStrategyEngine) for instance in quant_instances)


def test_quant_runtime_identities_participate_in_replay_coordinator_path(tmp_path: Path) -> None:
    override_path = tmp_path / "quant_replay_override.yaml"
    override_path.write_text(
        "\n".join(
            [
                f'database_url: "sqlite:///{tmp_path / "quant-replay.sqlite3"}"',
                f'probationary_artifacts_dir: "{tmp_path / "quant_replay_artifacts"}"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    settings = load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            override_path,
        ]
    )

    registry = build_strategy_runtime_registry(settings, include_approved_quant_runtime_rows=True)
    primary = registry.primary_engine_instance()
    assert primary is not None
    assert primary.repositories is not None
    assert primary.strategy_engine is not None
    container = ApplicationContainer(
        settings=settings,
        repositories=primary.repositories,
        replay_feed=ReplayFeed(settings),
        strategy_engine=primary.strategy_engine,
        strategy_runtime_registry=registry,
    )
    bar = _build_bar("GC", datetime(2026, 3, 23, 15, 0, tzinfo=ZoneInfo("America/New_York")))

    summary = StrategyServiceRunner(container).run_bars([bar])

    per_strategy = {row.standalone_strategy_id: row for row in summary.per_strategy_summaries}
    assert "breakout_metals_us_unknown_continuation__GC" in per_strategy
    assert per_strategy["breakout_metals_us_unknown_continuation__GC"].processed_bars == 1
