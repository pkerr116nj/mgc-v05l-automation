"""Tests for the additive research schema extension."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import Numeric, inspect

from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.bar_models import build_bar_id
from mgc_v05l.persistence.db import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.persistence.research_models import (
    DerivedFeatureRecord,
    ExperimentRunRecord,
    InstrumentRecord,
    SignalEvaluationRecord,
    TradeOutcomeRecord,
)
from mgc_v05l.persistence.tables import PERSISTENCE_TABLES


def _build_bar(symbol: str = "MGC", end_hour: int = 18) -> Bar:
    end_ts = datetime(2026, 3, 14, end_hour, 5, tzinfo=ZoneInfo("America/New_York"))
    start_ts = end_ts - timedelta(minutes=5)
    return Bar(
        bar_id=build_bar_id(symbol, "5m", end_ts),
        symbol=symbol,
        timeframe="5m",
        start_ts=start_ts,
        end_ts=end_ts,
        open=Decimal("100.0"),
        high=Decimal("101.0"),
        low=Decimal("99.5"),
        close=Decimal("100.75"),
        volume=150,
        is_final=True,
        session_asia=True,
        session_london=False,
        session_us=False,
        session_allowed=True,
    )


def test_schema_creation_includes_research_tables(tmp_path: Path) -> None:
    repositories = RepositorySet(build_engine(f"sqlite:///{tmp_path / 'schema.sqlite3'}"))
    inspector = inspect(repositories.engine)

    assert set(PERSISTENCE_TABLES).issubset(set(inspector.get_table_names()))


def test_numeric_columns_use_numeric_capable_types(tmp_path: Path) -> None:
    repositories = RepositorySet(build_engine(f"sqlite:///{tmp_path / 'numeric.sqlite3'}"))
    inspector = inspect(repositories.engine)

    bars_columns = {column["name"]: column["type"] for column in inspector.get_columns("bars")}
    derived_columns = {column["name"]: column["type"] for column in inspector.get_columns("derived_features")}
    signal_columns = {column["name"]: column["type"] for column in inspector.get_columns("signal_evaluations")}
    trade_columns = {column["name"]: column["type"] for column in inspector.get_columns("trade_outcomes")}
    instrument_columns = {column["name"]: column["type"] for column in inspector.get_columns("instruments")}

    assert isinstance(bars_columns["open"], Numeric)
    assert isinstance(bars_columns["high"], Numeric)
    assert isinstance(bars_columns["low"], Numeric)
    assert isinstance(bars_columns["close"], Numeric)
    assert isinstance(derived_columns["momentum_acceleration"], Numeric)
    assert isinstance(derived_columns["volume_ratio"], Numeric)
    assert isinstance(signal_columns["quality_score_long"], Numeric)
    assert isinstance(signal_columns["size_recommendation_long"], Numeric)
    assert isinstance(trade_columns["entry_price"], Numeric)
    assert isinstance(trade_columns["pnl"], Numeric)
    assert isinstance(instrument_columns["multiplier"], Numeric)


def test_instrument_upsert_behavior(tmp_path: Path) -> None:
    repositories = RepositorySet(build_engine(f"sqlite:///{tmp_path / 'instrument.sqlite3'}"))

    inserted = repositories.instruments.upsert(
        InstrumentRecord(
            ticker="MGC",
            cusip=None,
            asset_class="future",
            description="Micro Gold Futures",
            exchange="COMEX",
            multiplier=Decimal("10"),
            is_active=True,
        )
    )
    updated = repositories.instruments.upsert(
        InstrumentRecord(
            ticker="MGC",
            cusip="123456789",
            asset_class="future",
            description="Micro Gold Futures Updated",
            exchange="COMEX",
            multiplier=Decimal("10"),
            is_active=False,
        )
    )

    assert inserted.instrument_id is not None
    assert updated.instrument_id == inserted.instrument_id
    assert updated.cusip == "123456789"
    assert updated.description == "Micro Gold Futures Updated"
    assert updated.is_active is False


def test_bar_uniqueness_and_duplicate_protection(tmp_path: Path) -> None:
    repositories = RepositorySet(build_engine(f"sqlite:///{tmp_path / 'bars.sqlite3'}"))
    instrument = repositories.instruments.upsert(
        InstrumentRecord(ticker="MGC", asset_class="future", exchange="COMEX")
    )
    bar = _build_bar()

    repositories.bars.save(
        bar,
        instrument_id=instrument.instrument_id,
        asset_class=instrument.asset_class,
        data_source="schwab_history",
    )
    repositories.bars.save(
        bar,
        instrument_id=instrument.instrument_id,
        asset_class=instrument.asset_class,
        data_source="schwab_history",
    )

    stored = repositories.bars.get_row(bar.bar_id)

    assert repositories.bars.count() == 1
    assert stored is not None
    assert stored["ticker"] == "MGC"
    assert stored["instrument_id"] == instrument.instrument_id
    assert stored["data_source"] == "schwab_history"
    assert isinstance(stored["open"], Decimal)
    assert stored["open"] == Decimal("100.0000000000")
    assert isinstance(stored["close"], Decimal)


def test_store_and_retrieve_derived_features_and_signal_evaluations(tmp_path: Path) -> None:
    repositories = RepositorySet(build_engine(f"sqlite:///{tmp_path / 'features.sqlite3'}"))
    run = repositories.experiment_runs.create(
        ExperimentRunRecord(
            name="momentum-prototype",
            description="Research-only schema smoke test",
            market_universe="MGC",
            timeframe="5m",
            feature_version="v0",
            signal_version="v0",
            sizing_version="v0",
            config_json='{"mode":"research"}',
            started_at=datetime.now(ZoneInfo("UTC")),
        )
    )
    instrument = repositories.instruments.upsert(
        InstrumentRecord(ticker="MGC", asset_class="future", exchange="COMEX")
    )
    bar = _build_bar()
    repositories.bars.save(bar, instrument_id=instrument.instrument_id, asset_class=instrument.asset_class)

    saved_features = repositories.derived_features.save(
        DerivedFeatureRecord(
            bar_id=bar.bar_id,
            experiment_run_id=run.experiment_run_id,
            atr=Decimal("1.25"),
            vwap=Decimal("100.10"),
            ema_fast=Decimal("100.50"),
            ema_slow=Decimal("100.30"),
            velocity=Decimal("0.20"),
            velocity_delta=Decimal("0.05"),
            stretch_down=Decimal("1.10"),
            stretch_up=Decimal("0.60"),
            smoothed_close=Decimal("100.70"),
            momentum_raw=Decimal("0.44"),
            momentum_norm=Decimal("0.35"),
            momentum_delta=Decimal("0.08"),
            momentum_acceleration=Decimal("0.03"),
            volume_ratio=Decimal("1.40"),
            signed_impulse=Decimal("0.55"),
            smoothed_signed_impulse=Decimal("0.48"),
            impulse_delta=Decimal("0.07"),
            created_at=datetime.now(ZoneInfo("UTC")),
        )
    )
    saved_signal = repositories.signal_evaluations.save(
        SignalEvaluationRecord(
            bar_id=bar.bar_id,
            experiment_run_id=run.experiment_run_id or 0,
            bull_snap_raw=True,
            bear_snap_raw=False,
            asia_vwap_reclaim_raw=False,
            momentum_compressing_up=True,
            momentum_turning_positive=True,
            momentum_compressing_down=False,
            momentum_turning_negative=False,
            filter_pass_long=True,
            filter_pass_short=False,
            trigger_long_math=True,
            trigger_short_math=False,
            quality_score_long=Decimal("0.82"),
            quality_score_short=Decimal("0.15"),
            size_recommendation_long=Decimal("1.25"),
            size_recommendation_short=Decimal("0.25"),
            created_at=datetime.now(ZoneInfo("UTC")),
        )
    )

    fetched_features = repositories.derived_features.get_by_bar_id(bar.bar_id, run.experiment_run_id)
    fetched_signal = repositories.signal_evaluations.get_by_bar_id(bar.bar_id, run.experiment_run_id or 0)

    assert saved_features.feature_id is not None
    assert fetched_features is not None
    assert fetched_features.momentum_acceleration == Decimal("0.03")
    assert fetched_features.volume_ratio == Decimal("1.40")
    assert saved_signal.signal_eval_id is not None
    assert fetched_signal is not None
    assert fetched_signal.quality_score_long == Decimal("0.82")
    assert fetched_signal.size_recommendation_long == Decimal("1.25")
    assert isinstance(fetched_signal.size_recommendation_short, Decimal)


def test_store_trade_outcomes_linked_to_experiment_runs(tmp_path: Path) -> None:
    repositories = RepositorySet(build_engine(f"sqlite:///{tmp_path / 'trades.sqlite3'}"))
    run = repositories.experiment_runs.create(
        ExperimentRunRecord(
            name="trade-outcome-test",
            started_at=datetime.now(ZoneInfo("UTC")),
            timeframe="5m",
        )
    )
    instrument = repositories.instruments.upsert(
        InstrumentRecord(ticker="MGC", asset_class="future", exchange="COMEX")
    )
    entry_bar = _build_bar(end_hour=18)
    exit_bar = _build_bar(end_hour=19)
    repositories.bars.save(entry_bar, instrument_id=instrument.instrument_id, asset_class=instrument.asset_class)
    repositories.bars.save(exit_bar, instrument_id=instrument.instrument_id, asset_class=instrument.asset_class)

    saved_trade = repositories.trade_outcomes.save(
        TradeOutcomeRecord(
            experiment_run_id=run.experiment_run_id or 0,
            entry_bar_id=entry_bar.bar_id,
            exit_bar_id=exit_bar.bar_id,
            ticker="MGC",
            timeframe="5m",
            side="long",
            entry_family="K",
            entry_reason="bull_snap_raw",
            entry_price=Decimal("100.50"),
            exit_price=Decimal("101.25"),
            size=Decimal("1"),
            bars_held=3,
            pnl=Decimal("0.75"),
            mae=Decimal("-0.25"),
            mfe=Decimal("1.00"),
            exit_reason="LONG_TIME_EXIT",
            quality_score_at_entry=Decimal("0.84"),
            size_recommendation_at_entry=Decimal("1.10"),
            created_at=datetime.now(ZoneInfo("UTC")),
        )
    )

    trades = repositories.trade_outcomes.list_by_experiment_run(run.experiment_run_id or 0)
    loaded_run = repositories.experiment_runs.get(run.experiment_run_id or 0)

    assert saved_trade.trade_id is not None
    assert len(trades) == 1
    assert trades[0].pnl == Decimal("0.75")
    assert trades[0].entry_family == "K"
    assert isinstance(trades[0].entry_price, Decimal)
    assert isinstance(trades[0].size_recommendation_at_entry, Decimal)
    assert loaded_run is not None
    assert loaded_run.name == "trade-outcome-test"
