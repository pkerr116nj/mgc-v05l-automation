"""Tests for research-only EMA structure labels."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.bar_models import build_bar_id
from mgc_v05l.persistence.db import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.persistence.research_models import (
    DerivedFeatureRecord,
    ExperimentRunRecord,
    InstrumentRecord,
    SignalEvaluationRecord,
)
from mgc_v05l.research import EMAStructureResearchLabeler


def _build_settings(tmp_path: Path):
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{tmp_path / "structure.sqlite3"}"\n',
        encoding="utf-8",
    )
    return load_settings_from_files([Path("config/base.yaml"), overlay_path])


def _build_bar(
    end_ts: datetime,
    open_price: str,
    high_price: str,
    low_price: str,
    close_price: str,
    volume: int = 100,
) -> Bar:
    start_ts = end_ts - timedelta(minutes=5)
    return Bar(
        bar_id=build_bar_id("MGC", "5m", end_ts),
        symbol="MGC",
        timeframe="5m",
        start_ts=start_ts,
        end_ts=end_ts,
        open=Decimal(open_price),
        high=Decimal(high_price),
        low=Decimal(low_price),
        close=Decimal(close_price),
        volume=volume,
        is_final=True,
        session_asia=True,
        session_london=False,
        session_us=False,
        session_allowed=True,
    )


def _seed_run(tmp_path: Path):
    settings = _build_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    instrument = repositories.instruments.upsert(
        InstrumentRecord(ticker="MGC", asset_class="future", exchange="COMEX")
    )
    run = repositories.experiment_runs.create(
        ExperimentRunRecord(
            name="structure-test",
            started_at=datetime.now(ZoneInfo("UTC")),
            timeframe="5m",
            feature_version="ema-v1",
            signal_version="ema-structure-v1",
        )
    )
    return repositories, instrument, run.experiment_run_id or 0


def _save_bar_feature_and_signal(
    repositories: RepositorySet,
    instrument: InstrumentRecord,
    run_id: int,
    bar: Bar,
    *,
    atr: str = "1.0",
    vwap: str = "100.0",
    momentum_norm: str = "0.0",
    momentum_acceleration: str = "0.0",
    signed_impulse: str = "0.0",
) -> None:
    repositories.bars.save(
        bar,
        instrument_id=instrument.instrument_id,
        asset_class=instrument.asset_class,
        data_source="replay",
    )
    repositories.derived_features.save(
        DerivedFeatureRecord(
            bar_id=bar.bar_id,
            experiment_run_id=run_id,
            atr=Decimal(atr),
            vwap=Decimal(vwap),
            momentum_norm=Decimal(momentum_norm),
            momentum_acceleration=Decimal(momentum_acceleration),
            signed_impulse=Decimal(signed_impulse),
            created_at=bar.end_ts,
        )
    )
    repositories.signal_evaluations.save(
        SignalEvaluationRecord(
            bar_id=bar.bar_id,
            experiment_run_id=run_id,
            bull_snap_raw=False,
            bear_snap_raw=False,
            asia_vwap_reclaim_raw=False,
            momentum_compressing_up=False,
            momentum_turning_positive=False,
            momentum_compressing_down=False,
            momentum_turning_negative=False,
            filter_pass_long=False,
            filter_pass_short=False,
            trigger_long_math=False,
            trigger_short_math=False,
            created_at=bar.end_ts,
        )
    )


def test_long_compression_labeling(tmp_path: Path) -> None:
    repositories, instrument, run_id = _seed_run(tmp_path)
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(datetime(2026, 3, 15, 18, 0, tzinfo=ny), "100.0", "100.2", "99.6", "99.8"),
        _build_bar(datetime(2026, 3, 15, 18, 5, tzinfo=ny), "99.8", "100.0", "99.5", "99.9"),
    ]
    _save_bar_feature_and_signal(
        repositories, instrument, run_id, bars[0], momentum_norm="-0.80", momentum_acceleration="0.00", signed_impulse="-1.00"
    )
    _save_bar_feature_and_signal(
        repositories, instrument, run_id, bars[1], momentum_norm="-0.30", momentum_acceleration="0.20", signed_impulse="-0.30"
    )

    results = EMAStructureResearchLabeler(repositories).label_and_persist(bars, run_id)

    assert results[0].compression_long is False
    assert results[1].compression_long is True
    persisted = repositories.signal_evaluations.get_by_bar_id(bars[1].bar_id, run_id)
    assert persisted is not None
    assert persisted.compression_long is True


def test_short_compression_labeling(tmp_path: Path) -> None:
    repositories, instrument, run_id = _seed_run(tmp_path)
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(datetime(2026, 3, 15, 18, 0, tzinfo=ny), "100.0", "100.6", "99.9", "100.4"),
        _build_bar(datetime(2026, 3, 15, 18, 5, tzinfo=ny), "100.4", "100.5", "99.8", "100.0"),
    ]
    _save_bar_feature_and_signal(
        repositories, instrument, run_id, bars[0], momentum_norm="0.90", momentum_acceleration="0.00", signed_impulse="1.20"
    )
    _save_bar_feature_and_signal(
        repositories, instrument, run_id, bars[1], momentum_norm="0.25", momentum_acceleration="-0.30", signed_impulse="0.20"
    )

    results = EMAStructureResearchLabeler(repositories).label_and_persist(bars, run_id)

    assert results[1].compression_short is True
    persisted = repositories.signal_evaluations.get_by_bar_id(bars[1].bar_id, run_id)
    assert persisted is not None
    assert persisted.compression_short is True


def test_reclaim_and_failure_labels_on_simple_sequences(tmp_path: Path) -> None:
    repositories, instrument, run_id = _seed_run(tmp_path)
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(datetime(2026, 3, 15, 18, 0, tzinfo=ny), "99.2", "99.8", "98.9", "99.3"),
        _build_bar(datetime(2026, 3, 15, 18, 5, tzinfo=ny), "99.3", "99.9", "99.0", "99.4"),
        _build_bar(datetime(2026, 3, 15, 18, 10, tzinfo=ny), "99.5", "100.7", "99.4", "100.4"),
        _build_bar(datetime(2026, 3, 15, 18, 15, tzinfo=ny), "100.4", "100.5", "99.0", "99.1"),
    ]
    _save_bar_feature_and_signal(
        repositories, instrument, run_id, bars[0], vwap="100.0", momentum_norm="-0.60", momentum_acceleration="0.00", signed_impulse="-0.60"
    )
    _save_bar_feature_and_signal(
        repositories, instrument, run_id, bars[1], vwap="100.0", momentum_norm="-0.40", momentum_acceleration="0.10", signed_impulse="-0.30"
    )
    _save_bar_feature_and_signal(
        repositories, instrument, run_id, bars[2], vwap="100.0", momentum_norm="-0.10", momentum_acceleration="0.15", signed_impulse="0.40"
    )
    _save_bar_feature_and_signal(
        repositories, instrument, run_id, bars[3], vwap="100.0", momentum_norm="0.20", momentum_acceleration="-0.20", signed_impulse="-0.50"
    )

    results = EMAStructureResearchLabeler(repositories).label_and_persist(bars, run_id)

    assert results[2].reclaim_long is True
    assert results[3].failure_short is True


def test_separation_and_combined_candidate_labels(tmp_path: Path) -> None:
    repositories, instrument, run_id = _seed_run(tmp_path)
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(datetime(2026, 3, 15, 18, 0, tzinfo=ny), "99.4", "99.8", "99.0", "99.2"),
        _build_bar(datetime(2026, 3, 15, 18, 5, tzinfo=ny), "99.2", "100.3", "99.1", "100.2"),
        _build_bar(datetime(2026, 3, 15, 18, 10, tzinfo=ny), "100.4", "100.8", "100.35", "100.6"),
        _build_bar(datetime(2026, 3, 15, 18, 15, tzinfo=ny), "100.6", "100.8", "99.7", "100.1"),
        _build_bar(datetime(2026, 3, 15, 18, 20, tzinfo=ny), "99.8", "99.9", "99.1", "99.2"),
    ]
    specs = [
        {"momentum_norm": "-0.80", "momentum_acceleration": "0.00", "signed_impulse": "-0.90"},
        {"momentum_norm": "-0.30", "momentum_acceleration": "0.20", "signed_impulse": "-0.10"},
        {"momentum_norm": "0.10", "momentum_acceleration": "0.10", "signed_impulse": "0.40"},
        {"momentum_norm": "0.70", "momentum_acceleration": "0.00", "signed_impulse": "0.80"},
        {"momentum_norm": "0.20", "momentum_acceleration": "-0.25", "signed_impulse": "0.10"},
    ]
    for bar, spec in zip(bars, specs):
        _save_bar_feature_and_signal(
            repositories,
            instrument,
            run_id,
            bar,
            atr="1.0",
            vwap="100.0",
            **spec,
        )

    results = EMAStructureResearchLabeler(repositories).label_and_persist(bars, run_id)

    assert results[2].separation_long is True
    assert results[2].structure_long_candidate is True
    assert results[4].separation_short is True
    assert results[4].structure_short_candidate is True


def test_structure_labels_are_causal_and_prefix_stable(tmp_path: Path) -> None:
    repositories, instrument, run_id = _seed_run(tmp_path)
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(datetime(2026, 3, 15, 18, 0, tzinfo=ny), "99.4", "99.8", "99.0", "99.2"),
        _build_bar(datetime(2026, 3, 15, 18, 5, tzinfo=ny), "99.2", "100.3", "99.1", "100.2"),
        _build_bar(datetime(2026, 3, 15, 18, 10, tzinfo=ny), "100.4", "100.8", "100.35", "100.6"),
        _build_bar(datetime(2026, 3, 15, 18, 15, tzinfo=ny), "100.6", "100.7", "100.0", "100.1"),
    ]
    specs = [
        {"momentum_norm": "-0.80", "momentum_acceleration": "0.00", "signed_impulse": "-0.90"},
        {"momentum_norm": "-0.30", "momentum_acceleration": "0.20", "signed_impulse": "-0.10"},
        {"momentum_norm": "0.10", "momentum_acceleration": "0.10", "signed_impulse": "0.40"},
        {"momentum_norm": "0.20", "momentum_acceleration": "-0.05", "signed_impulse": "0.20"},
    ]
    for bar, spec in zip(bars, specs):
        _save_bar_feature_and_signal(
            repositories,
            instrument,
            run_id,
            bar,
            atr="1.0",
            vwap="100.0",
            **spec,
        )

    labeler = EMAStructureResearchLabeler(repositories)
    prefix = labeler.label_and_persist(bars[:3], run_id)
    full = labeler.label_and_persist(bars, run_id)

    assert [
        (
            point.compression_long,
            point.reclaim_long,
            point.separation_long,
            point.structure_long_candidate,
        )
        for point in prefix
    ] == [
        (
            point.compression_long,
            point.reclaim_long,
            point.separation_long,
            point.structure_long_candidate,
        )
        for point in full[:3]
    ]
