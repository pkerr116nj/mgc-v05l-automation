"""Tests for the research-only EMA momentum evaluation layer."""

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
from mgc_v05l.persistence.research_models import ExperimentRunRecord, InstrumentRecord, SignalEvaluationRecord
from mgc_v05l.research import EMAMomentumResearchEvaluator, EMAMomentumResearchService


def _build_settings(tmp_path: Path):
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{tmp_path / "eval.sqlite3"}"\n',
        encoding="utf-8",
    )
    return load_settings_from_files([Path("config/base.yaml"), overlay_path])


def _build_bar(end_ts: datetime, close_price: str, volume: int) -> Bar:
    start_ts = end_ts - timedelta(minutes=5)
    close = Decimal(close_price)
    return Bar(
        bar_id=build_bar_id("MGC", "5m", end_ts),
        symbol="MGC",
        timeframe="5m",
        start_ts=start_ts,
        end_ts=end_ts,
        open=close,
        high=close + Decimal("0.50"),
        low=close - Decimal("0.50"),
        close=close,
        volume=volume,
        is_final=True,
        session_asia=True,
        session_london=False,
        session_us=False,
        session_allowed=True,
    )


def _seed_run_and_bars(tmp_path: Path):
    settings = _build_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    instrument = repositories.instruments.upsert(
        InstrumentRecord(ticker="MGC", asset_class="future", exchange="COMEX")
    )
    run = repositories.experiment_runs.create(
        ExperimentRunRecord(
            name="eval-test",
            started_at=datetime.now(ZoneInfo("UTC")),
            timeframe="5m",
            feature_version="ema-v1",
            signal_version="ema-eval-v1",
        )
    )
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(datetime(2026, 3, 15, 18, 0, tzinfo=ny), "100.0", 100),
        _build_bar(datetime(2026, 3, 15, 18, 5, tzinfo=ny), "99.0", 120),
        _build_bar(datetime(2026, 3, 15, 18, 10, tzinfo=ny), "99.5", 150),
        _build_bar(datetime(2026, 3, 15, 18, 15, tzinfo=ny), "100.0", 130),
    ]
    for bar in bars:
        repositories.bars.save(bar, instrument_id=instrument.instrument_id, asset_class=instrument.asset_class)
    EMAMomentumResearchService(repositories=repositories, settings=settings).compute_and_persist(
        bars=bars,
        experiment_run_id=run.experiment_run_id or 0,
    )
    return settings, repositories, run.experiment_run_id or 0, bars


def _save_signal_row(
    repositories: RepositorySet,
    bar: Bar,
    experiment_run_id: int,
    *,
    bull_snap_raw: bool = False,
    bear_snap_raw: bool = False,
    asia_vwap_reclaim_raw: bool = False,
    momentum_compressing_up: bool = False,
    momentum_turning_positive: bool = False,
    momentum_compressing_down: bool = False,
    momentum_turning_negative: bool = False,
) -> None:
    repositories.signal_evaluations.save(
        SignalEvaluationRecord(
            bar_id=bar.bar_id,
            experiment_run_id=experiment_run_id,
            bull_snap_raw=bull_snap_raw,
            bear_snap_raw=bear_snap_raw,
            asia_vwap_reclaim_raw=asia_vwap_reclaim_raw,
            momentum_compressing_up=momentum_compressing_up,
            momentum_turning_positive=momentum_turning_positive,
            momentum_compressing_down=momentum_compressing_down,
            momentum_turning_negative=momentum_turning_negative,
            filter_pass_long=False,
            filter_pass_short=False,
            trigger_long_math=False,
            trigger_short_math=False,
            created_at=bar.end_ts,
        )
    )


def test_filter_track_labeling_logic(tmp_path: Path) -> None:
    _, repositories, run_id, bars = _seed_run_and_bars(tmp_path)
    _save_signal_row(
        repositories,
        bars[0],
        run_id,
        bull_snap_raw=True,
        momentum_compressing_up=True,
        momentum_turning_positive=True,
    )
    _save_signal_row(
        repositories,
        bars[1],
        run_id,
        bull_snap_raw=True,
        momentum_compressing_up=True,
        momentum_turning_positive=False,
    )

    results = EMAMomentumResearchEvaluator(repositories).evaluate_and_persist(bars[:2], run_id)

    assert results[0].filter_pass_long is True
    assert results[0].quality_score_long == Decimal("1")
    assert results[1].filter_pass_long is False
    assert results[1].quality_score_long == Decimal("0.5")


def test_math_trigger_labeling_logic(tmp_path: Path) -> None:
    _, repositories, run_id, bars = _seed_run_and_bars(tmp_path)
    _save_signal_row(
        repositories,
        bars[0],
        run_id,
        momentum_compressing_up=True,
        momentum_turning_positive=False,
    )
    _save_signal_row(
        repositories,
        bars[1],
        run_id,
        momentum_compressing_down=False,
        momentum_turning_negative=True,
    )

    results = EMAMomentumResearchEvaluator(repositories).evaluate_and_persist(bars[:2], run_id)

    assert results[0].trigger_long_math is True
    assert results[0].filter_pass_long is False
    assert results[1].trigger_short_math is True
    assert results[1].filter_pass_short is False


def test_causal_sequential_evaluation_is_prefix_stable(tmp_path: Path) -> None:
    _, repositories, run_id, bars = _seed_run_and_bars(tmp_path)
    for bar in bars:
        _save_signal_row(
            repositories,
            bar,
            run_id,
            bull_snap_raw=True,
            momentum_compressing_up=True,
            momentum_turning_positive=(bar is not bars[0]),
        )

    evaluator = EMAMomentumResearchEvaluator(repositories)
    prefix = evaluator.evaluate_and_persist(bars[:3], run_id)
    full = evaluator.evaluate_and_persist(bars, run_id)

    assert [(point.filter_pass_long, point.trigger_long_math, point.quality_score_long) for point in prefix] == [
        (point.filter_pass_long, point.trigger_long_math, point.quality_score_long) for point in full[:3]
    ]


def test_no_strategy_behavior_changes_baseline_context_preserved(tmp_path: Path) -> None:
    _, repositories, run_id, bars = _seed_run_and_bars(tmp_path)
    _save_signal_row(
        repositories,
        bars[0],
        run_id,
        bull_snap_raw=True,
        asia_vwap_reclaim_raw=True,
        bear_snap_raw=False,
        momentum_compressing_up=True,
        momentum_turning_positive=True,
    )

    EMAMomentumResearchEvaluator(repositories).evaluate_and_persist(bars[:1], run_id)
    row = repositories.signal_evaluations.get_by_bar_id(bars[0].bar_id, run_id)

    assert row is not None
    assert row.bull_snap_raw is True
    assert row.asia_vwap_reclaim_raw is True
    assert row.bear_snap_raw is False
    assert row.filter_pass_long is True


def test_evaluation_outputs_persist_with_research_schema(tmp_path: Path) -> None:
    _, repositories, run_id, bars = _seed_run_and_bars(tmp_path)
    _save_signal_row(
        repositories,
        bars[-1],
        run_id,
        bear_snap_raw=True,
        momentum_compressing_down=True,
        momentum_turning_negative=True,
    )

    results = EMAMomentumResearchEvaluator(repositories).evaluate_and_persist(bars, run_id)
    persisted = repositories.signal_evaluations.get_by_bar_id(bars[-1].bar_id, run_id)

    assert results[-1].baseline_short_context_present is True
    assert persisted is not None
    assert persisted.filter_pass_short is True
    assert persisted.trigger_short_math is True
    assert persisted.warmup_complete is False
    assert persisted.quality_score_short == Decimal("1")
    assert persisted.size_recommendation_short == Decimal("1")
