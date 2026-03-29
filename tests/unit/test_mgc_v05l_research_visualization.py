"""Tests for the research-only EMA visualization path."""

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
from mgc_v05l.research import (
    EMAMomentumVisualizationRequest,
    load_ema_momentum_visualization_rows,
    write_ema_momentum_visualization_html,
)


def _build_settings(tmp_path: Path):
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{tmp_path / "research_viz.sqlite3"}"\n',
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


def _seed_visualization_run(tmp_path: Path):
    settings = _build_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    instrument = repositories.instruments.upsert(
        InstrumentRecord(ticker="MGC", asset_class="future", exchange="COMEX")
    )
    run = repositories.experiment_runs.create(
        ExperimentRunRecord(
            name="ema-viz-test",
            started_at=datetime.now(ZoneInfo("UTC")),
            timeframe="5m",
            feature_version="ema-v1",
            signal_version="ema-viz-v1",
        )
    )
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(datetime(2026, 3, 15, 18, 0, tzinfo=ny), "100.0", 100),
        _build_bar(datetime(2026, 3, 15, 18, 5, tzinfo=ny), "100.5", 120),
        _build_bar(datetime(2026, 3, 15, 18, 10, tzinfo=ny), "99.8", 140),
    ]
    for index, bar in enumerate(bars):
        repositories.bars.save(bar, instrument_id=instrument.instrument_id, asset_class=instrument.asset_class)
        repositories.derived_features.save(
            DerivedFeatureRecord(
                bar_id=bar.bar_id,
                experiment_run_id=run.experiment_run_id or 0,
                vwap=Decimal("100.0"),
                smoothed_close=Decimal("100.0") + Decimal(index) / Decimal("10"),
                momentum_norm=Decimal("-0.2") + Decimal(index) / Decimal("10"),
                momentum_acceleration=Decimal("0.1") * Decimal(index),
                signed_impulse=Decimal("0.2") * Decimal(index),
                smoothed_signed_impulse=Decimal("0.15") * Decimal(index),
                created_at=bar.end_ts,
            )
        )
        repositories.signal_evaluations.save(
            SignalEvaluationRecord(
                bar_id=bar.bar_id,
                experiment_run_id=run.experiment_run_id or 0,
                bull_snap_raw=index == 0,
                bear_snap_raw=index == 2,
                asia_vwap_reclaim_raw=False,
                momentum_compressing_up=index == 0,
                momentum_turning_positive=index >= 1,
                momentum_compressing_down=False,
                momentum_turning_negative=index == 2,
                filter_pass_long=index == 0,
                filter_pass_short=False,
                trigger_long_math=index == 1,
                trigger_short_math=index == 2,
                warmup_complete=index >= 1,
                compression_long=index == 0,
                reclaim_long=index == 1,
                separation_long=index == 1,
                structure_long_candidate=index == 1,
                compression_short=index == 2,
                failure_short=index == 2,
                separation_short=index == 2,
                structure_short_candidate=index == 2,
                created_at=bar.end_ts,
            )
        )
    return repositories, run.experiment_run_id or 0, bars


def test_visualization_loader_includes_expected_labels_and_filters(tmp_path: Path) -> None:
    repositories, run_id, bars = _seed_visualization_run(tmp_path)

    rows = load_ema_momentum_visualization_rows(
        repositories,
        EMAMomentumVisualizationRequest(
            experiment_run_id=run_id,
            ticker="MGC",
            timeframe="5m",
            start_timestamp=bars[1].end_ts,
        ),
    )

    assert len(rows) == 2
    assert rows[0].timestamp == bars[1].end_ts.isoformat()
    assert rows[0].trigger_long_math is True
    assert rows[0].reclaim_long is True
    assert rows[0].structure_long_candidate is True
    assert rows[1].trigger_short_math is True
    assert rows[1].failure_short is True
    assert rows[1].structure_short_candidate is True


def test_visualization_html_artifact_generation(tmp_path: Path) -> None:
    repositories, run_id, _ = _seed_visualization_run(tmp_path)
    rows = load_ema_momentum_visualization_rows(
        repositories,
        EMAMomentumVisualizationRequest(experiment_run_id=run_id),
    )
    output_path = tmp_path / "ema_viz.html"

    written = write_ema_momentum_visualization_html(rows, output_path, title="MGC EMA Viz")
    html = written.read_text(encoding="utf-8")

    assert written == output_path
    assert "MGC EMA Viz" in html
    assert "trigger_long_math" in html
    assert "structure_long_candidate" in html
    assert "compression_short" in html
    assert "Research-only historical visualization" in html


def test_visualization_loader_is_read_only(tmp_path: Path) -> None:
    repositories, run_id, bars = _seed_visualization_run(tmp_path)
    before = repositories.signal_evaluations.get_by_bar_id(bars[1].bar_id, run_id)

    rows = load_ema_momentum_visualization_rows(
        repositories,
        EMAMomentumVisualizationRequest(experiment_run_id=run_id),
    )
    after = repositories.signal_evaluations.get_by_bar_id(bars[1].bar_id, run_id)

    assert len(rows) == 3
    assert before == after
