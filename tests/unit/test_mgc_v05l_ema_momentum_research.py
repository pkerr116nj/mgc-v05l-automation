"""Tests for EMA-based momentum research features."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.domain.models import Bar
from mgc_v05l.indicators.feature_engine import compute_features
from mgc_v05l.market_data.bar_models import build_bar_id
from mgc_v05l.persistence.db import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.persistence.research_models import ExperimentRunRecord, InstrumentRecord
from mgc_v05l.research import EMAMomentumResearchService, compute_ema_momentum_core
from mgc_v05l.strategy.trade_state import build_initial_state

_STORED_PRECISION = Decimal("0.0000000001")


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


def _build_settings(tmp_path: Path):
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{tmp_path / "research.sqlite3"}"\n',
        encoding="utf-8",
    )
    return load_settings_from_files([Path("config/base.yaml"), overlay_path])


def test_ema_smoothing_is_causal() -> None:
    prefix = compute_ema_momentum_core(
        closes=[Decimal("100"), Decimal("101"), Decimal("103")],
        atr_values=[Decimal("1"), Decimal("1"), Decimal("1")],
        volumes=[100, 100, 100],
    )
    extended = compute_ema_momentum_core(
        closes=[Decimal("100"), Decimal("101"), Decimal("103"), Decimal("110")],
        atr_values=[Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1")],
        volumes=[100, 100, 100, 100],
    )

    assert [point.smoothed_close for point in prefix] == [point.smoothed_close for point in extended[:3]]


def test_momentum_fields_compute_correctly_across_bars() -> None:
    points = compute_ema_momentum_core(
        closes=[Decimal("100"), Decimal("101"), Decimal("103")],
        atr_values=[Decimal("1"), Decimal("1"), Decimal("1")],
        volumes=[100, 100, 100],
    )

    assert [point.smoothed_close for point in points] == [
        Decimal("100"),
        Decimal("100.5"),
        Decimal("101.75"),
    ]
    assert [point.momentum_raw for point in points] == [
        Decimal("0"),
        Decimal("0.5"),
        Decimal("1.25"),
    ]
    assert [point.momentum_norm for point in points] == [
        Decimal("0"),
        Decimal("0.5"),
        Decimal("1.25"),
    ]
    assert [point.momentum_delta for point in points] == [
        Decimal("0"),
        Decimal("0.5"),
        Decimal("0.75"),
    ]
    assert [point.momentum_acceleration for point in points] == [
        Decimal("0"),
        Decimal("0.5"),
        Decimal("0.25"),
    ]


def test_normalization_floor_is_respected() -> None:
    points = compute_ema_momentum_core(
        closes=[Decimal("100"), Decimal("101")],
        atr_values=[Decimal("0"), Decimal("0")],
        volumes=[100, 100],
        normalization_floor=Decimal("0.01"),
    )

    assert points[1].momentum_raw == Decimal("0.5")
    assert points[1].momentum_norm == Decimal("50")


def test_volume_ratio_and_impulse_fields_compute_correctly() -> None:
    points = compute_ema_momentum_core(
        closes=[Decimal("100"), Decimal("101"), Decimal("100")],
        atr_values=[Decimal("1"), Decimal("1"), Decimal("1")],
        volumes=[100, 200, 100],
        volume_window=2,
    )

    assert points[0].volume_ratio == Decimal("1")
    assert points[1].volume_ratio == Decimal("200") / Decimal("150")
    assert points[1].signed_impulse == Decimal("200") / Decimal("150")
    assert points[2].volume_ratio == Decimal("100") / Decimal("150")
    assert points[2].signed_impulse == -(Decimal("100") / Decimal("150"))


def test_research_service_persists_features_and_flags(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    instrument = repositories.instruments.upsert(
        InstrumentRecord(ticker="MGC", asset_class="future", exchange="COMEX")
    )
    run = repositories.experiment_runs.create(
        ExperimentRunRecord(
            name="ema-momentum-prototype",
            started_at=datetime.now(ZoneInfo("UTC")),
            timeframe="5m",
            feature_version="ema-v1",
            signal_version="ema-v1",
        )
    )
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(datetime(2026, 3, 15, 18, 0, tzinfo=ny), "100.0", 100),
        _build_bar(datetime(2026, 3, 15, 18, 5, tzinfo=ny), "99.0", 120),
        _build_bar(datetime(2026, 3, 15, 18, 10, tzinfo=ny), "99.5", 150),
    ]
    for bar in bars:
        repositories.bars.save(bar, instrument_id=instrument.instrument_id, asset_class=instrument.asset_class)

    service = EMAMomentumResearchService(repositories=repositories, settings=settings)
    points = service.compute_and_persist(bars=bars, experiment_run_id=run.experiment_run_id or 0)

    derived = repositories.derived_features.get_by_bar_id(bars[-1].bar_id, run.experiment_run_id)
    signal_eval = repositories.signal_evaluations.get_by_bar_id(bars[-1].bar_id, run.experiment_run_id or 0)

    assert len(points) == 3
    assert derived is not None
    assert derived.smoothed_close == points[-1].smoothed_close
    assert derived.momentum_norm == points[-1].momentum_norm
    assert derived.signed_impulse == points[-1].signed_impulse.quantize(_STORED_PRECISION)
    assert signal_eval is not None
    assert signal_eval.momentum_turning_positive == points[-1].momentum_turning_positive
    assert signal_eval.filter_pass_long == points[-1].filter_pass_long
    assert signal_eval.trigger_long_math == points[-1].trigger_long_math


def test_research_service_base_features_match_feature_engine_on_small_history(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(datetime(2026, 3, 15, 18, 0, tzinfo=ny), "100.0", 100),
        _build_bar(datetime(2026, 3, 15, 18, 5, tzinfo=ny), "99.0", 120),
        _build_bar(datetime(2026, 3, 15, 18, 10, tzinfo=ny), "99.5", 150),
        _build_bar(datetime(2026, 3, 15, 18, 15, tzinfo=ny), "100.5", 140),
    ]

    points = EMAMomentumResearchService(repositories=repositories, settings=settings).compute(bars)

    state = build_initial_state(bars[0].end_ts)
    expected_features = []
    for index in range(len(bars)):
        features = compute_features(bars[: index + 1], state, settings)
        expected_features.append(features)
        state = replace(
            state,
            last_swing_low=features.last_swing_low,
            last_swing_high=features.last_swing_high,
        )

    for point, expected in zip(points, expected_features):
        assert point.atr == expected.atr
        assert point.vwap == expected.vwap
        assert point.ema_fast == expected.turn_ema_fast
        assert point.ema_slow == expected.turn_ema_slow
        assert point.velocity == expected.velocity
        assert point.velocity_delta == expected.velocity_delta
        assert point.stretch_down == expected.downside_stretch
        assert point.stretch_up == expected.upside_stretch
