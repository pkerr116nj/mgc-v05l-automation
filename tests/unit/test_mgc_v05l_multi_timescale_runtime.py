from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from mgc_v05l.config_models import EnvironmentMode, RuntimeMode, StrategySettings, load_settings_from_files
from mgc_v05l.domain.models import Bar
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.strategy.strategy_engine import StrategyEngine


def _settings(tmp_path: Path, **updates: object) -> StrategySettings:
    base = load_settings_from_files([Path("config/base.yaml")])
    payload = {
        **base.model_dump(),
        "mode": RuntimeMode.LIVE,
        "environment_mode": EnvironmentMode.LIVE_EXECUTION,
        "database_url": f"sqlite:///{tmp_path / 'multi_timescale.sqlite3'}",
        "probationary_artifacts_dir": str(tmp_path / "outputs"),
        "structural_signal_timeframe": "5m",
        "execution_timeframe": "1m",
        "artifact_timeframe": "5m",
        "context_timeframes": ("5m",),
    }
    payload.update(updates)
    return StrategySettings(**payload)


def _bar(index: int, *, close: str = "100.0") -> Bar:
    end_ts = datetime(2026, 4, 2, 14, 0, tzinfo=timezone.utc) + timedelta(minutes=index)
    return Bar(
        bar_id=f"MGC|1m|{end_ts.isoformat()}",
        symbol="MGC",
        timeframe="1m",
        start_ts=end_ts - timedelta(minutes=1),
        end_ts=end_ts,
        open=Decimal(close),
        high=Decimal(close),
        low=Decimal(close),
        close=Decimal(close),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )


def test_multi_timescale_settings_reject_context_below_execution(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="context_timeframes must not be lower than execution_timeframe"):
        _settings(
            tmp_path,
            structural_signal_timeframe="5m",
            execution_timeframe="5m",
            context_timeframes=("1m",),
        )


def test_shared_paper_config_declares_1m_execution_with_completed_5m_context() -> None:
    settings = load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
        ]
    )

    assert settings.mode is RuntimeMode.PAPER
    assert settings.resolved_execution_timeframe == "1m"
    assert settings.resolved_structural_signal_timeframe == "5m"
    assert settings.resolved_context_timeframes == ("5m",)


def test_strategy_engine_evaluates_each_1m_bar_and_only_advances_completed_5m_context(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    engine = StrategyEngine(settings=settings, repositories=repositories)

    first_four = [_bar(index) for index in range(1, 5)]
    for bar in first_four:
        engine.process_bar(bar)

    cadence_before_context = engine.runtime_cadence_snapshot()
    assert cadence_before_context["execution_timeframe"] == "1m"
    assert cadence_before_context["context_timeframes"] == ["5m"]
    assert cadence_before_context["last_execution_bar_id"] == first_four[-1].bar_id
    assert cadence_before_context["last_execution_bar_evaluated_at"] == first_four[-1].end_ts.isoformat()
    assert cadence_before_context["last_completed_context_bars_at"] == {"5m": None}
    assert repositories.processed_bars.count() == 4
    assert repositories.bars.list_recent(symbol="MGC", timeframe="5m", limit=10) == []

    fifth_bar = _bar(5)
    engine.process_bar(fifth_bar)

    cadence_at_first_context_close = engine.runtime_cadence_snapshot()
    assert cadence_at_first_context_close["last_execution_bar_id"] == fifth_bar.bar_id
    assert cadence_at_first_context_close["last_execution_bar_evaluated_at"] == fifth_bar.end_ts.isoformat()
    assert cadence_at_first_context_close["last_completed_context_bars_at"] == {"5m": fifth_bar.end_ts.isoformat()}
    resampled_context = repositories.bars.list_recent(symbol="MGC", timeframe="5m", limit=10)
    assert len(resampled_context) == 1
    assert resampled_context[0].end_ts == fifth_bar.end_ts

    sixth_bar = _bar(6)
    engine.process_bar(sixth_bar)

    cadence_after_next_execution = engine.runtime_cadence_snapshot()
    assert cadence_after_next_execution["last_execution_bar_id"] == sixth_bar.bar_id
    assert cadence_after_next_execution["last_execution_bar_evaluated_at"] == sixth_bar.end_ts.isoformat()
    assert cadence_after_next_execution["last_completed_context_bars_at"] == {"5m": fifth_bar.end_ts.isoformat()}
    assert repositories.processed_bars.count() == 6
    assert repositories.processed_bars.latest_end_ts() == sixth_bar.end_ts
