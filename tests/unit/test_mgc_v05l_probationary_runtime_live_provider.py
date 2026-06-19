from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import mgc_v05l.app.probationary_runtime as probationary_runtime_module
from mgc_v05l.config_models import (
    MarketDataProvider,
    ProbationaryPaperMarketDataSource,
    load_settings_from_files,
)
from mgc_v05l.market_data.live_feed import DatabentoRawLivePollingClient, Phase1RuntimeArtifactPollingClient
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.repositories import RepositorySet


def _settings(tmp_path: Path):
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text(
        'mode: "paper"\n'
        f'database_url: "sqlite:///{tmp_path / "probationary.sqlite3"}"\n'
        f'probationary_artifacts_dir: "{tmp_path / "paper_session"}"\n',
        encoding="utf-8",
    )
    base = load_settings_from_files([Path("config/base.yaml"), overlay_path])
    return base.model_copy(update={"market_data_provider": MarketDataProvider.DATABENTO, "symbol": "MGC"})


def test_build_live_polling_service_uses_databento_live_gateway_when_provider_is_databento(tmp_path: Path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))

    class _FakeProvider:
        def __init__(self, _settings, *, repo_root=None):
            self.settings = _settings
            self.repo_root = repo_root

        def describe_symbol(self, internal_symbol: str):
            assert internal_symbol == "MGC"
            return {"dataset": "GLBX.MDP3", "schema_by_timeframe": {"1m": "ohlcv-1m"}}

    monkeypatch.setattr(probationary_runtime_module, "DatabentoMarketDataProvider", _FakeProvider)
    monkeypatch.setattr(
        probationary_runtime_module,
        "CanonicalMarketDataMaintenanceService",
        lambda database_url: SimpleNamespace(database_url=database_url),
    )

    def _unexpected_load_schwab_market_data_config(_path):
        raise AssertionError("Schwab config should not be loaded for Databento live polling.")

    monkeypatch.setattr(
        probationary_runtime_module,
        "load_schwab_market_data_config",
        _unexpected_load_schwab_market_data_config,
    )

    service = probationary_runtime_module._build_live_polling_service(settings, repositories, None)

    assert isinstance(service._client, DatabentoRawLivePollingClient)  # noqa: SLF001
    assert service._data_source == "databento_live"  # noqa: SLF001
    assert service._provider == "databento"  # noqa: SLF001
    assert service._provenance_tag == "databento_raw_live_poll"  # noqa: SLF001


def test_build_live_polling_service_selects_phase1_artifact_source_for_configured_paper_runtime(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _settings(tmp_path).model_copy(
        update={"probationary_paper_market_data_source": ProbationaryPaperMarketDataSource.PHASE1_RUNTIME_ARTIFACT}
    )
    repositories = RepositorySet(build_engine(settings.database_url))

    def _unexpected_provider(*_args, **_kwargs):
        raise AssertionError("Direct Databento provider should not be built for phase1_runtime_artifact source.")

    monkeypatch.setattr(probationary_runtime_module, "DatabentoMarketDataProvider", _unexpected_provider)
    monkeypatch.setattr(
        probationary_runtime_module,
        "CanonicalMarketDataMaintenanceService",
        lambda database_url: SimpleNamespace(database_url=database_url),
    )

    service = probationary_runtime_module._build_live_polling_service(settings, repositories, None)

    assert isinstance(service._client, Phase1RuntimeArtifactPollingClient)  # noqa: SLF001
    assert service._data_source == "phase1_runtime_artifact"  # noqa: SLF001
    assert service._provider == "databento_phase1_runtime_artifact"  # noqa: SLF001
    assert service._provenance_tag == "DATABENTO_REALTIME_PHASE1"  # noqa: SLF001


def test_ibkr_paper_bridge_lane_uses_phase1_artifacts_even_when_profile_is_provider_live_poll(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    spec = probationary_runtime_module.ProbationaryPaperLaneSpec(
        lane_id="mes_london_open_active_participation_long",
        display_name="MES London Open Long",
        symbol="MES",
        long_sources=("PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1",),
        short_sources=(),
        session_restriction="LONDON_OPEN",
        point_value="5",
        trade_size=1,
        catastrophic_open_loss="-300",
        lane_mode="PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
        strategy_family="paper_active_evidence",
        runtime_kind="track_b_rule_runner_paper_strategy_engine",
        execution_mode=probationary_runtime_module.PAPER_EXECUTION_MODE_IBKR_BRIDGE,
        execution_timeframe="1m",
        structural_signal_timeframe="1m",
        artifact_timeframe="1m",
        context_timeframes=("1m",),
        live_poll_lookback_minutes=1440,
        database_url=f"sqlite:///{tmp_path / 'lane.sqlite3'}",
        artifacts_dir=str(tmp_path / "lane"),
        paper_only=True,
        runtime_overlay_params={
            "execution_mode": probationary_runtime_module.PAPER_EXECUTION_MODE_IBKR_BRIDGE,
            "current_order_destination": "ibkr_paper_bridge_submit_capable",
        },
    )

    lane_settings = probationary_runtime_module._build_probationary_paper_lane_settings(settings, spec)

    assert settings.probationary_paper_market_data_source is ProbationaryPaperMarketDataSource.PROVIDER_LIVE_POLL
    assert lane_settings.probationary_paper_market_data_source is ProbationaryPaperMarketDataSource.PHASE1_RUNTIME_ARTIFACT


def test_mnq_restored_review_overlay_selects_phase1_artifact_source() -> None:
    settings = load_settings_from_files(
        [
            Path("config/base.yaml"),
            Path("config/live.yaml"),
            Path("config/probationary_pattern_engine.yaml"),
            Path("config/headless_supervised_paper_runtime.yaml"),
            Path("config/probationary_pattern_engine_paper.yaml"),
            Path("config/probationary_pattern_engine_paper_mnq_restored_review.yaml"),
        ]
    )

    assert settings.mode.value == "paper"
    assert settings.probationary_paper_market_data_source is ProbationaryPaperMarketDataSource.PHASE1_RUNTIME_ARTIFACT
    assert settings.market_data_provider is MarketDataProvider.DATABENTO
