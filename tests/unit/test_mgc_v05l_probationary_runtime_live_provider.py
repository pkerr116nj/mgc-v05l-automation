from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import mgc_v05l.app.probationary_runtime as probationary_runtime_module
from mgc_v05l.config_models import MarketDataProvider, load_settings_from_files
from mgc_v05l.market_data.live_feed import DatabentoRawLivePollingClient
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
