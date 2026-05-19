from __future__ import annotations

import json
import socket
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import mgc_v05l.app.operator_dashboard as operator_dashboard_module
import mgc_v05l.config_models.loader as config_loader_module
from mgc_v05l.app.operator_dashboard import OperatorDashboardService
from mgc_v05l.app.probationary_runtime import (
    ProbationaryRuntimeTransportFailure,
    _run_probationary_runtime_market_data_transport_probe,
)
from mgc_v05l.config_models import MarketDataProvider, ProbationaryPaperMarketDataSource, RuntimeMode


def test_runtime_transport_probe_writes_dns_failure_artifact(monkeypatch, tmp_path: Path) -> None:
    settings = SimpleNamespace(
        probationary_artifacts_path=tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session",
        symbol="MGC",
        timeframe="5m",
        resolved_execution_timeframe="5m",
        resolved_context_timeframes=("15m",),
        timezone_info=timezone.utc,
        live_poll_lookback_minutes=180,
        market_data_provider=None,
    )
    schwab_config = SimpleNamespace(
        market_data_base_url="https://api.schwabapi.com/marketdata/v1",
        auth=SimpleNamespace(token_store_path=tmp_path / "tokens.json"),
    )
    adapter = SimpleNamespace(
        map_historical_symbol=lambda _symbol: "/MGC",
        map_timeframe=lambda _tf: SimpleNamespace(frequency_type="minute", frequency=5),
    )

    def _boom(*_args, **_kwargs):
        raise socket.gaierror(8, "nodename nor servname provided, or not known")

    monkeypatch.setattr(socket, "getaddrinfo", _boom)

    with pytest.raises(ProbationaryRuntimeTransportFailure) as excinfo:
        _run_probationary_runtime_market_data_transport_probe(
            settings=settings,
            schwab_config_path=tmp_path / "schwab.local.json",
            schwab_config=schwab_config,
            adapter=adapter,
        )

    payload = excinfo.value.payload
    assert payload["failure_kind"] == "dns_resolution_failed"
    assert payload["blocker_label"] == "market_data_transport_failure"
    assert payload["target_host"] == "api.schwabapi.com"
    assert "HTTP_PROXY" in payload["proxy_env"]
    artifact_path = Path(payload["artifact_path"])
    assert artifact_path.exists()
    stored = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert stored["failure_kind"] == "dns_resolution_failed"


def test_operator_dashboard_tolerates_market_data_transport_failure_artifact(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path
    runtime_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "market_data_transport_failure.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-01T18:10:00+00:00",
                "blocker_label": "market_data_transport_failure",
                "target_host": "api.schwabapi.com",
                "rendered_url": "https://api.schwabapi.com/marketdata/v1/pricehistory",
                "exception_text": "nodename nor servname provided, or not known",
                "next_fix": "Verify host DNS and proxy settings, then rerun the shared market-data transport probe.",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        config_loader_module,
        "load_settings_from_files",
        lambda *_args, **_kwargs: SimpleNamespace(database_url="sqlite:///tmp/test.sqlite3"),
    )
    service = OperatorDashboardService(repo_root)

    snapshot = service._runtime_snapshot("paper")  # noqa: SLF001

    assert snapshot["running"] is False
    assert snapshot["status"]["health_status"] == "UNKNOWN"
    assert snapshot["status"]["market_data_semantics"] == "UNKNOWN"


def test_runtime_transport_probe_uses_databento_without_touching_schwab(
    monkeypatch, tmp_path: Path
) -> None:
    settings = SimpleNamespace(
        probationary_artifacts_path=tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session",
        symbol="MGC",
        timeframe="1m",
        timezone_info=timezone.utc,
        live_poll_lookback_minutes=180,
        resolved_execution_timeframe="1m",
        resolved_context_timeframes=("3m",),
        market_data_provider=MarketDataProvider.DATABENTO,
    )

    class _FakeProvider:
        def __init__(self, _settings, *, repo_root=None):
            self._config = SimpleNamespace(historical_base_url="https://hist.databento.com", api_key_env="DATABENTO_API_KEY")
            self._api_key = "db-test-abcde"
            self._settings = _settings
            self.repo_root = repo_root
            self.requests = []

        def describe_symbol(self, internal_symbol: str):
            assert internal_symbol == "MGC"
            return {
                "request_symbol": "MGCM6",
                "dataset": "GLBX.MDP3",
                "schema_by_timeframe": {"1m": "ohlcv-1m"},
            }

    provider_instances: list[_FakeProvider] = []

    def _provider_factory(_settings, *, repo_root=None):
        provider = _FakeProvider(_settings, repo_root=repo_root)
        provider_instances.append(provider)
        return provider

    monkeypatch.setattr(
        "mgc_v05l.app.probationary_runtime.DatabentoMarketDataProvider",
        _provider_factory,
    )

    def _unexpected_load_schwab_market_data_config(_path):
        raise AssertionError("Schwab config should not be loaded for Databento transport probe.")

    monkeypatch.setattr(
        "mgc_v05l.app.probationary_runtime.load_schwab_market_data_config",
        _unexpected_load_schwab_market_data_config,
    )
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda host, port, type=None: [(None, None, None, None, ("34.1.2.3", port))],
    )
    transcript = iter(
        [
            "gateway=live\n",
            "challenge=1|cram=test-cram\n",
            "success=1|session_id=paper-test\n",
        ]
    )

    class _FakeSocketFile:
        def readline(self):
            return next(transcript, "")

        def write(self, _value):
            return None

        def flush(self):
            return None

    class _FakeSocket:
        def settimeout(self, _value):
            return None

        def makefile(self, *_args, **_kwargs):
            return _FakeSocketFile()

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    monkeypatch.setattr(
        "mgc_v05l.app.probationary_runtime.socket.create_connection",
        lambda *_args, **_kwargs: _FakeSocket(),
    )

    payload = _run_probationary_runtime_market_data_transport_probe(
        settings=settings,
        schwab_config_path=tmp_path / "schwab.local.json",
    )

    assert payload["status"] == "ok"
    assert payload["target_host"] == "glbx-mdp3.lsg.databento.com"
    assert payload["authenticated_probe_succeeds"] is True
    assert provider_instances


def test_phase1_artifact_paper_transport_probe_skips_direct_databento_dns(
    monkeypatch, tmp_path: Path
) -> None:
    settings = SimpleNamespace(
        probationary_artifacts_path=tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session",
        symbol="MGC",
        timeframe="1m",
        timezone_info=timezone.utc,
        live_poll_lookback_minutes=180,
        resolved_execution_timeframe="1m",
        resolved_context_timeframes=("3m",),
        market_data_provider=MarketDataProvider.DATABENTO,
        mode=RuntimeMode.PAPER,
        probationary_paper_market_data_source=ProbationaryPaperMarketDataSource.PHASE1_RUNTIME_ARTIFACT,
    )
    checked: list[tuple[str, str]] = []

    def _unexpected_provider(*_args, **_kwargs):
        raise AssertionError("Direct Databento provider should not be built for phase1_runtime_artifact startup.")

    def _unexpected_dns(*_args, **_kwargs):
        raise AssertionError("Direct Databento DNS probe should not run for phase1_runtime_artifact startup.")

    class _FakePhase1ArtifactClient:
        def __init__(self, *, artifact_root=None, required_source=None, now_fn=None):
            assert required_source == "DATABENTO_REALTIME_PHASE1"
            self.artifact_root = artifact_root

        def artifact_path(self, *, internal_symbol: str, internal_timeframe: str) -> Path:
            return tmp_path / "phase1_runtime_market_data" / internal_symbol / internal_timeframe / "latest_runtime_candles.json"

        def poll_live_bars(self, _external_symbol, external_timeframe, request):
            checked.append((request.internal_symbol, external_timeframe))
            return [SimpleNamespace(end_ts=datetime(2026, 5, 19, 7, 9, tzinfo=timezone.utc))]

    monkeypatch.setattr(
        "mgc_v05l.app.probationary_runtime.DatabentoMarketDataProvider",
        _unexpected_provider,
    )
    monkeypatch.setattr(socket, "getaddrinfo", _unexpected_dns)
    monkeypatch.setattr(
        "mgc_v05l.app.probationary_runtime.Phase1RuntimeArtifactPollingClient",
        _FakePhase1ArtifactClient,
    )

    payload = _run_probationary_runtime_market_data_transport_probe(
        settings=settings,
        schwab_config_path=tmp_path / "schwab.local.json",
    )

    assert payload["status"] == "ok"
    assert payload["runtime_ready"] is True
    assert payload["market_data_source"] == "phase1_runtime_artifact"
    assert payload["required_provenance"] == "DATABENTO_REALTIME_PHASE1"
    assert payload["direct_databento_transport_probe_attempted"] is False
    assert payload["phase1_artifact_probe_succeeds"] is True
    assert checked == [("MGC", "1m"), ("MGC", "3m")]
    artifact_path = Path(payload["artifact_path"])
    assert artifact_path.exists()
    stored = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert stored["direct_databento_transport_probe_attempted"] is False
    assert stored["phase1_artifact_probe_succeeds"] is True


def test_phase1_artifact_paper_transport_probe_fails_closed_on_stale_artifact(
    monkeypatch, tmp_path: Path
) -> None:
    settings = SimpleNamespace(
        probationary_artifacts_path=tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session",
        symbol="MNQ",
        timeframe="1m",
        timezone_info=timezone.utc,
        live_poll_lookback_minutes=180,
        resolved_execution_timeframe="1m",
        resolved_context_timeframes=(),
        market_data_provider=MarketDataProvider.DATABENTO,
        mode=RuntimeMode.PAPER,
        probationary_paper_market_data_source=ProbationaryPaperMarketDataSource.PHASE1_RUNTIME_ARTIFACT,
    )

    class _FakePhase1ArtifactClient:
        def __init__(self, *, artifact_root=None, required_source=None, now_fn=None):
            self.artifact_root = artifact_root

        def artifact_path(self, *, internal_symbol: str, internal_timeframe: str) -> Path:
            return tmp_path / "phase1_runtime_market_data" / internal_symbol / internal_timeframe / "latest_runtime_candles.json"

        def poll_live_bars(self, *_args, **_kwargs):
            raise RuntimeError("Phase-1 runtime candle artifact is stale")

    monkeypatch.setattr(
        "mgc_v05l.app.probationary_runtime.Phase1RuntimeArtifactPollingClient",
        _FakePhase1ArtifactClient,
    )
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("Direct Databento DNS probe should not run for phase1 artifact failures.")
        ),
    )

    with pytest.raises(ProbationaryRuntimeTransportFailure) as excinfo:
        _run_probationary_runtime_market_data_transport_probe(
            settings=settings,
            schwab_config_path=tmp_path / "schwab.local.json",
        )

    payload = excinfo.value.payload
    assert payload["failure_kind"] == "phase1_runtime_artifact_unhealthy"
    assert payload["market_data_source"] == "phase1_runtime_artifact"
    assert payload["phase1_artifact_probe_attempted"] is True
    assert payload["phase1_artifact_probe_succeeds"] is False
    assert "stale" in payload["exception_text"]
