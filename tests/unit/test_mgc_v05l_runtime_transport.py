from __future__ import annotations

import json
import socket
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import mgc_v05l.app.operator_dashboard as operator_dashboard_module
from mgc_v05l.app.operator_dashboard import OperatorDashboardService
from mgc_v05l.app.probationary_runtime import (
    ProbationaryRuntimeTransportFailure,
    _run_probationary_runtime_market_data_transport_probe,
)


def test_runtime_transport_probe_writes_dns_failure_artifact(monkeypatch, tmp_path: Path) -> None:
    settings = SimpleNamespace(
        probationary_artifacts_path=tmp_path / "outputs" / "probationary_pattern_engine" / "paper_session",
        symbol="MGC",
        timeframe="5m",
        timezone_info=timezone.utc,
        live_poll_lookback_minutes=180,
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


def test_operator_dashboard_surfaces_market_data_transport_failure(monkeypatch, tmp_path: Path) -> None:
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
        operator_dashboard_module,
        "load_settings_from_files",
        lambda *_args, **_kwargs: SimpleNamespace(database_url="sqlite:///tmp/test.sqlite3"),
    )
    service = OperatorDashboardService(repo_root)

    snapshot = service._runtime_snapshot("paper")  # noqa: SLF001

    assert snapshot["running"] is False
    assert snapshot["status"]["runtime_blocker"] == "market_data_transport_failure"
    assert snapshot["status"]["health_status"] == "BLOCKED"
    assert snapshot["status"]["market_data_semantics"] == "TRANSPORT FAILURE"
