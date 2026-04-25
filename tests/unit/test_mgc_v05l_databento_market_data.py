from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.market_data.databento_provider import DatabentoHistoricalHttpClient, DatabentoMarketDataProvider
from mgc_v05l.market_data.provider_models import HistoricalBarsRequest


class _FakeDatabentoTransport:
    def __init__(self, lines: list[str], *, billable_size: int = 1024) -> None:
        self._lines = list(lines)
        self._billable_size = billable_size
        self.requests: list[dict[str, object]] = []

    def request_text(
        self,
        *,
        url: str,
        headers: dict[str, str],
        method: str = "POST",
        form: dict[str, object] | None = None,
        query: dict[str, object] | None = None,
    ) -> str:
        self.requests.append({"url": url, "headers": headers, "method": method, "form": dict(form or {}), "query": dict(query or {})})
        if url.endswith("metadata.get_billable_size"):
            return str(self._billable_size)
        return "\n".join(self._lines)

    def request_json(
        self,
        *,
        url: str,
        headers: dict[str, str],
        method: str = "POST",
        form: dict[str, object] | None = None,
        query: dict[str, object] | None = None,
    ):
        raise AssertionError("JSON endpoints are not expected in this fake transport")

    def request_lines(self, *, url: str, headers: dict[str, str], form: dict[str, object]) -> list[str]:
        self.requests.append({"url": url, "headers": headers, "form": dict(form)})
        return list(self._lines)


class _FakeStagedDatabentoTransport:
    def __init__(self, lines: list[str], *, billable_size: int = 1024) -> None:
        self._lines = list(lines)
        self._billable_size = billable_size
        self.download_requests: list[dict[str, object]] = []
        self.request_lines_called = False
        self.text_requests: list[dict[str, object]] = []

    def request_text(
        self,
        *,
        url: str,
        headers: dict[str, str],
        method: str = "POST",
        form: dict[str, object] | None = None,
        query: dict[str, object] | None = None,
    ) -> str:
        self.text_requests.append({"url": url, "headers": headers, "method": method, "form": dict(form or {}), "query": dict(query or {})})
        if url.endswith("metadata.get_billable_size"):
            return str(self._billable_size)
        raise AssertionError(f"Unexpected text endpoint: {url}")

    def request_json(
        self,
        *,
        url: str,
        headers: dict[str, str],
        method: str = "POST",
        form: dict[str, object] | None = None,
        query: dict[str, object] | None = None,
    ):
        raise AssertionError("JSON endpoints are not expected in this fake transport")

    def request_lines(self, *, url: str, headers: dict[str, str], form: dict[str, object]) -> list[str]:
        self.request_lines_called = True
        raise AssertionError("staged download path should not call request_lines")

    def download_to_file(self, *, url: str, headers: dict[str, str], form: dict[str, object], destination: Path) -> dict[str, object]:
        self.download_requests.append({"url": url, "headers": headers, "form": dict(form), "destination": str(destination)})
        destination.write_text("\n".join(self._lines) + "\n", encoding="utf-8")
        return {"byte_count": destination.stat().st_size}

    def download_url_to_file(self, *, url: str, headers: dict[str, str], destination: Path) -> dict[str, object]:
        destination.write_text("\n".join(self._lines) + "\n", encoding="utf-8")
        return {"byte_count": destination.stat().st_size}


class _FakeBatchDatabentoTransport(_FakeStagedDatabentoTransport):
    def __init__(self, lines: list[str], *, billable_size: int) -> None:
        super().__init__(lines, billable_size=billable_size)
        self.batch_job_id = "GLBX-TEST-JOB"

    def request_json(
        self,
        *,
        url: str,
        headers: dict[str, str],
        method: str = "POST",
        form: dict[str, object] | None = None,
        query: dict[str, object] | None = None,
    ):
        if url.endswith("batch.submit_job"):
            return {"id": self.batch_job_id, "state": "received"}
        if url.endswith("batch.list_jobs"):
            return [{"id": self.batch_job_id, "state": "done"}]
        if url.endswith("batch.list_files"):
            return [
                {"filename": "metadata.json", "size": 10, "urls": {"https": "https://download.example/metadata.json"}},
                {"filename": "glbx-mdp3-part-000.json", "size": 100, "urls": {"https": "https://download.example/glbx-mdp3-part-000.json"}},
            ]
        raise AssertionError(f"Unexpected JSON endpoint: {url}")


def _build_settings(tmp_path: Path):
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{tmp_path / "databento.sqlite3"}"\n',
        encoding="utf-8",
    )
    return load_settings_from_files([Path("config/base.yaml"), overlay_path])


def test_databento_provider_normalizes_ohlcv_json_lines(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    transport = _FakeDatabentoTransport(
        [
            json.dumps(
                {
                    "ts_event": "2026-02-03T18:00:00+00:00",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.75,
                    "close": 10.25,
                    "volume": 12,
                    "symbol": "MGCG6",
                    "instrument_id": 123,
                    "publisher_id": 1,
                }
            ),
            json.dumps(
                {
                    "ts_event": "2026-02-03T18:01:00+00:00",
                    "open": 10.25,
                    "high": 10.75,
                    "low": 10.0,
                    "close": 10.5,
                    "volume": 20,
                    "symbol": "MGCG6",
                    "instrument_id": 123,
                    "publisher_id": 1,
                }
            ),
        ]
    )
    client = DatabentoHistoricalHttpClient(
        api_key="test-key",
        base_url="https://hist.databento.com/v0",
        transport=transport,
    )
    provider = DatabentoMarketDataProvider(settings, api_key="test-key", client=client)

    result = provider.fetch_historical_bars(
        HistoricalBarsRequest(
            internal_symbol="MGC",
            timeframe="1m",
            start=datetime.fromisoformat("2026-02-03T18:00:00+00:00"),
            end=datetime.fromisoformat("2026-02-03T18:02:00+00:00"),
        )
    )

    assert result.provider == "databento"
    assert result.data_source == "historical_1m_canonical"
    assert result.dataset == "GLBX.MDP3"
    assert result.schema_name == "ohlcv-1m"
    assert len(result.bars) == 2
    assert result.bars[0].symbol == "MGC"
    assert result.bars[0].timeframe == "1m"
    assert result.bar_provenance[result.bars[0].bar_id].raw_symbol == "MGCG6"
    assert transport.requests[0]["form"]["symbols"] == "MGC.v.0"
    assert transport.requests[0]["form"]["schema"] == "ohlcv-1m"


def test_databento_provider_stages_download_to_file_without_loading_full_response_body(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    transport = _FakeStagedDatabentoTransport(
        [
            json.dumps(
                {
                    "ts_event": "2026-02-03T18:00:00+00:00",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.75,
                    "close": 10.25,
                    "volume": 12,
                    "symbol": "MGCG6",
                    "instrument_id": 123,
                    "publisher_id": 1,
                }
            ),
            json.dumps(
                {
                    "ts_event": "2026-02-03T18:01:00+00:00",
                    "open": 10.25,
                    "high": 10.75,
                    "low": 10.0,
                    "close": 10.5,
                    "volume": 20,
                    "symbol": "MGCG6",
                    "instrument_id": 123,
                    "publisher_id": 1,
                }
            ),
        ]
    )
    client = DatabentoHistoricalHttpClient(
        api_key="test-key",
        base_url="https://hist.databento.com/v0",
        transport=transport,
    )
    provider = DatabentoMarketDataProvider(settings, api_key="test-key", client=client)
    request = HistoricalBarsRequest(
        internal_symbol="MGC",
        timeframe="1m",
        start=datetime.fromisoformat("2026-02-03T18:00:00+00:00"),
        end=datetime.fromisoformat("2026-02-03T18:02:00+00:00"),
    )

    stage = provider.stage_historical_bars(request)
    try:
        batches = list(provider.iter_staged_historical_bar_batches(stage, request=request, batch_size=1))
    finally:
        for path in stage.staged_artifact_paths:
            Path(path).unlink(missing_ok=True)
        if stage.manifest_path:
            Path(stage.manifest_path).unlink(missing_ok=True)
        Path(stage.staged_path).rmdir()

    assert transport.request_lines_called is False
    assert len(transport.download_requests) == 1
    assert Path(stage.staged_path).exists() is False
    assert len(batches) == 2
    assert [len(batch.bars) for batch in batches] == [1, 1]
    assert batches[0].bars[0].symbol == "MGC"


def test_databento_provider_uses_chunked_stream_route_for_medium_billable_request(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    transport = _FakeStagedDatabentoTransport(
        [
            json.dumps(
                {
                    "ts_event": "2026-02-03T18:00:00+00:00",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.75,
                    "close": 10.25,
                    "volume": 12,
                    "symbol": "MGCG6",
                    "instrument_id": 123,
                    "publisher_id": 1,
                }
            )
        ],
        billable_size=2 * 1024 * 1024 * 1024,
    )
    client = DatabentoHistoricalHttpClient(api_key="test-key", base_url="https://hist.databento.com/v0", transport=transport)
    provider = DatabentoMarketDataProvider(settings, api_key="test-key", client=client)
    request = HistoricalBarsRequest(
        internal_symbol="MGC",
        timeframe="1m",
        start=datetime.fromisoformat("2026-01-01T00:00:00+00:00"),
        end=datetime.fromisoformat("2026-09-30T00:00:00+00:00"),
    )

    stage = provider.stage_historical_bars(request)
    try:
        assert stage.route == "staged_get_range_chunked"
        assert stage.estimated_billable_bytes == 2 * 1024 * 1024 * 1024
        assert len(stage.staged_artifact_paths) >= 2
        manifest = json.loads(Path(stage.manifest_path).read_text(encoding="utf-8"))
        assert manifest["route"] == "staged_get_range_chunked"
        assert len(manifest["artifact_rows"]) == len(stage.staged_artifact_paths)
    finally:
        for path in stage.staged_artifact_paths:
            Path(path).unlink(missing_ok=True)
        if stage.manifest_path:
            Path(stage.manifest_path).unlink(missing_ok=True)
        Path(stage.staged_path).rmdir()


def test_databento_provider_uses_batch_route_for_large_billable_request(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    transport = _FakeBatchDatabentoTransport(
        [
            json.dumps(
                {
                    "ts_event": "2026-02-03T18:00:00+00:00",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.75,
                    "close": 10.25,
                    "volume": 12,
                    "symbol": "MGCG6",
                    "instrument_id": 123,
                    "publisher_id": 1,
                }
            )
        ],
        billable_size=6 * 1024 * 1024 * 1024,
    )
    client = DatabentoHistoricalHttpClient(api_key="test-key", base_url="https://hist.databento.com/v0", transport=transport)
    provider = DatabentoMarketDataProvider(settings, api_key="test-key", client=client)
    request = HistoricalBarsRequest(
        internal_symbol="MGC",
        timeframe="1m",
        start=datetime.fromisoformat("2024-01-01T00:00:00+00:00"),
        end=datetime.fromisoformat("2026-04-21T23:59:00+00:00"),
    )

    stage = provider.stage_historical_bars(request)
    try:
        assert stage.route == "batch_submit_job"
        assert len(stage.staged_artifact_paths) == 1
        assert Path(stage.staged_artifact_paths[0]).name == "glbx-mdp3-part-000.json"
        manifest = json.loads(Path(stage.manifest_path).read_text(encoding="utf-8"))
        assert manifest["route"] == "batch_submit_job"
        assert manifest["artifact_rows"][0]["job_id"] == transport.batch_job_id
    finally:
        for path in stage.staged_artifact_paths:
            Path(path).unlink(missing_ok=True)
        if stage.manifest_path:
            Path(stage.manifest_path).unlink(missing_ok=True)
        Path(stage.staged_path).rmdir()
