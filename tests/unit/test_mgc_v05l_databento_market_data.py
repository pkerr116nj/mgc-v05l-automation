from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.market_data.databento_provider import DatabentoHistoricalHttpClient, DatabentoMarketDataProvider
from mgc_v05l.market_data.provider_models import HistoricalBarsRequest


class _FakeDatabentoTransport:
    def __init__(self, lines: list[str]) -> None:
        self._lines = list(lines)
        self.requests: list[dict[str, object]] = []

    def request_lines(self, *, url: str, headers: dict[str, str], form: dict[str, object]) -> list[str]:
        self.requests.append({"url": url, "headers": headers, "form": dict(form)})
        return list(self._lines)


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
