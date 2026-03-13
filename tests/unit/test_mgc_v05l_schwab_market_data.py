"""Tests for the Schwab market-data adapter scaffolding."""

from datetime import datetime
from pathlib import Path
from typing import Sequence

import pytest
from sqlalchemy import select

from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.market_data.historical_service import HistoricalBackfillService
from mgc_v05l.market_data.live_feed import LivePollingService
from mgc_v05l.market_data.schwab_adapter import SchwabMarketDataAdapter
from mgc_v05l.market_data.schwab_models import (
    SchwabAuthConfig,
    SchwabBarFieldMap,
    SchwabHistoricalRequest,
    SchwabLivePollRequest,
    SchwabMarketDataConfig,
    TimestampSemantics,
)
from mgc_v05l.persistence.db import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.persistence.tables import bars_table


class _FakeHistoricalClient:
    def __init__(self, records: Sequence[dict]) -> None:
        self._records = records

    def fetch_historical_bars(self, external_symbol: str, external_timeframe: str, request: SchwabHistoricalRequest):
        assert external_symbol == "MGC_EXTERNAL"
        assert external_timeframe == "FIVE_MINUTES"
        return self._records


class _FakeLivePollingClient:
    def __init__(self, records: Sequence[dict]) -> None:
        self._records = records

    def poll_live_bars(self, external_symbol: str, external_timeframe: str, request: SchwabLivePollRequest):
        assert external_symbol == "MGC_EXTERNAL"
        assert external_timeframe == "FIVE_MINUTES"
        return self._records


def _build_settings(tmp_path: Path):
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{tmp_path / "schwab.sqlite3"}"\n',
        encoding="utf-8",
    )
    return load_settings_from_files([Path("config/base.yaml"), overlay_path])


def _build_config() -> SchwabMarketDataConfig:
    return SchwabMarketDataConfig(
        auth=SchwabAuthConfig(client_id="placeholder-client"),
        symbol_map={"MGC": "MGC_EXTERNAL"},
        timeframe_map={"5m": "FIVE_MINUTES"},
        field_map=SchwabBarFieldMap(
            timestamp_field="timestamp",
            open_field="open",
            high_field="high",
            low_field="low",
            close_field="close",
            volume_field="volume",
            is_final_field="isFinal",
            timestamp_semantics=TimestampSemantics.END,
        ),
    )


def test_schwab_adapter_maps_and_normalizes_historical_records(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    adapter = SchwabMarketDataAdapter(settings, _build_config())
    records = [
        {
            "timestamp": "2026-03-13T18:05:00-04:00",
            "open": "100",
            "high": "101",
            "low": "99",
            "close": "100.5",
            "volume": 120,
            "isFinal": True,
        }
    ]

    bars = adapter.normalize_historical_records(records, "MGC", "5m")

    assert adapter.map_symbol("MGC") == "MGC_EXTERNAL"
    assert adapter.map_timeframe("5m") == "FIVE_MINUTES"
    assert len(bars) == 1
    assert bars[0].symbol == "MGC"
    assert bars[0].session_asia is True
    assert bars[0].is_final is True


def test_historical_backfill_service_persists_normalized_bars(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    adapter = SchwabMarketDataAdapter(settings, _build_config())
    client = _FakeHistoricalClient(
        [
            {
                "timestamp": "2026-03-13T18:05:00-04:00",
                "open": "100",
                "high": "101",
                "low": "99",
                "close": "100.5",
                "volume": 120,
                "isFinal": True,
            }
        ]
    )
    service = HistoricalBackfillService(adapter=adapter, client=client, repositories=repositories)

    bars = service.fetch_bars(
        SchwabHistoricalRequest(
            internal_symbol="MGC",
            start_at=datetime.fromisoformat("2026-03-13T18:00:00-04:00"),
            end_at=datetime.fromisoformat("2026-03-13T18:05:00-04:00"),
        ),
        internal_timeframe="5m",
    )

    with repositories.engine.begin() as connection:
        persisted = connection.execute(select(bars_table.c.bar_id)).all()

    assert len(bars) == 1
    assert len(persisted) == 1


def test_live_polling_service_returns_same_internal_bar_model(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    adapter = SchwabMarketDataAdapter(settings, _build_config())
    client = _FakeLivePollingClient(
        [
            {
                "timestamp": "2026-03-13T08:35:00-04:00",
                "open": "100",
                "high": "101",
                "low": "99.5",
                "close": "100.75",
                "volume": 110,
                "isFinal": False,
            }
        ]
    )
    service = LivePollingService(adapter=adapter, client=client)

    bars = service.poll_bars(SchwabLivePollRequest(internal_symbol="MGC"), internal_timeframe="5m")

    assert len(bars) == 1
    assert bars[0].symbol == "MGC"
    assert bars[0].timeframe == "5m"
    assert bars[0].session_us is True
    assert bars[0].is_final is False


def test_schwab_services_raise_placeholders_without_confirmed_clients(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    adapter = SchwabMarketDataAdapter(settings, _build_config())
    service = HistoricalBackfillService(adapter=adapter)

    with pytest.raises(NotImplementedError):
        service.fetch_bars(
            SchwabHistoricalRequest(
                internal_symbol="MGC",
                start_at=datetime.fromisoformat("2026-03-13T18:00:00-04:00"),
                end_at=datetime.fromisoformat("2026-03-13T18:05:00-04:00"),
            ),
            internal_timeframe="5m",
        )
