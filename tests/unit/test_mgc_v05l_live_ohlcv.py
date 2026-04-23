from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from sqlite3 import connect

import pytest

from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.market_data.canonical_maintenance import CanonicalMarketDataMaintenanceService
from mgc_v05l.market_data.live_ohlcv import CanonicalLiveTradeCaptureService, LiveTradeOhlcvAggregator
from mgc_v05l.market_data.provider_models import TradePrint


def _build_settings(tmp_path: Path):
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{tmp_path / "live_capture.sqlite3"}"\n',
        encoding="utf-8",
    )
    return load_settings_from_files([Path("config/base.yaml"), overlay_path])


def _trade(*, symbol: str, timestamp: str, price: str, size: int = 1) -> TradePrint:
    return TradePrint(
        internal_symbol=symbol,
        price=Decimal(price),
        size=size,
        occurred_at=datetime.fromisoformat(timestamp),
        provider="test_stream",
    )


def test_live_trade_aggregator_builds_expected_minute_ohlcv(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    aggregator = LiveTradeOhlcvAggregator(settings)

    assert aggregator.ingest_trade(_trade(symbol="MGC", timestamp="2026-04-17T09:30:05-04:00", price="100.0", size=2)) == []
    assert aggregator.ingest_trade(_trade(symbol="MGC", timestamp="2026-04-17T09:30:17-04:00", price="101.5", size=3)) == []
    assert aggregator.ingest_trade(_trade(symbol="MGC", timestamp="2026-04-17T09:30:44-04:00", price="99.5", size=4)) == []

    finalized = aggregator.ingest_trade(
        _trade(symbol="MGC", timestamp="2026-04-17T09:31:02-04:00", price="100.5", size=5)
    )

    assert len(finalized) == 1
    bar = finalized[0]
    assert bar.symbol == "MGC"
    assert bar.start_ts.isoformat() == "2026-04-17T09:30:00-04:00"
    assert bar.end_ts.isoformat() == "2026-04-17T09:31:00-04:00"
    assert bar.open == Decimal("100.0")
    assert bar.high == Decimal("101.5")
    assert bar.low == Decimal("99.5")
    assert bar.close == Decimal("99.5")
    assert bar.volume == 9
    assert bar.session_us is True


def test_live_trade_capture_service_persists_canonical_1m_and_derived_5m(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    db_path = tmp_path / "live_capture.sqlite3"
    maintenance = CanonicalMarketDataMaintenanceService(database_url=f"sqlite:///{db_path}")
    service = CanonicalLiveTradeCaptureService(
        settings,
        canonical_maintenance=maintenance,
        raw_data_source="simulated_live_trade",
        provider="simulated_feed",
        provenance_tag="simulated_live_trade",
        derive_timeframes=("5m",),
    )

    trades = [
        _trade(symbol="MGC", timestamp="2026-04-17T09:30:05-04:00", price="100.0", size=1),
        _trade(symbol="MGC", timestamp="2026-04-17T09:31:05-04:00", price="101.0", size=2),
        _trade(symbol="MGC", timestamp="2026-04-17T09:32:05-04:00", price="102.0", size=3),
        _trade(symbol="MGC", timestamp="2026-04-17T09:33:05-04:00", price="103.0", size=4),
        _trade(symbol="MGC", timestamp="2026-04-17T09:34:05-04:00", price="104.0", size=5),
        _trade(symbol="MGC", timestamp="2026-04-17T09:35:05-04:00", price="105.0", size=6),
    ]

    for trade in trades:
        service.capture_trade(trade)
    result = service.flush_completed(datetime.fromisoformat("2026-04-17T09:36:00-04:00"))

    assert result.persisted_bar_count == 1
    assert result.derived_timeframes == ("5m",)

    connection = connect(db_path)
    try:
        raw_count = connection.execute(
            "select count(*) from bars where ticker = 'MGC' and timeframe = '1m' and data_source = 'simulated_live_trade'"
        ).fetchone()[0]
        canonical_count = connection.execute(
            "select count(*) from bars where ticker = 'MGC' and timeframe = '1m' and data_source = 'historical_1m_canonical'"
        ).fetchone()[0]
        derived_row = connection.execute(
            """
            select count(*), min(end_ts), max(end_ts)
            from bars
            where ticker = 'MGC' and timeframe = '5m' and data_source = 'historical_5m_canonical'
            """
        ).fetchone()
    finally:
        connection.close()

    assert raw_count == 6
    assert canonical_count == 6
    assert int(derived_row[0]) == 1
    assert derived_row[1] == "2026-04-17T09:35:00-04:00"
    assert derived_row[2] == "2026-04-17T09:35:00-04:00"


def test_live_trade_aggregator_rejects_out_of_order_trade_for_finalized_bucket(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    aggregator = LiveTradeOhlcvAggregator(settings)

    aggregator.ingest_trade(_trade(symbol="MGC", timestamp="2026-04-17T09:30:05-04:00", price="100.0"))
    aggregator.ingest_trade(_trade(symbol="MGC", timestamp="2026-04-17T09:31:05-04:00", price="101.0"))

    with pytest.raises(ValueError, match="Out-of-order trade"):
        aggregator.ingest_trade(_trade(symbol="MGC", timestamp="2026-04-17T09:30:30-04:00", price="99.0"))
