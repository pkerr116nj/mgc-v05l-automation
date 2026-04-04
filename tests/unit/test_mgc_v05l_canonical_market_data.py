from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import select

from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.canonical_maintenance import CanonicalMarketDataMaintenanceService
from mgc_v05l.market_data.canonical_subset import build_schema_preserving_canonical_subset
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.persistence.tables import bars_table


def _build_bar(*, end_ts: datetime, price: Decimal) -> Bar:
    return Bar(
        bar_id=f"MGC|1m|{end_ts.isoformat()}",
        symbol="MGC",
        timeframe="1m",
        start_ts=end_ts - timedelta(minutes=1),
        end_ts=end_ts,
        open=price,
        high=price + Decimal("1"),
        low=price - Decimal("1"),
        close=price + Decimal("0.5"),
        volume=10,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )


def test_canonical_maintenance_persists_completed_1m_bars_and_derives_10m(tmp_path: Path) -> None:
    database_path = tmp_path / "canonical.sqlite3"
    service = CanonicalMarketDataMaintenanceService(database_url=f"sqlite:///{database_path}")
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(end_ts=datetime(2026, 3, 13, 18, minute, tzinfo=ny), price=Decimal("100") + Decimal(str(minute)))
        for minute in range(1, 11)
    ]

    merge_audit = service.persist_completed_1m_bars(
        bars=bars,
        raw_data_source="schwab_live_poll",
        provider="schwab_market_data",
        provenance_tag="test_live_poll",
    )
    assert merge_audit is not None
    coverage = service.audit_coverage(symbol="MGC")
    derivation = service.derive_timeframe(symbol="MGC", target_timeframe="10m")

    repositories = RepositorySet(build_engine(f"sqlite:///{database_path}"))
    with repositories.engine.begin() as connection:
        derived_rows = connection.execute(
            select(bars_table).where(
                bars_table.c.ticker == "MGC",
                bars_table.c.timeframe == "10m",
                bars_table.c.data_source == "historical_10m_canonical",
            )
        ).mappings().all()

    assert coverage.earliest == bars[0].end_ts.isoformat()
    assert coverage.latest == bars[-1].end_ts.isoformat()
    assert coverage.gap_count == 0
    assert derivation.derived_bar_count == 1
    assert derivation.skipped_incomplete_buckets == 0
    assert len(derived_rows) == 1


def test_canonical_maintenance_flags_unexpected_gap(tmp_path: Path) -> None:
    database_path = tmp_path / "canonical-gap.sqlite3"
    service = CanonicalMarketDataMaintenanceService(database_url=f"sqlite:///{database_path}")
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(end_ts=datetime(2026, 3, 13, 18, 1, tzinfo=ny), price=Decimal("100")),
        _build_bar(end_ts=datetime(2026, 3, 13, 18, 3, tzinfo=ny), price=Decimal("101")),
    ]

    service.persist_completed_1m_bars(
        bars=bars,
        raw_data_source="schwab_live_poll",
        provider="schwab_market_data",
        provenance_tag="test_live_poll",
    )
    coverage = service.audit_coverage(symbol="MGC")

    assert coverage.gap_count == 1
    assert coverage.gaps[0].missing_minutes == 1


def test_schema_preserving_subset_keeps_bar_constraints(tmp_path: Path) -> None:
    source_database_path = tmp_path / "canonical-source.sqlite3"
    subset_database_path = tmp_path / "canonical-subset.sqlite3"
    service = CanonicalMarketDataMaintenanceService(database_url=f"sqlite:///{source_database_path}")
    ny = ZoneInfo("America/New_York")
    bars = [
        _build_bar(end_ts=datetime(2026, 3, 13, 18, minute, tzinfo=ny), price=Decimal("100") + Decimal(str(minute)))
        for minute in range(1, 11)
    ]

    service.persist_completed_1m_bars(
        bars=bars,
        raw_data_source="databento_raw",
        provider="databento",
        provenance_tag="subset_test",
    )
    service.derive_timeframe(symbol="MGC", target_timeframe="5m")
    build_schema_preserving_canonical_subset(
        source_db_path=source_database_path,
        target_db_path=subset_database_path,
        symbols=["MGC"],
        timeframes=["1m", "5m"],
        data_sources=["historical_1m_canonical", "historical_5m_canonical"],
    )

    repositories = RepositorySet(build_engine(f"sqlite:///{subset_database_path}"))
    with repositories.engine.begin() as connection:
        indexes = connection.exec_driver_sql("PRAGMA index_list(bars)").fetchall()
        derived_rows = connection.execute(
            select(bars_table).where(
                bars_table.c.ticker == "MGC",
                bars_table.c.timeframe == "5m",
                bars_table.c.data_source == "historical_5m_canonical",
            )
        ).mappings().all()

    index_names = {str(row[1]) for row in indexes}
    assert "sqlite_autoindex_bars_1" in index_names
    assert "sqlite_autoindex_bars_2" in index_names
    assert "ix_bars_source_lookup" in index_names
    assert len(derived_rows) == 2
