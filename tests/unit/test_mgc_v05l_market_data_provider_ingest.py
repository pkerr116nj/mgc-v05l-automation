from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from sqlite3 import connect

from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.market_data.bar_models import build_bar_id
from mgc_v05l.market_data.databento_provider import DatabentoHistoricalHttpClient, DatabentoMarketDataProvider
from mgc_v05l.market_data.provider_ingest import HistoricalMarketDataIngestionService
from mgc_v05l.market_data.provider_models import CoverageChange, HistoricalBarsRequest


class _FakeDatabentoTransport:
    def __init__(self, lines: list[str]) -> None:
        self._lines = list(lines)

    def request_lines(self, *, url: str, headers: dict[str, str], form: dict[str, object]) -> list[str]:
        return list(self._lines)


def _build_settings(tmp_path: Path):
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{tmp_path / "canonical.sqlite3"}"\n',
        encoding="utf-8",
    )
    return load_settings_from_files([Path("config/base.yaml"), overlay_path])


def test_historical_ingest_merges_into_canonical_base_with_provenance(tmp_path: Path) -> None:
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
    ingestion = HistoricalMarketDataIngestionService(database_url=settings.database_url)

    audit = ingestion.ingest(
        provider=provider,
        request=HistoricalBarsRequest(
            internal_symbol="MGC",
            timeframe="1m",
            start=datetime.fromisoformat("2026-02-03T18:00:00+00:00"),
            end=datetime.fromisoformat("2026-02-03T18:02:00+00:00"),
        ),
    )

    assert audit.change is CoverageChange.INITIAL
    assert audit.inserted_bar_count == 2
    assert audit.after.earliest is not None
    assert audit.report_path is not None

    db_path = tmp_path / "canonical.sqlite3"
    connection = connect(db_path)
    try:
        bars_row = connection.execute(
            "select count(*), min(end_ts), max(end_ts) from bars where ticker = 'MGC' and timeframe = '1m' and data_source = 'historical_1m_canonical'"
        ).fetchone()
        provenance_row = connection.execute(
            "select count(*) from market_data_bar_provenance where internal_symbol = 'MGC' and data_source = 'historical_1m_canonical'"
        ).fetchone()
    finally:
        connection.close()

    assert int(bars_row[0]) == 2
    assert provenance_row[0] == 2

    second_audit = ingestion.ingest(
        provider=provider,
        request=HistoricalBarsRequest(
            internal_symbol="MGC",
            timeframe="1m",
            start=datetime.fromisoformat("2026-02-03T18:00:00+00:00"),
            end=datetime.fromisoformat("2026-02-03T18:02:00+00:00"),
        ),
    )

    assert second_audit.change is CoverageChange.MATCHED
    assert second_audit.inserted_bar_count == 0
    assert second_audit.skipped_existing_count == 2


def test_historical_ingest_keeps_existing_other_source_rows_at_same_timestamp(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    db_path = tmp_path / "canonical.sqlite3"
    connection = connect(db_path)
    try:
        connection.executescript(
            """
            create table if not exists instruments (
              instrument_id integer primary key autoincrement,
              ticker text not null,
              cusip text,
              asset_class text not null,
              description text,
              exchange text,
              multiplier numeric,
              is_active boolean not null default 1
            );
            create unique index if not exists uq_instruments_ticker_asset_class on instruments (ticker, asset_class);
            create table if not exists bars (
              bar_id text primary key,
              instrument_id integer,
              ticker text not null,
              cusip text,
              asset_class text,
              data_source text not null,
              timestamp text not null,
              symbol text not null,
              timeframe text not null,
              start_ts text not null,
              end_ts text not null,
              open numeric not null,
              high numeric not null,
              low numeric not null,
              close numeric not null,
              volume integer not null,
              is_final boolean not null,
              session_asia boolean not null,
              session_london boolean not null,
              session_us boolean not null,
              session_allowed boolean not null,
              created_at text not null
            );
            create unique index if not exists uq_bars_identity on bars (ticker, timeframe, timestamp, data_source);
            create table if not exists market_data_ingest_runs (
              ingest_run_id text primary key,
              provider text not null,
              dataset text,
              schema_name text,
              request_symbol text,
              internal_symbol text not null,
              timeframe text not null,
              data_source text not null,
              coverage_start text,
              coverage_end text,
              ingest_started_at text not null,
              ingest_completed_at text,
              status text not null,
              payload_json text
            );
            create table if not exists market_data_bar_provenance (
              provenance_id text primary key,
              ingest_run_id text not null,
              bar_id text not null,
              data_source text not null,
              provider text not null,
              dataset text,
              schema_name text,
              internal_symbol text not null,
              raw_symbol text,
              request_symbol text,
              stype_in text,
              stype_out text,
              interval text not null,
              source_timestamp text not null,
              ingest_time text not null,
              coverage_start text,
              coverage_end text,
              provenance_tag text not null,
              provider_metadata_json text
            );
            """
        )
        connection.execute(
            "insert into instruments (ticker, asset_class, is_active) values ('MGC', 'future', 1)"
        )
        start_ts = "2026-02-03T13:00:00-05:00"
        end_ts = "2026-02-03T13:01:00-05:00"
        connection.execute(
            """
            insert into bars (
              bar_id, instrument_id, ticker, cusip, asset_class, data_source, timestamp, symbol, timeframe,
              start_ts, end_ts, open, high, low, close, volume, is_final,
              session_asia, session_london, session_us, session_allowed, created_at
            ) values (?, 1, 'MGC', null, 'future', 'schwab_history', ?, 'MGC', '1m', ?, ?, 10, 10.5, 9.75, 10.25, 12, 1, 0, 0, 1, 1, ?)
            """,
            (
                build_bar_id("MGC", "1m", datetime.fromisoformat(end_ts)),
                end_ts,
                start_ts,
                end_ts,
                end_ts,
            ),
        )
        connection.commit()
    finally:
        connection.close()

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
        ]
    )
    client = DatabentoHistoricalHttpClient(
        api_key="test-key",
        base_url="https://hist.databento.com/v0",
        transport=transport,
    )
    provider = DatabentoMarketDataProvider(settings, api_key="test-key", client=client)
    ingestion = HistoricalMarketDataIngestionService(database_url=settings.database_url)

    audit = ingestion.ingest(
        provider=provider,
        request=HistoricalBarsRequest(
            internal_symbol="MGC",
            timeframe="1m",
            start=datetime.fromisoformat("2026-02-03T18:00:00+00:00"),
            end=datetime.fromisoformat("2026-02-03T18:01:00+00:00"),
        ),
    )

    assert audit.inserted_bar_count == 1
    connection = connect(db_path)
    try:
        counts = connection.execute(
            """
            select data_source, count(*)
            from bars
            where ticker = 'MGC' and timeframe = '1m'
            group by data_source
            order by data_source
            """
        ).fetchall()
    finally:
        connection.close()

    assert counts == [("historical_1m_canonical", 1), ("schwab_history", 1)]
