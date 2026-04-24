from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.research_data_integrity import run_research_data_integrity_audit
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.db import create_schema
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.research.trend_participation.storage import materialize_parquet_dataset
from mgc_v05l.research.warehouse_historical_evaluator.layout import build_layout as build_warehouse_layout


def _provider_config(tmp_path: Path) -> Path:
    path = tmp_path / "providers.json"
    path.write_text(
        json.dumps(
            {
                "provider_selection": {
                    "historical_research": ["databento", "schwab_market_data"],
                    "live_market_data": ["schwab_market_data", "databento"],
                    "execution_truth": ["schwab_execution"],
                },
                "data_source_precedence": {
                    "historical_research": {
                        "1m": ["historical_1m_canonical"],
                        "5m": ["historical_5m_canonical", "vendor_5m_extended"],
                        "10m": ["historical_10m_canonical", "historical_1m_canonical"],
                    }
                },
                "schwab_market_data": {
                    "config_path": "config/schwab.local.json",
                    "provenance_tag": "schwab_market_data",
                    "canonical_data_source_by_timeframe": {"1m": "historical_1m_canonical", "5m": "historical_5m_canonical"},
                },
                "databento": {
                    "enabled": True,
                    "api_key_env": "DATABENTO_API_KEY",
                    "historical_base_url": "https://hist.databento.com/v0",
                    "encoding": "json",
                    "compression": "none",
                    "pretty_px": True,
                    "pretty_ts": True,
                    "map_symbols": True,
                    "provenance_tag": "databento_historical",
                    "canonical_data_source_by_timeframe": {"1m": "historical_1m_canonical"},
                    "pilot_symbols": {
                        "MGC": {
                            "request_symbol": "MGC.v.0",
                            "dataset": "GLBX.MDP3",
                            "stype_in": "continuous",
                            "stype_out": "instrument_id",
                            "schema_by_timeframe": {"1m": "ohlcv-1m"},
                            "asset_class": "future",
                            "exchange": "COMEX",
                            "description": "Micro Gold futures",
                        },
                        "ES": {
                            "request_symbol": "ES.v.0",
                            "dataset": "GLBX.MDP3",
                            "stype_in": "continuous",
                            "stype_out": "instrument_id",
                            "schema_by_timeframe": {"1m": "ohlcv-1m"},
                            "asset_class": "future",
                            "exchange": "CME",
                            "description": "E-mini S&P 500 futures",
                        },
                    },
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def _bar(*, symbol: str, timeframe: str, end_ts: datetime, bar_id_suffix: str = "", price: str = "100") -> Bar:
    minutes = int(timeframe.removesuffix("m"))
    return Bar(
        bar_id=f"{symbol}|{timeframe}|{end_ts.isoformat()}|{bar_id_suffix}",
        symbol=symbol,
        timeframe=timeframe,
        start_ts=end_ts - timedelta(minutes=minutes),
        end_ts=end_ts,
        open=Decimal(price),
        high=Decimal(price) + Decimal("1"),
        low=Decimal(price) - Decimal("1"),
        close=Decimal(price) + Decimal("0.5"),
        volume=10,
        is_final=True,
        session_asia=True,
        session_london=False,
        session_us=False,
        session_allowed=True,
    )


def _seed_sqlite(db_path: Path) -> None:
    engine = build_engine(f"sqlite:///{db_path}")
    create_schema(engine)
    repositories = RepositorySet(engine)
    ny = ZoneInfo("America/New_York")

    repositories.bars.save(_bar(symbol="MGC", timeframe="1m", end_ts=datetime(2026, 3, 2, 18, 1, tzinfo=ny)), data_source="historical_1m_canonical")
    repositories.bars.save(_bar(symbol="MGC", timeframe="1m", end_ts=datetime(2026, 3, 2, 18, 3, tzinfo=ny)), data_source="historical_1m_canonical")
    repositories.bars.save(_bar(symbol="ES", timeframe="1m", end_ts=datetime(2026, 3, 2, 18, 1, tzinfo=ny)), data_source="historical_1m_canonical")
    repositories.bars.save(_bar(symbol="ES", timeframe="1m", end_ts=datetime(2026, 3, 2, 18, 2, tzinfo=ny)), data_source="historical_1m_canonical")
    repositories.bars.save(_bar(symbol="ES", timeframe="1m", end_ts=datetime(2026, 3, 2, 18, 3, tzinfo=ny)), data_source="historical_1m_canonical")
    repositories.bars.save(_bar(symbol="MGC", timeframe="5m", end_ts=datetime(2026, 3, 2, 18, 5, tzinfo=ny)), data_source="historical_5m_canonical")
    repositories.bars.save(_bar(symbol="MGC", timeframe="5m", end_ts=datetime(2026, 3, 2, 18, 5, tzinfo=ny), bar_id_suffix="forbidden"), data_source="schwab_history")


def _materialize_warehouse(
    root: Path,
    *,
    raw_latest: datetime,
    derived_latest: datetime,
    duplicate_raw: bool = False,
    trade_latest: datetime | None = None,
) -> None:
    layout = build_warehouse_layout(root)
    year = raw_latest.year
    shard = "2026Q1"
    raw_rows = [
        {
            "symbol": "MGC",
            "bar_ts": raw_latest - timedelta(minutes=1),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 10,
            "provider": "databento",
            "dataset": "GLBX.MDP3",
            "schema": "ohlcv-1m",
            "instrument_identity": "MGC.v.0",
            "data_source": "historical_1m_canonical",
            "ingest_ts": raw_latest,
            "coverage_window_start": raw_latest - timedelta(days=1),
            "coverage_window_end": raw_latest,
            "provenance_tag": "databento_historical",
        },
        {
            "symbol": "MGC",
            "bar_ts": raw_latest,
            "open": 101.0,
            "high": 102.0,
            "low": 100.0,
            "close": 101.5,
            "volume": 11,
            "provider": "databento",
            "dataset": "GLBX.MDP3",
            "schema": "ohlcv-1m",
            "instrument_identity": "MGC.v.0",
            "data_source": "historical_1m_canonical",
            "ingest_ts": raw_latest,
            "coverage_window_start": raw_latest - timedelta(days=1),
            "coverage_window_end": raw_latest,
            "provenance_tag": "databento_historical",
        },
        {
            "symbol": "ES",
            "bar_ts": raw_latest,
            "open": 5000.0,
            "high": 5001.0,
            "low": 4999.0,
            "close": 5000.5,
            "volume": 20,
            "provider": "databento",
            "dataset": "GLBX.MDP3",
            "schema": "ohlcv-1m",
            "instrument_identity": "ES.v.0",
            "data_source": "historical_1m_canonical",
            "ingest_ts": raw_latest,
            "coverage_window_start": raw_latest - timedelta(days=1),
            "coverage_window_end": raw_latest,
            "provenance_tag": "databento_historical",
        },
    ]
    if duplicate_raw:
        raw_rows.append(dict(raw_rows[1]))
    materialize_parquet_dataset(
        layout["raw_bars_1m"] / "symbol=MGC" / f"year={year}" / f"shard_id={shard}" / "bars.parquet",
        [row for row in raw_rows if row["symbol"] == "MGC"],
    )
    materialize_parquet_dataset(
        layout["raw_bars_1m"] / "symbol=ES" / f"year={year}" / f"shard_id={shard}" / "bars.parquet",
        [row for row in raw_rows if row["symbol"] == "ES"],
    )
    materialize_parquet_dataset(
        layout["derived_bars_5m"] / "symbol=MGC" / f"year={year}" / f"shard_id={shard}" / "bars.parquet",
        [
            {
                "symbol": "MGC",
                "timeframe": "5m",
                "bar_ts": derived_latest,
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.5,
                "volume": 21,
                "source_data_source": "historical_1m_canonical",
                "derived_rule": "complete_bucket_resample_from_canonical_1m",
                "materialized_from_raw_version": "rawv1",
                "materialized_ts": derived_latest,
                "provenance_tag": "derived:5m:rawv1",
            }
        ],
    )
    materialize_parquet_dataset(
        layout["derived_bars_5m"] / "symbol=ES" / f"year={year}" / f"shard_id={shard}" / "bars.parquet",
        [
            {
                "symbol": "ES",
                "timeframe": "5m",
                "bar_ts": derived_latest,
                "open": 5000.0,
                "high": 5001.0,
                "low": 4999.0,
                "close": 5000.5,
                "volume": 20,
                "source_data_source": "historical_1m_canonical",
                "derived_rule": "complete_bucket_resample_from_canonical_1m",
                "materialized_from_raw_version": "rawv1",
                "materialized_ts": derived_latest,
                "provenance_tag": "derived:5m:rawv1",
            }
        ],
    )
    if trade_latest is not None:
        materialize_parquet_dataset(
            layout["lane_entries"] / "symbol=MGC" / f"year={year}" / f"shard_id={shard}" / "entries.parquet",
            [{"symbol": "MGC", "entry_ts": trade_latest}],
        )
        materialize_parquet_dataset(
            layout["lane_closed_trades"] / "symbol=MGC" / f"year={year}" / f"shard_id={shard}" / "closed_trades.parquet",
            [{"symbol": "MGC", "entry_ts": trade_latest}],
        )


def test_research_market_data_integrity_audit_flags_gap_and_trade_misalignment(tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)
    db_path = tmp_path / "replay.sqlite3"
    warehouse_root = tmp_path / "warehouse"
    output_dir = tmp_path / "report"
    _seed_sqlite(db_path)
    ny = ZoneInfo("America/New_York")
    _materialize_warehouse(
        warehouse_root,
        raw_latest=datetime(2026, 3, 2, 18, 3, tzinfo=ny),
        derived_latest=datetime(2026, 3, 2, 18, 5, tzinfo=ny),
        trade_latest=datetime(2026, 3, 2, 18, 2, tzinfo=ny),
    )

    result = run_research_data_integrity_audit(
        output_dir=output_dir,
        replay_db_path=db_path,
        warehouse_root=warehouse_root,
        provider_config=provider_cfg,
        lane_symbol_map={"MGC": ["test_lane__MGC"]},
        end_timestamp=datetime(2026, 4, 21, 0, 0, tzinfo=ny),
    )

    payload = result["payload"]
    assert payload["health"]["overall_status"] == "failed"
    assert payload["trade_alignment"]["analysis_allowed"] is False
    mgc_row = next(row for row in payload["health"]["instrument_rows"] if row["instrument"] == "MGC")
    assert "missing_bars" in mgc_row["issues"]
    assert len(payload["repair_plan"]["repair_commands"]) == 2
    assert Path(result["artifacts"]["summary_json_path"]).exists()


def test_research_market_data_integrity_labels_forbidden_five_minute_sources(tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)
    db_path = tmp_path / "replay.sqlite3"
    warehouse_root = tmp_path / "warehouse"
    _seed_sqlite(db_path)
    ny = ZoneInfo("America/New_York")
    _materialize_warehouse(
        warehouse_root,
        raw_latest=datetime(2026, 3, 2, 18, 3, tzinfo=ny),
        derived_latest=datetime(2026, 3, 2, 18, 5, tzinfo=ny),
        trade_latest=datetime(2026, 3, 2, 18, 3, tzinfo=ny),
    )

    result = run_research_data_integrity_audit(
        output_dir=tmp_path / "report",
        replay_db_path=db_path,
        warehouse_root=warehouse_root,
        provider_config=provider_cfg,
        lane_symbol_map={"MGC": ["test_lane__MGC"]},
    )
    rows = [row for row in result["payload"]["replay_audit"]["five_minute_surface_rows"] if row["instrument"] == "MGC"]
    trusts = {row["data_source"]: row["trust_classification"] for row in rows}
    assert trusts["historical_5m_canonical"] == "canonical"
    assert trusts["schwab_history"] == "forbidden"


def test_research_market_data_integrity_flags_warehouse_duplicate_timestamps(tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)
    db_path = tmp_path / "replay.sqlite3"
    warehouse_root = tmp_path / "warehouse"
    _seed_sqlite(db_path)
    ny = ZoneInfo("America/New_York")
    _materialize_warehouse(
        warehouse_root,
        raw_latest=datetime(2026, 3, 2, 18, 3, tzinfo=ny),
        derived_latest=datetime(2026, 3, 2, 18, 5, tzinfo=ny),
        duplicate_raw=True,
        trade_latest=datetime(2026, 3, 2, 18, 3, tzinfo=ny),
    )

    result = run_research_data_integrity_audit(
        output_dir=tmp_path / "report",
        replay_db_path=db_path,
        warehouse_root=warehouse_root,
        provider_config=provider_cfg,
        lane_symbol_map={"MGC": ["test_lane__MGC"]},
    )
    mgc_row = next(row for row in result["payload"]["health"]["instrument_rows"] if row["instrument"] == "MGC")
    assert "warehouse_duplicate_bars" in mgc_row["issues"]


def test_research_market_data_integrity_daily_plan_shape_and_read_only_audit(tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)
    db_path = tmp_path / "replay.sqlite3"
    warehouse_root = tmp_path / "warehouse"
    _seed_sqlite(db_path)
    ny = ZoneInfo("America/New_York")
    _materialize_warehouse(
        warehouse_root,
        raw_latest=datetime(2026, 3, 2, 18, 3, tzinfo=ny),
        derived_latest=datetime(2026, 3, 2, 18, 5, tzinfo=ny),
        trade_latest=datetime(2026, 3, 2, 18, 3, tzinfo=ny),
    )

    connection = sqlite3.connect(db_path)
    try:
        before_count = connection.execute("select count(*) from bars").fetchone()[0]
    finally:
        connection.close()

    result = run_research_data_integrity_audit(
        output_dir=tmp_path / "report",
        replay_db_path=db_path,
        warehouse_root=warehouse_root,
        provider_config=provider_cfg,
        lane_symbol_map={"MGC": ["test_lane__MGC"]},
    )

    connection = sqlite3.connect(db_path)
    try:
        after_count = connection.execute("select count(*) from bars").fetchone()[0]
    finally:
        connection.close()

    assert before_count == after_count
    daily_plan = result["payload"]["daily_maintenance_plan"]
    assert daily_plan["mode_supported"] == ["incremental_update", "full_backfill", "dry_run_validation"]
    assert len(daily_plan["per_instrument"]) == 2
    assert "next_incremental_start" in daily_plan["per_instrument"][0]
