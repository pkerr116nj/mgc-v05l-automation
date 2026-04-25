from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
import importlib
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.provider_models import CoverageChange, CoverageSnapshot, HistoricalIngestAudit
from mgc_v05l.market_data.research_data_integrity import (
    execute_research_market_data_backfill,
    rebuild_canonical_warehouse_surfaces,
    run_research_data_integrity_audit,
)
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.db import create_schema
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.research.trend_participation.storage import materialize_parquet_dataset
from mgc_v05l.research.warehouse_historical_evaluator.layout import build_layout as build_warehouse_layout

integrity_module = importlib.import_module("mgc_v05l.market_data.research_data_integrity")


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


def _fake_ingest_audit(
    *,
    symbol: str,
    fetched_bar_count: int,
    inserted_bar_count: int,
) -> HistoricalIngestAudit:
    before = CoverageSnapshot(
        symbol=symbol,
        timeframe="1m",
        data_source="historical_1m_canonical",
        bar_count=0,
        earliest=None,
        latest=None,
    )
    after = CoverageSnapshot(
        symbol=symbol,
        timeframe="1m",
        data_source="historical_1m_canonical",
        bar_count=inserted_bar_count,
        earliest=None,
        latest="2026-04-21T23:59:00-04:00" if inserted_bar_count else None,
    )
    return HistoricalIngestAudit(
        provider="databento",
        internal_symbol=symbol,
        timeframe="1m",
        data_source="historical_1m_canonical",
        before=before,
        after=after,
        change=CoverageChange.APPENDED if inserted_bar_count else CoverageChange.MATCHED,
        fetched_bar_count=fetched_bar_count,
        inserted_bar_count=inserted_bar_count,
        skipped_existing_count=0,
        ingest_run_id=f"ingest-{symbol.lower()}",
        report_path=None,
    )


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


def _seed_holiday_session_divergence(db_path: Path) -> None:
    engine = build_engine(f"sqlite:///{db_path}")
    create_schema(engine)
    repositories = RepositorySet(engine)
    ny = ZoneInfo("America/New_York")
    for hour in (18, 19):
        repositories.bars.save(
            _bar(symbol="GC", timeframe="1m", end_ts=datetime(2026, 4, 1, hour, 1, tzinfo=ny)),
            data_source="historical_1m_canonical",
        )
        repositories.bars.save(
            _bar(symbol="GC", timeframe="1m", end_ts=datetime(2026, 4, 5, hour, 1, tzinfo=ny)),
            data_source="historical_1m_canonical",
        )
        repositories.bars.save(
            _bar(symbol="MGC", timeframe="1m", end_ts=datetime(2026, 4, 1, hour, 1, tzinfo=ny)),
            data_source="historical_1m_canonical",
        )
        repositories.bars.save(
            _bar(symbol="MGC", timeframe="1m", end_ts=datetime(2026, 4, 5, hour, 1, tzinfo=ny)),
            data_source="historical_1m_canonical",
        )
    for minute in (1, 2, 3):
        repositories.bars.save(
            _bar(symbol="ES", timeframe="1m", end_ts=datetime(2026, 4, 2, 18, minute, tzinfo=ny)),
            data_source="historical_1m_canonical",
        )


def _seed_true_missing_session_with_noncanonical_evidence(db_path: Path) -> None:
    engine = build_engine(f"sqlite:///{db_path}")
    create_schema(engine)
    repositories = RepositorySet(engine)
    ny = ZoneInfo("America/New_York")
    repositories.bars.save(
        _bar(symbol="GC", timeframe="1m", end_ts=datetime(2026, 4, 1, 18, 1, tzinfo=ny)),
        data_source="historical_1m_canonical",
    )
    repositories.bars.save(
        _bar(symbol="GC", timeframe="1m", end_ts=datetime(2026, 4, 3, 18, 1, tzinfo=ny)),
        data_source="historical_1m_canonical",
    )
    repositories.bars.save(
        _bar(symbol="GC", timeframe="1m", end_ts=datetime(2026, 4, 2, 18, 1, tzinfo=ny), bar_id_suffix="schwab"),
        data_source="schwab_history",
    )


def _seed_mbt_nonblocking_provider_gap(db_path: Path) -> None:
    engine = build_engine(f"sqlite:///{db_path}")
    create_schema(engine)
    repositories = RepositorySet(engine)
    ny = ZoneInfo("America/New_York")
    repositories.bars.save(
        _bar(symbol="MBT", timeframe="1m", end_ts=datetime(2026, 3, 13, 18, 1, tzinfo=ny)),
        data_source="historical_1m_canonical",
    )
    repositories.bars.save(
        _bar(symbol="MBT", timeframe="1m", end_ts=datetime(2026, 3, 16, 20, 1, tzinfo=ny)),
        data_source="historical_1m_canonical",
    )
    repositories.bars.save(
        _bar(symbol="MBT", timeframe="1m", end_ts=datetime(2026, 3, 15, 18, 1, tzinfo=ny), bar_id_suffix="schwab"),
        data_source="schwab_history",
    )


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
    assert "warehouse_raw_1m_misaligned" not in mgc_row["issues"]
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


def test_research_market_data_integrity_scoped_symbol_audit_and_phase_timings(tmp_path: Path) -> None:
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

    progress_events: list[dict[str, object]] = []
    result = run_research_data_integrity_audit(
        output_dir=tmp_path / "report",
        replay_db_path=db_path,
        warehouse_root=warehouse_root,
        provider_config=provider_cfg,
        lane_symbol_map={"MGC": ["test_lane__MGC"]},
        symbols=["MGC"],
        progress_callback=progress_events.append,
    )

    payload = result["payload"]
    assert payload["instrument_registry"]["instruments"] == ["MGC"]
    assert payload["instrument_registry"]["instrument_count"] == 1
    assert {row["instrument"] for row in payload["replay_audit"]["canonical_coverage_rows"]} == {"MGC"}
    phase_names = [row["phase"] for row in payload["audit_runtime"]["phase_rows"]]
    assert phase_names == [
        "registry_load",
        "canonical_1m_coverage",
        "source_overlap_checks",
        "warehouse_integrity_checks",
        "trade_replay_artifact_alignment",
        "report_write",
    ]
    assert all(float(row["duration_seconds"]) >= 0 for row in payload["audit_runtime"]["phase_rows"])
    assert any(event["phase"] == "canonical_1m_coverage" and event["event"] == "start" for event in progress_events)
    assert any(event["phase"] == "canonical_1m_coverage" and event["event"] == "end" for event in progress_events)


def test_research_market_data_integrity_fetch_only_validation_skips_warehouse_checks(tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)
    db_path = tmp_path / "replay.sqlite3"
    warehouse_root = tmp_path / "warehouse"
    _seed_sqlite(db_path)

    result = run_research_data_integrity_audit(
        output_dir=tmp_path / "report",
        replay_db_path=db_path,
        warehouse_root=warehouse_root,
        provider_config=provider_cfg,
        lane_symbol_map={"MGC": ["test_lane__MGC"]},
        symbols=["MGC"],
        skip_warehouse_checks=True,
    )

    payload = result["payload"]
    assert payload["analysis_allowed"] is True
    assert payload["overall_status"] == "healthy"
    assert payload["health"]["validation_scope"] == "fetch_only_replay"
    assert payload["trade_alignment"]["mode"] == "fetch_only_validation"
    assert payload["warehouse_audit"]["audit_skipped"] is True
    assert payload["warehouse_audit"]["reason"] == "skip_warehouse_checks"
    phase_names = [row["phase"] for row in payload["audit_runtime"]["phase_rows"]]
    assert phase_names == [
        "registry_load",
        "canonical_1m_coverage",
        "source_overlap_checks",
        "trade_replay_artifact_alignment",
        "report_write",
    ]


def test_research_market_data_integrity_writes_partial_report_on_timeout(monkeypatch, tmp_path: Path) -> None:
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

    def _raise_timeout(**_: object) -> dict[str, object]:
        raise integrity_module.AuditPhaseTimeout(phase="source_overlap_checks", timeout_seconds=0.01)

    monkeypatch.setattr(integrity_module, "_audit_source_overlap_checks", _raise_timeout)

    result = run_research_data_integrity_audit(
        output_dir=tmp_path / "report",
        replay_db_path=db_path,
        warehouse_root=warehouse_root,
        provider_config=provider_cfg,
        lane_symbol_map={"MGC": ["test_lane__MGC"]},
        symbols=["MGC"],
    )

    payload = result["payload"]
    assert payload["audit_runtime"]["status"] == "blocked"
    assert payload["audit_runtime"]["reason"] == "audit_phase_timeout"
    assert payload["audit_runtime"]["phase"] == "source_overlap_checks"
    assert payload["health"]["overall_status"] == "blocked"
    assert payload["analysis_allowed"] is False
    summary_path = Path(result["artifacts"]["summary_json_path"])
    assert summary_path.exists()
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["audit_runtime"]["status"] == "blocked"


def test_research_market_data_integrity_writes_partial_report_on_failure(monkeypatch, tmp_path: Path) -> None:
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

    def _raise_failure(**_: object) -> dict[str, object]:
        raise RuntimeError("synthetic warehouse failure")

    monkeypatch.setattr(integrity_module, "_audit_warehouse", _raise_failure)

    result = run_research_data_integrity_audit(
        output_dir=tmp_path / "report",
        replay_db_path=db_path,
        warehouse_root=warehouse_root,
        provider_config=provider_cfg,
        lane_symbol_map={"MGC": ["test_lane__MGC"]},
        symbols=["MGC"],
    )

    payload = result["payload"]
    assert payload["audit_runtime"]["status"] == "failed"
    assert payload["audit_runtime"]["reason"] == "RuntimeError"
    assert payload["analysis_allowed"] is False
    assert Path(result["artifacts"]["summary_json_path"]).exists()


def test_research_market_data_integrity_uses_canonical_only_research_surface_for_overlap(tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)
    db_path = tmp_path / "replay.sqlite3"
    warehouse_root = tmp_path / "warehouse"
    _seed_sqlite(db_path)
    engine = build_engine(f"sqlite:///{db_path}")
    repositories = RepositorySet(engine)
    ny = ZoneInfo("America/New_York")
    repositories.bars.save(
        _bar(symbol="MGC", timeframe="1m", end_ts=datetime(2026, 3, 2, 18, 1, tzinfo=ny), bar_id_suffix="schwab"),
        data_source="schwab_history",
    )
    _materialize_warehouse(
        warehouse_root,
        raw_latest=datetime(2026, 3, 2, 18, 3, tzinfo=ny),
        derived_latest=datetime(2026, 3, 2, 18, 5, tzinfo=ny),
        trade_latest=datetime(2026, 3, 2, 18, 1, tzinfo=ny),
    )

    result = run_research_data_integrity_audit(
        output_dir=tmp_path / "report",
        replay_db_path=db_path,
        warehouse_root=warehouse_root,
        provider_config=provider_cfg,
        lane_symbol_map={"MGC": ["test_lane__MGC"]},
        symbols=["MGC"],
    )

    payload = result["payload"]
    assert all(int(row["duplicate_overlap_rows"]) == 0 for row in payload["replay_audit"]["source_overlap_rows"])
    assert any(int(row["duplicate_overlap_rows"]) > 0 for row in payload["replay_audit"]["storage_source_overlap_rows"])
    mgc_row = next(row for row in payload["health"]["instrument_rows"] if row["instrument"] == "MGC")
    assert "mixed_source_overlap" not in mgc_row["issues"]
    assert "forbidden_research_source_present" not in mgc_row["issues"]


def test_research_market_data_integrity_trade_alignment_uses_materialized_shard_coverage(tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)
    db_path = tmp_path / "replay.sqlite3"
    warehouse_root = tmp_path / "warehouse"
    _seed_sqlite(db_path)
    ny = ZoneInfo("America/New_York")
    _materialize_warehouse(
        warehouse_root,
        raw_latest=datetime(2026, 3, 2, 18, 3, tzinfo=ny),
        derived_latest=datetime(2026, 3, 2, 18, 5, tzinfo=ny),
        trade_latest=datetime(2026, 3, 2, 18, 1, tzinfo=ny),
    )

    result = run_research_data_integrity_audit(
        output_dir=tmp_path / "report",
        replay_db_path=db_path,
        warehouse_root=warehouse_root,
        provider_config=provider_cfg,
        lane_symbol_map={"MGC": ["test_lane__MGC"]},
        symbols=["MGC"],
    )

    payload = result["payload"]
    assert payload["trade_alignment"]["analysis_allowed"] is True
    assert payload["health"]["overall_status"] == "healthy"


def test_research_market_data_integrity_scoped_audit_ignores_unrequested_lane_symbols(tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)
    db_path = tmp_path / "replay.sqlite3"
    warehouse_root = tmp_path / "warehouse"
    _seed_sqlite(db_path)
    ny = ZoneInfo("America/New_York")
    _materialize_warehouse(
        warehouse_root,
        raw_latest=datetime(2026, 3, 2, 18, 3, tzinfo=ny),
        derived_latest=datetime(2026, 3, 2, 18, 5, tzinfo=ny),
        trade_latest=datetime(2026, 3, 2, 18, 1, tzinfo=ny),
    )

    result = run_research_data_integrity_audit(
        output_dir=tmp_path / "report",
        replay_db_path=db_path,
        warehouse_root=warehouse_root,
        provider_config=provider_cfg,
        lane_symbol_map={"MGC": ["test_lane__MGC"], "GC": ["test_lane__GC"]},
        symbols=["ES"],
    )

    payload = result["payload"]
    assert payload["trade_alignment"]["analysis_allowed"] is True
    assert payload["trade_alignment"]["symbol_rows"] == []


def test_research_market_data_integrity_empty_configured_scope_short_circuits_cleanly(tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)
    db_path = tmp_path / "replay.sqlite3"
    warehouse_root = tmp_path / "warehouse"

    result = run_research_data_integrity_audit(
        output_dir=tmp_path / "report",
        replay_db_path=db_path,
        warehouse_root=warehouse_root,
        provider_config=provider_cfg,
        lane_symbol_map={},
        symbols=["YM", "MBT"],
    )

    payload = result["payload"]
    assert payload["instrument_registry"]["instrument_count"] == 0
    assert payload["instrument_registry"]["unknown_requested_symbols"] == ["MBT", "YM"]
    assert payload["analysis_allowed"] is False
    assert payload["overall_status"] == "skipped"
    assert payload["audit_runtime"]["status"] == "completed"
    assert payload["audit_runtime"]["reason"] == "empty_configured_scope"
    assert payload["health"]["reason"] == "empty_configured_scope"
    assert payload["health"]["unmapped_symbols"] == ["MBT", "YM"]
    phase_names = [row["phase"] for row in payload["audit_runtime"]["phase_rows"]]
    assert phase_names == ["registry_load", "report_write"]


def test_session_audit_does_not_use_es_holiday_activity_to_require_metals(tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)
    db_path = tmp_path / "replay.sqlite3"
    warehouse_root = tmp_path / "warehouse"
    _seed_holiday_session_divergence(db_path)
    ny = ZoneInfo("America/New_York")
    _materialize_warehouse(
        warehouse_root,
        raw_latest=datetime(2026, 4, 5, 18, 1, tzinfo=ny),
        derived_latest=datetime(2026, 4, 5, 18, 5, tzinfo=ny),
    )

    result = run_research_data_integrity_audit(
        output_dir=tmp_path / "report",
        replay_db_path=db_path,
        warehouse_root=warehouse_root,
        provider_config=provider_cfg,
        lane_symbol_map={},
        symbols=["ES", "GC", "MGC"],
    )

    gaps = result["payload"]["replay_audit"]["session_audit"]["session_gap_rows"]
    assert not any(row["instrument"] in {"GC", "MGC"} and row.get("gap_start") == "2026-04-02" for row in gaps)


def test_session_audit_flags_true_missing_session_when_noncanonical_source_has_evidence(tmp_path: Path) -> None:
    db_path = tmp_path / "replay.sqlite3"
    _seed_true_missing_session_with_noncanonical_evidence(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        gaps = integrity_module._session_audit(conn, instruments=["GC"])["session_gap_rows"]
    finally:
        conn.close()
    assert any(
        row["instrument"] == "GC"
        and row["gap_start"] == "2026-04-02"
        and row["missing_reason"] == "session_gap"
        and "schwab_history" in str(row.get("evidence_data_sources") or "")
        for row in gaps
    )


def test_session_audit_classifies_known_provider_no_data_gap_as_nonblocking(tmp_path: Path) -> None:
    db_path = tmp_path / "replay.sqlite3"
    _seed_mbt_nonblocking_provider_gap(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        session_audit = integrity_module._session_audit(conn, instruments=["MBT"])
    finally:
        conn.close()
    assert session_audit["session_gap_rows"] == [
        {
            "instrument": "MBT",
            "gap_start": "2026-03-15",
            "gap_end": "2026-03-15",
            "missing_session_count": 1,
            "missing_reason": "provider_continuous_no_data",
            "gap_reason": "databento_continuous_zero_records_exact_session_window",
            "blocking": False,
            "evidence_data_sources": "schwab_history",
        }
    ]

    health = integrity_module._build_health_report(
        replay_audit={
            "duplicate_rows": [],
            "source_overlap_rows": [],
            "canonical_coverage_rows": [
                {
                    "instrument": "MBT",
                    "latest_ts": "2026-04-21T23:59:00-04:00",
                    "missing_bar_count": 0,
                }
            ],
            "session_audit": session_audit,
        },
        warehouse_audit={
            "dataset_reports": {
                "raw_bars_1m": {
                    "overall_rows": [
                        {
                            "symbol": "MBT",
                            "latest_ts": "2026-04-21T23:59:00-04:00",
                            "duplicate_timestamp_count": 0,
                        }
                    ]
                },
                "derived_bars_5m": {
                    "overall_rows": [
                        {
                            "symbol": "MBT",
                            "latest_ts": "2026-04-22T03:55:00+00:00",
                            "duplicate_timestamp_count": 0,
                        }
                    ]
                },
            }
        },
        trade_alignment={"analysis_allowed": True, "blocking_issues": [], "symbol_rows": []},
        policy=integrity_module.IntegritySourcePolicy(),
    )
    assert health["overall_status"] == "healthy"
    assert health["blocking_issues"] == []
    assert health["instrument_rows"][0]["instrument"] == "MBT"
    assert health["instrument_rows"][0]["accepted_missing_sessions"] == 1
    assert health["instrument_rows"][0]["missing_sessions"] == 0
    assert health["instrument_rows"][0]["status"] == "healthy"


def test_backfill_skips_unmapped_symbols_without_failing_configured_symbols(monkeypatch, tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)

    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

    class FakeIngestion:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def ingest(
            self,
            *,
            provider: object,
            request: object,
            allow_canonical_overwrite: bool = False,
            progress_callback: object | None = None,
        ) -> HistoricalIngestAudit:
            return _fake_ingest_audit(symbol=request.internal_symbol, fetched_bar_count=2, inserted_bar_count=2)  # type: ignore[attr-defined]

    class FakeMaintenance:
        def __init__(self, *_: object, **__: object) -> None:
            pass

    monkeypatch.setattr(integrity_module, "load_settings_from_files", lambda *_args, **_kwargs: SimpleNamespace(database_url="sqlite:///tmp/test.sqlite3"))
    monkeypatch.setattr(integrity_module, "DatabentoMarketDataProvider", FakeProvider)
    monkeypatch.setattr(integrity_module, "HistoricalMarketDataIngestionService", FakeIngestion)
    monkeypatch.setattr(integrity_module, "CanonicalMarketDataMaintenanceService", FakeMaintenance)

    result = execute_research_market_data_backfill(
        replay_db_path=tmp_path / "replay.sqlite3",
        provider_config=provider_cfg,
        symbols=["MGC", "GC"],
        start_ts=datetime.fromisoformat("2024-01-01T18:00:00-05:00"),
        end_ts=datetime.fromisoformat("2024-01-31T23:59:00-05:00"),
    )

    assert result["configured_symbols"] == ["MGC"]
    assert result["unmapped_symbols"] == ["GC"]
    assert any(row["symbol"] == "GC" and row["outcome"] == "unmapped_symbol" for row in result["symbol_results"])
    assert any(row["symbol"] == "MGC" and row["outcome"] == "fetch_completed" for row in result["symbol_results"])


def test_backfill_fetch_only_does_not_invoke_gap_repair_or_derivation(monkeypatch, tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)

    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

    class FakeIngestion:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def ingest(
            self,
            *,
            provider: object,
            request: object,
            allow_canonical_overwrite: bool = False,
            progress_callback: object | None = None,
        ) -> HistoricalIngestAudit:
            return _fake_ingest_audit(symbol=request.internal_symbol, fetched_bar_count=3, inserted_bar_count=3)  # type: ignore[attr-defined]

    class FakeMaintenance:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def backfill_detected_gaps(self, *_: object, **__: object) -> dict[str, object]:
            raise AssertionError("gap repair should not run in fetch-only mode")

        def derive_timeframe(self, *_: object, **__: object) -> dict[str, object]:
            raise AssertionError("derivation should not run in fetch-only mode")

    monkeypatch.setattr(integrity_module, "load_settings_from_files", lambda *_args, **_kwargs: SimpleNamespace(database_url="sqlite:///tmp/test.sqlite3"))
    monkeypatch.setattr(integrity_module, "DatabentoMarketDataProvider", FakeProvider)
    monkeypatch.setattr(integrity_module, "HistoricalMarketDataIngestionService", FakeIngestion)
    monkeypatch.setattr(integrity_module, "CanonicalMarketDataMaintenanceService", FakeMaintenance)

    result = execute_research_market_data_backfill(
        replay_db_path=tmp_path / "replay.sqlite3",
        provider_config=provider_cfg,
        symbols=["MGC"],
        start_ts=datetime.fromisoformat("2024-01-01T18:00:00-05:00"),
        end_ts=datetime.fromisoformat("2024-01-31T23:59:00-05:00"),
    )

    assert result["run_gap_repair"] is False
    assert result["derive_timeframes"] == []
    assert result["derivations"] == []
    assert [row["label"] for row in result["progress_rows"]] == ["request_started", "fetch_completed"]


def test_backfill_progress_labels_include_zero_record_outcome(monkeypatch, tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)
    progress_events: list[dict[str, object]] = []

    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

    class FakeIngestion:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def ingest(
            self,
            *,
            provider: object,
            request: object,
            allow_canonical_overwrite: bool = False,
            progress_callback: object | None = None,
        ) -> HistoricalIngestAudit:
            return _fake_ingest_audit(symbol=request.internal_symbol, fetched_bar_count=0, inserted_bar_count=0)  # type: ignore[attr-defined]

    class FakeMaintenance:
        def __init__(self, *_: object, **__: object) -> None:
            pass

    monkeypatch.setattr(integrity_module, "load_settings_from_files", lambda *_args, **_kwargs: SimpleNamespace(database_url="sqlite:///tmp/test.sqlite3"))
    monkeypatch.setattr(integrity_module, "DatabentoMarketDataProvider", FakeProvider)
    monkeypatch.setattr(integrity_module, "HistoricalMarketDataIngestionService", FakeIngestion)
    monkeypatch.setattr(integrity_module, "CanonicalMarketDataMaintenanceService", FakeMaintenance)

    result = execute_research_market_data_backfill(
        replay_db_path=tmp_path / "replay.sqlite3",
        provider_config=provider_cfg,
        symbols=["MGC"],
        start_ts=datetime.fromisoformat("2024-01-01T18:00:00-05:00"),
        end_ts=datetime.fromisoformat("2024-01-02T23:59:00-05:00"),
        progress_callback=progress_events.append,
    )

    assert result["symbol_results"][0]["outcome"] == "zero_records_no_data"
    assert [row["label"] for row in result["progress_rows"]] == ["request_started", "zero_records_no_data"]
    assert [row["label"] for row in progress_events] == ["request_started", "zero_records_no_data"]


def test_backfill_explicit_maintenance_emits_subphase_labels(monkeypatch, tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)

    @dataclass(frozen=True)
    class FakeDerivation:
        derived_bar_count: int
        target_timeframe: str

    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

    class FakeIngestion:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def ingest(
            self,
            *,
            provider: object,
            request: object,
            allow_canonical_overwrite: bool = False,
            progress_callback: object | None = None,
        ) -> HistoricalIngestAudit:
            return _fake_ingest_audit(symbol=request.internal_symbol, fetched_bar_count=4, inserted_bar_count=4)  # type: ignore[attr-defined]

    class FakeMaintenance:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def backfill_detected_gaps(self, *_: object, **__: object) -> dict[str, object]:
            return {"gap_count": 0, "repairs": []}

        def derive_timeframe(self, *_: object, **__: object):
            return FakeDerivation(derived_bar_count=2, target_timeframe="5m")

    monkeypatch.setattr(integrity_module, "load_settings_from_files", lambda *_args, **_kwargs: SimpleNamespace(database_url="sqlite:///tmp/test.sqlite3"))
    monkeypatch.setattr(integrity_module, "DatabentoMarketDataProvider", FakeProvider)
    monkeypatch.setattr(integrity_module, "HistoricalMarketDataIngestionService", FakeIngestion)
    monkeypatch.setattr(integrity_module, "CanonicalMarketDataMaintenanceService", FakeMaintenance)

    result = execute_research_market_data_backfill(
        replay_db_path=tmp_path / "replay.sqlite3",
        provider_config=provider_cfg,
        symbols=["MGC"],
        start_ts=datetime.fromisoformat("2024-01-01T18:00:00-05:00"),
        end_ts=datetime.fromisoformat("2024-01-31T23:59:00-05:00"),
        run_gap_repair=True,
        derive_timeframes=("5m",),
    )

    assert [row["label"] for row in result["progress_rows"]] == [
        "request_started",
        "fetch_completed",
        "gap_repair_started",
        "gap_repair_completed",
        "derive_5m_started",
        "derive_5m_completed",
    ]


def test_backfill_surfaces_staged_fetch_progress_labels(monkeypatch, tmp_path: Path) -> None:
    provider_cfg = _provider_config(tmp_path)

    class FakeProvider:
        def __init__(self, *_: object, **__: object) -> None:
            pass

    class FakeIngestion:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def ingest(
            self,
            *,
            provider: object,
            request: object,
            allow_canonical_overwrite: bool = False,
            progress_callback: object | None = None,
        ) -> HistoricalIngestAudit:
            assert progress_callback is not None
            progress_callback({"label": "download_started", "status": "running", "detail": {}})
            progress_callback({"label": "download_completed", "status": "completed", "detail": {"byte_count": 42}})
            progress_callback({"label": "parse_started", "status": "running", "detail": {}})
            progress_callback({"label": "rows_persisted", "status": "running", "detail": {"rows_in_batch": 2}})
            progress_callback({"label": "parse_completed", "status": "completed", "detail": {"fetched_bar_count": 2}})
            return _fake_ingest_audit(symbol=request.internal_symbol, fetched_bar_count=2, inserted_bar_count=2)  # type: ignore[attr-defined]

    class FakeMaintenance:
        def __init__(self, *_: object, **__: object) -> None:
            pass

    monkeypatch.setattr(integrity_module, "load_settings_from_files", lambda *_args, **_kwargs: SimpleNamespace(database_url="sqlite:///tmp/test.sqlite3"))
    monkeypatch.setattr(integrity_module, "DatabentoMarketDataProvider", FakeProvider)
    monkeypatch.setattr(integrity_module, "HistoricalMarketDataIngestionService", FakeIngestion)
    monkeypatch.setattr(integrity_module, "CanonicalMarketDataMaintenanceService", FakeMaintenance)

    result = execute_research_market_data_backfill(
        replay_db_path=tmp_path / "replay.sqlite3",
        provider_config=provider_cfg,
        symbols=["MGC"],
        start_ts=datetime.fromisoformat("2024-01-01T18:00:00-05:00"),
        end_ts=datetime.fromisoformat("2024-01-31T23:59:00-05:00"),
    )

    assert [row["label"] for row in result["progress_rows"]] == [
        "request_started",
        "download_started",
        "download_completed",
        "parse_started",
        "rows_persisted",
        "parse_completed",
        "fetch_completed",
    ]


def test_warehouse_rebuild_iterates_multiple_quarters_and_emits_progress(monkeypatch, tmp_path: Path) -> None:
    raw_calls: list[tuple[str, str]] = []
    derived_calls: list[tuple[str, str]] = []
    replay_db_path = tmp_path / "replay.sqlite3"
    engine = build_engine(f"sqlite:///{replay_db_path}")
    create_schema(engine)
    repositories = RepositorySet(engine)
    ny = ZoneInfo("America/New_York")
    for symbol in ("ZT", "ZF"):
        repositories.bars.save(
            _bar(symbol=symbol, timeframe="1m", end_ts=datetime(2024, 1, 1, 18, 1, tzinfo=ny)),
            data_source="historical_1m_canonical",
        )
        repositories.bars.save(
            _bar(symbol=symbol, timeframe="1m", end_ts=datetime(2024, 7, 15, 23, 59, tzinfo=ny)),
            data_source="historical_1m_canonical",
        )

    def _fake_export(
        *,
        root_dir: Path,
        sqlite_path: Path,
        symbol: str,
        shard_id: str,
        start_ts: datetime,
        end_ts: datetime,
        data_source: str = "historical_1m_canonical",
    ) -> dict[str, object]:
        raw_calls.append((symbol, shard_id))
        partition_path = root_dir / "datasets" / "raw_bars_1m" / f"symbol={symbol}" / f"year={start_ts.year}" / f"shard_id={shard_id}" / "bars.parquet"
        return {
            "partition_path": str(partition_path),
            "row_count": 10,
            "raw_version": f"{symbol}-{shard_id}",
            "cache": {"cache_hit": False},
        }

    def _fake_derive(
        *,
        root_dir: Path,
        symbol: str,
        shard_id: str,
        year: int,
        timeframe: str,
        raw_partition_path: Path,
        raw_version: str,
        materialized_ts: datetime | None = None,
    ) -> dict[str, object]:
        derived_calls.append((symbol, shard_id, timeframe))
        partition_path = root_dir / "datasets" / f"derived_bars_{timeframe}" / f"symbol={symbol}" / f"year={year}" / f"shard_id={shard_id}" / "bars.parquet"
        return {
            "partition_path": str(partition_path),
            "row_count": 2,
            "cache": {"cache_hit": False},
        }

    monkeypatch.setattr(integrity_module, "export_canonical_1m_partition", _fake_export)
    monkeypatch.setattr(integrity_module, "materialize_derived_timeframe_partition", _fake_derive)

    progress_events: list[dict[str, object]] = []
    result = rebuild_canonical_warehouse_surfaces(
        warehouse_root=tmp_path / "warehouse",
        replay_db_path=replay_db_path,
        instruments=["ZT", "ZF"],
        start_ts=datetime.fromisoformat("2024-01-01T18:00:00-05:00"),
        end_ts=datetime.fromisoformat("2024-07-15T23:59:00-04:00"),
        progress_callback=progress_events.append,
    )

    planned_shards = [row["shard_id"] for row in result["planned_shards"]]
    assert planned_shards == ["2024Q1", "2024Q2", "2024Q3"]
    assert raw_calls == [
        ("ZF", "2024Q1"),
        ("ZF", "2024Q2"),
        ("ZF", "2024Q3"),
        ("ZT", "2024Q1"),
        ("ZT", "2024Q2"),
        ("ZT", "2024Q3"),
    ]
    expected_derived_calls = []
    for symbol, shard_id in raw_calls:
        for timeframe in ("5m", "15m", "60m", "240m", "daily"):
            expected_derived_calls.append((symbol, shard_id, timeframe))
    assert derived_calls == expected_derived_calls
    assert len(result["results"]) == 6
    labels = [str(row["label"]) for row in progress_events]
    assert labels[0] == "rebuild_started"
    assert "raw_1m_started" in labels
    assert "raw_1m_completed" in labels
    assert "derived_5m_started" in labels
    assert "derived_5m_completed" in labels
    assert "derived_15m_started" in labels
    assert "derived_60m_started" in labels
    assert "derived_240m_started" in labels
    assert "derived_daily_started" in labels
    assert "symbol_shard_completed" in labels
    assert labels[-1] == "rebuild_completed"


def test_warehouse_rebuild_uses_earliest_canonical_start_per_symbol_when_start_omitted(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "replay.sqlite3"
    engine = build_engine(f"sqlite:///{db_path}")
    create_schema(engine)
    repositories = RepositorySet(engine)
    ny = ZoneInfo("America/New_York")

    repositories.bars.save(
        _bar(symbol="GC", timeframe="1m", end_ts=datetime(2020, 1, 1, 18, 1, tzinfo=ny)),
        data_source="historical_1m_canonical",
    )
    repositories.bars.save(
        _bar(symbol="GC", timeframe="1m", end_ts=datetime(2026, 4, 21, 23, 59, tzinfo=ny)),
        data_source="historical_1m_canonical",
    )
    repositories.bars.save(
        _bar(symbol="MGC", timeframe="1m", end_ts=datetime(2021, 4, 1, 18, 1, tzinfo=ny)),
        data_source="historical_1m_canonical",
    )
    repositories.bars.save(
        _bar(symbol="MGC", timeframe="1m", end_ts=datetime(2026, 4, 21, 23, 59, tzinfo=ny)),
        data_source="historical_1m_canonical",
    )

    raw_calls: list[tuple[str, str, datetime, datetime]] = []
    derived_calls: list[tuple[str, str, str]] = []

    def _fake_export(
        *,
        root_dir: Path,
        sqlite_path: Path,
        symbol: str,
        shard_id: str,
        start_ts: datetime,
        end_ts: datetime,
        data_source: str = "historical_1m_canonical",
    ) -> dict[str, object]:
        raw_calls.append((symbol, shard_id, start_ts, end_ts))
        partition_path = root_dir / "datasets" / "raw_bars_1m" / f"symbol={symbol}" / f"year={start_ts.year}" / f"shard_id={shard_id}" / "bars.parquet"
        return {
            "partition_path": str(partition_path),
            "row_count": 10,
            "raw_version": f"{symbol}-{shard_id}",
            "coverage": {"start": start_ts.isoformat(), "end": end_ts.isoformat()},
            "cache": {"cache_hit": False},
        }

    def _fake_derive(
        *,
        root_dir: Path,
        symbol: str,
        shard_id: str,
        year: int,
        timeframe: str,
        raw_partition_path: Path,
        raw_version: str,
        materialized_ts: datetime | None = None,
    ) -> dict[str, object]:
        derived_calls.append((symbol, shard_id, timeframe))
        partition_path = root_dir / "datasets" / f"derived_bars_{timeframe}" / f"symbol={symbol}" / f"year={year}" / f"shard_id={shard_id}" / "bars.parquet"
        coverage_start = next(
            start
            for raw_symbol, raw_shard, start, _ in raw_calls
            if raw_symbol == symbol and raw_shard == shard_id
        )
        return {
            "partition_path": str(partition_path),
            "row_count": 2,
            "coverage": {"start": coverage_start.isoformat(), "end": coverage_start.isoformat()},
            "cache": {"cache_hit": False},
        }

    monkeypatch.setattr(integrity_module, "export_canonical_1m_partition", _fake_export)
    monkeypatch.setattr(integrity_module, "materialize_derived_timeframe_partition", _fake_derive)

    result = rebuild_canonical_warehouse_surfaces(
        warehouse_root=tmp_path / "warehouse",
        replay_db_path=db_path,
        instruments=["GC", "MGC"],
        start_ts=None,
        end_ts=datetime.fromisoformat("2026-04-21T23:59:00-04:00"),
    )

    assert result["symbol_ranges"]["GC"]["canonical_start_ts"] == "2020-01-01T18:01:00-05:00"
    assert result["symbol_ranges"]["GC"]["effective_start_ts"] == "2020-01-01T18:01:00-05:00"
    assert result["symbol_ranges"]["MGC"]["canonical_start_ts"] == "2021-04-01T18:01:00-04:00"
    assert result["symbol_ranges"]["MGC"]["effective_start_ts"] == "2021-04-01T18:01:00-04:00"
    assert result["planned_shards"][0]["shard_id"] == "2020Q1"
    assert result["planned_shards"][-1]["shard_id"] == "2026Q2"

    gc_shards = [row["shard_id"] for row in result["planned_symbol_shards"] if row["symbol"] == "GC"]
    mgc_shards = [row["shard_id"] for row in result["planned_symbol_shards"] if row["symbol"] == "MGC"]
    assert gc_shards[0] == "2020Q1"
    assert mgc_shards[0] == "2021Q2"

    assert raw_calls[0][0] == "GC"
    assert raw_calls[0][1] == "2020Q1"
    assert raw_calls[0][2].isoformat() == "2020-01-01T18:01:00-05:00"
    assert ("GC", "2020Q1", "5m") in derived_calls
    assert ("MGC", "2021Q2", "daily") in derived_calls


def test_warehouse_rebuild_ensures_supporting_indexes(tmp_path: Path) -> None:
    db_path = tmp_path / "replay.sqlite3"
    engine = build_engine(f"sqlite:///{db_path}")
    create_schema(engine)

    ensured = integrity_module._ensure_research_rebuild_indexes(db_path)

    connection = sqlite3.connect(db_path)
    try:
        index_names = {
            row[0]
            for row in connection.execute(
                "select name from sqlite_master where type='index'"
            ).fetchall()
        }
    finally:
        connection.close()

    assert "ix_bars_symbol_timeframe_source_end_ts" in index_names
    assert "ix_market_data_bar_provenance_bar_source_ingest" in index_names
    assert ensured == [
        "ix_bars_symbol_timeframe_source_end_ts",
        "ix_market_data_bar_provenance_bar_source_ingest",
    ]
