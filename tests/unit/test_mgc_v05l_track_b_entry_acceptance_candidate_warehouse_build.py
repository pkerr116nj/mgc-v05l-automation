from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from mgc_v05l.app import track_b_entry_acceptance_candidate_warehouse_build as builder
from mgc_v05l.research.trend_participation.storage import materialize_parquet_dataset
from mgc_v05l.research.warehouse_historical_evaluator._warehouse_common import read_parquet_rows
from mgc_v05l.research.warehouse_historical_evaluator.layout import build_layout
from mgc_v05l.research.warehouse_historical_evaluator.raw_materializer import (
    build_dataset_partition_path,
    coverage_range,
)


def test_build_candidate_warehouse_partition_from_temp_replay_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    db_path = tmp_path / "replay.sqlite3"
    _seed_replay_db(db_path)
    _patch_candidate_stages(monkeypatch)

    manifest = builder.build_candidate_warehouse_partition(
        builder.CandidateWarehouseBuildConfig(
            warehouse_root=tmp_path / "warehouse",
            replay_db=db_path,
            symbol="GC",
            shard_id="2024Q2",
            start=datetime.fromisoformat("2024-04-01T00:01:00+00:00"),
            end=datetime.fromisoformat("2024-04-01T00:15:00+00:00"),
            lane_id="gc_asia_early_normal_breakout_retest_hold_turn__GC",
        )
    )

    manifest_path = Path(manifest["manifest_path"])
    validation_path = Path(manifest["validation_summary_path"])
    assert manifest_path.exists()
    assert validation_path.exists()

    saved_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert saved_manifest["research_execution_mode"] == builder.RESEARCH_EXECUTION_MODE
    assert saved_manifest["base_timeframe"] == "1m"
    assert saved_manifest["derived_timeframe"] == "5m"
    assert saved_manifest["aggregation_method"] == "complete_bucket_resample_from_canonical_1m"
    assert saved_manifest["anchor_rule"].startswith("UTC epoch-aligned")
    assert saved_manifest["row_counts"]["raw_bars_1m"] == 15
    assert saved_manifest["row_counts"]["derived_bars_5m"] == 3
    assert saved_manifest["row_counts"]["shared_features_5m"] == 3
    assert saved_manifest["row_counts"]["lane_candidates"] == 1
    assert saved_manifest["safety_flags"]["strategy_behavior_changed"] is False
    assert saved_manifest["safety_flags"]["lane_execution_performed"] is False
    assert validation["shared_features_matches_derived"] is True
    assert validation["candidate_flags_can_be_produced"] is True
    assert validation["candidate_flag_true_count"] == 1
    assert Path(saved_manifest["outputs"]["raw_bars_1m"]).exists()
    assert Path(saved_manifest["outputs"]["derived_bars_5m"]).exists()
    assert Path(saved_manifest["outputs"]["shared_features_5m"]).exists()
    assert Path(saved_manifest["outputs"]["lane_candidates"]).exists()


def test_rejects_nonhistorical_source(tmp_path: Path) -> None:
    db_path = tmp_path / "replay.sqlite3"
    _seed_replay_db(db_path)

    with pytest.raises(ValueError, match="historical_1m_canonical"):
        builder.build_candidate_warehouse_partition(
            builder.CandidateWarehouseBuildConfig(
                warehouse_root=tmp_path / "warehouse",
                replay_db=db_path,
                symbol="GC",
                shard_id="2024Q2",
                start=datetime.fromisoformat("2024-04-01T00:01:00+00:00"),
                end=datetime.fromisoformat("2024-04-01T00:15:00+00:00"),
                lane_id="gc_asia_early_normal_breakout_retest_hold_turn__GC",
                source="runtime_candles",
            )
        )


def test_rejects_duplicate_source_timestamps(tmp_path: Path) -> None:
    db_path = tmp_path / "replay.sqlite3"
    _seed_replay_db(db_path, duplicate_timestamp=True)

    with pytest.raises(ValueError, match="Duplicate canonical 1m timestamps"):
        builder.build_candidate_warehouse_partition(
            builder.CandidateWarehouseBuildConfig(
                warehouse_root=tmp_path / "warehouse",
                replay_db=db_path,
                symbol="GC",
                shard_id="2024Q2",
                start=datetime.fromisoformat("2024-04-01T00:01:00+00:00"),
                end=datetime.fromisoformat("2024-04-01T00:15:00+00:00"),
                lane_id="gc_asia_early_normal_breakout_retest_hold_turn__GC",
            )
        )


def test_rejects_mixed_timeframe_source_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "replay.sqlite3"
    _seed_replay_db(db_path, mixed_timeframe=True)

    with pytest.raises(ValueError, match="Mixed timeframe"):
        builder.build_candidate_warehouse_partition(
            builder.CandidateWarehouseBuildConfig(
                warehouse_root=tmp_path / "warehouse",
                replay_db=db_path,
                symbol="GC",
                shard_id="2024Q2",
                start=datetime.fromisoformat("2024-04-01T00:01:00+00:00"),
                end=datetime.fromisoformat("2024-04-01T00:15:00+00:00"),
                lane_id="gc_asia_early_normal_breakout_retest_hold_turn__GC",
            )
        )


def test_new_builder_has_no_forbidden_mutation_import_terms() -> None:
    module_text = Path(builder.__file__).read_text(encoding="utf-8")
    lowered = module_text.lower()
    forbidden = [
        "ib" + "api",
        "place" + "order",
        "submit" + "_attempt",
        "order" + "_intent",
        "mgc_v05l.execution",
        "mgc_v05l.strategy",
        "mgc_v05l.app.main",
    ]
    assert all(item.lower() not in lowered for item in forbidden)


def _patch_candidate_stages(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(builder, "materialize_shared_features_5m_partition", _fake_shared_features_5m_partition)
    monkeypatch.setattr(builder, "materialize_shared_features_1m_timing_partition", _fake_shared_features_1m_timing_partition)
    monkeypatch.setattr(builder, "materialize_family_event_tables_partition", _fake_family_event_tables_partition)
    monkeypatch.setattr(builder, "materialize_lane_candidates_partition", _fake_lane_candidates_partition)


def _fake_shared_features_5m_partition(
    *,
    root_dir: Path,
    symbol: str,
    shard_id: str,
    year: int,
    derived_5m_partition_path: Path,
    derived_from_version: str,
) -> dict[str, object]:
    layout = build_layout(root_dir)
    derived_rows = read_parquet_rows(derived_5m_partition_path)
    rows = [
        {
            "symbol": symbol,
            "shard_id": shard_id,
            "decision_ts": row["bar_ts"],
            "bar_id": f"{symbol}:5m:{row['bar_ts'].isoformat()}",
            "timeframe": "5m",
            builder.ASIA_BREAKOUT_RETEST_HOLD_FLAG: index == 1,
            "derived_from_version": derived_from_version,
            "provenance_tag": f"shared_features_5m:{symbol}:{shard_id}:test",
        }
        for index, row in enumerate(derived_rows)
    ]
    output_path = build_dataset_partition_path(
        dataset_root=layout["shared_features_5m"],
        symbol=symbol,
        year=year,
        shard_id=shard_id,
        filename="features.parquet",
    )
    materialize_parquet_dataset(output_path, rows)
    return {
        "dataset_name": "shared_features_5m",
        "partition_path": output_path,
        "row_count": len(rows),
        "coverage": coverage_range(rows, timestamp_key="decision_ts"),
        "rows": rows,
    }


def _fake_shared_features_1m_timing_partition(
    *,
    root_dir: Path,
    symbol: str,
    shard_id: str,
    year: int,
    raw_1m_partition_path: Path,
    raw_version: str,
) -> dict[str, object]:
    layout = build_layout(root_dir)
    raw_rows = read_parquet_rows(raw_1m_partition_path)
    rows = [
        {
            "symbol": symbol,
            "shard_id": shard_id,
            "timing_ts": row["bar_ts"],
            "bar_id": f"{symbol}:1m:{row['bar_ts'].isoformat()}",
            "timeframe": "1m",
            "raw_version": raw_version,
            "provenance_tag": f"shared_features_1m_timing:{symbol}:{shard_id}:test",
        }
        for row in raw_rows
    ]
    output_path = build_dataset_partition_path(
        dataset_root=layout["shared_features_1m_timing"],
        symbol=symbol,
        year=year,
        shard_id=shard_id,
        filename="timing.parquet",
    )
    materialize_parquet_dataset(output_path, rows)
    return {
        "dataset_name": "shared_features_1m_timing",
        "partition_path": output_path,
        "row_count": len(rows),
        "coverage": coverage_range(rows, timestamp_key="timing_ts"),
        "rows": rows,
    }


def _fake_family_event_tables_partition(
    *,
    root_dir: Path,
    symbol: str,
    shard_id: str,
    year: int,
    shared_features_5m_partition_path: Path,
    shared_features_1m_timing_partition_path: Path,
) -> dict[str, object]:
    layout = build_layout(root_dir)
    feature_rows = read_parquet_rows(shared_features_5m_partition_path)
    rows = [
        {
            "event_id": f"{symbol}:{shard_id}:event:1",
            "symbol": symbol,
            "family": builder.LEGACY_SOURCE_EVENT_FAMILY,
            "shard_id": shard_id,
            "candidate_ts": feature_rows[1]["decision_ts"],
            "decision_ts": feature_rows[1]["decision_ts"],
            "feature_bar_id": feature_rows[1]["bar_id"],
            "provenance_tag": f"family_event_tables:{symbol}:{shard_id}:test",
        }
    ]
    output_path = build_dataset_partition_path(
        dataset_root=layout["family_event_tables"],
        symbol=symbol,
        year=year,
        shard_id=shard_id,
        filename="events.parquet",
    )
    materialize_parquet_dataset(output_path, rows)
    return {
        "dataset_name": "family_event_tables",
        "partition_path": output_path,
        "row_count": len(rows),
        "coverage": coverage_range(rows, timestamp_key="candidate_ts"),
        "rows": rows,
    }


def _fake_lane_candidates_partition(
    *,
    root_dir: Path,
    symbol: str,
    shard_id: str,
    year: int,
    lane_ids: list[str],
    family_event_tables_partition_path: Path,
    shared_features_5m_partition_path: Path,
) -> dict[str, object]:
    layout = build_layout(root_dir)
    events = read_parquet_rows(family_event_tables_partition_path)
    rows = [
        {
            "candidate_id": f"{lane_ids[0]}:{events[0]['event_id']}",
            "event_id": events[0]["event_id"],
            "source_event_family": builder.LEGACY_SOURCE_EVENT_FAMILY,
            "lane_id": lane_ids[0],
            "family": builder.LEGACY_SOURCE_EVENT_FAMILY,
            "symbol": symbol,
            "shard_id": shard_id,
            "candidate_ts": events[0]["candidate_ts"],
            "decision_ts": events[0]["decision_ts"],
            "side": "LONG",
            "eligibility_label": "lane_feature_flag_true",
            "feature_bar_id": events[0]["feature_bar_id"],
            "provenance_tag": f"lane_candidates:{lane_ids[0]}:{shard_id}:test",
        }
    ]
    output_path = build_dataset_partition_path(
        dataset_root=layout["lane_candidates"],
        symbol=symbol,
        year=year,
        shard_id=shard_id,
        filename="candidates.parquet",
    )
    materialize_parquet_dataset(output_path, rows)
    return {
        "dataset_name": "lane_candidates",
        "partition_path": output_path,
        "row_count": len(rows),
        "coverage": coverage_range(rows, timestamp_key="candidate_ts"),
        "rows": rows,
    }


def _seed_replay_db(
    db_path: Path,
    *,
    duplicate_timestamp: bool = False,
    mixed_timeframe: bool = False,
) -> None:
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            """
            create table bars (
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
            create table market_data_bar_provenance (
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
        start = datetime.fromisoformat("2024-04-01T00:01:00+00:00")
        for index in range(15):
            end_ts = start + timedelta(minutes=index)
            _insert_bar(connection, bar_id=f"bar-{index}", end_ts=end_ts, price=2200.0 + index * 0.1)
        if duplicate_timestamp:
            _insert_bar(connection, bar_id="bar-duplicate", end_ts=start + timedelta(minutes=4), price=2201.0)
        if mixed_timeframe:
            _insert_bar(
                connection,
                bar_id="bar-5m",
                end_ts=start + timedelta(minutes=4),
                price=2201.0,
                timeframe="5m",
            )
        connection.commit()
    finally:
        connection.close()


def _insert_bar(
    connection: sqlite3.Connection,
    *,
    bar_id: str,
    end_ts: datetime,
    price: float,
    timeframe: str = "1m",
) -> None:
    start_ts = end_ts - timedelta(minutes=1)
    timestamp = end_ts.isoformat()
    connection.execute(
        """
        insert into bars (
            bar_id, instrument_id, ticker, cusip, asset_class, data_source, timestamp, symbol, timeframe,
            start_ts, end_ts, open, high, low, close, volume, is_final, session_asia, session_london,
            session_us, session_allowed, created_at
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            bar_id,
            1,
            "GC",
            None,
            "future",
            builder.CANONICAL_1M_SOURCE,
            timestamp,
            "GC",
            timeframe,
            start_ts.isoformat(),
            timestamp,
            price,
            price + 0.3,
            price - 0.2,
            price + 0.1,
            10,
            True,
            True,
            False,
            False,
            True,
            timestamp,
        ],
    )
    connection.execute(
        """
        insert into market_data_bar_provenance (
            provenance_id, ingest_run_id, bar_id, data_source, provider, dataset, schema_name,
            internal_symbol, raw_symbol, request_symbol, stype_in, stype_out, interval,
            source_timestamp, ingest_time, coverage_start, coverage_end, provenance_tag,
            provider_metadata_json
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            f"prov-{bar_id}",
            "ingest-test",
            bar_id,
            builder.CANONICAL_1M_SOURCE,
            "unit_test_provider",
            "unit_test_dataset",
            "ohlcv-1m",
            "GC",
            "GC",
            "GC",
            "raw_symbol",
            "continuous",
            "1m",
            end_ts.isoformat(),
            end_ts.isoformat(),
            end_ts.isoformat(),
            end_ts.isoformat(),
            f"historical_1m_canonical:GC:{bar_id}",
            "{}",
        ],
    )
