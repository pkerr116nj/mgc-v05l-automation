from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from mgc_v05l.app.track_b_advisory_layer_backfill_pilot import (
    BackfillPilotConfig,
    LAYERS,
    main,
    run_backfill_pilot,
)
from mgc_v05l.research.trend_participation.storage import materialize_parquet_dataset, write_storage_manifest


def test_apply_writes_episode_centric_advisory_rows_and_manifests(tmp_path: Path) -> None:
    candidate_root = tmp_path / "candidate_partitions"
    base_root = tmp_path / "base_1m" / "futures"
    output_root = tmp_path / "advisory"
    _write_symbol_inputs(candidate_root, base_root, "GC")

    result = run_backfill_pilot(
        _config(
            candidate_root=candidate_root,
            base_root=base_root,
            output_root=output_root,
            symbols=("GC",),
            expected_episodes=2,
        )
    )

    assert result["episode_count"] == 2
    assert result["advisory_row_count"] == 2 * 4 * len(LAYERS)
    assert result["validation_summary"]["validation_passed"] is True
    assert result["global_bar_backfill"] is False
    assert len(result["planned_partitions"]) == 1
    assert len(result["written_partitions"]) == 1
    assert len(result["skipped_partitions"]) == 0
    assert result["runtime_activity"] is False
    assert result["strategy_behavior_changed"] is False

    manifest = json.loads(Path(result["output_paths"]["manifest"]).read_text(encoding="utf-8"))
    validation = json.loads(Path(result["validation_summary_path"]).read_text(encoding="utf-8"))
    assert manifest["episode_centric"] is True
    assert manifest["raw_candidate_counts"] == {"GC_2024Q1": 3}
    assert manifest["skipped_by_dedupe"] == {"GC_2024Q1": 1}
    assert validation["advisory_row_count_by_layer"]["lifecycle_awareness"] == 8

    import pyarrow.parquet as pq

    lifecycle_path = (
        output_root
        / "parquet"
        / "layer=lifecycle_awareness"
        / "strategy_family=exact_baseline"
        / "instrument=GC"
        / "year=2024"
        / "Q1"
        / "advisory_rows.parquet"
    )
    rows = pq.ParquetFile(lifecycle_path).read().to_pylist()
    assert len(rows) == 8
    assert {row["layer_name"] for row in rows} == {"lifecycle_awareness"}
    assert all(row["runtime_trade_eligible"] is False for row in rows)
    assert all(row["order_intent_created"] is False for row in rows)


def test_expected_episode_mismatch_stops_on_compatibility_uncertainty(tmp_path: Path) -> None:
    candidate_root = tmp_path / "candidate_partitions"
    base_root = tmp_path / "base_1m" / "futures"
    output_root = tmp_path / "advisory"
    _write_symbol_inputs(candidate_root, base_root, "GC")

    with pytest.raises(SystemExit) as excinfo:
        run_backfill_pilot(
            _config(
                candidate_root=candidate_root,
                base_root=base_root,
                output_root=output_root,
                symbols=("GC",),
                expected_episodes=3,
            )
        )

    assert "EXPECTED_TOTAL_EPISODE_COUNT_MISMATCH" in str(excinfo.value)
    assert "compatibility_uncertain" in str(excinfo.value)
    assert not output_root.exists()


def test_missing_hot_cache_manifest_fails_before_writing(tmp_path: Path) -> None:
    candidate_root = tmp_path / "candidate_partitions"
    base_root = tmp_path / "base_1m" / "futures"
    output_root = tmp_path / "advisory"
    _write_symbol_inputs(candidate_root, base_root, "GC")
    (base_root / "GC" / "2024" / "Q1" / "partition_manifest.json").unlink()

    with pytest.raises(SystemExit) as excinfo:
        run_backfill_pilot(
            _config(
                candidate_root=candidate_root,
                base_root=base_root,
                output_root=output_root,
                symbols=("GC",),
                expected_episodes=2,
            )
        )

    assert "required base 1m partition manifest not found" in str(excinfo.value)
    assert not output_root.exists()


def test_apply_refuses_to_overwrite_existing_outputs(tmp_path: Path) -> None:
    candidate_root = tmp_path / "candidate_partitions"
    base_root = tmp_path / "base_1m" / "futures"
    output_root = tmp_path / "advisory"
    _write_symbol_inputs(candidate_root, base_root, "GC")
    config = _config(
        candidate_root=candidate_root,
        base_root=base_root,
        output_root=output_root,
        symbols=("GC",),
        expected_episodes=2,
    )
    run_backfill_pilot(config)

    with pytest.raises(SystemExit) as excinfo:
        run_backfill_pilot(config)

    assert "REFUSING_TO_OVERWRITE_EXISTING_PARTITION" in str(excinfo.value)


def test_multi_quarter_run_reconciles_and_writes_partitioned_outputs(tmp_path: Path) -> None:
    candidate_root = tmp_path / "candidate_partitions"
    base_root = tmp_path / "base_1m" / "futures"
    output_root = tmp_path / "advisory"
    _write_symbol_inputs(candidate_root, base_root, "GC", year=2024, quarter="Q1", start=datetime(2024, 1, 1, tzinfo=UTC))
    _write_symbol_inputs(candidate_root, base_root, "GC", year=2024, quarter="Q2", start=datetime(2024, 4, 1, tzinfo=UTC))
    expected_path = tmp_path / "expected.json"
    expected_path.write_text(json.dumps({"by_year_quarter": {"2024Q1": {"episode_count": 2}, "2024Q2": 2}}))

    result = run_backfill_pilot(
        BackfillPilotConfig(
            mode="apply",
            symbols=("GC",),
            year=None,
            quarter=None,
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 6, 30, tzinfo=UTC),
            candidate_root=candidate_root,
            base_1m_root=base_root,
            output_root=output_root,
            window_bars=3,
            dedupe_bars=12,
            expected_episodes=4,
            expected_episode_counts_json=expected_path,
            no_delete=True,
            skip_existing=False,
        )
    )

    assert result["episode_count"] == 4
    assert result["observed_quarter_episode_counts"] == {"2024Q1": 2, "2024Q2": 2}
    assert len(result["planned_partitions"]) == 2
    assert len(result["written_partitions"]) == 2
    assert result["validation_summary"]["validation_passed"] is True
    assert (
        output_root
        / "parquet"
        / "layer=regime_session"
        / "strategy_family=exact_baseline"
        / "instrument=GC"
        / "year=2024"
        / "Q2"
        / "advisory_rows.parquet"
    ).exists()


def test_boundary_candidate_reads_next_quarter_bars_but_writes_entry_partition(tmp_path: Path) -> None:
    candidate_root = tmp_path / "candidate_partitions"
    base_root = tmp_path / "base_1m" / "futures"
    output_root = tmp_path / "advisory"
    _write_symbol_inputs(
        candidate_root,
        base_root,
        "GC",
        year=2024,
        quarter="Q1",
        start=datetime(2024, 3, 31, 23, 40, tzinfo=UTC),
        candidate_offsets=(3,),
        bars_count=5,
    )
    _write_symbol_inputs(
        candidate_root,
        base_root,
        "GC",
        year=2024,
        quarter="Q2",
        start=datetime(2024, 4, 1, 0, 5, tzinfo=UTC),
        candidate_offsets=(),
        bars_count=5,
    )

    result = run_backfill_pilot(
        _config(
            candidate_root=candidate_root,
            base_root=base_root,
            output_root=output_root,
            symbols=("GC",),
            expected_episodes=1,
        )
    )

    assert result["episode_count"] == 1
    assert result["observed_quarter_episode_counts"] == {"2024Q1": 1}
    assert result["planned_partitions"][0]["spillover_partitions_read"] == ["GC_2024Q2"]
    assert result["planned_partitions"][0]["episodes_requiring_spillover"] == ["GC:candidate:offset-3"]
    assert (
        output_root
        / "parquet"
        / "layer=lifecycle_awareness"
        / "strategy_family=exact_baseline"
        / "instrument=GC"
        / "year=2024"
        / "Q1"
        / "advisory_rows.parquet"
    ).exists()
    assert not (
        output_root
        / "parquet"
        / "layer=lifecycle_awareness"
        / "strategy_family=exact_baseline"
        / "instrument=GC"
        / "year=2024"
        / "Q2"
        / "advisory_rows.parquet"
    ).exists()

    partition_manifest_path = (
        output_root
        / "partitions"
        / "strategy_family=exact_baseline"
        / "instrument=GC"
        / "year=2024"
        / "Q1"
        / "partition_manifest.json"
    )
    partition_manifest = json.loads(partition_manifest_path.read_text(encoding="utf-8"))
    assert partition_manifest["spillover_partitions_read"] == ["GC_2024Q2"]
    assert partition_manifest["episode_quarter_ownership_rule"] == "ENTRY_DECISION_TIMESTAMP_UTC"
    assert partition_manifest["missing_spillover_failures"] == []


def test_utc_entry_ownership_can_write_to_different_quarter_than_source_shard(tmp_path: Path) -> None:
    candidate_root = tmp_path / "candidate_partitions"
    base_root = tmp_path / "base_1m" / "futures"
    output_root = tmp_path / "advisory"
    _write_symbol_inputs(
        candidate_root,
        base_root,
        "GC",
        year=2024,
        quarter="Q1",
        start=datetime(2024, 4, 1, 0, 5, tzinfo=UTC),
        candidate_offsets=(0,),
        bars_count=5,
    )

    result = run_backfill_pilot(
        _config(
            candidate_root=candidate_root,
            base_root=base_root,
            output_root=output_root,
            symbols=("GC",),
            expected_episodes=1,
        )
    )

    assert result["observed_quarter_episode_counts"] == {"2024Q2": 1}
    assert result["boundary_shifted_episodes"][0]["source_partition_id"] == "GC_2024Q1"
    assert result["boundary_shifted_episodes"][0]["ownership_partition_id"] == "GC_2024Q2"
    assert (
        output_root
        / "parquet"
        / "layer=lifecycle_awareness"
        / "strategy_family=exact_baseline"
        / "instrument=GC"
        / "year=2024"
        / "Q2"
        / "advisory_rows.parquet"
    ).exists()
    assert not (
        output_root
        / "parquet"
        / "layer=lifecycle_awareness"
        / "strategy_family=exact_baseline"
        / "instrument=GC"
        / "year=2024"
        / "Q1"
        / "advisory_rows.parquet"
    ).exists()


def test_total_match_but_quarter_mismatch_fails_with_boundary_diagnostic(tmp_path: Path) -> None:
    candidate_root = tmp_path / "candidate_partitions"
    base_root = tmp_path / "base_1m" / "futures"
    output_root = tmp_path / "advisory"
    _write_symbol_inputs(
        candidate_root,
        base_root,
        "GC",
        year=2024,
        quarter="Q1",
        start=datetime(2024, 4, 1, 0, 5, tzinfo=UTC),
        candidate_offsets=(0,),
        bars_count=5,
    )
    expected_path = tmp_path / "expected.json"
    expected_path.write_text(json.dumps({"by_year_quarter": {"2024Q1": 1}}))

    with pytest.raises(SystemExit) as excinfo:
        run_backfill_pilot(
            BackfillPilotConfig(
                **{
                    **_config(
                        candidate_root=candidate_root,
                        base_root=base_root,
                        output_root=output_root,
                        symbols=("GC",),
                        expected_episodes=1,
                    ).__dict__,
                    "expected_episode_counts_json": expected_path,
                }
            )
        )

    assert "EXPECTED_QUARTER_EPISODE_COUNT_MISMATCH:2024Q1" in str(excinfo.value)
    assert "boundary_shifted_episodes" in str(excinfo.value)
    assert "GC_2024Q2" in str(excinfo.value)
    assert not output_root.exists()


def test_boundary_candidate_missing_next_quarter_fails_clearly(tmp_path: Path) -> None:
    candidate_root = tmp_path / "candidate_partitions"
    base_root = tmp_path / "base_1m" / "futures"
    output_root = tmp_path / "advisory"
    _write_symbol_inputs(
        candidate_root,
        base_root,
        "GC",
        year=2024,
        quarter="Q1",
        start=datetime(2024, 3, 31, 23, 40, tzinfo=UTC),
        candidate_offsets=(3,),
        bars_count=5,
    )

    with pytest.raises(SystemExit) as excinfo:
        run_backfill_pilot(
            _config(
                candidate_root=candidate_root,
                base_root=base_root,
                output_root=output_root,
                symbols=("GC",),
                expected_episodes=1,
            )
        )

    assert "boundary_spillover_missing" in str(excinfo.value)
    assert "BOUNDARY_SPILLOVER_MISSING" in str(excinfo.value)
    assert "expected_window_bars" in str(excinfo.value)
    assert not output_root.exists()


def test_skip_existing_valid_partition_is_validated_and_recorded(tmp_path: Path) -> None:
    candidate_root = tmp_path / "candidate_partitions"
    base_root = tmp_path / "base_1m" / "futures"
    output_root = tmp_path / "advisory"
    _write_symbol_inputs(candidate_root, base_root, "GC")
    config = _config(
        candidate_root=candidate_root,
        base_root=base_root,
        output_root=output_root,
        symbols=("GC",),
        expected_episodes=2,
    )
    first = run_backfill_pilot(config)
    partition_manifest_path = (
        output_root
        / "partitions"
        / "strategy_family=exact_baseline"
        / "instrument=GC"
        / "year=2024"
        / "Q1"
        / "partition_manifest.json"
    )
    before = partition_manifest_path.read_text(encoding="utf-8")

    second = run_backfill_pilot(
        BackfillPilotConfig(
            **{
                **config.__dict__,
                "skip_existing": True,
            }
        )
    )

    assert len(first["written_partitions"]) == 1
    assert len(second["written_partitions"]) == 0
    assert len(second["skipped_partitions"]) == 1
    assert partition_manifest_path.read_text(encoding="utf-8") == before


def test_skip_existing_invalid_partition_fails(tmp_path: Path) -> None:
    candidate_root = tmp_path / "candidate_partitions"
    base_root = tmp_path / "base_1m" / "futures"
    output_root = tmp_path / "advisory"
    _write_symbol_inputs(candidate_root, base_root, "GC")
    config = _config(
        candidate_root=candidate_root,
        base_root=base_root,
        output_root=output_root,
        symbols=("GC",),
        expected_episodes=2,
    )
    run_backfill_pilot(config)
    partition_manifest_path = (
        output_root
        / "partitions"
        / "strategy_family=exact_baseline"
        / "instrument=GC"
        / "year=2024"
        / "Q1"
        / "partition_manifest.json"
    )
    manifest = json.loads(partition_manifest_path.read_text(encoding="utf-8"))
    manifest["episode_count"] = 999
    partition_manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        run_backfill_pilot(BackfillPilotConfig(**{**config.__dict__, "skip_existing": True}))

    assert "invalid_existing_partition" in str(excinfo.value)
    assert "EXISTING_PARTITION_EPISODE_COUNT_MISMATCH" in str(excinfo.value)


def test_quarter_episode_reconciliation_catches_mismatch(tmp_path: Path) -> None:
    candidate_root = tmp_path / "candidate_partitions"
    base_root = tmp_path / "base_1m" / "futures"
    output_root = tmp_path / "advisory"
    _write_symbol_inputs(candidate_root, base_root, "GC")
    expected_path = tmp_path / "expected.json"
    expected_path.write_text(json.dumps({"by_year_quarter": {"2024Q1": {"episode_count": 3}}}))

    with pytest.raises(SystemExit) as excinfo:
        run_backfill_pilot(
            BackfillPilotConfig(
                **{
                    **_config(
                        candidate_root=candidate_root,
                        base_root=base_root,
                        output_root=output_root,
                        symbols=("GC",),
                        expected_episodes=None,
                    ).__dict__,
                    "expected_episode_counts_json": expected_path,
                }
            )
        )

    assert "EXPECTED_QUARTER_EPISODE_COUNT_MISMATCH:2024Q1" in str(excinfo.value)
    assert not output_root.exists()


def test_cli_prints_compact_summary(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    candidate_root = tmp_path / "candidate_partitions"
    base_root = tmp_path / "base_1m" / "futures"
    output_root = tmp_path / "advisory"
    _write_symbol_inputs(candidate_root, base_root, "GC")

    exit_code = main(
        [
            "--mode",
            "dry-run",
            "--symbols",
            "GC",
            "--year",
            "2024",
            "--quarter",
            "Q1",
            "--candidate-root",
            str(candidate_root),
            "--base-1m-root",
            str(base_root),
            "--output-root",
            str(output_root),
            "--window-bars",
            "3",
            "--dedupe-bars",
            "12",
            "--expected-episodes",
            "2",
            "--no-delete",
        ]
    )
    summary = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert summary["episode_count"] == 2
    assert summary["advisory_row_count"] == 32
    assert summary["wrote_outputs"] is False


def test_forbidden_imports_and_authority_calls_absent() -> None:
    path = Path("src/mgc_v05l/app/track_b_advisory_layer_backfill_pilot.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "mgc_v05l.app.main",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {
        "submit",
        "cancel",
        "close",
        "placeOrder",
        "place_order",
        "create_order_intent",
        "mutate_lifecycle",
    }
    violations: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith(forbidden_import_roots):
                    violations.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module.startswith(forbidden_import_roots):
                violations.append(node.module)
        elif isinstance(node, ast.Call):
            call_name: str | None = None
            if isinstance(node.func, ast.Name):
                call_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                call_name = node.func.attr
            if call_name in forbidden_call_names:
                violations.append(call_name)

    assert violations == []


def _config(
    *,
    candidate_root: Path,
    base_root: Path,
    output_root: Path,
    symbols: tuple[str, ...],
    expected_episodes: int | None,
) -> BackfillPilotConfig:
    return BackfillPilotConfig(
        mode="apply",
        symbols=symbols,
        year=2024,
        quarter="Q1",
        start=None,
        end=None,
        candidate_root=candidate_root,
        base_1m_root=base_root,
        output_root=output_root,
        window_bars=3,
        dedupe_bars=12,
        expected_episodes=expected_episodes,
        expected_episode_counts_json=None,
        no_delete=True,
        skip_existing=False,
    )


def _write_symbol_inputs(
    candidate_root: Path,
    base_root: Path,
    symbol: str,
    *,
    year: int = 2024,
    quarter: str = "Q1",
    start: datetime | None = None,
    candidate_offsets: tuple[int, ...] = (0, 4, 13),
    bars_count: int = 20,
) -> None:
    shard = f"{year}{quarter}"
    candidate_path = (
        candidate_root
        / symbol
        / shard
        / "datasets"
        / "lane_candidates"
        / f"symbol={symbol}"
        / f"year={year}"
        / f"shard_id={shard}"
        / "candidates.parquet"
    )
    bars_path = (
        candidate_root
        / symbol
        / shard
        / "datasets"
        / "derived_bars_5m"
        / f"symbol={symbol}"
        / f"year={year}"
        / f"shard_id={shard}"
        / "bars.parquet"
    )
    hot_cache_path = base_root / symbol / str(year) / quarter / "bars.parquet"
    hot_manifest_path = hot_cache_path.parent / "partition_manifest.json"
    start = start or datetime(year, 1, 1, 0, 0, tzinfo=UTC)
    candidates = [
        _candidate(
            symbol,
            start + timedelta(minutes=5 * offset),
            suffix=f"offset-{offset}",
            shard=shard,
        )
        for offset in candidate_offsets
    ]
    derived_bars = [_bar(symbol, start + timedelta(minutes=5 * index), index) for index in range(bars_count)]
    base_bars = [
        {
            "instrument": symbol,
            "timestamp_utc": start + timedelta(minutes=index),
            "open": 100.0 + index * 0.1,
            "high": 100.2 + index * 0.1,
            "low": 99.9 + index * 0.1,
            "close": 100.1 + index * 0.1,
            "volume": 10 + index,
            "data_source": "historical_1m_canonical",
        }
        for index in range(max(20, bars_count * 5))
    ]
    materialize_parquet_dataset(candidate_path, candidates)
    materialize_parquet_dataset(bars_path, derived_bars)
    materialize_parquet_dataset(hot_cache_path, base_bars)
    write_storage_manifest(
        hot_manifest_path,
        {
            "timeframe": "1m",
            "data_source": "historical_1m_canonical",
            "source_sqlite_path": "/tmp/source.sqlite3",
            "row_count": len(base_bars),
        },
    )


def _candidate(symbol: str, decision_ts: datetime, *, suffix: str = "first", shard: str = "2024Q1") -> dict[str, object]:
    return {
        "candidate_id": f"{symbol}:candidate:{suffix}",
        "event_id": f"{symbol}:event:{suffix}",
        "source_event_family": "asiaEarlyNormalBreakoutRetestHoldTurn",
        "lane_id": f"{symbol.lower()}_exact_baseline",
        "strategy_key": f"{symbol.lower()}_exact_baseline",
        "family": "asiaEarlyNormalBreakoutRetestHoldTurn",
        "symbol": symbol,
        "shard_id": shard,
        "candidate_ts": decision_ts,
        "decision_ts": decision_ts,
        "timing_ts": decision_ts,
        "side": "LONG",
        "execution_model": "RESEARCH_SYNTHETIC",
        "eligibility_label": "lane_feature_flag_true",
        "blocker_label": "",
        "feature_bar_id": f"{symbol}:5m:{decision_ts.isoformat()}",
        "timing_bar_id": f"{symbol}:1m:{decision_ts.isoformat()}",
        "materialized_ts": datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        "provenance_tag": "test",
    }


def _bar(symbol: str, bar_ts: datetime, index: int) -> dict[str, object]:
    open_px = 100.0 + index
    return {
        "symbol": symbol,
        "timeframe": "5m",
        "bar_ts": bar_ts,
        "open": open_px,
        "high": open_px + 1.0,
        "low": open_px - 0.5,
        "close": open_px + 0.4,
        "volume": 100 + index,
        "source_data_source": "historical_1m_canonical",
        "derived_rule": "complete_bucket_resample_from_canonical_1m",
        "materialized_from_raw_version": "test",
        "materialized_ts": datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
        "provenance_tag": "test",
    }
