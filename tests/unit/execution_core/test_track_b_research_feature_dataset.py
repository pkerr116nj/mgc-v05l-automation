from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_research_feature_dataset import (
    RESEARCH_FEATURE_DATASET_DIR,
    RESEARCH_FEATURE_DATASET_JSONL,
    build_research_feature_dataset_summary,
    build_research_feature_rows,
    run_crfd_provider_comparison,
    run_research_feature_dataset_builder,
)
from mgc_v05l.execution_core.track_b_canonical_research_data_provider import build_research_data_provider


START = datetime(2026, 6, 30, 10, 0, tzinfo=UTC)
NOW = datetime(2026, 6, 30, 14, 0, tzinfo=UTC)


def test_dataset_row_generation_and_schema() -> None:
    rows = build_research_feature_rows(
        candles_by_symbol={"GC": _candles(count=90, step=0.25)},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=15,
        max_rows=10,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={},
    )

    assert rows
    row = rows[0]
    assert row["schema_version"] == "track_b_research_feature_dataset_v1"
    assert row["instrument"] == "GOLD"
    assert row["contract"] == "GC"
    assert row["timeframe"] == "1m"
    assert row["diagnostic_only"] is True
    assert row["backfill"] is True
    assert row["broker_authority"] is False
    assert row["runtime_authority"] is False
    assert row["strategy_authority"] is False
    assert row["managed_exit_authority"] is False
    assert row["classification_feature_max_ts"] == row["observation_time"]


def test_feature_availability_flags_are_explicit() -> None:
    rows = build_research_feature_rows(
        candles_by_symbol={"MGC": _candles(count=12, step=0.1)},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=5,
        max_rows=10,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={"trend_overlay": {"exists": True, "path": "trend.json"}},
    )

    flags = rows[0]["feature_availability"]
    assert flags["has_vwap"] is True
    assert flags["has_anchor_vwap"] is False
    assert flags["has_prior_session"] is False
    assert flags["has_overnight_range"] is False
    assert flags["has_opening_range"] is False
    assert flags["has_trend_overlay"] is True
    assert rows[0]["trend_overlay"]["research_reference_only"] is True


def test_vwap_cumulative_calculation() -> None:
    candles = (
        _candle(START, open_=100.0, high=101.0, low=99.0, close=100.0, volume=10),
        _candle(START + timedelta(minutes=1), open_=103.0, high=104.0, low=102.0, close=103.0, volume=30),
    )
    rows = build_research_feature_rows(
        candles_by_symbol={"GC": candles},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=1,
        max_rows=10,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={},
    )

    assert rows[0]["vwap"] == 100.0
    assert rows[1]["vwap"] == 102.25
    assert rows[1]["distance_from_vwap_points"] == 0.75
    assert rows[1]["vwap_relation"] == "above_vwap"


def test_vwap_resets_at_session_boundary() -> None:
    candles = (
        _candle(datetime(2026, 6, 30, 12, 19, tzinfo=UTC), open_=100.0, high=101.0, low=99.0, close=100.0, volume=10),
        _candle(datetime(2026, 6, 30, 12, 20, tzinfo=UTC), open_=200.0, high=201.0, low=199.0, close=200.0, volume=10),
    )
    rows = build_research_feature_rows(
        candles_by_symbol={"GC": candles},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=1,
        max_rows=10,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={},
    )

    assert rows[0]["vwap_session"] == "LONDON"
    assert rows[1]["vwap_session"] == "US"
    assert rows[1]["vwap"] == 200.0
    assert rows[1]["vwap_slope"] is None


def test_vwap_does_not_use_future_candles() -> None:
    candles = (
        _candle(START, open_=100.0, high=101.0, low=99.0, close=100.0, volume=10),
        _candle(START + timedelta(minutes=1), open_=1000.0, high=1001.0, low=999.0, close=1000.0, volume=1000),
    )
    rows = build_research_feature_rows(
        candles_by_symbol={"GC": candles},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=1,
        max_rows=10,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={},
    )

    assert rows[0]["vwap"] == 100.0
    assert rows[0]["classification_feature_max_ts"] == rows[0]["observation_time"]
    assert rows[1]["vwap"] > rows[0]["vwap"]


def test_vwap_unavailable_for_zero_or_missing_volume() -> None:
    candles = (
        _candle(START, open_=100.0, high=101.0, low=99.0, close=100.0, volume=0),
        _candle(START + timedelta(minutes=1), open_=101.0, high=102.0, low=100.0, close=101.0, volume=None),
    )
    rows = build_research_feature_rows(
        candles_by_symbol={"GC": candles},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=1,
        max_rows=10,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={},
    )

    assert all(row["has_vwap"] is False for row in rows)
    assert all(row["vwap_relation"] == "unavailable" for row in rows)
    assert all(row["vwap_unavailable_reason"] == "missing_or_zero_volume" for row in rows)
    assert all(row["feature_availability"]["has_vwap"] is False for row in rows)


def test_anchored_vwap_cumulative_calculation() -> None:
    candles = (
        _candle(datetime(2026, 6, 30, 22, 0, tzinfo=UTC), open_=100.0, high=101.0, low=99.0, close=100.0, volume=10),
        _candle(datetime(2026, 6, 30, 22, 1, tzinfo=UTC), open_=103.0, high=104.0, low=102.0, close=103.0, volume=30),
    )
    rows = build_research_feature_rows(
        candles_by_symbol={"GC": candles},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=1,
        max_rows=10,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={},
    )

    row = rows[1]
    assert row["has_avwap_globex_session_open_18et"] is True
    assert row["avwap_globex_session_open_18et"] == 102.25
    assert row["distance_from_avwap_globex_session_open_18et_points"] == 0.75
    assert row["avwap_relation_globex_session_open_18et"] == "above_avwap"
    assert row["anchored_vwap"]["globex_session_open_18et"]["anchor_time"] == "2026-06-30T22:00:00+00:00"


def test_anchored_vwap_resets_at_18et_futures_session_restart() -> None:
    candles = (
        _candle(datetime(2026, 6, 30, 21, 59, tzinfo=UTC), open_=90.0, high=91.0, low=89.0, close=90.0, volume=10),
        _candle(datetime(2026, 6, 30, 22, 0, tzinfo=UTC), open_=100.0, high=101.0, low=99.0, close=100.0, volume=10),
        _candle(datetime(2026, 6, 30, 22, 1, tzinfo=UTC), open_=102.0, high=103.0, low=101.0, close=102.0, volume=10),
    )
    rows = build_research_feature_rows(
        candles_by_symbol={"GC": candles},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=1,
        max_rows=10,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={},
    )

    assert rows[0]["has_avwap_globex_session_open_18et"] is False
    assert rows[0]["avwap_unavailable_reason_globex_session_open_18et"] == "anchor_not_present_in_retained_candles"
    assert rows[1]["avwap_globex_session_open_18et"] == 100.0
    assert rows[2]["avwap_globex_session_open_18et"] == 101.0


def test_london_open_anchor_starts_only_after_anchor_time() -> None:
    candles = (
        _candle(datetime(2026, 6, 30, 6, 59, tzinfo=UTC), open_=99.0, high=100.0, low=98.0, close=99.0, volume=10),
        _candle(datetime(2026, 6, 30, 7, 0, tzinfo=UTC), open_=100.0, high=101.0, low=99.0, close=100.0, volume=10),
    )
    rows = build_research_feature_rows(
        candles_by_symbol={"GC": candles},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=1,
        max_rows=10,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={},
    )

    assert rows[0]["has_avwap_london_open"] is False
    assert rows[0]["avwap_unavailable_reason_london_open"] == "observation_before_anchor"
    assert rows[1]["has_avwap_london_open"] is True
    assert rows[1]["avwap_london_open"] == 100.0


def test_us_rth_open_anchor_starts_only_after_anchor_time() -> None:
    candles = (
        _candle(datetime(2026, 6, 30, 13, 29, tzinfo=UTC), open_=99.0, high=100.0, low=98.0, close=99.0, volume=10),
        _candle(datetime(2026, 6, 30, 13, 30, tzinfo=UTC), open_=100.0, high=101.0, low=99.0, close=100.0, volume=10),
    )
    rows = build_research_feature_rows(
        candles_by_symbol={"GC": candles},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=1,
        max_rows=10,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={},
    )

    assert rows[0]["has_avwap_us_rth_open"] is False
    assert rows[0]["avwap_unavailable_reason_us_rth_open"] == "observation_before_anchor"
    assert rows[1]["has_avwap_us_rth_open"] is True
    assert rows[1]["avwap_us_rth_open"] == 100.0


def test_anchored_vwap_does_not_use_future_candles() -> None:
    candles = (
        _candle(datetime(2026, 6, 30, 22, 0, tzinfo=UTC), open_=100.0, high=101.0, low=99.0, close=100.0, volume=10),
        _candle(datetime(2026, 6, 30, 22, 1, tzinfo=UTC), open_=1000.0, high=1001.0, low=999.0, close=1000.0, volume=1000),
    )
    rows = build_research_feature_rows(
        candles_by_symbol={"GC": candles},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=1,
        max_rows=10,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={},
    )

    assert rows[0]["avwap_globex_session_open_18et"] == 100.0
    assert rows[1]["avwap_globex_session_open_18et"] > rows[0]["avwap_globex_session_open_18et"]


def test_anchored_vwap_unavailable_when_anchor_missing_from_retained_candles() -> None:
    candles = (
        _candle(datetime(2026, 6, 30, 22, 5, tzinfo=UTC), open_=100.0, high=101.0, low=99.0, close=100.0, volume=10),
    )
    rows = build_research_feature_rows(
        candles_by_symbol={"GC": candles},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=1,
        max_rows=10,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={},
    )

    assert rows[0]["has_avwap_globex_session_open_18et"] is False
    assert rows[0]["avwap_unavailable_reason_globex_session_open_18et"] == "anchor_not_present_in_retained_candles"


def test_anchored_vwap_unavailable_for_zero_or_missing_volume() -> None:
    candles = (
        _candle(datetime(2026, 6, 30, 22, 0, tzinfo=UTC), open_=100.0, high=101.0, low=99.0, close=100.0, volume=0),
        _candle(datetime(2026, 6, 30, 22, 1, tzinfo=UTC), open_=101.0, high=102.0, low=100.0, close=101.0, volume=None),
    )
    rows = build_research_feature_rows(
        candles_by_symbol={"GC": candles},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=1,
        max_rows=10,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={},
    )

    assert all(row["has_avwap_globex_session_open_18et"] is False for row in rows)
    assert all(row["avwap_unavailable_reason_globex_session_open_18et"] == "missing_or_zero_volume" for row in rows)


def test_forward_outcome_attachment_only_uses_available_horizons() -> None:
    rows = build_research_feature_rows(
        candles_by_symbol={"GC": _candles(count=70, step=0.5)},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=60,
        max_rows=3,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={},
    )

    first = rows[0]
    second = rows[1]
    assert first["forward_returns"]["5m"] == 2.5
    assert first["forward_returns"]["15m"] == 7.5
    assert first["forward_returns"]["30m"] == 15.0
    assert first["forward_returns"]["60m"] == 30.0
    assert first["mfe"] is not None
    assert first["mae"] is not None
    assert second["forward_returns"]["15m"] is None
    assert "15m" in second["missing_forward_horizons"]


def test_empty_dataset_summary_is_clear() -> None:
    summary = build_research_feature_dataset_summary(
        [],
        candles_by_symbol={},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=5,
        rows_path=Path("rows.jsonl"),
        auxiliary_sources={},
    )

    assert summary["observation_count"] == 0
    assert summary["readiness_for_gre"] == "NO_RESEARCH_FEATURE_ROWS"
    assert summary["time_coverage"]["coverage_minutes"] is None
    assert summary["readiness_for_future_plugins"]["GRE"] == "NOT_READY_INSTRUMENTS_MISSING"


def test_shallow_history_summary_is_diagnostic_only() -> None:
    rows = build_research_feature_rows(
        candles_by_symbol={"GC": _candles(count=20, step=0.1)},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=5,
        max_rows=10,
        backfill=True,
        output_root=Path("outputs/track_b_execution_core"),
        auxiliary_sources={},
    )
    summary = build_research_feature_dataset_summary(
        rows,
        candles_by_symbol={"GC": _candles(count=20, step=0.1)},
        generated_at=NOW,
        timeframe="1m",
        cadence_minutes=5,
        rows_path=Path("rows.jsonl"),
        auxiliary_sources={},
    )

    assert summary["diagnostic_only"] is True
    assert summary["readiness_for_gre"] == "SHALLOW_HISTORY_DIAGNOSTIC_ONLY"
    assert summary["readiness_for_future_plugins"]["GRE"] == "READY_FOR_RESEARCH_INPUT"
    assert summary["vwap_coverage"]["available_count"] == len(rows)
    assert summary["vwap_coverage"]["unavailable_count"] == 0
    assert summary["vwap_coverage"]["relation_counts"]["above_vwap"] > 0
    assert "anchored_vwap_coverage" in summary
    assert "globex_session_open_18et" in summary["anchored_vwap_coverage"]["by_anchor"]


def test_runner_writes_bounded_outputs_and_docs(tmp_path: Path) -> None:
    output_root = tmp_path / "outputs" / "track_b_execution_core"
    _write_phase1(output_root, "GC", "1m", _candles(count=70, step=0.2))

    result = run_research_feature_dataset_builder(output_root=output_root, now=NOW, cadence_minutes=15)
    rows = [json.loads(line) for line in result.rows_path.read_text(encoding="utf-8").splitlines()]

    assert result.rows_path.name == RESEARCH_FEATURE_DATASET_JSONL
    assert str(RESEARCH_FEATURE_DATASET_DIR) in str(result.rows_path)
    assert rows
    assert result.summary["observation_count"] == len(rows)
    assert result.summary_path.exists()
    assert result.summary_markdown_path.exists()
    assert result.schema_path.exists()
    assert result.migration_notes_path.exists()


def test_retained_provider_matches_existing_phase1_candle_behavior(tmp_path: Path) -> None:
    output_root = tmp_path / "outputs" / "track_b_execution_core"
    expected = _candles(count=5, step=0.2)
    _write_phase1(output_root, "GC", "1m", expected)

    provider = build_research_data_provider("retained", output_root=output_root)
    loaded = provider.load_candles(symbols=("GC",), timeframe="1m")["GC"]

    assert provider.provider_metadata()["provider_id"] == "retained"
    assert len(loaded) == len(expected)
    assert loaded[0].timestamp == expected[0]["timestamp"]
    assert loaded[0].close == expected[0]["close"]


def test_parquet_provider_loads_historical_candles(tmp_path: Path) -> None:
    pyarrow = __import__("pyarrow")
    parquet = __import__("pyarrow.parquet").parquet
    store_root = tmp_path / "outputs" / "reports" / "trend_participation_engine"
    path = store_root / "raw_bars" / "databento_minute_backfill" / "symbol=GC" / "year=2026" / "month=07" / "bars.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    parquet.write_table(
        pyarrow.Table.from_pylist(
            [
                {
                    "bar_end": START,
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.0,
                    "close": 10.5,
                    "volume": 100,
                }
            ]
        ),
        path,
    )

    provider = build_research_data_provider("parquet", output_root=tmp_path, research_store_root=store_root)
    loaded = provider.load_candles(symbols=("GC",), timeframe="1m")["GC"]

    assert provider.provider_metadata()["provider_id"] == "parquet"
    assert len(loaded) == 1
    assert loaded[0].timestamp == START
    assert loaded[0].source_ref == str(path)


def test_provider_selection_rejects_unknown_provider(tmp_path: Path) -> None:
    try:
        build_research_data_provider("mystery", output_root=tmp_path)
    except ValueError as exc:
        assert "Unsupported research data provider" in str(exc)
    else:
        raise AssertionError("unsupported provider should fail closed")


def test_identical_candle_windows_produce_identical_crfd_features(tmp_path: Path) -> None:
    pyarrow = __import__("pyarrow")
    parquet = __import__("pyarrow.parquet").parquet
    output_root = tmp_path / "outputs" / "track_b_execution_core"
    store_root = tmp_path / "outputs" / "reports" / "trend_participation_engine"
    candles = _candles(count=20, step=0.25)
    _write_phase1(output_root, "GC", "1m", candles)
    path = store_root / "raw_bars" / "databento_minute_backfill" / "symbol=GC" / "year=2026" / "month=06" / "bars.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    parquet.write_table(
        pyarrow.Table.from_pylist(
            [
                {
                    "bar_end": row["timestamp"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                }
                for row in candles
            ]
        ),
        path,
    )

    retained = run_research_feature_dataset_builder(
        output_root=output_root,
        now=NOW,
        instruments=("GC",),
        cadence_minutes=5,
        max_rows=5,
        provider="retained",
        research_store_root=store_root,
    )
    parquet_result = run_research_feature_dataset_builder(
        output_root=output_root,
        now=NOW,
        instruments=("GC",),
        cadence_minutes=5,
        max_rows=5,
        provider="parquet",
        research_store_root=store_root,
    )

    keys = ("observation_time", "open", "high", "low", "close", "has_vwap", "vwap", "forward_returns")
    assert [{key: row.get(key) for key in keys} for row in retained.rows] == [
        {key: row.get(key) for key in keys} for row in parquet_result.rows
    ]
    assert retained.summary["provider_id"] == "retained"
    assert parquet_result.summary["provider_id"] == "parquet"


def test_provider_comparison_writes_input_only_report(tmp_path: Path) -> None:
    output_root = tmp_path / "outputs" / "track_b_execution_core"
    store_root = tmp_path / "outputs" / "reports" / "trend_participation_engine"
    _write_phase1(output_root, "GC", "1m", _candles(count=8, step=0.1))

    result = run_crfd_provider_comparison(
        output_root=output_root,
        now=NOW,
        instruments=("GC",),
        cadence_minutes=5,
        max_rows=5,
        research_store_root=store_root,
    )

    assert result.json_path.exists()
    assert result.markdown_path.exists()
    assert result.comparison["source_assessment"]["gre_scores_compared"] is False
    assert result.comparison["providers"]["retained"]["provider_id"] == "retained"
    assert result.comparison["providers"]["parquet"]["provider_id"] == "parquet"


def test_research_feature_dataset_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_canonical_research_data_provider.py"),
        Path("src/mgc_v05l/execution_core/track_b_research_feature_dataset.py"),
        Path("src/mgc_v05l/app/track_b_research_feature_dataset.py"),
    ]
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {"submit", "cancel", "placeOrder", "create_order_intent", "mutate_lifecycle", "flatten"}
    violations: list[str] = []
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(forbidden_import_roots):
                        violations.append(f"{path}:{alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith(forbidden_import_roots):
                    violations.append(f"{path}:{node.module}")
            elif isinstance(node, ast.Call):
                call_name: str | None = None
                if isinstance(node.func, ast.Name):
                    call_name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    call_name = node.func.attr
                if call_name in forbidden_call_names:
                    violations.append(f"{path}:{call_name}")
    assert violations == []


def _candles(*, count: int, step: float) -> tuple[dict, ...]:
    rows: list[dict] = []
    for idx in range(count):
        close = 3300.0 + idx * step
        rows.append(
            {
                "timestamp": START + timedelta(minutes=idx),
                "open": close - 0.1,
                "high": close + 0.4,
                "low": close - 0.4,
                "close": close,
                "volume": 100 + idx,
            }
        )
    return tuple(rows)


def _candle(
    timestamp: datetime,
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float | None,
) -> dict:
    return {
        "timestamp": timestamp,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def _write_phase1(output_root: Path, symbol: str, timeframe: str, candles: tuple[dict, ...]) -> None:
    path = output_root / "phase1_runtime_market_data" / symbol / timeframe / "latest_runtime_candles.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "bar_end": row["timestamp"].isoformat(),
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "completed": True,
        }
        for row in candles
    ]
    path.write_text(json.dumps({"bars": rows}), encoding="utf-8")
