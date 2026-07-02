from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_market_context import (
    HistoricalVixProvider,
    VixProvider,
    audit_historical_vix_sources,
    build_canonical_market_context_summary,
    classify_vix_regime,
    run_canonical_market_context,
)


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def test_vix_provider_with_available_data_publishes_context(tmp_path: Path) -> None:
    source = tmp_path / "vix.jsonl"
    _write_jsonl(
        source,
        [
            {"vix_asof_ts": "2026-07-01T21:15:00+00:00", "vix_close": 14.0},
            {"vix_asof_ts": "2026-07-02T21:15:00+00:00", "vix_close": 18.0},
        ],
    )

    result = run_canonical_market_context(
        vix_source_path=source,
        output_dir=tmp_path / "out",
        now="2026-07-03T00:00:00+00:00",
    )

    assert result.summary["vix"]["available"] is True
    assert result.summary["vix"]["row_count"] == 2
    assert result.rows[-1]["vix_level"] == 18.0
    assert result.rows[-1]["vix_regime"] == "NORMAL"
    assert result.context_path.exists()
    assert result.summary_path.exists()


def test_vix_provider_unavailable_still_publishes_clean_summary(tmp_path: Path) -> None:
    result = run_canonical_market_context(
        vix_source_path=tmp_path / "missing.jsonl",
        output_dir=tmp_path / "out",
        now="2026-07-03T00:00:00+00:00",
    )

    assert result.summary["vix"]["available"] is False
    assert result.summary["vix"]["join_readiness"] == "MISSING_SOURCE_DATA"
    row = result.rows[0]
    assert row["provider_kind"] == "historical_vix"
    assert row["vix_available"] is False
    assert row["vix_source"] == str(tmp_path / "missing.jsonl")
    assert row["vix_unavailable_reason"] == "missing_vix_source_data"
    assert row["source_provenance"]["source_ref"] == str(tmp_path / "missing.jsonl")
    assert row["diagnostic_only"] is True
    assert row["production_effect"] is False


def test_timestamp_join_uses_nearest_prior_not_future_observation(tmp_path: Path) -> None:
    source = tmp_path / "vix.jsonl"
    _write_jsonl(
        source,
        [
            {"vix_asof_ts": "2026-07-01T21:15:00+00:00", "vix_close": 16.0},
            {"vix_asof_ts": "2026-07-02T21:15:00+00:00", "vix_close": 22.0},
        ],
    )
    provider = VixProvider(vix_source_path=source)

    joined = provider.join_at(datetime(2026, 7, 2, 12, 0, tzinfo=UTC))

    assert joined["vix_available"] is True
    assert joined["vix_level"] == 16.0
    assert joined["vix_observation_time"] == "2026-07-01T21:15:00+00:00"


def test_staleness_seconds_calculated_from_join_target(tmp_path: Path) -> None:
    source = tmp_path / "vix.jsonl"
    _write_jsonl(source, [{"vix_asof_ts": "2026-07-02T21:15:00+00:00", "vix_close": 18.0}])
    provider = VixProvider(vix_source_path=source)

    joined = provider.join_at(datetime(2026, 7, 2, 21, 20, tzinfo=UTC))

    assert joined["vix_staleness_seconds"] == 300


def test_vix_regime_classification_thresholds() -> None:
    assert classify_vix_regime(None) is None
    assert classify_vix_regime(12.0) == "LOW"
    assert classify_vix_regime(18.0) == "NORMAL"
    assert classify_vix_regime(25.0) == "ELEVATED"
    assert classify_vix_regime(35.0) == "EXTREME"


def test_missing_optional_vix_fields_do_not_fail(tmp_path: Path) -> None:
    source = tmp_path / "vix.csv"
    source.write_text("vix_asof_ts,vix_close,vix_level_bucket\n2026-07-02T21:15:00+00:00,18.5,MID\n", encoding="utf-8")

    provider = VixProvider(vix_source_path=source)
    rows = provider.context_rows(generated_at=datetime(2026, 7, 3, tzinfo=UTC))

    assert rows[0]["vix_available"] is True
    assert rows[0]["vix_regime"] == "NORMAL"
    assert rows[0]["vix_daily_change"] is None
    assert rows[0]["vix_ma_20"] is None
    assert rows[0]["vix_ma_50"] is None


def test_historical_vix_provider_reports_source_audit_and_coverage(tmp_path: Path) -> None:
    warehouse = tmp_path / "warehouse"
    source = warehouse / "datasets" / "vix_daily" / "vix.jsonl"
    _write_jsonl(
        source,
        [
            {"vix_trade_date": "2026-07-01", "vix_close": 17.0},
            {"vix_trade_date": "2026-07-02", "vix_close": 22.0},
        ],
    )
    provider = HistoricalVixProvider(warehouse_root=warehouse)

    coverage = provider.coverage_report()
    freshness = provider.freshness_report(generated_at=datetime(2026, 7, 3, 0, 0, tzinfo=UTC))

    assert coverage["available"] is True
    assert coverage["row_count"] == 2
    assert coverage["source_ref"].endswith("datasets/vix_daily")
    assert any(item["source_name"] == "warehouse_vix_daily" and item["exists"] for item in coverage["source_candidates"])
    assert freshness["freshness_available"] is True


def test_vix_daily_change_and_percentile_ma_derivation(tmp_path: Path) -> None:
    source = tmp_path / "vix.jsonl"
    rows = [
        {"vix_asof_ts": f"2026-06-{day:02d}T21:15:00+00:00", "vix_close": float(day)}
        for day in range(1, 22)
    ]
    _write_jsonl(source, rows)
    provider = HistoricalVixProvider(vix_source_path=source)

    context_rows = provider.context_rows(generated_at=datetime(2026, 6, 22, tzinfo=UTC))

    assert context_rows[1]["vix_daily_change"] == 1.0
    assert context_rows[18]["vix_ma_20"] is None
    assert context_rows[19]["vix_ma_20"] == 10.5
    assert context_rows[19]["vix_percentile"] == 1.0
    assert context_rows[19]["vix_ma_50"] is None


def test_historical_provider_reports_are_published(tmp_path: Path) -> None:
    source = tmp_path / "vix.csv"
    source.write_text("vix_asof_ts,vix_close\n2026-07-02T21:15:00+00:00,18.5\n", encoding="utf-8")

    result = run_canonical_market_context(
        vix_source_path=source,
        output_dir=tmp_path / "out",
        now="2026-07-03T00:00:00+00:00",
    )

    assert result.historical_provider_report_path.exists()
    assert result.vix_historical_data_quality_path.exists()
    assert "HistoricalVixProvider" in result.historical_provider_report_path.read_text(encoding="utf-8")


def test_historical_vix_source_audit_marks_missing_sources(tmp_path: Path) -> None:
    audit = audit_historical_vix_sources(warehouse_root=tmp_path / "warehouse")

    assert [row["source_name"] for row in audit] == ["warehouse_vol_regime_daily", "warehouse_vix_daily"]
    assert all(row["exists"] is False for row in audit)


def test_empty_provider_set_is_handled_safely() -> None:
    summary = build_canonical_market_context_summary(
        [],
        rows=[],
        generated_at=datetime(2026, 7, 3, tzinfo=UTC),
    )

    assert summary["provider_count"] == 0
    assert summary["row_count"] == 0
    assert summary["vix"]["available"] is False


def test_module_does_not_import_broker_runtime_or_strategy_mutation_paths() -> None:
    path = Path("src/mgc_v05l/execution_core/track_b_canonical_market_context.py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)

    forbidden = {
        "ib_insync",
        "mgc_v05l.execution.ibkr_paper_strategy_bridge",
        "mgc_v05l.app.probationary_runtime",
        "mgc_v05l.execution_core.track_b_managed_exit",
    }
    assert imported.isdisjoint(forbidden)
