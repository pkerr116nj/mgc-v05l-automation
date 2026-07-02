from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_market_context import (
    VixProvider,
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
    assert result.rows == [
        {
            "schema_version": "track_b_canonical_market_context_v1",
            "generated_at": "2026-07-03T00:00:00+00:00",
            "provider_name": "VIX",
            "context_key": "vix",
            "symbol": "VIX",
            "vix_available": False,
            "vix_level": None,
            "vix_observation_time": None,
            "vix_staleness_seconds": None,
            "vix_source": str(tmp_path / "missing.jsonl"),
            "vix_unavailable_reason": "missing_vix_source_data",
            "vix_daily_change": None,
            "vix_regime": None,
            "vix_percentile": None,
            "vix_ma_20": None,
            "vix_ma_50": None,
            "data_quality_flags": ["missing_vix_source_data"],
            "source_refs": {"vix_source": str(tmp_path / "missing.jsonl")},
            "diagnostic_only": True,
            "production_effect": False,
        }
    ]


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
