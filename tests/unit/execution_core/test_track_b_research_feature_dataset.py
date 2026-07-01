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
    run_research_feature_dataset_builder,
)


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
    assert flags["has_vwap"] is False
    assert flags["has_anchor_vwap"] is False
    assert flags["has_prior_session"] is False
    assert flags["has_overnight_range"] is False
    assert flags["has_opening_range"] is False
    assert flags["has_trend_overlay"] is True
    assert rows[0]["trend_overlay"]["research_reference_only"] is True


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


def test_research_feature_dataset_import_boundary() -> None:
    paths = [
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
