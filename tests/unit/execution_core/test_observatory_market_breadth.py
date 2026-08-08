from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.observatory_market_breadth import (
    SCHEMA_VERSION,
    BreadthSymbolSpec,
    build_market_breadth,
    write_market_breadth,
)


BASE_TS = datetime(2026, 8, 7, 19, 50, tzinfo=UTC)


def test_builds_factual_breadth_counts(tmp_path: Path) -> None:
    _write_candles(tmp_path, "SPY", 100.0, 101.0)
    _write_candles(tmp_path, "QQQ", 200.0, 198.0)
    _write_candles(tmp_path, "DIA", 300.0, 300.0)

    snapshot = build_market_breadth(
        repo_root=tmp_path,
        generated_at=BASE_TS,
        universe=(
            BreadthSymbolSpec("SPY", "SPY", "SPX_PROXY"),
            BreadthSymbolSpec("QQQ", "QQQ", "NDX_PROXY"),
            BreadthSymbolSpec("DIA", "DIA", "DJIA_PROXY"),
        ),
    )

    assert snapshot["schema_version"] == SCHEMA_VERSION
    assert snapshot["metric_kind"] == "FACTUAL_BREADTH_METRIC"
    assert snapshot["status"] == "READY"
    assert snapshot["eligible_symbol_count"] == 3
    assert snapshot["valid_symbol_count"] == 3
    assert snapshot["advancing_count"] == 1
    assert snapshot["declining_count"] == 1
    assert snapshot["unchanged_count"] == 1
    assert snapshot["percent_advancing"] == 33.333333
    assert snapshot["percent_declining"] == 33.333333
    assert snapshot["guardrails"]["regime_classification"] is False


def test_missing_universe_fails_soft(tmp_path: Path) -> None:
    snapshot = build_market_breadth(repo_root=tmp_path, generated_at=BASE_TS)

    assert snapshot["status"] == "NOT_READY"
    assert snapshot["valid_symbol_count"] == 0
    assert snapshot["coverage_ratio"] == 0.0
    assert {row["status"] for row in snapshot["symbols"]} == {"MISSING_SOURCE"}


def test_partial_coverage_is_warning(tmp_path: Path) -> None:
    _write_candles(tmp_path, "SPY", 100.0, 101.0)

    snapshot = build_market_breadth(
        repo_root=tmp_path,
        generated_at=BASE_TS,
        universe=(BreadthSymbolSpec("SPY", "SPY", "SPX_PROXY"), BreadthSymbolSpec("QQQ", "QQQ", "NDX_PROXY")),
    )

    assert snapshot["status"] == "VALID_WITH_WARNINGS"
    assert snapshot["valid_symbol_count"] == 1
    assert snapshot["coverage_ratio"] == 0.5


def test_freshness_uses_prepared_source_window_for_sparse_bars(tmp_path: Path) -> None:
    _write_candles(
        tmp_path,
        "SPY",
        100.0,
        101.0,
        latest_end=BASE_TS - timedelta(minutes=20),
        source_window_end=BASE_TS - timedelta(minutes=2),
        freshness_seconds=180,
    )

    snapshot = build_market_breadth(
        repo_root=tmp_path,
        generated_at=BASE_TS,
        universe=(BreadthSymbolSpec("SPY", "SPY", "SPX_PROXY"),),
    )

    row = snapshot["symbols"][0]
    assert row["status"] == "VALID"
    assert row["source_timestamp"] == (BASE_TS - timedelta(minutes=20)).isoformat()
    assert row["age_seconds"] == 120.0


def test_json_output_round_trip(tmp_path: Path) -> None:
    snapshot = build_market_breadth(repo_root=tmp_path, generated_at=BASE_TS, universe=())
    output = tmp_path / "breadth.json"

    write_market_breadth(snapshot, output)

    parsed = json.loads(output.read_text(encoding="utf-8"))
    assert parsed["schema_version"] == SCHEMA_VERSION


def test_no_broker_runtime_mutation_imports_or_calls() -> None:
    path = Path("src/mgc_v05l/execution_core/observatory_market_breadth.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    forbidden_import_roots = ("mgc_v05l.execution.", "mgc_v05l.strategy", "mgc_v05l.brokers", "ibapi")
    forbidden_call_names = {"submit", "cancel", "close", "placeOrder", "flatten", "computeSafeState", "computeReadiness"}
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            violations.extend(alias.name for alias in node.names if alias.name.startswith(forbidden_import_roots))
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith(forbidden_import_roots):
            violations.append(node.module)
        elif isinstance(node, ast.Call):
            name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else None
            if name in forbidden_call_names:
                violations.append(name)
    assert violations == []


def _write_candles(
    root: Path,
    symbol: str,
    previous: float,
    latest: float,
    *,
    latest_end: datetime = BASE_TS,
    source_window_end: datetime | None = None,
    freshness_seconds: int = 180,
) -> None:
    previous_end = latest_end - timedelta(minutes=1)
    payload = {
        "bars": [
            {
                "bar_start": (previous_end - timedelta(minutes=1)).isoformat(),
                "bar_end": previous_end.isoformat(),
                "close": previous,
                "completed": True,
            },
            {
                "bar_start": (latest_end - timedelta(minutes=1)).isoformat(),
                "bar_end": latest_end.isoformat(),
                "close": latest,
                "completed": True,
            },
        ],
        "latest_bar_freshness_seconds": freshness_seconds,
        "source_window_end": (source_window_end or latest_end).isoformat(),
    }
    path = root / "outputs" / "track_b_execution_core" / "observatory" / "equity_market_data" / symbol / "1m" / "latest_runtime_candles.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
