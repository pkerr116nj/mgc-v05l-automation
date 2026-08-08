from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.observatory_market_canvas import (
    SCHEMA_VERSION,
    build_market_canvas_snapshot,
    write_snapshot,
)


BASE_TS = datetime(2026, 8, 7, 15, 0, tzinfo=UTC)


def test_builds_partial_real_canvas_from_runtime_candles(tmp_path: Path) -> None:
    root = tmp_path
    _write_candles(root, "NQ", _persistent_candles("bullish"))
    _write_candles(root, "ES", _persistent_candles("bearish", start=5000.0))

    snapshot = build_market_canvas_snapshot(repo_root=root, generated_at=BASE_TS, symbols=("NQ", "ES"))

    assert snapshot["schema_version"] == SCHEMA_VERSION
    assert snapshot["status"] == "VALID_WITH_WARNINGS"
    assert snapshot["dimensions"]["coherence"]["status"] == "VALID"
    assert snapshot["dimensions"]["coherence"]["mapping"] == "DIRECT_NORMALIZATION"
    assert snapshot["dimensions"]["magnitude"]["status"] == "VALID"
    assert snapshot["dimensions"]["edge"]["status"] == "VALID"
    assert snapshot["dimensions"]["spread"]["status"] == "UNKNOWN"
    assert snapshot["dimensions"]["spread"]["mapping"] == "UNSUPPORTED"
    assert snapshot["guardrails"]["display_only"] is True
    assert snapshot["guardrails"]["broker_authority"] is False


def test_spread_uses_factual_breadth_when_available(tmp_path: Path) -> None:
    root = tmp_path
    _write_candles(root, "NQ", _persistent_candles("bullish"))
    breadth_path = root / "outputs" / "track_b_execution_core" / "observatory" / "market_breadth" / "latest_market_breadth.json"
    breadth_path.parent.mkdir(parents=True)
    breadth_path.write_text(
        json.dumps(
            {
                "schema_version": "observatory_market_breadth_v1",
                "status": "READY",
                "metric_kind": "FACTUAL_BREADTH_METRIC",
                "eligible_symbol_count": 4,
                "valid_symbol_count": 4,
                "coverage_ratio": 1.0,
                "percent_advancing": 75.0,
                "percent_declining": 25.0,
            }
        ),
        encoding="utf-8",
    )

    snapshot = build_market_canvas_snapshot(repo_root=root, generated_at=BASE_TS, symbols=("NQ",))

    spread = snapshot["dimensions"]["spread"]
    assert spread["status"] == "VALID"
    assert spread["mapping"] == "DIRECT_NORMALIZATION"
    assert spread["source_ref"] == "observatory_market_breadth_v1"
    assert "FACTUAL_BREADTH_METRIC" in spread["producer_state"]


def test_missing_sources_fail_soft_per_dimension(tmp_path: Path) -> None:
    snapshot = build_market_canvas_snapshot(repo_root=tmp_path, generated_at=BASE_TS, symbols=("NQ",))

    assert snapshot["status"] == "NOT_READY"
    assert snapshot["dimensions"]["coherence"]["status"] == "MISSING"
    assert snapshot["dimensions"]["spread"]["status"] == "UNKNOWN"
    assert snapshot["source_reports"][0]["status"] == "MISSING"


def test_stale_runtime_candles_do_not_emit_valid_dimensions(tmp_path: Path) -> None:
    root = tmp_path
    old = BASE_TS - timedelta(hours=2)
    _write_candles(root, "NQ", _persistent_candles("bullish", end=old))

    snapshot = build_market_canvas_snapshot(repo_root=root, generated_at=BASE_TS, symbols=("NQ",))

    assert snapshot["dimensions"]["coherence"]["status"] == "MISSING"
    assert snapshot["source_reports"][0]["status"] == "STALE"
    assert "STALE_INPUT" in snapshot["source_reports"][0]["confidence_failure_reasons"]


def test_research_paths_are_not_used_as_sources(tmp_path: Path) -> None:
    root = tmp_path
    research_path = root / "outputs" / "track_b_research" / "latest.json"
    research_path.parent.mkdir(parents=True)
    research_path.write_text("{}", encoding="utf-8")
    _write_candles(root, "NQ", _persistent_candles("bullish"))

    snapshot = build_market_canvas_snapshot(repo_root=root, generated_at=BASE_TS, symbols=("NQ",))

    assert all("outputs/track_b_research" not in source for source in snapshot["source_artifacts"])
    assert snapshot["source_reports"][0]["input_source_category"] == "RUNTIME"


def test_snapshot_mjs_output_round_trip(tmp_path: Path) -> None:
    root = tmp_path
    _write_candles(root, "NQ", _persistent_candles("bullish"))
    snapshot = build_market_canvas_snapshot(repo_root=root, generated_at=BASE_TS, symbols=("NQ",))
    output = tmp_path / "market_canvas_snapshot.generated.mjs"

    write_snapshot(snapshot, output)

    text = output.read_text(encoding="utf-8")
    assert text.startswith("export const marketCanvasSnapshot = ")
    parsed = json.loads(text.removeprefix("export const marketCanvasSnapshot = ").removesuffix(";\n"))
    assert parsed["schema_version"] == SCHEMA_VERSION
    assert parsed["guardrails"]["research_authority"] is False


def test_phase1_bar_end_is_adapted_to_classifier_timestamp(tmp_path: Path) -> None:
    root = tmp_path
    bars = _persistent_candles("bullish")
    for row in bars:
        row["bar_end"] = row.pop("timestamp")
    _write_candles(root, "NQ", bars)

    snapshot = build_market_canvas_snapshot(repo_root=root, generated_at=BASE_TS, symbols=("NQ",))

    assert snapshot["source_reports"][0]["status"] == "VALID"
    assert snapshot["dimensions"]["coherence"]["status"] == "VALID"


def test_fingerprints_are_deterministic(tmp_path: Path) -> None:
    root = tmp_path
    _write_candles(root, "NQ", _persistent_candles("bullish"))

    first = build_market_canvas_snapshot(repo_root=root, generated_at=BASE_TS, symbols=("NQ",))
    second = build_market_canvas_snapshot(repo_root=root, generated_at=BASE_TS, symbols=("NQ",))

    assert first["source_fingerprints"] == second["source_fingerprints"]
    assert first["dimensions"] == second["dimensions"]


def test_no_broker_runtime_mutation_imports_or_calls() -> None:
    path = Path("src/mgc_v05l/execution_core/observatory_market_canvas.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.brokers",
        "ibapi",
    )
    forbidden_call_names = {"submit", "cancel", "close", "placeOrder", "flatten", "computeSafeState", "computeReadiness"}
    violations: list[str] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "mgc_v05l.execution" or alias.name.startswith(forbidden_import_roots):
                    violations.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "mgc_v05l.execution" or node.module.startswith(forbidden_import_roots):
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


def _write_candles(root: Path, symbol: str, bars: list[dict]) -> None:
    path = root / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data" / symbol / "5m" / "latest_runtime_candles.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"bars": bars}, indent=2) + "\n", encoding="utf-8")


def _persistent_candles(direction: str, *, start: float = 100.0, end: datetime = BASE_TS - timedelta(minutes=5)) -> list[dict]:
    step = 1.0 if direction == "bullish" else -1.0
    bars: list[dict] = []
    first = end - timedelta(minutes=5 * 23)
    price = start
    for index in range(24):
        timestamp = first + timedelta(minutes=5 * index)
        open_price = price
        close = price + step * 0.7
        high = max(open_price, close) + 0.2
        low = min(open_price, close) - 0.2
        bars.append(
            {
                "timestamp": timestamp.isoformat(),
                "open": round(open_price, 4),
                "high": round(high, 4),
                "low": round(low, 4),
                "close": round(close, 4),
                "volume": 100 + index,
                "completed": True,
                "timeframe": "5m",
            }
        )
        price = close
    return bars
