from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_gold_regime_engine import GOLD_REGIME_OUTPUT_DIR
from mgc_v05l.execution_core.track_b_gre_historical_backfill import (
    BACKFILL_ROWS_JSONL,
    generate_backfill_observations,
    run_gre_historical_backfill,
)


START = datetime(2026, 6, 30, 10, 0, tzinfo=UTC)
NOW = datetime(2026, 6, 30, 14, 0, tzinfo=UTC)


def test_backfill_does_not_use_future_candles_for_classification() -> None:
    candles = _candles(count=90, step=0.25)
    rows = generate_backfill_observations(
        candles_by_symbol={"GC": {"1m": candles, "5m": candles[::5]}},
        generated_at=NOW,
        cadence_minutes=15,
        max_observations=20,
        output_root=Path("outputs/track_b_execution_core"),
    )

    assert rows
    for row in rows:
        assert row["lookahead_safe"] is True
        assert row["classification_candle_max_ts"] <= row["gre_generated_at"]


def test_observation_cadence_limits_duplicates() -> None:
    candles = _candles(count=100, step=0.1)
    rows_5m = generate_backfill_observations(
        candles_by_symbol={"GC": {"1m": candles, "5m": candles[::5]}},
        generated_at=NOW,
        cadence_minutes=5,
        max_observations=100,
        output_root=Path("outputs/track_b_execution_core"),
    )
    rows_15m = generate_backfill_observations(
        candles_by_symbol={"GC": {"1m": candles, "5m": candles[::5]}},
        generated_at=NOW,
        cadence_minutes=15,
        max_observations=100,
        output_root=Path("outputs/track_b_execution_core"),
    )

    assert len(rows_5m) > len(rows_15m)
    assert all(row["observation_cadence_minutes"] == 15 for row in rows_15m)


def test_forward_validation_attaches_only_available_horizons() -> None:
    candles = _candles(count=50, step=0.2)
    rows = generate_backfill_observations(
        candles_by_symbol={"GC": {"1m": candles, "5m": candles[::5]}},
        generated_at=NOW,
        cadence_minutes=20,
        max_observations=10,
        output_root=Path("outputs/track_b_execution_core"),
    )

    assert rows
    assert all(row["validation_status"] in {"PARTIAL_FORWARD_DATA", "VALIDATED"} for row in rows)
    assert any(row["missing_horizons"] for row in rows)


def test_insufficient_history_produces_clear_summary_not_failure(tmp_path: Path) -> None:
    output_root = tmp_path / "outputs" / "track_b_execution_core"
    _write_phase1(output_root, "GC", "1m", _candles(count=5, step=0.1))
    _write_phase1(output_root, "GC", "5m", _candles(count=1, step=0.1))

    result = run_gre_historical_backfill(output_root=output_root, now=NOW)

    assert result.summary["observation_count"] == 0
    assert result.summary["coverage_assessment"] == "INSUFFICIENT_RETAINED_HISTORY_FOR_BACKFILL"
    assert result.rows_path.exists()
    assert result.summary_path.exists()


def test_backfilled_rows_are_marked_backfill_and_diagnostic(tmp_path: Path) -> None:
    output_root = tmp_path / "outputs" / "track_b_execution_core"
    candles = _candles(count=90, step=0.2)
    _write_phase1(output_root, "GC", "1m", candles)
    _write_phase1(output_root, "GC", "5m", candles[::5])

    result = run_gre_historical_backfill(output_root=output_root, now=NOW, cadence_minutes=15)
    rows = [json.loads(line) for line in result.rows_path.read_text(encoding="utf-8").splitlines()]

    assert rows
    assert result.rows_path.name == BACKFILL_ROWS_JSONL
    assert all(row["backfill"] is True for row in rows)
    assert all(row["diagnostic_only"] is True for row in rows)
    assert all(row["broker_state_mutated"] is False for row in rows)
    assert result.scorecard["diagnostic_only"] is True
    assert result.analyzer["backfill"] is True


def test_gre_backfill_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_gre_historical_backfill.py"),
        Path("src/mgc_v05l/app/track_b_gre_historical_backfill.py"),
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
