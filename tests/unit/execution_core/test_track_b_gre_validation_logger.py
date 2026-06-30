from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_gold_regime_engine import GOLD_REGIME_OUTPUT_DIR, LATEST_GOLD_REGIME_JSON
from mgc_v05l.execution_core.track_b_gre_validation_logger import (
    PARTIAL_FORWARD_DATA,
    PENDING_FORWARD_DATA,
    VALIDATED,
    build_gre_validation_row,
    run_gre_validation_logger,
)


GRE_TIME = datetime(2026, 6, 30, 11, 0, tzinfo=UTC)
NOW = datetime(2026, 6, 30, 12, 5, tzinfo=UTC)


def test_pending_row_when_no_forward_data_exists() -> None:
    row = build_gre_validation_row(_gre("LONG", "BULLISH"), candles=[], generated_at=NOW, gre_path="gre.json")

    assert row["validation_status"] == PENDING_FORWARD_DATA
    assert row["forward_returns"] == {}
    assert row["direction_correctness"] is None
    assert row["diagnostic_only"] is True
    assert row["broker_state_mutated"] is False


def test_partial_validation_when_only_5m_and_15m_exist() -> None:
    candles = _candles([0, 5, 15], [3300.0, 3302.0, 3304.0])

    row = build_gre_validation_row(_gre("LONG", "BULLISH"), candles=candles, generated_at=NOW, gre_path="gre.json")

    assert row["validation_status"] == PARTIAL_FORWARD_DATA
    assert row["forward_returns"] == {"5m": 2.0, "15m": 4.0}
    assert row["available_horizons"] == ["5m", "15m"]
    assert row["missing_horizons"] == ["30m", "60m"]
    assert row["direction_correctness"] is True


def test_full_validation_with_all_forward_horizons() -> None:
    candles = _candles([0, 5, 15, 30, 60], [3300.0, 3302.0, 3304.0, 3301.0, 3306.0])

    row = build_gre_validation_row(_gre("LONG", "BULLISH"), candles=candles, generated_at=NOW, gre_path="gre.json")

    assert row["validation_status"] == VALIDATED
    assert row["forward_returns"] == {"5m": 2.0, "15m": 4.0, "30m": 1.0, "60m": 6.0}
    assert row["mfe"] is not None
    assert row["mae"] is not None
    assert row["best_observed_holding_window"] == "60m"


def test_short_direction_correctness() -> None:
    candles = _candles([0, 5, 15, 30, 60], [3300.0, 3298.0, 3296.0, 3299.0, 3294.0])

    row = build_gre_validation_row(_gre("SHORT", "BEARISH"), candles=candles, generated_at=NOW, gre_path="gre.json")

    assert row["validation_status"] == VALIDATED
    assert row["direction_correctness"] is True
    assert row["best_observed_holding_window"] == "60m"


def test_chop_and_transition_do_not_force_directional_correctness() -> None:
    candles = _candles([0, 5, 15, 30, 60], [3300.0, 3302.0, 3304.0, 3301.0, 3306.0])

    chop = build_gre_validation_row(_gre("CHOP", "NEUTRAL"), candles=candles, generated_at=NOW, gre_path="gre.json")
    transition = build_gre_validation_row(_gre("TRANSITION", "MIXED"), candles=candles, generated_at=NOW, gre_path="gre.json")

    assert chop["direction_correctness"] is None
    assert chop["best_observed_holding_window"] is None
    assert transition["direction_correctness"] is None
    assert transition["best_observed_holding_window"] is None


def test_validation_logger_writes_bounded_rows_and_summary(tmp_path: Path) -> None:
    output_root = tmp_path / "outputs" / "track_b_execution_core"
    gre_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    gre_dir.mkdir(parents=True)
    (gre_dir / LATEST_GOLD_REGIME_JSON).write_text(json.dumps(_gre("LONG", "BULLISH")), encoding="utf-8")
    _write_phase1(output_root, _candles([0, 5, 15, 30, 60], [3300.0, 3302.0, 3304.0, 3301.0, 3306.0]))

    result = run_gre_validation_logger(output_root=output_root, now=NOW)

    assert result.row["validation_status"] == VALIDATED
    assert result.rows_path.exists()
    assert result.summary_path.exists()
    assert result.markdown_path.exists()
    assert result.rows_path.stat().st_size < 65536
    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert summary["diagnostic_only"] is True
    assert summary["validation_status_counts"] == {VALIDATED: 1}


def test_gre_validation_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_gre_validation_logger.py"),
        Path("src/mgc_v05l/app/track_b_gre_validation_logger.py"),
    ]
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {
        "submit",
        "cancel",
        "placeOrder",
        "create_order_intent",
        "mutate_lifecycle",
        "flatten",
    }
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


def _gre(label: str, bias: str) -> dict:
    return {
        "schema_version": "track_b_gold_regime_engine_v1",
        "generated_at": GRE_TIME.isoformat(),
        "instrument": "GOLD",
        "symbols": ["GC", "MGC"],
        "session_label": "LONDON_LATE",
        "regime_label": label,
        "confidence": 76,
        "directional_bias": bias,
        "diagnostic_only": True,
        "positive_evidence": [{"feature": "trend", "points": 10, "explanation": "trend"}],
        "negative_evidence": [{"feature": "risk", "points": -2, "explanation": "risk"}],
        "conflicting_evidence": [],
        "missing_evidence": ["VWAP unavailable in GRE MVP"],
        "source_refs": {"gre_source": "fixture"},
    }


def _candles(offsets: list[int], closes: list[float]) -> list[dict]:
    rows: list[dict] = []
    for offset, close in zip(offsets, closes):
        rows.append(
            {
                "timestamp": GRE_TIME + timedelta(minutes=offset),
                "open": close - 0.25,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
            }
        )
    return rows


def _write_phase1(output_root: Path, candles: list[dict]) -> None:
    path = output_root / "phase1_runtime_market_data" / "GC" / "1m" / "latest_runtime_candles.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for row in candles:
        rows.append(
            {
                "bar_end": row["timestamp"].isoformat(),
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "completed": True,
            }
        )
    path.write_text(json.dumps({"bars": rows}), encoding="utf-8")
