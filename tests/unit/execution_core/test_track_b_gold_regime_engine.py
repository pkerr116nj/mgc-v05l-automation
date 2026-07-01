from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_gold_regime_engine import (
    GOLD_REGIME_OUTPUT_DIR,
    LATEST_GOLD_REGIME_JSON,
    LATEST_GOLD_REGIME_MD,
    REGIME_CHOP,
    REGIME_INSUFFICIENT_EVIDENCE,
    REGIME_LONG,
    REGIME_SHORT,
    REGIME_TRANSITION,
    GoldRegimePlugin,
    RegimeEngineContext,
    run_gold_regime_engine,
)


NOW = datetime(2026, 6, 30, 11, 30, tzinfo=UTC)


def test_gold_regime_long_classification_is_explainable() -> None:
    report = _evaluate(_bars(start=3300.0, step=1.5, count=30), _bars(start=3290.0, step=4.0, count=12))

    assert report["regime_label"] == REGIME_LONG
    assert report["directional_bias"] == "BULLISH"
    _assert_explainable(report)
    assert any(row["feature"] == "trend_persistence_1m" for row in report["positive_evidence"])


def test_gold_regime_short_classification_is_explainable() -> None:
    report = _evaluate(_bars(start=3300.0, step=-1.5, count=30), _bars(start=3310.0, step=-4.0, count=12))

    assert report["regime_label"] == REGIME_SHORT
    assert report["directional_bias"] == "BEARISH"
    _assert_explainable(report)
    assert any(row["feature"] == "trend_persistence_1m" for row in report["negative_evidence"])


def test_gold_regime_chop_classification_is_explainable() -> None:
    report = _evaluate(_chop_bars(count=30), _chop_bars(count=12, amplitude=0.35))

    assert report["regime_label"] == REGIME_CHOP
    assert report["directional_bias"] == "NEUTRAL"
    _assert_explainable(report)
    assert any(row["feature"] == "range_chop_index" for row in report["negative_evidence"])


def test_gold_regime_transition_classification_is_explainable() -> None:
    report = _evaluate(_bars(start=3300.0, step=0.8, count=30), _bars(start=3320.0, step=-1.1, count=12))

    assert report["regime_label"] == REGIME_TRANSITION
    assert report["directional_bias"] == "MIXED"
    _assert_explainable(report)
    assert report["conflicting_evidence"]


def test_gold_regime_insufficient_evidence_reports_missing_inputs() -> None:
    report = _evaluate(_bars(start=3300.0, step=1.0, count=3), _bars(start=3300.0, step=1.0, count=2))

    assert report["regime_label"] == REGIME_INSUFFICIENT_EVIDENCE
    assert report["confidence"] == 0
    assert report["missing_evidence"]
    assert report["diagnostic_only"] is True
    assert report["broker_state_mutated"] is False
    assert report["submit_allowed"] is False


def test_gold_regime_reports_missing_future_feature_providers() -> None:
    report = _evaluate(_bars(start=3300.0, step=1.5, count=30), _bars(start=3290.0, step=4.0, count=12))

    assert "VWAP unavailable in GRE MVP" in report["missing_evidence"]
    assert "anchored VWAP unavailable in GRE MVP" in report["missing_evidence"]
    assert 0 <= report["confidence"] <= 100


def test_crfd_vwap_available_removes_vwap_missing_provider() -> None:
    one_minute = _bars(start=3300.0, step=1.5, count=30)
    report = _evaluate(
        one_minute,
        _bars(start=3290.0, step=4.0, count=12),
        analytics_extra={"crfd_rows": [_crfd_row(one_minute[-1], relation="above_vwap", distance=1.2, slope=0.15)]},
    )

    assert "VWAP unavailable in GRE MVP" not in report["missing_evidence"]
    assert "anchored VWAP unavailable in GRE MVP" in report["missing_evidence"]
    assert report["features"]["crfd_vwap"]["available"] is True


def test_crfd_above_vwap_adds_positive_long_evidence() -> None:
    one_minute = _bars(start=3300.0, step=1.5, count=30)
    report = _evaluate(
        one_minute,
        _bars(start=3290.0, step=4.0, count=12),
        analytics_extra={"crfd_rows": [_crfd_row(one_minute[-1], relation="above_vwap", distance=1.2, slope=0.15)]},
    )

    assert any(row["feature"] == "crfd_vwap" for row in report["positive_evidence"])


def test_crfd_below_vwap_adds_positive_short_evidence() -> None:
    one_minute = _bars(start=3300.0, step=-1.5, count=30)
    report = _evaluate(
        one_minute,
        _bars(start=3310.0, step=-4.0, count=12),
        analytics_extra={"crfd_rows": [_crfd_row(one_minute[-1], relation="below_vwap", distance=-1.2, slope=-0.15)]},
    )

    assert any(row["feature"] == "crfd_vwap" for row in report["negative_evidence"])


def test_crfd_vwap_conflict_is_reported() -> None:
    one_minute = _bars(start=3300.0, step=1.5, count=30)
    report = _evaluate(
        one_minute,
        _bars(start=3290.0, step=4.0, count=12),
        analytics_extra={"crfd_rows": [_crfd_row(one_minute[-1], relation="below_vwap", distance=-1.2, slope=-0.15)]},
    )

    assert any(row["feature"] == "crfd_vwap" for row in report["conflicting_evidence"])


def test_crfd_missing_preserves_vwap_missing_provider() -> None:
    report = _evaluate(
        _bars(start=3300.0, step=1.5, count=30),
        _bars(start=3290.0, step=4.0, count=12),
        analytics_extra={"crfd_rows": []},
    )

    assert "VWAP unavailable in GRE MVP" in report["missing_evidence"]
    assert report["features"]["crfd_vwap"]["available"] is False


def test_crfd_vwap_alone_cannot_create_high_confidence_signal() -> None:
    one_minute = _bars(start=3300.0, step=0.0, count=30)
    report = _evaluate(
        one_minute,
        _bars(start=3300.0, step=0.0, count=12),
        analytics_extra={
            "side_session_summary": None,
            "crfd_rows": [_crfd_row(one_minute[-1], relation="above_vwap", distance=0.8, slope=0.1)],
        },
    )

    vwap_rows = [row for row in report["positive_evidence"] if row["feature"] == "crfd_vwap"]
    assert vwap_rows
    assert vwap_rows[0]["points"] <= 2
    assert report["confidence"] < 60


def test_gold_regime_plugin_architecture_is_explicit() -> None:
    report = _evaluate(_bars(start=3300.0, step=1.5, count=30), _bars(start=3290.0, step=4.0, count=12))

    assert report["plugin_id"] == "GRE"
    assert report["plugin_architecture"]["instrument_plugin"] == "GoldRegimePlugin"
    assert report["plugin_architecture"]["gold_hardcoded_in_framework"] is False
    assert {"GRE", "NRE", "ERE", "TRE"}.issubset(set(report["plugin_architecture"]["framework_supports"]))


def test_gold_regime_cli_runner_writes_bounded_json_and_markdown(tmp_path: Path) -> None:
    output_root = tmp_path / "outputs" / "track_b_execution_core"
    _write_phase1(output_root, "GC", "1m", _bars(start=3300.0, step=1.5, count=30))
    _write_phase1(output_root, "GC", "5m", _bars(start=3290.0, step=4.0, count=12))

    report = run_gold_regime_engine(output_root=output_root, now=NOW)
    json_path = output_root / GOLD_REGIME_OUTPUT_DIR / LATEST_GOLD_REGIME_JSON
    md_path = output_root / GOLD_REGIME_OUTPUT_DIR / LATEST_GOLD_REGIME_MD
    written = json.loads(json_path.read_text(encoding="utf-8"))

    assert report["regime_label"] == REGIME_LONG
    assert written["diagnostic_only"] is True
    assert json_path.exists()
    assert md_path.exists()
    assert json_path.stat().st_size < 5 * 1024 * 1024
    assert "Gold Regime Engine" in md_path.read_text(encoding="utf-8")


def test_gold_regime_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_gold_regime_engine.py"),
        Path("src/mgc_v05l/app/track_b_gold_regime_engine.py"),
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
        "managed_exit",
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


def _evaluate(one_minute: list[dict], five_minute: list[dict], *, analytics_extra: dict | None = None) -> dict:
    analytics = {
        "side_session_summary": {"classification": "AVAILABLE"},
        "trend_overlay_summary": {"classification": "AVAILABLE", "path_available_count": 3},
        "trade_pairing_summary": {"paired_trades": 3},
    }
    if analytics_extra:
        analytics.update(analytics_extra)
    context = RegimeEngineContext(
        instrument="GOLD",
        symbols=("GC", "MGC"),
        candles_by_symbol_timeframe={"GC": {"1m": tuple(one_minute), "5m": tuple(five_minute)}},
        source_refs={"test": "synthetic"},
        analytics=analytics,
        generated_at=NOW,
    )
    return GoldRegimePlugin().evaluate(context)


def _bars(*, start: float, step: float, count: int) -> list[dict]:
    rows: list[dict] = []
    for idx in range(count):
        open_price = start + idx * step
        close = open_price + step * 0.8
        high = max(open_price, close) + max(abs(step) * 0.25, 0.2)
        low = min(open_price, close) - max(abs(step) * 0.25, 0.2)
        rows.append(
            {
                "timestamp": NOW - timedelta(minutes=count - idx),
                "open": round(open_price, 4),
                "high": round(high, 4),
                "low": round(low, 4),
                "close": round(close, 4),
                "volume": 100 + idx,
            }
        )
    return rows


def _chop_bars(*, count: int, amplitude: float = 0.45) -> list[dict]:
    rows: list[dict] = []
    base = 3300.0
    for idx in range(count):
        direction = 1 if idx % 2 == 0 else -1
        open_price = base - direction * amplitude
        close = base + direction * amplitude
        rows.append(
            {
                "timestamp": NOW - timedelta(minutes=count - idx),
                "open": round(open_price, 4),
                "high": round(base + amplitude + 0.1, 4),
                "low": round(base - amplitude - 0.1, 4),
                "close": round(close, 4),
                "volume": 100 + idx,
            }
        )
    return rows


def _write_phase1(output_root: Path, symbol: str, timeframe: str, bars: list[dict]) -> None:
    path = output_root / "phase1_runtime_market_data" / symbol / timeframe / "latest_runtime_candles.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized_bars = []
    for row in bars:
        serialized_bars.append(
            {
                **{key: value for key, value in row.items() if key != "timestamp"},
                "bar_end": row["timestamp"].isoformat(),
                "completed": True,
            }
        )
    path.write_text(json.dumps({"bars": serialized_bars, "generated_at": NOW.isoformat()}), encoding="utf-8")


def _crfd_row(candle: dict, *, relation: str, distance: float, slope: float) -> dict:
    observation_time = candle["timestamp"]
    return {
        "schema_version": "track_b_research_feature_dataset_v1",
        "observation_time": observation_time.isoformat(),
        "instrument": "GOLD",
        "contract": "GC",
        "session": "LONDON",
        "session_label": "LONDON_LATE",
        "has_vwap": True,
        "vwap": candle["close"] - distance,
        "distance_from_vwap_points": distance,
        "distance_from_vwap_pct": distance / max(candle["close"], 1.0) * 100.0,
        "vwap_relation": relation,
        "vwap_session": "LONDON",
        "vwap_source_timeframe": "1m",
        "vwap_slope": slope,
        "vwap_reclaim_candidate": relation == "above_vwap",
        "vwap_rejection_candidate": relation == "below_vwap",
        "diagnostic_only": True,
    }


def _assert_explainable(report: dict) -> None:
    assert report["diagnostic_only"] is True
    assert report["explanation"]
    assert isinstance(report["positive_evidence"], list)
    assert isinstance(report["negative_evidence"], list)
    assert isinstance(report["conflicting_evidence"], list)
    assert isinstance(report["missing_evidence"], list)
    assert report["positive_evidence"] or report["negative_evidence"] or report["conflicting_evidence"]
    assert 0 <= report["confidence"] <= 100
    assert report["broker_authority"] is False
    assert report["runtime_authority"] is False
    assert report["managed_exit_authority"] is False
    assert report["strategy_authority"] is False
