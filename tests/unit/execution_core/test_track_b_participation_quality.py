from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.track_b_participation_quality import (
    BEARISH_IMPULSE_DECAYING,
    BEARISH_PARTICIPATION_COLLAPSE,
    BULLISH_CONTINUATION_CONFIRMED,
    BULLISH_HEALTHY_PULLBACK,
    BULLISH_IMPULSE_DECAYING,
    BULLISH_IMPULSE_ONLY,
    BULLISH_PARTICIPATION_COLLAPSE,
    CHOP_BALANCED,
    LOW_CONFIDENCE_THIN_DATA,
    MALFORMED_CANDLES,
    PERSISTENT_BULLISH_PRESSURE,
    PERSISTENT_BEARISH_PRESSURE,
    RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE,
    STALE_INPUT,
    THIN_DATA,
    build_participation_quality_state,
    write_participation_quality_state,
)


BASE_TS = datetime(2026, 5, 8, 14, 0, tzinfo=UTC)


def test_persistent_bullish_pressure() -> None:
    report = _evaluate(_persistent_candles("bullish"))

    assert report["participation_state"] == PERSISTENT_BULLISH_PRESSURE
    assert report["long_hold_quality"] == "SUPPORTIVE"
    assert report["short_hold_quality"] == "HOSTILE"
    assert report["long_exit_urgency_context"] == "LOW"
    assert report["runtime_trade_eligible"] is False
    assert report["confidence_failure_reasons"] == []


def test_persistent_bearish_pressure() -> None:
    report = _evaluate(_persistent_candles("bearish"))

    assert report["participation_state"] == PERSISTENT_BEARISH_PRESSURE
    assert report["short_hold_quality"] == "SUPPORTIVE"
    assert report["long_hold_quality"] == "HOSTILE"
    assert report["short_exit_urgency_context"] == "LOW"


def test_impulse_only() -> None:
    report = _evaluate(_impulse_only_candles("bullish"))

    assert report["participation_state"] == BULLISH_IMPULSE_ONLY
    assert report["feature_summary"]["impulse_direction"] == "BULLISH"
    assert report["long_hold_quality"] == "SUPPORTIVE"


def test_impulse_decaying() -> None:
    bullish = _evaluate(_impulse_decaying_candles("bullish"))
    bearish = _evaluate(_impulse_decaying_candles("bearish"))

    assert bullish["participation_state"] == BULLISH_IMPULSE_DECAYING
    assert bullish["long_hold_quality"] == "DEGRADING"
    assert bullish["long_exit_urgency_context"] == "ELEVATED"
    assert bearish["participation_state"] == BEARISH_IMPULSE_DECAYING
    assert bearish["short_hold_quality"] == "DEGRADING"


def test_healthy_pullback() -> None:
    report = _evaluate(_healthy_pullback_candles("bullish"))

    assert report["participation_state"] == BULLISH_HEALTHY_PULLBACK
    assert report["pullback_health"] == "HEALTHY"
    assert report["long_exit_urgency_context"] == "LOW"
    assert 0.12 <= report["feature_summary"]["pullback_depth"] <= 0.58


def test_continuation_confirmed() -> None:
    report = _evaluate(_continuation_confirmed_candles("bullish"))

    assert report["participation_state"] == BULLISH_CONTINUATION_CONFIRMED
    assert report["pullback_health"] == "HEALTHY"
    assert report["continuation_confidence"] >= 0.60


def test_participation_collapse() -> None:
    bullish = _evaluate(_collapse_candles("bullish"))
    bearish = _evaluate(_collapse_candles("bearish"))

    assert bullish["participation_state"] == BULLISH_PARTICIPATION_COLLAPSE
    assert bullish["long_hold_quality"] == "HOSTILE"
    assert bullish["long_exit_urgency_context"] == "HIGH"
    assert bearish["participation_state"] == BEARISH_PARTICIPATION_COLLAPSE
    assert bearish["short_hold_quality"] == "HOSTILE"


def test_chop_balanced() -> None:
    report = _evaluate(_chop_candles())

    assert report["participation_state"] == CHOP_BALANCED
    assert report["long_hold_quality"] == "NEUTRAL"
    assert report["short_hold_quality"] == "NEUTRAL"


def test_thin_data_fails_closed() -> None:
    report = _evaluate(_persistent_candles("bullish")[:6])

    assert report["participation_state"] == LOW_CONFIDENCE_THIN_DATA
    assert THIN_DATA in report["confidence_failure_reasons"]
    assert report["confidence"] == 0.0


def test_stale_input_fails_closed() -> None:
    candles = _persistent_candles("bullish")
    report = build_participation_quality_state(
        _payload(candles),
        now=_timestamp(candles[-1]) + timedelta(hours=1),
    )

    assert report["participation_state"] == LOW_CONFIDENCE_THIN_DATA
    assert STALE_INPUT in report["confidence_failure_reasons"]
    assert report["freshness_status"] == "STALE"


def test_research_source_rejected_as_runtime_truth() -> None:
    payload = _payload(_persistent_candles("bullish"))
    payload["input_source_path"] = "outputs/track_b_research/snapshots/latest_decision_bar_snapshots.jsonl"
    payload["input_source_category"] = "RESEARCH"

    report = build_participation_quality_state(
        payload,
        now=_now(payload["candles"]),
        input_mode="RUNTIME_DECISION",
    )

    assert report["participation_state"] == LOW_CONFIDENCE_THIN_DATA
    assert RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE in report["confidence_failure_reasons"]
    assert report["runtime_trade_eligible"] is False


def test_incomplete_candles_excluded_when_completed_lookback_remains_valid() -> None:
    candles = _persistent_candles("bullish")
    forming = dict(candles[-1])
    forming["timestamp"] = (_timestamp(candles[-1]) + timedelta(minutes=5)).isoformat()
    forming["completed"] = False

    report = _evaluate([*candles, forming])

    assert report["participation_state"] == PERSISTENT_BULLISH_PRESSURE
    assert report["candles_received"] == 13
    assert report["completed_candles_used"] == 12
    assert report["confidence_failure_reasons"] == []
    assert any("excluded 1 incomplete" in warning for warning in report["warnings"])


def test_malformed_candles_fail_closed() -> None:
    candles = _persistent_candles("bullish")
    del candles[3]["high"]

    report = _evaluate(candles)

    assert report["participation_state"] == LOW_CONFIDENCE_THIN_DATA
    assert MALFORMED_CANDLES in report["confidence_failure_reasons"]


def test_safety_flags_always_false() -> None:
    report = _evaluate(_persistent_candles("bullish"))

    for field in (
        "strategy_authority",
        "runtime_trade_eligible",
        "runtime_eligible",
        "broker_state_mutated",
        "submit_attempted",
        "cancel_attempted",
        "close_attempted",
        "place_order_attempted",
        "direct_trade_command",
        "live_money_eligible",
        "live_money_readiness",
    ):
        assert report[field] is False


def test_no_broker_mutation_symbols_or_imports() -> None:
    path = Path("src/mgc_v05l/execution_core/track_b_participation_quality.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    forbidden_import_roots = (
        "mgc_v05l.execution",
        "mgc_v05l.strategy",
        "mgc_v05l.app",
        "ibapi",
    )
    forbidden_call_names = {"submit", "cancel", "close", "placeOrder", "flatten"}
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


def test_artifact_write_to_tmp_path(tmp_path: Path) -> None:
    output_path = tmp_path / "latest_participation_quality_state.json"

    report = write_participation_quality_state(
        _payload(_persistent_candles("bullish")),
        output_path=output_path,
        now=BASE_TS + timedelta(minutes=60),
    )
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert output_path.exists()
    assert report["artifact_path"] == str(output_path.resolve())
    assert written["artifact_path"] == str(output_path.resolve())
    assert written["participation_state"] == PERSISTENT_BULLISH_PRESSURE
    assert written["runtime_trade_eligible"] is False


def _evaluate(candles: list[dict[str, Any]]) -> dict[str, Any]:
    return build_participation_quality_state(_payload(candles), now=_now(candles))


def _payload(candles: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "instrument": "MGC",
        "candle_timeframe": "5m",
        "candles": candles,
        "input_source_path": "outputs/track_b_execution_core/runtime/mgc_5m_latest.json",
        "input_source_category": "RUNTIME",
        "input_mode": "RUNTIME_DECISION",
        "runtime_provenance": {"source_id": "unit-runtime-candles"},
    }


def _now(candles: list[dict[str, Any]]) -> datetime:
    return _timestamp(candles[-1]) + timedelta(minutes=5)


def _timestamp(candle: dict[str, Any]) -> datetime:
    return datetime.fromisoformat(str(candle["timestamp"]).replace("Z", "+00:00")).astimezone(UTC)


def _persistent_candles(side: str) -> list[dict[str, Any]]:
    candles: list[dict[str, Any]] = []
    price = 100.0
    for index in range(12):
        if side == "bullish":
            open_price = price
            close = price + 0.70
            low = open_price - 0.05
            high = close + 0.05
        else:
            open_price = price
            close = price - 0.70
            high = open_price + 0.05
            low = close - 0.05
        candles.append(_candle(index, open_price, high, low, close))
        price = close
    return candles


def _impulse_only_candles(side: str) -> list[dict[str, Any]]:
    rows = _balanced_rows(11)
    if side == "bullish":
        rows.append((100.02, 101.45, 99.96, 101.35))
    else:
        rows.append((100.02, 100.08, 98.55, 98.65))
    return [_candle(index, *row) for index, row in enumerate(rows)]


def _impulse_decaying_candles(side: str) -> list[dict[str, Any]]:
    if side == "bullish":
        rows = [
            *_balanced_rows(6),
            (100.00, 101.45, 99.95, 101.35),
            (101.35, 101.42, 101.20, 101.34),
            (101.34, 101.40, 101.18, 101.32),
            (101.32, 101.38, 101.16, 101.30),
            (101.30, 101.36, 101.15, 101.29),
            (101.29, 101.35, 101.14, 101.28),
        ]
    else:
        rows = [
            *_balanced_rows(6),
            (100.00, 100.05, 98.55, 98.65),
            (98.65, 98.80, 98.60, 98.66),
            (98.66, 98.82, 98.61, 98.68),
            (98.68, 98.84, 98.62, 98.70),
            (98.70, 98.86, 98.63, 98.71),
            (98.71, 98.87, 98.64, 98.72),
        ]
    return [_candle(index, *row) for index, row in enumerate(rows)]


def _healthy_pullback_candles(side: str) -> list[dict[str, Any]]:
    if side == "bullish":
        rows = [
            *_balanced_rows(4),
            (100.00, 101.10, 99.95, 101.00),
            (101.00, 102.10, 100.95, 102.00),
            (102.00, 103.10, 101.95, 103.00),
            (103.00, 103.25, 102.50, 102.70),
            (102.70, 102.95, 102.35, 102.55),
            (102.55, 102.85, 102.20, 102.45),
            (102.45, 102.75, 102.25, 102.55),
            (102.55, 102.90, 102.40, 102.75),
        ]
    else:
        raise AssertionError("bearish healthy pullback fixture not needed in this slice")
    return [_candle(index, *row) for index, row in enumerate(rows)]


def _continuation_confirmed_candles(side: str) -> list[dict[str, Any]]:
    if side == "bullish":
        rows = [
            *_balanced_rows(3),
            (100.00, 101.10, 99.95, 101.00),
            (101.00, 102.10, 100.95, 102.00),
            (102.00, 103.10, 101.95, 103.00),
            (103.00, 103.20, 102.30, 102.50),
            (102.50, 102.80, 102.15, 102.35),
            (102.35, 102.80, 102.30, 102.70),
            (102.70, 103.15, 102.60, 103.05),
            (103.05, 103.55, 102.95, 103.45),
            (103.45, 103.90, 103.35, 103.80),
        ]
    else:
        raise AssertionError("bearish continuation fixture not needed in this slice")
    return [_candle(index, *row) for index, row in enumerate(rows)]


def _collapse_candles(side: str) -> list[dict[str, Any]]:
    if side == "bullish":
        rows = [
            *_balanced_rows(4),
            (100.00, 101.10, 99.95, 101.00),
            (101.00, 102.10, 100.95, 102.00),
            (102.00, 103.10, 101.95, 103.00),
            (103.00, 103.05, 101.85, 102.00),
            (102.00, 102.10, 100.85, 101.00),
            (101.00, 101.10, 99.85, 100.00),
            (100.00, 100.15, 99.40, 99.55),
            (99.55, 99.75, 99.05, 99.15),
        ]
    else:
        rows = [
            *_balanced_rows(4),
            (100.00, 100.05, 98.90, 99.00),
            (99.00, 99.05, 97.90, 98.00),
            (98.00, 98.05, 96.90, 97.00),
            (97.00, 98.15, 96.95, 98.00),
            (98.00, 99.15, 97.90, 99.00),
            (99.00, 100.15, 98.90, 100.00),
            (100.00, 100.75, 99.85, 100.55),
            (100.55, 101.05, 100.35, 100.95),
        ]
    return [_candle(index, *row) for index, row in enumerate(rows)]


def _chop_candles() -> list[dict[str, Any]]:
    rows = []
    price = 100.0
    for index in range(12):
        if index % 2 == 0:
            open_price = price
            close = price + 0.65
            high = close + 0.10
            low = open_price - 0.10
        else:
            open_price = price
            close = price - 0.65
            high = open_price + 0.10
            low = close - 0.10
        rows.append((open_price, high, low, close))
        price = close
    return [_candle(index, *row) for index, row in enumerate(rows)]


def _balanced_rows(count: int) -> list[tuple[float, float, float, float]]:
    rows = []
    for index in range(count):
        open_price = 100.0 + (0.01 if index % 2 == 0 else -0.01)
        close = 100.0 - (0.01 if index % 2 == 0 else -0.01)
        rows.append((open_price, max(open_price, close) + 0.45, min(open_price, close) - 0.45, close))
    return rows


def _candle(index: int, open_price: float, high: float, low: float, close: float) -> dict[str, Any]:
    return {
        "timestamp": (BASE_TS + timedelta(minutes=5 * index)).isoformat(),
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": 100 + index,
        "timeframe": "5m",
        "completed": True,
    }
