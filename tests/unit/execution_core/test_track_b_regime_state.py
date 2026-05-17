from __future__ import annotations

import ast
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_regime_state import (
    CHOP_BALANCED,
    COMPRESSION_COILING,
    DIRECTIONAL_BALANCED,
    DIRECTIONAL_BULLISH,
    EXPANSION_DIRECTIONAL,
    INCOMPLETE_CANDLES,
    LIQUIDITY_NORMAL,
    LIQUIDITY_STALE_OR_INCOMPLETE,
    LOW_CONFIDENCE_TREND_CHOP,
    MIXED_TIMEFRAME,
    RANGE_COMPRESSED,
    RANGE_EXPANDED,
    REGIME_CHOP_BALANCED,
    REGIME_COMPRESSION,
    REGIME_EXPANSION,
    REGIME_THIN_OR_STALE,
    STALE_INPUT,
    THIN_DATA,
    VOL_HIGH,
    VOL_LOW,
    build_regime_session_context_state,
)


BASE_TS = datetime(2026, 5, 8, 14, 0, tzinfo=UTC)


def test_trending_expansion_classification() -> None:
    report = _evaluate(_trending_expansion_candles())

    assert report["schema_version"] == "track_b_regime_session_context_v1"
    assert report["authority_mode"] == "ADVISORY_REGIME_CONTEXT_ONLY"
    assert report["session_bucket"].startswith("SESSION_")
    assert report["market_regime_state"] == REGIME_EXPANSION
    assert report["volatility_range_state"] == RANGE_EXPANDED
    assert report["trend_chop_state"] == EXPANSION_DIRECTIONAL
    assert report["liquidity_state"] == LIQUIDITY_NORMAL
    assert report["directional_context"] == DIRECTIONAL_BULLISH
    assert report["confidence"] > 0.50
    assert report["failure_reasons"] == []


def test_chop_balanced_classification() -> None:
    report = _evaluate(_chop_candles())

    assert report["market_regime_state"] == REGIME_CHOP_BALANCED
    assert report["trend_chop_state"] == CHOP_BALANCED
    assert report["directional_context"] == DIRECTIONAL_BALANCED
    assert report["failure_reasons"] == []


def test_compression_classification() -> None:
    report = _evaluate(_compression_candles())

    assert report["market_regime_state"] == REGIME_COMPRESSION
    assert report["volatility_range_state"] == RANGE_COMPRESSED
    assert report["volatility_state"] == VOL_LOW
    assert report["trend_chop_state"] == COMPRESSION_COILING


def test_high_vol_range_expanded_classification() -> None:
    report = _evaluate(_high_vol_expansion_candles())

    assert report["market_regime_state"] == REGIME_EXPANSION
    assert report["volatility_range_state"] == RANGE_EXPANDED
    assert report["volatility_state"] == VOL_HIGH
    assert report["feature_summary"]["range_expansion_ratio"] >= 1.65


def test_thin_and_stale_fail_closed() -> None:
    thin = _evaluate(_trending_expansion_candles()[:4])
    candles = _trending_expansion_candles()
    stale = build_regime_session_context_state(_payload(candles), now=_timestamp(candles[-1]) + timedelta(hours=1))

    for report in (thin, stale):
        assert report["market_regime_state"] in {REGIME_THIN_OR_STALE, "REGIME_LOW_CONFIDENCE"}
        assert report["trend_chop_state"] == LOW_CONFIDENCE_TREND_CHOP
        assert report["runtime_trade_eligible"] is False
        assert report["strategy_authority"] is False

    assert THIN_DATA in thin["failure_reasons"]
    assert STALE_INPUT in stale["failure_reasons"]
    assert stale["liquidity_state"] == LIQUIDITY_STALE_OR_INCOMPLETE


def test_mixed_and_incomplete_timeframe_fail_closed() -> None:
    mixed_candles = _trending_expansion_candles()
    mixed_candles[3]["timeframe"] = "1m"
    mixed = _evaluate(mixed_candles)

    incomplete_candles = _trending_expansion_candles()
    incomplete_candles[-1]["completed"] = False
    incomplete = _evaluate(incomplete_candles)

    for report in (mixed, incomplete):
        assert report["trend_chop_state"] == LOW_CONFIDENCE_TREND_CHOP
        assert report["volatility_range_state"] == "RANGE_THIN_OR_INVALID"
        assert report["runtime_trade_eligible"] is False

    assert MIXED_TIMEFRAME in mixed["failure_reasons"]
    assert INCOMPLETE_CANDLES in incomplete["failure_reasons"]


def test_missing_provenance_fails_closed() -> None:
    payload = _payload(_trending_expansion_candles())
    payload.pop("input_source_path")
    payload.pop("input_source_category")
    payload.pop("runtime_provenance")

    report = build_regime_session_context_state(payload, now=_now(payload["candles"]))

    assert report["market_regime_state"] == "REGIME_LOW_CONFIDENCE"
    assert "MISSING_PROVENANCE" in report["failure_reasons"]
    assert report["runtime_trade_eligible"] is False


def test_safety_flags_always_false() -> None:
    reports = [
        _evaluate(_trending_expansion_candles()),
        _evaluate(_chop_candles()),
        _evaluate(_trending_expansion_candles()[:4]),
    ]

    for report in reports:
        for field in (
            "strategy_authority",
            "broker_state_mutated",
            "submit_attempted",
            "order_intent_created",
            "lifecycle_mutated",
            "runtime_trade_eligible",
        ):
            assert report[field] is False


def test_no_broker_runtime_order_lifecycle_imports_or_calls() -> None:
    path = Path("src/mgc_v05l/execution_core/track_b_regime_state.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    forbidden_import_roots = (
        "mgc_v05l.execution",
        "mgc_v05l.strategy",
        "mgc_v05l.app",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {
        "submit",
        "cancel",
        "close",
        "placeOrder",
        "flatten",
        "create_order_intent",
        "mutate_lifecycle",
    }
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


def _evaluate(candles: list[dict[str, Any]]) -> dict[str, Any]:
    return build_regime_session_context_state(_payload(candles), now=_now(candles))


def _payload(candles: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "instrument": "MGC",
        "source_id": "unit-regime-state",
        "input_source_path": "outputs/track_b_execution_core/runtime/mgc_regime_source.json",
        "input_source_category": "RUNTIME",
        "input_mode": "RUNTIME_DECISION",
        "runtime_provenance": {"source_id": "unit-runtime-candles"},
        "timeframe_context": {
            "primary_timeframe": "5m",
            "context_timeframe": "5m",
            "timeframe_source": "NATIVE",
            "base_timeframe_if_derived": None,
            "aggregation_method": "NATIVE_PROVIDER_CANDLES",
            "anchor_rule": "UTC_5M_BOUNDARY",
            "timeframe_alignment_status": "ALIGNED",
        },
        "candles": candles,
    }


def _trending_expansion_candles() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    price = 100.0
    for index in range(8):
        rows.append(_candle(index, price, price + 0.45, price - 0.10, price + 0.35))
        price += 0.35
    for index in range(8, 12):
        rows.append(_candle(index, price, price + 1.00, price - 0.15, price + 0.85))
        price += 0.85
    return rows


def _high_vol_expansion_candles() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    price = 100.0
    for index in range(8):
        close = price + (0.12 if index % 2 == 0 else -0.12)
        rows.append(_candle(index, price, max(price, close) + 0.18, min(price, close) - 0.18, close))
        price = close
    for index in range(8, 12):
        close = price + 1.05
        rows.append(_candle(index, price, close + 0.75, price - 0.55, close))
        price = close
    return rows


def _chop_candles() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    price = 100.0
    for index in range(12):
        close = price + (0.45 if index % 2 == 0 else -0.45)
        rows.append(_candle(index, price, max(price, close) + 0.20, min(price, close) - 0.20, close))
        price = close
    return rows


def _compression_candles() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    price = 100.0
    for index in range(8):
        close = price + (0.28 if index % 2 == 0 else -0.24)
        rows.append(_candle(index, price, max(price, close) + 0.70, min(price, close) - 0.70, close))
        price = close
    for index in range(8, 12):
        close = price + (0.05 if index % 2 == 0 else -0.04)
        rows.append(_candle(index, price, max(price, close) + 0.12, min(price, close) - 0.12, close))
        price = close
    return rows


def _candle(index: int, open_price: float, high: float, low: float, close: float) -> dict[str, Any]:
    return {
        "timestamp": (BASE_TS + timedelta(minutes=5 * index)).isoformat(),
        "open": round(open_price, 4),
        "high": round(high, 4),
        "low": round(low, 4),
        "close": round(close, 4),
        "volume": 100 + index,
        "timeframe": "5m",
        "completed": True,
    }


def _timestamp(row: Mapping[str, Any]) -> datetime:
    return datetime.fromisoformat(str(row["timestamp"])).astimezone(UTC)


def _now(candles: list[dict[str, Any]]) -> datetime:
    return _timestamp(candles[-1]) + timedelta(minutes=5)
