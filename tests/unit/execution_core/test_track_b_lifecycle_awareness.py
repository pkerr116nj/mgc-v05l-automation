from __future__ import annotations

import ast
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.track_b_lifecycle_awareness import (
    ADVERSE_DOMINANCE,
    CLOSED_OR_FLAT,
    DECAYING,
    FAVORABLE_EXPANSION,
    HEALTHY_PULLBACK,
    INCOMPLETE_CANDLES,
    LOW_CONFIDENCE_STALE_OR_UNRECONCILED,
    MALFORMED_CANDLES,
    MIXED_TIMEFRAME,
    STALE_INPUT,
    STALLED,
    THIN_DATA,
    UNRECONCILED_POSITION_CONTEXT,
    EXIT_PENDING_OBSERVED,
    build_lifecycle_awareness_state,
)
from mgc_v05l.execution_core.track_b_participation_quality import participation_pressure_context_v1


BASE_TS = datetime(2026, 5, 8, 14, 0, tzinfo=UTC)


def test_favorable_expansion_classification() -> None:
    report = _evaluate(mfe_points=2.4, mae_points=0.35, current_unrealized_points=1.9, bars_since_entry=5)

    assert report["lifecycle_awareness_state"] == FAVORABLE_EXPANSION
    assert report["hold_quality_context"] == "STRONG_HOLD"
    assert report["exit_urgency_context"] == "LOW"
    assert report["add_size_context"] == "ONLY_AFTER_CONTINUATION_CONFIRMATION"
    assert report["reduce_size_context"] == "NOT_INDICATED"
    assert report["patience_context"] == "PATIENCE_SUPPORTED"
    assert report["mfe_points"] == 2.4
    assert report["mae_points"] == 0.35
    assert report["progress_ratio"] > 0.70
    assert report["failure_reasons"] == []


def test_healthy_pullback_classification() -> None:
    report = _evaluate(mfe_points=2.0, mae_points=0.45, current_unrealized_points=0.9, bars_since_entry=6)

    assert report["lifecycle_awareness_state"] == HEALTHY_PULLBACK
    assert report["hold_quality_context"] == "CONSTRUCTIVE_HOLD"
    assert report["exit_urgency_context"] == "WATCH"
    assert report["patience_context"] == "PATIENCE_SUPPORTED"
    assert "pullback is bounded" in report["state_reasons"][0]


def test_stalled_classification() -> None:
    report = _evaluate(mfe_points=0.45, mae_points=0.40, current_unrealized_points=0.05, bars_since_entry=10)

    assert report["lifecycle_awareness_state"] == STALLED
    assert report["hold_quality_context"] == "GUARDED_HOLD"
    assert report["exit_urgency_context"] == "WATCH"
    assert report["patience_context"] == "PATIENCE_DECLINING"


def test_decaying_classification() -> None:
    report = _evaluate(mfe_points=1.8, mae_points=0.70, current_unrealized_points=0.45, bars_since_entry=7)

    assert report["lifecycle_awareness_state"] == DECAYING
    assert report["hold_quality_context"] == "GUARDED_HOLD"
    assert report["exit_urgency_context"] == "ELEVATED"
    assert report["reduce_size_context"] == "CONSIDER_ONLY_AFTER_AUTHORIZED_REVIEW"
    assert report["add_size_context"] == "BLOCKED_DEGRADED_CONTEXT"


def test_adverse_dominance_classification() -> None:
    report = _evaluate(mfe_points=0.8, mae_points=1.9, current_unrealized_points=-1.1, bars_since_entry=5)

    assert report["lifecycle_awareness_state"] == ADVERSE_DOMINANCE
    assert report["hold_quality_context"] == "WEAK_HOLD"
    assert report["exit_urgency_context"] == "HIGH_ADVISORY"
    assert report["reduce_size_context"] == "ELEVATED_ADVISORY"
    assert report["add_size_context"] == "BLOCKED_DEGRADED_CONTEXT"


def test_exit_pending_observed_classification() -> None:
    payload = _payload(
        lifecycle_status="EXIT_PENDING",
        reconciliation_status="EXIT_PENDING",
        mfe_points=1.1,
        mae_points=0.6,
        current_unrealized_points=0.2,
        bars_since_entry=6,
    )

    report = build_lifecycle_awareness_state(payload, now=_now(payload["candles"]))

    assert report["lifecycle_awareness_state"] == EXIT_PENDING_OBSERVED
    assert report["exit_urgency_context"] == "PENDING_OBSERVED"
    assert report["runtime_trade_eligible"] is False
    assert report["order_intent_created"] is False


def test_closed_or_flat_classification() -> None:
    payload = _payload(lifecycle_status="CLOSED_FLAT", reconciliation_status="FLAT")

    report = build_lifecycle_awareness_state(payload, now=_now(payload["candles"]))

    assert report["lifecycle_awareness_state"] == CLOSED_OR_FLAT
    assert report["hold_quality_context"] == "NO_POSITION"
    assert report["reduce_size_context"] == "NO_POSITION"
    assert report["exit_urgency_context"] == "NONE"


def test_lifecycle_awareness_passes_through_participation_pressure_context_without_authority() -> None:
    payload = _payload(mfe_points=2.4, mae_points=0.35, current_unrealized_points=1.9, bars_since_entry=5)
    pressure_context = participation_pressure_context_v1(
        {
            "schema_version": "track_b_participation_quality_state_v1",
            "participation_state": "BULLISH_HEALTHY_PULLBACK",
            "long_hold_quality": "SUPPORTIVE",
            "long_exit_urgency_context": "LOW",
            "continuation_confidence": 0.72,
            "pullback_health": "HEALTHY",
            "confidence": 0.8,
            "confidence_failure_reasons": [],
            "state_reasons": ["pullback depth is controlled."],
            "warnings": [],
            "feature_summary": {},
        },
        side="LONG",
    )
    payload["participation_pressure_context_v1"] = pressure_context

    report = build_lifecycle_awareness_state(payload, now=_now(payload["candles"]))

    assert report["lifecycle_awareness_state"] == FAVORABLE_EXPANSION
    assert report["participation_pressure_context_v1"] == pressure_context
    assert report["strategy_authority"] is False
    assert report["order_intent_created"] is False
    assert report["lifecycle_mutated"] is False


def test_malformed_thin_stale_and_unreconciled_fail_closed() -> None:
    malformed_candles = _candles()
    del malformed_candles[2]["high"]
    malformed = build_lifecycle_awareness_state(_payload(candles=malformed_candles), now=_now(malformed_candles))

    thin_payload = _payload(candles=_candles(count=4))
    thin = build_lifecycle_awareness_state(thin_payload, now=_now(thin_payload["candles"]))

    stale_payload = _payload()
    stale = build_lifecycle_awareness_state(stale_payload, now=BASE_TS + timedelta(hours=2))

    unreconciled_payload = _payload(reconciliation_status="MISMATCH")
    unreconciled = build_lifecycle_awareness_state(unreconciled_payload, now=_now(unreconciled_payload["candles"]))

    for report in (malformed, thin, stale, unreconciled):
        assert report["lifecycle_awareness_state"] == LOW_CONFIDENCE_STALE_OR_UNRECONCILED
        assert report["hold_quality_context"] == "NO_HOLD_LOW_CONFIDENCE"
        assert report["exit_urgency_context"] == "LOW_CONFIDENCE_BLOCKED"
        assert report["reduce_size_context"] == "BLOCKED_LOW_CONFIDENCE"
        assert report["confidence"] == 0.0

    assert MALFORMED_CANDLES in malformed["failure_reasons"]
    assert THIN_DATA in thin["failure_reasons"]
    assert STALE_INPUT in stale["failure_reasons"]
    assert UNRECONCILED_POSITION_CONTEXT in unreconciled["failure_reasons"]


def test_mixed_and_incomplete_timeframe_fail_closed() -> None:
    mixed_payload = _payload()
    mixed_payload["timeframe_context"]["timeframe_alignment_status"] = "MIXED_TIMEFRAME_BLOCKED"
    mixed = build_lifecycle_awareness_state(mixed_payload, now=_now(mixed_payload["candles"]))

    incomplete_payload = _payload()
    incomplete_payload["candles"][-1]["completed"] = False
    incomplete = build_lifecycle_awareness_state(incomplete_payload, now=_now(incomplete_payload["candles"]))

    candle_mixed_payload = _payload()
    candle_mixed_payload["candles"][3]["timeframe"] = "1m"
    candle_mixed = build_lifecycle_awareness_state(candle_mixed_payload, now=_now(candle_mixed_payload["candles"]))

    for report in (mixed, incomplete, candle_mixed):
        assert report["lifecycle_awareness_state"] == LOW_CONFIDENCE_STALE_OR_UNRECONCILED
        assert report["runtime_trade_eligible"] is False

    assert MIXED_TIMEFRAME in mixed["failure_reasons"]
    assert INCOMPLETE_CANDLES in incomplete["failure_reasons"]
    assert MIXED_TIMEFRAME in candle_mixed["failure_reasons"]


def test_safety_flags_always_false() -> None:
    reports = [
        _evaluate(mfe_points=2.0, mae_points=0.3, current_unrealized_points=1.8, bars_since_entry=4),
        _evaluate(mfe_points=0.3, mae_points=1.8, current_unrealized_points=-1.2, bars_since_entry=5),
        build_lifecycle_awareness_state(_payload(candles=_candles(count=3)), now=BASE_TS + timedelta(minutes=20)),
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


def test_no_broker_order_lifecycle_mutation_imports_or_calls() -> None:
    path = Path("src/mgc_v05l/execution_core/track_b_lifecycle_awareness.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    forbidden_import_roots = (
        "mgc_v05l.execution",
        "mgc_v05l.strategy",
        "mgc_v05l.app",
        "ibapi",
    )
    forbidden_call_names = {
        "submit",
        "cancel",
        "placeOrder",
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


def _evaluate(
    *,
    mfe_points: float,
    mae_points: float,
    current_unrealized_points: float,
    bars_since_entry: int,
) -> dict[str, Any]:
    payload = _payload(
        mfe_points=mfe_points,
        mae_points=mae_points,
        current_unrealized_points=current_unrealized_points,
        bars_since_entry=bars_since_entry,
    )
    return build_lifecycle_awareness_state(payload, now=_now(payload["candles"]))


def _payload(
    *,
    candles: list[dict[str, Any]] | None = None,
    lifecycle_status: str = "OPEN_MANAGED",
    reconciliation_status: str = "MATCHED",
    mfe_points: float = 2.0,
    mae_points: float = 0.5,
    current_unrealized_points: float = 1.5,
    bars_since_entry: int = 5,
) -> dict[str, Any]:
    resolved_candles = candles or _candles()
    fill_timestamp = _timestamp(resolved_candles[0])
    return {
        "source_id": "unit-lifecycle-awareness",
        "input_source_path": "tests/fixtures/track_b_lifecycle_awareness/unit_payload.json",
        "input_source_category": "TEST_FIXTURE",
        "input_mode": "TEST",
        "lifecycle_position_record": {
            "lifecycle_position_id": "life-001",
            "strategy_id": "strategy-alpha",
            "lane_id": "lane-paper",
            "instrument": "MGC",
            "contract_key": "MGC-202606",
            "side": "LONG",
            "quantity": 1,
            "entry_timestamp": fill_timestamp.isoformat(),
            "fill_timestamp": fill_timestamp.isoformat(),
            "average_fill_price": 2400.0,
            "lifecycle_status": lifecycle_status,
            "lifecycle_owned": True,
        },
        "entry_metadata": {
            "strategy_family": "asiaEarlyNormalBreakoutRetestHoldLong",
            "entry_timeframe": "5m",
            "entry_acceptance_class": "EXACT_STRUCTURAL_MATCH",
            "confidence": 0.91,
        },
        "broker_reconciliation_context": {
            "status": reconciliation_status,
            "source": "unit-read-only",
        },
        "timeframe_context": {
            "lifecycle_evaluation_timeframe": "5m",
            "fast_reaction_timeframe": "1m",
            "trend_context_timeframe": "15m",
            "timeframe_source": "NATIVE",
            "base_timeframe_if_derived": None,
            "aggregation_method": "NATIVE_PROVIDER_CANDLES",
            "anchor_rule": "UTC_5M_BOUNDARY",
            "timeframe_alignment_status": "ALIGNED",
        },
        "mfe_mae_context": {
            "mfe_points": mfe_points,
            "mae_points": mae_points,
            "mfe_ticks": mfe_points * 10,
            "mae_ticks": mae_points * 10,
            "mfe_dollars": mfe_points * 10,
            "mae_dollars": mae_points * 10,
        },
        "unrealized_progress_context": {
            "current_unrealized_points": current_unrealized_points,
            "current_unrealized_ticks": current_unrealized_points * 10,
            "current_unrealized_dollars": current_unrealized_points * 10,
        },
        "position_age_context": {
            "bars_since_entry": bars_since_entry,
            "wall_clock_seconds_since_entry": bars_since_entry * 300,
            "session_seconds_since_entry": bars_since_entry * 300,
        },
        "candles": resolved_candles,
    }


def _candles(count: int = 12) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    price = 2400.0
    for index in range(count):
        timestamp = BASE_TS + timedelta(minutes=5 * index)
        open_price = price + index * 0.10
        close_price = open_price + 0.12
        rows.append(
            {
                "timestamp": timestamp.isoformat(),
                "open": round(open_price, 2),
                "high": round(close_price + 0.25, 2),
                "low": round(open_price - 0.20, 2),
                "close": round(close_price, 2),
                "timeframe": "5m",
                "completed": True,
            }
        )
    return rows


def _timestamp(row: Mapping[str, Any]) -> datetime:
    return datetime.fromisoformat(str(row["timestamp"]))


def _now(candles: list[dict[str, Any]]) -> datetime:
    return _timestamp(candles[-1]) + timedelta(minutes=5)
