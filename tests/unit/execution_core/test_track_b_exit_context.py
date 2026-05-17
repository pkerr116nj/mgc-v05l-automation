from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.track_b_exit_context import (
    DEFENSIVE_TIGHT,
    MALFORMED_CANDLES,
    MIXED_TIMEFRAME,
    NO_EXIT_LOW_CONFIDENCE,
    PARTICIPATION_COLLAPSE_EXIT,
    PATIENT_CONTINUATION,
    PROTECTIVE_HARD_STOP,
    STALE_INPUT,
    THIN_DATA,
    TIME_BOXED,
    build_exit_context_state,
    write_exit_context_state,
)
from mgc_v05l.execution_core.track_b_participation_quality import participation_pressure_context_v1


BASE_TS = datetime(2026, 5, 8, 14, 0, tzinfo=UTC)


def test_healthy_pullback_supports_hold() -> None:
    report = _evaluate(
        participation_state="BULLISH_HEALTHY_PULLBACK",
        long_hold_quality="SUPPORTIVE",
        pullback_health="HEALTHY",
        mfe_points=1.4,
        mae_points=0.35,
        current_favorable_points=1.1,
        bars_since_fill=5,
    )

    assert report["exit_profile_context"] == PATIENT_CONTINUATION
    assert report["exit_urgency_context"] == "LOW"
    assert report["hold_quality_context"] == "SUPPORTIVE"
    assert report["reduce_size_context"] == "HOLD_FULL_SIZE"
    assert report["scale_up_context"] == "SCALE_ONLY_AFTER_CONTINUATION_CONFIRMATION"
    assert report["exit_reasons"] == []
    assert report["failure_reasons"] == []


def test_impulse_decay_raises_exit_urgency_context() -> None:
    report = _evaluate(
        participation_state="BULLISH_IMPULSE_DECAYING",
        long_hold_quality="DEGRADING",
        pullback_health="QUESTIONABLE",
        mfe_points=1.2,
        mae_points=0.9,
        current_favorable_points=0.6,
        bars_since_fill=6,
    )

    assert report["exit_profile_context"] == DEFENSIVE_TIGHT
    assert report["exit_urgency_context"] == "ELEVATED"
    assert report["hold_quality_context"] == "DEGRADED"
    assert "impulse or participation context is decaying." in report["exit_reasons"]


def test_participation_collapse_flags_high_urgency_context() -> None:
    report = _evaluate(
        participation_state="BULLISH_PARTICIPATION_COLLAPSE",
        long_hold_quality="HOSTILE",
        pullback_health="FAILED",
        mfe_points=0.8,
        mae_points=1.2,
        current_favorable_points=0.0,
        bars_since_fill=7,
    )

    assert report["exit_profile_context"] == PARTICIPATION_COLLAPSE_EXIT
    assert report["exit_urgency_context"] == "HIGH"
    assert report["hold_quality_context"] == "HOSTILE"
    assert report["reduce_size_context"] == "REDUCE_SIZE_CONTEXT"
    assert report["scale_up_context"] == "NO_ADD_DEGRADED_ENTRY_OR_WEAK_PARTICIPATION"


def test_exit_context_consumes_participation_pressure_contract() -> None:
    payload = _payload(_candles())
    pressure_context = participation_pressure_context_v1(
        {
            "schema_version": "track_b_participation_quality_state_v1",
            "participation_state": "BULLISH_PARTICIPATION_COLLAPSE",
            "long_hold_quality": "HOSTILE",
            "long_exit_urgency_context": "HIGH",
            "continuation_confidence": 0.12,
            "pullback_health": "FAILED",
            "confidence": 0.78,
            "confidence_failure_reasons": [],
            "state_reasons": ["pullback failure indicates collapse."],
            "warnings": [],
            "feature_summary": {},
        },
        side="LONG",
    )
    payload.pop("participation_quality_context")
    payload["participation_pressure_context_v1"] = pressure_context

    report = build_exit_context_state(payload, now=_now(payload["candles"]))

    assert report["exit_profile_context"] == PARTICIPATION_COLLAPSE_EXIT
    assert report["participation_quality_context"]["participation_state"] == "BULLISH_PARTICIPATION_COLLAPSE"
    assert report["participation_quality_context"]["side_hold_quality"] == "HOSTILE"
    assert report["order_intent_created"] is False
    assert report["lifecycle_mutated"] is False


def test_hard_protective_stop_context() -> None:
    report = _evaluate(
        participation_state="BULLISH_HEALTHY_PULLBACK",
        long_hold_quality="SUPPORTIVE",
        mfe_points=0.4,
        mae_points=3.25,
        protective_stop_breached=True,
        bars_since_fill=4,
    )

    assert report["exit_profile_context"] == PROTECTIVE_HARD_STOP
    assert report["exit_urgency_context"] == "PROTECTIVE"
    assert report["protective_exit_context"]["active"] is True
    assert "hard protective stop context is active." in report["exit_reasons"]
    assert report["order_intent_created"] is False


def test_time_box_expiry_context() -> None:
    report = _evaluate(
        participation_state="BULLISH_IMPULSE_ONLY",
        long_hold_quality="SUPPORTIVE",
        mfe_points=0.2,
        mae_points=0.4,
        bars_since_fill=13,
        max_bars_in_trade=12,
    )

    assert report["exit_profile_context"] == TIME_BOXED
    assert report["exit_urgency_context"] == "ELEVATED"
    assert any("time-box expired" in reason for reason in report["exit_reasons"])


def test_reduce_size_context_from_adverse_giveback() -> None:
    report = _evaluate(
        participation_state="BULLISH_HEALTHY_PULLBACK",
        long_hold_quality="SUPPORTIVE",
        mfe_points=2.0,
        mae_points=0.8,
        current_favorable_points=0.5,
        bars_since_fill=8,
    )

    assert report["exit_profile_context"] == DEFENSIVE_TIGHT
    assert report["exit_urgency_context"] == "ELEVATED"
    assert report["reduce_size_context"] == "REDUCE_SIZE_CONTEXT"
    assert "adverse excursion or MFE giveback supports reduce-size context." in report["exit_reasons"]


def test_stale_thin_and_malformed_data_fail_closed() -> None:
    stale = build_exit_context_state(_payload(_candles()), now=BASE_TS + timedelta(hours=2))
    thin = build_exit_context_state(_payload(_candles(count=6)), now=BASE_TS + timedelta(minutes=35))
    malformed_candles = _candles()
    del malformed_candles[3]["high"]
    malformed = build_exit_context_state(_payload(malformed_candles), now=_now(malformed_candles))

    for report in (stale, thin, malformed):
        assert report["exit_profile_context"] == NO_EXIT_LOW_CONFIDENCE
        assert report["exit_urgency_context"] == "NONE"
        assert report["hold_quality_context"] == "LOW_CONFIDENCE"
        assert report["runtime_trade_eligible"] is False

    assert STALE_INPUT in stale["failure_reasons"]
    assert THIN_DATA in thin["failure_reasons"]
    assert MALFORMED_CANDLES in malformed["failure_reasons"]


def test_mixed_timeframe_fails_closed() -> None:
    payload = _payload(_candles())
    payload["timeframe_context"]["timeframe_alignment_status"] = "MIXED"

    report = build_exit_context_state(payload, now=_now(payload["candles"]))

    assert report["exit_profile_context"] == NO_EXIT_LOW_CONFIDENCE
    assert MIXED_TIMEFRAME in report["failure_reasons"]
    assert report["reduce_size_context"] == "NO_SIZE_ADJUSTMENT_LOW_CONFIDENCE"


def test_safety_flags_always_false() -> None:
    reports = [
        _evaluate(participation_state="BULLISH_HEALTHY_PULLBACK", long_hold_quality="SUPPORTIVE"),
        _evaluate(participation_state="BULLISH_PARTICIPATION_COLLAPSE", long_hold_quality="HOSTILE"),
        build_exit_context_state(_payload(_candles(count=3)), now=BASE_TS + timedelta(minutes=20)),
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


def test_no_broker_order_lifecycle_mutation_imports_or_symbols() -> None:
    path = Path("src/mgc_v05l/execution_core/track_b_exit_context.py")
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


def test_artifact_write_to_tmp_path(tmp_path: Path) -> None:
    output_path = tmp_path / "latest_exit_context_state.json"

    report = write_exit_context_state(_payload(_candles()), output_path=output_path, now=BASE_TS + timedelta(minutes=60))
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert output_path.exists()
    assert report["artifact_path"] == str(output_path.resolve())
    assert written["artifact_path"] == str(output_path.resolve())
    assert written["strategy_authority"] is False
    assert written["broker_state_mutated"] is False


def _evaluate(
    *,
    participation_state: str,
    long_hold_quality: str,
    pullback_health: str = "HEALTHY",
    mfe_points: float = 1.0,
    mae_points: float = 0.5,
    current_favorable_points: float = 0.8,
    bars_since_fill: int = 4,
    max_bars_in_trade: int = 12,
    protective_stop_breached: bool = False,
) -> dict[str, Any]:
    payload = _payload(_candles())
    payload["participation_quality_context"].update(
        {
            "participation_state": participation_state,
            "long_hold_quality": long_hold_quality,
            "pullback_health": pullback_health,
        }
    )
    payload["position_metrics"].update(
        {
            "mfe_points": mfe_points,
            "mae_points": mae_points,
            "current_favorable_points": current_favorable_points,
            "bars_since_fill": bars_since_fill,
            "max_bars_in_trade": max_bars_in_trade,
            "protective_stop_breached": protective_stop_breached,
        }
    )
    return build_exit_context_state(payload, now=_now(payload["candles"]))


def _payload(candles: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "instrument": "MGC",
        "candles": candles,
        "input_source_path": "outputs/track_b_execution_core/runtime/mgc_exit_context_source.json",
        "input_source_category": "RUNTIME",
        "input_mode": "RUNTIME_DECISION",
        "runtime_provenance": {"source_id": "unit-exit-context"},
        "lifecycle_position": {
            "position_id": "pos-123",
            "strategy_id": "strategy-a",
            "lane_id": "lane-a",
            "symbol": "MGC",
            "side": "LONG",
            "quantity": 1,
            "entry_timestamp": BASE_TS.isoformat(),
        },
        "entry_acceptance_context": {
            "entry_type": "BREAKOUT_RETEST",
            "entry_quality": "ACCEPTED",
            "entry_timeframe": "5m",
            "acceptance_reason": "unit fixture",
        },
        "participation_quality_context": {
            "participation_state": "BULLISH_HEALTHY_PULLBACK",
            "long_hold_quality": "SUPPORTIVE",
            "long_exit_urgency_context": "LOW",
            "pullback_health": "HEALTHY",
            "continuation_confidence": 0.72,
            "confidence": 0.80,
            "confidence_failure_reasons": [],
        },
        "position_metrics": {
            "mfe_points": 1.0,
            "mae_points": 0.5,
            "current_favorable_points": 0.8,
            "bars_since_fill": 4,
            "max_bars_in_trade": 12,
            "protective_stop_breached": False,
        },
        "reconciliation_context": {
            "status": "MATCHED",
            "source": "unit fixture",
        },
        "timeframe_context": {
            "entry_timeframe": "5m",
            "exit_management_timeframe": "5m",
            "fast_reaction_timeframe": "1m",
            "trend_context_timeframe": "15m",
            "timeframe_source": "unit fixture",
            "base_timeframe_if_derived": "1m",
            "aggregation_method": "completed_bar_aggregation",
            "anchor_rule": "right_closed_session_anchor",
            "timeframe_alignment_status": "ALIGNED",
        },
    }


def _candles(count: int = 12) -> list[dict[str, Any]]:
    candles: list[dict[str, Any]] = []
    price = 100.0
    for index in range(count):
        open_price = price
        close = price + 0.25
        high = close + 0.08
        low = open_price - 0.08
        candles.append(
            {
                "timestamp": (BASE_TS + timedelta(minutes=5 * index)).isoformat(),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": 100 + index,
                "timeframe": "5m",
                "completed": True,
            }
        )
        price = close
    return candles


def _now(candles: list[dict[str, Any]]) -> datetime:
    return datetime.fromisoformat(str(candles[-1]["timestamp"])).astimezone(UTC) + timedelta(minutes=5)
