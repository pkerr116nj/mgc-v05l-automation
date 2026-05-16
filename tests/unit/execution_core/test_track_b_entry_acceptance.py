from __future__ import annotations

import ast
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from mgc_v05l.execution_core.track_b_entry_acceptance import (
    DEGRADED_BUT_VALID_MATCH,
    EXACT_STRUCTURAL_MATCH,
    LOW_CONFIDENCE_INSUFFICIENT_DATA,
    MALFORMED_CANDLES,
    NEAR_STRUCTURAL_MATCH,
    RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE,
    STALE_INPUT,
    STRUCTURALLY_INVALID,
    THIN_DATA,
    build_entry_acceptance_state,
    write_entry_acceptance_state,
)


BASE_TS = datetime(2026, 5, 8, 14, 0, tzinfo=UTC)


def test_exact_match() -> None:
    report = _evaluate(_candidate(structural_similarity=0.96))

    assert report["acceptance_class"] == EXACT_STRUCTURAL_MATCH
    assert report["acceptance_score"] >= 0.85
    assert report["entry_quality_context"]["quality_label"] == "STRONG"
    assert report["suggested_exit_profile_context"]["advisory_only"] is True
    assert report["initial_size_context"]["advisory_only"] is True
    assert report["primary_timeframe"] == "5m"
    assert report["candidate_timeframe"] == "5m"
    assert report["context_timeframe"] == "5m"
    assert report["timeframe_source"] == "NATIVE"
    assert report["timeframe_alignment_status"] == "ALIGNED"
    assert report["runtime_trade_eligible"] is False


def test_near_structural_match() -> None:
    candidate = _candidate(
        structural_similarity=0.78,
        directional_alignment=0.82,
        timing_session_fit=0.82,
        pullback_retest_quality=0.76,
        failure_risk_score=0.90,
        near_miss_predicates=["retest_depth_slightly_shallow"],
    )

    report = _evaluate(candidate)

    assert report["acceptance_class"] == NEAR_STRUCTURAL_MATCH
    assert 0.70 <= report["acceptance_score"] < 0.85
    assert report["entry_quality_context"]["quality_label"] == "ACCEPTABLE"
    assert report["suggested_exit_profile_context"]["profile_hint"] == "TIGHTER_INVALIDATION"


def test_degraded_but_valid() -> None:
    candidate = _candidate(
        structural_similarity=0.62,
        directional_alignment=0.62,
        timing_session_fit=0.62,
        volatility_range_fit=0.55,
        pullback_retest_quality=0.46,
        failure_risk_score=0.62,
    )

    report = _evaluate(candidate)

    assert report["acceptance_class"] == DEGRADED_BUT_VALID_MATCH
    assert 0.50 <= report["acceptance_score"] < 0.70
    assert report["entry_quality_context"]["quality_label"] == "MARGINAL"
    assert report["initial_size_context"]["size_quality_label"] in {"REDUCED_CONTEXT", "MINIMUM_CONTEXT"}


def test_structurally_invalid() -> None:
    candidate = _candidate(
        structural_similarity=0.35,
        directional_alignment=0.90,
        missing_required_predicates=["required_retest_hold"],
    )

    report = _evaluate(candidate)

    assert report["acceptance_class"] == STRUCTURALLY_INVALID
    assert "MISSING_REQUIRED_PREDICATES" in report["failure_reasons"]
    assert report["initial_size_context"]["size_quality_label"] == "NO_SIZE_CONTEXT"
    assert report["runtime_trade_eligible"] is False


def test_wrong_side_directional_contradiction_is_invalid() -> None:
    candidate = _candidate(structural_similarity=0.94, directional_alignment=0.10)

    report = _evaluate(candidate)

    assert report["acceptance_class"] == STRUCTURALLY_INVALID
    assert "WRONG_SIDE_DIRECTIONAL_CONTRADICTION" in report["failure_reasons"]


def test_thin_data_fails_closed() -> None:
    report = build_entry_acceptance_state(
        _payload(_candidate(), candles=_bullish_candles()[:4]),
        now=_now(_bullish_candles()[:4]),
    )

    assert report["acceptance_class"] == LOW_CONFIDENCE_INSUFFICIENT_DATA
    assert THIN_DATA in report["failure_reasons"]
    assert report["confidence"] == 0.0
    assert report["runtime_trade_eligible"] is False


def test_stale_data_fails_closed() -> None:
    candles = _bullish_candles()

    report = build_entry_acceptance_state(
        _payload(_candidate(), candles=candles),
        now=_timestamp(candles[-1]) + timedelta(hours=1),
    )

    assert report["acceptance_class"] == LOW_CONFIDENCE_INSUFFICIENT_DATA
    assert STALE_INPUT in report["failure_reasons"]
    assert report["freshness_status"] == "STALE"


def test_malformed_data_fails_closed() -> None:
    candles = _bullish_candles()
    del candles[2]["high"]

    report = _evaluate(_candidate(), candles=candles)

    assert report["acceptance_class"] == LOW_CONFIDENCE_INSUFFICIENT_DATA
    assert MALFORMED_CANDLES in report["failure_reasons"]


def test_research_source_runtime_rejection() -> None:
    payload = _payload(_candidate())
    payload["input_source_path"] = "outputs/track_b_research/snapshots/latest_decision_bar_snapshots.jsonl"
    payload["input_source_category"] = "RESEARCH"

    report = build_entry_acceptance_state(
        payload,
        now=_now(payload["candles"]),
        input_mode="RUNTIME_DECISION",
    )

    assert report["acceptance_class"] == LOW_CONFIDENCE_INSUFFICIENT_DATA
    assert RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE in report["failure_reasons"]
    assert report["source_provenance_status"] == RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE
    assert report["runtime_trade_eligible"] is False


def test_mixed_timeframe_schema_fails_closed() -> None:
    payload = _payload(_candidate())
    payload["timeframe_context"] = {
        "primary_timeframe": "5m",
        "candidate_timeframe": "10m",
        "context_timeframe": "5m",
        "timeframe_source": "NATIVE",
    }

    report = build_entry_acceptance_state(payload, now=_now(payload["candles"]))

    assert report["acceptance_class"] == LOW_CONFIDENCE_INSUFFICIENT_DATA
    assert "MIXED_TIMEFRAME_SCHEMA" in report["failure_reasons"]
    assert report["timeframe_alignment_status"] == "MIXED_TIMEFRAME_BLOCKED"


def test_unapproved_derived_timeframe_fails_closed() -> None:
    payload = _payload(_candidate())
    payload["timeframe_context"] = {
        "primary_timeframe": "10m",
        "candidate_timeframe": "10m",
        "context_timeframe": "10m",
        "timeframe_source": "DERIVED",
        "base_timeframe_if_derived": "5m",
        "aggregation_method": "OHLCV_ROLLUP_VOLUME_SUM",
        "anchor_rule": "SESSION_ANCHORED_UTC_BOUNDARY",
    }
    for candle in payload["candles"]:
        candle["timeframe"] = "10m"

    report = build_entry_acceptance_state(payload, now=_now(payload["candles"]))

    assert report["acceptance_class"] == LOW_CONFIDENCE_INSUFFICIENT_DATA
    assert "UNAPPROVED_DERIVED_TIMEFRAME" in report["failure_reasons"]
    assert report["timeframe_alignment_status"] == "MIXED_TIMEFRAME_BLOCKED"


def test_participation_supportive_boosts_context_without_authority() -> None:
    no_participation = _evaluate(_candidate(structural_similarity=0.82))
    supportive = _evaluate(
        _candidate(structural_similarity=0.82),
        participation_quality={
            "long_hold_quality": "SUPPORTIVE",
            "short_hold_quality": "HOSTILE",
            "continuation_confidence": 0.86,
            "confidence": 0.90,
        },
    )

    assert supportive["acceptance_score"] > no_participation["acceptance_score"]
    assert supportive["dimension_scores"]["participation_support"] > no_participation["dimension_scores"]["participation_support"]
    assert any("participation quality is supportive" in reason for reason in supportive["supporting_reasons"])
    assert supportive["strategy_authority"] is False
    assert supportive["runtime_trade_eligible"] is False


def test_participation_hostile_degrades_context() -> None:
    supportive = _evaluate(
        _candidate(structural_similarity=0.92),
        participation_quality={
            "long_hold_quality": "SUPPORTIVE",
            "short_hold_quality": "HOSTILE",
            "continuation_confidence": 0.85,
            "confidence": 0.90,
        },
    )
    hostile = _evaluate(
        _candidate(structural_similarity=0.92),
        participation_quality={
            "long_hold_quality": "HOSTILE",
            "short_hold_quality": "SUPPORTIVE",
            "continuation_confidence": 0.20,
            "confidence": 0.90,
        },
    )

    assert hostile["acceptance_score"] < supportive["acceptance_score"]
    assert hostile["dimension_scores"]["participation_support"] < 0.40
    assert "PARTICIPATION_HOSTILE" in hostile["failure_reasons"]
    assert hostile["runtime_trade_eligible"] is False


def test_safety_flags_always_false() -> None:
    report = _evaluate(_candidate())

    for field in (
        "strategy_authority",
        "broker_state_mutated",
        "submit_attempted",
        "order_intent_created",
        "lifecycle_mutated",
        "runtime_trade_eligible",
    ):
        assert report[field] is False
        assert report["safety_flags"][field] is False


def test_writer_is_tmp_path_only(tmp_path: Path) -> None:
    output_path = tmp_path / "latest_entry_acceptance_state.json"

    report = write_entry_acceptance_state(
        _payload(_candidate()),
        output_path=output_path,
        now=_now(_bullish_candles()),
    )
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert output_path.exists()
    assert report["artifact_path"] == str(output_path.resolve())
    assert written["artifact_path"] == str(output_path.resolve())
    assert written["runtime_trade_eligible"] is False


def test_writer_rejects_non_tmp_runtime_artifact_path() -> None:
    with pytest.raises(ValueError, match="tmp/test output paths"):
        write_entry_acceptance_state(
            _payload(_candidate()),
            output_path="outputs/track_b_execution_core/entry_acceptance/latest_entry_acceptance_state.json",
            now=_now(_bullish_candles()),
        )


def test_no_broker_order_or_lifecycle_mutation_imports_or_calls() -> None:
    path = Path("src/mgc_v05l/execution_core/track_b_entry_acceptance.py")
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


def _evaluate(
    candidate: dict[str, Any],
    *,
    candles: list[dict[str, Any]] | None = None,
    participation_quality: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_candles = candles or _bullish_candles()
    return build_entry_acceptance_state(
        _payload(candidate, candles=resolved_candles, participation_quality=participation_quality),
        now=_now(resolved_candles),
    )


def _payload(
    candidate: dict[str, Any],
    *,
    candles: list[dict[str, Any]] | None = None,
    participation_quality: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "instrument": "MGC",
        "candle_timeframe": "5m",
        "candidate": candidate,
        "candles": candles or _bullish_candles(),
        "input_source_path": "outputs/track_b_execution_core/runtime/mgc_5m_latest.json",
        "input_source_category": "RUNTIME",
        "input_mode": "RUNTIME_DECISION",
        "runtime_provenance": {"source_id": "unit-runtime-candles"},
    }
    if participation_quality is not None:
        payload["participation_quality"] = participation_quality
    return payload


def _candidate(
    *,
    structural_similarity: float = 0.96,
    directional_alignment: float = 0.94,
    timing_session_fit: float = 0.94,
    volatility_range_fit: float = 0.88,
    pullback_retest_quality: float = 0.90,
    failure_risk_score: float = 0.95,
    near_miss_predicates: list[str] | None = None,
    missing_required_predicates: list[str] | None = None,
) -> dict[str, Any]:
    candidate = {
        "candidate_id": "track_b_entry_fixture_long",
        "candidate_family": "fixture_breakout_retest",
        "side": "LONG",
        "rule_id": "fixture_rule_v1",
        "required_predicates": ["impulse", "pullback", "retest_hold"],
        "matched_predicates": ["impulse", "pullback", "retest_hold"],
        "structural_similarity": structural_similarity,
        "directional_alignment": directional_alignment,
        "timing_session_fit": timing_session_fit,
        "volatility_range_fit": volatility_range_fit,
        "pullback_retest_quality": pullback_retest_quality,
        "failure_risk_score": failure_risk_score,
    }
    if near_miss_predicates:
        candidate["near_miss_predicates"] = near_miss_predicates
    if missing_required_predicates:
        candidate["missing_required_predicates"] = missing_required_predicates
    return candidate


def _bullish_candles() -> list[dict[str, Any]]:
    candles: list[dict[str, Any]] = []
    price = 100.0
    for index in range(12):
        if index < 7:
            open_price = price
            close = price + 0.55
            high = close + 0.10
            low = open_price - 0.10
        elif index < 10:
            open_price = price
            close = price - 0.18
            high = open_price + 0.08
            low = close - 0.12
        else:
            open_price = price
            close = price + 0.42
            high = close + 0.10
            low = open_price - 0.08
        candles.append(_candle(index, open_price, high, low, close))
        price = close
    return candles


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


def _now(candles: list[dict[str, Any]]) -> datetime:
    return _timestamp(candles[-1]) + timedelta(minutes=5)


def _timestamp(candle: dict[str, Any]) -> datetime:
    return datetime.fromisoformat(str(candle["timestamp"]).replace("Z", "+00:00")).astimezone(UTC)
