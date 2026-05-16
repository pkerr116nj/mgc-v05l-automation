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
    build_asia_early_normal_breakout_retest_hold_entry_acceptance_payload,
    build_entry_acceptance_state,
    write_entry_acceptance_state,
)
from mgc_v05l.execution_core.track_b_strategy_rule_runner import (
    TrackBStrategyRuleRunnerVerdict,
    run_track_b_strategy_rule,
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


def test_asia_early_normal_breakout_retest_hold_exact_current_pass_scores_exact() -> None:
    payload = build_asia_early_normal_breakout_retest_hold_entry_acceptance_payload(
        event_payload=_asia_breakout_event(),
        completed_candles=_bullish_candles(),
        input_source_path="outputs/track_b_execution_core/runtime/mgc_5m_latest.json",
    )

    report = build_entry_acceptance_state(payload, now=_now(_bullish_candles()))

    assert report["acceptance_class"] == EXACT_STRUCTURAL_MATCH
    assert report["candidate_family"] == "asiaEarlyNormalBreakoutRetestHoldLong"
    assert report["candidate_side"] == "LONG"
    assert report["runtime_trade_eligible"] is False
    assert report["order_intent_created"] is False


def test_asia_early_normal_breakout_retest_hold_one_soft_weakness_scores_near() -> None:
    event = _asia_breakout_event()
    features = _asia_breakout_features(event)
    features["breakout_bar_expansion_is_normal"] = False
    features["breakout_range_expansion_ratio"] = "1.27"

    payload = build_asia_early_normal_breakout_retest_hold_entry_acceptance_payload(
        event_payload=event,
        completed_candles=_bullish_candles(),
        input_source_path="outputs/track_b_execution_core/runtime/mgc_5m_latest.json",
    )

    report = build_entry_acceptance_state(payload, now=_now(_bullish_candles()))

    assert report["acceptance_class"] == NEAR_STRUCTURAL_MATCH
    assert "breakout_range_expansion_near_normal" in payload["candidate"]["near_miss_predicates"]
    assert "MISSING_REQUIRED_PREDICATES" not in report["failure_reasons"]


def test_asia_early_normal_breakout_retest_hold_multiple_margins_scores_degraded() -> None:
    event = _asia_breakout_event(close="102.01")
    metadata = event["metadata"]
    assert isinstance(metadata, dict)
    state = metadata["asia_early_normal_breakout_retest_hold_long_state"]
    features = metadata["asia_early_normal_breakout_retest_hold_long_features"]
    assert isinstance(state, dict)
    assert isinstance(features, dict)
    state["asia_early_or_gc_mgc_london_open"] = False
    state["no_first_bull_snap_turn"] = False
    state["prior_bars_since_long_setup_gt_anti_churn"] = False
    features["breakout_bar_expansion_is_normal"] = False
    features["breakout_range_expansion_ratio"] = "1.31"
    features["breakout_bar_slope_is_flat"] = False
    features["breakout_normalized_slope"] = "0.24"

    payload = build_asia_early_normal_breakout_retest_hold_entry_acceptance_payload(
        event_payload=event,
        completed_candles=_bullish_candles(),
        input_source_path="outputs/track_b_execution_core/runtime/mgc_5m_latest.json",
    )

    report = build_entry_acceptance_state(payload, now=_now(_bullish_candles()))

    assert report["acceptance_class"] == DEGRADED_BUT_VALID_MATCH
    assert report["entry_quality_context"]["quality_label"] == "MARGINAL"
    assert "MISSING_REQUIRED_PREDICATES" not in report["failure_reasons"]


def test_asia_early_normal_breakout_retest_hold_missing_breakout_or_hold_scores_invalid() -> None:
    event = _asia_breakout_event()
    features = _asia_breakout_features(event)
    features["breakout_breaks_prior_1_high"] = False

    payload = build_asia_early_normal_breakout_retest_hold_entry_acceptance_payload(
        event_payload=event,
        completed_candles=_bullish_candles(),
        input_source_path="outputs/track_b_execution_core/runtime/mgc_5m_latest.json",
    )

    report = build_entry_acceptance_state(payload, now=_now(_bullish_candles()))

    assert report["acceptance_class"] == STRUCTURALLY_INVALID
    assert "MISSING_REQUIRED_PREDICATES" in report["failure_reasons"]
    assert "breakout_breaks_prior_1_high" in payload["candidate"]["missing_required_predicates"]


def test_asia_early_normal_breakout_retest_hold_stale_provenance_scores_low_confidence() -> None:
    candles = _bullish_candles()
    payload = build_asia_early_normal_breakout_retest_hold_entry_acceptance_payload(
        event_payload=_asia_breakout_event(),
        completed_candles=candles,
        input_source_path="outputs/track_b_execution_core/runtime/mgc_5m_latest.json",
    )

    report = build_entry_acceptance_state(payload, now=_now(candles) + timedelta(hours=1))

    assert report["acceptance_class"] == LOW_CONFIDENCE_INSUFFICIENT_DATA
    assert STALE_INPUT in report["failure_reasons"]


def test_asia_early_normal_breakout_retest_hold_rule_output_unchanged_by_adapter(tmp_path: Path) -> None:
    event = _asia_breakout_event()
    before = run_track_b_strategy_rule(
        input_event_payload=event,
        input_event_path=tmp_path / "breakout_retest_hold_long_state.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_test_breakout_retest_hold_long",
        rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports_before",
        strategy_adapter_output_root=tmp_path / "adapter_reports_before",
        candle_producer_output_root=tmp_path / "candle_reports_before",
        writer_output_root=tmp_path / "writer_reports_before",
        runner_id="rule-runner-breakout-retest-before",
        now=BASE_TS,
    )

    payload = build_asia_early_normal_breakout_retest_hold_entry_acceptance_payload(
        event_payload=event,
        completed_candles=_bullish_candles(),
        input_source_path="outputs/track_b_execution_core/runtime/mgc_5m_latest.json",
    )
    report = build_entry_acceptance_state(payload, now=_now(_bullish_candles()))

    after = run_track_b_strategy_rule(
        input_event_payload=event,
        input_event_path=tmp_path / "breakout_retest_hold_long_state.json",
        inbox_dir=tmp_path / "inbox_after",
        expected_account_id="DUM882026",
        source_id="unit_test_breakout_retest_hold_long",
        rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports_after",
        strategy_adapter_output_root=tmp_path / "adapter_reports_after",
        candle_producer_output_root=tmp_path / "candle_reports_after",
        writer_output_root=tmp_path / "writer_reports_after",
        runner_id="rule-runner-breakout-retest-after",
        now=BASE_TS,
    )

    assert report["strategy_authority"] is False
    assert before.verdict == TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL
    assert after.verdict == before.verdict
    assert after.report["decision"] == before.report["decision"] == "LONG"
    assert after.report["signal_emitted"] == before.report["signal_emitted"] is True
    assert after.report["submit_attempted"] == before.report["submit_attempted"] is False
    assert after.report["broker_state_mutated"] == before.report["broker_state_mutated"] is False


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


def _asia_breakout_event(**overrides: Any) -> dict[str, Any]:
    event: dict[str, Any] = {
        "account_id": "DUM882026",
        "expected_account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        "signal_family": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        "lane_id": "mgc_asia_early_normal_breakout_retest_hold_long",
        "rule_mode": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        "source_id": "unit_test_breakout_retest_hold_long",
        "timeframe": "5m",
        "candle_timestamp": BASE_TS.isoformat(),
        "observed_at": BASE_TS.isoformat(),
        "generated_at": BASE_TS.isoformat(),
        "open": "101.80",
        "high": "102.50",
        "low": "101.70",
        "close": "102.30",
        "last": "102.30",
        "volume": "120",
        "quote_provider_mode": "REALTIME",
        "input_quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "quote_freshness_verdict": "CURRENT_QUOTE_FRESHNESS_ACCEPTED_STRICT_MAX_AGE",
        "metadata": {
            "source_payload_path": "outputs/track_b_execution_core/runtime/mgc_5m_latest.json",
            "source_bar_count": 12,
            "signal_side_if_ready": "LONG",
            "quote_provider_mode": "REALTIME",
            "realtime_quote_received": True,
            "current_quote_available": True,
            "asia_early_normal_breakout_retest_hold_long_state": {
                "derivative_phase": "ASIA_EARLY",
                "session_asia": True,
                "allow_asia": True,
                "asia_early_or_gc_mgc_london_open": True,
                "no_first_bull_snap_turn": True,
                "prior_bars_since_long_setup_gt_anti_churn": True,
                "timeframe": "5m",
            },
            "asia_early_normal_breakout_retest_hold_long_features": {
                "feature_version": "asia_early_normal_breakout_retest_hold_long_v1_phase1",
                "calibration_profile": "probationary_baseline_v1",
                "breakout_bar_slope_is_flat": True,
                "breakout_bar_expansion_is_normal": True,
                "breakout_breaks_prior_1_high": True,
                "signal_retests_and_holds_breakout_level": True,
                "breakout_normalized_slope": "0.05",
                "breakout_abs_slope_max": "0.20",
                "breakout_range_expansion_ratio": "1.00",
                "breakout_min_range_expansion_ratio": "0.85",
                "breakout_max_range_expansion_ratio": "1.25",
                "breakout_level": "102.00",
            },
        },
        "paper_proof_cli_called": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
    }
    event.update(overrides)
    return event


def _asia_breakout_features(event: dict[str, Any]) -> dict[str, Any]:
    metadata = event["metadata"]
    assert isinstance(metadata, dict)
    features = metadata["asia_early_normal_breakout_retest_hold_long_features"]
    assert isinstance(features, dict)
    return features


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
