from __future__ import annotations

import ast
from pathlib import Path

from mgc_v05l.execution_core.track_b_sizing_position_management import (
    BASE_UNIT,
    HOLD_FULL,
    NO_POSITION,
    NOT_ALLOWED_V1,
    REDUCE_PARTIAL,
    REDUCED_UNIT,
    UNKNOWN,
    build_sizing_position_management_state,
)


MODULE_PATH = (
    Path(__file__).resolve().parents[3]
    / "src"
    / "mgc_v05l"
    / "execution_core"
    / "track_b_sizing_position_management.py"
)


def _clean_payload() -> dict:
    return {
        "entry_acceptance_context": {
            "acceptance_class": "EXACT_STRUCTURAL_MATCH",
            "confidence": 0.91,
            "strategy_family": "gc_1x_exact_baseline",
            "candidate_timeframe": "5m",
            "instrument": "MGC",
        },
        "lifecycle_awareness_context": {
            "lifecycle_awareness_state": "FAVORABLE_EXPANSION",
            "hold_quality_context": "STRONG_HOLD",
            "exit_urgency_context": "LOW",
            "confidence": 0.88,
            "failure_reasons": [],
        },
        "participation_pressure_context_v1": {
            "pressure_state": "PERSISTENT_BULLISH_PRESSURE",
            "directional_bias": "BULLISH",
            "hold_quality_context": "SUPPORTIVE",
            "exit_urgency_context": "LOW",
            "pressure_confidence": 0.84,
            "failure_reasons": [],
        },
        "regime_session_context_v1": {
            "market_regime_state": "REGIME_EXPANSION",
            "trend_chop_state": "EXPANSION_DIRECTIONAL",
            "liquidity_state": "LIQUIDITY_NORMAL",
            "directional_context": "DIRECTIONAL_BULLISH",
            "confidence": 0.82,
            "failure_reasons": [],
        },
        "exit_context": {
            "exit_profile_context": "PATIENT_CONTINUATION",
            "exit_urgency_context": "LOW",
            "confidence": 0.80,
            "failure_reasons": [],
        },
    }


def test_base_unit_clean_context() -> None:
    result = build_sizing_position_management_state(_clean_payload())

    assert result["initial_size_context"] == BASE_UNIT
    assert result["add_size_context"] == NOT_ALLOWED_V1
    assert result["confidence"] > 0.75
    assert result["strategy_family"] == "gc_1x_exact_baseline"
    assert result["exit_profile"] == "PATIENT_CONTINUATION"
    assert result["instrument"] == "MGC"
    assert result["timeframe"] == "5m"
    assert result["failure_reasons"] == []


def test_reduced_unit_degraded_context() -> None:
    payload = _clean_payload()
    payload["entry_acceptance_context"]["acceptance_class"] = "DEGRADED_BUT_VALID_MATCH"
    payload["entry_acceptance_context"]["confidence"] = 0.58
    payload["lifecycle_awareness_context"]["lifecycle_awareness_state"] = "HEALTHY_PULLBACK"
    payload["participation_pressure_context_v1"]["pressure_state"] = "BULLISH_IMPULSE_DECAYING"

    result = build_sizing_position_management_state(payload)

    assert result["initial_size_context"] == REDUCED_UNIT
    assert result["add_size_context"] == NOT_ALLOWED_V1
    assert "DEGRADED_CONTEXT_REDUCED_UNIT" in result["sizing_reasons"]
    assert result["failure_reasons"] == []


def test_no_position_fail_closed_for_stale_unreconciled_context() -> None:
    payload = _clean_payload()
    payload["lifecycle_awareness_context"] = {
        "lifecycle_awareness_state": "LOW_CONFIDENCE_STALE_OR_UNRECONCILED",
        "confidence": 0.0,
        "failure_reasons": ["STALE_INPUT", "UNRECONCILED_POSITION_CONTEXT"],
    }
    payload["regime_session_context_v1"] = {
        "market_regime_state": "REGIME_THIN_OR_STALE",
        "liquidity_state": "LIQUIDITY_STALE_OR_INCOMPLETE",
        "confidence": 0.0,
        "failure_reasons": ["STALE_INPUT"],
    }

    result = build_sizing_position_management_state(payload)

    assert result["initial_size_context"] == NO_POSITION
    assert result["in_position_size_context"] == UNKNOWN
    assert result["add_size_context"] == NOT_ALLOWED_V1
    assert result["confidence"] == 0.0
    assert "STALE_INPUT" in result["failure_reasons"]


def test_in_position_hold_full_for_supportive_context() -> None:
    result = build_sizing_position_management_state(_clean_payload())

    assert result["in_position_size_context"] == HOLD_FULL
    assert "SUPPORTIVE_CONTEXT_HOLD_FULL" in result["management_reasons"]


def test_in_position_reduce_partial_for_hostile_context() -> None:
    payload = _clean_payload()
    payload["lifecycle_awareness_context"]["lifecycle_awareness_state"] = "DECAYING"
    payload["participation_pressure_context_v1"]["pressure_state"] = "BULLISH_PARTICIPATION_COLLAPSE"
    payload["exit_context"]["exit_profile_context"] = "DEFENSIVE_TIGHT"

    result = build_sizing_position_management_state(payload)

    assert result["initial_size_context"] == REDUCED_UNIT
    assert result["in_position_size_context"] == REDUCE_PARTIAL
    assert result["add_size_context"] == NOT_ALLOWED_V1
    assert "HOSTILE_CONTEXT_REDUCE_PARTIAL" in result["management_reasons"]


def test_add_size_not_allowed_v1_for_all_paths() -> None:
    payloads = [
        _clean_payload(),
        {
            "entry_acceptance_context": {
                "acceptance_class": "NEAR_STRUCTURAL_MATCH",
                "confidence": 0.66,
            }
        },
        {
            "lifecycle_awareness_context": {
                "lifecycle_awareness_state": "LOW_CONFIDENCE_STALE_OR_UNRECONCILED",
                "confidence": 0.0,
            }
        },
    ]

    for payload in payloads:
        assert build_sizing_position_management_state(payload)["add_size_context"] == NOT_ALLOWED_V1


def test_safety_flags_are_always_false() -> None:
    result = build_sizing_position_management_state(_clean_payload())

    for key in (
        "strategy_authority",
        "broker_state_mutated",
        "submit_attempted",
        "order_intent_created",
        "lifecycle_mutated",
        "runtime_trade_eligible",
    ):
        assert result[key] is False


def test_forbidden_imports_and_authority_calls_absent() -> None:
    tree = ast.parse(MODULE_PATH.read_text())
    forbidden_import_roots = {
        "ibapi",
        "ib_insync",
        "mgc_v05l.app",
        "mgc_v05l.brokers",
        "mgc_v05l.strategies",
        "mgc_v05l.strategy",
    }
    forbidden_call_names = {
        "cancel",
        "close",
        "flatten",
        "mutate_lifecycle",
        "placeOrder",
        "submit",
    }

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert alias.name not in forbidden_import_roots
                assert not any(alias.name.startswith(f"{root}.") for root in forbidden_import_roots)
        if isinstance(node, ast.ImportFrom) and node.module:
            assert node.module not in forbidden_import_roots
            assert not any(node.module.startswith(f"{root}.") for root in forbidden_import_roots)
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                assert node.func.id not in forbidden_call_names
            if isinstance(node.func, ast.Attribute):
                assert node.func.attr not in forbidden_call_names
