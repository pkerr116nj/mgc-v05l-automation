from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from mgc_v05l.execution_core.track_b_entry_acceptance import (
    build_asia_early_normal_breakout_retest_hold_entry_acceptance_payload,
    build_entry_acceptance_state,
)


FIXTURE_PATH = (
    Path(__file__).parents[2]
    / "fixtures"
    / "entry_acceptance"
    / "asia_early_normal_breakout_retest_hold_cases.json"
)
STATE_KEY = "asia_early_normal_breakout_retest_hold_long_state"
FEATURES_KEY = "asia_early_normal_breakout_retest_hold_long_features"
NUMERIC_CONTEXT_FIELDS = {
    "retest_depth_ticks_or_points",
    "retest_depth_normalized",
    "hold_margin_ticks_or_points",
    "hold_margin_normalized",
    "bars_since_breakout",
    "bars_since_retest",
    "range_expansion_ratio",
    "close_location",
    "body_to_range_ratio",
    "churn_score",
    "snap_turn_conflict_strength",
}


def test_breakout_retest_hold_fixture_cases_score_expected_acceptance_classes() -> None:
    fixture = _load_fixture()
    for case in fixture["cases"]:
        report, scorer_payload = _score_case(fixture, case)

        assert report["acceptance_class"] == case["expected_acceptance_class"], case["case_id"]
        assert report["strategy_authority"] is False
        assert report["broker_state_mutated"] is False
        assert report["submit_attempted"] is False
        assert report["order_intent_created"] is False
        assert report["lifecycle_mutated"] is False
        assert report["runtime_trade_eligible"] is False

        for reason in case.get("expected_failure_reasons", []):
            assert reason in report["failure_reasons"], case["case_id"]

        if report["acceptance_class"] != "LOW_CONFIDENCE_INSUFFICIENT_DATA":
            context = scorer_payload["candidate"]["breakout_retest_hold_context"]
            assert NUMERIC_CONTEXT_FIELDS.issubset(context), case["case_id"]


def test_breakout_retest_hold_fixture_cases_cover_required_taxonomy() -> None:
    fixture = _load_fixture()
    observed = {case["expected_acceptance_class"] for case in fixture["cases"]}

    assert observed == {
        "EXACT_STRUCTURAL_MATCH",
        "NEAR_STRUCTURAL_MATCH",
        "DEGRADED_BUT_VALID_MATCH",
        "STRUCTURALLY_INVALID",
        "LOW_CONFIDENCE_INSUFFICIENT_DATA",
    }


def _score_case(fixture: dict[str, Any], case: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    event = copy.deepcopy(fixture["base_event"])
    metadata = event["metadata"]
    assert isinstance(metadata, dict)
    state = metadata[STATE_KEY]
    features = metadata[FEATURES_KEY]
    assert isinstance(state, dict)
    assert isinstance(features, dict)

    metadata.update(case.get("metadata_overrides", {}))
    for key in case.get("metadata_remove", []):
        metadata.pop(key, None)
    state.update(case.get("state_overrides", {}))
    features.update(case.get("feature_overrides", {}))

    input_source_path = case.get("input_source_path", metadata.get("source_payload_path"))
    scorer_payload = build_asia_early_normal_breakout_retest_hold_entry_acceptance_payload(
        event_payload=event,
        completed_candles=case.get("completed_candles", fixture["completed_candles"]),
        participation_quality=case.get("participation_quality"),
        input_source_path=input_source_path,
        input_source_category=case.get("input_source_category", "RUNTIME"),
    )
    report = build_entry_acceptance_state(scorer_payload, now=case["now"])
    return report, scorer_payload


def _load_fixture() -> dict[str, Any]:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        pytest.fail("entry acceptance fixture must be a JSON object.")
    return payload
