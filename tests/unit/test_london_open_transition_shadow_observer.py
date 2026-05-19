from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.app import london_open_transition_shadow_observer as observer


NY = ZoneInfo("America/New_York")


def _artifact(**overrides):
    payload = {
        "phase1_artifact_present": True,
        "required_market_data_provenance": "DATABENTO_REALTIME_PHASE1",
        "generated_at": datetime(2026, 5, 19, 4, 10, tzinfo=NY),
        "timestamp": datetime(2026, 5, 19, 4, 10, tzinfo=NY),
        "bar_id": "GC-20260519-0410",
        "instrument": "GC",
        "session": "LONDON_OPEN",
        "canonical_session_label": "LONDON_OPEN",
        "candidate_family": "asiaEarlyNormalBreakoutRetestHoldLong",
        "direction": "long",
        "range_regime": "range_normal",
        "range_expansion_ratio": 1.0,
        "breakout_breaks_prior_1_high": True,
        "signal_retests_and_holds_breakout_level": True,
        "breakout_bar_expansion_is_normal": True,
        "breakout_bar_slope_is_flat": True,
        "freshness_state": "FRESH",
        "readiness_state": "READY_SUBMIT_CAPABLE",
        "startup_catchup_flag": False,
        "next_bar_open": 2400.5,
    }
    payload.update(overrides)
    return payload


def _evaluate(payload):
    return observer.evaluate_london_open_transition_shadow_candidate(
        payload,
        now=datetime(2026, 5, 19, 4, 11, tzinfo=NY),
    )


def test_post_04_plus_10m_is_accepted_and_non_routing(tmp_path):
    decision = observer.LondonOpenTransitionShadowObserver(output_root=tmp_path, now=datetime(2026, 5, 19, 4, 11, tzinfo=NY)).evaluate_and_emit(
        _artifact()
    )

    assert decision["accepted"] is True
    assert decision["decision"] == "ACCEPTED_SHADOW_CANDIDATE"
    assert decision["candidate_id"] == observer.CANDIDATE_ID
    assert decision["non_authoritative"] is True
    assert decision["diagnostic_only"] is True
    assert decision["order_intent_created"] is False
    assert decision["route_attempted"] is False
    assert decision["broker_state_mutated"] is False
    assert decision["lifecycle_mutated"] is False
    assert decision["route_capable"] is False
    assert decision["submit_capable"] is False

    latest = json.loads((tmp_path / "latest_state.json").read_text(encoding="utf-8"))
    history_rows = (tmp_path / "shadow_diagnostic_history.jsonl").read_text(encoding="utf-8").splitlines()
    assert latest["candidate_id"] == observer.CANDIDATE_ID
    assert len(history_rows) == 1
    assert json.loads(history_rows[0])["accepted"] is True


def test_exact_04_boundary_is_rejected():
    decision = _evaluate(_artifact(timestamp=datetime(2026, 5, 19, 4, 0, tzinfo=NY)))

    assert decision["accepted"] is False
    assert decision["reject_reason"] == "exact_04_00_boundary"
    assert decision["order_intent_created"] is False
    assert decision["route_attempted"] is False


def test_plus_15m_is_rejected():
    decision = _evaluate(_artifact(timestamp=datetime(2026, 5, 19, 4, 15, tzinfo=NY)))

    assert decision["accepted"] is False
    assert decision["reject_reason"] == "outside_post_04_10m_window"


def test_compressed_range_is_rejected():
    decision = _evaluate(_artifact(range_regime="range_compressed", range_expansion_ratio=0.8))

    assert decision["accepted"] is False
    assert decision["reject_reason"] == "compressed_range"


def test_wrong_and_stale_provenance_reject_fail_closed():
    wrong = _evaluate(_artifact(required_market_data_provenance="SIMULATED_BACKFILL"))
    stale = _evaluate(_artifact(generated_at=datetime(2026, 5, 19, 3, 55, tzinfo=NY)))

    assert wrong["accepted"] is False
    assert wrong["reject_reason"] == "wrong_provenance"
    assert stale["accepted"] is False
    assert stale["reject_reason"] == "stale_artifact"


def test_missing_phase1_and_structural_fields_reject_fail_closed():
    missing_artifact = _evaluate(_artifact(phase1_artifact_present=False))
    missing_structure = _artifact()
    missing_structure.pop("signal_retests_and_holds_breakout_level")

    structural_decision = _evaluate(missing_structure)

    assert missing_artifact["accepted"] is False
    assert missing_artifact["reject_reason"] == "missing_phase1_artifact"
    assert structural_decision["accepted"] is False
    assert structural_decision["reject_reason"] == "missing_structural_predicate_fields"


def test_missing_freshness_and_readiness_reject_fail_closed():
    missing_freshness = _artifact()
    missing_freshness.pop("freshness_state")
    missing_readiness = _artifact()
    missing_readiness.pop("readiness_state")

    freshness_decision = _evaluate(missing_freshness)
    readiness_decision = _evaluate(missing_readiness)

    assert freshness_decision["accepted"] is False
    assert freshness_decision["reject_reason"] == "missing_freshness_state"
    assert readiness_decision["accepted"] is False
    assert readiness_decision["reject_reason"] == "missing_readiness_state"


def test_observer_source_has_no_order_broker_route_or_lifecycle_calls():
    source = Path(observer.__file__).read_text(encoding="utf-8")
    prohibited_call_tokens = (
        "OrderIntent(",
        "OrderIntentCreatedEvent(",
        ".submit(",
        ".placeOrder(",
        ".cancelOrder(",
        ".close(",
        ".route(",
        "PaperBroker(",
        "StateMachine(",
    )
    prohibited_imports = (
        "mgc_v05l.execution",
        "mgc_v05l.strategy.strategy_engine",
        "mgc_v05l.strategy.state_machine",
        "mgc_v05l.strategy.reconcile",
    )

    for token in prohibited_call_tokens + prohibited_imports:
        assert token not in source
