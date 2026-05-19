from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.app import london_open_transition_shadow_manual as manual


NY = ZoneInfo("America/New_York")


def _phase1_doc(**overrides):
    payload = {
        "source": "DATABENTO_REALTIME_PHASE1",
        "generated_at": "2026-05-19T04:10:00-04:00",
        "freshness_seconds": 600.0,
        "realtime_feed_block_reason": "READY",
        "instrument": "GC",
        "symbol": "GC",
        "observed_session_label": "LONDON_OPEN",
        "canonical_session_label": "LONDON_OPEN",
        "candidate_family": "asiaEarlyNormalBreakoutRetestHoldLong",
        "direction": "long",
        "range_regime": "range_normal",
        "range_expansion_ratio": 1.0,
        "breakout_breaks_prior_1_high": True,
        "signal_retests_and_holds_breakout_level": True,
        "breakout_bar_expansion_is_normal": True,
        "breakout_bar_slope_is_flat": True,
        "bars": [
            {
                "bar_start": "2026-05-19T04:05:00-04:00",
                "bar_end": "2026-05-19T04:10:00-04:00",
                "completed": True,
                "open": 2401.0,
                "high": 2403.0,
                "low": 2400.0,
                "close": 2402.0,
            }
        ],
    }
    payload.update(overrides)
    return payload


def _write_artifact(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_manual_run_writes_latest_state_and_history_only(tmp_path):
    artifact_path = _write_artifact(tmp_path / "phase1.json", _phase1_doc())
    output_root = tmp_path / "shadow"

    report = manual.run_manual_shadow_check(
        input_artifact_path=artifact_path,
        output_root=output_root,
        write_shadow=True,
        now=datetime(2026, 5, 19, 4, 11, tzinfo=NY),
    )

    assert report["mode"] == "manual_shadow_run"
    assert report["candidate"]["accepted"] is True
    assert report["non_routing_flags"]["observer_only"] is True
    assert report["non_routing_flags"]["order_intent_created"] is False
    assert report["non_routing_flags"]["route_attempted"] is False
    assert report["non_routing_flags"]["broker_mutation"] is False
    assert report["non_routing_flags"]["lifecycle_mutation"] is False
    assert report["non_routing_flags"]["live_money_eligible"] is False
    assert sorted(child.name for child in output_root.iterdir()) == [
        "latest_state.json",
        "shadow_diagnostic_history.jsonl",
    ]


def test_preflight_check_does_not_write_without_explicit_request(tmp_path):
    artifact_path = _write_artifact(tmp_path / "phase1.json", _phase1_doc())
    output_root = tmp_path / "shadow"

    report = manual.run_manual_shadow_check(
        input_artifact_path=artifact_path,
        output_root=output_root,
        write_shadow=False,
        now=datetime(2026, 5, 19, 4, 11, tzinfo=NY),
    )

    assert report["mode"] == "preflight_check"
    assert report["wrote_shadow_artifacts"] is False
    assert report["artifact_paths"] == {}
    assert not output_root.exists()


def test_stale_and_wrong_provenance_artifacts_fail_closed(tmp_path):
    stale_path = _write_artifact(
        tmp_path / "stale.json",
        _phase1_doc(generated_at="2026-05-19T03:40:00-04:00"),
    )
    wrong_path = _write_artifact(
        tmp_path / "wrong.json",
        _phase1_doc(source="SIMULATED_BACKFILL"),
    )

    stale = manual.run_manual_shadow_check(
        input_artifact_path=stale_path,
        output_root=tmp_path / "out-stale",
        now=datetime(2026, 5, 19, 4, 11, tzinfo=NY),
    )
    wrong = manual.run_manual_shadow_check(
        input_artifact_path=wrong_path,
        output_root=tmp_path / "out-wrong",
        now=datetime(2026, 5, 19, 4, 11, tzinfo=NY),
    )

    assert stale["candidate"]["accepted"] is False
    assert stale["candidate"]["reject_reason"] == "stale_artifact"
    assert stale["freshness_status"]["state"] == "STALE"
    assert wrong["candidate"]["accepted"] is False
    assert wrong["candidate"]["reject_reason"] == "wrong_provenance"
    assert wrong["provenance_status"]["ok"] is False


def test_phase1_candle_missing_structural_fields_fails_closed(tmp_path):
    doc = _phase1_doc()
    doc.pop("signal_retests_and_holds_breakout_level")
    artifact_path = _write_artifact(tmp_path / "phase1.json", doc)

    report = manual.run_manual_shadow_check(
        input_artifact_path=artifact_path,
        output_root=tmp_path / "shadow",
        now=datetime(2026, 5, 19, 4, 11, tzinfo=NY),
    )

    assert report["candidate"]["accepted"] is False
    assert report["candidate"]["reject_reason"] == "missing_structural_predicate_fields"


def test_manual_source_has_no_order_bridge_route_or_lifecycle_calls():
    sources = [
        Path(manual.__file__).read_text(encoding="utf-8"),
        Path("src/mgc_v05l/app/london_open_transition_shadow_observer.py").read_text(encoding="utf-8"),
    ]
    prohibited = (
        "OrderIntent(",
        "OrderIntentCreatedEvent(",
        ".submit(",
        ".placeOrder(",
        ".cancelOrder(",
        ".route(",
        "PaperBroker(",
        "StateMachine(",
        "strategy_bridge",
        "research_runtime_bridge",
        "mgc_v05l.execution",
        "mgc_v05l.strategy.strategy_engine",
        "mgc_v05l.strategy.state_machine",
        "mgc_v05l.strategy.reconcile",
    )

    for source in sources:
        for token in prohibited:
            assert token not in source
