from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.execution_core.track_b_p0_observe_only_loop import (
    P0_OBSERVE_LOOP_BLOCKED_CONTROL_PLANE,
    P0_OBSERVE_LOOP_BLOCKED_SAFE_STATE,
    P0_OBSERVE_LOOP_COMPLETED,
    P0_OBSERVE_LOOP_ITERATION_OK,
    P0_STRATEGY_IDS,
    TrackBP0ObserveOnlyLoopConfig,
    TrackBP0ObserveOnlyLoopStages,
    run_track_b_p0_observe_only_loop,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 25, 0, 30, tzinfo=timezone.utc)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def seed_candles(root: Path) -> None:
    for symbol, timeframe in (("MGC", "1m"), ("MGC", "5m"), ("MNQ", "5m")):
        write_json(
            root
            / "outputs"
            / "track_b_execution_core"
            / "phase1_runtime_market_data"
            / symbol
            / timeframe
            / "latest_runtime_candles.json",
            {
                "generated_at": aware_now().isoformat(),
                "symbol": symbol,
                "timeframe": timeframe,
                "source_id": "phase1-test",
                "completed_candles_only": True,
                "bars": [
                    {
                        "bar_end": "2026-05-25T00:25:00+00:00",
                        "open": 1,
                        "high": 2,
                        "low": 1,
                        "close": 2,
                        "completed": True,
                    }
                ],
            },
        )


def config(tmp_path: Path, *, iterations: int = 1) -> TrackBP0ObserveOnlyLoopConfig:
    seed_candles(tmp_path)
    return TrackBP0ObserveOnlyLoopConfig(
        repo_root=tmp_path,
        output_path=Path("outputs/track_b_execution_core/p0_observe_only/latest_p0_observe_only_loop.json"),
        event_log_path=Path("outputs/track_b_execution_core/p0_observe_only/p0_observe_only_loop_events.jsonl"),
        iterations=iterations,
        sleep_seconds=0,
        update_operator_status=False,
    )


class Calls:
    def __init__(self) -> None:
        self.phase1 = 0
        self.proof = 0
        self.control = 0
        self.asian = 0
        self.session = 0
        self.snap = 0
        self.cycle = 0
        self.sleep = 0
        self.submit_flags: list[tuple[bool, bool]] = []


def ready_snapshot(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "generated_at": aware_now().isoformat(),
        "classification": "CONTROL_PLANE_SNAPSHOT_READY",
        "control_plane_snapshot_id": "snapshot-1",
        "shared_truth_refresh_generation_id": "generation-1",
        "shared_truth_coherence_status": "COHERENT",
        "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
        "supervisor_mode": "READY_FOR_OPERATOR_START",
        "proof_window_status": "ready",
        "safe_state_classification": "SAFE_STATE_NORMAL",
        "safe_state_observe_only": False,
        "safe_state_submit_allowed": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "blockers": [],
        "warnings": [],
    }
    payload.update(overrides)
    return payload


def fake_stages(tmp_path: Path, calls: Calls, *, snapshot: dict[str, object] | None = None) -> TrackBP0ObserveOnlyLoopStages:
    snapshot_payload = snapshot or ready_snapshot()

    def phase1(_config: TrackBP0ObserveOnlyLoopConfig, _now: datetime) -> dict[str, object]:
        calls.phase1 += 1
        return {
            "generated_at": aware_now().isoformat(),
            "row_count": 2,
            "ready_ticker_count": 2,
            "rows": [
                {"symbol": "MGC", "runtime_candles_ready": True, "derived_features_ready": True},
                {"symbol": "MNQ", "runtime_candles_ready": True, "derived_features_ready": True},
            ],
        }

    def proof(_config: TrackBP0ObserveOnlyLoopConfig, _now: datetime) -> dict[str, object]:
        calls.proof += 1
        return {"classification": "READY_FOR_PROOF", "paper_proof_invoked": False, "live_money_eligible": False}

    def control(_config: TrackBP0ObserveOnlyLoopConfig, _now: datetime) -> dict[str, object]:
        calls.control += 1
        return dict(snapshot_payload)

    def asian(_config: TrackBP0ObserveOnlyLoopConfig, _now: datetime) -> SimpleNamespace:
        calls.asian += 1
        state_path = write_json(
            tmp_path / "outputs/track_b_execution_core/asian_drift_state/latest_asian_drift_5m_state_snapshot.json",
            {"strategy_id": "asian_drift_v1"},
        )
        report = {
            "asian_drift_watch_verdict": "ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION",
            "asian_drift_state_snapshot_path": str(state_path),
            "submit_allowed": False,
            "no_mutation": True,
        }
        return SimpleNamespace(report=report, report_json=tmp_path / "asian_report.json")

    def session(_config: TrackBP0ObserveOnlyLoopConfig, _now: datetime) -> SimpleNamespace:
        calls.session += 1
        pause = write_json(tmp_path / "pause.json", {"strategy_id": "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"})
        breakout = write_json(
            tmp_path / "breakout.json",
            {"strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"},
        )
        return SimpleNamespace(
            report={"session_strategy_envelope_producer_verdict": "TRACK_B_SESSION_STRATEGY_ENVELOPE_PRODUCER_WROTE_ENVELOPES"},
            report_json=tmp_path / "session_report.json",
            asia_early_pause_resume_short_event_json=pause,
            asia_early_normal_breakout_retest_hold_long_event_json=breakout,
        )

    def snap(_config: TrackBP0ObserveOnlyLoopConfig, _now: datetime) -> SimpleNamespace:
        calls.snap += 1
        bear = write_json(tmp_path / "bear.json", {"strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1"})
        bull = write_json(tmp_path / "bull.json", {"strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1"})
        return SimpleNamespace(
            report={"snap_turn_envelope_producer_verdict": "TRACK_B_SNAP_TURN_ENVELOPE_PRODUCER_WROTE_ENVELOPES"},
            report_json=tmp_path / "snap_report.json",
            first_bear_snap_turn_event_json=bear,
            first_bull_snap_turn_event_json=bull,
        )

    def cycle(
        cycle_config: TrackBP0ObserveOnlyLoopConfig,
        _now: datetime,
        envelope_context: dict[str, object],
    ) -> SimpleNamespace:
        calls.cycle += 1
        # The default loop stage owns construction of the real cycle config.
        # The fake stage receives envelope context and proves no mutation by
        # returning the same public report shape.
        assert cycle_config.update_operator_status is False
        assert envelope_context["asian_drift_event_json"]
        calls.submit_flags.append((False, False))
        return SimpleNamespace(
            report_json=tmp_path / "cycle_report.json",
            report={
                "multi_strategy_runtime_cycle_verdict": "TRACK_B_MULTI_STRATEGY_RUNTIME_NO_SIGNAL_NO_MUTATION",
                "multi_strategy_cycle_authorization_classification": "MULTI_STRATEGY_CYCLE_OBSERVE_ONLY",
                "cycle_submit_allowed": False,
                "cycle_broker_mutation_allowed": False,
                "candidate_signals": [],
                "suppressed_signals": [],
                "evaluated_strategies": [
                    {
                        "strategy_id": strategy_id,
                        "strategy_runtime_verdict": "NO_SIGNAL_NO_MUTATION",
                        "decision": "NO_SIGNAL",
                        "decision_reason": "test no setup",
                        "signal_emitted": False,
                        "rule_conditions": {"required_predicate": False},
                        "rule_blockers": ["required_predicate=false_or_missing"],
                    }
                    for strategy_id in P0_STRATEGY_IDS
                ],
                "primary_blocker": None,
                "required_next_action": "continue observe-only",
            },
        )

    return TrackBP0ObserveOnlyLoopStages(
        phase1_readiness=phase1,
        proof_readiness=proof,
        control_plane_snapshot=control,
        asian_drift=asian,
        session_envelopes=session,
        snap_turn_envelopes=snap,
        multi_strategy_cycle=cycle,
        sleep=lambda seconds: setattr(calls, "sleep", calls.sleep + 1),
    )


def test_one_iteration_success_writes_latest_and_event_log(tmp_path: Path) -> None:
    calls = Calls()
    payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path),
        stages=fake_stages(tmp_path, calls),
        now_factory=aware_now,
        loop_id="loop-test",
    )

    assert payload["classification"] == P0_OBSERVE_LOOP_COMPLETED
    assert payload["completed_iterations"] == 1
    assert payload["iterations"][0]["classification"] == P0_OBSERVE_LOOP_ITERATION_OK
    assert payload["iterations"][0]["cycle_verdict"] == "TRACK_B_MULTI_STRATEGY_RUNTIME_NO_SIGNAL_NO_MUTATION"
    assert payload["iterations"][0]["submit_attempted"] is False
    assert payload["iterations"][0]["broker_state_mutated"] is False
    assert calls.phase1 == calls.proof == calls.control == calls.asian == calls.session == calls.snap == calls.cycle == 1
    latest = tmp_path / "outputs/track_b_execution_core/p0_observe_only/latest_p0_observe_only_loop.json"
    events = tmp_path / "outputs/track_b_execution_core/p0_observe_only/p0_observe_only_loop_events.jsonl"
    assert json.loads(latest.read_text(encoding="utf-8"))["classification"] == P0_OBSERVE_LOOP_COMPLETED
    assert len(events.read_text(encoding="utf-8").strip().splitlines()) == 1


def test_loop_repeats_refresh_each_iteration(tmp_path: Path) -> None:
    calls = Calls()
    payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path, iterations=2),
        stages=fake_stages(tmp_path, calls),
        now_factory=aware_now,
    )

    assert payload["classification"] == P0_OBSERVE_LOOP_COMPLETED
    assert payload["completed_iterations"] == 2
    assert calls.phase1 == 2
    assert calls.asian == 2
    assert calls.session == 2
    assert calls.snap == 2
    assert calls.cycle == 2


def test_loop_stops_on_control_plane_block(tmp_path: Path) -> None:
    calls = Calls()
    payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path, iterations=3),
        stages=fake_stages(
            tmp_path,
            calls,
            snapshot=ready_snapshot(classification="CONTROL_PLANE_SNAPSHOT_BLOCKED"),
        ),
        now_factory=aware_now,
    )

    assert payload["classification"] == P0_OBSERVE_LOOP_BLOCKED_CONTROL_PLANE
    assert payload["completed_iterations"] == 1
    assert calls.asian == 0
    assert calls.cycle == 0


def test_loop_stops_on_safe_state_hard_hold(tmp_path: Path) -> None:
    calls = Calls()
    payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path),
        stages=fake_stages(
            tmp_path,
            calls,
            snapshot=ready_snapshot(safe_state_classification="SAFE_STATE_HARD_HOLD"),
        ),
        now_factory=aware_now,
    )

    assert payload["classification"] == P0_OBSERVE_LOOP_BLOCKED_SAFE_STATE
    assert "SAFE_STATE_HARD_HOLD" in payload["iterations"][0]["primary_blocker"]
    assert calls.asian == 0
    assert calls.cycle == 0


def test_missing_anchor_diagnostic_surfaces_in_iteration_report(tmp_path: Path) -> None:
    calls = Calls()
    stages = fake_stages(tmp_path, calls)

    def asian_with_late_join(_config: TrackBP0ObserveOnlyLoopConfig, _now: datetime) -> SimpleNamespace:
        calls.asian += 1
        state_path = write_json(
            tmp_path / "outputs/track_b_execution_core/asian_drift_state/latest_asian_drift_5m_state_snapshot.json",
            {"strategy_id": "asian_drift_v1"},
        )
        return SimpleNamespace(
            report_json=tmp_path / "asian_report.json",
            report={
                "asian_drift_watch_verdict": "ASIAN_DRIFT_BLOCKED_MISSING_SESSION_ANCHOR_CONTEXT",
                "asian_drift_diagnostic_classification": "ASIAN_DRIFT_BLOCKED_MISSING_SESSION_ANCHOR_CONTEXT",
                "late_join_classification": "ASIAN_DRIFT_LATE_JOIN_STRONG_DRIFT_OBSERVED",
                "late_join_diagnostic": True,
                "anchor_required": True,
                "anchor_observed": False,
                "missing_anchor_reason": "18:00 ET anchor missing",
                "late_join_policy": "DIAGNOSTIC_ONLY",
                "hypothetical_late_join_score": 5.72,
                "operator_explanation": (
                    "Strong drift observed, but 18:00 ET session anchor context is missing; no signal by design."
                ),
                "asian_drift_state_snapshot_path": str(state_path),
                "submit_allowed": False,
                "no_mutation": True,
            },
        )

    payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path),
        stages=TrackBP0ObserveOnlyLoopStages(
            phase1_readiness=stages.phase1_readiness,
            proof_readiness=stages.proof_readiness,
            control_plane_snapshot=stages.control_plane_snapshot,
            asian_drift=asian_with_late_join,
            session_envelopes=stages.session_envelopes,
            snap_turn_envelopes=stages.snap_turn_envelopes,
            multi_strategy_cycle=stages.multi_strategy_cycle,
            sleep=stages.sleep,
        ),
        now_factory=aware_now,
    )

    diagnostic = payload["iterations"][0]["late_join_asian_drift_diagnostic"]
    assert diagnostic["late_join_diagnostic"] is True
    assert diagnostic["late_join_policy"] == "DIAGNOSTIC_ONLY"
    assert diagnostic["submit_allowed"] is False
    assert "Strong drift observed" in diagnostic["operator_explanation"]


def test_dashboard_projection_is_not_consumed(tmp_path: Path) -> None:
    calls = Calls()
    dashboard_projection = tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_control_plane_snapshot.json"
    write_json(
        dashboard_projection,
        {
            "projection_only": True,
            "not_routing_authority": True,
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_state_classification": "SAFE_STATE_NORMAL",
        },
    )

    payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path),
        stages=fake_stages(tmp_path, calls),
        now_factory=aware_now,
    )

    assert payload["classification"] == P0_OBSERVE_LOOP_COMPLETED
    assert calls.control == 1
    assert calls.cycle == 1
