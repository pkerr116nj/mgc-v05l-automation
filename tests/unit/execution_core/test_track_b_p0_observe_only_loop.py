from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.execution_core.track_b_p0_observe_only_loop import (
    APPROVED_PAPER_ROSTER,
    APPROVED_PAPER_STRATEGY_IDS,
    OLDER_APPROVED_PAPER_STRATEGY_IDS,
    P0_OBSERVE_LOOP_BLOCKED_CONTROL_PLANE,
    P0_OBSERVE_LOOP_BLOCKED_SAFE_STATE,
    P0_OBSERVE_LOOP_COMPLETED,
    P0_OBSERVE_LOOP_ITERATION_OK,
    P0_GUARDED_PAPER_LOOP_BLOCKED_ROSTER,
    P0_GUARDED_PAPER_LOOP_CANDIDATE_HANDOFF,
    P0_GUARDED_PAPER_LOOP_COMPLETED,
    P0_GUARDED_PAPER_LOOP_NO_CANDIDATE,
    P0_LOOP_MODE_GUARDED_PAPER,
    P0_LOOP_MODE_OBSERVE_ONLY,
    P0_LOOP_MODE_SUBMIT_DISABLED,
    P0_SUBMIT_DISABLED_LOOP_CANDIDATE_PREVIEW_READY,
    P0_SUBMIT_DISABLED_LOOP_NO_CANDIDATE,
    P0_SUBMIT_DISABLED_LOOP_SAFETY_BOUNDARY_FAILED,
    P0_ROSTER,
    P0_STRATEGY_IDS,
    TRACK_B_GUARDED_PAPER_ROSTER_BLOCKED_INVALID_CONFIG,
    TRACK_B_GUARDED_PAPER_ROSTER_READY_WITH_REJECTIONS,
    TrackBP0ObserveOnlyLoopConfig,
    TrackBP0ObserveOnlyLoopStages,
    _multi_strategy_cycle_config,
    run_track_b_p0_observe_only_loop,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 25, 0, 30, tzinfo=timezone.utc)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def seed_candles(root: Path) -> None:
    for symbol, timeframe in (("MGC", "1m"), ("MGC", "5m"), ("MNQ", "1m"), ("MNQ", "5m")):
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


def config(
    tmp_path: Path,
    *,
    iterations: int = 1,
    mode: str = P0_LOOP_MODE_OBSERVE_ONLY,
    roster_name: str = P0_ROSTER,
    roster_config_path: Path | None = None,
) -> TrackBP0ObserveOnlyLoopConfig:
    seed_candles(tmp_path)
    return TrackBP0ObserveOnlyLoopConfig(
        repo_root=tmp_path,
        mode=mode,
        roster_name=roster_name,
        enabled_strategy_ids=APPROVED_PAPER_STRATEGY_IDS if roster_name == APPROVED_PAPER_ROSTER else P0_STRATEGY_IDS,
        roster_config_path=roster_config_path,
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
        "runtime_resume_proposed_next_runtime_generation_id": "runtime-generation-1",
        "runtime_resume_action_policy": "NEW_RUNTIME_GENERATION_ALLOWED",
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "blockers": [],
        "warnings": [],
    }
    payload.update(overrides)
    return payload


def fake_stages(
    tmp_path: Path,
    calls: Calls,
    *,
    snapshot: dict[str, object] | None = None,
    candidate_signal: bool = False,
    cycle_report_overrides: dict[str, object] | None = None,
) -> TrackBP0ObserveOnlyLoopStages:
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
        write_json(
            tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
            snapshot_payload,
        )
        write_json(
            tmp_path / "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json",
            {
                "generated_at": aware_now().isoformat(),
                "safe_state_classification": snapshot_payload.get("safe_state_classification", "SAFE_STATE_NORMAL"),
                "control_plane_snapshot_id": snapshot_payload.get("control_plane_snapshot_id"),
                "shared_truth_generation_id": snapshot_payload.get("shared_truth_refresh_generation_id"),
                "runtime_generation_id": snapshot_payload.get(
                    "runtime_resume_proposed_next_runtime_generation_id",
                    "runtime-generation-1",
                ),
                "submit_allowed": snapshot_payload.get("safe_state_submit_allowed", True),
                "broker_mutation_allowed": True,
                "observe_only": snapshot_payload.get("safe_state_observe_only", False),
                "tripped_limits": [],
                "live_money_eligible": snapshot_payload.get("live_money_eligible", False),
                "paper_proof_invoked": snapshot_payload.get("paper_proof_invoked", False),
            },
        )
        write_json(
            tmp_path / "outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json",
            {
                "generated_at": aware_now().isoformat(),
                "supervisor_decision_id": snapshot_payload.get("runtime_supervisor_decision_id", "supervisor-1"),
                "classification": snapshot_payload.get("runtime_supervisor_classification"),
                "live_money_eligible": snapshot_payload.get("live_money_eligible", False),
            },
        )
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
        guarded = cycle_config.mode == P0_LOOP_MODE_GUARDED_PAPER
        calls.submit_flags.append((guarded, guarded))
        candidate_signals = []
        chosen_strategy_id = None
        if candidate_signal:
            chosen_strategy_id = "MNQ_FIRST_BULL_SNAP_TURN_V1"
            candidate_signals = [
                {
                    "strategy_id": chosen_strategy_id,
                    "rule_mode": "MNQ_FIRST_BULL_SNAP_TURN_V1",
                    "signal_source": "MNQ_FIRST_BULL_SNAP_TURN_V1",
                    "real_strategy_signal": True,
                    "signal_emitted": True,
                    "signal_direction": "LONG",
                    "decision": "LONG",
                    "paper_eligible": True,
                    "live_money_eligible": False,
                }
            ]
        report = {
            "multi_strategy_runtime_cycle_verdict": (
                "TRACK_B_MULTI_STRATEGY_RUNTIME_SIGNAL_READY_NO_SUBMIT"
                if candidate_signal
                else "TRACK_B_MULTI_STRATEGY_RUNTIME_NO_SIGNAL_NO_MUTATION"
            ),
            "multi_strategy_cycle_authorization_classification": "MULTI_STRATEGY_CYCLE_OBSERVE_ONLY",
            "cycle_submit_allowed": False,
            "cycle_broker_mutation_allowed": False,
            "chosen_strategy_id": chosen_strategy_id,
            "candidate_signals": candidate_signals,
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
        }
        report.update(cycle_report_overrides or {})
        return SimpleNamespace(
            report_json=tmp_path / "cycle_report.json",
            report=report,
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
    assert payload["iterations"][0]["p0_near_miss_shadow"]["submit_allowed"] is False
    assert payload["iterations"][0]["p0_near_miss_shadow"]["research_only"] is True
    assert payload["iterations"][0]["submit_attempted"] is False
    assert payload["iterations"][0]["broker_state_mutated"] is False
    assert calls.phase1 == calls.proof == calls.control == calls.asian == calls.session == calls.snap == calls.cycle == 1
    latest = tmp_path / "outputs/track_b_execution_core/p0_observe_only/latest_p0_observe_only_loop.json"
    events = tmp_path / "outputs/track_b_execution_core/p0_observe_only/p0_observe_only_loop_events.jsonl"
    assert json.loads(latest.read_text(encoding="utf-8"))["classification"] == P0_OBSERVE_LOOP_COMPLETED
    assert len(events.read_text(encoding="utf-8").strip().splitlines()) == 1


def test_default_mode_is_observe_only(tmp_path: Path) -> None:
    assert config(tmp_path).mode == P0_LOOP_MODE_OBSERVE_ONLY


def test_default_roster_is_original_p0(tmp_path: Path) -> None:
    payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path),
        stages=fake_stages(tmp_path, Calls()),
        now_factory=aware_now,
    )

    assert payload["roster_name"] == P0_ROSTER
    assert tuple(payload["enabled_strategy_ids"]) == P0_STRATEGY_IDS
    assert tuple(payload["older_approved_paper_strategy_ids"]) == OLDER_APPROVED_PAPER_STRATEGY_IDS


def test_approved_paper_roster_expands_enabled_strategy_ids(tmp_path: Path) -> None:
    calls = Calls()
    payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path, mode=P0_LOOP_MODE_GUARDED_PAPER, roster_name=APPROVED_PAPER_ROSTER),
        stages=fake_stages(tmp_path, calls),
        now_factory=aware_now,
        loop_id="approved-paper-roster",
    )

    assert payload["roster_name"] == APPROVED_PAPER_ROSTER
    assert tuple(payload["enabled_strategy_ids"]) == APPROVED_PAPER_STRATEGY_IDS
    assert "FIRST_BULL_SNAP_TURN_V1" in payload["enabled_strategy_ids"]
    assert "MNQ_US_DERIVATIVE_BEAR_TURN_V1" in payload["enabled_strategy_ids"]
    assert payload["classification"] == P0_GUARDED_PAPER_LOOP_COMPLETED


def test_hot_reload_roster_file_controls_enabled_strategy_ids(tmp_path: Path) -> None:
    roster = write_json(
        tmp_path / "config/track_b_guarded_paper_roster.json",
        {
            "schema_version": "track_b_guarded_paper_roster_v1",
            "paper_account_id": "DUM882026",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "enabled_strategy_ids": [
                "asian_drift_v1",
                "FIRST_BULL_SNAP_TURN_V1",
                "mgc_ema_momentum_reclaim_long_v1",
            ],
            "disabled_strategy_ids": ["asian_drift_v1"],
            "max_quantity_per_strategy": 1,
        },
    )

    payload = run_track_b_p0_observe_only_loop(
        config=config(
            tmp_path,
            mode=P0_LOOP_MODE_GUARDED_PAPER,
            roster_name=APPROVED_PAPER_ROSTER,
            roster_config_path=roster.relative_to(tmp_path),
        ),
        stages=fake_stages(tmp_path, Calls()),
        now_factory=aware_now,
        loop_id="hot-roster",
    )

    roster_validation = payload["iterations"][0]["roster_validation"]
    assert roster_validation["classification"] == TRACK_B_GUARDED_PAPER_ROSTER_READY_WITH_REJECTIONS
    assert roster_validation["enabled_strategy_ids"] == ["FIRST_BULL_SNAP_TURN_V1"]
    assert roster_validation["hot_reload_enabled"] is True
    assert roster_validation["dashboard_projection_consumed"] is False
    assert "asian_drift_v1" in roster_validation["rejected_strategy_ids"]
    assert "mgc_ema_momentum_reclaim_long_v1" in roster_validation["rejected_strategy_ids"]


def test_hot_reload_roster_blocks_when_no_hardened_strategy_remains(tmp_path: Path) -> None:
    roster = write_json(
        tmp_path / "config/track_b_guarded_paper_roster.json",
        {
            "schema_version": "track_b_guarded_paper_roster_v1",
            "paper_account_id": "DUM882026",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "enabled_strategy_ids": ["mgc_ema_momentum_reclaim_long_v1"],
            "disabled_strategy_ids": [],
            "max_quantity_per_strategy": 1,
        },
    )
    calls = Calls()

    payload = run_track_b_p0_observe_only_loop(
        config=config(
            tmp_path,
            mode=P0_LOOP_MODE_GUARDED_PAPER,
            roster_name=APPROVED_PAPER_ROSTER,
            roster_config_path=roster.relative_to(tmp_path),
        ),
        stages=fake_stages(tmp_path, calls),
        now_factory=aware_now,
        loop_id="empty-hot-roster",
    )

    assert payload["classification"] == P0_GUARDED_PAPER_LOOP_BLOCKED_ROSTER
    assert payload["iterations"][0]["roster_validation"]["enabled_strategy_ids"] == []
    assert "No guarded PAPER strategies" in payload["iterations"][0]["primary_blocker"]
    assert calls.asian == 0
    assert calls.cycle == 0


def test_guarded_paper_roster_blocks_malformed_authority_config(tmp_path: Path) -> None:
    roster = write_json(
        tmp_path / "config/track_b_guarded_paper_roster.json",
        {
            "enabled_strategy_ids": ["FIRST_BULL_SNAP_TURN_V1"],
        },
    )
    calls = Calls()

    payload = run_track_b_p0_observe_only_loop(
        config=config(
            tmp_path,
            mode=P0_LOOP_MODE_GUARDED_PAPER,
            roster_name=APPROVED_PAPER_ROSTER,
            roster_config_path=roster.relative_to(tmp_path),
        ),
        stages=fake_stages(tmp_path, calls),
        now_factory=aware_now,
        loop_id="malformed-hot-roster",
    )

    roster_validation = payload["iterations"][0]["roster_validation"]
    assert payload["classification"] == P0_GUARDED_PAPER_LOOP_BLOCKED_ROSTER
    assert roster_validation["classification"] == TRACK_B_GUARDED_PAPER_ROSTER_BLOCKED_INVALID_CONFIG
    assert "schema_version" in roster_validation["primary_blocker"]
    assert roster_validation["enabled_strategy_ids"] == []
    assert calls.asian == 0
    assert calls.cycle == 0


def test_approved_paper_cycle_config_wires_all_older_event_paths(tmp_path: Path) -> None:
    cfg = config(tmp_path, mode=P0_LOOP_MODE_GUARDED_PAPER, roster_name=APPROVED_PAPER_ROSTER)
    envelope_context = {
        "asian_drift_event_json": tmp_path / "asian.json",
        "pause_resume_short_event_json": tmp_path / "pause.json",
        "breakout_retest_hold_long_event_json": tmp_path / "breakout.json",
        "first_bull_snap_turn_event_json": tmp_path / "first_bull.json",
        "first_bear_snap_turn_event_json": tmp_path / "first_bear.json",
        "london_late_pause_resume_short_event_json": tmp_path / "london.json",
        "asia_late_flat_pullback_pause_resume_long_event_json": tmp_path / "asia_late.json",
        "us_derivative_bear_turn_event_json": tmp_path / "us_bear.json",
        "mnq_us_derivative_bear_turn_event_json": tmp_path / "mnq_us_bear.json",
        "mnq_first_bear_snap_turn_event_json": tmp_path / "mnq_bear.json",
        "mnq_first_bull_snap_turn_event_json": tmp_path / "mnq_bull.json",
        "us_late_pause_resume_long_event_json": tmp_path / "us_late.json",
    }

    cycle_config = _multi_strategy_cycle_config(config=cfg, envelope_context=envelope_context)

    assert cycle_config.enabled_strategy_ids == APPROVED_PAPER_STRATEGY_IDS
    assert cycle_config.submit_paper is True
    assert cycle_config.confirm_paper_submit is True
    assert cycle_config.first_bull_snap_turn_event_json == tmp_path / "first_bull.json"
    assert cycle_config.first_bear_snap_turn_event_json == tmp_path / "first_bear.json"
    assert cycle_config.london_late_pause_resume_short_event_json == tmp_path / "london.json"
    assert cycle_config.asia_late_flat_pullback_pause_resume_long_event_json == tmp_path / "asia_late.json"
    assert cycle_config.us_derivative_bear_turn_event_json == tmp_path / "us_bear.json"
    assert cycle_config.mnq_us_derivative_bear_turn_event_json == tmp_path / "mnq_us_bear.json"
    assert cycle_config.us_late_pause_resume_long_event_json == tmp_path / "us_late.json"


def test_submit_disabled_mode_with_no_candidate_stays_safe(tmp_path: Path) -> None:
    calls = Calls()
    payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path, mode=P0_LOOP_MODE_SUBMIT_DISABLED),
        stages=fake_stages(tmp_path, calls),
        now_factory=aware_now,
        loop_id="submit-disabled-no-candidate",
    )

    iteration = payload["iterations"][0]
    assert payload["classification"] == P0_SUBMIT_DISABLED_LOOP_NO_CANDIDATE
    assert payload["loop_mode"] == P0_LOOP_MODE_SUBMIT_DISABLED
    assert iteration["classification"] == P0_SUBMIT_DISABLED_LOOP_NO_CANDIDATE
    assert iteration["candidate_signal_count"] == 0
    assert iteration["submit_authorization_preview"]["would_submit_preview"] is False
    assert iteration["final_submit_allowed"] is False
    assert iteration["broker_adapter_constructed"] is False
    assert iteration["submit_limit_order_called"] is False
    assert iteration["broker_state_mutated"] is False


def test_guarded_paper_mode_with_no_candidate_runs_hardened_loop_without_submit(tmp_path: Path) -> None:
    calls = Calls()
    payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path, mode=P0_LOOP_MODE_GUARDED_PAPER),
        stages=fake_stages(tmp_path, calls),
        now_factory=aware_now,
        loop_id="guarded-paper-no-candidate",
    )

    iteration = payload["iterations"][0]
    assert payload["classification"] == P0_GUARDED_PAPER_LOOP_COMPLETED
    assert payload["loop_mode"] == P0_LOOP_MODE_GUARDED_PAPER
    assert payload["guarded_paper_enabled"] is True
    assert iteration["classification"] == P0_GUARDED_PAPER_LOOP_NO_CANDIDATE
    assert iteration["candidate_signal_count"] == 0
    assert iteration["submit_delegation_forced_off"] is False
    assert iteration["submit_attempted"] is False
    assert iteration["broker_state_mutated"] is False
    assert calls.submit_flags == [(True, True)]


def test_guarded_paper_mode_candidate_reaches_handoff_boundary(tmp_path: Path) -> None:
    calls = Calls()
    payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path, mode=P0_LOOP_MODE_GUARDED_PAPER),
        stages=fake_stages(
            tmp_path,
            calls,
            candidate_signal=True,
            cycle_report_overrides={
                "multi_strategy_cycle_authorization_classification": "MULTI_STRATEGY_CYCLE_AUTHORIZED",
                "cycle_submit_allowed": True,
                "cycle_broker_mutation_allowed": True,
                "paper_runner_report_path": str(tmp_path / "paper_runner.json"),
                "submit_attempted": False,
                "broker_state_mutated": False,
            },
        ),
        now_factory=aware_now,
        loop_id="guarded-paper-candidate",
    )

    iteration = payload["iterations"][0]
    assert payload["classification"] == P0_GUARDED_PAPER_LOOP_CANDIDATE_HANDOFF
    assert iteration["classification"] == P0_GUARDED_PAPER_LOOP_CANDIDATE_HANDOFF
    assert iteration["candidate_signal_count"] == 1
    assert iteration["chosen_strategy_id"] == "MNQ_FIRST_BULL_SNAP_TURN_V1"
    assert iteration["final_submit_allowed"] is True
    assert iteration["broker_adapter_constructed"] is True
    assert iteration["broker_state_mutated"] is False


def test_submit_disabled_mode_with_candidate_builds_preview_without_submit(tmp_path: Path) -> None:
    calls = Calls()
    payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path, mode=P0_LOOP_MODE_SUBMIT_DISABLED),
        stages=fake_stages(tmp_path, calls, candidate_signal=True),
        now_factory=aware_now,
        loop_id="submit-disabled-candidate",
    )

    iteration = payload["iterations"][0]
    preview = iteration["submit_authorization_preview"]
    assert payload["classification"] == P0_SUBMIT_DISABLED_LOOP_CANDIDATE_PREVIEW_READY
    assert iteration["classification"] == P0_SUBMIT_DISABLED_LOOP_CANDIDATE_PREVIEW_READY
    assert iteration["candidate_signal_count"] == 1
    assert iteration["chosen_strategy_id"] == "MNQ_FIRST_BULL_SNAP_TURN_V1"
    assert preview["would_submit_preview"] is True
    assert preview["authorization_classification"] == "MULTI_STRATEGY_CYCLE_AUTHORIZED"
    assert preview["final_submit_allowed"] is False
    assert preview["broker_mutation_allowed"] is False
    assert preview["execution_enabled"] is False
    assert preview["broker_adapter_constructed"] is False
    assert preview["submit_limit_order_called"] is False
    assert iteration["final_submit_allowed"] is False
    assert iteration["broker_adapter_constructed"] is False
    assert iteration["submit_limit_order_called"] is False
    assert iteration["broker_state_mutated"] is False


def test_submit_disabled_mode_stops_if_broker_boundary_reached(tmp_path: Path) -> None:
    calls = Calls()
    payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path, mode=P0_LOOP_MODE_SUBMIT_DISABLED),
        stages=fake_stages(
            tmp_path,
            calls,
            candidate_signal=True,
            cycle_report_overrides={
                "paper_runner_report_path": str(tmp_path / "paper_runner.json"),
                "submit_attempted": True,
            },
        ),
        now_factory=aware_now,
        loop_id="submit-disabled-safety-failure",
    )

    iteration = payload["iterations"][0]
    assert payload["classification"] == P0_SUBMIT_DISABLED_LOOP_SAFETY_BOUNDARY_FAILED
    assert iteration["classification"] == P0_SUBMIT_DISABLED_LOOP_SAFETY_BOUNDARY_FAILED
    assert "Submit-disabled mode reached" in iteration["primary_blocker"]
    assert iteration["broker_state_mutated"] is False


def test_submit_disabled_mode_blocks_live_money_and_paper_proof(tmp_path: Path) -> None:
    calls_live = Calls()
    live_payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path / "live", mode=P0_LOOP_MODE_SUBMIT_DISABLED),
        stages=fake_stages(
            tmp_path / "live",
            calls_live,
            snapshot=ready_snapshot(live_money_eligible=True),
        ),
        now_factory=aware_now,
    )
    assert live_payload["classification"] == "P0_SUBMIT_DISABLED_LOOP_BLOCKED_CONTROL_PLANE"
    assert "live_money_eligible=false" in live_payload["iterations"][0]["primary_blocker"]
    assert calls_live.cycle == 0

    calls_proof = Calls()
    proof_payload = run_track_b_p0_observe_only_loop(
        config=config(tmp_path / "proof", mode=P0_LOOP_MODE_SUBMIT_DISABLED),
        stages=fake_stages(
            tmp_path / "proof",
            calls_proof,
            snapshot=ready_snapshot(paper_proof_invoked=True),
        ),
        now_factory=aware_now,
    )
    assert proof_payload["classification"] == "P0_SUBMIT_DISABLED_LOOP_BLOCKED_CONTROL_PLANE"
    assert "paper_proof_invoked=false" in proof_payload["iterations"][0]["primary_blocker"]
    assert calls_proof.cycle == 0


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
    shadow = payload["iterations"][0]["research_shadow_diagnostics"]
    assert diagnostic["late_join_diagnostic"] is True
    assert diagnostic["late_join_policy"] == "DIAGNOSTIC_ONLY"
    assert diagnostic["submit_allowed"] is False
    assert "Strong drift observed" in diagnostic["operator_explanation"]
    assert shadow["late_join_asian_drift_shadow_classification"] == "LATE_JOIN_ASIAN_DRIFT_SHADOW_CANDIDATE"
    assert shadow["late_join_hypothetical_score"] == 5.72
    assert shadow["submit_allowed"] is False
    assert shadow["not_order_authority"] is True


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
