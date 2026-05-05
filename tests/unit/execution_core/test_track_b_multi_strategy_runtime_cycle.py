from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_multi_strategy_runtime_cycle import (
    TrackBMultiStrategyInput,
    TrackBMultiStrategyRuntimeCycleConfig,
    TrackBMultiStrategyRuntimeCycleStages,
    TrackBMultiStrategyRuntimeCycleVerdict,
    run_track_b_multi_strategy_runtime_cycle,
)
from mgc_v05l.execution_core.track_b_strategy_paper_runner import (
    TrackBStrategyPaperRunnerResult,
    TrackBStrategyPaperRunnerVerdict,
)
from mgc_v05l.execution_core.track_b_strategy_rule_runner import (
    TrackBStrategyRuleRunnerResult,
    TrackBStrategyRuleRunnerVerdict,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 5, 1, 30, tzinfo=timezone.utc)


class Calls:
    def __init__(self) -> None:
        self.strategy: list[str] = []
        self.paper = 0


def base_event(strategy_id: str) -> dict[str, object]:
    return {
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "strategy_id": strategy_id,
        "lane_id": "mgc_test_lane",
        "timeframe": "5m",
        "candle_timestamp": aware_now().isoformat(),
        "observed_at": aware_now().isoformat(),
        "close": "4575.0",
        "metadata": {"source_report_path": "fixture_quote.json"},
    }


def cycle_config(tmp_path: Path, **overrides: object) -> TrackBMultiStrategyRuntimeCycleConfig:
    payload = {
        "asian_drift_event_payload": base_event("asian_drift_v1"),
        "pause_resume_short_event_payload": base_event("ASIA_EARLY_PAUSE_RESUME_SHORT_V1"),
        "breakout_retest_hold_long_event_payload": base_event("ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"),
        "inbox_dir": tmp_path / "inbox",
        "output_root": tmp_path / "cycle",
        "strategy_rule_output_root": tmp_path / "rules",
        "strategy_paper_runner_output_root": tmp_path / "paper",
    }
    payload.update(overrides)
    return TrackBMultiStrategyRuntimeCycleConfig(**payload)


def rule_report(
    strategy_id: str,
    *,
    rule_mode: str,
    decision: str = "NO_SIGNAL",
    emitted: bool = False,
    direction: str | None = None,
    real: bool = True,
    paper_eligible: bool = True,
    live_money_eligible: bool = False,
    verdict: str | None = None,
) -> dict[str, object]:
    strategy_verdict = verdict or (
        TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL.value if emitted else TrackBStrategyRuleRunnerVerdict.NO_SIGNAL.value
    )
    return {
        "strategy_rule_runner_verdict": strategy_verdict,
        "strategy_registry_id": strategy_id,
        "strategy_registry_rule_id": strategy_id,
        "strategy_registry_rule_mode": rule_mode,
        "strategy_registry_instrument_family": "MGC",
        "strategy_registry_timeframe": "5m",
        "strategy_registry_feature_version": "test_feature_v1",
        "strategy_registry_calibration_profile": "test_calibration",
        "strategy_registry_paper_eligible": paper_eligible,
        "strategy_registry_live_money_eligible": live_money_eligible,
        "strategy_id": strategy_id,
        "rule_mode": rule_mode,
        "signal_source": rule_mode if real else "DEMO_WIRING_PROOF",
        "real_strategy_signal": real,
        "decision": decision,
        "signal_emitted": emitted,
        "signal_direction": direction,
        "primary_blocker": None,
        "required_next_action": "strategy next",
        "report_json_path": f"/tmp/{strategy_id}.json",
    }


def strategy_result(tmp_path: Path, report: dict[str, object]) -> TrackBStrategyRuleRunnerResult:
    path = tmp_path / "strategy_reports" / f"{report['strategy_registry_id']}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report), encoding="utf-8")
    emitted = report.get("signal_emitted") is True
    return TrackBStrategyRuleRunnerResult(
        verdict=TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL if emitted else TrackBStrategyRuleRunnerVerdict.NO_SIGNAL,
        report_json=path,
        report={**report, "report_json_path": str(path)},
        downstream_strategy_adapter_report_json=None,
        downstream_candle_producer_report_json=None,
        downstream_signal_batch_writer_report_json=None,
        output_batch_json=None,
    )


def paper_result(tmp_path: Path, *, passed: bool = True, mutated: bool = True, side_blocked: bool = False) -> TrackBStrategyPaperRunnerResult:
    path = tmp_path / "paper_runner_report.json"
    verdict = (
        TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_PASSED
        if passed
        else TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST if side_blocked else TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED
    )
    report = {
        "strategy_paper_runner_verdict": verdict.value,
        "readiness_invoked": not side_blocked,
        "paper_proof_invoked": mutated,
        "submit_attempted": mutated,
        "broker_state_mutated": mutated,
        "paper_proof_classification": "TRACK_B_PAPER_PROOF_PASSED" if passed else None,
        "paper_proof_lifecycle_status": "PROOF_COMPLETE_FLAT" if passed else None,
        "final_flat": True if passed else None,
        "primary_blocker": "side mismatch" if side_blocked else None,
        "required_next_action": "paper next",
    }
    path.write_text(json.dumps(report), encoding="utf-8")
    return TrackBStrategyPaperRunnerResult(
        verdict=verdict,
        report_json=path,
        report=report,
    )


def stages_for(
    tmp_path: Path,
    calls: Calls,
    reports_by_strategy: dict[str, dict[str, object]],
    *,
    paper: TrackBStrategyPaperRunnerResult | None = None,
) -> TrackBMultiStrategyRuntimeCycleStages:
    def strategy_stage(
        strategy_input: TrackBMultiStrategyInput,
        config: TrackBMultiStrategyRuntimeCycleConfig,
    ) -> TrackBStrategyRuleRunnerResult:
        calls.strategy.append(strategy_input.strategy_id)
        return strategy_result(tmp_path, reports_by_strategy[strategy_input.strategy_id])

    def paper_stage(
        config: TrackBMultiStrategyRuntimeCycleConfig,
        strategy_input: TrackBMultiStrategyInput,
        chosen_signal: dict[str, object],
    ) -> TrackBStrategyPaperRunnerResult:
        calls.paper += 1
        assert paper is not None
        return paper

    return TrackBMultiStrategyRuntimeCycleStages(strategy_rule=strategy_stage, paper_runner=paper_stage)


def default_reports() -> dict[str, dict[str, object]]:
    return {
        "asian_drift_v1": rule_report("asian_drift_v1", rule_mode="ASIAN_DRIFT_V1"),
        "ASIA_EARLY_PAUSE_RESUME_SHORT_V1": rule_report(
            "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        ),
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1": rule_report(
            "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        ),
    }


def test_all_three_no_signal_no_mutation(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_multi_strategy_runtime_cycle(
        config=cycle_config(tmp_path),
        stages=stages_for(tmp_path, calls, default_reports()),
        cycle_id="cycle-no-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBMultiStrategyRuntimeCycleVerdict.NO_SIGNAL_NO_MUTATION
    assert calls.strategy == ["asian_drift_v1", "ASIA_EARLY_PAUSE_RESUME_SHORT_V1", "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"]
    assert calls.paper == 0
    assert result.report["candidate_signals"] == []
    assert result.report["readiness_invoked"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_one_signal_without_paper_flags_reports_ready_no_submit(tmp_path: Path) -> None:
    calls = Calls()
    reports = default_reports()
    reports["ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"] = rule_report(
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        decision="LONG",
        emitted=True,
        direction="LONG",
    )

    result = run_track_b_multi_strategy_runtime_cycle(
        config=cycle_config(tmp_path),
        stages=stages_for(tmp_path, calls, reports),
        cycle_id="cycle-one-signal-no-submit",
        now=aware_now(),
    )

    assert result.verdict == TrackBMultiStrategyRuntimeCycleVerdict.SIGNAL_READY_NO_SUBMIT
    assert result.report["chosen_strategy_id"] == "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    assert calls.paper == 0
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False


def test_one_signal_with_paper_flags_delegates_once(tmp_path: Path) -> None:
    calls = Calls()
    reports = default_reports()
    reports["ASIA_EARLY_PAUSE_RESUME_SHORT_V1"] = rule_report(
        "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        decision="SHORT",
        emitted=True,
        direction="SHORT",
    )
    result = run_track_b_multi_strategy_runtime_cycle(
        config=cycle_config(
            tmp_path,
            side="SELL",
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.0",
            manual_close_limit_price="4575.3",
        ),
        stages=stages_for(tmp_path, calls, reports, paper=paper_result(tmp_path)),
        cycle_id="cycle-one-signal-paper",
        now=aware_now(),
    )

    assert result.verdict == TrackBMultiStrategyRuntimeCycleVerdict.PAPER_PROOF_PASSED
    assert calls.paper == 1
    assert result.report["chosen_strategy_id"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    assert result.report["paper_proof_invoked"] is True
    assert result.report["submit_attempted"] is True
    assert result.report["broker_state_mutated"] is True
    assert result.report["live_money_readiness"] is False


def test_multiple_same_direction_signals_without_arbitration_blocks(tmp_path: Path) -> None:
    calls = Calls()
    reports = default_reports()
    reports["asian_drift_v1"] = rule_report("asian_drift_v1", rule_mode="ASIAN_DRIFT_V1", decision="LONG", emitted=True, direction="LONG")
    reports["ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"] = rule_report(
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        decision="LONG",
        emitted=True,
        direction="LONG",
    )

    result = run_track_b_multi_strategy_runtime_cycle(
        config=cycle_config(tmp_path, submit_paper=True, confirm_paper_submit=True, quantity=1),
        stages=stages_for(tmp_path, calls, reports, paper=paper_result(tmp_path)),
        cycle_id="cycle-same-direction-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBMultiStrategyRuntimeCycleVerdict.ARBITRATION_BLOCKED
    assert result.report["arbitration_result"]["paper_candidate_count"] == 2
    assert len(result.report["suppressed_signals"]) == 2
    assert calls.paper == 0
    assert result.report["submit_attempted"] is False


def test_conflicting_long_short_signals_block(tmp_path: Path) -> None:
    calls = Calls()
    reports = default_reports()
    reports["ASIA_EARLY_PAUSE_RESUME_SHORT_V1"] = rule_report(
        "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        decision="SHORT",
        emitted=True,
        direction="SHORT",
    )
    reports["ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"] = rule_report(
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        decision="LONG",
        emitted=True,
        direction="LONG",
    )

    result = run_track_b_multi_strategy_runtime_cycle(
        config=cycle_config(tmp_path),
        stages=stages_for(tmp_path, calls, reports),
        cycle_id="cycle-conflict",
        now=aware_now(),
    )

    assert result.verdict == TrackBMultiStrategyRuntimeCycleVerdict.ARBITRATION_BLOCKED
    assert "Conflicting" in str(result.report["primary_blocker"])
    assert len(result.report["suppressed_signals"]) == 2
    assert calls.paper == 0


def test_missing_envelope_is_not_ready(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_multi_strategy_runtime_cycle(
        config=cycle_config(tmp_path, asian_drift_event_payload=None),
        stages=stages_for(tmp_path, calls, default_reports()),
        cycle_id="cycle-missing-envelope",
        now=aware_now(),
    )

    missing = [item for item in result.report["evaluated_strategies"] if item["strategy_id"] == "asian_drift_v1"][0]
    assert missing["strategy_runtime_verdict"] == "NOT_READY"
    assert "not supplied" in missing["primary_blocker"]
    assert result.report["submit_attempted"] is False


def test_demo_signal_is_rejected_as_real_candidate(tmp_path: Path) -> None:
    calls = Calls()
    reports = default_reports()
    reports["asian_drift_v1"] = rule_report(
        "asian_drift_v1",
        rule_mode="ASIAN_DRIFT_V1",
        decision="LONG",
        emitted=True,
        direction="LONG",
        real=False,
    )
    result = run_track_b_multi_strategy_runtime_cycle(
        config=cycle_config(tmp_path, submit_paper=True, confirm_paper_submit=True, quantity=1),
        stages=stages_for(tmp_path, calls, reports, paper=paper_result(tmp_path)),
        cycle_id="cycle-demo-rejected",
        now=aware_now(),
    )

    assert result.verdict == TrackBMultiStrategyRuntimeCycleVerdict.NO_SIGNAL_NO_MUTATION
    assert result.report["candidate_signals"] == []
    assert calls.paper == 0
    assert result.report["broker_state_mutated"] is False


def test_side_mismatch_blocks_inside_guarded_paper_runner(tmp_path: Path) -> None:
    calls = Calls()
    reports = default_reports()
    reports["ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"] = rule_report(
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        decision="LONG",
        emitted=True,
        direction="LONG",
    )
    result = run_track_b_multi_strategy_runtime_cycle(
        config=cycle_config(
            tmp_path,
            side="SELL",
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages_for(tmp_path, calls, reports, paper=paper_result(tmp_path, passed=False, mutated=False, side_blocked=True)),
        cycle_id="cycle-side-mismatch",
        now=aware_now(),
    )

    assert calls.paper == 1
    assert result.verdict == TrackBMultiStrategyRuntimeCycleVerdict.PAPER_PROOF_REVIEW_REQUIRED
    assert result.report["paper_runner_verdict"] == "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_INVALID_SUBMIT_REQUEST"
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False


def test_unregistered_strategy_rejects_in_registry() -> None:
    from mgc_v05l.execution_core.track_b_strategy_registry import resolve_track_b_strategy_registry_entry

    assert resolve_track_b_strategy_registry_entry(
        rule_mode="ATP_STAGED_ADD_V1",
        rule_id="ATP_STAGED_ADD_V1",
        strategy_id="ATP_STAGED_ADD_V1",
    ) is None


def test_multi_strategy_runtime_cycle_does_not_define_private_broker_calls() -> None:
    source = Path("src/mgc_v05l/execution_core/track_b_multi_strategy_runtime_cycle.py").read_text(encoding="utf-8")
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "paper_proof_cli" not in source
