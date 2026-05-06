from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_missed_move_forensic_replay import (
    build_track_b_missed_move_forensic_replay,
)


NOW = datetime(2026, 5, 6, 20, 30, tzinfo=UTC)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def runtime_report(
    tmp_path: Path,
    *,
    cycle_id: str,
    generated_at: str,
    strategies: list[dict[str, object]],
    candidates: list[dict[str, object]] | None = None,
    suppressed: list[dict[str, object]] | None = None,
) -> Path:
    return write_json(
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "track_b_multi_strategy_runtime_cycle"
        / cycle_id
        / "track_b_multi_strategy_runtime_cycle_report.json",
        {
            "schema_version": "track_b_multi_strategy_runtime_cycle_v1",
            "generated_at": generated_at,
            "track_b_multi_strategy_runtime_cycle_id": cycle_id,
            "multi_strategy_runtime_cycle_verdict": "TRACK_B_MULTI_STRATEGY_RUNTIME_NO_SIGNAL_NO_MUTATION",
            "evaluated_strategies": strategies,
            "candidate_signals": candidates or [],
            "suppressed_signals": suppressed or [],
            "paper_proof_invoked": False,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "live_money_readiness": False,
        },
    )


def strategy(
    strategy_id: str,
    *,
    instrument: str,
    decision: str = "NO_SIGNAL",
    conditions: dict[str, bool] | None = None,
    blockers: list[str] | None = None,
) -> dict[str, object]:
    return {
        "strategy_id": strategy_id,
        "signal_source": strategy_id,
        "decision": decision,
        "signal_emitted": decision == "SIGNAL",
        "decision_reason": "test reason",
        "rule_conditions": conditions or {},
        "rule_blockers": blockers or [],
        "registry_metadata": {
            "strategy_registry_instrument_family": instrument,
            "strategy_registry_paper_eligible": True,
            "strategy_registry_live_money_eligible": False,
        },
    }


def monitor_report(tmp_path: Path, *, instruments: dict[str, list[str]]) -> None:
    rows = [
        {
            "instrument_family": family,
            "enabled_strategies": strategies,
            "feature_context_ready": True,
            "live_execution_approved": True,
            "paper_evaluation_allowed": True,
            "latest_completed_5m_timestamp": "2026-05-06T20:25:00+00:00",
        }
        for family, strategies in instruments.items()
    ]
    write_json(
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "track_b_shadow_monitor"
        / "track_b_shadow_monitor_test"
        / "track_b_shadow_monitor_report.json",
        {
            "completed_at": "2026-05-06T20:26:00+00:00",
            "cycle_id": "monitor-cycle",
            "cycle_index": 1,
            "mode": "PAPER",
            "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
            "instrument_reports": rows,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "paper_proof_invoked": False,
            "live_money_readiness": False,
        },
    )


def completed_5m(tmp_path: Path, instrument: str, *, open_value: str = "100", close_value: str = "110") -> None:
    write_json(
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "databento_live_runtime_feed"
        / f"latest_live_{instrument.lower()}_completed_5m_candles.json",
        {
            "generated_at": NOW.isoformat(),
            "candles": [
                {
                    "candle_timestamp": "2026-05-06T20:25:00+00:00",
                    "open": open_value,
                    "high": str(max(float(open_value), float(close_value))),
                    "low": str(min(float(open_value), float(close_value))),
                    "close": close_value,
                    "volume": "10",
                }
            ],
        },
    )


def test_replay_classifies_operational_gate_separately_from_strategy_predicates(tmp_path: Path) -> None:
    monitor_report(tmp_path, instruments={"MGC": ["FIRST_BULL_SNAP_TURN_V1"]})
    completed_5m(tmp_path, "MGC")
    runtime_report(
        tmp_path,
        cycle_id="cycle-op",
        generated_at="2026-05-06T20:26:00+00:00",
        strategies=[
            strategy(
                "FIRST_BULL_SNAP_TURN_V1",
                instrument="MGC",
                decision="NOT_READY",
                blockers=["metadata.first_bull_snap_turn_state missing"],
            )
        ],
    )

    result = build_track_b_missed_move_forensic_replay(
        repo_root=tmp_path,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )

    gates = {item["reason"]: item["count"] for item in result.report["top_blockers_by_gate_class"]}
    assert gates["OPERATIONAL_GATE"] == 1
    assert result.report["completed_decision_bars"][0]["strategy_results"][0]["missing_fields"]


def test_replay_detects_one_and_two_predicate_near_misses(tmp_path: Path) -> None:
    monitor_report(tmp_path, instruments={"MGC": ["A_LONG", "B_LONG"]})
    completed_5m(tmp_path, "MGC")
    runtime_report(
        tmp_path,
        cycle_id="cycle-near",
        generated_at="2026-05-06T20:26:00+00:00",
        strategies=[
            strategy("A_LONG", instrument="MGC", conditions={"a": True, "b": False}),
            strategy("B_LONG", instrument="MGC", conditions={"a": True, "b": False, "c": False}),
        ],
    )

    result = build_track_b_missed_move_forensic_replay(
        repo_root=tmp_path,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )

    assert result.report["one_predicate_away_count"] == 1
    assert result.report["two_predicate_away_count"] == 1
    assert result.report["diagnosis_classification"] == "STRATEGY_GATES_MUTED_NEAR_MISSES"


def test_replay_classifies_missing_long_trend_strategy_as_coverage_gap(tmp_path: Path) -> None:
    monitor_report(tmp_path, instruments={"MNQ": ["MNQ_US_DERIVATIVE_BEAR_TURN_V1"]})
    completed_5m(tmp_path, "MNQ", open_value="100", close_value="125")
    runtime_report(
        tmp_path,
        cycle_id="cycle-coverage",
        generated_at="2026-05-06T20:26:00+00:00",
        strategies=[
            strategy(
                "MNQ_US_DERIVATIVE_BEAR_TURN_V1",
                instrument="MNQ",
                conditions={"a": False, "b": False, "c": False},
            )
        ],
    )

    result = build_track_b_missed_move_forensic_replay(
        repo_root=tmp_path,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )

    assert result.report["diagnosis_classification"] == "STRATEGY_COVERAGE_GAP_TREND_CONTINUATION"
    assert result.report["directional_coverage_assessment"]["MNQ"]["trend_continuation_gap"] is True
    assert result.report["top_blockers_by_gate_class"][0]["reason"] == "STRATEGY_PREDICATE_GATE"


def test_replay_is_diagnostic_only_and_preserves_instrument_scoping(tmp_path: Path) -> None:
    monitor_report(
        tmp_path,
        instruments={
            "MGC": ["FIRST_BULL_SNAP_TURN_V1"],
            "MNQ": ["MNQ_FIRST_BULL_SNAP_TURN_V1"],
        },
    )
    completed_5m(tmp_path, "MGC", open_value="100", close_value="101")
    completed_5m(tmp_path, "MNQ", open_value="200", close_value="201")
    runtime_report(
        tmp_path,
        cycle_id="cycle-multi",
        generated_at="2026-05-06T20:26:00+00:00",
        strategies=[
            strategy("FIRST_BULL_SNAP_TURN_V1", instrument="MGC", conditions={"a": False, "b": False, "c": False}),
            strategy("MNQ_FIRST_BULL_SNAP_TURN_V1", instrument="MNQ", conditions={"x": False, "y": False, "z": False}),
        ],
    )

    result = build_track_b_missed_move_forensic_replay(
        repo_root=tmp_path,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )

    assert result.report["execution_live_source"] is False
    assert result.report["historical_or_artifact_replay_is_not_execution_live"] is True
    assert result.report["safety"]["broker_commands_invoked"] is False
    assert result.report["instrument_summaries"]["MGC"]["strategy_evaluations"] == 1
    assert result.report["instrument_summaries"]["MNQ"]["strategy_evaluations"] == 1


def test_replay_classifies_signal_blocked_before_handoff(tmp_path: Path) -> None:
    monitor_report(tmp_path, instruments={"MGC": ["FIRST_BEAR_SNAP_TURN_V1", "FIRST_BULL_SNAP_TURN_V1"]})
    completed_5m(tmp_path, "MGC", open_value="110", close_value="100")
    runtime_report(
        tmp_path,
        cycle_id="cycle-signal-blocked",
        generated_at="2026-05-06T20:26:00+00:00",
        candidates=[{"strategy_id": "FIRST_BEAR_SNAP_TURN_V1", "signal_direction": "SHORT"}],
        strategies=[
            strategy(
                "FIRST_BEAR_SNAP_TURN_V1",
                instrument="MGC",
                decision="SIGNAL",
                conditions={"a": True, "b": True},
            ),
            strategy("FIRST_BULL_SNAP_TURN_V1", instrument="MGC", conditions={"x": False, "y": False, "z": False}),
        ],
    )
    report_path = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "track_b_multi_strategy_runtime_cycle"
        / "cycle-signal-blocked"
        / "track_b_multi_strategy_runtime_cycle_report.json"
    )
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["chosen_strategy_id"] = "FIRST_BEAR_SNAP_TURN_V1"
    payload["paper_runner_report_path"] = "outputs/track_b_strategy_paper_runner/blocked/report.json"
    payload["paper_runner_verdict"] = "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_READINESS"
    report_path.write_text(json.dumps(payload), encoding="utf-8")

    result = build_track_b_missed_move_forensic_replay(
        repo_root=tmp_path,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )

    assert result.report["diagnosis_classification"] == "ARBITRATION_OR_HANDOFF_BLOCKED"
    assert result.report["total_signals"] == 1
    assert result.report["total_handoffs"] == 1
