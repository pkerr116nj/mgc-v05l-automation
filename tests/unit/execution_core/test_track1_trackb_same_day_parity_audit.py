from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track1_trackb_same_day_parity_audit import (
    build_track1_trackb_same_day_parity_audit,
)


NOW = datetime(2026, 5, 6, 22, 0, tzinfo=UTC)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def write_forensic(tmp_path: Path) -> Path:
    return write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "diagnostics" / "latest_track_b_missed_move_forensic_replay.json",
        {
            "per_decision_bar": [
                {
                    "decision_bar_timestamp": "2026-05-06T15:00:00+00:00",
                    "instrument": "MNQ",
                    "strategy_results": [
                        {
                            "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
                            "decision_bar_timestamp": "2026-05-06T15:00:00+00:00",
                            "result": "NO_SIGNAL",
                            "failed_predicates": ["bull_snap_close_strong"],
                            "passed_predicates": ["session_allowed"],
                            "missing_fields": [],
                        }
                    ],
                }
            ]
        },
    )


def write_postmortem(tmp_path: Path) -> Path:
    return write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "diagnostics" / "latest_track_b_same_day_postmortem.json",
        {
            "trade_lifecycle_reconstruction": [
                {
                    "strategy": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
                    "instrument": "MNQ",
                    "closed_by_guarded_paper_proof_lifecycle": True,
                    "closed_by_normal_strategy_logic": False,
                    "hold_duration_seconds": 0.07884,
                    "exit_reason": "lifecycle_guardrail",
                }
            ]
        },
    )


def build(tmp_path: Path, **kwargs: object):
    forensic = write_forensic(tmp_path)
    postmortem = write_postmortem(tmp_path)
    return build_track1_trackb_same_day_parity_audit(
        repo_root=tmp_path,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        forensic_path=forensic,
        postmortem_path=postmortem,
        now=NOW,
        **kwargs,
    )


def strategy_row(report: dict[str, object], strategy_id: str) -> dict[str, object]:
    rows = report["strategy_parity"]
    assert isinstance(rows, list)
    for row in rows:
        assert isinstance(row, dict)
        if row["track_b_strategy_id"] == strategy_id:
            return row
    raise AssertionError(strategy_id)


def test_extra_track_b_predicate_is_detected(tmp_path: Path) -> None:
    result = build(
        tmp_path,
        scoped_strategy_ids=("MNQ_FIRST_BULL_SNAP_TURN_V1",),
        track1_predicate_overrides={"MNQ_FIRST_BULL_SNAP_TURN_V1": ["session_allowed"]},
        trackb_predicate_overrides={"MNQ_FIRST_BULL_SNAP_TURN_V1": ["session_allowed", "surprise_extra_gate"]},
    )

    row = strategy_row(result.report, "MNQ_FIRST_BULL_SNAP_TURN_V1")
    assert row["parity_status"] == "TRACK_B_EXTRA_GATES_MUTED_TRACK1_SIGNALS"
    assert row["extra_track_b_predicates"] == ["surprise_extra_gate"]
    assert "TRACK_B_EXTRA_GATES_MUTED_TRACK1_SIGNALS" in result.report["classifications"]


def test_missing_track_b_predicate_is_detected(tmp_path: Path) -> None:
    result = build(
        tmp_path,
        scoped_strategy_ids=("MNQ_FIRST_BULL_SNAP_TURN_V1",),
        track1_predicate_overrides={"MNQ_FIRST_BULL_SNAP_TURN_V1": ["session_allowed", "bull_snap_close_strong"]},
        trackb_predicate_overrides={"MNQ_FIRST_BULL_SNAP_TURN_V1": ["session_allowed"]},
    )

    row = strategy_row(result.report, "MNQ_FIRST_BULL_SNAP_TURN_V1")
    assert row["parity_status"] == "TRACK_B_MISSING_TRACK1_PREDICATES"
    assert row["missing_track_b_predicates"] == ["bull_snap_close_strong"]


def test_field_mapping_mismatch_is_detected(tmp_path: Path) -> None:
    result = build(
        tmp_path,
        scoped_strategy_ids=("MNQ_FIRST_BULL_SNAP_TURN_V1",),
        track1_predicate_overrides={"MNQ_FIRST_BULL_SNAP_TURN_V1": ["session_allowed"]},
        trackb_predicate_overrides={"MNQ_FIRST_BULL_SNAP_TURN_V1": ["session_allowed"]},
        feature_mapping_overrides={
            "MNQ_FIRST_BULL_SNAP_TURN_V1": {
                "track1": ["bull_snap_close_strong", "session_allowed"],
                "trackb": ["session_allowed"],
            }
        },
    )

    row = strategy_row(result.report, "MNQ_FIRST_BULL_SNAP_TURN_V1")
    assert row["parity_status"] == "TRACK_B_FIELD_MISMATCH"
    assert row["feature_field_mapping"]["missing_track_b_fields"] == ["bull_snap_close_strong"]


def test_session_mismatch_is_detected(tmp_path: Path) -> None:
    result = build(
        tmp_path,
        scoped_strategy_ids=("US_DERIVATIVE_BEAR_TURN_V1",),
        track1_predicate_overrides={"US_DERIVATIVE_BEAR_TURN_V1": ["allow_us"]},
        trackb_predicate_overrides={"US_DERIVATIVE_BEAR_TURN_V1": ["allow_us"]},
        session_convention_overrides={"US_DERIVATIVE_BEAR_TURN_V1": {"track1": "US", "trackb": "ASIA"}},
    )

    row = strategy_row(result.report, "US_DERIVATIVE_BEAR_TURN_V1")
    assert row["parity_status"] == "TRACK_B_SESSION_MISMATCH"


def test_bar_alignment_mismatch_is_detected(tmp_path: Path) -> None:
    result = build(
        tmp_path,
        scoped_strategy_ids=("US_DERIVATIVE_BEAR_TURN_V1",),
        track1_predicate_overrides={"US_DERIVATIVE_BEAR_TURN_V1": ["allow_us"]},
        trackb_predicate_overrides={"US_DERIVATIVE_BEAR_TURN_V1": ["allow_us"]},
        bar_alignment_overrides={"US_DERIVATIVE_BEAR_TURN_V1": {"track1": "next_bar_open", "trackb": "current_bar_close"}},
    )

    row = strategy_row(result.report, "US_DERIVATIVE_BEAR_TURN_V1")
    assert row["parity_status"] == "TRACK_B_BAR_ALIGNMENT_MISMATCH"


def test_safety_gates_are_not_counted_as_strategy_predicates(tmp_path: Path) -> None:
    result = build(
        tmp_path,
        scoped_strategy_ids=("MNQ_FIRST_BULL_SNAP_TURN_V1",),
        track1_predicate_overrides={"MNQ_FIRST_BULL_SNAP_TURN_V1": ["session_allowed"]},
        trackb_predicate_overrides={
            "MNQ_FIRST_BULL_SNAP_TURN_V1": ["session_allowed", "submit_allowed", "live_money_readiness"]
        },
    )

    row = strategy_row(result.report, "MNQ_FIRST_BULL_SNAP_TURN_V1")
    assert row["extra_track_b_predicates"] == []
    assert row["safety_or_operational_track_b_fields_excluded_from_strategy_predicate_diff"] == [
        "live_money_readiness",
        "submit_allowed",
    ]


def test_missing_track1_reference_artifacts_are_not_fake_pass(tmp_path: Path) -> None:
    result = build(
        tmp_path,
        scoped_strategy_ids=("MNQ_FIRST_BULL_SNAP_TURN_V1",),
        track1_predicate_overrides={"MNQ_FIRST_BULL_SNAP_TURN_V1": ["session_allowed"]},
        trackb_predicate_overrides={"MNQ_FIRST_BULL_SNAP_TURN_V1": ["session_allowed"]},
    )

    row = strategy_row(result.report, "MNQ_FIRST_BULL_SNAP_TURN_V1")
    assert row["parity_status"] == "TRACK1_REFERENCE_UNAVAILABLE"
    assert "TRACK1_REFERENCE_UNAVAILABLE" in result.report["classifications"]
    assert result.report["summary"]["parity_can_be_proven_from_same_day_track1_replay"] is False


def test_strategy_not_migrated_is_reported_clearly(tmp_path: Path) -> None:
    result = build(
        tmp_path,
        scoped_strategy_ids=("TRACK1_ONLY_STRATEGY_V1",),
        migrated_strategy_ids_override=(),
    )

    row = strategy_row(result.report, "TRACK1_ONLY_STRATEGY_V1")
    assert row["parity_status"] == "TRACK_B_MISSING_TRACK1_STRATEGIES"
    assert "TRACK_B_MISSING_TRACK1_STRATEGIES" in result.report["classifications"]


def test_audit_does_not_invoke_broker_or_paper_paths(tmp_path: Path) -> None:
    result = build(tmp_path, scoped_strategy_ids=("MNQ_FIRST_BULL_SNAP_TURN_V1",))

    assert result.report["safety"]["broker_commands_invoked"] is False
    assert result.report["safety"]["paper_proof_cli_invoked"] is False
    assert result.report["safety"]["submit_cancel_place_order_invoked"] is False
    assert result.report["safety"]["broker_state_mutated"] is False

