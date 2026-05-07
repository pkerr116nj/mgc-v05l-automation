from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_strategy_activity_calibration import (
    TrackBStrategyActivityCalibrationConfig,
    classify_quiet_strategy,
    create_track_b_strategy_activity_calibration,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 7, 18, 30, tzinfo=timezone.utc)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")
    return path


def runtime_report(path: Path, payload: dict[str, object]) -> Path:
    return write_json(path / "track_b_multi_strategy_runtime_cycle_report.json", payload)


def strategy(
    strategy_id: str,
    *,
    instrument: str = "MGC",
    conditions: dict[str, bool] | None = None,
    blockers: list[str] | None = None,
    decision: str = "NO_SIGNAL",
) -> dict[str, object]:
    return {
        "strategy_id": strategy_id,
        "decision": decision,
        "signal_emitted": decision == "SIGNAL",
        "registry_metadata": {
            "strategy_registry_id": strategy_id,
            "strategy_registry_instrument_family": instrument,
            "strategy_registry_paper_eligible": True,
            "strategy_registry_live_money_eligible": False,
        },
        "rule_conditions": conditions or {},
        "rule_blockers": blockers or [],
    }


def test_classifies_session_filter_quiet_strategy() -> None:
    row = {
        "registered": True,
        "enabled": True,
        "session": "ASIA",
        "strategy_evaluations": 10,
        "hard_signals": 0,
        "suppressed_count": 0,
        "intent_count": 0,
        "session_filter_inactive_bars": 8,
        "top_missing_fields": [],
        "top_failed_predicates": [{"reason": "session_asia", "count": 8}],
        "one_predicate_away": 0,
        "two_predicates_away": 0,
    }

    assert classify_quiet_strategy(row) == "QUIET_DUE_TO_SESSION_FILTER"


def test_classifies_extra_track_b_gate_before_thresholds() -> None:
    row = {
        "registered": True,
        "enabled": True,
        "session": "US",
        "strategy_evaluations": 4,
        "hard_signals": 0,
        "suppressed_count": 0,
        "intent_count": 0,
        "session_filter_inactive_bars": 0,
        "top_missing_fields": [],
        "top_failed_predicates": [{"reason": "no_first_bull_snap_turn", "count": 4}],
        "one_predicate_away": 0,
        "two_predicates_away": 0,
    }

    assert classify_quiet_strategy(row) == "QUIET_DUE_TO_EXTRA_TRACK_B_GATE"


def test_classifies_enabled_but_unevaluated_strategy_as_not_wired() -> None:
    row = {
        "registered": True,
        "enabled": True,
        "session": "ASIA",
        "strategy_evaluations": 0,
        "hard_signals": 0,
        "suppressed_count": 0,
        "intent_count": 0,
        "session_filter_inactive_bars": 0,
        "top_missing_fields": [],
        "top_failed_predicates": [],
        "one_predicate_away": 0,
        "two_predicates_away": 0,
    }

    assert classify_quiet_strategy(row) == "QUIET_DUE_TO_NOT_MIGRATED_OR_NOT_ENABLED"


def test_activity_calibration_writes_json_and_markdown(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    runtime_report(
        runtime_root / "mgc-1",
        {
            "generated_at": "2026-05-07T12:00:00+00:00",
            "candidate_signals": [],
            "suppressed_signals": [],
            "evaluated_strategies": [
                strategy(
                    "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
                    conditions={"session_asia": False, "close_below_open": False},
                    blockers=["session_asia=false_or_missing", "close_below_open=false_or_missing"],
                ),
                strategy(
                    "FIRST_BULL_SNAP_TURN_V1",
                    conditions={"session_allowed": True, "first_bull_snap_turn": False},
                    blockers=["first_bull_snap_turn=false_or_missing"],
                ),
            ],
        },
    )
    runtime_report(
        runtime_root / "mnq-1",
        {
            "generated_at": "2026-05-07T16:25:00+00:00",
            "candidate_signals": [{"strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1"}],
            "paper_runner_report_path": "outputs/runner.json",
            "evaluated_strategies": [
                strategy("MNQ_FIRST_BULL_SNAP_TURN_V1", instrument="MNQ", decision="SIGNAL"),
            ],
        },
    )
    intents_jsonl = write_jsonl(
        tmp_path / "intents.jsonl",
        [
            {
                "created_at": "2026-05-07T16:26:00+00:00",
                "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
                "intent_id": "intent-1",
            }
        ],
    )
    trade_summary = write_json(
        tmp_path / "summary.json",
        {
            "managed_strategy_trade_count": 1,
            "open_position_count": 0,
            "review_required_count": 0,
            "recent_trades": [
                {
                    "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
                    "paper_lifecycle_type": "STRATEGY_MANAGED",
                    "broker_backed_position_confirmed": True,
                    "final_position_status": "CLOSED_FLAT",
                }
            ],
        },
    )
    write_json(tmp_path / "preflight.json", {"classification": "TRACK1_SIGNALS_CONTINUED_HANDOFF_BROKE"})
    write_json(tmp_path / "breakpoint.json", {"classification": "HANDOFF_INTENT_NOT_CREATED", "missing_link": "signal_to_intent"})
    write_json(tmp_path / "parity.json", {"summary": {"primary_classification": "TRACK1_REFERENCE_UNAVAILABLE"}})

    result = create_track_b_strategy_activity_calibration(
        config=TrackBStrategyActivityCalibrationConfig(
            repo_root=tmp_path,
            runtime_cycle_root=Path("runtime"),
            intents_jsonl=intents_jsonl,
            trade_summary_json=trade_summary,
            track1_preflight_json=Path("preflight.json"),
            track1_breakpoint_json=Path("breakpoint.json"),
            track1_parity_json=Path("parity.json"),
            output_json=Path("diagnostics/calibration.json"),
            output_md=Path("diagnostics/calibration.md"),
        ),
        now=aware_now(),
    )

    assert result.report_json.exists()
    assert result.report_md.exists()
    strategies = {row["strategy_id"]: row for row in result.report["strategies"]}
    assert strategies["ASIA_EARLY_PAUSE_RESUME_SHORT_V1"]["classification"] == "QUIET_DUE_TO_SESSION_FILTER"
    assert strategies["FIRST_BULL_SNAP_TURN_V1"]["one_predicate_away"] == 1
    assert strategies["MNQ_FIRST_BULL_SNAP_TURN_V1"]["hard_signals"] == 1
    assert result.report["totals"]["meaningful_managed_trades"] == 1
    assert result.report["broker_commands_invoked"] is False
    assert "Track B Strategy Activity Calibration" in result.report_md.read_text(encoding="utf-8")
