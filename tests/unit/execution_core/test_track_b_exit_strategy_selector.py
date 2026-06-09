from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_exit_strategy_selector import (
    TrackBExitStrategySelectorConfig,
    build_track_b_exit_strategy_selector_report,
)


NOW = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)


def test_flat_exit_decision_source_produces_flat_selector_report(tmp_path: Path) -> None:
    report = _report(tmp_path, decisions=[], classification="EXIT_DECISION_FLAT")

    assert report["classification"] == "EXIT_STRATEGY_SELECTOR_FLAT"
    assert report["selected_count"] == 0
    assert report["review_required_count"] == 0


def test_hold_decisions_are_diagnostic_not_selected(tmp_path: Path) -> None:
    report = _report(tmp_path, decisions=[_decision(action="HOLD", reason="no_exit_condition_triggered")])

    assert report["classification"] == "EXIT_STRATEGY_SELECTOR_NO_ACTIONABLE_EXITS"
    assert report["selected_decisions"] == []
    assert report["held_decision_count"] == 1
    assert report["diagnostic_rows"][0]["reason"] == "hold_decision_not_actionable"


def test_hard_stop_beats_timebox_and_partial_scale_out(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        decisions=[
            _decision(decision_id="timebox", action="FULL_CLOSE", reason="timebox_or_exit_due"),
            _decision(decision_id="partial", action="REDUCE", reason="partial_scale_out_due", reduce_qty="1"),
            _decision(decision_id="hard", action="PROTECT", reason="hard_stop"),
        ],
    )

    selected = report["selected_decisions"][0]
    assert report["classification"] == "EXIT_STRATEGY_SELECTOR_SELECTED"
    assert selected["strategy_type"] == "hard_stop"
    assert selected["decision_id"] == "hard"
    assert selected["priority"] == 10


def test_operator_close_beats_protective_close(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        decisions=[
            _decision(decision_id="protect", action="PROTECT", reason="protective_close_due"),
            _decision(decision_id="operator", action="FULL_CLOSE", reason="operator_close_requested"),
        ],
    )

    selected = report["selected_decisions"][0]
    assert selected["strategy_type"] == "operator_close"
    assert selected["decision_id"] == "operator"


def test_profit_target_beats_trailing_vwap_timebox_and_partial(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        decisions=[
            _decision(decision_id="partial", action="REDUCE", reason="partial_scale_out_due"),
            _decision(decision_id="timebox", action="FULL_CLOSE", reason="timebox_or_exit_due"),
            _decision(decision_id="vwap", action="FULL_CLOSE", reason="VWAP_reclaim_loss"),
            _decision(decision_id="trail", action="FULL_CLOSE", reason="trailing_stop"),
            _decision(decision_id="target", action="FULL_CLOSE", reason="profit_target"),
        ],
    )

    selected = report["selected_decisions"][0]
    assert selected["strategy_type"] == "profit_target"
    assert selected["decision_id"] == "target"


def test_reversal_selection_is_close_only_and_never_entry_or_flip(tmp_path: Path) -> None:
    report = _report(tmp_path, decisions=[_decision(action="REVERSE_CONSIDER", reason="reversal_signal")])

    selected = report["selected_decisions"][0]
    assert selected["strategy_type"] == "reversal_close_only"
    assert selected["close_only"] is True
    assert selected["entry_allowed"] is False
    assert selected["flip_allowed"] is False
    assert selected["selected_decision"]["action"] == "REVERSE_CONSIDER"


def test_same_priority_tie_produces_review_required(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        decisions=[
            _decision(decision_id="target_a", action="FULL_CLOSE", reason="profit_target_a"),
            _decision(decision_id="target_b", action="FULL_CLOSE", reason="profit_target_b"),
        ],
    )

    assert report["classification"] == "EXIT_STRATEGY_SELECTOR_REVIEW_REQUIRED"
    assert report["selected_decisions"] == []
    assert report["review_required"][0]["reason"] == "same_priority_exit_strategy_tie"
    assert report["review_required"][0]["candidate_decision_ids"] == ["target_a", "target_b"]


def test_multiple_positions_select_independently(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        decisions=[
            _decision(decision_id="mes_timebox", local_symbol="MESM6", con_id=770561194, reason="timebox_or_exit_due"),
            _decision(decision_id="mnq_partial", local_symbol="MNQM6", con_id=770561201, action="REDUCE", reason="partial_scale_out_due"),
        ],
    )

    assert report["classification"] == "EXIT_STRATEGY_SELECTOR_SELECTED"
    assert report["selected_count"] == 2
    rows = {row["local_symbol"]: row for row in report["selected_decisions"]}
    assert rows["MESM6"]["strategy_type"] == "timebox_close"
    assert rows["MNQM6"]["strategy_type"] == "partial_scale_out"


def test_blocked_decision_source_blocks_selector(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        decisions=[_decision(action="FULL_CLOSE")],
        classification="EXIT_DECISION_POSITION_STATE_BLOCKED",
    )

    assert report["classification"] == "EXIT_STRATEGY_SELECTOR_DECISION_SOURCE_BLOCKED"
    assert report["selected_count"] == 0


def test_source_refs_diagnostics_and_read_only_flags_are_preserved(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        decisions=[
            _decision(
                action="FULL_CLOSE",
                diagnostics=[{"source": "position_state", "historical_only": True}],
            )
        ],
    )

    selected = report["selected_decisions"][0]
    assert [ref["name"] for ref in selected["source_artifact_refs"]] == ["exit_decision"]
    assert selected["diagnostics"][0]["historical_only"] is True
    assert report["broker_state_mutated"] is False
    assert report["submit_attempted"] is False
    assert report["cancel_attempted"] is False
    assert report["close_attempted"] is False
    assert report["service_started"] is False
    assert report["runtime_restarted"] is False


def test_missing_source_is_reported(tmp_path: Path) -> None:
    report = build_track_b_exit_strategy_selector_report(
        config=TrackBExitStrategySelectorConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert report["classification"] == "EXIT_STRATEGY_SELECTOR_SOURCE_MISSING"
    assert report["selected_count"] == 0


def _report(
    tmp_path: Path,
    *,
    decisions: list[dict],
    classification: str = "EXIT_DECISION_READY",
) -> dict:
    return build_track_b_exit_strategy_selector_report(
        config=TrackBExitStrategySelectorConfig(repo_root=tmp_path),
        now=NOW,
        input_overrides={
            "exit_decision": {
                "schema_version": "track_b_exit_decision_report_v1",
                "generated_at": NOW.isoformat(),
                "classification": classification,
                "decisions": decisions,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            }
        },
    )


def _decision(
    *,
    decision_id: str = "decision_1",
    action: str = "FULL_CLOSE",
    reason: str = "timebox_or_exit_due",
    execution_domain: str = "TRACK_B_PAPER",
    account_id: str = "DUM882026",
    con_id: int = 770561194,
    local_symbol: str = "MESM6",
    instrument: str = "MES",
    side: str = "SHORT",
    qty: str = "1",
    reduce_qty: str | None = None,
    diagnostics: list[dict] | None = None,
) -> dict:
    row = {
        "schema_version": "track_b_exit_decision_v1",
        "decision_id": decision_id,
        "action": action,
        "execution_domain": execution_domain,
        "account_id": account_id,
        "con_id": con_id,
        "local_symbol": local_symbol,
        "instrument": instrument,
        "side": side,
        "qty": qty,
        "reason": reason,
        "priority": 50,
        "urgency": "NORMAL",
        "source_policy_id": None,
        "position": {"local_symbol": local_symbol, "con_id": con_id},
        "source_artifact_refs": [],
        "diagnostics": diagnostics or [],
    }
    if reduce_qty is not None:
        row["reduce_qty"] = reduce_qty
    return row
