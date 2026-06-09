from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_exit_decision import (
    TrackBExitDecisionReportConfig,
    build_track_b_exit_decision_report,
)


NOW = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)


def test_flat_position_state_produces_no_decisions(tmp_path: Path) -> None:
    report = _report(tmp_path, positions=[])

    assert report["classification"] == "EXIT_DECISION_FLAT"
    assert report["decision_count"] == 0
    assert report["decisions"] == []
    assert report["action_counts"]["HOLD"] == 0


def test_current_position_defaults_to_hold(tmp_path: Path) -> None:
    report = _report(tmp_path, positions=[_position()])

    decision = report["decisions"][0]
    assert report["classification"] == "EXIT_DECISION_READY"
    assert decision["action"] == "HOLD"
    assert decision["reason"] == "no_exit_condition_triggered"
    assert decision["priority"] == 100
    assert decision["urgency"] == "LOW"
    assert decision["position"]["local_symbol"] == "MESM6"


def test_timebox_due_produces_full_close(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        positions=[_position(lifecycle_id="lifecycle_1")],
        decision_inputs={"lifecycle_1": {"timebox_due": True, "source_policy_id": "TIMEBOX_60M"}},
    )

    decision = report["decisions"][0]
    assert decision["action"] == "FULL_CLOSE"
    assert decision["reason"] == "timebox_or_exit_due"
    assert decision["priority"] == 50
    assert decision["source_policy_id"] == "TIMEBOX_60M"


def test_bars_since_entry_reaching_timebox_produces_full_close(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        positions=[_position(local_symbol="MNQM6", con_id=770561201, instrument="MNQ")],
        decision_inputs={"MNQM6": {"bars_since_entry": 12, "timebox_bars": 12}},
    )

    assert report["decisions"][0]["action"] == "FULL_CLOSE"


def test_partial_scale_out_produces_reduce(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        positions=[_position(qty="3", lifecycle_id="lifecycle_1")],
        decision_inputs={"lifecycle_1": {"partial_scale_out_due": True, "reduce_qty": "1"}},
    )

    decision = report["decisions"][0]
    assert decision["action"] == "REDUCE"
    assert decision["reduce_qty"] == "1"
    assert decision["reason"] == "partial_scale_out_due"


def test_hard_stop_produces_protect(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        positions=[_position(strategy_id="strategy_1")],
        decision_inputs={"strategy_1": {"hard_stop_triggered": True, "exit_reason": "hard_stop"}},
    )

    decision = report["decisions"][0]
    assert decision["action"] == "PROTECT"
    assert decision["reason"] == "hard_stop"
    assert decision["priority"] == 10
    assert decision["urgency"] == "HIGH"


def test_reversal_signal_produces_reverse_consider(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        positions=[_position(lane_id="lane_1")],
        decision_inputs={"lane_1": {"reversal_signal": True}},
    )

    decision = report["decisions"][0]
    assert decision["action"] == "REVERSE_CONSIDER"
    assert decision["reason"] == "reversal_close_only_consideration"
    assert decision["priority"] == 20


def test_position_state_blocked_stops_decision_publication(tmp_path: Path) -> None:
    report = build_track_b_exit_decision_report(
        config=TrackBExitDecisionReportConfig(repo_root=tmp_path),
        now=NOW,
        input_overrides={
            "position_state": {
                "schema_version": "track_b_position_state_report_v1",
                "generated_at": NOW.isoformat(),
                "classification": "POSITION_STATE_BLOCKED_WRONG_SCOPE",
                "positions": [_position()],
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            }
        },
    )

    assert report["classification"] == "EXIT_DECISION_POSITION_STATE_BLOCKED"
    assert report["decision_count"] == 0
    assert report["decisions"] == []


def test_source_refs_and_diagnostics_are_preserved(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        positions=[
            _position(
                diagnostic_rows=[
                    {"source": "managed_positions", "kind": "historical_review_positions", "historical_only": True}
                ]
            )
        ],
    )

    decision = report["decisions"][0]
    assert [ref["name"] for ref in decision["source_artifact_refs"]] == ["position_state"]
    assert decision["diagnostics"][0]["historical_only"] is True
    assert report["broker_state_mutated"] is False
    assert report["submit_attempted"] is False
    assert report["cancel_attempted"] is False
    assert report["close_attempted"] is False
    assert report["service_started"] is False
    assert report["runtime_restarted"] is False


def test_no_position_state_source_is_missing(tmp_path: Path) -> None:
    report = build_track_b_exit_decision_report(config=TrackBExitDecisionReportConfig(repo_root=tmp_path), now=NOW)

    assert report["classification"] == "EXIT_DECISION_SOURCE_MISSING"
    assert report["decision_count"] == 0


def _report(
    tmp_path: Path,
    *,
    positions: list[dict],
    decision_inputs: dict[str, dict] | None = None,
) -> dict:
    return build_track_b_exit_decision_report(
        config=TrackBExitDecisionReportConfig(repo_root=tmp_path),
        now=NOW,
        input_overrides={
            "position_state": {
                "schema_version": "track_b_position_state_report_v1",
                "generated_at": NOW.isoformat(),
                "classification": "POSITION_STATE_CURRENT_POSITIONS" if positions else "POSITION_STATE_FLAT",
                "positions": positions,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            }
        },
        decision_inputs=decision_inputs or {},
    )


def _position(
    *,
    execution_domain: str = "TRACK_B_PAPER",
    account_id: str = "DUM882026",
    con_id: int = 770561194,
    local_symbol: str = "MESM6",
    instrument: str = "MES",
    side: str = "SHORT",
    qty: str = "1",
    lifecycle_id: str | None = None,
    trade_id: str | None = None,
    strategy_id: str | None = None,
    lane_id: str | None = None,
    diagnostic_rows: list[dict] | None = None,
) -> dict:
    return {
        "schema_version": "track_b_position_state_v1",
        "execution_domain": execution_domain,
        "account_id": account_id,
        "con_id": con_id,
        "local_symbol": local_symbol,
        "instrument": instrument,
        "side": side,
        "qty": qty,
        "owned_qty": qty if lifecycle_id else None,
        "attribution_status": "ATTRIBUTED" if lifecycle_id and trade_id and strategy_id and lane_id else "UNATTRIBUTED",
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
        "strategy_id": strategy_id,
        "lane_id": lane_id,
        "current_scope": True,
        "source_artifact_refs": [],
        "diagnostic_rows": diagnostic_rows or [],
    }
