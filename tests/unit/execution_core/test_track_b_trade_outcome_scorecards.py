from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_trade_outcome_scorecards import (
    build_trade_outcome_scorecards,
    run_trade_outcome_scorecards,
)


NOW = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)


def test_strategy_scorecard_aggregation() -> None:
    scorecards = build_trade_outcome_scorecards(
        [_outcome("lane_a", pnl=10.0), _outcome("lane_a", pnl=-2.0), _outcome("lane_b", pnl=3.0)],
        generated_at=NOW,
    )
    groups = {row["key"]: row for row in scorecards["scorecards"]["strategy_edge"]["groups"]}

    assert groups["lane_a_strategy | lane_a"]["trade_count"] == 2
    assert groups["lane_a_strategy | lane_a"]["average_pnl_proxy"] == 4.0
    assert groups["lane_a_strategy | lane_a"]["win_rate"] == 0.5


def test_exit_policy_aggregation() -> None:
    scorecards = build_trade_outcome_scorecards(
        [_outcome("lane_a", pnl=10.0, exit_policy="TIMEBOX"), _outcome("lane_b", pnl=-6.0, exit_policy="TIMEBOX")],
        generated_at=NOW,
    )
    groups = {row["key"]: row for row in scorecards["scorecards"]["exit_policy"]["groups"]}

    assert groups["TIMEBOX"]["trade_count"] == 2
    assert groups["TIMEBOX"]["average_pnl_proxy"] == 2.0


def test_session_and_instrument_aggregation() -> None:
    scorecards = build_trade_outcome_scorecards(
        [
            _outcome("lane_a", pnl=10.0, session="LONDON", instrument="GC", contract="GCQ6"),
            _outcome("lane_b", pnl=-4.0, session="US", instrument="MGC", contract="MGCQ6"),
        ],
        generated_at=NOW,
    )
    sessions = {row["key"]: row for row in scorecards["scorecards"]["session_edge"]["groups"]}
    instruments = {row["key"]: row for row in scorecards["scorecards"]["instrument_edge"]["groups"]}

    assert sessions["LONDON"]["trade_count"] == 1
    assert instruments["GC | GCQ6"]["average_pnl_proxy"] == 10.0


def test_sample_size_warnings() -> None:
    scorecards = build_trade_outcome_scorecards([_outcome("lane_a", pnl=10.0)], generated_at=NOW)
    group = scorecards["scorecards"]["strategy_edge"]["groups"][0]

    assert group["sample_size_warning"] == "TOO_THIN"
    assert group["scorecard_label"] == "TOO_THIN"


def test_review_candidate_selection() -> None:
    scorecards = build_trade_outcome_scorecards(
        [
            _outcome("winner", pnl=20.0),
            _outcome("loser", pnl=-30.0, hold_seconds=7200.0),
            _outcome("small", pnl=1.0),
        ],
        generated_at=NOW,
    )
    review = scorecards["review_candidates"]

    assert review["worst_realized_pnl_proxy_trades"][0]["lane_id"] == "loser"
    assert review["best_realized_pnl_proxy_trades"][0]["lane_id"] == "winner"
    assert review["long_hold_losers"][0]["lane_id"] == "loser"


def test_missing_mfe_mae_does_not_produce_false_exit_conclusions() -> None:
    scorecards = build_trade_outcome_scorecards(
        [_outcome("lane_a", pnl=10.0, mfe=None, mae=None), _outcome("lane_b", pnl=5.0, mfe=None, mae=None)],
        generated_at=NOW,
    )

    assert scorecards["data_quality"]["exit_quality_conclusion_allowed"] is False
    assert scorecards["scorecards"]["exit_policy"]["recommendations"] == [
        "MFE/MAE coverage is too sparse for strong early/late exit conclusions."
    ]


def test_empty_outcome_set_handled_safely(tmp_path: Path) -> None:
    outcomes = tmp_path / "outcomes.jsonl"
    outcomes.write_text("", encoding="utf-8")
    result = run_trade_outcome_scorecards(
        outcomes_path=outcomes,
        outcome_summary_path=tmp_path / "missing_summary.json",
        t1_scorecard_path=tmp_path / "missing_t1.json",
        output_dir=tmp_path / "scorecards",
        now=NOW,
    )

    assert result.scorecards["overall"]["outcome_count"] == 0
    assert result.json_path.exists()
    assert result.strategy_path.exists()


def test_trade_outcome_scorecard_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_trade_outcome_scorecards.py"),
        Path("src/mgc_v05l/app/track_b_trade_outcome_scorecards.py"),
    ]
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {"submit", "cancel", "modify", "placeOrder", "create_order_intent", "mutate_lifecycle", "flatten"}
    violations: list[str] = []
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(forbidden_import_roots):
                        violations.append(f"{path}:{alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith(forbidden_import_roots):
                    violations.append(f"{path}:{node.module}")
            elif isinstance(node, ast.Call):
                call_name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else None
                if call_name in forbidden_call_names:
                    violations.append(f"{path}:{call_name}")
    assert violations == []


def _outcome(
    lane: str,
    *,
    pnl: float,
    points: float | None = None,
    mfe: float | None = 8.0,
    mae: float | None = -2.0,
    exit_policy: str = "TIMEBOX",
    session: str = "LONDON",
    instrument: str = "GC",
    contract: str = "GCQ6",
    hold_seconds: float = 1800.0,
) -> dict:
    flags = ["missing_realized_r_proxy"]
    if mfe is None:
        flags.append("missing_mfe")
    if mae is None:
        flags.append("missing_mae")
    return {
        "schema_version": "track_b_canonical_trade_outcome_v1",
        "trade_outcome_id": f"outcome_{lane}_{pnl}",
        "strategy_id": f"{lane}_strategy",
        "lane_id": lane,
        "instrument": instrument,
        "contract": contract,
        "side": "LONG",
        "entry_time": "2026-07-01T10:00:00Z",
        "exit_time": "2026-07-01T10:30:00Z",
        "hold_seconds": hold_seconds,
        "realized_points": points if points is not None else pnl,
        "realized_pnl_proxy": pnl,
        "realized_r_proxy": None,
        "mfe_points": mfe,
        "mae_points": mae,
        "exit_policy": exit_policy,
        "exit_reason": exit_policy,
        "session_at_entry": session,
        "data_quality_flags": flags,
        "diagnostic_only": True,
    }
