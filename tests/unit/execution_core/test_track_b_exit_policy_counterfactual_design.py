from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_exit_policy_counterfactual_design import (
    build_exit_counterfactual_observations,
    build_exit_counterfactual_summary,
    run_exit_policy_counterfactual_design,
)


NOW = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)


def test_timebox_counterfactuals_are_blocked_without_path_data() -> None:
    rows = build_exit_counterfactual_observations(
        [_outcome("t1", pnl=10.0, mfe=None, mae=None)],
        attributions=[_attribution("t1")],
        generated_at=NOW,
    )

    row = rows[0]
    assert row["timebox_exit"] is True
    assert row["shorter_timebox_counterfactual"]["status"] == "NEEDS_INTRATRADE_EXCURSION_CURVE"
    assert row["longer_timebox_counterfactual"]["status"] == "NEEDS_POST_EXIT_FORWARD_PATH"
    assert row["counterfactual_testability"] == "NOT_TESTABLE_MISSING_MFE_MAE_AND_PATH"
    assert row["diagnostic_only"] is True
    assert row["production_recommendation"] is False
    assert row["trading_gate"] is False


def test_profitable_before_timeout_detected_when_mfe_present() -> None:
    rows = build_exit_counterfactual_observations(
        [_outcome("t1", pnl=-2.0, mfe=5.0, mae=-1.0)],
        attributions=[_attribution("t1")],
        generated_at=NOW,
    )

    assert rows[0]["profitable_before_timeout"] is True
    assert rows[0]["counterfactual_testability"] == "BOUNDS_ONLY_MFE_MAE_PRESENT_PATH_MISSING"


def test_losing_early_never_recovered_detected_when_mfe_mae_present() -> None:
    rows = build_exit_counterfactual_observations(
        [_outcome("t1", pnl=-10.0, mfe=0.0, mae=-4.0)],
        attributions=[_attribution("t1")],
        generated_at=NOW,
    )

    assert rows[0]["losing_early_never_recovered"] is True


def test_summary_ranks_adaptive_exit_lanes() -> None:
    outcomes = [
        _outcome("a1", lane="lane_a", pnl=-10.0),
        _outcome("a2", lane="lane_a", pnl=-5.0),
        _outcome("a3", lane="lane_a", pnl=-3.0),
        _outcome("a4", lane="lane_a", pnl=1.0),
        _outcome("a5", lane="lane_a", pnl=2.0),
        _outcome("b1", lane="lane_b", pnl=10.0),
    ]
    attributions = [_attribution(row["trade_outcome_id"]) for row in outcomes]
    rows = build_exit_counterfactual_observations(outcomes, attributions=attributions, generated_at=NOW)
    summary = build_exit_counterfactual_summary(rows, generated_at=NOW)

    assert summary["adaptive_exit_lane_priorities"][0]["lane_id"] == "lane_a"
    assert summary["adaptive_exit_lane_priorities"][0]["research_label"] == "ADAPTIVE_EXIT_RESEARCH_PRIORITY"
    assert summary["timebox_counterfactuals"]["shorter_timebox_testable_now"] == 0


def test_missing_vwap_atr_structure_data_reported() -> None:
    rows = build_exit_counterfactual_observations([_outcome("t1", pnl=1.0)], generated_at=NOW)
    summary = build_exit_counterfactual_summary(rows, generated_at=NOW)
    missing = {row["field"]: row for row in summary["data_missing_to_test_vwap_atr_structure_exits"]}

    assert missing["timestamped_in_trade_excursion_curve"]["missing_count"] == 1
    assert missing["atr_at_entry"]["missing_count"] == 1
    assert missing["vwap_relation_at_exit"]["missing_count"] == 1


def test_run_writes_json_and_markdown_artifacts(tmp_path: Path) -> None:
    outcomes = tmp_path / "outcomes.jsonl"
    enrichments = tmp_path / "enrichments.jsonl"
    attributions = tmp_path / "attributions.jsonl"
    outcomes.write_text(json.dumps(_outcome("t1", pnl=3.0)) + "\n", encoding="utf-8")
    enrichments.write_text(json.dumps(_enrichment("t1")) + "\n", encoding="utf-8")
    attributions.write_text(json.dumps(_attribution("t1")) + "\n", encoding="utf-8")

    result = run_exit_policy_counterfactual_design(
        outcomes_path=outcomes,
        enrichments_path=enrichments,
        attributions_path=attributions,
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert json.loads(result.summary_json_path.read_text())["schema_version"] == "ra4_exit_policy_counterfactual_summary_v1"
    assert result.summary_markdown_path.exists()
    assert result.lane_priority_path.exists()
    assert len(result.observations) == 1


def test_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_exit_policy_counterfactual_design.py"),
        Path("src/mgc_v05l/app/track_b_exit_policy_counterfactual_design.py"),
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
    trade_id: str,
    *,
    lane: str = "gc_us_active_participation_long",
    pnl: float = 1.0,
    mfe: float | None = None,
    mae: float | None = None,
) -> dict:
    return {
        "trade_outcome_id": trade_id,
        "strategy_id": "PAPER_ACTIVE_EVIDENCE_GC_US_PARTICIPATION_LONG_V1",
        "lane_id": lane,
        "instrument": "GC",
        "contract": "GCQ6",
        "side": "LONG",
        "entry_time": "2026-07-01T01:00:00Z",
        "exit_time": "2026-07-01T02:00:00Z",
        "session_at_entry": "US",
        "hold_seconds": 3600.0,
        "realized_pnl_proxy": pnl,
        "realized_points": pnl / 100,
        "mfe_points": mfe,
        "mae_points": mae,
        "exit_policy": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        "exit_reason": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
    }


def _attribution(trade_id: str) -> dict:
    return {
        "trade_outcome_id": trade_id,
        "exit": {
            "canonical_exit_reason": "MANAGED_EXIT_TIMEOUT",
            "managed_exit_involved": True,
            "required_completed_bars": 12,
            "elapsed_completed_bars": 12,
        },
    }


def _enrichment(trade_id: str) -> dict:
    return {
        "trade_outcome_id": trade_id,
        "gre_validity_classification": "VALID",
        "gre_label": "LONG",
        "crfd_validity_classification": "VALID",
        "vwap_relation": "above_vwap",
        "avwap_relation": "above_avwap",
        "vix_regime": "NORMAL",
        "vix_percentile": 0.5,
    }
