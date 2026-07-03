from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_research_discovery_engine import (
    build_research_discovery_candidates,
    build_research_discovery_summary,
    run_research_discovery_engine,
    sample_class_for_count,
)


NOW = datetime(2026, 7, 3, 12, 0, tzinfo=UTC)


def test_candidate_generation() -> None:
    outcomes, enrichments = _fixture_rows(12)

    candidates = build_research_discovery_candidates(outcomes, enrichments=enrichments, generated_at=NOW)

    assert candidates
    assert any(row["family"] == "strategy_x_session" for row in candidates)
    assert all(row["diagnostic_only"] is True for row in candidates)


def test_sample_class_assignment() -> None:
    assert sample_class_for_count(1) == "EXPLORATORY"
    assert sample_class_for_count(10) == "PRELIMINARY"
    assert sample_class_for_count(30) == "DEVELOPING"
    assert sample_class_for_count(100) == "RESEARCH_GRADE"


def test_candidate_ranking_orders_larger_effects_first() -> None:
    outcomes, enrichments = _fixture_rows(20)
    for idx, outcome in enumerate(outcomes):
        if idx < 10:
            outcome["strategy_id"] = "strong_strategy"
            outcome["realized_pnl_proxy"] = 500.0
        else:
            outcome["strategy_id"] = "weak_strategy"
            outcome["realized_pnl_proxy"] = -50.0
        enrichments[idx]["strategy_id"] = outcome["strategy_id"]

    candidates = build_research_discovery_candidates(outcomes, enrichments=enrichments, generated_at=NOW)

    assert candidates[0]["ranking_score"] >= candidates[-1]["ranking_score"]
    assert candidates[0]["production_recommendation"] is False


def test_low_sample_warning() -> None:
    outcomes, enrichments = _fixture_rows(3)

    candidates = build_research_discovery_candidates(outcomes, enrichments=enrichments, generated_at=NOW)

    assert candidates
    assert any(row["confidence_class"] == "EXPLORATORY" for row in candidates)
    assert any("sample_class_exploratory" in row["data_quality_flags"] for row in candidates)


def test_diagnostic_and_no_production_flags() -> None:
    outcomes, enrichments = _fixture_rows(12)

    candidates = build_research_discovery_candidates(outcomes, enrichments=enrichments, generated_at=NOW)
    summary = build_research_discovery_summary(
        candidates,
        outcomes=outcomes,
        enrichments=enrichments,
        enrichment_summary={},
        t3_scorecard={},
        t6_analytics={},
        generated_at=NOW,
    )

    assert summary["diagnostic_only"] is True
    assert summary["production_recommendation"] is False
    assert summary["trading_gate"] is False
    assert all(row["strategy_change_recommendation"] is False for row in candidates)


def test_missing_context_validity_handled_safely() -> None:
    outcomes, enrichments = _fixture_rows(5)
    for row in enrichments:
        row["gre_validity_classification"] = "UNAVAILABLE"
        row["gre_label"] = None
        row["crfd_validity_classification"] = "UNAVAILABLE"
        row["vwap_relation"] = None
        row["avwap_relation"] = None

    candidates = build_research_discovery_candidates(outcomes, enrichments=enrichments, generated_at=NOW)

    assert candidates
    assert not any(row["family"] == "strategy_x_valid_gre_label" for row in candidates)
    assert not any(row["family"] == "vwap_avwap_relation_x_outcome" for row in candidates)


def test_empty_enrichment_dataset_handled_safely(tmp_path: Path) -> None:
    outcomes = tmp_path / "outcomes.jsonl"
    enrichments = tmp_path / "enrichments.jsonl"
    outcomes.write_text("", encoding="utf-8")
    enrichments.write_text("", encoding="utf-8")

    result = run_research_discovery_engine(
        outcomes_path=outcomes,
        enrichments_path=enrichments,
        enrichment_summary_path=tmp_path / "missing_summary.json",
        t3_scorecard_path=tmp_path / "missing_t3.json",
        t6_analytics_path=tmp_path / "missing_t6.json",
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert result.candidates == []
    assert result.summary["input_counts"]["candidates"] == 0
    assert result.candidates_path.exists()
    assert result.summary_path.exists()


def test_research_discovery_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_research_discovery_engine.py"),
        Path("src/mgc_v05l/app/track_b_research_discovery_engine.py"),
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


def _fixture_rows(count: int) -> tuple[list[dict], list[dict]]:
    outcomes: list[dict] = []
    enrichments: list[dict] = []
    for idx in range(count):
        trade_id = f"outcome_{idx}"
        strategy = "strategy_a" if idx % 2 == 0 else "strategy_b"
        session = "LONDON_OPEN" if idx % 2 == 0 else "US"
        pnl = 100.0 if idx % 3 else -25.0
        outcome = {
            "schema_version": "track_b_canonical_trade_outcome_v1",
            "trade_outcome_id": trade_id,
            "strategy_id": strategy,
            "lane_id": f"{strategy}_lane",
            "instrument": "GC",
            "contract": "GCQ6",
            "side": "LONG" if idx % 2 == 0 else "SHORT",
            "entry_time": "2026-07-01T10:00:00Z",
            "exit_time": "2026-07-01T10:30:00Z",
            "session_at_entry": session,
            "exit_policy": "TIMEBOX",
            "realized_points": pnl / 10,
            "realized_pnl_proxy": pnl,
            "hold_seconds": 1800,
            "data_quality_flags": ["missing_realized_r_proxy"],
            "diagnostic_only": True,
        }
        enrichment = {
            "schema_version": "track_b_trade_outcome_enrichment_v1",
            "trade_outcome_id": trade_id,
            "strategy_id": strategy,
            "lane_id": f"{strategy}_lane",
            "session": session,
            "instrument": "GC",
            "contract": "GCQ6",
            "side": outcome["side"],
            "vix_percentile": 0.35 if idx % 2 == 0 else 0.55,
            "vix_regime": "NORMAL",
            "market_context_validity_classification": "VALID",
            "gre_label": "LONG" if idx % 2 == 0 else "SHORT",
            "gre_validity_classification": "VALID",
            "crfd_validity_classification": "VALID",
            "vwap_relation": "above_vwap" if idx % 2 == 0 else "below_vwap",
            "avwap_relation": "above_avwap" if idx % 2 == 0 else "below_avwap",
            "data_quality_flags": ["missing_realized_r_proxy"],
            "diagnostic_only": True,
        }
        outcomes.append(outcome)
        enrichments.append(enrichment)
    return outcomes, enrichments


def _json_line(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True)
