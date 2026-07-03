from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_research_discovery_review_pack import (
    build_research_discovery_review_pack,
    run_research_discovery_review_pack,
    select_manual_review_candidates,
    select_top_candidates,
)


NOW = datetime(2026, 7, 3, 12, 0, tzinfo=UTC)


def test_manual_review_filtering() -> None:
    candidates = [_candidate("a", "MANUAL_REVIEW"), _candidate("b", "OBSERVE")]

    manual = select_manual_review_candidates(candidates)

    assert [row["candidate_id"] for row in manual] == ["a"]


def test_top_candidate_selection() -> None:
    candidates = [
        _candidate("a", "MANUAL_REVIEW", ranking_score=10),
        _candidate("b", "MANUAL_REVIEW", ranking_score=30),
        _candidate("c", "MANUAL_REVIEW", ranking_score=20),
    ]

    top = select_top_candidates(candidates, limit=2)

    assert [row["candidate_id"] for row in top] == ["b", "c"]


def test_research_grade_candidate_inclusion() -> None:
    candidates = [
        _candidate("a", "MANUAL_REVIEW", sample_class="PRELIMINARY"),
        _candidate("b", "OBSERVE", sample_class="RESEARCH_GRADE"),
    ]

    pack = build_research_discovery_review_pack(
        candidates,
        discovery_summary={},
        outcome_count=2,
        enrichment_count=2,
        generated_at=NOW,
    )

    assert pack["input_counts"]["research_grade_candidates"] == 1
    assert pack["research_grade_candidates"][0]["candidate_id"] == "b"


def test_guardrail_flags_preserved() -> None:
    pack = build_research_discovery_review_pack(
        [_candidate("a", "MANUAL_REVIEW")],
        discovery_summary={},
        outcome_count=1,
        enrichment_count=1,
        generated_at=NOW,
    )

    reviewed = pack["reviewed_candidates"][0]
    assert reviewed["diagnostic_only"] is True
    assert reviewed["production_recommendation"] is False
    assert reviewed["trading_gate"] is False
    assert pack["guardrails"]["strategy_change_recommendation"] is False


def test_empty_manual_review_set_handled_safely(tmp_path: Path) -> None:
    candidates = tmp_path / "candidates.jsonl"
    candidates.write_text(json.dumps(_candidate("a", "OBSERVE")) + "\n", encoding="utf-8")

    result = run_research_discovery_review_pack(
        candidates_path=candidates,
        discovery_summary_path=tmp_path / "missing_summary.json",
        outcomes_path=tmp_path / "missing_outcomes.jsonl",
        enrichments_path=tmp_path / "missing_enrichments.jsonl",
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert result.review_pack["input_counts"]["manual_review_candidates"] == 0
    assert result.review_pack["top10_overall"] == []
    assert result.json_path.exists()
    assert result.markdown_path.exists()


def test_review_pack_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_research_discovery_review_pack.py"),
        Path("src/mgc_v05l/app/track_b_research_discovery_review_pack.py"),
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


def _candidate(
    candidate_id: str,
    recommendation: str,
    *,
    sample_class: str = "PRELIMINARY",
    ranking_score: float = 1.0,
) -> dict:
    return {
        "schema_version": "track_b_research_discovery_candidate_v1",
        "candidate_id": candidate_id,
        "family": "strategy_x_session",
        "hypothesis_text": "Research hypothesis only.",
        "dimensions": ["strategy_id", "session"],
        "filters": {"strategy_id": "strategy", "session": "LONDON"},
        "sample_size": 10 if sample_class != "EXPLORATORY" else 3,
        "comparison_group": {"sample_size": 100, "average_pnl_proxy": 10.0, "win_rate": 0.5},
        "metrics": {
            "win_rate": 0.6,
            "average_pnl_proxy": 20.0,
            "median_realized_points": 2.0,
            "best_trade": {"trade_outcome_id": "best"},
            "worst_trade": {"trade_outcome_id": "worst"},
            "average_hold_seconds": 1200,
        },
        "effect_size_proxy": 10.0,
        "confidence_class": sample_class,
        "data_quality_flags": ["missing_realized_r_proxy"],
        "context_validity_requirements": {},
        "recommendation_level": recommendation,
        "ranking_score": ranking_score,
        "source_refs": {},
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "strategy_change_recommendation": False,
    }
