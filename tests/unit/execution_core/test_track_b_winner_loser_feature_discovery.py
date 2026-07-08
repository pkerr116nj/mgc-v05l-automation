from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_winner_loser_feature_discovery import (
    build_feature_rows,
    build_winner_loser_feature_discovery,
    run_winner_loser_feature_discovery,
)


NOW = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)


def test_generates_winner_loser_hypotheses_for_lane() -> None:
    rows = build_feature_rows(_outcomes(), enrichments=_enrichments())
    summary, hypotheses = build_winner_loser_feature_discovery(rows, generated_at=NOW)

    assert summary["overall"]["trade_count"] == 4
    assert summary["overall"]["hypothesis_count"] > 0
    assert summary["lanes"][0]["winner_count"] == 2
    assert summary["lanes"][0]["loser_count"] == 2
    assert any("tend to" in row["hypothesis_text"] for row in hypotheses)
    assert summary["diagnostic_only"] is True
    assert summary["production_recommendation"] is False
    assert summary["trading_gate"] is False


def test_categorical_feature_ranking_separates_sessions() -> None:
    rows = build_feature_rows(_outcomes(), enrichments=_enrichments())
    summary, _ = build_winner_loser_feature_discovery(rows, generated_at=NOW)
    rankings = summary["lanes"][0]["feature_rankings"]

    session_ranking = next(row for row in rankings if row["feature"] == "session_at_entry")
    assert session_ranking["value"] == "US_ACTIVE"
    assert session_ranking["direction"] == "WINNER_ASSOCIATED"
    assert session_ranking["separation_score"] == 1.0


def test_numeric_feature_ranking_detects_vix_difference() -> None:
    rows = build_feature_rows(_outcomes(), enrichments=_enrichments())
    summary, _ = build_winner_loser_feature_discovery(rows, generated_at=NOW)
    rankings = summary["lanes"][0]["feature_rankings"]

    vix_ranking = next(row for row in rankings if row["feature"] == "vix_level")
    assert vix_ranking["direction"] == "WINNER_LOWER"
    assert vix_ranking["winner_mean"] < vix_ranking["loser_mean"]


def test_low_sample_and_missing_context_flags_are_explicit() -> None:
    rows = build_feature_rows(_outcomes()[:2], enrichments=[])
    summary, hypotheses = build_winner_loser_feature_discovery(rows, generated_at=NOW)
    flags = summary["lanes"][0]["data_quality_flags"]

    assert hypotheses == []
    assert "low_total_sample" in flags
    assert "missing_valid_gre_context" in flags
    assert "missing_mfe_mae_context" in flags


def test_hypotheses_preserve_research_guardrails() -> None:
    rows = build_feature_rows(_outcomes(), enrichments=_enrichments())
    _, hypotheses = build_winner_loser_feature_discovery(rows, generated_at=NOW)

    assert hypotheses
    assert all(row["diagnostic_only"] is True for row in hypotheses)
    assert all(row["production_recommendation"] is False for row in hypotheses)
    assert all(row["trading_gate"] is False for row in hypotheses)


def test_run_writes_parseable_artifacts(tmp_path: Path) -> None:
    outcomes = tmp_path / "outcomes.jsonl"
    enrichments = tmp_path / "enrichments.jsonl"
    empty = tmp_path / "empty.jsonl"
    outcomes.write_text("".join(json.dumps(row) + "\n" for row in _outcomes()), encoding="utf-8")
    enrichments.write_text("".join(json.dumps(row) + "\n" for row in _enrichments()), encoding="utf-8")
    empty.write_text("", encoding="utf-8")

    result = run_winner_loser_feature_discovery(
        outcomes_path=outcomes,
        enrichments_path=enrichments,
        attributions_path=empty,
        trade_paths_path=empty,
        output_dir=tmp_path / "out",
        now=NOW,
    )

    parsed = json.loads(result.summary_json_path.read_text(encoding="utf-8"))
    hypotheses = [json.loads(line) for line in result.hypotheses_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert parsed["schema_version"] == "ra9_winner_loser_feature_discovery_v1"
    assert hypotheses
    assert result.summary_markdown_path.exists()
    assert result.lane_scorecard_path.exists()
    assert result.feature_rankings_path.exists()
    assert result.data_quality_path.exists()
    assert result.contract_path.exists()


def test_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_winner_loser_feature_discovery.py"),
        Path("src/mgc_v05l/app/track_b_winner_loser_feature_discovery.py"),
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


def _outcomes() -> list[dict]:
    return [
        _outcome("t1", 120.0, "US_ACTIVE", "2026-07-07T13:05:00Z"),
        _outcome("t2", 90.0, "US_ACTIVE", "2026-07-07T13:10:00Z"),
        _outcome("t3", -80.0, "GLOBEX", "2026-07-07T00:05:00Z"),
        _outcome("t4", -50.0, "ASIA", "2026-07-07T00:15:00Z"),
    ]


def _outcome(trade_id: str, pnl: float, session: str, entry_time: str) -> dict:
    return {
        "trade_outcome_id": f"outcome_{trade_id}",
        "strategy_id": "strategy",
        "lane_id": "gc_research_lane",
        "instrument": "GC",
        "contract": "GCQ6",
        "side": "LONG",
        "entry_time": entry_time,
        "exit_time": "2026-07-07T14:05:00Z",
        "hold_seconds": 3600,
        "realized_pnl_proxy": pnl,
        "realized_points": pnl / 10,
        "session_at_entry": session,
        "exit_policy": "TIMEBOX_60M",
    }


def _enrichments() -> list[dict]:
    return [
        _enrichment("t1", 15.0, 0.25, "LONG", 0.8, "ABOVE"),
        _enrichment("t2", 15.5, 0.30, "LONG", 0.7, "ABOVE"),
        _enrichment("t3", 23.0, 0.75, "SHORT", 0.6, "BELOW"),
        _enrichment("t4", 24.0, 0.80, "SHORT", 0.5, "BELOW"),
    ]


def _enrichment(trade_id: str, vix: float, percentile: float, gre_label: str, gre_confidence: float, vwap: str) -> dict:
    return {
        "trade_outcome_id": f"outcome_{trade_id}",
        "vix_level": vix,
        "vix_percentile": percentile,
        "vix_regime": "NORMAL",
        "market_context_validity_classification": "VALID",
        "gre_validity_classification": "VALID",
        "gre_label": gre_label,
        "gre_confidence": gre_confidence,
        "vwap_relation": vwap,
        "avwap_relation": vwap,
    }
