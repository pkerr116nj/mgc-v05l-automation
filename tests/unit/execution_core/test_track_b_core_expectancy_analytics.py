from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_core_expectancy_analytics import (
    aggregate_expectancy_group,
    build_analytics_coverage_matrix,
    build_core_expectancy_analytics,
    percentile_distribution,
    run_core_expectancy_analytics,
    sample_class_for_count,
)


NOW = datetime(2026, 7, 3, 12, 0, tzinfo=UTC)


def test_analytics_coverage_classification() -> None:
    rows = [
        _row("a", pnl=10.0, vix_valid=True, gre_valid=True),
        _row("b", pnl=-5.0, vix_valid=True, gre_valid=False),
    ]

    matrix = build_analytics_coverage_matrix(rows, generated_at=NOW)
    capabilities = {row["capability"]: row for row in matrix["capabilities"]}

    assert capabilities["strategy_expectancy"]["status"] == "READY"
    assert capabilities["vix_conditioned_performance"]["status"] == "READY"
    assert capabilities["gre_conditioned_performance"]["status"] == "PARTIAL"
    assert capabilities["r_multiple_analytics"]["status"] == "BLOCKED"


def test_expectancy_aggregation() -> None:
    group = aggregate_expectancy_group("fixture", [_row("a", pnl=10.0, points=2.0), _row("b", pnl=-4.0, points=-1.0)])

    assert group["count"] == 2
    assert group["win_rate"] == 0.5
    assert group["average_pnl_proxy"] == 3.0
    assert group["median_realized_points"] == 0.5
    assert group["best_trade"]["trade_outcome_id"] == "a"
    assert group["worst_trade"]["trade_outcome_id"] == "b"


def test_distribution_percentiles() -> None:
    dist = percentile_distribution([1.0, 2.0, 3.0, 4.0, 5.0])

    assert dist["p0"] == 1.0
    assert dist["p50"] == 3.0
    assert dist["p100"] == 5.0


def test_sample_class_labeling() -> None:
    assert sample_class_for_count(1) == "EXPLORATORY"
    assert sample_class_for_count(10) == "PRELIMINARY"
    assert sample_class_for_count(30) == "DEVELOPING"
    assert sample_class_for_count(100) == "RESEARCH_GRADE"


def test_missing_r_mfe_mae_handled_safely() -> None:
    rows = [_row("a", pnl=10.0), _row("b", pnl=-3.0)]

    matrix = build_analytics_coverage_matrix(rows, generated_at=NOW)
    analytics = build_core_expectancy_analytics(rows, coverage_matrix=matrix, generated_at=NOW)

    assert analytics["data_quality"]["missing_realized_r_proxy"] == 2
    assert analytics["data_quality"]["missing_mfe"] == 2
    assert analytics["data_quality"]["missing_mae"] == 2
    assert analytics["production_recommendation"] is False
    assert analytics["trading_gate"] is False


def test_empty_dataset_handled_safely(tmp_path: Path) -> None:
    outcomes = tmp_path / "outcomes.jsonl"
    enrichments = tmp_path / "enrichments.jsonl"
    outcomes.write_text("", encoding="utf-8")
    enrichments.write_text("", encoding="utf-8")

    result = run_core_expectancy_analytics(
        outcomes_path=outcomes,
        enrichments_path=enrichments,
        enrichment_summary_path=tmp_path / "missing.json",
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert result.analytics["overall"]["count"] == 0
    assert result.coverage_matrix["coverage"]["total_outcomes"] == 0
    assert result.analytics_json_path.exists()
    assert result.coverage_markdown_path.exists()


def test_core_expectancy_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_core_expectancy_analytics.py"),
        Path("src/mgc_v05l/app/track_b_core_expectancy_analytics.py"),
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


def test_cli_artifact_json_parse(tmp_path: Path) -> None:
    outcomes = tmp_path / "outcomes.jsonl"
    enrichments = tmp_path / "enrichments.jsonl"
    rows = [_row("a", pnl=10.0, vix_valid=True), _row("b", pnl=-2.0, vix_valid=True)]
    outcomes.write_text("\n".join(json.dumps(_outcome_only(row)) for row in rows) + "\n", encoding="utf-8")
    enrichments.write_text("\n".join(json.dumps(_enrichment_only(row)) for row in rows) + "\n", encoding="utf-8")

    result = run_core_expectancy_analytics(
        outcomes_path=outcomes,
        enrichments_path=enrichments,
        enrichment_summary_path=tmp_path / "missing.json",
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert json.loads(result.analytics_json_path.read_text())["overall"]["count"] == 2
    assert json.loads(result.coverage_json_path.read_text())["coverage"]["vix_valid_coverage"] == 1.0


def _row(
    trade_id: str,
    *,
    pnl: float,
    points: float | None = None,
    vix_valid: bool = False,
    gre_valid: bool = False,
) -> dict:
    return {
        "trade_outcome_id": trade_id,
        "strategy_id": "strategy_a",
        "lane_id": "lane_a",
        "session_at_entry": "LONDON",
        "instrument": "GC",
        "contract": "GCQ6",
        "side": "LONG",
        "exit_policy": "TIMEBOX",
        "entry_time": "2026-07-01T10:00:00Z",
        "hold_seconds": 1800.0,
        "realized_points": pnl if points is None else points,
        "realized_pnl_proxy": pnl,
        "realized_r_proxy": None,
        "mfe_points": None,
        "mae_points": None,
        "vix_regime": "NORMAL" if vix_valid else None,
        "vix_percentile": 0.4 if vix_valid else None,
        "market_context_validity_classification": "VALID" if vix_valid else "UNAVAILABLE",
        "gre_label": "LONG" if gre_valid else None,
        "gre_confidence": 64 if gre_valid else None,
        "gre_validity_classification": "VALID" if gre_valid else "UNAVAILABLE",
        "crfd_validity_classification": "VALID" if gre_valid else "UNAVAILABLE",
        "data_quality_flags": ["missing_realized_r_proxy", "missing_mfe", "missing_mae"],
        "diagnostic_only": True,
    }


def _outcome_only(row: dict) -> dict:
    return {key: value for key, value in row.items() if not key.startswith(("vix_", "gre_", "market_context_", "crfd_"))}


def _enrichment_only(row: dict) -> dict:
    return {
        "trade_outcome_id": row["trade_outcome_id"],
        "vix_regime": row.get("vix_regime"),
        "vix_percentile": row.get("vix_percentile"),
        "market_context_validity_classification": row.get("market_context_validity_classification"),
        "gre_label": row.get("gre_label"),
        "gre_confidence": row.get("gre_confidence"),
        "gre_validity_classification": row.get("gre_validity_classification"),
        "crfd_validity_classification": row.get("crfd_validity_classification"),
        "data_quality_flags": row.get("data_quality_flags", []),
        "diagnostic_only": True,
    }
