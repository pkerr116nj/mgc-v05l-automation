from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_offline_research_experiment_engine import (
    build_experiment_population,
    calculate_metrics,
    create_experiment,
    run_experiment,
    run_offline_research_experiments,
)


NOW = datetime(2026, 7, 9, 12, 0, tzinfo=UTC)


def test_create_experiment_preserves_guardrails() -> None:
    experiment = create_experiment(
        experiment_id="exp",
        title="Experiment",
        hypothesis="Research hypothesis only.",
        experiment_type="ENTRY_FILTER",
        generated_at=NOW,
    )

    assert experiment["schema_version"] == "canonical_research_experiment_v1"
    assert experiment["status"] == "DRAFT"
    assert experiment["diagnostic_only"] is True
    assert experiment["production_recommendation"] is False
    assert experiment["trading_gate"] is False
    assert experiment["deterministic_fingerprint"]


def test_run_timebox_experiment_with_complete_path_fixture() -> None:
    rows = build_experiment_population([_outcome("t1", 10.0)], enrichments=[_enrichment("t1")], trade_paths=[_path("t1", complete=True)])
    experiment = create_experiment(
        experiment_id="timebox",
        title="Timebox",
        hypothesis="Timebox variant can be tested.",
        experiment_type="EXIT_POLICY",
        variant_definition={"kind": "timebox_grid", "minutes": [15, 30]},
        data_requirements=["complete_entry_to_exit_path"],
        generated_at=NOW,
    )

    result = run_experiment(experiment, rows=rows, generated_at=NOW)

    assert result["status"] == "COMPLETE"
    assert result["variant_count"] == 1
    assert len(result["variant_metrics"]["grid_results"]) == 2
    assert result["guardrails"]["trading_gate"] is False


def test_blocked_timebox_experiment_when_path_missing() -> None:
    rows = build_experiment_population([_outcome("t1", 10.0)], enrichments=[_enrichment("t1")], trade_paths=[])
    experiment = create_experiment(
        experiment_id="timebox",
        title="Timebox",
        hypothesis="Timebox variant requires paths.",
        experiment_type="EXIT_POLICY",
        variant_definition={"kind": "timebox_grid", "minutes": [15]},
        generated_at=NOW,
    )

    result = run_experiment(experiment, rows=rows, generated_at=NOW)

    assert result["status"] == "BLOCKED"
    assert result["confidence_label"] == "INSUFFICIENT_DATA"
    assert "blocked_missing_complete_path_samples" in result["data_quality_flags"]


def test_run_entry_filter_experiment() -> None:
    rows = build_experiment_population(
        [_outcome("t1", 10.0), _outcome("t2", -5.0)],
        enrichments=[_enrichment("t1", vix=0.7), _enrichment("t2", vix=0.2)],
    )
    experiment = create_experiment(
        experiment_id="vix_filter",
        title="VIX Filter",
        hypothesis="VIX filter is diagnostic.",
        experiment_type="ENTRY_FILTER",
        variant_definition={"kind": "field_filter", "filters": [{"field": "vix_percentile", "op": "gte", "value": 0.5}]},
        generated_at=NOW,
    )

    result = run_experiment(experiment, rows=rows, generated_at=NOW)

    assert result["status"] == "COMPLETE"
    assert result["baseline_count"] == 2
    assert result["variant_count"] == 1
    assert result["variant_metrics"]["total_pnl_proxy"] == 10.0


def test_missing_context_excluded_and_counted() -> None:
    rows = build_experiment_population([_outcome("t1", 10.0), _outcome("t2", -5.0)], enrichments=[_enrichment("t1", vix=0.7)])
    experiment = create_experiment(
        experiment_id="vix_filter",
        title="VIX Filter",
        hypothesis="Missing context is excluded.",
        experiment_type="ENTRY_FILTER",
        variant_definition={"filters": [{"field": "vix_percentile", "op": "gte", "value": 0.5}]},
        generated_at=NOW,
    )

    result = run_experiment(experiment, rows=rows, generated_at=NOW)

    assert result["variant_count"] == 1
    assert result["exclusion_reasons"]["missing_vix_percentile"] == 1


def test_metrics_are_deterministic() -> None:
    metrics = calculate_metrics([_outcome("t1", 10.0), _outcome("t2", -5.0), _outcome("t3", 15.0)])

    assert metrics["trade_count"] == 3
    assert metrics["total_pnl_proxy"] == 20.0
    assert metrics["win_rate"] == 0.666667
    assert metrics["profit_factor_proxy"] == 5.0
    assert metrics["payoff_ratio"] == 2.5


def test_fingerprints_are_stable() -> None:
    rows = build_experiment_population([_outcome("t1", 10.0)], enrichments=[_enrichment("t1")])
    experiment = create_experiment(
        experiment_id="vix_filter",
        title="VIX Filter",
        hypothesis="Stable result.",
        experiment_type="ENTRY_FILTER",
        variant_definition={"filters": [{"field": "vix_percentile", "op": "gte", "value": 0.5}]},
        generated_at=NOW,
    )

    first = run_experiment(experiment, rows=rows, generated_at=NOW)
    second = run_experiment(experiment, rows=rows, generated_at=NOW)

    assert first["deterministic_fingerprint"] == second["deterministic_fingerprint"]


def test_run_sample_experiments_writes_parseable_artifacts(tmp_path: Path) -> None:
    outcomes = tmp_path / "outcomes.jsonl"
    enrichments = tmp_path / "enrichments.jsonl"
    paths = tmp_path / "paths.jsonl"
    hypotheses = tmp_path / "hypotheses.jsonl"
    outcomes.write_text(json.dumps(_outcome("t1", 10.0)) + "\n", encoding="utf-8")
    enrichments.write_text(json.dumps(_enrichment("t1")) + "\n", encoding="utf-8")
    paths.write_text(json.dumps(_path("t1", complete=False)) + "\n", encoding="utf-8")
    hypotheses.write_text(json.dumps({"lane_id": "lane", "separation_score": 0.5}) + "\n", encoding="utf-8")

    result = run_offline_research_experiments(
        outcomes_path=outcomes,
        enrichments_path=enrichments,
        trade_paths_path=paths,
        ra9_hypotheses_path=hypotheses,
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert json.loads(result.sample_experiments_path.read_text())[0]["schema_version"] == "canonical_research_experiment_v1"
    assert json.loads(result.sample_results_path.read_text())[0]["schema_version"] == "canonical_research_experiment_result_v1"
    assert result.summary["experiment_count"] == 4
    assert result.summary_path.exists()
    assert result.data_quality_path.exists()
    assert result.blocked_path.exists()


def test_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_offline_research_experiment_engine.py"),
        Path("src/mgc_v05l/app/track_b_offline_research_experiment_engine.py"),
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


def _outcome(trade_id: str, pnl: float) -> dict:
    return {
        "trade_outcome_id": f"outcome_{trade_id}",
        "lane_id": "lane",
        "strategy_id": "strategy",
        "instrument": "GC",
        "contract": "GCQ6",
        "side": "LONG",
        "entry_time": "2026-07-01T10:00:00Z",
        "exit_time": "2026-07-01T11:00:00Z",
        "entry_price": 100.0,
        "exit_price": 101.0,
        "realized_points": pnl / 10.0,
        "realized_pnl_proxy": pnl,
        "session_at_entry": "US_ACTIVE",
    }


def _enrichment(trade_id: str, *, vix: float = 0.7) -> dict:
    return {
        "trade_outcome_id": f"outcome_{trade_id}",
        "vix_percentile": vix,
        "vix_regime": "NORMAL",
        "gre_validity_classification": "VALID",
        "gre_label": "LONG",
    }


def _path(trade_id: str, *, complete: bool) -> dict:
    return {
        "trade_outcome_id": f"outcome_{trade_id}",
        "canonical_trade_path_id": f"path_{trade_id}",
        "path_coverage_status": "COMPLETE" if complete else "PARTIAL",
        "path_complete_entry_to_exit": complete,
        "path_sample_count": 3,
        "mfe": 2.0,
        "mae": -1.0,
        "entry_to_exit_path": [
            {"bar_end": "2026-07-01T10:00:00Z", "close": 100.0},
            {"bar_end": "2026-07-01T10:15:00Z", "close": 101.0},
            {"bar_end": "2026-07-01T10:30:00Z", "close": 102.0},
        ],
        "counterfactual_ready": {"timebox": complete, "trailing": complete, "vwap_avwap": False, "atr": False},
    }
