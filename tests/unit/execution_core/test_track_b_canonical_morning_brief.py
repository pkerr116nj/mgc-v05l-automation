from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_morning_brief import (
    build_canonical_morning_brief,
    run_canonical_morning_brief,
)


NOW = datetime(2026, 7, 4, 12, 0, tzinfo=UTC)


def test_aggregation_succeeds_with_all_inputs(tmp_path: Path) -> None:
    paths = _seed_artifacts(tmp_path)

    result = run_canonical_morning_brief(output_dir=tmp_path / "brief", artifact_paths=paths, now=NOW)

    assert result.brief["platform"]["certification_classification"] == "PLATFORM_CERTIFIED_WITH_WARNINGS"
    assert result.brief["analytics"]["latest_insight_count"] == 1
    assert result.brief["research"]["manual_review_count"] == 2
    assert result.brief["market_context"]["current_market_context_status"] == "READY"
    assert result.json_path.exists()
    assert result.markdown_path.exists()


def test_aggregation_succeeds_with_missing_optional_artifacts(tmp_path: Path) -> None:
    paths = _seed_artifacts(tmp_path)
    paths["cae_insights"] = tmp_path / "missing_insights.json"
    paths["research_discovery_candidates"] = tmp_path / "missing_candidates.jsonl"

    result = run_canonical_morning_brief(output_dir=tmp_path / "brief", artifact_paths=paths, now=NOW)

    assert result.brief["analytics"]["latest_insight_count"] == 0
    assert result.brief["research"]["top_research_grade_candidates"] == []
    assert any(item["present"] is False for item in result.brief["component_inventory"])


def test_deterministic_output_ordering(tmp_path: Path) -> None:
    paths = _seed_artifacts(tmp_path)
    first = run_canonical_morning_brief(output_dir=tmp_path / "a", artifact_paths=paths, now=NOW).brief
    second = run_canonical_morning_brief(output_dir=tmp_path / "b", artifact_paths=paths, now=NOW).brief

    assert first["component_inventory"] == second["component_inventory"]
    assert first["sections"] == second["sections"]


def test_provenance_retained(tmp_path: Path) -> None:
    paths = _seed_artifacts(tmp_path)

    result = run_canonical_morning_brief(output_dir=tmp_path / "brief", artifact_paths=paths, now=NOW)

    provenance = result.brief["data_provenance"]
    assert provenance
    assert all("component" in item and "path" in item for item in provenance)


def test_guardrails_retained(tmp_path: Path) -> None:
    paths = _seed_artifacts(tmp_path)

    result = run_canonical_morning_brief(output_dir=tmp_path / "brief", artifact_paths=paths, now=NOW)

    assert result.brief["diagnostic_only"] is True
    assert result.brief["production_recommendation"] is False
    assert result.brief["trading_gate"] is False


def test_build_from_loaded_artifacts_directly() -> None:
    loaded = {
        "operational_certification": {"path": "cert.json", "present": True, "loaded": True, "artifact_type": "json", "sha256": "abc", "payload": {"classification": "OK"}},
        "cae_insights": {"path": "insights.json", "present": True, "loaded": True, "artifact_type": "json", "sha256": "def", "payload": []},
    }

    brief = build_canonical_morning_brief(loaded, generated_at=NOW)

    assert brief["schema_version"] == "cae9_canonical_morning_brief_v1"
    assert brief["sections"]["platform"]["headline"] == "Platform: OK"


def test_morning_brief_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_canonical_morning_brief.py"),
        Path("src/mgc_v05l/app/track_b_canonical_morning_brief.py"),
    ]
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {"submit", "cancel", "modify", "placeOrder", "create_order_intent", "mutate_lifecycle", "flatten", "global_cancel"}
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


def _seed_artifacts(tmp_path: Path) -> dict[str, Path]:
    paths = {
        "operational_certification": tmp_path / "cert.json",
        "safe_state": tmp_path / "safe_state.json",
        "guardian": tmp_path / "guardian.json",
        "managed_exit": tmp_path / "managed_exit.json",
        "research_discovery_summary": tmp_path / "research_summary.json",
        "research_discovery_candidates": tmp_path / "candidates.jsonl",
        "trade_outcome_enrichment_summary": tmp_path / "enrichment_summary.json",
        "canonical_market_context_summary": tmp_path / "cmc_summary.json",
        "cae_catalog_summary": tmp_path / "catalog.json",
        "cae_saved_query_summary": tmp_path / "saved.json",
        "cae_execution_summary": tmp_path / "execution.json",
        "cae_result_diff": tmp_path / "diff.json",
        "cae_insights": tmp_path / "insights.json",
    }
    _write_json(paths["operational_certification"], {"classification": "PLATFORM_CERTIFIED_WITH_WARNINGS", "summary": {"warnings": ["runtime_domain_warning"], "critical_failures": []}, "domains": {"runtime": {"classification": "WARN"}}})
    _write_json(paths["safe_state"], {"classification": "SAFE_STATE_NORMAL"})
    _write_json(paths["guardian"], {"classification": "BROKER_POSITION_GUARDIAN_READY"})
    _write_json(paths["managed_exit"], {"status": "NO_ELIGIBLE_EXITS", "generated_at": NOW.isoformat()})
    _write_json(paths["research_discovery_summary"], {"input_counts": {"candidates": 3}, "candidate_counts": {"by_recommendation_level": {"MANUAL_REVIEW": 2}, "by_sample_class": {"RESEARCH_GRADE": 1}, "by_family": {"strategy_x_session": 3}}, "top_candidates": []})
    paths["research_discovery_candidates"].write_text(json.dumps({"candidate_id": "c1", "confidence_class": "RESEARCH_GRADE"}) + "\n", encoding="utf-8")
    _write_json(paths["trade_outcome_enrichment_summary"], {"context_validity": {"gre": {"classification_counts": {"VALID": 2}}, "market_context": {"classification_counts": {"VALID": 3}}}})
    _write_json(paths["canonical_market_context_summary"], {"provider_count": 1, "vix": {"available": True, "join_readiness": "READY", "row_count": 10}})
    _write_json(paths["cae_catalog_summary"], {"schema_version": "track_b_canonical_analytics_catalog_v1", "dimensions": {"strategy": {}}, "metrics": {"trade_count": {}}, "filters": {"valid_vix_only": {}}})
    _write_json(paths["cae_saved_query_summary"], {"saved_query_count": 7, "preset_count": 7})
    _write_json(paths["cae_execution_summary"], {"query_id": "expectancy_by_strategy", "outcome_count_matched": 431})
    _write_json(paths["cae_result_diff"], {"query_id": "expectancy_by_strategy", "status": "UNCHANGED", "comparison_classification": "UNCHANGED"})
    _write_json(paths["cae_insights"], [{"title": "No meaningful analytics changes", "diagnostic_only": True, "production_recommendation": False, "trading_gate": False}])
    return paths


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
