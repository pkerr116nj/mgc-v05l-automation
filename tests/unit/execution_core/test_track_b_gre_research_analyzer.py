from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_gold_regime_engine import GOLD_REGIME_OUTPUT_DIR
from mgc_v05l.execution_core.track_b_gre_research_analyzer import (
    INSUFFICIENT_SAMPLE,
    LATEST_GRE_RESEARCH_ANALYZER_JSON,
    LATEST_GRE_RESEARCH_ANALYZER_MD,
    SHADOW_RESEARCH_READY,
    build_gre_research_analyzer,
    run_gre_research_analyzer,
)
from mgc_v05l.execution_core.track_b_regime_engine_interface import (
    regime_plugin_interface_contract,
    validate_regime_output_compatibility,
)


NOW = datetime(2026, 6, 30, 16, 0, tzinfo=UTC)


def test_tiny_sample_returns_insufficient_sample_and_data_collection_recommendation() -> None:
    analyzer = build_gre_research_analyzer(
        gre_report=_gre_report(),
        scorecard=_scorecard(total=2, validated=2),
        validation_summary={"directional_correctness_rate": None},
        validation_rows=[],
        generated_at=NOW,
    )

    assert analyzer["sample_assessment"]["sample_status"] == INSUFFICIENT_SAMPLE
    assert analyzer["sample_assessment"]["sample_sufficient"] is False
    assert "Historical candle/session backfill" in analyzer["recommended_next_feature_provider"]
    assert any("Do not tune GRE scoring" in item for item in analyzer["overfitting_warnings"])


def test_stronger_synthetic_scorecard_recommends_provider() -> None:
    analyzer = build_gre_research_analyzer(
        gre_report=_gre_report(),
        scorecard=_scorecard(total=120, validated=100),
        validation_summary={"directional_correctness_rate": 0.58},
        validation_rows=[],
        generated_at=NOW,
    )

    assert analyzer["sample_assessment"]["sample_status"] == SHADOW_RESEARCH_READY
    assert analyzer["recommended_next_feature_provider"] == "VWAP"
    assert "shadow replay" in analyzer["recommended_next_research_experiment"]


def test_missing_providers_are_ranked_from_rows_when_scorecard_missing() -> None:
    rows = [
        {"missing_providers": ["VWAP unavailable in GRE MVP", "overnight high/low unavailable in GRE MVP"]},
        {"missing_providers": ["VWAP unavailable in GRE MVP"]},
    ]

    analyzer = build_gre_research_analyzer(
        gre_report=_gre_report(),
        scorecard={"overall_metrics": {"total_observations": 2, "validated_observations": 2}},
        validation_summary={},
        validation_rows=rows,
        generated_at=NOW,
    )

    assert analyzer["missing_provider_impact"][0] == {"provider": "VWAP unavailable in GRE MVP", "count": 2}
    assert analyzer["missing_provider_impact"][1] == {"provider": "overnight high/low unavailable in GRE MVP", "count": 1}


def test_generic_interface_schema_compatibility() -> None:
    contract = regime_plugin_interface_contract()
    compatibility = validate_regime_output_compatibility(_gre_report())

    assert contract["schema_version"] == "track_b_regime_plugin_interface_v1"
    assert {"GRE", "NRE", "ERE", "TRE"}.issubset(set(contract["supported_future_plugins"]))
    assert compatibility["compatible"] is True


def test_analyzer_runner_writes_outputs_and_interface_docs(tmp_path: Path) -> None:
    output_root = tmp_path / "outputs" / "track_b_execution_core"
    gold_dir = output_root / GOLD_REGIME_OUTPUT_DIR
    gold_dir.mkdir(parents=True)
    (gold_dir / "latest_gold_regime_engine.json").write_text(json.dumps(_gre_report()), encoding="utf-8")
    (gold_dir / "latest_gre_scorecard.json").write_text(json.dumps(_scorecard(total=2, validated=2)), encoding="utf-8")
    (gold_dir / "latest_gre_validation_summary.json").write_text(json.dumps({"directional_correctness_rate": None}), encoding="utf-8")
    (gold_dir / "gre_validation_rows.jsonl").write_text("", encoding="utf-8")

    result = run_gre_research_analyzer(output_root=output_root, now=NOW)

    assert result.json_path == gold_dir / LATEST_GRE_RESEARCH_ANALYZER_JSON
    assert result.markdown_path == gold_dir / LATEST_GRE_RESEARCH_ANALYZER_MD
    assert result.json_path.exists()
    assert result.markdown_path.exists()
    assert result.interface_json_path.exists()
    assert result.interface_markdown_path.exists()
    assert result.analyzer["diagnostic_only"] is True
    assert result.analyzer["broker_authority"] is False


def test_gre_research_analyzer_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_gre_research_analyzer.py"),
        Path("src/mgc_v05l/app/track_b_gre_research_analyzer.py"),
        Path("src/mgc_v05l/execution_core/track_b_regime_engine_interface.py"),
    ]
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
    forbidden_call_names = {"submit", "cancel", "placeOrder", "create_order_intent", "mutate_lifecycle", "flatten"}
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
                call_name: str | None = None
                if isinstance(node.func, ast.Name):
                    call_name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    call_name = node.func.attr
                if call_name in forbidden_call_names:
                    violations.append(f"{path}:{call_name}")

    assert violations == []


def _gre_report() -> dict:
    return {
        "schema_version": "track_b_gold_regime_engine_v1",
        "generated_at": NOW.isoformat(),
        "plugin_id": "GRE",
        "instrument": "GOLD",
        "symbols": ["GC", "MGC"],
        "session_label": "LONDON_LATE",
        "regime_label": "TRANSITION",
        "confidence": 45,
        "directional_bias": "MIXED",
        "diagnostic_only": True,
        "positive_evidence": [{"feature": "session_label", "points": 8, "explanation": "London Late"}],
        "negative_evidence": [],
        "conflicting_evidence": [],
        "missing_evidence": ["VWAP unavailable in GRE MVP"],
        "source_refs": {"fixture": "test"},
    }


def _scorecard(*, total: int, validated: int) -> dict:
    return {
        "schema_version": "track_b_gre_validation_scorecard_v1",
        "overall_metrics": {
            "total_observations": total,
            "validated_observations": validated,
            "partial_observations": 0,
            "pending_observations": 0,
            "insufficient_observations": 0,
        },
        "readiness_assessment": {
            "classification": "RESEARCH_TOO_EARLY" if total < 30 else "SUFFICIENT_FOR_SHADOW",
            "reason": "fixture",
        },
        "confidence_bands": {
            "40-60": {"observations": total, "average_outcome": 1.2},
            "60-80": {"observations": total if total >= 30 else 0, "average_outcome": 1.5},
            "80-100": {"observations": total if total >= 100 else 0, "average_outcome": 2.0},
        },
        "regime_breakdown": {
            "LONG": {"observation_count": total // 3},
            "SHORT": {"observation_count": total // 3},
            "TRANSITION": {"observation_count": total - 2 * (total // 3)},
            "CHOP": {"observation_count": 0},
            "INSUFFICIENT_EVIDENCE": {"observation_count": 0},
        },
        "feature_effectiveness": [
            {"feature": "session_label", "observation_count": total, "average_outcome": 1.2},
        ],
        "missing_provider_analysis": [
            {"provider": "VWAP unavailable in GRE MVP", "count": total},
            {"provider": "anchored VWAP unavailable in GRE MVP", "count": total},
        ],
    }
