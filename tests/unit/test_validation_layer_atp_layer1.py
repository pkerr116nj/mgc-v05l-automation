from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from validation_layer.config.defaults import build_debug_config
from validation_layer.layer1.atp_adapter import (
    _validate_lane_identity,
    build_strategy_backtest_from_atp_source,
    load_atp_layer1_source,
)
from validation_layer.layer1.pilot import run_atp_layer1_pilot_rerun
from validation_layer.orchestration.pipeline import run_validation_pipeline


def _baseline_source():
    return load_atp_layer1_source(
        lane_id="atp_companion_v1_asia_us",
        subject_label="atp_baseline_mgc_asia_us",
        source_config_path="config/probationary_pattern_engine_paper_atp_companion_v1_asia_us.yaml",
        lane_dir="outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_asia_us",
        runtime_config_in_force_path="outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json",
        family_classification="baseline_lane",
        baseline_reference_path="config/atp_companion_baseline_v1_asia_us.yaml",
    )


def _candidate_source():
    return load_atp_layer1_source(
        lane_id="atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only",
        subject_label="atp_candidate_mgc_asia_promotion_1_075r_favorable_only",
        source_config_path="config/probationary_pattern_engine_paper_atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only.yaml",
        lane_dir="outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only",
        runtime_config_in_force_path="outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json",
        family_classification="candidate_lane",
        baseline_reference_path="config/atp_companion_baseline_v1_asia_us.yaml",
        candidate_registry_path="config/atp_promotion_add_candidate_registry.yaml",
        candidate_config_path="config/atp_companion_candidate_promotion_1_075r_favorable_only.yaml",
    )


def test_atp_adapter_maps_provenance_and_lineage() -> None:
    backtest = build_strategy_backtest_from_atp_source(_baseline_source(), code_version="test")
    report = run_validation_pipeline(backtest, build_debug_config())
    module_map = {result.module_name: result for result in report.module_results}

    assert backtest.provenance is not None
    assert backtest.provenance.producer_id == "probationary_runtime.atp_companion_lane_artifacts"
    assert backtest.provenance.execution_assumptions.fill_policy == "current_candle_vwap"
    assert backtest.provenance.execution_assumptions.slippage_model.model_name == "paper_runtime_recorded_slippage_cost"
    assert backtest.provenance.execution_assumptions.fee_model.model_name == "paper_runtime_recorded_fee_cost"
    assert backtest.provenance.artifact_references["runtime_state"].endswith("runtime_state.json")
    assert backtest.trades[0].validation_metadata is not None
    assert backtest.trades[0].validation_metadata.signal_id is not None
    assert backtest.trades[0].validation_metadata.tags["lane_id"] == "atp_companion_v1_asia_us"
    assert module_map["layer1_prerequisites"].status == "pass"


def test_atp_adapter_distinguishes_baseline_and_candidate_identities() -> None:
    baseline = build_strategy_backtest_from_atp_source(_baseline_source(), code_version="test")
    candidate = build_strategy_backtest_from_atp_source(_candidate_source(), code_version="test")

    assert baseline.metadata["family_classification"] == "baseline_lane"
    assert baseline.parameters["candidate_id"] is None
    assert baseline.metadata["candidate_config"] is None

    assert candidate.metadata["family_classification"] == "candidate_lane"
    assert candidate.parameters["candidate_id"] == "promotion_1_075r_favorable_only"
    assert candidate.metadata["candidate_config"].endswith("config/atp_companion_candidate_promotion_1_075r_favorable_only.yaml")


def test_atp_pipeline_reports_insufficient_evidence_when_provenance_is_removed() -> None:
    adapted = build_strategy_backtest_from_atp_source(_baseline_source(), code_version="test")
    report = run_validation_pipeline(replace(adapted, provenance=None), build_debug_config())
    module = next(result for result in report.module_results if result.module_name == "layer1_prerequisites")

    assert module.status == "warn"
    assert module.metrics["insufficient_evidence"] is True
    assert report.overall_status == "insufficient_evidence"


def test_atp_pilot_rerun_writes_strategy_backtests_and_validation_reports(tmp_path: Path) -> None:
    artifacts = run_atp_layer1_pilot_rerun(output_dir=tmp_path / "atp_layer1")
    manifest = json.loads(artifacts.manifest_path.read_text(encoding="utf-8"))

    assert manifest["pilot_count"] == 2
    for report_paths in artifacts.reports.values():
        assert json.loads(Path(report_paths["strategy_backtest_json"]).read_text(encoding="utf-8"))
        payload = json.loads(Path(report_paths["validation_report_json"]).read_text(encoding="utf-8"))
        module_names = {row["module_name"]: row for row in payload["module_results"]}
        assert module_names["layer1_prerequisites"]["status"] == "pass"


def test_atp_lane_identity_tolerates_missing_optional_runtime_fields_and_normalized_paths() -> None:
    source = _baseline_source()
    configured_lane = {
        "lane_id": "atp_companion_v1_asia_us",
        "symbol": "MGC",
        "standalone_strategy_id": "atp_companion_v1__paper_mgc_asia_us",
        "runtime_kind": "atp_companion_benchmark_paper",
        "lane_mode": "ATP_COMPANION_BENCHMARK",
        "strategy_family": "active_trend_participation_engine",
        "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
        "artifacts_dir": "./outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_asia_us",
    }
    runtime_lane = {
        "lane_id": "atp_companion_v1_asia_us",
        "symbol": "MGC",
        "standalone_strategy_id": None,
        "runtime_kind": "atp_companion_benchmark_paper",
        "lane_mode": "ATP_COMPANION_BENCHMARK",
        "strategy_family": "active_trend_participation_engine",
        "quality_bucket_policy": "MEDIUM_HIGH_ONLY",
        "artifacts_dir": "outputs/probationary_pattern_engine/paper_session/lanes/atp_companion_v1_asia_us",
    }
    operator_status = {
        "lane_id": "atp_companion_v1_asia_us",
        "symbol": "MGC",
        "display_name": "ATP Companion Baseline",
    }

    _validate_lane_identity(source, configured_lane, runtime_lane, operator_status)
