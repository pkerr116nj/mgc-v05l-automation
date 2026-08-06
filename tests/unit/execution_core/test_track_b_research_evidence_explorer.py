from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_research_evidence_explorer import (
    build_cohorts,
    build_extreme_trade_forensic_audit,
    build_investigation_index,
    build_investigation_records,
    build_loss_attribution,
    build_inv_004,
    build_inv_005,
    build_research_evidence_explorer,
    build_within_instrument_comparison,
    classify_loss_trade,
    filter_population,
    investigation_metrics,
    nq_breadth_classification,
    nq_concentration_by_dimensions,
    nq_controlled_comparisons,
    nq_contradictory_evidence,
    nq_tail_sensitivity,
    nq_rolling_windows,
    inv_005_breadth_fragility_classification,
    inv_005_focal_trade_peer_comparisons,
    inv_005_peer_assignments,
    inv_005_peer_metrics,
    inv_005_top_winner_cohorts,
    reconcile_extreme_trade_pnl,
    render_presentation_html,
    render_investigation_index_html,
    render_inv_004_html,
    render_inv_005_html,
    run_research_evidence_explorer,
    run_research_investigations,
    trimmed_mean,
)


NOW = datetime(2026, 8, 6, 12, 0, tzinfo=UTC)


def test_top_and_bottom_decile_include_ties() -> None:
    rows = [_normalized_row(index, pnl) for index, pnl in enumerate([-5, -4, -4, -1, 0, 1, 2, 3, 4, 9, 9])]

    cohorts = build_cohorts(rows)

    assert sorted(row["realized_pnl_proxy"] for row in cohorts["bottom_decile"]) == [-5.0, -4.0, -4.0]
    assert sorted(row["realized_pnl_proxy"] for row in cohorts["top_decile"]) == [9.0, 9.0]


def test_missing_optional_ra8_does_not_exclude_population(tmp_path: Path) -> None:
    rows = [_crr_row("trade_1", pnl=10.0, layers=("ctol", "ctoe", "ra7", "ra3"))]

    analysis, population, validation = build_research_evidence_explorer(
        rows,
        crr_validation=_crr_validation(status="VALID_WITH_WARNINGS", ra8_ready="SOURCE_COVERAGE_LIMIT"),
        crr_path=tmp_path / "crr.jsonl",
        crr_validation_path=tmp_path / "validation.json",
        generated_at=NOW,
    )

    assert population["included_count"] == 1
    assert population["coverage"]["ra8_missing_count"] == 1
    assert analysis["cohorts"]["top_decile"]["trade_count"] == 1
    assert validation["status"] == "VALID_WITH_WARNINGS"


def test_missing_required_exact_layer_is_excluded(tmp_path: Path) -> None:
    rows = [_crr_row("trade_1", pnl=10.0, layers=("ctol", "ctoe", "ra7"))]

    _, population, validation = build_research_evidence_explorer(
        rows,
        crr_validation=_crr_validation(),
        crr_path=tmp_path / "crr.jsonl",
        crr_validation_path=tmp_path / "validation.json",
        generated_at=NOW,
    )

    assert population["included_count"] == 0
    assert population["excluded_count"] == 1
    assert population["exclusions"][0]["reason"] == "missing_required_exact_layers:ra3"
    assert validation["status"] == "INVALID"
    assert "top_decile_empty" in validation["blockers"]


def test_metrics_sample_sizes_and_missing_fields_are_deterministic(tmp_path: Path) -> None:
    rows = [
        _crr_row("trade_1", pnl=-10.0, side="LONG", instrument="ES", mfe=None, mae=None),
        _crr_row("trade_2", pnl=5.0, side="SHORT", instrument="NQ", mfe=2.0, mae=-1.0),
        _crr_row("trade_3", pnl=15.0, side="SHORT", instrument="NQ", mfe=3.0, mae=-0.5),
    ]

    first, _, _ = build_research_evidence_explorer(
        rows,
        crr_validation=_crr_validation(),
        crr_path=tmp_path / "crr.jsonl",
        crr_validation_path=tmp_path / "validation.json",
        generated_at=NOW,
    )
    second, _, _ = build_research_evidence_explorer(
        rows,
        crr_validation=_crr_validation(),
        crr_path=tmp_path / "crr.jsonl",
        crr_validation_path=tmp_path / "validation.json",
        generated_at=NOW,
    )

    assert first["cohorts"]["winners"]["trade_count"] == 2
    assert first["cohorts"]["losers"]["trade_count"] == 1
    assert first["cohorts"]["short"]["instrument_distribution"] == {"NQ": 2}
    assert first["population"]["date_coverage"]["first_exit_time"] == "2026-08-06T12:30:00Z"
    assert first["population"]["instrument_coverage"] == {"ES": 1, "NQ": 2}
    assert first["cohorts"]["bottom_decile"]["missing_field_counts"]["mfe_points"] == 1
    assert first["comparability_disclosure"]["global_comparison_interpretation"] == "Portfolio-outcome analysis."
    assert first["deterministic_fingerprint"] == second["deterministic_fingerprint"]


def test_within_instrument_comparison_reports_samples_and_exclusions() -> None:
    rows = [
        *[_normalized_row(index, pnl, instrument="ES") for index, pnl in enumerate(range(30))],
        *[_normalized_row(100 + index, pnl, instrument="MGC") for index, pnl in enumerate([1, 2, 3])],
    ]

    controlled = build_within_instrument_comparison(rows, min_sample_size=10)

    assert controlled["method"] == "standardized_within_instrument_realized_pnl_percentile"
    assert controlled["included_instruments"]["ES"]["sample_size"] == 30
    assert controlled["included_instruments"]["ES"]["top_within_instrument"]["trade_count"] == 3
    assert controlled["included_instruments"]["ES"]["bottom_within_instrument"]["trade_count"] == 3
    assert controlled["excluded_instruments"]["MGC"]["sample_size"] == 3
    assert controlled["excluded_instruments"]["MGC"]["reason"] == "below_minimum_sample_size"


def test_run_writes_parseable_artifacts_and_static_presentation(tmp_path: Path) -> None:
    crr_path = tmp_path / "crr.jsonl"
    validation_path = tmp_path / "validation.json"
    output_dir = tmp_path / "explorer"
    _write_jsonl(crr_path, [_crr_row("trade_1", pnl=-10.0), _crr_row("trade_2", pnl=25.0, side="SHORT")])
    validation_path.write_text(json.dumps(_crr_validation()), encoding="utf-8")

    result = run_research_evidence_explorer(
        crr_path=crr_path,
        crr_validation_path=validation_path,
        output_dir=output_dir,
        now=NOW,
    )

    assert json.loads(result.analysis_path.read_text(encoding="utf-8"))["schema_version"] == "research_evidence_explorer_v1"
    assert "comparability_controlled" in json.loads(result.analysis_path.read_text(encoding="utf-8"))
    assert json.loads(result.population_path.read_text(encoding="utf-8"))["included_count"] == 2
    assert json.loads(result.validation_path.read_text(encoding="utf-8"))["status"] == "VALID_WITH_WARNINGS"
    html = result.presentation_path.read_text(encoding="utf-8")
    assert "prepared-artifact" in html
    assert "fetch(" not in html
    assert "XMLHttpRequest" not in html


def test_presentation_renders_controlled_table_without_visible_raw_json() -> None:
    rows = [
        *[_crr_row(f"es_{index}", pnl=float(index), instrument="ES") for index in range(35)],
        *[_crr_row(f"nq_{index}", pnl=float(index * 10), instrument="NQ", side="SHORT") for index in range(35)],
    ]
    analysis, _, _ = build_research_evidence_explorer(
        rows,
        crr_validation=_crr_validation(),
        crr_path=Path("crr.jsonl"),
        crr_validation_path=Path("validation.json"),
        generated_at=NOW,
    )

    rendered = render_presentation_html(analysis)
    controlled_section = rendered.split('<section id="controlled">', 1)[1].split('<section id="distributions">', 1)[0]

    assert "Within-Instrument Controlled View" in controlled_section
    assert ">ES<" in controlled_section
    assert ">NQ<" in controlled_section
    assert "standardized_within_instrument_realized_pnl_percentile" not in controlled_section
    assert "{" not in controlled_section
    assert "$" in controlled_section
    assert "%" in rendered


def test_presentation_renders_distribution_empty_states_and_drilldown_fields() -> None:
    rows = [
        _crr_row("trade_1", pnl=-10.0, mfe=None, mae=None, layers=("ctol", "ctoe", "ra7", "ra3")),
        _crr_row("trade_2", pnl=25.0, side="SHORT", mfe=None, mae=None),
    ]
    analysis, _, _ = build_research_evidence_explorer(
        rows,
        crr_validation=_crr_validation(status="VALID_WITH_WARNINGS", ra8_ready="SOURCE_COVERAGE_LIMIT"),
        crr_path=Path("crr.jsonl"),
        crr_validation_path=Path("validation.json"),
        generated_at=NOW,
    )

    rendered = render_presentation_html(analysis)

    assert "Sparse data" in rendered
    assert "No chartable values are available for this metric." in rendered
    assert "Research Record ID" in rendered
    assert "Strategy" in rendered
    assert "Lane" in rendered
    assert "Exit Policy / Reason" in rendered
    assert "RA8 path available" in rendered
    assert "Detailed RA8 path unavailable" in rendered
    assert "Missing Fields" in rendered


def test_eligibility_source_integrity_exclusions_drive_population_views(tmp_path: Path) -> None:
    rows = [_crr_row("ordinary", pnl=25.0), _crr_row("anomaly", pnl=-1000.0, instrument="GC")]
    anomaly_id = "crr_anomaly"
    rows[1]["research_record_id"] = anomaly_id
    eligibility = [
        {
            "research_record_id": "crr_ordinary",
            "classification": "ELIGIBLE_WITH_LIMITATIONS",
            "review_required": False,
            "limitations": ["ra8_source_coverage_limit"],
        },
        {
            "research_record_id": anomaly_id,
            "source_trade_id": "anomaly",
            "classification": "EXCLUDED_CONFIRMED_SOURCE_INTEGRITY_ANOMALY",
            "review_required": False,
            "confidence": "HIGH",
            "instrument": "GC",
            "side": "LONG",
            "realized_pnl_proxy": -1000.0,
            "evidence_basis": [
                "INV-001 classified this record as CONTRACT_MULTIPLIER_OR_SCALE_MISMATCH.",
                "Entry-fill persistence accepted foreign-domain price evidence.",
            ],
            "supporting_ids": {"source_trade_id": "anomaly", "entry_exec_id": "exec_1"},
            "supporting_artifact_paths": ["outputs/track_b_execution_core/strategy_performance/canonical_trade_records.jsonl"],
        },
    ]

    analysis, population, validation = build_research_evidence_explorer(
        rows,
        crr_validation=_crr_validation(),
        crr_path=tmp_path / "crr.jsonl",
        crr_validation_path=tmp_path / "validation.json",
        eligibility_records=eligibility,
        eligibility_summary={"schema_version": "research_eligibility_summary_v1"},
        eligibility_records_path=tmp_path / "eligibility.jsonl",
        eligibility_summary_path=tmp_path / "eligibility_summary.json",
        generated_at=NOW,
    )

    assert population["active_population_view"] == "SOURCE_INTEGRITY_QUALIFIED"
    assert population["included_count"] == 1
    assert population["excluded_count"] == 1
    assert population["exclusions"][0]["reason"] == "excluded_confirmed_source_integrity_anomaly"
    assert analysis["population_views"]["FULL_HISTORICAL"]["included_count"] == 2
    assert analysis["population_views"]["SOURCE_INTEGRITY_QUALIFIED"]["included_count"] == 1
    assert analysis["source_confirmed_anomalies"][0]["research_record_id"] == anomaly_id
    assert validation["status"] == "VALID_WITH_WARNINGS"


def test_inv_001_preserves_eligibility_excluded_anomaly_evidence(tmp_path: Path) -> None:
    rows = [_crr_row(f"trade_{index}", pnl=float(index - 30)) for index in range(40)]
    anomaly = {
        "research_record_id": "excluded_anomaly",
        "source_trade_id": "trade_anomaly",
        "classification": "EXCLUDED_CONFIRMED_SOURCE_INTEGRITY_ANOMALY",
        "confidence": "HIGH",
        "instrument": "NQ",
        "side": "LONG",
        "realized_pnl_proxy": -500000.0,
        "evidence_basis": [
            "INV-001 classified this record as DUPLICATE_OR_REUSED_EXECUTION_EVIDENCE.",
            "Entry-fill persistence allowed the same broker exec ID to attach to multiple lifecycle/source trade IDs.",
        ],
        "supporting_ids": {"source_trade_id": "trade_anomaly", "entry_exec_id": "exec_dup"},
        "supporting_artifact_paths": ["outputs/track_b_execution_core/research_analytics/investigations/INV-001/anomaly_root_cause.json"],
    }
    analysis, _, _ = build_research_evidence_explorer(
        rows,
        crr_validation=_crr_validation(),
        crr_path=tmp_path / "crr.jsonl",
        crr_validation_path=tmp_path / "validation.json",
        eligibility_records=[anomaly],
        eligibility_summary={"schema_version": "research_eligibility_summary_v1"},
        generated_at=NOW,
    )

    records = build_investigation_records(
        analysis,
        generated_at=NOW,
        crr_path=tmp_path / "crr.jsonl",
        crr_validation_path=tmp_path / "validation.json",
        explorer_path=tmp_path / "explorer.json",
    )

    audit = records["INV-001"]["extreme_trade_forensic_audit"]
    trace_records = audit["anomaly_source_trace"]["records"]
    assert audit["eligibility_excluded_source_confirmed_anomaly_count"] == 1
    assert trace_records[-1]["research_record_id"] == "excluded_anomaly"
    assert trace_records[-1]["classification"] == "DUPLICATE_OR_REUSED_EXECUTION_EVIDENCE"
    assert trace_records[-1]["selection_status"] == "EXCLUDED_FROM_SOURCE_INTEGRITY_QUALIFIED_POPULATION"


def test_investigation_index_exposes_population_and_review_counts() -> None:
    index = build_investigation_index(
        {
            "INV-001": {
                "investigation_id": "INV-001",
                "title": "Extreme Loss Concentration",
                "question": "Question?",
                "conclusion_status": "PARTIALLY_SUPPORTED",
                "confidence": "PARTIAL",
                "active_population_view": "SOURCE_INTEGRITY_QUALIFIED",
                "source_confirmed_anomalies": [{"research_record_id": "a"}],
                "review_required_count": 2,
                "deterministic_fingerprint": "fp",
            }
        },
        generated_at=NOW,
        output_dir=Path("outputs"),
    )
    html = render_investigation_index_html(index)

    assert index["investigations"][0]["active_population_view"] == "SOURCE_INTEGRITY_QUALIFIED"
    assert index["investigations"][0]["source_confirmed_anomaly_count"] == 1
    assert "Source-Confirmed Anomalies" in html
    assert "SOURCE_INTEGRITY_QUALIFIED" in html


def test_guardrails_and_no_prohibited_imports_or_actions() -> None:
    source_path = Path("src/mgc_v05l/execution_core/track_b_research_evidence_explorer.py")
    app_path = Path("src/mgc_v05l/app/track_b_research_evidence_explorer.py")
    prohibited_import_roots = {
        "ibapi",
        "mgc_v05l.execution",
        "mgc_v05l.runtime",
        "mgc_v05l.strategy",
        "mgc_v05l.guardian",
        "mgc_v05l.safe_state",
        "mgc_v05l.reconciliation",
    }
    prohibited_actions = ("placeOrder", "cancelOrder", "globalCancel", "transmit")

    for path in (source_path, app_path):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }
        imports.update(alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names)
        assert not any(name == root or name.startswith(f"{root}.") for name in imports for root in prohibited_import_roots)
        text = path.read_text(encoding="utf-8")
        assert not any(action in text for action in prohibited_actions)

    analysis, _, _ = build_research_evidence_explorer(
        [_crr_row("trade_1", pnl=1.0)],
        crr_validation=_crr_validation(),
        crr_path=Path("crr.jsonl"),
        crr_validation_path=Path("validation.json"),
        generated_at=NOW,
    )
    assert analysis["guardrails"] == {
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }
    assert analysis["presentation_contract"]["hidden_recomputation"] is False
    assert analysis["presentation_contract"]["advisory_outputs"] is False


def test_query_filters_are_deterministic_and_account_for_exclusions() -> None:
    rows = [
        _drill_row("a", pnl=1.0, instrument="ES", side="LONG", session="US", ra8=True),
        _drill_row("b", pnl=-2.0, instrument="NQ", side="SHORT", session="US", ra8=False),
        _drill_row("c", pnl=3.0, instrument="ES", side="SHORT", session="GLOBEX", ra8=True),
    ]

    included, excluded = filter_population(rows, {"instrument": "ES", "side": "SHORT", "ra8": "available"})

    assert [row["research_record_id"] for row in included] == ["c"]
    assert len(excluded) == 2
    assert {row["reason"] for row in excluded} == {"side_not_selected", "instrument_not_selected"}


def test_investigation_metrics_trimmed_mean_payoff_and_expectancy() -> None:
    rows = [_drill_row(str(index), pnl=pnl) for index, pnl in enumerate([-100, -10, -5, 5, 20, 90])]

    metrics = investigation_metrics(rows)

    assert trimmed_mean([-100, -10, -5, 5, 20, 90], trim_fraction=0.20) == 2.5
    assert metrics["average_winner"] == 38.333333
    assert metrics["average_loser"] == -38.333333
    assert metrics["payoff_ratio"] == 1.0
    assert metrics["expectancy"] == 0.0


def test_investigation_records_have_fingerprints_guardrails_and_ra8_optional(tmp_path: Path) -> None:
    rows = []
    for index in range(40):
        rows.append(_crr_row(f"long_{index}", pnl=float(index - 35), side="LONG", instrument="ES", layers=("ctol", "ctoe", "ra7", "ra3")))
        rows.append(_crr_row(f"short_{index}", pnl=float(index - 20), side="SHORT", instrument="ES"))
    analysis, _, _ = build_research_evidence_explorer(
        rows,
        crr_validation=_crr_validation(status="VALID_WITH_WARNINGS", ra8_ready="SOURCE_COVERAGE_LIMIT"),
        crr_path=tmp_path / "crr.jsonl",
        crr_validation_path=tmp_path / "validation.json",
        generated_at=NOW,
    )

    records = build_investigation_records(
        analysis,
        generated_at=NOW,
        crr_path=tmp_path / "crr.jsonl",
        crr_validation_path=tmp_path / "validation.json",
        explorer_path=tmp_path / "explorer.json",
    )

    assert sorted(records) == ["INV-001", "INV-002", "INV-003", "INV-004", "INV-005"]
    assert records["INV-001"]["deterministic_fingerprint"]
    assert records["INV-002"]["conclusion_status"] in {"PARTIALLY_SUPPORTED", "INCONCLUSIVE"}
    assert records["INV-003"]["evidence"]["milestone_periods"]["boundaries"]
    assert all(record["guardrails"]["diagnostic_only"] is True for record in records.values())
    assert all(record["production_recommendation"] is False for record in records.values())


def test_run_investigations_writes_artifacts_and_durable_summaries(tmp_path: Path) -> None:
    crr_path = tmp_path / "crr.jsonl"
    validation_path = tmp_path / "validation.json"
    output_dir = tmp_path / "investigations"
    docs_dir = tmp_path / "docs"
    rows = []
    for index in range(30):
        rows.append(_crr_row(f"long_{index}", pnl=float(index - 20), side="LONG", instrument="ES"))
        rows.append(_crr_row(f"short_{index}", pnl=float(index - 15), side="SHORT", instrument="ES"))
    _write_jsonl(crr_path, rows)
    validation_path.write_text(json.dumps(_crr_validation()), encoding="utf-8")

    result = run_research_investigations(
        crr_path=crr_path,
        crr_validation_path=validation_path,
        explorer_output_dir=tmp_path,
        output_dir=output_dir,
        docs_dir=docs_dir,
        now=NOW,
    )

    assert result.index_json_path.exists()
    assert result.index_html_path.exists()
    assert (output_dir / "INV-001" / "investigation.json").exists()
    assert (docs_dir / "INV-001-extreme-loss-concentration.md").exists()
    assert (output_dir / "INV-001" / "loss_attribution.html").exists()
    assert (output_dir / "INV-001" / "loss_classification.json").exists()
    assert (output_dir / "INV-001" / "anomaly_source_trace.json").exists()
    assert (output_dir / "INV-001" / "anomaly_root_cause_review.html").exists()
    assert json.loads((output_dir / "INV-002" / "validation_report.json").read_text())["status"] in {"VALID", "VALID_WITH_WARNINGS"}
    assert (output_dir / "INV-004" / "nq_performance_attribution.html").exists()
    assert (docs_dir / "INV-004-nq-qualified-performance-attribution.md").exists()


def test_inv_004_qualified_nq_population_accounting_and_tail_sensitivity() -> None:
    rows = _nq_rows()

    inv = build_inv_004(rows, _common())

    assert inv["population"]["count"] == 60
    assert inv["performance_summary"]["metrics"]["total_realized_pnl_proxy"] == 10150.0
    assert inv["tail_sensitivity"]["excluding_single_largest_winner"]["excluded_count"] == 1
    assert inv["tail_sensitivity"]["excluding_top_10_percent"]["metrics"]["total_realized_pnl_proxy"] > 0
    assert inv["top_bottom_trades"]["top_20_winners"][0]["eligibility_classification"] == "ELIGIBLE_WITH_LIMITATIONS"
    assert inv["conclusion_status"] == "PARTIALLY_SUPPORTED"


def test_inv_004_concentration_percentages_and_breadth_are_deterministic() -> None:
    rows = _nq_rows()

    concentration = nq_concentration_by_dimensions(rows)
    tail = nq_tail_sensitivity(rows)
    breadth = nq_breadth_classification(tail, concentration)

    strategy_total_pct = sum(item["percentage_of_nq_total_contribution"] for item in concentration["dimensions"]["strategy"])
    assert abs(strategy_total_pct - 1.0) < 0.00001
    assert breadth["classification"] in {"BROADLY_DISTRIBUTED", "MODERATELY_CONCENTRATED", "HIGHLY_CONCENTRATED"}
    assert tail["deterministic_fingerprint"] == nq_tail_sensitivity(rows)["deterministic_fingerprint"]


def test_inv_004_controlled_comparisons_apply_minimum_sample_thresholds() -> None:
    rows = _nq_rows()
    sparse_rows = rows[:15]

    controlled = nq_controlled_comparisons(rows)
    sparse = nq_controlled_comparisons(sparse_rows)

    assert controlled["global_long_short"]["long_count"] == 30
    assert controlled["global_long_short"]["short_count"] == 30
    assert controlled["summary"]["controlled_cell_count"] >= 1
    assert sparse["summary"]["controlled_cell_count"] == 0
    assert sparse["controls"]["side_within_session"]["excluded_cells"]


def test_inv_004_contradictory_evidence_and_html_are_explicit() -> None:
    rows = _nq_rows()
    concentration = nq_concentration_by_dimensions(rows)
    tail = nq_tail_sensitivity(rows)
    rolling = nq_rolling_windows(rows)

    contradictory = nq_contradictory_evidence(rows, concentration=concentration, tail=tail, rolling=rolling)
    inv = build_inv_004(rows, _common())
    html = render_inv_004_html(inv)

    assert contradictory["summary"]
    assert "Sparse path/excursion evidence limits entry-versus-exit attribution." in contradictory["summary"]
    assert "No production recommendation" in html
    assert "Top Strategy Contributors" in html


def test_inv_004_explorer_highlight_and_no_hidden_recommendations(tmp_path: Path) -> None:
    analysis, _, _ = build_research_evidence_explorer(
        [_crr_row_from_drill(row) for row in _nq_rows()],
        crr_validation=_crr_validation(),
        crr_path=tmp_path / "crr.jsonl",
        crr_validation_path=tmp_path / "validation.json",
        generated_at=NOW,
    )
    rendered = render_presentation_html(analysis)

    assert analysis["investigation_highlights"]["INV-004"]["qualified_nq_trade_count"] == 60
    assert "INV-004: NQ Qualified Performance Attribution" in rendered
    assert "Descriptive only" in rendered


def test_inv_005_top_winner_cohorts_are_deterministic_with_ties() -> None:
    rows = _nq_rows()
    rows[-1]["realized_pnl_proxy"] = rows[-2]["realized_pnl_proxy"]

    cohorts = inv_005_top_winner_cohorts(rows)

    assert cohorts["top_1_percent"]["trade_count"] == 1
    assert cohorts["top_5_percent"]["trade_count"] == 3
    assert cohorts["top_10_percent"]["trade_count"] == 6
    assert cohorts["top_20_winners"]["trade_count"] == 20
    assert cohorts["deterministic_fingerprint"] == inv_005_top_winner_cohorts(rows)["deterministic_fingerprint"]


def test_inv_005_peer_level_fallback_and_minimum_thresholds() -> None:
    rows = _nq_rows()
    top_rows = inv_005_top_winner_cohorts(rows)["top_20_winners"]["rows"]

    assignments = inv_005_peer_assignments(rows, top_rows)

    usable = [item for item in assignments["assignments"] if item["peer_count"] >= 10]
    assert usable
    assert all(item["selected_peer_level"] in {"A", "B", "C", "D"} for item in usable)
    assert all(item["sample_class"] in {"DESCRIPTIVE_COHORT", "STRONG_COHORT"} for item in usable)


def test_inv_005_peer_metrics_exclude_focal_and_top_decile() -> None:
    rows = _nq_rows()
    top_rows = inv_005_top_winner_cohorts(rows)["top_20_winners"]["rows"]
    assignments = inv_005_peer_assignments(rows, top_rows)

    metrics = inv_005_peer_metrics(assignments)
    comparisons = inv_005_focal_trade_peer_comparisons(assignments)

    first = next(item for item in metrics["records"] if item.get("peer_count", 0) >= 10)
    comparison = next(item for item in comparisons["comparisons"] if item["focal_trade_id"] == first["focal_trade_id"])
    assert first["peer_without_focal_metrics"]["trade_count"] == first["peer_count"] - 1
    assert first["peer_excluding_top_10_percent_metrics"]["trade_count"] < first["peer_count"]
    assert comparison["focal_percentile_within_peer"] is not None
    assert first["classification"] in {
        "REPRESENTATIVE_OF_POSITIVE_PEER_COHORT",
        "OUTLIER_WITHIN_POSITIVE_PEER_COHORT",
        "OUTLIER_WITHIN_WEAK_OR_NEGATIVE_PEER_COHORT",
        "INCONCLUSIVE",
    }


def test_inv_005_breadth_classification_and_html_are_explicit() -> None:
    rows = _nq_rows()
    inv = build_inv_005(rows, _common())
    html = render_inv_005_html(inv)

    assert inv["breadth_fragility_classification"]["classification"] in {
        "REPEATABLE_ACROSS_MULTIPLE_PEER_COHORTS",
        "POSITIVE_BUT_TAIL_DEPENDENT",
        "HIGHLY_FRAGILE_AND_OUTLIER_DEPENDENT",
        "MIXED_OR_INCONCLUSIVE",
        "INSUFFICIENT_EVIDENCE",
    }
    assert inv["source_contract"]["ra8_required"] is False
    assert "No production recommendation" in html
    assert "Peer Cohort Metrics" in html


def test_inv_005_explorer_highlight_and_guardrails(tmp_path: Path) -> None:
    analysis, _, _ = build_research_evidence_explorer(
        [_crr_row_from_drill(row) for row in _nq_rows()],
        crr_validation=_crr_validation(),
        crr_path=tmp_path / "crr.jsonl",
        crr_validation_path=tmp_path / "validation.json",
        generated_at=NOW,
    )
    rendered = render_presentation_html(analysis)

    assert analysis["investigation_highlights"]["INV-005"]["qualified_nq_trade_count"] == 60
    assert "INV-005: NQ Winner Peer Cohorts" in rendered
    assert analysis["guardrails"]["production_recommendation"] is False


def test_loss_classification_defaults_to_insufficient_evidence_and_requires_review() -> None:
    row = _drill_row("loss_1", pnl=-100.0)

    classification = classify_loss_trade(row)

    assert classification["classification"] == "INSUFFICIENT_EVIDENCE"
    assert classification["requires_review"] is True
    assert "loss size alone is not evidence" in classification["reasoning"]


def test_loss_classification_requires_supporting_evidence() -> None:
    quantity_row = _drill_row("loss_1", pnl=-100.0)
    quantity_row["quantity"] = "2"
    data_quality_row = _drill_row("loss_2", pnl=-50.0)
    data_quality_row["data_quality_flags"] = ["missing_realized_pnl_proxy"]

    assert classify_loss_trade(quantity_row)["classification"] == "POSITION_SIZE_OR_CONTRACT_SCALE_EFFECT"
    assert classify_loss_trade(data_quality_row)["classification"] == "PNL_PROXY_OR_DATA_QUALITY_CONCERN"


def test_loss_attribution_has_no_silent_exclusions_and_reconciles_population() -> None:
    rows = [_drill_row(str(index), pnl=float(-index - 1)) for index in range(40)]

    attribution = build_loss_attribution(rows)

    full = attribution["population_sensitivity"]["full_historical_population"]
    assert full["included_count"] == 40
    assert full["excluded_count"] == 0
    assert attribution["summary"]["unresolved_review_count"] >= 4
    assert attribution["loss_classification"]["unresolved_review_queue"]
    assert attribution["loss_concentration"]["bottom_10_percent"]["instrument"][0]["count"] == 4


def test_extreme_trade_pnl_reconciliation_uses_direction_and_quantity() -> None:
    row = _drill_row("short_loss", pnl=-40.0, side="SHORT")
    row.update({"entry_price": "100.0", "exit_price": "102.0", "realized_points": -2.0, "quantity": "2"})

    reconciliation = reconcile_extreme_trade_pnl(row)

    assert reconciliation["direction_sign"] == -1
    assert reconciliation["directed_points_from_prices"] == -2.0
    assert reconciliation["contract_multiplier_or_point_value_used"] == 10.0
    assert reconciliation["expected_arithmetic_pnl_from_available_fields"] == -40.0
    assert reconciliation["pnl_proxy_reconciled_to_available_fields"] is True


def test_extreme_trade_forensic_audit_does_not_invent_contract_economics() -> None:
    rows = [_drill_row(str(index), pnl=float(-index - 1)) for index in range(25)]

    audit = build_extreme_trade_forensic_audit(rows)

    first = audit["pnl_reconciliation"]["records"][0]
    assert first["contract_economics_source"] == "IMPLIED_FROM_EMITTED_PNL_PROXY_AND_REALIZED_POINTS"
    assert audit["classification"]["classification_counts"] == {"SOURCE_EVIDENCE_INCOMPLETE": 20}
    assert audit["summary"]["unresolved_count"] == 20


def test_extreme_trade_forensic_detects_price_scale_and_duplicate_execution() -> None:
    rows = [_drill_row(str(index), pnl=float(-index - 1)) for index in range(22)]
    rows[0].update({"instrument": "GC", "entry_price": "28757", "exit_price": "4001.7", "realized_points": -24755.3, "realized_pnl_proxy": -2475530.0})
    rows[20].update({"entry_exec_id": "dup_exec"})
    rows[21].update({"entry_exec_id": "dup_exec"})

    audit = build_extreme_trade_forensic_audit(rows)
    by_id = {item["research_record_id"]: item for item in audit["classification"]["records"]}

    assert by_id["0"]["classification"] == "CONTRACT_MULTIPLIER_OR_SCALE_MISMATCH"
    assert by_id["20"]["classification"] == "DUPLICATE_OR_REUSED_EXECUTION_EVIDENCE"
    assert audit["execution_lineage"]["duplicate_execution_summary"]["entry_exec_id"]["duplicate_key_count"] == 1
    trace_by_id = {item["research_record_id"]: item for item in audit["anomaly_source_trace"]["records"]}
    assert trace_by_id["0"]["first_defective_layer"] == "entry_fill_persistence"
    assert trace_by_id["0"]["hypothesis_tests"]["cross_instrument_entry_exit_pairing"].startswith("NOT_PROVEN_IN_CANONICAL_PAIRING")
    assert audit["anomaly_root_cause"]["summary"]["first_defective_layer"] == "entry_fill_persistence"
    assert audit["anomaly_repair_plan"]["status"] == "ROUTINE_TRACK_REPAIR_RECOMMENDED_NOT_IMPLEMENTED"


def test_extreme_trade_population_impact_has_no_silent_exclusions() -> None:
    rows = [_drill_row(str(index), pnl=float(-index - 1)) for index in range(30)]

    audit = build_extreme_trade_forensic_audit(rows)
    views = audit["population_impact"]["views"]

    assert views["full_population"]["included_count"] == 30
    assert views["excluding_only_pnl_unreconciled_records"]["excluded_count"] == 0
    assert views["excluding_only_source_confirmed_data_anomalies"]["excluded_count"] == 0
    assert views["economically_reconciled_trades_only"]["included_count"] == 0
    assert audit["deterministic_fingerprint"] == build_extreme_trade_forensic_audit(rows)["deterministic_fingerprint"]


def _normalized_row(index: int, pnl: float, *, instrument: str = "ES") -> dict[str, object]:
    return {
        "research_record_id": f"rr_{index}",
        "realized_pnl_proxy": float(pnl),
        "side": "LONG",
        "instrument": instrument,
    }


def _crr_validation(*, status: str = "VALID_WITH_WARNINGS", ra8_ready: str = "READY") -> dict[str, object]:
    readiness = [
        {"source_name": "canonical_trade_records", "readiness_classification": "READY"},
        {"source_name": "ctol", "readiness_classification": "READY"},
        {"source_name": "ctoe", "readiness_classification": "READY"},
        {"source_name": "ra7", "readiness_classification": "READY"},
        {"source_name": "ra3", "readiness_classification": "READY"},
        {"source_name": "ra8", "readiness_classification": ra8_ready},
    ]
    return {
        "schema_version": "canonical_research_record_validation_v1",
        "status": status,
        "upstream_readiness": readiness,
    }


def _crr_row(
    trade_id: str,
    *,
    pnl: float,
    side: str = "LONG",
    instrument: str = "ES",
    mfe: float | None = 2.0,
    mae: float | None = -1.0,
    layers: tuple[str, ...] = ("ctol", "ctoe", "ra7", "ra3", "ra8"),
) -> dict[str, object]:
    exact = [{"layer": layer, "method": "fixture", "quality": "EXACT"} for layer in layers]
    missing = [
        {"layer": layer, "reason": "fixture_missing", "quality": "MISSING"}
        for layer in ("ctol", "ctoe", "ra7", "ra3", "ra8")
        if layer not in layers
    ]
    return {
        "schema_version": "canonical_research_record_v1",
        "research_record_id": f"crr_{trade_id}",
        "trade_identity": {
            "trade_id": trade_id,
            "source_trade_id": trade_id,
            "instrument": instrument,
            "contract": f"{instrument}U6",
            "side": side,
            "quantity": "1",
        },
        "entry_anchor": {
            "entry_time": "2026-08-06T12:00:00Z",
            "entry_price": "100.0",
            "strategy_id": f"{instrument}_strategy",
            "lane_id": f"{instrument}_lane",
        },
        "exit_anchor": {
            "exit_time": "2026-08-06T12:30:00Z",
            "exit_price": "101.0",
            "exit_reason": "TIMEBOX",
            "exit_policy": "TIMEBOX",
        },
        "outcome_summary": {
            "realized_pnl_proxy": pnl,
            "realized_points": pnl / 10.0,
            "hold_seconds": 1800.0,
            "mfe_points": mfe,
            "mae_points": mae,
        },
        "enrichment_ref": {
            "context_validity_summary": {
                "session": "US_RTH",
                "gre_validity_classification": "VALID",
                "market_context_validity_classification": "VALID",
                "vix_percentile": 0.42,
            }
        },
        "path_ref": {
            "canonical_trade_path_id": f"path_{trade_id}" if "ra7" in layers else None,
            "capture_id": f"capture_{trade_id}" if "ra8" in layers else None,
            "path_status": "PARTIAL_UNKNOWN_SPARSE_GAP" if "ra7" in layers else None,
        },
        "attribution_ref": {
            "trade_decision_attribution_id": f"attr_{trade_id}" if "ra3" in layers else None,
            "entry_attribution_status": "KNOWN",
            "exit_attribution_status": "KNOWN",
        },
        "join_quality": {
            "overall": "EXACT" if len(layers) == 5 else "INCOMPLETE",
            "exact": exact,
            "missing": missing,
            "broken": [],
            "tolerance": [],
        },
        "source_provenance": [{"source_name": "canonical_trade_records", "record_fingerprint": f"fp_{trade_id}"}],
        "guardrails": {
            "diagnostic_only": True,
            "production_recommendation": False,
            "trading_gate": False,
        },
    }


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _drill_row(
    research_record_id: str,
    *,
    pnl: float,
    instrument: str = "ES",
    side: str = "LONG",
    session: str = "US",
    ra8: bool = True,
) -> dict[str, object]:
    entry_price = 100.0
    realized_points = pnl / 10.0
    direction_sign = 1 if side == "LONG" else -1
    exit_price = entry_price + (realized_points / direction_sign)
    return {
        "research_record_id": research_record_id,
        "source_trade_id": research_record_id,
        "trade_id": research_record_id,
        "lifecycle_id": f"lifecycle_{research_record_id}",
        "con_id": 12345,
        "instrument": instrument,
        "contract": f"{instrument}U6",
        "side": side,
        "quantity": "1",
        "strategy_id": f"{instrument}_strategy",
        "lane_id": f"{instrument}_lane",
        "session": session,
        "regime": "VALID",
        "exit_policy": "TIMEBOX",
        "exit_reason": "TIMEBOX",
        "entry_time": "2026-08-06T12:00:00Z",
        "exit_time": "2026-08-06T12:30:00Z",
        "entry_price": str(entry_price),
        "exit_price": str(exit_price),
        "entry_exec_id": f"entry_exec_{research_record_id}",
        "exit_exec_id": f"exit_exec_{research_record_id}",
        "entry_order_id": f"entry_order_{research_record_id}",
        "exit_order_id": f"exit_order_{research_record_id}",
        "entry_perm_id": f"entry_perm_{research_record_id}",
        "exit_perm_id": f"exit_perm_{research_record_id}",
        "realized_pnl_proxy": pnl,
        "realized_points": realized_points,
        "pnl_source_artifact": "outputs/fixture/ctol.jsonl",
        "pnl_source_record_id": f"outcome_{research_record_id}",
        "hold_seconds": 1800.0,
        "mfe_points": None,
        "mae_points": None,
        "path_status": {"ra7": "PARTIAL", "ra8": "EXACT" if ra8 else "MISSING"},
        "missing_fields": [] if ra8 else ["ra8_finalized_capture"],
        "source_provenance": [{"source_name": "fixture", "record_fingerprint": research_record_id}],
    }


def _nq_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    pnl_values = [2500.0, 2000.0, 1500.0, 1000.0, 850.0, 700.0, 600.0, 550.0, 500.0, 450.0]
    pnl_values.extend([400.0] * 20)
    pnl_values.extend([-300.0] * 20)
    pnl_values.extend([-250.0] * 10)
    for index, pnl in enumerate(pnl_values):
        side = "LONG" if index % 2 == 0 else "SHORT"
        session = "GLOBEX" if index < 30 else "US_RTH"
        strategy = "nq_globex_long" if session == "GLOBEX" and side == "LONG" else f"nq_{session.lower()}_{side.lower()}"
        row = _drill_row(f"nq_{index:03d}", pnl=pnl, instrument="NQ", side=side, session=session, ra8=index < 5)
        row.update(
            {
                "strategy_id": strategy,
                "lane_id": f"{strategy}_lane",
                "regime": "HIGH_VOL" if index % 3 == 0 else "NORMAL",
                "contract": "NQU6",
                "research_eligibility": {"classification": "ELIGIBLE_WITH_LIMITATIONS"},
                "calendar_month": "2026-08",
            }
        )
        rows.append(row)
    return rows


def _common() -> dict[str, object]:
    return {
        "schema_version": "research_investigation_v1",
        "generated_at": NOW.isoformat().replace("+00:00", "Z"),
        "active_population_view": "SOURCE_INTEGRITY_QUALIFIED",
        "source_artifacts": {
            "crr": "crr.jsonl",
            "explorer": "research_evidence_explorer_v1.json",
        },
        "source_fingerprints": {"explorer": "fixture"},
        "guardrails": {
            "diagnostic_only": True,
            "production_recommendation": False,
            "trading_gate": False,
        },
        "production_recommendation": False,
        "trading_gate": False,
    }


def _crr_row_from_drill(row: dict[str, object]) -> dict[str, object]:
    crr = _crr_row(
        str(row["research_record_id"]),
        pnl=float(row["realized_pnl_proxy"]),
        side=str(row["side"]),
        instrument=str(row["instrument"]),
        mfe=None,
        mae=None,
    )
    crr["research_record_id"] = row["research_record_id"]
    crr["trade_identity"].update(
        {
            "trade_id": row["trade_id"],
            "source_trade_id": row["source_trade_id"],
            "instrument": row["instrument"],
            "contract": row["contract"],
            "side": row["side"],
            "quantity": row["quantity"],
        }
    )
    crr["entry_anchor"].update(
        {
            "entry_time": row["entry_time"],
            "strategy_id": row["strategy_id"],
            "lane_id": row["lane_id"],
        }
    )
    crr["exit_anchor"].update(
        {
            "exit_time": row["exit_time"],
            "exit_policy": row["exit_policy"],
            "exit_reason": row["exit_reason"],
        }
    )
    crr["outcome_summary"]["realized_pnl_proxy"] = row["realized_pnl_proxy"]
    crr["enrichment_ref"]["context_validity_summary"].update(
        {
            "session": row["session"],
            "market_context_validity_classification": "VALID",
        }
    )
    crr["path_ref"].update(
        {
            "canonical_trade_path_id": f"path_{row['research_record_id']}",
            "capture_id": f"capture_{row['research_record_id']}" if row.get("path_status", {}).get("ra8") == "EXACT" else None,
        }
    )
    crr["source_provenance"] = row["source_provenance"]
    return crr
