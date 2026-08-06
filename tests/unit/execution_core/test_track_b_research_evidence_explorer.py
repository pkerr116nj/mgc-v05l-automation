from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_research_evidence_explorer import (
    build_cohorts,
    build_investigation_records,
    build_research_evidence_explorer,
    build_within_instrument_comparison,
    filter_population,
    investigation_metrics,
    render_presentation_html,
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

    assert sorted(records) == ["INV-001", "INV-002", "INV-003"]
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
    assert json.loads((output_dir / "INV-002" / "validation_report.json").read_text())["status"] in {"VALID", "VALID_WITH_WARNINGS"}


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
    return {
        "research_record_id": research_record_id,
        "source_trade_id": research_record_id,
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
        "realized_pnl_proxy": pnl,
        "hold_seconds": 1800.0,
        "mfe_points": None,
        "mae_points": None,
        "path_status": {"ra7": "PARTIAL", "ra8": "EXACT" if ra8 else "MISSING"},
        "missing_fields": [] if ra8 else ["ra8_finalized_capture"],
        "source_provenance": [{"source_name": "fixture", "record_fingerprint": research_record_id}],
    }
