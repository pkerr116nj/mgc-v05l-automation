from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_research_record import (
    build_canonical_research_records,
    run_canonical_research_record,
    validate_canonical_research_records,
)


NOW = datetime(2026, 8, 6, 12, 0, tzinfo=UTC)


def test_exact_complete_fixture_produces_stable_crr_row() -> None:
    canonical = _canonical()
    outcome = _outcome()
    enrichment = _enrichment()
    path = _path()
    attribution = _attribution()
    finalized = _finalized_capture()

    first = build_canonical_research_records(
        [canonical],
        outcomes=[outcome],
        enrichments=[enrichment],
        trade_paths=[path],
        attributions=[attribution],
        finalized_captures=[finalized],
        generated_at=NOW,
        source_paths=_source_paths(),
    )
    second = build_canonical_research_records(
        [canonical],
        outcomes=[outcome],
        enrichments=[enrichment],
        trade_paths=[path],
        attributions=[attribution],
        finalized_captures=[finalized],
        generated_at=NOW,
        source_paths=_source_paths(),
    )

    row = first[0]
    assert row["schema_version"] == "canonical_research_record_v1"
    assert row["trade_identity"]["source_trade_id"] == "trade_1"
    assert row["outcome_ref"]["trade_outcome_id"] == "outcome_1"
    assert row["enrichment_ref"]["context_validity_summary"]["gre_validity_classification"] == "VALID"
    assert row["path_ref"]["canonical_trade_path_id"] == "path_1"
    assert row["path_ref"]["capture_id"] == "capture_1"
    assert row["attribution_ref"]["trade_decision_attribution_id"] == "attr_1"
    assert row["join_quality"]["overall"] == "EXACT"
    assert row["deterministic_fingerprint"] == second[0]["deterministic_fingerprint"]
    assert row["guardrails"] == {
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }


def test_missing_path_evidence_remains_missing() -> None:
    rows = build_canonical_research_records(
        [_canonical()],
        outcomes=[_outcome()],
        enrichments=[_enrichment()],
        attributions=[_attribution()],
        generated_at=NOW,
        source_paths=_source_paths(),
    )

    row = rows[0]
    missing_layers = {item["layer"] for item in row["join_quality"]["missing"]}
    assert "ra7" in missing_layers
    assert "ra8" in missing_layers
    assert row["path_ref"]["canonical_trade_path_id"] is None
    assert row["path_ref"]["path_status"] is None


def test_inconsistent_exact_path_evidence_is_broken_and_invalid() -> None:
    bad_path = dict(_path())
    bad_path["source_trade_id"] = "other_trade"
    rows = build_canonical_research_records(
        [_canonical()],
        outcomes=[_outcome()],
        trade_paths=[bad_path],
        generated_at=NOW,
        source_paths=_source_paths(),
    )
    validation = validate_canonical_research_records(rows, canonical_records=[_canonical()], outcomes=[_outcome()], generated_at=NOW)

    assert rows[0]["join_quality"]["overall"] == "BROKEN"
    assert validation["status"] == "INVALID_BROKEN_JOIN"
    assert validation["broken_by_layer"] == {"ra7": 1}


def test_identity_and_entry_exit_caches_reconcile_to_canonical_records() -> None:
    rows = build_canonical_research_records([_canonical()], outcomes=[_outcome()], generated_at=NOW)
    validation = validate_canonical_research_records(rows, canonical_records=[_canonical()], outcomes=[_outcome()], generated_at=NOW)

    assert validation["counts"]["reconciliation_mismatch_count"] == 0
    assert rows[0]["entry_anchor"]["entry_time"] == "2026-08-06T12:00:00Z"
    assert rows[0]["exit_anchor"]["exit_time"] == "2026-08-06T12:30:00Z"


def test_ctol_cache_fields_reconcile() -> None:
    rows = build_canonical_research_records([_canonical()], outcomes=[_outcome()], generated_at=NOW)
    validation = validate_canonical_research_records(rows, canonical_records=[_canonical()], outcomes=[_outcome()], generated_at=NOW)

    assert rows[0]["outcome_summary"]["realized_pnl_proxy"] == 25.0
    assert rows[0]["outcome_summary"]["realized_points"] == 2.5
    assert validation["status"] == "VALID_WITH_WARNINGS"


def test_provenance_is_present_for_joined_sources() -> None:
    rows = build_canonical_research_records(
        [_canonical()],
        outcomes=[_outcome()],
        enrichments=[_enrichment()],
        trade_paths=[_path()],
        attributions=[_attribution()],
        finalized_captures=[_finalized_capture()],
        generated_at=NOW,
        source_paths=_source_paths(),
    )
    names = {item["source_name"] for item in rows[0]["source_provenance"]}

    assert {"canonical_trade_records", "ctol", "ctoe", "ra7_canonical_trade_paths", "ra3_trade_decision_attribution", "ra8_finalized_capture"} <= names
    assert all("record_fingerprint" in item for item in rows[0]["source_provenance"])


def test_run_writes_json_jsonl_and_reports(tmp_path: Path) -> None:
    canonical_path = tmp_path / "canonical.jsonl"
    outcomes_path = tmp_path / "outcomes.jsonl"
    enrichments_path = tmp_path / "enrichments.jsonl"
    paths_path = tmp_path / "paths.jsonl"
    attributions_path = tmp_path / "attributions.jsonl"
    finalized_path = tmp_path / "finalized.jsonl"
    _write_jsonl(canonical_path, [_canonical()])
    _write_jsonl(outcomes_path, [_outcome()])
    _write_jsonl(enrichments_path, [_enrichment()])
    _write_jsonl(paths_path, [_path()])
    _write_jsonl(attributions_path, [_attribution()])
    _write_jsonl(finalized_path, [_finalized_capture()])

    result = run_canonical_research_record(
        canonical_records_path=canonical_path,
        outcomes_path=outcomes_path,
        enrichments_path=enrichments_path,
        trade_paths_path=paths_path,
        attributions_path=attributions_path,
        finalized_captures_path=finalized_path,
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert result.records_path.exists()
    assert result.schema_path.exists()
    assert result.validation_json_path.exists()
    assert result.validation_markdown_path.exists()
    assert result.summary_markdown_path.exists()
    rows = [json.loads(line) for line in result.records_path.read_text().splitlines()]
    assert len(rows) == 1
    assert json.loads(result.validation_json_path.read_text())["status"] == "VALID"


def test_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_canonical_research_record.py"),
        Path("src/mgc_v05l/app/track_b_canonical_research_record.py"),
    ]
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "mgc_v05l.execution_core.track_b_guardian",
        "mgc_v05l.execution_core.track_b_safe_state",
        "mgc_v05l.execution_core.track_b_reconciliation",
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


def _canonical() -> dict[str, object]:
    return {
        "schema_version": "canonical_trade_record_v1",
        "trade_id": "trade_1",
        "pairing_status": "PAIRED",
        "trade_status": "CLOSED",
        "lifecycle_id": "life_1",
        "symbol": "MGC",
        "local_symbol": "MGCQ6",
        "con_id": 123,
        "side": "LONG",
        "quantity": 1,
        "entry_time": "2026-08-06T12:00:00Z",
        "entry_price": 2400.0,
        "entry_order_id": "entry_order_1",
        "entry_perm_id": "entry_perm_1",
        "entry_exec_id": "entry_exec_1",
        "exit_time": "2026-08-06T12:30:00Z",
        "exit_price": 2402.5,
        "exit_order_id": "exit_order_1",
        "exit_perm_id": "exit_perm_1",
        "exit_exec_id": "exit_exec_1",
        "exit_policy": "MANAGED_EXIT_TIMEOUT",
        "exit_reason": "TIMEOUT",
        "strategy_id": "strategy_1",
        "lane_id": "lane_1",
        "path_capture": {
            "capture_id": "capture_1",
            "trade_identity": {
                "source_trade_id": "trade_1",
                "lifecycle_id": "life_1",
                "instrument": "MGC",
                "contract": "MGCQ6",
                "side": "LONG",
                "quantity": "1",
            },
        },
    }


def _outcome() -> dict[str, object]:
    return {
        "schema_version": "track_b_canonical_trade_outcome_v1",
        "trade_outcome_id": "outcome_1",
        "entry_trade_id": "trade_1",
        "exit_trade_id": "trade_1",
        "instrument": "MGC",
        "contract": "MGCQ6",
        "side": "LONG",
        "entry_time": "2026-08-06T12:00:00Z",
        "exit_time": "2026-08-06T12:30:00Z",
        "entry_price": 2400.0,
        "exit_price": 2402.5,
        "realized_points": 2.5,
        "realized_pnl_proxy": 25.0,
        "hold_seconds": 1800.0,
        "source_refs": {"source_trade_id": "trade_1"},
        "diagnostic_only": True,
    }


def _enrichment() -> dict[str, object]:
    return {
        "schema_version": "track_b_trade_outcome_enrichment_v1",
        "trade_outcome_id": "outcome_1",
        "gre_validity_classification": "VALID",
        "crfd_validity_classification": "VALID",
        "market_context_validity_classification": "VALID",
        "session": "US",
        "diagnostic_only": True,
    }


def _path() -> dict[str, object]:
    return {
        "schema_version": "canonical_trade_path_v1",
        "canonical_trade_path_id": "path_1",
        "source_trade_id": "trade_1",
        "trade_outcome_id": "outcome_1",
        "path_coverage_status": "COMPLETE",
        "deterministic_fingerprint": "path_fp_1",
        "provenance": {"source_refs": {"capture_id": "capture_1"}},
        "diagnostic_only": True,
    }


def _attribution() -> dict[str, object]:
    return {
        "schema_version": "canonical_trade_decision_attribution_v1",
        "trade_decision_attribution_id": "attr_1",
        "trade_outcome_id": "outcome_1",
        "entry": {"entry_authority": "TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE", "lane_id": "lane_1"},
        "exit": {"exit_authority": "MANAGED_EXIT", "canonical_exit_reason": "MANAGED_EXIT_TIMEOUT"},
        "diagnostic_only": True,
    }


def _finalized_capture() -> dict[str, object]:
    return {
        "schema_version": "ra8_finalized_trade_path_capture_v1",
        "capture_id": "capture_1",
        "source_trade_id": "trade_1",
        "capture_lifecycle_state": "FINALIZED",
        "coverage_status": "COMPLETE",
        "deterministic_fingerprint": "capture_fp_1",
    }


def _source_paths() -> dict[str, Path]:
    return {
        "canonical_trade_records": Path("canonical.jsonl"),
        "ctol": Path("outcomes.jsonl"),
        "ctoe": Path("enrichments.jsonl"),
        "ra7_canonical_trade_paths": Path("paths.jsonl"),
        "ra3_trade_decision_attribution": Path("attributions.jsonl"),
        "ra8_finalized_capture": Path("finalized.jsonl"),
    }


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
