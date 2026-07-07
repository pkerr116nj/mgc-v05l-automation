from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_trade_path_layer import (
    build_canonical_trade_path_summary,
    build_canonical_trade_paths,
    run_canonical_trade_path_layer,
)


NOW = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)


def test_creates_canonical_trade_path_from_retained_path() -> None:
    rows = build_canonical_trade_paths(
        [_outcome("t1")],
        enrichments=[_enrichment("outcome_t1")],
        attributions=[_attribution("outcome_t1")],
        retained_paths=[_retained("t1", "outcome_t1")],
        generated_at=NOW,
        source_paths={"retained_trade_path_capture": Path("retained.jsonl")},
    )

    row = rows[0]
    assert row["source_trade_id"] == "t1"
    assert row["path_source"] == "RETAINED_TRADE_PATH_CAPTURE"
    assert row["path_coverage_status"] == "COMPLETE"
    assert row["path_complete_entry_to_exit"] is True
    assert row["path_sample_count"] == 2
    assert row["mfe"] == 5.0
    assert row["mae"] == -2.0
    assert row["trade_enrichment_id"] == "enrichment_outcome_t1"
    assert row["trade_decision_attribution_id"] == "decision_outcome_t1"
    assert row["provenance"]["source_refs"]["retained_path_capture_id"] == "retained_t1"
    assert row["diagnostic_only"] is True
    assert row["production_recommendation"] is False
    assert row["trading_gate"] is False


def test_missing_path_is_classified_without_fabricating_samples() -> None:
    rows = build_canonical_trade_paths([_outcome("t1")], generated_at=NOW)
    row = rows[0]

    assert row["path_source"] == "NONE"
    assert row["path_coverage_status"] == "MISSING_SOURCE"
    assert row["path_sample_count"] == 0
    assert row["mfe"] is None
    assert row["mae"] is None
    assert row["counterfactual_ready"] == {
        "timebox": False,
        "trailing": False,
        "vwap_avwap": False,
        "atr": False,
    }


def test_fingerprint_is_stable_for_same_trade_path_payload() -> None:
    kwargs = {
        "outcomes": [_outcome("t1")],
        "retained_paths": [_retained("t1", "outcome_t1")],
        "generated_at": NOW,
    }
    first = build_canonical_trade_paths(**kwargs)[0]
    second = build_canonical_trade_paths(**kwargs)[0]

    assert first["deterministic_fingerprint"] == second["deterministic_fingerprint"]


def test_counterfactual_readiness_reflects_path_completeness_and_available_inputs() -> None:
    rows = build_canonical_trade_paths(
        [_outcome("t1")],
        retained_paths=[_retained("t1", "outcome_t1")],
        generated_at=NOW,
    )
    readiness = rows[0]["counterfactual_ready"]

    assert readiness["timebox"] is True
    assert readiness["trailing"] is True
    assert readiness["vwap_avwap"] is False
    assert readiness["atr"] is False


def test_summary_reports_coverage_and_provenance_counts() -> None:
    rows = build_canonical_trade_paths(
        [_outcome("t1"), _outcome("t2")],
        enrichments=[_enrichment("outcome_t1")],
        attributions=[_attribution("outcome_t1")],
        retained_paths=[_retained("t1", "outcome_t1")],
        generated_at=NOW,
    )
    summary = build_canonical_trade_path_summary(rows, generated_at=NOW)

    assert summary["overall"]["canonical_trade_path_count"] == 2
    assert summary["overall"]["path_available_count"] == 1
    assert summary["overall"]["missing_source_count"] == 1
    assert summary["provenance"]["ctol_link_count"] == 2
    assert summary["provenance"]["ctoe_link_count"] == 1
    assert summary["provenance"]["ra3_link_count"] == 1
    assert summary["provenance"]["retained_capture_link_count"] == 1


def test_run_writes_canonical_json_jsonl_and_reports(tmp_path: Path) -> None:
    outcomes = tmp_path / "outcomes.jsonl"
    enrichments = tmp_path / "enrichments.jsonl"
    attributions = tmp_path / "attributions.jsonl"
    retained = tmp_path / "retained.jsonl"
    outcomes.write_text(json.dumps(_outcome("t1")) + "\n", encoding="utf-8")
    enrichments.write_text(json.dumps(_enrichment("outcome_t1")) + "\n", encoding="utf-8")
    attributions.write_text(json.dumps(_attribution("outcome_t1")) + "\n", encoding="utf-8")
    retained.write_text(json.dumps(_retained("t1", "outcome_t1")) + "\n", encoding="utf-8")

    result = run_canonical_trade_path_layer(
        outcomes_path=outcomes,
        enrichments_path=enrichments,
        attributions_path=attributions,
        retained_paths_path=retained,
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert json.loads(result.summary_path.read_text())["schema_version"] == "ra7_canonical_trade_path_summary_v1"
    assert len([json.loads(line) for line in result.canonical_paths_path.read_text().splitlines() if line.strip()]) == 1
    assert result.contract_path.exists()
    assert result.schema_path.exists()
    assert result.coverage_path.exists()
    assert result.provenance_path.exists()
    assert result.readiness_path.exists()


def test_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_canonical_trade_path_layer.py"),
        Path("src/mgc_v05l/app/track_b_canonical_trade_path_layer.py"),
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


def _outcome(trade_id: str) -> dict:
    return {
        "trade_outcome_id": f"outcome_{trade_id}",
        "strategy_id": "strategy",
        "lane_id": "lane",
        "instrument": "GC",
        "contract": "GCQ6",
        "side": "LONG",
        "quantity": 1,
        "entry_time": "2026-07-07T00:00:00Z",
        "exit_time": "2026-07-07T00:20:00Z",
        "entry_price": 100.0,
        "exit_price": 102.0,
        "source_refs": {"source_trade_id": trade_id},
    }


def _enrichment(outcome_id: str) -> dict:
    return {
        "trade_outcome_enrichment_id": f"enrichment_{outcome_id}",
        "trade_outcome_id": outcome_id,
    }


def _attribution(outcome_id: str) -> dict:
    return {
        "trade_decision_attribution_id": f"decision_{outcome_id}",
        "trade_outcome_id": outcome_id,
    }


def _retained(trade_id: str, outcome_id: str) -> dict:
    return {
        "schema_version": "retained_trade_path_capture_v1",
        "retained_path_capture_id": f"retained_{trade_id}",
        "deterministic_fingerprint": f"fingerprint_{trade_id}",
        "source_trade_id": trade_id,
        "trade_outcome_id": outcome_id,
        "instrument": "GC",
        "contract": "GCQ6",
        "side": "LONG",
        "entry_time": "2026-07-07T00:00:00Z",
        "exit_time": "2026-07-07T00:20:00Z",
        "entry_price": 100.0,
        "exit_price": 102.0,
        "mfe_points": 5.0,
        "mae_points": -2.0,
        "entry_to_exit_path": [
            {
                "bar_end": "2026-07-07T00:00:00Z",
                "favorable_excursion_points": 0.0,
                "adverse_excursion_points": 0.0,
            },
            {
                "bar_end": "2026-07-07T00:20:00Z",
                "favorable_excursion_points": 5.0,
                "adverse_excursion_points": -2.0,
            },
        ],
        "post_exit_forward_windows": {
            "15m": {
                "available": True,
                "bar_count": 1,
            }
        },
    }
