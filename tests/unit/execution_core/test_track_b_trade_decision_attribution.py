from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_trade_decision_attribution import (
    build_trade_decision_attributions,
    build_trade_decision_summary,
    run_trade_decision_attribution,
)


NOW = datetime(2026, 7, 7, 12, 0, tzinfo=UTC)


def test_deterministic_join_and_managed_exit_timeout(tmp_path: Path) -> None:
    entry_lifecycle = tmp_path / "entry_lifecycle.json"
    exit_lifecycle = tmp_path / "exit_lifecycle.json"
    entry_lifecycle.write_text(
        json.dumps(
            {
                "entry_intent": {
                    "signal_reason": "LONG",
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
                },
                "entry_submit_attempt": {"submitted": True},
                "final_position_status": "OPEN_MANAGED",
            }
        ),
        encoding="utf-8",
    )
    exit_lifecycle.write_text(
        json.dumps(
            {
                "close_intent": {
                    "close_reason": "TIME_BOXED_EXIT",
                    "exit_family": "DIAGNOSTIC_TIME",
                    "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
                    "required_completed_5m_bars": 3,
                    "elapsed_completed_5m_bars": 3,
                },
                "close_submit_attempt": {"broker_order_id": "9"},
                "close_fill": {"execution_id": "exit_exec"},
                "final_broker_state_classification": "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT",
            }
        ),
        encoding="utf-8",
    )

    rows = build_trade_decision_attributions(
        [_outcome()],
        enrichments=[_enrichment()],
        canonical_records=[_canonical(entry_lifecycle, exit_lifecycle)],
        generated_at=NOW,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["entry"]["entry_signal"] == "LONG"
    assert row["entry"]["entry_authority"] == "TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE"
    assert row["exit"]["canonical_exit_reason"] == "MANAGED_EXIT_TIMEOUT"
    assert row["exit"]["exit_authority"] == "MANAGED_EXIT"
    assert row["exit"]["managed_exit_involved"] is True
    assert row["attribution_confidence"] == "HIGH"
    assert row["diagnostic_only"] is True
    assert row["production_recommendation"] is False
    assert row["trading_gate"] is False


def test_unknown_handling_when_sources_missing() -> None:
    rows = build_trade_decision_attributions([_outcome()], generated_at=NOW)

    row = rows[0]
    assert row["entry"]["entry_authority"] == "UNKNOWN"
    assert row["exit"]["canonical_exit_reason"] == "TIMEBOX"
    assert "missing_canonical_trade_record_join" in row["unknown_reasons"]
    assert "missing_entry_lifecycle_report" in row["unknown_reasons"]


def test_source_trade_id_join_tolerates_timestamp_drift(tmp_path: Path) -> None:
    outcome = _outcome()
    outcome["exit_time"] = "2026-07-01T01:15:00.000250Z"
    outcome["source_refs"] = {"source_trade_id": "trade_fixture"}
    canonical = _canonical(tmp_path / "missing_entry.json", tmp_path / "missing_exit.json")
    canonical["exit_time"] = "2026-07-01T01:15:00.000001Z"

    rows = build_trade_decision_attributions([outcome], canonical_records=[canonical], generated_at=NOW)

    assert "missing_canonical_trade_record_join" not in rows[0]["unknown_reasons"]
    assert rows[0]["provenance"]["source_refs"]["canonical_trade_id"] == "trade_fixture"


def test_summary_distributions_and_coverage(tmp_path: Path) -> None:
    rows = build_trade_decision_attributions(
        [_outcome()],
        enrichments=[_enrichment()],
        canonical_records=[_canonical(tmp_path / "missing_entry.json", tmp_path / "missing_exit.json")],
        generated_at=NOW,
    )
    summary = build_trade_decision_summary(rows, generated_at=NOW)

    assert summary["overall"]["completed_trade_attributions"] == 1
    assert summary["distributions"]["canonical_exit_reason"]["TIMEBOX"] == 1
    assert summary["diagnostic_only"] is True
    assert summary["trading_gate"] is False


def test_idempotent_generation(tmp_path: Path) -> None:
    outcomes = tmp_path / "outcomes.jsonl"
    enrichments = tmp_path / "enrichments.jsonl"
    canonical = tmp_path / "canonical.jsonl"
    outcomes.write_text(json.dumps(_outcome()) + "\n", encoding="utf-8")
    enrichments.write_text(json.dumps(_enrichment()) + "\n", encoding="utf-8")
    canonical.write_text(json.dumps(_canonical(tmp_path / "missing_entry.json", tmp_path / "missing_exit.json")) + "\n", encoding="utf-8")

    first = run_trade_decision_attribution(
        outcomes_path=outcomes,
        enrichments_path=enrichments,
        canonical_records_path=canonical,
        output_dir=tmp_path / "first",
        now=NOW,
    )
    second = run_trade_decision_attribution(
        outcomes_path=outcomes,
        enrichments_path=enrichments,
        canonical_records_path=canonical,
        output_dir=tmp_path / "second",
        now=NOW,
    )

    assert first.attributions[0]["deterministic_fingerprint"] == second.attributions[0]["deterministic_fingerprint"]
    assert json.loads(first.summary_json_path.read_text())["overall"] == json.loads(second.summary_json_path.read_text())["overall"]


def test_json_artifacts_parse(tmp_path: Path) -> None:
    outcomes = tmp_path / "outcomes.jsonl"
    enrichments = tmp_path / "enrichments.jsonl"
    canonical = tmp_path / "canonical.jsonl"
    outcomes.write_text(json.dumps(_outcome()) + "\n", encoding="utf-8")
    enrichments.write_text(json.dumps(_enrichment()) + "\n", encoding="utf-8")
    canonical.write_text(json.dumps(_canonical(tmp_path / "missing_entry.json", tmp_path / "missing_exit.json")) + "\n", encoding="utf-8")

    result = run_trade_decision_attribution(
        outcomes_path=outcomes,
        enrichments_path=enrichments,
        canonical_records_path=canonical,
        output_dir=tmp_path / "out",
        now=NOW,
    )

    assert json.loads(result.summary_json_path.read_text())["schema_version"] == "ra3_trade_decision_attribution_summary_v1"
    assert json.loads(result.schema_path.read_text())["title"] == "CanonicalTradeDecisionAttribution"
    assert json.loads(result.sample_path.read_text())[0]["trade_outcome_id"] == "trade_outcome_fixture"


def test_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_trade_decision_attribution.py"),
        Path("src/mgc_v05l/app/track_b_trade_decision_attribution.py"),
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


def _outcome() -> dict:
    return {
        "trade_outcome_id": "trade_outcome_fixture",
        "strategy_id": "PAPER_ACTIVE_EVIDENCE_GC_GLOBEX_PARTICIPATION_LONG_V1",
        "lane_id": "gc_globex_active_participation_long",
        "instrument": "GC",
        "contract": "GCQ6",
        "side": "LONG",
        "entry_time": "2026-07-01T01:00:00Z",
        "exit_time": "2026-07-01T01:15:00Z",
        "session_at_entry": "GLOBEX",
        "exit_policy": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        "exit_reason": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
    }


def _enrichment() -> dict:
    return {
        "trade_outcome_id": "trade_outcome_fixture",
        "market_context_validity_classification": "VALID",
        "vix_regime": "NORMAL",
        "vix_level": 16.2,
        "vix_percentile": 0.42,
        "market_context_timestamp": "2026-06-30T20:15:00Z",
        "gre_validity_classification": "VALID",
        "gre_label": "LONG",
        "gre_confidence": 70,
        "gre_timestamp": "2026-07-01T00:59:00Z",
        "crfd_validity_classification": "VALID",
        "crfd_observation_time": "2026-07-01T00:59:00Z",
        "vwap_relation": "above_vwap",
        "avwap_relation": "above_avwap",
    }


def _canonical(entry_lifecycle: Path, exit_lifecycle: Path) -> dict:
    return {
        "trade_status": "CLOSED",
        "pairing_status": "PAIRED",
        "trade_id": "trade_fixture",
        "strategy_id": "PAPER_ACTIVE_EVIDENCE_GC_GLOBEX_PARTICIPATION_LONG_V1",
        "lane_id": "gc_globex_active_participation_long",
        "symbol": "GC",
        "local_symbol": "GCQ6",
        "side": "LONG",
        "entry_time": "2026-07-01T01:00:00Z",
        "exit_time": "2026-07-01T01:15:00Z",
        "entry_thesis": "generic_active_participation",
        "exit_policy": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        "exit_reason": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        "source_refs": {
            "managed_lifecycle_report": str(entry_lifecycle),
            "exit_source_artifact": str(exit_lifecycle),
            "entry_fill": "entry_fills.jsonl",
            "exit_fill": "live_trade_events.jsonl",
        },
    }
