from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution_core.track_b_nq_prospective_cohort_monitor import (
    DISCOVERY_CUTOFF_UTC,
    build_checkpoint_history,
    build_discovery_baselines,
    build_prospective_results,
    compare_discovery_vs_prospective,
    freeze_cohort_definitions,
    merge_checkpoint_history,
    qualified_nq_rows,
    run_nq_prospective_cohort_monitor,
    validation_state_for_count,
)


NOW = "2026-08-09T12:00:00+00:00"


def test_discovery_cutoff_and_prospective_membership_are_deterministic() -> None:
    rows = [_crr_row("before", "2026-08-06T20:00:00+00:00"), _crr_row("after", DISCOVERY_CUTOFF_UTC)]
    eligibility = {row["research_record_id"]: _eligibility(row["research_record_id"]) for row in rows}

    qualified = qualified_nq_rows(rows, eligibility_by_id=eligibility)
    discovery = [row for row in qualified if row["exit_time"] < DISCOVERY_CUTOFF_UTC]
    prospective = [row for row in qualified if row["exit_time"] >= DISCOVERY_CUTOFF_UTC]

    assert [row["research_record_id"] for row in discovery] == ["before"]
    assert [row["research_record_id"] for row in prospective] == ["after"]


def test_baseline_remains_unchanged_after_prospective_rows() -> None:
    discovery = [_flat("d1", pnl=100.0), _flat("d2", pnl=-20.0)]
    definitions = freeze_cohort_definitions(discovery, {"assignments": []})

    first = build_discovery_baselines(
        discovery,
        cohort_definitions=definitions,
        generated_at=NOW,
        source_fingerprints={},
        peer_metrics={},
        period_comparison={},
    )
    second = build_discovery_baselines(
        [*discovery, _flat("future", pnl=9999.0, exit_time=DISCOVERY_CUTOFF_UTC)],
        cohort_definitions=definitions,
        generated_at=NOW,
        source_fingerprints={},
        peer_metrics={},
        period_comparison={},
    )

    assert first["baselines"]["nq_globex_participation_long"]["discovery_trade_count"] == 2
    assert second["baselines"]["nq_globex_participation_long"]["discovery_trade_count"] == 3
    assert first["deterministic_fingerprint"] != second["deterministic_fingerprint"]


def test_checkpoint_creation_thresholds_are_deterministic() -> None:
    rows = [_flat(f"p{i:03d}", pnl=float(i), exit_time=f"2026-08-07T12:{i:02d}:00+00:00") for i in range(20)]
    prospective = build_prospective_results(rows, cohort_definitions=[{"cohort_id": "all", "source": "fixture", "definition": {"instrument": "NQ"}}], generated_at=NOW)
    checkpoints = build_checkpoint_history(prospective, generated_at=NOW)

    assert [row["checkpoint_trade_count"] for row in checkpoints] == [10, 20]
    assert checkpoints == build_checkpoint_history(prospective, generated_at=NOW)


def test_checkpoint_history_is_append_only_by_checkpoint_key(tmp_path: Path) -> None:
    history = tmp_path / "checkpoint_history.jsonl"
    existing = {
        "cohort_id": "all",
        "checkpoint_trade_count": 10,
        "generated_at": "2026-08-08T00:00:00+00:00",
        "deterministic_fingerprint": "old",
    }
    history.write_text(json.dumps(existing) + "\n", encoding="utf-8")

    merged = merge_checkpoint_history(
        history,
        [
            existing | {"deterministic_fingerprint": "new_duplicate"},
            {"cohort_id": "all", "checkpoint_trade_count": 20, "generated_at": NOW, "deterministic_fingerprint": "new"},
        ],
    )

    assert [row["checkpoint_trade_count"] for row in merged] == [10, 20]
    assert merged[0]["deterministic_fingerprint"] == "old"


def test_validation_state_thresholds() -> None:
    assert validation_state_for_count(9, None) == "NOT_ENOUGH_PROSPECTIVE_DATA"
    assert validation_state_for_count(10, None) == "EARLY_MIXED_EVIDENCE"
    assert validation_state_for_count(20, {"expectancy": {"absolute_delta": 1.0}}) == "PROSPECTIVE_CONFIRMATION_STRENGTHENING"
    assert validation_state_for_count(20, {"expectancy": {"absolute_delta": -1.0}}) == "PROSPECTIVE_CONFIRMATION_WEAKENING"


def test_run_monitor_writes_read_only_artifacts_and_preserves_ra8_optionality(tmp_path: Path) -> None:
    crr = tmp_path / "crr.jsonl"
    eligibility = tmp_path / "eligibility.jsonl"
    validation = tmp_path / "crr_validation.json"
    peer_assignments = tmp_path / "peer_assignments.json"
    peer_metrics = tmp_path / "peer_metrics.json"
    inv006_period = tmp_path / "period.json"
    output_dir = tmp_path / "out"
    rows = [_crr_row("d1", "2026-08-06T20:00:00+00:00"), _crr_row("p1", DISCOVERY_CUTOFF_UTC, ra8="MISSING")]
    crr.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    eligibility.write_text("".join(json.dumps(_eligibility(row["research_record_id"])) + "\n" for row in rows), encoding="utf-8")
    validation.write_text(json.dumps({"status": "VALID_WITH_WARNINGS"}), encoding="utf-8")
    peer_assignments.write_text(json.dumps({"assignments": []}), encoding="utf-8")
    peer_metrics.write_text(json.dumps({"deterministic_fingerprint": "peer"}), encoding="utf-8")
    inv006_period.write_text(json.dumps({"deterministic_fingerprint": "period"}), encoding="utf-8")

    result = run_nq_prospective_cohort_monitor(
        crr_path=crr,
        crr_validation_path=validation,
        eligibility_records_path=eligibility,
        inv_005_peer_assignments_path=peer_assignments,
        inv_005_peer_metrics_path=peer_metrics,
        inv_006_period_comparison_path=inv006_period,
        output_dir=output_dir,
        now=NOW,
    )

    assert result.prospective_population["included_count"] == 1
    assert result.validation_report["status"] == "VALID_WITH_WARNINGS"
    assert result.prospective_results["cohorts"]["nq_globex_participation_long"]["ra8_coverage"]["missing"] == 1
    assert result.discovery_baselines["guardrails"]["live_money_eligible"] is False
    assert (output_dir / "prospective_nq_cohort_monitor.html").exists()


def test_no_runtime_broker_imports_or_authority_terms() -> None:
    source = Path("src/mgc_v05l/execution_core/track_b_nq_prospective_cohort_monitor.py").read_text()
    import_lines = [line for line in source.splitlines() if line.startswith(("import ", "from "))]
    prohibited_imports = ("ibapi", "mgc_v05l.runtime", "mgc_v05l.broker", "mgc_v05l.guardian", "mgc_v05l.safe_state")
    prohibited_actions = ("placeOrder(", "cancelOrder(", "globalCancel(", "submit_order(", "flatten_position(")

    assert not any(term in line for line in import_lines for term in prohibited_imports)
    assert not any(term in source for term in prohibited_actions)
    assert "No production recommendation" in source


def _crr_row(record_id: str, exit_time: str, *, ra8: str = "EXACT") -> dict[str, object]:
    return {
        "research_record_id": record_id,
        "deterministic_fingerprint": f"fp_{record_id}",
        "trade_identity": {
            "instrument": "NQ",
            "contract": "NQU6",
            "side": "LONG",
            "quantity": "1",
            "source_trade_id": f"trade_{record_id}",
            "trade_id": f"trade_{record_id}",
            "lifecycle_id": f"lifecycle_{record_id}",
        },
        "entry_anchor": {
            "strategy_id": "PAPER_ACTIVE_EVIDENCE_NQ_GLOBEX_PARTICIPATION_LONG_V1",
            "lane_id": "nq_globex_active_participation_long",
            "entry_time": "2026-08-06T19:45:00+00:00",
        },
        "exit_anchor": {"exit_time": exit_time, "exit_policy": "TIMEBOX", "exit_reason": "TIMEBOX"},
        "outcome_summary": {"realized_pnl_proxy": 100.0, "hold_seconds": 900.0},
        "enrichment_ref": {"context_validity_summary": {"session": "GLOBEX", "market_context_validity_classification": "VALID"}},
        "path_ref": {"ra8_join_quality": ra8, "path_status": "MISSING_SOURCE"},
        "attribution_ref": {"trade_decision_attribution_id": f"attr_{record_id}"},
        "join_quality": {"broken": [], "exact": [{"layer": "ctol"}, {"layer": "ctoe"}, {"layer": "ra7"}, {"layer": "ra3"}]},
        "source_provenance": [],
        "guardrails": {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False},
    }


def _eligibility(record_id: str) -> dict[str, object]:
    return {
        "research_record_id": record_id,
        "classification": "ELIGIBLE_ORDINARY_STRATEGY_EVIDENCE",
        "source_fingerprint": {"crr_record_fingerprint": f"fp_{record_id}"},
    }


def _flat(record_id: str, *, pnl: float, exit_time: str = "2026-08-06T20:00:00+00:00") -> dict[str, object]:
    row = _crr_row(record_id, exit_time)
    flat = {
        "research_record_id": record_id,
        "instrument": "NQ",
        "strategy_id": "PAPER_ACTIVE_EVIDENCE_NQ_GLOBEX_PARTICIPATION_LONG_V1",
        "side": "LONG",
        "session": "GLOBEX",
        "exit_time": exit_time,
        "realized_pnl_proxy": pnl,
        "ra8_join_quality": "MISSING",
    }
    return flat | {"source_fingerprint": row["deterministic_fingerprint"]}
