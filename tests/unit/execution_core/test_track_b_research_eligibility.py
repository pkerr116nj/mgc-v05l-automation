from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_research_eligibility import (
    ELIGIBLE_LIMITED,
    EXCLUDED_SOURCE_INTEGRITY,
    FULL_HISTORICAL,
    SOURCE_INTEGRITY_QUALIFIED,
    build_research_eligibility_records,
    run_research_eligibility,
)


NOW = datetime(2026, 8, 6, 12, 0, tzinfo=UTC)


def test_one_eligibility_row_per_crr_row_and_five_anomalies() -> None:
    rows = [_crr_row(index) for index in range(6)]
    anomalies = _anomaly_root(rows[:5])
    classifications = _extreme_classification(rows[:5])

    records = build_research_eligibility_records(
        rows,
        crr_validation=_crr_validation(),
        anomaly_root_cause=anomalies,
        extreme_classification=classifications,
        source_paths=_source_paths(),
        generated_at=NOW,
    )

    assert len(records) == 6
    assert sum(1 for record in records if record["classification"] == EXCLUDED_SOURCE_INTEGRITY) == 5
    assert sum(1 for record in records if record["classification"] == ELIGIBLE_LIMITED) == 1


def test_run_writes_parseable_artifacts_and_population_views(tmp_path: Path) -> None:
    crr_path = tmp_path / "crr.jsonl"
    validation_path = tmp_path / "crr_validation.json"
    anomaly_path = tmp_path / "anomaly_root_cause.json"
    classification_path = tmp_path / "extreme_trade_classification.json"
    output_dir = tmp_path / "eligibility"
    rows = [_crr_row(index, pnl=-100.0 if index < 5 else 10.0) for index in range(6)]
    _write_jsonl(crr_path, rows)
    validation_path.write_text(json.dumps(_crr_validation()), encoding="utf-8")
    anomaly_path.write_text(json.dumps(_anomaly_root(rows[:5])), encoding="utf-8")
    classification_path.write_text(json.dumps(_extreme_classification(rows[:5])), encoding="utf-8")

    result = run_research_eligibility(
        crr_path=crr_path,
        crr_validation_path=validation_path,
        anomaly_root_cause_path=anomaly_path,
        extreme_classification_path=classification_path,
        output_dir=output_dir,
        now=NOW,
    )

    assert result.validation["status"] == "VALID_WITH_WARNINGS"
    assert len([json.loads(line) for line in result.records_path.read_text().splitlines()]) == 6
    summary = json.loads(result.summary_path.read_text())
    assert summary["views"][FULL_HISTORICAL]["included_count"] == 6
    assert summary["views"][SOURCE_INTEGRITY_QUALIFIED]["included_count"] == 1
    assert summary["views"][SOURCE_INTEGRITY_QUALIFIED]["excluded_count"] == 5


def test_no_row_excluded_without_source_backed_evidence() -> None:
    rows = [_crr_row(index) for index in range(3)]

    records = build_research_eligibility_records(
        rows,
        crr_validation=_crr_validation(),
        anomaly_root_cause={"records": []},
        extreme_classification={"records": []},
        source_paths=_source_paths(),
        generated_at=NOW,
    )

    assert all(record["classification"] != EXCLUDED_SOURCE_INTEGRITY for record in records)
    assert all(record["review_required"] is False for record in records)


def test_fallback_recovers_source_integrity_anomalies_from_crr_when_inv_artifacts_are_empty() -> None:
    rows = [_crr_row(index, pnl=float(index + 1)) for index in range(25)]
    rows[0] = _crr_row(0, pnl=-2475530.0, entry_price="28757", exit_price="4001.7", entry_exec_id="scale_1")
    rows[1] = _crr_row(1, pnl=-510108.0, entry_price="4073.6", exit_price="29579", entry_exec_id="scale_2")
    rows[2] = _crr_row(2, pnl=-18065.0, entry_exec_id="dup_a", source_trade_id="trade_dup_a")
    rows[3] = _crr_row(3, pnl=-13942.24, entry_exec_id="dup_b", source_trade_id="trade_dup_b")
    rows[4] = _crr_row(4, pnl=-13152.52, entry_exec_id="dup_c", source_trade_id="trade_dup_c")
    rows[20] = _crr_row(20, pnl=501125.0, entry_exec_id="dup_a", source_trade_id="other_dup_a")
    rows[21] = _crr_row(21, pnl=6377.48, entry_exec_id="dup_c", source_trade_id="other_dup_c")
    rows[22] = _crr_row(22, pnl=25.0, entry_exec_id="dup_b", source_trade_id="other_dup_b")

    records = build_research_eligibility_records(
        rows,
        crr_validation=_crr_validation(),
        anomaly_root_cause={"records": []},
        extreme_classification={"records": []},
        source_paths=_source_paths(),
        generated_at=NOW,
    )

    excluded = [record for record in records if record["classification"] == EXCLUDED_SOURCE_INTEGRITY]
    assert len(excluded) == 5
    assert {
        record["supporting_ids"]["entry_exec_id"]
        for record in excluded
        if "DUPLICATE_OR_REUSED_EXECUTION_EVIDENCE" in record["evidence_basis"][0]
    } == {"dup_a", "dup_b", "dup_c"}
    assert sum("CONTRACT_MULTIPLIER_OR_SCALE_MISMATCH" in record["evidence_basis"][0] for record in excluded) == 2


def test_deterministic_membership_and_fingerprints() -> None:
    rows = [_crr_row(index) for index in range(5)]
    first = build_research_eligibility_records(
        rows,
        crr_validation=_crr_validation(),
        anomaly_root_cause=_anomaly_root(rows[:5]),
        extreme_classification=_extreme_classification(rows[:5]),
        source_paths=_source_paths(),
        generated_at=NOW,
    )
    second = build_research_eligibility_records(
        rows,
        crr_validation=_crr_validation(),
        anomaly_root_cause=_anomaly_root(rows[:5]),
        extreme_classification=_extreme_classification(rows[:5]),
        source_paths=_source_paths(),
        generated_at=NOW,
    )

    assert [record["classification"] for record in first] == [record["classification"] for record in second]
    assert [record["deterministic_fingerprint"] for record in first] == [record["deterministic_fingerprint"] for record in second]


def test_guardrails_and_no_prohibited_imports_or_actions() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_research_eligibility.py"),
        Path("src/mgc_v05l/app/track_b_research_eligibility.py"),
    ]
    prohibited_import_roots = {
        "ibapi",
        "mgc_v05l.execution",
        "mgc_v05l.runtime",
        "mgc_v05l.strategy",
        "mgc_v05l.guardian",
        "mgc_v05l.safe_state",
        "mgc_v05l.reconciliation",
    }
    prohibited_actions = ("placeOrder", "cancelOrder", "globalCancel", "transmit", "paper_proof")
    for path in paths:
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


def _crr_row(
    index: int,
    *,
    pnl: float = 1.0,
    entry_price: str = "100.0",
    exit_price: str = "99.0",
    entry_exec_id: str | None = None,
    source_trade_id: str | None = None,
) -> dict[str, object]:
    record_id = f"canonical_research_record_{index}"
    trade_id = source_trade_id or f"trade_{index}"
    return {
        "schema_version": "canonical_research_record_v1",
        "research_record_id": record_id,
        "deterministic_fingerprint": f"fingerprint_{index}",
        "trade_identity": {
            "source_trade_id": trade_id,
            "trade_id": trade_id,
            "lifecycle_id": f"lifecycle_{index}",
            "instrument": "NQ",
            "side": "LONG",
        },
        "entry_anchor": {"strategy_id": "strategy", "lane_id": "lane", "entry_price": entry_price, "entry_exec_id": entry_exec_id},
        "exit_anchor": {"exit_price": exit_price, "exit_exec_id": f"exit_{index}"},
        "enrichment_ref": {"context_validity_summary": {"session": "US", "market_context_validity_classification": "VALID"}},
        "outcome_ref": {"trade_outcome_id": f"outcome_{index}"},
        "path_ref": {"canonical_trade_path_id": f"path_{index}", "capture_id": f"capture_{index}"},
        "attribution_ref": {"trade_decision_attribution_id": f"attribution_{index}"},
        "join_quality": {
            "exact": [{"layer": layer, "quality": "EXACT"} for layer in ("ctol", "ctoe", "ra7", "ra3")],
            "missing": [{"layer": "ra8", "quality": "MISSING"}],
            "broken": [],
            "tolerance": [],
        },
        "outcome_summary": {
            "realized_pnl_proxy": pnl,
            "mfe_points": None,
            "mae_points": None,
            "data_quality_flags": ["missing_mfe", "missing_mae"],
        },
        "source_provenance": [{"source_artifact_path": "crr.jsonl", "source_name": "canonical_trade_records"}],
    }


def _anomaly_root(rows: list[dict[str, object]]) -> dict[str, object]:
    return {
        "records": [
            {
                "research_record_id": row["research_record_id"],
                "confidence": "HIGH",
                "summary": "Source-confirmed anomaly.",
                "contradictory_evidence": ["Canonical history is preserved."],
            }
            for row in rows
        ]
    }


def _extreme_classification(rows: list[dict[str, object]]) -> dict[str, object]:
    classes = ["CONTRACT_MULTIPLIER_OR_SCALE_MISMATCH", "DUPLICATE_OR_REUSED_EXECUTION_EVIDENCE"]
    return {
        "records": [
            {
                "research_record_id": row["research_record_id"],
                "source_trade_id": row["trade_identity"]["source_trade_id"],
                "classification": classes[index % 2],
                "confidence": "HIGH",
                "reasoning": "Source-confirmed anomaly.",
                "supporting_artifact_paths": ["artifact.json"],
                "supporting_ids": {"source_trade_id": row["trade_identity"]["source_trade_id"]},
            }
            for index, row in enumerate(rows)
        ]
    }


def _crr_validation() -> dict[str, object]:
    return {
        "status": "VALID_WITH_WARNINGS",
        "refresh_guidance": [{"source_name": "ra8", "classification": "SOURCE_COVERAGE_LIMIT"}],
    }


def _source_paths() -> dict[str, Path]:
    return {
        "crr": Path("crr.jsonl"),
        "crr_validation": Path("crr_validation.json"),
        "anomaly_root_cause": Path("anomaly_root_cause.json"),
        "extreme_trade_classification": Path("extreme_trade_classification.json"),
    }


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
