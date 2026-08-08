from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.observatory_operational_health import (
    SCHEMA_VERSION,
    build_operational_health,
    write_operational_health,
)


BASE_TS = datetime(2026, 8, 7, 19, 30, tzinfo=UTC)


def test_operational_leds_preserve_producer_statuses(tmp_path: Path) -> None:
    _write_json(
        tmp_path,
        "outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_listener_status.json",
        {"schema_version": "phase1", "final_classification": "PHASE1_DATABENTO_LIVE_LISTENER_RUNNING", "generated_at": BASE_TS.isoformat()},
    )
    _write_json(
        tmp_path,
        "outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json",
        {"schema_version": "runtime", "classification": "STALE_RUNTIME_TRUTH", "generated_at": BASE_TS.isoformat()},
    )
    _write_json(
        tmp_path,
        "outputs/operator_dashboard/runtime/latest_broker_session_authority.json",
        {"schema_version": "broker", "classification": "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE", "generated_at": BASE_TS.isoformat()},
    )
    _write_json(
        tmp_path,
        "outputs/track_b_execution_core/managed_exit_service/latest_managed_exit_service_status.json",
        {"schema_version": "managed_exit", "classification": "NO_ELIGIBLE_EXITS", "generated_at": BASE_TS.isoformat()},
    )
    _write_json(
        tmp_path,
        "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {"schema_version": "reconciliation", "classification": "WAITING_FOR_BROKER_TRUTH_SETTLEMENT", "generated_at": BASE_TS.isoformat()},
    )
    _write_json(
        tmp_path,
        "outputs/track_b_execution_core/research_analytics/canonical_research_record/canonical_research_record_validation_report.json",
        {"schema_version": "crr", "status": "VALID_WITH_WARNINGS", "generated_at": BASE_TS.isoformat()},
    )
    _write_json(
        tmp_path,
        "outputs/track_b_execution_core/research_analytics/prospective_nq_cohort_monitor/validation_report.json",
        {"schema_version": "prospective", "status": "VALID_WITH_WARNINGS", "generated_at": BASE_TS.isoformat()},
    )

    snapshot = build_operational_health(repo_root=tmp_path, generated_at=BASE_TS)
    by_id = {row["indicator_id"]: row for row in snapshot["indicators"]}

    assert snapshot["schema_version"] == SCHEMA_VERSION
    assert by_id["market_data"]["led_state"] == "GREEN"
    assert by_id["regime_context"]["led_state"] == "GRAY"
    assert by_id["magic_runtime"]["led_state"] == "AMBER"
    assert by_id["broker_tws"]["producer_status"] == "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE"
    assert by_id["broker_tws"]["led_state"] == "AMBER"
    assert by_id["trade_evidence"]["led_state"] == "GREEN"
    assert by_id["crr_research"]["led_state"] == "AMBER"
    assert snapshot["guardrails"]["readiness_computation"] is False


def test_missing_sources_are_gray_not_inferred_healthy(tmp_path: Path) -> None:
    snapshot = build_operational_health(repo_root=tmp_path, generated_at=BASE_TS)

    assert all(row["led_state"] == "GRAY" for row in snapshot["indicators"] if row["indicator_id"] != "regime_context")
    assert snapshot["guardrails"]["safe_state_computation"] is False


def test_json_output_round_trip(tmp_path: Path) -> None:
    snapshot = build_operational_health(repo_root=tmp_path, generated_at=BASE_TS)
    output = tmp_path / "health.json"

    write_operational_health(snapshot, output)

    parsed = json.loads(output.read_text(encoding="utf-8"))
    assert parsed["schema_version"] == SCHEMA_VERSION


def test_no_broker_runtime_mutation_imports_or_calls() -> None:
    path = Path("src/mgc_v05l/execution_core/observatory_operational_health.py")
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    forbidden_import_roots = ("mgc_v05l.execution.", "mgc_v05l.strategy", "mgc_v05l.brokers", "ibapi")
    forbidden_call_names = {"submit", "cancel", "close", "placeOrder", "flatten", "computeSafeState", "computeReadiness"}
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            violations.extend(alias.name for alias in node.names if alias.name.startswith(forbidden_import_roots))
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.startswith(forbidden_import_roots):
            violations.append(node.module)
        elif isinstance(node, ast.Call):
            name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else None
            if name in forbidden_call_names:
                violations.append(name)
    assert violations == []


def _write_json(root: Path, relative_path: str, payload: dict) -> None:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
