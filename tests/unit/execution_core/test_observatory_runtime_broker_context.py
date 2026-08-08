from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.observatory_runtime_broker_context import (
    SCHEMA_VERSION,
    build_runtime_broker_context,
    write_runtime_broker_context,
)


BASE_TS = datetime(2026, 8, 7, 19, 40, tzinfo=UTC)


def test_runtime_broker_context_passes_through_prepared_fields(tmp_path: Path) -> None:
    _write_json(
        tmp_path,
        "outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json",
        {
            "schema_version": "runtime",
            "classification": "STALE_RUNTIME_TRUTH",
            "generated_at": BASE_TS.isoformat(),
            "fresh_until": BASE_TS.isoformat(),
            "mode": "PAPER",
            "source_pid": 123,
            "submit_authority": False,
            "runtime": {"commit": "abc", "profile": "paper", "lane_count": 71, "trading_loop_entered": True},
        },
    )
    _write_json(
        tmp_path,
        "outputs/operator_dashboard/runtime/latest_broker_session_authority.json",
        {
            "schema_version": "broker",
            "classification": "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE",
            "generated_at": BASE_TS.isoformat(),
            "mode": "PAPER",
            "server_version": 157,
            "lease_state": "INVALIDATED_CONTRADICTION",
            "callback_missing_reason": "order_status_callback_missing",
        },
    )
    _write_json(
        tmp_path,
        "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {"schema_version": "orders", "classification": "NO_OPEN_ORDERS", "broker_open_orders": [], "submit_authority": False},
    )
    _write_json(
        tmp_path,
        "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {"schema_version": "managed", "classification": "OPEN_MANAGED_EXIT_DUE", "managed_position_count": 2, "submit_authority": False},
    )
    _write_json(
        tmp_path,
        "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {
            "schema_version": "recon",
            "classification": "WAITING_FOR_BROKER_TRUTH_SETTLEMENT",
            "broker_truth_lease_state": "ACTIVE",
            "broker_truth_reliable_for_position": True,
            "broker_truth_reliable_for_order_status": False,
            "track_b_broker_position_count": 2,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "review_required_count": 0,
        },
    )

    snapshot = build_runtime_broker_context(repo_root=tmp_path, generated_at=BASE_TS)

    assert snapshot["schema_version"] == SCHEMA_VERSION
    assert snapshot["runtime"]["classification"] == "STALE_RUNTIME_TRUTH"
    assert snapshot["runtime"]["loaded_commit"] == "abc"
    assert snapshot["runtime"]["active_lane_count"] == 71
    assert snapshot["runtime"]["submit_authority"] is False
    assert snapshot["runtime"]["submit_authority_source"] == "producer_authored"
    assert snapshot["broker"]["server_version"] == 157
    assert snapshot["broker"]["open_order_count"] == 0
    assert snapshot["broker"]["broker_position_count"] == 2
    assert snapshot["broker"]["broker_truth_reliable_for_order_status"] is False
    assert snapshot["guardrails"]["submit_authority_computation"] is False


def test_missing_sources_fail_soft(tmp_path: Path) -> None:
    snapshot = build_runtime_broker_context(repo_root=tmp_path, generated_at=BASE_TS)

    assert snapshot["status"] == "VALID_WITH_WARNINGS"
    assert snapshot["runtime"]["source_status"] == "MISSING"
    assert snapshot["broker"]["broker_session_source_status"] == "MISSING"


def test_json_output_round_trip(tmp_path: Path) -> None:
    snapshot = build_runtime_broker_context(repo_root=tmp_path, generated_at=BASE_TS)
    output = tmp_path / "runtime_broker.json"

    write_runtime_broker_context(snapshot, output)

    parsed = json.loads(output.read_text(encoding="utf-8"))
    assert parsed["schema_version"] == SCHEMA_VERSION


def test_no_broker_runtime_mutation_imports_or_calls() -> None:
    path = Path("src/mgc_v05l/execution_core/observatory_runtime_broker_context.py")
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
