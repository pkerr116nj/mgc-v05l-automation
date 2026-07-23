from __future__ import annotations

import ast
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.app.track_b_safe_state_refresh import main as safe_state_refresh_main
from mgc_v05l.execution_core.track_b_runtime_safe_state_envelope import (
    SAFE_STATE_BROKER_MUTATION_LIMIT_HIT,
    SAFE_STATE_DUPLICATE_INTENT_RISK,
    SAFE_STATE_HARD_HOLD,
    SAFE_STATE_LIFECYCLE_DISAGREEMENT_LIMIT_HIT,
    SAFE_STATE_NORMAL,
    SAFE_STATE_POSITION_LIMIT_HIT,
    TrackBRuntimeSafeStateEnvelopeConfig,
    build_track_b_runtime_safe_state_envelope,
    refresh_track_b_runtime_safe_state_envelope,
    write_track_b_runtime_safe_state_envelope,
)
from mgc_v05l.execution_core.track_b_operational_certification import build_operational_certification


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_clean_state_is_normal(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_NORMAL
    assert payload["broker_mutation_allowed"] is True
    assert payload["runtime_start_allowed"] is True
    assert payload["submit_allowed"] is True
    assert payload["observe_only"] is False
    assert payload["recovery_only"] is False
    assert payload["tripped_limits"] == []


def test_running_runtime_normal_safe_state_allows_submit_without_runtime_start(tmp_path: Path) -> None:
    _seed_base(tmp_path, safe_to_start_runtime=False)

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_NORMAL
    assert payload["runtime_start_allowed"] is False
    assert payload["submit_allowed"] is True
    assert payload["broker_mutation_allowed"] is True
    assert payload["tripped_limits"] == []


def test_live_money_true_hard_holds(tmp_path: Path) -> None:
    _seed_base(tmp_path, live_money_eligible=True)

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_HARD_HOLD
    assert payload["runtime_start_allowed"] is False
    assert payload["submit_allowed"] is False
    assert payload["broker_mutation_allowed"] is False
    assert any(row["limit_id"] == "live_money_eligible" for row in payload["tripped_limits"])


def test_duplicate_writer_hard_holds(tmp_path: Path) -> None:
    _seed_base(tmp_path, duplicate_writer=True)

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_HARD_HOLD
    assert payload["observe_only"] is True
    assert any(row["limit_id"] == "duplicate_runtime_writer" for row in payload["tripped_limits"])


def test_broker_position_guardian_hard_hold_blocks_submit(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/broker_position_guardian/latest_broker_position_guardian.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "BROKER_POSITION_GUARDIAN_HARD_HOLD",
            "hard_classifications": ["UNAUTHORIZED_REVERSE_EXPOSURE"],
            "operator_explanation": "Unauthorized reverse exposure detected.",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_HARD_HOLD
    assert payload["submit_allowed"] is False
    assert payload["broker_mutation_allowed"] is False
    assert payload["entry_mutation_allowed"] is False
    assert payload["managed_close_mutation_allowed"] is False
    assert any(row["limit_id"] == "broker_position_guardian_hard_hold" for row in payload["tripped_limits"])


def test_invalidated_guardian_hard_hold_is_diagnostic_only(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/broker_position_guardian/latest_broker_position_guardian.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "BROKER_POSITION_GUARDIAN_HARD_HOLD",
            "hard_classifications": ["BROKER_LIFECYCLE_POSITION_MISMATCH"],
            "current_truth_invalidation": {
                "current_scope_active": False,
                "diagnostic_only": True,
                "invalidated_by_current_truth": True,
                "invalidated_findings": [
                    {
                        "classification": "BROKER_LIFECYCLE_POSITION_MISMATCH",
                        "historical_only": True,
                        "diagnostic_only": True,
                        "current_scope_active": False,
                        "invalidated_by_current_truth": True,
                    }
                ],
            },
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_NORMAL
    assert payload["tripped_limits"] == []
    assert payload["submit_allowed"] is True


def test_broker_position_guardian_hard_hold_allows_exact_managed_close_only(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/broker_position_guardian/latest_broker_position_guardian.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "BROKER_POSITION_GUARDIAN_HARD_HOLD",
            "hard_classifications": ["UNAUTHORIZED_REVERSE_EXPOSURE", "BROKER_LIFECYCLE_POSITION_MISMATCH"],
            "managed_close_mutation_allowed": True,
            "managed_close_authority": {
                "classification": "BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING",
                "allowed": True,
                "risk_reducing_only": True,
                "reason_codes": [],
                "candidates": [
                    {
                        "trade_id": "trade-1",
                        "lifecycle_id": "lifecycle-1",
                        "local_symbol": "MNQM6",
                        "con_id": 770561201,
                        "action": "SELL",
                        "quantity": "1",
                    }
                ],
            },
            "operator_explanation": "Unauthorized reverse exposure detected.",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_HARD_HOLD
    assert payload["broker_mutation_allowed"] is False
    assert payload["entry_mutation_allowed"] is False
    assert payload["submit_allowed"] is False
    assert payload["managed_close_mutation_allowed"] is True
    assert payload["close_authority"]["classification"] == "MANAGED_CLOSE_MUTATION_ALLOWED"
    assert payload["close_authority"]["broad_flatten_allowed"] is False
    assert payload["close_authority"]["global_flatten_allowed"] is False
    assert payload["close_authority_reason_codes"] == []


def test_stale_runtime_with_exit_due_position_surfaces_risk_reducing_close_path(tmp_path: Path) -> None:
    _seed_base(tmp_path, safe_to_start_runtime=False)
    _write(
        tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "generated_at": NOW.isoformat(),
            "control_plane_snapshot_id": "snapshot-1",
            "runtime_authority_exposure_classification": "RUNTIME_AUTHORITY_STALE_WITH_BROKER_EXPOSURE",
            "runtime_authority_stale_with_broker_exposure": True,
            "submit_authority": False,
            "broker_mutation": False,
            "live_money_eligible": False,
        },
    )
    _seed_exit_due_mnq_authority(tmp_path)

    payload = _build(tmp_path)

    assert payload["submit_allowed"] is False
    assert payload["entry_mutation_allowed"] is False
    assert payload["broker_mutation_allowed"] is False
    assert payload["managed_close_mutation_allowed"] is True
    assert payload["risk_reducing_close_classification"] == (
        "RISK_REDUCING_CLOSE_ALLOWED_RUNTIME_STALE_WITH_BROKER_EXPOSURE"
    )
    assert payload["risk_reducing_close_allowed_runtime_stale"] is True
    assert payload["risk_reducing_close_candidate"]["action"] == "BUY"
    assert payload["risk_reducing_close_candidate"]["local_symbol"] == "MNQM6"


def test_unrelated_hard_hold_still_blocks_managed_close(tmp_path: Path) -> None:
    _seed_base(tmp_path, duplicate_writer=True)
    _write(
        tmp_path / "outputs/track_b_execution_core/broker_position_guardian/latest_broker_position_guardian.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "BROKER_POSITION_GUARDIAN_HARD_HOLD",
            "hard_classifications": ["UNAUTHORIZED_REVERSE_EXPOSURE"],
            "managed_close_authority": {
                "classification": "BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING",
                "allowed": True,
                "reason_codes": [],
            },
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_HARD_HOLD
    assert payload["managed_close_mutation_allowed"] is False
    assert "SAFE_STATE_LIMIT_BLOCKS_CLOSE:duplicate_runtime_writer" in payload["close_authority_reason_codes"]


def test_too_many_submits_hits_broker_mutation_limit(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/strategy_bridge/latest_strategy_bridge_submit_report.json",
        {
            "generated_at": NOW.isoformat(),
            "submits_per_symbol_window": {"MGC": 4},
            "broker_mutation_attempts_per_window": 4,
            "failed_broker_mutations_per_window": 0,
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_BROKER_MUTATION_LIMIT_HIT
    assert payload["recovery_only"] is True
    assert payload["submit_allowed"] is False
    assert any(row["limit_id"] == "submits_per_symbol_per_window" for row in payload["tripped_limits"])


def test_historical_managed_order_rows_do_not_count_toward_order_budget(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "NO_MANAGED_ORDERS",
            "managed_orders": [
                {
                    "broker_order_id": str(index),
                    "historical_only": True,
                    "diagnostic_only": True,
                    "current_scope_active": False,
                    "invalidated_by_current_truth": True,
                }
                for index in range(12)
            ],
            "summary": {"managed_order_count": 12, "working_close_order_count": 12},
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_NORMAL
    assert payload["limit_counters"]["orders_per_runtime_generation_id"] == 0
    assert payload["tripped_limits"] == []


def test_historical_managed_position_rows_do_not_count_toward_lane_limit(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "NO_MANAGED_POSITIONS",
            "managed_positions": [
                {
                    "strategy_lane_id": "lane-1",
                    "classification": "OPEN_MANAGED",
                    "historical_only": True,
                    "diagnostic_only": True,
                    "current_scope_active": False,
                    "invalidated_by_current_truth": True,
                },
                {
                    "strategy_lane_id": "lane-1",
                    "classification": "OPEN_MANAGED",
                    "historical_only": True,
                    "diagnostic_only": True,
                    "current_scope_active": False,
                    "invalidated_by_current_truth": True,
                },
            ],
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_NORMAL
    assert payload["limit_counters"]["max_managed_open_positions_per_strategy_lane_observed"] == 0
    assert payload["tripped_limits"] == []


def test_invalidated_open_order_duplicate_classification_is_diagnostic_only(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "DUPLICATE_CLOSE_ORDER",
            "canonical_refresh_scope": "GLOBAL_COMPLETE",
            "duplicate_close_order_groups": [{"duplicate_key": "DUM882026|MNQM6|SELL|1", "count": 2}],
            "current_truth_invalidation": {
                "current_scope_active": False,
                "diagnostic_only": True,
                "invalidated_by_current_truth": True,
            },
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_NORMAL
    assert payload["limit_counters"]["duplicate_intent_attempts"] == 0
    assert payload["tripped_limits"] == []


def test_active_open_order_duplicate_classification_still_blocks(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "DUPLICATE_CLOSE_ORDER",
            "canonical_refresh_scope": "GLOBAL_COMPLETE",
            "duplicate_close_order_groups": [{"duplicate_key": "DUM882026|MNQM6|SELL|1", "count": 2}],
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_DUPLICATE_INTENT_RISK


def test_invalidated_control_plane_duplicate_writer_is_diagnostic_only(tmp_path: Path) -> None:
    _seed_base(tmp_path, duplicate_writer=True)
    _write(
        tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "generated_at": NOW.isoformat(),
            "control_plane_snapshot_id": "snapshot-1",
            "safe_to_start_runtime": True,
            "agent_health_has_duplicate_writer": True,
            "duplicate_process_count": 2,
            "current_truth_invalidation": {
                "current_scope_active": False,
                "diagnostic_only": True,
                "invalidated_by_current_truth": True,
            },
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_NORMAL
    assert payload["tripped_limits"] == []


def test_lifecycle_disagreements_shift_to_observe_only(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/lifecycle_state/latest_lifecycle_state_summary.json",
        {
            "generated_at": NOW.isoformat(),
            "consecutive_reconciliation_disagreement_count": 3,
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_LIFECYCLE_DISAGREEMENT_LIMIT_HIT
    assert payload["observe_only"] is True
    assert payload["broker_mutation_allowed"] is False


def test_invalidated_lifecycle_disagreement_counter_is_diagnostic_only(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/lifecycle_state/latest_lifecycle_state_summary.json",
        {
            "generated_at": NOW.isoformat(),
            "consecutive_reconciliation_disagreement_count": 3,
            "current_truth_invalidation": {
                "current_scope_active": False,
                "diagnostic_only": True,
                "invalidated_by_current_truth": True,
            },
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_NORMAL
    assert payload["limit_counters"]["consecutive_lifecycle_reconciliation_disagreements"] == 0
    assert payload["tripped_limits"] == []


def test_position_limit_hit_hard_holds(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "OPEN_MANAGED_POSITIONS",
            "managed_positions": [
                {"strategy_lane_id": "lane-1", "classification": "OPEN_MANAGED"},
                {"strategy_lane_id": "lane-1", "classification": "OPEN_MANAGED"},
            ],
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_POSITION_LIMIT_HIT
    assert payload["runtime_start_allowed"] is False
    assert any(row["limit_id"] == "managed_open_positions_per_strategy_lane" for row in payload["tripped_limits"])


def test_safe_state_risk_policy_loads_from_explicit_config(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _seed_runtime_profile(tmp_path)
    _write(
        tmp_path / "config/track_b_safe_state_risk_policy.json",
        {
            "schema_version": "safe_state_risk_policy_v1",
            "profile_id": "mnq_mes_full_session_active_evidence",
            "limits": {
                "max_active_managed_exposures": 8,
                "max_open_broker_orders": 3,
                "max_broker_mutation_events_per_runtime_generation": 6,
            },
        },
    )

    payload = _build(tmp_path)

    policy = payload["safe_state_risk_policy"]
    assert policy["policy_source"] == "explicit_safe_state_risk_policy_config"
    assert policy["profile_id"] == "mnq_mes_full_session_active_evidence"
    assert policy["runtime_roster_lane_count"] == 2
    assert policy["runtime_roster_symbols"] == ["ES", "NQ"]
    assert policy["legacy_fallback_used"] is False
    assert policy["limits"]["max_active_managed_exposures"] == 8
    assert payload["policy_source"] == "explicit_safe_state_risk_policy_config"


def test_safe_state_risk_policy_marks_legacy_fallback(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _seed_runtime_profile(tmp_path)

    payload = _build(tmp_path)

    policy = payload["safe_state_risk_policy"]
    assert policy["policy_source"] == "legacy_config_fallback"
    assert policy["legacy_fallback_used"] is True
    assert policy["profile_id"] == "mnq_mes_full_session_active_evidence"
    assert policy["limits"]["max_active_managed_exposures"] == 4
    assert policy["limits"]["max_broker_mutation_events_per_runtime_generation"] == 4


def test_active_managed_exposure_limit_is_distinct_from_broker_mutation_limit(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "OPEN_MANAGED_POSITIONS",
            "managed_positions": [
                {"strategy_lane_id": f"lane-{index}", "symbol": "ES", "classification": "OPEN_MANAGED"}
                for index in range(5)
            ],
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_POSITION_LIMIT_HIT
    assert payload["submit_allowed"] is False
    assert payload["entry_mutation_allowed"] is False
    assert payload["limit_counters"]["active_managed_exposure_count"] == 5
    assert payload["limit_counters"]["broker_mutation_events_per_runtime_generation"] == 0
    assert any(row["limit_id"] == "active_managed_exposure_count" for row in payload["tripped_limits"])


def test_open_broker_order_limit_is_distinct_from_active_exposure_limit(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "OPEN_ORDERS_PRESENT",
            "order_states": [{"order_id": index, "classification": "ACTIVE"} for index in range(5)],
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_BROKER_MUTATION_LIMIT_HIT
    assert payload["limit_counters"]["open_broker_order_count"] == 5
    assert payload["limit_counters"]["active_managed_exposure_count"] == 0
    assert any(row["limit_id"] == "open_broker_order_count" for row in payload["tripped_limits"])


def test_deprecated_order_counter_remains_compatible(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "MANAGED_ORDER_ACTIVE",
            "managed_orders": [{"order_id": index, "classification": "ACTIVE"} for index in range(2)],
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["limit_counters"]["orders_per_runtime_generation_id"] == 2
    assert payload["limit_counters"]["broker_mutation_events_per_runtime_generation"] == 2
    assert payload["limit_counters"]["orders_per_runtime_generation_id_deprecated"] is True


def test_dashboard_projection_is_not_consumed(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write(
        tmp_path / "outputs/operator_dashboard/runtime/latest_runtime_safe_state_envelope.json",
        {"safe_state_classification": SAFE_STATE_HARD_HOLD, "projection_only": True},
    )

    payload = _build(tmp_path)

    assert payload["safe_state_classification"] == SAFE_STATE_NORMAL
    assert "operator_dashboard" not in json.dumps(payload["source_artifact_paths"])


def test_write_authority_artifact(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    config = TrackBRuntimeSafeStateEnvelopeConfig(repo_root=tmp_path)
    payload = build_track_b_runtime_safe_state_envelope(config=config, now=NOW)

    path = write_track_b_runtime_safe_state_envelope(config=config, payload=payload)

    assert path == tmp_path / "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json"
    written = json.loads(path.read_text(encoding="utf-8"))
    assert written["safe_state_classification"] == SAFE_STATE_NORMAL


def test_safe_state_refresh_writes_fresh_envelope(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    result = refresh_track_b_runtime_safe_state_envelope(
        config=TrackBRuntimeSafeStateEnvelopeConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert result.authority_path == tmp_path / "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json"
    assert result.payload["safe_state_classification"] == SAFE_STATE_NORMAL
    assert result.payload["generated_at"] == NOW.isoformat()
    assert result.summary["authority_path"] == str(result.authority_path)
    assert result.summary["read_only"] is True
    assert result.summary["paper_proof_invoked"] is False


def test_safe_state_refresh_replaces_stale_envelope(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    stale_path = tmp_path / "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json"
    _write(stale_path, {"generated_at": "2026-05-23T10:00:00+00:00", "classification": SAFE_STATE_NORMAL})

    refresh_track_b_runtime_safe_state_envelope(
        config=TrackBRuntimeSafeStateEnvelopeConfig(repo_root=tmp_path),
        now=NOW,
    )

    written = json.loads(stale_path.read_text(encoding="utf-8"))
    assert written["generated_at"] == NOW.isoformat()
    assert written["classification"] == SAFE_STATE_NORMAL


def test_safe_state_refresh_cli_writes_read_only_envelope(tmp_path: Path, capsys) -> None:
    _seed_base(tmp_path)

    exit_code = safe_state_refresh_main(["--repo-root", str(tmp_path), "--json"])

    assert exit_code == 0
    captured = json.loads(capsys.readouterr().out)
    assert captured["safe_state_classification"] == SAFE_STATE_NORMAL
    assert captured["read_only"] is True
    assert captured["broker_mutation_execution_performed"] is False
    written = json.loads(
        (tmp_path / "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json").read_text(
            encoding="utf-8"
        )
    )
    assert written["classification"] == SAFE_STATE_NORMAL


def test_certification_safe_state_fresh_after_refresh_path(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    refresh_track_b_runtime_safe_state_envelope(
        config=TrackBRuntimeSafeStateEnvelopeConfig(repo_root=tmp_path),
        now=NOW,
    )
    _seed_operational_certification_dependencies(tmp_path)

    result = build_operational_certification(repo_root=tmp_path, now=NOW, write=False)

    safe_domain = result.report["domains"]["safe_state_guardian"]
    assert safe_domain["status"] == "PASS"
    assert any(check["code"] == "safe_state_fresh" and check["status"] == "PASS" for check in safe_domain["checks"])


def test_safe_state_refresh_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_runtime_safe_state_envelope.py"),
        Path("src/mgc_v05l/app/track_b_safe_state_refresh.py"),
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


def _build(root: Path) -> dict:
    return build_track_b_runtime_safe_state_envelope(
        config=TrackBRuntimeSafeStateEnvelopeConfig(repo_root=root),
        now=NOW,
    )


def _seed_base(
    root: Path,
    *,
    live_money_eligible: bool = False,
    duplicate_writer: bool = False,
    safe_to_start_runtime: bool = True,
) -> None:
    _write(
        root / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "generated_at": NOW.isoformat(),
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
            "safe_to_start_runtime": safe_to_start_runtime,
            "runtime_resume_proposed_next_runtime_generation_id": "runtime-generation-next",
            "agent_health_has_duplicate_writer": duplicate_writer,
            "duplicate_process_count": 1 if duplicate_writer else 0,
            "live_money_eligible": live_money_eligible,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/runtime_resume/latest_runtime_resume_semantics.json",
        {
            "generated_at": NOW.isoformat(),
            "proposed_next_runtime_generation_id": "runtime-generation-next",
            "previous_runtime_generation_id": "runtime-generation-previous",
            "live_money_eligible": live_money_eligible,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/recovery_budget/latest_recovery_budget_ledger.json",
        {"generated_at": NOW.isoformat(), "classification": "RECOVERY_BUDGET_AVAILABLE"},
    )
    _write(
        root / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_recovery_attempt_history.json",
        {"generated_at": NOW.isoformat(), "recent_attempts": []},
    )
    _write(
        root / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "NO_MANAGED_ORDERS",
            "managed_orders": [],
            "summary": {"managed_order_count": 0},
            "live_money_eligible": live_money_eligible,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "NO_OPEN_ORDERS",
            "order_states": [],
            "live_money_eligible": live_money_eligible,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/position_truth/latest_position_truth.json",
        {"generated_at": NOW.isoformat(), "classification": "CLEAN_FLAT_READY", "live_money_eligible": live_money_eligible},
    )
    _write(
        root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "NO_MANAGED_POSITIONS",
            "managed_positions": [],
            "live_money_eligible": live_money_eligible,
        },
    )
    _write(
        root / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {"generated_at": NOW.isoformat(), "classification": "TRACK_B_PAPER_BROKER_RECONCILED", "live_money_eligible": live_money_eligible},
    )


def _seed_runtime_profile(root: Path) -> None:
    _write(
        root / "outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json",
        {
            "generated_at": NOW.isoformat(),
            "profile": "mnq_mes_full_session_active_evidence",
            "active_lane_ids": ["lane-nq", "lane-es"],
            "lanes": [
                {"lane_id": "lane-nq", "symbol": "NQ", "max_concurrent_entries": 1},
                {"lane_id": "lane-es", "bridge_execution_target": {"symbol": "ES"}, "max_concurrent_entries": 1},
            ],
        },
    )
    _write(
        root
        / "outputs/probationary_pattern_engine/paper_session/runtime/paper_stack_mnq_mes_full_session_active_evidence_guarded_roster.json",
        {
            "schema_version": "track_b_guarded_paper_roster_v1",
            "profile": "mnq_mes_full_session_active_evidence",
            "max_quantity_per_strategy": 1,
        },
    )


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_operational_certification_dependencies(root: Path) -> None:
    fresh = NOW.isoformat()
    _write(
        root / "outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_truth.json",
        {
            "generated_at": fresh,
            "runtime_started_at": fresh,
            "producer_pid": os.getpid(),
            "source_commit": "test-commit",
            "lane_count": 71,
        },
    )
    _write(
        root / "outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper_detached_child_status.json",
        {"generated_at": fresh, "events": [{"state": "TRADING_LOOP_ENTERED"}]},
    )
    _write(root / "outputs/probationary_pattern_engine/paper_session/live_timing_summary_latest.json", {"generated_at": fresh})
    _write(
        root / "outputs/track_b_execution_core/managed_exit_service/latest_managed_exit_service_status.json",
        {"generated_at": fresh, "pid": os.getpid(), "classification": "NO_ELIGIBLE_EXITS", "exit_due_count": 0, "eligible_count": 0},
    )
    _write(
        root / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json",
        {
            "generated_at": fresh,
            "dmc_metadata": {"artifact_family": "latest_broker_truth_lease"},
            "current_truth_invalidation": {},
        },
    )
    _write(
        root / "outputs/track_b_execution_core/broker_position_guardian/latest_broker_position_guardian.json",
        {
            "generated_at": fresh,
            "classification": "BROKER_POSITION_GUARDIAN_READY",
            "hard_classifications": [],
            "dmc_metadata": {"artifact_family": "latest_broker_position_guardian"},
            "current_truth_invalidation": {},
        },
    )
    _write(root / "outputs/probationary_pattern_engine/paper_session/operator_status.json", {"generated_at": fresh})
    _write(root / "outputs/reports/ibkr_runtime_route_dispatch/test/ibkr_paper_strategy_bridge_report.json", {"generated_at": fresh})
    _write(root / "outputs/reports/ibkr_strategy_governance/strategy_probation_dashboard.json", {"generated_at": fresh})
    _write(
        root / "outputs/track_b_execution_core/operations_maintenance/phase_o1_hot_path_storage_audit/operations_maintenance_hot_path_audit.json",
        {"sizes": {"outputs": "1 MiB"}},
    )
    _write(
        root / "outputs/track_b_execution_core/operations_maintenance/o5a_archive_retention_contract/archive_retention_contract.json",
        {"artifact_families": []},
    )


def _seed_exit_due_mnq_authority(root: Path) -> None:
    position = {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "trade_id": "trade-1",
        "lifecycle_id": "lifecycle-1",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
        "side": "SHORT",
        "broker_position": {
            "account_id": "DUM882026",
            "symbol": "MNQ",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "quantity": "-1",
        },
        "lifecycle_position": {
            "entry_exec_id": "0000e1a7.test.01.01",
            "entry_perm_id": 1421894440,
            "entry_broker_identity": {"exec_id": "0000e1a7.test.01.01", "perm_id": 1421894440},
        },
    }
    _write(
        root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [position],
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "managed_orders": [
                {
                    "classification": "POSITION_WITHOUT_CLOSE_ORDER",
                    "trade_id": "trade-1",
                    "lifecycle_id": "lifecycle-1",
                    "action": "BUY",
                    "quantity": "1",
                    "working": False,
                }
            ],
            "summary": {"managed_order_count": 1},
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/broker_position_guardian/latest_broker_position_guardian.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "BROKER_POSITION_GUARDIAN_READY",
            "managed_close_authority": {
                "classification": "BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING",
                "allowed": True,
                "reason_codes": [],
                "candidates": [
                    {
                        "trade_id": "trade-1",
                        "lifecycle_id": "lifecycle-1",
                        "account_id": "DUM882026",
                        "symbol": "MNQ",
                        "local_symbol": "MNQM6",
                        "con_id": 770561201,
                        "action": "BUY",
                        "quantity": "1",
                    }
                ],
            },
            "live_money_eligible": False,
        },
    )
