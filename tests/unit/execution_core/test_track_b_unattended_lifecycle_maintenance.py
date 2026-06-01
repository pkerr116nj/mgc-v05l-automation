from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core import track_b_unattended_lifecycle_maintenance as module


def _now() -> datetime:
    return datetime(2026, 5, 25, 18, 10, tzinfo=timezone.utc)


def _config(tmp_path: Path, **overrides):
    values = {
        "repo_root": tmp_path,
        "output_path": tmp_path / "report.json",
        "readiness_recovery_handoff_enabled": False,
    }
    values.update(overrides)
    return module.TrackBUnattendedLifecycleMaintenanceConfig(**values)


@dataclass(frozen=True)
class _FakeAdoptionResult:
    classification: str = "TRACK_B_PAPER_LIFECYCLE_ADOPTION_APPLIED"
    audit_path: Path = Path("adopt.json")


def _patch_truth(monkeypatch, *, managed_position: dict | None = None, guardian: str = "BROKER_POSITION_GUARDIAN_READY"):
    managed_positions = [managed_position] if managed_position else []
    monkeypatch.setattr(
        module,
        "build_track_b_position_truth",
        lambda **_kwargs: {"summary": {"overall_classification": "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING"}},
    )
    monkeypatch.setattr(module, "write_track_b_position_truth", lambda **_kwargs: Path("position_truth.json"))
    monkeypatch.setattr(module, "build_track_b_open_order_truth", lambda **_kwargs: {"classification": "BROKER_POSITION_WITHOUT_CLOSE_ORDER"})
    monkeypatch.setattr(module, "write_track_b_open_order_truth", lambda **_kwargs: Path("open_order_truth.json"))
    monkeypatch.setattr(
        module,
        "build_track_b_managed_position_registry",
        lambda **_kwargs: {"classification": "OPEN_MANAGED_MATCHED", "managed_positions": managed_positions},
    )
    monkeypatch.setattr(module, "write_track_b_managed_position_registry", lambda **_kwargs: Path("managed_positions.json"))
    monkeypatch.setattr(
        module,
        "build_track_b_managed_order_registry",
        lambda **_kwargs: {"classification": "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING"},
    )
    monkeypatch.setattr(module, "write_track_b_managed_order_registry", lambda **_kwargs: Path("managed_orders.json"))
    monkeypatch.setattr(module, "build_track_b_broker_position_guardian", lambda **_kwargs: {"classification": guardian})
    monkeypatch.setattr(module, "write_track_b_broker_position_guardian", lambda **_kwargs: Path("guardian.json"))


def _patch_clean_truth(monkeypatch):
    monkeypatch.setattr(
        module,
        "build_track_b_position_truth",
        lambda **_kwargs: {"classification": "CLEAN_FLAT_READY", "summary": {"overall_classification": "CLEAN_FLAT_READY"}},
    )
    monkeypatch.setattr(module, "write_track_b_position_truth", lambda **_kwargs: Path("position_truth.json"))
    monkeypatch.setattr(module, "build_track_b_open_order_truth", lambda **_kwargs: {"classification": "NO_OPEN_ORDERS"})
    monkeypatch.setattr(module, "write_track_b_open_order_truth", lambda **_kwargs: Path("open_order_truth.json"))
    monkeypatch.setattr(
        module,
        "build_track_b_managed_position_registry",
        lambda **_kwargs: {"classification": "NO_MANAGED_POSITIONS", "managed_positions": []},
    )
    monkeypatch.setattr(module, "write_track_b_managed_position_registry", lambda **_kwargs: Path("managed_positions.json"))
    monkeypatch.setattr(
        module,
        "build_track_b_managed_order_registry",
        lambda **_kwargs: {"classification": "NO_MANAGED_ORDERS", "managed_orders": []},
    )
    monkeypatch.setattr(module, "write_track_b_managed_order_registry", lambda **_kwargs: Path("managed_orders.json"))
    monkeypatch.setattr(
        module,
        "build_track_b_broker_position_guardian",
        lambda **_kwargs: {"classification": "BROKER_POSITION_GUARDIAN_READY"},
    )
    monkeypatch.setattr(module, "write_track_b_broker_position_guardian", lambda **_kwargs: Path("guardian.json"))


def test_unattended_maintenance_auto_adopts_known_broker_backed_fill(monkeypatch, tmp_path: Path) -> None:
    adoption_calls = []

    monkeypatch.setattr(
        module,
        "reconcile_track_b_paper_broker_truth",
        lambda **_kwargs: {
            "classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT",
            "blockers": [
                {
                    "broker_backed_entry_adoption": {
                        "account_id": "DUM882026",
                        "broker_order_id": "39",
                        "client_id": 11940,
                        "contract": {"symbol": "MGC", "local_symbol": "MGCM6", "expiry": "20260626"},
                        "lane_id": "mgc_1x_all_lanes__asia_early_long",
                        "qty": 1,
                    }
                }
            ],
        },
    )
    monkeypatch.setattr(
        module,
        "run_track_b_paper_lifecycle_adoption",
        lambda **kwargs: adoption_calls.append(kwargs["config"]) or _FakeAdoptionResult(),
    )
    _patch_truth(monkeypatch)

    report = module.run_unattended_lifecycle_maintenance(
        config=_config(tmp_path),
        now=_now(),
    )

    assert adoption_calls
    assert adoption_calls[0].local_symbol == "MGCM6"
    assert report["adoption_results"][0]["broker_mutated_by_adoption"] is False
    assert report["dashboard_projection_consumed"] is False
    assert report["paper_proof_invoked"] is False


def test_unattended_maintenance_reports_active_hold_when_exit_not_due(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(module, "reconcile_track_b_paper_broker_truth", lambda **_kwargs: {"classification": "TRACK_B_PAPER_BROKER_RECONCILED"})
    _patch_truth(
        monkeypatch,
        managed_position={
            "lifecycle_id": "life-1",
            "exit_due": False,
            "managed_exit_policy_id": "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1",
        },
    )

    report = module.run_unattended_lifecycle_maintenance(
        config=_config(tmp_path),
        now=_now(),
    )

    assert report["classification"] == module.UNATTENDED_LIFECYCLE_MAINTENANCE_ACTIVE_HOLD
    assert report["managed_exit_plans"][0]["classification"] == "MANAGED_EXIT_NOT_DUE_ACTIVE_HOLD"


def test_unattended_maintenance_routes_exit_due_to_managed_exit_boundary(monkeypatch, tmp_path: Path) -> None:
    exit_calls = []
    monkeypatch.setattr(module, "reconcile_track_b_paper_broker_truth", lambda **_kwargs: {"classification": "TRACK_B_PAPER_BROKER_RECONCILED"})
    _patch_truth(
        monkeypatch,
        managed_position={
            "account_id": "DUM882026",
            "symbol": "MGC",
            "local_symbol": "MGCM6",
            "contract_key": "MGC-202606",
            "con_id": 712565978,
            "side": "LONG",
            "quantity": "1",
            "strategy_id": "gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__asia_early_long",
            "lifecycle_id": "life-1",
            "exit_due": True,
            "broker_position": {"expiry": "20260626"},
        },
    )
    monkeypatch.setattr(
        module,
        "run_track_b_managed_exit_attach",
        lambda **kwargs: exit_calls.append(kwargs["config"])
        or {
            "classification": "MANAGED_EXIT_DUE_READY_FOR_APPLY",
            "plan_classification": "MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE",
            "apply_boundary_classification": "MANAGED_EXIT_APPLY_DISABLED",
            "broker_state_mutated": False,
            "submit_attempted": False,
            "target_identity": {"local_symbol": "MGCM6"},
        },
    )

    report = module.run_unattended_lifecycle_maintenance(
        config=_config(tmp_path),
        now=_now(),
    )

    assert exit_calls
    assert exit_calls[0].local_symbol == "MGCM6"
    assert exit_calls[0].apply is False
    assert report["classification"] == module.UNATTENDED_LIFECYCLE_MAINTENANCE_EXIT_DUE


def test_unattended_maintenance_rebuilds_current_order_plan_and_routes_modify(monkeypatch, tmp_path: Path) -> None:
    modify_calls = []
    managed_order = {
        "account_id": "DUM882026",
        "symbol": "MGC",
        "contract": "MGCM6",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "action": "SELL",
        "quantity": "1",
        "broker_order_id": 45,
        "perm_id": 1306860537,
        "classification": "WORKING_CLOSE_ORDER",
        "lifecycle_id": "life-1",
        "strategy_id": "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        "source_order": {"account_id": "DUM882026", "symbol": "MGC", "local_symbol": "MGCM6"},
    }
    current_plan = {
        "classification": module.MODIFY_IN_PLACE_ELIGIBLE,
        "broker_order_id": 45,
        "perm_id": 1306860537,
        "symbol": "MGC",
        "contract": "MGCM6",
        "con_id": 712565978,
        "action": "SELL",
        "quantity": "1",
        "limit_price": "4528.7",
        "lifecycle_id": "life-1",
        "identity": {
            "account_id": "DUM882026",
            "broker_order_id": 45,
            "perm_id": 1306860537,
            "contract": "MGCM6",
            "con_id": 712565978,
            "action": "SELL",
            "quantity": "1",
        },
        "market_reference": {"reference_price": "4525.2"},
    }

    monkeypatch.setattr(module, "reconcile_track_b_paper_broker_truth", lambda **_kwargs: {"classification": "TRACK_B_PAPER_BROKER_RECONCILED"})
    _patch_truth(
        monkeypatch,
        managed_position={"lifecycle_id": "life-1", "exit_due": False, "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"},
    )
    monkeypatch.setattr(
        module,
        "build_track_b_managed_order_registry",
        lambda **_kwargs: {"classification": "WORKING_CLOSE_ORDER", "managed_orders": [managed_order]},
    )
    monkeypatch.setattr(
        module,
        "build_track_b_order_adjustment_plan",
        lambda **_kwargs: {
            "classification": module.MODIFY_IN_PLACE_ELIGIBLE,
            "summary": {"plan_count": 1, "modify_in_place_eligible_count": 1},
            "plans": [current_plan],
        },
    )
    monkeypatch.setattr(module, "write_track_b_order_adjustment_plan", lambda **_kwargs: Path("order_adjustment.json"))
    monkeypatch.setattr(
        module,
        "run_track_b_managed_order_modify_in_place",
        lambda **kwargs: modify_calls.append(kwargs["config"])
        or {
            "classification": "MODIFY_IN_PLACE_DRY_RUN_READY",
            "detail": "ready",
            "broker_mutation_attempted": False,
            "broker_mutation_performed": False,
            "new_order_created": False,
            "artifact_path": "modify.json",
        },
    )

    report = module.run_unattended_lifecycle_maintenance(
        config=_config(tmp_path),
        now=_now(),
    )

    assert modify_calls
    assert modify_calls[0].broker_order_id == "45"
    assert modify_calls[0].contract == "MGCM6"
    assert modify_calls[0].current_known_limit == "4528.7"
    assert modify_calls[0].new_limit == "4524.4"
    assert modify_calls[0].apply is False
    assert report["classification"] == module.UNATTENDED_LIFECYCLE_MAINTENANCE_CLOSE_ORDER_MANAGED
    assert report["root_cause_controls"]["fresh_order_adjustment_plan_rebuilt_each_pass"] is True
    assert report["root_cause_controls"]["working_close_orders_managed_by_modify_in_place"] is True


def test_unattended_maintenance_uses_reprice_policy_from_fresh_order_plan(monkeypatch, tmp_path: Path) -> None:
    modify_calls = []
    managed_order = {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "contract": "MNQM6",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "action": "SELL",
        "quantity": "1",
        "broker_order_id": 59,
        "perm_id": 1955790754,
        "classification": "WORKING_CLOSE_ORDER",
        "lifecycle_id": "life-mnq",
        "source_order": {"account_id": "DUM882026", "symbol": "MNQ", "local_symbol": "MNQM6"},
    }
    current_plan = {
        "classification": module.MODIFY_IN_PLACE_ELIGIBLE,
        "broker_order_id": 59,
        "perm_id": 1955790754,
        "symbol": "MNQ",
        "contract": "MNQM6",
        "con_id": 770561201,
        "action": "SELL",
        "quantity": "1",
        "limit_price": "30582.25",
        "lifecycle_id": "life-mnq",
        "identity": {
            "account_id": "DUM882026",
            "broker_order_id": 59,
            "perm_id": 1955790754,
            "contract": "MNQM6",
            "con_id": 770561201,
            "action": "SELL",
            "quantity": "1",
        },
        "managed_close_reprice_policy": {
            "classification": "MANAGED_CLOSE_PRICED",
            "limit_price": "30523",
            "marketable_limit_offset_ticks": 8.0,
            "max_slippage_ticks": 16.0,
        },
        "market_reference": {"reference_price": "30525", "reference_age_seconds": 5.0},
    }

    monkeypatch.setattr(module, "reconcile_track_b_paper_broker_truth", lambda **_kwargs: {"classification": "TRACK_B_PAPER_BROKER_RECONCILED"})
    _patch_truth(
        monkeypatch,
        managed_position={"lifecycle_id": "life-mnq", "exit_due": False, "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"},
    )
    monkeypatch.setattr(
        module,
        "build_track_b_managed_order_registry",
        lambda **_kwargs: {"classification": "WORKING_CLOSE_ORDER", "managed_orders": [managed_order]},
    )
    monkeypatch.setattr(
        module,
        "build_track_b_order_adjustment_plan",
        lambda **_kwargs: {
            "classification": module.MODIFY_IN_PLACE_ELIGIBLE,
            "summary": {"plan_count": 1, "modify_in_place_eligible_count": 1},
            "plans": [current_plan],
        },
    )
    monkeypatch.setattr(module, "write_track_b_order_adjustment_plan", lambda **_kwargs: Path("order_adjustment.json"))
    monkeypatch.setattr(
        module,
        "run_track_b_managed_order_modify_in_place",
        lambda **kwargs: modify_calls.append(kwargs["config"])
        or {
            "classification": "MODIFY_IN_PLACE_DRY_RUN_READY",
            "broker_mutation_attempted": False,
            "broker_mutation_performed": False,
            "new_order_created": False,
            "artifact_path": "modify.json",
        },
    )

    report = module.run_unattended_lifecycle_maintenance(config=_config(tmp_path), now=_now())

    assert modify_calls
    assert modify_calls[0].broker_order_id == "59"
    assert modify_calls[0].current_known_limit == "30582.25"
    assert modify_calls[0].new_limit == "30523"
    assert modify_calls[0].apply is False
    assert report["classification"] == module.UNATTENDED_LIFECYCLE_MAINTENANCE_CLOSE_ORDER_MANAGED


def test_unattended_maintenance_persists_known_close_when_broker_flat(monkeypatch, tmp_path: Path) -> None:
    cleanup_calls = []
    plan = {
        "classification": module.BROKER_FLAT_NO_REPLACE,
        "broker_order_id": 45,
        "perm_id": 1306860537,
        "symbol": "MGC",
        "contract": "MGCM6",
        "con_id": 712565978,
        "lifecycle_id": "life-1",
        "source_order": {"client_id": 17086},
    }

    monkeypatch.setattr(module, "reconcile_track_b_paper_broker_truth", lambda **_kwargs: {"classification": "TRACK_B_PAPER_BROKER_RECONCILED"})
    _patch_truth(monkeypatch)
    monkeypatch.setattr(
        module,
        "build_track_b_managed_order_registry",
        lambda **_kwargs: {"classification": "BROKER_FLAT_WITH_WORKING_CLOSE", "managed_orders": []},
    )
    monkeypatch.setattr(
        module,
        "build_track_b_order_adjustment_plan",
        lambda **_kwargs: {
            "classification": module.BROKER_FLAT_NO_REPLACE,
            "summary": {"plan_count": 1, "modify_in_place_eligible_count": 0},
            "plans": [plan],
        },
    )
    monkeypatch.setattr(module, "write_track_b_order_adjustment_plan", lambda **_kwargs: Path("order_adjustment.json"))
    monkeypatch.setattr(
        module,
        "resolve_known_managed_exit_order_disappearance",
        lambda **kwargs: cleanup_calls.append(kwargs["config"])
        or {
            "classification": "KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP",
            "detail": "persisted",
            "lifecycle_close": {"persisted": True},
            "artifact_path": "resolution.json",
        },
    )

    report = module.run_unattended_lifecycle_maintenance(
        config=_config(tmp_path),
        now=_now(),
    )

    assert cleanup_calls
    assert cleanup_calls[0].lifecycle_id == "life-1"
    assert cleanup_calls[0].broker_order_id == "45"
    assert cleanup_calls[0].apply is True
    assert report["classification"] == module.UNATTENDED_LIFECYCLE_MAINTENANCE_READY
    assert report["broker_flat_close_cleanup"][0]["lifecycle_close"]["persisted"] is True


def test_unattended_maintenance_cleans_lifecycle_without_broker_from_known_close(monkeypatch, tmp_path: Path) -> None:
    cleanup_calls = []
    lifecycle_id = "life-1"
    attach_path = tmp_path / "outputs/track_b_execution_core/managed_exit_attach/latest_managed_exit_attach_plan.json"
    attach_path.parent.mkdir(parents=True)
    attach_path.write_text(
        """
{
  "close_intent_preview": {
    "lifecycle_id": "life-1",
    "local_symbol": "MGCM6",
    "con_id": 712565978,
    "symbol": "MGC"
  },
  "apply_result": {
    "close_intent": {
      "lifecycle_id": "life-1",
      "local_symbol": "MGCM6",
      "con_id": 712565978,
      "symbol": "MGC"
    },
    "close_submit_attempt": {
      "broker_order_id": "45",
      "client_id": 17086,
      "perm_id": 1306860537
    }
  }
}
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(module, "reconcile_track_b_paper_broker_truth", lambda **_kwargs: {"classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT"})
    _patch_truth(
        monkeypatch,
        managed_position={
            "classification": "LIFECYCLE_WITHOUT_BROKER",
            "lifecycle_id": lifecycle_id,
            "symbol": "MGC",
            "local_symbol": "MGCM6",
            "con_id": 712565978,
        },
        guardian="BROKER_POSITION_GUARDIAN_READY",
    )
    monkeypatch.setattr(
        module,
        "build_track_b_order_adjustment_plan",
        lambda **_kwargs: {
            "classification": "NO_ACTION_NEEDED",
            "summary": {"plan_count": 0, "modify_in_place_eligible_count": 0},
            "plans": [],
        },
    )
    monkeypatch.setattr(module, "write_track_b_order_adjustment_plan", lambda **_kwargs: Path("order_adjustment.json"))
    monkeypatch.setattr(
        module,
        "resolve_known_managed_exit_order_disappearance",
        lambda **kwargs: cleanup_calls.append(kwargs["config"])
        or {
            "classification": "KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP",
            "detail": "persisted",
            "lifecycle_close": {"persisted": True},
            "artifact_path": "resolution.json",
        },
    )

    report = module.run_unattended_lifecycle_maintenance(
        config=_config(tmp_path),
        now=_now(),
    )

    assert cleanup_calls
    assert cleanup_calls[0].lifecycle_id == lifecycle_id
    assert cleanup_calls[0].broker_order_id == "45"
    assert cleanup_calls[0].apply is True
    assert report["classification"] == module.UNATTENDED_LIFECYCLE_MAINTENANCE_READY
    assert report["lifecycle_without_broker_cleanup"][0]["lifecycle_close"]["persisted"] is True
    assert report["root_cause_controls"]["lifecycle_without_broker_flat_cleanup_enabled"] is True


def test_unattended_maintenance_clean_truth_refreshes_readiness_and_control_plane(monkeypatch, tmp_path: Path) -> None:
    proof_calls = []
    control_plane_calls = []

    monkeypatch.setattr(
        module,
        "reconcile_track_b_paper_broker_truth",
        lambda **_kwargs: {"classification": "TRACK_B_PAPER_BROKER_RECONCILED"},
    )
    _patch_clean_truth(monkeypatch)
    monkeypatch.setattr(
        module,
        "build_track_b_paper_proof_readiness",
        lambda **kwargs: proof_calls.append(kwargs["config"])
        or {
            "classification": module.READY_FOR_PROOF,
            "primary_blocker": None,
        },
    )
    monkeypatch.setattr(module, "write_track_b_paper_proof_readiness", lambda **_kwargs: tmp_path / "proof.json")
    monkeypatch.setattr(
        module,
        "build_track_b_control_plane_snapshot",
        lambda **kwargs: control_plane_calls.append(kwargs["config"])
        or {
            "classification": module.CONTROL_PLANE_SNAPSHOT_READY,
            "shared_truth_coherence_status": "COHERENT",
            "safe_state_classification": "SAFE_STATE_NORMAL",
            "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "proof_window_status": "ready",
        },
    )
    monkeypatch.setattr(module, "write_track_b_control_plane_snapshot", lambda **_kwargs: tmp_path / "control_plane.json")

    report = module.run_unattended_lifecycle_maintenance(
        config=_config(tmp_path, readiness_recovery_handoff_enabled=True),
        now=_now(),
    )

    assert proof_calls
    assert control_plane_calls
    assert report["classification"] == module.UNATTENDED_LIFECYCLE_MAINTENANCE_READY
    assert report["readiness_recovery_handoff"]["classification"] == "READINESS_RECOVERY_HANDOFF_CONTROL_PLANE_READY"
    assert report["root_cause_controls"]["control_plane_rebuilt_after_readiness_refresh"] is True


def test_unattended_maintenance_keeps_real_stale_phase1_as_readiness_blocker(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        module,
        "reconcile_track_b_paper_broker_truth",
        lambda **_kwargs: {"classification": "TRACK_B_PAPER_BROKER_RECONCILED"},
    )
    _patch_clean_truth(monkeypatch)
    monkeypatch.setattr(
        module,
        "build_track_b_paper_proof_readiness",
        lambda **_kwargs: {
            "classification": "PHASE1_DATA_UNHEALTHY",
            "primary_blocker": {"code": "phase1_mgc_5m_not_ready"},
        },
    )
    monkeypatch.setattr(module, "write_track_b_paper_proof_readiness", lambda **_kwargs: tmp_path / "proof.json")
    monkeypatch.setattr(
        module,
        "build_track_b_control_plane_snapshot",
        lambda **_kwargs: {
            "classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
            "shared_truth_coherence_status": "COHERENT",
            "safe_state_classification": "SAFE_STATE_NORMAL",
            "runtime_supervisor_classification": "SUPERVISOR_SHARED_TRUTH_STALE",
            "proof_window_status": "data_stale",
        },
    )
    monkeypatch.setattr(module, "write_track_b_control_plane_snapshot", lambda **_kwargs: tmp_path / "control_plane.json")

    report = module.run_unattended_lifecycle_maintenance(
        config=_config(tmp_path, readiness_recovery_handoff_enabled=True),
        now=_now(),
    )

    assert report["readiness_recovery_handoff"]["classification"] == "READINESS_RECOVERY_HANDOFF_BLOCKED_PROOF_READINESS"
    assert report["readiness_recovery_handoff"]["proof_readiness_primary_blocker"]["code"] == "phase1_mgc_5m_not_ready"
