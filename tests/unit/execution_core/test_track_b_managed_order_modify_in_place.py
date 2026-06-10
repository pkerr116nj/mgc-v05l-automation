from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.execution_core.track_b_managed_order_modify_in_place import (
    IbkrPaperManagedOrderModifyAdapter,
    MODIFY_IN_PLACE_APPLIED,
    MODIFY_IN_PLACE_BLOCKED_DUPLICATE_RISK,
    MODIFY_IN_PLACE_BLOCKED_NOT_WORKING_ORDER,
    MODIFY_IN_PLACE_BLOCKED_OPERATOR_AUTH_REQUIRED,
    MODIFY_IN_PLACE_BLOCKED_ORDER_IDENTITY_MISMATCH,
    MODIFY_IN_PLACE_BLOCKED_SHARED_TRUTH,
    MODIFY_IN_PLACE_BLOCKED_SUSPICIOUS_ORDER,
    MODIFY_IN_PLACE_DRY_RUN_READY,
    MODIFY_IN_PLACE_VERIFICATION_FAILED,
    ManagedOrderModifyInPlaceConfig,
    main,
    run_track_b_managed_order_modify_in_place,
)
from mgc_v05l.execution_core.track_b_paper_autonomous_recovery_planner import (
    PLAN_MANAGED_ORDER_MODIFY,
    PLAN_TARGETED_CANCEL_REPLACE,
)
from mgc_v05l.execution_core.track_b_pre_action_snapshot_validator import PRE_ACTION_SNAPSHOT_VALID


NOW = datetime(2026, 5, 23, 15, 5, tzinfo=UTC)


def test_dry_run_ready_for_clean_working_managed_close_order(tmp_path: Path) -> None:
    _seed_authorities(tmp_path)

    report = _run(tmp_path)

    assert report["classification"] == MODIFY_IN_PLACE_DRY_RUN_READY
    assert report["broker_mutation_attempted"] is False
    assert report["new_order_created"] is False
    assert report["pre_action_snapshot_would_block_apply"] is True
    assert report["shared_truth_evidence"]["dashboard_projection_consumed"] is False


def test_actual_modify_blocked_without_operator_flag(tmp_path: Path) -> None:
    _seed_authorities(tmp_path)

    report = _run(tmp_path, apply=True, operator_authorized_modify=False)

    assert report["classification"] == MODIFY_IN_PLACE_BLOCKED_OPERATOR_AUTH_REQUIRED
    assert report["broker_mutation_attempted"] is False


def test_actual_modify_blocked_on_identity_mismatch(tmp_path: Path) -> None:
    _seed_authorities(tmp_path)

    report = _run(tmp_path, current_known_limit="29550.00")

    assert report["classification"] == MODIFY_IN_PLACE_BLOCKED_ORDER_IDENTITY_MISMATCH
    assert "Limit price mismatch" in report["detail"]


def test_suspicious_sentinel_order_blocks_modify(tmp_path: Path) -> None:
    _seed_authorities(
        tmp_path,
        open_order_classification="SUSPICIOUS_ORDER_STATE",
        managed_order_classification="CLOSE_ORDER_SUSPICIOUS",
        planner_classification="REVIEW_REQUIRED_SUSPICIOUS_STATE",
        suspicious_reasons=["sentinel_filled_quantity", "missing_remaining_quantity"],
        filled_quantity="1.7976931348623157e+308",
        remaining_quantity=None,
    )

    report = _run(tmp_path)

    assert report["classification"] == MODIFY_IN_PLACE_BLOCKED_SUSPICIOUS_ORDER
    assert "SUSPICIOUS" in report["detail"]


def test_known_working_close_tolerates_ibkr_sentinel_status_gap(tmp_path: Path) -> None:
    _seed_authorities(
        tmp_path,
        open_order_classification="OPEN_CLOSE_ORDER_WORKING",
        managed_order_classification="WORKING_CLOSE_ORDER",
        planner_classification="MODIFY_IN_PLACE_ELIGIBLE",
        filled_quantity="1.7976931348623157e+308",
        remaining_quantity=None,
    )

    report = _run(tmp_path)

    assert report["classification"] == MODIFY_IN_PLACE_DRY_RUN_READY
    assert report["broker_mutation_attempted"] is False


def test_stale_known_close_can_use_modify_in_place_boundary(tmp_path: Path) -> None:
    _seed_authorities(
        tmp_path,
        open_order_classification="CLOSE_ORDER_STALE",
        managed_order_classification="CLOSE_ORDER_CANCEL_REPLACE_REQUIRED",
        planner_classification="MODIFY_IN_PLACE_ELIGIBLE",
        filled_quantity="1.7976931348623157e+308",
        remaining_quantity=None,
    )

    report = _run(tmp_path)

    assert report["classification"] == MODIFY_IN_PLACE_DRY_RUN_READY
    assert report["new_order_created"] is False


def test_duplicate_close_risk_blocks_modify(tmp_path: Path) -> None:
    _seed_authorities(
        tmp_path,
        open_order_classification="DUPLICATE_CLOSE_ORDER",
        managed_order_classification="DUPLICATE_CLOSE_ORDER_BLOCKED",
        planner_classification="DO_NOT_REPLACE_DUPLICATE_RISK",
    )

    report = _run(tmp_path)

    assert report["classification"] == MODIFY_IN_PLACE_BLOCKED_DUPLICATE_RISK


def test_planner_cancel_replace_or_review_blocks_modify(tmp_path: Path) -> None:
    _seed_authorities(tmp_path, planner_classification="TARGETED_CANCEL_REPLACE_REQUIRED")

    report = _run(tmp_path)

    assert report["classification"] in {MODIFY_IN_PLACE_BLOCKED_SUSPICIOUS_ORDER, MODIFY_IN_PLACE_BLOCKED_SHARED_TRUTH}
    assert "TARGETED_CANCEL_REPLACE_REQUIRED" in report["detail"]


def test_broker_flat_blocks_modify(tmp_path: Path) -> None:
    _seed_authorities(
        tmp_path,
        open_order_classification="BROKER_FLAT_WITH_OPEN_CLOSE_ORDER",
        managed_order_classification="BROKER_FLAT_WITH_WORKING_CLOSE",
        planner_classification="BROKER_FLAT_NO_REPLACE",
        broker_positions=[],
        managed_positions=[],
    )

    report = _run(tmp_path)

    assert report["classification"] in {MODIFY_IN_PLACE_BLOCKED_NOT_WORKING_ORDER, MODIFY_IN_PLACE_BLOCKED_SHARED_TRUTH}
    assert "flat" in report["detail"].lower() or "BROKER_FLAT" in report["detail"]


def test_applied_path_uses_same_order_identity_and_no_replacement(tmp_path: Path) -> None:
    _seed_authorities(tmp_path)
    _write_pre_action_snapshot_for_modify(tmp_path)
    calls: list[str] = []

    def pre_refresh(config: ManagedOrderModifyInPlaceConfig) -> dict:
        calls.append("pre")
        return {"open_orders": [_broker_order(limit_price=config.current_known_limit)]}

    def modify(config: ManagedOrderModifyInPlaceConfig) -> dict:
        calls.append("modify")
        return {
            "accepted": True,
            "broker_order_id": config.broker_order_id,
            "perm_id": config.perm_id,
            "new_limit": config.new_limit,
        }

    def post_refresh(config: ManagedOrderModifyInPlaceConfig) -> dict:
        calls.append("post")
        return {"open_orders": [_broker_order(limit_price=config.new_limit)]}

    report = run_track_b_managed_order_modify_in_place(
        config=_config(tmp_path, apply=True, operator_authorized_modify=True),
        now=NOW,
        pre_modify_open_order_refresh=pre_refresh,
        modify_order_limit=modify,
        post_modify_open_order_refresh=post_refresh,
    )

    assert calls == ["pre", "modify", "post"]
    assert report["classification"] == MODIFY_IN_PLACE_APPLIED
    assert report["pre_action_snapshot_validation"]["classification"] == PRE_ACTION_SNAPSHOT_VALID
    assert report["broker_mutation_performed"] is True
    assert report["new_order_created"] is False
    assert report["verified_order"]["broker_order_id"] == "27"
    assert report["verified_order"]["perm_id"] == "347068546"
    assert report["control_plane_snapshot_id"] == "snapshot-managed-order-modify"
    assert report["shared_truth_refresh_generation_id"] == "generation-managed-order-modify"
    assert report["post_modify_verification"]["verified"] is True
    assert report["post_modify_verification"]["updated_limit_observed"] is True


def test_apply_tolerates_exact_single_order_with_sentinel_status_gap_and_missing_limit(tmp_path: Path) -> None:
    _seed_authorities(tmp_path)
    _write_pre_action_snapshot_for_modify(tmp_path)
    calls: list[str] = []

    def pre_refresh(config: ManagedOrderModifyInPlaceConfig) -> dict:
        calls.append("pre")
        return {
            "open_orders": [
                _broker_order(
                    limit_price=None,  # type: ignore[arg-type]
                    filled_quantity="1.7976931348623157e+308",
                    remaining_quantity=None,
                )
            ]
        }

    def modify(config: ManagedOrderModifyInPlaceConfig) -> dict:
        calls.append("modify")
        return {"accepted": True, "broker_order_id": config.broker_order_id, "perm_id": config.perm_id}

    def post_refresh(config: ManagedOrderModifyInPlaceConfig) -> dict:
        calls.append("post")
        return {"open_orders": [_broker_order(limit_price=None)]}  # type: ignore[arg-type]

    report = run_track_b_managed_order_modify_in_place(
        config=_config(tmp_path, apply=True, operator_authorized_modify=True),
        now=NOW,
        pre_modify_open_order_refresh=pre_refresh,
        modify_order_limit=modify,
        post_modify_open_order_refresh=post_refresh,
    )

    assert calls == ["pre", "modify", "post"]
    assert report["classification"] == MODIFY_IN_PLACE_APPLIED
    assert report["new_order_created"] is False
    assert report["post_modify_verification"]["verified"] is True
    assert report["post_modify_verification"]["updated_limit_observed"] is False
    assert report["post_modify_verification"]["broker_limit_omitted_or_not_echoed"] is True


def test_post_modify_verification_failure_is_loud(tmp_path: Path) -> None:
    _seed_authorities(tmp_path)
    _write_pre_action_snapshot_for_modify(tmp_path)

    report = run_track_b_managed_order_modify_in_place(
        config=_config(tmp_path, apply=True, operator_authorized_modify=True),
        now=NOW,
        pre_modify_open_order_refresh=lambda config: {"open_orders": [_broker_order(limit_price=config.current_known_limit)]},
        modify_order_limit=lambda config: {"accepted": True},
        post_modify_open_order_refresh=lambda config: {"open_orders": [_broker_order(limit_price=config.current_known_limit)]},
    )

    assert report["classification"] == MODIFY_IN_PLACE_VERIFICATION_FAILED
    assert report["broker_mutation_attempted"] is True
    assert report["post_modify_verification"]["verified"] is False


def test_ibkr_adapter_modifies_same_order_id_without_replacement(tmp_path: Path) -> None:
    config = _config(tmp_path, apply=True, operator_authorized_modify=True)
    fake_client_cls = _fake_ibkr_client_class()
    fake_wrapper_cls = type("_FakeWrapper", (), {})
    adapter = IbkrPaperManagedOrderModifyAdapter(
        config=config,
        module_loader=lambda name: SimpleNamespace(
            EWrapper=fake_wrapper_cls,
            EClient=fake_client_cls,
        ),
    )

    adapter.connect()
    pre_refresh = adapter.refresh_open_orders(config)
    modify_result = adapter.modify_order_limit(config)
    post_refresh = adapter.refresh_open_orders(config)
    bridge = adapter._bridge  # noqa: SLF001 - test-only inspection of fake transport.

    assert pre_refresh["open_orders"][0]["limit_price"] == "29555.5"
    assert modify_result["place_order_called"] is True
    assert modify_result["new_order_created"] is False
    assert len(bridge.place_order_calls) == 1
    assert bridge.place_order_calls[0]["order_id"] == 27
    assert bridge.place_order_calls[0]["order"].lmtPrice == 29554.5
    assert post_refresh["open_orders"][0]["limit_price"] == "29554.5"
    adapter.disconnect()


def test_live_money_evidence_blocks_modify(tmp_path: Path) -> None:
    _seed_authorities(tmp_path, live_money_eligible=True)

    report = _run(tmp_path)

    assert report["classification"] == MODIFY_IN_PLACE_BLOCKED_SHARED_TRUTH
    assert "live_money_eligible" in report["detail"]


def test_modify_apply_treats_missing_pre_action_snapshot_as_diagnostic(tmp_path: Path) -> None:
    _seed_authorities(tmp_path)

    def _raise_if_called(_config: ManagedOrderModifyInPlaceConfig) -> dict:
        raise AssertionError("missing broker hooks should block before broker refresh")

    report = run_track_b_managed_order_modify_in_place(
        config=_config(tmp_path, apply=True, operator_authorized_modify=True),
        now=NOW,
    )

    assert report["classification"] == MODIFY_IN_PLACE_VERIFICATION_FAILED
    assert report["pre_action_snapshot_validation"]["classification"] == "PRE_ACTION_BLOCKED_SNAPSHOT_MISSING"
    assert report["pre_action_snapshot_required_for_apply"] is False
    assert report["pre_action_snapshot_diagnostic_only_for_managed_close_modify"] is True
    assert report["broker_mutation_attempted"] is False


def test_modify_apply_treats_pre_action_plan_mismatch_as_diagnostic(tmp_path: Path) -> None:
    _seed_authorities(tmp_path)
    _write_pre_action_snapshot_for_modify(
        tmp_path,
        plan_classification=PLAN_TARGETED_CANCEL_REPLACE,
        action_type="TARGETED_CANCEL_REPLACE",
    )

    report = _run(tmp_path, apply=True, operator_authorized_modify=True)

    assert report["classification"] == MODIFY_IN_PLACE_VERIFICATION_FAILED
    assert report["pre_action_snapshot_validation"]["classification"] == "PRE_ACTION_BLOCKED_PLAN_MISMATCH"
    assert report["pre_action_snapshot_required_for_apply"] is False


def test_modify_apply_treats_pre_action_target_mismatch_as_diagnostic(tmp_path: Path) -> None:
    _seed_authorities(tmp_path)
    target = _modify_pre_action_target()
    target["contract"] = "MGCM6"
    _write_pre_action_snapshot_for_modify(tmp_path, target_identity=target)

    report = _run(tmp_path, apply=True, operator_authorized_modify=True)

    assert report["classification"] == MODIFY_IN_PLACE_VERIFICATION_FAILED
    assert report["pre_action_snapshot_validation"]["classification"] == "PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH"
    assert report["pre_action_snapshot_required_for_apply"] is False


def test_modify_apply_valid_snapshot_reaches_existing_next_gate(tmp_path: Path) -> None:
    _seed_authorities(tmp_path)
    _write_pre_action_snapshot_for_modify(tmp_path)

    report = _run(tmp_path, apply=True, operator_authorized_modify=True)

    assert report["classification"] == MODIFY_IN_PLACE_VERIFICATION_FAILED
    assert report["pre_action_snapshot_validation"]["classification"] == PRE_ACTION_SNAPSHOT_VALID


def test_runtime_supervisor_stale_is_diagnostic_for_exact_modify(tmp_path: Path) -> None:
    _seed_authorities(tmp_path, runtime_supervisor_classification="SUPERVISOR_SHARED_TRUTH_STALE")

    report = _run(tmp_path)

    assert report["classification"] == MODIFY_IN_PLACE_DRY_RUN_READY
    diagnostics = report["shared_truth_evidence"]["diagnostic_only_blockers"]
    assert any("SUPERVISOR_SHARED_TRUTH_STALE" in item for item in diagnostics)
    assert not report["shared_truth_evidence"]["blockers"]
    assert report["broker_mutation_attempted"] is False


def test_lifecycle_plus_ownership_id_is_sufficient_without_manifest(tmp_path: Path) -> None:
    _seed_authorities(tmp_path)
    managed_path = tmp_path / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json"
    managed_payload = json.loads(managed_path.read_text(encoding="utf-8"))
    managed_payload["managed_orders"][0]["manifest_id"] = None
    managed_path.write_text(json.dumps(managed_payload), encoding="utf-8")
    plan_path = tmp_path / "outputs/track_b_execution_core/managed_orders/latest_order_adjustment_plan.json"
    plan_payload = json.loads(plan_path.read_text(encoding="utf-8"))
    plan_payload["plans"][0]["manifest_id"] = None
    plan_path.write_text(json.dumps(plan_payload), encoding="utf-8")

    report = _run(tmp_path)

    assert report["classification"] == MODIFY_IN_PLACE_DRY_RUN_READY
    assert report["target_evidence"]["managed_order_match"]["ownership_id"] == "owner_mnq"


def test_cli_writes_dry_run_audit(tmp_path: Path, capsys) -> None:
    _seed_authorities(tmp_path, generated_at=datetime.now(UTC).isoformat())

    exit_code = main(
        [
            "--repo-root",
            str(tmp_path),
            "--broker-order-id",
            "27",
            "--perm-id",
            "347068546",
            "--symbol",
            "MNQ",
            "--contract",
            "MNQM6",
            "--con-id",
            "770561201",
            "--action",
            "SELL",
            "--quantity",
            "1",
            "--current-known-limit",
            "29555.50",
            "--new-limit",
            "29554.50",
        ]
    )

    assert exit_code == 0
    assert "classification=MODIFY_IN_PLACE_DRY_RUN_READY" in capsys.readouterr().out
    audit = json.loads(
        (tmp_path / "outputs/reports/track_b_managed_order_modify_in_place/latest_managed_order_modify_in_place.json").read_text(
            encoding="utf-8"
        )
    )
    assert audit["classification"] == MODIFY_IN_PLACE_DRY_RUN_READY
    assert audit["requested_identity"]["tws_client_id"] == 17086


def test_cli_explicit_tws_client_id_overrides_order_owner(tmp_path: Path, capsys) -> None:
    _seed_authorities(tmp_path, generated_at=datetime.now(UTC).isoformat())

    exit_code = main(
        [
            "--repo-root",
            str(tmp_path),
            "--broker-order-id",
            "27",
            "--perm-id",
            "347068546",
            "--symbol",
            "MNQ",
            "--contract",
            "MNQM6",
            "--con-id",
            "770561201",
            "--action",
            "SELL",
            "--quantity",
            "1",
            "--current-known-limit",
            "29555.50",
            "--new-limit",
            "29554.50",
            "--tws-client-id",
            "1967",
        ]
    )

    assert exit_code == 0
    assert "classification=MODIFY_IN_PLACE_DRY_RUN_READY" in capsys.readouterr().out
    audit = json.loads(
        (tmp_path / "outputs/reports/track_b_managed_order_modify_in_place/latest_managed_order_modify_in_place.json").read_text(
            encoding="utf-8"
        )
    )
    assert audit["requested_identity"]["tws_client_id"] == 1967


def test_dashboard_projection_is_not_consumed() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    forbidden = "outputs/operator_dashboard/runtime/latest_track_b_control_plane_snapshot.json"
    module = repo_root / "src/mgc_v05l/execution_core/track_b_managed_order_modify_in_place.py"

    assert forbidden not in module.read_text(encoding="utf-8")


def _run(
    root: Path,
    *,
    apply: bool = False,
    operator_authorized_modify: bool = False,
    current_known_limit: str = "29555.50",
) -> dict:
    return run_track_b_managed_order_modify_in_place(
        config=_config(
            root,
            apply=apply,
            operator_authorized_modify=operator_authorized_modify,
            current_known_limit=current_known_limit,
        ),
        now=NOW,
    )


def _config(
    root: Path,
    *,
    apply: bool = False,
    operator_authorized_modify: bool = False,
    current_known_limit: str = "29555.50",
) -> ManagedOrderModifyInPlaceConfig:
    return ManagedOrderModifyInPlaceConfig(
        repo_root=root,
        broker_order_id="27",
        perm_id="347068546",
        symbol="MNQ",
        contract="MNQM6",
        con_id="770561201",
        action="SELL",
        quantity="1",
        current_known_limit=current_known_limit,
        new_limit="29554.50",
        apply=apply,
        operator_authorized_modify=operator_authorized_modify,
    )


def _seed_authorities(
    root: Path,
    *,
    generated_at: str = NOW.isoformat(),
    open_order_classification: str = "OPEN_CLOSE_ORDER_WORKING",
    managed_order_classification: str = "WORKING_CLOSE_ORDER",
    planner_classification: str = "MODIFY_IN_PLACE_ELIGIBLE",
    suspicious_reasons: list[str] | None = None,
    filled_quantity: str = "0",
    remaining_quantity: str | None = "1",
    broker_positions: list[dict] | None = None,
    managed_positions: list[dict] | None = None,
    live_money_eligible: bool = False,
    runtime_supervisor_classification: str = "SUPERVISOR_NO_ACTION_NEEDED",
) -> None:
    broker_positions = [_position()] if broker_positions is None else broker_positions
    managed_positions = [_managed_position()] if managed_positions is None else managed_positions
    _write_json(
        root / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {
            "generated_at": generated_at,
            "classification": open_order_classification,
            "order_states": [_broker_order(filled_quantity=filled_quantity, remaining_quantity=remaining_quantity)],
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        {
            "generated_at": generated_at,
            "classification": managed_order_classification,
            "managed_orders": [
                {
                    **_managed_order(classification=managed_order_classification),
                    "suspicious_reasons": suspicious_reasons or [],
                    "source_order": _broker_order(
                        filled_quantity=filled_quantity,
                        remaining_quantity=remaining_quantity,
                    ),
                }
            ],
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/managed_orders/latest_order_adjustment_plan.json",
        {
            "generated_at": generated_at,
            "classification": planner_classification,
            "plans": [{**_managed_order(classification=planner_classification), "classification": planner_classification}],
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/position_truth/latest_position_truth.json",
        {
            "generated_at": generated_at,
            "summary": {"overall_classification": "ATTENTION_REQUIRED" if broker_positions else "CLEAN_FLAT_READY"},
            "broker_positions": broker_positions,
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": generated_at,
            "classification": "OPEN_MANAGED_MATCHED" if managed_positions else "NO_MANAGED_POSITIONS",
            "managed_positions": managed_positions,
        },
    )
    simple_authorities = {
        "outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json": {
            "classification": runtime_supervisor_classification
        },
        "outputs/track_b_execution_core/self_recover/latest_self_recover_rules.json": {
            "classification": "NO_ACTION_NEEDED"
        },
        "outputs/track_b_execution_core/runtime_resume/latest_runtime_resume_semantics.json": {
            "classification": "RESUME_BLOCKED_OPEN_ORDER"
        },
        "outputs/track_b_execution_core/crash_loop_protection/latest_crash_loop_protection.json": {
            "classification": "NO_CRASH_LOOP"
        },
        "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json": {"classification": "ACTIVE"},
    }
    for relative, payload in simple_authorities.items():
        _write_json(root / relative, {"generated_at": generated_at, **payload})
    _write_json(
        root / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {
            "generated_at": generated_at,
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "live_money_eligible": live_money_eligible,
            "paper_proof_invoked": False,
        },
    )


def _write_pre_action_snapshot_for_modify(
    root: Path,
    *,
    plan_classification: str = PLAN_MANAGED_ORDER_MODIFY,
    action_type: str = "MANAGED_ORDER_MODIFY",
    target_identity: dict | None = None,
) -> None:
    generated_at = NOW.isoformat()
    snapshot_id = "snapshot-managed-order-modify"
    generation_id = "generation-managed-order-modify"
    supervisor_decision_id = "supervisor-managed-order-modify"
    target = _modify_pre_action_target() if target_identity is None else target_identity
    _write_json(
        root / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "generated_at": generated_at,
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "control_plane_snapshot_id": snapshot_id,
            "shared_truth_refresh_generation_id": generation_id,
            "shared_truth_coherence_status": "COHERENT",
            "runtime_supervisor_decision_id": supervisor_decision_id,
            "runtime_supervisor_classification": "SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME",
            "safe_to_start_runtime": False,
            "live_money_eligible": False,
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json",
        {
            "generated_at": generated_at,
            "supervisor_decision_id": supervisor_decision_id,
            "classification": "SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME",
            "live_money_eligible": False,
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json",
        {
            "generated_at": generated_at,
            "classification": plan_classification,
            "control_plane_snapshot_id": snapshot_id,
            "shared_truth_refresh_generation_id": generation_id,
            "execution_enabled": False,
            "proposed_actions": [
                {
                    "action_id": "modify_mnq_27",
                    "action_type": action_type,
                    "target_identity": target,
                    "execution_enabled": False,
                }
            ],
        },
    )


def _modify_pre_action_target() -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "contract": "MNQM6",
        "con_id": "770561201",
        "broker_order_id": "27",
        "perm_id": "347068546",
        "action": "SELL",
        "quantity": "1",
    }


def _managed_order(*, classification: str = "WORKING_CLOSE_ORDER") -> dict:
    return {
        "classification": classification,
        "symbol": "MNQ",
        "contract": "MNQM6",
        "local_symbol": "MNQM6",
        "con_id": "770561201",
        "account_id": "DUM882026",
        "action": "SELL",
        "quantity": "1",
        "broker_order_id": "27",
        "perm_id": "347068546",
        "client_id": 17086,
        "broker_status": "Submitted",
        "limit_price": "29555.50",
        "lifecycle_id": "lifecycle_mnq",
        "manifest_id": "manifest_mnq",
        "ownership_id": "owner_mnq",
    }


def _broker_order(
    *,
    limit_price: str = "29555.50",
    filled_quantity: str = "0",
    remaining_quantity: str | None = "1",
) -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "contract": "MNQM6",
        "local_symbol": "MNQM6",
        "con_id": "770561201",
        "broker_order_id": "27",
        "order_id": "27",
        "perm_id": "347068546",
        "client_id": 17086,
        "action": "SELL",
        "quantity": "1",
        "status": "Submitted",
        "broker_status": "Submitted",
        "limit_price": limit_price,
        "filled_quantity": filled_quantity,
        "remaining_quantity": remaining_quantity,
    }


def _fake_ibkr_client_class():
    class _FakeContract:
        symbol = "MNQ"
        localSymbol = "MNQM6"
        conId = "770561201"

    class _FakeOrder:
        account = "DUM882026"
        permId = "347068546"
        action = "SELL"
        totalQuantity = "1"
        lmtPrice = 29555.5
        filledQuantity = "0"
        remainingQuantity = "1"

    class _FakeOrderState:
        status = "Submitted"

    class _FakeClient:
        def __init__(self, wrapper) -> None:
            self.wrapper = wrapper
            self.order = _FakeOrder()
            self.contract = _FakeContract()
            self.place_order_calls: list[dict] = []
            self.disconnected = False

        def connect(self, host, port, client_id) -> None:
            self.connected = (host, port, client_id)
            self.wrapper.nextValidId(9001)

        def run(self) -> None:
            return None

        def disconnect(self) -> None:
            self.disconnected = True

        def reqOpenOrders(self) -> None:
            self.wrapper.openOrder(27, self.contract, self.order, _FakeOrderState())
            self.wrapper.openOrderEnd()

        def reqAllOpenOrders(self) -> None:
            self.reqOpenOrders()

        def placeOrder(self, order_id, contract, order) -> None:
            self.place_order_calls.append({"order_id": order_id, "contract": contract, "order": order})

    return _FakeClient


def _position() -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "contract": "MNQM6",
        "local_symbol": "MNQM6",
        "con_id": "770561201",
        "quantity": "1",
    }


def _managed_position() -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "contract": "MNQM6",
        "local_symbol": "MNQM6",
        "con_id": "770561201",
        "quantity": "1",
        "classification": "OPEN_MANAGED_MATCHED",
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
