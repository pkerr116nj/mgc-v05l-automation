from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timezone

from mgc_v05l.execution.ibkr_unattended_paper_rest_cancel import (
    IbkrUnattendedPaperRestCancelArtifacts,
    IbkrUnattendedPaperRestCancelConfig,
    _build_working_order_acceptance,
    _classify_lifecycle,
    _derive_non_marketable_buy_limit,
    evaluate_unattended_paper_caller,
    render_ibkr_unattended_paper_rest_cancel_markdown,
    run_ibkr_unattended_paper_rest_cancel,
    write_ibkr_unattended_paper_rest_cancel_artifacts,
)
from mgc_v05l.execution_core.track_b_paper_autonomous_recovery_planner import (
    PLAN_MANAGED_ORDER_MODIFY,
    PLAN_TARGETED_CANCEL_REPLACE,
)
from mgc_v05l.execution_core.track_b_pre_action_snapshot_validator import PRE_ACTION_SNAPSHOT_VALID


def _config(tmp_path: Path, **overrides: object) -> IbkrUnattendedPaperRestCancelConfig:
    payload = {
        "repo_root": tmp_path,
        "mode": "PAPER",
        "host": "127.0.0.1",
        "port": 7497,
        "client_id": 9181,
        "unattended_paper": True,
        "account_id": "DUM882026",
    }
    payload.update(overrides)
    return IbkrUnattendedPaperRestCancelConfig(**payload)


def test_strategy_style_caller_fails_closed() -> None:
    result = evaluate_unattended_paper_caller(
        caller_path="unattended_paper_cli",
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.strategy.paper"}))],
    )

    assert result["passed"] is False


def test_derive_non_marketable_buy_limit_uses_delayed_bid(tmp_path: Path) -> None:
    limit_price = _derive_non_marketable_buy_limit(
        quote_context={
            "quote_source_label": "DELAYED",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "bid_price": 4590.9,
            "last_price": 4591.0,
        },
        contract_report={"api_contract_details": [{"min_tick": 0.1}]},
        delayed_quote_max_age_seconds=9999.0,
        offset_ticks=1.0,
    )

    assert limit_price == 4590.8


def test_run_blocks_without_unattended_flag(tmp_path: Path) -> None:
    artifacts = run_ibkr_unattended_paper_rest_cancel(config=_config(tmp_path, unattended_paper=False))

    assert artifacts.classification == "IBKR_UNATTENDED_REST_CANCEL_UNKNOWN"
    assert artifacts.report["lifecycle"]["status"] == "blocked"
    assert artifacts.report["lower_level_cancel_path"] is True
    assert artifacts.report["preferred_path"] == "track_b_managed_exit_cancel_replace"


def test_rest_cancel_blocked_unless_emergency_flag(tmp_path: Path) -> None:
    artifacts = run_ibkr_unattended_paper_rest_cancel(config=_config(tmp_path))

    assert artifacts.classification == "IBKR_UNATTENDED_REST_CANCEL_UNKNOWN"
    assert artifacts.report["emergency_only"] is True
    assert artifacts.report["emergency_legacy_rest_cancel"] is False
    assert any(
        check["name"] == "explicit_emergency_legacy_rest_cancel_required" and not check["passed"]
        for check in artifacts.report["guardrail_checks"]
    )


def test_rest_cancel_blocked_without_control_plane_snapshot(tmp_path: Path) -> None:
    def _raise_if_called(**_kwargs):
        raise AssertionError("snapshot gate should block before transport construction")

    artifacts = run_ibkr_unattended_paper_rest_cancel(
        config=_config(tmp_path, emergency_legacy_rest_cancel=True),
        transport_factory=_raise_if_called,
    )

    assert artifacts.classification == "IBKR_UNATTENDED_REST_CANCEL_UNKNOWN"
    assert artifacts.report["pre_action_snapshot_validation"]["classification"] == "PRE_ACTION_BLOCKED_SNAPSHOT_MISSING"
    assert artifacts.open_order_after_submit["status"] == "not_run"
    assert artifacts.open_order_after_cancel["status"] == "not_run"


def test_rest_cancel_blocked_on_snapshot_identity_mismatch(tmp_path: Path) -> None:
    target = _rest_cancel_pre_action_target()
    target["contract"] = "MNQM6"
    _write_pre_action_snapshot_for_rest_cancel(tmp_path, target_identity=target)

    artifacts = run_ibkr_unattended_paper_rest_cancel(
        config=_config(tmp_path, emergency_legacy_rest_cancel=True),
        transport_factory=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must block before transport")),
    )

    assert artifacts.classification == "IBKR_UNATTENDED_REST_CANCEL_UNKNOWN"
    assert (
        artifacts.report["pre_action_snapshot_validation"]["classification"]
        == "PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH"
    )


def test_rest_cancel_blocked_on_snapshot_action_mismatch(tmp_path: Path) -> None:
    _write_pre_action_snapshot_for_rest_cancel(
        tmp_path,
        plan_classification=PLAN_MANAGED_ORDER_MODIFY,
        action_type="MANAGED_ORDER_MODIFY",
    )

    artifacts = run_ibkr_unattended_paper_rest_cancel(
        config=_config(tmp_path, emergency_legacy_rest_cancel=True),
        transport_factory=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must block before transport")),
    )

    assert artifacts.classification == "IBKR_UNATTENDED_REST_CANCEL_UNKNOWN"
    assert artifacts.report["pre_action_snapshot_validation"]["classification"] == "PRE_ACTION_BLOCKED_PLAN_MISMATCH"


def test_rest_cancel_valid_snapshot_reaches_existing_next_gate_without_mutation(tmp_path: Path) -> None:
    _write_pre_action_snapshot_for_rest_cancel(tmp_path)

    def _raise_after_snapshot(**_kwargs):
        raise RuntimeError("next gate reached")

    artifacts = run_ibkr_unattended_paper_rest_cancel(
        config=_config(tmp_path, emergency_legacy_rest_cancel=True),
        transport_factory=_raise_after_snapshot,
    )

    assert artifacts.classification == "IBKR_UNATTENDED_REST_CANCEL_UNKNOWN"
    assert artifacts.report["pre_action_snapshot_validation"]["classification"] == PRE_ACTION_SNAPSHOT_VALID
    assert "next gate reached" in artifacts.report["lifecycle"]["detail"]


def test_rest_cancel_does_not_consume_dashboard_projection_as_authority() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    source = (repo_root / "src/mgc_v05l/execution/ibkr_unattended_paper_rest_cancel.py").read_text(encoding="utf-8")

    assert "outputs/operator_dashboard/runtime/latest_track_b_control_plane_snapshot.json" not in source


def test_classify_lifecycle_variants() -> None:
    assert _classify_lifecycle({"status": "passed"}) == "IBKR_UNATTENDED_REST_CANCEL_PASSED"
    assert _classify_lifecycle({"status": "dialog_blocked"}) == "IBKR_UNATTENDED_REST_CANCEL_DIALOG_BLOCKED"
    assert _classify_lifecycle({"status": "rejected"}) == "IBKR_UNATTENDED_REST_CANCEL_REJECTED"
    assert _classify_lifecycle({"status": "filled_unexpectedly"}) == "IBKR_UNATTENDED_REST_CANCEL_FILLED_UNEXPECTEDLY"
    assert _classify_lifecycle({"status": "cancel_unknown"}) == "IBKR_UNATTENDED_REST_CANCEL_UNKNOWN"


def test_working_order_acceptance_prefers_tws_confirmation(tmp_path: Path) -> None:
    accepted = _build_working_order_acceptance(
        config=_config(tmp_path, visible_in_tws=True, canceled_in_tws=True),
        lifecycle_result={
            "status": "passed",
            "submitted_order_id": 1,
            "open_order_after_submit": {"open_orders": [{"broker_order_id": 1}], "open_order_count": 1},
            "latest_order_status": {"status": "Submitted"},
        },
    )

    assert accepted["classification"] == "WORKING_ORDER_API_AND_TWS_VISIBLE"
    assert accepted["api_working_order_visible"] is True


def test_working_order_acceptance_distinguishes_fast_fill(tmp_path: Path) -> None:
    accepted = _build_working_order_acceptance(
        config=_config(tmp_path),
        lifecycle_result={"status": "filled_unexpectedly"},
    )

    assert accepted["classification"] == "WORKING_ORDER_FILLED_BEFORE_VISUAL_CONFIRMATION"


def test_write_artifacts_and_markdown(tmp_path: Path) -> None:
    artifacts = IbkrUnattendedPaperRestCancelArtifacts(
        classification="IBKR_UNATTENDED_REST_CANCEL_PASSED",
        report={
            "classification": "IBKR_UNATTENDED_REST_CANCEL_PASSED",
            "generated_at": "2026-04-28T14:00:00+00:00",
            "mode": "PAPER",
            "host": "127.0.0.1",
            "port": 7497,
            "account_id": "DUM882026",
            "client_id": 9181,
            "exact_contract": {"expiry": "20260626", "con_id": 712565978, "local_symbol": "MGCM6"},
            "limit_price": 4590.8,
            "quote_context": {"quote_source_label": "DELAYED"},
            "pricing_context": {
                "quote_snapshot": {"bid_price": 4590.9},
                "distance_from_reference_price": 0.1,
                "distance_ticks": 1.0,
            },
            "open_order_before": {"open_order_count": 0},
            "working_order_acceptance": {
                "classification": "WORKING_ORDER_API_AND_TWS_VISIBLE",
                "api_working_order_visible": True,
                "visible_in_tws": True,
                "canceled_in_tws": True,
                "tws_visibility_pause_seconds": 12.0,
            },
            "lifecycle": {"status": "passed", "detail": "Passed.", "submitted_order_id": 1, "submitted_perm_id": 490000001, "latest_order_status": {"status": "Cancelled"}},
            "callback_timeline_event_count": 4,
        },
        audit_events=[{"event_type": "submit_attempted"}],
        open_order_before={"open_order_count": 0},
        open_order_after_submit={"open_order_count": 1},
        open_order_after_cancel={"open_order_count": 0},
        callback_timeline=[{"callback_name": "openOrder"}],
        extra_artifacts={"executions_after_submit": []},
    )

    write_ibkr_unattended_paper_rest_cancel_artifacts(output_dir=tmp_path, artifacts=artifacts)

    assert (tmp_path / "ibkr_unattended_paper_rest_cancel_report.json").exists()
    assert (tmp_path / "ibkr_unattended_paper_rest_cancel_report.md").exists()
    assert (tmp_path / "ibkr_unattended_paper_rest_cancel_audit.jsonl").exists()
    assert (tmp_path / "ibkr_unattended_paper_rest_cancel_callback_timeline.jsonl").exists()
    payload = json.loads((tmp_path / "ibkr_unattended_paper_rest_cancel_report.json").read_text(encoding="utf-8"))
    markdown = render_ibkr_unattended_paper_rest_cancel_markdown(payload)
    assert "IBKR_UNATTENDED_REST_CANCEL_PASSED" in markdown
    assert "WORKING_ORDER_API_AND_TWS_VISIBLE" in markdown


def _write_pre_action_snapshot_for_rest_cancel(
    root: Path,
    *,
    plan_classification: str = PLAN_TARGETED_CANCEL_REPLACE,
    action_type: str = "TARGETED_CANCEL_REPLACE",
    target_identity: dict[str, object] | None = None,
) -> None:
    generated_at = datetime.now(timezone.utc).isoformat()
    snapshot_id = "snapshot-emergency-rest-cancel"
    generation_id = "generation-emergency-rest-cancel"
    supervisor_decision_id = "supervisor-emergency-rest-cancel"
    target = _rest_cancel_pre_action_target() if target_identity is None else target_identity
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
                    "action_id": "emergency_rest_cancel_mgc",
                    "action_type": action_type,
                    "target_identity": target,
                    "execution_enabled": False,
                }
            ],
        },
    )


def _rest_cancel_pre_action_target() -> dict[str, object]:
    return {
        "account_id": "DUM882026",
        "symbol": "MGC",
        "contract": "MGCM6",
        "con_id": "712565978",
        "action": "BUY",
        "quantity": "1.0",
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
