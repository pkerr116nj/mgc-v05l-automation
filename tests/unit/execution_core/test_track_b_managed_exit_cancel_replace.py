from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.models import BrokerOrder, FillEvent
from mgc_v05l.execution_core import track_b_managed_exit_cancel_replace as cancel_replace_module
from mgc_v05l.execution_core.track_b_managed_exit_cancel_replace import (
    GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH,
    GUARDED_CANCEL_REPLACE_ORDER_NOT_FOUND,
    GUARDED_CANCEL_REPLACE_READY,
    GUARDED_CANCEL_REPLACE_REPLACEMENT_FAILED,
    GUARDED_CANCEL_REPLACE_REPLACEMENT_FILLED,
    GUARDED_CANCEL_REPLACE_REPLACEMENT_WORKING,
    GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED,
    ManagedExitCancelReplaceConfig,
    run_guarded_managed_exit_cancel_replace,
)
from mgc_v05l.execution_core.track_b_paper_autonomous_recovery_planner import (
    PLAN_MANAGED_ORDER_MODIFY,
    PLAN_TARGETED_CANCEL_REPLACE,
)
from mgc_v05l.execution_core.track_b_pre_action_snapshot_validator import PRE_ACTION_SNAPSHOT_VALID


NOW = datetime(2026, 5, 15, 11, 0, tzinfo=timezone.utc)


def test_exact_known_managed_order_proposal_is_accepted(tmp_path: Path) -> None:
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_READY
    assert report["broker_mutation_performed"] is False
    assert report["ready"]["proposal"]["allowed_route"] == "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY"
    assert report["shared_truth_evidence"]["source_authority"] == "execution_core_authority"
    assert report["shared_truth_evidence"]["dashboard_projection_consumed"] is False


def test_dry_run_does_not_construct_adapter(tmp_path: Path) -> None:
    def _raise_if_called(**_kwargs: Any) -> _FakeAdapter:
        raise AssertionError("dry-run must not construct a broker adapter")

    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
        adapter_factory=_raise_if_called,
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_READY
    assert report["broker_mutation_attempted"] is False
    assert report["broker_mutation_performed"] is False
    assert report["pre_action_snapshot_would_block_apply"] is True


def test_unknown_open_order_proposal_is_refused(tmp_path: Path) -> None:
    reconciliation = _reconciliation_report(known_orders=[])
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path),
        now=NOW,
        reconciliation_runner=lambda _config: reconciliation,
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH
    assert "not a known managed exit order" in report["detail"]


def test_cancel_replace_blocks_without_exact_managed_order_identity(tmp_path: Path) -> None:
    _write_shared_truth_for_cancel_replace(tmp_path, broker_order_id="99")

    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, skip_shared_truth_write=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED
    assert "Managed Order Registry does not contain the exact target order identity." in report["detail"]


def test_cancel_replace_blocks_when_planner_requires_suspicious_review(tmp_path: Path) -> None:
    _write_shared_truth_for_cancel_replace(
        tmp_path,
        order_adjustment_classification="REVIEW_REQUIRED_SUSPICIOUS_STATE",
    )

    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, skip_shared_truth_write=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED
    assert "MANUAL_TWS_REVIEW_REQUIRED" in report["detail"]


def test_cancel_replace_blocks_when_old_order_still_open_without_terminal_plan(tmp_path: Path) -> None:
    _write_shared_truth_for_cancel_replace(
        tmp_path,
        order_adjustment_classification="WAIT_FOR_WORKING_ORDER",
        existing_close_order_live=True,
    )

    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, skip_shared_truth_write=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED
    assert "Existing close order is still live" in report["detail"]


def test_cancel_replace_allowed_only_after_terminal_cancel_and_position_still_open(tmp_path: Path) -> None:
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_READY
    assert report["shared_truth_evidence"]["order_adjustment_plan_match"]["terminal_state_confirmed"] is True
    assert report["shared_truth_evidence"]["order_adjustment_plan_match"]["position_open"] is True


def test_cancel_replace_does_not_consume_dashboard_projection_as_authority() -> None:
    source = (
        Path(__file__).resolve().parents[3]
        / "src"
        / "mgc_v05l"
        / "execution_core"
        / "track_b_managed_exit_cancel_replace.py"
    ).read_text(encoding="utf-8")

    forbidden = [
        "latest_track_b_open_order_truth.json",
        "latest_track_b_managed_orders.json",
        "latest_track_b_position_truth.json",
        "latest_track_b_managed_positions.json",
        "latest_track_b_runtime_supervisor_authority.json",
    ]
    assert all(path not in source for path in forbidden)


def test_mismatched_broker_order_id_is_refused(tmp_path: Path) -> None:
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, broker_order_id="2"),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_ORDER_NOT_FOUND


def test_mismatched_client_or_perm_id_is_refused(tmp_path: Path) -> None:
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, client_id=999),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH


def test_mismatched_contract_identity_is_refused(tmp_path: Path) -> None:
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(open_order_overrides={"local_symbol": "MGCM6"}),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_IDENTITY_MISMATCH
    assert "local_symbol mismatch" in report["detail"]


def test_live_money_or_paper_proof_state_is_refused(tmp_path: Path) -> None:
    live_report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(live_money_eligible=True),
    )
    proof_report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(paper_proof_invoked=True),
    )

    assert live_report["classification"] == GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED
    assert proof_report["classification"] == GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED


def test_apply_cancels_exact_order_and_persists_working_replacement(tmp_path: Path) -> None:
    _write_shared_truth_for_cancel_replace(tmp_path)
    _write_pre_action_snapshot_for_cancel_replace(tmp_path)
    fake = _FakeAdapter(fill=None)
    refresh_calls = []
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, apply=True, skip_shared_truth_write=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
        adapter_factory=lambda **_kwargs: fake,
        post_mutation_refresher=lambda **kwargs: refresh_calls.append(kwargs)
        or {"classification": "POST_BROKER_MUTATION_REFRESH_SUCCEEDED", "trigger": kwargs["trigger"]},
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_REPLACEMENT_WORKING
    assert report["pre_action_snapshot_validation"]["classification"] == PRE_ACTION_SNAPSHOT_VALID
    assert report["broker_mutation_performed"] is True
    assert fake.cancelled_order_ids == ["1"]
    assert fake.broad_cancel_called is False
    assert fake.submitted_intents[0].action.value == "SELL"
    state_path = tmp_path / "outputs" / "track_b_execution_core" / "managed_exit_orders" / "latest_known_managed_exit_orders.json"
    assert state_path.exists()
    assert "2" in state_path.read_text(encoding="utf-8")
    assert report["post_broker_mutation_refresh"]["trigger"] == "managed_exit_cancel_replace_working"
    assert refresh_calls[0]["mutation_report"]["broker_mutation_performed"] is True


def test_cancel_replace_allowlist_canonicalizes_mes_shorthand_expiry() -> None:
    proposal = {
        "replacement_order": {
            "account_id": "DUM882026",
            "symbol": "MES",
            "local_symbol": "MESM6",
            "expiry": "202606",
            "con_id": 770561194,
            "action": "BUY",
            "quantity": "1.0",
            "order_type": "LMT",
            "tif": "DAY",
            "limit_price": 7578.7,
            "min_tick": 0.25,
        },
        "cancel_identity": {
            "account_id": "DUM882026",
            "symbol": "MES",
            "local_symbol": "MESM6",
            "expiry": "20260618",
            "con_id": 770561194,
        },
    }

    entry = cancel_replace_module._contract_allowlist_entry(proposal)

    assert entry["symbol"] == "MES"
    assert entry["local_symbol"] == "MESM6"
    assert entry["con_id"] == 770561194
    assert entry["contract_month"] == "202606"
    assert entry["expiry"] == "20260618"


def test_replacement_fill_persists_lifecycle_close(tmp_path: Path) -> None:
    _write_shared_truth_for_cancel_replace(tmp_path)
    _write_pre_action_snapshot_for_cancel_replace(tmp_path)
    fake = _FakeAdapter(fill=_fill())
    ledger_calls: list[dict[str, Any]] = []

    @dataclass(frozen=True)
    class _LedgerResult:
        trade_record_written: bool = True
        ledger_jsonl: Path = tmp_path / "ledger.jsonl"

    def _ledger_update(**kwargs: Any) -> _LedgerResult:
        ledger_calls.append(dict(kwargs))
        return _LedgerResult()

    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, apply=True, skip_shared_truth_write=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
        adapter_factory=lambda **_kwargs: fake,
        ledger_update_runner=_ledger_update,
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_REPLACEMENT_FILLED
    assert report["lifecycle_close"]["persisted"] is True
    assert ledger_calls[0]["filled_bridge_result"]["intent_type"] == "SELL_TO_CLOSE"
    assert ledger_calls[0]["filled_bridge_result"]["broker_order_id"] == "2"


def test_cancel_succeeded_replacement_failed_is_explicit_review_classification(tmp_path: Path) -> None:
    _write_shared_truth_for_cancel_replace(tmp_path)
    _write_pre_action_snapshot_for_cancel_replace(tmp_path)
    fake = _FakeAdapter(replacement_error=RuntimeError("submit rejected"))
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, apply=True, skip_shared_truth_write=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
        adapter_factory=lambda **_kwargs: fake,
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_REPLACEMENT_FAILED
    assert "submit rejected" in report["detail"]
    assert fake.cancelled_order_ids == ["1"]


def test_cancel_replace_apply_blocked_without_snapshot(tmp_path: Path) -> None:
    def _raise_if_called(**_kwargs: Any) -> _FakeAdapter:
        raise AssertionError("snapshot gate should block before adapter construction")

    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, apply=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
        adapter_factory=_raise_if_called,
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED
    assert report["pre_action_snapshot_validation"]["classification"] == "PRE_ACTION_BLOCKED_SNAPSHOT_MISSING"
    assert report["broker_mutation_attempted"] is False


def test_cancel_replace_apply_blocked_on_pre_action_plan_mismatch(tmp_path: Path) -> None:
    _write_shared_truth_for_cancel_replace(tmp_path)
    _write_pre_action_snapshot_for_cancel_replace(
        tmp_path,
        plan_classification=PLAN_MANAGED_ORDER_MODIFY,
        action_type="MANAGED_ORDER_MODIFY",
    )

    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, apply=True, skip_shared_truth_write=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
        adapter_factory=lambda **_kwargs: _FakeAdapter(fill=None),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED
    assert report["pre_action_snapshot_validation"]["classification"] == "PRE_ACTION_BLOCKED_PLAN_MISMATCH"


def test_cancel_replace_apply_blocked_on_pre_action_target_mismatch(tmp_path: Path) -> None:
    _write_shared_truth_for_cancel_replace(tmp_path)
    target = _cancel_replace_pre_action_target()
    target["contract"] = "MNQM6"
    _write_pre_action_snapshot_for_cancel_replace(tmp_path, target_identity=target)

    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, apply=True, skip_shared_truth_write=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
        adapter_factory=lambda **_kwargs: _FakeAdapter(fill=None),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_REVIEW_REQUIRED
    assert report["pre_action_snapshot_validation"]["classification"] == "PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH"


def test_cancel_replace_valid_snapshot_reaches_existing_next_gate(tmp_path: Path) -> None:
    _write_shared_truth_for_cancel_replace(tmp_path)
    _write_pre_action_snapshot_for_cancel_replace(tmp_path)

    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, apply=True, skip_shared_truth_write=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
        adapter_factory=lambda **_kwargs: _FailingConnectAdapter(),
    )

    assert report["classification"] == GUARDED_CANCEL_REPLACE_REPLACEMENT_FAILED
    assert report["pre_action_snapshot_validation"]["classification"] == PRE_ACTION_SNAPSHOT_VALID
    assert "next gate reached" in report["detail"]


def test_original_order_already_gone_does_not_blind_replace(tmp_path: Path) -> None:
    report = run_guarded_managed_exit_cancel_replace(
        config=_config(tmp_path, apply=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(open_orders=[]),
        adapter_factory=lambda **_kwargs: _FakeAdapter(fill=None),
    )

    assert report["classification"] == "GUARDED_CANCEL_REPLACE_CANCELLED_OR_FILLED_BEFORE_ACTION"


def _config(tmp_path: Path, **overrides: Any) -> ManagedExitCancelReplaceConfig:
    skip_shared_truth_write = bool(overrides.pop("skip_shared_truth_write", False))
    payload = {
        "repo_root": tmp_path,
        "broker_order_id": "1",
        "client_id": 10815,
        "perm_id": 614029377,
    }
    payload.update(overrides)
    if not skip_shared_truth_write:
        _write_shared_truth_for_cancel_replace(
            tmp_path,
            broker_order_id=str(payload["broker_order_id"]),
            client_id=payload.get("client_id"),
            perm_id=payload.get("perm_id"),
        )
    return ManagedExitCancelReplaceConfig(**payload)


def _reconciliation_report(
    *,
    known_orders: list[dict[str, Any]] | None = None,
    open_orders: list[dict[str, Any]] | None = None,
    open_order_overrides: dict[str, Any] | None = None,
    live_money_eligible: bool = False,
    paper_proof_invoked: bool = False,
) -> dict[str, Any]:
    open_order = {
        "account_id": "DUM882026",
        "broker_order_id": 1,
        "client_id": 10815,
        "perm_id": 614029377,
        "symbol": "GC",
        "local_symbol": "GCM6",
        "expiry": "20260626",
        "con_id": 430360630,
        "action": "SELL",
        "quantity": "1.0",
        "status": "Submitted",
    }
    open_order.update(open_order_overrides or {})
    known_order = {
        **open_order,
        "managed_order_status": "KNOWN_MANAGED_HARD_EXIT_ORDER_REPRICE_REQUIRED",
        "lifecycle_id": "bridge_fill_GC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN",
        "strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
        "lane_id": "gc_1x_all_lanes__london_early_long",
        "order_intent_id": "GC|1m|2026-05-15T08:06:00Z|SELL_TO_CLOSE",
        "exit_reason": "forced_session_initial_stop",
        "order_type": "LMT",
        "limit_price": 4574.7,
        "tif": "DAY",
        "managed_order_policy": {
            "classification": "KNOWN_MANAGED_HARD_EXIT_ORDER_REPRICE_REQUIRED",
            "recommended_action": "PREPARE_EXACT_CANCEL_REPLACE_FOR_KNOWN_MANAGED_ORDER",
        },
        "guarded_cancel_replace_proposal": {
            "enabled": False,
            "requires_explicit_operator_authorization": True,
            "broker_mutation_performed": False,
            "allowed_route": "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY",
            "forbidden_routes": ["broad_cancel", "reqGlobalCancel", "paper_proof", "live_money"],
            "cancel_identity": {
                "account_id": "DUM882026",
                "broker_order_id": 1,
                "client_id": 10815,
                "perm_id": 614029377,
                "symbol": "GC",
                "local_symbol": "GCM6",
                "expiry": "20260626",
                "con_id": 430360630,
                "action": "SELL",
                "quantity": "1.0",
            },
            "replacement_order": {
                "account_id": "DUM882026",
                "symbol": "GC",
                "local_symbol": "GCM6",
                "expiry": "20260626",
                "con_id": 430360630,
                "action": "SELL",
                "quantity": "1.0",
                "order_type": "LMT",
                "tif": "DAY",
                "limit_price": 4564.9,
                "min_tick": 0.1,
            },
        },
    }
    actual_open_orders = [open_order] if open_orders is None else open_orders
    return {
        "classification": "TRACK_B_PAPER_BROKER_RECONCILED_WITH_KNOWN_MANAGED_EXIT_ORDER",
        "broker_reconciled": True,
        "live_money_eligible": live_money_eligible,
        "paper_proof_invoked": paper_proof_invoked,
        "review_required_count": 0,
        "unknown_broker_open_order_count": 0,
        "track_b_broker_open_order_count": len(actual_open_orders),
        "track_b_broker_open_orders": actual_open_orders,
        "track_b_broker_position_count": 2,
        "lifecycle_open_position_count": 2,
        "known_managed_exit_order_count": len([known_order] if known_orders is None else known_orders),
        "known_managed_exit_orders": [known_order] if known_orders is None else known_orders,
    }


def _write_shared_truth_for_cancel_replace(
    repo: Path,
    *,
    broker_order_id: str = "1",
    client_id: int | None = 10815,
    perm_id: int | None = 614029377,
    managed_order_classification: str = "ORDER_TERMINAL_CANCELLED",
    order_adjustment_classification: str = "TARGETED_CANCEL_REPLACE_REQUIRED",
    open_order_truth_classification: str = "OPEN_CLOSE_ORDER_WORKING",
    runtime_supervisor_classification: str = "SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME",
    existing_close_order_live: bool = False,
) -> None:
    generated_at = NOW.isoformat()
    source_order = {
        "account_id": "DUM882026",
        "broker_order_id": broker_order_id,
        "client_id": client_id,
        "perm_id": perm_id,
        "symbol": "GC",
        "local_symbol": "GCM6",
        "con_id": 430360630,
        "action": "SELL",
        "quantity": "1.0",
        "status": "Cancelled" if order_adjustment_classification == "TARGETED_CANCEL_REPLACE_REQUIRED" else "Submitted",
    }
    managed_order = {
        "classification": managed_order_classification,
        "recommended_next_action": "TARGETED_CANCEL_REPLACE_CANDIDATE",
        "account_id": "DUM882026",
        "symbol": "GC",
        "contract": "GCM6",
        "local_symbol": "GCM6",
        "con_id": 430360630,
        "action": "SELL",
        "quantity": "1.0",
        "broker_order_id": broker_order_id,
        "client_id": client_id,
        "perm_id": perm_id,
        "broker_status": source_order["status"],
        "source_order": source_order,
    }
    plan = {
        "classification": order_adjustment_classification,
        "recommended_operator_action": "TARGETED_CANCEL_REPLACE_CANDIDATE",
        "account_id": "DUM882026",
        "symbol": "GC",
        "contract": "GCM6",
        "con_id": 430360630,
        "action": "SELL",
        "quantity": "1.0",
        "broker_order_id": broker_order_id,
        "client_id": client_id,
        "perm_id": perm_id,
        "broker_status": source_order["status"],
        "position_open": True,
        "existing_close_order_live": existing_close_order_live,
        "terminal_state_confirmed": order_adjustment_classification == "TARGETED_CANCEL_REPLACE_REQUIRED",
        "source_order": source_order,
        "identity": {
            "account_id": "DUM882026",
            "contract": "GCM6",
            "con_id": 430360630,
            "broker_order_id": broker_order_id,
            "client_id": client_id,
            "perm_id": perm_id,
            "action": "SELL",
            "quantity": "1.0",
        },
    }
    position_row = {
        "classification": "OPEN_MANAGED_MATCHED",
        "account_id": "DUM882026",
        "symbol": "GC",
        "local_symbol": "GCM6",
        "con_id": 430360630,
        "quantity": "1.0",
        "lifecycle_status": "OPEN_MANAGED",
    }
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json",
        {"generated_at": generated_at, "classification": open_order_truth_classification, "order_states": []},
    )
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json",
        {"generated_at": generated_at, "classification": managed_order_classification, "managed_orders": [managed_order]},
    )
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_order_adjustment_plan.json",
        {"generated_at": generated_at, "classification": order_adjustment_classification, "plans": [plan]},
    )
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {"generated_at": generated_at, "classification": "ATTENTION_REQUIRED", "broker_positions": [position_row]},
    )
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json",
        {"generated_at": generated_at, "classification": "OPEN_MANAGED_MATCHED", "managed_positions": [position_row]},
    )
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "runtime_supervisor" / "latest_runtime_supervisor_authority.json",
        {"generated_at": generated_at, "classification": runtime_supervisor_classification},
    )
    _write_json(
        repo / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json",
        {"generated_at": generated_at, "classification": "ACTIVE"},
    )


def _write_pre_action_snapshot_for_cancel_replace(
    repo: Path,
    *,
    plan_classification: str = PLAN_TARGETED_CANCEL_REPLACE,
    action_type: str = "TARGETED_CANCEL_REPLACE",
    target_identity: dict[str, Any] | None = None,
) -> None:
    generated_at = NOW.isoformat()
    snapshot_id = "snapshot-targeted-cancel-replace"
    generation_id = "generation-targeted-cancel-replace"
    supervisor_decision_id = "supervisor-targeted-cancel-replace"
    target = _cancel_replace_pre_action_target() if target_identity is None else target_identity
    _write_json(
        repo / "outputs" / "track_b_execution_core" / "control_plane" / "latest_control_plane_snapshot.json",
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
        repo / "outputs" / "track_b_execution_core" / "runtime_supervisor" / "latest_runtime_supervisor_authority.json",
        {
            "generated_at": generated_at,
            "supervisor_decision_id": supervisor_decision_id,
            "classification": "SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME",
            "live_money_eligible": False,
        },
    )
    _write_json(
        repo
        / "outputs"
        / "track_b_execution_core"
        / "paper_autonomous_recovery"
        / "latest_paper_autonomous_recovery_plan.json",
        {
            "generated_at": generated_at,
            "classification": plan_classification,
            "control_plane_snapshot_id": snapshot_id,
            "shared_truth_refresh_generation_id": generation_id,
            "execution_enabled": False,
            "proposed_actions": [
                {
                    "action_id": "targeted_cancel_replace_gc_1",
                    "action_type": action_type,
                    "target_identity": target,
                    "execution_enabled": False,
                }
            ],
        },
    )


def _cancel_replace_pre_action_target() -> dict[str, str]:
    return {
        "account_id": "DUM882026",
        "symbol": "GC",
        "contract": "GCM6",
        "con_id": "430360630",
        "broker_order_id": "1",
        "perm_id": "614029377",
        "action": "SELL",
        "quantity": "1.0",
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


class _FakeAdapter:
    def __init__(self, *, fill: FillEvent | None = None, replacement_error: Exception | None = None) -> None:
        self.fill = fill
        self.replacement_error = replacement_error
        self.cancelled_order_ids: list[str] = []
        self.submitted_intents: list[Any] = []
        self.broad_cancel_called = False

    def connect(self) -> None: ...
    def disconnect(self) -> None: ...
    def managed_accounts(self) -> tuple[str, ...]:
        return ("DUM882026",)
    def require_configured_account(self) -> str:
        return "DUM882026"
    def register_existing_order_for_cancel(self, **_kwargs: Any) -> None: ...
    def cancel_order(self, *, submit_attempt_id: str, broker_order_id: str) -> None:
        self.cancelled_order_ids.append(str(broker_order_id))
    def wait_for_cancel(self, *, submit_attempt_id: str, timeout_seconds: float | None = None) -> None: ...
    def submit_limit_order(self, *, submit_attempt: Any, order_intent: Any) -> int:
        if self.replacement_error is not None:
            raise self.replacement_error
        self.submitted_intents.append(order_intent)
        return 2
    def wait_for_broker_order(self, *, submit_attempt_id: str, timeout_seconds: float | None = None) -> BrokerOrder:
        return BrokerOrder(
            broker_order_event_id="broker-order-2",
            run_id="run",
            submit_attempt_id=submit_attempt_id,
            account_id="DUM882026",
            broker_order_id="2",
            perm_id="614029400",
            client_id=10941,
            contract_key="GC-202606",
            action="SELL",
            quantity=1,
            order_type="LMT",
            limit_price="4564.9",
            status="Submitted",
            filled_quantity=0,
            remaining_quantity=1,
            average_fill_price=None,
            observed_at=NOW,
        )
    def wait_for_fill(self, *, submit_attempt_id: str, timeout_seconds: float | None = None) -> FillEvent:
        if self.fill is None:
            raise TimeoutError("still working")
        return self.fill
    def submit_diagnostics(self, submit_attempt_id: str | None = None) -> dict[str, Any]:
        return {}


class _FailingConnectAdapter(_FakeAdapter):
    def __init__(self) -> None:
        super().__init__(fill=None)

    def connect(self) -> None:
        raise RuntimeError("next gate reached")


def _fill() -> FillEvent:
    return FillEvent(
        fill_event_id="fill-2",
        run_id="run",
        submit_attempt_id="replacement",
        order_intent_id="replacement",
        account_id="DUM882026",
        broker_order_id="2",
        perm_id="614029400",
        execution_id="exec-2",
        contract_key="GC-202606",
        action="SELL",
        quantity=1,
        price="4564.9",
        filled_at=NOW,
    )
