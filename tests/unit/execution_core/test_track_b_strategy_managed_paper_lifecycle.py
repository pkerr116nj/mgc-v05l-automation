from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.models import BrokerOrder, IntentKind, PositionSource, PositionState
from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEventType
from mgc_v05l.execution_core.track_b_live_trade_registry import (
    append_live_trade_registry_event,
    make_live_trade_registry_event,
)
from mgc_v05l.execution_core.track_b_open_order_truth import (
    BROKER_FLAT_WITH_OPEN_CLOSE_ORDER,
    DUPLICATE_CLOSE_ORDER,
    OPEN_CLOSE_ORDER_WORKING,
    SUSPICIOUS_ORDER_STATE,
)
import mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle as lifecycle_module
from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import (
    CLOSE_ORDER_ALREADY_WORKING,
    STRATEGY_SUBMIT_BLOCKED_ENTRY_EXPOSURE,
    TrackBManagedExitPolicy,
    TrackBManagedPaperLifecycleClassification,
    TrackBStrategyManagedPaperLifecycleConfig,
    TrackBStrategyManagedPaperLifecycleStages,
    maintain_open_track_b_strategy_managed_paper_lifecycle,
    run_track_b_strategy_managed_paper_lifecycle,
    write_open_managed_lifecycle_report_from_filled_bridge_result,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 6, 23, 11, tzinfo=timezone.utc)


def base_config(tmp_path: Path, **overrides: object) -> TrackBStrategyManagedPaperLifecycleConfig:
    payload: dict[str, object] = {
        "mode": "PAPER",
        "account_id": "DUM882026",
        "expected_account_id": "DUM882026",
        "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        "instrument_family": "MGC",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "side": "BUY",
        "quantity": 1,
        "signal_timestamp": "2026-05-06T23:10:00+00:00",
        "signal_reason": "unit-test signal",
        "decision_bar_timestamp": "2026-05-06T23:10:00+00:00",
        "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
        "pricing_policy": "MARKETABLE_LIMIT_FROM_LIVE_CONTEXT",
        "entry_limit_price": "4704.6",
        "close_limit_price": "4705.1",
        "output_root": tmp_path / "managed",
        "paper_trade_ledger_output_root": tmp_path / "ledger",
        "repo_root": tmp_path,
        "runtime_generation_id": "runtime-generation-1",
        "lane_id": "unit-test-lane",
        "pre_action_snapshot_max_age_seconds": 10_000_000,
    }
    payload.update(overrides)
    return TrackBStrategyManagedPaperLifecycleConfig(**payload)


def seed_strategy_submit_authority(
    tmp_path: Path,
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    intent_payload: Mapping[str, Any],
    intent_kind: IntentKind,
    limit_price: object,
    safe_state_overrides: Mapping[str, Any] | None = None,
    snapshot_overrides: Mapping[str, Any] | None = None,
    planner_target_overrides: Mapping[str, Any] | None = None,
) -> None:
    target = lifecycle_module._strategy_submit_target_identity(
        config=config,
        intent_payload=intent_payload,
        intent_kind=intent_kind,
        limit_price=limit_price,
    )
    target.update({str(key): str(value) for key, value in (planner_target_overrides or {}).items()})
    plan_classification = (
        lifecycle_module.PLAN_STRATEGY_MANAGED_CLOSE_SUBMIT
        if intent_kind is IntentKind.CLOSE
        else lifecycle_module.PLAN_STRATEGY_MANAGED_ENTRY_SUBMIT
    )
    action_type = (
        lifecycle_module.ACTION_STRATEGY_MANAGED_CLOSE_SUBMIT
        if intent_kind is IntentKind.CLOSE
        else lifecycle_module.ACTION_STRATEGY_MANAGED_ENTRY_SUBMIT
    )
    snapshot = {
        "generated_at": aware_now().isoformat(),
        "control_plane_snapshot_id": "snapshot-1",
        "shared_truth_refresh_generation_id": "generation-1",
        "shared_truth_coherence_status": "COHERENT",
        "runtime_supervisor_decision_id": "supervisor-1",
        "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
        "safe_to_start_runtime": True,
        "safe_to_leave_runtime_running": False,
        "runtime_resume_action_policy": "NEW_RUNTIME_GENERATION_ALLOWED",
        "runtime_resume_proposed_next_runtime_generation_id": config.runtime_generation_id,
        "runtime_resume_generation_reuse_allowed": False,
        "runtime_resume_must_start_new_generation": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "open_order_truth_classification": "NO_OPEN_ORDERS",
        "managed_order_registry_classification": "MANAGED_ORDER_REGISTRY_READY",
        "position_truth_classification": "CLEAN_FLAT_READY",
        "managed_position_registry_classification": "NO_MANAGED_POSITIONS",
        "agent_health_top_blockers": [],
        "agent_health_has_duplicate_writer": False,
    }
    snapshot.update(dict(snapshot_overrides or {}))
    _write_json(tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json", snapshot)
    _write_json(
        tmp_path / "outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json",
        {
            "generated_at": aware_now().isoformat(),
            "supervisor_decision_id": "supervisor-1",
            "classification": snapshot.get("runtime_supervisor_classification"),
            "live_money_eligible": snapshot.get("live_money_eligible"),
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json",
        {
            "generated_at": aware_now().isoformat(),
            "classification": plan_classification,
            "control_plane_snapshot_id": snapshot.get("control_plane_snapshot_id"),
            "shared_truth_refresh_generation_id": snapshot.get("shared_truth_refresh_generation_id"),
            "execution_enabled": False,
            "live_money_eligible": snapshot.get("live_money_eligible"),
            "proposed_actions": [
                {
                    "action_id": action_type.lower(),
                    "action_type": action_type,
                    "target_identity": target,
                    "execution_enabled": False,
                }
            ],
        },
    )
    safe_state = {
        "generated_at": aware_now().isoformat(),
        "safe_state_classification": "SAFE_STATE_NORMAL",
        "control_plane_snapshot_id": snapshot.get("control_plane_snapshot_id"),
        "shared_truth_generation_id": snapshot.get("shared_truth_refresh_generation_id"),
        "runtime_generation_id": config.runtime_generation_id,
        "submit_allowed": True,
        "broker_mutation_allowed": True,
        "observe_only": False,
        "tripped_limits": [],
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    safe_state.update(dict(safe_state_overrides or {}))
    _write_json(tmp_path / "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json", safe_state)
    if intent_kind is IntentKind.CLOSE:
        _seed_registry_backed_close_truth(tmp_path, config=config, intent_payload=intent_payload)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_registry_backed_close_truth(
    tmp_path: Path,
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    intent_payload: Mapping[str, Any],
) -> None:
    lifecycle_id = str(intent_payload.get("lifecycle_id") or "open-managed-existing")
    trade_id = str(intent_payload.get("trade_id") or "").strip()
    if not trade_id and lifecycle_id == "open-managed-existing":
        trade_id = "MNQ_FIRST_BULL_SNAP_TURN_V1:open-managed-existing"
    if not trade_id:
        trade_id = f"{config.strategy_id}:{lifecycle_id}"
    side = "LONG" if str(intent_payload.get("order_action") or "SELL").upper() == "SELL" else "SHORT"
    base = {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "lane_id": str(config.lane_id or config.strategy_id),
        "thesis_strategy_id": str(config.strategy_id),
        "account_id": str(config.account_id),
        "symbol": str(config.instrument_family),
        "con_id": int(config.con_id),
        "local_symbol": str(config.local_symbol),
        "expiry": str(config.contract_expiry or config.contract_key.split("-", 1)[-1]),
        "side": side,
        "action": "BUY" if side == "LONG" else "SELL",
        "qty": intent_payload.get("quantity") or config.quantity,
        "source_artifact_path": str(tmp_path / "registry_fixture.json"),
        "generated_at": aware_now(),
    }
    for event_type, extra in (
        (TradeEventType.ENTRY_INTENT_CREATED, {}),
        (TradeEventType.ENTRY_ORDER_SUBMITTED, {"order_id": "11", "client_id": "17086"}),
        (TradeEventType.ENTRY_FILL_BROKER_BACKED, {"order_id": "11", "client_id": "17086", "perm_id": "347068100", "exec_id": "exec-1", "price": "28729"}),
        (TradeEventType.LIFECYCLE_OPEN_MANAGED, {"order_id": "11", "client_id": "17086", "perm_id": "347068100", "exec_id": "exec-1", "price": "28729"}),
    ):
        append_live_trade_registry_event(
            repo_root=tmp_path,
            event=make_live_trade_registry_event(event_type=event_type, **base, **extra),
        )
    _write_json(
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {
            "generated_at": aware_now().isoformat(),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "review_required_count": 0,
            "track_b_broker_open_order_count": 0,
            "track_b_broker_positions": [
                {
                    "account_id": config.account_id,
                    "symbol": config.instrument_family,
                    "local_symbol": config.local_symbol,
                    "con_id": config.con_id,
                    "quantity": str(intent_payload.get("quantity") or config.quantity),
                }
            ],
            "track_b_lifecycle_positions": [
                {
                    "account_id": "MULTIPLE",
                    "strategy_id": config.strategy_id,
                    "lane_id": config.lane_id,
                    "track_b_root": config.instrument_family,
                    "instrument_family": config.instrument_family,
                    "contract_key": config.contract_key,
                    "local_symbol": config.local_symbol,
                    "con_id": config.con_id,
                    "quantity": str(intent_payload.get("quantity") or config.quantity),
                    "side": side,
                    "entry_perm_id": "347068100",
                    "entry_exec_id": "exec-1",
                    "lifecycle_id": lifecycle_id,
                }
            ],
            "live_money_eligible": False,
            "blockers": [],
            "block_reasons": [],
        },
    )


def fake_stages(*, close: bool = False) -> TrackBStrategyManagedPaperLifecycleStages:
    def entry_submitter(
        config: TrackBStrategyManagedPaperLifecycleConfig,
        entry_intent: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        return {
            "submitted": True,
            "broker_state_mutated": True,
            "broker_order_id": "1001",
            "entry_fill": {
                "price": "4704.6",
                "quantity": 1,
                "filled_at": "2026-05-06T23:11:12+00:00",
            },
        }

    def exit_policy(
        config: TrackBStrategyManagedPaperLifecycleConfig,
        open_state: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        required_bars = lifecycle_module._required_completed_5m_bars_for_policy(
            str(config.managed_exit_policy_id or ""),
            configured_bars=int(config.managed_exit_policy_max_completed_5m_bars),
        )
        time_boxed_ready = (
            lifecycle_module._is_timeboxed_managed_close_policy(str(config.managed_exit_policy_id or ""))
            and int(config.completed_5m_bars_since_entry or 0) >= required_bars
        )
        if not close and not time_boxed_ready:
            return None
        return {
            "order_action": "SELL",
            "quantity": 1,
            "close_limit_price": "4705.1",
            "exit_family": "DIAGNOSTIC_TIME",
            "close_reason": "TIME_BOXED_EXIT" if time_boxed_ready else "DIAGNOSTIC_TIME_EXIT_IMMEDIATE",
            "managed_exit_policy_id": config.managed_exit_policy_id,
            "hard_exit": False,
            "discretionary_exit": not time_boxed_ready,
            "risk_control_exit": time_boxed_ready,
            "maintenance_exit": time_boxed_ready,
            "elapsed_completed_5m_bars": config.completed_5m_bars_since_entry,
            "required_completed_5m_bars": required_bars,
            "bars_since_fill": int(config.completed_5m_bars_since_entry or 0),
            "bars_since_signal": None if config.completed_5m_bars_since_signal is None else int(config.completed_5m_bars_since_signal),
            "fill_timestamp_source": config.fill_timestamp_source or "BROKER_ENTRY_FILL",
            "mfe": None,
            "mae": None,
            "data_freshness_state": config.data_freshness_state,
            "broker_truth_state": config.broker_truth_state,
            "suppressed_due_to_stale_data": False,
        }

    def close_submitter(
        config: TrackBStrategyManagedPaperLifecycleConfig,
        close_intent: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        return {
            "submitted": True,
            "broker_state_mutated": True,
            "broker_order_id": "1002",
            "close_fill": {
                "price": "4705.1",
                "quantity": 1,
                "filled_at": "2026-05-06T23:16:12+00:00",
            },
        }

    return TrackBStrategyManagedPaperLifecycleStages(
        entry_submitter=entry_submitter,
        exit_policy=exit_policy,
        close_submitter=close_submitter,
    )


def open_managed_report() -> dict[str, object]:
    return {
        "lifecycle_id": "open-managed-existing",
        "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
        "instrument_family": "MNQ",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "account_id": "DUM882026",
        "expected_account_id": "DUM882026",
        "managed_exit_policy_id": TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
        "managed_exit_policy_max_completed_5m_bars": 3,
        "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
        "final_position_status": "OPEN_MANAGED",
        "entry_intent": {
            "lifecycle_id": "open-managed-existing",
            "trade_id": "MNQ_FIRST_BULL_SNAP_TURN_V1:open-managed-existing",
            "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
            "instrument_family": "MNQ",
            "contract_key": "MNQ-202606",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "account_id": "DUM882026",
            "expected_account_id": "DUM882026",
            "side": "LONG",
            "order_action": "BUY",
            "quantity": 1,
            "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
            "entry_limit_price": "28729",
            "managed_exit_policy_id": TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
        },
        "entry_submit_attempt": {
            "submitted": True,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "broker_order_id": "11",
        },
        "entry_fill": {
            "broker_order_id": "11",
            "execution_id": "exec-1",
            "price": "28729",
            "quantity": "1",
            "filled_at": "2026-05-07T16:26:07+00:00",
        },
        "review_required": False,
        "broker_reconciled": False,
    }


def _broker_order(order_id: str, perm_id: str) -> BrokerOrder:
    return BrokerOrder(
        broker_order_event_id=f"broker-order-{order_id}",
        run_id="run",
        submit_attempt_id=f"submit-{order_id}",
        account_id="DUM882026",
        broker_order_id=order_id,
        perm_id=perm_id,
        client_id=17086,
        contract_key="MNQ-202606",
        action="SELL",
        quantity=1,
        order_type="LMT",
        limit_price="29555.50",
        status="Submitted",
        filled_quantity=0,
        remaining_quantity=1,
        average_fill_price=None,
        observed_at=aware_now(),
    )


def test_missing_exit_policy_does_not_submit(tmp_path: Path) -> None:
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(tmp_path),
        stages=fake_stages(),
        lifecycle_id="missing-exit-policy",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.EXIT_POLICY_MISSING
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["live_money_readiness"] is False


def test_submit_disabled_reports_managed_submit_blocked_without_position(tmp_path: Path) -> None:
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            submit_enabled=False,
        ),
        lifecycle_id="submit-disabled",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.LIFECYCLE_NOT_AVAILABLE
    assert result.report["submit_enabled"] is False
    assert result.report["managed_paper_submit_enabled"] is False
    assert result.report["managed_submit_blocked_reason"] == "SUBMIT_DISABLED"
    assert result.report["entry_submit_attempt"]["submit_attempted"] is False
    assert result.report["entry_fill"] is None
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False


def test_submit_enabled_blocks_without_control_plane_snapshot(tmp_path: Path, monkeypatch) -> None:
    class MustNotInstantiateAdapter:
        def __init__(self, **_kwargs: Any) -> None:
            raise AssertionError("snapshot gate must block before adapter construction")

    monkeypatch.setattr(lifecycle_module, "IbkrPaperAdapter", MustNotInstantiateAdapter)

    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            managed_exit_policy_id=TrackBManagedExitPolicy.MANAGED_HOLD_REQUIRES_EXTERNAL_EXIT_SIGNAL.value,
            submit_enabled=True,
        ),
        lifecycle_id="missing-snapshot",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
    attempt = result.report["entry_submit_attempt"]
    assert attempt["classification"] == lifecycle_module.STRATEGY_SUBMIT_BLOCKED_NO_SNAPSHOT
    assert attempt["submit_attempted"] is False
    assert attempt["broker_state_mutated"] is False
    assert attempt["strategy_submit_authorization"]["dashboard_projection_consumed"] is False


def test_submit_enabled_blocks_by_safe_state_observe_only(tmp_path: Path, monkeypatch) -> None:
    class MustNotInstantiateAdapter:
        def __init__(self, **_kwargs: Any) -> None:
            raise AssertionError("safe-state gate must block before adapter construction")

    config = base_config(
        tmp_path,
        managed_exit_policy_id=TrackBManagedExitPolicy.MANAGED_HOLD_REQUIRES_EXTERNAL_EXIT_SIGNAL.value,
        submit_enabled=True,
    )
    intent = lifecycle_module._entry_intent(config=config, lifecycle_id="observe-only", now=aware_now())
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload=intent,
        intent_kind=IntentKind.OPEN,
        limit_price="4704.6",
        safe_state_overrides={
            "safe_state_classification": "SAFE_STATE_OBSERVE_ONLY",
            "submit_allowed": False,
            "broker_mutation_allowed": False,
            "observe_only": True,
        },
    )
    monkeypatch.setattr(lifecycle_module, "IbkrPaperAdapter", MustNotInstantiateAdapter)

    result = run_track_b_strategy_managed_paper_lifecycle(
        config=config,
        lifecycle_id="observe-only",
        now=aware_now(),
    )

    attempt = result.report["entry_submit_attempt"]
    assert attempt["classification"] == lifecycle_module.STRATEGY_SUBMIT_BLOCKED_SAFE_STATE
    assert attempt["strategy_submit_authorization"]["safe_state_classification"] == "SAFE_STATE_OBSERVE_ONLY"
    assert attempt["broker_state_mutated"] is False


def test_submit_enabled_blocks_by_broker_position_guardian_hard_hold(tmp_path: Path, monkeypatch) -> None:
    class MustNotInstantiateAdapter:
        def __init__(self, **_kwargs: Any) -> None:
            raise AssertionError("guardian hard hold must block before adapter construction")

    config = base_config(
        tmp_path,
        managed_exit_policy_id=TrackBManagedExitPolicy.MANAGED_HOLD_REQUIRES_EXTERNAL_EXIT_SIGNAL.value,
        submit_enabled=True,
    )
    intent = lifecycle_module._entry_intent(config=config, lifecycle_id="guardian-hard-hold", now=aware_now())
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload=intent,
        intent_kind=IntentKind.OPEN,
        limit_price="4704.6",
        snapshot_overrides={
            "broker_position_guardian_classification": "BROKER_POSITION_GUARDIAN_HARD_HOLD",
            "broker_position_guardian_blocks_submit": True,
        },
    )
    monkeypatch.setattr(lifecycle_module, "IbkrPaperAdapter", MustNotInstantiateAdapter)

    result = run_track_b_strategy_managed_paper_lifecycle(
        config=config,
        lifecycle_id="guardian-hard-hold",
        now=aware_now(),
    )

    attempt = result.report["entry_submit_attempt"]
    assert attempt["classification"] == lifecycle_module.STRATEGY_SUBMIT_BLOCKED_POSITION_TRUTH
    assert "Broker Position Guardian hard hold" in attempt["primary_blocker"]
    assert attempt["broker_state_mutated"] is False


def test_submit_enabled_blocks_live_money_and_paper_proof_flags(tmp_path: Path, monkeypatch) -> None:
    class MustNotInstantiateAdapter:
        def __init__(self, **_kwargs: Any) -> None:
            raise AssertionError("hard invariant gate must block before adapter construction")

    monkeypatch.setattr(lifecycle_module, "IbkrPaperAdapter", MustNotInstantiateAdapter)

    config_live = base_config(
        tmp_path / "live",
        managed_exit_policy_id=TrackBManagedExitPolicy.MANAGED_HOLD_REQUIRES_EXTERNAL_EXIT_SIGNAL.value,
        submit_enabled=True,
    )
    intent_live = lifecycle_module._entry_intent(config=config_live, lifecycle_id="live-money", now=aware_now())
    seed_strategy_submit_authority(
        tmp_path / "live",
        config=config_live,
        intent_payload=intent_live,
        intent_kind=IntentKind.OPEN,
        limit_price="4704.6",
        snapshot_overrides={"live_money_eligible": True},
        safe_state_overrides={"live_money_eligible": True},
    )
    live_result = run_track_b_strategy_managed_paper_lifecycle(
        config=config_live,
        lifecycle_id="live-money",
        now=aware_now(),
    )
    assert live_result.report["entry_submit_attempt"]["classification"] == lifecycle_module.STRATEGY_SUBMIT_BLOCKED_LIVE_MONEY

    config_proof = base_config(
        tmp_path / "proof",
        managed_exit_policy_id=TrackBManagedExitPolicy.MANAGED_HOLD_REQUIRES_EXTERNAL_EXIT_SIGNAL.value,
        submit_enabled=True,
    )
    intent_proof = lifecycle_module._entry_intent(config=config_proof, lifecycle_id="paper-proof", now=aware_now())
    seed_strategy_submit_authority(
        tmp_path / "proof",
        config=config_proof,
        intent_payload=intent_proof,
        intent_kind=IntentKind.OPEN,
        limit_price="4704.6",
        snapshot_overrides={"paper_proof_invoked": True},
    )
    proof_result = run_track_b_strategy_managed_paper_lifecycle(
        config=config_proof,
        lifecycle_id="paper-proof",
        now=aware_now(),
    )
    assert proof_result.report["entry_submit_attempt"]["classification"] == lifecycle_module.STRATEGY_SUBMIT_BLOCKED_PAPER_PROOF


def test_submit_enabled_blocks_target_identity_mismatch(tmp_path: Path, monkeypatch) -> None:
    class MustNotInstantiateAdapter:
        def __init__(self, **_kwargs: Any) -> None:
            raise AssertionError("target mismatch must block before adapter construction")

    config = base_config(
        tmp_path,
        managed_exit_policy_id=TrackBManagedExitPolicy.MANAGED_HOLD_REQUIRES_EXTERNAL_EXIT_SIGNAL.value,
        submit_enabled=True,
    )
    intent = lifecycle_module._entry_intent(config=config, lifecycle_id="target-mismatch", now=aware_now())
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload=intent,
        intent_kind=IntentKind.OPEN,
        limit_price="4704.6",
        planner_target_overrides={"contract": "WRONG"},
    )
    monkeypatch.setattr(lifecycle_module, "IbkrPaperAdapter", MustNotInstantiateAdapter)

    result = run_track_b_strategy_managed_paper_lifecycle(
        config=config,
        lifecycle_id="target-mismatch",
        now=aware_now(),
    )

    attempt = result.report["entry_submit_attempt"]
    assert attempt["classification"] == lifecycle_module.STRATEGY_SUBMIT_BLOCKED_TARGET_MISMATCH
    assert attempt["submit_attempted"] is False
    assert attempt["broker_state_mutated"] is False


def test_adapter_stage_failure_preserves_submit_attempt_diagnostics(tmp_path: Path) -> None:
    def bad_entry(
        config: TrackBStrategyManagedPaperLifecycleConfig,
        entry_intent: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        return {
            "submitted": False,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "review_required": True,
            "broker_order_id": "1001",
            "primary_blocker": "Managed PAPER adapter submit stage failed: missing openOrder/orderStatus callback",
            "submit_diagnostics": {
                "contract_fields_submitted_to_ibkr": {"lastTradeDateOrContractMonth": "202606"},
                "canonical_broker_contract_fields": {"lastTradeDateOrContractMonth": "20260626"},
                "contract_consistency_check_passed": True,
                "contract_mismatch_reason": None,
                "pre_submit_blocked": False,
                "place_order_called": True,
                "order_transmit_flag": True,
                "broker_order_id_allocated": "1001",
                "openOrder_seen": False,
                "orderStatus_seen": False,
            },
        }

    stages = TrackBStrategyManagedPaperLifecycleStages(
        entry_submitter=bad_entry,
        exit_policy=lambda config, open_state: None,
        close_submitter=lambda config, close_intent: {},
    )
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            submit_enabled=True,
        ),
        stages=stages,
        lifecycle_id="adapter-stage-failure",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
    assert result.report["submit_enabled"] is True
    assert result.report["entry_submit_attempt"]["submit_attempted"] is True
    assert result.report["entry_submit_attempt"]["submit_diagnostics"]["place_order_called"] is True
    assert result.report["entry_submit_attempt"]["submit_diagnostics"]["order_transmit_flag"] is True
    assert result.report["contract_fields_submitted_to_ibkr"] == {"lastTradeDateOrContractMonth": "202606"}
    assert result.report["canonical_broker_contract_fields"] == {"lastTradeDateOrContractMonth": "20260626"}
    assert result.report["contract_consistency_check_passed"] is True
    assert result.report["pre_submit_blocked"] is False
    assert result.report["entry_fill"] is None
    assert result.report["submit_attempted"] is True
    assert result.report["broker_state_mutated"] is True
    assert result.report["review_required"] is True


def test_adapter_stage_error_478_is_classified_as_contract_rejected(tmp_path: Path) -> None:
    def rejected_entry(
        config: TrackBStrategyManagedPaperLifecycleConfig,
        entry_intent: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        return {
            "submitted": False,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "review_required": True,
            "broker_order_id": "10",
            "primary_blocker": (
                "IBKR_CONTRACT_REJECTED: Parameters in request conflicts with contract parameters received by contract id: "
                "requested expiry 202606, in contract 20260626;"
            ),
            "ibkr_error_code": 478,
            "ibkr_error_message": "Parameters in request conflicts with contract parameters received by contract id: requested expiry 202606, in contract 20260626;",
            "submit_diagnostics": {
                "place_order_called": True,
                "order_transmit_flag": True,
                "broker_order_id_allocated": "10",
                "error_callbacks_after_submit": [
                    {
                        "error_code": 478,
                        "error_string": "Parameters in request conflicts with contract parameters received by contract id: requested expiry 202606, in contract 20260626;",
                    }
                ],
            },
        }

    stages = TrackBStrategyManagedPaperLifecycleStages(
        entry_submitter=rejected_entry,
        exit_policy=lambda config, open_state: None,
        close_submitter=lambda config, close_intent: {},
    )
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            submit_enabled=True,
        ),
        stages=stages,
        lifecycle_id="contract-rejected",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
    assert result.report["primary_blocker"].startswith("IBKR_CONTRACT_REJECTED")
    assert result.report["ibkr_error_code"] == 478
    assert "20260626" in result.report["ibkr_error_message"]
    assert result.report["paper_proof_invoked"] is False


def test_valid_exit_policy_creates_open_managed_state(tmp_path: Path) -> None:
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            managed_exit_policy_id=TrackBManagedExitPolicy.MANAGED_HOLD_REQUIRES_EXTERNAL_EXIT_SIGNAL.value,
        ),
        stages=fake_stages(),
        lifecycle_id="open-managed",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.OPEN_MANAGED
    assert result.report["entry_intent"]["order_action"] == "BUY"
    assert result.report["entry_fill"]["price"] == "4704.6"
    assert result.report["close_intent"] is None
    assert result.report["final_position_status"] == "OPEN_MANAGED"
    assert result.report["paper_proof_invoked"] is False


def test_entry_submit_authorization_blocks_same_lane_reentry_before_adapter(tmp_path: Path) -> None:
    config = base_config(
        tmp_path,
        strategy_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
        lane_id="mnq_first_bear_snap_turn",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="SELL",
        entry_limit_price="30000",
        submit_enabled=True,
        managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
    )
    lifecycle_id = "new-mnq-same-lane"
    entry_intent = lifecycle_module._entry_intent(config=config, lifecycle_id=lifecycle_id, now=aware_now())
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload=entry_intent,
        intent_kind=IntentKind.OPEN,
        limit_price=config.entry_limit_price,
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "classification": "OPEN_MANAGED_MATCHED",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_MATCHED",
                    "account_id": "DUM882026",
                    "instrument_family": "MNQ",
                    "contract_key": "MNQ-202606",
                    "local_symbol": "MNQM6",
                    "con_id": 770561201,
                    "aggregate_qty": "-1",
                    "lifecycle_units": [
                        {
                            "lifecycle_id": "existing-life",
                            "entry_intent_id": "existing-life",
                            "strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
                            "lane_id": "mnq_first_bear_snap_turn",
                            "account_id": "DUM882026",
                            "instrument_family": "MNQ",
                            "contract_key": "MNQ-202606",
                            "local_symbol": "MNQM6",
                            "con_id": 770561201,
                            "side": "SHORT",
                            "quantity": "1",
                            "signed_qty": "-1",
                            "exit_status": "OPEN_MANAGED",
                        }
                    ],
                    "broker_qty_match": True,
                }
            ],
        },
    )

    result = run_track_b_strategy_managed_paper_lifecycle(
        config=config,
        lifecycle_id=lifecycle_id,
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
    assert result.report["entry_submit_attempt"]["classification"] == STRATEGY_SUBMIT_BLOCKED_ENTRY_EXPOSURE
    assert result.report["entry_submit_attempt"]["strategy_submit_authorization"]["entry_exposure_gate"][
        "classification"
    ] == "SAME_LANE_REENTRY_BLOCKED_PYRAMIDING_NOT_ALLOWED"
    assert result.report["entry_submit_attempt"]["broker_state_mutated"] is False


def test_direct_bridge_writer_refuses_open_managed_without_broker_fill_identity(tmp_path: Path) -> None:
    report_path = write_open_managed_lifecycle_report_from_filled_bridge_result(
        filled_bridge_result={
            "order_intent_id": "MGC|1m|2026-05-22T01:39:00Z|BUY_TO_OPEN",
            "intent_type": "BUY_TO_OPEN",
            "lane_id": "track_b_paper_execution_test_mule_v1__mgc",
            "strategy_id": "track_b_paper_execution_test_mule_v1__mgc",
            "instrument": "MGC",
            "symbol": "MGC",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "con_id": 712565978,
            "quantity": 1,
            "managed_exit_policy_id": TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            "broker_order_id": None,
            "perm_id": None,
            "fill_price": None,
            "fill_timestamp": None,
            "paper_proof_invoked": False,
            "live_money_readiness": False,
        },
        output_root=tmp_path / "managed",
        now=aware_now(),
    )

    assert report_path is None
    assert not list((tmp_path / "managed").glob("**/track_b_strategy_managed_paper_lifecycle_report.json"))


def test_direct_bridge_writer_preserves_lane_and_runtime_generation(tmp_path: Path) -> None:
    report_path = write_open_managed_lifecycle_report_from_filled_bridge_result(
        filled_bridge_result={
            "order_intent_id": "MGC|1m|2026-05-22T01:39:00Z|BUY_TO_OPEN",
            "intent_type": "BUY_TO_OPEN",
            "lane_id": "mgc_1x_all_lanes__asia_early_long",
            "strategy_id": "gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__asia_early_long",
            "runtime_generation_id": "track-b-paper-runtime-generation-20260525T101253Z",
            "instrument": "MGC",
            "symbol": "MGC",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "con_id": 712565978,
            "quantity": 1,
            "managed_exit_policy_id": TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            "broker_order_id": "31",
            "perm_id": "123",
            "fill_price": "4572.897",
            "fill_timestamp": aware_now().isoformat(),
            "paper_proof_invoked": False,
            "live_money_readiness": False,
        },
        output_root=tmp_path / "managed",
        now=aware_now(),
    )

    assert report_path is not None
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["lane_id"] == "mgc_1x_all_lanes__asia_early_long"
    assert report["runtime_generation_id"] == "track-b-paper-runtime-generation-20260525T101253Z"
    assert report["entry_intent"]["lane_id"] == "mgc_1x_all_lanes__asia_early_long"
    assert report["open_state"]["runtime_generation_id"] == "track-b-paper-runtime-generation-20260525T101253Z"


def test_exit_policy_generates_close_and_closed_flat(tmp_path: Path) -> None:
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            managed_exit_policy_id=TrackBManagedExitPolicy.DIAGNOSTIC_TIME_EXIT_IMMEDIATE.value,
        ),
        stages=fake_stages(close=True),
        lifecycle_id="closed-flat",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
    assert result.report["close_intent"]["order_action"] == "SELL"
    assert result.report["close_fill"]["price"] == "4705.1"
    assert result.report["realized_pnl"] == "0.5"
    assert result.report["final_position_status"] == "CLOSED_FLAT"


def test_time_boxed_exit_policy_waits_until_required_completed_bars(tmp_path: Path) -> None:
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            completed_5m_bars_since_entry=2,
        ),
        stages=fake_stages(),
        lifecycle_id="time-boxed-wait",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.OPEN_MANAGED
    assert result.report["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert result.report["managed_exit_policy_max_completed_5m_bars"] == 3
    assert result.report["open_position_age_completed_5m_bars"] == 2
    assert result.report["expected_exit_condition"] == "TIME_BOXED_EXIT_AFTER_3_COMPLETED_5M_BARS"
    assert result.report["close_intent_status"] == "WAITING_FOR_EXIT_POLICY_CONDITION"
    assert result.report["close_intent"] is None


def test_continuation_aware_preview_is_diagnostic_only_for_first_p0_strategy(tmp_path: Path) -> None:
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            strategy_id="asian_drift_v1",
            side="BUY",
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            completed_5m_bars_since_entry=3,
            continuation_completed_5m_candles=(
                {"open": "100.0", "high": "101.4", "low": "99.8", "close": "101.1"},
                {"open": "101.1", "high": "102.2", "low": "100.9", "close": "102.0"},
            ),
            continuation_microtrend_state={"trend": "ALIGNED_CONTINUATION"},
            continuation_participation_state={"participation": "STRONG_PARTICIPATING"},
            continuation_mfe="2.0",
            continuation_mae="-0.2",
            continuation_unrealized_pnl="1.2",
        ),
        stages=fake_stages(),
        lifecycle_id="continuation-preview-diagnostic",
        now=aware_now(),
    )

    preview = result.report["continuation_aware_exit_preview"]
    assert result.classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
    assert result.report["close_intent"]["close_reason"] == "TIME_BOXED_EXIT"
    assert preview["exit_policy_id"] == "TIME_PLUS_CONTINUATION_EXIT_V1"
    assert preview["exit_profile_id"] == "ASIAN_DRIFT_CONTINUATION_LONG_LEASH_V1"
    assert preview["exit_state"] == "HOLD_CONTINUATION_CONFIRMED"
    assert preview["evidence_summary"]["completed_5m_candle_count"] == 2
    assert preview["dry_run_only"] is True
    assert preview["not_order_authority"] is True
    assert preview["not_lifecycle_authority"] is True
    assert preview["should_request_close"] is False

    latest_preview = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "continuation_aware_exit"
        / "latest_continuation_aware_exit_preview.json"
    )
    preview_events = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "continuation_aware_exit"
        / "continuation_aware_exit_previews.jsonl"
    )
    diagnostic_preview = json.loads(latest_preview.read_text(encoding="utf-8"))
    assert diagnostic_preview["strategy_id"] == "asian_drift_v1"
    assert diagnostic_preview["dry_run_only"] is True
    assert diagnostic_preview["not_order_authority"] is True
    assert diagnostic_preview["not_lifecycle_authority"] is True
    assert diagnostic_preview["close_intent_preview"]["would_submit"] is False
    assert len(preview_events.read_text(encoding="utf-8").strip().splitlines()) == 1


def test_continuation_aware_preview_missing_evidence_does_not_block_time_boxed_exit(tmp_path: Path) -> None:
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            side="SELL",
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            completed_5m_bars_since_entry=3,
        ),
        stages=fake_stages(),
        lifecycle_id="continuation-preview-missing-evidence",
        now=aware_now(),
    )

    preview = result.report["continuation_aware_exit_preview"]
    assert result.classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
    assert result.report["close_intent"]["close_reason"] == "TIME_BOXED_EXIT"
    assert preview["exit_state"] == "INSUFFICIENT_DATA_HOLD_OR_FALLBACK"
    assert "completed_5m_candles" in preview["missing_inputs"]
    assert preview["should_request_close"] is False


def test_remaining_p0_continuation_previews_are_evidence_fed_diagnostic_only(tmp_path: Path) -> None:
    cases = (
        (
            "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            "MGC",
            "BUY",
            "BREAKOUT_RETEST_CONTINUATION_HOLD_V1",
            (
                {"open": "100.0", "high": "100.7", "low": "99.8", "close": "100.5"},
                {"open": "100.5", "high": "101.0", "low": "100.2", "close": "100.8"},
            ),
            {"microtrend": "HEALTHY_BREAKOUT_RETEST_CONTINUATION"},
            {"participation": "STRONG_PARTICIPATING"},
        ),
        (
            "MNQ_FIRST_BEAR_SNAP_TURN_V1",
            "MNQ",
            "SELL",
            "SNAP_TURN_FAST_DECAY_V1",
            (
                {"open": "19000.0", "high": "19005.0", "low": "18960.0", "close": "18970.0"},
                {"open": "18970.0", "high": "18980.0", "low": "18920.0", "close": "18935.0"},
            ),
            {"microtrend": "FAST_SNAP_TURN_CONTINUATION"},
            {"participation": "STRONG_PARTICIPATING"},
        ),
        (
            "MNQ_FIRST_BULL_SNAP_TURN_V1",
            "MNQ",
            "BUY",
            "SNAP_TURN_FAST_DECAY_V1",
            (
                {"open": "19000.0", "high": "19045.0", "low": "18990.0", "close": "19035.0"},
                {"open": "19035.0", "high": "19095.0", "low": "19020.0", "close": "19080.0"},
            ),
            {"microtrend": "FAST_SNAP_TURN_CONTINUATION"},
            {"participation": "STRONG_PARTICIPATING"},
        ),
    )

    for strategy_id, symbol, side, expected_profile, candles, microtrend, participation in cases:
        result = run_track_b_strategy_managed_paper_lifecycle(
            config=base_config(
                tmp_path,
                strategy_id=strategy_id,
                instrument_family=symbol,
                side=side,
                managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
                completed_5m_bars_since_entry=3,
                continuation_completed_5m_candles=candles,
                continuation_microtrend_state=microtrend,
                continuation_participation_state=participation,
                continuation_position_age_minutes=20,
                continuation_mfe="2.0",
                continuation_mae="-0.3",
                continuation_unrealized_pnl="1.1",
            ),
            stages=fake_stages(),
            lifecycle_id=f"continuation-preview-{strategy_id}",
            now=aware_now(),
        )

        preview = result.report["continuation_aware_exit_preview"]
        assert result.classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
        assert result.report["close_intent"]["close_reason"] == "TIME_BOXED_EXIT"
        assert preview["exit_profile_id"] == expected_profile
        assert preview["evidence_summary"]["completed_5m_candle_count"] == 2
        assert preview["evidence_summary"]["microtrend_state"] == microtrend
        assert preview["evidence_summary"]["participation_state"] == participation
        assert preview["dry_run_only"] is True
        assert preview["not_order_authority"] is True
        assert preview["not_lifecycle_authority"] is True
        assert preview["should_request_close"] is False


def test_remaining_p0_missing_evidence_fallbacks_do_not_block_time_boxed_exit(tmp_path: Path) -> None:
    for strategy_id, symbol, side in (
        ("ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1", "MGC", "BUY"),
        ("MNQ_FIRST_BEAR_SNAP_TURN_V1", "MNQ", "SELL"),
        ("MNQ_FIRST_BULL_SNAP_TURN_V1", "MNQ", "BUY"),
    ):
        result = run_track_b_strategy_managed_paper_lifecycle(
            config=base_config(
                tmp_path,
                strategy_id=strategy_id,
                instrument_family=symbol,
                side=side,
                managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
                completed_5m_bars_since_entry=3,
            ),
            stages=fake_stages(),
            lifecycle_id=f"continuation-preview-missing-{strategy_id}",
            now=aware_now(),
        )

        preview = result.report["continuation_aware_exit_preview"]
        assert result.classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
        assert result.report["close_intent"]["close_reason"] == "TIME_BOXED_EXIT"
        assert preview["exit_state"] == "INSUFFICIENT_DATA_HOLD_OR_FALLBACK"
        assert "completed_5m_candles" in preview["missing_inputs"]
        assert preview["should_request_close"] is False


def test_time_boxed_exit_policy_creates_close_intent_after_required_completed_bars(tmp_path: Path) -> None:
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            completed_5m_bars_since_entry=3,
        ),
        stages=fake_stages(),
        lifecycle_id="time-boxed-close-pending",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
    assert result.report["close_intent"]["close_reason"] == "TIME_BOXED_EXIT"
    assert result.report["close_intent"]["elapsed_completed_5m_bars"] == 3
    assert result.report["close_intent"]["required_completed_5m_bars"] == 3
    assert result.report["close_intent_status"] == "CLOSE_INTENT_CREATED"
    assert result.report["broker_state_mutated"] is True
    assert result.report["paper_proof_invoked"] is False
    assert result.report["bars_since_fill"] == 3
    assert result.report["close_intent"]["bars_since_fill"] == 3
    assert result.report["close_intent"]["fill_timestamp_source"] == "BROKER_ENTRY_FILL"
    assert result.report["close_intent"]["hard_exit"] is False
    assert result.report["close_intent"]["discretionary_exit"] is False
    assert result.report["close_intent"]["risk_control_exit"] is True
    assert result.report["close_intent"]["maintenance_exit"] is True


def test_time_boxed_exit_policy_is_not_discretionary_stale_data_exit(tmp_path: Path) -> None:
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            completed_5m_bars_since_entry=3,
            suppress_discretionary_exits_due_to_stale_data=True,
            data_freshness_state="STALE_RESTRICT_DISCRETIONARY_EXITS",
        ),
        stages=fake_stages(),
        lifecycle_id="time-boxed-close-stale-signal-data",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
    assert result.report["close_intent"]["close_reason"] == "TIME_BOXED_EXIT"
    assert result.report["close_intent"]["discretionary_exit"] is False
    assert result.report["close_intent"]["risk_control_exit"] is True


def test_discretionary_immediate_exit_policy_remains_suppressed_by_stale_data(tmp_path: Path) -> None:
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            managed_exit_policy_id=TrackBManagedExitPolicy.DIAGNOSTIC_TIME_EXIT_IMMEDIATE.value,
            completed_5m_bars_since_entry=3,
            suppress_discretionary_exits_due_to_stale_data=True,
            data_freshness_state="STALE_RESTRICT_DISCRETIONARY_EXITS",
        ),
        stages=fake_stages(),
        lifecycle_id="discretionary-close-stale-signal-data",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.OPEN_MANAGED
    assert result.report["close_intent"] is None
    assert result.report["suppressed_due_to_stale_data"] is True


def test_forced_session_segment_exit_policy_uses_time_boxed_close(tmp_path: Path) -> None:
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            managed_exit_policy_id=TrackBManagedExitPolicy.FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1.value,
            completed_5m_bars_since_entry=3,
        ),
        stages=fake_stages(),
        lifecycle_id="forced-session-time-boxed-close",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
    assert result.report["managed_exit_policy_id"] == "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1"
    assert result.report["expected_exit_condition"] == "TIME_BOXED_EXIT_AFTER_3_COMPLETED_5M_BARS"
    assert result.report["close_intent"]["close_reason"] == "TIME_BOXED_EXIT"
    assert result.report["close_intent"]["managed_exit_policy_id"] == "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1"
    assert result.report["broker_state_mutated"] is True


def test_degraded_stale_data_blocks_new_entries_before_exit_panic(tmp_path: Path) -> None:
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            data_freshness_state="DEGRADED_BLOCK_NEW_ENTRIES",
            block_new_entries_due_to_stale_data=True,
            submit_enabled=True,
        ),
        stages=fake_stages(),
        lifecycle_id="degraded-block-new-entry",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.LIFECYCLE_NOT_AVAILABLE
    assert "blocks new managed entries" in result.report["primary_blocker"]
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_maintenance_open_managed_age_zero_keeps_waiting_without_entry_resubmit(tmp_path: Path) -> None:
    calls = {"entry": 0, "close": 0}

    def entry_submitter(_config, _entry_intent):
        calls["entry"] += 1
        raise AssertionError("maintenance must not resubmit entry")

    def close_submitter(_config, _close_intent):
        calls["close"] += 1
        return {}

    stages = TrackBStrategyManagedPaperLifecycleStages(
        entry_submitter=entry_submitter,
        exit_policy=fake_stages().exit_policy,
        close_submitter=close_submitter,
    )
    result = maintain_open_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
            instrument_family="MNQ",
            contract_key="MNQ-202606",
            local_symbol="MNQM6",
            con_id=770561201,
            side="LONG",
            entry_limit_price="28729",
            close_limit_price="28728.5",
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            completed_5m_bars_since_entry=0,
        ),
        existing_lifecycle_report=open_managed_report(),
        stages=stages,
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.OPEN_MANAGED
    assert result.report["open_position_age_completed_5m_bars"] == 0
    assert result.report["close_intent_status"] == "WAITING_FOR_EXIT_POLICY_CONDITION"
    assert result.report["close_intent"] is None
    assert calls == {"entry": 0, "close": 0}


def test_maintenance_open_managed_age_three_closes_flat_without_paper_proof(tmp_path: Path) -> None:
    result = maintain_open_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
            instrument_family="MNQ",
            contract_key="MNQ-202606",
            local_symbol="MNQM6",
            con_id=770561201,
            side="LONG",
            entry_limit_price="28729",
            close_limit_price="28728.5",
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            completed_5m_bars_since_entry=3,
        ),
        existing_lifecycle_report=open_managed_report(),
        stages=fake_stages(),
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
    assert result.report["close_intent"]["close_reason"] == "TIME_BOXED_EXIT"
    assert result.report["close_intent"]["elapsed_completed_5m_bars"] == 3
    assert result.report["close_submit_attempt"]["broker_order_id"] == "1002"
    assert result.report["close_fill"]["price"] == "4705.1"
    assert result.report["final_position_status"] == "CLOSED_FLAT"
    assert result.report["paper_proof_invoked"] is False


def test_maintenance_blocks_duplicate_working_close_order_before_submit(tmp_path: Path, monkeypatch) -> None:
    class ExistingCloseAdapter:
        def __init__(self, **_kwargs: Any) -> None:
            self.submitted = False

        def connect(self) -> None: ...

        def disconnect(self) -> None: ...

        def managed_accounts(self) -> tuple[str, ...]:
            return ("DUM882026",)

        def require_configured_account(self) -> str:
            return "DUM882026"

        def refresh_open_orders(self, *, contract_key: str | None = None) -> tuple[BrokerOrder, ...]:
            assert contract_key == "MNQ-202606"
            return (
                BrokerOrder(
                    broker_order_event_id="broker-order-existing-close",
                    run_id="run",
                    submit_attempt_id="existing-close-submit",
                    account_id="DUM882026",
                    broker_order_id="27",
                    perm_id="347068546",
                    client_id=17086,
                    contract_key="MNQ-202606",
                    action="SELL",
                    quantity=1,
                    order_type="LMT",
                    limit_price="29555.50",
                    status="Submitted",
                    filled_quantity=0,
                    remaining_quantity=1,
                    average_fill_price=None,
                    observed_at=aware_now(),
                ),
            )

        def submit_limit_order(self, **_kwargs: Any) -> int:
            self.submitted = True
            raise AssertionError("duplicate close must not submit")

    monkeypatch.setattr(lifecycle_module, "IbkrPaperAdapter", ExistingCloseAdapter)
    config = base_config(
        tmp_path,
        strategy_id="track_b_paper_execution_test_mule_v1__mnq",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="LONG",
        entry_limit_price="28729",
        close_limit_price="28728.5",
        managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
        completed_5m_bars_since_entry=3,
        submit_enabled=True,
    )
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload={
            "lifecycle_id": "open-managed-existing",
            "order_action": "SELL",
            "quantity": 1,
            "close_limit_price": "28728.5",
        },
        intent_kind=IntentKind.CLOSE,
        limit_price="28728.5",
    )
    result = maintain_open_track_b_strategy_managed_paper_lifecycle(
        config=config,
        existing_lifecycle_report=open_managed_report(),
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
    assert result.report["close_submit_attempt"]["classification"] == CLOSE_ORDER_ALREADY_WORKING
    assert result.report["close_submit_attempt"]["submitted"] is False
    assert result.report["close_submit_attempt"]["broker_state_mutated"] is False
    assert result.report["close_submit_attempt"]["existing_working_close_order"]["broker_order_id"] == "27"
    assert result.report["close_submit_attempt"]["open_order_truth"]["classification"] == OPEN_CLOSE_ORDER_WORKING
    assert (
        result.report["close_submit_attempt"]["existing_working_close_order"][
            "open_order_truth_overall_classification"
        ]
        == OPEN_CLOSE_ORDER_WORKING
    )
    assert result.report["primary_blocker"] == (
        "Existing working close order for exact contract/action/quantity blocks duplicate managed PAPER close submit."
    )


def test_maintenance_blocks_prior_lifecycle_close_submit_before_second_submit(tmp_path: Path) -> None:
    def close_submitter(
        _config: TrackBStrategyManagedPaperLifecycleConfig,
        _close_intent: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        raise AssertionError("prior lifecycle close submit must block duplicate broker submit")

    stages = TrackBStrategyManagedPaperLifecycleStages(
        entry_submitter=fake_stages().entry_submitter,
        exit_policy=fake_stages(close=True).exit_policy,
        close_submitter=close_submitter,
    )
    report = open_managed_report()
    report["close_submit_attempt"] = {
        "broker_order_id": "36",
        "broker_state_mutated": True,
        "submit_attempted": True,
        "submitted": False,
        "primary_blocker": "missing execDetails callback",
    }

    result = maintain_open_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
            instrument_family="MNQ",
            contract_key="MNQ-202606",
            local_symbol="MNQM6",
            con_id=770561201,
            side="LONG",
            close_limit_price="28728.5",
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            completed_5m_bars_since_entry=3,
            submit_enabled=True,
        ),
        existing_lifecycle_report=report,
        stages=stages,
        now=aware_now(),
    )

    close_attempt = result.report["close_submit_attempt"]
    assert result.classification == TrackBManagedPaperLifecycleClassification.OPEN_MANAGED
    assert close_attempt["classification"] == CLOSE_ORDER_ALREADY_WORKING
    assert close_attempt["broker_order_id"] == "36"
    assert close_attempt["submitted"] is False
    assert close_attempt["broker_state_mutated"] is False
    assert "Existing managed close submit" in close_attempt["primary_blocker"]


def test_maintenance_blocks_prior_lifecycle_close_fill_before_second_submit(tmp_path: Path) -> None:
    def close_submitter(
        _config: TrackBStrategyManagedPaperLifecycleConfig,
        _close_intent: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        raise AssertionError("prior lifecycle close fill must block duplicate broker submit")

    stages = TrackBStrategyManagedPaperLifecycleStages(
        entry_submitter=fake_stages().entry_submitter,
        exit_policy=fake_stages(close=True).exit_policy,
        close_submitter=close_submitter,
    )
    report = open_managed_report()
    report["close_fill"] = {
        "broker_order_id": "36",
        "execution_id": "exec-close-36",
        "price": "29978",
        "quantity": "1",
        "filled_at": "2026-05-25T11:53:55+00:00",
    }

    result = maintain_open_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
            instrument_family="MNQ",
            contract_key="MNQ-202606",
            local_symbol="MNQM6",
            con_id=770561201,
            side="LONG",
            close_limit_price="28728.5",
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            completed_5m_bars_since_entry=3,
            submit_enabled=True,
        ),
        existing_lifecycle_report=report,
        stages=stages,
        now=aware_now(),
    )

    close_attempt = result.report["close_submit_attempt"]
    assert result.classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
    assert close_attempt["classification"] == CLOSE_ORDER_ALREADY_WORKING
    assert close_attempt["existing_close_fill"]["broker_order_id"] == "36"
    assert close_attempt["broker_state_mutated"] is False


def test_maintenance_blocks_duplicate_close_using_open_order_truth(tmp_path: Path, monkeypatch) -> None:
    class DuplicateCloseAdapter:
        def __init__(self, **_kwargs: Any) -> None: ...

        def connect(self) -> None: ...

        def disconnect(self) -> None: ...

        def managed_accounts(self) -> tuple[str, ...]:
            return ("DUM882026",)

        def require_configured_account(self) -> str:
            return "DUM882026"

        def refresh_open_orders(self, *, contract_key: str | None = None) -> tuple[BrokerOrder, ...]:
            assert contract_key == "MNQ-202606"
            return (
                _broker_order("27", "347068546"),
                _broker_order("28", "347068547"),
            )

        def submit_limit_order(self, **_kwargs: Any) -> int:
            raise AssertionError("duplicate close must not submit")

    monkeypatch.setattr(lifecycle_module, "IbkrPaperAdapter", DuplicateCloseAdapter)

    config = base_config(
        tmp_path,
        strategy_id="track_b_paper_execution_test_mule_v1__mnq",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="LONG",
        close_limit_price="28728.5",
        managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
        completed_5m_bars_since_entry=3,
        submit_enabled=True,
    )
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload={
            "lifecycle_id": "open-managed-existing",
            "order_action": "SELL",
            "quantity": 1,
            "close_limit_price": "28728.5",
        },
        intent_kind=IntentKind.CLOSE,
        limit_price="28728.5",
    )
    result = maintain_open_track_b_strategy_managed_paper_lifecycle(
        config=config,
        existing_lifecycle_report=open_managed_report(),
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
    close_attempt = result.report["close_submit_attempt"]
    assert close_attempt["classification"] == CLOSE_ORDER_ALREADY_WORKING
    assert close_attempt["open_order_truth"]["classification"] == DUPLICATE_CLOSE_ORDER
    assert close_attempt["open_order_truth"]["summary"]["duplicate_close_order_group_count"] == 1
    assert close_attempt["submitted"] is False
    assert close_attempt["broker_state_mutated"] is False


def test_maintenance_surfaces_suspicious_sentinel_close_order_via_open_order_truth(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class SuspiciousCloseAdapter:
        def __init__(self, **_kwargs: Any) -> None: ...

        def connect(self) -> None: ...

        def disconnect(self) -> None: ...

        def managed_accounts(self) -> tuple[str, ...]:
            return ("DUM882026",)

        def require_configured_account(self) -> str:
            return "DUM882026"

        def refresh_open_orders(self, *, contract_key: str | None = None) -> tuple[dict[str, Any], ...]:
            assert contract_key == "MNQ-202606"
            return (
                {
                    "account_id": "DUM882026",
                    "contract_key": "MNQ-202606",
                    "symbol": "MNQ",
                    "local_symbol": "MNQM6",
                    "broker_order_id": "27",
                    "perm_id": "347068546",
                    "client_id": 17086,
                    "action": "SELL",
                    "quantity": "1",
                    "order_type": "LMT",
                    "limit_price": "29555.50",
                    "status": "Submitted",
                    "filled_quantity": "1.7976931348623157e+308",
                    "remaining_quantity": None,
                    "updated_at": aware_now().isoformat(),
                },
            )

        def submit_limit_order(self, **_kwargs: Any) -> int:
            raise AssertionError("suspicious close must not submit a duplicate")

    monkeypatch.setattr(lifecycle_module, "IbkrPaperAdapter", SuspiciousCloseAdapter)

    config = base_config(
        tmp_path,
        strategy_id="track_b_paper_execution_test_mule_v1__mnq",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="LONG",
        close_limit_price="28728.5",
        managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
        completed_5m_bars_since_entry=3,
        submit_enabled=True,
    )
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload={
            "lifecycle_id": "open-managed-existing",
            "order_action": "SELL",
            "quantity": 1,
            "close_limit_price": "28728.5",
        },
        intent_kind=IntentKind.CLOSE,
        limit_price="28728.5",
    )
    result = maintain_open_track_b_strategy_managed_paper_lifecycle(
        config=config,
        existing_lifecycle_report=open_managed_report(),
        now=aware_now(),
    )

    close_attempt = result.report["close_submit_attempt"]
    assert close_attempt["classification"] == CLOSE_ORDER_ALREADY_WORKING
    assert close_attempt["open_order_truth"]["classification"] == SUSPICIOUS_ORDER_STATE
    assert close_attempt["existing_working_close_order"]["open_order_truth_suspicious_reasons"] == [
        "sentinel_filled_quantity",
        "missing_remaining_quantity",
    ]


def test_broker_flat_with_open_close_order_surfaces_from_open_order_truth(tmp_path: Path) -> None:
    config = base_config(
        tmp_path,
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="LONG",
    )
    truth = lifecycle_module._managed_close_open_order_truth(
        config=config,
        close_intent={"order_action": "SELL", "quantity": 1},
        open_orders=(_broker_order("27", "347068546"),),
        include_expected_broker_position=False,
    )

    assert truth["classification"] == BROKER_FLAT_WITH_OPEN_CLOSE_ORDER
    assert truth["summary"]["broker_flat_with_open_close_order_count"] == 1


def test_maintenance_with_no_open_orders_continues_to_close_submit(tmp_path: Path, monkeypatch) -> None:
    class NoOpenOrdersAdapter:
        def __init__(self, **_kwargs: Any) -> None: ...

        def connect(self) -> None: ...

        def disconnect(self) -> None: ...

        def managed_accounts(self) -> tuple[str, ...]:
            return ("DUM882026",)

        def require_configured_account(self) -> str:
            return "DUM882026"

        def refresh_open_orders(self, *, contract_key: str | None = None) -> tuple[BrokerOrder, ...]:
            assert contract_key == "MNQ-202606"
            return ()

        def refresh_positions(self, *, contract_key: str) -> PositionState:
            assert contract_key == "MNQ-202606"
            return PositionState(
                position_state_id="position-mnq-long",
                run_id="run",
                source=PositionSource.BROKER,
                account_id="DUM882026",
                contract_key="MNQ-202606",
                signed_quantity=1,
                average_price="28729",
                open_order_ids=(),
                observed_at=aware_now(),
            )

        def submit_limit_order(self, *, submit_attempt, order_intent) -> int:
            assert order_intent.intent_kind.value == "CLOSE"
            return 1002

        def wait_for_broker_order(self, *, submit_attempt_id: str):
            assert submit_attempt_id
            return BrokerOrder(
                broker_order_event_id="broker-order-close",
                run_id="run",
                submit_attempt_id=submit_attempt_id,
                account_id="DUM882026",
                broker_order_id="1002",
                perm_id="347068999",
                client_id=17086,
                contract_key="MNQ-202606",
                action="SELL",
                quantity=1,
                order_type="LMT",
                limit_price="28728.5",
                status="Filled",
                filled_quantity=1,
                remaining_quantity=0,
                average_fill_price="28728.5",
                observed_at=aware_now(),
            )

        def wait_for_fill(self, *, submit_attempt_id: str):
            return type(
                "Fill",
                (),
                {
                    "price": "28728.5",
                    "quantity": 1,
                    "filled_at": aware_now(),
                    "broker_order_id": "1002",
                    "perm_id": "347068999",
                    "execution_id": "exec-1002",
                },
            )()

        def submit_diagnostics(self, submit_attempt_id: str) -> dict[str, Any]:
            return {"submit_attempt_id": submit_attempt_id, "place_order_called": True}

    monkeypatch.setattr(lifecycle_module, "IbkrPaperAdapter", NoOpenOrdersAdapter)

    config = base_config(
        tmp_path,
        strategy_id="track_b_paper_execution_test_mule_v1__mnq",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="LONG",
        close_limit_price="28728.5",
        managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
        completed_5m_bars_since_entry=3,
        submit_enabled=True,
    )
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload={
            "lifecycle_id": "open-managed-existing",
            "order_action": "SELL",
            "quantity": 1,
            "close_limit_price": "28728.5",
        },
        intent_kind=IntentKind.CLOSE,
        limit_price="28728.5",
    )
    result = maintain_open_track_b_strategy_managed_paper_lifecycle(
        config=config,
        existing_lifecycle_report=open_managed_report(),
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
    assert result.report["close_submit_attempt"]["submitted"] is True
    assert result.report["close_submit_attempt"]["broker_order_id"] == "1002"
    assert (
        result.report["close_submit_attempt"]["strategy_submit_authorization"]["authorization_classification"]
        == lifecycle_module.STRATEGY_SUBMIT_AUTHORIZED
    )


def test_managed_cleanup_close_allows_prior_runtime_generation_owner(tmp_path: Path) -> None:
    config = base_config(
        tmp_path,
        strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="LONG",
        close_limit_price="28728.5",
        managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
        runtime_generation_id="runtime-generation-that-opened-position",
    )
    close_intent = {
        "lifecycle_id": "open-managed-existing",
        "trade_id": "MNQ_FIRST_BULL_SNAP_TURN_V1:open-managed-existing",
        "order_action": "SELL",
        "quantity": 1,
        "close_limit_price": "28728.5",
    }
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="28728.5",
        snapshot_overrides={
            "runtime_supervisor_classification": "SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME",
            "safe_to_start_runtime": False,
            "runtime_resume_action_policy": "QUARANTINE_OBSERVE_ONLY",
            "runtime_resume_proposed_next_runtime_generation_id": "runtime-generation-after-cleanup",
            "open_order_truth_classification": "BROKER_POSITION_WITHOUT_CLOSE_ORDER",
            "managed_order_registry_classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "position_truth_classification": "REVIEW_REQUIRED",
        },
        safe_state_overrides={
            "runtime_generation_id": "runtime-generation-after-cleanup",
            "submit_allowed": False,
            "broker_mutation_allowed": True,
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json",
        {
            "generated_at": aware_now().isoformat(),
            "classification": "PLAN_BLOCKED_STALE_EVIDENCE",
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
            "execution_enabled": False,
            "live_money_eligible": False,
            "proposed_actions": [],
        },
    )

    authorization = lifecycle_module.build_strategy_managed_submit_authorization(
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="28728.5",
        now=aware_now(),
    )

    assert authorization["classification"] == lifecycle_module.STRATEGY_SUBMIT_AUTHORIZED
    assert authorization["runtime_generation_id"] == "runtime-generation-that-opened-position"
    assert (
        authorization["runtime_resume_proposed_next_runtime_generation_id"]
        == "runtime-generation-after-cleanup"
    )
    assert authorization["pre_action_validation"]["strategy_managed_close_exact_cleanup"] is True
    assert authorization["submit_allowed"] is True


def test_managed_exit_close_authority_allows_when_entry_submit_readiness_is_blocked(tmp_path: Path) -> None:
    config = base_config(
        tmp_path,
        strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="LONG",
        close_limit_price="28728.5",
        managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
    )
    close_intent = {
        "lifecycle_id": "open-managed-existing",
        "trade_id": "MNQ_FIRST_BULL_SNAP_TURN_V1:open-managed-existing",
        "order_action": "SELL",
        "quantity": 1,
        "close_limit_price": "28728.5",
    }
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="28728.5",
        safe_state_overrides={
            "submit_allowed": False,
            "broker_mutation_allowed": True,
        },
        snapshot_overrides={
            "position_truth_classification": "REVIEW_REQUIRED",
            "managed_position_registry_classification": "OPEN_MANAGED_EXIT_DUE",
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json",
        {
            "generated_at": aware_now().isoformat(),
            "classification": "PLAN_BLOCKED_ENTRY_READINESS",
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
            "execution_enabled": False,
            "live_money_eligible": False,
            "proposed_actions": [],
        },
    )

    authorization = lifecycle_module.build_strategy_managed_submit_authorization(
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="28728.5",
        now=aware_now(),
    )

    assert authorization["authority_mode"] == lifecycle_module.MANAGED_EXIT_CLOSE_AUTHORITY
    assert authorization["classification"] == lifecycle_module.STRATEGY_SUBMIT_AUTHORIZED
    assert authorization["managed_exit_close_authority"]["allowed"] is True
    assert authorization["managed_exit_close_authority"]["requires_entry_submit_authority"] is False
    assert authorization["managed_exit_close_authority"]["requires_flat_position_state"] is False


def test_managed_exit_close_authority_blocks_unsafe_safe_state(tmp_path: Path) -> None:
    config = base_config(tmp_path)
    close_intent = {
        "lifecycle_id": "open-managed-existing",
        "trade_id": "MNQ_FIRST_BULL_SNAP_TURN_V1:open-managed-existing",
        "order_action": "SELL",
        "quantity": 1,
        "close_limit_price": "4705.1",
    }
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="4705.1",
        safe_state_overrides={"broker_mutation_allowed": False},
    )

    authorization = lifecycle_module.build_strategy_managed_submit_authorization(
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="4705.1",
        now=aware_now(),
    )

    assert authorization["classification"] == lifecycle_module.STRATEGY_SUBMIT_BLOCKED_ENTRY_EXPOSURE
    assert "SAFE_STATE_BROKER_MUTATION_NOT_ALLOWED" in authorization["reason"]


def test_managed_exit_close_authority_allows_registry_verified_close_under_guardian_hold(tmp_path: Path) -> None:
    config = base_config(
        tmp_path,
        strategy_id="mnq_us_active_participation_long",
        lane_id="mnq_us_active_participation_long",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="LONG",
        close_limit_price="30444.5",
        managed_exit_policy_id=TrackBManagedExitPolicy.US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1.value,
    )
    close_intent = {
        "lifecycle_id": "reserved-submit-mnq",
        "trade_id": "trade-d642",
        "order_action": "SELL",
        "quantity": 1,
        "close_limit_price": "30444.5",
    }
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="30444.5",
        safe_state_overrides={
            "safe_state_classification": "SAFE_STATE_HARD_HOLD",
            "submit_allowed": False,
            "broker_mutation_allowed": False,
            "entry_mutation_allowed": False,
            "managed_close_mutation_allowed": True,
            "observe_only": True,
            "tripped_limits": [
                {
                    "limit_id": "broker_position_guardian_hard_hold",
                    "classification": "SAFE_STATE_HARD_HOLD",
                }
            ],
            "close_authority_reason_codes": [],
        },
    )

    authorization = lifecycle_module.build_strategy_managed_submit_authorization(
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="30444.5",
        now=aware_now(),
    )

    assert authorization["classification"] == lifecycle_module.STRATEGY_SUBMIT_AUTHORIZED
    assert authorization["managed_exit_close_authority"]["allowed"] is True
    assert authorization["managed_exit_close_authority"]["safe_state_broker_mutation_allowed"] is False
    assert authorization["managed_exit_close_authority"]["safe_state_managed_close_mutation_allowed"] is True


def test_managed_exit_close_authority_blocks_stale_control_plane(tmp_path: Path) -> None:
    config = base_config(tmp_path, pre_action_snapshot_max_age_seconds=60)
    close_intent = {
        "lifecycle_id": "open-managed-existing",
        "trade_id": "MNQ_FIRST_BULL_SNAP_TURN_V1:open-managed-existing",
        "order_action": "SELL",
        "quantity": 1,
        "close_limit_price": "4705.1",
    }
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="4705.1",
        snapshot_overrides={"generated_at": "2026-05-06T22:00:00+00:00"},
    )

    authorization = lifecycle_module.build_strategy_managed_submit_authorization(
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="4705.1",
        now=aware_now(),
    )

    assert authorization["classification"] == lifecycle_module.STRATEGY_SUBMIT_BLOCKED_ENTRY_EXPOSURE
    assert "CONTROL_PLANE_CLOSE_AUTHORITY_STALE" in authorization["reason"]


def test_managed_exit_close_authority_blocks_missing_trade_id(tmp_path: Path) -> None:
    config = base_config(tmp_path)
    close_intent = {
        "lifecycle_id": "open-managed-existing",
        "order_action": "SELL",
        "quantity": 1,
        "close_limit_price": "4705.1",
    }
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="4705.1",
    )

    authorization = lifecycle_module.build_strategy_managed_submit_authorization(
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="4705.1",
        now=aware_now(),
    )

    assert authorization["classification"] == lifecycle_module.STRATEGY_SUBMIT_BLOCKED_ENTRY_EXPOSURE
    assert "TRADE_ID_MISSING" in authorization["reason"]


def test_managed_exit_close_authority_blocks_conflicting_close_order(tmp_path: Path) -> None:
    config = base_config(tmp_path)
    close_intent = {
        "lifecycle_id": "open-managed-existing",
        "trade_id": "MNQ_FIRST_BULL_SNAP_TURN_V1:open-managed-existing",
        "order_action": "SELL",
        "quantity": 1,
        "close_limit_price": "4705.1",
    }
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="4705.1",
        snapshot_overrides={
            "open_order_truth_classification": "DUPLICATE_CLOSE_ORDER",
            "managed_order_registry_classification": "DUPLICATE_CLOSE_ORDER",
        },
    )

    authorization = lifecycle_module.build_strategy_managed_submit_authorization(
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="4705.1",
        now=aware_now(),
    )

    assert authorization["classification"] == lifecycle_module.STRATEGY_SUBMIT_BLOCKED_ENTRY_EXPOSURE
    assert "CONFLICTING_CLOSE_ORDER" in authorization["reason"]


def test_blocked_managed_close_preserves_open_managed_lifecycle(tmp_path: Path) -> None:
    def blocked_close_submitter(
        config: TrackBStrategyManagedPaperLifecycleConfig,
        close_intent: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        return {
            "submitted": False,
            "submit_attempted": True,
            "broker_state_mutated": False,
            "review_required": True,
            "primary_blocker": "Managed-exit close authority blocked close: SAFE_STATE_BROKER_MUTATION_NOT_ALLOWED",
            "close_intent": dict(close_intent),
        }

    stages = TrackBStrategyManagedPaperLifecycleStages(
        entry_submitter=fake_stages().entry_submitter,
        exit_policy=fake_stages(close=True).exit_policy,
        close_submitter=blocked_close_submitter,
    )

    result = maintain_open_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
            instrument_family="MNQ",
            contract_key="MNQ-202606",
            local_symbol="MNQM6",
            con_id=770561201,
            side="LONG",
            close_limit_price="28728.5",
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            completed_5m_bars_since_entry=12,
            submit_enabled=True,
        ),
        existing_lifecycle_report=open_managed_report(),
        stages=stages,
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.OPEN_MANAGED
    assert result.report["final_position_status"] == "OPEN_MANAGED"
    assert result.report["review_required"] is False
    assert result.report["close_submit_attempt"]["broker_state_mutated"] is False
    assert "SAFE_STATE_BROKER_MUTATION_NOT_ALLOWED" in result.report["primary_blocker"]


def test_managed_exit_close_authority_uses_registry_reconciliation_when_phase1_gate_is_stale(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = base_config(
        tmp_path,
        strategy_id="mnq_us_active_participation_long",
        lane_id="mnq_us_active_participation_long",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="LONG",
        close_limit_price="30437.5",
        managed_exit_policy_id=TrackBManagedExitPolicy.US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1.value,
    )
    close_intent = {
        "lifecycle_id": "open-managed-existing",
        "trade_id": "MNQ_FIRST_BULL_SNAP_TURN_V1:open-managed-existing",
        "order_action": "SELL",
        "quantity": 1,
        "close_limit_price": "30437.5",
    }
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="30437.5",
    )
    monkeypatch.setattr(
        lifecycle_module,
        "evaluate_phase1_broker_reconciliation_submit_gate",
        lambda **_kwargs: {
            "ready": False,
            "classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT",
            "track_b_lifecycle_positions": [],
            "track_b_broker_positions": [
                {
                    "account_id": "DUM882026",
                    "symbol": "MNQ",
                    "local_symbol": "MNQM6",
                    "con_id": 770561201,
                    "quantity": "1",
                }
            ],
        },
    )
    _write_json(
        tmp_path / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {
            "registry_reconciliation": {
                "classification": "REGISTRY_RECONCILIATION_MATCHED",
                "blocking": False,
                "mapped_records": [
                    {
                        "trade_id": "MNQ_FIRST_BULL_SNAP_TURN_V1:open-managed-existing",
                        "lifecycle_id": "open-managed-existing",
                        "account_id": "DUM882026",
                        "instrument_family": "MNQ",
                        "local_symbol": "MNQM6",
                        "con_id": 770561201,
                        "quantity": "1",
                        "side": "LONG",
                        "lane_id": "mnq_us_active_participation_long",
                        "strategy_id": "mnq_us_active_participation_long",
                        "entry_perm_id": "347068100",
                        "entry_exec_id": "exec-1",
                    }
                ],
            },
            "track_b_broker_positions": [
                {
                    "account_id": "DUM882026",
                    "symbol": "MNQ",
                    "local_symbol": "MNQM6",
                    "con_id": 770561201,
                    "quantity": "1",
                }
            ],
        },
    )

    authorization = lifecycle_module.build_strategy_managed_submit_authorization(
        config=config,
        intent_payload=close_intent,
        intent_kind=IntentKind.CLOSE,
        limit_price="30437.5",
        now=aware_now(),
    )

    assert authorization["classification"] == lifecycle_module.STRATEGY_SUBMIT_AUTHORIZED
    assert authorization["managed_exit_close_authority"]["registry_exit_validation"]["allowed"] is True


def test_maintenance_blocks_close_when_broker_position_missing_before_submit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class MissingPositionAdapter:
        def __init__(self, **_kwargs: Any) -> None: ...

        def connect(self) -> None: ...

        def disconnect(self) -> None: ...

        def managed_accounts(self) -> tuple[str, ...]:
            return ("DUM882026",)

        def require_configured_account(self) -> str:
            return "DUM882026"

        def refresh_open_orders(self, *, contract_key: str | None = None) -> tuple[BrokerOrder, ...]:
            assert contract_key == "MNQ-202606"
            return ()

        def refresh_positions(self, *, contract_key: str) -> PositionState:
            assert contract_key == "MNQ-202606"
            raise RuntimeError("missing position callback for exact contract")

        def submit_limit_order(self, **_kwargs: Any) -> int:
            raise AssertionError("missing broker position must block managed close submit")

    monkeypatch.setattr(lifecycle_module, "IbkrPaperAdapter", MissingPositionAdapter)

    config = base_config(
        tmp_path,
        strategy_id="track_b_paper_execution_test_mule_v1__mnq",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="LONG",
        close_limit_price="28728.5",
        managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
        completed_5m_bars_since_entry=3,
        submit_enabled=True,
    )
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload={
            "lifecycle_id": "open-managed-existing",
            "order_action": "SELL",
            "quantity": 1,
            "close_limit_price": "28728.5",
        },
        intent_kind=IntentKind.CLOSE,
        limit_price="28728.5",
    )
    result = maintain_open_track_b_strategy_managed_paper_lifecycle(
        config=config,
        existing_lifecycle_report=open_managed_report(),
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
    close_attempt = result.report["close_submit_attempt"]
    assert close_attempt["classification"] == "BROKER_POSITION_NOT_OPEN_FOR_MANAGED_CLOSE"
    assert close_attempt["submitted"] is False
    assert close_attempt["broker_state_mutated"] is False


def test_maintenance_blocks_close_when_broker_position_direction_mismatches(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class WrongSidePositionAdapter:
        def __init__(self, **_kwargs: Any) -> None: ...

        def connect(self) -> None: ...

        def disconnect(self) -> None: ...

        def managed_accounts(self) -> tuple[str, ...]:
            return ("DUM882026",)

        def require_configured_account(self) -> str:
            return "DUM882026"

        def refresh_open_orders(self, *, contract_key: str | None = None) -> tuple[BrokerOrder, ...]:
            assert contract_key == "MNQ-202606"
            return ()

        def refresh_positions(self, *, contract_key: str) -> PositionState:
            assert contract_key == "MNQ-202606"
            return PositionState(
                position_state_id="position-mnq-short",
                run_id="run",
                source=PositionSource.BROKER,
                account_id="DUM882026",
                contract_key="MNQ-202606",
                signed_quantity=-1,
                average_price="28729",
                open_order_ids=(),
                observed_at=aware_now(),
            )

        def submit_limit_order(self, **_kwargs: Any) -> int:
            raise AssertionError("wrong-side broker position must block managed close submit")

    monkeypatch.setattr(lifecycle_module, "IbkrPaperAdapter", WrongSidePositionAdapter)

    config = base_config(
        tmp_path,
        strategy_id="track_b_paper_execution_test_mule_v1__mnq",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="LONG",
        close_limit_price="28728.5",
        managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
        completed_5m_bars_since_entry=3,
        submit_enabled=True,
    )
    seed_strategy_submit_authority(
        tmp_path,
        config=config,
        intent_payload={
            "lifecycle_id": "open-managed-existing",
            "order_action": "SELL",
            "quantity": 1,
            "close_limit_price": "28728.5",
        },
        intent_kind=IntentKind.CLOSE,
        limit_price="28728.5",
    )
    result = maintain_open_track_b_strategy_managed_paper_lifecycle(
        config=config,
        existing_lifecycle_report=open_managed_report(),
        now=aware_now(),
    )

    close_attempt = result.report["close_submit_attempt"]
    assert close_attempt["classification"] == "CLOSE_WOULD_INCREASE_REVERSE_EXPOSURE"
    assert close_attempt["broker_position"]["signed_quantity"] == -1
    assert close_attempt["submitted"] is False
    assert close_attempt["broker_state_mutated"] is False


def test_maintenance_close_submit_without_fill_marks_review_required(tmp_path: Path) -> None:
    def close_without_fill(_config, close_intent):
        return {
            "submitted": True,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "broker_order_id": "1002",
            "close_intent": dict(close_intent),
            "primary_blocker": "close fill callback missing",
        }

    stages = TrackBStrategyManagedPaperLifecycleStages(
        entry_submitter=fake_stages().entry_submitter,
        exit_policy=fake_stages().exit_policy,
        close_submitter=close_without_fill,
    )
    result = maintain_open_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
            instrument_family="MNQ",
            contract_key="MNQ-202606",
            local_symbol="MNQM6",
            con_id=770561201,
            side="LONG",
            entry_limit_price="28729",
            close_limit_price="28728.5",
            managed_exit_policy_id=TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value,
            completed_5m_bars_since_entry=3,
        ),
        existing_lifecycle_report=open_managed_report(),
        stages=stages,
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
    assert result.report["review_required"] is True
    assert result.report["primary_blocker"] == "close fill callback missing"


def test_missing_broker_exact_position_callback_becomes_review_required(tmp_path: Path) -> None:
    def bad_entry(
        config: TrackBStrategyManagedPaperLifecycleConfig,
        entry_intent: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        raise RuntimeError("missing position callback for exact contract")

    stages = TrackBStrategyManagedPaperLifecycleStages(
        entry_submitter=bad_entry,
        exit_policy=lambda config, open_state: None,
        close_submitter=lambda config, close_intent: {},
    )
    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            managed_exit_policy_id=TrackBManagedExitPolicy.MANAGED_HOLD_REQUIRES_EXTERNAL_EXIT_SIGNAL.value,
        ),
        stages=stages,
        lifecycle_id="review-required",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
    assert "missing position callback for exact contract" in result.report["primary_blocker"]
    assert result.report["review_required"] is True


def test_existing_unresolved_proof_run_position_blocks_new_managed_entry(tmp_path: Path) -> None:
    ledger_root = tmp_path / "ledger"
    status_json = ledger_root / "latest_track_b_live_position_status.json"
    status_json.parent.mkdir(parents=True, exist_ok=True)
    status_json.write_text(
        json.dumps(
            {
                "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
                "broker_reconciled": False,
                "review_required_count": 1,
                "positions": [
                    {
                        "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
                        "contract_key": "MGC-202606",
                        "quantity": "1",
                        "review_required": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = run_track_b_strategy_managed_paper_lifecycle(
        config=base_config(
            tmp_path,
            managed_exit_policy_id=TrackBManagedExitPolicy.MANAGED_HOLD_REQUIRES_EXTERNAL_EXIT_SIGNAL.value,
            paper_trade_ledger_output_root=ledger_root,
        ),
        stages=fake_stages(),
        lifecycle_id="blocked-existing-review",
        now=aware_now(),
    )

    assert result.classification == TrackBManagedPaperLifecycleClassification.BLOCKED_EXISTING_REVIEW_REQUIRED
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert "review-required" in result.report["primary_blocker"]
