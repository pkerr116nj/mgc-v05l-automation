from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import (
    TrackBManagedExitPolicy,
    TrackBManagedPaperLifecycleClassification,
    TrackBStrategyManagedPaperLifecycleConfig,
    TrackBStrategyManagedPaperLifecycleStages,
    maintain_open_track_b_strategy_managed_paper_lifecycle,
    run_track_b_strategy_managed_paper_lifecycle,
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
    }
    payload.update(overrides)
    return TrackBStrategyManagedPaperLifecycleConfig(**payload)


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
        time_boxed_ready = (
            config.managed_exit_policy_id == TrackBManagedExitPolicy.PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1.value
            and int(config.completed_5m_bars_since_entry or 0) >= int(config.managed_exit_policy_max_completed_5m_bars)
        )
        if not close and not time_boxed_ready:
            return None
        return {
            "order_action": "SELL",
            "quantity": 1,
            "close_limit_price": "4705.1",
            "close_reason": "TIME_BOXED_EXIT" if time_boxed_ready else "DIAGNOSTIC_TIME_EXIT_IMMEDIATE",
            "elapsed_completed_5m_bars": config.completed_5m_bars_since_entry,
            "required_completed_5m_bars": config.managed_exit_policy_max_completed_5m_bars,
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
