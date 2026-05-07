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
