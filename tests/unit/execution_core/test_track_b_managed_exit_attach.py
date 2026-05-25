from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_managed_exit_attach import (
    MANAGED_EXIT_APPLY_DISABLED,
    MANAGED_EXIT_BLOCKED_DUPLICATE_CLOSE_ORDER,
    MANAGED_EXIT_BLOCKED_POSITION_MISMATCH,
    MANAGED_EXIT_NOT_YET_ELIGIBLE,
    MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE,
    TrackBManagedExitAttachConfig,
    build_track_b_managed_exit_attach_plan,
)


NOW = datetime(2026, 5, 25, 7, 48, tzinfo=timezone.utc)


def test_plan_ready_when_3x5m_elapsed(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=3)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_TIMEBOX_CLOSE_ELIGIBLE
    assert payload["timebox_exit_eligible"] is True
    assert payload["apply_boundary_classification"] == MANAGED_EXIT_APPLY_DISABLED
    assert payload["close_intent_preview"]["order_action"] == "SELL"
    assert payload["close_intent_preview"]["quantity"] == 1
    assert payload["close_intent_preview"]["would_submit"] is False
    assert payload["broker_state_mutated"] is False
    assert payload["exit_roster_compatible"] is True
    assert payload["exit_strategy_id"] == "timeboxed_3x5m_managed_limit_close_v1"


def test_not_eligible_before_3x5m(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=2)

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_NOT_YET_ELIGIBLE
    assert payload["timebox_exit_eligible"] is False
    assert payload["close_intent_preview"]["submit_allowed"] is False


def test_blocked_on_position_mismatch(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=3, position_overrides={"local_symbol": "WRONG"})

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_BLOCKED_POSITION_MISMATCH
    assert payload["position_identity_verified"] is False
    assert payload["broker_state_mutated"] is False


def test_blocked_on_duplicate_close_order(tmp_path: Path) -> None:
    config = _seed(
        tmp_path,
        completed_bars=3,
        managed_order_overrides={
            "classification": "WORKING_CLOSE_ORDER",
            "managed_orders": [
                {
                    "working": True,
                    "local_symbol": "MNQM6",
                    "action": "SELL",
                    "quantity": "1",
                }
            ],
        },
    )

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_BLOCKED_DUPLICATE_CLOSE_ORDER
    assert payload["duplicate_close_order_detected"] is True


def test_apply_disabled_without_both_flags(tmp_path: Path) -> None:
    config = TrackBManagedExitAttachConfig(**{**_seed(tmp_path, completed_bars=3).__dict__, "apply": True})

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == MANAGED_EXIT_APPLY_DISABLED
    assert payload["apply_enabled"] is False
    assert payload["submit_attempted"] is False
    assert payload["broker_state_mutated"] is False


def test_no_live_money_or_paper_proof_route(tmp_path: Path) -> None:
    config = _seed(tmp_path, completed_bars=3, snapshot_overrides={"live_money_eligible": True})

    payload = build_track_b_managed_exit_attach_plan(config=config, now=NOW)

    assert payload["classification"] == "MANAGED_EXIT_BLOCKED_CONTROL_PLANE"
    assert payload["paper_only"] is True
    assert payload["paper_proof_invoked"] is False
    assert payload["broad_cancel_allowed"] is False
    assert payload["global_flatten_allowed"] is False


def _seed(
    tmp_path: Path,
    *,
    completed_bars: int,
    position_overrides: Mapping[str, Any] | None = None,
    managed_order_overrides: Mapping[str, Any] | None = None,
    snapshot_overrides: Mapping[str, Any] | None = None,
) -> TrackBManagedExitAttachConfig:
    config = TrackBManagedExitAttachConfig(repo_root=tmp_path, refresh_control_plane=False)
    lifecycle_path = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
        / config.lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle = {
        "schema_version": "track_b_strategy_managed_paper_lifecycle_v1",
        "lifecycle_id": config.lifecycle_id,
        "strategy_id": config.strategy_id,
        "instrument_family": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "account_id": config.account_id,
        "expected_account_id": config.expected_account_id,
        "mode": "PAPER",
        "managed_exit_policy_id": config.managed_exit_policy_id,
        "managed_exit_policy_max_completed_5m_bars": config.required_completed_5m_bars,
        "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
        "final_position_status": "OPEN_MANAGED",
        "entry_intent": {
            "side": "LONG",
            "quantity": 1,
            "managed_exit_policy_id": config.managed_exit_policy_id,
        },
        "entry_fill": {
            "price": "29923.75",
            "quantity": "1",
            "filled_at": "2026-05-25T07:26:12+00:00",
            "broker_order_id": "33",
        },
    }
    _write_json(lifecycle_path, lifecycle)
    position = {
        "account_id": config.account_id,
        "instrument_family": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "quantity": "1",
        "side": "LONG",
        "strategy_id": config.strategy_id,
        "lifecycle_id": config.lifecycle_id,
        "entry_timestamp": "2026-05-25T07:26:12+00:00",
        "managed_exit_policy_id": config.managed_exit_policy_id,
        "paper_lifecycle_report_path": str(lifecycle_path),
    }
    position.update(dict(position_overrides or {}))
    _write_json(
        tmp_path / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json",
        {
            "schema_version": "track_b_live_position_status_v1",
            "open_position_count": 1,
            "open_order_count": 0,
            "positions_by_instrument": {config.contract_key: position},
            "source_artifact_paths": [str(lifecycle_path)],
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
            "shared_truth_coherence_status": "COHERENT",
            "runtime_supervisor_classification": "SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME",
            "open_order_truth_classification": "BROKER_POSITION_WITHOUT_CLOSE_ORDER",
            "managed_order_registry_classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            **dict(snapshot_overrides or {}),
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json",
        {
            "safe_state_classification": "SAFE_STATE_NORMAL",
            "submit_allowed": False,
            "broker_mutation_allowed": True,
            "observe_only": False,
            "tripped_limits": [],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {"classification": "BROKER_POSITION_WITHOUT_CLOSE_ORDER"},
    )
    position_truth_position = {
        "account_id": config.account_id,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "quantity": "1",
        "symbol": "MNQ",
    }
    position_truth_position.update(dict(position_overrides or {}))
    _write_json(
        tmp_path / "outputs/track_b_execution_core/position_truth/latest_position_truth.json",
        {"broker_positions": [position_truth_position], "live_money_eligible": False, "paper_proof_invoked": False},
    )
    managed_orders = {
        "classification": "POSITION_WITHOUT_CLOSE_ORDER",
        "managed_orders": [
            {
                "working": False,
                "local_symbol": config.local_symbol,
                "action": "SELL",
                "quantity": "1",
                "classification": "POSITION_WITHOUT_CLOSE_ORDER",
            }
        ],
    }
    managed_orders.update(dict(managed_order_overrides or {}))
    _write_json(tmp_path / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json", managed_orders)
    bars = [
        {"bar_end": f"2026-05-25T07:{30 + idx * 5:02d}:00+00:00", "close": "29960"}
        for idx in range(completed_bars)
    ]
    _write_json(
        tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data/MNQ/5m/latest_runtime_candles.json",
        {"bars": bars},
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data/MNQ/1m/latest_runtime_candles.json",
        {"bars": [{"bar_end": "2026-05-25T07:47:00+00:00", "close": "29965.5"}]},
    )
    return config


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
