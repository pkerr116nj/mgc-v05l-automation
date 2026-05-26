from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.track_b_managed_exit_order_resolution import (
    KNOWN_MANAGED_EXIT_ORDER_DISAPPEARED_REVIEW_REQUIRED,
    KNOWN_MANAGED_EXIT_ORDER_EXPIRED_OR_GONE_POSITION_STILL_OPEN,
    KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP,
    KNOWN_MANAGED_EXIT_ORDER_PARTIAL_FILL_REVIEW_REQUIRED,
    ManagedExitOrderResolutionConfig,
    resolve_known_managed_exit_order_disappearance,
)
from mgc_v05l.execution_core.track_b_paper_trade_ledger import update_track_b_paper_trade_ledger_from_filled_bridge_result


NOW = datetime(2026, 5, 15, 11, 30, tzinfo=timezone.utc)
LIFECYCLE_ID = "bridge_fill_GC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN"


def test_known_managed_exit_disappears_and_broker_flat_persists_lifecycle_close(tmp_path: Path) -> None:
    _seed_open_gc_position(tmp_path)

    report = resolve_known_managed_exit_order_disappearance(
        config=_config(tmp_path, apply=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
    )

    assert report["classification"] == KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP
    assert report["lifecycle_close"]["persisted"] is True
    live_status = _load_json(tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json")
    assert live_status["open_position_count"] == 0
    close_record = report["lifecycle_close"]["trade_record"]
    assert close_record["final_position_status"] == "CLOSED_FLAT"
    assert close_record["exit_order_id"] == "1"
    assert close_record["exit_fill_price"] is None


def test_resolver_close_payload_uses_known_order_lane_when_lifecycle_row_lacks_lane(tmp_path: Path) -> None:
    _seed_open_gc_position(tmp_path)

    lifecycle_without_lane = dict(_gc_lifecycle_position())
    lifecycle_without_lane.pop("lane_id", None)
    report = resolve_known_managed_exit_order_disappearance(
        config=_config(tmp_path, apply=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(
            lifecycle_positions=[lifecycle_without_lane],
        ),
    )

    assert report["classification"] == KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP
    close_record = report["lifecycle_close"]["trade_record"]
    assert close_record["lane_id"] == "gc_1x_all_lanes__london_early_long"
    assert close_record["exit_fill_confirmed"] is True


def test_broker_flat_aggregate_lifecycle_group_persists_all_unit_closes(tmp_path: Path) -> None:
    unit_1 = "bridge_fill_GC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN|unit1"
    unit_2 = "bridge_fill_GC|1m|2026-05-15T07:07:00Z|BUY_TO_OPEN|unit2"
    _seed_open_gc_position_with_intent(tmp_path, order_intent_id=unit_1.removeprefix("bridge_fill_"), broker_order_id="10")
    _seed_open_gc_position_with_intent(tmp_path, order_intent_id=unit_2.removeprefix("bridge_fill_"), broker_order_id="11")

    aggregate = {
        **_gc_lifecycle_position(),
        "lifecycle_id": unit_2,
        "quantity": "2",
        "aggregate_qty": "2",
        "lifecycle_unit_count": 2,
        "lifecycle_ids": [unit_1, unit_2],
        "lifecycle_units": [
            {**_gc_lifecycle_position(), "lifecycle_id": unit_1, "entry_intent_id": unit_1, "quantity": "1", "entry_order_id": "10"},
            {**_gc_lifecycle_position(), "lifecycle_id": unit_2, "entry_intent_id": unit_2, "quantity": "1", "entry_order_id": "11"},
        ],
    }
    known_order = {**_known_order(), "lifecycle_id": unit_2, "broker_order_id": "12", "quantity": "2"}

    report = resolve_known_managed_exit_order_disappearance(
        config=_config(tmp_path, lifecycle_id=unit_2, broker_order_id="12", apply=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(
            lifecycle_positions=[aggregate],
            known_orders=[known_order],
        ),
    )

    assert report["classification"] == KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP
    assert report["aggregate_lifecycle_close_persistence"]["lifecycle_count"] == 2
    assert report["lifecycle_close"]["persisted_count"] == 2
    live_status = _load_json(tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json")
    assert live_status["open_position_count"] == 0


def test_disappeared_order_with_broker_still_open_marks_pending_not_filled(tmp_path: Path) -> None:
    _seed_open_gc_position(tmp_path)

    report = resolve_known_managed_exit_order_disappearance(
        config=_config(tmp_path, apply=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(
            broker_positions=[_broker_gc_position(quantity="1.0")],
        ),
    )

    assert report["classification"] == KNOWN_MANAGED_EXIT_ORDER_EXPIRED_OR_GONE_POSITION_STILL_OPEN
    live_status = _load_json(tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json")
    assert live_status["open_position_count"] == 1


def test_disappeared_order_with_partial_broker_position_requires_review(tmp_path: Path) -> None:
    _seed_open_gc_position(tmp_path)

    report = resolve_known_managed_exit_order_disappearance(
        config=_config(tmp_path),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(
            broker_positions=[_broker_gc_position(quantity="0.5")],
        ),
    )

    assert report["classification"] == KNOWN_MANAGED_EXIT_ORDER_PARTIAL_FILL_REVIEW_REQUIRED


def test_unknown_order_disappearance_does_not_auto_close_lifecycle(tmp_path: Path) -> None:
    _seed_open_gc_position(tmp_path)

    report = resolve_known_managed_exit_order_disappearance(
        config=_config(tmp_path, client_id=None, perm_id=None, apply=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(known_orders=[]),
    )

    assert report["classification"] == KNOWN_MANAGED_EXIT_ORDER_DISAPPEARED_REVIEW_REQUIRED
    live_status = _load_json(tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json")
    assert live_status["open_position_count"] == 1


def test_managed_exit_attach_artifact_allows_missing_perm_id_when_broker_flat(tmp_path: Path) -> None:
    _seed_open_gc_position(tmp_path)
    attach_path = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "managed_exit_attach"
        / "latest_managed_exit_attach_plan.json"
    )
    attach_path.parent.mkdir(parents=True, exist_ok=True)
    attach_path.write_text(
        json.dumps(
            {
                "lifecycle_id": LIFECYCLE_ID,
                "strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
                "lane_id": "gc_1x_all_lanes__london_early_long",
                "symbol": "GC",
                "local_symbol": "GCM6",
                "con_id": 430360630,
                "close_intent_preview": {
                    "order_action": "SELL",
                    "quantity": 1,
                    "local_symbol": "GCM6",
                    "con_id": 430360630,
                    "close_limit_price": "4571.0",
                    "close_reason": "TIME_BOXED_EXIT",
                },
                "apply_result": {
                    "close_submit_attempt": {
                        "broker_order_id": "2",
                        "client_id": 10815,
                        "adapter_exception": "missing execDetails callback",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    report = resolve_known_managed_exit_order_disappearance(
        config=_config(tmp_path, broker_order_id="2", client_id=10815, perm_id=None),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(known_orders=[]),
    )

    assert report["classification"] == KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP
    assert report["known_exit_order"]["source"] == "TRACK_B_MANAGED_EXIT_ATTACH_APPLY_ARTIFACT"
    assert report["filled_bridge_result"]["broker_order_id"] == "2"
    assert report["filled_bridge_result"]["quantity"] == "1"


def test_managed_exit_attach_artifact_matches_remaining_aggregate_unit(tmp_path: Path) -> None:
    unit_1 = "bridge_fill_GC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN|unit1"
    unit_2 = "bridge_fill_GC|1m|2026-05-15T07:07:00Z|BUY_TO_OPEN|unit2"
    _seed_open_gc_position_with_intent(tmp_path, order_intent_id=unit_1.removeprefix("bridge_fill_"), broker_order_id="10")
    _seed_open_gc_position_with_intent(tmp_path, order_intent_id=unit_2.removeprefix("bridge_fill_"), broker_order_id="11")
    attach_path = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "managed_exit_attach"
        / "latest_managed_exit_attach_plan.json"
    )
    attach_path.parent.mkdir(parents=True, exist_ok=True)
    attach_path.write_text(
        json.dumps(
            {
                "aggregate_exit_group": {"lifecycle_ids": [unit_1, unit_2], "unit_count": 2, "aggregate_close_quantity": "2"},
                "close_intent_preview": {
                    "lifecycle_id": unit_2,
                    "order_action": "SELL",
                    "quantity": 2,
                    "local_symbol": "GCM6",
                    "con_id": 430360630,
                },
                "apply_result": {
                    "close_intent": {"lifecycle_id": unit_2, "order_action": "SELL", "quantity": 2},
                    "close_submit_attempt": {"broker_order_id": "12", "client_id": 10815},
                },
            }
        ),
        encoding="utf-8",
    )
    remaining_aggregate = {
        **_gc_lifecycle_position(),
        "lifecycle_id": unit_1,
        "quantity": "1",
        "lifecycle_unit_count": 1,
        "lifecycle_units": [
            {**_gc_lifecycle_position(), "lifecycle_id": unit_1, "entry_intent_id": unit_1, "quantity": "1", "entry_order_id": "10"},
        ],
    }

    report = resolve_known_managed_exit_order_disappearance(
        config=_config(tmp_path, lifecycle_id=unit_1, broker_order_id="12", client_id=10815, perm_id=None, apply=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(
            lifecycle_positions=[remaining_aggregate],
            known_orders=[],
        ),
    )

    assert report["classification"] == KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP
    assert report["known_exit_order"]["source"] == "TRACK_B_MANAGED_EXIT_ATTACH_APPLY_ARTIFACT"
    assert report["lifecycle_close"]["persisted_count"] == 1


def test_mismatched_contract_identity_refuses_resolution(tmp_path: Path) -> None:
    _seed_open_gc_position(tmp_path)

    report = resolve_known_managed_exit_order_disappearance(
        config=_config(tmp_path, local_symbol="MGCM6"),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(),
    )

    assert report["classification"] == KNOWN_MANAGED_EXIT_ORDER_DISAPPEARED_REVIEW_REQUIRED
    assert "local_symbol mismatch" in report["detail"]


def test_mgc_position_is_unaffected_by_gc_resolution(tmp_path: Path) -> None:
    _seed_open_gc_position(tmp_path)
    _seed_open_mgc_position(tmp_path)

    report = resolve_known_managed_exit_order_disappearance(
        config=_config(tmp_path, apply=True),
        now=NOW,
        reconciliation_runner=lambda _config: _reconciliation_report(
            broker_positions=[_broker_mgc_position()],
            lifecycle_positions=[_gc_lifecycle_position(), _mgc_lifecycle_position()],
        ),
    )

    assert report["classification"] == KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP
    live_status = _load_json(tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json")
    assert live_status["open_position_count"] == 1
    assert "MGC-202606" in live_status["positions_by_instrument"]
    assert "GC-202606" not in live_status["positions_by_instrument"]


def _config(tmp_path: Path, **overrides: Any) -> ManagedExitOrderResolutionConfig:
    payload = {
        "repo_root": tmp_path,
        "lifecycle_id": LIFECYCLE_ID,
        "broker_order_id": "1",
        "client_id": 10815,
        "perm_id": 614029377,
        "symbol": "GC",
        "local_symbol": "GCM6",
        "con_id": 430360630,
    }
    payload.update(overrides)
    return ManagedExitOrderResolutionConfig(**payload)


def _seed_open_gc_position(tmp_path: Path) -> None:
    _seed_open_gc_position_with_intent(
        tmp_path,
        order_intent_id="GC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN",
        broker_order_id="1",
    )


def _seed_open_gc_position_with_intent(tmp_path: Path, *, order_intent_id: str, broker_order_id: str) -> None:
    update_track_b_paper_trade_ledger_from_filled_bridge_result(
        filled_bridge_result={
            "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
            "strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
            "lane_id": "gc_1x_all_lanes__london_early_long",
            "instrument": "GC",
            "symbol": "GC",
            "action": "BUY",
            "quantity": 1,
            "order_intent_id": order_intent_id,
            "intent_type": "BUY_TO_OPEN",
            "broker_order_id": broker_order_id,
            "account_id": None,
            "perm_id": 614029365,
            "client_id": 10892,
            "exec_id": "0000e1a7.6a0ce92d.01.01",
            "local_symbol": "GCM6",
            "con_id": 430360630,
            "contract": {"symbol": "GC", "local_symbol": "GCM6", "expiry": "202606", "multiplier": "100"},
            "managed_exit_policy_id": "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1",
            "fill_price": "4574.6",
            "fill_timestamp": "2026-05-15T07:06:24.040216+00:00",
            "paper_proof_invoked": False,
            "live_money_readiness": False,
        },
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger",
        now=NOW,
    )


def _seed_open_mgc_position(tmp_path: Path) -> None:
    update_track_b_paper_trade_ledger_from_filled_bridge_result(
        filled_bridge_result={
            "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
            "strategy_id": "gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__london_early_long",
            "lane_id": "mgc_1x_all_lanes__london_early_long",
            "instrument": "MGC",
            "symbol": "MGC",
            "action": "BUY",
            "quantity": 1,
            "order_intent_id": "MGC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN",
            "intent_type": "BUY_TO_OPEN",
            "broker_order_id": "1",
            "account_id": None,
            "perm_id": 614029371,
            "client_id": 10817,
            "exec_id": "0000e1a7.6a0ce935.01.01",
            "local_symbol": "MGCM6",
            "con_id": 712565978,
            "contract": {"symbol": "MGC", "local_symbol": "MGCM6", "expiry": "202606", "multiplier": "10"},
            "managed_exit_policy_id": "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1",
            "fill_price": "4574.3",
            "fill_timestamp": "2026-05-15T07:06:27.957789+00:00",
            "paper_proof_invoked": False,
            "live_money_readiness": False,
        },
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger",
        now=NOW,
    )


def _reconciliation_report(
    *,
    broker_positions: list[dict[str, Any]] | None = None,
    lifecycle_positions: list[dict[str, Any]] | None = None,
    known_orders: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_broker_positions = [] if broker_positions is None else broker_positions
    actual_lifecycle_positions = [_gc_lifecycle_position()] if lifecycle_positions is None else lifecycle_positions
    return {
        "classification": "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
        "broker_reconciled": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "review_required_count": 0,
        "track_b_broker_open_order_count": 0,
        "unknown_broker_open_order_count": 0,
        "track_b_broker_open_orders": [],
        "track_b_broker_position_count": len([row for row in actual_broker_positions if row.get("quantity") != "0"]),
        "track_b_broker_positions": actual_broker_positions,
        "lifecycle_open_position_count": len(actual_lifecycle_positions),
        "track_b_lifecycle_positions": actual_lifecycle_positions,
        "known_managed_exit_order_count": len([_known_order()] if known_orders is None else known_orders),
        "known_managed_exit_orders": [_known_order()] if known_orders is None else known_orders,
    }


def _known_order() -> dict[str, Any]:
    return {
        "source": "TRACK_B_LIFECYCLE_PENDING_EXIT_ORDER",
        "lifecycle_id": LIFECYCLE_ID,
        "strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
        "lane_id": "gc_1x_all_lanes__london_early_long",
        "order_intent_id": "GC|1m|2026-05-15T08:06:00Z|SELL_TO_CLOSE",
        "broker_order_id": "1",
        "client_id": 10815,
        "perm_id": 614029377,
        "symbol": "GC",
        "local_symbol": "GCM6",
        "con_id": 430360630,
        "action": "SELL",
        "quantity": "1",
        "fill_timestamp": "2026-05-15T10:55:11.358660+00:00",
    }


def _gc_lifecycle_position() -> dict[str, Any]:
    return {
        "lifecycle_id": LIFECYCLE_ID,
        "strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
        "lane_id": "gc_1x_all_lanes__london_early_long",
        "track_b_root": "GC",
        "instrument_family": "GC",
        "contract_key": "GC-202606",
        "local_symbol": "GCM6",
        "con_id": 430360630,
        "quantity": "1",
        "side": "LONG",
        "account_id": None,
    }


def _mgc_lifecycle_position() -> dict[str, Any]:
    return {
        "lifecycle_id": "bridge_fill_MGC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN",
        "strategy_id": "gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__london_early_long",
        "lane_id": "mgc_1x_all_lanes__london_early_long",
        "track_b_root": "MGC",
        "instrument_family": "MGC",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "quantity": "1",
        "side": "LONG",
        "account_id": None,
    }


def _broker_gc_position(*, quantity: str) -> dict[str, Any]:
    return {
        "account_id": "DUM882026",
        "symbol": "GC",
        "track_b_root": "GC",
        "local_symbol": "GCM6",
        "expiry": "20260626",
        "con_id": 430360630,
        "quantity": quantity,
    }


def _broker_mgc_position() -> dict[str, Any]:
    return {
        "account_id": "DUM882026",
        "symbol": "MGC",
        "track_b_root": "MGC",
        "local_symbol": "MGCM6",
        "expiry": "20260626",
        "con_id": 712565978,
        "quantity": "1.0",
    }


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
