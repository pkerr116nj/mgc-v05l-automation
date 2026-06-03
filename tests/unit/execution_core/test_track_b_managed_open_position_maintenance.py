from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

import pytest

from mgc_v05l.execution_core.track_b_managed_open_position_maintenance import (
    TrackBManagedOpenPositionMaintenanceConfig,
    run_track_b_managed_open_position_maintenance,
)
from mgc_v05l.execution_core.track_b_position_management_manifest import OPEN_MANAGED_METADATA_INCOMPLETE
from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import (
    TrackBStrategyManagedPaperLifecycleConfig,
    TrackBStrategyManagedPaperLifecycleStages,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 7, 16, 45, tzinfo=timezone.utc)


def write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def lifecycle_payload(
    *,
    entry_filled_at: str = "2026-05-07T16:26:07+00:00",
    signal_timestamp: str | None = None,
    strategy_id: str = "MNQ_FIRST_BULL_SNAP_TURN_V1",
    instrument: str = "MNQ",
    contract_key: str = "MNQ-202606",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
    side: str = "LONG",
    entry_price: str = "28729",
) -> dict[str, Any]:
    lifecycle_id = "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
    order_action = "BUY" if side == "LONG" else "SELL"
    return {
        "schema_version": "track_b_strategy_managed_paper_lifecycle_v1",
        "lifecycle_id": lifecycle_id,
        "trade_id": f"{strategy_id}:{lifecycle_id}",
        "strategy_id": strategy_id,
        "instrument_family": instrument,
        "contract_key": contract_key,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "account_id": "DUM882026",
        "expected_account_id": "DUM882026",
        "mode": "PAPER",
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        "managed_exit_policy_max_completed_5m_bars": 3,
        "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
        "strategy_managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
        "final_position_status": "OPEN_MANAGED",
        "entry_intent": {
            "lifecycle_id": lifecycle_id,
            "trade_id": f"{strategy_id}:{lifecycle_id}",
            "strategy_id": strategy_id,
            "instrument_family": instrument,
            "contract_key": contract_key,
            "local_symbol": local_symbol,
            "con_id": con_id,
            "account_id": "DUM882026",
            "expected_account_id": "DUM882026",
            "side": side,
            "order_action": order_action,
            "quantity": 1,
            "signal_timestamp": signal_timestamp,
            "decision_bar_timestamp": signal_timestamp,
            "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
            "entry_limit_price": entry_price,
            "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
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
            "price": entry_price,
            "quantity": "1",
            "filled_at": entry_filled_at,
        },
        "close_intent": None,
        "close_submit_attempt": None,
        "close_fill": None,
        "review_required": False,
        "broker_reconciled": False,
        "report_json_path": "unused",
    }


def seed_open_position(
    tmp_path: Path,
    *,
    completed_timestamps: list[str],
    entry_filled_at: str = "2026-05-07T16:26:07+00:00",
    signal_timestamp: str | None = None,
    latest_1m_age_seconds: float | None = None,
    strategy_id: str = "MNQ_FIRST_BULL_SNAP_TURN_V1",
    instrument: str = "MNQ",
    contract_key: str = "MNQ-202606",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
    side: str = "LONG",
    entry_price: str = "28729",
) -> TrackBManagedOpenPositionMaintenanceConfig:
    lifecycle_id = "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
    lifecycle_path = (
        tmp_path
        / "managed"
        / lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    payload = lifecycle_payload(
        entry_filled_at=entry_filled_at,
        signal_timestamp=signal_timestamp,
        strategy_id=strategy_id,
        instrument=instrument,
        contract_key=contract_key,
        local_symbol=local_symbol,
        con_id=con_id,
        side=side,
        entry_price=entry_price,
    )
    payload["report_json_path"] = str(lifecycle_path)
    payload["latest_report_json_path"] = str(tmp_path / "managed" / "latest_track_b_strategy_managed_paper_lifecycle_report.json")
    write_json(lifecycle_path, payload)
    write_json(
        tmp_path / "ledger" / "latest_track_b_live_position_status.json",
        {
            "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
            "broker_reconciled": False,
            "open_position_count": 1,
            "open_order_count": 0,
            "positions_by_instrument": {
                contract_key: {
                    "lifecycle_id": lifecycle_id,
                    "account_id": "DUM882026",
                    "strategy_id": strategy_id,
                    "instrument_family": instrument,
                    "contract_key": contract_key,
                    "local_symbol": local_symbol,
                    "con_id": con_id,
                    "side": side,
                    "quantity": "1",
                    "avg_entry_price": entry_price,
                    "entry_timestamp": entry_filled_at,
                    "signal_timestamp": signal_timestamp,
                    "entry_order_id": "11",
                    "entry_perm_id": "194800011",
                    "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
                    "entry_broker_identity": {
                        "broker_order_id": "11",
                        "perm_id": "194800011",
                        "exec_id": "exec-1",
                        "con_id": con_id,
                        "local_symbol": local_symbol,
                    },
                    "review_required": False,
                }
            },
            "source_artifact_paths": [str(lifecycle_path)],
            "review_required_positions": [],
        },
    )
    write_json(
        tmp_path / "ledger" / "latest_track_b_paper_trade_summary.json",
        {"recent_trades": [], "open_position_count": 1, "review_required_count": 0},
    )
    signed_qty = "-1" if side == "SHORT" else "1"
    write_json(
        tmp_path / "managed_positions" / "latest_managed_positions.json",
        {
            "schema_version": "track_b_managed_position_registry_v1",
            "generated_at": aware_now().isoformat(),
            "classification": "OPEN_MANAGED_MATCHED",
            "positions": [
                {
                    "classification": "OPEN_MANAGED_MATCHED",
                    "trade_id": payload["trade_id"],
                    "lifecycle_id": lifecycle_id,
                    "lane_id": strategy_id,
                    "strategy_id": strategy_id,
                    "symbol": instrument,
                    "instrument_family": instrument,
                    "contract_key": contract_key,
                    "local_symbol": local_symbol,
                    "con_id": con_id,
                    "account_id": "DUM882026",
                    "side": side,
                    "quantity": "1",
                    "aggregate_qty": signed_qty,
                    "entry_time": entry_filled_at,
                    "entry_price": entry_price,
                    "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
                    "broker_qty_match": True,
                    "reconciliation_status": "OPEN_MANAGED_MATCHED",
                    "entry_order_ids": ["11"],
                    "entry_perm_ids": ["194800011"],
                    "entry_exec_ids": ["exec-1"],
                    "paper_lifecycle_report_path": str(lifecycle_path),
                    "broker_position": {
                        "account_id": "DUM882026",
                        "symbol": instrument,
                        "local_symbol": local_symbol,
                        "con_id": con_id,
                        "quantity": signed_qty,
                    },
                    "lifecycle_position": {
                        "trade_id": payload["trade_id"],
                        "lifecycle_id": lifecycle_id,
                        "lane_id": strategy_id,
                        "strategy_id": strategy_id,
                        "account_id": "DUM882026",
                        "instrument_family": instrument,
                        "contract_key": contract_key,
                        "local_symbol": local_symbol,
                        "con_id": con_id,
                        "quantity": "1",
                        "aggregate_qty": signed_qty,
                        "side": side,
                        "avg_entry_price": entry_price,
                        "entry_timestamp": entry_filled_at,
                        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
                        "entry_order_ids": ["11"],
                        "entry_perm_ids": ["194800011"],
                        "entry_exec_ids": ["exec-1"],
                        "paper_lifecycle_report_path": str(lifecycle_path),
                    },
                }
            ],
        },
    )
    write_json(
        tmp_path / "live" / f"latest_live_{instrument.lower()}_completed_5m_candles.json",
        {
            "candles": [{"candle_timestamp": ts, "close": "28720"} for ts in completed_timestamps],
            "bars_available": len(completed_timestamps),
        },
    )
    write_json(
        tmp_path / "live" / f"latest_live_{instrument.lower()}_1m_candles.json",
        {
            "latest_1m_age_seconds": latest_1m_age_seconds,
            "bars": [
                {
                    "candle_timestamp": completed_timestamps[-1] if completed_timestamps else "2026-05-07T16:30:00+00:00",
                    "close": "28720",
                }
            ],
        },
    )
    return TrackBManagedOpenPositionMaintenanceConfig(
        mode="PAPER",
        submit_enabled=True,
        live_runtime_feed_output_root=tmp_path / "live",
        managed_lifecycle_output_root=tmp_path / "managed",
        paper_trade_ledger_output_root=tmp_path / "ledger",
        paper_trade_summary_json=tmp_path / "ledger" / "latest_track_b_paper_trade_summary.json",
        live_position_status_json=tmp_path / "ledger" / "latest_track_b_live_position_status.json",
        managed_position_projection_json=tmp_path / "managed_positions" / "latest_managed_positions.json",
        diagnostic_json=tmp_path / "diagnostics" / "latest_track_b_managed_open_position_maintenance.json",
    )


def fake_close_stages() -> TrackBStrategyManagedPaperLifecycleStages:
    def entry_submitter(_config: TrackBStrategyManagedPaperLifecycleConfig, _entry_intent: Mapping[str, Any]) -> Mapping[str, Any]:
        raise AssertionError("maintenance must not submit another entry")

    def close_submitter(_config: TrackBStrategyManagedPaperLifecycleConfig, close_intent: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            "submitted": True,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "broker_order_id": "12",
            "submitted_at": "2026-05-07T16:45:02+00:00",
            "close_intent": dict(close_intent),
            "close_fill": {
                "broker_order_id": "12",
                "execution_id": "exec-close",
                "price": "28719.5",
                "quantity": "1",
                "filled_at": "2026-05-07T16:45:04+00:00",
            },
        }

    from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import default_managed_lifecycle_stages

    defaults = default_managed_lifecycle_stages()
    return TrackBStrategyManagedPaperLifecycleStages(
        entry_submitter=entry_submitter,
        exit_policy=defaults.exit_policy,
        close_submitter=close_submitter,
    )


def _add_duplicate_open_lifecycle(
    *,
    tmp_path: Path,
    cfg: TrackBManagedOpenPositionMaintenanceConfig,
    original_lifecycle_id: str,
    duplicate_lifecycle_id: str,
    strategy_id: str,
) -> None:
    original_path = (
        tmp_path
        / "managed"
        / original_lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    duplicate_path = (
        tmp_path
        / "managed"
        / duplicate_lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    payload = json.loads(original_path.read_text(encoding="utf-8"))
    duplicate_trade_id = f"{strategy_id}:{duplicate_lifecycle_id}"
    payload["lifecycle_id"] = duplicate_lifecycle_id
    payload["trade_id"] = duplicate_trade_id
    payload["strategy_id"] = strategy_id
    payload["report_json_path"] = str(duplicate_path)
    payload["entry_intent"]["lifecycle_id"] = duplicate_lifecycle_id
    payload["entry_intent"]["trade_id"] = duplicate_trade_id
    payload["entry_intent"]["strategy_id"] = strategy_id
    payload["entry_submit_attempt"]["broker_order_id"] = "22"
    payload["entry_submit_attempt"]["perm_id"] = "194800022"
    payload["entry_fill"]["broker_order_id"] = "22"
    payload["entry_fill"]["perm_id"] = "194800022"
    payload["entry_fill"]["execution_id"] = "exec-duplicate"
    write_json(duplicate_path, payload)

    live_status_path = cfg.live_position_status_json
    live_status = json.loads(live_status_path.read_text(encoding="utf-8"))
    original_position = dict(live_status["positions_by_instrument"]["MES-202606"])
    original_position.update(
        {
            "lifecycle_id": duplicate_lifecycle_id,
            "trade_id": duplicate_trade_id,
            "strategy_id": strategy_id,
            "entry_order_id": "22",
            "entry_perm_id": "194800022",
            "entry_broker_identity": {
                "broker_order_id": "22",
                "perm_id": "194800022",
                "exec_id": "exec-duplicate",
                "con_id": 770561194,
                "local_symbol": "MESM6",
            },
        }
    )
    live_status["positions_by_instrument"]["MES-202606-duplicate"] = original_position
    live_status["source_artifact_paths"].append(str(duplicate_path))
    write_json(live_status_path, live_status)


def test_open_managed_position_age_two_keeps_waiting(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        entry_filled_at="2026-05-07T16:31:07+00:00",
        completed_timestamps=[
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
        ],
    )

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["completed_bars_since_entry"] == 2
    assert position["exit_eligible"] is False
    assert position.get("close_intent_created") is not True
    assert position.get("close_submitted") is not True


def test_missing_exit_policy_recovers_from_lane_registry(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        completed_timestamps=[
            "2026-05-07T16:30:00+00:00",
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
        ],
    )
    lifecycle_path = tmp_path / "managed" / "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b" / "track_b_strategy_managed_paper_lifecycle_report.json"
    payload = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    payload["managed_exit_policy_id"] = None
    payload["entry_intent"]["managed_exit_policy_id"] = None
    lifecycle_path.write_text(json.dumps(payload), encoding="utf-8")
    registry = tmp_path / "paper_config_in_force.json"
    write_json(
        registry,
        {
            "lanes": [
                {
                    "lane_id": "mnq_1x_ny_early_core__us_midday_long",
                    "standalone_strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
                    "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
                }
            ]
        },
    )
    cfg = TrackBManagedOpenPositionMaintenanceConfig(
        **{**cfg.__dict__, "lane_registry_paths": (registry,)}
    )

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert position["close_intent_created"] is True
    assert position["close_submitted"] is True


def test_missing_exit_policy_is_incomplete_and_does_not_submit(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        completed_timestamps=[
            "2026-05-07T16:30:00+00:00",
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
        ],
    )
    lifecycle_path = tmp_path / "managed" / "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b" / "track_b_strategy_managed_paper_lifecycle_report.json"
    payload = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    payload["managed_exit_policy_id"] = None
    payload["entry_intent"]["managed_exit_policy_id"] = None
    payload["strategy_id"] = "UNKNOWN_STRATEGY_WITHOUT_POLICY"
    lifecycle_path.write_text(json.dumps(payload), encoding="utf-8")
    live_status_path = tmp_path / "ledger" / "latest_track_b_live_position_status.json"
    live_status = json.loads(live_status_path.read_text(encoding="utf-8"))
    live_status["positions_by_instrument"]["MNQ-202606"]["managed_exit_policy_id"] = None
    live_status["positions_by_instrument"]["MNQ-202606"]["strategy_id"] = "UNKNOWN_STRATEGY_WITHOUT_POLICY"
    live_status_path.write_text(json.dumps(live_status), encoding="utf-8")
    managed_positions = json.loads(cfg.managed_position_projection_json.read_text(encoding="utf-8"))
    managed_positions["positions"][0]["managed_exit_policy_id"] = None
    managed_positions["positions"][0]["strategy_id"] = "UNKNOWN_STRATEGY_WITHOUT_POLICY"
    managed_positions["positions"][0]["lane_id"] = "UNKNOWN_STRATEGY_WITHOUT_POLICY"
    managed_positions["positions"][0]["lifecycle_position"]["managed_exit_policy_id"] = None
    managed_positions["positions"][0]["lifecycle_position"]["strategy_id"] = "UNKNOWN_STRATEGY_WITHOUT_POLICY"
    cfg.managed_position_projection_json.write_text(json.dumps(managed_positions), encoding="utf-8")

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["final_classification"] == OPEN_MANAGED_METADATA_INCOMPLETE
    assert position["review_required"] is True
    assert position.get("close_intent_created") is not True
    assert position.get("close_submitted") is not True


def test_missing_lifecycle_report_recovers_from_complete_mule_position_metadata(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        strategy_id="track_b_paper_execution_test_mule_v1__mgc",
        instrument="MGC",
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
        side="LONG",
        entry_price="4543",
        completed_timestamps=[
            "2026-05-07T16:30:00+00:00",
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
        ],
    )
    lifecycle_path = tmp_path / "managed" / "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b" / "track_b_strategy_managed_paper_lifecycle_report.json"
    lifecycle_path.unlink()
    live_status_path = tmp_path / "ledger" / "latest_track_b_live_position_status.json"
    live_status = json.loads(live_status_path.read_text(encoding="utf-8"))
    live_status["source_artifact_paths"] = []
    live_status_path.write_text(json.dumps(live_status), encoding="utf-8")

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["lifecycle_report_recovered"] is True
    assert position["exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert position["close_intent_created"] is True
    assert position["close_submitted"] is True
    recovered = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    assert recovered["source"] == "TRACK_B_STRATEGY_MANAGED_LIFECYCLE"
    assert recovered["entry_intent"]["side"] == "LONG"
    assert recovered["close_intent"]["order_action"] == "SELL"


def test_open_managed_position_age_three_submits_close_and_clears_open_summary(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        completed_timestamps=[
            "2026-05-07T16:30:00+00:00",
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
        ],
    )

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["completed_bars_since_entry"] == 3
    assert position["exit_eligible"] is True
    assert position["close_intent_created"] is True
    assert position["close_submitted"] is True
    assert position["close_filled"] is True
    assert position["close_order_id"] == "12"
    assert position["final_position_status"] == "CLOSED_FLAT"
    lifecycle = json.loads(
        (
            tmp_path
            / "managed"
            / "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
            / "track_b_strategy_managed_paper_lifecycle_report.json"
        ).read_text()
    )
    assert lifecycle["paper_proof_invoked"] is False
    assert lifecycle["close_intent"]["close_reason"] == "TIME_BOXED_EXIT"
    summary = json.loads((tmp_path / "ledger" / "latest_track_b_paper_trade_summary.json").read_text())
    positions = json.loads((tmp_path / "ledger" / "latest_track_b_live_position_status.json").read_text())
    assert summary["open_position_count"] == 0
    assert summary["managed_strategy_trade_count"] == 1
    assert summary["completed_trade_count"] == 1
    assert positions["open_position_count"] == 0


def test_exit_due_projection_creates_close_despite_stale_lifecycle_waiting_report(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        entry_filled_at="2026-05-07T16:31:07+00:00",
        completed_timestamps=[
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
        ],
    )
    lifecycle_path = (
        tmp_path
        / "managed"
        / "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    lifecycle["managed_exit_policy_id"] = "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    lifecycle["managed_exit_policy_max_completed_5m_bars"] = 12
    lifecycle["bars_since_fill"] = 0
    lifecycle["open_position_age_completed_5m_bars"] = 0
    lifecycle["close_intent_status"] = "WAITING_FOR_EXIT_POLICY_CONDITION"
    lifecycle_path.write_text(json.dumps(lifecycle), encoding="utf-8")
    write_json(
        cfg.managed_position_projection_json,
        {
            "schema_version": "track_b_managed_positions_v1",
            "generated_at": aware_now().isoformat(),
            "positions": [
                {
                    "trade_id": lifecycle["trade_id"],
                    "lifecycle_id": lifecycle["lifecycle_id"],
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "exit_due": True,
                    "bars_since_entry": 17,
                    "required_completed_5m_bars": 12,
                    "close_order_present": False,
                }
            ],
        },
    )

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["completed_bars_since_entry"] == 2
    assert position["effective_completed_bars_since_entry"] == 17
    assert position["managed_position_projection_classification"] == "OPEN_MANAGED_EXIT_DUE"
    assert position["managed_position_projection_exit_due"] is True
    assert position["lifecycle_report_stale_exit_due_conflict"] is True
    assert position["required_completed_5m_bars"] == 12
    assert position["exit_eligible"] is True
    assert position["close_intent_created"] is True
    assert position["close_submitted"] is True


def test_maintenance_refreshes_managed_position_authority_before_worklist(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = seed_open_position(
        tmp_path,
        entry_filled_at="2026-05-07T16:31:07+00:00",
        completed_timestamps=[
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
            "2026-05-07T16:45:00+00:00",
        ],
    )
    cfg = TrackBManagedOpenPositionMaintenanceConfig(
        **{
            **cfg.__dict__,
            "repo_root": tmp_path,
        }
    )
    fresh_projection = json.loads(cfg.managed_position_projection_json.read_text(encoding="utf-8"))
    stale_lifecycle_id = "reserved_submit_mes_globex_active_participation_short_1"
    write_json(
        cfg.managed_position_projection_json,
        {
            "schema_version": "track_b_managed_position_registry_v1",
            "generated_at": aware_now().isoformat(),
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "trade_id": "trade_submit_owner_mes_globex_short",
                    "lifecycle_id": stale_lifecycle_id,
                    "symbol": "MES",
                    "contract_key": "MES-202606",
                    "local_symbol": "MESM6",
                    "con_id": 770561194,
                    "side": "SHORT",
                    "quantity": "1",
                    "exit_due": True,
                    "bars_since_entry": 18,
                }
            ],
        },
    )

    import mgc_v05l.execution_core.track_b_managed_order_registry as managed_order_registry
    import mgc_v05l.execution_core.track_b_managed_position_registry as managed_position_registry

    def build_positions(*, config: Any, now: datetime | None = None) -> dict[str, Any]:
        return dict(fresh_projection)

    def write_positions(*, config: Any, payload: Mapping[str, Any], now: datetime | None = None) -> tuple[Path, list[dict[str, Any]]]:
        return config.resolve(config.output_path), []

    def build_orders(*, config: Any, now: datetime | None = None) -> dict[str, Any]:
        return {
            "schema_version": "track_b_managed_order_registry_v1",
            "generated_at": aware_now().isoformat(),
            "classification": "NO_MANAGED_ORDERS",
            "managed_orders": [],
        }

    def write_orders(*, config: Any, payload: Mapping[str, Any], now: datetime | None = None) -> tuple[Path, list[dict[str, Any]]]:
        return config.resolve(config.output_path), []

    monkeypatch.setattr(managed_position_registry, "build_track_b_managed_position_registry", build_positions)
    monkeypatch.setattr(managed_position_registry, "write_track_b_managed_position_registry", write_positions)
    monkeypatch.setattr(managed_order_registry, "build_track_b_managed_order_registry", build_orders)
    monkeypatch.setattr(managed_order_registry, "write_track_b_managed_order_registry", write_orders)

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["lifecycle_id"] == "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
    assert position["lifecycle_id"] != stale_lifecycle_id
    assert result.report["maintenance_authority_diagnostics"]["canonical_current_claim_count"] == 1
    assert position["close_intent_created"] is True
    assert position["close_submitted"] is True


def test_maintenance_fails_closed_when_managed_position_authority_refresh_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = seed_open_position(
        tmp_path,
        entry_filled_at="2026-05-07T16:31:07+00:00",
        completed_timestamps=[
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
            "2026-05-07T16:45:00+00:00",
        ],
    )
    cfg = TrackBManagedOpenPositionMaintenanceConfig(
        **{
            **cfg.__dict__,
            "repo_root": tmp_path,
        }
    )

    import mgc_v05l.execution_core.track_b_managed_position_registry as managed_position_registry

    def build_positions(*, config: Any, now: datetime | None = None) -> dict[str, Any]:
        raise RuntimeError("authority unavailable")

    monkeypatch.setattr(managed_position_registry, "build_track_b_managed_position_registry", build_positions)

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["close_intent_created"] is False
    assert position["close_submitted"] is False
    assert position["canonical_owner_authority_blocker"]["classification"] == "MAINTENANCE_CANONICAL_OWNER_REQUIRED"


def test_maintenance_overlays_canonical_owner_identity_on_synthetic_lifecycle_report(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        entry_filled_at="2026-05-07T16:31:07+00:00",
        completed_timestamps=[
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
            "2026-05-07T16:45:00+00:00",
        ],
    )
    cfg = TrackBManagedOpenPositionMaintenanceConfig(
        **{
            **cfg.__dict__,
            "refresh_managed_position_authority": False,
        }
    )
    canonical_lifecycle_id = "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
    canonical_trade_id = f"MNQ_FIRST_BULL_SNAP_TURN_V1:{canonical_lifecycle_id}"
    canonical_report_path = (
        tmp_path
        / "managed"
        / canonical_lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    synthetic_lifecycle_id = "bridge_fill_MNQ|1m|2026-05-07T16:31:00Z|BUY_TO_OPEN"
    synthetic_report_path = (
        tmp_path
        / "managed"
        / synthetic_lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    synthetic_report = json.loads(canonical_report_path.read_text(encoding="utf-8"))
    synthetic_report["lifecycle_id"] = synthetic_lifecycle_id
    synthetic_report["trade_id"] = "trade_bridge_fill_MNQ_1m_2026_05_07T16_31_00Z_BUY_TO_OPEN"
    synthetic_report["entry_intent"]["lifecycle_id"] = synthetic_lifecycle_id
    synthetic_report["entry_intent"]["trade_id"] = synthetic_report["trade_id"]
    synthetic_report["open_state"] = {
        "lifecycle_id": synthetic_lifecycle_id,
        "trade_id": synthetic_report["trade_id"],
        "entry_timestamp": "2026-05-07T16:31:07+00:00",
        "entry_price": "28729",
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
    }
    write_json(synthetic_report_path, synthetic_report)
    projection = json.loads(cfg.managed_position_projection_json.read_text(encoding="utf-8"))
    projection["managed_positions"] = projection.pop("positions")
    projection["managed_positions"][0]["projection_authority_source"] = "CURRENT_EXPOSURE_OWNER_RESOLVER"
    projection["managed_positions"][0]["paper_lifecycle_report_path"] = str(synthetic_report_path)
    projection["managed_positions"][0]["lifecycle_position"]["source"] = "CURRENT_EXPOSURE_OWNER_RESOLVER"
    projection["managed_positions"][0]["lifecycle_position"]["paper_lifecycle_report_path"] = str(synthetic_report_path)
    write_json(cfg.managed_position_projection_json, projection)

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["trade_id"] == canonical_trade_id
    assert position["lifecycle_id"] == canonical_lifecycle_id
    assert position["close_intent_created"] is True
    assert position["close_submitted"] is True
    assert position.get("canonical_owner_authority_blocker") is None


def test_exit_due_projection_canonicalizes_mes_close_expiry_from_broker_position(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        instrument="MES",
        strategy_id="mes_globex_active_participation_short",
        contract_key="MES-202606",
        local_symbol="MESM6",
        con_id=770561194,
        side="SHORT",
        entry_price="7598.75",
        completed_timestamps=[
            "2026-06-01T22:10:00+00:00",
            "2026-06-01T22:15:00+00:00",
        ],
    )
    lifecycle_path = (
        tmp_path
        / "managed"
        / "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    lifecycle["managed_exit_policy_id"] = "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    lifecycle["managed_exit_policy_max_completed_5m_bars"] = 12
    lifecycle["bars_since_fill"] = 0
    lifecycle["close_intent_status"] = "WAITING_FOR_EXIT_POLICY_CONDITION"
    lifecycle_path.write_text(json.dumps(lifecycle), encoding="utf-8")
    write_json(
        cfg.managed_position_projection_json,
        {
            "schema_version": "track_b_managed_positions_v1",
            "generated_at": aware_now().isoformat(),
            "positions": [
                {
                    "trade_id": lifecycle["trade_id"],
                    "lifecycle_id": lifecycle["lifecycle_id"],
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "exit_due": True,
                    "bars_since_entry": 17,
                    "required_completed_5m_bars": 12,
                    "close_order_present": False,
                    "broker_position": {
                        "account_id": "DUM882026",
                        "symbol": "MES",
                        "local_symbol": "MESM6",
                        "con_id": 770561194,
                        "expiry": "20260618",
                        "quantity": "-1",
                    },
                }
            ],
        },
    )
    captured_configs: list[TrackBStrategyManagedPaperLifecycleConfig] = []

    def entry_submitter(_config: TrackBStrategyManagedPaperLifecycleConfig, _entry_intent: Mapping[str, Any]) -> Mapping[str, Any]:
        raise AssertionError("maintenance must not submit another entry")

    def close_submitter(config: TrackBStrategyManagedPaperLifecycleConfig, close_intent: Mapping[str, Any]) -> Mapping[str, Any]:
        captured_configs.append(config)
        return {
            "submitted": True,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "broker_order_id": "12",
            "close_intent": dict(close_intent),
        }

    from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import default_managed_lifecycle_stages

    defaults = default_managed_lifecycle_stages()
    stages = TrackBStrategyManagedPaperLifecycleStages(
        entry_submitter=entry_submitter,
        exit_policy=defaults.exit_policy,
        close_submitter=close_submitter,
    )

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=stages,
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["close_intent_created"] is True
    assert position["close_submitted"] is True
    assert captured_configs[0].contract_expiry == "20260618"
    assert captured_configs[0].local_symbol == "MESM6"
    assert captured_configs[0].con_id == 770561194


def test_maintenance_uses_canonical_owner_when_raw_lifecycle_status_is_stale(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        instrument="MES",
        strategy_id="mes_us_active_participation_short",
        contract_key="MES-202606",
        local_symbol="MESM6",
        con_id=770561194,
        side="SHORT",
        entry_price="7604.75",
        completed_timestamps=[
            "2026-06-03T13:40:00+00:00",
            "2026-06-03T13:45:00+00:00",
        ],
    )
    stale_lifecycle_id = "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
    current_lifecycle_id = "reserved_submit_current_owner_20260603T133628Z"
    current_trade_id = "trade-current-owner"
    stale_path = tmp_path / "managed" / stale_lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"
    current_path = tmp_path / "managed" / current_lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"
    current_lifecycle = json.loads(stale_path.read_text(encoding="utf-8"))
    current_lifecycle["lifecycle_id"] = current_lifecycle_id
    current_lifecycle["trade_id"] = current_trade_id
    current_lifecycle["report_json_path"] = str(current_path)
    current_lifecycle["managed_exit_policy_id"] = "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    current_lifecycle["managed_exit_policy_max_completed_5m_bars"] = 12
    current_lifecycle["bars_since_fill"] = 0
    current_lifecycle["entry_intent"]["lifecycle_id"] = current_lifecycle_id
    current_lifecycle["entry_intent"]["trade_id"] = current_trade_id
    current_lifecycle["entry_intent"]["managed_exit_policy_id"] = "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    current_lifecycle["entry_submit_attempt"]["broker_order_id"] = "1"
    current_lifecycle["entry_submit_attempt"]["perm_id"] = "1421892784"
    current_lifecycle["entry_fill"]["broker_order_id"] = "1"
    current_lifecycle["entry_fill"]["perm_id"] = "1421892784"
    current_lifecycle["entry_fill"]["execution_id"] = "exec-current"
    write_json(current_path, current_lifecycle)
    write_json(
        cfg.managed_position_projection_json,
        {
            "schema_version": "track_b_managed_position_registry_v1",
            "generated_at": aware_now().isoformat(),
            "positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "trade_id": current_trade_id,
                    "lifecycle_id": current_lifecycle_id,
                    "lane_id": "mes_us_active_participation_short",
                    "strategy_id": "mes_us_active_participation_short",
                    "symbol": "MES",
                    "contract_key": "MES-202606",
                    "local_symbol": "MESM6",
                    "con_id": 770561194,
                    "account_id": "DUM882026",
                    "side": "SHORT",
                    "quantity": "1",
                    "entry_time": "2026-06-03T13:36:30+00:00",
                    "entry_price": "7604.75",
                    "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
                    "exit_due": True,
                    "bars_since_entry": 17,
                    "required_completed_5m_bars": 12,
                    "broker_qty_match": True,
                    "reconciliation_status": "OPEN_MANAGED_MATCHED",
                    "entry_order_ids": ["1"],
                    "entry_perm_ids": ["1421892784"],
                    "entry_exec_ids": ["exec-current"],
                    "paper_lifecycle_report_path": str(current_path),
                    "broker_position": {
                        "account_id": "DUM882026",
                        "symbol": "MES",
                        "local_symbol": "MESM6",
                        "con_id": 770561194,
                        "expiry": "20260618",
                        "quantity": "-1",
                    },
                }
            ],
        },
    )
    submitted_lifecycle_ids: list[str] = []

    def close_submitter(config: TrackBStrategyManagedPaperLifecycleConfig, close_intent: Mapping[str, Any]) -> Mapping[str, Any]:
        submitted_lifecycle_ids.append(str(close_intent.get("lifecycle_id") or config.strategy_id))
        return {
            "submitted": True,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "broker_order_id": "77",
            "close_intent": dict(close_intent),
        }

    from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import default_managed_lifecycle_stages

    defaults = default_managed_lifecycle_stages()
    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=TrackBStrategyManagedPaperLifecycleStages(
            entry_submitter=lambda _config, _intent: {},
            exit_policy=defaults.exit_policy,
            close_submitter=close_submitter,
        ),
        now=aware_now(),
    )

    assert submitted_lifecycle_ids == [current_lifecycle_id]
    position = result.report["positions"][0]
    assert position["lifecycle_id"] == current_lifecycle_id
    assert position["close_intent_created"] is True
    assert position["close_submitted"] is True
    stale = result.report["maintenance_authority_diagnostics"]["stale_lifecycle_diagnostic_only"]
    assert any(item.get("lifecycle_id") == stale_lifecycle_id for item in stale)


def test_current_mes_regression_uses_canonical_owner_before_close_intent(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        instrument="MES",
        strategy_id="mes_us_active_participation_short",
        contract_key="MES-M6",
        local_symbol="MESM6",
        con_id=770561194,
        side="SHORT",
        entry_price="7604.75",
        completed_timestamps=[
            "2026-06-03T15:10:00+00:00",
            "2026-06-03T15:15:00+00:00",
            "2026-06-03T15:20:00+00:00",
            "2026-06-03T15:25:00+00:00",
            "2026-06-03T15:30:00+00:00",
            "2026-06-03T15:35:00+00:00",
            "2026-06-03T15:40:00+00:00",
            "2026-06-03T15:45:00+00:00",
            "2026-06-03T15:50:00+00:00",
            "2026-06-03T15:55:00+00:00",
            "2026-06-03T16:00:00+00:00",
            "2026-06-03T16:05:00+00:00",
            "2026-06-03T16:10:00+00:00",
            "2026-06-03T16:15:00+00:00",
            "2026-06-03T16:20:00+00:00",
            "2026-06-03T16:25:00+00:00",
            "2026-06-03T16:30:00+00:00",
        ],
    )
    stale_lifecycle_id = "reserved_submit_mes_us_active_participation_short_20260602T185428677911Z_0c5caf5f40f7"
    canonical_lifecycle_id = "reserved_submit_mes_us_active_participation_short_20260603T133628483955Z_3fe308fd049d"
    canonical_trade_id = "trade_df6b60cf-9848-4c2e-b71d-a0b649317b52"
    original_lifecycle_id = "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
    original_path = tmp_path / "managed" / original_lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"
    stale_path = tmp_path / "managed" / stale_lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"
    canonical_path = tmp_path / "managed" / canonical_lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"
    stale_lifecycle = json.loads(original_path.read_text(encoding="utf-8"))
    stale_lifecycle["lifecycle_id"] = stale_lifecycle_id
    stale_lifecycle["trade_id"] = "stale-diagnostic-only"
    stale_lifecycle["report_json_path"] = str(stale_path)
    stale_lifecycle["entry_intent"]["lifecycle_id"] = stale_lifecycle_id
    stale_lifecycle["entry_intent"]["trade_id"] = "stale-diagnostic-only"
    write_json(stale_path, stale_lifecycle)
    canonical_lifecycle = json.loads(original_path.read_text(encoding="utf-8"))
    canonical_lifecycle["lifecycle_id"] = canonical_lifecycle_id
    canonical_lifecycle["trade_id"] = canonical_trade_id
    canonical_lifecycle["report_json_path"] = str(canonical_path)
    canonical_lifecycle["managed_exit_policy_id"] = "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    canonical_lifecycle["managed_exit_policy_max_completed_5m_bars"] = 12
    canonical_lifecycle["entry_intent"]["lifecycle_id"] = canonical_lifecycle_id
    canonical_lifecycle["entry_intent"]["trade_id"] = canonical_trade_id
    canonical_lifecycle["entry_intent"]["managed_exit_policy_id"] = "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    canonical_lifecycle["entry_submit_attempt"]["broker_order_id"] = "1"
    canonical_lifecycle["entry_submit_attempt"]["perm_id"] = "1421892784"
    canonical_lifecycle["entry_fill"]["broker_order_id"] = "1"
    canonical_lifecycle["entry_fill"]["perm_id"] = "1421892784"
    canonical_lifecycle["entry_fill"]["execution_id"] = "0000e1a7.current.01.01"
    write_json(canonical_path, canonical_lifecycle)
    live_status = json.loads(cfg.live_position_status_json.read_text(encoding="utf-8"))
    live_status["positions_by_instrument"] = {
        "MES-M6": {
            **live_status["positions_by_instrument"]["MES-M6"],
            "lifecycle_id": stale_lifecycle_id,
            "trade_id": "stale-diagnostic-only",
        }
    }
    live_status["source_artifact_paths"] = [str(stale_path)]
    write_json(cfg.live_position_status_json, live_status)
    write_json(
        cfg.managed_position_projection_json,
        {
            "schema_version": "track_b_managed_position_registry_v1",
            "generated_at": aware_now().isoformat(),
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "trade_id": canonical_trade_id,
                    "lifecycle_id": canonical_lifecycle_id,
                    "lane_id": "mes_us_active_participation_short",
                    "strategy_id": "mes_us_active_participation_short",
                    "symbol": "MES",
                    "contract_key": "MES-M6",
                    "local_symbol": "MESM6",
                    "con_id": 770561194,
                    "account_id": "DUM882026",
                    "side": "SHORT",
                    "quantity": "1",
                    "entry_time": "2026-06-03T13:36:30+00:00",
                    "entry_price": "7604.75",
                    "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
                    "exit_due": True,
                    "bars_since_entry": 17,
                    "required_completed_5m_bars": 12,
                    "broker_qty_match": True,
                    "reconciliation_status": "OPEN_MANAGED_MATCHED",
                    "entry_order_ids": ["1"],
                    "entry_perm_ids": ["1421892784"],
                    "entry_exec_ids": ["0000e1a7.current.01.01"],
                    "paper_lifecycle_report_path": str(canonical_path),
                    "broker_position": {
                        "account_id": "DUM882026",
                        "symbol": "MES",
                        "local_symbol": "MESM6",
                        "con_id": 770561194,
                        "expiry": "20260618",
                        "quantity": "-1",
                    },
                }
            ],
        },
    )
    submitted: list[Mapping[str, Any]] = []

    def close_submitter(_config: TrackBStrategyManagedPaperLifecycleConfig, close_intent: Mapping[str, Any]) -> Mapping[str, Any]:
        submitted.append(dict(close_intent))
        return {
            "submitted": True,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "broker_order_id": "77",
            "close_intent": dict(close_intent),
        }

    from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import default_managed_lifecycle_stages

    defaults = default_managed_lifecycle_stages()
    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=TrackBStrategyManagedPaperLifecycleStages(
            entry_submitter=lambda _config, _intent: {},
            exit_policy=defaults.exit_policy,
            close_submitter=close_submitter,
        ),
        now=aware_now(),
    )

    assert [position["lifecycle_id"] for position in result.report["positions"]] == [canonical_lifecycle_id]
    assert submitted and submitted[0]["order_action"] == "BUY"
    assert submitted[0]["lifecycle_id"] == canonical_lifecycle_id
    assert result.report["positions"][0]["close_intent_created"] is True
    stale = result.report["maintenance_authority_diagnostics"]["stale_lifecycle_diagnostic_only"]
    assert any(item.get("lifecycle_id") == stale_lifecycle_id for item in stale)


def test_stale_lifecycle_report_path_is_blocked_before_close_intent(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        instrument="MES",
        strategy_id="mes_us_active_participation_short",
        contract_key="MES-M6",
        local_symbol="MESM6",
        con_id=770561194,
        side="SHORT",
        entry_price="7604.75",
        completed_timestamps=[
            "2026-06-03T15:10:00+00:00",
            "2026-06-03T15:15:00+00:00",
            "2026-06-03T15:20:00+00:00",
        ],
    )
    stale_lifecycle_id = "reserved_submit_stale_owner_20260602T185428Z"
    canonical_lifecycle_id = "reserved_submit_current_owner_20260603T133628Z"
    canonical_trade_id = "trade-current-owner"
    original_lifecycle_id = "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
    original_path = tmp_path / "managed" / original_lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"
    stale_path = tmp_path / "managed" / stale_lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"
    stale_lifecycle = json.loads(original_path.read_text(encoding="utf-8"))
    stale_lifecycle["lifecycle_id"] = stale_lifecycle_id
    stale_lifecycle["trade_id"] = "trade-stale-owner"
    stale_lifecycle["report_json_path"] = str(stale_path)
    stale_lifecycle["entry_intent"]["lifecycle_id"] = stale_lifecycle_id
    stale_lifecycle["entry_intent"]["trade_id"] = "trade-stale-owner"
    write_json(stale_path, stale_lifecycle)
    write_json(
        cfg.managed_position_projection_json,
        {
            "schema_version": "track_b_managed_position_registry_v1",
            "generated_at": aware_now().isoformat(),
            "managed_positions": [
                {
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "trade_id": canonical_trade_id,
                    "lifecycle_id": canonical_lifecycle_id,
                    "lane_id": "mes_us_active_participation_short",
                    "strategy_id": "mes_us_active_participation_short",
                    "symbol": "MES",
                    "contract_key": "MES-M6",
                    "local_symbol": "MESM6",
                    "con_id": 770561194,
                    "account_id": "DUM882026",
                    "side": "SHORT",
                    "quantity": "1",
                    "entry_time": "2026-06-03T13:36:30+00:00",
                    "entry_price": "7604.75",
                    "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
                    "exit_due": True,
                    "bars_since_entry": 17,
                    "required_completed_5m_bars": 12,
                    "broker_qty_match": True,
                    "reconciliation_status": "OPEN_MANAGED_MATCHED",
                    "entry_order_ids": ["1"],
                    "entry_perm_ids": ["1421892784"],
                    "entry_exec_ids": ["exec-current"],
                    "paper_lifecycle_report_path": str(stale_path),
                    "broker_position": {
                        "account_id": "DUM882026",
                        "symbol": "MES",
                        "local_symbol": "MESM6",
                        "con_id": 770561194,
                        "expiry": "20260618",
                        "quantity": "-1",
                    },
                }
            ],
        },
    )
    submitted: list[Mapping[str, Any]] = []

    def close_submitter(_config: TrackBStrategyManagedPaperLifecycleConfig, close_intent: Mapping[str, Any]) -> Mapping[str, Any]:
        submitted.append(dict(close_intent))
        raise AssertionError("stale lifecycle report must be blocked before close intent submission")

    from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import default_managed_lifecycle_stages

    defaults = default_managed_lifecycle_stages()
    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=TrackBStrategyManagedPaperLifecycleStages(
            entry_submitter=lambda _config, _intent: {},
            exit_policy=defaults.exit_policy,
            close_submitter=close_submitter,
        ),
        now=aware_now(),
    )

    assert submitted == []
    assert result.report["close_intent_created_count"] == 0
    position = result.report["positions"][0]
    assert position["lifecycle_id"] == canonical_lifecycle_id
    assert position["close_intent_created"] is False
    assert position["final_classification"] == "STALE_LIFECYCLE_REPORT_DIAGNOSTIC_ONLY"
    blocker = position["canonical_owner_authority_blocker"]
    assert blocker["source"] == "managed_open_position_maintenance_lifecycle_identity_filter"
    assert blocker["owner_lifecycle_id"] == canonical_lifecycle_id
    assert blocker["blocked_lifecycle_id"] == stale_lifecycle_id


def test_contract_close_lock_blocks_second_lifecycle_close_submit_same_contract(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        instrument="MES",
        strategy_id="mes_us_active_participation_long",
        contract_key="MES-202606",
        local_symbol="MESM6",
        con_id=770561194,
        side="LONG",
        entry_price="7617.75",
        completed_timestamps=[
            "2026-05-07T16:30:00+00:00",
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
        ],
    )
    original_lifecycle_id = "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
    duplicate_lifecycle_id = "reserved_submit_mes_globex_active_participation_long_duplicate"
    _add_duplicate_open_lifecycle(
        tmp_path=tmp_path,
        cfg=cfg,
        original_lifecycle_id=original_lifecycle_id,
        duplicate_lifecycle_id=duplicate_lifecycle_id,
        strategy_id="mes_globex_active_participation_long",
    )
    submit_calls: list[str] = []

    def entry_submitter(_config: TrackBStrategyManagedPaperLifecycleConfig, _entry_intent: Mapping[str, Any]) -> Mapping[str, Any]:
        raise AssertionError("maintenance must not submit another entry")

    def close_submitter(config: TrackBStrategyManagedPaperLifecycleConfig, close_intent: Mapping[str, Any]) -> Mapping[str, Any]:
        submit_calls.append(str(close_intent.get("lifecycle_id") or config.strategy_id))
        return {
            "submitted": True,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "broker_order_id": "65",
            "submitted_at": "2026-06-02T22:58:55+00:00",
            "close_intent": dict(close_intent),
            "submit_diagnostics": {"orderStatus_seen": True},
        }

    from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import default_managed_lifecycle_stages

    defaults = default_managed_lifecycle_stages()
    stages = TrackBStrategyManagedPaperLifecycleStages(
        entry_submitter=entry_submitter,
        exit_policy=defaults.exit_policy,
        close_submitter=close_submitter,
    )

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=stages,
        now=aware_now(),
    )

    assert len(submit_calls) == 1
    assert len(result.report["positions"]) == 1
    first = result.report["positions"][0]
    assert first["close_submitted"] is True
    stale = result.report["maintenance_authority_diagnostics"]["stale_lifecycle_diagnostic_only"]
    assert stale
    assert stale[0]["classification"] == "STALE_LIFECYCLE_REPORT_DIAGNOSTIC_ONLY"


@pytest.mark.parametrize(
    ("broker_qty", "expected_blocker"),
    [
        ("0", "CLOSE_NOT_RISK_REDUCING_BROKER_FLAT"),
        ("-1", "CLOSE_WOULD_INCREASE_REVERSE_EXPOSURE"),
    ],
)
def test_close_submit_rechecks_broker_position_freshness_before_submit(
    tmp_path: Path,
    broker_qty: str,
    expected_blocker: str,
) -> None:
    cfg = seed_open_position(
        tmp_path,
        instrument="MES",
        strategy_id="mes_us_active_participation_long",
        contract_key="MES-202606",
        local_symbol="MESM6",
        con_id=770561194,
        side="LONG",
        entry_price="7617.75",
        completed_timestamps=[
            "2026-05-07T16:30:00+00:00",
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
        ],
    )
    lifecycle_path = (
        tmp_path
        / "managed"
        / "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    write_json(
        cfg.managed_position_projection_json,
        {
            "schema_version": "track_b_managed_positions_v1",
            "positions": [
                {
                    "trade_id": lifecycle["trade_id"],
                    "lifecycle_id": lifecycle["lifecycle_id"],
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "exit_due": True,
                    "broker_position": {
                        "account_id": "DUM882026",
                        "symbol": "MES",
                        "local_symbol": "MESM6",
                        "con_id": 770561194,
                        "quantity": broker_qty,
                    },
                }
            ],
        },
    )

    def close_submitter(_config: TrackBStrategyManagedPaperLifecycleConfig, _close_intent: Mapping[str, Any]) -> Mapping[str, Any]:
        raise AssertionError("freshness guard must block before the close submitter")

    from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import default_managed_lifecycle_stages

    defaults = default_managed_lifecycle_stages()
    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=TrackBStrategyManagedPaperLifecycleStages(
            entry_submitter=lambda _config, _intent: {},
            exit_policy=defaults.exit_policy,
            close_submitter=close_submitter,
        ),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["close_intent_created"] is True
    assert position["close_submitted"] is False
    assert expected_blocker in str(position["blocker"])


def test_stale_superseded_lifecycle_chain_cannot_submit_managed_close(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        instrument="MES",
        strategy_id="mes_stale_lifecycle_only_long",
        contract_key="MES-202606",
        local_symbol="MESM6",
        con_id=770561194,
        side="LONG",
        entry_price="7617.75",
        completed_timestamps=[
            "2026-05-07T16:30:00+00:00",
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
        ],
    )
    stale_lifecycle_id = "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
    canonical_lifecycle_id = "reserved_submit_mes_us_active_participation_long_canonical"
    lifecycle_path = tmp_path / "managed" / stale_lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"
    lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    write_json(
        cfg.managed_position_projection_json,
        {
            "schema_version": "track_b_managed_positions_v1",
            "positions": [
                {
                    "trade_id": lifecycle["trade_id"],
                    "lifecycle_id": stale_lifecycle_id,
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "exit_due": True,
                    "broker_qty_match": False,
                    "broker_position": {
                        "account_id": "DUM882026",
                        "symbol": "MES",
                        "local_symbol": "MESM6",
                        "con_id": 770561194,
                        "quantity": "1",
                    },
                },
                {
                    "trade_id": "trade-canonical",
                    "lifecycle_id": canonical_lifecycle_id,
                    "classification": "OPEN_MANAGED_EXIT_DUE",
                    "reconciliation_status": "OPEN_MANAGED_MATCHED",
                    "broker_qty_match": True,
                    "broker_position": {
                        "account_id": "DUM882026",
                        "symbol": "MES",
                        "local_symbol": "MESM6",
                        "con_id": 770561194,
                        "quantity": "1",
                    },
                },
            ],
        },
    )

    def close_submitter(_config: TrackBStrategyManagedPaperLifecycleConfig, _close_intent: Mapping[str, Any]) -> Mapping[str, Any]:
        raise AssertionError("non-canonical lifecycle owner must not reach close submitter")

    from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import default_managed_lifecycle_stages

    defaults = default_managed_lifecycle_stages()
    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=TrackBStrategyManagedPaperLifecycleStages(
            entry_submitter=lambda _config, _intent: {},
            exit_policy=defaults.exit_policy,
            close_submitter=close_submitter,
        ),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["lifecycle_id"] == canonical_lifecycle_id
    assert position.get("close_intent_created") is not True
    assert position.get("close_submitted") is not True
    assert position["blocker"] == "OPEN_MANAGED lifecycle report is missing."
    stale = result.report["maintenance_authority_diagnostics"]["stale_lifecycle_diagnostic_only"]
    assert any(item.get("lifecycle_id") == stale_lifecycle_id for item in stale)


@pytest.mark.parametrize(
    ("instrument", "strategy_id", "contract_key", "local_symbol", "con_id", "side", "expected_close_action"),
    [
        ("MGC", "track_b_paper_execution_test_mule_v1__mgc", "MGC-202606", "MGCM6", 712565978, "LONG", "SELL"),
        ("MGC", "track_b_paper_execution_test_mule_v1__mgc", "MGC-202606", "MGCM6", 712565978, "SHORT", "BUY"),
        ("MNQ", "track_b_paper_execution_test_mule_v1__mnq", "MNQ-202606", "MNQM6", 770561201, "LONG", "SELL"),
        ("MNQ", "track_b_paper_execution_test_mule_v1__mnq", "MNQ-202606", "MNQM6", 770561201, "SHORT", "BUY"),
    ],
)
def test_execution_test_mule_time_box_exits_use_correct_close_action(
    tmp_path: Path,
    instrument: str,
    strategy_id: str,
    contract_key: str,
    local_symbol: str,
    con_id: int,
    side: str,
    expected_close_action: str,
) -> None:
    cfg = seed_open_position(
        tmp_path,
        strategy_id=strategy_id,
        instrument=instrument,
        contract_key=contract_key,
        local_symbol=local_symbol,
        con_id=con_id,
        side=side,
        entry_price="4543" if instrument == "MGC" else "28729",
        completed_timestamps=[
            "2026-05-07T16:30:00+00:00",
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
        ],
    )

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["exit_eligible"] is True
    assert position["close_intent_created"] is True
    assert position["close_submitted"] is True
    lifecycle = json.loads(
        (
            tmp_path
            / "managed"
            / "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
            / "track_b_strategy_managed_paper_lifecycle_report.json"
        ).read_text()
    )
    assert lifecycle["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert lifecycle["close_intent"]["order_action"] == expected_close_action
    assert lifecycle["final_position_status"] == "CLOSED_FLAT"


def test_uses_canonical_phase1_runtime_market_data_root(tmp_path: Path) -> None:
    cfg = seed_open_position(tmp_path, completed_timestamps=[])
    phase1_root = tmp_path / "phase1_runtime_market_data"
    write_json(
        phase1_root / "MNQ" / "5m" / "latest_runtime_candles.json",
        {
            "symbol": "MNQ",
            "timeframe": "5m",
            "generated_at": "2026-05-07T16:45:00+00:00",
            "last_completed_bar_ts": "2026-05-07T16:40:00+00:00",
            "realtime_feed_confirmed": True,
            "bars": [
                {"bar_end": "2026-05-07T16:30:00+00:00", "close": "28721"},
                {"bar_end": "2026-05-07T16:35:00+00:00", "close": "28720.5"},
                {"bar_end": "2026-05-07T16:40:00+00:00", "close": "28720"},
            ],
        },
    )
    write_json(
        phase1_root / "MNQ" / "1m" / "latest_runtime_candles.json",
        {
            "symbol": "MNQ",
            "timeframe": "1m",
            "generated_at": "2026-05-07T16:45:00+00:00",
            "last_completed_bar_ts": "2026-05-07T16:44:00+00:00",
            "realtime_feed_confirmed": True,
            "candles": [
                {"bar_end": "2026-05-07T16:44:00+00:00", "close": "28720"},
            ],
        },
    )
    cfg = TrackBManagedOpenPositionMaintenanceConfig(
        **{**cfg.__dict__, "live_runtime_feed_output_root": phase1_root}
    )

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["completed_bars_since_entry"] == 3
    assert position["data_freshness_state"] == "FRESH"
    assert position["exit_eligible"] is True
    assert position["close_intent_created"] is True


def test_submit_disabled_maintenance_is_diagnostic_only(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        completed_timestamps=[
            "2026-05-07T16:30:00+00:00",
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
        ],
    )
    cfg = TrackBManagedOpenPositionMaintenanceConfig(
        **{**cfg.__dict__, "submit_enabled": False}
    )

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["maintenance_mode"] == "DIAGNOSTIC_DRY_RUN_SUBMIT_DISABLED"
    assert position["exit_eligible"] is True
    assert position["close_intent_created"] is True
    assert position["close_submitted"] is False
    assert position["final_position_status"] == "OPEN_MANAGED"
    assert result.report["broker_state_mutated"] is False
    assert result.report["submit_attempted"] is False
    lifecycle = json.loads(
        (
            tmp_path
            / "managed"
            / "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
            / "track_b_strategy_managed_paper_lifecycle_report.json"
        ).read_text()
    )
    assert lifecycle["final_position_status"] == "OPEN_MANAGED"
    summary = json.loads((tmp_path / "ledger" / "latest_track_b_live_position_status.json").read_text())
    assert summary["review_required_positions"] == []


def test_gc_style_recent_fill_clock_does_not_use_stale_signal_age(tmp_path: Path) -> None:
    old_signal_bars = [
        (datetime(2026, 5, 6, 15, 15, tzinfo=timezone.utc) + timedelta(minutes=5 * index)).isoformat()
        for index in range(302)
    ]
    cfg = seed_open_position(
        tmp_path,
        signal_timestamp="2026-05-06T15:10:00+00:00",
        entry_filled_at="2026-05-07T16:44:08+00:00",
        completed_timestamps=old_signal_bars,
    )

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["bars_since_fill"] == 0
    assert position["bars_since_signal"] == 302
    assert position["close_intent_created"] is False
    assert position["fill_timestamp_source"] == "BROKER_ENTRY_FILL"


def test_stale_restrict_state_suppresses_discretionary_time_exit(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        latest_1m_age_seconds=500.0,
        completed_timestamps=[
            "2026-05-07T16:30:00+00:00",
            "2026-05-07T16:35:00+00:00",
            "2026-05-07T16:40:00+00:00",
        ],
    )

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["data_freshness_state"] == "STALE_RESTRICT_DISCRETIONARY_EXITS"
    assert position["suppressed_due_to_stale_data"] is True
    assert position["close_intent_created"] is False


def test_micro_stale_warns_without_becoming_emergency_exit_state(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
        latest_1m_age_seconds=200.0,
        completed_timestamps=[
            "2026-05-07T16:30:00+00:00",
            "2026-05-07T16:35:00+00:00",
        ],
    )

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["data_freshness_state"] == "MICRO_STALE_WARNING"
    assert position["suppressed_due_to_stale_data"] is False
    assert position["close_intent_created"] is False
