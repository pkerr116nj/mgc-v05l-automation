from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

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
) -> dict[str, Any]:
    lifecycle_id = "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
    return {
        "schema_version": "track_b_strategy_managed_paper_lifecycle_v1",
        "lifecycle_id": lifecycle_id,
        "trade_id": f"MNQ_FIRST_BULL_SNAP_TURN_V1:{lifecycle_id}",
        "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
        "instrument_family": "MNQ",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
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
            "trade_id": f"MNQ_FIRST_BULL_SNAP_TURN_V1:{lifecycle_id}",
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
            "signal_timestamp": signal_timestamp,
            "decision_bar_timestamp": signal_timestamp,
            "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
            "entry_limit_price": "28729",
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
            "price": "28729",
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
) -> TrackBManagedOpenPositionMaintenanceConfig:
    lifecycle_id = "strategy_managed_fe30248d4d6c42acaf106c8313b0b33b"
    lifecycle_path = (
        tmp_path
        / "managed"
        / lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    payload = lifecycle_payload(entry_filled_at=entry_filled_at, signal_timestamp=signal_timestamp)
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
                "MNQ-202606": {
                    "lifecycle_id": lifecycle_id,
                    "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
                    "instrument_family": "MNQ",
                    "contract_key": "MNQ-202606",
                    "local_symbol": "MNQM6",
                    "quantity": "1",
                    "avg_entry_price": "28729",
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
    write_json(
        tmp_path / "live" / "latest_live_mnq_completed_5m_candles.json",
        {
            "candles": [{"candle_timestamp": ts, "close": "28720"} for ts in completed_timestamps],
            "bars_available": len(completed_timestamps),
        },
    )
    write_json(
        tmp_path / "live" / "latest_live_mnq_1m_candles.json",
        {
            "latest_1m_age_seconds": latest_1m_age_seconds,
            "candles": [
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


def test_open_managed_position_age_two_keeps_waiting(tmp_path: Path) -> None:
    cfg = seed_open_position(
        tmp_path,
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
    assert position["completed_bars_since_entry"] == 2
    assert position["exit_eligible"] is False
    assert position["close_intent_created"] is False
    assert position["close_submitted"] is False


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

    result = run_track_b_managed_open_position_maintenance(
        config=cfg,
        lifecycle_stages=fake_close_stages(),
        now=aware_now(),
    )

    position = result.report["positions"][0]
    assert position["final_classification"] == OPEN_MANAGED_METADATA_INCOMPLETE
    assert position["review_required"] is True
    assert position["close_intent_created"] is False
    assert position["close_submitted"] is False


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
