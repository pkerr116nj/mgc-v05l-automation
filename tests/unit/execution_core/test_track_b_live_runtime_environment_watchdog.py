from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEvent, TradeEventType
from mgc_v05l.execution_core.track_b_live_runtime_environment_watchdog import (
    DEGRADED_AUTHORITY_STALE,
    DEGRADED_DATA_STALE,
    DEGRADED_LANES_NOT_EVALUATING,
    OUT_OF_WINDOW_BUT_HEALTHY,
    READY_SUBMIT_CAPABLE,
    RECOVERY_REQUIRED,
    REVIEW_REQUIRED,
    TrackBLiveRuntimeEnvironmentWatchdogConfig,
    build_track_b_live_runtime_environment_watchdog,
)


NOW = datetime(2026, 6, 1, 1, 0, tzinfo=UTC)


def test_alive_but_authority_stale_is_degraded(tmp_path: Path) -> None:
    config = _write_clean_fixture(tmp_path)
    _write(config.resolve(config.authority_refresh_path), {"generated_at": _iso(NOW - timedelta(minutes=10))})

    payload = build_track_b_live_runtime_environment_watchdog(
        config=config,
        now=NOW,
        pid_running=lambda pid: True,
        source_commit_resolver=lambda root: "abc",
    )

    assert payload["classification"] == DEGRADED_AUTHORITY_STALE
    assert "AUTHORITY_HEARTBEAT_STALE" in payload["reason_codes"]


def test_alive_but_data_stale_during_open_market_is_degraded(tmp_path: Path) -> None:
    config = _write_clean_fixture(tmp_path)
    _write(
        config.resolve(config.phase1_listener_status_path),
        {"generated_at": _iso(NOW - timedelta(minutes=10)), "latest_record_at": _iso(NOW - timedelta(minutes=10))},
    )

    payload = build_track_b_live_runtime_environment_watchdog(
        config=config,
        now=NOW,
        pid_running=lambda pid: True,
        source_commit_resolver=lambda root: "abc",
    )

    assert payload["classification"] == DEGRADED_DATA_STALE
    assert "MARKET_DATA_STALE" in payload["reason_codes"]


def test_out_of_window_runtime_with_fresh_authority_is_healthy_observation(tmp_path: Path) -> None:
    config = _write_clean_fixture(tmp_path, in_window=False, ready_submit_capable=False)

    payload = build_track_b_live_runtime_environment_watchdog(
        config=config,
        now=NOW,
        pid_running=lambda pid: True,
        source_commit_resolver=lambda root: "abc",
    )

    assert payload["classification"] == OUT_OF_WINDOW_BUT_HEALTHY
    assert payload["lanes"]["all_active_lanes_out_of_window"] is True


def test_in_window_processed_bars_without_execution_context_advancing_is_degraded(tmp_path: Path) -> None:
    config = _write_clean_fixture(tmp_path)
    operator_status = _read(config.resolve(config.operator_status_path))
    operator_status["lanes"][0]["last_execution_bar_evaluated_at"] = _iso(NOW - timedelta(minutes=15))
    _write(config.resolve(config.operator_status_path), operator_status)

    payload = build_track_b_live_runtime_environment_watchdog(
        config=config,
        now=NOW,
        pid_running=lambda pid: True,
        source_commit_resolver=lambda root: "abc",
    )

    assert payload["classification"] == DEGRADED_LANES_NOT_EVALUATING
    assert "LANES_NOT_EVALUATING" in payload["reason_codes"]
    assert payload["lanes"]["stalled_lanes"][0]["lane_id"] == "mnq_globex_active_participation_long"


def test_process_git_head_mismatch_is_flagged(tmp_path: Path) -> None:
    config = _write_clean_fixture(tmp_path)

    payload = build_track_b_live_runtime_environment_watchdog(
        config=config,
        now=NOW,
        pid_running=lambda pid: True,
        source_commit_resolver=lambda root: "def",
    )

    assert payload["classification"] == READY_SUBMIT_CAPABLE
    assert "RUNTIME_CODE_VERSION_MISMATCH" in payload["reason_codes"]
    assert payload["runtime"]["code_version_matches_head"] is False


def test_no_automatic_restart_with_open_positions(tmp_path: Path) -> None:
    config = _write_clean_fixture(tmp_path)
    _write(
        config.resolve(config.broker_reconciliation_path),
        {
            "generated_at": _iso(NOW),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "track_b_broker_position_count": 1,
            "track_b_broker_open_order_count": 0,
            "lifecycle_open_position_count": 1,
        },
    )

    payload = build_track_b_live_runtime_environment_watchdog(
        config=config,
        now=NOW,
        pid_running=lambda pid: False,
        source_commit_resolver=lambda root: "abc",
    )

    assert payload["classification"] == RECOVERY_REQUIRED
    assert payload["restart_policy"]["process_died_restart_allowed"] is False
    assert "NO_AUTOMATIC_RESTART_OPEN_EXPOSURE_WITHOUT_PROVEN_IDENTITY" in payload["restart_policy"]["reason_codes"]


def test_restart_allowed_with_exact_registry_backed_managed_exposure(tmp_path: Path) -> None:
    config = _write_clean_fixture(tmp_path)
    _write(
        config.resolve(config.broker_reconciliation_path),
        {
            "generated_at": _iso(NOW),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            "track_b_broker_position_count": 1,
            "track_b_broker_open_order_count": 0,
            "lifecycle_open_position_count": 0,
            "track_b_broker_positions": [
                {
                    "account_id": "DUM882026",
                    "symbol": "MES",
                    "track_b_root": "MES",
                    "local_symbol": "MESM6",
                    "expiry": "202606",
                    "quantity": "-1",
                }
            ],
            "track_b_lifecycle_positions": [],
            "track_b_broker_open_orders": [],
        },
    )
    _write_registry_open_managed_events(tmp_path)

    payload = build_track_b_live_runtime_environment_watchdog(
        config=config,
        now=NOW,
        pid_running=lambda pid: False,
        source_commit_resolver=lambda root: "abc",
    )

    assert payload["restart_policy"]["process_died_restart_allowed"] is True
    assert payload["restart_policy"]["owned_exposure_restart_allowed"] is True
    assert payload["restart_policy"]["reason_codes"] == []
    assert (
        payload["pre_restart_exposure_resolution"]["classification"]
        == "PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED"
    )


def test_safe_restart_allowed_only_when_flat_clean(tmp_path: Path) -> None:
    config = _write_clean_fixture(tmp_path)

    payload = build_track_b_live_runtime_environment_watchdog(
        config=config,
        now=NOW,
        pid_running=lambda pid: False,
        source_commit_resolver=lambda root: "abc",
    )

    assert payload["classification"] == RECOVERY_REQUIRED
    assert payload["restart_policy"]["process_died_restart_allowed"] is True
    assert payload["restart_policy"]["reason_codes"] == []


def test_current_scope_zero_diagnostics_outrank_stale_raw_lifecycle_count(tmp_path: Path) -> None:
    config = _write_clean_fixture(tmp_path)
    reconciliation = _read(config.resolve(config.broker_reconciliation_path))
    reconciliation["lifecycle_open_position_count"] = 1
    reconciliation["track_b_lifecycle_positions"] = [
        {
            "trade_id": "trade_closed",
            "lifecycle_id": "life_closed",
            "symbol": "MES",
            "local_symbol": "MESM6",
            "con_id": 770561194,
            "quantity": "1",
        }
    ]
    _write(config.resolve(config.broker_reconciliation_path), reconciliation)
    registry = _read(config.resolve(config.registry_diagnostics_path))
    registry["current_scope_review_required_count"] = 0
    registry["review_required_trade_ids"] = ["historical_full_audit_only"]
    registry["lifecycle_open_position_count"] = 0
    _write(config.resolve(config.registry_diagnostics_path), registry)

    payload = build_track_b_live_runtime_environment_watchdog(
        config=config,
        now=NOW,
        pid_running=lambda pid: True,
        source_commit_resolver=lambda root: "abc",
    )

    assert payload["classification"] == READY_SUBMIT_CAPABLE
    assert payload["liveness_contract"]["registry_reconciliation_matched"] is True
    assert "REGISTRY_RECONCILIATION_NOT_MATCHED" not in payload["reason_codes"]
    assert payload["restart_policy"]["reason_codes"] == []


def _write_clean_fixture(
    tmp_path: Path,
    *,
    in_window: bool = True,
    ready_submit_capable: bool = True,
) -> TrackBLiveRuntimeEnvironmentWatchdogConfig:
    config = TrackBLiveRuntimeEnvironmentWatchdogConfig(repo_root=tmp_path)
    for path in (
        config.output_path,
        config.runtime_truth_path,
        config.operator_status_path,
        config.canonical_readiness_path,
        config.authority_refresh_path,
        config.control_plane_snapshot_path,
        config.safe_state_envelope_path,
        config.runtime_supervisor_authority_path,
        config.broker_reconciliation_path,
        config.broker_truth_refresh_status_path,
        config.registry_diagnostics_path,
        config.phase1_listener_status_path,
        config.recovery_status_path,
    ):
        config.resolve(path).parent.mkdir(parents=True, exist_ok=True)

    _write(
        config.resolve(config.runtime_truth_path),
        {
            "generated_at": _iso(NOW),
            "producer_pid": 1234,
            "source_commit": "abc",
            "lane_count": 8,
        },
    )
    lane = {
        "lane_id": "mnq_globex_active_participation_long",
        "eligible_now": in_window,
        "current_session_window_classification": "IN_WINDOW" if in_window else "OUT_OF_WINDOW",
        "last_processed_bar_end_ts": _iso(NOW - timedelta(minutes=1)),
        "last_execution_bar_evaluated_at": _iso(NOW - timedelta(minutes=1)),
    }
    _write(
        config.resolve(config.operator_status_path),
        {
            "generated_at": _iso(NOW),
            "active_lane_ids": [lane["lane_id"]],
            "lanes": [lane],
        },
    )
    _write(
        config.resolve(config.canonical_readiness_path),
        {
            "generated_at": _iso(NOW),
            "canonical_readiness": "READY_SUBMIT_CAPABLE" if ready_submit_capable else "READY_DIAGNOSTIC_ONLY",
            "submit_allowed": ready_submit_capable,
            "market_schedule_state": "MARKET_OPEN_EXPECT_FRESH_BARS",
        },
    )
    _write(
        config.resolve(config.authority_refresh_path),
        {
            "generated_at": _iso(NOW),
            "classification": "AUTHORITY_REFRESHED",
            "latest_successful_refresh_at": _iso(NOW),
        },
    )
    for path in (
        config.control_plane_snapshot_path,
        config.safe_state_envelope_path,
        config.runtime_supervisor_authority_path,
    ):
        _write(config.resolve(path), {"generated_at": _iso(NOW), "classification": "FRESH"})
    _write(
        config.resolve(config.broker_reconciliation_path),
        {
            "generated_at": _iso(NOW),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "lifecycle_open_position_count": 0,
        },
    )
    _write(config.resolve(config.broker_truth_refresh_status_path), {"generated_at": _iso(NOW), "fresh": True})
    _write(
        config.resolve(config.registry_diagnostics_path),
        {
            "generated_at": _iso(NOW),
            "classification": "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE",
            "current_scope_review_required_count": 0,
            "broker_open_order_count": 0,
            "lifecycle_open_position_count": 0,
            "track_b_managed_futures_position_count": 0,
        },
    )
    _write(
        config.resolve(config.phase1_listener_status_path),
        {"generated_at": _iso(NOW), "latest_record_at": _iso(NOW - timedelta(seconds=30))},
    )
    _write(config.resolve(config.recovery_status_path), {"generated_at": _iso(NOW), "classification": "RECOVERY_ACTIVE"})
    return config


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_registry_open_managed_events(root: Path) -> None:
    path = root / "outputs" / "track_b_execution_core" / "trade_registry" / "live_trade_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    base = {
        "trade_id": "trade_mes",
        "lifecycle_id": "life_mes",
        "lane_id": "mes_globex_active_participation_short",
        "thesis_strategy_id": "mes_globex_active_participation_short",
        "account_id": "DUM882026",
        "symbol": "MES",
        "con_id": 770561194,
        "local_symbol": "MESM6",
        "expiry": "202606",
        "side": "SHORT",
        "action": "SELL",
        "qty": Decimal("1"),
        "source_artifact_path": "outputs/track_b_execution_core/test.json",
    }
    events = [
        TradeEvent(
            event_id="trade_mes_fill",
            event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
            generated_at=NOW,
            order_id="1",
            client_id="111",
            perm_id="perm_mes",
            exec_id="exec_mes",
            price=Decimal("7598.75"),
            **base,
        ),
        TradeEvent(
            event_id="trade_mes_open",
            event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
            generated_at=NOW + timedelta(seconds=1),
            metadata={"managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"},
            **base,
        ),
    ]
    path.write_text("\n".join(json.dumps(event.to_dict(), sort_keys=True) for event in events) + "\n", encoding="utf-8")


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()
