from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_shared_truth_refresh_cli import (
    DEFAULT_RECONCILIATION_ARTIFACT,
    TrackBSharedTruthRefreshConfig,
    main,
    refresh_track_b_shared_truth,
)


NOW = datetime(2026, 5, 22, 18, 0, tzinfo=UTC)
OLD = "2026-05-22T17:00:00+00:00"


def test_refresh_clean_flat_stack(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)

    result = _refresh(tmp_path)

    assert result["exit_code"] == 0
    assert result["classifications"]["Open Order Truth"] == "NO_OPEN_ORDERS"
    assert result["classifications"]["Managed Order Registry"] == "NO_MANAGED_ORDERS"
    assert result["classifications"]["Position Truth"] == "CLEAN_FLAT_READY"
    assert result["classifications"]["Runtime Environment Truth"] == "RUNTIME_DOWN_CLEAN"
    assert result["classifications"]["Managed Position Registry"] == "NO_MANAGED_POSITIONS"
    assert result["classifications"]["Reconciliation"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert result["unsafe_blockers"] == []
    assert _read(tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json")[
        "classification"
    ] == "NO_OPEN_ORDERS"


def test_refresh_replaces_stale_upstream_authority_artifact(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    stale_path = tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json"
    _write(stale_path, {"generated_at": OLD, "classification": "ORDER_TRUTH_STALE"})

    result = _refresh(tmp_path)

    refreshed = _read(stale_path)
    assert result["exit_code"] == 0
    assert refreshed["classification"] == "NO_OPEN_ORDERS"
    assert refreshed["generated_at"] == NOW.isoformat()


def test_refresh_broker_exposure_produces_attention_required(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _write_reconciliation(
        tmp_path,
        classification="BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        broker_reconciled=False,
        broker_positions=[{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1.0", "account": "DUM882026"}],
    )
    _write_broker_status(
        tmp_path,
        positions=[{"symbol": "MGC", "local_symbol": "MGCM6", "quantity": "1.0", "account": "DUM882026"}],
    )

    result = _refresh(tmp_path)

    assert result["exit_code"] == 2
    assert result["classifications"]["Open Order Truth"] == "BROKER_POSITION_WITHOUT_CLOSE_ORDER"
    assert result["classifications"]["Position Truth"] == "ATTENTION_REQUIRED"
    assert result["classifications"]["Runtime Environment Truth"] == "RUNTIME_DOWN_WITH_BROKER_EXPOSURE"
    assert any(blocker["code"] == "position_truth_attention_required" for blocker in result["unsafe_blockers"])


def test_dashboard_projections_are_not_consumed_or_written(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)

    result = _refresh(tmp_path)

    assert result["exit_code"] == 0
    projection_paths = [
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_open_order_truth.json",
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_managed_orders.json",
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_position_truth.json",
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_runtime_environment_truth.json",
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_managed_positions.json",
    ]
    assert all(not path.exists() for path in projection_paths)
    for path in result["artifact_paths"].values():
        assert "outputs/operator_dashboard/runtime/latest_track_b_" not in str(path)


def test_main_runtime_down_clean_exits_zero(tmp_path: Path, capsys) -> None:
    _seed_clean_stack(tmp_path, now=datetime.now(UTC))

    exit_code = main(["--repo-root", str(tmp_path), "--no-broker-lease-history"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Runtime Environment Truth" in output
    assert "RUNTIME_DOWN_CLEAN" in output


def _refresh(root: Path) -> dict:
    return refresh_track_b_shared_truth(
        config=TrackBSharedTruthRefreshConfig(repo_root=root, broker_lease_history_path=None),
        now=NOW,
        pid_running=lambda _pid: False,
        process_root_resolver=lambda _pid: None,
        source_commit_resolver=lambda _root: "test-head",
    )


def _seed_clean_stack(root: Path, *, now: datetime = NOW) -> None:
    _write_reconciliation(root, now=now)
    _write_broker_status(root, now=now)
    _write_live_position_status(root, now=now)
    _write_trade_summary(root, now=now)
    _write(
        root / "outputs/track_b_execution_core/position_truth/latest_position_truth.json",
        {
            "generated_at": now.isoformat(),
            "summary": {
                "overall_classification": "CLEAN_FLAT_READY",
                "broker_exposure_present": False,
            },
            "broker_positions": [],
            "open_broker_orders": [],
        },
    )
    _write(
        root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": now.isoformat(),
            "classification": "NO_MANAGED_POSITIONS",
            "managed_positions": [],
            "summary": {"managed_position_count": 0},
        },
    )


def _write_reconciliation(
    root: Path,
    *,
    now: datetime = NOW,
    classification: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_reconciled: bool = True,
    broker_positions: list[dict] | None = None,
) -> None:
    positions = broker_positions or []
    _write(
        root / DEFAULT_RECONCILIATION_ARTIFACT,
        {
            "generated_at": now.isoformat(),
            "classification": classification,
            "broker_reconciled": broker_reconciled,
            "live_money_eligible": False,
            "track_b_broker_positions": positions,
            "track_b_broker_open_orders": [],
            "track_b_lifecycle_positions": [],
            "unknown_broker_open_orders": [],
            "known_managed_exit_orders": [],
            "unresolved_submit_intent_ownership_records": [],
            "track_b_broker_position_count": len(positions),
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "review_required_count": 0,
            "unresolved_submit_intent_ownership_count": 0,
            "lifecycle_open_position_count": 0,
            "lifecycle_open_order_count": 0,
            "position_match_report": {"state": "BROKER_AND_LIFECYCLE_FLAT", "matched": broker_reconciled},
            "blockers": [] if broker_reconciled else [{"code": "broker_position_without_lifecycle"}],
        },
    )


def _write_broker_status(root: Path, *, now: datetime = NOW, positions: list[dict] | None = None) -> None:
    payload = {
        "classification": "BROKER_TRUTH_REFRESH_READY",
        "account": "DUM882026",
        "generated_at": now.isoformat(),
        "positions_complete": True,
        "open_orders_complete": True,
        "positions": positions or [],
        "open_orders": [],
        "live_money_eligible": False,
    }
    _write(
        root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json",
        {
            **payload,
            "last_successful_broker_truth": payload,
            "latest_attempt_status": payload,
        },
    )
    _write(root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_latest_attempt_status.json", payload)


def _write_live_position_status(root: Path, *, now: datetime = NOW) -> None:
    _write(
        root / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json",
        {
            "generated_at": now.isoformat(),
            "open_position_count": 0,
            "open_order_count": 0,
            "open_positions": [],
            "review_required_positions": [],
            "live_money_eligible": False,
        },
    )


def _write_trade_summary(root: Path, *, now: datetime = NOW) -> None:
    _write(
        root / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_paper_trade_summary.json",
        {
            "generated_at": now.isoformat(),
            "review_required_count": 0,
            "unknown_open_order_count": 0,
            "unresolved_intent_count": 0,
            "live_money_eligible": False,
        },
    )


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))
