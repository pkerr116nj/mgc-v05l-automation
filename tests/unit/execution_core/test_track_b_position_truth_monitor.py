from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_position_truth_monitor import (
    CLOSE_ORDER_SUSPICIOUS,
    FLAT_CLEAN,
    OPEN_MANAGED_MATCHED,
    TrackBPositionTruthMonitorConfig,
    build_track_b_position_truth,
    write_track_b_position_truth,
)


NOW = datetime(2026, 5, 22, 9, 40, tzinfo=UTC)


def test_position_truth_reports_clean_flat(tmp_path: Path) -> None:
    _seed_clean(tmp_path)

    payload = build_track_b_position_truth(config=TrackBPositionTruthMonitorConfig(repo_root=tmp_path), now=NOW)

    assert payload["read_only"] is True
    assert payload["submit_authority"] is False
    assert payload["summary"]["overall_classification"] == "CLEAN_FLAT_READY"
    assert payload["reconciliation"]["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert payload["live_money_eligible"] is False
    assert {row["classification"] for row in payload["position_states"]} == {FLAT_CLEAN}


def test_position_truth_flags_suspicious_working_close_order(tmp_path: Path) -> None:
    _seed_suspicious_mnq_close(tmp_path)

    payload = build_track_b_position_truth(config=TrackBPositionTruthMonitorConfig(repo_root=tmp_path), now=NOW)
    mnq = _state(payload, "MNQ")

    assert mnq["classification"] == CLOSE_ORDER_SUSPICIOUS
    reasons = mnq["suspicious_order_findings"][0]["reasons"]
    assert "sentinel_filled_quantity" in reasons
    assert "missing_remaining_quantity" in reasons
    assert "open_close_order_without_execDetails" in reasons
    assert "marketable_unfilled_beyond_threshold" in reasons
    assert payload["summary"]["suspicious_order_count"] == 1


def test_stale_runtime_truth_does_not_report_runtime_running(tmp_path: Path) -> None:
    _seed_clean(tmp_path)
    _write_json(
        _runtime_truth_path(tmp_path),
        {
            "runtime_instance_id": "runtime-test",
            "producer_pid": 123,
            "heartbeat_state": "HEALTHY",
            "freshness_state": "FRESH",
            "freshness_ttl_seconds": 180,
            "writer_authority": "SINGLE_WRITER",
            "generated_at": "2026-05-22T08:00:00+00:00",
        },
    )

    payload = build_track_b_position_truth(config=TrackBPositionTruthMonitorConfig(repo_root=tmp_path), now=NOW)

    assert payload["runtime_status"]["runtime_running"] is False
    assert payload["runtime_status"]["runtime_truth_fresh"] is False


def test_trade_outcome_events_append_only_on_meaningful_changes(tmp_path: Path) -> None:
    config = TrackBPositionTruthMonitorConfig(repo_root=tmp_path)
    _seed_suspicious_mnq_close(tmp_path)

    first = build_track_b_position_truth(config=config, now=NOW)
    _, first_events = write_track_b_position_truth(config=config, payload=first, now=NOW)
    _, repeated_events = write_track_b_position_truth(config=config, payload=first, now=NOW)

    assert any(event["event_type"] == "SUSPICIOUS_ORDER_DETECTED" for event in first_events)
    assert repeated_events == []

    _seed_clean(tmp_path)
    clean = build_track_b_position_truth(config=config, now=NOW)
    _, clean_events = write_track_b_position_truth(config=config, payload=clean, now=NOW)

    event_types = {event["event_type"] for event in clean_events}
    assert "RECONCILIATION_CLEAN" in event_types
    assert "LIFECYCLE_CLOSED_FLAT" in event_types
    lines = config.resolve(config.event_log_path).read_text(encoding="utf-8").splitlines()
    assert len(lines) == len(first_events) + len(clean_events)


def test_open_managed_matched_classification(tmp_path: Path) -> None:
    _seed_open_managed(tmp_path)

    payload = build_track_b_position_truth(config=TrackBPositionTruthMonitorConfig(repo_root=tmp_path), now=NOW)
    mgc = _state(payload, "MGC")

    assert mgc["classification"] == OPEN_MANAGED_MATCHED
    assert mgc["open_managed_valid"] is True
    assert mgc["broker_quantity"] == "1"
    assert mgc["lifecycle_quantity"] == "1"


def _seed_clean(root: Path) -> None:
    _write_json(
        _reconciliation_path(root),
        {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "generated_at": NOW.isoformat(),
            "broker_reconciled": True,
            "symbols": ["MGC", "MNQ"],
            "track_b_broker_positions": [],
            "track_b_broker_open_orders": [],
            "track_b_lifecycle_positions": [],
            "unknown_broker_open_orders": [],
            "known_managed_exit_orders": [],
            "unresolved_submit_intent_ownership_records": [],
            "lifecycle_open_position_count": 0,
            "review_required_count": 0,
            "unresolved_submit_intent_ownership_count": 0,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write_json(
        _broker_lease_path(root),
        {"lease_state": "ACTIVE", "live_money_eligible": False, "operator_action_required": False},
    )
    _write_json(
        _runtime_truth_path(root),
        {
            "runtime_instance_id": "runtime-test",
            "producer_pid": 123,
            "heartbeat_state": "HEALTHY",
            "freshness_state": "FRESH",
            "writer_authority": "SINGLE_WRITER",
            "lane_count": 17,
            "generated_at": NOW.isoformat(),
        },
    )


def _seed_open_managed(root: Path) -> None:
    _seed_clean(root)
    payload = json.loads(_reconciliation_path(root).read_text(encoding="utf-8"))
    payload["broker_reconciled"] = True
    payload["track_b_broker_positions"] = [
        {"symbol": "MGC", "track_b_root": "MGC", "local_symbol": "MGCM6", "quantity": "1", "average_cost": "45230.0"}
    ]
    payload["track_b_lifecycle_positions"] = [
        {
            "instrument_family": "MGC",
            "local_symbol": "MGCM6",
            "quantity": "1",
            "lifecycle_id": "bridge_fill_mgc",
            "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        }
    ]
    payload["lifecycle_open_position_count"] = 1
    _write_json(_reconciliation_path(root), payload)


def _seed_suspicious_mnq_close(root: Path) -> None:
    _seed_clean(root)
    payload = json.loads(_reconciliation_path(root).read_text(encoding="utf-8"))
    order = {
        "symbol": "MNQ",
        "track_b_root": "MNQ",
        "local_symbol": "MNQM6",
        "broker_order_id": 27,
        "client_id": 17086,
        "perm_id": 347068546,
        "action": "SELL",
        "quantity": "1",
        "filled_quantity": "1.7976931348623157e+308",
        "remaining_quantity": None,
        "order_type": "LMT",
        "limit_price": "29555.5",
        "status": "Submitted",
    }
    payload.update(
        {
            "classification": "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE",
            "broker_reconciled": False,
            "track_b_broker_positions": [
                {"symbol": "MNQ", "track_b_root": "MNQ", "local_symbol": "MNQM6", "quantity": "1", "average_cost": "59220.12"}
            ],
            "track_b_broker_open_orders": [order],
            "unknown_broker_open_orders": [order],
            "review_required_count": 1,
        }
    )
    _write_json(_reconciliation_path(root), payload)
    _write_json(
        root
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / "MNQ"
        / "1m"
        / "latest_runtime_candles.json",
        {
            "generated_at": NOW.isoformat(),
            "bars": [{"bar_end": NOW.isoformat(), "close": "29560.0"}],
        },
    )
    _write_json(
        root
        / "outputs"
        / "track_b_execution_core"
        / "track_b_strategy_managed_paper_lifecycle"
        / "bridge_fill_submit_owner_a91ce1353813fac5eebfd954"
        / "track_b_strategy_managed_paper_lifecycle_report.json",
        {
            "lifecycle_id": "bridge_fill_submit_owner_a91ce1353813fac5eebfd954",
            "instrument_family": "MNQ",
            "local_symbol": "MNQM6",
            "final_position_status": "REVIEW_REQUIRED",
            "review_required": True,
            "close_submit_attempt": {
                "broker_order_id": "27",
                "submitted_at": "2026-05-22T09:01:59+00:00",
                "submit_diagnostics": {"execDetails_seen": False},
            },
        },
    )


def _state(payload: dict, symbol: str) -> dict:
    return next(row for row in payload["position_states"] if row["symbol"] == symbol)


def _reconciliation_path(root: Path) -> Path:
    return (
        root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )


def _broker_lease_path(root: Path) -> Path:
    return root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"


def _runtime_truth_path(root: Path) -> Path:
    return root / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_runtime_truth.json"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
