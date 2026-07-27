from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_position_truth_monitor import (
    CLOSE_ORDER_SUSPICIOUS,
    CLOSE_ORDER_WORKING,
    FLAT_CLEAN,
    OPEN_MANAGED_MATCHED,
    TrackBPositionTruthMonitorConfig,
    build_track_b_position_truth,
    write_track_b_position_truth,
)
from mgc_v05l.execution_core.track_b_managed_order_registry import ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING


NOW = datetime(2026, 5, 22, 9, 40, tzinfo=UTC)


def test_position_truth_reports_clean_flat(tmp_path: Path) -> None:
    _seed_clean(tmp_path)

    payload = build_track_b_position_truth(config=TrackBPositionTruthMonitorConfig(repo_root=tmp_path), now=NOW)

    assert payload["read_only"] is True
    assert payload["submit_authority"] is False
    assert payload["summary"]["overall_classification"] == "CLEAN_FLAT_READY"
    assert payload["open_order_truth"]["classification"] == "NO_OPEN_ORDERS"
    assert payload["managed_order_registry"]["classification"] == "NO_MANAGED_ORDERS"
    assert payload["summary"]["managed_order_count"] == 0
    assert payload["open_order_truth"]["source"] == "OPEN_ORDER_TRUTH_BUILDER_DIRECT"
    assert payload["reconciliation"]["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert payload["live_money_eligible"] is False
    assert {row["classification"] for row in payload["position_states"]} == {FLAT_CLEAN}


def test_position_truth_includes_backward_compatible_dmc_metadata(tmp_path: Path) -> None:
    _seed_clean(tmp_path)
    config = TrackBPositionTruthMonitorConfig(repo_root=tmp_path)

    payload = build_track_b_position_truth(config=config, now=NOW)

    assert payload["schema_version"] == "track_b_position_truth_v1"
    assert payload["generated_at"] == NOW.isoformat()
    assert payload["summary"]["overall_classification"] == "CLEAN_FLAT_READY"

    metadata = payload["dmc_metadata"]
    assert metadata["schema_version"] == "track_b_dmc_metadata_envelope_v1"
    assert metadata["artifact_family"] == "latest_position_truth"
    assert metadata["authority_tier"] == "Tier 1 – Canonical"
    assert metadata["publisher_id"] == "track_b_position_truth.py"
    assert metadata["owner_id"] == "Position Truth"
    assert metadata["generated_at"] == NOW.isoformat()
    assert metadata["source_observed_at"] == NOW.isoformat()
    assert metadata["refresh_scope"] == {
        "scope_type": "GLOBAL_COMPLETE",
        "account_scope": "Track B PAPER",
        "symbols": "ALL_TRACK_B_FUTURES_FROM_RECONCILIATION",
        "partial": False,
    }
    assert metadata["retention_model"] == "rolling latest snapshot with append-only trade outcome event companion"
    assert metadata["append_only"] is False
    assert metadata["diagnostic_only"] is False
    assert metadata["analytics_only"] is False
    assert metadata["can_influence_runtime"] is True
    assert metadata["can_influence_managed_exit"] is True
    assert {source["artifact_family"] for source in metadata["source_artifacts"]} == {
        "track_b_paper_broker_reconciliation",
        "open_order_truth",
        "managed_order_registry",
        "runtime_truth",
    }


def test_written_position_truth_sample_preserves_existing_consumer_fields_with_dmc_metadata(tmp_path: Path) -> None:
    _seed_clean(tmp_path)
    config = TrackBPositionTruthMonitorConfig(repo_root=tmp_path)
    payload = build_track_b_position_truth(config=config, now=NOW)

    authority_path, _ = write_track_b_position_truth(config=config, payload=payload, now=NOW)

    written = json.loads(authority_path.read_text(encoding="utf-8"))
    assert written["schema_version"] == "track_b_position_truth_v1"
    assert written["summary"]["overall_classification"] == "CLEAN_FLAT_READY"
    assert written["broker_positions"] == []
    assert written["open_broker_orders"] == []
    assert written["dmc_metadata"]["artifact_family"] == "latest_position_truth"
    assert written["dmc_metadata"]["publisher_id"] == "track_b_position_truth.py"


def test_position_truth_surfaces_suspicious_managed_order_evidence(tmp_path: Path) -> None:
    _seed_clean(tmp_path)
    _write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json",
        {
            "schema_version": "track_b_managed_order_registry_v1",
            "generated_at": NOW.isoformat(),
            "classification": "CLOSE_ORDER_SUSPICIOUS",
            "summary": {
                "managed_order_count": 1,
                "suspicious_order_count": 1,
                "duplicate_close_order_count": 0,
                "working_close_order_count": 1,
                "modifiable_close_order_count": 0,
            },
        },
    )

    payload = build_track_b_position_truth(config=TrackBPositionTruthMonitorConfig(repo_root=tmp_path), now=NOW)

    assert payload["managed_order_registry"]["classification"] == "CLOSE_ORDER_SUSPICIOUS"
    assert payload["summary"]["managed_order_count"] == 1
    assert payload["summary"]["managed_order_suspicious_order_count"] == 1


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
    assert mnq["suspicious_order_findings"][0]["source"] == "OPEN_ORDER_TRUTH"
    assert payload["summary"]["suspicious_order_count"] == 1
    assert payload["open_order_truth"]["classification"] == "SUSPICIOUS_ORDER_STATE"


def test_position_truth_flags_working_close_order_via_open_order_truth(tmp_path: Path) -> None:
    _seed_working_mnq_close(tmp_path)

    payload = build_track_b_position_truth(config=TrackBPositionTruthMonitorConfig(repo_root=tmp_path), now=NOW)
    mnq = _state(payload, "MNQ")

    assert payload["open_order_truth"]["summary"]["working_close_order_count"] == 1
    assert payload["summary"]["working_close_order_count"] == 1
    assert mnq["classification"] == CLOSE_ORDER_WORKING
    assert mnq["open_order_truth_states"][0]["classification"] == "OPEN_CLOSE_ORDER_WORKING"


def test_position_truth_handles_missing_open_order_truth_artifact_with_builder(tmp_path: Path) -> None:
    _seed_working_mnq_close(tmp_path)

    payload = build_track_b_position_truth(config=TrackBPositionTruthMonitorConfig(repo_root=tmp_path), now=NOW)

    assert payload["open_order_truth"]["source"] == "OPEN_ORDER_TRUTH_BUILDER_DIRECT"
    assert payload["open_order_truth"]["stale_or_missing"] is False
    assert not (tmp_path / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json").exists()


def test_position_truth_fails_loud_when_open_order_truth_builder_unavailable(tmp_path: Path, monkeypatch) -> None:
    _seed_clean(tmp_path)

    def _raise(*, config, now):  # noqa: ANN001
        raise RuntimeError("open order truth unavailable")

    monkeypatch.setattr(
        "mgc_v05l.execution_core.track_b_position_truth_monitor.build_track_b_open_order_truth",
        _raise,
    )

    payload = build_track_b_position_truth(config=TrackBPositionTruthMonitorConfig(repo_root=tmp_path), now=NOW)

    assert payload["open_order_truth"]["source"] == "OPEN_ORDER_TRUTH_AUTHORITY_ARTIFACT_FALLBACK"
    assert payload["open_order_truth"]["stale_or_missing"] is True


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


def test_authority_artifact_and_event_log_live_under_execution_core(tmp_path: Path) -> None:
    config = TrackBPositionTruthMonitorConfig(repo_root=tmp_path)
    _seed_clean(tmp_path)
    payload = build_track_b_position_truth(config=config, now=NOW)

    authority_path, _ = write_track_b_position_truth(config=config, payload=payload, now=NOW)
    dashboard_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]

    assert authority_path == tmp_path / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
    assert config.resolve(config.event_log_path) == (
        tmp_path / "outputs" / "track_b_execution_core" / "position_truth" / "track_b_trade_outcome_events.jsonl"
    )
    assert authority_path.exists()
    assert dashboard_path.exists()
    projection = json.loads(dashboard_path.read_text(encoding="utf-8"))
    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)
    assert projection["authority_owner"] == "execution_core"


def test_critical_runtime_paths_do_not_consume_dashboard_projection_as_authority() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    forbidden = "outputs/operator_dashboard/runtime/latest_track_b_position_truth.json"
    forbidden_open_order_truth = "outputs/operator_dashboard/runtime/latest_track_b_open_order_truth.json"
    forbidden_managed_orders = "outputs/operator_dashboard/runtime/latest_track_b_managed_orders.json"
    critical_paths = [
        repo_root / "src/mgc_v05l/app/probationary_runtime.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_readiness_state.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_readiness_authority.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_self_healing_supervisor.py",
        repo_root / "src/mgc_v05l/app/track_b_self_healing_supervisor.py",
        repo_root / "src/mgc_v05l/app/track_b_readiness_maintenance_supervisor.py",
    ]

    offenders = [
        str(path)
        for path in critical_paths
        if forbidden in path.read_text(encoding="utf-8")
        or forbidden_open_order_truth in path.read_text(encoding="utf-8")
        or forbidden_managed_orders in path.read_text(encoding="utf-8")
    ]

    assert offenders == []


def test_open_managed_matched_classification(tmp_path: Path) -> None:
    _seed_open_managed(tmp_path)

    payload = build_track_b_position_truth(config=TrackBPositionTruthMonitorConfig(repo_root=tmp_path), now=NOW)
    mgc = _state(payload, "MGC")

    assert payload["summary"]["overall_classification"] == OPEN_MANAGED_MATCHED
    assert mgc["classification"] == OPEN_MANAGED_MATCHED
    assert mgc["open_managed_valid"] is True
    assert mgc["broker_quantity"] == "1"
    assert mgc["lifecycle_quantity"] == "1"


def test_broker_contradicted_lifecycle_debris_is_diagnostic_with_open_managed_position(tmp_path: Path) -> None:
    _seed_open_managed(tmp_path)
    payload = json.loads(_reconciliation_path(tmp_path).read_text(encoding="utf-8"))
    payload["broker_reconciled"] = False
    payload["symbols"] = ["MGC", "ES"]
    payload["track_b_lifecycle_positions"].append(
        {
            "instrument_family": "ES",
            "local_symbol": "ESU6",
            "quantity": "1",
            "lifecycle_id": "stale_es_lifecycle",
            "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        }
    )
    _write_json(_reconciliation_path(tmp_path), payload)

    position_truth = build_track_b_position_truth(config=TrackBPositionTruthMonitorConfig(repo_root=tmp_path), now=NOW)

    assert position_truth["summary"]["overall_classification"] == OPEN_MANAGED_MATCHED
    assert position_truth["summary"]["diagnostic_lifecycle_debris_count"] == 1
    assert position_truth["summary"]["diagnostic_lifecycle_debris_ignored"] is True
    assert _state(position_truth, "ES")["classification"] == "LIFECYCLE_POSITION_WITHOUT_BROKER"


def test_current_broker_position_without_lifecycle_still_requires_adoption(tmp_path: Path) -> None:
    _seed_clean(tmp_path)
    payload = json.loads(_reconciliation_path(tmp_path).read_text(encoding="utf-8"))
    payload["track_b_broker_positions"] = [
        {"symbol": "MGC", "track_b_root": "MGC", "local_symbol": "MGCM6", "quantity": "1", "average_cost": "45230.0"}
    ]
    _write_json(_reconciliation_path(tmp_path), payload)

    position_truth = build_track_b_position_truth(config=TrackBPositionTruthMonitorConfig(repo_root=tmp_path), now=NOW)

    assert position_truth["summary"]["overall_classification"] == "ATTENTION_REQUIRED"
    assert _state(position_truth, "MGC")["classification"] == "BROKER_POSITION_REQUIRES_ADOPTION"


def test_active_timed_hold_is_not_attention_required_summary(tmp_path: Path) -> None:
    _seed_open_managed(tmp_path)
    _write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json",
        {
            "schema_version": "track_b_managed_order_registry_v1",
            "generated_at": NOW.isoformat(),
            "classification": ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING,
            "summary": {
                "managed_order_count": 1,
                "active_hold_managed_timed_exit_pending_count": 1,
                "suspicious_order_count": 0,
                "duplicate_close_order_count": 0,
                "working_close_order_count": 0,
                "modifiable_close_order_count": 0,
            },
        },
    )

    payload = build_track_b_position_truth(config=TrackBPositionTruthMonitorConfig(repo_root=tmp_path), now=NOW)

    assert payload["summary"]["overall_classification"] == ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING
    assert payload["summary"]["active_hold_managed_timed_exit_pending"] is True


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
    _write_json(
        root / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json",
        {
            "schema_version": "track_b_managed_order_registry_v1",
            "generated_at": NOW.isoformat(),
            "classification": "NO_MANAGED_ORDERS",
            "summary": {
                "managed_order_count": 0,
                "suspicious_order_count": 0,
                "duplicate_close_order_count": 0,
                "working_close_order_count": 0,
                "modifiable_close_order_count": 0,
            },
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


def _seed_working_mnq_close(root: Path) -> None:
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
        "filled_quantity": "0",
        "remaining_quantity": "1",
        "order_type": "LMT",
        "limit_price": "29555.5",
        "observed_at": NOW.isoformat(),
        "submitted_at": NOW.isoformat(),
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
            "review_required_count": 0,
        }
    )
    _write_json(_reconciliation_path(root), payload)


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
