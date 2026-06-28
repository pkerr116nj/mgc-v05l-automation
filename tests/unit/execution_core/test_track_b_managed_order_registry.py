from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEvent, TradeEventType
from mgc_v05l.execution_core.track_b_managed_order_registry import (
    ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING,
    BROKER_FLAT_WITH_WORKING_CLOSE,
    CLOSE_ORDER_MODIFIABLE,
    CLOSE_ORDER_NOT_MARKETABLE,
    CLOSE_ORDER_SUSPICIOUS,
    DUPLICATE_CLOSE_ORDER_BLOCKED,
    MODIFY_IN_PLACE_CANDIDATE,
    NO_MANAGED_ORDERS,
    POSITION_WITHOUT_CLOSE_ORDER,
    TARGETED_CANCEL_REPLACE_CANDIDATE,
    TrackBManagedOrderRegistryConfig,
    WORKING_CLOSE_ORDER,
    build_track_b_managed_order_registry,
    write_track_b_managed_order_registry,
)
import mgc_v05l.execution_core.track_b_managed_order_registry as managed_order_registry_module


NOW = datetime(2026, 5, 22, 17, 15, tzinfo=UTC)


def test_no_orders_reports_no_managed_orders(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == NO_MANAGED_ORDERS
    assert payload["summary"]["managed_order_count"] == 0
    assert payload["read_only"] is True
    assert payload["submit_authority"] is False
    assert payload["paper_proof_invoked"] is False
    assert payload["live_money_eligible"] is False


def test_managed_order_registry_includes_backward_compatible_dmc_metadata(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["schema_version"] == "track_b_managed_order_registry_v1"
    assert payload["generated_at"] == NOW.isoformat()
    assert payload["classification"] == NO_MANAGED_ORDERS
    assert payload["summary"]["managed_order_count"] == 0

    metadata = payload["dmc_metadata"]
    assert metadata["schema_version"] == "track_b_dmc_metadata_envelope_v1"
    assert metadata["artifact_family"] == "latest_managed_orders"
    assert metadata["authority_tier"] == "Tier 1 – Canonical"
    assert metadata["publisher_id"] == "track_b_managed_order_registry.py"
    assert metadata["owner_id"] == "Managed Order Registry"
    assert metadata["generated_at"] == NOW.isoformat()
    assert metadata["source_observed_at"] == NOW.isoformat()
    assert metadata["refresh_scope"] == {
        "scope_type": "GLOBAL_COMPLETE",
        "account_scope": "Track B PAPER",
        "symbols": "ALL_TRACK_B_FUTURES_FROM_OPEN_ORDER_TRUTH",
        "partial": False,
    }
    assert metadata["retention_model"] == "rolling latest snapshot with append-only managed order event companion"
    assert metadata["append_only"] is False
    assert metadata["diagnostic_only"] is False
    assert metadata["analytics_only"] is False
    assert metadata["can_influence_runtime"] is True
    assert metadata["can_influence_managed_exit"] is True
    assert {source["artifact_family"] for source in metadata["source_artifacts"]} == {
        "open_order_truth",
        "position_truth",
        "managed_position_registry",
        "track_b_paper_broker_reconciliation",
    }


def test_one_working_close_order_is_tracked(tmp_path: Path) -> None:
    _seed_base(tmp_path, order_states=[_order_state()])

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == WORKING_CLOSE_ORDER
    order = payload["managed_orders"][0]
    assert order["classification"] == WORKING_CLOSE_ORDER
    assert order["broker_order_id"] == "27"
    assert order["recommended_next_action"] == "WAIT"


def test_suspicious_sentinel_order_is_tracked(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        order_states=[
            _order_state(
                classification="SUSPICIOUS_ORDER_STATE",
                suspicious=True,
                suspicious_reasons=["sentinel_filled_quantity", "missing_remaining_quantity"],
            )
        ],
    )

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == CLOSE_ORDER_SUSPICIOUS
    assert payload["managed_orders"][0]["classification"] == CLOSE_ORDER_SUSPICIOUS
    assert payload["managed_orders"][0]["recommended_next_action"] == TARGETED_CANCEL_REPLACE_CANDIDATE


def test_diagnostic_sentinel_status_gap_does_not_block_working_close(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        order_states=[
            _order_state(
                classification="OPEN_CLOSE_ORDER_WORKING",
                suspicious=False,
                suspicious_reasons=[],
                condition_flags=[],
            )
            | {
                "diagnostic_status_gaps": ["missing_remaining_quantity", "sentinel_filled_quantity"],
                "ibkr_order_status_quantity_unreliable": True,
                "order": {
                    **_order_state()["order"],
                    "filled_quantity": "1.7976931348623157e+308",
                    "remaining_quantity": None,
                },
            }
        ],
    )

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == WORKING_CLOSE_ORDER
    assert payload["managed_orders"][0]["classification"] == WORKING_CLOSE_ORDER
    assert payload["managed_orders"][0]["recommended_next_action"] == "WAIT"


def test_duplicate_close_order_is_blocked(tmp_path: Path) -> None:
    first = _order_state(order_id="27", perm_id="1001", duplicate_key="DUM882026|770561201|SELL|1")
    second = _order_state(order_id="28", perm_id="1002", duplicate_key="DUM882026|770561201|SELL|1")
    _seed_base(
        tmp_path,
        order_states=[first, second],
        duplicate_groups=[{"duplicate_key": "DUM882026|770561201|SELL|1", "orders": [first, second]}],
    )

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == DUPLICATE_CLOSE_ORDER_BLOCKED
    assert {order["classification"] for order in payload["managed_orders"]} == {DUPLICATE_CLOSE_ORDER_BLOCKED}
    assert payload["managed_orders"][0]["recommended_next_action"] == "DO_NOT_REPLACE_DUPLICATE_RISK"


def test_broker_flat_with_working_close_is_review_required(tmp_path: Path) -> None:
    state = _order_state()
    _seed_base(tmp_path, order_states=[state], flat_with_close=[state])

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == BROKER_FLAT_WITH_WORKING_CLOSE
    assert payload["managed_orders"][0]["recommended_next_action"] == "REVIEW_REQUIRED"


def test_position_without_close_order_is_tracked(tmp_path: Path) -> None:
    _seed_base(tmp_path, positions_without_close=[_broker_position()])

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    assert payload["managed_orders"][0]["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    assert payload["managed_orders"][0]["action"] == "SELL"


def test_position_without_close_order_uses_pre_restart_resolved_identity(tmp_path: Path) -> None:
    broker = _broker_position()
    broker.pop("con_id")
    _seed_base(tmp_path, positions_without_close=[broker])
    _write_registry_open_managed_events(tmp_path)

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    row = payload["managed_orders"][0]
    assert payload["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    assert row["lifecycle_id"] == "life_mnq"
    assert row["strategy_id"] == "mnq_globex_active_participation_long"
    assert (
        payload["pre_restart_exposure_resolution"]["classification"]
        == "PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED"
    )


def test_terminal_closed_flat_suppresses_stale_position_without_close_row(tmp_path: Path) -> None:
    stale_position = _broker_position()
    _seed_base(tmp_path, positions_without_close=[stale_position])
    _write_terminal_closed_flat_events(tmp_path)
    _write_lifecycle_report(
        tmp_path,
        {
            "lifecycle_id": "life_mnq",
            "symbol": "MNQ",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "quantity": "1",
            "side": "LONG",
            "final_position_status": "OPEN_MANAGED",
        },
    )
    _write_json(
        tmp_path / "outputs" / "reports" / "track_b_paper_broker_reconciliation" / "latest_track_b_paper_broker_reconciliation.json",
        {
            "schema_version": "track_b_paper_broker_reconciliation_v1",
            "generated_at": NOW.isoformat(),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_open_order_count": 0,
            "track_b_broker_position_count": 0,
            "track_b_broker_positions": [],
            "track_b_broker_open_orders": [],
            "unresolved_submit_intent_ownership_count": 0,
            "live_money_eligible": False,
        },
    )

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == NO_MANAGED_ORDERS
    assert payload["managed_orders"] == []
    overlay = payload["terminal_registry_truth_overlay"]
    assert overlay["superseded_full_audit_only_count"] == 1
    assert overlay["superseded_full_audit_only"][0]["classification"] == "STALE_SUPERSEDED_LIFECYCLE_PROJECTION"


def test_managed_timed_hold_pending_is_not_review_required(tmp_path: Path) -> None:
    managed_position = {
        "classification": "OPEN_MANAGED_MATCHED",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "side": "LONG",
        "quantity": "1",
        "lifecycle_id": "current_managed_mnq",
        "lane_id": "mnq_1x_asia_london_participation__asia_london_long_v6",
        "strategy_id": "asia_london_long_v6",
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        "exit_due": False,
        "attention_required": False,
    }
    _seed_base(
        tmp_path,
        positions_without_close=[_broker_position()],
        managed_positions=[managed_position],
    )

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING
    row = payload["managed_orders"][0]
    assert row["classification"] == ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING
    assert row["recommended_next_action"] == "WAIT"
    assert row["managed_exit_profile_present"] is True
    assert row["exit_not_yet_eligible"] is True
    assert row["close_order_required_now"] is False


def test_exit_due_without_close_order_is_managed_order_blocker_only(tmp_path: Path) -> None:
    managed_position = {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "side": "LONG",
        "quantity": "1",
        "lifecycle_id": "current_managed_mnq",
        "lane_id": "mnq_globex_active_participation_long",
        "strategy_id": "mnq_globex_active_participation_long",
        "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        "exit_due": True,
        "attention_required": False,
    }
    _seed_base(
        tmp_path,
        positions_without_close=[_broker_position()],
        managed_positions=[managed_position],
    )

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    assert payload["summary"]["managed_order_count"] == 1
    row = payload["managed_orders"][0]
    assert row["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    assert row["close_order_required_now"] is True
    assert row["managed_active_hold"] is False


def test_exit_due_managed_position_creates_blocker_without_open_order_truth_row(tmp_path: Path) -> None:
    managed_position = {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "symbol": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "side": "SHORT",
        "quantity": "1",
        "aggregate_qty": "-1",
        "signed_broker_qty": "-1",
        "lifecycle_id": "current_managed_mes_short",
        "lane_id": "mes_us_active_participation_short",
        "strategy_id": "mes_us_active_participation_short",
        "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        "exit_due": True,
        "attention_required": False,
        "working_close_qty": "0",
        "broker_position": {
            "account_id": "DUM882026",
            "symbol": "MES",
            "local_symbol": "MESM6",
            "con_id": 770561194,
            "expiry": "20260618",
            "quantity": "-1",
        },
    }
    _seed_base(tmp_path, managed_positions=[managed_position])

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    row = payload["managed_orders"][0]
    assert row["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    assert row["lifecycle_id"] == "current_managed_mes_short"
    assert row["action"] == "BUY"
    assert row["close_order_required_now"] is True


def test_stale_due_managed_position_still_reports_position_without_close_order(tmp_path: Path) -> None:
    managed_position = {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "side": "SHORT",
        "quantity": "1",
        "aggregate_qty": "-1",
        "signed_broker_qty": "-1",
        "lifecycle_id": "current_managed_mnq_short",
        "trade_id": "trade_mnq_short",
        "lane_id": "mnq_us_active_participation_short",
        "strategy_id": "mnq_us_active_participation_short",
        "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        "exit_due": True,
        "exit_due_state": "EXIT_DUE",
        "freshness_state": "STALE_DEPENDENCY",
        "exit_due_evidence_stale": True,
        "apply_authority_degraded": True,
        "stale_dependency_sources": ["position_truth"],
        "attention_required": True,
        "working_close_qty": "0",
        "broker_position": {
            "account_id": "DUM882026",
            "symbol": "MNQ",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "expiry": "20260618",
            "quantity": "-1",
        },
    }
    _seed_base(tmp_path, managed_positions=[managed_position])
    managed_positions_path = (
        tmp_path / "outputs" / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
    )
    managed_positions = json.loads(managed_positions_path.read_text(encoding="utf-8"))
    managed_positions["generated_at"] = (NOW - timedelta(minutes=10)).isoformat()
    managed_positions["source_freshness"] = {
        "stale": True,
        "stale_sources": ["position_truth"],
        "ttl_seconds": 180,
    }
    managed_positions_path.write_text(json.dumps(managed_positions, indent=2, sort_keys=True), encoding="utf-8")

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["source_freshness"]["stale"] is True
    assert payload["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    row = payload["managed_orders"][0]
    assert row["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    assert row["close_order_required_now"] is True
    assert row["managed_active_hold"] is False
    assert row["managed_position_freshness_state"] == "STALE_DEPENDENCY"
    assert row["exit_due_evidence_stale"] is True
    assert row["apply_authority_degraded"] is True
    assert row["stale_dependency_sources"] == ["position_truth"]


def test_managed_order_uses_current_owner_when_managed_position_artifact_is_stale(
    tmp_path: Path,
    monkeypatch,
) -> None:
    broker_position = {
        "account_id": "DUM882026",
        "symbol": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "quantity": "-1",
    }
    stale_managed_position = {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "symbol": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "side": "SHORT",
        "quantity": "1",
        "aggregate_qty": "-1",
        "signed_broker_qty": "-1",
        "trade_id": "trade_stale_globex_owner",
        "lifecycle_id": "stale_globex_lifecycle",
        "lane_id": "mes_globex_active_participation_short",
        "strategy_id": "mes_globex_active_participation_short",
        "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        "exit_due": True,
        "attention_required": False,
        "working_close_qty": "0",
        "broker_position": broker_position,
    }
    fresh_lifecycle = {
        "trade_id": "trade_fresh_us_owner",
        "lifecycle_id": "fresh_us_lifecycle",
        "symbol": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "side": "SHORT",
        "quantity": "1",
        "lane_id": "mes_us_active_participation_short",
        "strategy_id": "mes_us_active_participation_short",
        "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
    }
    _seed_base(tmp_path, positions_without_close=[broker_position], managed_positions=[stale_managed_position])
    monkeypatch.setattr(
        managed_order_registry_module,
        "resolve_pre_restart_exposure_reconciliation",
        lambda **_kwargs: {
            "classification": "OWNED_MANAGED_EXIT_DUE",
            "owned_exposure_count": 1,
            "owned_exposures": [
                {
                    "classification": "OWNED_MANAGED_EXIT_DUE",
                    "broker_position": broker_position,
                    "canonical_broker_position": broker_position,
                    "lifecycle_position": fresh_lifecycle,
                    "trade_id": fresh_lifecycle["trade_id"],
                    "lifecycle_id": fresh_lifecycle["lifecycle_id"],
                    "exit_due": True,
                    "reason_codes": ["NEWEST_EXACT_BROKER_BACKED_LIFECYCLE_REPORT_SELECTED"],
                }
            ],
            "resolved_lifecycle_positions": [fresh_lifecycle],
        },
    )

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    row = payload["managed_orders"][0]
    assert row["trade_id"] == "trade_fresh_us_owner"
    assert row["lifecycle_id"] == "fresh_us_lifecycle"
    assert row["action"] == "BUY"
    assert row["close_order_required_now"] is True


def test_owner_repaired_matched_position_with_policy_remains_active_hold(
    tmp_path: Path,
    monkeypatch,
) -> None:
    broker_position = {
        "account_id": "DUM882026",
        "symbol": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "quantity": "1",
    }
    fresh_lifecycle = {
        "classification": "OPEN_MANAGED_MATCHED",
        "trade_id": "trade_fresh_us_owner",
        "lifecycle_id": "fresh_us_lifecycle",
        "symbol": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "side": "LONG",
        "quantity": "1",
        "aggregate_qty": "1",
        "lane_id": "mes_us_active_participation_long",
        "strategy_id": "mes_us_active_participation_long",
        "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        "exit_due": False,
        "attention_required": False,
    }
    _seed_base(tmp_path, positions_without_close=[broker_position], managed_positions=[])
    reconciliation_path = (
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation["lifecycle_open_position_count"] = 1
    reconciliation["track_b_lifecycle_positions"] = [fresh_lifecycle]
    reconciliation["track_b_broker_positions"] = [broker_position]
    _write_json(reconciliation_path, reconciliation)
    monkeypatch.setattr(
        managed_order_registry_module,
        "resolve_pre_restart_exposure_reconciliation",
        lambda **_kwargs: _owner_resolution_payload(
            broker_position=broker_position,
            lifecycle_position=fresh_lifecycle,
        ),
    )

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING
    row = payload["managed_orders"][0]
    assert row["classification"] == ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING
    assert row["trade_id"] == "trade_fresh_us_owner"
    assert row["managed_exit_profile_present"] is True
    assert row["close_order_required_now"] is False


def test_owner_repaired_matched_position_without_policy_still_requires_review(
    tmp_path: Path,
    monkeypatch,
) -> None:
    broker_position = {
        "account_id": "DUM882026",
        "symbol": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "quantity": "1",
    }
    repaired_lifecycle = {
        "classification": "OPEN_MANAGED_MATCHED",
        "trade_id": "trade_fresh_us_owner",
        "lifecycle_id": "fresh_us_lifecycle",
        "symbol": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "side": "LONG",
        "quantity": "1",
        "aggregate_qty": "1",
        "lane_id": "mes_us_active_participation_long",
        "strategy_id": "mes_us_active_participation_long",
        "exit_due": False,
        "attention_required": False,
    }
    _seed_base(tmp_path, positions_without_close=[broker_position], managed_positions=[])
    monkeypatch.setattr(
        managed_order_registry_module,
        "resolve_pre_restart_exposure_reconciliation",
        lambda **_kwargs: _owner_resolution_payload(
            broker_position=broker_position,
            lifecycle_position=repaired_lifecycle,
        ),
    )

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    row = payload["managed_orders"][0]
    assert payload["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    assert row["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    assert row["managed_exit_profile_present"] is False
    assert row["close_order_required_now"] is True


def test_owner_repaired_matched_position_with_current_review_still_blocks(
    tmp_path: Path,
    monkeypatch,
) -> None:
    broker_position = {
        "account_id": "DUM882026",
        "symbol": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "quantity": "1",
    }
    repaired_lifecycle = {
        "classification": "OPEN_MANAGED_MATCHED",
        "trade_id": "trade_fresh_us_owner",
        "lifecycle_id": "fresh_us_lifecycle",
        "symbol": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "side": "LONG",
        "quantity": "1",
        "aggregate_qty": "1",
        "lane_id": "mes_us_active_participation_long",
        "strategy_id": "mes_us_active_participation_long",
        "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        "exit_due": False,
        "attention_required": False,
    }
    _seed_base(tmp_path, positions_without_close=[broker_position], managed_positions=[])
    reconciliation_path = (
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation["current_scope_review_required_count"] = 1
    _write_json(reconciliation_path, reconciliation)
    monkeypatch.setattr(
        managed_order_registry_module,
        "resolve_pre_restart_exposure_reconciliation",
        lambda **_kwargs: _owner_resolution_payload(
            broker_position=broker_position,
            lifecycle_position=repaired_lifecycle,
        ),
    )

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    row = payload["managed_orders"][0]
    assert payload["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    assert row["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    assert row["managed_exit_profile_present"] is False
    assert row["close_order_required_now"] is True


def test_canonical_exit_due_position_dedupes_open_order_truth_missing_con_id(tmp_path: Path) -> None:
    managed_position = {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "symbol": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "side": "SHORT",
        "quantity": "1",
        "aggregate_qty": "-1",
        "signed_broker_qty": "-1",
        "lifecycle_id": "current_managed_mes_short",
        "lane_id": "mes_us_active_participation_short",
        "strategy_id": "mes_us_active_participation_short",
        "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        "exit_due": True,
        "attention_required": False,
        "working_close_qty": "0",
        "broker_position": {
            "account_id": "DUM882026",
            "symbol": "MES",
            "local_symbol": "MESM6",
            "con_id": 770561194,
            "expiry": "20260618",
            "quantity": "-1",
        },
    }
    broker_position_without_con_id = {
        "account_id": "DUM882026",
        "symbol": "MES",
        "track_b_root": "MES",
        "local_symbol": "MESM6",
        "quantity": "-1",
        "average_cost": "38023.13",
    }
    _seed_base(
        tmp_path,
        positions_without_close=[broker_position_without_con_id],
        managed_positions=[managed_position],
    )

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == POSITION_WITHOUT_CLOSE_ORDER
    assert payload["summary"]["managed_order_count"] == 1
    assert payload["summary"]["position_without_close_order_count"] == 1
    row = payload["managed_orders"][0]
    assert row["lifecycle_id"] == "current_managed_mes_short"
    assert row["local_symbol"] == "MESM6"
    assert row["con_id"] == 770561194
    assert row["action"] == "BUY"


def test_marketable_close_order_is_modify_in_place_candidate(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        order_states=[
            _order_state(
                classification="CLOSE_ORDER_MARKETABLE_NOT_FILLED",
                marketable=True,
                condition_flags=["marketable_unfilled_beyond_threshold"],
            )
        ],
    )

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == CLOSE_ORDER_MODIFIABLE
    assert payload["managed_orders"][0]["recommended_next_action"] == MODIFY_IN_PLACE_CANDIDATE


def test_non_marketable_close_order_requires_operator_review(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        order_states=[
            _order_state(
                classification="OPEN_CLOSE_ORDER_WORKING",
                marketable=False,
            )
            | {
                "market_reference": {
                    "reference_price": "29560",
                    "reference_age_seconds": 1.0,
                    "pricing_source": "DATABENTO_RUNTIME_1M",
                }
            }
        ],
    )

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == CLOSE_ORDER_NOT_MARKETABLE
    assert payload["managed_orders"][0]["recommended_next_action"] == TARGETED_CANCEL_REPLACE_CANDIDATE


def test_stale_close_order_is_cancel_replace_candidate(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        order_states=[
            _order_state(
                classification="CLOSE_ORDER_STALE",
                condition_flags=["close_order_stale"],
            )
        ],
    )

    payload = build_track_b_managed_order_registry(
        config=TrackBManagedOrderRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == "CLOSE_ORDER_CANCEL_REPLACE_REQUIRED"
    assert payload["managed_orders"][0]["recommended_next_action"] == TARGETED_CANCEL_REPLACE_CANDIDATE


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    config = TrackBManagedOrderRegistryConfig(repo_root=tmp_path)
    payload = build_track_b_managed_order_registry(config=config, now=NOW)

    authority_path, events = write_track_b_managed_order_registry(config=config, payload=payload, now=NOW)
    projection_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]

    assert authority_path == tmp_path / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json"
    assert config.resolve(config.event_log_path) == (
        tmp_path / "outputs" / "track_b_execution_core" / "managed_orders" / "managed_order_events.jsonl"
    )
    assert authority_path.exists()
    assert projection_path.exists()
    assert events
    projection = json.loads(projection_path.read_text(encoding="utf-8"))
    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)
    assert projection["authority_owner"] == "execution_core"


def test_written_managed_order_registry_preserves_existing_consumer_fields_with_dmc_metadata(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    config = TrackBManagedOrderRegistryConfig(repo_root=tmp_path)
    payload = build_track_b_managed_order_registry(config=config, now=NOW)

    authority_path, _ = write_track_b_managed_order_registry(config=config, payload=payload, now=NOW)

    written = json.loads(authority_path.read_text(encoding="utf-8"))
    assert written["schema_version"] == "track_b_managed_order_registry_v1"
    assert written["classification"] == NO_MANAGED_ORDERS
    assert written["summary"]["managed_order_count"] == 0
    assert written["managed_orders"] == []
    assert written["dmc_metadata"]["artifact_family"] == "latest_managed_orders"
    assert written["dmc_metadata"]["publisher_id"] == "track_b_managed_order_registry.py"


def test_critical_paths_do_not_consume_dashboard_projection_as_authority() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    forbidden = "outputs/operator_dashboard/runtime/latest_track_b_managed_orders.json"
    critical_paths = [
        repo_root / "src/mgc_v05l/app/probationary_runtime.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_runtime_environment_truth.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_position_truth_monitor.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_paper_broker_reconciliation.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_broker_truth_lease.py",
    ]

    offenders = [str(path) for path in critical_paths if forbidden in path.read_text(encoding="utf-8")]

    assert offenders == []


def _seed_base(
    root: Path,
    *,
    order_states: list[dict] | None = None,
    duplicate_groups: list[dict] | None = None,
    flat_with_close: list[dict] | None = None,
    positions_without_close: list[dict] | None = None,
    managed_positions: list[dict] | None = None,
) -> None:
    order_states = order_states or []
    positions_without_close = positions_without_close or []
    managed_positions = managed_positions or []
    _write_json(
        root / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json",
        {
            "schema_version": "track_b_open_order_truth_v1",
            "generated_at": NOW.isoformat(),
            "classification": "NO_OPEN_ORDERS" if not order_states and not positions_without_close else "OPEN_CLOSE_ORDER_WORKING",
            "order_states": order_states,
            "duplicate_close_order_groups": duplicate_groups or [],
            "broker_flat_with_open_close_order": flat_with_close or [],
            "broker_positions_without_close_order": positions_without_close,
            "summary": {
                "open_order_count": len(order_states),
                "classification": "NO_OPEN_ORDERS",
            },
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {"schema_version": "track_b_position_truth_v1", "generated_at": NOW.isoformat(), "summary": {"overall_classification": "CLEAN_FLAT_READY"}},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json",
        {
            "schema_version": "track_b_managed_position_registry_v1",
            "generated_at": NOW.isoformat(),
            "classification": "NO_MANAGED_POSITIONS" if not managed_positions else managed_positions[0]["classification"],
            "managed_positions": managed_positions,
        },
    )
    _write_json(
        root / "outputs" / "reports" / "track_b_paper_broker_reconciliation" / "latest_track_b_paper_broker_reconciliation.json",
        {
            "schema_version": "track_b_paper_broker_reconciliation_v1",
            "generated_at": NOW.isoformat(),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_open_order_count": len(order_states),
            "track_b_broker_position_count": len(positions_without_close),
            "unresolved_submit_intent_ownership_count": 0,
            "live_money_eligible": False,
        },
    )


def _order_state(
    *,
    order_id: str = "27",
    perm_id: str = "347068546",
    classification: str = "OPEN_CLOSE_ORDER_WORKING",
    suspicious: bool = False,
    suspicious_reasons: list[str] | None = None,
    marketable: bool = False,
    condition_flags: list[str] | None = None,
    duplicate_key: str = "DUM882026|770561201|SELL|1",
) -> dict:
    return {
        "classification": classification,
        "broker_order_id": order_id,
        "perm_id": perm_id,
        "client_id": 17089,
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "action": "SELL",
        "quantity": "1",
        "status": "Submitted",
        "limit_price": "29555.50",
        "marketable": marketable,
        "working": True,
        "is_close_order": True,
        "is_entry_order": False,
        "suspicious": suspicious,
        "suspicious_reasons": suspicious_reasons or [],
        "condition_flags": condition_flags or [],
        "duplicate_key": duplicate_key,
        "order": {
            "account_id": "DUM882026",
            "symbol": "MNQ",
            "track_b_root": "MNQ",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "broker_order_id": order_id,
            "client_id": 17089,
            "perm_id": perm_id,
            "action": "SELL",
            "quantity": "1",
            "filled_quantity": "0",
            "remaining_quantity": "1",
            "order_type": "LMT",
            "limit_price": "29555.50",
            "time_in_force": "DAY",
            "status": "Submitted",
        },
    }


def _broker_position() -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "track_b_root": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
        "average_cost": "59138.12",
    }


def _owner_resolution_payload(*, broker_position: dict, lifecycle_position: dict) -> dict:
    return {
        "classification": "OWNED_MANAGED_EXPOSURE",
        "owned_exposure_count": 1,
        "review_required_exposure_count": 0,
        "owned_exposures": [
            {
                "classification": "OWNED_MANAGED_EXPOSURE",
                "broker_position": broker_position,
                "canonical_broker_position": broker_position,
                "lifecycle_position": lifecycle_position,
                "trade_id": lifecycle_position["trade_id"],
                "lifecycle_id": lifecycle_position["lifecycle_id"],
                "exit_due": lifecycle_position.get("exit_due") is True,
                "reason_codes": ["NEWEST_EXACT_BROKER_BACKED_LIFECYCLE_REPORT_SELECTED"],
            }
        ],
        "review_required_exposures": [],
        "resolved_lifecycle_positions": [lifecycle_position],
    }


def _write_registry_open_managed_events(root: Path) -> None:
    path = root / "outputs" / "track_b_execution_core" / "trade_registry" / "live_trade_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    base = {
        "trade_id": "trade_mnq",
        "lifecycle_id": "life_mnq",
        "lane_id": "mnq_globex_active_participation_long",
        "thesis_strategy_id": "mnq_globex_active_participation_long",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "con_id": 770561201,
        "local_symbol": "MNQM6",
        "expiry": "202606",
        "side": "LONG",
        "action": "BUY",
        "qty": Decimal("1"),
        "source_artifact_path": "outputs/track_b_execution_core/test.json",
    }
    events = [
        TradeEvent(
            event_id="trade_mnq_fill",
            event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
            generated_at=NOW,
            order_id="1",
            client_id="111",
            perm_id="perm_mnq",
            exec_id="exec_mnq",
            price=Decimal("29569.06"),
            **base,
        ),
        TradeEvent(
            event_id="trade_mnq_open",
            event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
            generated_at=NOW,
            metadata={"managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"},
            **base,
        ),
    ]
    path.write_text("\n".join(json.dumps(event.to_dict(), sort_keys=True) for event in events) + "\n", encoding="utf-8")


def _write_terminal_closed_flat_events(root: Path) -> None:
    path = root / "outputs" / "track_b_execution_core" / "trade_registry" / "live_trade_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    base = {
        "trade_id": "trade_mnq",
        "lifecycle_id": "life_mnq",
        "lane_id": "mnq_globex_active_participation_long",
        "thesis_strategy_id": "mnq_globex_active_participation_long",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "con_id": 770561201,
        "local_symbol": "MNQM6",
        "expiry": "20260618",
        "side": "LONG",
        "qty": Decimal("1"),
        "source_artifact_path": "outputs/track_b_execution_core/test.json",
    }
    events = [
        TradeEvent(
            event_id="trade_mnq_entry_fill",
            event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
            generated_at=NOW,
            action="BUY",
            order_id="1",
            client_id="111",
            perm_id="perm_mnq_entry",
            exec_id="exec_mnq_entry",
            price=Decimal("29569.06"),
            **base,
        ),
        TradeEvent(
            event_id="trade_mnq_open",
            event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
            generated_at=NOW + timedelta(seconds=1),
            action="BUY",
            metadata={"managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"},
            **base,
        ),
        TradeEvent(
            event_id="trade_mnq_exit_fill",
            event_type=TradeEventType.EXIT_FILL_BROKER_BACKED,
            generated_at=NOW + timedelta(seconds=2),
            action="SELL",
            order_id="2",
            client_id="111",
            perm_id="perm_mnq_exit",
            exec_id="exec_mnq_exit",
            price=Decimal("29571.25"),
            **base,
        ),
        TradeEvent(
            event_id="trade_mnq_reconciled_flat",
            event_type=TradeEventType.RECONCILED_FLAT,
            generated_at=NOW + timedelta(seconds=3),
            action="SELL",
            **base,
        ),
    ]
    path.write_text("\n".join(json.dumps(event.to_dict(), sort_keys=True) for event in events) + "\n", encoding="utf-8")


def _write_lifecycle_report(root: Path, payload: dict) -> None:
    lifecycle_id = str(payload.get("lifecycle_id") or "life_mnq")
    report = {
        "generated_at": NOW.isoformat(),
        "lifecycle_id": lifecycle_id,
        "final_position_status": "OPEN_MANAGED",
        **payload,
    }
    _write_json(
        root
        / "outputs"
        / "track_b_execution_core"
        / "track_b_strategy_managed_paper_lifecycle"
        / lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json",
        report,
    )
    _write_json(
        root
        / "outputs"
        / "track_b_execution_core"
        / "track_b_strategy_managed_paper_lifecycle"
        / "latest_track_b_strategy_managed_paper_lifecycle_report.json",
        report,
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
