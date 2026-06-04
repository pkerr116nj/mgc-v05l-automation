from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEvent, TradeEventType
import mgc_v05l.execution_core.track_b_managed_position_registry as managed_position_registry_module
from mgc_v05l.execution_core.track_b_managed_position_registry import (
    BROKER_BACKED_ADOPTION_REQUIRED,
    LIFECYCLE_WITHOUT_BROKER,
    MANAGED_POSITION_METADATA_INCOMPLETE,
    NO_MANAGED_POSITIONS,
    OPEN_MANAGED_CLOSE_WORKING,
    OPEN_MANAGED_EXIT_DUE,
    OPEN_MANAGED_MATCHED,
    PROJECTION_AUTHORITY_COHERENT,
    REVIEW_REQUIRED,
    TrackBManagedPositionRegistryConfig,
    build_track_b_managed_position_registry,
    write_track_b_managed_position_registry,
)


NOW = datetime(2026, 5, 22, 16, 35, tzinfo=UTC)


def test_no_positions_reports_no_managed_positions(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == NO_MANAGED_POSITIONS
    assert payload["summary"]["managed_position_count"] == 0
    assert payload["read_only"] is True
    assert payload["submit_authority"] is False
    assert payload["paper_proof_invoked"] is False
    assert payload["live_money_eligible"] is False


def test_historical_review_required_lifecycle_ignored_when_active_truth_clean_flat(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write_position_truth_clean_flat(tmp_path)
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id="old_review_required_lifecycle",
        review_required=True,
        paper_lifecycle_classification="TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == NO_MANAGED_POSITIONS
    assert payload["managed_positions"] == []
    assert payload["review_required_positions"] == []
    assert payload["historical_review_positions"][0]["current_hot_path_scope"] == "HISTORICAL_UNRESOLVED_FULL_AUDIT_ONLY"


def test_historical_review_required_lifecycle_with_cleanup_evidence_stays_full_audit_only(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write_position_truth_clean_flat(tmp_path)
    _write_json(
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "generated_at": NOW.isoformat(),
            "broker_reconciled": True,
            "track_b_broker_positions": [],
            "track_b_lifecycle_positions": [],
            "track_b_broker_open_orders": [],
            "review_required_positions": [],
            "unresolved_submit_intent_ownership_records": [],
            "review_required_count": 1,
            "registry_reconciliation": {
                "classification": "REGISTRY_RECONCILIATION_MATCHED",
                "blocking": False,
                "mapped_trade_ids": ["old_trade"],
                "broker_position_count": 0,
                "lifecycle_position_count": 0,
                "broker_open_order_count": 0,
            },
        },
    )
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id="old_review_required_lifecycle",
        review_required=True,
        paper_lifecycle_classification="TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == NO_MANAGED_POSITIONS
    assert payload["managed_positions"] == []
    assert payload["review_required_positions"] == []
    assert payload["historical_review_positions"][0]["current_hot_path_scope"] == "HISTORICAL_UNRESOLVED_FULL_AUDIT_ONLY"


def test_active_review_required_lifecycle_still_surfaces(tmp_path: Path) -> None:
    review = _lifecycle_position()
    _seed_base(tmp_path, review_positions=[review])
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id=review["lifecycle_id"],
        review_required=True,
        paper_lifecycle_classification="TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == REVIEW_REQUIRED
    assert payload["managed_positions"][0]["classification"] == REVIEW_REQUIRED
    assert payload["managed_positions"][0]["review_required_position"]["current_hot_path_scope"] == "CURRENT_SCOPE"


def test_open_order_linked_review_lifecycle_still_surfaces_current_scope(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        order_states=[
            {
                "is_close_order": True,
                "order": {"local_symbol": "MNQM6", "con_id": 770561201, "symbol": "MNQ"},
            }
        ],
    )
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id="old_review_required_lifecycle",
        review_required=True,
        paper_lifecycle_classification="TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == REVIEW_REQUIRED
    assert payload["managed_positions"][0]["review_required_position"]["current_hot_path_scope"] == "CURRENT_SCOPE"


def test_retryable_unmutated_aggregate_close_review_does_not_mask_exit_due(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(bars_since_fill=3)
    _seed_base(tmp_path, broker_positions=[_broker_position()], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id=lifecycle["lifecycle_id"],
        bars_since_fill=3,
        review_required=True,
        paper_lifecycle_classification="TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
        broker_state_mutated=False,
        primary_blocker="Managed PAPER lifecycle close maintenance error: quantity must be exactly 1 for milestone one.",
        close_intent={
            "lifecycle_id": lifecycle["lifecycle_id"],
            "strategy_id": "track_b_paper_execution_test_mule_v1__mnq",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "order_action": "BUY",
            "quantity": 3,
            "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        },
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_EXIT_DUE
    assert payload["review_required_positions"] == []
    assert payload["managed_positions"][0]["attention_required"] is False


def test_retryable_pre_submit_contract_review_does_not_mask_exit_due(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(bars_since_fill=3)
    _seed_base(tmp_path, broker_positions=[_broker_position()], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id=lifecycle["lifecycle_id"],
        bars_since_fill=3,
        review_required=True,
        paper_lifecycle_classification="TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
        broker_state_mutated=False,
        primary_blocker=(
            "Managed PAPER adapter submit stage failed: CONTRACT_EXPIRY_MISMATCH_PRE_SUBMIT: "
            "configured shorthand contract month 202606 has no canonical IBKR expiry"
        ),
        close_intent={
            "lifecycle_id": lifecycle["lifecycle_id"],
            "strategy_id": "track_b_paper_execution_test_mule_v1__mnq",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "order_action": "BUY",
            "quantity": 1,
            "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        },
        close_submit_attempt={
            "submitted": False,
            "broker_state_mutated": False,
            "broker_order_id": None,
            "submit_diagnostics": {
                "pre_submit_blocked": True,
                "place_order_called": False,
            },
        },
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_EXIT_DUE
    assert payload["review_required_positions"] == []
    assert payload["managed_positions"][0]["attention_required"] is False


def test_no_broker_effect_terminal_lifecycle_is_not_registry_eligible(tmp_path: Path) -> None:
    lifecycle = {
        **_lifecycle_position(),
        "final_position_status": "BLOCKED_NO_BROKER_EFFECT",
        "lifecycle_status": "BLOCKED_NO_BROKER_EFFECT",
        "paper_lifecycle_classification": "BLOCKED_NO_BROKER_EFFECT",
    }
    _seed_base(tmp_path, lifecycle_positions=[lifecycle])

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == NO_MANAGED_POSITIONS
    assert payload["managed_positions"] == []


def test_valid_lifecycle_and_broker_match_reports_open_managed_matched(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(bars_since_fill=1)
    _seed_base(
        tmp_path,
        broker_positions=[_broker_position()],
        lifecycle_positions=[lifecycle],
    )
    _write_lifecycle_report(tmp_path, lifecycle_id=lifecycle["lifecycle_id"], bars_since_fill=1)

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_MATCHED
    assert payload["managed_positions"][0]["classification"] == OPEN_MANAGED_MATCHED
    assert payload["managed_positions"][0]["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert payload["managed_positions"][0]["attention_required"] is False


def test_current_owner_overlay_prevents_stale_duplicate_lifecycle_aggregate(tmp_path: Path) -> None:
    broker = {
        **_broker_position(),
        "symbol": "MES",
        "track_b_root": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "quantity": "-1.0",
    }
    aggregate_lifecycle = {
        **_lifecycle_position(lifecycle_id="old_lifecycle"),
        "account_id": "MULTIPLE",
        "instrument_family": "MES",
        "track_b_root": "MES",
        "contract_key": "MES-202606",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "quantity": "2",
        "aggregate_qty": "-2",
        "lifecycle_id": "old_lifecycle",
        "lifecycle_ids": ["old_lifecycle", "current_lifecycle"],
        "trade_ids": ["old_trade", "current_trade"],
        "lifecycle_unit_count": 2,
        "lifecycle_units": [
            {"trade_id": "old_trade", "lifecycle_id": "old_lifecycle", "signed_qty": "-1"},
            {"trade_id": "current_trade", "lifecycle_id": "current_lifecycle", "signed_qty": "-1"},
        ],
    }
    _seed_base(tmp_path, broker_positions=[broker], lifecycle_positions=[aggregate_lifecycle])
    _write_registry_open_managed_events_for(
        tmp_path,
        trade_id="old_trade",
        lifecycle_id="old_lifecycle",
        generated_at=NOW - timedelta(days=1),
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        order_id="2",
        perm_id="old_perm",
        exec_id="old_exec",
    )
    _write_registry_open_managed_events_for(
        tmp_path,
        trade_id="current_trade",
        lifecycle_id="current_lifecycle",
        generated_at=NOW,
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        order_id="1",
        perm_id="current_perm",
        exec_id="current_exec",
        append=True,
    )
    _write_lifecycle_report(tmp_path, lifecycle_id="current_lifecycle")

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_MATCHED
    assert payload["summary"]["managed_position_count"] == 1
    position = payload["managed_positions"][0]
    assert position["local_symbol"] == "MESM6"
    assert position["trade_id"] == "current_trade"
    assert position["lifecycle_id"] == "current_lifecycle"
    assert position["aggregate_qty"] == "-1"
    assert position["broker_qty_match"] is True
    assert position["duplicate_same_lane_exposure"] is False
    assert payload["superseded_lifecycle_projections"][0]["classification"] == (
        "STALE_DUPLICATE_LIFECYCLE_AGGREGATION_FULL_AUDIT_ONLY"
    )


def test_historical_review_required_same_contract_does_not_pollute_active_matched_position(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(lifecycle_id="current_managed_mnq", bars_since_fill=1)
    _seed_base(tmp_path, broker_positions=[_broker_position()], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(tmp_path, lifecycle_id=lifecycle["lifecycle_id"], bars_since_fill=1)
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id="old_review_required_mnq",
        review_required=True,
        paper_lifecycle_classification="TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_MATCHED
    assert payload["review_required_positions"] == []
    assert payload["managed_positions"][0]["lifecycle_id"] == "current_managed_mnq"


def test_exit_due_from_policy_and_completed_bars(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(bars_since_fill=3)
    _seed_base(tmp_path, broker_positions=[_broker_position()], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(tmp_path, lifecycle_id=lifecycle["lifecycle_id"], bars_since_fill=3)

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_EXIT_DUE
    assert payload["managed_positions"][0]["exit_due"] is True
    assert payload["managed_positions"][0]["recommended_operator_action"].startswith("Observe runtime-managed exit")


def test_globex_active_exit_policy_ignores_stale_three_bar_report_threshold(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1", bars_since_fill=3)
    _seed_base(tmp_path, broker_positions=[_broker_position()], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id=lifecycle["lifecycle_id"],
        policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        bars_since_fill=3,
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_MATCHED
    assert payload["managed_positions"][0]["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    assert payload["managed_positions"][0]["bars_since_entry"] == 3
    assert payload["managed_positions"][0]["exit_due"] is False


def test_globex_active_exit_policy_due_after_twelve_completed_5m_bars(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1", bars_since_fill=12)
    _seed_base(tmp_path, broker_positions=[_broker_position()], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id=lifecycle["lifecycle_id"],
        policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        bars_since_fill=12,
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_EXIT_DUE
    assert payload["managed_positions"][0]["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    assert payload["managed_positions"][0]["bars_since_entry"] == 12
    assert payload["managed_positions"][0]["exit_due"] is True


def test_exit_due_uses_current_phase1_completed_5m_bars_over_stale_lifecycle_counter(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(bars_since_fill=1)
    _seed_base(tmp_path, broker_positions=[_broker_position()], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(tmp_path, lifecycle_id=lifecycle["lifecycle_id"], bars_since_fill=1)
    _write_phase1_5m_bars(
        tmp_path,
        symbol="MNQ",
        bar_ends=[
            "2026-05-22T16:25:00+00:00",
            "2026-05-22T16:30:00+00:00",
            "2026-05-22T16:35:00+00:00",
        ],
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_EXIT_DUE
    assert payload["managed_positions"][0]["bars_since_entry"] == 3
    assert payload["managed_positions"][0]["exit_due"] is True


def test_close_working_comes_from_open_order_truth(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(bars_since_fill=2)
    _seed_base(
        tmp_path,
        broker_positions=[_broker_position()],
        lifecycle_positions=[lifecycle],
        order_states=[
            {
                "classification": "OPEN_CLOSE_ORDER_WORKING",
                "is_close_order": True,
                "order": {"local_symbol": "MNQM6", "action": "BUY", "quantity": "1"},
                "broker_order_id": "30",
            }
        ],
    )
    _write_lifecycle_report(tmp_path, lifecycle_id=lifecycle["lifecycle_id"], bars_since_fill=2)

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_CLOSE_WORKING
    assert payload["managed_positions"][0]["close_order_state"]["broker_order_id"] == "30"


def test_close_working_comes_from_managed_order_registry(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(bars_since_fill=2)
    _seed_base(
        tmp_path,
        broker_positions=[_broker_position()],
        lifecycle_positions=[lifecycle],
        managed_order_states=[
            {
                "classification": "WORKING_CLOSE_ORDER",
                "is_close_order": True,
                "local_symbol": "MNQM6",
                "action": "BUY",
                "quantity": "1",
                "broker_order_id": "31",
                "recommended_next_action": "WAIT",
            }
        ],
    )
    _write_lifecycle_report(tmp_path, lifecycle_id=lifecycle["lifecycle_id"], bars_since_fill=2)

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_CLOSE_WORKING
    assert payload["managed_order_registry"]["classification"] == "WORKING_CLOSE_ORDER"
    assert payload["managed_positions"][0]["managed_order_state"]["broker_order_id"] == "31"


def test_broker_backed_position_without_lifecycle_requires_adoption(tmp_path: Path) -> None:
    _seed_base(tmp_path, broker_positions=[_broker_position()])

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == BROKER_BACKED_ADOPTION_REQUIRED
    assert payload["managed_positions"][0]["recommended_operator_action"].startswith("Run scoped broker-backed adoption")


def test_registry_backed_broker_position_repairs_stale_lifecycle_projection(tmp_path: Path) -> None:
    broker = _broker_position()
    broker.pop("con_id")
    _seed_base(tmp_path, broker_positions=[broker])
    _write_registry_open_managed_events(tmp_path)
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id="bridge_fill_mnq_short",
        policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        bars_since_fill=12,
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_EXIT_DUE
    assert payload["managed_positions"][0]["trade_id"] == "trade_mnq"
    assert payload["managed_positions"][0]["lifecycle_id"] == "bridge_fill_mnq_short"
    assert payload["managed_positions"][0]["attention_required"] is False
    assert (
        payload["pre_restart_exposure_resolution"]["classification"]
        == "PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED"
    )


def test_owner_resolution_overlay_repairs_missing_managed_position_projection(
    tmp_path: Path,
    monkeypatch,
) -> None:
    broker = _broker_position()
    lifecycle = _lifecycle_position(policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1", bars_since_fill=12)
    _seed_base(tmp_path, broker_positions=[broker])
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id=lifecycle["lifecycle_id"],
        policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        bars_since_fill=12,
    )

    def fake_resolver(**_kwargs):
        return {
            "classification": "OWNED_MANAGED_EXIT_DUE",
            "broker_position_count": 1,
            "broker_open_order_count": 0,
            "owned_exposure_count": 1,
            "review_required_exposure_count": 0,
            "owned_exposures": [
                {
                    "classification": "OWNED_MANAGED_EXIT_DUE",
                    "broker_position": broker,
                    "canonical_broker_position": broker,
                    "lifecycle_position": lifecycle,
                    "trade_id": "trade_mnq",
                    "lifecycle_id": lifecycle["lifecycle_id"],
                    "position_key": "DUM882026|MNQM6|770561201",
                    "exit_due": True,
                }
            ],
            "review_required_exposures": [],
            "resolved_lifecycle_positions": [lifecycle],
            "stale_superseded_full_audit_only": [],
            "read_only": True,
            "no_broad_flatten_generated": True,
        }

    monkeypatch.setattr(managed_position_registry_module, "resolve_pre_restart_exposure_reconciliation", fake_resolver)
    monkeypatch.setattr(
        managed_position_registry_module,
        "_merge_resolved_lifecycle_positions",
        lambda *, lifecycle_positions, resolved_lifecycle_positions: list(lifecycle_positions),
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_EXIT_DUE
    assert payload["summary"]["managed_position_count"] == 1
    position = payload["managed_positions"][0]
    assert position["trade_id"] == "trade_mnq"
    assert position["lifecycle_id"] == lifecycle["lifecycle_id"]
    assert position["classification"] == OPEN_MANAGED_EXIT_DUE
    assert position["projection_authority_owner_confirmed"] is True
    diagnostics = payload["projection_authority_diagnostics"]
    assert diagnostics["classification"] == PROJECTION_AUTHORITY_COHERENT
    assert diagnostics["repaired_missing_owner_count"] == 1
    assert diagnostics["repairs"][0]["classification"] == "CURRENT_OWNER_PROJECTION_REPAIRED"


def test_reconciled_owned_exposure_cannot_publish_no_managed_positions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    broker = _broker_position()
    lifecycle = _lifecycle_position(policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1", bars_since_fill=12)
    _seed_base(tmp_path, broker_positions=[broker], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id=lifecycle["lifecycle_id"],
        policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        bars_since_fill=12,
    )
    reconciliation_path = (
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation["classification"] = "TRACK_B_PAPER_BROKER_RECONCILED"
    reconciliation["broker_reconciled"] = True
    reconciliation["current_exposure_owner_resolution"] = {
        "classification": "OWNED_MANAGED_EXIT_DUE",
        "broker_position_count": 1,
        "broker_open_order_count": 0,
        "owned_exposure_count": 1,
        "owned_exposures": [
            {
                "classification": "OWNED_MANAGED_EXIT_DUE",
                "broker_position": broker,
                "canonical_broker_position": broker,
                "lifecycle_position": lifecycle,
                "trade_id": "trade_mnq",
                "lifecycle_id": lifecycle["lifecycle_id"],
                "position_key": "DUM882026|MNQM6|770561201",
                "exit_due": True,
            }
        ],
        "review_required_exposure_count": 0,
        "review_required_exposures": [],
        "resolved_lifecycle_positions": [lifecycle],
        "read_only": True,
        "no_broad_flatten_generated": True,
    }
    reconciliation_path.write_text(json.dumps(reconciliation), encoding="utf-8")

    monkeypatch.setattr(managed_position_registry_module, "_managed_positions", lambda **_kwargs: [])

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == "PROJECTION_AUTHORITY_DIVERGENCE"
    assert payload["classification"] != NO_MANAGED_POSITIONS
    assert payload["summary"]["broker_position_count"] == 1
    assert payload["projection_authority_diagnostics"]["classification"] == "PROJECTION_AUTHORITY_DIVERGENCE"
    assert payload["projection_authority_diagnostics"]["divergences"][0]["owner_trade_id"] == "trade_mnq"
    assert payload["managed_positions"][0]["classification"] == "PROJECTION_AUTHORITY_DIVERGENCE"


def test_reconciliation_match_report_owner_cannot_disappear_from_managed_positions(tmp_path: Path) -> None:
    broker = _broker_position()
    lifecycle = _lifecycle_position(policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1", bars_since_fill=12)
    lifecycle["trade_id"] = "trade_mnq_match_report_owner"
    _seed_base(tmp_path)
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id=lifecycle["lifecycle_id"],
        policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        bars_since_fill=12,
    )
    reconciliation_path = (
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation.update(
        {
            "classification": "BROKER_LIFECYCLE_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_positions": None,
            "track_b_lifecycle_positions": None,
            "position_match_report": {
                "state": "BROKER_AND_LIFECYCLE_OPEN_MATCHED",
                "matched": True,
                "matches": [{"broker_position": broker, "lifecycle_position": lifecycle}],
            },
        }
    )
    reconciliation_path.write_text(json.dumps(reconciliation), encoding="utf-8")

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == "OPEN_MANAGED_EXIT_DUE"
    assert payload["managed_positions"][0]["trade_id"] == lifecycle["trade_id"]
    assert payload["managed_positions"][0]["lifecycle_id"] == lifecycle["lifecycle_id"]


def test_lifecycle_missing_policy_is_metadata_incomplete(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(policy="")
    _seed_base(tmp_path, broker_positions=[_broker_position()], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(tmp_path, lifecycle_id=lifecycle["lifecycle_id"], policy="")

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == MANAGED_POSITION_METADATA_INCOMPLETE
    assert payload["managed_positions"][0]["attention_required"] is True


def test_lifecycle_without_broker_is_classified(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position()
    _seed_base(tmp_path, lifecycle_positions=[lifecycle])
    _write_lifecycle_report(tmp_path, lifecycle_id=lifecycle["lifecycle_id"])

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == LIFECYCLE_WITHOUT_BROKER
    assert payload["managed_positions"][0]["attention_required"] is True


def test_terminal_registry_truth_suppresses_stale_lifecycle_open_projection(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(lifecycle_id="life_terminal_superseded")
    lifecycle["trade_id"] = "trade_terminal_superseded"
    _seed_base(tmp_path, lifecycle_positions=[lifecycle])
    _write_lifecycle_report(tmp_path, lifecycle_id="life_terminal_superseded")
    _write_registry_closed_flat_events(
        tmp_path,
        trade_id="trade_terminal_superseded",
        lifecycle_id="life_terminal_superseded",
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == NO_MANAGED_POSITIONS
    assert payload["managed_positions"] == []
    assert payload["superseded_lifecycle_projections"][0]["classification"] == "STALE_SUPERSEDED_LIFECYCLE_PROJECTION"


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    config = TrackBManagedPositionRegistryConfig(repo_root=tmp_path)
    payload = build_track_b_managed_position_registry(config=config, now=NOW)

    authority_path, events = write_track_b_managed_position_registry(config=config, payload=payload, now=NOW)
    projection_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]

    assert authority_path == (
        tmp_path / "outputs" / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
    )
    assert config.resolve(config.event_log_path) == (
        tmp_path / "outputs" / "track_b_execution_core" / "managed_positions" / "managed_position_events.jsonl"
    )
    assert authority_path.exists()
    assert projection_path.exists()
    assert events
    projection = json.loads(projection_path.read_text(encoding="utf-8"))
    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)


def test_critical_paths_do_not_consume_dashboard_projection_as_authority() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    forbidden = "outputs/operator_dashboard/runtime/latest_track_b_managed_positions.json"
    forbidden_managed_orders = "outputs/operator_dashboard/runtime/latest_track_b_managed_orders.json"
    critical_paths = [
        repo_root / "src/mgc_v05l/app/probationary_runtime.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_runtime_environment_truth.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_position_truth_monitor.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_paper_broker_reconciliation.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_broker_truth_lease.py",
    ]

    offenders = [
        str(path)
        for path in critical_paths
        if forbidden in path.read_text(encoding="utf-8")
        or forbidden_managed_orders in path.read_text(encoding="utf-8")
    ]

    assert offenders == []


def _seed_base(
    root: Path,
    *,
    broker_positions: list[dict] | None = None,
    lifecycle_positions: list[dict] | None = None,
    review_positions: list[dict] | None = None,
    order_states: list[dict] | None = None,
    managed_order_states: list[dict] | None = None,
) -> None:
    broker_positions = broker_positions or []
    lifecycle_positions = lifecycle_positions or []
    review_positions = review_positions or []
    order_states = order_states or []
    managed_order_states = managed_order_states or []
    _write_json(
        root / "outputs" / "reports" / "track_b_paper_broker_reconciliation" / "latest_track_b_paper_broker_reconciliation.json",
        {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED"
            if not broker_positions and not lifecycle_positions and not review_positions
            else "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            "generated_at": NOW.isoformat(),
            "broker_reconciled": not broker_positions and not lifecycle_positions and not review_positions,
            "track_b_broker_positions": broker_positions,
            "track_b_lifecycle_positions": lifecycle_positions,
            "review_required_positions": review_positions,
            "unresolved_submit_intent_ownership_records": [],
            "track_b_broker_open_orders": [],
            "known_managed_exit_orders": [],
            "review_required_count": len(review_positions),
            "unresolved_submit_intent_ownership_count": 0,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {
            "schema_version": "track_b_position_truth_v1",
            "generated_at": NOW.isoformat(),
            "summary": {"overall_classification": "SEE_MANAGED_REGISTRY"},
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json",
        {
            "schema_version": "track_b_open_order_truth_v1",
            "generated_at": NOW.isoformat(),
            "classification": "NO_OPEN_ORDERS" if not order_states else "OPEN_CLOSE_ORDER_WORKING",
            "order_states": order_states,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json",
        {
            "schema_version": "track_b_managed_order_registry_v1",
            "generated_at": NOW.isoformat(),
            "classification": "NO_MANAGED_ORDERS" if not managed_order_states else managed_order_states[0]["classification"],
            "managed_orders": managed_order_states,
            "summary": {
                "managed_order_count": len(managed_order_states),
                "working_close_order_count": sum(1 for row in managed_order_states if row.get("is_close_order") is True),
                "suspicious_order_count": sum(
                    1 for row in managed_order_states if row.get("classification") == "CLOSE_ORDER_SUSPICIOUS"
                ),
                "duplicate_close_order_count": sum(
                    1 for row in managed_order_states if row.get("classification") == "DUPLICATE_CLOSE_ORDER_BLOCKED"
                ),
            },
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json",
        {
            "generated_at": NOW.isoformat(),
            "open_position_count": len(lifecycle_positions),
            "review_required_positions": review_positions,
        },
    )


def _broker_position() -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "track_b_root": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "expiry": "20260618",
        "quantity": "-1.0",
        "average_cost": "59377.38",
    }


def _lifecycle_position(
    *,
    policy: str = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
    bars_since_fill: int = 1,
    lifecycle_id: str = "bridge_fill_mnq_short",
) -> dict:
    return {
        "account_id": "DUM882026",
        "instrument_family": "MNQ",
        "track_b_root": "MNQ",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
        "side": "SHORT",
        "strategy_id": "track_b_paper_execution_test_mule_v1__mnq",
        "lifecycle_id": lifecycle_id,
        "avg_entry_price": "29688.69",
        "entry_timestamp": "2026-05-22T16:20:00+00:00",
        "managed_exit_policy_id": policy,
        "bars_since_fill": bars_since_fill,
        "paper_lifecycle_report_path": "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle/bridge_fill_mnq_short/track_b_strategy_managed_paper_lifecycle_report.json",
        "position_management_manifest_path": "outputs/track_b_execution_core/position_management_manifests/mnq_short.json",
    }


def _write_lifecycle_report(
    root: Path,
    *,
    lifecycle_id: str,
    policy: str = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
    bars_since_fill: int = 1,
    review_required: bool = False,
    paper_lifecycle_classification: str = "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
    broker_state_mutated: bool = True,
    primary_blocker: str | None = None,
    close_intent: dict | None = None,
    close_submit_attempt: dict | None = None,
) -> None:
    _write_json(
        root
        / "outputs"
        / "track_b_execution_core"
        / "track_b_strategy_managed_paper_lifecycle"
        / lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json",
        {
            "schema_version": "track_b_strategy_managed_paper_lifecycle_v1",
            "generated_at": NOW.isoformat(),
            "lifecycle_id": lifecycle_id,
            "strategy_id": "track_b_paper_execution_test_mule_v1__mnq",
            "instrument_family": "MNQ",
            "contract_key": "MNQ-202606",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "managed_exit_policy_id": policy,
            "managed_exit_policy_max_completed_5m_bars": 3,
            "bars_since_fill": bars_since_fill,
            "open_position_age_completed_5m_bars": bars_since_fill,
            "paper_lifecycle_classification": paper_lifecycle_classification,
            "final_position_status": "OPEN_MANAGED",
            "review_required": review_required,
            "broker_state_mutated": broker_state_mutated,
            "primary_blocker": primary_blocker,
            "close_intent": close_intent,
            "close_submit_attempt": close_submit_attempt,
            "entry_intent": {"side": "SHORT", "order_action": "SELL", "quantity": 1},
            "entry_fill": {"price": "29688.69", "filled_at": "2026-05-22T16:20:00+00:00"},
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "position_management_manifests" / "mnq_short.json",
        {
            "schema_version": "track_b_position_management_manifest_v1",
            "entry_intent_id": "mnq-short-intent",
            "lane_id": "track_b_paper_execution_test_mule_v1__mnq",
            "strategy_id": "track_b_paper_execution_test_mule_v1__mnq",
            "lifecycle_id": lifecycle_id,
            "managed_exit_policy_id": policy,
            "lifecycle_status": "OPEN_MANAGED",
        },
    )


def _write_phase1_5m_bars(root: Path, *, symbol: str, bar_ends: list[str]) -> None:
    _write_json(
        root
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / symbol
        / "5m"
        / "latest_runtime_candles.json",
        {
            "schema_version": "phase1_runtime_candles_v1",
            "symbol": symbol,
            "timeframe": "5m",
            "bars": [{"bar_end": value, "close": "100.0"} for value in bar_ends],
        },
    )


def _write_registry_open_managed_events(root: Path) -> None:
    path = root / "outputs" / "track_b_execution_core" / "trade_registry" / "live_trade_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    events = [
        TradeEvent(
            event_id="trade_mnq_entry_fill",
            event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
            generated_at=NOW,
            trade_id="trade_mnq",
            lifecycle_id="bridge_fill_mnq_short",
            lane_id="track_b_paper_execution_test_mule_v1__mnq",
            thesis_strategy_id="track_b_paper_execution_test_mule_v1__mnq",
            account_id="DUM882026",
            symbol="MNQ",
            con_id=770561201,
            local_symbol="MNQM6",
            expiry="20260618",
            side="SHORT",
            action="SELL",
            qty=Decimal("1"),
            source_artifact_path="outputs/track_b_execution_core/test_entry.json",
            order_id="1",
            client_id="111",
            perm_id="perm_mnq",
            exec_id="exec_mnq",
            price=Decimal("29688.69"),
        ),
        TradeEvent(
            event_id="trade_mnq_open_managed",
            event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
            generated_at=NOW,
            trade_id="trade_mnq",
            lifecycle_id="bridge_fill_mnq_short",
            lane_id="track_b_paper_execution_test_mule_v1__mnq",
            thesis_strategy_id="track_b_paper_execution_test_mule_v1__mnq",
            account_id="DUM882026",
            symbol="MNQ",
            con_id=770561201,
            local_symbol="MNQM6",
            expiry="20260618",
            side="SHORT",
            action="SELL",
            qty=Decimal("1"),
            source_artifact_path="outputs/track_b_execution_core/test_lifecycle.json",
            metadata={"managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"},
        ),
    ]
    path.write_text("\n".join(json.dumps(event.to_dict(), sort_keys=True) for event in events) + "\n", encoding="utf-8")


def _write_registry_open_managed_events_for(
    root: Path,
    *,
    trade_id: str,
    lifecycle_id: str,
    generated_at: datetime,
    symbol: str,
    local_symbol: str,
    con_id: int,
    order_id: str,
    perm_id: str,
    exec_id: str,
    append: bool = False,
) -> None:
    path = root / "outputs" / "track_b_execution_core" / "trade_registry" / "live_trade_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    events = [
        TradeEvent(
            event_id=f"{trade_id}_entry_fill",
            event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
            generated_at=generated_at,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            lane_id=f"{symbol.lower()}_us_active_participation_short",
            thesis_strategy_id=f"{symbol.lower()}_us_active_participation_short",
            account_id="DUM882026",
            symbol=symbol,
            con_id=con_id,
            local_symbol=local_symbol,
            expiry="20260618",
            side="SHORT",
            action="SELL",
            qty=Decimal("1"),
            source_artifact_path="outputs/track_b_execution_core/test_entry.json",
            order_id=order_id,
            client_id="111",
            perm_id=perm_id,
            exec_id=exec_id,
            price=Decimal("7604.75"),
        ),
        TradeEvent(
            event_id=f"{trade_id}_open_managed",
            event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
            generated_at=generated_at + timedelta(seconds=1),
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            lane_id=f"{symbol.lower()}_us_active_participation_short",
            thesis_strategy_id=f"{symbol.lower()}_us_active_participation_short",
            account_id="DUM882026",
            symbol=symbol,
            con_id=con_id,
            local_symbol=local_symbol,
            expiry="20260618",
            side="SHORT",
            action="SELL",
            qty=Decimal("1"),
            source_artifact_path="outputs/track_b_execution_core/test_lifecycle.json",
            metadata={"managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"},
        ),
    ]
    mode = "a" if append and path.exists() else "w"
    with path.open(mode, encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event.to_dict(), sort_keys=True) + "\n")


def _write_registry_closed_flat_events(root: Path, *, trade_id: str, lifecycle_id: str) -> None:
    path = root / "outputs" / "track_b_execution_core" / "trade_registry" / "live_trade_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    events = [
        TradeEvent(
            event_id="trade_terminal_entry_fill",
            event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
            generated_at=NOW,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            lane_id="track_b_paper_execution_test_mule_v1__mnq",
            thesis_strategy_id="track_b_paper_execution_test_mule_v1__mnq",
            account_id="DUM882026",
            symbol="MNQ",
            con_id=770561201,
            local_symbol="MNQM6",
            expiry="20260618",
            side="SHORT",
            action="SELL",
            qty=Decimal("1"),
            source_artifact_path="outputs/track_b_execution_core/test_entry.json",
            order_id="1",
            client_id="111",
            perm_id="perm_mnq_entry",
            exec_id="exec_mnq_entry",
            price=Decimal("29688.69"),
        ),
        TradeEvent(
            event_id="trade_terminal_open_managed",
            event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
            generated_at=NOW + timedelta(seconds=1),
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            lane_id="track_b_paper_execution_test_mule_v1__mnq",
            thesis_strategy_id="track_b_paper_execution_test_mule_v1__mnq",
            account_id="DUM882026",
            symbol="MNQ",
            con_id=770561201,
            local_symbol="MNQM6",
            expiry="20260618",
            side="SHORT",
            action="SELL",
            qty=Decimal("1"),
            source_artifact_path="outputs/track_b_execution_core/test_lifecycle.json",
            order_id="1",
            client_id="111",
            perm_id="perm_mnq_entry",
            exec_id="exec_mnq_entry",
        ),
        TradeEvent(
            event_id="trade_terminal_exit_fill",
            event_type=TradeEventType.EXIT_FILL_BROKER_BACKED,
            generated_at=NOW + timedelta(seconds=2),
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            lane_id="track_b_paper_execution_test_mule_v1__mnq",
            thesis_strategy_id="track_b_paper_execution_test_mule_v1__mnq",
            account_id="DUM882026",
            symbol="MNQ",
            con_id=770561201,
            local_symbol="MNQM6",
            expiry="20260618",
            side="SHORT",
            action="BUY",
            qty=Decimal("1"),
            source_artifact_path="outputs/track_b_execution_core/test_exit.json",
            order_id="2",
            client_id="111",
            perm_id="perm_mnq_exit",
            exec_id="exec_mnq_exit",
            price=Decimal("29690.00"),
        ),
        TradeEvent(
            event_id="trade_terminal_review_noise",
            event_type=TradeEventType.REVIEW_REQUIRED,
            generated_at=NOW + timedelta(seconds=3),
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            lane_id="track_b_paper_execution_test_mule_v1__mnq",
            thesis_strategy_id="track_b_paper_execution_test_mule_v1__mnq",
            account_id="DUM882026",
            symbol="MNQ",
            con_id=770561201,
            local_symbol="MNQM6",
            expiry="20260618",
            side="SHORT",
            action="RECONCILE",
            qty=Decimal("1"),
            source_artifact_path="outputs/track_b_execution_core/test_reconcile.json",
            reason_codes=("REGISTRY_RECONCILIATION_REVIEW_REQUIRED",),
        ),
    ]
    path.write_text("\n".join(json.dumps(event.to_dict(), sort_keys=True) for event in events) + "\n", encoding="utf-8")


def _write_position_truth_clean_flat(root: Path) -> None:
    _write_json(
        root / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {
            "schema_version": "track_b_position_truth_v1",
            "generated_at": NOW.isoformat(),
            "classification": "CLEAN_FLAT_READY",
            "summary": {"overall_classification": "CLEAN_FLAT_READY", "broker_exposure_present": False},
            "broker_positions": [],
            "open_broker_orders": [],
        },
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
