from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEvent, TradeEventType
import mgc_v05l.execution_core.track_b_managed_position_registry as managed_position_registry_module
from mgc_v05l.execution_core.track_b_managed_position_registry import (
    LIFECYCLE_WITHOUT_BROKER,
    MANUAL_OPERATOR_EXPOSURE,
    MANAGED_POSITION_METADATA_INCOMPLETE,
    NO_MANAGED_POSITIONS,
    OPEN_MANAGED_CLOSE_WORKING,
    OPEN_MANAGED_EXIT_DUE,
    OPEN_MANAGED_MATCHED,
    PROJECTION_AUTHORITY_COHERENT,
    REVIEW_REQUIRED,
    STALE_MANAGED_POSITION_EVIDENCE,
    TrackBManagedPositionRegistryConfig,
    build_track_b_managed_position_registry,
    write_track_b_managed_position_registry,
    _required_close_action,
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


def test_required_close_action_prefers_current_broker_quantity_over_stale_side() -> None:
    assert _required_close_action(side="SHORT", signed_broker_qty=Decimal("1")) == "SELL"
    assert _required_close_action(side="LONG", signed_broker_qty=Decimal("-1")) == "BUY"


def test_submitted_entry_with_broker_effect_projects_adoptable_lifecycle_candidate() -> None:
    event = SimpleNamespace(
        event_type="ENTRY_ORDER_SUBMITTED",
        generated_at=NOW,
        trade_id="trade_mbt_submitted",
        lifecycle_id="life_mbt_submitted",
        lane_id="mbt_globex_active_participation_short",
        thesis_strategy_id="mbt_globex_active_participation_short",
        account_id="DUM882026",
        symbol="MBT",
        local_symbol="MBTU6",
        con_id=772435596,
        expiry="20260925",
        side="SHORT",
        action="SELL",
        qty=Decimal("1"),
        order_id="1",
        perm_id="1618435662",
        exec_id=None,
        price=Decimal("62220.0"),
    )
    record = SimpleNamespace(current_state="WORKING_ENTRY", event_chain=(event,))
    broker_position = {
        "account_id": "DUM882026",
        "security_type": "FUT",
        "symbol": "MBT",
        "local_symbol": "MBTU6",
        "con_id": 772435596,
        "expiry": "20260925",
        "quantity": "-1.0",
    }

    candidate = managed_position_registry_module._registry_lifecycle_candidate_for_broker_position(
        broker_position=broker_position,
        broker_open_orders=[],
        terminal_records=(record,),
    )

    assert candidate is not None
    assert candidate["source"] == "TRACK_B_LIVE_TRADE_REGISTRY_SUBMITTED_ENTRY_BROKER_EFFECT"
    assert candidate["broker_effect_observed_after_submitted_entry"] is True
    assert candidate["local_symbol"] == "MBTU6"
    assert candidate["side"] == "SHORT"
    assert candidate["aggregate_qty"] == "-1"
    assert candidate["entry_perm_ids"] == ["1618435662"]


def test_submitted_entry_broker_effect_waits_when_same_contract_order_still_open() -> None:
    event = SimpleNamespace(
        event_type="ENTRY_ORDER_SUBMITTED",
        generated_at=NOW,
        trade_id="trade_mbt_submitted",
        lifecycle_id="life_mbt_submitted",
        lane_id="mbt_globex_active_participation_short",
        thesis_strategy_id="mbt_globex_active_participation_short",
        account_id="DUM882026",
        symbol="MBT",
        local_symbol="MBTU6",
        con_id=772435596,
        expiry="20260925",
        side="SHORT",
        action="SELL",
        qty=Decimal("1"),
        order_id="1",
        perm_id="1618435662",
        exec_id=None,
        price=Decimal("62220.0"),
    )
    record = SimpleNamespace(current_state="WORKING_ENTRY", event_chain=(event,))
    broker_position = {
        "account_id": "DUM882026",
        "security_type": "FUT",
        "symbol": "MBT",
        "local_symbol": "MBTU6",
        "con_id": 772435596,
        "expiry": "20260925",
        "quantity": "-1.0",
    }
    open_order = {
        "account_id": "DUM882026",
        "symbol": "MBT",
        "local_symbol": "MBTU6",
        "con_id": 772435596,
    }

    candidate = managed_position_registry_module._registry_lifecycle_candidate_for_broker_position(
        broker_position=broker_position,
        broker_open_orders=[open_order],
        terminal_records=(record,),
    )

    assert candidate is None


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


def test_historical_review_debris_with_stale_sources_projects_current_scope_flat(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write_position_truth_clean_flat(tmp_path)
    position_truth_path = (
        tmp_path / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
    )
    position_truth = json.loads(position_truth_path.read_text(encoding="utf-8"))
    position_truth["generated_at"] = (NOW - timedelta(minutes=10)).isoformat()
    position_truth_path.write_text(json.dumps(position_truth, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    open_order_truth_path = (
        tmp_path / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
    )
    open_order_truth = json.loads(open_order_truth_path.read_text(encoding="utf-8"))
    open_order_truth["classification"] = "ORDER_TRUTH_STALE"
    open_order_truth["summary"] = {"classification": "ORDER_TRUTH_STALE", "open_order_count": 0}
    open_order_truth_path.write_text(json.dumps(open_order_truth, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id="old_test_mule_review_required_lifecycle",
        review_required=True,
        paper_lifecycle_classification="TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
    )

    config = TrackBManagedPositionRegistryConfig(repo_root=tmp_path)
    payload = build_track_b_managed_position_registry(config=config, now=NOW)
    authority_path, _events = write_track_b_managed_position_registry(config=config, payload=payload, now=NOW)
    projection_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]
    projection = json.loads(projection_path.read_text(encoding="utf-8"))

    assert payload["classification"] == NO_MANAGED_POSITIONS
    assert payload["managed_positions"] == []
    assert payload["review_required_positions"] == []
    assert payload["historical_review_positions"][0]["current_hot_path_scope"] == "HISTORICAL_UNRESOLVED_FULL_AUDIT_ONLY"
    assert payload["source_freshness"]["stale"] is False
    assert payload["source_freshness"]["diagnostic_stale"] is True
    assert payload["source_freshness"]["stale_diagnostic_only"] is True
    assert payload["source_freshness"]["current_scope_flat_authority_clean"] is True
    assert "position_truth" in payload["source_freshness"]["stale_sources"]
    assert payload["open_order_truth"]["classification"] == "ORDER_TRUTH_STALE"
    assert projection["classification"] == NO_MANAGED_POSITIONS
    assert projection["managed_positions"] == []
    assert projection["review_required_positions"] == []
    assert projection["historical_review_positions"]
    assert projection["projection_only"] is True
    assert projection["source_authority_path"] == str(authority_path)


def test_stale_live_position_review_row_does_not_override_clean_current_flat_authority(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    stale_review = {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "track_b_root": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
        "side": "LONG",
        "lifecycle_id": "bridge_fill_MNQ|1m|2026-05-22T17:42:00Z|BUY_TO_OPEN",
        "trade_id": "trade_old_bridge_fill",
    }
    _write_json(
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "paper_trade_ledger"
        / "latest_track_b_live_position_status.json",
        {
            "generated_at": NOW.isoformat(),
            "open_position_count": 1,
            "positions": [stale_review],
            "review_required_positions": [stale_review],
        },
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == NO_MANAGED_POSITIONS
    assert payload["managed_positions"] == []
    assert payload["review_required_positions"] == []
    assert payload["summary"]["managed_position_count"] == 0
    assert payload["historical_review_positions"][0]["lifecycle_id"] == stale_review["lifecycle_id"]
    assert payload["historical_review_positions"][0]["historical_only"] is True
    assert payload["historical_review_positions"][0]["current_scope_linked"] is False


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


def test_stale_managed_order_projection_does_not_stale_current_position_authority(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(bars_since_fill=3)
    _seed_base(tmp_path, broker_positions=[_broker_position()], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(tmp_path, lifecycle_id=lifecycle["lifecycle_id"], bars_since_fill=3)
    managed_orders_path = (
        tmp_path / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json"
    )
    managed_orders = json.loads(managed_orders_path.read_text(encoding="utf-8"))
    managed_orders["generated_at"] = (NOW - timedelta(hours=2)).isoformat()
    managed_orders_path.write_text(json.dumps(managed_orders, indent=2, sort_keys=True), encoding="utf-8")

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["source_freshness"]["stale"] is False
    assert "managed_order_registry" not in payload["source_freshness"]["ages_seconds"]
    assert payload["classification"] == OPEN_MANAGED_EXIT_DUE
    assert payload["managed_positions"][0]["classification"] == OPEN_MANAGED_EXIT_DUE


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


def test_reconciliation_review_row_same_contract_does_not_pollute_active_matched_position(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(lifecycle_id="current_managed_mnq", bars_since_fill=1)
    lifecycle["trade_id"] = "current_trade"
    stale_review = _lifecycle_position(lifecycle_id="old_review_required_mnq", bars_since_fill=99)
    stale_review["trade_id"] = "old_trade"
    stale_review["review_required"] = True
    stale_review["paper_lifecycle_classification"] = "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED"
    _seed_base(
        tmp_path,
        broker_positions=[_broker_position()],
        lifecycle_positions=[lifecycle],
        review_positions=[stale_review],
    )
    _write_lifecycle_report(tmp_path, lifecycle_id=lifecycle["lifecycle_id"], bars_since_fill=1)

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_MATCHED
    assert payload["review_required_positions"] == []
    assert payload["managed_positions"][0]["lifecycle_id"] == "current_managed_mnq"
    assert payload["managed_positions"][0]["trade_id"] == "current_trade"
    assert payload["historical_review_positions"][0]["lifecycle_id"] == "old_review_required_mnq"
    assert payload["historical_review_positions"][0]["current_scope_linked"] is False
    assert payload["historical_review_positions"][0]["historical_only"] is True


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


def test_globex_active_15m_exit_policy_holds_before_three_completed_5m_bars(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1", bars_since_fill=2)
    _seed_base(tmp_path, broker_positions=[_broker_position()], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id=lifecycle["lifecycle_id"],
        policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        bars_since_fill=2,
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_MATCHED
    assert payload["managed_positions"][0]["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert payload["managed_positions"][0]["bars_since_entry"] == 2
    assert payload["managed_positions"][0]["exit_due"] is False


def test_globex_active_15m_exit_policy_due_after_three_completed_5m_bars(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1", bars_since_fill=3)
    _seed_base(tmp_path, broker_positions=[_broker_position()], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id=lifecycle["lifecycle_id"],
        policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        bars_since_fill=3,
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_EXIT_DUE
    assert payload["managed_positions"][0]["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert payload["managed_positions"][0]["bars_since_entry"] == 3
    assert payload["managed_positions"][0]["exit_due"] is True


def test_swept_active_position_recovers_fill_time_and_policy_for_15m_due(tmp_path: Path) -> None:
    lifecycle = {
        **_lifecycle_position(policy="", bars_since_fill=0),
        "instrument_family": "MES",
        "track_b_root": "MES",
        "symbol": "MES",
        "contract_key": "MES-202609",
        "local_symbol": "MESU6",
        "con_id": 793356217,
        "quantity": "1",
        "aggregate_qty": "1",
        "side": "LONG",
        "strategy_id": "mes_us_active_participation_long",
        "lane_id": "mes_us_active_participation_long",
        "lifecycle_id": "life-mes-active",
        "trade_id": "trade-mes-active",
        "entry_timestamp": None,
        "managed_exit_policy_id": None,
        "bars_since_fill": None,
        "lifecycle_units": [
            {
                "lifecycle_id": "life-mes-active",
                "trade_id": "trade-mes-active",
                "lane_id": "mes_us_active_participation_long",
                "entry_order_id": "1",
                "entry_perm_id": "1871421812",
                "entry_exec_id": "0000e1a7.6a4255b6.01.01",
                "entry_time": None,
                "managed_exit_policy_id": None,
            }
        ],
    }
    _seed_base(tmp_path, broker_positions=[_broker_position_mes_long()], lifecycle_positions=[lifecycle])
    _write_phase1_5m_bars(
        tmp_path,
        symbol="MES",
        bar_ends=[
            "2026-05-22T16:25:00+00:00",
            "2026-05-22T16:30:00+00:00",
            "2026-05-22T16:35:00+00:00",
        ],
    )
    _write_entry_fill_event(
        tmp_path,
        trade_id="trade-mes-active",
        lifecycle_id="life-mes-active",
        lane_id="mes_us_active_participation_long",
        symbol="MES",
        local_symbol="MESU6",
        con_id=793356217,
        generated_at=datetime(2026, 5, 22, 16, 20, tzinfo=UTC),
        side="LONG",
        action="BUY",
        order_id="1",
        perm_id="1871421812",
        exec_id="0000e1a7.6a4255b6.01.01",
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    position = payload["managed_positions"][0]
    assert payload["classification"] == OPEN_MANAGED_EXIT_DUE
    assert position["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert position["entry_time"] == "2026-05-22T16:20:00+00:00"
    assert position["lifecycle_units"][0]["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert position["lifecycle_units"][0]["entry_time"] == "2026-05-22T16:20:00+00:00"
    assert position["bars_since_entry"] == 3
    assert position["exit_due"] is True


def test_swept_active_position_with_two_completed_bars_remains_hold(tmp_path: Path) -> None:
    lifecycle = {
        **_lifecycle_position(policy="", bars_since_fill=0),
        "instrument_family": "MES",
        "track_b_root": "MES",
        "symbol": "MES",
        "contract_key": "MES-202609",
        "local_symbol": "MESU6",
        "con_id": 793356217,
        "quantity": "1",
        "aggregate_qty": "1",
        "side": "LONG",
        "strategy_id": "mes_us_active_participation_long",
        "lane_id": "mes_us_active_participation_long",
        "lifecycle_id": "life-mes-young",
        "trade_id": "trade-mes-young",
        "entry_timestamp": None,
        "managed_exit_policy_id": None,
        "bars_since_fill": None,
    }
    _seed_base(tmp_path, broker_positions=[_broker_position_mes_long()], lifecycle_positions=[lifecycle])
    _write_phase1_5m_bars(
        tmp_path,
        symbol="MES",
        bar_ends=[
            "2026-05-22T16:25:00+00:00",
            "2026-05-22T16:30:00+00:00",
        ],
    )
    _write_entry_fill_event(
        tmp_path,
        trade_id="trade-mes-young",
        lifecycle_id="life-mes-young",
        lane_id="mes_us_active_participation_long",
        symbol="MES",
        local_symbol="MESU6",
        con_id=793356217,
        generated_at=datetime(2026, 5, 22, 16, 20, tzinfo=UTC),
        side="LONG",
        action="BUY",
        order_id="1",
        perm_id="1871421812",
        exec_id="0000e1a7.6a4255b6.01.01",
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_MATCHED
    assert payload["managed_positions"][0]["bars_since_entry"] == 2
    assert payload["managed_positions"][0]["exit_due"] is False


def test_existing_globex_active_60m_placeholder_is_due_after_three_completed_5m_bars(tmp_path: Path) -> None:
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

    assert payload["classification"] == OPEN_MANAGED_EXIT_DUE
    assert payload["managed_positions"][0]["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    assert payload["managed_positions"][0]["bars_since_entry"] == 3
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


def test_exit_due_survives_stale_dependency_freshness_when_current_owner_is_exact(tmp_path: Path) -> None:
    lifecycle = {**_lifecycle_position(bars_since_fill=3), "trade_id": "trade_mnq_due"}
    _seed_base(tmp_path, broker_positions=[_broker_position()], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(tmp_path, lifecycle_id=lifecycle["lifecycle_id"], bars_since_fill=3)
    position_truth_path = (
        tmp_path / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
    )
    position_truth = json.loads(position_truth_path.read_text(encoding="utf-8"))
    position_truth["generated_at"] = (NOW - timedelta(minutes=10)).isoformat()
    position_truth_path.write_text(json.dumps(position_truth, indent=2, sort_keys=True), encoding="utf-8")

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == OPEN_MANAGED_EXIT_DUE
    assert payload["source_freshness"]["stale"] is True
    position = payload["managed_positions"][0]
    assert position["classification"] == OPEN_MANAGED_EXIT_DUE
    assert position["exit_due"] is True
    assert position["exit_due_state"] == "EXIT_DUE"
    assert position["freshness_state"] == "STALE_DEPENDENCY"
    assert position["exit_due_evidence_stale"] is True
    assert position["apply_authority_degraded"] is True
    assert "position_truth" in position["stale_dependency_sources"]
    assert position["required_close_action"] == "BUY"
    assert position["required_close_quantity"] == "1"
    assert position["lifecycle_id"] == lifecycle["lifecycle_id"]
    assert position["trade_id"] == "trade_mnq_due"


def test_owner_confirmed_timebox_due_supersedes_stale_lifecycle_review(tmp_path: Path) -> None:
    lifecycle = {
        **_lifecycle_position(
            policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
            bars_since_fill=0,
            lifecycle_id="life-stale-review-due",
        ),
        "trade_id": "trade-stale-review-due",
        "bars_since_fill": None,
    }
    _seed_base(tmp_path, broker_positions=[_broker_position()], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id=lifecycle["lifecycle_id"],
        policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        bars_since_fill=0,
        review_required=True,
        paper_lifecycle_classification="TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
        primary_blocker="Existing OPEN_MANAGED lifecycle has no broker-confirmed entry fill.",
    )
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

    position = payload["managed_positions"][0]
    assert payload["classification"] == OPEN_MANAGED_EXIT_DUE
    assert position["projection_authority_owner_confirmed"] is True
    assert position["bars_since_entry"] == 3
    assert position["exit_due"] is True
    assert position["exit_due_state"] == "EXIT_DUE"
    assert position["required_close_action"] == "BUY"
    assert position["required_close_quantity"] == "1"
    assert (
        position["stale_review_due_repair"]["classification"]
        == "OWNER_CONFIRMED_TIMEBOX_DUE_SUPERSEDES_STALE_REVIEW"
    )
    assert (
        position["review_required_position"]["primary_blocker"]
        == "Existing OPEN_MANAGED lifecycle has no broker-confirmed entry fill."
    )


def test_stale_lifecycle_review_without_owner_confirmation_does_not_become_due() -> None:
    row = {
        "classification": REVIEW_REQUIRED,
        "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        "bars_since_entry": 3,
        "broker_position": _broker_position(),
        "lifecycle_position": _lifecycle_position(policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"),
    }

    repaired = managed_position_registry_module._repair_owner_confirmed_timebox_due_positions([row])

    assert repaired[0]["classification"] == REVIEW_REQUIRED
    assert repaired[0].get("exit_due") is not True
    assert "stale_review_due_repair" not in repaired[0]


def test_owner_confirmed_timebox_due_supersedes_stale_dependency_classification() -> None:
    row = {
        "classification": STALE_MANAGED_POSITION_EVIDENCE,
        "projection_authority_owner_confirmed": True,
        "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        "bars_since_entry": 3,
        "broker_position": _broker_position(),
        "lifecycle_position": _lifecycle_position(policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"),
    }

    repaired = managed_position_registry_module._repair_owner_confirmed_timebox_due_positions([row])

    assert repaired[0]["classification"] == OPEN_MANAGED_EXIT_DUE
    assert repaired[0]["exit_due"] is True
    assert repaired[0]["required_close_action"] == "BUY"
    assert repaired[0]["required_close_quantity"] == "1"


def test_owner_confirmed_timebox_due_repair_keeps_account_mismatch_in_review() -> None:
    broker = {**_broker_position(), "account_id": "OTHER_ACCOUNT"}
    lifecycle = _lifecycle_position(policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1")
    row = {
        "classification": REVIEW_REQUIRED,
        "projection_authority_owner_confirmed": True,
        "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        "bars_since_entry": 3,
        "broker_position": broker,
        "lifecycle_position": lifecycle,
    }

    repaired = managed_position_registry_module._repair_owner_confirmed_timebox_due_positions([row])

    assert repaired[0]["classification"] == REVIEW_REQUIRED
    assert repaired[0].get("exit_due") is not True
    assert "stale_review_due_repair" not in repaired[0]


def test_stale_dependency_does_not_invent_exit_due_without_bar_evidence(tmp_path: Path) -> None:
    lifecycle = {**_lifecycle_position(bars_since_fill=1), "trade_id": "trade_mnq_not_due"}
    _seed_base(tmp_path, broker_positions=[_broker_position()], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(tmp_path, lifecycle_id=lifecycle["lifecycle_id"], bars_since_fill=1)
    position_truth_path = (
        tmp_path / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
    )
    position_truth = json.loads(position_truth_path.read_text(encoding="utf-8"))
    position_truth["generated_at"] = (NOW - timedelta(minutes=10)).isoformat()
    position_truth_path.write_text(json.dumps(position_truth, indent=2, sort_keys=True), encoding="utf-8")

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == STALE_MANAGED_POSITION_EVIDENCE
    position = payload["managed_positions"][0]
    assert position["classification"] == STALE_MANAGED_POSITION_EVIDENCE
    assert position["exit_due"] is False
    assert position["freshness_state"] == "STALE_DEPENDENCY"


def test_stale_reconciliation_does_not_erase_fresh_broker_backed_managed_position(tmp_path: Path) -> None:
    broker = {
        "account_id": "DUM882026",
        "security_type": "FUT",
        "symbol": "MNQ",
        "track_b_root": "MNQ",
        "local_symbol": "MNQU6",
        "con_id": 793356225,
        "expiry": "20260918",
        "quantity": "-1.0",
    }
    lifecycle = {
        **_lifecycle_position(bars_since_fill=1),
        "trade_id": "trade_mnq_current",
        "contract_key": "MNQ-202609",
        "local_symbol": "MNQU6",
        "con_id": 793356225,
    }
    _seed_base(tmp_path, broker_positions=[broker], lifecycle_positions=[lifecycle])
    _write_lifecycle_report(tmp_path, lifecycle_id=lifecycle["lifecycle_id"], bars_since_fill=1)
    _write_json(
        tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json",
        {
            "account": "DUM882026",
            "positions_complete": True,
            "positions": [broker],
        },
    )
    _write_json(
        tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json",
        {
            "account": "DUM882026",
            "open_orders_complete": True,
            "open_orders": [],
        },
    )
    reconciliation_path = (
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation["generated_at"] = (NOW - timedelta(minutes=10)).isoformat()
    reconciliation_path.write_text(json.dumps(reconciliation, indent=2, sort_keys=True), encoding="utf-8")

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["source_freshness"]["stale_sources"] == ["reconciliation"]
    assert payload["classification"] == OPEN_MANAGED_MATCHED
    position = payload["managed_positions"][0]
    assert position["classification"] == OPEN_MANAGED_MATCHED
    assert position["attention_required"] is False
    assert position["side"] == "SHORT"
    assert position["signed_broker_qty"] == "-1"
    assert position["signed_lifecycle_qty"] == "-1"


def test_stale_reconciliation_does_not_hide_fresh_broker_backed_adoption_candidate(tmp_path: Path) -> None:
    broker = {
        "account_id": "DUM882026",
        "security_type": "FUT",
        "symbol": "MNQ",
        "track_b_root": "MNQ",
        "local_symbol": "MNQU6",
        "con_id": 793356225,
        "expiry": "20260918",
        "quantity": "-1.0",
    }
    _seed_base(tmp_path, broker_positions=[broker])
    _write_json(
        tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json",
        {
            "account": "DUM882026",
            "positions_complete": True,
            "positions": [broker],
        },
    )
    _write_json(
        tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json",
        {
            "account": "DUM882026",
            "open_orders_complete": True,
            "open_orders": [],
        },
    )
    reconciliation_path = (
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation["generated_at"] = (NOW - timedelta(minutes=10)).isoformat()
    reconciliation_path.write_text(json.dumps(reconciliation, indent=2, sort_keys=True), encoding="utf-8")

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["source_freshness"]["stale_sources"] == ["reconciliation"]
    assert payload["classification"] == MANUAL_OPERATOR_EXPOSURE
    position = payload["managed_positions"][0]
    assert position["classification"] == MANUAL_OPERATOR_EXPOSURE
    assert position["side"] == "SHORT"
    assert position["signed_broker_qty"] == "-1"


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


def test_broker_only_position_without_track_b_owner_is_manual_operator_exposure(tmp_path: Path) -> None:
    _seed_base(tmp_path, broker_positions=[_broker_position()])

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == MANUAL_OPERATOR_EXPOSURE
    assert payload["managed_positions"][0]["reconciliation_status"] == MANUAL_OPERATOR_EXPOSURE
    assert "do not adopt" in payload["managed_positions"][0]["recommended_operator_action"]


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
    assert payload["pre_restart_exposure_resolution"]["resolved_managed_exposure_count"] == 1


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


def test_registry_owner_supersedes_stale_same_contract_projection(
    tmp_path: Path,
    monkeypatch,
) -> None:
    broker = _broker_position()
    stale = _lifecycle_position(
        lifecycle_id="reserved_submit_mnq_london_open_active_participation_short_20260612T073709027361Z_20a34f036564",
        policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
    )
    current = {
        **_lifecycle_position(
            lifecycle_id="reserved_submit_mnq_globex_active_participation_short_20260614T224221875069Z_d054ab235060",
            policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        ),
        "trade_id": "trade_current_mnq_globex_short",
        "lane_id": "mnq_globex_active_participation_short",
        "strategy_id": "mnq_globex_active_participation_short",
        "entry_timestamp": "2026-06-14T22:42:22+00:00",
        "entry_order_ids": ["2"],
        "entry_perm_ids": ["1793991648"],
        "entry_exec_ids": ["0000e1a7.6a431355.01.01"],
    }
    _seed_base(tmp_path, broker_positions=[broker], lifecycle_positions=[stale])
    reconciliation_path = (
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation["current_exposure_owner_resolution"] = {
        "classification": "OWNED_MANAGED_EXPOSURE",
        "broker_position_count": 1,
        "owned_exposure_count": 1,
        "owned_exposures": [
            {
                "classification": "OWNED_MANAGED_EXPOSURE",
                "reason_codes": ["STALE_CACHED_RECONCILIATION_OWNER"],
                "broker_position": broker,
                "canonical_broker_position": broker,
                "lifecycle_position": stale,
                "trade_id": "stale_trade",
                "lifecycle_id": stale["lifecycle_id"],
                "position_key": "DUM882026|MNQM6|770561201",
                "exit_due": False,
            }
        ],
        "resolved_lifecycle_positions": [stale],
    }
    reconciliation_path.write_text(json.dumps(reconciliation), encoding="utf-8")
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id=stale["lifecycle_id"],
        policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        bars_since_fill=1,
    )

    def fake_resolver(**_kwargs):
        owner_resolution = {
            "classification": "OWNED_MANAGED_EXPOSURE",
            "broker_position_count": 1,
            "broker_open_order_count": 0,
            "owned_exposure_count": 1,
            "review_required_exposure_count": 0,
            "owned_exposures": [
                {
                    "classification": "OWNED_MANAGED_EXPOSURE",
                    "reason_codes": ["REGISTRY_OPEN_MANAGED_MATCHED_BROKER_POSITION"],
                    "broker_position": broker,
                    "canonical_broker_position": broker,
                    "lifecycle_position": current,
                    "trade_id": current["trade_id"],
                    "lifecycle_id": current["lifecycle_id"],
                    "position_key": "DUM882026|MNQM6|770561201",
                    "exit_due": False,
                }
            ],
            "review_required_exposures": [],
            "resolved_lifecycle_positions": [current],
            "stale_superseded_full_audit_only": [],
            "read_only": True,
            "no_broad_flatten_generated": True,
        }
        return {
            "classification": "PROJECTION_STALE_MANAGED_EXPOSURE_RESOLVED",
            "broker_position_count": 1,
            "broker_open_order_count": 0,
            "resolved_managed_exposure_count": 1,
            "review_required_exposure_count": 0,
            "resolved_lifecycle_positions": [current],
            "managed_exposures": [],
            "review_required_exposures": [],
            "restart_with_owned_exposure_allowed": True,
            "no_broad_flatten_generated": True,
            "read_only": True,
            "current_exposure_owner_resolution": owner_resolution,
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

    position = payload["managed_positions"][0]
    assert position["lifecycle_id"] == current["lifecycle_id"]
    assert position["trade_id"] == current["trade_id"]
    assert position["projection_authority_owner_confirmed"] is True
    assert stale["lifecycle_id"] not in {row.get("lifecycle_id") for row in payload["managed_positions"]}
    assert payload["projection_authority_diagnostics"]["classification"] == PROJECTION_AUTHORITY_COHERENT


def test_registry_long_fills_supersede_stale_opposite_side_lifecycle_same_contract(tmp_path: Path) -> None:
    broker = {
        **_broker_position_mes_long(),
        "quantity": "2.0",
    }
    stale_short = {
        **_lifecycle_position(
            lifecycle_id="reserved_submit_mes_globex_active_participation_short_20260612T064610876433Z_7525093fc5e5",
            policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        ),
        "account_id": "MULTIPLE",
        "instrument_family": "MES",
        "track_b_root": "MES",
        "symbol": "MES",
        "contract_key": "MES-202609",
        "local_symbol": "MESU6",
        "con_id": 793356217,
        "expiry": "20260918",
        "quantity": "1",
        "aggregate_qty": "-1",
        "side": "SHORT",
        "trade_id": "trade_stale_mes_short",
        "entry_timestamp": "2026-05-21T06:46:12+00:00",
    }
    _seed_base(tmp_path, broker_positions=[broker], lifecycle_positions=[stale_short])
    _write_entry_fill_event(
        tmp_path,
        trade_id="trade_mes_long_a",
        lifecycle_id="reserved_submit_mes_london_open_active_participation_long_20260617T071508366407Z_9774b038702f",
        lane_id="mes_london_open_active_participation_long",
        symbol="MES",
        local_symbol="MESU6",
        con_id=793356217,
        generated_at=NOW + timedelta(minutes=1),
        side="LONG",
        action="BUY",
        order_id="9",
        perm_id="2130844547",
        exec_id="0000e1a7.6a46e8e6.01.01",
    )
    _write_entry_fill_event(
        tmp_path,
        trade_id="trade_mes_long_b",
        lifecycle_id="reserved_submit_mes_london_open_active_participation_long_20260617T071508486753Z_b7b64a634aac",
        lane_id="mes_london_open_active_participation_long",
        symbol="MES",
        local_symbol="MESU6",
        con_id=793356217,
        generated_at=NOW + timedelta(minutes=2),
        side="LONG",
        action="BUY",
        order_id="1",
        perm_id="2130844505",
        exec_id="0000e1a7.6a46e8e5.01.01",
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    position = payload["managed_positions"][0]
    assert position["classification"] in {OPEN_MANAGED_MATCHED, OPEN_MANAGED_EXIT_DUE}
    assert position["side"] == "LONG"
    assert position["quantity"] == "2"
    assert position["signed_lifecycle_qty"] == "2"
    assert position["signed_broker_qty"] == "2"
    assert position["broker_qty_match"] is True
    assert position["lane_id"] == "mes_london_open_active_participation_long"
    assert position["lifecycle_id"] == (
        "reserved_submit_mes_london_open_active_participation_long_20260617T071508486753Z_b7b64a634aac"
    )
    assert stale_short["lifecycle_id"] not in {row.get("lifecycle_id") for row in payload["managed_positions"]}


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


def test_terminal_flat_cleanup_prevents_stale_owner_overlay_from_resurrecting_lifecycle(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(
        lifecycle_id="fresh-globex-life",
        policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
    )
    lifecycle.update(
        {
            "trade_id": "fresh-globex-trade",
            "lane_id": "mnq_globex_active_participation_short",
            "strategy_id": "mnq_globex_active_participation_short",
            "local_symbol": "MNQU6",
            "con_id": 793356225,
            "contract_key": "MNQ-202609",
            "expiry": "20260918",
            "entry_timestamp": "2026-06-14T22:20:14+00:00",
            "avg_entry_price": "30369.75",
        }
    )
    _seed_base(tmp_path)
    reconciliation_path = (
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation["current_exposure_owner_resolution"] = {
        "classification": "OWNED_MANAGED_EXPOSURE",
        "broker_position_count": 0,
        "broker_open_order_count": 0,
        "owned_exposure_count": 1,
        "owned_exposures": [
            {
                "classification": "OWNED_MANAGED_EXPOSURE",
                "broker_position": {
                    "account_id": "DUM882026",
                    "symbol": "MNQ",
                    "track_b_root": "MNQ",
                    "local_symbol": "MNQU6",
                    "con_id": 793356225,
                    "expiry": "20260918",
                    "quantity": "-1.0",
                },
                "canonical_broker_position": {
                    "account_id": "DUM882026",
                    "symbol": "MNQ",
                    "track_b_root": "MNQ",
                    "local_symbol": "MNQU6",
                    "con_id": 793356225,
                    "expiry": "20260918",
                    "quantity": "-1.0",
                },
                "lifecycle_position": lifecycle,
                "trade_id": "fresh-globex-trade",
                "lifecycle_id": "fresh-globex-life",
                "position_key": "DUM882026|MNQU6|793356225",
                "reason_codes": ["NEWEST_EXACT_BROKER_BACKED_LIFECYCLE_REPORT_SELECTED"],
            }
        ],
        "resolved_lifecycle_positions": [lifecycle],
        "read_only": True,
    }
    reconciliation_path.write_text(json.dumps(reconciliation), encoding="utf-8")
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id="fresh-globex-life",
        policy="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
    )
    _write_terminal_cleanup_trade_events(
        tmp_path,
        trade_id="fresh-globex-trade",
        lifecycle_id="fresh-globex-life",
        lane_id="mnq_globex_active_participation_short",
        local_symbol="MNQU6",
        con_id=793356225,
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == NO_MANAGED_POSITIONS
    assert payload["managed_positions"] == []
    repairs = payload["projection_authority_diagnostics"]["repairs"]
    assert repairs[0]["classification"] == "CURRENT_OWNER_TERMINAL_CLOSED_SUPPRESSED"
    assert repairs[0]["terminal_registry_truth"]["classification"] == "BROKER_FLAT_EVIDENCE_GATED_CLEANUP_TERMINAL"


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


def test_validated_expanded_futures_project_from_normalized_contract_identity(tmp_path: Path) -> None:
    contracts = (
        ("MGC", "MGCQ6", 732156883, "20260827", "MGC-202608"),
        ("GC", "GCQ6", 732156872, "20260827", "GC-202608"),
        ("ES", "ESU6", 649180671, "20260918", "ES-202609"),
        ("NQ", "NQU6", 770561204, "20260918", "NQ-202609"),
        ("MNQ", "MNQU6", 793356225, "20260918", "MNQ-202609"),
        ("MES", "MESU6", 793356217, "20260918", "MES-202609"),
        ("ZT", "ZTU6", 842590391, "20260930", "ZT-202609"),
        ("ZF", "ZFU6", 842590380, "20260930", "ZF-202609"),
        ("ZN", "ZNU6", 840227361, "20260921", "ZN-202609"),
        ("ZB", "ZBU6", 840227357, "20260921", "ZB-202609"),
        ("MBT", "MBTU6", 772435596, "20260925", "MBT-202609"),
        ("MET", "METN6", 850790385, "20260731", "MET-202607"),
        ("MSL", "MSLU6", 772435607, "20260925", "MSL-202609"),
    )
    broker_positions = [
        {
            "account_id": "DUM882026",
            "security_type": "FUT",
            "symbol": symbol,
            "local_symbol": local_symbol,
            "expiry": expiry,
            "quantity": "1.0",
            "average_cost": "1000",
        }
        for symbol, local_symbol, _con_id, expiry, _contract_key in contracts
    ]
    lifecycle_positions = [
        {
            "account_id": "DUM882026",
            "instrument_family": symbol,
            "local_symbol": local_symbol,
            "quantity": "1",
            "aggregate_qty": "1",
            "side": "LONG",
            "strategy_id": f"{symbol.lower()}_globex_active_participation_long",
            "lane_id": f"{symbol.lower()}_globex_active_participation_long",
            "lifecycle_id": f"life_{symbol.lower()}_current",
            "trade_id": f"trade_{symbol.lower()}_current",
            "entry_timestamp": "2026-05-22T16:20:00+00:00",
            "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
            "bars_since_fill": 2,
        }
        for symbol, local_symbol, _con_id, _expiry, _contract_key in contracts
    ]
    _seed_base(tmp_path, broker_positions=broker_positions, lifecycle_positions=lifecycle_positions)

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    by_symbol = {row["symbol"]: row for row in payload["managed_positions"]}
    assert set(by_symbol) == {symbol for symbol, *_rest in contracts}
    assert payload["classification"] == OPEN_MANAGED_MATCHED
    for symbol, local_symbol, con_id, expiry, contract_key in contracts:
        row = by_symbol[symbol]
        assert row["classification"] == OPEN_MANAGED_MATCHED
        assert row["local_symbol"] == local_symbol
        assert row["con_id"] == con_id
        assert row["contract_key"] == contract_key
        assert row["broker_position"]["expiry"] == expiry
        assert row["broker_position"]["contract_identity"]["source"] == "VALIDATED_TRACK_B_FUTURES_CONTRACT_REGISTRY"


def test_current_owner_overlay_replaces_stale_mes_short_with_current_long(tmp_path: Path) -> None:
    broker = {
        "account_id": "DUM882026",
        "security_type": "FUT",
        "symbol": "MES",
        "local_symbol": "MESU6",
        "expiry": "20260918",
        "quantity": "1.0",
        "average_cost": "38108.12",
    }
    stale_short = {
        "account_id": "DUM882026",
        "instrument_family": "MES",
        "local_symbol": "MESU6",
        "quantity": "1",
        "aggregate_qty": "-1",
        "side": "SHORT",
        "strategy_id": "mes_globex_active_participation_short",
        "lane_id": "mes_globex_active_participation_short",
        "lifecycle_id": "old_mes_short",
        "trade_id": "old_mes_short_trade",
        "entry_timestamp": "2026-05-22T15:20:00+00:00",
        "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
    }
    current_long = {
        "account_id": "DUM882026",
        "instrument_family": "MES",
        "local_symbol": "MESU6",
        "quantity": "1",
        "aggregate_qty": "1",
        "side": "LONG",
        "strategy_id": "mes_globex_active_participation_long",
        "lane_id": "mes_globex_active_participation_long",
        "lifecycle_id": "current_mes_long",
        "trade_id": "current_mes_long_trade",
        "entry_timestamp": "2026-05-22T16:20:00+00:00",
        "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
    }
    _seed_base(tmp_path, broker_positions=[broker], lifecycle_positions=[stale_short])
    reconciliation_path = (
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    reconciliation["current_exposure_owner_resolution"] = {
        "classification": "OWNED_MANAGED_EXPOSURE",
        "broker_position_count": 1,
        "broker_open_order_count": 0,
        "owned_exposure_count": 1,
        "owned_exposures": [
            {
                "classification": "OWNED_MANAGED_EXPOSURE",
                "broker_position": broker,
                "canonical_broker_position": broker,
                "lifecycle_position": current_long,
                "trade_id": current_long["trade_id"],
                "lifecycle_id": current_long["lifecycle_id"],
                "reason_codes": ["NEWEST_EXACT_BROKER_BACKED_LIFECYCLE_REPORT_SELECTED"],
            }
        ],
        "resolved_lifecycle_positions": [current_long],
        "read_only": True,
    }
    reconciliation_path.write_text(json.dumps(reconciliation), encoding="utf-8")

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert len(payload["managed_positions"]) == 1
    position = payload["managed_positions"][0]
    assert position["lifecycle_id"] == "current_mes_long"
    assert position["trade_id"] == "current_mes_long_trade"
    assert position["side"] == "LONG"
    assert position["signed_broker_qty"] == "1"
    assert position["signed_lifecycle_qty"] == "1"
    assert position["projection_authority_owner_confirmed"] is True


def test_fresh_broker_snapshot_registry_fill_projects_validated_futures_when_reconciliation_is_narrow(
    tmp_path: Path,
) -> None:
    contracts = (
        ("MGC", "MGCQ6", 732156883, "20260827"),
        ("GC", "GCQ6", 732156872, "20260827"),
        ("ES", "ESU6", 649180671, "20260918"),
        ("NQ", "NQU6", 770561204, "20260918"),
        ("MNQ", "MNQU6", 793356225, "20260918"),
        ("MES", "MESU6", 793356217, "20260918"),
        ("ZT", "ZTU6", 842590391, "20260930"),
        ("ZF", "ZFU6", 842590380, "20260930"),
        ("ZN", "ZNU6", 840227361, "20260921"),
        ("ZB", "ZBU6", 840227357, "20260921"),
    )
    _seed_base(tmp_path)
    _write_json(
        tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json",
        {
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "positions_complete": True,
            "positions": [
                {
                    "account_id": "DUM882026",
                    "security_type": "FUT",
                    "symbol": symbol,
                    "local_symbol": local_symbol,
                    "expiry": expiry,
                    "quantity": "1.0",
                }
                for symbol, local_symbol, _con_id, expiry in contracts
            ],
        },
    )
    _write_json(
        tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json",
        {
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "open_orders_complete": True,
            "open_orders": [],
        },
    )
    for index, (symbol, local_symbol, con_id, expiry) in enumerate(contracts):
        _write_live_entry_fill_event(
            tmp_path,
            symbol=symbol,
            local_symbol=local_symbol,
            con_id=con_id,
            expiry=expiry,
            lifecycle_id=f"life_{symbol.lower()}_registry",
            trade_id=f"trade_{symbol.lower()}_registry",
            lane_id=f"{symbol.lower()}_globex_active_participation_long",
            generated_at=NOW + timedelta(minutes=index),
            append=index > 0,
        )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW + timedelta(minutes=10),
    )

    by_symbol = {row["symbol"]: row for row in payload["managed_positions"]}
    assert set(by_symbol) == {symbol for symbol, *_rest in contracts}
    assert payload["classification"] == OPEN_MANAGED_MATCHED
    for symbol, local_symbol, con_id, _expiry in contracts:
        row = by_symbol[symbol]
        assert row["classification"] == OPEN_MANAGED_MATCHED
        assert row["local_symbol"] == local_symbol
        assert row["con_id"] == con_id
        assert row["lifecycle_id"] == f"life_{symbol.lower()}_registry"
        assert row["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"


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


def test_fresh_broker_flat_truth_demotes_lifecycle_only_residue(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(lifecycle_id="life_broker_flat_residue")
    _seed_base(tmp_path, lifecycle_positions=[lifecycle])
    _write_lifecycle_report(tmp_path, lifecycle_id="life_broker_flat_residue")
    _write_fresh_broker_positions_snapshot(tmp_path, positions=[])
    _write_fresh_broker_open_orders_snapshot(tmp_path, open_orders=[])

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == NO_MANAGED_POSITIONS
    assert payload["managed_positions"] == []
    demoted = payload["superseded_lifecycle_projections"][0]
    assert demoted["classification"] == "STALE_BROKER_FLAT_LIFECYCLE_DIAGNOSTIC"
    assert demoted["lifecycle_id"] == "life_broker_flat_residue"
    assert demoted["current_hot_path_scope"] == "HISTORICAL_BROKER_FLAT_DIAGNOSTIC_ONLY"


def test_broker_flat_lifecycle_residue_stays_visible_when_open_order_truth_incomplete(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(lifecycle_id="life_open_order_truth_unknown")
    _seed_base(tmp_path, lifecycle_positions=[lifecycle])
    _write_lifecycle_report(tmp_path, lifecycle_id="life_open_order_truth_unknown")
    _write_fresh_broker_positions_snapshot(tmp_path, positions=[])

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == LIFECYCLE_WITHOUT_BROKER
    assert payload["managed_positions"][0]["lifecycle_id"] == "life_open_order_truth_unknown"


def test_broker_flat_lifecycle_residue_stays_visible_with_same_contract_open_order(tmp_path: Path) -> None:
    lifecycle = _lifecycle_position(lifecycle_id="life_same_contract_order")
    _seed_base(tmp_path, lifecycle_positions=[lifecycle])
    _write_lifecycle_report(tmp_path, lifecycle_id="life_same_contract_order")
    _write_fresh_broker_positions_snapshot(tmp_path, positions=[])
    _write_fresh_broker_open_orders_snapshot(
        tmp_path,
        open_orders=[
            {
                "account_id": "DUM882026",
                "security_type": "FUT",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "expiry": "20260618",
                "action": "BUY",
                "quantity": "1",
                "status": "Submitted",
            }
        ],
    )

    payload = build_track_b_managed_position_registry(
        config=TrackBManagedPositionRegistryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == LIFECYCLE_WITHOUT_BROKER
    assert payload["managed_positions"][0]["lifecycle_id"] == "life_same_contract_order"



def test_current_truth_invalidation_marks_flat_managed_projection_historical() -> None:
    row = {
        "account_id": "DUM882026",
        "symbol": "ZN",
        "local_symbol": "ZNU6",
        "con_id": 800111222,
        "contract_key": "ZN-202609",
        "classification": OPEN_MANAGED_EXIT_DUE,
        "exit_due": True,
        "exit_due_state": "EXIT_DUE",
        "broker_position": {
            "account_id": "DUM882026",
            "symbol": "ZN",
            "local_symbol": "ZNU6",
            "con_id": 800111222,
            "quantity": "1",
        },
    }

    active, invalidated = managed_position_registry_module._active_managed_positions_from_current_position_truth(
        managed_positions=[row],
        broker_positions=[],
        positions_snapshot={
            "generated_at": NOW.isoformat(),
            "positions_complete": True,
            "ok": True,
            "selected_account_id": "DUM882026",
            "positions": [],
        },
        open_order_truth={
            "generated_at": NOW.isoformat(),
            "classification": "NO_OPEN_ORDERS",
            "canonical_refresh_scope": "GLOBAL_COMPLETE",
        },
        fresh_broker_positions_complete=True,
        now=NOW,
    )

    assert active == []
    assert invalidated[0]["historical_only"] is True
    assert invalidated[0]["diagnostic_only"] is True
    assert invalidated[0]["current_scope_active"] is False
    assert invalidated[0]["invalidated_by_current_truth"] is True
    assert invalidated[0]["exit_due"] is False
    assert invalidated[0]["exit_due_state"] == "INVALIDATED_BY_CURRENT_TRUTH"
    assert invalidated[0]["source_refs"]["broker_positions_complete"] is True


def test_current_truth_invalidation_keeps_current_nonzero_broker_position() -> None:
    broker = {
        "account_id": "DUM882026",
        "security_type": "FUT",
        "symbol": "ZN",
        "local_symbol": "ZNU6",
        "con_id": 800111222,
        "expiry": "20260921",
        "quantity": "1",
    }
    row = {
        "account_id": "DUM882026",
        "symbol": "ZN",
        "local_symbol": "ZNU6",
        "con_id": 800111222,
        "classification": OPEN_MANAGED_EXIT_DUE,
        "exit_due": True,
        "broker_position": broker,
    }
    normalized_broker = managed_position_registry_module._normalize_contract_row(broker)

    active, invalidated = managed_position_registry_module._active_managed_positions_from_current_position_truth(
        managed_positions=[row],
        broker_positions=[normalized_broker],
        positions_snapshot={
            "generated_at": NOW.isoformat(),
            "positions_complete": True,
            "ok": True,
            "selected_account_id": "DUM882026",
            "positions": [broker],
        },
        open_order_truth={"classification": "NO_OPEN_ORDERS", "canonical_refresh_scope": "GLOBAL_COMPLETE"},
        fresh_broker_positions_complete=True,
        now=NOW,
    )

    assert active == [row]
    assert invalidated == []


def test_current_truth_invalidation_keeps_row_when_broker_truth_incomplete() -> None:
    row = {
        "account_id": "DUM882026",
        "symbol": "ZN",
        "local_symbol": "ZNU6",
        "con_id": 800111222,
        "classification": OPEN_MANAGED_EXIT_DUE,
        "exit_due": True,
    }

    active, invalidated = managed_position_registry_module._active_managed_positions_from_current_position_truth(
        managed_positions=[row],
        broker_positions=[],
        positions_snapshot={"generated_at": NOW.isoformat(), "positions_complete": False, "ok": False},
        open_order_truth={"classification": "NO_OPEN_ORDERS", "canonical_refresh_scope": "GLOBAL_COMPLETE"},
        fresh_broker_positions_complete=False,
        now=NOW,
    )

    assert active == [row]
    assert invalidated == []


def test_current_truth_invalidation_keeps_row_on_account_mismatch() -> None:
    row = {
        "account_id": "OTHER_ACCOUNT",
        "symbol": "ZN",
        "local_symbol": "ZNU6",
        "con_id": 800111222,
        "classification": OPEN_MANAGED_EXIT_DUE,
        "exit_due": True,
    }

    active, invalidated = managed_position_registry_module._active_managed_positions_from_current_position_truth(
        managed_positions=[row],
        broker_positions=[],
        positions_snapshot={
            "generated_at": NOW.isoformat(),
            "positions_complete": True,
            "ok": True,
            "selected_account_id": "DUM882026",
            "positions": [],
        },
        open_order_truth={"classification": "NO_OPEN_ORDERS", "canonical_refresh_scope": "GLOBAL_COMPLETE"},
        fresh_broker_positions_complete=True,
        now=NOW,
    )

    assert active == [row]
    assert invalidated == []


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


def _write_fresh_broker_positions_snapshot(root: Path, *, positions: list[dict]) -> None:
    _write_json(
        root / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json",
        {
            "generated_at": NOW.isoformat(),
            "source": "IBKR_TWS_API_REQ_POSITIONS",
            "positions_complete": True,
            "ok": True,
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "positions": positions,
        },
    )


def _write_fresh_broker_open_orders_snapshot(root: Path, *, open_orders: list[dict]) -> None:
    _write_json(
        root / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json",
        {
            "generated_at": NOW.isoformat(),
            "source": "IBKR_TWS_API_REQ_ALL_OPEN_ORDERS",
            "open_orders_complete": True,
            "ok": True,
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "open_orders": open_orders,
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


def _broker_position_mes_long() -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": "MES",
        "track_b_root": "MES",
        "local_symbol": "MESU6",
        "con_id": 793356217,
        "expiry": "20260918",
        "quantity": "1.0",
        "average_cost": "37483.12",
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


def _write_live_entry_fill_event(
    root: Path,
    *,
    symbol: str,
    local_symbol: str,
    con_id: int,
    expiry: str,
    lifecycle_id: str,
    trade_id: str,
    lane_id: str,
    generated_at: datetime,
    append: bool = False,
) -> None:
    path = root / "outputs" / "track_b_execution_core" / "trade_registry" / "live_trade_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    event = TradeEvent(
        event_id=f"{trade_id}_entry_fill",
        event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
        generated_at=generated_at,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id=lane_id,
        thesis_strategy_id=lane_id,
        account_id="DUM882026",
        symbol=symbol,
        con_id=con_id,
        local_symbol=local_symbol,
        expiry=expiry,
        side="LONG",
        action="BUY",
        qty=Decimal("1"),
        source_artifact_path="outputs/track_b_execution_core/test_entry.json",
        order_id="1",
        client_id="10110",
        perm_id=f"perm_{symbol.lower()}",
        exec_id=f"exec_{symbol.lower()}",
        price=Decimal("100"),
    )
    mode = "a" if append and path.exists() else "w"
    with path.open(mode, encoding="utf-8") as handle:
        handle.write(json.dumps(event.to_dict(), sort_keys=True) + "\n")


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


def _write_entry_fill_event(
    root: Path,
    *,
    trade_id: str,
    lifecycle_id: str,
    lane_id: str,
    symbol: str,
    local_symbol: str,
    con_id: int,
    generated_at: datetime,
    side: str,
    action: str,
    order_id: str,
    perm_id: str,
    exec_id: str,
) -> None:
    path = root / "outputs" / "track_b_execution_core" / "trade_registry" / "live_trade_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    event = TradeEvent(
        event_id=f"{trade_id}_entry_fill",
        event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
        generated_at=generated_at,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id=lane_id,
        thesis_strategy_id=lane_id,
        account_id="DUM882026",
        symbol=symbol,
        con_id=con_id,
        local_symbol=local_symbol,
        expiry="20260918",
        side=side,
        action=action,
        qty=Decimal("1"),
        source_artifact_path="outputs/track_b_execution_core/test_entry.json",
        order_id=order_id,
        client_id="10110",
        perm_id=perm_id,
        exec_id=exec_id,
        price=Decimal("7496.5"),
    )
    with path.open("a", encoding="utf-8") as handle:
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


def _write_terminal_cleanup_trade_events(
    root: Path,
    *,
    trade_id: str,
    lifecycle_id: str,
    lane_id: str,
    local_symbol: str,
    con_id: int,
) -> None:
    path = root / "outputs" / "track_b_execution_core" / "trade_registry" / "live_trade_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    events = [
        TradeEvent(
            event_id=f"{trade_id}_entry_fill",
            event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
            generated_at=NOW,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            lane_id=lane_id,
            thesis_strategy_id=lane_id,
            account_id="DUM882026",
            symbol="MNQ",
            con_id=con_id,
            local_symbol=local_symbol,
            expiry="20260918",
            side="SHORT",
            action="SELL",
            qty=Decimal("1"),
            source_artifact_path="outputs/track_b_execution_core/test_entry.json",
            order_id="1",
            client_id="9864",
            perm_id="1793991637",
            exec_id="0000e1a7.6a430fc2.01.01",
            price=Decimal("30369.75"),
        ),
        TradeEvent(
            event_id=f"{trade_id}_open_managed",
            event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
            generated_at=NOW + timedelta(seconds=1),
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            lane_id=lane_id,
            thesis_strategy_id=lane_id,
            account_id="DUM882026",
            symbol="MNQ",
            con_id=con_id,
            local_symbol=local_symbol,
            expiry="20260918",
            side="SHORT",
            action="SELL",
            qty=Decimal("1"),
            source_artifact_path="outputs/track_b_execution_core/test_lifecycle.json",
        ),
        TradeEvent(
            event_id=f"{trade_id}_flat_cleanup",
            event_type=TradeEventType.RECONCILED_FLAT_HISTORICAL_CLEANUP,
            generated_at=NOW + timedelta(seconds=2),
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            lane_id=lane_id,
            thesis_strategy_id=lane_id,
            account_id="DUM882026",
            symbol="MNQ",
            con_id=con_id,
            local_symbol=local_symbol,
            expiry="20260918",
            side="SHORT",
            action="HISTORICAL_FLAT_CLEANUP",
            qty=Decimal("1"),
            source_artifact_path="outputs/track_b_execution_core/test_cleanup.json",
            reason_codes=(
                "BROKER_FLAT_PROOF_CONFIRMED",
                "NO_OPEN_ORDER_PROOF_CONFIRMED",
                "NOT_CURRENT_EXPOSURE",
                "NOT_CURRENT_OPEN_ORDER",
            ),
            metadata={
                "not_current_exposure": True,
                "not_current_open_order": True,
                "broker_flat_proof_path": "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json",
                "open_orders_proof_path": "outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json",
            },
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
