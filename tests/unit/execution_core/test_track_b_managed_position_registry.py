from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_managed_position_registry import (
    BROKER_BACKED_ADOPTION_REQUIRED,
    LIFECYCLE_WITHOUT_BROKER,
    MANAGED_POSITION_METADATA_INCOMPLETE,
    NO_MANAGED_POSITIONS,
    OPEN_MANAGED_CLOSE_WORKING,
    OPEN_MANAGED_EXIT_DUE,
    OPEN_MANAGED_MATCHED,
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
