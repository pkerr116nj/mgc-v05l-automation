from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_managed_order_registry import (
    BROKER_FLAT_WITH_WORKING_CLOSE,
    CLOSE_ORDER_MODIFIABLE,
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
) -> None:
    order_states = order_states or []
    positions_without_close = positions_without_close or []
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
            "classification": "NO_MANAGED_POSITIONS",
            "managed_positions": [],
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


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
