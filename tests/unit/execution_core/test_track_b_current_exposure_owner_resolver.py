from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEvent, TradeEventType, reduce_trade_events
from mgc_v05l.execution_core.track_b_current_exposure_owner_resolver import (
    AMBIGUOUS_EXPOSURE_OWNERSHIP,
    NO_OPEN_EXPOSURE,
    OWNED_MANAGED_EXIT_DUE,
    UNMANAGED_BROKER_EXPOSURE,
    CurrentExposureOwnerResolverConfig,
    resolve_current_exposure_ownership,
)


NOW = datetime(2026, 6, 3, 2, 49, tzinfo=UTC)


def test_current_mnq_regression_new_exact_owner_beats_stale_submit_owner(tmp_path: Path) -> None:
    stale = _registry_record(
        trade_id="trade_submit_owner_mnq_globex_short",
        lifecycle_id="reserved_submit_mnq_globex_active_participation_short_1",
        generated_at=NOW - timedelta(days=2),
        exit_due=False,
    )
    current = _registry_record(
        trade_id="trade_4636ee35-f089-47dd-be5b-dae468ee3228",
        lifecycle_id="reserved_submit_mnq_globex_active_participation_short_20260603T024821054865Z_6e2aaf2cc5da",
        generated_at=NOW,
        exit_due=True,
    )

    payload = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=[_broker_position(quantity="-1")],
        broker_open_orders=[],
        registry_records=[stale, current],
        lifecycle_positions=[],
    )

    assert payload["classification"] == OWNED_MANAGED_EXIT_DUE
    assert payload["owned_exposures"][0]["trade_id"] == "trade_4636ee35-f089-47dd-be5b-dae468ee3228"
    assert payload["owned_exposures"][0]["exit_due"] is True
    assert payload["stale_superseded_full_audit_only"][0]["trade_id"] == "trade_submit_owner_mnq_globex_short"


def test_true_unmanaged_exposure_remains_review_required(tmp_path: Path) -> None:
    payload = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=[_broker_position(quantity="-1")],
        registry_records=[],
    )

    assert payload["classification"] == UNMANAGED_BROKER_EXPOSURE
    assert payload["review_required_exposures"][0]["classification"] == UNMANAGED_BROKER_EXPOSURE


def test_two_equally_plausible_current_owners_fail_closed(tmp_path: Path) -> None:
    first = _registry_record(trade_id="trade_one", lifecycle_id="life_one", generated_at=NOW, exit_due=True)
    second = _registry_record(trade_id="trade_two", lifecycle_id="life_two", generated_at=NOW, exit_due=True)

    payload = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=[_broker_position(quantity="-1")],
        registry_records=[first, second],
    )

    assert payload["classification"] == AMBIGUOUS_EXPOSURE_OWNERSHIP
    assert payload["review_required_exposures"][0]["matching_trade_ids"] == ["trade_one", "trade_two"]


def test_broker_flat_with_stale_open_rows_reports_no_open_exposure(tmp_path: Path) -> None:
    stale = _registry_record(trade_id="trade_stale", lifecycle_id="life_stale", generated_at=NOW, exit_due=True)

    payload = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=[],
        registry_records=[stale],
    )

    assert payload["classification"] == NO_OPEN_EXPOSURE
    assert payload["owned_exposure_count"] == 0


def test_open_order_is_reported_and_prevents_restart_consumers_from_allowing_start(tmp_path: Path) -> None:
    current = _registry_record(trade_id="trade_current", lifecycle_id="life_current", generated_at=NOW, exit_due=True)

    payload = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=[_broker_position(quantity="-1")],
        broker_open_orders=[{"account_id": "DUM882026", "local_symbol": "MNQM6", "con_id": 770561201}],
        registry_records=[current],
    )

    assert payload["classification"] == OWNED_MANAGED_EXIT_DUE
    assert payload["broker_open_order_count"] == 1


def _registry_record(
    *,
    trade_id: str,
    lifecycle_id: str,
    generated_at: datetime,
    exit_due: bool,
):
    events = [
        _event(
            event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            generated_at=generated_at,
            order_id="2",
            client_id="10898",
            perm_id=f"perm_{trade_id}",
            exec_id=f"exec_{trade_id}",
        ),
        _event(
            event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            generated_at=generated_at + timedelta(seconds=1),
        ),
    ]
    if exit_due:
        events.append(
            _event(
                event_type=TradeEventType.EXIT_INTENT_CREATED,
                trade_id=trade_id,
                lifecycle_id=lifecycle_id,
                generated_at=generated_at + timedelta(minutes=60),
            )
        )
    return reduce_trade_events(events)


def _event(
    *,
    event_type: TradeEventType,
    trade_id: str,
    lifecycle_id: str,
    generated_at: datetime,
    order_id: str | None = None,
    client_id: str | None = None,
    perm_id: str | None = None,
    exec_id: str | None = None,
) -> TradeEvent:
    return TradeEvent(
        event_id=f"{trade_id}_{event_type.value}_{generated_at.timestamp()}",
        event_type=event_type,
        generated_at=generated_at,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mnq_globex_active_participation_short",
        thesis_strategy_id="mnq_globex_active_participation_short",
        account_id="DUM882026",
        symbol="MNQ",
        con_id=770561201,
        local_symbol="MNQM6",
        expiry="20260618",
        side="SHORT",
        action="SELL",
        qty=Decimal("1"),
        source_artifact_path="outputs/test.json",
        order_id=order_id,
        client_id=client_id,
        perm_id=perm_id,
        exec_id=exec_id,
        price=Decimal("30675"),
        metadata={"managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"},
    )


def _broker_position(*, quantity: str) -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "track_b_root": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "expiry": "20260618",
        "quantity": quantity,
    }
