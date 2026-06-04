from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEvent, TradeEventType, reduce_trade_events
from mgc_v05l.execution_core import track_b_current_exposure_owner_resolver as owner_resolver_module
from mgc_v05l.execution_core.track_b_current_exposure_owner_resolver import (
    AMBIGUOUS_EXPOSURE_OWNERSHIP,
    NO_OPEN_EXPOSURE,
    OWNED_MANAGED_EXPOSURE,
    OWNED_MANAGED_EXIT_DUE,
    UNMANAGED_BROKER_EXPOSURE,
    STALE_DUPLICATE_LIFECYCLE_AGGREGATION_FULL_AUDIT_ONLY,
    CurrentExposureOwnerResolverConfig,
    apply_current_exposure_owner_lifecycle_overlay,
    resolve_current_exposure_ownership,
)
from mgc_v05l.execution_core.track_b_fresh_truth_contract import EXPIRED_DIAGNOSTIC_ONLY


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
    assert payload["stale_superseded_full_audit_only"][0]["classification"] == EXPIRED_DIAGNOSTIC_ONLY
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


def test_stale_owner_candidate_does_not_create_ambiguity_against_fresh_exact_owner(tmp_path: Path) -> None:
    stale = _registry_record(
        trade_id="trade_stale_same_contract",
        lifecycle_id="life_stale_same_contract",
        generated_at=NOW - timedelta(hours=3),
        exit_due=True,
    )
    current = _registry_record(
        trade_id="trade_fresh_same_contract",
        lifecycle_id="life_fresh_same_contract",
        generated_at=NOW,
        exit_due=True,
    )

    payload = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=[_broker_position(quantity="-1")],
        registry_records=[stale, current],
        lifecycle_positions=[
            {
                "trade_id": "trade_stale_same_contract",
                "lifecycle_id": "life_stale_same_contract",
                "account_id": "DUM882026",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "aggregate_qty": "-1",
                "quantity": "1",
                "side": "SHORT",
            }
        ],
    )

    assert payload["classification"] == OWNED_MANAGED_EXIT_DUE
    assert payload["owned_exposures"][0]["trade_id"] == "trade_fresh_same_contract"
    assert not payload["review_required_exposures"]
    assert payload["stale_superseded_full_audit_only"][0]["classification"] == EXPIRED_DIAGNOSTIC_ONLY


def test_broker_flat_with_stale_open_rows_reports_no_open_exposure(tmp_path: Path) -> None:
    stale = _registry_record(trade_id="trade_stale", lifecycle_id="life_stale", generated_at=NOW, exit_due=True)

    payload = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=[],
        registry_records=[stale],
    )

    assert payload["classification"] == NO_OPEN_EXPOSURE
    assert payload["owned_exposure_count"] == 0


def test_broker_flat_without_supplied_records_skips_registry_load(monkeypatch, tmp_path: Path) -> None:
    def fail_registry_load(*args, **kwargs):
        raise AssertionError("flat no-exposure resolution must not reduce historical registry records")

    monkeypatch.setattr(owner_resolver_module, "load_live_trade_registry_records", fail_registry_load)

    payload = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=[],
        broker_open_orders=[],
    )

    assert payload["classification"] == NO_OPEN_EXPOSURE
    assert payload["bounded_current_scope_fast_path"]["used"] is True
    assert payload["bounded_current_scope_fast_path"]["skipped_full_registry_reduction"] is True


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


def test_current_owner_overlay_deduplicates_stale_same_contract_lifecycle_aggregate(tmp_path: Path) -> None:
    stale = _registry_record(
        trade_id="trade_old_mes_short",
        lifecycle_id="life_old_mes_short",
        generated_at=NOW - timedelta(days=1),
        exit_due=False,
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
    )
    current = _registry_record(
        trade_id="trade_current_mes_short",
        lifecycle_id="life_current_mes_short",
        generated_at=NOW,
        exit_due=False,
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
    )
    broker_position = _broker_position(quantity="-1")
    broker_position.update({"symbol": "MES", "track_b_root": "MES", "local_symbol": "MESM6", "con_id": 770561194})

    owner_resolution = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=[broker_position],
        registry_records=[stale, current],
        lifecycle_positions=[
            {
                "account_id": "MULTIPLE",
                "local_symbol": "MESM6",
                "con_id": 770561194,
                "aggregate_qty": "-2",
                "quantity": "2",
                "side": "SHORT",
                "lifecycle_id": "life_old_mes_short",
                "lifecycle_ids": ["life_old_mes_short", "life_current_mes_short"],
                "trade_ids": ["trade_old_mes_short", "trade_current_mes_short"],
                "lifecycle_units": [
                    {"lifecycle_id": "life_old_mes_short", "signed_qty": "-1"},
                    {"lifecycle_id": "life_current_mes_short", "signed_qty": "-1"},
                ],
            }
        ],
    )

    current_scope, superseded = apply_current_exposure_owner_lifecycle_overlay(
        lifecycle_positions=[
            {
                "account_id": "MULTIPLE",
                "local_symbol": "MESM6",
                "con_id": 770561194,
                "aggregate_qty": "-2",
                "quantity": "2",
                "side": "SHORT",
                "lifecycle_id": "life_old_mes_short",
                "lifecycle_ids": ["life_old_mes_short", "life_current_mes_short"],
            }
        ],
        owner_resolution=owner_resolution,
    )

    assert owner_resolution["classification"] == OWNED_MANAGED_EXPOSURE
    assert current_scope == [owner_resolution["owned_exposures"][0]["lifecycle_position"]]
    assert current_scope[0]["trade_id"] == "trade_current_mes_short"
    assert current_scope[0]["aggregate_qty"] == "-1"
    assert current_scope[0]["lifecycle_unit_count"] == 1
    assert superseded[0]["classification"] == STALE_DUPLICATE_LIFECYCLE_AGGREGATION_FULL_AUDIT_ONLY
    assert superseded[0]["raw_lifecycle_position"]["aggregate_qty"] == "-2"


def test_fresh_submit_owner_with_exact_lifecycle_fill_beats_stale_same_contract_owner(tmp_path: Path) -> None:
    stale = _registry_record(
        trade_id="trade_submit_owner_mes_globex_short",
        lifecycle_id="reserved_submit_mes_globex_active_participation_short_1",
        generated_at=NOW - timedelta(days=1),
        exit_due=False,
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
    )
    current = _submit_owner_record(
        trade_id="trade_6b84a270-86cc-4a30-bbe5-90c0de1040a0",
        lifecycle_id="reserved_submit_mes_us_active_participation_short_20260603T175011604765Z_1528ebb927d8",
        generated_at=NOW,
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        order_id="2",
        client_id="11011",
        perm_id="1421892956",
    )
    broker_position = _broker_position(quantity="-1")
    broker_position.update({"symbol": "MES", "track_b_root": "MES", "local_symbol": "MESM6", "con_id": 770561194})

    report = _lifecycle_report(
        trade_id="trade_bridge_fill_MES_1m_2026-06-03T17_49_00Z_SELL_TO_OPEN",
        lifecycle_id="bridge_fill_MES|1m|2026-06-03T17:49:00Z|SELL_TO_OPEN",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        order_id="2",
        perm_id="1421892956",
        exec_id="0000e1a7.6a316b2f.01.01",
        filled_at=NOW + timedelta(seconds=2),
        lane_id="mes_us_active_participation_short",
    )
    report_path = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "track_b_strategy_managed_paper_lifecycle"
        / "bridge_fill_MES|1m|2026-06-03T17:49:00Z|SELL_TO_OPEN"
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report), encoding="utf-8")

    payload = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=[broker_position],
        registry_records=[stale, current],
    )

    assert payload["classification"] == OWNED_MANAGED_EXPOSURE
    exposure = payload["owned_exposures"][0]
    assert exposure["trade_id"] == "trade_6b84a270-86cc-4a30-bbe5-90c0de1040a0"
    assert exposure["lifecycle_id"] == "reserved_submit_mes_us_active_participation_short_20260603T175011604765Z_1528ebb927d8"
    assert exposure["lifecycle_position"]["entry_exec_ids"] == ["0000e1a7.6a316b2f.01.01"]
    assert exposure["lifecycle_position"]["managed_exit_policy_id"] == "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    assert payload["stale_superseded_full_audit_only"][0]["trade_id"] == "trade_submit_owner_mes_globex_short"


def _registry_record(
    *,
    trade_id: str,
    lifecycle_id: str,
    generated_at: datetime,
    exit_due: bool,
    symbol: str = "MNQ",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
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
            symbol=symbol,
            local_symbol=local_symbol,
            con_id=con_id,
        ),
        _event(
            event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            generated_at=generated_at + timedelta(seconds=1),
            symbol=symbol,
            local_symbol=local_symbol,
            con_id=con_id,
        ),
    ]
    if exit_due:
        events.append(
            _event(
                event_type=TradeEventType.EXIT_INTENT_CREATED,
                trade_id=trade_id,
                lifecycle_id=lifecycle_id,
                generated_at=generated_at + timedelta(minutes=60),
                symbol=symbol,
                local_symbol=local_symbol,
                con_id=con_id,
            )
        )
    return reduce_trade_events(events)


def _submit_owner_record(
    *,
    trade_id: str,
    lifecycle_id: str,
    generated_at: datetime,
    symbol: str,
    local_symbol: str,
    con_id: int,
    order_id: str,
    client_id: str,
    perm_id: str,
):
    events = [
        _event(
            event_type=TradeEventType.ENTRY_INTENT_CREATED,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            generated_at=generated_at,
            order_id=None,
            client_id=client_id,
            perm_id=None,
            exec_id=None,
            symbol=symbol,
            local_symbol=local_symbol,
            con_id=con_id,
        ),
        _event(
            event_type=TradeEventType.ENTRY_ORDER_SUBMITTED,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            generated_at=generated_at + timedelta(seconds=1),
            order_id=order_id,
            client_id=client_id,
            perm_id=perm_id,
            exec_id=None,
            symbol=symbol,
            local_symbol=local_symbol,
            con_id=con_id,
        ),
        _event(
            event_type=TradeEventType.REVIEW_REQUIRED,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            generated_at=generated_at + timedelta(seconds=2),
            order_id=order_id,
            client_id=client_id,
            perm_id=perm_id,
            exec_id=None,
            symbol=symbol,
            local_symbol=local_symbol,
            con_id=con_id,
        ),
    ]
    return reduce_trade_events(events)


def _lifecycle_report(
    *,
    trade_id: str,
    lifecycle_id: str,
    symbol: str,
    local_symbol: str,
    con_id: int,
    order_id: str,
    perm_id: str,
    exec_id: str,
    filled_at: datetime,
    lane_id: str,
) -> dict:
    return {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "account_id": "DUM882026",
        "instrument_family": symbol,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "lane_id": lane_id,
        "strategy_id": lane_id,
        "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        "entry_fill": {
            "broker_order_id": order_id,
            "perm_id": perm_id,
            "execution_id": exec_id,
            "filled_at": filled_at.isoformat(),
            "price": "7579.75",
            "quantity": "1",
        },
    }


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
    symbol: str = "MNQ",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
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
        symbol=symbol,
        con_id=con_id,
        local_symbol=local_symbol,
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
