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


def test_current_registry_owner_without_lifecycle_report_carries_fill_identity_and_policy(tmp_path: Path) -> None:
    stale = _registry_record(
        trade_id="trade_old_mes_london_short",
        lifecycle_id="life_old_mes_london_short",
        generated_at=NOW - timedelta(days=2),
        exit_due=False,
        symbol="MES",
        local_symbol="MESU6",
        con_id=793356224,
        lane_id="mes_london_open_active_participation_short",
    )
    current = _registry_record(
        trade_id="trade_current_mes_globex_short",
        lifecycle_id="reserved_submit_mes_globex_active_participation_short_20260614T224221875069Z_d054ab235060",
        generated_at=NOW,
        exit_due=False,
        symbol="MES",
        local_symbol="MESU6",
        con_id=793356224,
        lane_id="mes_globex_active_participation_short",
        managed_exit_policy_id=None,
    )
    broker_position = _broker_position(quantity="-1")
    broker_position.update({"symbol": "MES", "track_b_root": "MES", "local_symbol": "MESU6", "con_id": 793356224})
    _write_lifecycle_report(
        tmp_path,
        "life_old_mes_london_short",
        _lifecycle_report(
            trade_id=stale.trade_id,
            lifecycle_id="life_old_mes_london_short",
            symbol="MES",
            local_symbol="MESU6",
            con_id=793356224,
            order_id="1",
            perm_id="old_perm",
            exec_id="old_exec",
            filled_at=NOW - timedelta(days=2),
            lane_id="mes_london_open_active_participation_short",
        ),
    )

    owner_resolution = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=[broker_position],
        registry_records=[stale, current],
        lifecycle_positions=[
            {
                "trade_id": stale.trade_id,
                "lifecycle_id": "life_old_mes_london_short",
                "lane_id": "mes_london_open_active_participation_short",
                "account_id": "DUM882026",
                "local_symbol": "MESU6",
                "con_id": 793356224,
                "aggregate_qty": "-1",
                "quantity": "1",
                "side": "SHORT",
                "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
            }
        ],
    )

    assert owner_resolution["classification"] == OWNED_MANAGED_EXPOSURE
    exposure = owner_resolution["owned_exposures"][0]
    assert exposure["trade_id"] == "trade_current_mes_globex_short"
    assert set(exposure["reason_codes"]) & {
        "NEWEST_EXACT_BROKER_BACKED_ENTRY_SELECTED",
        "REGISTRY_OPEN_MANAGED_MATCHED_BROKER_POSITION",
    }
    lifecycle = exposure["lifecycle_position"]
    assert lifecycle["lifecycle_id"] == current.ownership_identity.lifecycle_id
    assert lifecycle["lane_id"] == "mes_globex_active_participation_short"
    assert lifecycle["entry_timestamp"] == NOW.isoformat()
    assert lifecycle["entry_order_ids"] == ["2"]
    assert lifecycle["entry_perm_ids"] == ["perm_trade_current_mes_globex_short"]
    assert lifecycle["entry_exec_ids"] == ["exec_trade_current_mes_globex_short"]
    assert lifecycle["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"

    current_scope, superseded = apply_current_exposure_owner_lifecycle_overlay(
        lifecycle_positions=[
            {
                "trade_id": stale.trade_id,
                "lifecycle_id": "life_old_mes_london_short",
                "lane_id": "mes_london_open_active_participation_short",
                "account_id": "DUM882026",
                "local_symbol": "MESU6",
                "con_id": 793356224,
                "aggregate_qty": "-1",
                "quantity": "1",
                "side": "SHORT",
            }
        ],
        owner_resolution=owner_resolution,
    )

    assert current_scope == [lifecycle]
    assert superseded[0]["owner_lifecycle_id"] == current.ownership_identity.lifecycle_id


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


def test_same_fill_duplicate_lifecycle_report_owners_collapse_to_one_owner(tmp_path: Path) -> None:
    trade_id = "trade_current_mnq"
    lifecycle_id = "reserved_submit_mnq_us_active_participation_long_20260604T144415893875Z_89996348b109"
    current = _submit_owner_record(
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        generated_at=NOW,
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        order_id="1",
        client_id="10887",
        perm_id="1092553520",
    )
    filled_at = NOW + timedelta(seconds=2)
    _write_lifecycle_report(
        tmp_path,
        "bridge_fill_MNQ|1m|2026-06-04T14:43:00Z|BUY_TO_OPEN",
        _lifecycle_report(
            trade_id=trade_id,
            lifecycle_id="bridge_fill_MNQ|1m|2026-06-04T14:43:00Z|BUY_TO_OPEN",
            symbol="MNQ",
            local_symbol="MNQM6",
            con_id=770561201,
            order_id="1",
            perm_id="1092553520",
            exec_id="0000e1a7.6a3488ea.01.01",
            filled_at=filled_at,
            lane_id="mnq_us_active_participation_long",
        ),
    )
    _write_lifecycle_report(
        tmp_path,
        lifecycle_id,
        _lifecycle_report(
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            symbol="MNQ",
            local_symbol="MNQM6",
            con_id=770561201,
            order_id="1",
            perm_id="1092553520",
            exec_id="0000e1a7.6a3488ea.01.01",
            filled_at=filled_at,
            lane_id="mnq_us_active_participation_long",
        ),
    )

    payload = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=[_broker_position(quantity="1")],
        registry_records=[current],
    )

    assert payload["classification"] == OWNED_MANAGED_EXPOSURE
    assert payload["owned_exposure_count"] == 1
    assert payload["review_required_exposure_count"] == 0
    exposure = payload["owned_exposures"][0]
    assert exposure["trade_id"] == trade_id
    assert exposure["lifecycle_id"] == lifecycle_id
    diagnostic = payload["stale_superseded_full_audit_only"]
    assert any(
        "DUPLICATE_EXACT_BROKER_FILL_LIFECYCLE_REPORT_OWNER_COLLAPSED" in row.get("reason_codes", [])
        for row in diagnostic
    )


def test_conflicting_same_timestamp_lifecycle_report_owners_still_fail_closed(tmp_path: Path) -> None:
    first = _submit_owner_record(
        trade_id="trade_current_mnq_one",
        lifecycle_id="life_current_mnq_one",
        generated_at=NOW,
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        order_id="1",
        client_id="10887",
        perm_id="1092553520",
    )
    second = _submit_owner_record(
        trade_id="trade_current_mnq_two",
        lifecycle_id="life_current_mnq_two",
        generated_at=NOW,
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        order_id="2",
        client_id="10887",
        perm_id="1092553521",
    )
    filled_at = NOW + timedelta(seconds=2)
    _write_lifecycle_report(
        tmp_path,
        "life_current_mnq_one",
        _lifecycle_report(
            trade_id="trade_current_mnq_one",
            lifecycle_id="life_current_mnq_one",
            symbol="MNQ",
            local_symbol="MNQM6",
            con_id=770561201,
            order_id="1",
            perm_id="1092553520",
            exec_id="exec_one",
            filled_at=filled_at,
            lane_id="mnq_us_active_participation_long",
        ),
    )
    _write_lifecycle_report(
        tmp_path,
        "life_current_mnq_two",
        _lifecycle_report(
            trade_id="trade_current_mnq_two",
            lifecycle_id="life_current_mnq_two",
            symbol="MNQ",
            local_symbol="MNQM6",
            con_id=770561201,
            order_id="2",
            perm_id="1092553521",
            exec_id="exec_two",
            filled_at=filled_at,
            lane_id="mnq_us_active_participation_long",
        ),
    )

    payload = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=[_broker_position(quantity="1")],
        registry_records=[first, second],
    )

    assert payload["classification"] == AMBIGUOUS_EXPOSURE_OWNERSHIP
    assert payload["owned_exposure_count"] == 0
    assert payload["review_required_exposures"][0]["reason_codes"] == [
        "MULTIPLE_EXACT_BROKER_BACKED_LIFECYCLE_REPORT_OWNERS"
    ]


def test_distinct_mnq_mes_current_positions_resolve_as_separate_owned_exposures(tmp_path: Path) -> None:
    mnq = _registry_record(
        trade_id="trade_current_mnq",
        lifecycle_id="life_current_mnq",
        generated_at=NOW,
        exit_due=False,
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
    )
    mes = _registry_record(
        trade_id="trade_current_mes",
        lifecycle_id="life_current_mes",
        generated_at=NOW,
        exit_due=False,
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
    )
    mes_position = _broker_position(quantity="-1")
    mes_position.update({"symbol": "MES", "track_b_root": "MES", "local_symbol": "MESM6", "con_id": 770561194})

    payload = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=[_broker_position(quantity="-1"), mes_position],
        registry_records=[mnq, mes],
    )

    assert payload["classification"] == OWNED_MANAGED_EXPOSURE
    assert payload["owned_exposure_count"] == 2
    assert payload["review_required_exposure_count"] == 0
    assert {row["trade_id"] for row in payload["owned_exposures"]} == {
        "trade_current_mnq",
        "trade_current_mes",
    }


def test_validated_futures_positions_without_broker_con_id_resolve_current_owners(tmp_path: Path) -> None:
    contracts = (
        ("MGC", "MGCQ6", "20260827", 732156883),
        ("GC", "GCQ6", "20260827", 732156872),
        ("NQ", "NQU6", "20260918", 770561204),
        ("ES", "ESU6", "20260918", 649180671),
        ("MNQ", "MNQU6", "20260918", 793356225),
        ("MES", "MESU6", "20260918", 793356217),
    )
    records = [
        _registry_record(
            trade_id=f"trade_current_{symbol.lower()}",
            lifecycle_id=f"reserved_submit_{symbol.lower()}_globex_active_participation_long",
            generated_at=NOW,
            exit_due=False,
            symbol=symbol,
            local_symbol=local_symbol,
            con_id=con_id,
            lane_id=f"{symbol.lower()}_globex_active_participation_long",
            managed_exit_policy_id=None if symbol == "ES" else "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
            side="LONG",
            action="BUY",
        )
        for symbol, local_symbol, _expiry, con_id in contracts
    ]
    broker_positions = [
        {
            "account_id": "DUM882026",
            "security_type": "FUT",
            "symbol": symbol,
            "local_symbol": local_symbol,
            "expiry": expiry,
            "quantity": "1",
        }
        for symbol, local_symbol, expiry, _con_id in contracts
    ]

    payload = resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=tmp_path),
        broker_positions=broker_positions,
        registry_records=records,
    )

    assert payload["classification"] == OWNED_MANAGED_EXPOSURE
    assert payload["owned_exposure_count"] == 6
    assert payload["review_required_exposure_count"] == 0
    exposures = {row["canonical_broker_position"]["symbol"]: row for row in payload["owned_exposures"]}
    assert set(exposures) == {symbol for symbol, *_rest in contracts}
    for symbol, local_symbol, expiry, con_id in contracts:
        exposure = exposures[symbol]
        assert exposure["canonical_identity_resolution"]["source"] == "VALIDATED_TRACK_B_FUTURES_CONTRACT_REGISTRY"
        assert exposure["canonical_broker_position"]["con_id"] == con_id
        assert exposure["canonical_broker_position"]["local_symbol"] == local_symbol
        assert exposure["canonical_broker_position"]["expiry"] == expiry
        assert exposure["lifecycle_position"]["local_symbol"] == local_symbol
        assert exposure["lifecycle_position"]["con_id"] == con_id
        assert exposure["lifecycle_position"]["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"


def _registry_record(
    *,
    trade_id: str,
    lifecycle_id: str,
    generated_at: datetime,
    exit_due: bool,
    symbol: str = "MNQ",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
    lane_id: str = "mnq_globex_active_participation_short",
    managed_exit_policy_id: str | None = "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
    side: str = "SHORT",
    action: str = "SELL",
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
            lane_id=lane_id,
            managed_exit_policy_id=managed_exit_policy_id,
            side=side,
            action=action,
        ),
        _event(
            event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            generated_at=generated_at + timedelta(seconds=1),
            symbol=symbol,
            local_symbol=local_symbol,
            con_id=con_id,
            lane_id=lane_id,
            managed_exit_policy_id=managed_exit_policy_id,
            side=side,
            action=action,
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
                    lane_id=lane_id,
                    managed_exit_policy_id=managed_exit_policy_id,
                    side=side,
                    action=action,
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


def _write_lifecycle_report(root: Path, lifecycle_id: str, payload: dict) -> None:
    report_path = (
        root
        / "outputs"
        / "track_b_execution_core"
        / "track_b_strategy_managed_paper_lifecycle"
        / lifecycle_id
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload), encoding="utf-8")


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
    lane_id: str = "mnq_globex_active_participation_short",
    managed_exit_policy_id: str | None = "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
    side: str = "SHORT",
    action: str = "SELL",
) -> TradeEvent:
    return TradeEvent(
        event_id=f"{trade_id}_{event_type.value}_{generated_at.timestamp()}",
        event_type=event_type,
        generated_at=generated_at,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id=lane_id,
        thesis_strategy_id=lane_id,
        account_id="DUM882026",
        symbol=symbol,
        con_id=con_id,
        local_symbol=local_symbol,
        expiry="20260618",
        side=side,
        action=action,
        qty=Decimal("1"),
        source_artifact_path="outputs/test.json",
        order_id=order_id,
        client_id=client_id,
        perm_id=perm_id,
        exec_id=exec_id,
        price=Decimal("30675"),
        metadata={"managed_exit_policy_id": managed_exit_policy_id} if managed_exit_policy_id else {},
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
