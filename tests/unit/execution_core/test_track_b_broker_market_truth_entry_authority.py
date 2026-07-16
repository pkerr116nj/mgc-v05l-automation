from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_current_state_authority import EXIT_CAPABILITY_APPLY_BLOCKED
from mgc_v05l.execution_core.track_b_broker_market_truth_entry_authority import (
    BROKER_MARKET_TRUTH_ENTRY_ALLOWED,
    BROKER_MARKET_TRUTH_ENTRY_BLOCKED,
    BrokerMarketTruthEntryAuthorityInput,
    build_broker_market_truth_entry_authority_from_repo,
    evaluate_broker_market_truth_entry_authority,
)


ACTIVE_LANES = (
    "mnq_us_active_participation_long",
    "mnq_us_active_participation_short",
    "mes_us_active_participation_long",
    "mes_us_active_participation_short",
    "mnq_globex_active_participation_long",
    "mnq_globex_active_participation_short",
    "mes_globex_active_participation_long",
    "mes_globex_active_participation_short",
    "mnq_london_open_active_participation_long",
    "mnq_london_open_active_participation_short",
    "mes_london_open_active_participation_long",
    "mes_london_open_active_participation_short",
    "mnq_london_late_active_participation_short",
    "mes_london_late_active_participation_short",
)


def _positions(*, rows: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "ok": True,
        "account": "DUM882026",
        "positions": list(rows or []),
    }


def _orders(*, rows: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "ok": True,
        "account": "DUM882026",
        "open_orders_complete": True,
        "open_orders": list(rows or []),
    }


def _managed_exit_status(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "pid": 12345,
        "mode": "GUARDED_CLOSE_ONLY_APPLY",
        "classification": "NO_ELIGIBLE_EXITS",
        "generated_at": "2026-06-11T14:00:30+00:00",
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    payload.update(overrides)
    return payload


def _input(**overrides: object) -> BrokerMarketTruthEntryAuthorityInput:
    payload = {
        "account_id": "DUM882026",
        "mode": "PAPER",
        "route_destination": "ibkr_paper_bridge_submit_capable",
        "execution_mode": "IBKR_PAPER_BRIDGE",
        "lane_id": "mnq_us_active_participation_long",
        "instrument": "MNQ",
        "action": "BUY",
        "quantity": 1.0,
        "active_profile_lane_ids": ACTIVE_LANES,
        "broker_positions_snapshot": _positions(),
        "broker_open_orders_snapshot": _orders(),
        "open_order_truth": {"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
        "runtime_price": {
            "price": 29500.25,
            "timestamp": "2026-06-11T14:00:00+00:00",
        },
        "contract": {
            "symbol": "MNQ",
            "contract_month": "202609",
            "expiry": "20260918",
            "local_symbol": "MNQU6",
            "con_id": 793356225,
        },
        "managed_exit_status": _managed_exit_status(),
        "now": datetime(2026, 6, 11, 14, 1, tzinfo=timezone.utc),
    }
    payload.update(overrides)
    return BrokerMarketTruthEntryAuthorityInput(**payload)


def _classification(**overrides: object) -> tuple[str, list[str]]:
    result = evaluate_broker_market_truth_entry_authority(_input(**overrides))
    return str(result["classification"]), list(result["block_reasons"])


def test_all_active_profile_lanes_are_allowed_with_clean_broker_and_market_truth() -> None:
    for lane_id in ACTIVE_LANES:
        instrument = "MES" if lane_id.startswith("mes_") else "MNQ"
        local_symbol = "MESU6" if instrument == "MES" else "MNQU6"
        con_id = 793356194 if instrument == "MES" else 793356225
        result = evaluate_broker_market_truth_entry_authority(
            _input(
                lane_id=lane_id,
                instrument=instrument,
                contract={
                    "symbol": instrument,
                    "contract_month": "202609",
                    "expiry": "20260918",
                    "local_symbol": local_symbol,
                    "con_id": con_id,
                },
            )
        )

        assert result["classification"] == BROKER_MARKET_TRUTH_ENTRY_ALLOWED
        assert result["block_reasons"] == []


def test_diagnostic_subsystem_disagreements_do_not_block_clean_broker_market_truth() -> None:
    result = evaluate_broker_market_truth_entry_authority(
        _input(
            diagnostics={
                "cached_governance": {"submit_allowed": False, "block_reasons": ["lane_not_yet_submit_ported"]},
                "reconciliation": {"classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT"},
                "registry": {"classification": "CURRENT_HOT_PATH_EXPOSURE_BLOCKED"},
            }
        )
    )

    assert result["classification"] == BROKER_MARKET_TRUTH_ENTRY_ALLOWED
    assert result["diagnostics"]["cached_governance"]["submit_allowed"] is False


def test_broker_nonflat_flat_start_blocks() -> None:
    classification, reasons = _classification(
        broker_positions_snapshot=_positions(
            rows=[
                {
                    "account_id": "DUM882026",
                    "security_type": "FUT",
                    "symbol": "MNQ",
                    "local_symbol": "MNQU6",
                    "quantity": "-1",
                }
            ]
        )
    )

    assert classification == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert "broker_nonflat_flat_start_violation" in reasons


def test_mes_position_blocks_mes_but_allows_mnq_flat_start() -> None:
    mes_position = _positions(
        rows=[
            {
                "account_id": "DUM882026",
                "security_type": "FUT",
                "symbol": "MES",
                "local_symbol": "MESU6",
                "quantity": "-1",
            }
        ]
    )

    mnq_result = evaluate_broker_market_truth_entry_authority(
        _input(broker_positions_snapshot=mes_position)
    )
    mes_result = evaluate_broker_market_truth_entry_authority(
        _input(
            lane_id="mes_globex_active_participation_short",
            instrument="MES",
            broker_positions_snapshot=mes_position,
            runtime_price={
                "price": 7400.25,
                "timestamp": "2026-06-11T14:00:00+00:00",
            },
            contract={
                "symbol": "MES",
                "contract_month": "202609",
                "expiry": "20260918",
                "local_symbol": "MESU6",
                "con_id": 793356217,
            },
        )
    )

    assert mnq_result["classification"] == BROKER_MARKET_TRUTH_ENTRY_ALLOWED
    assert mnq_result["broker_truth"]["instrument_nonflat_position_count"] == 0
    assert mes_result["classification"] == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert "broker_nonflat_flat_start_violation" in mes_result["block_reasons"]


def test_mnq_position_blocks_mnq_but_allows_mes_flat_start() -> None:
    mnq_position = _positions(
        rows=[
            {
                "account_id": "DUM882026",
                "security_type": "FUT",
                "symbol": "MNQ",
                "local_symbol": "MNQU6",
                "quantity": "-1",
            }
        ]
    )

    mnq_result = evaluate_broker_market_truth_entry_authority(
        _input(broker_positions_snapshot=mnq_position)
    )
    mes_result = evaluate_broker_market_truth_entry_authority(
        _input(
            lane_id="mes_globex_active_participation_short",
            instrument="MES",
            broker_positions_snapshot=mnq_position,
            runtime_price={
                "price": 7400.25,
                "timestamp": "2026-06-11T14:00:00+00:00",
            },
            contract={
                "symbol": "MES",
                "contract_month": "202609",
                "expiry": "20260918",
                "local_symbol": "MESU6",
                "con_id": 793356217,
            },
        )
    )

    assert mnq_result["classification"] == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert "broker_nonflat_flat_start_violation" in mnq_result["block_reasons"]
    assert mes_result["classification"] == BROKER_MARKET_TRUTH_ENTRY_ALLOWED
    assert mes_result["broker_truth"]["track_b_nonflat_position_count"] == 1
    assert mes_result["broker_truth"]["instrument_nonflat_position_count"] == 0


def test_mbt_working_close_blocks_mbt_but_not_unrelated_symbol_entry() -> None:
    mbt_working_close = _orders(
        rows=[
            {
                "account_id": "DUM882026",
                "security_type": "FUT",
                "symbol": "MBT",
                "local_symbol": "MBTU6",
                "order_id": "389",
                "action": "BUY",
                "quantity": "1",
                "status": "Submitted",
            }
        ]
    )
    crypto_lanes = (
        "mbt_us_active_participation_short",
        "met_us_active_participation_long",
    )

    mbt_result = evaluate_broker_market_truth_entry_authority(
        _input(
            lane_id="mbt_us_active_participation_short",
            instrument="MBT",
            action="SELL_TO_OPEN",
            active_profile_lane_ids=crypto_lanes,
            broker_open_orders_snapshot=mbt_working_close,
            runtime_price={
                "price": 61000.0,
                "timestamp": "2026-06-11T14:00:00+00:00",
            },
            contract={
                "symbol": "MBT",
                "contract_month": "202609",
                "expiry": "20260925",
                "local_symbol": "MBTU6",
                "con_id": 772435596,
            },
        )
    )
    met_result = evaluate_broker_market_truth_entry_authority(
        _input(
            lane_id="met_us_active_participation_long",
            instrument="MET",
            action="BUY_TO_OPEN",
            active_profile_lane_ids=crypto_lanes,
            broker_open_orders_snapshot=mbt_working_close,
            runtime_price={
                "price": 160.0,
                "timestamp": "2026-06-11T14:00:00+00:00",
            },
            contract={
                "symbol": "MET",
                "contract_month": "202609",
                "expiry": "20260925",
                "local_symbol": "METU6",
                "con_id": 772435602,
            },
        )
    )

    assert mbt_result["classification"] == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert "duplicate_or_conflicting_working_order" in mbt_result["block_reasons"]
    assert met_result["classification"] == BROKER_MARKET_TRUTH_ENTRY_ALLOWED
    assert met_result["broker_truth"]["track_b_open_order_count"] == 1
    assert met_result["broker_truth"]["instrument_open_order_count"] == 0


def test_rates_position_blocks_same_rate_but_allows_other_rates_flat_start() -> None:
    rates_lanes = (
        "zf_globex_active_participation_long",
        "zn_globex_active_participation_long",
        "zb_globex_active_participation_long",
    )
    zf_position = _positions(
        rows=[
            {
                "account_id": "DUM882026",
                "security_type": "FUT",
                "symbol": "ZF",
                "local_symbol": "ZFU6",
                "quantity": "1",
            }
        ]
    )

    zf_result = evaluate_broker_market_truth_entry_authority(
        _input(
            lane_id="zf_globex_active_participation_long",
            instrument="ZF",
            active_profile_lane_ids=rates_lanes,
            broker_positions_snapshot=zf_position,
            runtime_price={
                "price": 106.83,
                "timestamp": "2026-06-11T14:00:00+00:00",
            },
            contract={
                "symbol": "ZF",
                "contract_month": "202609",
                "expiry": "20260930",
                "local_symbol": "ZFU6",
                "con_id": 842590380,
            },
        )
    )
    zn_result = evaluate_broker_market_truth_entry_authority(
        _input(
            lane_id="zn_globex_active_participation_long",
            instrument="ZN",
            active_profile_lane_ids=rates_lanes,
            broker_positions_snapshot=zf_position,
            runtime_price={
                "price": 110.50,
                "timestamp": "2026-06-11T14:00:00+00:00",
            },
            contract={
                "symbol": "ZN",
                "contract_month": "202609",
                "expiry": "20260930",
                "local_symbol": "ZNU6",
                "con_id": 842590388,
            },
        )
    )
    zb_result = evaluate_broker_market_truth_entry_authority(
        _input(
            lane_id="zb_globex_active_participation_long",
            instrument="ZB",
            active_profile_lane_ids=rates_lanes,
            broker_positions_snapshot=zf_position,
            runtime_price={
                "price": 117.25,
                "timestamp": "2026-06-11T14:00:00+00:00",
            },
            contract={
                "symbol": "ZB",
                "contract_month": "202609",
                "expiry": "20260930",
                "local_symbol": "ZBU6",
                "con_id": 842590397,
            },
        )
    )

    assert zf_result["classification"] == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert "broker_nonflat_flat_start_violation" in zf_result["block_reasons"]
    assert zn_result["classification"] == BROKER_MARKET_TRUTH_ENTRY_ALLOWED
    assert zn_result["broker_truth"]["instrument_nonflat_position_count"] == 0
    assert zb_result["classification"] == BROKER_MARKET_TRUTH_ENTRY_ALLOWED
    assert zb_result["broker_truth"]["instrument_nonflat_position_count"] == 0


def test_unknown_order_blocks() -> None:
    classification, reasons = _classification(open_order_truth={"unknown_open_order_count": 1})

    assert classification == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert "unknown_open_orders" in reasons


def test_missing_price_blocks() -> None:
    classification, reasons = _classification(runtime_price={})

    assert classification == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert "runtime_price_unavailable" in reasons


def test_unresolved_contract_blocks() -> None:
    classification, reasons = _classification(contract={"symbol": "MNQ", "contract_month": "202609"})

    assert classification == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert "unresolved_con_id" in reasons


def test_repo_authority_enriches_missing_con_id_from_zero_broker_position(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json",
        {"active_lane_ids": ["mnq_globex_active_participation_short"]},
    )
    _write_json(
        tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json",
        _positions(
            rows=[
                {
                    "account_id": "DUM882026",
                    "security_type": "FUT",
                    "symbol": "MNQ",
                    "local_symbol": "MNQU6",
                    "con_id": 793356225,
                    "expiry": "20260918",
                    "quantity": "0.0",
                    "multiplier": "2",
                    "currency": "USD",
                }
            ]
        ),
    )
    _write_json(
        tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json",
        _orders(),
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data/MNQ/1m/latest_runtime_candles.json",
        {
            "bars": [
                {
                    "bar_end": "2026-06-11T14:00:00+00:00",
                    "close": 29000.25,
                }
            ]
        },
    )
    _write_managed_exit_status(tmp_path)

    result = build_broker_market_truth_entry_authority_from_repo(
        repo_root=tmp_path,
        account_id="DUM882026",
        mode="PAPER",
        route_destination="ibkr_paper_bridge_submit_capable",
        execution_mode="IBKR_PAPER_BRIDGE",
        lane_id="mnq_globex_active_participation_short",
        instrument="MNQ",
        action="SELL_TO_OPEN",
        quantity=1,
        contract={"symbol": "MNQ", "contract_month": "202606", "exchange": "CME", "currency": "USD"},
        now=datetime(2026, 6, 11, 14, 1, tzinfo=timezone.utc),
    )

    assert result["classification"] == BROKER_MARKET_TRUTH_ENTRY_ALLOWED
    assert result["market_truth"]["contract"]["con_id"] == 793356225
    assert result["market_truth"]["contract"]["local_symbol"] == "MNQU6"


def test_repo_authority_enriches_missing_con_id_from_trade_ledger_identity(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json",
        {"active_lane_ids": ["mes_globex_active_participation_long"]},
    )
    _write_json(
        tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json",
        _positions(
            rows=[
                {
                    "account_id": "DUM882026",
                    "security_type": "FUT",
                    "symbol": "MES",
                    "local_symbol": "MESU6",
                    "expiry": "20260918",
                    "quantity": "0.0",
                    "currency": "USD",
                }
            ]
        ),
    )
    _write_json(
        tmp_path
        / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_broker_reconciled_live_position_status.json",
        {
            "broker_reconciled_state": "BROKER_AND_LIFECYCLE_FLAT",
            "positions_by_instrument": {
                "MES-U6": {
                    "instrument_family": "MES",
                    "local_symbol": "MESU6",
                    "con_id": 793356217,
                    "expiry": "20260918",
                    "exchange": "CME",
                    "currency": "USD",
                }
            },
        },
    )
    _write_json(tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json", _orders())
    _write_json(
        tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data/MES/1m/latest_runtime_candles.json",
        {"bars": [{"bar_end": "2026-06-11T14:00:00+00:00", "close": 7400.25}]},
    )
    _write_managed_exit_status(tmp_path)

    result = build_broker_market_truth_entry_authority_from_repo(
        repo_root=tmp_path,
        account_id="DUM882026",
        mode="PAPER",
        route_destination="ibkr_paper_bridge_submit_capable",
        execution_mode="IBKR_PAPER_BRIDGE",
        lane_id="mes_globex_active_participation_long",
        instrument="MES",
        action="BUY_TO_OPEN",
        quantity=1,
        contract={"symbol": "MES", "contract_month": "202606", "exchange": "CME", "currency": "USD"},
        now=datetime(2026, 6, 11, 14, 1, tzinfo=timezone.utc),
    )

    assert result["classification"] == BROKER_MARKET_TRUTH_ENTRY_ALLOWED
    assert result["market_truth"]["contract"]["con_id"] == 793356217
    assert result["market_truth"]["contract"]["local_symbol"] == "MESU6"


def test_repo_authority_prefers_next_contract_identity_when_broker_snapshot_lacks_symbol(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json",
        {"active_lane_ids": ["mnq_globex_active_participation_short"]},
    )
    _write_json(tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json", _positions())
    _write_json(
        tmp_path
        / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_broker_reconciled_paper_trade_summary.json",
        {
            "recent_trades": [
                {
                    "instrument_family": "MNQ",
                    "local_symbol": "MNQM6",
                    "con_id": 770561201,
                    "contract_key": "MNQ-M6",
                },
                {
                    "instrument_family": "MNQ",
                    "local_symbol": "MNQU6",
                    "con_id": 793356225,
                    "contract_key": "MNQ-U6",
                },
            ]
        },
    )
    _write_json(tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json", _orders())
    _write_json(
        tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data/MNQ/1m/latest_runtime_candles.json",
        {"bars": [{"bar_end": "2026-06-11T14:00:00+00:00", "close": 29000.25}]},
    )
    _write_managed_exit_status(tmp_path)

    result = build_broker_market_truth_entry_authority_from_repo(
        repo_root=tmp_path,
        account_id="DUM882026",
        mode="PAPER",
        route_destination="ibkr_paper_bridge_submit_capable",
        execution_mode="IBKR_PAPER_BRIDGE",
        lane_id="mnq_globex_active_participation_short",
        instrument="MNQ",
        action="SELL_TO_OPEN",
        quantity=1,
        contract={"symbol": "MNQ", "contract_month": "202606", "exchange": "CME", "currency": "USD"},
        now=datetime(2026, 6, 11, 14, 1, tzinfo=timezone.utc),
    )

    assert result["classification"] == BROKER_MARKET_TRUTH_ENTRY_ALLOWED
    assert result["market_truth"]["contract"]["con_id"] == 793356225
    assert result["market_truth"]["contract"]["local_symbol"] == "MNQU6"


def test_repo_authority_still_blocks_when_contract_identity_cannot_be_enriched(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json",
        {"active_lane_ids": ["mnq_globex_active_participation_short"]},
    )
    _write_json(tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json", _positions())
    _write_json(tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json", _orders())
    _write_json(
        tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {"classification": "NO_OPEN_ORDERS", "unknown_open_order_count": 0},
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data/MNQ/1m/latest_runtime_candles.json",
        {"bars": [{"bar_end": "2026-06-11T14:00:00+00:00", "close": 29000.25}]},
    )
    _write_managed_exit_status(tmp_path)

    result = build_broker_market_truth_entry_authority_from_repo(
        repo_root=tmp_path,
        account_id="DUM882026",
        mode="PAPER",
        route_destination="ibkr_paper_bridge_submit_capable",
        execution_mode="IBKR_PAPER_BRIDGE",
        lane_id="mnq_globex_active_participation_short",
        instrument="MNQ",
        action="SELL_TO_OPEN",
        quantity=1,
        contract={"symbol": "MNQ", "contract_month": "202606"},
        now=datetime(2026, 6, 11, 14, 1, tzinfo=timezone.utc),
    )

    assert result["classification"] == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert "unresolved_con_id" in result["block_reasons"]


def test_wrong_account_live_proof_and_non_paper_block() -> None:
    classification, reasons = _classification(
        account_id="DU_BAD",
        mode="LIVE",
        paper_only=False,
        live_money_eligible=True,
        paper_proof=True,
    )

    assert classification == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert {
        "wrong_account",
        "non_paper_mode",
        "paper_only_false",
        "live_money_eligible",
        "paper_proof_true",
    }.issubset(set(reasons))


def _write_managed_exit_status(path_root: Path, **overrides: object) -> None:
    _write_json(
        path_root / "outputs/track_b_execution_core/managed_exit_service/latest_managed_exit_service_status.json",
        _managed_exit_status(**overrides),
    )

def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_managed_exit_apply_blocked_blocks_entry_authority() -> None:
    result = evaluate_broker_market_truth_entry_authority(
        _input(managed_exit_status=_managed_exit_status(classification="APPLY_BLOCKED"))
    )

    assert result["classification"] == BROKER_MARKET_TRUTH_ENTRY_BLOCKED
    assert EXIT_CAPABILITY_APPLY_BLOCKED in result["block_reasons"]
    assert result["current_state_authority"]["exit_capability"]["ready"] is False
